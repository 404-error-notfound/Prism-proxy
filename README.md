# proxy-hub

通用共享代理池服务：从供应商 API 提取短效代理 IP，健康探测，按**项目隔离池**（独立 token、独立成本），经 Redis ZSET 滚动补位 + 分布式锁实现**多副本主备**，支持冷热时段分档降本与运行期热配置。

## 解决什么问题

多个服务/多台机器各自维护代理池时：代理额度消耗 ×N、健康探测流量 ×N、各实例决策互相踩踏。proxy-hub 把代理池收敛为一个独立服务：

- **多项目隔离**：admin 注册项目拿到独立 token，每项目一个池（Redis ZSET）、独立成本统计、互不影响；项目池大小可单独覆盖。
- **滚动补位**：池子保持固定长度，IP 用满整个供应商生命周期（以 supplier deadline 为准；当前生产用 qg.net 2min 套餐）。巡检每轮清自然过期 → 判死剔除 → **缺几个补几个**；不做整批提前热切，不浪费配额。
- **两种消费模式**：
  - 隧道（推荐）：proxy-hub 自建 HTTP 转发代理监听固定端口（默认 8101），消费方代理指向 `http://<项目名>:<项目token>@<proxy-hub>:8101` 即可，每连接自动挑上游 IP，**按目标站防重轮换**（同一 IP 不连续打同一目标站，搜索引擎流量也生效）；
  - Pull：`GET /proxy`（带项目 token）直接取一个可用代理 URL，可选 `?target=` 声明目标站参与防重。
- **全局只抽一份 IP**：Redis 锁（`SET NX PX`，按项目粒度）保证同一时刻只有一个副本调供应商 API；主备副本跑相同逻辑，主宕机锁过期后备机自动补位，无需选主。
- **冷热分档**：按时段表（peak/normal/offpeak）调整巡检频率与目标池大小；低谷期 QPS 突增自动升档。
- **后台热配置**：`PUT /admin/config` 改时段表/档位参数/判死轮数，≤15s 生效，带审计快照与回滚。
- **巡检稳定**：IP 连续 N 轮（默认 2）探测失败才剔除，消除单次抖动误杀；转发层连接失败也计入判死轮数。

## 代理 IP 生命周期

proxy-hub 首发对接 qg.net 短效住宅代理，当前生产用 2min 套餐（以 supplier deadline 为唯一事实源，换套餐无需改代码）：

1. **提取**：按项目从供应商 API 取 `num` 个 IP。
2. **体检**：并发三态探测（见下节），**流式入池**——谁先探完谁先写入 Redis ZSET（不等整批探完，唤热/空池快补的首个可用 IP 从 ~8s 缩到 ~2s），只把活 IP 给消费方。
3. **滚动补位**：ZSET `pool:{project}` 以 `deadline_epoch` 为 score，IP 自然到期自动清理；中途死的 IP 由巡检剔除，**缺几个补几个**。
4. **分批防抖**（热配置 `topup_stagger_*`）：缺口 ≥3 且池未空时，把一次 top-up 拆成 2 个一批、批间随机 sleep 2-5s 的小批抽取——同批 IP 的 deadline 随抽取时刻错开，消除"同批齐到期 → 整批补位 → 2min 后重演"的过期波。池空/紧急时整批快补（不防抖）；**紧急按"可下发数"判定**——池里全是躺在下发禁区的将死 IP 时视同紧急，不再错峰（2026-09-08 禁区波：带错峰的 refresh 全程 8-20s，拖爆消费方等待预算）。
5. **提前补位**：维护轮按"再过 `alloc_min_ttl_s` 仍可下发"投影池水位（实现口径：剩余寿命 ≥ 2×`alloc_min_ttl_s` 才计入投影）——即将到期的 IP 不提前杀（用满寿命），但新 IP 提前到位，避免到期波后才补位的真空窗（小池下唯一幸存者可能正躺在下发禁区里）。
6. **低水位快补**：可下发 IP 不到目标一半（含空池）时，供应商失败冷却从 15s 缩到 `low_pool_cooldown_s`（默认 3s，热配置），且 refresh_tick 快补节奏压到 1s 一轮——无水没有"省配额"的余地，恢复时间逼近"供应商往返（fetch ~2.5s + 首个探活 ~2s）"的物理下限。**"无水"= 无可下发 IP**：池非空但全体进禁区（齐到期波）同样走快补与告警，不等 15s 巡检。无水持续 30s 打 ERROR（5 分钟节流），可直接配 ES 告警。
7. **下发禁区 + 禁区回退**：`alloc_min_ttl_s=25s`（热配置），剩余寿命低于 25s 的 IP 不发放（防"给到就死"）；**全体躺进禁区时回退发剩余寿命最长的那个**——将死 IP 好歹值一次 forwarder 换 IP 重试（引擎侧有质量检测兜底），而返回"无可用上游"是隧道秒拒 502，连搏的机会都没有（2026-09-04 实测根因）。
8. **风控反哺拉黑**：HTTPS 隧道加密，forwarder 看不到风控页——消费方引擎层识别验证码/秒拒后经 `POST /block-signal` 上报目标站，forwarder 近期建联映射（project × target，近 30s）关联出当时用的上游 IP，**按目标站拉黑标记**：该 IP 只是不再派给这个站，其他站照常用（风控按段×目标站聚集，一刀切剔除浪费 IP）；同一 IP 被 ≥3 个目标站风控才真剔出池（每项目每分钟限剔 3 个防信号风暴）。某 target 的未标记候选归零（池内 IP 全被该站标记、fail-open 在发脏 IP）时 pick_proxy 顺带触发**定向扩池**（热配置 `targeted_topup_*`：开关/间隔/批量/池上限=目标大小×倍数），主动提取一小批新 IP 而非干等 TTL 换血——共享池对 baidu 这类超严站 ~80% 出生即被标记，不扩池该站可用率锁死在 0 附近。
9. **城市级自动换段**（热配置 `auto_area_exclude_*`）：风控标记按出口城市滑窗聚点——某城市 10 分钟内 ≥3 个不同 IP 被风控 → 提取时动态 `area_ex` 排除该城市，4 小时 TTL 到期自动放回（搜索站对 IP 段的风控封禁通常小时级起步，放回过短等于把脏段又收回来；实测风控在城市级有持续性——2026-09-14 汕头段连续两天 baidu 风控率 75%）。排除上限 20 个城市防膨胀，当前生效清单见 `/status` 的 `dynamic_area_ex`。
9. **用完即走**：不做整批提前热切，IP 用满整个生命周期，不浪费配额。

> 为什么默认 25s？qg.net 短效套餐实际死亡时刻在 deadline 前 25-35s 已开始波动，25s 卡波动下沿：更低的必然将死禁发，25-35 之间的交给禁区回退 + 换 IP 重试兜底。

## 快速开始

```bash
pip install -e ".[dev]"

# 最小配置
export PROXY_HUB_REDIS_URL=redis://localhost:6379/0
export PROXY_HUB_PROVIDER_API_URL='https://share.proxy.qg.net/get?key=YOUR_KEY&num=6&distinct=true'
export PROXY_HUB_ADMIN_TOKEN=change-me

python -m proxy_hub.main   # 或 uvicorn proxy_hub.main:app --port 8100
```

注册项目并接入：

```bash
# 1. admin 注册项目（token 仅此一次完整返回，当场保存）
curl -X POST http://localhost:8100/admin/projects \
  -H "Authorization: Bearer change-me" -H "Content-Type: application/json" \
  -d '{"name": "crawler", "pool_size": 0}'    # pool_size=0 跟随档位 fetch_num

# 2a. 隧道模式：消费方代理指向（searxng/httpx/requests 通用）
#     http://crawler:<项目token>@<proxy-hub>:8101

# 2b. Pull 模式：自取 IP
curl http://localhost:8100/proxy?target=baidu.com \
  -H "Authorization: Bearer <项目token>"
```

**本机无 Docker 拉网时**（镜像源不可达）：不用 Docker 也能开发——本机 Redis + venv 直跑：

```bash
pip install -e ".[dev]"
set PROXY_HUB_REDIS_URL=redis://127.0.0.1:6379/0   # 注意用 127.0.0.1：localhost 可能解析到 ::1
set PROXY_HUB_ADMIN_TOKEN=dev-token
uvicorn proxy_hub.main:app --reload --port 8100
```

生产部署（与 prism 同一套 docker 约定：代码 bind mount + uvicorn `--reload` 热重载）：

```bash
cp .env.example .env   # 填 Redis（可与 prism 共享实例）、ADMIN_TOKEN、供应商 API
docker compose up -d --build
# 主备：在第二台机器同样跑一次即可（同一 Redis，分布式锁自动协调，无需选主；
# 两个副本各自监听 8101 隧道口，消费方可指向任一副本）
```

## API

消费端点一律 `Authorization: Bearer <项目token>`；admin 端点一律 `Authorization: Bearer <ADMIN_TOKEN>`。

| 端点 | 说明 |
|---|---|
| `GET /healthz` | 健康检查（无鉴权） |
| `GET /proxy?target=` | 取一个可用代理 URL（池空 503）；target 可选，声明目标站参与防重轮换 |
| `GET /proxies` | 当前项目全池 |
| `POST /activity` | 消费方活跃信号（建议进程内节流 ≥10s/次），驱动按需补位与 QPS 升档；走隧道的消费方不用调（建连即隐式活跃）。可选 JSON body `{"query_id": "...", "query": "..."}`：query_id 精确锚点（日志落 `query_id` 字段 term 可达），query 查询词人可读锚点（进 `metrics.query` 模糊可搜，不必翻消费方日志复制 uuid） |
| `POST /refresh` | 强制换批：新 IP 补满后再淘汰旧批次（两阶段替换，刷新期间池子不空），IP 大面积失效时用。响应 `{"ok", "pool_size", "executed", "waited"}`：executed=本次真正换批、waited=等过他方刷新。可选 body `{"query_id", "query", "wait_s"}`：wait_s（≤30）= 锁被占时服务端最多等待秒数——持锁方正在刷就等它落地（等锁成功且池已可下发则直接复用，不重复抽），胜过消费方秒拒后自己再撞同一坏窗口；query_id/query 为日志关联锚点（同上） |
| `POST /block-signal` | 风控信号：消费方引擎层识别"连得上但被目标站验证码/秒拒"时上报（HTTPS 隧道加密，forwarder 数据面看不到风控页，只能消费方反哺）。body `{"target": "baidu", "query_id", "query"}`：target 为引擎名片段，按 forwarder 近期建联映射（project × target，近 30s，子串匹配）关联出当时用的上游 IP，**按目标站拉黑标记**（该 IP 不再派给此站，其他站照用；被 ≥3 站风控才真剔除，每项目每分钟限剔 3 个）。响应 `{"ok", "marked", "killed", "pool_size"}` |
| `GET /status` | 项目池状态 / 当前档位 / 10 分钟 QPS / 当日成本计数 |
| `GET /admin/config` | 生效配置 + 覆盖项 |
| `PUT /admin/config` | 部分更新热配置（非法值 400 拒绝） |
| `POST /admin/config/rollback` | 回滚上一版（保留最近 5 版快照） |
| `DELETE /admin/config` | 清除覆盖回 env 默认 |
| `POST /admin/projects` | 注册项目 `{name, pool_size, pool_type}`，201 返回 token（仅此一次完整返回）。`pool_type`：`"shared"`（默认，短效共享池）/ `"exclusive"`（独享池，需配 `EXCLUSIVE_*` env），非法值 400 |
| `GET /admin/projects` | 项目列表 |
| `DELETE /admin/projects/{name}` | 注销 token + 清池 |

## Redis 存结构（前缀 `proxyhub:`，单 key 操作保 cluster 兼容）

| key | 类型 | 说明 |
|---|---|---|
| `projects` | hash | 项目注册表：field=项目名，value=JSON `{token, pool_size, pool_type, created_at}`（老数据无 pool_type 按 shared 处理） |
| `pool:{project}` | zset | 项目池：member=代理 URL，score=该 IP 的 deadline_epoch（唯一事实源） |
| `lock:monitor:{project}` | string | 巡检/补位分布式锁（SET NX PX） |
| `state:last_fail:{project}` | string | 供应商故障冷却（跨副本生效） |
| `state:last_activity:{project}` | string | 该项目最近活跃 epoch |
| `stats:activity:{project}:{minute}` | string | /activity 按分钟计数（QPS 滑窗） |
| `dashboard:cost:{project}:{date}` | hash | 每项目每日成本（fetch_count/ip_count，分档 field） |

## 巡检、补位与冷热分档

proxy-hub 靠两个 tick 维护池：

- **`refresh_tick`**：池空/全灭兜底。默认每 5s 轮询一次，返回当前档位决定的下次间隔。
- **`monitor_tick`**：健康巡检 + 滚动补位。启动间隔由档位 `health_recheck_interval_s` 决定（peak/normal 15s，offpeak 30s），APScheduler 按返回值动态调度。

时段表格式：`peak=08:00-22:00,offpeak=22:00-08:00`（支持跨午夜，未覆盖时段 = normal）。

| 参数 | peak | normal | offpeak |
|---|---|---|---|
| `check_interval_s` 池缺口轮询 | 5 | 5 | 15 |
| `health_recheck_interval_s` 健康巡检 | 15 | 15 | 30 |
| `fetch_num` 目标池大小 | 6 | 4 | 3 |
| `idle_threshold_s` 空闲阈值 | 300 | 300 | 600 |

低谷档近 10 分钟 QPS（按 `/activity` 计数）≥ `adaptive_qps_threshold`（默认 0.5）时自动升 peak 档。

**判死防抖**：IP 需连续 `health_dead_after` 轮（默认 2）探测失败才会剔除，消除单次抖动误杀。

**巡检设计原则**：巡检只消耗池内现有 IP 的探测流量，不触发新提取，因此频率不影响 IP 成本。`health_recheck_interval_s` 不应超过套餐时效，否则死 IP 会长期残留。

**按需驱动（空闲零消耗）**：项目距最近活跃（`/activity` 或隧道建连，隐式活跃）超过档位 `idle_threshold_s` → 停止补位/巡检 → 池内 IP 自然过期归零 → 空闲期零消耗。再次活跃时由消费方唤热（活跃信号或 `/refresh`，如 prism 侧的 `ensure_warm`）。

热配置示例（≤15s 生效）：

```bash
curl -X PUT http://localhost:8100/admin/config \
  -H "Authorization: Bearer change-me" -H "Content-Type: application/json" \
  -d '{"schedule": "peak=08:00-22:00,offpeak=22:00-08:00", "health_dead_after": 3, "alloc_min_ttl_s": 25}'
```

## 健康探测三态

所有 IP 入池前和巡检时都经一次真实 bing 搜索体检：

| 状态 | 分值 | 判定 | 用途 |
|---|---|---|---|
| **SEARCHABLE** | 2 | 命中 `id="b_results"` / `class="b_algo"` 或 body ≥20KB | 优先使用 |
| **REACHABLE** | 1 | 拿到任意响应（含 403/验证页） | 兜底使用，**绝不让池子空** |
| **DEAD** | 0 | 连接失败或探测全程 > `health_max_latency_s`（默认 2s） | 丢弃 |

- 探测 URL 默认 `https://cn.bing.com/search?q=新闻`；**不要改成百度**，否则可能全误杀。
- 流式读 body：拿到响应头即保底 REACHABLE，读够 20KB 即判 SEARCHABLE，不必等整页下完。
- 转发层连接失败也会记入判死计数，连续 2 轮确认死亡后剔除。

## 目标站防重轮换

每个 IP 在其生命周期内记录服务过的目标站（进程内，隧道取 CONNECT host、pull 取 `?target=`）。分配时优先发没服务过该目标的 IP；池内 IP 都服务过则清记录开第二轮。搜索引擎风控按"单 IP 对单站频率"计，此机制把同一 IP 的压力摊到不同站点。

## 接入新供应商

实现 `proxy_hub/providers/base.py` 的 `Provider` 协议（`fetch(num) -> list[ProxyItem]`；独享型产品另实现可选的 `release`/`replace`），参照 `providers/qgnet.py` / `providers/qgnet_exclusive.py`，并在 `main.py` 的 provider 装配处注册到对应池类型键（`"shared"` / `"exclusive"`）。

## 配置全表（env，`PROXY_HUB_` 前缀）

| 变量 | 默认 | 说明 |
|---|---|---|
| `REDIS_URL` / `REDIS_PASSWORD` / `REDIS_KEY_PREFIX` | `redis://localhost:6379/0` / 空 / `proxyhub:` | Redis 连接与命名空间 |
| `ADMIN_TOKEN` | 空（admin 端点全拒） | Bearer token |
| `PROVIDER` / `PROVIDER_API_URL` | `qgnet` / 空 | 供应商插件与提取 API（空=禁抓取） |
| `PROVIDER_AUTH_KEY` / `PROVIDER_AUTH_PWD` | 空 | 供应商账密鉴权；留空走 IP 白名单 |
| `EXCLUSIVE_API_URL` / `EXCLUSIVE_AUTH_KEY` / `EXCLUSIVE_AUTH_PWD` | 空 | 青果独享代理（地区锁，基址 `https://exclusive.proxy.qg.net`，doc/1843）：`EXCLUSIVE_API_URL` 直接填控制台生成的完整提取链接（`.../get?key=...`，provider 会剥掉端点名归一基址，裸基址填法也支持）；`AUTH_KEY/PWD` 是**代理建连账密**（控制台"代理验证-账密"，独享代理不带账密建连 407），不是 API key。配了才启用独享池，未配置时独享项目补位跳过不 crash |
| `EXCLUSIVE_DEFAULT_TTL_S` | `86400` | 独享 IP 默认租期（秒），仅提取响应缺到期字段时用；正常以响应的 deadline 为准 |
| `EXCLUSIVE_HEALTH_URL` | `https://connect.rom.miui.com/generate_204` | 独享池体检探针（轻量中立连通性探测，204 无反爬）：独享 IP 反复打搜索引擎探测页等于主动烧自己；独享池禁用 slow-as-dead（慢≠死），判死轮数 = `health_dead_after`+1 |
| `IP_TTL_FALLBACK_S` | `60` | IP 有效期兜底（秒），仅供应商响应缺 deadline 时用；正常以每 IP 的 deadline 为准，换套餐不用改 |
| `FORWARD_PORT` | `8101` | 隧道监听端口（自建转发代理）；0=纯 pull 模式 |
| `FORWARD_MAX_CONNS` | `512` | 隧道并发连接上限 |
| `HEALTH_CHECK_URL` / `HEALTH_CHECK_MARKERS` / `HEALTH_CHECK_TIMEOUT_S` / `HEALTH_MIN_BODY_BYTES` | bing / b_results,b_algo / 4s / 20000 | 三态探测目标与判定 |
| `HEALTH_MAX_LATENCY_S` | `2.0` | 体检延迟门槛：探测全程超过此值按死处理（对齐转发层 2s connect 预算，0=不启用） |
| `FAIL_COOLDOWN_S` | 15 | 供应商故障冷却（跨副本生效） |
| `REFRESH_LOCK_TTL_S` | 60 | 维护锁 TTL（持锁副本宕机后的补位时延上限） |
| `LOG_ES_ENABLED` / `LOG_ES_URL` / `LOG_ES_INDEX` / `LOG_ES_MIN_LEVEL` / `LOG_ES_BATCH_SIZE` / `LOG_ES_FLUSH_INTERVAL_S` | `false` / `http://localhost:9200` / `proxyhub_logs` / `INFO` / 100 / 5s | ES 日志落库（可选）：结构化 JSON 日志经非阻塞队列异步批量写独立索引 |

时段表 / 三档参数 / 自适应阈值 / 判死轮数 / 下发禁区（`alloc_min_ttl_s`，默认 25s：IP 剩余寿命低于此值不再发放，全体进禁区时回退发剩余寿命最长的）/ 分批补位防抖（`topup_stagger_min_n`=3、`topup_chunk_size`=2、`topup_stagger_delay_min_s`=2、`topup_stagger_delay_max_s`=5）/ 低水位快冷却（`low_pool_cooldown_s`=3：池 ≤ 目标一半时供应商失败冷却按此值而非默认 15s）/ 城市级自动换段（`auto_area_exclude_enabled`=true、`area_block_window_s`=600、`area_block_threshold`=3、`area_exclude_ttl_s`=14400、`area_exclude_max`=20）/ 定向扩池（`targeted_topup_enabled`=true、`targeted_topup_min_interval_s`=30、`targeted_topup_batch`=3、`targeted_topup_pool_cap_multiplier`=2.0）/ 独享池（`exclusive_targets`=["baidu","quark"] 按 target 路由名单，空=关闭；`exclusive_replace_cooldown_s`=60 风控后更换IP的单 IP 冷却；`exclusive_per_ip_qps`=0.2 独享 IP 建联门控，0=关闭）走**热配置**（`/admin/config`），不做 env。

## 独享代理池（pool_type=exclusive，按 target 自动路由）

给 baidu/quark 这类超严站用的青果**独享代理**（地区锁）：IP 单用户独占、有租期（小时/天级，到期失效可再提取）。**消费方零改动**——不需要独享项目 token：

1. admin 注册独享项目：`POST /admin/projects {"name": "expool", "pool_type": "exclusive"}`。**该项目的 token 不给任何消费方**，项目纯作池的维护载体（refresh/monitor tick 照常补位、清过期）。
2. 消费方照常拿共享项目 token 连隧道（或 pull）。target（CONNECT authority / `?target=`）命中热配置 `exclusive_targets`（默认 `["baidu", "quark"]`，空列表=关闭；双向子串匹配，"baidu" ⊂ "www.baidu.com"）时，proxy-hub 内部自动从独享池选 IP；独享池拿不出 IP 时**自动落回共享池**（共享兜底，消费方无需重试）。
3. 独享池维护与共享池同用滚动补位骨架（自然过期清理 / 提前补位 / 下发禁区 / 冷热分档都不变），差异：
   - 走独立的独享 provider（`EXCLUSIVE_*` env 装配）与独享账密；不回退共享 provider。
   - 旁路城市级自动换段：独享地区锁在提取时指定，不做动态 `area_ex`。
   - IP 被风控标记（`/block-signal`，按 URL 实际所在池记录归属）后异步调供应商"更换IP"——注意青果 `/replace` 语义是**释放全部已提取资源再重新提取**（全池替换）：返回的新 IP 列表整体 ZADD，旧 IP 按差集 ZREM（不误删刚入池的新 IP），节流 `exclusive_replace_cooldown_s`（按项目计）；失败仅记日志，旧 IP 留在池里靠标记兜底。
   - 独享 IP 建联前做每 IP QPS 门控（`exclusive_per_ip_qps` 默认 0.2/s，按 upstream URL 池归属判定——命中名单的 baidu/quark 流量被门控，其他引擎不受影响）。
   - 通道满自愈：`/get` 返回 `NO_AVAILABLE_CHANNEL`（通道已满）时不算失败不进冷却，回退调 `/query` 把在用 IP **认养进池**（与正常提取同走体检 + ZADD）——服务重启、Redis 池被清、手工提取占用通道等场景都能自愈；`NO_RESOURCE_FOUND` 等其他稳态空结果不认养。
   - 体检用**轻量中立探针**（默认 `https://connect.rom.miui.com/generate_204`，env `EXCLUSIVE_HEALTH_URL` 可覆盖）：拿到任意 HTTP 响应即活，**禁用 slow-as-dead 降级**（独享 IP 慢≠死），判死轮数 = `health_dead_after`+1。独享 IP 7×24 被每 5~15s 打一次搜索引擎探测页等于主动烧自己（2026-09-21 测试服实测独享池 5-15s 一轮"认养→误杀→再认养"抖动循环）。
4. 独享项目没有自己的消费方活跃信号——路由命中时由 pool 代记活跃（60s 节流），保证按需驱动不会把独享池当空闲项目停补。

> 接口形态已按真实 API 校准（doc/1843 + 调试工具实测）：基址 `https://exclusive.proxy.qg.net`，路径 `/get` `/query` `/delete` `/replace`；`/get`/`/replace` 列表在 `data.ips`，`/query` 在 `data.tasks[*].ips`（已拍平解析）；`NO_AVAILABLE_CHANNEL`（通道已满）/ `NO_RESOURCE_FOUND` 是稳态正常空结果，返回空列表不进失败冷却。剩余待真实凭证确认项：`/delete` 的 IP 参数名（当前同时带 `ip=` 与 `server=`）。

## 与消费方日志关联（跨链路排查）

滚动补位模型下池维护与单个查询解耦，数据面连接默认只有时间维度。确定性关联靠两个机制：

1. **query_id 锚点**（精确）：消费方在 `/refresh`、`/activity` 带 `{"query_id": "..."}`，proxyhub_logs 直接按 `query_id` 检索（prism 侧自动携带）。例：`query_id : "b368c0ab-..."` → 看到该查询触发的刷新/活跃信号与同一秒的池事件。
2. **query 锚点**（人可读）：信号同时携带查询词（prism 自动，落 `metrics.query`），直接在日志里搜"asyncio"之类关键词即可定位那批池事件——不必先翻消费方日志复制 uuid。注意 `/activity` 有 10s 节流，只锚定踩中节流边界的查询；精确归因仍靠 query_id。
2. **forwarder 连接日志**：每条隧道连接一行 INFO：`forwarder: conn <project> <client_ip> → <target_host> via <upstream_ip> <ms>ms`（metrics 含 project/target/upstream/client/elapsed_ms）。client_ip 对应消费方 SearXNG 实例（docker 网络内固定），与其自身日志按时间窗拼接即得"哪个查询走了哪个上游 IP"。

## 测试

```bash
pytest          # 纯函数 + 内存 Redis stub + 真实 TCP 模拟上游，不依赖真实 Redis/供应商
```

## 主备与故障语义

- **Redis 分布式锁**：抽 IP / 巡检前抢 `lock:monitor:{project}`（`SET NX PX`）；锁 TTL 60s，主宕机后 ≤60s 内备机接管。
- **池状态唯一事实源**：ZSET 在 Redis，副本重启/接管先 `sync_project`，不重复抽 IP。
- **供应商故障冷却**：`state:last_fail:{project}` 跨副本生效，防止多副本轮番狂抽烧额度。
- **两阶段替换**：`POST /refresh` 先补新 IP 入池，再淘汰旧批次，刷新期间池子不空。锁被占时按 `wait_s` 等待持锁方落地（防消费方被秒拒后重试又撞同一坏窗口）；等锁成功且池已可下发则直接复用，不重复抽。
- **服务整体不可用时**：隧道连接失败（502），消费方应处理为重试/降级；pull 模式消费方应将 `GET /proxy` 503 处理为"直连/降级"。
- **target 防重历史是进程内的**：多副本各自监听隧道、独立分配，副本切换后历史重来（影响有界 ≈ 单 IP 供应商生命周期，当前 2min 套餐）。

## 故障排查速查

| 症状 | 原因 | 解法 |
|---|---|---|
| `all fetched proxy IPs failed health check` | 供应商 key/网络/探测 URL 问题 | 查 `PROVIDER_API_URL`、key 余额；确认 `HEALTH_CHECK_URL` 是 bing 不是百度 |
| `refresh skipped, maintenance lock held` | 主备锁竞争，另一副本正在维护 | 正常现象；持续超锁 TTL 则检查是否有僵死副本 |
| `pool_size=0` 长时间不补 | 项目空闲超 `idle_threshold_s` 未收到 `/activity` | 触发 `/activity` 或 `/refresh`；检查 prism URL/token |
| 隧道里 `tried 0 proxies` / 上游 TimeoutError | 池真空（全部自然过期），禁区回退也无 IP 可发 | 立即 `POST /refresh`（带 `wait_s`）；检查 `/status` 的 `available` |
| 消费方 407 | 隧道账密错误 / 项目未注册 | 确认 `http://<项目名>:<项目token>@host:8101` |

## 从旧版（gost 批次池）迁移

破坏性变更，升级时注意：

- 消费端点全部要求项目 token（先 `POST /admin/projects` 注册）；旧的无鉴权调用一律 403。
- gost sidecar 已删除，隧道改为 proxy-hub 自建转发代理（仍 8101 端口）；消费方代理地址改为 `http://<项目名>:<项目token>@<host>:8101`。
- `PROXY_HUB_GOST_API_URLS` env 删除；旧 Redis key（`pool:current` 等）不再使用，可手工清理。
- 热配置字段 `ttl_early_ratio` 删除（滚动补位下 IP 用满生命周期，无提前热切）。
