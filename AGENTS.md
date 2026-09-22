# AGENTS.md

proxy-hub：通用代理池服务（Python 3.11+ / FastAPI）。本文面向 AI 编码代理，汇总架构、构建测试命令、代码约定与部署方式。更详尽的 API/配置说明见 [README.md](README.md)（单一事实源，改动后需同步）。

## 项目概述

从供应商 API（首发 qg.net）提取代理 IP。IP 过期时刻以供应商返回的每 IP `deadline` 为准，换套餐无需改代码；当前生产用 2min 套餐。健康探测后按**项目隔离**维护池：admin 注册项目发独立 token，每项目一个池、独立成本统计。Redis ZSET + 分布式锁实现**多副本主备**：同一时刻全局只有持锁副本抽 IP / 巡检，主宕机锁过期后备机自动补位。

**两种池类型**（项目级，`POST /admin/projects` 的 `pool_type`，默认 `shared`）：
- `shared`：qg.net 短效共享池（现有行为，多用户共用 IP）。
- `exclusive`：青果独享代理（地区锁）——IP 单用户独占、有租期（小时/天级），被风控时调供应商"更换IP"换新。需配置 `PROXY_HUB_EXCLUSIVE_API_URL`/`EXCLUSIVE_AUTH_KEY`/`EXCLUSIVE_AUTH_PWD` 三件套才装配独享 provider；未配置时独享项目补位跳过（记 warn，不 crash）。独享池与共享池同用滚动补位骨架（自然过期清理/提前补位/下发禁区不变），差异：旁路城市级自动换段（地区锁在提取时指定）、风控标记后走 `_exclusive_replace` 换新（节流 `exclusive_replace_cooldown_s`）、独享 IP 建联前每 IP QPS 门控（`exclusive_per_ip_qps`）。

**独享池按 target 路由，消费方零改动**：独享项目由 admin 注册，**它的 token 不给任何消费方**——纯作池的维护载体（refresh/monitor tick 照常补位/清过期）。消费方照常拿共享项目 token 连隧道；target（CONNECT authority / pull 的 `?target=`）命中热配置 `exclusive_targets`（默认 `["baidu", "quark"]`，空列表=关闭；双向子串匹配）时 `pick_proxy_routed` 优先从独享池选 IP，独享池拿不出 IP 自动落回共享池（共享兜底，无需消费方重试）。独享项目没有自己的 /activity 来源，路由命中时由 pool 代记活跃（60s 节流），否则按需驱动会把独享池当空闲项目停补归零。

池是**滚动补位**模型：ZSET 存 `pool:{project}`（member=代理 URL，score=该 IP 的 deadline_epoch），巡检每轮清自然过期 → 连续 N 轮判死剔除 → **缺几个补几个**，池保持固定长度（目标大小 = 档位 fetch_num 或项目 pool_size 覆盖）。IP 用满整个供应商生命周期，不做整批提前热切。批量补位（缺口≥3）分批抽取错峰（`topup_stagger_*` 热配置），且按"再过 alloc_min_ttl_s 仍可下发"**提前补位**（投影口径 = 剩余寿命 ≥ 2×alloc_min_ttl_s）——防同批 IP 同 deadline 齐到期形成过期波。体检**流式入池**（谁先探完谁先可下发）；低水位（可下发数 ≤ 目标一半，含空池/全禁区）冷却缩到 `low_pool_cooldown_s`（默认 3s）且 refresh_tick 压到 1s 一轮快补，无水持续 30s 打 ERROR。

两种消费模式：
- **隧道（push，推荐）**：proxy-hub **自建 HTTP 转发代理**（`forwarder.py`，默认 8101 端口，无 gost）。消费方代理指向 `http://<项目名>:<项目token>@<本机>:8101`；每连接按目标站防重轮换挑上游 IP，上游失败换 IP 重试 ≤3 次（connect/握手各 2s）。
- **Pull**：`GET /proxy`（Bearer 项目 token）取一个可用代理 URL，可选 `?target=` 参与防重；池空 503。

核心特性：
- **冷热分档**：peak/normal/offpeak 时段表调节奏降本；低谷 QPS 突增自动升 peak。
- **按需驱动 / 唤热**：空闲超 `idle_threshold_s` 停止补位巡检，池自然归零。消费方 `/activity` 或隧道建连重新唤醒；prism 侧 `ensure_warm()` 会主动检查 `/status` 并在 cold 时调 `/refresh`。
- **两阶段刷新**：`POST /refresh` 先补新 IP 再淘汰旧批次，刷新期间池子不空；锁被占时按 body `wait_s` 等待持锁方落地（等锁成功且池已可下发则直接复用不重复抽），补位撞供应商冷却且旧池不可下发时等短冷却后再试一次。
- **防抖判死**：连续 N 轮（默认 2）探测失败才剔除。
- **热配置**：`PUT /admin/config` 运行期生效（≤15s），带审计快照与回滚。

## 技术栈与构建

- Python ≥3.11；包管理用 `pyproject.toml`（hatchling），依赖：fastapi、uvicorn、httpx、redis、apscheduler、pydantic(-settings)。
- 构建/安装：`pip install -e ".[dev]"`
- 启动：`python -m proxy_hub.main` 或 `uvicorn proxy_hub.main:app --port 8100`
- 本地全量验证：`docker compose up --build`（proxy-hub + 外部 redis）

## 常用命令

```bash
# 测试（179 个用例；纯函数 + 内存 Redis stub + 真实 TCP 模拟上游，不依赖真实 Redis/供应商）
.venv/Scripts/python -m pytest -q

# Lint（ruff：E/F/I/UP，line-length=100，E501 忽略，py311）
ruff check proxy_hub tests
```

Windows 下用 `.venv/Scripts/python`（不是 `.venv/bin/python`）。

## 代码结构

```
proxy_hub/
  main.py            入口：FastAPI lifespan 装配 + APScheduler 2 个 tick（refresh/monitor）+ forwarder 启停
  pool.py            池核心：多项目 ZSET 滚动补位 + SET NX PX 分布式锁 + pick_proxy 防重分配；模块级状态由 configure() 注入
  projects.py        项目注册表：Redis hash `projects`（name→{token, pool_size, pool_type, created_at}），token 解析带 10s 缓存
  forwarder.py       自建 HTTP 转发代理：CONNECT/普通 HTTP 经上游代理转发，Proxy-Authorization 认项目
  config.py          env 配置（pydantic-settings，前缀 PROXY_HUB_）
  runtime_config.py  热配置：env 默认 + Redis config:runtime 覆盖合并，审计 list 保留最近 5 版可回滚，进程内 10s 缓存
  schedule.py        冷热分档纯函数：时段表解析（支持跨午夜）、resolve_tier、QPS 自适应、set_fetch_num
  health.py          三态探测（SEARCHABLE/REACHABLE/DEAD，流式读 body）、FailCounter 连续判死
  proxy_url.py       代理 URL 工具：make_proxy_url（server→URL）/ addr_and_auth（拆解）
  redis_client.py    Redis 单例，standalone/cluster 双模式（cluster 用非事务 pipeline，禁用跨 slot 多 key 命令）
  api/routes.py      HTTP 端点：消费端点（项目 token）+ /admin/config + /admin/projects CRUD
  providers/         供应商插件：base.py 定义 Provider 协议（fetch + 可选 release/replace），
                     qgnet.py 短效共享（"shared"），qgnet_exclusive.py 青果独享（"exclusive"）
  logging_setup.py   结构化日志（dev 文本 / prod JSON）+ 非阻塞 ES handler
  log_shipper.py     日志队列 → ES _bulk 批量落库（索引 proxyhub_logs），故障隔离（满则丢弃、退避）
tests/               conftest.py 提供 FakeRedis/FakePipeline 内存 stub（string/hash/list/zset + pipeline，锁的 NX 语义）
```

## 关键约定（改代码时必须遵守）

- **冷热分档/自适应/判死轮数等节奏参数走热配置**（`PUT /admin/config` → Redis 覆盖），不要为此改代码或加 env；env 只管基础设施（Redis、token、供应商 URL/账密、转发端口、探测目标）。独享池的更换冷却/每 IP QPS 门控也走热配置（`exclusive_replace_cooldown_s` / `exclusive_per_ip_qps`）。
- **独享池**（`pool_type=exclusive`）：provider 按池类型分键装配（`pool.configure({"shared": ..., "exclusive": ...})`），`_top_up_once` 按项目 `pool_type` 选 provider 与账密（独享用 `EXCLUSIVE_AUTH_*`）；独享不回退共享 provider。**消费方不感知独享池**：forwarder/pull 统一走 `pool.pick_proxy_routed`，target 命中热配置 `exclusive_targets` 才路由到独享项目池（独享项目定位缓存 30s，见 `_exclusive_project`）。`/block-signal` 上报的 url 可能属于独享池而非凭证项目池——`record_block` 按 URL 实际所在池记录（镜像不含时 ZSCORE 回源确认归属），独享池 IP 标记后照常触发 `_exclusive_replace`：青果 `/replace` 是**"释放全部再提取"的全池替换**语义，新 IP 整体 ZADD、旧 IP 差集 ZREM。独享 API 已按 doc/1843 实测校准（`/get` `/query` `/delete` `/replace`；`/get`/`/replace` 列表在 `data.ips`，`/query` 在 `data.tasks[*].ips`；`NO_AVAILABLE_CHANNEL`/`NO_RESOURCE_FOUND` 映射为空结果不进冷却）；**通道满（NO_AVAILABLE_CHANNEL）时 pool 回退调 `/query` 认养在用 IP 入池**（`provider.last_fetch_channel_full` 标记 → `alive_items()`，见 `_top_up_once` 独享路径，防服务重启/手工占通道后池填不上）。**独享池体检用轻量中立探针**（`_health.probe_liveness` / `probe_stream_liveness`，默认 generate_204 端点，env `EXCLUSIVE_HEALTH_URL`）：独享 IP 反复打搜索引擎探测页等于主动烧自己；独享禁用 slow-as-dead 降级（慢≠死），判死轮数 = `health_dead_after`+1。剩余待确认项（/delete 参数名）见 `providers/qgnet_exclusive.py` 顶部注释。
- **多项目隔离**：所有池状态 key 带 `{project}` 段（`pool:{project}` / `lock:monitor:{project}` / `state:*` / `stats:*` / `dashboard:cost:{project}:{date}`）；消费端点与隧道都靠项目 token 认项目，禁止无 token 的默认池。
- **主备安全模型**：池状态唯一事实源是 Redis ZSET；抽 IP/巡检剔除前必须抢锁（`SET NX PX`，锁带 TTL，按项目粒度）；供应商故障冷却写 Redis 跨副本生效。进程内镜像经 `sync_project` 回源。
- **滚动补位**：新增 IP 走 `_top_up`（批量防抖分批 → `_top_up_once`：fetch → filter_alive → ZADD score=deadline_epoch）；剔除走 ZREM/ZREMRANGEBYSCORE；目标大小 = 项目 pool_size 覆盖 > 档位 fetch_num。维护轮按"再过 alloc_min_ttl_s 仍可下发"投影水位提前补位（实现口径：剩余寿命 ≥ 2×alloc_min_ttl_s 才计入投影）。**低水位/紧急/"无水"一律按可下发数（`_allocatable`）判定而非 pool_size**——池非空但全在下发禁区视同无水（refresh_tick 快补、refresh 的 ok 口径、错峰防抖的紧急旁路都走这一口径）。不要再引入整批快照/提前热切。
- **Redis cluster 兼容**：生产是 Redis 集群，禁止跨 slot 多 key 命令（如 MGET），多 key 用 `asyncio.gather` 并发单 GET；pipeline 一律走 `redis_client.make_pipeline`。
- **时区**：qg.net 的 deadline 是北京时间无时区标识，比较前必须显式挂 `Asia/Shanghai`（`ZoneInfo`），容器默认 UTC 会算错 +8h。
- **代理 URL**：供应商返回的 `server`（中转网关 ip:port）才是要连的地址，`proxy_ip` 仅展示；必须包成 `http://[key:pwd@]server`（`proxy_url.make_proxy_url`），缺 scheme 的 URL 会让 httpx 直接抛异常。
- **日志规范**（与 prism/crawler 统一）：字段 level/logger/msg/service(`"proxy-hub"`)/host/pid + 追踪键 `batch_id`/`instance_id`/`query_id`（`extra=` 传入）+ 结构化块 `event`/`metrics`；ES 索引 `proxyhub_logs`。
- **探测目标勿换百度**（会全误杀）；默认 bing 三态判定，UA 用真实 Chrome UA。
- **风控反哺通道**：HTTPS 隧道加密，forwarder 数据面无法识别风控页——消费方引擎层识别后 `POST /block-signal`（body 带 target 引擎名片段），forwarder 的 `_recent_conns`（project × target 近 30s 建联映射，子串匹配）关联出上游 IP，`pool.record_block` **按目标站拉黑标记**（`_target_blocked`，pick_proxy 按 target 过滤、无人可选时忽略标记照发）；同一 IP 被 ≥3 站风控才真 ZREM（每项目每分钟限 3 个）。某 target 未标记候选归零时 pick_proxy 顺带触发**定向扩池**（`_schedule_targeted_topup` → 后台 `_targeted_topup`：热配置 `targeted_topup_*` 三旋钮——间隔/批量/池上限=目标大小×倍数；与 refresh/巡检共用项目锁、非阻塞拿不到就跳过），不然超严站（baidu ~80% 出生即被标记）可用率只能干等 TTL 换血。不要把这层改成数据面嗅探（CONNECT 后看不到内容），也不要改回一刀切剔除（被单站风控的 IP 对其他站可能干净）。
- **城市级自动换段**：marked 事件按 `_ip_area`（提取时记录的 area_code）滑窗聚点（Redis `state:area_block:{project}` hash），超阈值写动态排除集合（`state:area_ex:{project}` hash，值=放回时刻），`_top_up_once` 提取时与静态 area_ex 合并下发。TTL 自动放回 + 上限 `area_exclude_max`——别改成永久排除（风控漂移会让可选城市越排越少）。
- 新增供应商：实现 `providers/base.py` 的 `Provider` 协议（独享型产品另实现可选的 `release`/`replace`）并在 `main.py` 装配处注册到对应池类型键。

## 测试策略

- `pytest`（asyncio_mode=auto），测试全部可离线跑：不引 fakeredis，`tests/conftest.py` 手写最小 FakeRedis stub（string/hash/list/zset + pipeline，锁的 NX 语义）。
- `_reset_module_state` autouse fixture 每个测试前重置 pool/runtime_config/projects 模块级状态——**新增模块级全局变量时必须在此登记重置**。
- 决策逻辑（分档解析、判死计数、防重分配）做成纯函数/纯方法便于单测。
- forwarder 测试用真实 TCP（随机端口 + 模拟上游）；**回显数据无换行时断言用 `read(n)`，不要用 `readline()`**（会一直等到连接关闭）。

## 部署

- 生产：`cp .env.example .env` 填配置后 `docker compose up -d --build`；主备 = 第二台机器跑同一份 compose（同一 Redis，锁自动协调；两个副本各自监听 8101，消费方可指向任一）。
- Docker 约定（与 prism 相同）：代码 bind mount `.:/app` + uvicorn `--reload` 热重载；镜像走 daocloud/阿里云国内加速；**不要加 `# syntax=docker/dockerfile:1` 指令**（BuildKit 拉前端镜像在国内会失败）。
- 兼容 CentOS 7 + Docker 18.09：compose 里 `seccomp:unconfined` 等 `x-kernel-opts` 是旧内核绕 clone3 用的，宿主升级 Docker 20.10+ 前勿删。
- 端口：8100（REST API）、8101（隧道数据口，消费方配这个）。

## 与 prism 集成要点

prism 作为消费方需三类配置：`PROXY_SERVICE_URL`（控制面，可逗号分隔多副本）、`PROXY_PROJECT_TOKEN`（注册 "prism" 项目获得）、`PROXY_TUNNEL_URL`（数据面隧道，`http://prism:<token>@<host>:8101`）。交互节奏：

- 每个搜索请求节流调用 `/activity`（≥10s）。
- 空闲首查前 `ensure_warm()`：GET `/status` → cold 则 POST `/refresh`。
- 熔断 / `ProxyQualityError` 时异步 `/refresh`，并用 `PROXY_RETRY_EXTRA_BUDGET_S` 延长预算重试。

更详细的 prism 侧排错与预算说明见 prism `CLAUDE.md`。

## 常见问题

- **多副本是否需要选主？** 不需要。全局只有持 Redis 锁的副本抽 IP/巡检，主宕机锁过期（TTL 60s）后备机自动补位。
- **换供应商套餐要改代码吗？** 不需要。IP 有效期以供应商返回的 `deadline_epoch` 为准，换套餐只需更新 `PROVIDER_API_URL`。
- **为什么 offpeak 巡检间隔从 120s 改成 30s？** 2min 套餐配 120s 等于 IP 终生不再复检，死 IP 会残留池中。
- **隧道连接报 407？** 检查 `http://<项目名>:<项目token>@host:8101` 是否拼对，项目是否已在 proxy-hub 注册。
- **生产用 host 网络还是 bridge？** 默认 bridge + 端口映射 8100:8100 / 8101:8101；只有在需要大量源端口时才考虑 host 网络。

## 安全注意事项

- `PROXY_HUB_ADMIN_TOKEN` 为空时 admin 端点全部拒绝（安全默认）；`/admin/*` 一律 Bearer 鉴权。
- 项目 token 在 `POST /admin/projects` 响应里只完整返回一次；Redis 里存明文（与 admin token 同级运维安全模型）。
- `.env` 含真实凭据（Redis 密码、admin token、供应商 key），已在 .gitignore，**绝不提交、勿打印到日志**。
- Redis key 用前缀 `proxyhub:` 与 prism 的 `prism:` 隔离（生产共用 Redis 集群）。
