"""池核心 —— 多项目 ZSET 滚动补位池 + 分布式锁 + 巡检/补位 tick。

存储模型（每项目一个 ZSET，唯一事实源在 Redis）：
  `pool:{project}`  member=代理 URL，score=该 IP 的 deadline_epoch。
  巡检每轮：清自然过期（ZREMRANGEBYSCORE）→ 探测判死剔除（ZREM）→
  缺几个补几个（fetch deficit 后 ZADD），池子保持固定长度，永远有可用 IP。
  IP 用满整个供应商生命周期（过期时刻以供应商返回的每 IP deadline 为准），
  不做提前热切丢弃。

多副本（主备）安全模型：
  - 任何副本宕机，其他副本读 ZSET 直接续管，不重复抽 IP。
  - 巡检/补位前抢锁（SET NX PX，锁带 TTL，按项目粒度）。
  - 供应商故障冷却写 Redis（`state:last_fail:{project}`），跨副本生效。
  - target 防重历史是进程内的：各副本独立监听隧道、独立分配，无需共享。

循环以 tick 函数形式实现（refresh_tick/monitor_tick），由 main.py 的
APScheduler 按"当前档位参数"动态调间隔——每个 tick 结束返回下一次间隔秒数。
"""
from __future__ import annotations

import asyncio
import logging
import random
import socket
import time
import uuid
from typing import Any, NamedTuple

from proxy_hub import projects, runtime_config
from proxy_hub.config import settings
from proxy_hub.health import (
    PROXY_DEAD,
    PROXY_REACHABLE,
    PROXY_SEARCHABLE,
    FailCounter,
    HealthChecker,
)
from proxy_hub.providers.base import Provider
from proxy_hub.proxy_url import make_proxy_url
from proxy_hub.redis_client import get_redis, make_pipeline, redis_key
from proxy_hub.schedule import TierParams, resolve_tier

logger = logging.getLogger(__name__)

# 实例标识：锁持有者身份标记
INSTANCE_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

# ---------------------------------------------------------------------------
# Redis keys（都过 redis_key 加前缀；{project} 段按项目隔离）
# ---------------------------------------------------------------------------
K_POOL = "pool:{project}"                    # ZSET：项目池（member=URL, score=deadline）
K_LOCK_MONITOR = "lock:monitor:{project}"    # 巡检/补位锁
K_LAST_FAIL = "state:last_fail:{project}"    # 最近一次补位失败 epoch（跨副本冷却）
K_LAST_ACTIVITY = "state:last_activity:{project}"  # 该项目消费方最近活跃 epoch
K_QPS_MIN = "stats:activity:{project}:{minute}"    # /activity 按分钟计数
K_COST_DAY = "dashboard:cost:{project}:{date}"     # 成本埋点 hash
K_AREA_BLOCK = "state:area_block:{project}"  # HASH field=f"{area_code}|{url}" value=标记 epoch（滑窗聚点）
K_AREA_EX = "state:area_ex:{project}"        # HASH field=area_code value=排除截止 epoch（动态换段）

# deadline 缺失/异常时的有效期兜底读 settings.ip_ttl_fallback_s（env
# PROXY_HUB_IP_TTL_FALLBACK_S，默认 60s）；正常路径以供应商
# 返回的每 IP deadline 为准，换套餐不用改配置。
# 分配安全边：剩余寿命低于此值（秒）的 IP 不发放（防给到客户端就死，下发禁区）
# 由 runtime config 的 alloc_min_ttl_s 驱动，refresh_tick 每轮从配置刷新
# （10s 缓存，热生效 ≤15s）
_alloc_min_ttl_s = 25.0
# /activity 计数 key 保留窗口（QPS 滑窗 10 分钟 + 余量）
_QPS_KEY_TTL_S = 1200
_COST_KEY_TTL_S = 35 * 86400

# ---------------------------------------------------------------------------
# 模块级装配（main.py startup 时 configure）
# ---------------------------------------------------------------------------
_providers: dict[str, Provider] = {}  # "shared"（短效共享）/ "exclusive"（独享）
_health: HealthChecker | None = None

# 进程内镜像（pull API / 隧道分配用；每 tick 从 Redis ZSET 同步）
# {project: [(url, deadline_epoch), ...]}
_pools: dict[str, list[tuple[str, float]]] = {}
_rr_index: dict[str, int] = {}
# target 防重：{project: {url: {target, ...}}} —— 该 IP 本轮生命周期内服务过的目标
_target_hist: dict[str, dict[str, set[str]]] = {}
_fail_counters: dict[str, FailCounter] = {}
_last_fail_mono: dict[str, float] = {}       # 本进程补位失败冷却（monotonic）
_last_tier: dict[str, str] = {}
# 空池告警：project → 首次发现空池/上次 ERROR 告警的 monotonic
_empty_since: dict[str, float] = {}
_empty_alerted: dict[str, float] = {}
EMPTY_ALERT_AFTER_S = 30.0    # 活跃项目空池持续多久打 ERROR
EMPTY_ALERT_REPEAT_S = 300.0  # ERROR 节流：同一项目最短重复间隔
# has_recent_activity 的 Redis 读取缓存（10s/项目），避免每 tick 多次 GET
_activity_cache: dict[str, tuple[float, float]] = {}  # project → (cached_at_mono, epoch)


def configure(provider: Provider | dict[str, Provider] | None,
              health: HealthChecker) -> None:
    """装配 provider。兼容旧单 provider 签名（视为 {"shared": provider}）。"""
    global _providers, _health
    if provider is None:
        _providers = {}
    elif isinstance(provider, dict):
        _providers = {k: v for k, v in provider.items() if v is not None}
    else:
        _providers = {"shared": provider}
    _health = health


def _provider_for(pool_type: str) -> Provider | None:
    """按项目池类型选 provider。exclusive 不回退 shared（IP 语义不同，
    短效 IP 不能当独享租期管理）；缺独享 provider 时返回 None，补位跳过。"""
    return _providers.get(pool_type)


def _fail_counter(project: str) -> FailCounter:
    if project not in _fail_counters:
        _fail_counters[project] = FailCounter(2)
    return _fail_counters[project]


# ---------------------------------------------------------------------------
# 活跃信号与 QPS（按项目）
# ---------------------------------------------------------------------------

async def record_activity(project: str) -> None:
    """消费方调用 /activity（或隧道建立连接）时记录：最近活跃时刻 + 按分钟计数。"""
    r = get_redis()
    now = time.time()
    minute = time.strftime("%Y%m%d%H%M", time.localtime(now))
    try:
        pipe = make_pipeline(r)
        pipe.set(redis_key(K_LAST_ACTIVITY.format(project=project)), str(now), ex=900)
        pipe.incr(redis_key(K_QPS_MIN.format(project=project, minute=minute)))
        pipe.expire(redis_key(K_QPS_MIN.format(project=project, minute=minute)),
                    _QPS_KEY_TTL_S)
        await pipe.execute()
        _activity_cache[project] = (time.monotonic(), now)  # 本地缓存同步，省一次回读
    except Exception as e:
        logger.warning("pool: record_activity failed: %s", e)


async def _last_activity_epoch(project: str) -> float:
    cached_at, cached_val = _activity_cache.get(project, (0.0, 0.0))
    if time.monotonic() - cached_at < 10.0:
        return cached_val
    try:
        raw = await get_redis().get(redis_key(K_LAST_ACTIVITY.format(project=project)))
        val = float(raw) if raw else 0.0
    except Exception:
        val = cached_val
    _activity_cache[project] = (time.monotonic(), val)
    return val


async def has_recent_activity(project: str, idle_threshold_s: float) -> bool:
    last = await _last_activity_epoch(project)
    return last > 0 and (time.time() - last) <= idle_threshold_s


async def get_qps_10min(project: str) -> float:
    """该项目近 10 分钟 /activity 均值 QPS（用于自适应升档）。"""
    r = get_redis()
    now = time.localtime()
    minutes = []
    base = time.mktime(now)
    for i in range(10):
        m = time.strftime("%Y%m%d%H%M", time.localtime(base - i * 60))
        minutes.append(redis_key(K_QPS_MIN.format(project=project, minute=m)))
    try:
        counts = await asyncio.gather(*(r.get(k) for k in minutes))  # 不用 MGET：集群代理限制跨 slot 多 key 命令
    except Exception as e:
        logger.warning("pool: get_qps_10min failed: %s", e)
        return 0.0
    total = sum(int(c) for c in counts if c)
    return total / 600.0


# ---------------------------------------------------------------------------
# 池读写（ZSET）
# ---------------------------------------------------------------------------

async def sync_project(project: str) -> bool:
    """从 Redis ZSET 同步项目池到进程内镜像。返回池是否非空。"""
    try:
        rows = await get_redis().zrange(
            redis_key(K_POOL.format(project=project)), 0, -1, withscores=True
        )
    except Exception as e:
        logger.warning("pool: read pool failed: %s", e)
        return bool(_pools.get(project))
    _pools[project] = [(str(url), float(score)) for url, score in rows]
    _prune_project_state(project)
    return bool(_pools[project])


def _prune_project_state(project: str) -> None:
    """清掉已不在池中的 IP 的进程内状态（防重历史 / 判死计数）。"""
    live = {url for url, _ in _pools.get(project, [])}
    hist = _target_hist.get(project)
    if hist:
        for url in list(hist):
            if url not in live:
                del hist[url]
    if project in _fail_counters:
        _fail_counters[project].prune(list(live))


async def delete_pool(project: str) -> None:
    """删除项目池（admin 删项目时调用）：清 Redis ZSET + 进程内状态。"""
    try:
        await get_redis().delete(redis_key(K_POOL.format(project=project)))
    except Exception as e:
        logger.warning("pool: delete pool failed: %s", e)
    for store in (_pools, _rr_index, _target_hist, _fail_counters,
                  _last_fail_mono, _last_tier, _activity_cache):
        store.pop(project, None)


def get_proxy_urls(project: str) -> list[str]:
    return [url for url, _ in _pools.get(project, [])]


def pool_size(project: str) -> int:
    return len(_pools.get(project, []))


def record_failure(project: str, url: str) -> None:
    """转发层连接失败记一次（计入判死轮数，连续 N 次后由巡检确认剔除）。"""
    _fail_counter(project).record(url, PROXY_DEAD)


# ---------------------------------------------------------------------------
# 风控信号处理（消费方上报"连得上但被目标站风控"）
# ---------------------------------------------------------------------------

# 消费方（prism 引擎层）识别到验证码/风控后经 /block-signal 上报目标站，
# routes 关联 forwarder 的近期建联映射找到当时用的上游 IP，调 record_block。
# 语义是**按目标站拉黑标记**而非直接剔除：被 bing 风控的 IP 对 baidu 可能
# 干净（实测风控按段×目标站聚集），一刀切剔除浪费 IP；pick_proxy 按 target
# 过滤被标记的 IP。只有被 ≥_BLOCK_KILL_TARGETS 个目标站都风控（整体脏 IP）
# 才真正剔出池。标记进程内保存，随 IP 到期出池自动清理（_maintain_project）。
_BLOCK_KILL_TARGETS = 3
_target_blocked: dict[str, dict[str, set[str]]] = {}  # project → url → {targets}
_ip_area: dict[str, str] = {}  # 池条目 url → area_code（提取时记录，出池清理）

# 真剔除（多站风控的脏 IP）仍受限流保护：防信号风暴抽空池子
_BLOCK_KILL_WINDOW_S = 60.0
_BLOCK_KILL_MAX_PER_WINDOW = 3   # 每项目每分钟最多按信号剔 3 个
_block_kill_ts: dict[str, list[float]] = {}

# 独享池"更换IP"节流（project → monotonic）：青果 /replace 是"释放全部已提取
# 资源再重新提取"的全池替换语义，冷却键按项目而非单 IP；调度硬下限防热路径
# 扎堆创建后台任务，执行节流走热配置 exclusive_replace_cooldown_s
_EXCLUSIVE_REPLACE_SCHED_MIN_S = 10.0
_exclusive_replace_sched: dict[str, float] = {}
_exclusive_replace_last: dict[str, float] = {}

# 独享项目定位缓存：扫 projects hash 找 pool_type=exclusive 的项目（projects
# 自带 10s 缓存，这里再叠 30s，隧道每连接路由不打 Redis）
_EXCLUSIVE_PROJECT_CACHE_TTL_S = 30.0
_exclusive_project_cache: tuple[float, str | None] = (0.0, None)

# 独享项目没有自己的消费方（token 不再下发），其"活跃度"由路由进来的流量
# 代记——否则按需驱动会把独享池当空闲项目停补归零。本地节流 60s 记一次。
_EXCLUSIVE_ACTIVITY_MARK_INTERVAL_S = 60.0
_exclusive_activity_marked: dict[str, float] = {}


def _block_throttle_ok(project: str) -> bool:
    now = time.monotonic()
    ts = [t for t in _block_kill_ts.get(project, []) if now - t < _BLOCK_KILL_WINDOW_S]
    if len(ts) >= _BLOCK_KILL_MAX_PER_WINDOW:
        _block_kill_ts[project] = ts
        return False
    ts.append(now)
    _block_kill_ts[project] = ts
    return True


async def record_block(project: str, url: str, target: str) -> str:
    """风控信号处理。返回 "marked" | "killed" | "throttled" | "noop"。

    默认只标记（该 IP 不再派给该目标站）；同一 IP 被 ≥3 个目标站风控才真剔
    （ZREM，受限流保护）。信号滞后于自然过期/巡检剔除时安静跳过（noop）。
    按 target 路由后 url 可能属于独享池而非凭证项目池——按 URL 实际所在的
    池记录（独享池镜像没有时 ZSCORE 回源确认归属）。
    """
    live = dict(_pools.get(project, []))
    owner = project
    if url not in live:
        # url 可能属于独享池而非隧道凭证对应的共享项目池（按 target 路由时
        # 凭证项目 ≠ 池所属项目）：ZSCORE 回源确认归属后按实际所在池拉黑
        excl = await _exclusive_project()
        if excl and excl != project and await get_redis().zscore(
                redis_key(K_POOL.format(project=excl)), url) is not None:
            await sync_project(excl)  # 镜像对齐 Redis
            live = dict(_pools.get(excl, []))
            owner = excl
    if url not in live:
        return "noop"
    blocked = _target_blocked.setdefault(owner, {}).setdefault(url, set())
    blocked.add(target)
    if len(blocked) < _BLOCK_KILL_TARGETS:
        logger.info(
            "pool: %s block-mark %s target=%s (marked %d/%d)",
            owner, url, target, len(blocked), _BLOCK_KILL_TARGETS,
            extra={"batch_id": owner,
                   "metrics": {"project": owner, "target": target}},
        )
        if projects.pool_type_of(await projects.get_project(owner)) == "exclusive":
            # 独享 IP 被风控：不走城市换段（地区锁在提取时指定），改为异步调
            # 供应商"更换IP"换新（节流见 _exclusive_replace）
            _schedule_exclusive_replace(owner, url)
        else:
            await _note_area_block(owner, url)  # 城市级滑窗聚点（动态换段输入）
        return "marked"
    if not _block_throttle_ok(owner):
        logger.warning(
            "pool: %s block-signal kill throttled (%d/%ds), keep marked %s",
            owner, _BLOCK_KILL_MAX_PER_WINDOW, int(_BLOCK_KILL_WINDOW_S), url,
            extra={"batch_id": owner},
        )
        return "throttled"
    await get_redis().zrem(redis_key(K_POOL.format(project=owner)), url)
    _target_blocked.get(owner, {}).pop(url, None)
    await sync_project(owner)
    logger.warning(
        "pool: %s killed %s on block signal (targets=%s, remaining=%d)",
        owner, url, sorted(blocked), len(live) - 1,
        extra={"batch_id": owner,
               "metrics": {"project": owner, "targets": sorted(blocked),
                           "remaining": len(live) - 1}},
    )
    return "killed"


def _schedule_exclusive_replace(project: str, url: str) -> None:
    """独享池被风控 IP 的更换调度：同步、廉价、不抛异常（同 _schedule_targeted_topup）。

    乐观节流：判定需要调度即先占位写时间戳；无 running loop 时安静跳过——
    下次风控信号还会再触发，不丢语义。
    """
    now = time.monotonic()
    if now - _exclusive_replace_sched.get(url, 0.0) < _EXCLUSIVE_REPLACE_SCHED_MIN_S:
        return
    _exclusive_replace_sched[url] = now
    try:
        asyncio.get_running_loop().create_task(_exclusive_replace(project, url))
    except RuntimeError:
        pass  # 无 running loop：安静跳过


async def _exclusive_replace(project: str, url: str) -> None:
    """独享池整池更换：青果 /replace 语义是"释放全部已提取资源再重新提取"
    （全池替换，不是单 IP 替换）——用返回的新 IP 列表整体替换独享池内容
    （新 IP ZADD、旧 IP ZREM，按差集剔除避免误删刚入池的新 IP）。

    后台 task，任何异常吞掉不外抛（失败仅记日志，旧 IP 留在池里由
    拉黑标记兜底，不再派给该 target）。执行节流走热配置
    exclusive_replace_cooldown_s（默认 60s，按项目计）。
    """
    try:
        cfg = await runtime_config.get_runtime_config()
        provider = _provider_for("exclusive")
        replace = getattr(provider, "replace", None) if provider else None
        if replace is None:
            return  # 独享 provider 未配置/不支持 replace：安静跳过
        now_mono = time.monotonic()
        if now_mono - _exclusive_replace_last.get(project, 0.0) < cfg.exclusive_replace_cooldown_s:
            return
        _exclusive_replace_last[project] = now_mono
        if url not in dict(_pools.get(project, [])):
            return  # 触发的 IP 已出池（巡检/自然过期/已被前一次整池替换），信号滞后
        items = await replace()  # 青果全池语义：不传单个 server
        if not items:
            return  # 供应商更换失败/无新货：仅记日志（provider 内已记）
        now = time.time()
        new_map = {
            make_proxy_url(it.server,
                           settings.exclusive_auth_key,
                           settings.exclusive_auth_pwd):
                (it.deadline_epoch if it.deadline_epoch > now
                 else now + settings.ip_ttl_fallback_s)
            for it in items
        }
        pool_key = redis_key(K_POOL.format(project=project))
        r = get_redis()
        await r.zadd(pool_key, new_map)
        # 旧 IP ZREM：差集剔除——/replace 返回里可能含未变的同 IP，
        # 直接全清会误删刚入池的新 IP
        old_urls = [u for u, _ in _pools.get(project, []) if u not in new_map]
        if old_urls:
            await r.zrem(pool_key, *old_urls)
        for u in old_urls:
            _target_blocked.get(project, {}).pop(u, None)
            _target_hist.get(project, {}).pop(u, None)
            _ip_area.pop(u, None)
        await sync_project(project)
        logger.info(
            "pool: %s exclusive replace: %d new IPs in, %d old out (trigger=%s)",
            project, len(new_map), len(old_urls), url,
            extra={"batch_id": project, "metrics": {
                "project": project, "new": len(new_map), "removed": len(old_urls)}},
        )
    except Exception as e:
        logger.warning("pool: %s exclusive replace error: %s: %s",
                       project, type(e).__name__, e)


def _prune_target_blocked(project: str) -> None:
    """清掉已出池 IP 的风控标记与地区记录（维护轮调用，随 IP 自然到期收敛）。"""
    live_urls = {u for u, _ in _pools.get(project, [])}
    blocked = _target_blocked.get(project)
    if blocked:
        for u in [u for u in blocked if u not in live_urls]:
            del blocked[u]
    for u in [u for u in _ip_area if u not in live_urls]:
        del _ip_area[u]


# ---------------------------------------------------------------------------
# 城市级自动换段（动态 area_ex）
# ---------------------------------------------------------------------------

# 实测风控在城市级有持续性（2026-09-14 汕头段连续两天 baidu 风控率 75%），而
# 城市粒度排除不伤源多样性（qg.net 当前 923 个可用城市）。某城市滑窗内被风控
# 标记的 IP 数超阈值 → 提取时动态 area_ex 排除该城市，TTL 到期自动放回（风控
# 会漂移，排除清单只增不减会把池子越排越小）。跨副本共享走 Redis。


async def _note_area_block(project: str, url: str) -> None:
    """marked 事件按出口城市滑窗聚点，超阈值写入动态排除集合。"""
    code = _ip_area.get(url)
    if not code:
        return
    cfg = await runtime_config.get_runtime_config()
    if not cfg.auto_area_exclude_enabled:
        return
    r = get_redis()
    now = time.time()
    key = redis_key(K_AREA_BLOCK.format(project=project))
    pipe = make_pipeline(r)
    pipe.hset(key, f"{code}|{url}", str(now))
    pipe.expire(key, int(cfg.area_block_window_s) * 2)
    await pipe.execute()
    members = await r.hgetall(key)
    recent = {f for f, ts in members.items()
              if f.startswith(f"{code}|")
              and now - float(ts) < cfg.area_block_window_s}
    if len(recent) >= cfg.area_block_threshold:
        until = now + cfg.area_exclude_ttl_s
        await r.hset(redis_key(K_AREA_EX.format(project=project)), code, str(until))
        logger.warning(
            "pool: %s auto-exclude area=%s for %ds (%d IPs blocked within %ds)",
            project, code, int(cfg.area_exclude_ttl_s), len(recent),
            int(cfg.area_block_window_s),
            extra={"batch_id": project,
                   "metrics": {"project": project, "area_code": code,
                               "blocked": len(recent)}},
        )


async def _dynamic_area_ex(project: str) -> list[str]:
    """当前生效的动态排除城市编码（已过期的自动放回）。按排除上限截断。"""
    cfg = await runtime_config.get_runtime_config()
    if not cfg.auto_area_exclude_enabled:
        return []
    try:
        members = await get_redis().hgetall(redis_key(K_AREA_EX.format(project=project)))
    except Exception:
        return []
    now = time.time()
    codes = sorted(c for c, exp in members.items() if float(exp) > now)
    return codes[: cfg.area_exclude_max]


def _allocatable(
    entries, ttl: float | None = None
) -> list[tuple[str, float]]:
    """剩余寿命在禁区外的（可下发）IP 列表——"可下发"谓词的唯一实现。

    ttl 缺省用模块全局 _alloc_min_ttl_s；调用方持更新鲜的热配置值时显式传入
    （如维护轮投影在 refresh_tick 刷新全局值之前跑）。
    """
    now = time.time()
    floor = _alloc_min_ttl_s if ttl is None else ttl
    return [(u, dl) for u, dl in entries if dl - now >= floor]


def pick_proxy(project: str, target: str = "") -> str | None:
    """按目标站防重轮换挑一个 IP。池空返回 None。

    优先发"本轮生命周期内没服务过该 target"的 IP；所有 IP 都服务过该 target
    时全体清掉该 target 的记录（开启第二轮）再轮询——避免同一 IP 反复打同一
    目标站被风控。剩余寿命 < _alloc_min_ttl_s（下发禁区）的不发；全体躺进
    禁区时回退发剩余寿命最长的那个（2026-09-04 NBA 零结果根因：禁区硬过滤下
    "池非空但发不出"= 隧道秒拒 502，forwarder 连换 IP 重试的机会都没有；
    将死 IP 好歹值一次重试，引擎侧还有质量检测兜底）。被消费方风控信号按
    目标站拉黑的 IP（_target_blocked）不派给对应站；无人可选时忽略拉黑照发，
    并顺带触发定向扩池补位（_schedule_targeted_topup，节流 + 池上限约束）。
    """
    live = _pools.get(project, [])
    candidates = [u for u, _ in _allocatable(live)]
    if not candidates:
        if not live:
            return None
        candidates = [max(live, key=lambda item: item[1])[0]]
    # 目标站拉黑过滤：被该站风控标记的 IP 不再派给它（对其他站照常用）。
    # 过滤后无人可选时忽略标记照发——能发总比返回 None 让隧道 502 强，
    # 与禁区回退同一哲学（引擎侧还有质量检测兜底）。
    if target:
        blocked = _target_blocked.get(project, {})

        def _is_blocked(u: str) -> bool:
            # 标记是引擎名片段（"baidu"），派发目标是 CONNECT authority
            # （"www.baidu.com"）——双向子串匹配对齐 recent_upstreams 的语义
            # （2026-09-16 实测：精确比较让 "baidu" ≠ "www.baidu.com"，拉黑
            # 从未生效，baidu 24h 全灭且定向扩池永不触发）
            return any(m in target or target in m for m in blocked.get(u, ()))

        unblocked = [u for u in candidates if not _is_blocked(u)]
        if unblocked:
            candidates = unblocked
        elif candidates:
            # 池里有可下发 IP 但全被该 target 风控标记：fail-open 照发的同时
            # 触发定向扩池——标记只挡派发不触发补位，不主动扩池的话该站可用率
            # 只能干等 TTL 换血（共享池对超严站 ~80% 出生即被标记时锁死在 0 附近）
            _schedule_targeted_topup(project, target)
    hist = _target_hist.setdefault(project, {})
    if target:
        fresh = [u for u in candidates if target not in hist.get(u, ())]
        if not fresh:
            # 都打过了：开启第二轮
            for u in candidates:
                hist.get(u, set()).discard(target)
            fresh = candidates
    else:
        fresh = candidates
    idx = _rr_index.get(project, 0)
    url = fresh[idx % len(fresh)]
    _rr_index[project] = idx + 1
    if target:
        hist.setdefault(url, set()).add(target)
    return url


# ---------------------------------------------------------------------------
# 独享池按 target 路由（消费方零改动：proxy-hub 内部按目标站选池）
# ---------------------------------------------------------------------------


async def _exclusive_project() -> str | None:
    """当前生效的独享项目名（按名字序取首个 pool_type=exclusive 的注册项目）。

    无独享项目返回 None。结果进程内缓存 30s（projects 表自带 10s 缓存），
    隧道每连接路由不扫 Redis。
    """
    global _exclusive_project_cache
    cached_at, cached = _exclusive_project_cache
    if time.monotonic() - cached_at < _EXCLUSIVE_PROJECT_CACHE_TTL_S:
        return cached
    excl = None
    try:
        for name, meta in sorted((await projects.list_projects()).items()):
            if projects.pool_type_of(meta) == "exclusive":
                excl = name
                break
    except Exception as e:
        logger.warning("pool: resolve exclusive project failed: %s", e)
    _exclusive_project_cache = (time.monotonic(), excl)
    return excl


def _is_exclusive_target(target: str, targets: list[str]) -> bool:
    """target（CONNECT authority 如 "www.baidu.com"）是否命中独享名单——
    双向子串匹配，与 pick_proxy 拉黑标记的语义对齐。"""
    return any(m in target or target in m for m in targets)


def _mark_exclusive_activity(project: str) -> None:
    """独享项目活跃代记（60s 节流）：路由命中独享池时让 ticks 视其为活跃。

    独享项目的 token 不再给任何消费方，没有人调 /activity——不代记的话
    按需驱动会把独享池当空闲项目停补归零。
    """
    now = time.monotonic()
    if now - _exclusive_activity_marked.get(project, 0.0) \
            < _EXCLUSIVE_ACTIVITY_MARK_INTERVAL_S:
        return
    _exclusive_activity_marked[project] = now
    try:
        asyncio.get_running_loop().create_task(record_activity(project))
    except RuntimeError:
        pass  # 无 running loop：安静跳过，下次路由还会再记


async def pick_proxy_routed(project: str, target: str = "") -> str | None:
    """按 target 路由的取代理入口（forwarder 隧道与 /proxy pull 统一走这里）。

    target 命中热配置 exclusive_targets 且存在独享项目池时，优先从独享池
    选 IP（独享池内部的防重轮换/拉黑过滤/fail-open 与共享池同一套 pick_proxy
    逻辑）；独享池拿不出 IP（池空）时自动落回共享池现有逻辑——共享兜底，
    消费方无需重试。未命中名单/未注册独享项目/功能关闭时行为与 pick_proxy
    完全一致。
    """
    if target:
        cfg = await runtime_config.get_runtime_config()
        if cfg.exclusive_targets and _is_exclusive_target(target, cfg.exclusive_targets):
            excl = await _exclusive_project()
            if excl and excl != project:
                # 命中独享名单即代记活跃（含落空兜底场景）——否则冷启动死锁：
                # 池空 → 落共享 → 不记活跃 → 按需驱动永不补位 → 池永远空
                _mark_exclusive_activity(excl)
                if not _pools.get(excl):
                    await sync_project(excl)  # 镜像空才回源（tick 每轮也会同步）
                url = pick_proxy(excl, target)
                if url is not None:
                    return url
                logger.info("pool: %s exclusive pool empty for target=%s, "
                            "falling back to shared pool %s", excl, target, project)
    return pick_proxy(project, target)


def is_exclusive_upstream(url: str) -> bool:
    """该 upstream URL 是否来自独享池（forwarder 每 IP QPS 门控判定）。

    路由产出的独享 IP 必在独享池进程内镜像里（pick_proxy 从镜像挑），
    按镜像成员判定即可；镜像滞后的边缘（刚被更换/剔除）至多漏一次门控。
    """
    excl = _exclusive_project_cache[1]
    return bool(excl) and any(u == url for u, _ in _pools.get(excl, []))


# ---------------------------------------------------------------------------
# 按目标站候选压力定向扩池（targeted top-up）
# ---------------------------------------------------------------------------

# 调度节流（pick_proxy 热路径上的粗节流，硬下限）：每项目每 target 最小间隔。
# 执行节流走热配置 targeted_topup_min_interval_s（配置调大后 ≤15s 生效）。
_TARGETED_TOPUP_MIN_INTERVAL_S = 30.0
_targeted_topup_last: dict[tuple[str, str], float] = {}       # (project, target) → 上次调度 mono
_targeted_topup_exec_last: dict[tuple[str, str], float] = {}  # (project, target) → 上次执行 mono


def _schedule_targeted_topup(project: str, target: str) -> None:
    """pick_proxy 热路径调度定向扩池：同步、廉价、不抛异常。

    乐观节流：判定需要调度即先占位写时间戳，并发 pick 不会扎堆创建后台任务；
    无 running loop（pick_proxy 被同步上下文调用）时安静跳过——下次 pick 还会
    再触发，不丢语义。
    """
    key = (project, target)
    now = time.monotonic()
    if now - _targeted_topup_last.get(key, 0.0) < _TARGETED_TOPUP_MIN_INTERVAL_S:
        return
    _targeted_topup_last[key] = now
    try:
        asyncio.get_running_loop().create_task(_targeted_topup(project, target))
    except RuntimeError:
        pass  # 无 running loop：安静跳过


async def _targeted_topup(project: str, target: str) -> None:
    """定向扩池后台任务：某 target 的未标记候选归零时主动提取一小批新 IP。

    后台 task，任何异常吞掉不外抛。成本三旋钮（热配置）：开关 / 执行间隔 /
    池上限（目标大小 × targeted_topup_pool_cap_multiplier，扩过了就不再膨胀）。
    与 refresh/巡检共用项目维护锁且非阻塞——拿不到说明 refresh/巡检正在补，
    别扎堆。
    """
    try:
        cfg = await runtime_config.get_runtime_config()
        if not cfg.targeted_topup_enabled:
            return
        key = (project, target)
        # 执行节流：按"距上次实际执行"校验（调度侧占位时间戳是本次自己刚写的，
        # 不能用作间隔判据；热配置把间隔调大后由这里兜底生效）
        if time.monotonic() - _targeted_topup_exec_last.get(key, 0.0) \
                < cfg.targeted_topup_min_interval_s:
            return
        tier, params = resolve_tier(cfg, _now_cn(), await get_qps_10min(project))
        target_size = _target_size(await projects.get_project(project), params)
        cap = target_size * cfg.targeted_topup_pool_cap_multiplier
        if pool_size(project) >= cap:
            logger.info(
                "pool: %s targeted top-up skipped, pool %d >= cap %.0f (target=%s)",
                project, pool_size(project), cap, target,
                extra={"batch_id": project,
                       "metrics": {"project": project, "target": target,
                                   "pool_size": pool_size(project)}},
            )
            return
        r = get_redis()
        lock_key = redis_key(K_LOCK_MONITOR.format(project=project))
        acquired = await r.set(lock_key, INSTANCE_ID, nx=True,
                               px=settings.refresh_lock_ttl_s * 1000)
        if not acquired:
            return  # refresh/巡检持锁补位中，别扎堆
        try:
            await sync_project(project)  # 镜像对齐 Redis（_top_up 的低水位判定读镜像）
            added = await _top_up(project, cfg.targeted_topup_batch, tier)
        finally:
            try:
                if await r.get(lock_key) == INSTANCE_ID:
                    await r.delete(lock_key)
            except Exception:
                pass
        _targeted_topup_exec_last[key] = time.monotonic()
        logger.info(
            "pool: %s targeted top-up target=%s added=%d",
            project, target, added,
            extra={"batch_id": project,
                   "metrics": {"project": project, "target": target, "added": added}},
        )
    except Exception as e:
        logger.warning("pool: targeted top-up error: %s: %s", type(e).__name__, e)


# ---------------------------------------------------------------------------
# 成本埋点（按项目）
# ---------------------------------------------------------------------------

async def _record_cost(project: str, tier: str, n_ips: int) -> None:
    key = redis_key(K_COST_DAY.format(project=project, date=time.strftime("%Y%m%d")))
    try:
        r = get_redis()
        pipe = make_pipeline(r)
        pipe.hincrby(key, "fetch_count", 1)
        pipe.hincrby(key, "ip_count", n_ips)
        pipe.hincrby(key, f"fetch_count:{tier}", 1)
        pipe.hincrby(key, f"ip_count:{tier}", n_ips)
        pipe.expire(key, _COST_KEY_TTL_S)
        await pipe.execute()
    except Exception as e:
        logger.warning("pool: cost metrics failed: %s", e)


async def get_cost_today(project: str) -> dict[str, int]:
    key = redis_key(K_COST_DAY.format(project=project, date=time.strftime("%Y%m%d")))
    try:
        raw = await get_redis().hgetall(key)
    except Exception:
        return {}
    return {k: int(v) for k, v in raw.items()}


# ---------------------------------------------------------------------------
# 补位（滚动池的核心：缺几个补几个）
# ---------------------------------------------------------------------------

_cooldown_log_mono: float = 0.0


async def _in_fail_cooldown(project: str, cooldown_s: float | None = None) -> bool:
    """失败冷却判定。返回 True 时打印真值（节流 60s），冷却卡死类问题不再靠猜。

    cooldown_s：本次采用的冷却窗口，默认 fail_cooldown_s（env）；低水位快补
    场景（_top_up 传入 low_pool_cooldown_s）用短窗口。
    """
    global _cooldown_log_mono
    threshold = settings.fail_cooldown_s if cooldown_s is None else cooldown_s
    mono_diff = time.monotonic() - _last_fail_mono.get(project, 0.0)
    if mono_diff < threshold:
        # 本地刚失败（冷却轮询的常态）：GET 结果不参与判定，直接返回，省一次 Redis 读
        if time.monotonic() - _cooldown_log_mono > 60:
            _cooldown_log_mono = time.monotonic()
            logger.info(
                "pool: cooldown active (mono_diff=%.1fs, threshold=%.1fs)",
                mono_diff, threshold,
            )
        return True
    # mono 干净才需要 GET 查跨副本冷却
    redis_diff = None
    try:
        raw = await get_redis().get(redis_key(K_LAST_FAIL.format(project=project)))
        if raw:
            redis_diff = time.time() - float(raw)
    except Exception:
        pass
    in_cd = redis_diff is not None and redis_diff < threshold
    if in_cd and time.monotonic() - _cooldown_log_mono > 60:
        _cooldown_log_mono = time.monotonic()
        logger.info(
            "pool: cooldown active (mono_diff=%.1fs, redis_diff=%.1fs, threshold=%.1fs)",
            mono_diff, redis_diff, threshold,
        )
    return in_cd


async def _mark_fail(project: str) -> None:
    _last_fail_mono[project] = time.monotonic()
    try:
        await get_redis().set(
            redis_key(K_LAST_FAIL.format(project=project)),
            str(time.time()), ex=int(settings.fail_cooldown_s) * 4,
        )
    except Exception:
        pass


def _target_size(proj_meta: dict | None, params: TierParams) -> int:
    """目标池大小：项目注册时的 pool_size 覆盖优先，否则跟随档位 fetch_num。"""
    override = (proj_meta or {}).get("pool_size") or 0
    return int(override) if override > 0 else params.fetch_num


async def _top_up(project: str, n: int, tier: str) -> int:
    """向供应商提取 n 个 IP，健康过滤后 ZADD 进池。返回实际入池数量。

    调用方必须已持有该项目锁。供应商/探测失败走冷却，不抛异常。

    批量补位防抖（热配置 topup_stagger_*）：缺口大且池未空时，把一次 top-up
    拆成若干小批，批间随机 sleep 几秒——同批 IP 的 deadline 随抽取时刻错开，
    避免"同批齐到期 → 整批补位 → 2min 后再齐到期"的过期波循环。IP 仍用满
    寿命，只是不同时抽。池空/紧急时整批快补（新 IP 尽快可用优先于错峰）。
    """
    if n <= 0:
        return 0
    proj_meta = await projects.get_project(project)
    pool_type = projects.pool_type_of(proj_meta)
    provider = _provider_for(pool_type)
    if provider is None:
        if pool_type == "exclusive":
            # 独享项目但未配置独享 provider（EXCLUSIVE_* env 未填）：跳过不 crash
            logger.warning(
                "pool: %s exclusive provider not configured, top-up skipped",
                project, extra={"batch_id": project},
            )
        return 0
    cfg = await runtime_config.get_runtime_config()
    # 低水位（现存 IP 不到目标一半，含空池）：冷却缩到 low_pool_cooldown_s。
    # 池空没有"省配额"的余地——2026-09-04 实测供应商崩盘时，15s 冷却把空池
    # 锁成"过期→重试→又失败→重新计时"的 50s+ 死循环；低水位快冷却把恢复
    # 时间压缩到供应商往返（fetch ~2.5s + 流式探活首个 ~2s）的物理下限附近。
    target = _target_size(proj_meta, cfg.tier_params(tier))
    # 低水位/紧急按"可下发数"而非"池存活数"判：池里全是躺在下发禁区的将死
    # IP 时，对消费方等同无水（/status available=false，pick_proxy 只能回退发
    # 将死 IP）——必须走快冷却 + 整批快补；此时还错峰 sleep 只会把等待方拖超时
    # （2026-09-08 生产：禁区波期间 refresh 带错峰全程 8-20s，prism 10s 预算全超）
    allocatable_n = len(_allocatable(_pools.get(project, [])))
    low_water = allocatable_n <= max(1, target // 2)
    cooldown_s = cfg.low_pool_cooldown_s if low_water else settings.fail_cooldown_s
    if await _in_fail_cooldown(project, cooldown_s):
        logger.info("pool: top-up skipped (fail cooldown)", extra={"batch_id": project})
        return 0
    emergency = allocatable_n <= max(1, n // 2)
    if n >= cfg.topup_stagger_min_n and not emergency:
        batch_id = uuid.uuid4().hex[:12]
        added = 0
        remaining = n
        while remaining > 0:
            chunk = min(cfg.topup_chunk_size, remaining)
            got = await _top_up_once(project, chunk, tier, batch_id, provider, pool_type)
            added += got
            remaining -= chunk
            if got == 0:
                break  # 供应商/探测失败：冷却已记，保留已入池的小批
            if remaining > 0:
                await asyncio.sleep(random.uniform(
                    cfg.topup_stagger_delay_min_s, cfg.topup_stagger_delay_max_s))
        return added
    return await _top_up_once(project, n, tier, uuid.uuid4().hex[:12], provider, pool_type)


async def _top_up_once(
    project: str, n: int, tier: str, batch_id: str,
    provider: Provider, pool_type: str,
) -> int:
    """单批次提取：provider.fetch(n) → 流式体检 → 探完一个入池一个。返回本批入池数。

    流式入池（2026-09-04 实测驱动的优化）：旧实现等全部 IP 探完才一次 ZADD，
    一批 10 个被最慢的连接超时拖 ~8s，唤热/refresh 整体 10.7s 超出 prism
    10s 等待预算 0.7s——水送到了人走了。改为 SEARCHABLE 探完即刻 ZADD +
    sync_project（隧道按进程内镜像挑 IP，sync 才可见），首个活 IP ~2s 即可
    下发；能出网不能搜索的 REACHABLE 暂存，全部探完确无 SEARCHABLE 才兜底
    入池（"优先能搜索"语义不变）。失败（供应商异常/探测全灭）记冷却并
    返回 0，由调用方决定是否续批。
    """
    try:
        # 城市级动态换段：风控聚点超阈值的城市在 TTL 内不再提取（与 URL 静态
        # area_ex 在 provider 内合并）。独享池旁路：地区锁在提取时指定，不换段
        if pool_type == "exclusive":
            area_ex = ""
            auth_key, auth_pwd = settings.exclusive_auth_key, settings.exclusive_auth_pwd
        else:
            area_ex = ",".join(await _dynamic_area_ex(project))
            auth_key, auth_pwd = settings.provider_auth_key, settings.provider_auth_pwd
        items = await provider.fetch(n, area_ex=area_ex)
        if not items and pool_type == "exclusive" \
                and getattr(provider, "last_fetch_channel_full", False):
            # 通道满 ≠ 没资源：独享通道里可能已有在用 IP（服务重启 / Redis 池
            # 被清 / 手工提取占用通道）——查询在用资源认养进池，之后走与正常
            # 提取完全相同的体检 + ZADD 流程
            alive_items = getattr(provider, "alive_items", None)
            if alive_items is not None:
                items = await alive_items()
                if items:
                    logger.info(
                        "pool: %s channel full, adopted %d alive IPs", project,
                        len(items), extra={"batch_id": batch_id},
                    )
        if not items:
            # 正常空结果（独享池通道已满/无资源等，provider 返回空列表而非抛
            # 异常）：不算失败、不进冷却，下轮巡检再来
            logger.info("pool: %s fetch returned 0 IPs (supplier pool full / "
                        "no resource), skip without cooldown", project,
                        extra={"batch_id": batch_id})
            return 0
        # server 是裸 ip:port，必须包成 http://[key:pwd@]server —— httpx 的
        # proxy 参数缺 scheme 会立刻抛异常（曾被误判为"探测全灭"）
        url_of = {
            it.server: make_proxy_url(it.server, auth_key, auth_pwd)
            for it in items
        }
        by_url = {url_of[it.server]: it for it in items}
        pool_key = redis_key(K_POOL.format(project=project))
        now = time.time()
        added_urls: list[str] = []
        pending_reachable: list[str] = []
        dead: list[str] = []

        async def _admit(url: str) -> None:
            it = by_url[url]
            dl = it.deadline_epoch if it.deadline_epoch > now else now + settings.ip_ttl_fallback_s
            await get_redis().zadd(pool_key, {url: dl})
            if it.area_code:
                _ip_area[url] = it.area_code  # 供风控聚点（动态换段）反查
            await sync_project(project)

        # 独享池用轻量中立探针体检（generate_204，任意 HTTP 响应即活，禁用
        # slow-as-dead）——独享 IP 反复打搜索引擎探测页等于主动烧自己
        stream = (_health.probe_stream_liveness(list(url_of.values()))
                  if pool_type == "exclusive"
                  else _health.probe_stream(list(url_of.values())))
        async for url, state in stream:
            if state == PROXY_SEARCHABLE:
                await _admit(url)  # 流式入池：探完一个发一个，不等整批
                added_urls.append(url)
            elif state >= PROXY_REACHABLE:
                pending_reachable.append(url)
            else:
                dead.append(url)
        if dead:
            logger.warning(
                "health: %d/%d IPs dead (connection failed), dropped: %s",
                len(dead), len(items), dead,
            )
        if not added_urls:
            if not pending_reachable:
                # 全灭必须走冷却（抛异常 → _mark_fail），否则每个 tick 都重抽烧配额
                raise ValueError("all fetched proxy IPs failed health check")
            logger.info("health: no search-capable IP, falling back to %d reachable-only",
                        len(pending_reachable))
            for url in pending_reachable:
                await _admit(url)
                added_urls.append(url)
        await _record_cost(project, tier, len(items))
        logger.info(
            "pool: %s topped up %d/%d IPs (batch %s)",
            project, len(added_urls), len(items), batch_id,
            extra={"batch_id": batch_id, "metrics": {
                "project": project, "alive": len(added_urls), "fetched": len(items),
                "tier": tier,
            }},
        )
        return len(added_urls)
    except Exception as e:
        await _mark_fail(project)
        logger.warning(
            "pool: %s top-up failed: %s: %s", project, type(e).__name__, e,
            extra={"batch_id": batch_id},
        )
        return 0


class RefreshResult(NamedTuple):
    """refresh() 结果。ok = 池有可下发 IP（剩余寿命 ≥ alloc_min_ttl_s，与 /status
    同口径——"池非空但全在禁区"对消费方就是无水，不能报 ok）；executed = 本次
    真正执行了换批（持锁 + 补到新货）；waited = 曾因维护锁等待持锁方落地。"""

    ok: bool
    executed: bool
    waited: bool


async def refresh(project: str, wait_s: float = 0.0) -> RefreshResult:
    """消费方强制刷新：补一批新 IP 入池后再淘汰旧批次（两阶段替换）。

    不能先清池再补：提取+体检要好几秒，空窗期内隧道请求全 502——实测
    gateway 一失败就调 /refresh，清池会把并发中的其他查询一起打死
    （2026-09-01 测试服 5 连零结果事故）。补位失败时保留旧池，有旧 IP
    可用总比空池强。锁门防扎堆。

    wait_s：维护锁被占时最多等待秒数。持锁方正在两阶段刷新（≈fetch+probe
    3-5s），等它落地胜过自己再抽一批（省配额）——这正是消费方"等到新 IP
    再用"的语义（2026-09-04 NBA 零结果：refresh 被锁/冷却秒拒，消费方 100ms
    后重试又撞同一坏窗口，快速认输返回 0）。等锁成功的请求若见池已可下发
    （持锁方刚补完），直接复用不重复抽，防等待方扎堆各抽一批烧配额。剩余
    的等待预算还用于"补位撞供应商失败冷却后再试一次"（低水位冷却仅 3s）。
    """
    cfg = await runtime_config.get_runtime_config()
    tier, params = resolve_tier(cfg, _now_cn(), await get_qps_10min(project))
    proj_meta = await projects.get_project(project)
    r = get_redis()
    lock_key = redis_key(K_LOCK_MONITOR.format(project=project))
    deadline = time.monotonic() + max(0.0, wait_s)
    acquired = await r.set(lock_key, INSTANCE_ID, nx=True,
                           px=settings.refresh_lock_ttl_s * 1000)
    waited = False
    while not acquired:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        await asyncio.sleep(min(0.3, remaining))
        acquired = await r.set(lock_key, INSTANCE_ID, nx=True,
                               px=settings.refresh_lock_ttl_s * 1000)
        waited = True
    if not acquired:
        logger.info("pool: %s refresh skipped, maintenance lock held", project)
        await sync_project(project)
        # ok 口径与 /status 一致（可下发而非池非空）：池里全是禁区将死 IP 时
        # 报 ok=true 会让消费方误以为池已热、放行查询撞风控（2026-09-08 实证）
        return RefreshResult(ok=bool(_allocatable(_pools.get(project, []))),
                             executed=False, waited=False)
    try:
        await sync_project(project)
        # url → deadline 快照：新批次若提到同 IP，ZADD 会刷新其 deadline，
        # 借此区分"存活下来的新批次成员"和"该淘汰的旧成员"
        old = dict(_pools.get(project, []))
        if waited and _allocatable(old.items()):
            # 等锁期间持锁方已两阶段补完：直接复用其成果，不重复抽
            return RefreshResult(ok=True, executed=False, waited=True)
        tier, _ = resolve_tier(cfg, _now_cn(), await get_qps_10min(project))
        target = _target_size(proj_meta, params)
        added = await _top_up(project, target, tier)
        if added == 0 and not _allocatable(_pools.get(project, [])):
            # 补位撞供应商失败冷却且旧池不可下发：等短冷却窗口（低水位 3s）
            # 过后再试一次——fetch+probe 期间新 IP 流式入池，消费方重试即命中
            cd_deadline = time.monotonic() + min(
                max(0.0, deadline - time.monotonic()),
                cfg.low_pool_cooldown_s + 1.0,
            )
            while time.monotonic() < cd_deadline and await _in_fail_cooldown(project):
                await asyncio.sleep(0.5)
            added = await _top_up(project, target, tier)
        if added > 0 and old:
            now_map = dict(_pools.get(project, []))
            stale = [u for u, dl in old.items() if now_map.get(u) == dl]
            if stale:
                await r.zrem(redis_key(K_POOL.format(project=project)), *stale)
                await sync_project(project)
        return RefreshResult(ok=bool(_allocatable(_pools.get(project, []))),
                             executed=added > 0, waited=waited)
    finally:
        try:
            if await r.get(lock_key) == INSTANCE_ID:
                await r.delete(lock_key)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# tick 循环体（APScheduler 调用；返回值 = 下次执行间隔秒数）
# ---------------------------------------------------------------------------


def _now_cn():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Shanghai"))


async def refresh_tick() -> float:
    """池缺口轮询 tick：无可下发 IP 且近期有活跃 → 补位（冷启动/大面积死亡/
    禁区波兜底——"池非空但全在下发禁区"同样算无水）。

    常规的"缺几个补几个"由 monitor_tick 顺带完成；本 tick 只兜"无水且巡检
    间隔未到"的死角。返回下次间隔。
    """
    global _alloc_min_ttl_s
    cfg = await runtime_config.get_runtime_config()
    if cfg.alloc_min_ttl_s != _alloc_min_ttl_s:
        logger.info("pool: alloc_min_ttl_s %.0fs → %.0fs", _alloc_min_ttl_s, cfg.alloc_min_ttl_s)
        _alloc_min_ttl_s = cfg.alloc_min_ttl_s
    names = list(await projects.list_projects())
    interval = cfg.tier_params("normal").check_interval_s
    empty_active = False  # 有活跃项目池空：本轮结束后 1s 内再来（专职快补）
    now_mono = time.monotonic()
    for project in names:
        try:
            qps = await get_qps_10min(project)
            tier, params = resolve_tier(cfg, _now_cn(), qps)
            interval = min(interval, params.check_interval_s)
            await sync_project(project)
            if _allocatable(_pools.get(project, [])):
                _empty_since.pop(project, None)
                continue
            # 池非空但全体进下发禁区（同批提取的短效 IP 齐到期波）对消费方等同
            # 无水——旧逻辑只看 pool_size>0 就跳过，禁区波期间消费方只能等
            # 15s 巡检才补位（2026-09-08 实证），这里同样走活跃快补
            if not await has_recent_activity(project, params.idle_threshold_s):
                continue  # 空闲项目：无水属预期（按需驱动零消耗），不告警不快补
            empty_active = True
            # 无水持续告警：活跃项目没有可下发 IP 超过 30s = 供应商大概率崩了，
            # ERROR 进 ES 可配告警
            since = _empty_since.setdefault(project, now_mono)
            empty_for = now_mono - since
            if empty_for >= EMPTY_ALERT_AFTER_S and \
                    now_mono - _empty_alerted.get(project, 0.0) >= EMPTY_ALERT_REPEAT_S:
                _empty_alerted[project] = now_mono
                logger.error(
                    "pool: %s EMPTY (no allocatable IP) for %.0fs while active — "
                    "supplier may be down, queries will stall", project, empty_for,
                    extra={"metrics": {"project": project, "empty_for_s": round(empty_for)}},
                )
            lock_key = redis_key(K_LOCK_MONITOR.format(project=project))
            acquired = await get_redis().set(
                lock_key, INSTANCE_ID, nx=True, px=settings.refresh_lock_ttl_s * 1000
            )
            if not acquired:
                continue
            try:
                proj_meta = await projects.get_project(project)
                await _top_up(project, _target_size(proj_meta, params), tier)
                if pool_size(project) > 0:
                    _empty_since.pop(project, None)  # 补位成功即清除空池计时
            finally:
                try:
                    if await get_redis().get(lock_key) == INSTANCE_ID:
                        await get_redis().delete(lock_key)
                except Exception:
                    pass
        except Exception as e:
            logger.warning("pool: refresh_tick(%s) error: %s: %s",
                           project, type(e).__name__, e)
    if empty_active:
        interval = 1.0  # 空池快补节奏：1s 内重扫（池空发现延迟不再吃整轮间隔）
    return interval


async def monitor_tick() -> float:
    """健康巡检 + 滚动补位 tick（按项目锁门，全局只有一个副本探测）。

    每轮每项目：清自然过期 → 探测判死剔除 → 缺口补位。返回下次间隔。
    """
    cfg = await runtime_config.get_runtime_config()
    names = list(await projects.list_projects())
    interval = cfg.tier_params("normal").health_recheck_interval_s or 60.0
    for project in names:
        try:
            qps = await get_qps_10min(project)
            tier, params = resolve_tier(cfg, _now_cn(), qps)
            if tier != _last_tier.get(project):
                logger.info("pool: %s tier switch %s → %s (qps_10min=%.3f)",
                            project, _last_tier.get(project), tier, qps)
                _last_tier[project] = tier
            p_interval = params.health_recheck_interval_s
            if p_interval <= 0:
                continue
            interval = min(interval, p_interval)

            await sync_project(project)
            active = await has_recent_activity(project, params.idle_threshold_s)
            if not active and pool_size(project) == 0:
                continue  # 空闲项目：不巡检不补位（按需驱动）

            r = get_redis()
            # 锁 TTL = 巡检间隔：本副本持有期间其他副本直接跳过本轮
            lock_key = redis_key(K_LOCK_MONITOR.format(project=project))
            acquired = await r.set(lock_key, INSTANCE_ID, nx=True,
                                   px=int(p_interval) * 1000)
            if not acquired:
                continue  # 探测副本可能改了池，下一轮 sync 即可

            try:
                await _maintain_project(project, params, tier, active)
            finally:
                try:
                    if await r.get(lock_key) == INSTANCE_ID:
                        await r.delete(lock_key)
                except Exception:
                    pass
        except Exception as e:
            logger.warning("pool: monitor_tick(%s) error: %s: %s",
                           project, type(e).__name__, e)
    return interval


async def _maintain_project(
    project: str, params: TierParams, tier: str, active: bool
) -> None:
    """持锁状态下的单项目维护：清过期 → 判死剔除 → 补缺口。"""
    r = get_redis()
    pool_key = redis_key(K_POOL.format(project=project))
    # 1) 清自然过期
    await r.zremrangebyscore(pool_key, 0, time.time())
    await sync_project(project)
    _prune_target_blocked(project)  # 已出池 IP 的风控标记随之清理

    # 2) 探测判死剔除（连续 N 轮才剔，防抖误杀）
    # 独享池：轻量中立探针（不打搜索引擎、禁用 slow-as-dead），且判死轮数
    # 比共享池多 1 轮——单 IP 池被误杀会直接抖动 baidu/quark 的独享路由
    proj_meta = await projects.get_project(project)
    pool_type = projects.pool_type_of(proj_meta)
    current = get_proxy_urls(project)
    if current:
        counter = _fail_counter(project)
        dead_after = (await runtime_config.get_runtime_config()).health_dead_after
        counter.set_dead_after(dead_after + (1 if pool_type == "exclusive" else 0))
        probe_fn = _health.probe_liveness if pool_type == "exclusive" else _health.probe
        probes = await asyncio.gather(*(probe_fn(p) for p in current))
        confirmed_dead = [p for p, s in zip(current, probes) if counter.record(p, s)]
        if confirmed_dead:
            await r.zrem(pool_key, *confirmed_dead)
            await sync_project(project)
            logger.warning(
                "pool: %s monitor — %d/%d IPs confirmed dead: %s",
                project, len(confirmed_dead), len(current), confirmed_dead,
                extra={"batch_id": project, "metrics": {
                    "project": project, "dead": len(confirmed_dead),
                    "total": len(current),
                }},
            )

    # 3) 补缺口（仅在有需求时；空闲项目让池自然过期归零）
    target = _target_size(proj_meta, params)
    # 3a) 提前补位：按"再过 alloc_min_ttl_s 仍可下发"投影池水位，而不是按当前
    # 存活数——即将到期的 IP 不提前杀（用满寿命），但新 IP 提前到位。否则到期
    # 波过后才检测到缺口、再等抽取+探测的几秒~几十秒（巡检间隔）就是真空窗：
    # 小池下唯一幸存者可能正躺在 30s 下发禁区里，表现为"池有 IP 却无可下发"。
    # 提前补位让池在任何时刻的"未来可下发水位"都不低于目标。
    # horizon 必须是 2×alloc_ttl（"再过 alloc_ttl 仍 ≥ alloc_ttl 剩余"⇔
    # 现在剩余 ≥ 2×alloc_ttl）——旧实现只传 alloc_ttl，投影 horizon=0，同批
    # 提取的短效 IP 齐进禁区时补位永远慢一拍（2026-09-08 禁区波根因）。
    alloc_ttl = (await runtime_config.get_runtime_config()).alloc_min_ttl_s
    projected = len(_allocatable(_pools.get(project, []), ttl=2 * alloc_ttl))
    deficit = target - projected
    if deficit > 0 and active:
        await _top_up(project, deficit, tier)


# ---------------------------------------------------------------------------
# 状态快照（/status 端点）
# ---------------------------------------------------------------------------

async def status_snapshot(project: str) -> dict[str, Any]:
    cfg = await runtime_config.get_runtime_config()
    qps = await get_qps_10min(project)
    tier, _ = resolve_tier(cfg, _now_cn(), qps)
    now = time.time()
    deadlines = [dl for _, dl in _pools.get(project, [])]
    proj_meta = await projects.get_project(project)
    pool_type = projects.pool_type_of(proj_meta)
    return {
        "instance_id": INSTANCE_ID,
        "project": project,
        "pool_type": pool_type,
        "pool_size": pool_size(project),
        "available": bool(_allocatable(_pools.get(project, []))),
        "earliest_expire_in_s": (
            round(max(0.0, min(deadlines) - now), 1) if deadlines else 0.0
        ),
        "tier": tier,
        "qps_10min": round(qps, 4),
        "provider_configured": _provider_for(pool_type) is not None,
        "forward_enabled": settings.forward_port > 0,
        "dynamic_area_ex": await _dynamic_area_ex(project),
        "cost_today": await get_cost_today(project),
    }
