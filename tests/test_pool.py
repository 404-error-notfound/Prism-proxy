"""池核心测试：ZSET 滚动补位、锁门、失败冷却、巡检 tick、target 防重分配。

Redis 用 conftest 的 FakeRedis stub；provider/health 用假实现注入。
"""
import asyncio
import time

from proxy_hub import pool, projects, runtime_config
from proxy_hub.health import PROXY_DEAD, PROXY_REACHABLE, PROXY_SEARCHABLE
from proxy_hub.providers.base import ProxyItem
from proxy_hub.redis_client import redis_key

PROJ = "proj"


class FakeProvider:
    def __init__(self, items: list[ProxyItem] | None = None, fail: bool = False):
        self.items = items or []
        self.fail = fail
        self.calls: list[int] = []
        self.area_ex_calls: list[str] = []

    async def fetch(self, num: int, area_ex: str = "") -> list[ProxyItem]:
        self.calls.append(num)
        self.area_ex_calls.append(area_ex)
        if self.fail:
            raise ValueError("provider down")
        return self.items[:num] if self.items else [
            ProxyItem(server=f"10.0.0.{i}:8000", deadline_epoch=time.time() + 120, exit_ip="")
            for i in range(num)
        ]


class SeqProvider:
    """每次 fetch 生成全新 IP（默认 FakeProvider 每次返回同一批，分批测试必须用本类）。"""

    def __init__(self):
        self.calls: list[int] = []
        self._next = 0

    async def fetch(self, num: int, area_ex: str = "") -> list[ProxyItem]:
        self.calls.append(num)
        out = []
        for _ in range(num):
            out.append(ProxyItem(
                server=f"10.9.0.{self._next}:8000",
                deadline_epoch=time.time() + 120, exit_ip=""))
            self._next += 1
        return out


class FakeHealth:
    def __init__(self):
        self.probe_calls: list[str] = []      # 搜索页三态探测（共享池）
        self.liveness_calls: list[str] = []   # 轻量连通性探测（独享池）

    async def probe(self, proxy_url):
        self.probe_calls.append(proxy_url)
        return PROXY_SEARCHABLE

    async def probe_liveness(self, proxy_url):
        self.liveness_calls.append(proxy_url)
        return PROXY_REACHABLE

    async def probe_stream(self, proxies):
        """与真实 HealthChecker.probe_stream 同语义：谁先探完谁先产出。"""
        tasks = {asyncio.ensure_future(self.probe(p)): p for p in proxies}
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                yield tasks[t], t.result()

    async def probe_stream_liveness(self, proxies):
        tasks = {asyncio.ensure_future(self.probe_liveness(p)): p for p in proxies}
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                yield tasks[t], t.result()


def _configure(provider=None):
    pool.configure(provider or FakeProvider(), FakeHealth())


async def _new_project(name=PROJ, pool_size=0) -> dict:
    return await projects.create_project(name, pool_size)


def _pool_key(project=PROJ) -> str:
    return redis_key(pool.K_POOL.format(project=project))


async def _seed_zset(fake_redis, entries: dict[str, float], project=PROJ):
    await fake_redis.zadd(_pool_key(project), entries)
    await pool.sync_project(project)


# ---------------------------------------------------------------------------
# refresh / _top_up（补位）
# ---------------------------------------------------------------------------

async def _release_lock_after(fake_redis, lock: str, delay_s: float = 0.25):
    """模拟持锁方 delay 秒后放锁（refresh 锁等待测试共用）。"""
    await asyncio.sleep(delay_s)
    await fake_redis.delete(lock)


class TestTopUp:
    async def test_refresh_fills_to_tier_size(self, fake_redis):
        provider = FakeProvider()
        _configure(provider)
        await _new_project()

        ok = await pool.refresh(PROJ)
        assert ok.ok is True
        # 默认 normal 档 fetch_num=4 → 补到 4
        assert provider.calls == [4]
        zset = fake_redis.zsets[_pool_key()]
        assert len(zset) == 4
        assert all(score > time.time() for score in zset.values())
        # 成本按项目记账
        cost = await fake_redis.hgetall(
            redis_key(f"dashboard:cost:{PROJ}:" + time.strftime("%Y%m%d")))
        assert cost["fetch_count"] == 1 and cost["ip_count"] == 4

    async def test_project_pool_size_override(self, fake_redis):
        provider = FakeProvider()
        _configure(provider)
        await _new_project(pool_size=2)

        await pool.refresh(PROJ)
        assert provider.calls == [2]
        assert pool.pool_size(PROJ) == 2

    async def test_proxies_wrapped_with_scheme(self, fake_redis):
        """回归：server(裸ip:port) 必须包成 http:// URL——缺 scheme 时 httpx 立刻抛异常。"""
        _configure()
        await _new_project()
        await pool.refresh(PROJ)
        assert all(p.startswith("http://") for p in pool.get_proxy_urls(PROJ))

    async def test_proxies_embed_auth_when_configured(self, fake_redis, monkeypatch):
        monkeypatch.setattr("proxy_hub.pool.settings.provider_auth_key", "K8")
        monkeypatch.setattr("proxy_hub.pool.settings.provider_auth_pwd", "P7")
        _configure()
        await _new_project()
        await pool.refresh(PROJ)
        assert all(p.startswith("http://K8:P7@") for p in pool.get_proxy_urls(PROJ))

    async def test_lock_held_by_other_replica(self, fake_redis):
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        # 模拟另一副本持锁
        await fake_redis.set(redis_key(pool.K_LOCK_MONITOR.format(project=PROJ)),
                             "other-instance", nx=True)
        ok = await pool.refresh(PROJ)
        assert ok.ok is False        # 池还是空
        assert provider.calls == []  # 本进程没有调供应商 API（关键：不重复抽 IP）

    async def test_refresh_waits_for_lock_then_reuses(self, fake_redis):
        """锁被占 + wait_s：等持锁方落地；等到时池已可下发则直接复用
        （executed=False），不重复抽 IP。"""
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {
            "http://10.0.0.1:8000": time.time() + 120,
            "http://10.0.0.2:8000": time.time() + 120,
        })
        lock = redis_key(pool.K_LOCK_MONITOR.format(project=PROJ))
        await fake_redis.set(lock, "other-instance", nx=True)

        task = asyncio.ensure_future(_release_lock_after(fake_redis, lock))
        result = await pool.refresh(PROJ, wait_s=2.0)
        await task
        assert result.waited and not result.executed and result.ok
        assert provider.calls == []  # 复用持锁方成果，未再抽

    async def test_refresh_waits_for_lock_then_executes(self, fake_redis):
        """锁被占 + wait_s：等到锁时池仍不可下发 → 本请求自己换批。"""
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        lock = redis_key(pool.K_LOCK_MONITOR.format(project=PROJ))
        await fake_redis.set(lock, "other-instance", nx=True)

        task = asyncio.ensure_future(_release_lock_after(fake_redis, lock))
        result = await pool.refresh(PROJ, wait_s=2.0)
        await task
        assert result.waited and result.executed and result.ok
        assert provider.calls == [4]

    async def test_lock_released_after_refresh(self, fake_redis):
        _configure()
        await _new_project()
        await pool.refresh(PROJ)
        assert await fake_redis.get(
            redis_key(pool.K_LOCK_MONITOR.format(project=PROJ))) is None

    async def test_provider_failure_cooldown(self, fake_redis):
        provider = FakeProvider(fail=True)
        _configure(provider)
        await _new_project()
        ok1 = await pool.refresh(PROJ)
        assert ok1.ok is False
        ok2 = await pool.refresh(PROJ)
        assert ok2.ok is False
        assert len(provider.calls) == 1  # 冷却期内不再调供应商
        # 跨副本冷却写 Redis
        assert await fake_redis.get(
            redis_key(pool.K_LAST_FAIL.format(project=PROJ))) is not None

    async def test_no_provider_returns_false(self, fake_redis):
        pool.configure(None, FakeHealth())
        await _new_project()
        assert (await pool.refresh(PROJ)).ok is False

    async def test_staggered_top_up_when_pool_alive(self, fake_redis, monkeypatch):
        """缺口大且池未空 → 分批抽取（deadline 错峰），批间 sleep（测试中收集不真睡）。"""
        sleeps: list[float] = []

        async def fake_sleep(s):
            sleeps.append(s)

        monkeypatch.setattr("proxy_hub.pool.asyncio.sleep", fake_sleep)
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        # 池里先有 3 个活 IP（emergency 判定：3 > 5//2=2 → 走分批）
        await _seed_zset(fake_redis, {
            f"http://10.0.0.{i}:8000": time.time() + 90 for i in range(3)
        })

        added = await pool._top_up(PROJ, 5, "normal")
        assert provider.calls == [2, 2, 1]   # chunk_size=2 分批
        assert added == 5
        assert len(sleeps) == 2              # 3 批之间 sleep 2 次

    async def test_top_up_emergency_bulk_when_pool_nearly_empty(self, fake_redis, monkeypatch):
        """池空/接近空 → 整批快补不防抖（新 IP 尽快可用优先于错峰）。"""
        async def boom(s):  # 分批路径才会 sleep，本测试不应走到
            raise AssertionError("emergency path must not sleep")
        monkeypatch.setattr("proxy_hub.pool.asyncio.sleep", boom)
        provider = FakeProvider()
        _configure(provider)
        await _new_project()

        added = await pool._top_up(PROJ, 5, "normal")
        assert provider.calls == [5]
        assert added == 5

    async def test_top_up_emergency_when_all_in_no_alloc_zone(self, fake_redis, monkeypatch):
        """池非空但全体躺在下发禁区（剩余 <25s）→ 按可下发数判紧急，整批快补不
        错峰 sleep。旧逻辑按 pool_size 判，禁区波期间 refresh 带错峰全程 8-20s，
        拖爆消费方等待预算（2026-09-08 禁区波根因修复）。"""
        async def boom(s):  # 分批路径才会 sleep，本测试不应走到
            raise AssertionError("emergency path must not sleep")
        monkeypatch.setattr("proxy_hub.pool.asyncio.sleep", boom)
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.0.9.{i}:8000": time.time() + 10 for i in range(3)
        })

        added = await pool._top_up(PROJ, 5, "normal")
        assert provider.calls == [5]
        assert added == 5

    async def test_top_up_small_gap_bulk(self, fake_redis):
        """缺口 < stagger_min_n（3）→ 不分批，一次快补。"""
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.0.0.{i}:8000": time.time() + 90 for i in range(3)
        })

        added = await pool._top_up(PROJ, 2, "normal")
        assert provider.calls == [2]
        assert added == 2

    async def test_all_dead_triggers_cooldown(self, fake_redis, monkeypatch):
        """回归：提取成功但探测全灭时必须走冷却，否则每个 tick 重抽烧配额。"""
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        monkeypatch.setattr(pool._health, "probe", lambda url: _const(PROXY_DEAD))
        assert (await pool.refresh(PROJ)).ok is False
        assert (await pool.refresh(PROJ)).ok is False
        assert len(provider.calls) == 1  # 第二次在冷却期内，不调供应商

    async def test_refresh_clears_stale_pool(self, fake_redis):
        """强制刷新 = 两阶段替换：新批次入池后旧 IP 被淘汰。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {"http://old:1": time.time() + 120})
        await pool.refresh(PROJ)
        urls = pool.get_proxy_urls(PROJ)
        assert "http://old:1" not in urls and len(urls) == 4

    async def test_refresh_ok_false_when_pool_all_in_no_alloc_zone(self, fake_redis):
        """池非空但全在下发禁区 + 补位失败 → ok=False（与 /status 同口径）。
        旧口径只看 pool_size>0，禁区波期间报假阳性 ok=true，消费方误以为池
        已热、放行查询撞风控（2026-09-08 禁区波根因修复）。"""
        provider = FakeProvider(fail=True)
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.0.9.{i}:8000": time.time() + 10 for i in range(3)
        })
        result = await pool.refresh(PROJ)
        assert result.ok is False

    async def test_sync_project(self, fake_redis):
        _configure()
        await _new_project()
        await pool.refresh(PROJ)
        # 模拟另一进程：清空内存镜像后从 Redis 同步
        pool._pools = {}
        assert await pool.sync_project(PROJ) is True
        assert pool.pool_size(PROJ) == 4


# ---------------------------------------------------------------------------
# pick_proxy（轮询 + 临近过期安全边 + target 防重）
# ---------------------------------------------------------------------------

    async def test_streaming_admission_searchable_immediately(self, fake_redis, monkeypatch):
        """流式入池：快 IP（SEARCHABLE 立即返回）探完即入池，不等慢 IP 探完。

        旧实现等全部探完才一次 ZADD，一批被最慢的连接超时拖 ~8s；此处锚定
        "慢 IP 探测结束时快 IP 已在池中"的流式语义。
        """
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        snapshots: list[set[str]] = []

        async def probe(p):
            if p.endswith("0.0:8000"):  # SeqProvider 首个 IP = 慢 IP
                await asyncio.sleep(0.05)
                snapshots.append(set(pool.get_proxy_urls(PROJ)))
                return PROXY_DEAD
            return PROXY_SEARCHABLE

        monkeypatch.setattr(pool._health, "probe", probe)
        added = await pool._top_up(PROJ, 3, "normal")
        assert added == 2
        assert snapshots, "probe 桩未被调用"
        assert any("http://10.9.0.1:8000" in s for s in snapshots), \
            "慢 IP 探完时快 IP 应已流式入池"

    async def test_low_pool_short_cooldown(self, fake_redis):
        """低水位快冷却：4s 前的失败，短窗口(3s,低水位)已过期可补，
        长窗口(15s,正常水位)仍在期内挡住。"""
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        # 高水位（池满 4 个）：15s 冷却生效 → 不 fetch
        await _seed_zset(fake_redis,
                         {f"http://10.0.0.{i}:8000": time.time() + 120 for i in range(4)})
        pool._last_fail_mono[PROJ] = time.monotonic() - 4.0
        await pool.refresh(PROJ)
        assert provider.calls == []
        # 低水位（池空）：3s 冷却已过期 → fetch 发生
        await fake_redis.delete(_pool_key())
        pool._pools = {}
        pool._last_fail_mono[PROJ] = time.monotonic() - 4.0
        await pool.refresh(PROJ)
        assert provider.calls == [4]


class TestPickProxy:
    async def test_round_robin(self, fake_redis):
        _configure()
        await _new_project()
        now = time.time()
        await _seed_zset(fake_redis, {
            "http://a:1": now + 120, "http://b:1": now + 120, "http://c:1": now + 120,
        })
        picks = [pool.pick_proxy(PROJ) for _ in range(3)]
        assert sorted(picks) == ["http://a:1", "http://b:1", "http://c:1"]

    async def test_empty_pool_returns_none(self, fake_redis):
        assert pool.pick_proxy(PROJ) is None

    async def test_skips_near_expiry(self, fake_redis):
        """剩余寿命 < 安全边的 IP 不发放（防给到客户端就死）。"""
        _configure()
        await _new_project()
        now = time.time()
        await _seed_zset(fake_redis, {
            "http://dying:1": now + 3,      # 剩余 3s < 25s 禁区
            "http://fresh:1": now + 120,
        })
        assert pool.pick_proxy(PROJ) == "http://fresh:1"

    async def test_fallback_to_longest_lived_when_all_in_ttl_buffer(self, fake_redis):
        """全体躺进下发禁区（尾段齐到期）时回退发剩余寿命最长的——将死 IP 好过
        隧道秒拒 502（2026-09-04 NBA 零结果根因：禁区硬过滤把"发不出"变成 None）。"""
        _configure()
        await _new_project()
        now = time.time()
        await _seed_zset(fake_redis, {
            "http://dying-a:1": now + 5,
            "http://dying-b:1": now + 10,
        })
        assert pool.pick_proxy(PROJ) == "http://dying-b:1"
        assert pool.pick_proxy(PROJ, "baidu.com") == "http://dying-b:1"

    async def test_target_anti_repeat(self, fake_redis):
        """同一 target 轮着发不同 IP；全发过一遍后开第二轮。"""
        _configure()
        await _new_project()
        now = time.time()
        await _seed_zset(fake_redis, {
            "http://a:1": now + 120, "http://b:1": now + 120,
        })
        first = pool.pick_proxy(PROJ, "baidu.com")
        second = pool.pick_proxy(PROJ, "baidu.com")
        assert first != second            # 防重：不连续发同一个 IP
        third = pool.pick_proxy(PROJ, "baidu.com")
        assert third == first             # 都打过了 → 第二轮从头开始

    async def test_other_target_prefers_untouched(self, fake_redis):
        _configure()
        await _new_project()
        now = time.time()
        await _seed_zset(fake_redis, {
            "http://a:1": now + 120, "http://b:1": now + 120,
        })
        used = pool.pick_proxy(PROJ, "baidu.com")
        other = pool.pick_proxy(PROJ, "bing.com")
        assert other != used              # bing 优先发没碰过它的 IP


# ---------------------------------------------------------------------------
# activity / QPS（按项目）
# ---------------------------------------------------------------------------

class TestActivity:
    async def test_record_and_query(self, fake_redis):
        assert await pool.has_recent_activity(PROJ, 300) is False
        for _ in range(6):
            await pool.record_activity(PROJ)
        assert await pool.has_recent_activity(PROJ, 300) is True
        # 模拟活跃信号过期：写入 400s 前的时间戳
        await fake_redis.set(redis_key(pool.K_LAST_ACTIVITY.format(project=PROJ)),
                             str(time.time() - 400))
        pool._activity_cache = {}  # 绕过本地缓存
        assert await pool.has_recent_activity(PROJ, 300) is False
        qps = await pool.get_qps_10min(PROJ)
        assert abs(qps - 6 / 600) < 1e-6

    async def test_projects_isolated(self, fake_redis):
        await pool.record_activity("a")
        assert await pool.has_recent_activity("a", 300) is True
        assert await pool.has_recent_activity("b", 300) is False


# ---------------------------------------------------------------------------
# monitor tick（清过期 + 连续判死 + 滚动补位 + 锁门）
# ---------------------------------------------------------------------------

class TestMonitor:
    async def test_first_failure_no_removal(self, fake_redis, monkeypatch):
        _configure()
        await _new_project()
        await pool.refresh(PROJ)
        await pool.record_activity(PROJ)
        monkeypatch.setattr(pool._health, "probe", lambda p: _const(PROXY_DEAD))
        await pool.monitor_tick()
        assert pool.pool_size(PROJ) == 4  # 一轮失败不剔除

    async def test_consecutive_failure_top_up(self, fake_redis, monkeypatch):
        """连续 2 轮判死 → 剔除 → 补位。补位新 IP 是 SeqProvider 的全新 IP，
        probe 桩只判死首批（流式入池后补位与巡检同走 probe，统一世界）。"""
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        await pool.refresh(PROJ)          # 抽 4 个
        doomed = set(pool.get_proxy_urls(PROJ))
        await pool.record_activity(PROJ)

        async def probe(p):
            return PROXY_DEAD if p in doomed else PROXY_SEARCHABLE
        monkeypatch.setattr(pool._health, "probe", probe)
        await pool.monitor_tick()         # 第 1 轮：记数
        await pool.monitor_tick()         # 第 2 轮：确认死亡 → 剔除 4 → 缺口补 4
        assert pool.pool_size(PROJ) == 4
        assert provider.calls == [4, 4]   # 首刷 4 + 补位 4

    async def test_clears_expired_without_probe(self, fake_redis, monkeypatch):
        """自然过期的 IP 直接 ZREMRANGEBYSCORE 清掉，不占用判死轮数。"""
        _configure()
        await _new_project(pool_size=1)  # 目标大小=1：清完过期后无缺口，不触发补位
        await _seed_zset(fake_redis, {
            "http://dead:1": time.time() - 1,   # 已过期
            "http://live:1": time.time() + 120,
        })
        await pool.record_activity(PROJ)
        await pool.monitor_tick()
        assert pool.get_proxy_urls(PROJ) == ["http://live:1"]

    async def test_monitor_lock_held_skips(self, fake_redis, monkeypatch):
        _configure()
        await _new_project()
        await pool.refresh(PROJ)
        await pool.record_activity(PROJ)
        probed = []
        monkeypatch.setattr(pool._health, "probe",
                            lambda p: probed.append(p) or _const(PROXY_SEARCHABLE))
        await fake_redis.set(redis_key(pool.K_LOCK_MONITOR.format(project=PROJ)),
                             "other", nx=True)
        await pool.monitor_tick()
        assert probed == []  # 锁被占 → 不探测（探测流量不 ×N）

    async def test_idle_project_skips_probe_and_topup(self, fake_redis, monkeypatch):
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        probed = []
        monkeypatch.setattr(pool._health, "probe",
                            lambda p: probed.append(p) or _const(PROXY_SEARCHABLE))
        await pool.monitor_tick()  # 无 activity + 池空 → 完全跳过
        assert probed == []
        assert provider.calls == []

    async def test_partial_death_top_up_deficit_only(self, fake_redis, monkeypatch):
        """死几个补几个（滚动补位核心）。probe 桩只判死首批里指定的一个，
        补位新 IP（SeqProvider 全新）判活。"""
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        await pool.refresh(PROJ)          # 4 个
        doomed = {pool.get_proxy_urls(PROJ)[0]}
        await pool.record_activity(PROJ)

        async def probe(p):
            return PROXY_DEAD if p in doomed else PROXY_SEARCHABLE
        monkeypatch.setattr(pool._health, "probe", probe)
        await pool.monitor_tick()         # 记数
        await pool.monitor_tick()         # 死 1 确认 → 补 1
        assert provider.calls == [4, 1]
        assert pool.pool_size(PROJ) == 4

    async def test_pre_replenish_before_expiry_wave(self, fake_redis, monkeypatch):
        """提前补位：3/4 个 IP 将在 30s 内到期（进入下发禁区）→ 维护轮就补 3，
        而不是等它们真死后再补——消除"池有 IP 却无可下发"的真空窗。

        种子寿命须在 [alloc_ttl, 2×alloc_ttl)（默认 [25,50)）：低于 25 触发紧急
        整批快补（不走分批），高于 50 又被投影算作可下发——都不走本条验证的
        分批路径。"""
        async def no_sleep(s):
            return None
        monkeypatch.setattr("proxy_hub.pool.asyncio.sleep", no_sleep)
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        now = time.time()
        await _seed_zset(fake_redis, {
            "http://a:8000": now + 30,   # 30s 后到期：投影（horizon 50s）视为不可下发
            "http://b:8000": now + 35,
            "http://c:8000": now + 40,
            "http://d:8000": now + 200,  # 唯一长期存活
        })
        await pool.record_activity(PROJ)
        await pool.monitor_tick()
        # target 4 - projected 1 = 3 → 分批 2+1
        assert provider.calls == [2, 1]
        # 旧 IP 不提前杀：池临时 4+3=7，到期自然回落到 4
        assert pool.pool_size(PROJ) == 7

    async def test_pre_replenish_horizon_is_2x_alloc_ttl(self, fake_redis, monkeypatch):
        """投影 horizon = 2×alloc_min_ttl_s：剩余寿命在 [alloc_ttl, 2×alloc_ttl)
        的 IP（现在可下发，但撑不过下一个禁区窗口）也必须触发提前补位。
        旧实现 horizon=alloc_ttl（=0 前瞻），同批短效 IP 齐进禁区时补位慢一拍。"""
        async def no_sleep(s):
            return None
        monkeypatch.setattr("proxy_hub.pool.asyncio.sleep", no_sleep)
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        now = time.time()
        # alloc_min_ttl_s 默认 25：+30s 现在可下发（≥25）但 < 50（horizon）→ 投影不计
        await _seed_zset(fake_redis, {
            f"http://10.0.8.{i}:8000": now + 30 for i in range(4)
        })
        await pool.record_activity(PROJ)
        await pool.monitor_tick()
        assert provider.calls == [2, 2]  # target 4 - projected 0 = 4 → 分批 2+2


# ---------------------------------------------------------------------------
# refresh tick（池空兜底）
# ---------------------------------------------------------------------------

class TestRefreshTick:
    async def test_empty_pool_with_activity_triggers_topup(self, fake_redis):
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await pool.record_activity(PROJ)
        await pool.refresh_tick()
        assert provider.calls == [4]
        assert pool.pool_size(PROJ) == 4

    async def test_no_alloc_zone_with_activity_triggers_topup(self, fake_redis):
        """池非空但全在下发禁区（齐到期波）+ 有活跃 → 同样走快补。
        旧逻辑只看 pool_size>0 就跳过，禁区波期间只能等 15s 巡检。"""
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.0.9.{i}:8000": time.time() + 10 for i in range(3)
        })
        await pool.record_activity(PROJ)
        await pool.refresh_tick()
        assert provider.calls == [4]  # 可下发数 0 → 紧急整批快补
        assert pool._allocatable(pool._pools[PROJ])  # 补完后有可下发 IP

    async def test_no_alloc_zone_idle_no_topup(self, fake_redis):
        """全禁区但项目空闲：不补（按需驱动零消耗）。"""
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.0.9.{i}:8000": time.time() + 10 for i in range(3)
        })
        await pool.refresh_tick()
        assert provider.calls == []

    async def test_empty_pool_idle_no_topup(self, fake_redis):
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await pool.refresh_tick()
        assert provider.calls == []  # 无活跃不抽（按需驱动）

    async def test_empty_pool_interval_1s(self, fake_redis):
        """活跃项目池空：refresh_tick 返回 1s 快补节奏（池空发现延迟 ≤1s）。"""
        _configure()
        await _new_project()
        await pool.record_activity(PROJ)
        interval = await pool.refresh_tick()
        assert interval == 1.0
        assert pool.pool_size(PROJ) > 0  # 顺带补上了

    async def test_empty_pool_active_error_alert(self, fake_redis, monkeypatch, caplog):
        """活跃项目空池持续 ≥30s：ERROR 告警（5 分钟节流）；
        池恢复后计时清除，空闲项目不告警。"""
        import logging

        monkeypatch.setattr(pool, "logger", logging.getLogger("tests.pool.alert"))
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await pool.record_activity(PROJ)
        # 预置 40s 前就已空池（绕过 30s 等待）
        pool._empty_since[PROJ] = time.monotonic() - 40.0
        with caplog.at_level(logging.ERROR, logger="tests.pool.alert"):
            await pool.refresh_tick()
        assert any("EMPTY" in r.message for r in caplog.records)
        # 节流：池仍空、立即再跑不重复告警
        await fake_redis.delete(_pool_key())
        pool._pools = {}
        caplog.clear()
        with caplog.at_level(logging.ERROR, logger="tests.pool.alert"):
            await pool.refresh_tick()
        assert not any("EMPTY" in r.message for r in caplog.records)
        # 池恢复后计时清除：再空 40s 才会再次告警
        assert PROJ not in pool._empty_since


# ---------------------------------------------------------------------------
# 城市级自动换段（动态 area_ex）
# ---------------------------------------------------------------------------

class TestAutoAreaExclude:
    """滑窗聚点超阈值 → 动态 area_ex；TTL 到期自动放回；传给 provider 提取。"""

    async def test_threshold_triggers_dynamic_exclude(self, fake_redis):
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.2.0.{i}:8000": time.time() + 100 for i in range(3)})
        for i in range(3):
            pool._ip_area[f"http://10.2.0.{i}:8000"] = "440500"
        for i in range(3):
            assert await pool.record_block(PROJ, f"http://10.2.0.{i}:8000", "baidu") == "marked"
        assert await pool._dynamic_area_ex(PROJ) == ["440500"]

    async def test_below_threshold_no_exclude(self, fake_redis):
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.2.0.{i}:8000": time.time() + 100 for i in range(2)})
        for i in range(2):
            pool._ip_area[f"http://10.2.0.{i}:8000"] = "440500"
            await pool.record_block(PROJ, f"http://10.2.0.{i}:8000", "baidu")
        assert await pool._dynamic_area_ex(PROJ) == []

    async def test_same_ip_multi_target_counts_once(self, fake_redis):
        """同一 IP 被多个目标站风控只算一个城市名额（阈值按不同 IP 数）。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {"http://10.2.0.1:8000": time.time() + 100})
        pool._ip_area["http://10.2.0.1:8000"] = "440500"
        await pool.record_block(PROJ, "http://10.2.0.1:8000", "baidu")
        await pool.record_block(PROJ, "http://10.2.0.1:8000", "bing")
        assert await pool._dynamic_area_ex(PROJ) == []

    async def test_dynamic_exclusion_passed_to_provider(self, fake_redis):
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await fake_redis.hset(
            redis_key(pool.K_AREA_EX.format(project=PROJ)),
            "440500", str(time.time() + 999))
        await pool._top_up(PROJ, 2, "normal")
        assert provider.area_ex_calls == ["440500"]

    async def test_expired_exclusion_released(self, fake_redis):
        """TTL 到期的城市自动放回（不传给 provider）。"""
        provider = FakeProvider()
        _configure(provider)
        await _new_project()
        await fake_redis.hset(
            redis_key(pool.K_AREA_EX.format(project=PROJ)),
            "440500", str(time.time() - 1))  # 已过期
        await pool._top_up(PROJ, 2, "normal")
        assert provider.area_ex_calls == [""]
        assert await pool._dynamic_area_ex(PROJ) == []


# ---------------------------------------------------------------------------
# 风控信号按目标站拉黑（/block-signal → record_block）
# ---------------------------------------------------------------------------

class TestRecordBlock:
    """消费方上报"被目标站风控"→ 按站拉黑标记；多站（≥3）风控才真剔。"""

    def setup_method(self):
        pool._block_kill_ts.clear()
        pool._target_blocked.clear()

    async def test_marks_target_without_killing(self, fake_redis):
        """单站风控只标记不剔除：IP 仍在池里，对其他站照常可用。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            "http://10.1.0.1:8000": time.time() + 100,
            "http://10.1.0.2:8000": time.time() + 100,
        })
        assert await pool.record_block(PROJ, "http://10.1.0.1:8000", "bing") == "marked"
        assert pool.get_proxy_urls(PROJ) == ["http://10.1.0.1:8000", "http://10.1.0.2:8000"]
        assert pool._target_blocked[PROJ]["http://10.1.0.1:8000"] == {"bing"}

    async def test_killed_after_multi_target_block(self, fake_redis):
        """同一 IP 被 3 个目标站风控 = 整体脏 IP，真剔出池。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            "http://10.1.0.1:8000": time.time() + 100,
            "http://10.1.0.2:8000": time.time() + 100,
        })
        assert await pool.record_block(PROJ, "http://10.1.0.1:8000", "bing") == "marked"
        assert await pool.record_block(PROJ, "http://10.1.0.1:8000", "baidu") == "marked"
        assert await pool.record_block(PROJ, "http://10.1.0.1:8000", "sogou") == "killed"
        assert pool.get_proxy_urls(PROJ) == ["http://10.1.0.2:8000"]
        assert "http://10.1.0.1:8000" not in pool._target_blocked.get(PROJ, {})

    async def test_not_in_pool_noop(self, fake_redis):
        """信号滞后于自然过期/巡检剔除时安静跳过。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {"http://10.1.0.1:8000": time.time() + 100})
        assert await pool.record_block(PROJ, "http://ghost:1", "baidu") == "noop"
        assert pool.get_proxy_urls(PROJ) == ["http://10.1.0.1:8000"]

    async def test_throttle_caps_kills(self, fake_redis):
        """每项目每分钟最多真剔 3 个（防信号风暴抽空池子）。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.1.0.{i}:8000": time.time() + 100 for i in range(4)
        })
        actions = []
        for i in range(4):
            for t in ("baidu", "bing", "sogou"):
                actions.append(
                    await pool.record_block(PROJ, f"http://10.1.0.{i}:8000", t))
        assert actions.count("killed") == 3
        assert actions[-1] == "throttled"  # 第 4 个脏 IP 触发限流，保留标记不剔
        assert len(pool.get_proxy_urls(PROJ)) == 1

    async def test_pick_proxy_skips_blocked_target(self, fake_redis):
        """被 bing 拉黑的 IP 不再派给 bing，但派给 baidu 照常用。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            "http://10.1.0.1:8000": time.time() + 100,
            "http://10.1.0.2:8000": time.time() + 100,
        })
        await pool.record_block(PROJ, "http://10.1.0.1:8000", "bing")
        for _ in range(4):  # 轮换多次都不应派到被拉黑的
            assert pool.pick_proxy(PROJ, "bing") == "http://10.1.0.2:8000"
        assert pool.pick_proxy(PROJ, "baidu") in (
            "http://10.1.0.1:8000", "http://10.1.0.2:8000")

    async def test_pick_proxy_fallback_when_all_blocked(self, fake_redis):
        """全部候选都被该站拉黑时忽略标记照发（能发总比 None 强）。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {"http://10.1.0.1:8000": time.time() + 100})
        await pool.record_block(PROJ, "http://10.1.0.1:8000", "bing")
        assert pool.pick_proxy(PROJ, "bing") == "http://10.1.0.1:8000"

    async def test_pick_proxy_matches_fragment_mark_against_full_host(self, fake_redis, monkeypatch):
        """标记是引擎名片段（"baidu"），派发目标是 CONNECT authority
        （"www.baidu.com"）——2026-09-16 实测精确比较让拉黑从未生效：
        baidu 24h 全灭且定向扩池永不触发。双向子串匹配后必须真正跳过。"""
        monkeypatch.setattr(pool, "_schedule_targeted_topup", lambda p, t: None)
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            "http://10.1.0.1:8000": time.time() + 100,
            "http://10.1.0.2:8000": time.time() + 100,
        })
        await pool.record_block(PROJ, "http://10.1.0.1:8000", "baidu")  # 引擎名片段
        for _ in range(4):
            # 派发用完整主机名：被片段标记的 IP 不许再派给该站
            assert pool.pick_proxy(PROJ, "www.baidu.com") == "http://10.1.0.2:8000"
        # 全部候选被片段标记时 fail-open 仍照发
        await pool.record_block(PROJ, "http://10.1.0.2:8000", "baidu")
        assert pool.pick_proxy(PROJ, "www.baidu.com") is not None


# ---------------------------------------------------------------------------
# 按目标站候选压力定向扩池（targeted top-up）
# ---------------------------------------------------------------------------

class TestTargetedTopUp:
    """某 target 的未标记候选归零（池里 IP 全被该站风控标记）时，pick_proxy
    fail-open 照发的同时触发后台定向扩池；节流/池上限/开关三旋钮约束成本。"""

    async def _seed_all_blocked(self, fake_redis, n=1):
        """种 n 个活 IP 并全部按 baidu 标记（模拟共享池对超严站全灭）。"""
        await _seed_zset(fake_redis, {
            f"http://10.1.0.{i}:8000": time.time() + 100 for i in range(n)
        })
        for i in range(n):
            await pool.record_block(PROJ, f"http://10.1.0.{i}:8000", "baidu")

    async def test_all_blocked_failopen_and_schedules_topup(self, fake_redis, monkeypatch):
        """全部候选被 target 标记：fail-open 仍返回 IP（返回值语义不变），
        且调度了定向补位后台任务。"""
        scheduled: list[tuple[str, str]] = []

        async def fake_topup(project, target):
            scheduled.append((project, target))

        monkeypatch.setattr(pool, "_targeted_topup", fake_topup)
        _configure()
        await _new_project()
        await self._seed_all_blocked(fake_redis)
        assert pool.pick_proxy(PROJ, "baidu") == "http://10.1.0.0:8000"  # fail-open 照发
        await asyncio.sleep(0)  # 让 create_task 的后台任务跑起来
        assert scheduled == [(PROJ, "baidu")]

    async def test_throttled_within_interval(self, fake_redis, monkeypatch):
        """调度节流：间隔（30s）内第二次触发不再调度。"""
        scheduled: list[tuple[str, str]] = []

        async def fake_topup(project, target):
            scheduled.append((project, target))

        monkeypatch.setattr(pool, "_targeted_topup", fake_topup)
        _configure()
        await _new_project()
        await self._seed_all_blocked(fake_redis)
        pool.pick_proxy(PROJ, "baidu")
        pool.pick_proxy(PROJ, "baidu")  # 间隔内第二次：被节流
        await asyncio.sleep(0)
        assert scheduled == [(PROJ, "baidu")]

    async def test_unblocked_candidate_no_trigger(self, fake_redis, monkeypatch):
        """有未标记候选时走正常路径，不触发定向补位。"""
        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(pool, "_schedule_targeted_topup",
                            lambda p, t: calls.append((p, t)))
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {
            "http://10.1.0.1:8000": time.time() + 100,
            "http://10.1.0.2:8000": time.time() + 100,
        })
        await pool.record_block(PROJ, "http://10.1.0.1:8000", "baidu")
        assert pool.pick_proxy(PROJ, "baidu") == "http://10.1.0.2:8000"  # 正常过滤路径
        assert calls == []

    async def test_topup_fetches_batch_and_keeps_old(self, fake_redis, monkeypatch):
        """端到端：全标记 → 定向补位提取 targeted_topup_batch(=3) 个新 IP 入池，
        旧 IP 保留（扩池不缩池），锁事后释放。"""
        async def no_sleep(s):
            return None
        monkeypatch.setattr("proxy_hub.pool.asyncio.sleep", no_sleep)
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        await self._seed_all_blocked(fake_redis, n=4)

        await pool._targeted_topup(PROJ, "baidu")
        assert provider.calls == [2, 1]   # batch=3，错峰分批 2+1
        assert pool.pool_size(PROJ) == 7  # 扩池：旧 4 + 新 3
        assert await fake_redis.get(
            redis_key(pool.K_LOCK_MONITOR.format(project=PROJ))) is None

    async def test_pool_cap_skips_fetch(self, fake_redis):
        """池大小已达 目标×multiplier（normal 4 × 2.0 = 8）→ 记日志跳过，不再膨胀。"""
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {
            f"http://10.1.0.{i}:8000": time.time() + 100 for i in range(8)
        })
        await pool._targeted_topup(PROJ, "baidu")
        assert provider.calls == []

    async def test_disabled_no_fetch(self, fake_redis):
        """热配置 targeted_topup_enabled=False → 不提取。"""
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        await _seed_zset(fake_redis, {"http://10.1.0.1:8000": time.time() + 100})
        await runtime_config.set_runtime_config({"targeted_topup_enabled": False})
        await pool._targeted_topup(PROJ, "baidu")
        assert provider.calls == []

    async def test_exec_interval_throttles_second_run(self, fake_redis, monkeypatch):
        """执行节流：一次实际执行后，配置间隔内第二次 _targeted_topup 不再提取。"""
        async def no_sleep(s):
            return None
        monkeypatch.setattr("proxy_hub.pool.asyncio.sleep", no_sleep)
        provider = SeqProvider()
        _configure(provider)
        await _new_project()
        await self._seed_all_blocked(fake_redis, n=4)
        await pool._targeted_topup(PROJ, "baidu")
        await pool._targeted_topup(PROJ, "baidu")  # 间隔内：跳过
        assert provider.calls == [2, 1]            # 只执行了一次（分批 2+1）


async def _const(v):
    return v


# ---------------------------------------------------------------------------
# 独享池（pool_type=exclusive）：独立 provider 补位 + 风控触发更换IP
# ---------------------------------------------------------------------------

class ExclusiveProvider:
    """带 replace/release 能力的假独享 provider（每次 fetch/replace 生成新 IP）。

    replace 模拟青果全池替换语义：忽略 server 参数，返回新 IP 列表。"""

    def __init__(self, fail_replace: bool = False):
        self.fetch_calls: list[tuple[int, str]] = []
        self.replace_calls: int = 0
        self.fail_replace = fail_replace
        self._next = 0

    async def fetch(self, num: int, area_ex: str = "") -> list[ProxyItem]:
        self.fetch_calls.append((num, area_ex))
        out = []
        for _ in range(num):
            out.append(ProxyItem(
                server=f"10.8.0.{self._next}:8000",
                deadline_epoch=time.time() + 3600, exit_ip=""))
            self._next += 1
        return out

    async def replace(self, server: str = "") -> list[ProxyItem] | None:
        self.replace_calls += 1
        if self.fail_replace:
            return None
        return [ProxyItem(
            server=f"10.8.9.{self.replace_calls}:8000",
            deadline_epoch=time.time() + 3600)]

    async def release(self, server: str) -> bool:
        return True


class ChannelFullProvider(ExclusiveProvider):
    """fetch 返回空并带通道满标记；alive_items 返回在用 IP（认养路径用）。"""

    def __init__(self, alive: list[ProxyItem] | None = None,
                 channel_full: bool = True):
        super().__init__()
        self.last_fetch_channel_full = False
        self._alive = alive or []
        self._channel_full = channel_full
        self.alive_calls = 0

    async def fetch(self, num: int, area_ex: str = "") -> list[ProxyItem]:
        self.fetch_calls.append((num, area_ex))
        self.last_fetch_channel_full = self._channel_full
        return []

    async def alive_items(self) -> list[ProxyItem]:
        self.alive_calls += 1
        return list(self._alive)


class TestExclusivePool:
    async def test_exclusive_topup_uses_exclusive_provider(self, fake_redis, monkeypatch):
        """exclusive 项目走独享 provider 补位；shared provider 不被调用；
        代理 URL 注入独享账密而非共享账密；独享池不做动态换段（area_ex=""）。"""
        monkeypatch.setattr("proxy_hub.pool.settings.provider_auth_key", "SK")
        monkeypatch.setattr("proxy_hub.pool.settings.provider_auth_pwd", "SP")
        monkeypatch.setattr("proxy_hub.pool.settings.exclusive_auth_key", "EK")
        monkeypatch.setattr("proxy_hub.pool.settings.exclusive_auth_pwd", "EP")
        shared, exclusive = SeqProvider(), ExclusiveProvider()
        pool.configure({"shared": shared, "exclusive": exclusive}, FakeHealth())
        await projects.create_project("exproj", pool_size=2, pool_type="exclusive")
        await projects.create_project("sharedproj", pool_size=2, pool_type="shared")
        # 种一个动态换段排除，验证独享提取旁路 area_ex
        await fake_redis.hset(redis_key(pool.K_AREA_EX.format(project="exproj")),
                              "440500", str(time.time() + 999))

        await pool.refresh("exproj")
        assert [n for n, _ in exclusive.fetch_calls] == [2]
        assert [ax for _, ax in exclusive.fetch_calls] == [""]  # 独享旁路城市换段
        assert shared.calls == []
        assert all(u.startswith("http://EK:EP@") for u in pool.get_proxy_urls("exproj"))

        # shared 项目仍走共享 provider + 共享账密（行为不变）
        await pool.refresh("sharedproj")
        assert shared.calls == [2]
        assert all(u.startswith("http://SK:SP@") for u in pool.get_proxy_urls("sharedproj"))

    async def test_exclusive_topup_skipped_when_provider_missing(self, fake_redis):
        """独享 provider 未配置（EXCLUSIVE_* env 未填）：补位跳过不 crash，不抽共享。"""
        shared = SeqProvider()
        pool.configure({"shared": shared}, FakeHealth())
        await projects.create_project(PROJ, pool_type="exclusive")
        result = await pool.refresh(PROJ)
        assert result.ok is False
        assert shared.calls == []
        assert pool.pool_size(PROJ) == 0

    async def test_channel_full_adopts_alive_ips(self, fake_redis):
        """通道满（NO_AVAILABLE_CHANNEL）时回退认养在用 IP：fetch 空 →
        alive_items 的在用 IP 走同等体检入池（服务重启/Redis 池被清/手工
        提取占用通道后自愈）。"""
        alive = [ProxyItem(server="10.8.5.1:8000",
                           deadline_epoch=time.time() + 3600)]
        exclusive = ChannelFullProvider(alive=alive, channel_full=True)
        pool.configure({"shared": SeqProvider(), "exclusive": exclusive}, FakeHealth())
        await projects.create_project(PROJ, pool_type="exclusive")

        result = await pool.refresh(PROJ)
        assert result.ok is True
        assert exclusive.alive_calls == 1
        assert pool.get_proxy_urls(PROJ) == ["http://10.8.5.1:8000"]
        assert PROJ not in pool._last_fail_mono  # 通道满不进失败冷却

    async def test_empty_fetch_non_channel_full_no_adopt(self, fake_redis):
        """fetch 空但非通道满（如 NO_RESOURCE_FOUND）：不认养、返回 0、不进冷却。"""
        alive = [ProxyItem(server="10.8.5.1:8000",
                           deadline_epoch=time.time() + 3600)]
        exclusive = ChannelFullProvider(alive=alive, channel_full=False)
        pool.configure({"shared": SeqProvider(), "exclusive": exclusive}, FakeHealth())
        await projects.create_project(PROJ, pool_type="exclusive")

        result = await pool.refresh(PROJ)
        assert result.ok is False
        assert exclusive.alive_calls == 0
        assert pool.pool_size(PROJ) == 0
        assert PROJ not in pool._last_fail_mono

    async def test_exclusive_topup_uses_liveness_probe(self, fake_redis):
        """独享池入池体检用轻量中立探针（probe_stream_liveness），不打搜索引擎。"""
        health = FakeHealth()
        pool.configure({"exclusive": ExclusiveProvider()}, health)
        await projects.create_project(PROJ, pool_type="exclusive")
        await pool.refresh(PROJ)
        assert health.liveness_calls           # 走的轻量探针
        assert health.probe_calls == []        # 没打搜索页探测

    async def test_shared_topup_uses_search_probe(self, fake_redis):
        """共享池行为不变：仍走搜索页三态探测。"""
        health = FakeHealth()
        pool.configure({"shared": SeqProvider()}, health)
        await _new_project()
        await pool.refresh(PROJ)
        assert health.probe_calls
        assert health.liveness_calls == []

    async def test_exclusive_monitor_liveness_and_extra_dead_round(
            self, fake_redis, monkeypatch):
        """独享巡检：probe_liveness 探测 + 判死轮数 = health_dead_after+1
        （默认 2+1=3 轮）——单 IP 池被误杀会直接抖动 baidu/quark 的独享路由。"""
        health = FakeHealth()
        pool.configure(
            {"exclusive": ChannelFullProvider(channel_full=False)}, health)
        await projects.create_project(PROJ, pool_type="exclusive")
        url = "http://10.8.0.1:8000"
        await _seed_zset(fake_redis, {url: time.time() + 3600})
        await pool.record_activity(PROJ)
        monkeypatch.setattr(health, "probe_liveness", lambda p: _const(PROXY_DEAD))
        await pool.monitor_tick()  # 第 1 轮
        assert pool.get_proxy_urls(PROJ) == [url]
        await pool.monitor_tick()  # 第 2 轮（共享池此时已剔）
        assert pool.get_proxy_urls(PROJ) == [url]
        assert health.probe_calls == []  # 独享巡检不打搜索页探测
        await pool.monitor_tick()  # 第 3 轮才确认剔除
        assert pool.get_proxy_urls(PROJ) == []

    async def test_record_block_triggers_replace(self, fake_redis):
        """独享 IP 被风控标记 → 异步调 provider.replace（青果全池替换语义），
        新 IP 入池、旧 IP 出池。"""
        exclusive = ExclusiveProvider()
        pool.configure({"shared": SeqProvider(), "exclusive": exclusive}, FakeHealth())
        await projects.create_project(PROJ, pool_type="exclusive")
        old_url = "http://10.8.0.1:8000"
        await _seed_zset(fake_redis, {old_url: time.time() + 3600})

        assert await pool.record_block(PROJ, old_url, "baidu") == "marked"
        for _ in range(50):  # 等后台更换任务跑完（FakeRedis 操作即时完成）
            await asyncio.sleep(0)
            if exclusive.replace_calls:
                break
        assert exclusive.replace_calls == 1
        urls = pool.get_proxy_urls(PROJ)
        assert old_url not in urls
        assert any(u.endswith("10.8.9.1:8000") for u in urls)
        # 旧 IP 的拉黑标记随之清除
        assert old_url not in pool._target_blocked.get(PROJ, {})

    async def test_replace_failure_keeps_old_ip(self, fake_redis):
        """供应商更换失败：仅记日志，旧 IP 留在池里（拉黑标记兜底）。"""
        exclusive = ExclusiveProvider(fail_replace=True)
        pool.configure({"exclusive": exclusive}, FakeHealth())
        await projects.create_project(PROJ, pool_type="exclusive")
        old_url = "http://10.8.0.1:8000"
        await _seed_zset(fake_redis, {old_url: time.time() + 3600})
        await pool._exclusive_replace(PROJ, old_url)
        assert pool.get_proxy_urls(PROJ) == [old_url]
        assert old_url not in pool._target_blocked.get(PROJ, {})

    async def test_replace_cooldown_throttles(self, fake_redis):
        """exclusive_replace_cooldown_s（默认 60s，按项目计）内不重复整池更换——
        失败尝试也写冷却时间戳（防失败风暴狂调供应商）。"""
        exclusive = ExclusiveProvider(fail_replace=True)
        pool.configure({"exclusive": exclusive}, FakeHealth())
        await projects.create_project(PROJ, pool_type="exclusive")
        old_url = "http://10.8.0.1:8000"
        await _seed_zset(fake_redis, {old_url: time.time() + 3600})
        await pool._exclusive_replace(PROJ, old_url)
        await pool._exclusive_replace(PROJ, old_url)  # 冷却内：不再调供应商
        assert exclusive.replace_calls == 1

    async def test_shared_project_record_block_no_replace(self, fake_redis):
        """shared 项目行为不变：标记走城市聚点，不触发独享更换。"""
        exclusive = ExclusiveProvider()
        pool.configure({"shared": SeqProvider(), "exclusive": exclusive}, FakeHealth())
        await _new_project()  # 默认 shared
        await _seed_zset(fake_redis, {"http://10.1.0.1:8000": time.time() + 100})
        assert await pool.record_block(PROJ, "http://10.1.0.1:8000", "bing") == "marked"
        await asyncio.sleep(0)
        assert exclusive.replace_calls == 0


# ---------------------------------------------------------------------------
# 独享池按 target 路由（消费方零改动：proxy-hub 内部按目标站选池）
# ---------------------------------------------------------------------------

EXPOOL = "expool"


class TestExclusiveRouting:
    """命中 exclusive_targets 的 target 优先走独享池；独享池空落共享兜底。"""

    async def _setup(self, fake_redis, exclusive_urls=(), shared_urls=()):
        """注册共享项目 proj + 独享项目 expool，各自种子池。"""
        _configure()
        await _new_project()  # proj，默认 shared
        await projects.create_project(EXPOOL, pool_type="exclusive")
        if shared_urls:
            await _seed_zset(fake_redis,
                             {u: time.time() + 3600 for u in shared_urls}, PROJ)
        if exclusive_urls:
            await _seed_zset(fake_redis,
                             {u: time.time() + 3600 for u in exclusive_urls}, EXPOOL)

    async def test_hit_target_routes_to_exclusive(self, fake_redis):
        """baidu 命中默认独享名单 → 从独享池选 IP，并给独享项目代记活跃
        （独享项目没有消费方，不代记会被按需驱动停补归零）。"""
        await self._setup(fake_redis,
                          exclusive_urls=["http://10.8.0.1:8000"],
                          shared_urls=["http://10.0.0.1:8000"])
        url = await pool.pick_proxy_routed(PROJ, "www.baidu.com")
        assert url == "http://10.8.0.1:8000"
        for _ in range(10):  # 等活跃代记后台任务跑完
            await asyncio.sleep(0)
            if await pool.has_recent_activity(EXPOOL, 300):
                break
        assert await pool.has_recent_activity(EXPOOL, 300) is True

    async def test_exclusive_empty_falls_back_to_shared(self, fake_redis):
        """独享池空（注册了独享项目但拿不出 IP）→ 自动落回共享池；
        且命中名单即给独享项目代记活跃（否则冷启动死锁：池空→落共享→
        不记活跃→永不补位→池永远空）。"""
        await self._setup(fake_redis, shared_urls=["http://10.0.0.1:8000"])
        url = await pool.pick_proxy_routed(PROJ, "www.baidu.com")
        assert url == "http://10.0.0.1:8000"
        for _ in range(10):  # 等活跃代记后台任务跑完
            await asyncio.sleep(0)
            if await pool.has_recent_activity(EXPOOL, 300):
                break
        assert await pool.has_recent_activity(EXPOOL, 300) is True

    async def test_non_listed_target_uses_shared(self, fake_redis):
        """bing 不在独享名单 → 走共享池，独享池有 IP 也不受影响。"""
        await self._setup(fake_redis,
                          exclusive_urls=["http://10.8.0.1:8000"],
                          shared_urls=["http://10.0.0.1:8000"])
        url = await pool.pick_proxy_routed(PROJ, "cn.bing.com")
        assert url == "http://10.0.0.1:8000"

    async def test_empty_targets_disables_routing(self, fake_redis):
        """exclusive_targets=[] = 功能关闭：baidu 也全走共享池。"""
        await runtime_config.set_runtime_config({"exclusive_targets": []})
        await self._setup(fake_redis,
                          exclusive_urls=["http://10.8.0.1:8000"],
                          shared_urls=["http://10.0.0.1:8000"])
        url = await pool.pick_proxy_routed(PROJ, "www.baidu.com")
        assert url == "http://10.0.0.1:8000"

    async def test_no_exclusive_project_uses_shared(self, fake_redis):
        """未注册独享项目：命中名单也直接走共享池（行为与路由引入前一致）。"""
        _configure()
        await _new_project()
        await _seed_zset(fake_redis, {"http://10.0.0.1:8000": time.time() + 3600})
        url = await pool.pick_proxy_routed(PROJ, "www.baidu.com")
        assert url == "http://10.0.0.1:8000"

    async def test_record_block_cross_pool_marks_exclusive(self, fake_redis):
        """record_block 跨池归属：隧道凭证是共享项目，url 实际在独享池——
        按 ZSCORE 确认的归属池（独享池）记录拉黑，并触发独享更换IP。"""
        await self._setup(fake_redis,
                          exclusive_urls=["http://10.8.0.1:8000"],
                          shared_urls=["http://10.0.0.1:8000"])
        # _setup 的 _configure 只配了 shared；record_block 触发更换前先补配独享
        # provider（后台任务执行时才读 _provider_for，此处无 await 不抢占）
        exclusive = ExclusiveProvider()
        pool.configure({"shared": SeqProvider(), "exclusive": exclusive}, FakeHealth())
        assert await pool.record_block(PROJ, "http://10.8.0.1:8000", "baidu") == "marked"
        # 拉黑记在独享项目名下，共享项目不受影响
        assert pool._target_blocked[EXPOOL]["http://10.8.0.1:8000"] == {"baidu"}
        assert PROJ not in pool._target_blocked
        for _ in range(50):  # 等后台更换任务跑完
            await asyncio.sleep(0)
            if exclusive.replace_calls:
                break
        assert exclusive.replace_calls == 1  # 全池替换：忽略单 server 参数
        excl_urls = pool.get_proxy_urls(EXPOOL)
        assert "http://10.8.0.1:8000" not in excl_urls
        assert any(u.endswith("10.8.9.1:8000") for u in excl_urls)

    async def test_is_exclusive_upstream(self, fake_redis):
        """门控判定：upstream URL 是否来自独享池（按镜像成员）。"""
        await self._setup(fake_redis,
                          exclusive_urls=["http://10.8.0.1:8000"],
                          shared_urls=["http://10.0.0.1:8000"])
        await pool._exclusive_project()  # 填充定位缓存
        assert pool.is_exclusive_upstream("http://10.8.0.1:8000") is True
        assert pool.is_exclusive_upstream("http://10.0.0.1:8000") is False
