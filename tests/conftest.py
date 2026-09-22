"""测试公共件：内存 Redis stub（项目不引 fakeredis，stub 只实现用到的最小接口）。"""
from __future__ import annotations

import pytest

from proxy_hub import forwarder, pool, projects, runtime_config


class FakePipeline:
    def __init__(self, store: FakeRedis) -> None:
        self._store = store
        self._ops: list[tuple] = []

    def set(self, key, val, ex=None):
        self._ops.append(("set", key, val, ex))
        return self

    def incr(self, key):
        self._ops.append(("incr", key))
        return self

    def expire(self, key, t):
        self._ops.append(("expire", key, t))
        return self

    def hincrby(self, key, field, amt):
        self._ops.append(("hincrby", key, field, amt))
        return self

    def hset(self, key, field, val):
        self._ops.append(("hset", key, field, val))
        return self

    def lpush(self, key, val):
        self._ops.append(("lpush", key, val))
        return self

    def ltrim(self, key, s, e):
        self._ops.append(("ltrim", key, s, e))
        return self

    def delete(self, key):
        self._ops.append(("delete", key))
        return self

    async def execute(self):
        for op in self._ops:
            getattr(self._store, f"_do_{op[0]}")(*op[1:])
        return True


class FakeRedis:
    """最小内存实现：string/hash/list/zset + pipeline。锁的 NX/PX 语义按测试需要实现。"""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, object]] = {}
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    # string
    async def get(self, key):
        return self.strings.get(key)

    async def set(self, key, val, ex=None, nx=False, px=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = str(val)
        return True

    def _do_set(self, key, val, ex=None):
        self.strings[key] = str(val)

    async def delete(self, key):
        self.strings.pop(key, None)
        self.hashes.pop(key, None)
        self.lists.pop(key, None)
        self.zsets.pop(key, None)
        return 1

    def _do_delete(self, key):
        self.strings.pop(key, None)

    # counter
    def _do_incr(self, key):
        self.strings[key] = str(int(self.strings.get(key, "0")) + 1)

    def _do_expire(self, key, t):
        pass

    # hash
    async def hset(self, key, field, val):
        self._do_hset(key, field, val)
        return 1

    def _do_hset(self, key, field, val):
        self.hashes.setdefault(key, {})[field] = val

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def hdel(self, key, *fields):
        h = self.hashes.get(key, {})
        n = 0
        for f in fields:
            if f in h:
                del h[f]
                n += 1
        return n

    async def hincrby(self, key, field, amt):
        self._do_hincrby(key, field, amt)

    def _do_hincrby(self, key, field, amt):
        h = self.hashes.setdefault(key, {})
        h[field] = int(h.get(field, 0)) + amt

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    # list
    def _do_lpush(self, key, val):
        self.lists.setdefault(key, []).insert(0, val)

    def _do_ltrim(self, key, s, e):
        lst = self.lists.get(key, [])
        self.lists[key] = lst[s:e + 1]

    async def lpop(self, key):
        lst = self.lists.get(key, [])
        return lst.pop(0) if lst else None

    # zset
    async def zadd(self, key, mapping: dict):
        self.zsets.setdefault(key, {}).update({m: float(s) for m, s in mapping.items()})
        return len(mapping)

    async def zrange(self, key, start, stop, withscores=False):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])
        if stop == -1:
            sliced = items[start:]
        else:
            sliced = items[start:stop + 1]
        if withscores:
            return sliced
        return [m for m, _ in sliced]

    async def zrem(self, key, *members):
        z = self.zsets.get(key, {})
        n = 0
        for m in members:
            if m in z:
                del z[m]
                n += 1
        return n

    async def zremrangebyscore(self, key, min_s, max_s):
        z = self.zsets.get(key, {})
        doomed = [m for m, s in z.items() if float(min_s) <= s <= float(max_s)]
        for m in doomed:
            del z[m]
        return len(doomed)

    async def zcard(self, key):
        return len(self.zsets.get(key, {}))

    async def zscore(self, key, member):
        return self.zsets.get(key, {}).get(member)

    def pipeline(self, transaction=True):
        return FakePipeline(self)


@pytest.fixture
def fake_redis(monkeypatch):
    """把 pool / runtime_config / projects 的 get_redis 都指向同一个内存 stub。"""
    store = FakeRedis()
    monkeypatch.setattr(pool, "get_redis", lambda: store)
    monkeypatch.setattr(runtime_config, "get_redis", lambda: store)
    monkeypatch.setattr(projects, "get_redis", lambda: store)
    yield store


@pytest.fixture(autouse=True)
def _reset_module_state(fake_redis, monkeypatch):
    """每个测试前重置模块级状态（池镜像、缓存、锁计数器）。"""
    pool._pools = {}
    pool._rr_index = {}
    pool._target_hist = {}
    pool._fail_counters = {}
    pool._last_fail_mono = {}
    pool._last_tier = {}
    pool._activity_cache = {}
    pool._empty_since = {}
    pool._empty_alerted = {}
    pool._target_blocked = {}
    pool._block_kill_ts = {}
    pool._ip_area = {}
    pool._targeted_topup_last = {}
    pool._targeted_topup_exec_last = {}
    pool._providers = {}
    pool._exclusive_replace_sched = {}
    pool._exclusive_replace_last = {}
    pool._exclusive_project_cache = (0.0, None)
    pool._exclusive_activity_marked = {}
    forwarder._ip_last_use = {}
    runtime_config._cache = None
    projects._table_cache = (0.0, {})
    yield
