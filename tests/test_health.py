"""健康巡检纯函数/类测试：连续判死计数；独享池轻量探针（probe_liveness）。"""
import asyncio

import pytest

from proxy_hub.health import (
    PROXY_DEAD,
    PROXY_REACHABLE,
    PROXY_SEARCHABLE,
    FailCounter,
    HealthChecker,
)


class TestFailCounter:
    def test_single_failure_not_dead(self):
        fc = FailCounter(dead_after=2)
        assert fc.record("p1", PROXY_DEAD) is False

    def test_consecutive_failures_confirmed_dead(self):
        fc = FailCounter(dead_after=2)
        fc.record("p1", PROXY_DEAD)
        assert fc.record("p1", PROXY_DEAD) is True

    def test_recovery_resets_count(self):
        fc = FailCounter(dead_after=2)
        fc.record("p1", PROXY_DEAD)
        fc.record("p1", PROXY_SEARCHABLE)   # 恢复 → 清零
        assert fc.record("p1", PROXY_DEAD) is False  # 重新计数，一轮不死

    def test_reachable_counts_as_alive(self):
        fc = FailCounter(dead_after=2)
        fc.record("p1", PROXY_DEAD)
        fc.record("p1", PROXY_REACHABLE)
        assert fc.record("p1", PROXY_DEAD) is False

    def test_dead_after_one_is_immediate(self):
        fc = FailCounter(dead_after=1)
        assert fc.record("p1", PROXY_DEAD) is True

    def test_invalid_dead_after(self):
        with pytest.raises(ValueError):
            FailCounter(dead_after=0)

    def test_set_dead_after_hot_update(self):
        fc = FailCounter(dead_after=2)
        fc.set_dead_after(3)
        fc.record("p1", PROXY_DEAD)
        fc.record("p1", PROXY_DEAD)
        assert fc.record("p1", PROXY_DEAD) is True
        fc.set_dead_after(0)  # 非法值收敛到 1
        assert fc.record("p2", PROXY_DEAD) is True

    def test_prune_removes_stale_entries(self):
        fc = FailCounter(dead_after=5)
        fc.record("p1", PROXY_DEAD)
        fc.record("p2", PROXY_DEAD)
        fc.prune(["p1"])
        assert "p2" not in fc._counts and "p1" in fc._counts


class TestProbeLiveness:
    """独享池轻量探针：中立 204 端点、任意 HTTP 响应即活、禁用 slow-as-dead。"""

    @staticmethod
    def _checker() -> HealthChecker:
        return HealthChecker(
            check_url="http://search.example/q", markers=(), timeout_s=5.0,
            min_body_bytes=1, user_agent="t", max_latency_s=0.05,
            liveness_url="http://liveness.example/generate_204",
        )

    @staticmethod
    def _stub_client(monkeypatch, seen, *, fail=False, delay_s=0.0):
        import httpx

        class FakeResp:
            status_code = 204

        class FakeClient:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, url):
                seen["url"] = url
                if delay_s:
                    await asyncio.sleep(delay_s)
                if fail:
                    raise ConnectionError("connect failed")
                return FakeResp()

        monkeypatch.setattr(httpx, "AsyncClient", FakeClient)

    async def test_slow_response_not_dead_and_uses_liveness_url(self, monkeypatch):
        """探测耗时超过 max_latency_s 仍判活（独享 IP 慢≠死）；请求打的是
        中立探针 URL 而非搜索引擎探测页。"""
        hc = self._checker()
        seen = {}
        self._stub_client(monkeypatch, seen, delay_s=0.1)  # 超过 0.05s 门槛
        assert await hc.probe_liveness("http://1.2.3.4:8000") == PROXY_REACHABLE
        assert seen["url"] == "http://liveness.example/generate_204"

    async def test_connection_failure_dead(self, monkeypatch):
        """连接层失败才判死。"""
        hc = self._checker()
        self._stub_client(monkeypatch, {}, fail=True)
        assert await hc.probe_liveness("http://1.2.3.4:8000") == PROXY_DEAD
