"""qg.net 独享代理适配测试：data.ips 响应解析（正常/异常/缺字段）、
稳态空结果错误码（NO_AVAILABLE_CHANNEL 等）不抛异常、replace 全池替换语义。

httpx 不真出网：mock provider._call 直接返回构造的响应 dict。
"""
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from proxy_hub.providers.qgnet_exclusive import QgNetExclusiveProvider


def _provider(default_ttl_s: float = 3600.0) -> QgNetExclusiveProvider:
    return QgNetExclusiveProvider(
        "https://exclusive.proxy.qg.net?key=KEY1",
        default_ttl_s=default_ttl_s, timeout_s=1.0,
    )


def _deadline_str(days: int = 1) -> str:
    """未来的北京时间 deadline 字符串（parse_deadline_epoch 按北京时间解析）。"""
    ts = time.time() + days * 86400
    return datetime.fromtimestamp(ts, ZoneInfo("Asia/Shanghai")).strftime(
        "%Y-%m-%d %H:%M:%S")


def _ips_payload(*servers: str) -> dict:
    """构造真实 /get 响应形态：data.ips 列表。"""
    return {"code": "SUCCESS", "request_id": "r1", "data": {
        "task_id": "t1", "num": len(servers),
        "ips": [{"proxy_ip": s.split(":")[0], "server": s,
                 "area_code": 370100, "area": "山东省济南市", "isp": "电信",
                 "deadline": _deadline_str()} for s in servers],
    }}


@pytest.mark.parametrize("api_url", [
    "https://exclusive.proxy.qg.net/get?key=XXX",   # 控制台生成的提取链接（推荐填法）
    "https://exclusive.proxy.qg.net?key=XXX",
    "https://exclusive.proxy.qg.net/?key=XXX",
])
def test_console_link_forms_normalize(api_url):
    """三种填法归一到同一基址；最终请求 URL 不重复拼接端点（/get/get 404 回归）。"""
    p = QgNetExclusiveProvider(api_url)
    assert p._url("/get", num=2) == \
        "https://exclusive.proxy.qg.net/get?key=XXX&num=2"
    assert p._url("/query") == "https://exclusive.proxy.qg.net/query?key=XXX"
    assert p._url("/replace") == "https://exclusive.proxy.qg.net/replace?key=XXX"
    assert p._url("/delete", ip="1.1.1.1:1") == \
        "https://exclusive.proxy.qg.net/delete?key=XXX&ip=1.1.1.1:1"


def test_api_url_splits_base_and_query():
    """控制台生成的链接自带 ?key=...：拆出基址，拼接口 URL 时原样带上。"""
    p = _provider()
    assert p.api_url == "https://exclusive.proxy.qg.net?key=KEY1"
    assert p._url("/get", num=2) == \
        "https://exclusive.proxy.qg.net/get?key=KEY1&num=2"


class TestFetch:
    async def test_fetch_parses_data_ips(self, monkeypatch):
        """真实响应形态：列表在 data.ips，deadline 北京时间解析为到期 epoch。"""
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(
            _ips_payload("140.250.148.7:11207")))
        items = await p.fetch(1)
        assert len(items) == 1
        assert items[0].server == "140.250.148.7:11207"
        assert items[0].exit_ip == "140.250.148.7"
        assert items[0].area_code == "370100"  # int 归一成 str
        assert items[0].deadline_epoch > time.time() + 86000

    async def test_fetch_accepts_legacy_wrappers(self, monkeypatch):
        """兼容兜底：data 直接是数组 / data.list 也能解析。"""
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const({
            "code": "SUCCESS",
            "data": {"list": [{"ip": "5.6.7.8", "port": 8080,
                               "expire_time": str(int((time.time() + 7200) * 1000))}]},
        }))
        items = await p.fetch(1)
        assert [it.server for it in items] == ["5.6.7.8:8080"]
        assert abs(items[0].deadline_epoch - (time.time() + 7200)) < 5

    async def test_fetch_missing_deadline_uses_default_ttl(self, monkeypatch):
        """缺到期字段 → 提取时刻 + 默认租期兜底。"""
        p = _provider(default_ttl_s=3600.0)
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const({
            "code": "SUCCESS", "data": {"ips": [{"server": "9.9.9.9:1000"}]},
        }))
        items = await p.fetch(1)
        assert abs(items[0].deadline_epoch - (time.time() + 3600.0)) < 5

    async def test_fetch_empty_ips_raises(self, monkeypatch):
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(
            {"code": "SUCCESS", "data": {"ips": []}}))
        with pytest.raises(ValueError, match="empty/unparseable"):
            await p.fetch(1)

    async def test_fetch_only_expired_raises(self, monkeypatch):
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const({
            "code": "SUCCESS", "data": {"ips": [
                {"server": "1.1.1.1:1", "deadline": "2020-01-01 00:00:00"}]},
        }))
        with pytest.raises(ValueError, match="empty/unparseable"):
            await p.fetch(1)


def _stub_raw_http(p, monkeypatch, status_code: int, body: str):
    """让 _call 走真实 HTTP 层：返回指定状态码 + body 的假响应。"""
    import json as _json

    import httpx

    class FakeResp:
        text = body

        def raise_for_status(self):
            if status_code >= 400:
                raise httpx.HTTPStatusError(
                    f"{status_code}", request=None, response=None)

        def json(self):
            return _json.loads(body)

    class FakeClient:
        def __init__(self, *a, **k): pass

        async def __aenter__(self): return self

        async def __aexit__(self, *a): return False

        async def get(self, url): return FakeResp()

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)


class TestBenignEmptyCodes:
    """NO_AVAILABLE_CHANNEL（通道已满）/ NO_RESOURCE_FOUND 是独享池稳态下的
    正常空结果：返回空而非抛异常，不进失败冷却。"""

    @pytest.mark.parametrize("code", ["NO_AVAILABLE_CHANNEL", "NO_RESOURCE_FOUND"])
    async def test_benign_codes_return_none(self, monkeypatch, code):
        p = _provider()
        _stub_raw_http(p, monkeypatch, 200, f'{{"code":"{code}"}}')
        assert await p._call("/get", num=1) is None


class TestHttp400WrappedBusinessCodes:
    """青果独享业务错误码用 HTTP 400 包裹返回（实测：通道满时 400 +
    {"code":"NO_AVAILABLE_CHANNEL",...}）——先按 body code 走业务逻辑，
    只有无 code / 非 JSON 才按 HTTP 层故障（raise_for_status）处理。"""

    async def test_400_channel_full_is_benign(self, monkeypatch):
        """HTTP 400 + NO_AVAILABLE_CHANNEL body → fetch 返回空列表且置
        last_fetch_channel_full，不抛异常（认养逻辑才有机会跑）。"""
        p = _provider()
        _stub_raw_http(p, monkeypatch, 400,
                       '{"code":"NO_AVAILABLE_CHANNEL","message":"没有可用的空闲通道",'
                       '"request_id":"r1"}')
        assert await p.fetch(1) == []
        assert p.last_fetch_channel_full is True

    async def test_400_other_business_code_raises_value_error(self, monkeypatch):
        """HTTP 400 + 其他业务错误码（INVALID_KEY 等）→ ValueError 带原文。"""
        p = _provider()
        _stub_raw_http(p, monkeypatch, 400,
                       '{"code":"INVALID_KEY","message":"key 不存在"}')
        with pytest.raises(ValueError, match="INVALID_KEY"):
            await p.fetch(1)

    async def test_400_without_code_raises_http_error(self, monkeypatch):
        """HTTP 400 + 无 code 的 body → 真 HTTP 层故障，抛 HTTPStatusError。"""
        import httpx

        p = _provider()
        _stub_raw_http(p, monkeypatch, 400, '{"error":"Bad Request"}')
        with pytest.raises(httpx.HTTPStatusError):
            await p.fetch(1)

    async def test_502_non_json_raises_http_error(self, monkeypatch):
        """HTTP 502 + 非 JSON 错误页 → HTTPStatusError。"""
        import httpx

        p = _provider()
        _stub_raw_http(p, monkeypatch, 502, "<html>Bad Gateway</html>")
        with pytest.raises(httpx.HTTPStatusError):
            await p.fetch(1)

    async def test_fetch_maps_benign_code_to_empty_list(self, monkeypatch):
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(None))
        assert await p.fetch(1) == []

    async def test_other_error_codes_raise(self, monkeypatch):
        p = _provider()

        async def boom(*a, **k):
            raise ValueError("exclusive API /get non-success: code='INVALID_KEY'")
        monkeypatch.setattr(p, "_call", boom)
        with pytest.raises(ValueError, match="INVALID_KEY"):
            await p.fetch(1)


class TestReplace:
    """青果 /replace 语义：释放全部已提取资源再重新提取（全池替换）。"""

    async def test_replace_returns_new_pool_list(self, monkeypatch):
        p = _provider()
        calls = []
        monkeypatch.setattr(p, "_call", lambda path, **kw: _const(
            calls.append((path, kw)) or _ips_payload("2.2.2.2:2000", "3.3.3.3:3000")))
        items = await p.replace("1.1.1.1:1000")  # server 参数保留兼容，实际忽略
        assert [it.server for it in items] == ["2.2.2.2:2000", "3.3.3.3:3000"]
        assert calls[0][0] == "/replace"
        assert "proxy" not in calls[0][1]  # 不传单个 server

    async def test_replace_failure_returns_none(self, monkeypatch):
        p = _provider()

        async def boom(*a, **k):
            raise ValueError("supplier down")
        monkeypatch.setattr(p, "_call", boom)
        assert await p.replace() is None

    async def test_replace_no_new_ip_returns_none(self, monkeypatch):
        """更换"成功"但响应没带新 IP：视为失败（不能让池被清空）。"""
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(
            {"code": "SUCCESS", "data": {"ips": []}}))
        assert await p.replace() is None

    async def test_replace_benign_code_returns_none(self, monkeypatch):
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(None))
        assert await p.replace() is None


class TestListAlive:
    async def test_query_flattens_tasks_ips(self, monkeypatch):
        """实测 /query 响应：列表嵌套在 data.tasks[*].ips（按任务分组拍平）。"""
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const({
            "code": "SUCCESS", "request_id": "r1", "data": {
                "num": 2,
                "tasks": [
                    {"task_id": "t1", "num": 1, "ips": [
                        {"proxy_ip": "140.250.148.7", "server": "140.250.148.7:11207",
                         "area_code": 370100, "area": "山东省济南市", "isp": "电信",
                         "deadline": _deadline_str()}]},
                    {"task_id": "t2", "num": 1, "ips": [
                        {"proxy_ip": "8.8.8.8", "server": "8.8.8.8:8080"}]},
                ],
            }}))
        alive = await p.list_alive()
        assert [d["server"] for d in alive] == ["140.250.148.7:11207", "8.8.8.8:8080"]

    async def test_query_falls_back_to_data_ips(self, monkeypatch):
        """无 tasks 字段时保留 data.ips 包装兼容。"""
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(
            _ips_payload("140.250.148.7:11207")))
        alive = await p.list_alive()
        assert [d["server"] for d in alive] == ["140.250.148.7:11207"]

    async def test_query_benign_empty(self, monkeypatch):
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(None))
        assert await p.list_alive() == []


class TestChannelFullFlag:
    """fetch 空结果区分"通道已满"（可认养在用 IP）与其他稳态空结果。"""

    @staticmethod
    def _stub_call(p, monkeypatch, code):
        async def fake_call(path, **kw):
            p._last_benign_code = code
            return None
        monkeypatch.setattr(p, "_call", fake_call)

    async def test_channel_full_sets_flag(self, monkeypatch):
        p = _provider()
        self._stub_call(p, monkeypatch, "NO_AVAILABLE_CHANNEL")
        assert await p.fetch(1) == []
        assert p.last_fetch_channel_full is True

    async def test_no_resource_found_clears_flag(self, monkeypatch):
        p = _provider()
        self._stub_call(p, monkeypatch, "NO_RESOURCE_FOUND")
        assert await p.fetch(1) == []
        assert p.last_fetch_channel_full is False

    async def test_success_resets_flag(self, monkeypatch):
        p = _provider()
        p.last_fetch_channel_full = True
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const(
            _ips_payload("1.2.3.4:1000")))
        assert len(await p.fetch(1)) == 1
        assert p.last_fetch_channel_full is False

    async def test_alive_items_parses_to_proxy_items(self, monkeypatch):
        """认养路径：/query 的 tasks[*].ips 解析成 ProxyItem（体检入池用）。"""
        from proxy_hub.providers.base import ProxyItem
        p = _provider()
        monkeypatch.setattr(p, "_call", lambda *a, **k: _const({
            "code": "SUCCESS", "data": {"num": 1, "tasks": [
                {"task_id": "t1", "num": 1, "ips": [
                    {"proxy_ip": "140.250.148.7", "server": "140.250.148.7:11207",
                     "area_code": 370100, "area": "山东省济南市", "isp": "电信",
                     "deadline": _deadline_str()}]}]},
        }))
        items = await p.alive_items()
        assert len(items) == 1
        assert isinstance(items[0], ProxyItem)
        assert items[0].server == "140.250.148.7:11207"
        assert items[0].deadline_epoch > time.time()


class TestRelease:
    async def test_release_true_on_success(self, monkeypatch):
        p = _provider()
        calls = []
        monkeypatch.setattr(p, "_call", lambda path, **kw: _const(
            calls.append((path, kw)) or {"code": "SUCCESS"}))
        assert await p.release("1.1.1.1:1000") is True
        # IP 参数按文档惯例同时带 ip= 与 server=（待真实凭证确认生效名）
        assert calls[0][1]["ip"] == "1.1.1.1:1000"
        assert calls[0][1]["server"] == "1.1.1.1:1000"

    async def test_release_false_on_failure(self, monkeypatch):
        p = _provider()

        async def boom(*a, **k):
            raise ValueError("supplier down")
        monkeypatch.setattr(p, "_call", boom)
        assert await p.release("1.1.1.1:1000") is False


async def _const(v):
    return v
