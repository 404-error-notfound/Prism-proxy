"""自建转发代理测试：认证、CONNECT 隧道、普通 HTTP 转发、失败重试。

用 asyncio 起真实 TCP 的模拟上游与转发器（端口 0 随机分配），端到端走字节。
"""
import asyncio
import base64
import time

import pytest

from proxy_hub import forwarder, pool, projects, runtime_config
from proxy_hub.proxy_url import make_proxy_url

PROJ = "proj"


# ---------------------------------------------------------------------------
# 模拟上游代理（接收 CONNECT / 普通 HTTP，回固定响应或回显）
# ---------------------------------------------------------------------------

class FakeUpstream:
    """模拟供应商中转网关：CONNECT 回 200 后回显；plain HTTP 回固定 body。"""

    def __init__(self) -> None:
        self.server: asyncio.AbstractServer | None = None
        self.port = 0
        self.connect_requests: list[bytes] = []
        self._writers: list[asyncio.StreamWriter] = []

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        # 先断开活跃连接，否则 wait_closed 要等 relay 空闲超时才返回
        for w in self._writers:
            w.close()
        self.server.close()
        await self.server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        self._writers.append(writer)
        try:
            request_line = await reader.readline()
            self.connect_requests.append(request_line)
            if request_line.startswith(b"CONNECT"):
                while await reader.readline() not in (b"\r\n", b"\n", b""):
                    pass
                writer.write(b"HTTP/1.1 200 OK\r\n\r\n")
                await writer.drain()
                # 隧道回显
                while data := await reader.read(1024):
                    writer.write(b"echo:" + data)
                    await writer.drain()
            else:
                while await reader.readline() not in (b"\r\n", b"\n", b""):
                    pass
                body = b"upstream-response"
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n"
                             b"Connection: close\r\n\r\n" % len(body) + body)
                await writer.drain()
        finally:
            writer.close()


# ---------------------------------------------------------------------------
# 测试用转发器（真实监听随机端口）
# ---------------------------------------------------------------------------

class TestForwarder:
    @pytest.fixture
    async def fwd(self):
        sem = asyncio.Semaphore(10)
        server = await asyncio.start_server(
            lambda r, w: forwarder._guarded_handle(sem, r, w), "127.0.0.1", 0
        )
        port = server.sockets[0].getsockname()[1]
        self._clients: list[asyncio.StreamWriter] = []
        yield port
        # 先断开客户端连接，否则 wait_closed 要等 relay 空闲超时才返回
        for w in self._clients:
            w.close()
        server.close()
        await server.wait_closed()

    async def _connect(self, port: int):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        self._clients.append(writer)
        return reader, writer

    @pytest.fixture
    async def upstream(self):
        up = FakeUpstream()
        await up.start()
        yield up
        await up.stop()

    async def _seed_project_with_upstream(self, upstream_port: int) -> str:
        proj = await projects.create_project(PROJ)
        url = make_proxy_url(f"127.0.0.1:{upstream_port}")
        pool._pools[PROJ] = [(url, time.time() + 120)]
        return proj["token"]

    @staticmethod
    def _auth_header(token: str) -> bytes:
        cred = base64.b64encode(f"{PROJ}:{token}".encode()).decode()
        return b"Proxy-Authorization: Basic " + cred.encode() + b"\r\n"

    async def test_missing_auth_407(self, fwd):
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT example.com:443 HTTP/1.1\r\n\r\n")
        await writer.drain()
        status = await reader.readline()
        assert b"407" in status
        writer.close()

    async def test_wrong_token_407(self, fwd):
        await projects.create_project(PROJ)
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT example.com:443 HTTP/1.1\r\n"
                     + self._auth_header("wrong-token") + b"\r\n")
        await writer.drain()
        status = await reader.readline()
        assert b"407" in status
        writer.close()

    async def test_connect_tunnel_echo(self, fwd, upstream):
        token = await self._seed_project_with_upstream(upstream.port)
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT example.com:443 HTTP/1.1\r\n"
                     + self._auth_header(token) + b"\r\n")
        await writer.drain()
        status = await reader.readline()
        assert b"200" in status
        # 吞掉剩余响应头
        while await reader.readline() not in (b"\r\n", b"\n", b""):
            pass
        writer.write(b"ping")
        await writer.drain()
        # 回显内容无换行符，不能用 readline（会等到连接关闭才返回）
        assert await asyncio.wait_for(reader.read(9), timeout=5) == b"echo:ping"
        # CONNECT 请求确实到达了模拟上游
        assert upstream.connect_requests[0].startswith(b"CONNECT example.com:443")
        # 建连记了一次项目活跃
        assert await pool.has_recent_activity(PROJ, 60) is True
        writer.close()

    async def test_plain_http_forward(self, fwd, upstream):
        token = await self._seed_project_with_upstream(upstream.port)
        reader, writer = await self._connect(fwd)
        writer.write(b"GET http://example.com/path HTTP/1.1\r\n"
                     b"Host: example.com\r\n"
                     + self._auth_header(token) + b"\r\n")
        await writer.drain()
        data = await asyncio.wait_for(reader.read(4096), timeout=5)
        assert b"upstream-response" in data
        writer.close()

    async def test_upstream_dead_502(self, fwd):
        """上游连不上 → 换 IP 重试（池里只有这一个）→ 502 + 记失败。"""
        proj = await projects.create_project(PROJ)
        url = make_proxy_url("127.0.0.1:1")  # 必连不上
        pool._pools[PROJ] = [(url, time.time() + 120)]
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT example.com:443 HTTP/1.1\r\n"
                     + self._auth_header(proj["token"]) + b"\r\n")
        await writer.drain()
        status = await reader.readline()
        assert b"502" in status
        assert pool._fail_counters[PROJ]._counts.get(url) == 1  # 记了一次失败
        writer.close()

    async def test_empty_pool_502(self, fwd):
        proj = await projects.create_project(PROJ)
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT example.com:443 HTTP/1.1\r\n"
                     + self._auth_header(proj["token"]) + b"\r\n")
        await writer.drain()
        status = await reader.readline()
        assert b"502" in status
        writer.close()

    async def test_target_recorded_for_anti_repeat(self, fwd, upstream):
        token = await self._seed_project_with_upstream(upstream.port)
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT www.baidu.com:443 HTTP/1.1\r\n"
                     + self._auth_header(token) + b"\r\n")
        await writer.drain()
        await reader.readline()
        url = pool.get_proxy_urls(PROJ)[0]
        assert "www.baidu.com" in pool._target_hist[PROJ][url]
        writer.close()

    async def test_connect_records_recent_conn_mapping(self, fwd, upstream):
        """CONNECT 成功建联后写入近期建联映射（/block-signal 风控反查依据）。"""
        forwarder._recent_conns.clear()
        token = await self._seed_project_with_upstream(upstream.port)
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT www.sogou.com:443 HTTP/1.1\r\n"
                     + self._auth_header(token) + b"\r\n")
        await writer.drain()
        await reader.readline()
        url = pool.get_proxy_urls(PROJ)[0]
        assert forwarder.recent_upstreams(PROJ, "sogou") == [url]
        writer.close()

    async def test_open_upstream_throttles_exclusive_only(self, fwd, upstream,
                                                          monkeypatch):
        """门控按 upstream URL 的池归属判定：命中独享名单的目标（baidu）被
        路由到独享池并门控；未命中目标（example.com）走共享池不门控。"""
        calls: list[str] = []

        async def fake_throttle(url):
            calls.append(url)

        monkeypatch.setattr(forwarder, "_throttle_exclusive_ip", fake_throttle)
        # 凭证项目 proj（shared）+ 独享项目 expool，两个池各一个独立上游
        # （URL 必须不同，否则门控按 URL 归属判定时无法区分）
        token = await self._seed_project_with_upstream(upstream.port)
        up2 = FakeUpstream()
        await up2.start()
        await projects.create_project("expool", pool_type="exclusive")
        excl_url = make_proxy_url(f"127.0.0.1:{up2.port}")
        pool._pools["expool"] = [(excl_url, time.time() + 120)]

        # baidu 目标：路由独享池 → 门控
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT www.baidu.com:443 HTTP/1.1\r\n"
                     + self._auth_header(token) + b"\r\n")
        await writer.drain()
        await reader.readline()
        writer.close()
        assert calls == [excl_url]

        # 未命中名单目标：共享池 → 不门控
        reader, writer = await self._connect(fwd)
        writer.write(b"CONNECT example.com:443 HTTP/1.1\r\n"
                     + self._auth_header(token) + b"\r\n")
        await writer.drain()
        await reader.readline()
        writer.close()
        assert calls == [excl_url]
        await up2.stop()


# ---------------------------------------------------------------------------
# 近期建联映射（/block-signal 风控信号反查上游 IP）
# ---------------------------------------------------------------------------

class TestRecentConns:
    def setup_method(self):
        forwarder._recent_conns.clear()

    def test_substring_match_and_project_isolation(self):
        """引擎名片段子串匹配 CONNECT authority；项目间隔离。"""
        forwarder.note_conn("p1", "www.baidu.com", "http://1.1.1.1:80")
        forwarder.note_conn("p1", "cn.bing.com", "http://2.2.2.2:80")
        forwarder.note_conn("p2", "www.baidu.com", "http://3.3.3.3:80")
        assert forwarder.recent_upstreams("p1", "baidu") == ["http://1.1.1.1:80"]
        assert forwarder.recent_upstreams("p1", "bing") == ["http://2.2.2.2:80"]
        assert forwarder.recent_upstreams("p1", "quark") == []

    def test_window_expiry(self, monkeypatch):
        forwarder.note_conn("p1", "www.baidu.com", "http://1.1.1.1:80")
        now = time.monotonic()
        monkeypatch.setattr(forwarder.time, "monotonic", lambda: now + 60)
        assert forwarder.recent_upstreams("p1", "baidu") == []


# ---------------------------------------------------------------------------
# 独享池每 IP QPS 门控（仅 pool_type=exclusive 项目生效）
# ---------------------------------------------------------------------------

class TestExclusiveIpThrottle:
    async def test_throttle_waits_between_uses(self, fake_redis):
        """同一独享 IP 两次使用间隔 < 1/qps 时 sleep 等待；不同 IP 互不影响。"""
        await runtime_config.set_runtime_config({"exclusive_per_ip_qps": 10.0})
        t0 = time.monotonic()
        await forwarder._throttle_exclusive_ip("http://1.1.1.1:1")
        await forwarder._throttle_exclusive_ip("http://1.1.1.1:1")
        assert time.monotonic() - t0 >= 0.09  # 1/10s = 100ms（留点抖动余量）
        # 不同 IP 不互相阻塞
        t0 = time.monotonic()
        await forwarder._throttle_exclusive_ip("http://2.2.2.2:2")
        assert time.monotonic() - t0 < 0.09

    async def test_throttle_disabled_when_qps_zero(self, fake_redis):
        """qps=0 = 关闭门控。"""
        await runtime_config.set_runtime_config({"exclusive_per_ip_qps": 0.0})
        await forwarder._throttle_exclusive_ip("http://1.1.1.1:1")
        t0 = time.monotonic()
        await forwarder._throttle_exclusive_ip("http://1.1.1.1:1")
        assert time.monotonic() - t0 < 0.04
