"""自建 HTTP 转发代理（隧道模式）—— 替代原 gost sidecar。

proxy-hub 自己监听 `settings.forward_port` 说代理协议：
  - 认证：Proxy-Authorization: Basic base64(<项目名>:<项目token>)，失败 407
  - CONNECT host:port（https）：挑上游 IP → TCP 连上游代理 → 向上游发
    CONNECT + 供应商账密 → 200 后回客户端 200 → 双向 relay
  - 普通 HTTP（GET http://... 绝对 URI）：连上游原样转发 + 供应商账密
  - 每个连接按目标站防重轮换挑 IP（pool.pick_proxy(project, target=host)）
  - 上游连接失败/CONNECT 被拒：记一次失败（FailCounter），换 IP 重试 ≤3 次
  - 每次成功建连记一次项目活跃（等价隐式 /activity，驱动按需补位）

消费方配置示例：http://<项目名>:<项目token>@<proxy-hub>:8101
"""
from __future__ import annotations

import asyncio
import base64
import collections
import logging
import time

from proxy_hub import pool, projects, runtime_config
from proxy_hub.config import settings
from proxy_hub.proxy_url import addr_and_auth

logger = logging.getLogger(__name__)

_MAX_HEADER_BYTES = 64 * 1024
_HEADER_TIMEOUT_S = 10.0
# 上游连接/握手超时都必须远小于 gateway 引擎超时（5s）：一个"连得上但不应答"
# 的烂 IP 烧掉的时间要小于重试换 IP 的预算，否则首发必挂（2026-08-31 生产实测：
# 整 5s 超时 + 重试秒回 = 握手无超时兜底 + 连接超时 8s 过长）
_UPSTREAM_CONNECT_TIMEOUT_S = 2.0
_UPSTREAM_HANDSHAKE_TIMEOUT_S = 2.0
_MAX_RETRIES = 3
# relay 单方向空闲多久算连接结束（长连接保活由客户端/目标站自己管）
_RELAY_IDLE_TIMEOUT_S = 300.0

_server: asyncio.AbstractServer | None = None


async def start_forwarder() -> None:
    """lifespan 里调用：forward_port>0 时启动监听。"""
    global _server
    if settings.forward_port <= 0:
        logger.info("forwarder: disabled (forward_port=0)")
        return
    sem = asyncio.Semaphore(settings.forward_max_conns)
    _server = await asyncio.start_server(
        lambda r, w: _guarded_handle(sem, r, w),
        host=settings.host, port=settings.forward_port,
    )
    logger.info("forwarder: listening on %s:%d (max_conns=%d)",
                settings.host, settings.forward_port, settings.forward_max_conns)


async def stop_forwarder() -> None:
    global _server
    if _server is not None:
        _server.close()
        await _server.wait_closed()
        _server = None


async def _guarded_handle(
    sem: asyncio.Semaphore, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    async with sem:
        try:
            await _handle(reader, writer)
        except Exception as e:
            logger.warning("forwarder: connection error: %s: %s", type(e).__name__, e)
            try:
                await _respond(writer, 502, b"Bad Gateway")
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    request_line, headers = await asyncio.wait_for(
        _read_request_head(reader), timeout=_HEADER_TIMEOUT_S
    )
    method, target, _version = _split_request_line(request_line)

    project = await _authenticate(headers)
    if project is None:
        await _respond(writer, 407, b"Proxy Authentication Required",
                       extra_headers=[(b"Proxy-Authenticate", b'Basic realm="proxy-hub"')])
        return
    await pool.record_activity(project)

    if method == "CONNECT":
        host, _, port_s = target.partition(":")
        await _handle_connect(project, host, int(port_s or 443), reader, writer)
        return

    # 普通 HTTP：只接受绝对 URI（http://host[:port]/path）
    if not target.startswith("http://"):
        await _respond(writer, 405, b"Method Not Allowed")
        return
    host_port, _, _path = target.removeprefix("http://").partition("/")
    host, _, port_s = host_port.partition(":")
    await _handle_plain_http(project, host, int(port_s or 80),
                             request_line, headers, reader, writer)


# ---------------------------------------------------------------------------
# CONNECT（https 隧道）
# ---------------------------------------------------------------------------

async def _handle_connect(
    project: str, host: str, port: int,
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
) -> None:
    peer = _peer_ip(writer)
    t0 = time.monotonic()
    upstream = await _open_upstream(
        project, host,
        lambda r, w, auth: _upstream_connect(r, w, host, port, auth),
    )
    if upstream is None:
        logger.info("forwarder: conn %s %s → %s:%d failed %dms (无可用上游)",
                    project, peer, host, port, (time.monotonic() - t0) * 1000)
        await _respond(writer, 502, b"Bad Gateway")
        return
    up_reader, up_writer, up_addr = upstream
    logger.info("forwarder: conn %s %s → %s:%d via %s %dms",
                project, peer, host, port, up_addr, (time.monotonic() - t0) * 1000,
                extra={"metrics": {"project": project, "target": f"{host}:{port}",
                                   "upstream": up_addr, "client": peer,
                                   "elapsed_ms": int((time.monotonic() - t0) * 1000)}})
    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
    await writer.drain()
    await _relay(reader, writer, up_reader, up_writer)


async def _upstream_connect(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    host: str, port: int, auth: tuple[str, str] | None,
) -> bool:
    """向上游代理发 CONNECT。返回隧道是否建立。"""
    lines = [
        f"CONNECT {host}:{port} HTTP/1.1",
        f"Host: {host}:{port}",
    ]
    if auth:
        cred = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        lines.append(f"Proxy-Authorization: Basic {cred}")
    lines.append("\r\n")
    writer.write("\r\n".join(lines).encode())
    await writer.drain()
    status_line = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT_S)
    parts = status_line.split(b" ", 2)
    if len(parts) < 2 or parts[1] != b"200":
        return False
    # 吞掉上游响应头（直到空行）
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=_HEADER_TIMEOUT_S)
        if line in (b"\r\n", b"\n", b""):
            break
    return True


# ---------------------------------------------------------------------------
# 普通 HTTP（绝对 URI 转发）
# ---------------------------------------------------------------------------

async def _handle_plain_http(
    project: str, host: str, port: int,
    request_line: bytes, headers: list[tuple[bytes, bytes]],
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
) -> None:
    head = _build_forward_head(request_line, headers)
    peer = _peer_ip(writer)
    t0 = time.monotonic()
    upstream = await _open_upstream(
        project, host,
        lambda r, w, auth: _upstream_plain_http(r, w, host, port, head, auth),
    )
    if upstream is None:
        logger.info("forwarder: conn %s %s → %s:%d failed %dms (无可用上游)",
                    project, peer, host, port, (time.monotonic() - t0) * 1000)
        await _respond(writer, 502, b"Bad Gateway")
        return
    up_reader, up_writer, up_addr = upstream
    logger.info("forwarder: conn %s %s → %s:%d via %s %dms",
                project, peer, host, port, up_addr, (time.monotonic() - t0) * 1000,
                extra={"metrics": {"project": project, "target": f"{host}:{port}",
                                   "upstream": up_addr, "client": peer,
                                   "elapsed_ms": int((time.monotonic() - t0) * 1000)}})
    await _relay(reader, writer, up_reader, up_writer)


def _build_forward_head(request_line: bytes, headers: list[tuple, ...]) -> bytes:
    """客户端请求头原样转发（剥掉 Proxy-Authorization/Proxy-Connection）。"""
    out = [request_line]
    for name, value in headers:
        low = name.lower()
        if low in (b"proxy-authorization", b"proxy-connection"):
            continue
        out.append(name + b": " + value)
    out.append(b"\r\n")
    return b"\r\n".join(out)


async def _upstream_plain_http(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    host: str, port: int, head: bytes, auth: tuple[str, str] | None,
) -> bool:
    payload = head
    if auth:
        cred = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        # 在请求头末尾（空行前）插入上游认证
        payload = head.replace(
            b"\r\n\r\n", f"\r\nProxy-Authorization: Basic {cred}\r\n\r\n".encode(), 1
        )
    writer.write(payload)
    await writer.drain()
    return True  # 普通 HTTP 无握手，relay 直接开始


# ---------------------------------------------------------------------------
# 上游选择与重试
# ---------------------------------------------------------------------------

# 近期建联映射：(project, target_host) → [(upstream_url, monotonic_ts)]。
# HTTPS 走 CONNECT 隧道后流量加密，forwarder 看不到响应内容，无法在数据面上识别
# "连得上但被目标站风控"；消费方（prism）从引擎层识别风控后经 /block-signal 上报，
# 按 (project, target) 在这里关联回当时用的上游 IP，交给 pool.record_block 剔除。
_RECENT_CONNS_MAX = 200        # 每个 (project, target) 保留的近期建联数
_recent_conns: dict[tuple[str, str], collections.deque] = {}

# 独享池每 IP QPS 门控：upstream URL → 上次使用 monotonic。仅 pool_type=
# exclusive 的项目生效（独享 IP 单用户独占，限速使用降低被风控概率）。
# 进程内状态：各副本独立限速，无需共享。
_ip_last_use: dict[str, float] = {}


async def _throttle_exclusive_ip(url: str) -> None:
    """同一独享 IP 两次使用间隔 < 1/exclusive_per_ip_qps 时 sleep 等待
    （asyncio，不阻塞事件循环）。qps<=0 = 关闭。"""
    cfg = await runtime_config.get_runtime_config()
    qps = cfg.exclusive_per_ip_qps
    if qps <= 0:
        return
    wait = _ip_last_use.get(url, 0.0) + 1.0 / qps - time.monotonic()
    if wait > 0:
        await asyncio.sleep(wait)
    _ip_last_use[url] = time.monotonic()


def note_conn(project: str, target_host: str, upstream_url: str) -> None:
    """记录一次成功建联的目标站与上游 IP 对应关系（供风控信号反查）。"""
    key = (project, target_host.lower())
    dq = _recent_conns.setdefault(key, collections.deque(maxlen=_RECENT_CONNS_MAX))
    dq.append((upstream_url, time.monotonic()))


def recent_upstreams(
    project: str, target_hint: str, window_s: float = 30.0
) -> list[str]:
    """近 window_s 内服务过目标站的 upstream URL 去重列表（最新在前）。

    target_hint 是引擎名片段（如 "baidu"），与 CONNECT authority 做子串匹配
    （"baidu" ⊂ "www.baidu.com"、"quark" ⊂ "quark.sm.cn"），免维护 host 清单。
    """
    hint = target_hint.lower()
    now = time.monotonic()
    seen: dict[str, None] = {}
    for (proj, host), dq in _recent_conns.items():
        if proj != project or hint not in host:
            continue
        for url, ts in reversed(dq):
            if now - ts <= window_s:
                seen.setdefault(url)
    return list(seen)


async def _open_upstream(project: str, target_host: str, handshake):
    """挑 IP → 连上游 → 握手；失败换 IP 重试 ≤_MAX_RETRIES 次。

    成功返回 (reader, writer, upstream_addr)；失败返回 None（已 WARN 日志）。
    """
    tried: list[str] = []
    for _ in range(_MAX_RETRIES):
        # 按 target 路由：命中 exclusive_targets 的目标站优先从独享池取 IP
        # （独享池空自动落回共享池），消费方零改动
        url = await pool.pick_proxy_routed(project, target_host)
        if url is None or url in tried:
            break
        tried.append(url)
        addr, auth = addr_and_auth(url)
        host, _, port_s = addr.partition(":")
        try:
            # 独享池每 IP QPS 门控：按 upstream URL 的池归属判定（而非隧道
            # 凭证项目）——baidu/quark 流量被门控，其他引擎不受影响
            if pool.is_exclusive_upstream(url):
                await _throttle_exclusive_ip(url)
            up_reader, up_writer = await asyncio.wait_for(
                asyncio.open_connection(host, int(port_s)),
                timeout=_UPSTREAM_CONNECT_TIMEOUT_S,
            )
            ok = await asyncio.wait_for(
                handshake(up_reader, up_writer, auth),
                timeout=_UPSTREAM_HANDSHAKE_TIMEOUT_S,
            )
            if ok:
                note_conn(project, target_host, url)
                return up_reader, up_writer, addr
            up_writer.close()
        except Exception as e:
            logger.info("forwarder: upstream %s failed: %s: %s",
                        addr, type(e).__name__, e)
        # 记一次失败：连续 N 轮（巡检口径）确认死亡后才剔除
        pool.record_failure(project, url)
    logger.warning("forwarder: %s → %s 无可用上游（试了 %d 个）",
                   project, target_host, len(tried))
    return None


def _peer_ip(writer: asyncio.StreamWriter) -> str:
    peer = writer.get_extra_info("peername")
    return str(peer[0]) if peer else "?"


# ---------------------------------------------------------------------------
# 双向 relay
# ---------------------------------------------------------------------------

async def _relay(
    client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter,
    up_reader: asyncio.StreamReader, up_writer: asyncio.StreamWriter,
) -> None:
    async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await asyncio.wait_for(src.read(65536),
                                              timeout=_RELAY_IDLE_TIMEOUT_S)
                if not data:
                    break
                dst.write(data)
                await dst.drain()
        except (TimeoutError, ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            try:
                dst.write_eof()
            except Exception:
                pass

    # 任一方向结束（对端关闭/空闲超时）即取消另一方向——半开的 pipe 不再
    # 挂到 _RELAY_IDLE_TIMEOUT_S，连接 teardown 立刻完成
    pipes = [
        asyncio.ensure_future(pipe(client_reader, up_writer)),
        asyncio.ensure_future(pipe(up_reader, client_writer)),
    ]
    _, pending = await asyncio.wait(pipes, return_when=asyncio.FIRST_COMPLETED)
    for p in pending:
        p.cancel()
    if pending:
        await asyncio.wait(pipes)
    try:
        up_writer.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 解析与响应工具
# ---------------------------------------------------------------------------

async def _read_request_head(
    reader: asyncio.StreamReader,
) -> tuple[bytes, list[tuple[bytes, bytes]]]:
    """读请求行 + 头（上限 _MAX_HEADER_BYTES）。返回 (请求行, [(name, value)])。

    请求体不归这里读：CONNECT 没有体；普通 HTTP 的体由 relay 原样转发。
    """
    request_line = await reader.readline()
    if not request_line:
        raise ConnectionError("client closed before request line")
    headers: list[tuple[bytes, bytes]] = []
    total = len(request_line)
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        total += len(line)
        if total > _MAX_HEADER_BYTES:
            raise ValueError("request head too large")
        name, sep, value = line.rstrip(b"\r\n").partition(b":")
        if sep:
            headers.append((name.strip(), value.strip()))
    return request_line.rstrip(b"\r\n"), headers


def _split_request_line(line: bytes) -> tuple[str, str, str]:
    parts = line.decode("latin-1").split(" ")
    if len(parts) != 3:
        raise ValueError(f"malformed request line: {line!r}")
    return parts[0].upper(), parts[1], parts[2]


async def _authenticate(headers: list[tuple[bytes, bytes]]) -> str | None:
    """Proxy-Authorization: Basic base64(<项目名>:<token>) → 项目名（校验 token）。"""
    for name, value in headers:
        if name.lower() != b"proxy-authorization":
            continue
        scheme, _, cred = value.partition(b" ")
        if scheme.lower() != b"basic" or not cred:
            return None
        try:
            user, _, token = base64.b64decode(cred).decode("latin-1").partition(":")
        except Exception:
            return None
        proj = await projects.get_project(user)
        if proj and token and proj.get("token") == token:
            return user
        return None
    return None


async def _respond(
    writer: asyncio.StreamWriter, status: int, body: bytes,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    reason = {407: b"Proxy Authentication Required", 405: b"Method Not Allowed",
              502: b"Bad Gateway"}.get(status, b"Error")
    head = [b"HTTP/1.1 %d %s" % (status, reason)]
    for name, value in extra_headers or []:
        head.append(name + b": " + value)
    head.append(b"Content-Length: %d" % len(body))
    head.append(b"Connection: close")
    head.append(b"\r\n")
    writer.write(b"\r\n".join(head) + body)
    await writer.drain()
