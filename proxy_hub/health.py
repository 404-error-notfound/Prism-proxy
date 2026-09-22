"""健康探测 —— 三态分级 + 并发过滤 + 连续判死计数。

三态：
  SEARCHABLE(2) 探测目标返回结果页（命中 marker 或响应体足够大）—— 优先使用
  REACHABLE(1)  拿到任意响应（含 403/验证页）—— 兜底使用（绝不让池子空）
  DEAD(0)       连接层失败 —— 丢弃
"""
from __future__ import annotations

import asyncio
import logging
import time

import httpx

logger = logging.getLogger(__name__)

PROXY_SEARCHABLE = 2
PROXY_REACHABLE = 1
PROXY_DEAD = 0


class HealthChecker:
    """探测目标/判定规则全部可配（构造时注入），不同项目配自己的验证站。"""

    def __init__(
        self,
        check_url: str,
        markers: tuple[bytes, ...],
        timeout_s: float,
        min_body_bytes: int,
        user_agent: str,
        max_latency_s: float = 0.0,
        liveness_url: str = "",
    ) -> None:
        self._check_url = check_url
        self._markers = markers
        self._timeout_s = timeout_s
        self._min_body_bytes = min_body_bytes
        self._user_agent = user_agent
        # 延迟门槛：探测全程超过此值按 DEAD 处理。意义是对齐转发层连接预算
        # （forwarder 2s connect）——体检 4s 放行的慢 IP 在隧道里必超时，
        # 放行进池等于埋雷（2026-09-01 测试服实测大批"体检过、转发全 TimeoutError"）
        self._max_latency_s = max_latency_s
        # 轻量中立探针（独享池专用，generate_204 类无反爬连通性探测端点）：
        # 独享 IP 7×24 被反复打搜索引擎探测页等于主动烧自己（2026-09-21 测试服
        # 实测：独享池 5-15s 一轮"认养→误杀→再认养"循环抖动）
        self._liveness_url = liveness_url

    async def probe(self, proxy_url: str) -> int:
        """一次探测把 IP 分三档。httpx 的 proxy 只能在客户端构造时指定，
        故每次探测新建客户端（并发跑，开销可忽略）。

        流式读取：住宅代理常"连得上但 body 传得慢"，整页下载会在超时内拿不完
        而被误判 DEAD。改为：拿到响应头即保底 REACHABLE（能出网可兜底），
        body 读够 min_body_bytes 即判 SEARCHABLE（无需等整页），中途断流
        但已拿到响应头也归 REACHABLE。
        """
        started = time.monotonic()
        # 开了延迟门槛时探测超时对齐门槛（+0.5s 余量）：超过门槛的 IP 反正按死处理，
        # 让它烧满 timeout_s 纯属拖慢唤热（2026-09-01 实测：冷启动唤热 ~10s 卡爆
        # gateway 等待预算，首查 0 结果）
        timeout_s = self._timeout_s
        if self._max_latency_s > 0:
            timeout_s = min(timeout_s, self._max_latency_s + 0.5)
        try:
            async with httpx.AsyncClient(
                proxy=proxy_url,
                timeout=httpx.Timeout(timeout_s),
                headers={"User-Agent": self._user_agent},
                follow_redirects=True,
            ) as client:
                async with client.stream("GET", self._check_url) as resp:
                    status = resp.status_code
                    body = b""
                    try:
                        async for chunk in resp.aiter_bytes(65536):
                            body += chunk
                            if len(body) >= self._min_body_bytes:
                                break
                    except Exception:
                        # 响应头已拿到（连接与鉴权没问题）但 body 断流/超时：能出网
                        return self._demote_if_slow(started, PROXY_REACHABLE, proxy_url)
        except Exception:
            return PROXY_DEAD  # 连接层失败才算死
        if status == 200 and (
            any(m in body for m in self._markers) or len(body) >= self._min_body_bytes
        ):
            return self._demote_if_slow(started, PROXY_SEARCHABLE, proxy_url)
        return self._demote_if_slow(started, PROXY_REACHABLE, proxy_url)

    def _demote_if_slow(self, started: float, state: int, proxy_url: str) -> int:
        """探测耗时长于转发层连接预算的 IP 对隧道无价值，按 DEAD 处理（0=不启用）。"""
        if self._max_latency_s > 0 and time.monotonic() - started > self._max_latency_s:
            logger.info("health: %s alive but slow (>%.1fs), treated as dead",
                        proxy_url, self._max_latency_s)
            return PROXY_DEAD
        return state

    async def probe_liveness(self, proxy_url: str) -> int:
        """轻量连通性探测（独享池专用）：中立探针，拿到任意 HTTP 响应即活。

        与 probe 的差异：不打搜索引擎（独享 IP 反复探测搜索页等于主动烧自己）；
        **禁用 slow-as-dead 降级**——独享 IP 可能物理距离远就是慢，慢≠死，
        只保留硬超时/连接层失败判死。返回 PROXY_REACHABLE / PROXY_DEAD。
        """
        try:
            async with httpx.AsyncClient(
                proxy=proxy_url,
                timeout=httpx.Timeout(self._timeout_s),
                headers={"User-Agent": self._user_agent},
                follow_redirects=True,
            ) as client:
                await client.get(self._liveness_url or self._check_url)
            return PROXY_REACHABLE
        except Exception:
            return PROXY_DEAD  # 连接层失败/硬超时才算死

    async def probe_stream_liveness(self, proxies: list[str]):
        """probe_stream 的轻量版（独享池入池体检用）：谁先探完谁先产出。"""
        tasks = {asyncio.ensure_future(self.probe_liveness(p)): p for p in proxies}
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                yield tasks[t], t.result()

    async def probe_stream(self, proxies: list[str]):
        """逐 IP 三态探测的异步生成器：谁先探完谁先产出 (url, state)。

        流式入池用（pool._top_up_once）：SEARCHABLE 不必等最慢的同伴，探完
        即刻可下发——唤热/空池快补的首个可用 IP 时间从"等全部探完（~8s，
        被最慢的连接超时拖住）"缩到"最快一个探完（~2s）"。filter_alive 是
        本生成器的非流式封装。
        """
        tasks = {asyncio.ensure_future(self.probe(p)): p for p in proxies}
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                yield tasks[t], t.result()

    async def filter_alive(self, proxies: list[str]) -> list[str]:
        """并发三态探测：优先返回"能搜索"的 IP；一个都没有时退用"能出网"的（绝不空池）。

        兼顾严格（优先用真能搜的高质量 IP）与宽松（永远填得满池子）。
        供应商短效 IP 常有到手即死的，连接失败的直接丢弃。
        """
        searchable: list[str] = []
        reachable: list[str] = []
        dead: list[str] = []
        async for url, state in self.probe_stream(proxies):
            if state == PROXY_SEARCHABLE:
                searchable.append(url)
            elif state >= PROXY_REACHABLE:
                reachable.append(url)
            else:
                dead.append(url)
        if dead:
            logger.warning(
                "health: %d/%d IPs dead (connection failed), dropped: %s",
                len(dead), len(proxies), dead,
            )
        if searchable:
            return searchable
        if reachable:
            logger.info("health: no search-capable IP, falling back to %d reachable-only",
                        len(reachable))
        return reachable


class FailCounter:
    """连续判死计数：IP 连续 dead_after 轮探测失败才确认死亡。

    单轮失败只记数不剔除——消除单次探测抖动（目标站偶发慢/丢包）误杀好 IP。
    探测恢复即清零。
    """

    def __init__(self, dead_after: int = 2) -> None:
        if dead_after < 1:
            raise ValueError("dead_after must be >= 1")
        self._dead_after = dead_after
        self._counts: dict[str, int] = {}

    def set_dead_after(self, dead_after: int) -> None:
        """热配置生效时调整判死轮数。"""
        self._dead_after = max(1, dead_after)

    def record(self, proxy_url: str, state: int) -> bool:
        """记录一轮探测结果，返回该 IP 是否"确认死亡"（应从池中剔除）。"""
        if state >= PROXY_REACHABLE:
            self._counts.pop(proxy_url, None)
            return False
        count = self._counts.get(proxy_url, 0) + 1
        if count >= self._dead_after:
            self._counts.pop(proxy_url, None)
            return True
        self._counts[proxy_url] = count
        return False

    def prune(self, live_urls: list[str]) -> None:
        """剔除已不在池中的 IP 的计数（换批后清理）。"""
        live = set(live_urls)
        for url in list(self._counts):
            if url not in live:
                del self._counts[url]
