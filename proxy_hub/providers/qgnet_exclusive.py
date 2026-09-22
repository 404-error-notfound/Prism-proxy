"""qg.net 独享代理（地区锁）适配 —— 与短效共享（qgnet.py）是不同产品/接口集。

基址 https://exclusive.proxy.qg.net，接口（调试工具与官方文档 doc/1843 已确认）：
  - 提取IP    GET /get?key=...&num=&area=&isp=&keep_avlie=
              （keep_avlie=存活分钟数，官方拼写就是 avlie；租期购买时定，这里不传）
  - 查询在用IP GET /query?key=...
  - 释放IP    GET /delete?key=...&ip=...
  - 更换IP    GET /replace?key=...&num=&area=...
              ⚠ 语义：调用后之前提取的所有资源都被释放并重新提取（全池替换），
              响应格式同 /get

/get 响应（已实测）：
  {"code": "SUCCESS", "request_id": "...",
   "data": {"task_id": "...", "num": 1, "ips": [{
     "proxy_ip": "140.250.148.7", "server": "140.250.148.7:11207",
     "area_code": 370100, "area": "山东省济南市", "isp": "电信",
     "deadline": "2026-09-22 14:46:50"   # 北京时间，无时区标识
   }]}}
列表在 data.ips（不是 data 直接是数组）；deadline 复用 qgnet.parse_deadline_epoch。

鉴权：API 只有 URL 上的 key= 参数（控制台生成的 API 链接自带，填进
EXCLUSIVE_API_URL 即可，本模块原样透传 base 里的 query）。代理**建连**鉴权
是另一套：控制台"代理验证-账密"（407 已实测），由 pool 侧用
settings.exclusive_auth_key/pwd 注入代理 URL（http://key:pwd@server）——
是代理账密，不是 API key。

已验证/文档列出的错误码：INVALID_KEY / UNAVAILABLE_KEY / ACCESS_DENY /
REQUEST_LIMIT_EXCEEDED / DELETE_LIMIT_EXCEEDED / NO_RESOURCE_FOUND /
FAILED_OPERATION / NO_AVAILABLE_CHANNEL / NO_ROUTE。
其中 NO_AVAILABLE_CHANNEL（通道已满）与 NO_RESOURCE_FOUND 是独享池稳态下的
**正常空结果**（不是故障）：映射为"返回空列表/None"，不进失败冷却。

剩余待真实凭证确认项：
  - /delete 的 IP 参数名（按文档惯例同时带 ip= 与 server=，服务端取其一）

已确认划掉：/query 响应结构（实测为 data.tasks[*].ips 按任务分组，已拍平
解析并保留 data.ips 兼容）。
"""
from __future__ import annotations

import logging
import time

import httpx

from proxy_hub.providers.base import ProxyItem
from proxy_hub.providers.qgnet import parse_deadline_epoch

logger = logging.getLogger(__name__)

# 接口路径（以 exclusive_api_url 为基址拼接；doc/1843 已确认）
_PATH_GET = "/get"        # 提取IP
_PATH_QUERY = "/query"    # 查询在用IP
_PATH_DELETE = "/delete"  # 释放IP
_PATH_REPLACE = "/replace"  # 更换IP（释放全部再提取，全池替换）

# 稳态下的正常空结果（独享通道满/无资源）：返回空而非抛异常，不进失败冷却
_BENIGN_EMPTY_CODES = ("NO_AVAILABLE_CHANNEL", "NO_RESOURCE_FOUND")


class QgNetExclusiveProvider:
    def __init__(
        self,
        api_url: str,
        default_ttl_s: float = 86400.0,
        timeout_s: float = 10.0,
    ) -> None:
        if not api_url:
            raise ValueError("exclusive api_url required")
        # api_url 三种填法都支持（测试服实测：直接填基址再拼 /get 会打成
        # /get/get → 404）：
        #   https://exclusive.proxy.qg.net/get?key=XXX  （控制台生成的提取链接，推荐）
        #   https://exclusive.proxy.qg.net?key=XXX
        #   https://exclusive.proxy.qg.net/?key=XXX
        # 路径最后一段是已知端点名时剥掉，得到真正的基址，之后按动作拼接目标
        # 端点；静态 query（key= 等）原样透传。
        base, _, query = api_url.partition("?")
        base = base.rstrip("/")
        for ep in (_PATH_GET, _PATH_QUERY, _PATH_DELETE, _PATH_REPLACE):
            if base.endswith(ep):
                base = base[: -len(ep)]
                break
        self._base = base.rstrip("/")
        self._query = query
        self._default_ttl_s = default_ttl_s
        self._timeout_s = timeout_s
        # 上次 fetch 的空结果是否因"通道已满"（NO_AVAILABLE_CHANNEL）——pool 据此
        # 回退"认养在用 IP"（见 pool._top_up_once 独享路径）；每次 fetch 开头重置
        self.last_fetch_channel_full = False
        self._last_benign_code: str | None = None

    @property
    def api_url(self) -> str:
        return self._base + (f"?{self._query}" if self._query else "")

    def _url(self, path: str, **params) -> str:
        """拼接口 URL：基址 + 路径 + 静态 query（含 key=）+ 本次参数。"""
        parts = [self._query] if self._query else []
        parts += [f"{k}={v}" for k, v in params.items() if v not in (None, "")]
        qs = "&".join(parts)
        return f"{self._base}{path}?{qs}" if qs else f"{self._base}{path}"

    async def _call(self, path: str, **params) -> dict | None:
        """调接口。返回响应 dict；稳态空结果（NO_AVAILABLE_CHANNEL 等）返回 None
        并记录 _last_benign_code（fetch 据此区分"通道已满"与其他空结果）。

        青果独享的业务错误码用 HTTP 400 包裹返回（实测：通道满时 400 +
        {"code":"NO_AVAILABLE_CHANNEL","message":"没有可用的空闲通道",...}）——
        必须先解析 JSON 按 code 走业务逻辑；只有 JSON 解析失败或 body 没有
        code 字段（真 HTTP 层故障）才 raise_for_status。
        """
        self._last_benign_code = None
        url = self._url(path, **params)
        async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_s)) as client:
            resp = await client.get(url)
        try:
            data = resp.json()
        except ValueError as e:
            resp.raise_for_status()  # 非 JSON：真 HTTP 层故障（5xx/网关错误页等）
            raise ValueError(
                f"exclusive API {path} returned non-JSON: {resp.text[:100]!r}"
            ) from e
        code = data.get("code")
        if code is None:
            resp.raise_for_status()  # JSON 但无 code：非业务响应，按 HTTP 层故障处理
            raise ValueError(
                f"exclusive API {path} response missing code: {str(data)[:100]!r}"
            )
        if code != "SUCCESS":
            if code in _BENIGN_EMPTY_CODES:
                # 通道已满/无资源：独享池稳态下的正常情况，不是故障
                logger.info("qgnet-exclusive: %s → %s（正常空结果）", path, code)
                self._last_benign_code = code
                return None
            raise ValueError(
                f"exclusive API {path} non-success: code={code!r} "
                f"msg={data.get('msg') or data.get('message')!r} "
                f"request_id={data.get('request_id')!r}"
            )
        return data

    @staticmethod
    def _unwrap_list(data: dict) -> list[dict]:
        """兼容三种包装：data.ips（实测格式）、data 直接是数组、data.list。"""
        payload = data.get("data") or []
        if isinstance(payload, dict):
            payload = payload.get("ips") or payload.get("list") or []
        return [d for d in payload if isinstance(d, dict)]

    def _parse_item(self, raw: dict, now_ts: float) -> ProxyItem | None:
        """单条 IP 记录 → ProxyItem。缺 server/到期已过的返回 None。

        到期字段优先 deadline（"YYYY-MM-DD HH:MM:SS" 北京时间，实测字段），
        兼容 expire_time/expired_time（epoch 秒/毫秒）；缺字段时用提取时刻 +
        默认租期兜底。
        """
        server = raw.get("server") or (
            f"{raw['ip']}:{raw['port']}" if raw.get("ip") and raw.get("port") else ""
        )
        if not server:
            logger.warning("qgnet-exclusive: item missing server/ip:port, skipped: %r", raw)
            return None
        epoch = (
            parse_deadline_epoch(raw.get("deadline") or "")
            or _parse_epochish(raw.get("expire_time") or raw.get("expired_time"))
            or 0.0
        )
        if epoch and epoch <= now_ts:
            logger.warning("qgnet-exclusive: skip already-expired IP %s", server)
            return None
        if not epoch:
            epoch = now_ts + self._default_ttl_s
        return ProxyItem(
            server=server,
            deadline_epoch=epoch,
            exit_ip=raw.get("proxy_ip") or server.split(":")[0],
            area=raw.get("area") or "",
            area_code=str(raw.get("area_code") or ""),
            isp=raw.get("isp") or "",
        )

    async def fetch(self, num: int, area_ex: str = "") -> list[ProxyItem]:
        """提取 num 个独享 IP。

        area_ex 忽略：独享地区锁在控制台/提取链接里指定，不做动态换段。
        通道已满（NO_AVAILABLE_CHANNEL）返回空列表并置 last_fetch_channel_full
        （稳态正常，调用方不进冷却，可回退认养在用 IP）；其他稳态空结果
        （NO_RESOURCE_FOUND）同样返回空但标记为 False。
        """
        self.last_fetch_channel_full = False
        data = await self._call(_PATH_GET, num=num)
        if data is None:
            self.last_fetch_channel_full = \
                self._last_benign_code == "NO_AVAILABLE_CHANNEL"
            return []
        now_ts = time.time()
        items = [it for raw in self._unwrap_list(data)
                 if (it := self._parse_item(raw, now_ts))]
        if not items:
            raise ValueError(
                f"exclusive API returned empty/unparseable data: {str(data)[:200]!r}"
            )
        logger.info(
            "qgnet-exclusive: fetched %d IPs, servers=%s",
            len(items), [it.server for it in items],
        )
        return items

    async def list_alive(self) -> list[dict]:
        """查询在用IP（观测/校准用；返回原始 dict 列表，不做字段假设）。

        实测 /query 响应与 /get 不同：列表嵌套在 data.tasks[*].ips（按提取
        任务分组，这里拍平）；保留 data.ips 包装兼容兜底。"""
        data = await self._call(_PATH_QUERY)
        if not data:
            return []
        payload = data.get("data") or {}
        tasks = payload.get("tasks") if isinstance(payload, dict) else None
        if tasks:
            return [ip for t in tasks if isinstance(t, dict)
                    for ip in (t.get("ips") or []) if isinstance(ip, dict)]
        return self._unwrap_list(data)

    async def alive_items(self) -> list[ProxyItem]:
        """查询在用IP并解析为 ProxyItem 列表（通道满时"认养在用 IP"用：
        服务重启 / Redis 池被清 / 手工提取占用通道后自愈）。"""
        now_ts = time.time()
        return [it for raw in await self.list_alive()
                if (it := self._parse_item(raw, now_ts))]

    async def release(self, server: str) -> bool:
        """释放一个在用独享 IP。IP 参数名按文档惯例同时带 ip= 与 server=
        （待真实凭证确认哪个生效，多带的参数服务端忽略）。"""
        try:
            await self._call(_PATH_DELETE, ip=server, server=server)
            return True
        except Exception as e:
            logger.warning("qgnet-exclusive: release %s failed: %s: %s",
                           server, type(e).__name__, e)
            return False

    async def replace(self, server: str = "") -> list[ProxyItem] | None:
        """更换IP：青果语义是**释放之前提取的全部资源并重新提取**（全池替换，
        不是单 IP 替换）。server 参数保留协议兼容，本实现忽略。

        返回新的 ProxyItem 列表（响应格式同 /get）；失败/无可用新 IP 返回 None
        （仅记日志，调用方保留旧池）。
        """
        try:
            data = await self._call(_PATH_REPLACE)
        except Exception as e:
            logger.warning("qgnet-exclusive: replace failed: %s: %s",
                           type(e).__name__, e)
            return None
        if data is None:
            return None  # NO_AVAILABLE_CHANNEL 等：更换不出新货
        now_ts = time.time()
        items = [it for raw in self._unwrap_list(data)
                 if (it := self._parse_item(raw, now_ts))]
        if not items:
            logger.warning("qgnet-exclusive: replace returned no usable new IP: %s",
                           str(data)[:200])
            return None
        logger.info("qgnet-exclusive: replaced pool with %d new IPs: %s",
                    len(items), [it.server for it in items])
        return items


def _parse_epochish(raw) -> float | None:
    """到期时间兼容解析：Unix 秒/毫秒数字串 → epoch；否则 None。"""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v > 1e12:  # 毫秒
        v /= 1000.0
    return v if v > 0 else None
