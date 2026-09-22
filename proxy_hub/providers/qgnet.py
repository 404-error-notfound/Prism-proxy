"""qg.net 短效住宅代理适配。

API 响应（get 接口，json 格式）：
  {
    "code": "SUCCESS",
    "data": [
      {
        "proxy_ip": "112.247.54.216",      # 住宅出口 IP（目标站看到的，仅展示用）
        "server":   "60.188.69.215:19239", # 中转网关 ip:port —— 实际要连的就是它
        "area": "...", "isp": "...",
        "deadline": "2026-06-18 14:38:23"  # 过期时刻（北京时间，无时区标识）
      }
    ],
    "request_id": "..."
  }
注意：代理连接必须用 server（中转网关），不是 proxy_ip。
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from proxy_hub.providers.base import ProxyItem

logger = logging.getLogger(__name__)

# 北京时区：qg.net 的 deadline 是北京时间无时区标识，显式挂时区比对，
# 避免容器在 UTC 下算出 +8h 的假有效期（拿着死代理却以为还活着）
_CN_TZ = ZoneInfo("Asia/Shanghai")


def parse_deadline_epoch(raw: str) -> float | None:
    """"2026-06-18 14:38:23"（北京时间）→ Unix 墙钟秒。解析失败返回 None。"""
    try:
        dl = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_CN_TZ)
    except (ValueError, TypeError):
        return None
    return dl.timestamp()


def earliest_deadline_epoch(items: list[ProxyItem]) -> float | None:
    """批内最早 deadline（保守：按最早值算刷新触发点）。全解析失败返回 None。"""
    epochs = [it.deadline_epoch for it in items if it.deadline_epoch > 0]
    return min(epochs) if epochs else None


class QgNetProvider:
    def __init__(self, api_url: str, timeout_s: float = 10.0) -> None:
        if "format=txt" in api_url:
            raise ValueError("qg.net get 接口需用默认 json 格式，URL 勿带 format=txt")
        self._api_url = api_url
        self._timeout_s = timeout_s
        # 地区名 → area_code 缓存（/resources 接口，变化极少，1h 一刷）
        self._area_code_map: dict[str, str] = {}
        self._area_code_map_at: float = 0.0

    @property
    def api_url(self) -> str:
        return self._api_url

    async def _resolve_area_code(self, area_name: str) -> str:
        """地区名（"广东省汕头市"）→ area_code（"440500"）。查不到返回 ""。"""
        if not area_name:
            return ""
        if time.monotonic() - self._area_code_map_at > 3600:
            try:
                m = re.search(r"key=([^&]+)", self._api_url)
                base = self._api_url.split("/get")[0]
                async with httpx.AsyncClient(
                        timeout=httpx.Timeout(self._timeout_s)) as client:
                    resp = await client.get(f"{base}/resources?key={m.group(1)}")
                data = resp.json()
                if data.get("code") == "SUCCESS":
                    self._area_code_map = {
                        d["area"]: str(d["area_code"]) for d in data.get("data", [])
                    }
                    self._area_code_map_at = time.monotonic()
            except Exception as e:
                logger.debug("qgnet: /resources refresh failed: %s", e)
        return self._area_code_map.get(area_name, "")

    def _merge_area_ex(self, url: str, dynamic: str) -> str:
        """动态排除地区与 URL 里已有的静态 area_ex 合并（去重）。"""
        if not dynamic:
            return url
        existing = re.search(r"([?&])area_ex=([^&]*)", url)
        codes = {c for c in dynamic.split(",") if c}
        if existing:
            codes |= {c for c in existing.group(2).split(",") if c}
            return url[:existing.start(2)] + ",".join(sorted(codes)) + url[existing.end(2):]
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}area_ex={','.join(sorted(codes))}"

    async def fetch(self, num: int, area_ex: str = "") -> list[ProxyItem]:
        from proxy_hub.schedule import set_fetch_num

        url = self._merge_area_ex(set_fetch_num(self._api_url, num), area_ex)
        async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_s)) as client:
            resp = await client.get(url)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            raise ValueError(
                f"proxy API returned non-JSON (qg.net get 接口默认 json): {resp.text[:100]!r}"
            ) from e

        if data.get("code") != "SUCCESS":
            # qg.net 失败码示例：Key 不存在/已过期、Key 被封禁、余额不足、提取过于频繁。
            # 带 request_id 便于向供应商提工单排查
            raise ValueError(
                f"proxy API returned non-success: code={data.get('code')!r} "
                f"msg={data.get('msg') or data.get('message')!r} "
                f"request_id={data.get('request_id')!r}"
            )

        items_raw = data.get("data", [])
        if not items_raw:
            raise ValueError("proxy API returned empty data")

        now_ts = datetime.now(_CN_TZ).timestamp()
        items: list[ProxyItem] = []
        for raw in items_raw:
            epoch = parse_deadline_epoch(raw.get("deadline") or "") or 0.0
            if epoch and epoch <= now_ts:
                logger.warning("qgnet: skip already-expired IP %s", raw.get("server"))
                continue
            items.append(
                ProxyItem(
                    server=raw["server"],
                    deadline_epoch=epoch,
                    exit_ip=raw.get("proxy_ip") or "",
                    area=raw.get("area") or "",
                    area_code=await self._resolve_area_code(raw.get("area") or ""),
                    isp=raw.get("isp") or "",
                )
            )
        if not items:
            raise ValueError("proxy API returned only expired/unparseable items")
        logger.info(
            "qgnet: fetched %d IPs, exit_ips=%s",
            len(items),
            [it.exit_ip for it in items],
        )
        return items
