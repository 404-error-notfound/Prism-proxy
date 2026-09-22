"""代理 URL 工具 —— 供应商 server 与代理 URL 的互转。"""
from __future__ import annotations

from urllib.parse import quote, unquote


def make_proxy_url(server: str, auth_key: str = "", auth_pwd: str = "") -> str:
    """中转网关 server(ip:port) → 代理 URL。配了账密则带 userinfo。"""
    if auth_key and auth_pwd:
        return f"http://{quote(auth_key, safe='')}:{quote(auth_pwd, safe='')}@{server}"
    return f"http://{server}"


def addr_and_auth(proxy_url: str) -> tuple[str, tuple[str, str] | None]:
    """从代理 URL 拆出 (host:port, (user, pwd) | None)。"""
    rest = proxy_url.removeprefix("http://")
    if "@" in rest:
        userinfo, addr = rest.rsplit("@", 1)
        user, _, pwd = userinfo.partition(":")
        return addr, (unquote(user), unquote(pwd))
    return rest, None
