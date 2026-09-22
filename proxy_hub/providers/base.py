"""供应商插件协议。新供应商只需实现 fetch() 并在 qgnet.py 同款目录注册。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ProxyItem:
    """一个提取到的代理。

    server: 实际要连的 ip:port（中转网关）；deadline_epoch: 过期时刻（Unix 墙钟秒）。
    exit_ip: 目标站看到的出口 IP，仅展示/日志用。
    area / area_code / isp: 出口归属（风控按地区聚集，供动态换段/观测用，
    拿不到时为空串，不影响主流程）。
    """

    server: str
    deadline_epoch: float
    exit_ip: str = ""
    area: str = ""
    area_code: str = ""
    isp: str = ""


class Provider(Protocol):
    async def fetch(self, num: int, area_ex: str = "") -> list[ProxyItem]:
        """提取 num 个代理。失败抛异常（调用方负责冷却与降级）。

        area_ex: 动态排除的地区编码（逗号分隔），与提取 URL 里的静态
        area_ex 合并——由调用方（pool）根据风控反馈计算传入。
        """
        ...

    # ---- 独享池可选能力（共享短效 provider 不实现，走下方默认行为）----
    # 结构性协议：QgNetProvider 未继承本类，运行期实际没有这两个方法——
    # pool 侧一律经 getattr(provider, "release"/"replace", None) 判定，
    # 此处默认实现仅供显式继承者与类型检查使用。

    async def release(self, server: str) -> bool:
        """释放一个在用独享 IP。成功返回 True；不支持的 provider 返回 False。"""
        return False

    async def replace(self, server: str = "") -> list[ProxyItem] | None:
        """更换独享 IP。青果独享语义是"释放全部已提取资源再重新提取"
        （全池替换，不是单 IP 替换）——返回新的 ProxyItem 列表；
        不支持/失败返回 None。server 参数保留协议兼容，实现可忽略。"""
        return None
