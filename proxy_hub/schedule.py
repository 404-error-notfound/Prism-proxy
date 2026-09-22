"""冷热分档 —— 时段表解析、档位解析、QPS 自适应（纯函数，不碰 Redis/网络）。

档位语义：
  - peak（高峰）：巡检勤、池子大
  - normal（常规）：默认行为
  - offpeak（低谷）：巡检稀、池子小、更长空闲容忍

时段表格式："peak=08:00-22:00,offpeak=22:00-08:00"，支持跨午夜（22:00-08:00），
未覆盖时段归为 normal。多个同名档位时段会合并；重叠时段先声明者优先。

QPS 自适应：offpeak 档且近 10 分钟 QPS ≥ adaptive_qps_threshold 时临时按 peak
参数跑（突发流量打穿固定时段表的保护），升档响应 < 1 个巡检周期。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

TIER_NAMES = ("peak", "normal", "offpeak")


@dataclass(frozen=True)
class TierParams:
    """一个档位的全部节奏参数。"""

    check_interval_s: float          # 池状态轮询间隔
    health_recheck_interval_s: float  # 健康巡检间隔（<=0 关闭巡检）
    fetch_num: int                   # 目标池大小（滚动补位补到此数）
    idle_threshold_s: float          # 距最近一次 /activity 超过此值视为空闲，不再补位/巡检


DEFAULT_TIERS: dict[str, TierParams] = {
    # check_interval_s 决定"池缺口/全灭"被发现的延迟上限，5s 轮询成本可忽略
    #（读时间戳 + 一次 Redis ZCARD）。
    # 巡检频率不省 IP 钱（探测只消耗池里现有 IP，不触发新提取）：offpeak 的
    # health_recheck 别超过套餐时效——2min 套餐配 120s 等于 IP 终生不再复检，
    # 中途夭折的死 IP 会一直留在池里被发出去
    "peak": TierParams(5.0, 15.0, 6, 300.0),
    "normal": TierParams(5.0, 15.0, 4, 300.0),
    "offpeak": TierParams(15.0, 30.0, 3, 600.0),
}


class TierParamsModel(BaseModel):
    """TierParams 的 pydantic 版本，供 runtime config 部分覆盖校验用。"""

    check_interval_s: float = Field(gt=0)
    health_recheck_interval_s: float = Field(ge=0)
    fetch_num: int = Field(ge=1, le=200)
    idle_threshold_s: float = Field(gt=0)


class RuntimeConfig(BaseModel):
    """运行期热配置（env 默认值 → Redis config:runtime 覆盖，≤15s 生效）。

    所有字段都有默认值：Redis 里没有覆盖时等价于"全天 normal + 现状参数"。
    """

    schedule: str = ""                 # 时段表，空 = 全天 normal
    tiers: dict[str, TierParamsModel] = {
        name: TierParamsModel(**vars(params)) for name, params in DEFAULT_TIERS.items()
    }
    adaptive_enabled: bool = True
    adaptive_qps_threshold: float = Field(default=0.5, gt=0)
    health_dead_after: int = Field(default=2, ge=1, le=10)
    # IP 剩余寿命低于此值不再发放（下发禁区）。按套餐时效调：尾段不可达的
    # 套餐调大（如 3min 套餐发现最后 30s 常断就设 40）。默认 25s：短效套餐
    # 实际死亡时刻在 deadline 前 25-35s 已开始波动，卡波动下沿——更低的必然
    # 将死禁发；25-35 之间的交给 pick_proxy 禁区回退 + forwarder 换 IP 重试兜底
    alloc_min_ttl_s: float = Field(default=25.0, ge=0, le=120)

    # 批量补位防抖：同一次 top-up 抽的 n 个 IP 共享几乎相同的 deadline，套餐
    # 到期时齐死形成"过期波"（一波 ConnectionReset + 整批补位 → 2min 后重演）。
    # 缺口 >= topup_stagger_min_n 且池未空时改分批抽取：每批 topup_chunk_size 个、
    # 批间随机 sleep [min,max] 秒——deadline 随抽取时刻天然错开，新 IP 随到随用，
    # IP 仍用满寿命（不提前丢弃）。池空/紧急（现存 IP 不足一半缺口）整批快补不防抖。
    topup_stagger_min_n: int = Field(default=3, ge=2, le=50)
    topup_chunk_size: int = Field(default=2, ge=1, le=20)
    topup_stagger_delay_min_s: float = Field(default=2.0, ge=0.0, le=60.0)
    topup_stagger_delay_max_s: float = Field(default=5.0, ge=0.0, le=120.0)

    # 按目标站候选压力定向扩池：pick_proxy 发现某 target 的"未被标记候选"归零
    # （池里 IP 全部被该站风控标记，fail-open 在发脏 IP）时，主动提取一小批新 IP
    # 扩池，而不是干等 TTL 换血。背景：共享池对 baidu 这类超严站 ~80% 出生即被
    # 标记，按站标记只挡派发不触发补位，不扩池该站可用率锁死在 0 附近。
    # 扩池有成本（提取量随之上浮），用 batch/间隔/上限三旋钮约束。
    targeted_topup_enabled: bool = True
    targeted_topup_min_interval_s: float = Field(default=30.0, ge=5, le=600)  # 每项目每 target 最小间隔
    targeted_topup_batch: int = Field(default=3, ge=1, le=20)                 # 每次提取数量
    targeted_topup_pool_cap_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)  # 池大小上限 = 目标大小 × 此倍数

    # 低水位快冷却：池子剩不到目标一半（尤其空池）时，供应商失败冷却从
    # fail_cooldown_s（env，默认 15s）缩到本值——池空没有"省配额"的余地，
    # 快速重试优先。2026-09-04 实测：供应商崩盘时空池被 15s 冷却反复锁死
    # 50s+（冷却过期→fetch→又全灭→重新计时），低水位缩到 3s 可把空窗
    # 压缩到"供应商往返 ~5s"的物理下限附近。
    low_pool_cooldown_s: float = Field(default=3.0, ge=0.0, le=300.0)

    # 城市级自动换段：某城市窗口内被风控标记（block-signal）的不同 IP 数超阈值 →
    # 提取时动态 area_ex 排除该城市，TTL 到期自动放回（风控会漂移，只排不放会把
    # 可选城市越排越少）。实测风控在城市级有持续性（2026-09-14 汕头段连续两天
    # baidu 风控率 75%），城市粒度排除不伤源多样性（当前 923 个可用城市）。
    auto_area_exclude_enabled: bool = True
    area_block_window_s: float = Field(default=600.0, gt=0)      # 聚点滑窗
    area_block_threshold: int = Field(default=3, ge=1, le=50)    # 窗口内不同 IP 数阈值
    area_exclude_ttl_s: float = Field(default=14400.0, gt=0)     # 排除时长（自动放回；搜索站对 IP 段的风控封禁通常小时级起步）
    area_exclude_max: int = Field(default=20, ge=1, le=100)      # 动态排除城市数上限

    # 独享池按 target 路由：target（CONNECT authority / pull 的 ?target=）命中
    # 名单（双向子串匹配，与拉黑标记同语义："baidu" ⊂ "www.baidu.com"）时优先
    # 从独享池选 IP，独享池空自动落回共享池——消费方零改动。空列表 = 功能关闭。
    exclusive_targets: list[str] = Field(default_factory=lambda: ["baidu", "quark"])
    # 独享池（pool_type=exclusive 项目）：IP 被风控标记（/block-signal）后异步调
    # 供应商"更换IP"接口换新，冷却窗口内同一 IP 不重复更换（防信号风暴狂换烧
    # 更换次数配额）。
    exclusive_replace_cooldown_s: float = Field(default=60.0, ge=0.0, le=3600.0)
    # 独享池每 IP QPS 门控：隧道建联前，同一独享 IP 两次使用间隔 < 1/qps 时
    # sleep 等待（独享 IP 单用户独占，慢速使用降低被目标站风控概率）。0=关闭。
    exclusive_per_ip_qps: float = Field(default=0.2, ge=0.0, le=10.0)

    @model_validator(mode="after")
    def _check_stagger_delay(self) -> RuntimeConfig:
        if self.topup_stagger_delay_max_s < self.topup_stagger_delay_min_s:
            raise ValueError("topup_stagger_delay_max_s 不得小于 min_s")
        return self

    @field_validator("exclusive_targets")
    @classmethod
    def _validate_exclusive_targets(cls, v: list[str]) -> list[str]:
        return [t.strip().lower() for t in v if t and t.strip()]

    @field_validator("schedule")
    @classmethod
    def _validate_schedule(cls, v: str) -> str:
        parse_schedule(v)  # 非法格式直接拒绝写入
        return v

    def tier_params(self, name: str) -> TierParams:
        m = self.tiers[name]
        return TierParams(
            check_interval_s=m.check_interval_s,
            health_recheck_interval_s=m.health_recheck_interval_s,
            fetch_num=m.fetch_num,
            idle_threshold_s=m.idle_threshold_s,
        )


# ---------------------------------------------------------------------------
# 时段表
# ---------------------------------------------------------------------------


def _parse_hhmm(s: str) -> int:
    """"08:00" → 480（当日分钟数）。非法格式抛 ValueError。"""
    try:
        h, m = s.split(":")
        hour, minute = int(h), int(m)
    except (ValueError, AttributeError) as e:
        raise ValueError(f"invalid time {s!r}, expect HH:MM") from e
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid time {s!r}, expect HH:MM in 00:00-23:59")
    return hour * 60 + minute


def parse_schedule(schedule: str) -> list[tuple[str, int, int]]:
    """解析时段表为 [(tier, start_min, end_min)]。空串 → 空列表（全天 normal）。

    跨午夜时段（start > end，如 22:00-08:00）原样保留，匹配时按"两段"处理。
    """
    segments: list[tuple[str, int, int]] = []
    for part in schedule.split(","):
        part = part.strip()
        if not part:
            continue
        tier, sep, span = part.partition("=")
        tier = tier.strip()
        if not sep or tier not in TIER_NAMES:
            raise ValueError(
                f"invalid schedule segment {part!r}, expect e.g. 'peak=08:00-22:00' "
                f"(tier in {TIER_NAMES})"
            )
        start_s, sep, end_s = span.partition("-")
        if not sep:
            raise ValueError(f"invalid schedule segment {part!r}, missing '-'")
        start, end = _parse_hhmm(start_s.strip()), _parse_hhmm(end_s.strip())
        if start == end:
            raise ValueError(f"invalid schedule segment {part!r}, start == end")
        segments.append((tier, start, end))
    return segments


def _minute_of_day(now: datetime) -> int:
    return now.hour * 60 + now.minute


def resolve_base_tier(segments: list[tuple[str, int, int]], now: datetime) -> str:
    """按当前时刻命中时段表，返回档位名；未命中任何时段 → normal。先声明者优先。"""
    minute = _minute_of_day(now)
    for tier, start, end in segments:
        if start < end:
            if start <= minute < end:
                return tier
        else:  # 跨午夜：22:00-08:00 = [22:00,24:00) ∪ [00:00,08:00)
            if minute >= start or minute < end:
                return tier
    return "normal"


def resolve_tier(
    cfg: RuntimeConfig, now: datetime, qps_10min: float
) -> tuple[str, TierParams]:
    """返回 (生效档位名, 该档参数)。

    自适应升档：仅 offpeak → peak（低谷被突发流量打穿时升到最高规格）。
    返回的档位名反映升档后的实际档位（"peak"），调用方用于成本埋点分档记账。
    """
    base = resolve_base_tier(parse_schedule(cfg.schedule), now)
    effective = base
    if (
        cfg.adaptive_enabled
        and base == "offpeak"
        and qps_10min >= cfg.adaptive_qps_threshold
    ):
        effective = "peak"
    return effective, cfg.tier_params(effective)


# ---------------------------------------------------------------------------
# 供应商 URL 参数重写
# ---------------------------------------------------------------------------


def set_fetch_num(api_url: str, num: int) -> str:
    """重写提取 API URL 的 num= 参数（无则追加），供滚动补位控制单次提取数量。"""
    import re

    if re.search(r"([?&])num=\d+", api_url):
        return re.sub(r"([?&])num=\d+", rf"\g<1>num={num}", api_url)
    sep = "&" if "?" in api_url else "?"
    return f"{api_url}{sep}num={num}"
