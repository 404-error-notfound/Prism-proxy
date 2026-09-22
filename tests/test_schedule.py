"""冷热分档纯函数测试：时段表解析、档位解析、自适应升档、fetch_num 重写、配置校验。"""
from datetime import datetime

import pytest
from pydantic import ValidationError

from proxy_hub.schedule import (
    DEFAULT_TIERS,
    RuntimeConfig,
    _parse_hhmm,
    parse_schedule,
    resolve_base_tier,
    resolve_tier,
    set_fetch_num,
)


def _at(hhmm: str) -> datetime:
    h, m = int(hhmm[:2]), int(hhmm[3:])
    return datetime(2026, 8, 24, h, m)


class TestParseSchedule:
    def test_empty(self):
        assert parse_schedule("") == []

    def test_normal(self):
        assert parse_schedule("peak=08:00-22:00") == [("peak", 480, 1320)]

    def test_cross_midnight(self):
        segs = parse_schedule("peak=08:00-22:00,offpeak=22:00-08:00")
        assert segs == [("peak", 480, 1320), ("offpeak", 1320, 480)]

    def test_invalid_tier_rejected(self):
        with pytest.raises(ValueError, match="tier"):
            parse_schedule("hot=08:00-22:00")

    def test_invalid_format_rejected(self):
        for bad in ("peak=8点-22:00", "peak=08:00", "peak08:00-22:00", "peak=25:00-26:00",
                    "peak=08:00-08:00"):
            with pytest.raises(ValueError):
                parse_schedule(bad)


class TestResolveBaseTier:
    SEGS = parse_schedule("peak=08:00-22:00,offpeak=22:00-08:00")

    @pytest.mark.parametrize("hhmm,expected", [
        ("08:00", "peak"), ("12:30", "peak"), ("21:59", "peak"),
        ("22:00", "offpeak"), ("23:59", "offpeak"), ("00:00", "offpeak"), ("07:59", "offpeak"),
    ])
    def test_boundaries(self, hhmm, expected):
        assert resolve_base_tier(self.SEGS, _at(hhmm)) == expected

    def test_unscheduled_is_normal(self):
        segs = parse_schedule("peak=08:00-10:00")
        assert resolve_base_tier(segs, _at("12:00")) == "normal"
        assert resolve_base_tier([], _at("12:00")) == "normal"


class TestResolveTier:
    SCHED = "peak=08:00-22:00,offpeak=22:00-08:00"

    def test_offpeak_low_qps_stays(self):
        cfg = RuntimeConfig(schedule=self.SCHED)
        tier, params = resolve_tier(cfg, _at("23:00"), qps_10min=0.1)
        assert tier == "offpeak"
        assert params.health_recheck_interval_s == DEFAULT_TIERS["offpeak"].health_recheck_interval_s

    def test_offpeak_qps_surge_escalates_to_peak(self):
        cfg = RuntimeConfig(schedule=self.SCHED, adaptive_qps_threshold=0.5)
        tier, params = resolve_tier(cfg, _at("23:00"), qps_10min=1.2)
        assert tier == "peak"
        assert params.fetch_num == DEFAULT_TIERS["peak"].fetch_num

    def test_adaptive_disabled_no_escalation(self):
        cfg = RuntimeConfig(schedule=self.SCHED, adaptive_enabled=False)
        tier, _ = resolve_tier(cfg, _at("23:00"), qps_10min=99.0)
        assert tier == "offpeak"

    def test_peak_never_escalates(self):
        cfg = RuntimeConfig(schedule=self.SCHED)
        tier, _ = resolve_tier(cfg, _at("12:00"), qps_10min=99.0)
        assert tier == "peak"

    def test_normal_not_escalated(self):
        cfg = RuntimeConfig(schedule="offpeak=22:00-08:00")  # 12:00 → normal
        tier, _ = resolve_tier(cfg, _at("12:00"), qps_10min=99.0)
        assert tier == "normal"


class TestSetFetchNum:
    def test_replaces_existing(self):
        url = "https://share.proxy.qg.net/get?key=abc&num=6&distinct=true"
        assert set_fetch_num(url, 3) == \
            "https://share.proxy.qg.net/get?key=abc&num=3&distinct=true"

    def test_appends_when_missing(self):
        assert set_fetch_num("https://api.example.com/get?key=abc", 5) == \
            "https://api.example.com/get?key=abc&num=5"
        assert set_fetch_num("https://api.example.com/get", 5) == \
            "https://api.example.com/get?num=5"


class TestRuntimeConfig:
    def test_defaults_equal_current_behavior(self):
        cfg = RuntimeConfig()
        assert cfg.schedule == ""
        assert cfg.adaptive_enabled is True
        assert cfg.health_dead_after == 2
        assert cfg.tier_params("normal").health_recheck_interval_s == 15.0

    def test_invalid_schedule_rejected(self):
        with pytest.raises(ValidationError):
            RuntimeConfig(schedule="peak=八点到十点")

    def test_invalid_tier_param_rejected(self):
        with pytest.raises(ValidationError):
            RuntimeConfig(tiers={"peak": {**vars(DEFAULT_TIERS["peak"]), "fetch_num": 0},
                                  "normal": vars(DEFAULT_TIERS["normal"]),
                                  "offpeak": vars(DEFAULT_TIERS["offpeak"])})

    def test_partial_tier_override(self):
        cfg = RuntimeConfig(tiers={
            "peak": {**vars(DEFAULT_TIERS["peak"]), "fetch_num": 10},
            "normal": vars(DEFAULT_TIERS["normal"]),
            "offpeak": vars(DEFAULT_TIERS["offpeak"]),
        })
        assert cfg.tier_params("peak").fetch_num == 10
        assert cfg.tier_params("offpeak").fetch_num == 3


def test_parse_hhmm():
    assert _parse_hhmm("00:00") == 0
    assert _parse_hhmm("23:59") == 1439
    with pytest.raises(ValueError):
        _parse_hhmm("24:00")
