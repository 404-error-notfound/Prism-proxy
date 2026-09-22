"""热配置测试：env 默认 → Redis 覆盖、校验拒绝、审计快照、回滚、清除。"""
import pytest
from pydantic import ValidationError

from proxy_hub import runtime_config


async def test_defaults_without_override(fake_redis):
    cfg = await runtime_config.get_runtime_config()
    assert cfg.schedule == ""
    assert cfg.health_dead_after == 2


async def test_set_and_get_override(fake_redis):
    cfg = await runtime_config.set_runtime_config({
        "schedule": "peak=08:00-22:00,offpeak=22:00-08:00",
        "health_dead_after": 3,
    })
    assert cfg.health_dead_after == 3

    runtime_config._cache = None  # 绕过缓存模拟另一进程读取
    cfg2 = await runtime_config.get_runtime_config()
    assert cfg2.schedule == "peak=08:00-22:00,offpeak=22:00-08:00"
    assert cfg2.health_dead_after == 3
    # 未覆盖项保持默认
    assert cfg2.adaptive_qps_threshold == 0.5


async def test_partial_tier_override(fake_redis):
    cfg = await runtime_config.set_runtime_config({"tiers": {"offpeak": {"fetch_num": 2}}})
    assert cfg.tier_params("offpeak").fetch_num == 2
    assert cfg.tier_params("offpeak").health_recheck_interval_s == 30.0  # 其余字段不动


async def test_invalid_value_rejected(fake_redis):
    with pytest.raises(ValidationError):
        await runtime_config.set_runtime_config({"schedule": "不是时段表"})
    with pytest.raises(ValidationError):
        await runtime_config.set_runtime_config({"health_dead_after": 99})
    # 拒绝后不落盘
    cfg = await runtime_config.get_runtime_config()
    assert cfg.health_dead_after == 2


async def test_rollback_restores_previous(fake_redis):
    await runtime_config.set_runtime_config({"health_dead_after": 3})
    await runtime_config.set_runtime_config({"health_dead_after": 5})
    cfg = await runtime_config.rollback_runtime_config()
    assert cfg.health_dead_after == 3
    cfg = await runtime_config.rollback_runtime_config()
    assert cfg.health_dead_after == 2  # 回到 env 默认


async def test_clear_goes_back_to_env(fake_redis):
    await runtime_config.set_runtime_config({"health_dead_after": 4})
    await runtime_config.clear_runtime_config()
    runtime_config._cache = None
    cfg = await runtime_config.get_runtime_config()
    assert cfg.health_dead_after == 2
    # clear 前的值可经 rollback 找回
    cfg2 = await runtime_config.rollback_runtime_config()
    assert cfg2.health_dead_after == 4


async def test_effective_view_marks_overrides(fake_redis):
    await runtime_config.set_runtime_config({"health_dead_after": 3})
    view = await runtime_config.get_effective_view()
    assert view["overrides"] == {"health_dead_after": 3}
    assert view["effective"]["health_dead_after"] == 3


async def test_audit_keeps_last_5(fake_redis):
    from proxy_hub.redis_client import redis_key

    for i in range(8):
        await runtime_config.set_runtime_config({"health_dead_after": (i % 9) + 1})
    store = fake_redis
    key = redis_key("config:runtime:audit")
    assert len(store.lists[key]) == 5
