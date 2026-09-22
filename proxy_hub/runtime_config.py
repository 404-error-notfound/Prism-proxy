"""运行期热配置 —— env 默认值 → Redis config:runtime 覆盖，≤15s 热生效。

存 Redis 的只是"覆盖项"（partial dict）：读取时合并到 env 默认之上。
变更写审计 list（最近 5 版快照），支持一键回滚。
进程内 10s TTL 缓存：后台 tick 每轮取用，配置变更至多 ~15s 生效。
"""
from __future__ import annotations

import json
import logging
import time

from proxy_hub.redis_client import get_redis, make_pipeline, redis_key
from proxy_hub.schedule import RuntimeConfig

logger = logging.getLogger(__name__)

K_CONFIG = "config:runtime"
K_AUDIT = "config:runtime:audit"

_AUDIT_KEEP = 5
_CACHE_TTL_S = 10.0

_cache: tuple[float, RuntimeConfig] | None = None  # (cached_at_mono, config)


def _default_config() -> RuntimeConfig:
    """env 默认值（当前 RuntimeConfig 字段默认即 env 行为；后续如需 env 覆盖默认在此接入）。"""
    return RuntimeConfig()


def _merge(override: dict) -> RuntimeConfig:
    base = _default_config().model_dump()
    _deep_update(base, override)
    return RuntimeConfig(**base)  # pydantic 校验（含时段表可解析），非法值抛 ValidationError


def _deep_update(base: dict, patch: dict) -> None:
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v


async def get_runtime_config() -> RuntimeConfig:
    """读取生效配置（env 默认 + Redis 覆盖）。Redis 异常/数据非法 → 回退 env 默认。"""
    global _cache
    if _cache and time.monotonic() - _cache[0] < _CACHE_TTL_S:
        return _cache[1]
    cfg = _default_config()
    try:
        raw = await get_redis().get(redis_key(K_CONFIG))
        if raw:
            cfg = _merge(json.loads(raw))
    except Exception as e:
        logger.warning("runtime_config: read failed, using env defaults: %s", e)
    _cache = (time.monotonic(), cfg)
    return cfg


async def get_effective_view() -> dict:
    """Admin GET 用：生效配置 + 覆盖项来源标注。"""
    try:
        raw = await get_redis().get(redis_key(K_CONFIG))
        override = json.loads(raw) if raw else {}
    except Exception:
        override = {}
    cfg = await get_runtime_config()
    return {"effective": cfg.model_dump(), "overrides": override}


def _bust_cache() -> None:
    global _cache
    _cache = None


async def set_runtime_config(patch: dict) -> RuntimeConfig:
    """部分更新覆盖项。先校验（合并后整体过 pydantic），非法值拒绝写入。"""
    r = get_redis()
    raw = await r.get(redis_key(K_CONFIG))
    current = json.loads(raw) if raw else {}
    merged_override = dict(current)
    _deep_update(merged_override, patch)
    cfg = _merge(merged_override)  # 校验失败在此抛 ValidationError，不落盘

    pipe = make_pipeline(r)
    pipe.lpush(redis_key(K_AUDIT), json.dumps(current))
    pipe.ltrim(redis_key(K_AUDIT), 0, _AUDIT_KEEP - 1)
    pipe.set(redis_key(K_CONFIG), json.dumps(merged_override))
    await pipe.execute()
    _bust_cache()
    logger.info("runtime_config: updated, override keys=%s", sorted(merged_override))
    return cfg


async def rollback_runtime_config() -> RuntimeConfig:
    """回滚到上一版覆盖快照。无快照时回 env 默认。"""
    r = get_redis()
    snapshot = await r.lpop(redis_key(K_AUDIT))
    override = json.loads(snapshot) if snapshot else {}
    if override:
        await r.set(redis_key(K_CONFIG), json.dumps(override))
    else:
        await r.delete(redis_key(K_CONFIG))
    _bust_cache()
    cfg = _merge(override)
    logger.info("runtime_config: rolled back, override keys=%s", sorted(override))
    return cfg


async def clear_runtime_config() -> None:
    """清除全部覆盖（回 env 默认），当前值先入审计可回滚。"""
    r = get_redis()
    raw = await r.get(redis_key(K_CONFIG))
    if raw:
        pipe = make_pipeline(r)
        pipe.lpush(redis_key(K_AUDIT), raw)
        pipe.ltrim(redis_key(K_AUDIT), 0, _AUDIT_KEEP - 1)
        pipe.delete(redis_key(K_CONFIG))
        await pipe.execute()
    _bust_cache()
    logger.info("runtime_config: cleared to env defaults")
