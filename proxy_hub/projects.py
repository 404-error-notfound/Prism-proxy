"""项目注册表 —— 多项目隔离：每项目独立 token、独立池、独立成本。

Redis hash `projects`：field=项目名，value=JSON {token, pool_size, created_at}。
token 只在创建响应里完整返回一次；Redis 存明文（与 admin token 同级的运维
安全模型——能读 Redis 的人本来就能改池子）。

消费端点与隧道转发都靠 token 认项目：REST 走 Authorization: Bearer <token>，
隧道走 Proxy-Authorization: Basic base64(<项目名>:<token>)。
"""
from __future__ import annotations

import json
import logging
import re
import secrets
import time

from proxy_hub.redis_client import get_redis, redis_key

logger = logging.getLogger(__name__)

K_PROJECTS = "projects"

_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")

# 池类型：shared=短效共享池（默认）；exclusive=独享池（青果独享代理，按项目
# 独立选 provider，见 pool._provider_for）。老项目 JSON 无此字段按 shared 处理。
POOL_TYPES = ("shared", "exclusive")

# resolve_token 的全表缓存（10s）：项目数量小，隧道每连接认证不能都打 Redis
_table_cache: tuple[float, dict[str, dict]] = (0.0, {})


def _validate_name(name: str) -> None:
    if not _NAME_RE.match(name):
        raise ValueError(
            f"invalid project name {name!r}: expect [a-z0-9][a-z0-9-]{{0,31}}"
        )


async def create_project(name: str, pool_size: int = 0,
                         pool_type: str = "shared") -> dict:
    """注册项目，返回含 token 的项目信息（token 仅此一次完整返回）。

    pool_size=0 表示跟随冷热分档的 fetch_num；>0 则固定为该值（覆盖档位）。
    pool_type: "shared"（默认，短效共享池）| "exclusive"（独享池）。
    """
    _validate_name(name)
    if pool_size < 0:
        raise ValueError("pool_size must be >= 0 (0 = follow tier fetch_num)")
    if pool_type not in POOL_TYPES:
        raise ValueError(f"pool_type must be one of {POOL_TYPES}")
    r = get_redis()
    if await r.hget(redis_key(K_PROJECTS), name) is not None:
        raise ValueError(f"project {name!r} already exists")
    proj = {"token": secrets.token_urlsafe(24), "pool_size": pool_size,
            "pool_type": pool_type, "created_at": time.time()}
    await r.hset(redis_key(K_PROJECTS), name, json.dumps(proj))
    _bust_cache()
    logger.info("projects: created %r (pool_size=%d, pool_type=%s)",
                name, pool_size, pool_type)
    return {"name": name, **proj}


async def list_projects() -> dict[str, dict]:
    """全量项目表（admin 展示用，含 token）。"""
    return await _load_table()


async def get_project(name: str) -> dict | None:
    return (await _load_table()).get(name)


async def delete_project(name: str) -> bool:
    """删除注册。池清理由调用方负责（pool.delete_pool）。"""
    r = get_redis()
    removed = await r.hdel(redis_key(K_PROJECTS), name)
    _bust_cache()
    if removed:
        logger.info("projects: deleted %r", name)
    return bool(removed)


async def resolve_token(token: str) -> str | None:
    """token → 项目名；未注册返回 None。"""
    for name, proj in (await _load_table()).items():
        if proj.get("token") == token:
            return name
    return None


def pool_type_of(proj_meta: dict | None) -> str:
    """项目池类型：老项目 JSON 无 pool_type 字段按 shared 处理。"""
    return (proj_meta or {}).get("pool_type") or "shared"


async def _load_table() -> dict[str, dict]:
    global _table_cache
    cached_at, cached = _table_cache
    if time.monotonic() - cached_at < 10.0:
        return cached
    try:
        raw = await get_redis().hgetall(redis_key(K_PROJECTS))
        table = {}
        for name, payload in raw.items():
            try:
                table[name] = json.loads(payload)
            except (ValueError, TypeError):
                logger.warning("projects: corrupt entry %r, ignored", name)
    except Exception as e:
        logger.warning("projects: read failed, using cache: %s", e)
        table = cached
    _table_cache = (time.monotonic(), table)
    return table


def _bust_cache() -> None:
    global _table_cache
    _table_cache = (0.0, {})
