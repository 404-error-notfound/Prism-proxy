"""Redis 连接 —— 单例 + key 前缀隔离，支持 standalone / cluster 两种模式。"""
from __future__ import annotations

import logging

import redis.asyncio as aioredis
from redis.asyncio.cluster import RedisCluster

from proxy_hub.config import settings

logger = logging.getLogger(__name__)

_client: aioredis.Redis | RedisCluster | None = None


def _cluster_url_with_password(url: str, password: str) -> str:
    # redis-py 8.x 在应用 kwargs 前就根据 URL 初始化启动节点，
    # 不把密码嵌进 URL 的话 CLUSTER SLOTS 会不带鉴权发出
    if password and "@" not in url:
        return url.replace("://", f"://:{password}@", 1)
    return url


def get_redis() -> aioredis.Redis | RedisCluster:
    global _client
    if _client is None:
        if settings.redis_cluster_mode:
            _client = RedisCluster.from_url(
                _cluster_url_with_password(settings.redis_url, settings.redis_password),
                password=settings.redis_password or None,
                decode_responses=True,
                protocol=2,
                socket_connect_timeout=2,
                socket_keepalive=True,
                health_check_interval=30,
            )
            logger.info("redis: cluster mode")
        else:
            _client = aioredis.Redis.from_url(
                settings.redis_url,
                password=settings.redis_password or None,
                decode_responses=True,
                protocol=2,
                socket_connect_timeout=2,
                socket_keepalive=True,
                health_check_interval=30,
            )
            logger.info("redis: standalone mode")
    return _client


def make_pipeline(client=None):
    """创建 pipeline。cluster 模式下用非事务 pipeline（跨 slot 按键路由）。"""
    r = client if client is not None else get_redis()
    if isinstance(r, RedisCluster):
        return r.pipeline(transaction=False)
    return r.pipeline()


def redis_key(key: str) -> str:
    """拼接命名空间前缀。"""
    return f"{settings.redis_key_prefix}{key}"


async def ping_redis() -> bool:
    try:
        return await get_redis().ping()
    except Exception as exc:
        logger.warning("redis ping failed: %s", exc)
        return False


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
