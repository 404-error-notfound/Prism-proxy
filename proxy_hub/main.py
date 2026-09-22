"""proxy-hub 服务入口 —— FastAPI + APScheduler（分档 tick 动态调间隔）+ 转发代理。

部署形态：同镜像跑 2 个副本即主备（Redis 分布式锁保证同一时刻只有一个
副本真正抽 IP / 巡检，主宕机锁过期后备机自动补位，无需选主逻辑）。
每个副本各自监听隧道端口（自建 HTTP 转发代理），消费方可指向任一副本。
"""
from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from proxy_hub import forwarder, log_shipper, pool
from proxy_hub.api.routes import router
from proxy_hub.config import settings
from proxy_hub.health import HealthChecker
from proxy_hub.logging_setup import es_log_queue, setup_logging
from proxy_hub.providers.qgnet import QgNetProvider
from proxy_hub.redis_client import close_redis, ping_redis

setup_logging()
logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


async def _run_tick(job_id: str, fn, fallback_interval: float) -> None:
    """执行一次 tick 并按其返回值重排下次执行时间（冷热分档热切换）。"""
    try:
        interval = await fn()
    except Exception as e:
        logger.warning("tick %s crashed: %s: %s", job_id, type(e).__name__, e)
        interval = fallback_interval
    if _scheduler is not None:
        _scheduler.reschedule_job(
            job_id, trigger="interval", seconds=max(float(interval), 1.0)
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler

    if not await ping_redis():
        logger.error("Redis not reachable at %s — aborting startup", settings.redis_url)
        sys.exit(1)

    # 装配池核心：provider 按池类型分键（"shared"=短效共享 / "exclusive"=独享），
    # 健康探测共用。未配置对应 env 时该键缺省——shared 缺省 = 禁抓取（退化
    # 只读），exclusive 缺省 = 独享项目补位跳过（pool 侧记 warn，不 crash）
    providers: dict = {}
    if settings.provider_api_url:
        if settings.provider != "qgnet":
            logger.error("unknown provider %r — aborting", settings.provider)
            sys.exit(1)
        providers["shared"] = QgNetProvider(
            settings.provider_api_url, settings.provider_api_timeout_s)
    if settings.exclusive_api_url and settings.exclusive_auth_key:
        from proxy_hub.providers.qgnet_exclusive import QgNetExclusiveProvider
        providers["exclusive"] = QgNetExclusiveProvider(
            settings.exclusive_api_url,
            default_ttl_s=settings.exclusive_default_ttl_s,
            timeout_s=settings.provider_api_timeout_s,
        )
    health = HealthChecker(
        check_url=settings.health_check_url,
        markers=tuple(
            m.strip().encode() for m in settings.health_check_markers.split(",") if m.strip()
        ),
        timeout_s=settings.health_check_timeout_s,
        min_body_bytes=settings.health_min_body_bytes,
        user_agent=settings.health_user_agent,
        max_latency_s=settings.health_max_latency_s,
        liveness_url=settings.exclusive_health_url,
    )
    pool.configure(providers, health)

    # 自建 HTTP 转发代理（隧道模式）：forward_port>0 时监听
    await forwarder.start_forwarder()

    # ES 日志落库（可选）：后台 task 消费日志队列批量写 ES，绝不阻塞主流程
    shipper_task = None
    if settings.log_es_enabled:
        shipper_task = asyncio.create_task(
            log_shipper.run(
                es_log_queue, settings.log_es_url, settings.log_es_index,
                batch_size=settings.log_es_batch_size,
                flush_interval=settings.log_es_flush_interval_s,
            )
        )

    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _run_tick, "interval", seconds=5, id="refresh",
        args=["refresh", pool.refresh_tick, 30.0],
    )
    _scheduler.add_job(
        _run_tick, "interval", seconds=10, id="monitor",
        args=["monitor", pool.monitor_tick, 15.0],
    )
    _scheduler.start()
    logger.info(
        "proxy-hub ready — providers=%s forward_port=%d",
        sorted(providers) or "disabled",
        settings.forward_port,
    )
    yield

    _scheduler.shutdown(wait=False)
    await forwarder.stop_forwarder()
    if shipper_task is not None:
        shipper_task.cancel()
        try:
            await shipper_task  # run() 取消时会尽力冲刷队列剩余日志
        except asyncio.CancelledError:
            pass
    await close_redis()
    logger.info("proxy-hub shutdown complete")


app = FastAPI(title="proxy-hub", version="0.1.0", lifespan=lifespan)
app.include_router(router)


def main() -> None:
    import uvicorn

    uvicorn.run(
        "proxy_hub.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        workers=1,
    )


if __name__ == "__main__":
    main()
