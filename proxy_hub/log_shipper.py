"""异步日志投递：内存队列 → httpx 批量写 Elasticsearch proxyhub_logs 索引。

不引 elasticsearch 包：直接 POST {es_url}/_bulk，body 为 index action + doc 交替的 NDJSON。
索引与业务数据完全独立，只用于存储 proxy-hub 自身的运行日志。

故障隔离保证：
- ESLogHandler.emit() 非阻塞：队列满时静默丢弃，绝不阻塞主流程
- ES 宕机时：flush 失败计数，连续 ≥3 次记一条 warning 后退避 60s
  （期间队列满即丢弃，绝不堆积内存）
- shipper task 意外崩溃时：外层守护循环自动重启（指数退避，最多 60s）
- 请求超时 5s，不因 ES 无响应而长时间阻塞事件循环
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
from typing import Any

import httpx

from proxy_hub.config import settings

logger = logging.getLogger(__name__)

_INDEX_MAPPING: dict[str, Any] = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "level": {"type": "keyword"},
            "logger": {"type": "keyword"},
            "msg": {"type": "text"},
            "service": {"type": "keyword"},
            "host": {"type": "keyword"},
            "batch_id": {"type": "keyword"},
            "instance_id": {"type": "keyword"},
            "exc": {"type": "text", "index": False},
        }
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
}

# 连续 flush 失败多少次后开始退避
_BACKOFF_AFTER_FAILURES = 3
# 退避时长（秒）；期间队列继续被消费但 ES 持续不可用时记录会被丢弃
_BACKOFF_S = 60.0
_REQUEST_TIMEOUT_S = 5.0


def _make_client(es_url: str) -> httpx.AsyncClient:
    # ES 集群开了安全认证（401 without credentials），账密从配置注入
    auth = None
    if settings.log_es_user:
        auth = (settings.log_es_user, settings.log_es_password)
    return httpx.AsyncClient(base_url=es_url, timeout=_REQUEST_TIMEOUT_S, auth=auth)


def _build_bulk_body(index: str, batch: list[dict[str, Any]]) -> str:
    """ES _bulk NDJSON：每个文档两行（index action + source），结尾换行。"""
    action = json.dumps({"index": {"_index": index}})
    lines: list[str] = []
    for doc in batch:
        lines.append(action)
        lines.append(json.dumps(doc, ensure_ascii=False))
    return "\n".join(lines) + "\n"


async def _ensure_index(client: httpx.AsyncClient, index: str) -> bool:
    """建立索引（若不存在）。返回 True 表示索引就绪，False 表示 ES 不可用。创建失败不致命。"""
    try:
        resp = await client.head(f"/{index}")
        if resp.status_code == 200:
            return True
        resp = await client.put(f"/{index}", json=_INDEX_MAPPING)
        if resp.status_code in (200, 201) or "resource_already_exists" in resp.text:
            logger.info("log_shipper: created index %s", index)
            return True
        logger.warning(
            "log_shipper: create index %s -> %s: %s", index, resp.status_code, resp.text[:200]
        )
        return False
    except Exception as e:
        logger.warning("log_shipper: ES unavailable, _ensure_index failed: %s", e)
        return False


async def _flush(client: httpx.AsyncClient, index: str, batch: list[dict[str, Any]]) -> bool:
    """返回 True 表示写入成功，False 表示 ES 不可用。"""
    try:
        resp = await client.post(
            "/_bulk",
            content=_build_bulk_body(index, batch),
            headers={"Content-Type": "application/x-ndjson"},
        )
        if resp.status_code != 200:
            logger.debug("log_shipper: bulk -> %s: %s", resp.status_code, resp.text[:200])
            return False
        if resp.json().get("errors"):
            logger.debug("log_shipper: bulk has item errors (partial write)")
        return True
    except Exception as e:
        logger.debug("log_shipper: flush failed (ES down?): %s", e)
        return False


async def _run_once(
    log_queue: queue.Queue,
    es_url: str,
    es_index: str,
    batch_size: int,
    flush_interval: float,
) -> None:
    """单次运行循环。意外崩溃时向上抛出，由外层守护负责重启。"""
    async with _make_client(es_url) as client:
        index_ready = await _ensure_index(client, es_index)
        consecutive_failures = 0

        try:
            while True:
                batch: list[dict] = []
                loop = asyncio.get_running_loop()
                deadline = loop.time() + flush_interval

                # 收集一批记录：满 batch_size 或到 flush_interval 为止
                while len(batch) < batch_size:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        batch.append(log_queue.get_nowait())
                    except queue.Empty:
                        await asyncio.sleep(min(0.05, remaining))

                if not batch:
                    continue

                # 索引尚未建好时尝试补建
                if not index_ready:
                    index_ready = await _ensure_index(client, es_index)

                if index_ready:
                    ok = await _flush(client, es_index, batch)
                    if ok:
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                        if consecutive_failures == _BACKOFF_AFTER_FAILURES:
                            logger.warning(
                                "log_shipper: ES unreachable after %d attempts, "
                                "backing off %.0fs (logs will be dropped if queue fills)",
                                consecutive_failures, _BACKOFF_S,
                            )
                        if consecutive_failures >= _BACKOFF_AFTER_FAILURES:
                            # ES 持续不可用：退避等待，期间让队列自然溢出丢弃
                            await asyncio.sleep(_BACKOFF_S)
                            index_ready = False  # 醒来后重新确认索引
                            consecutive_failures = 0
                # index 未就绪时记录已从队列取出但无法写入，只能丢弃——
                # 这比让队列无限堆积阻塞内存更安全

        except asyncio.CancelledError:
            # 优雅关闭：把队列里剩余记录一次性冲刷
            drain: list[dict] = []
            try:
                while True:
                    drain.append(log_queue.get_nowait())
            except queue.Empty:
                pass
            if drain and index_ready:
                await _flush(client, es_index, drain)
            raise


async def run(
    log_queue: queue.Queue,
    es_url: str,
    es_index: str,
    *,
    batch_size: int = 100,
    flush_interval: float = 5.0,
) -> None:
    """守护循环：_run_once 意外崩溃时自动重启，不影响主业务。"""
    logger.info(
        "log_shipper: started → %s/%s (batch=%d, interval=%.1fs)",
        es_url, es_index, batch_size, flush_interval,
    )
    restart_wait = 5.0
    while True:
        try:
            await _run_once(log_queue, es_url, es_index, batch_size, flush_interval)
        except asyncio.CancelledError:
            logger.info("log_shipper: stopped")
            return
        except Exception as exc:
            logger.error(
                "log_shipper: unexpected crash (restarting in %.0fs): %s",
                restart_wait, exc, exc_info=True,
            )
            await asyncio.sleep(restart_wait)
            restart_wait = min(restart_wait * 2, 60.0)  # 指数退避，最多 60s
