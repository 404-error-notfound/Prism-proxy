"""结构化日志体系（与 prism 统一规范）—— stdout（dev 文本 / prod JSON）+ 可选 ES 落库。

每条日志一个扁平 JSON 文档，字段：
  @timestamp  — 文件/stdout 用本地时间+时区偏移；ES 文档用 UTC
  level/logger/msg/service（"proxy-hub"）/host/pid
  batch_id/instance_id — 追踪键（换批 / 多副本实例）
  event={"category","type"} / metrics={...} — 结构化块（extra= 传入）
  exc — 异常堆栈字符串

ES 一路经非阻塞有界队列（满则丢弃，绝不阻塞主流程），
由 proxy_hub.log_shipper 异步批量写入 proxyhub_logs 索引。
"""
from __future__ import annotations

import json
import logging
import os
import queue
import socket
import sys
import traceback
from datetime import UTC, datetime
from typing import Any

from proxy_hub.config import settings

SERVICE_NAME = "proxy-hub"

# 有界队列：超出容量时丢弃新日志而不是无限堆积内存（10 000 条 × ~500B ≈ 5MB）
_ES_QUEUE_MAXSIZE = 10_000
es_log_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=_ES_QUEUE_MAXSIZE)

_STRUCTURED_KEYS = ("event", "metrics")
# batch_id=补位批次 / instance_id=副本标识 / query_id=消费方查询 ID（prism 信号
# body 带来，跨链路关联锚点——proxyhub_logs 按 query_id 检索直达消费方查询）
_TRACE_KEYS = ("batch_id", "instance_id", "query_id")


def _copy_structured(record: logging.LogRecord, doc: dict[str, Any]) -> None:
    """把 extra= 传入的追踪键（batch_id/instance_id）与结构化块（event/metrics）
    提取到日志文档。JsonFormatter（stdout）与 ESLogHandler（ES 索引）共用，两路字段一致。"""
    for key in _STRUCTURED_KEYS:
        if hasattr(record, key):
            doc[key] = getattr(record, key)
    for key in _TRACE_KEYS:
        val = getattr(record, key, None)
        if val:
            doc[key] = val


class JsonFormatter(logging.Formatter):
    """一行一个 JSON 文档，字段名与 ES 文档完全对齐：
    @timestamp/level/logger/msg/service/host/pid + batch_id/instance_id
    + 结构化块（event/metrics）。

    @timestamp 用本地时间+时区偏移（如 +08:00），人读友好且无歧义；
    ES 侧是 UTC（Kibana 按浏览器时区显示），两边可直接对照。
    """

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "@timestamp": datetime.fromtimestamp(record.created)
            .astimezone()
            .isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "service": SERVICE_NAME,
            "host": socket.gethostname(),
            "pid": os.getpid(),
        }
        _copy_structured(record, doc)
        if record.exc_info:
            doc["exc"] = "".join(traceback.format_exception(*record.exc_info))
        return json.dumps(doc, ensure_ascii=False)


class _InstanceFilter(logging.Filter):
    """给每条记录注入实例标识（锁持有者/池发布者身份），多副本日志可区分来源。

    extra={"instance_id": ...} 显式传入时不覆盖。
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not getattr(record, "instance_id", None):
            from proxy_hub import pool

            record.instance_id = pool.INSTANCE_ID  # type: ignore[attr-defined]
        return True


class ESLogHandler(logging.Handler):
    """非阻塞 ES 日志 handler：将结构化记录放入队列，由 log_shipper 异步批量写入。

    队列满时静默丢弃——ES 宕机绝不能阻塞业务路径。
    """

    def __init__(self, log_queue: queue.Queue, min_level: int = logging.INFO) -> None:
        super().__init__(min_level)
        self._queue = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            doc: dict[str, Any] = {
                "@timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
                "service": SERVICE_NAME,
                "host": socket.gethostname(),
                "pid": os.getpid(),
            }
            _copy_structured(record, doc)
            if record.exc_info:
                doc["exc"] = "".join(traceback.format_exception(*record.exc_info))
            try:
                self._queue.put_nowait(doc)
            except queue.Full:
                pass  # ES 不可用时丢弃日志，绝不阻塞请求路径
        except Exception:
            self.handleError(record)


def setup_logging() -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # stdout handler：dev 文本 / prod JSON
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.addFilter(_InstanceFilter())
    if settings.app_env == "prod":
        stdout_handler.setFormatter(JsonFormatter())
    else:
        stdout_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )

    handlers: list[logging.Handler] = [stdout_handler]

    # ES log handler（非阻塞，log_shipper 异步消费）
    if settings.log_es_enabled:
        es_min_level = getattr(logging, settings.log_es_min_level.upper(), logging.INFO)
        es_handler = ESLogHandler(es_log_queue, es_min_level)
        es_handler.addFilter(_InstanceFilter())
        handlers.append(es_handler)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    for h in handlers:
        root.addHandler(h)

    # 静音噪音三方库：apscheduler 每 tick 两条 Running job/executed successfully 纯属噪音
    for noisy in (
        "httpx", "httpcore", "urllib3", "uvicorn.access", "apscheduler.executors.default",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)
