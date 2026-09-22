"""日志体系测试：JsonFormatter 字段规范、ES handler 队列行为、shipper bulk 构造与故障隔离。

不连真实 ES：用 httpx.MockTransport 替换 log_shipper._make_client。
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
import sys

import httpx

from proxy_hub import log_shipper, pool
from proxy_hub.logging_setup import ESLogHandler, JsonFormatter, _InstanceFilter


def _record(msg: str = "hello", **kwargs) -> logging.LogRecord:
    record = logging.LogRecord(
        name="proxy_hub.test", level=logging.INFO, pathname=__file__, lineno=1,
        msg=msg, args=(), exc_info=None,
    )
    for k, v in kwargs.items():
        setattr(record, k, v)
    return record


class TestJsonFormatter:
    def test_flat_schema_fields(self):
        """字段规范：level/logger/msg/service（无 log.level/message 旧字段）。"""
        doc = json.loads(JsonFormatter().format(_record("刷新完成")))
        assert doc["level"] == "INFO"
        assert doc["logger"] == "proxy_hub.test"
        assert doc["msg"] == "刷新完成"
        assert doc["service"] == "proxy-hub"
        assert "@timestamp" in doc and doc["host"] and doc["pid"]
        for legacy in ("log.level", "log.logger", "message", "service.name"):
            assert legacy not in doc

    def test_trace_and_structured_keys(self):
        """追踪键 batch_id/instance_id 与结构化块 event/metrics 提升到顶层。"""
        r = _record(
            batch_id="b1", instance_id="i1",
            metrics={"alive": 3, "tier": "peak"},
            event={"category": "pool", "type": "refresh"},
        )
        doc = json.loads(JsonFormatter().format(r))
        assert doc["batch_id"] == "b1"
        assert doc["instance_id"] == "i1"
        assert doc["metrics"]["alive"] == 3
        assert doc["event"]["type"] == "refresh"

    def test_empty_trace_keys_omitted(self):
        doc = json.loads(JsonFormatter().format(_record()))
        assert "batch_id" not in doc and "instance_id" not in doc

    def test_exception_to_exc_string(self):
        try:
            raise ValueError("boom")
        except ValueError:
            record = _record("failed", exc_info=sys.exc_info())
        doc = json.loads(JsonFormatter().format(record))
        assert isinstance(doc["exc"], str)
        assert "ValueError: boom" in doc["exc"]

    def test_instance_filter_injects_instance_id(self):
        """_InstanceFilter 自动注入 pool.INSTANCE_ID；extra 显式传入时不覆盖。"""
        r = _record()
        _InstanceFilter().filter(r)
        assert r.instance_id == pool.INSTANCE_ID
        doc = json.loads(JsonFormatter().format(r))
        assert doc["instance_id"] == pool.INSTANCE_ID

        r2 = _record(instance_id="explicit")
        _InstanceFilter().filter(r2)
        assert r2.instance_id == "explicit"


class TestESLogHandler:
    def test_doc_shape(self):
        q: queue.Queue = queue.Queue(maxsize=10)
        ESLogHandler(q).emit(_record("pool refreshed", batch_id="b9"))
        doc = q.get_nowait()
        assert doc["msg"] == "pool refreshed"
        assert doc["level"] == "INFO"
        assert doc["service"] == "proxy-hub"
        assert doc["batch_id"] == "b9"
        assert doc["@timestamp"].endswith("+00:00")  # ES 侧 UTC

    def test_queue_full_drops_silently(self):
        """队列满：丢弃不阻塞、不抛。"""
        q: queue.Queue = queue.Queue(maxsize=1)
        handler = ESLogHandler(q)
        handler.emit(_record("first"))
        handler.emit(_record("dropped"))  # 不抛
        assert q.qsize() == 1
        assert q.get_nowait()["msg"] == "first"


class TestBulkBody:
    def test_ndjson_shape(self):
        """bulk body：action/doc 交替的 NDJSON，结尾换行。"""
        body = log_shipper._build_bulk_body("proxyhub_logs", [{"msg": "a"}, {"msg": "b"}])
        assert body.endswith("\n")
        lines = body.splitlines()
        assert len(lines) == 4
        assert json.loads(lines[0]) == {"index": {"_index": "proxyhub_logs"}}
        assert json.loads(lines[1]) == {"msg": "a"}
        assert json.loads(lines[2]) == {"index": {"_index": "proxyhub_logs"}}
        assert json.loads(lines[3]) == {"msg": "b"}


class TestShipper:
    async def test_flush_posts_bulk_ndjson(self):
        """_flush 用 httpx POST /_bulk，Content-Type 为 x-ndjson。"""
        seen: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["path"] = request.url.path
            seen["content_type"] = request.headers["content-type"]
            seen["body"] = request.content.decode()
            return httpx.Response(200, json={"errors": False, "items": []})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://es:9200"
        ) as client:
            ok = await log_shipper._flush(client, "proxyhub_logs", [{"msg": "a"}])
        assert ok is True
        assert seen["path"] == "/_bulk"
        assert seen["content_type"] == "application/x-ndjson"
        assert json.loads(seen["body"].splitlines()[0]) == {
            "index": {"_index": "proxyhub_logs"}
        }

    async def test_ensure_index_creates_when_missing(self):
        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(f"{request.method} {request.url.path}")
            if request.method == "HEAD":
                return httpx.Response(404)
            return httpx.Response(200, json={"acknowledged": True})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://es:9200"
        ) as client:
            assert await log_shipper._ensure_index(client, "proxyhub_logs") is True
        assert calls == ["HEAD /proxyhub_logs", "PUT /proxyhub_logs"]

    async def test_es_down_never_raises_or_blocks(self, monkeypatch):
        """ES 不可达：shipper 持续消费队列但不抛、不阻塞，可正常取消退出。"""

        def _boom_client(_url: str) -> httpx.AsyncClient:
            def handler(request: httpx.Request) -> httpx.Response:
                raise httpx.ConnectError("connection refused")

            return httpx.AsyncClient(transport=httpx.MockTransport(handler))

        monkeypatch.setattr(log_shipper, "_make_client", _boom_client)

        q: queue.Queue = queue.Queue(maxsize=100)
        q.put_nowait({"msg": "x"})
        task = asyncio.create_task(
            log_shipper.run(
                q, "http://es:9200", "proxyhub_logs",
                batch_size=10, flush_interval=0.05,
            )
        )
        await asyncio.sleep(0.3)
        assert not task.done()  # 仍在跑，没有崩溃退出
        task.cancel()
        await asyncio.wait_for(task, timeout=2.0)  # run() 捕获 CancelledError 正常返回
        assert q.empty()  # 队列被消费（记录被丢弃而非堆积）
