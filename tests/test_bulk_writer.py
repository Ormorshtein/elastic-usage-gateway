"""Tests for gateway.events — bulk writer, queue backpressure, and NDJSON building."""

import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock

from gateway.events import (
    _build_bulk_body,
    _flush_events,
    emit_event_background,
    start_bulk_writer,
    stop_bulk_writer,
)
from gateway import events as events_mod
from gateway import metrics


class TestBuildBulkBody:
    def test_single_event(self):
        events = [{"index": "products", "operation": "search"}]
        body = _build_bulk_body(events)
        lines = body.strip().split("\n")
        assert len(lines) == 2
        action = json.loads(lines[0])
        assert action == {"index": {"_index": ".usage-events"}}
        doc = json.loads(lines[1])
        assert doc == {"index": "products", "operation": "search"}

    def test_multiple_events(self):
        events = [
            {"index": "products", "operation": "search"},
            {"index": "logs", "operation": "bulk"},
            {"index": "orders", "operation": "doc_write"},
        ]
        body = _build_bulk_body(events)
        lines = body.strip().split("\n")
        assert len(lines) == 6  # 3 action lines + 3 doc lines

        for i in range(0, 6, 2):
            action = json.loads(lines[i])
            assert "index" in action
            assert action["index"]["_index"] == ".usage-events"

        docs = [json.loads(lines[i]) for i in range(1, 6, 2)]
        assert docs[0]["index"] == "products"
        assert docs[1]["index"] == "logs"
        assert docs[2]["index"] == "orders"

    def test_empty_events(self):
        body = _build_bulk_body([])
        assert body == "\n"

    def test_body_ends_with_newline(self):
        events = [{"x": 1}]
        body = _build_bulk_body(events)
        assert body.endswith("\n")


class TestFlushEvents:
    async def test_empty_list_no_call(self):
        with patch.object(events_mod._event_client, "post") as mock_post:
            await _flush_events([])
            mock_post.assert_not_called()

    async def test_successful_bulk_write(self):
        metrics.reset()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "errors": False,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 201}},
            ]
        }

        with patch.object(events_mod._event_client, "post", new_callable=AsyncMock, return_value=mock_resp) as mock_post:
            await _flush_events([{"a": 1}, {"b": 2}])
            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            assert call_kwargs.kwargs["headers"]["Content-Type"] == "application/x-ndjson"

        stats = metrics.get_all()
        assert stats["events_emitted"] == 2
        assert stats["events_failed"] == 0

    async def test_partial_failure_bulk_write(self):
        metrics.reset()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "errors": True,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
                {"index": {"status": 201}},
            ]
        }

        with patch.object(events_mod._event_client, "post", new_callable=AsyncMock, return_value=mock_resp):
            await _flush_events([{"a": 1}, {"b": 2}, {"c": 3}])

        stats = metrics.get_all()
        assert stats["events_emitted"] == 2
        assert stats["events_failed"] == 1

    async def test_http_error_marks_all_failed(self):
        metrics.reset()
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"

        with patch.object(events_mod._event_client, "post", new_callable=AsyncMock, return_value=mock_resp):
            await _flush_events([{"a": 1}, {"b": 2}])

        stats = metrics.get_all()
        assert stats["events_emitted"] == 0
        assert stats["events_failed"] == 2

    async def test_network_error_marks_all_failed(self):
        metrics.reset()
        import httpx
        with patch.object(events_mod._event_client, "post", new_callable=AsyncMock, side_effect=httpx.ConnectError("refused")):
            await _flush_events([{"a": 1}, {"b": 2}, {"c": 3}])

        stats = metrics.get_all()
        assert stats["events_emitted"] == 0
        assert stats["events_failed"] == 3


class TestEmitEventBackground:
    def test_drops_when_no_queue(self):
        """Events are dropped gracefully when bulk writer is not started."""
        old_queue = events_mod._event_queue
        events_mod._event_queue = None
        try:
            emit_event_background({"test": True})  # should not raise
        finally:
            events_mod._event_queue = old_queue

    def test_enqueues_event(self):
        old_queue = events_mod._event_queue
        events_mod._event_queue = asyncio.Queue(maxsize=10)
        try:
            emit_event_background({"test": True})
            assert events_mod._event_queue.qsize() == 1
        finally:
            events_mod._event_queue = old_queue

    def test_drops_when_queue_full(self):
        metrics.reset()
        old_queue = events_mod._event_queue
        events_mod._event_queue = asyncio.Queue(maxsize=2)
        try:
            emit_event_background({"a": 1})
            emit_event_background({"b": 2})
            emit_event_background({"c": 3})  # should be dropped
            assert events_mod._event_queue.qsize() == 2
            stats = metrics.get_all()
            assert stats["events_dropped"] == 1
        finally:
            events_mod._event_queue = old_queue


class TestBulkWriterLifecycle:
    async def test_start_and_stop(self):
        """Bulk writer starts and stops cleanly."""
        start_bulk_writer()
        assert events_mod._event_queue is not None
        assert events_mod._bulk_writer_task is not None
        assert not events_mod._bulk_writer_task.done()

        await stop_bulk_writer()
        assert events_mod._bulk_writer_task is None

    async def test_flushes_on_shutdown(self):
        """Remaining events in queue are flushed when writer is stopped."""
        metrics.reset()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "errors": False,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 201}},
            ]
        }

        # Patch client BEFORE starting the writer so all flushes go through mock
        with patch.object(events_mod._event_client, "post", new_callable=AsyncMock, return_value=mock_resp) as mock_post:
            start_bulk_writer()
            # Yield so the writer task enters its first await (queue.get)
            await asyncio.sleep(0)

            # Enqueue events directly
            events_mod._event_queue.put_nowait({"a": 1})
            events_mod._event_queue.put_nowait({"b": 2})

            # Give the writer loop a chance to pick up events or let shutdown flush them
            await stop_bulk_writer()

            # The mock should have been called (either by the loop or the shutdown flush)
            assert mock_post.called

        stats = metrics.get_all()
        assert stats["events_emitted"] == 2
