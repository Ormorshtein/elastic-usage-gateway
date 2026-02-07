"""Tests for gateway.metrics — simple in-memory counters."""

from gateway.metrics import inc, get_all, reset, observe_es_time, observe_request_time


class TestMetrics:
    def setup_method(self):
        reset()

    def test_increment(self):
        inc("requests_proxied")
        inc("requests_proxied")
        inc("requests_proxied")
        stats = get_all()
        assert stats["requests_proxied"] == 3

    def test_increment_different_counters(self):
        inc("events_emitted")
        inc("events_failed")
        inc("events_failed")
        stats = get_all()
        assert stats["events_emitted"] == 1
        assert stats["events_failed"] == 2

    def test_reset(self):
        inc("requests_proxied")
        inc("events_emitted")
        reset()
        stats = get_all()
        assert stats["requests_proxied"] == 0
        assert stats["events_emitted"] == 0

    def test_get_all_includes_uptime(self):
        stats = get_all()
        assert "uptime_seconds" in stats
        assert "startup_time" in stats
        assert isinstance(stats["uptime_seconds"], float)

    def test_get_all_includes_all_counters(self):
        stats = get_all()
        expected_keys = {
            "requests_proxied", "requests_failed",
            "events_emitted", "events_failed", "events_skipped",
            "extraction_errors",
            "metadata_refresh_ok", "metadata_refresh_failed",
            "startup_time", "uptime_seconds",
        }
        assert expected_keys.issubset(stats.keys())

    def test_zero_by_default(self):
        stats = get_all()
        for key in ("requests_proxied", "requests_failed", "events_emitted",
                     "events_failed", "events_skipped", "extraction_errors"):
            assert stats[key] == 0

    def test_es_time_avg_and_max(self):
        observe_es_time(10.0)
        observe_es_time(30.0)
        stats = get_all()
        assert stats["es_time_avg_ms"] == 20.0
        assert stats["es_time_max_ms"] == 30.0

    def test_es_time_uses_own_count(self):
        """ES time avg should use its own observation count, not requests_proxied."""
        observe_es_time(10.0)
        observe_es_time(20.0)
        observe_es_time(30.0)
        # requests_proxied is 0, but avg should still work
        stats = get_all()
        assert stats["es_time_avg_ms"] == 20.0
        assert stats["requests_proxied"] == 0

    def test_es_time_zero_when_no_observations(self):
        stats = get_all()
        assert stats["es_time_avg_ms"] == 0.0
        assert stats["es_time_max_ms"] == 0.0

    def test_es_time_reset(self):
        observe_es_time(50.0)
        reset()
        stats = get_all()
        assert stats["es_time_avg_ms"] == 0.0
        assert stats["es_time_max_ms"] == 0.0

    def test_request_time_avg_and_max(self):
        observe_request_time(1.0)
        observe_request_time(3.0)
        stats = get_all()
        assert stats["request_time_avg_ms"] == 2.0
        assert stats["request_time_max_ms"] == 3.0

    def test_request_time_zero_when_no_observations(self):
        stats = get_all()
        assert stats["request_time_avg_ms"] == 0.0
        assert stats["request_time_max_ms"] == 0.0

    def test_request_time_reset(self):
        observe_request_time(5.0)
        reset()
        stats = get_all()
        assert stats["request_time_avg_ms"] == 0.0
        assert stats["request_time_max_ms"] == 0.0
