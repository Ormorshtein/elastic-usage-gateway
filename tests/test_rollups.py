"""Tests for gateway.rollups — aggregation logic and config."""

from gateway.rollups import aggregate_events, get_rollup_config, set_rollup_config


class TestAggregateEventsBasic:
    def test_single_event(self):
        events = [{
            "index_group": "products",
            "index": "products",
            "sample_weight": 1.0,
            "fields": {
                "queried": ["title"],
                "filtered": ["price"],
                "aggregated": [],
                "sorted": [],
                "sourced": ["title", "price"],
                "written": [],
            },
            "lookback_seconds": None,
            "response_time_ms": 10.0,
        }]
        result = aggregate_events(events)
        assert len(result) == 1
        doc = result[0]
        assert doc["type"] == "rollup"
        assert doc["index_group"] == "products"
        assert doc["index"] == "products"
        assert doc["total_operations"] == 1.0
        assert doc["field_usage"]["title"]["queried"] == 1
        assert doc["field_usage"]["title"]["sourced"] == 1
        assert doc["field_usage"]["price"]["filtered"] == 1
        assert doc["field_usage"]["price"]["sourced"] == 1

    def test_multiple_events_same_index(self):
        events = [
            {
                "index_group": "products", "index": "products",
                "sample_weight": 1.0,
                "fields": {"queried": ["title"], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 10.0,
            },
            {
                "index_group": "products", "index": "products",
                "sample_weight": 1.0,
                "fields": {"queried": ["title", "description"], "filtered": ["price"],
                           "aggregated": [], "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 20.0,
            },
        ]
        result = aggregate_events(events)
        assert len(result) == 1
        doc = result[0]
        assert doc["total_operations"] == 2.0
        assert doc["field_usage"]["title"]["queried"] == 2
        assert doc["field_usage"]["description"]["queried"] == 1
        assert doc["field_usage"]["price"]["filtered"] == 1


class TestAggregateEventsWeighted:
    def test_weighted_events(self):
        events = [{
            "index_group": "logs", "index": "logs",
            "sample_weight": 5.0,
            "fields": {"queried": ["message"], "filtered": ["level"],
                       "aggregated": [], "sorted": [], "sourced": [], "written": []},
            "lookback_seconds": None, "response_time_ms": 15.0,
        }]
        result = aggregate_events(events)
        assert len(result) == 1
        doc = result[0]
        assert doc["total_operations"] == 5.0
        assert doc["field_usage"]["message"]["queried"] == 5
        assert doc["field_usage"]["level"]["filtered"] == 5

    def test_mixed_weights(self):
        events = [
            {
                "index_group": "logs", "index": "logs",
                "sample_weight": 1.0,
                "fields": {"queried": ["message"], "filtered": [],
                           "aggregated": [], "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 10.0,
            },
            {
                "index_group": "logs", "index": "logs",
                "sample_weight": 3.0,
                "fields": {"queried": ["message"], "filtered": [],
                           "aggregated": [], "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 20.0,
            },
        ]
        result = aggregate_events(events)
        doc = result[0]
        assert doc["total_operations"] == 4.0  # 1 + 3
        assert doc["field_usage"]["message"]["queried"] == 4  # 1 + 3


class TestAggregateEventsEmpty:
    def test_empty_input(self):
        result = aggregate_events([])
        assert result == []


class TestAggregateEventsMissingFields:
    def test_missing_fields_key(self):
        """Events without a 'fields' key should not crash."""
        events = [{
            "index_group": "x", "index": "x",
            "sample_weight": 1.0,
            "lookback_seconds": None, "response_time_ms": 5.0,
        }]
        result = aggregate_events(events)
        assert len(result) == 1
        assert result[0]["total_operations"] == 1.0
        assert result[0]["field_usage"] == {}

    def test_partial_fields(self):
        """Events with only some field categories."""
        events = [{
            "index_group": "x", "index": "x",
            "sample_weight": 1.0,
            "fields": {"queried": ["a"]},  # Missing filtered, aggregated, etc.
            "lookback_seconds": None, "response_time_ms": 5.0,
        }]
        result = aggregate_events(events)
        assert len(result) == 1
        assert result[0]["field_usage"]["a"]["queried"] == 1


class TestAggregateEventsMultipleIndices:
    def test_groups_by_index_group_and_index(self):
        events = [
            {
                "index_group": "logs", "index": "logs-2026.01",
                "sample_weight": 1.0,
                "fields": {"queried": ["msg"], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 10.0,
            },
            {
                "index_group": "logs", "index": "logs-2026.02",
                "sample_weight": 1.0,
                "fields": {"queried": ["msg"], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 20.0,
            },
        ]
        result = aggregate_events(events)
        assert len(result) == 2
        indices = {doc["index"] for doc in result}
        assert indices == {"logs-2026.01", "logs-2026.02"}


class TestAggregateEventsLookback:
    def test_lookback_stats(self):
        events = [
            {
                "index_group": "logs", "index": "logs",
                "sample_weight": 1.0,
                "fields": {"queried": [], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": 3600.0, "response_time_ms": 10.0,
            },
            {
                "index_group": "logs", "index": "logs",
                "sample_weight": 1.0,
                "fields": {"queried": [], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": 86400.0, "response_time_ms": 20.0,
            },
        ]
        result = aggregate_events(events)
        doc = result[0]
        assert doc["lookback_max_seconds"] == 86400.0
        assert doc["lookback_sum_seconds"] == 3600.0 + 86400.0
        assert doc["lookback_count"] == 2

    def test_lookback_null_ignored(self):
        events = [
            {
                "index_group": "x", "index": "x",
                "sample_weight": 1.0,
                "fields": {"queried": [], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 10.0,
            },
        ]
        result = aggregate_events(events)
        doc = result[0]
        assert doc["lookback_count"] == 0
        assert doc["lookback_sum_seconds"] == 0.0


class TestAggregateEventsResponseTime:
    def test_weighted_avg_response_time(self):
        events = [
            {
                "index_group": "x", "index": "x",
                "sample_weight": 1.0,
                "fields": {"queried": [], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 10.0,
            },
            {
                "index_group": "x", "index": "x",
                "sample_weight": 3.0,
                "fields": {"queried": [], "filtered": [], "aggregated": [],
                           "sorted": [], "sourced": [], "written": []},
                "lookback_seconds": None, "response_time_ms": 20.0,
            },
        ]
        result = aggregate_events(events)
        doc = result[0]
        # Weighted avg: (10*1 + 20*3) / 2 = 70 / 2 = 35
        assert doc["avg_response_time_ms"] == 35.0


class TestRollupDocSchema:
    def test_required_fields_present(self):
        events = [{
            "index_group": "test", "index": "test",
            "sample_weight": 1.0,
            "fields": {"queried": ["a"], "filtered": [], "aggregated": [],
                       "sorted": [], "sourced": [], "written": []},
            "lookback_seconds": None, "response_time_ms": 5.0,
        }]
        result = aggregate_events(events)
        doc = result[0]
        required = {"type", "index_group", "index", "total_operations",
                     "field_usage", "lookback_sum_seconds", "lookback_max_seconds",
                     "lookback_count", "avg_response_time_ms"}
        assert required.issubset(doc.keys())
        assert doc["type"] == "rollup"


class TestRollupConfig:
    def setup_method(self):
        set_rollup_config(interval_seconds=300, raw_retention_hours=1.0, rollup_retention_days=90)

    def teardown_method(self):
        set_rollup_config(interval_seconds=300, raw_retention_hours=1.0, rollup_retention_days=90)

    def test_get_defaults(self):
        config = get_rollup_config()
        assert config["interval_seconds"] == 300
        assert config["raw_retention_hours"] == 1.0
        assert config["rollup_retention_days"] == 90

    def test_set_interval(self):
        result = set_rollup_config(interval_seconds=120)
        assert result["interval_seconds"] == 120

    def test_clamp_interval_min(self):
        result = set_rollup_config(interval_seconds=10)
        assert result["interval_seconds"] == 60  # minimum 60

    def test_clamp_interval_max(self):
        result = set_rollup_config(interval_seconds=1000)
        assert result["interval_seconds"] == 600  # maximum 600

    def test_set_raw_retention(self):
        result = set_rollup_config(raw_retention_hours=2.0)
        assert result["raw_retention_hours"] == 2.0

    def test_clamp_raw_retention_min(self):
        result = set_rollup_config(raw_retention_hours=0.1)
        assert result["raw_retention_hours"] == 0.5

    def test_set_rollup_retention(self):
        result = set_rollup_config(rollup_retention_days=30)
        assert result["rollup_retention_days"] == 30

    def test_clamp_rollup_retention_min(self):
        result = set_rollup_config(rollup_retention_days=0)
        assert result["rollup_retention_days"] == 1


class TestAggregateEventsDefaultWeight:
    def test_missing_sample_weight_defaults_to_one(self):
        """Events without sample_weight should be treated as weight=1.0."""
        events = [{
            "index_group": "x", "index": "x",
            "fields": {"queried": ["a"], "filtered": [], "aggregated": [],
                       "sorted": [], "sourced": [], "written": []},
            "lookback_seconds": None, "response_time_ms": 5.0,
        }]
        result = aggregate_events(events)
        assert result[0]["total_operations"] == 1.0
        assert result[0]["field_usage"]["a"]["queried"] == 1
