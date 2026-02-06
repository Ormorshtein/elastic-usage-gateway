"""Tests for gateway.events — fingerprinting and event building."""

from unittest.mock import patch

from gateway.events import _compute_fingerprint, build_event, get_query_body_config, set_query_body_config
from gateway.extractor import FieldRefs
import gateway.events as events_mod


class TestComputeFingerprint:
    def test_same_body_same_hash(self):
        body = b'{"query": {"match": {"title": "laptop"}}}'
        assert _compute_fingerprint(body) == _compute_fingerprint(body)

    def test_key_order_irrelevant(self):
        body_a = b'{"a": 1, "b": 2}'
        body_b = b'{"b": 2, "a": 1}'
        assert _compute_fingerprint(body_a) == _compute_fingerprint(body_b)

    def test_different_values_different_hash(self):
        body_a = b'{"query": "laptop"}'
        body_b = b'{"query": "phone"}'
        assert _compute_fingerprint(body_a) != _compute_fingerprint(body_b)

    def test_empty_returns_none(self):
        assert _compute_fingerprint(b"") is None

    def test_none_returns_none(self):
        assert _compute_fingerprint(None) is None

    def test_invalid_json_returns_none(self):
        assert _compute_fingerprint(b"not json") is None

    def test_returns_hex_string(self):
        fp = _compute_fingerprint(b'{"x": 1}')
        assert fp is not None
        assert len(fp) == 64  # SHA-256 hex digest
        assert all(c in "0123456789abcdef" for c in fp)


class TestBuildEvent:
    def test_basic_fields(self):
        refs = FieldRefs(queried={"title"})
        event = build_event(
            index_name="products",
            operation="search",
            field_refs=refs,
            method="POST",
            path="/products/_search",
            response_status=200,
            elapsed_ms=42.5,
        )
        assert event["index"] == "products"
        assert event["operation"] == "search"
        assert event["http_method"] == "POST"
        assert event["response_status"] == 200
        assert event["response_time_ms"] == 42.5
        assert event["fields"]["queried"] == ["title"]
        assert "timestamp" in event

    def test_language_default(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
        )
        assert event["language"] == "dsl"

    def test_language_custom(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            language="sql",
        )
        assert event["language"] == "sql"

    def test_fingerprint_computed(self):
        refs = FieldRefs()
        body = b'{"query": {"match_all": {}}}'
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=body,
        )
        assert event["query_fingerprint"] is not None
        assert len(event["query_fingerprint"]) == 64

    def test_fingerprint_none_without_body(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
        )
        assert event["query_fingerprint"] is None

    def test_none_index_becomes_unknown(self):
        refs = FieldRefs()
        event = build_event(
            index_name=None, operation="bulk", field_refs=refs,
            method="POST", path="/_bulk", response_status=200, elapsed_ms=0,
        )
        assert event["index"] == "_unknown"

    def test_client_id(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            client_id="my-app",
        )
        assert event["client_id"] == "my-app"

    def test_index_group_explicit(self):
        refs = FieldRefs()
        event = build_event(
            index_name="logs-2026.02.04", operation="search", field_refs=refs,
            method="POST", path="/logs/_search", response_status=200, elapsed_ms=0,
            index_group="logs",
        )
        assert event["index_group"] == "logs"
        assert event["index"] == "logs-2026.02.04"

    def test_index_group_defaults_to_index(self):
        refs = FieldRefs()
        event = build_event(
            index_name="products", operation="search", field_refs=refs,
            method="POST", path="/products/_search", response_status=200, elapsed_ms=0,
        )
        assert event["index_group"] == "products"

    def test_index_group_none_index_defaults_to_unknown(self):
        refs = FieldRefs()
        event = build_event(
            index_name=None, operation="bulk", field_refs=refs,
            method="POST", path="/_bulk", response_status=200, elapsed_ms=0,
        )
        assert event["index_group"] == "_unknown"

    def test_lookback_fields_present(self):
        from gateway.extractor import LookbackInfo
        refs = FieldRefs(lookback=LookbackInfo(seconds=86400, field="timestamp", label="24h"))
        event = build_event(
            index_name="logs", operation="search", field_refs=refs,
            method="POST", path="/logs/_search", response_status=200, elapsed_ms=10,
        )
        assert event["lookback_seconds"] == 86400
        assert event["lookback_field"] == "timestamp"
        assert event["lookback_label"] == "24h"

    def test_lookback_null_when_absent(self):
        refs = FieldRefs()
        event = build_event(
            index_name="products", operation="search", field_refs=refs,
            method="POST", path="/products/_search", response_status=200, elapsed_ms=10,
        )
        assert event["lookback_seconds"] is None
        assert event["lookback_field"] is None
        assert event["lookback_label"] is None

    def test_query_body_stored(self):
        refs = FieldRefs()
        body = b'{"query": {"match_all": {}}}'
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=body,
        )
        assert event["query_body"] == '{"query": {"match_all": {}}}'

    def test_query_body_none_without_body(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
        )
        assert event["query_body"] is None

    def test_query_body_truncated(self):
        refs = FieldRefs()
        body = b"x" * 5000
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=body,
        )
        assert len(event["query_body"]) == 4096


class TestQueryBodyConfig:
    def setup_method(self):
        """Reset to defaults before each test."""
        set_query_body_config(enabled=True, sample_rate=1.0)

    def teardown_method(self):
        """Reset to defaults after each test."""
        set_query_body_config(enabled=True, sample_rate=1.0)

    def test_get_config_defaults(self):
        config = get_query_body_config()
        assert config["enabled"] is True
        assert config["sample_rate"] == 1.0

    def test_set_enabled_false(self):
        result = set_query_body_config(enabled=False)
        assert result["enabled"] is False
        assert result["sample_rate"] == 1.0

    def test_set_sample_rate(self):
        result = set_query_body_config(sample_rate=0.5)
        assert result["sample_rate"] == 0.5
        assert result["enabled"] is True

    def test_sample_rate_clamped(self):
        result = set_query_body_config(sample_rate=1.5)
        assert result["sample_rate"] == 1.0
        result = set_query_body_config(sample_rate=-0.5)
        assert result["sample_rate"] == 0.0

    def test_body_excluded_when_disabled(self):
        set_query_body_config(enabled=False)
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=b'{"query": {"match_all": {}}}',
        )
        assert event["query_body"] is None

    def test_body_excluded_when_rate_zero(self):
        set_query_body_config(sample_rate=0.0)
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=b'{"query": {"match_all": {}}}',
        )
        assert event["query_body"] is None

    @patch.object(events_mod._random, "random", return_value=0.3)
    def test_body_included_when_sampled(self, mock_rand):
        set_query_body_config(sample_rate=0.5)
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=b'{"test": true}',
        )
        assert event["query_body"] == '{"test": true}'

    @patch.object(events_mod._random, "random", return_value=0.8)
    def test_body_excluded_when_not_sampled(self, mock_rand):
        set_query_body_config(sample_rate=0.5)
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=b'{"test": true}',
        )
        assert event["query_body"] is None
