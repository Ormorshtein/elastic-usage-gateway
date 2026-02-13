"""Tests for gateway.events — fingerprinting, templating, and event building."""

from unittest.mock import patch

from gateway.events import _compute_fingerprint, _templatize, _compute_template, build_event, get_event_sample_config, set_event_sample_config, should_sample_event, get_query_body_config, set_query_body_config
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

    def test_client_ip(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            client_ip="10.2.34.56",
        )
        assert event["client_ip"] == "10.2.34.56"

    def test_client_user_agent(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            client_user_agent="elasticsearch-py/8.12",
        )
        assert event["client_user_agent"] == "elasticsearch-py/8.12"

    def test_client_fields_default_to_none(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
        )
        assert event["client_id"] is None
        assert event["client_ip"] is None
        assert event["client_user_agent"] is None

    def test_all_client_fields_together(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            client_id="order-service",
            client_ip="10.2.34.56",
            client_user_agent="kibana/8.12.2",
        )
        assert event["client_id"] == "order-service"
        assert event["client_ip"] == "10.2.34.56"
        assert event["client_user_agent"] == "kibana/8.12.2"

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

    @patch.object(events_mod._random, "random", return_value=0.5)
    def test_body_excluded_at_boundary(self, mock_rand):
        """random() == sample_rate should be excluded (strict < comparison)."""
        set_query_body_config(sample_rate=0.5)
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200, elapsed_ms=0,
            body=b'{"test": true}',
        )
        assert event["query_body"] is None


class TestEventSampleConfig:
    def setup_method(self):
        set_event_sample_config(sample_rate=1.0)

    def teardown_method(self):
        set_event_sample_config(sample_rate=1.0)

    def test_get_config_default(self):
        config = get_event_sample_config()
        assert config["sample_rate"] == 1.0

    def test_set_sample_rate(self):
        result = set_event_sample_config(sample_rate=0.5)
        assert result["sample_rate"] == 0.5

    def test_sample_rate_clamped_high(self):
        result = set_event_sample_config(sample_rate=2.0)
        assert result["sample_rate"] == 1.0

    def test_sample_rate_clamped_low(self):
        result = set_event_sample_config(sample_rate=-0.5)
        assert result["sample_rate"] == 0.0

    def test_should_sample_at_full_rate(self):
        set_event_sample_config(sample_rate=1.0)
        assert should_sample_event() is True

    def test_should_not_sample_at_zero(self):
        set_event_sample_config(sample_rate=0.0)
        assert should_sample_event() is False

    @patch.object(events_mod._random, "random", return_value=0.3)
    def test_should_sample_when_under_rate(self, mock_rand):
        set_event_sample_config(sample_rate=0.5)
        assert should_sample_event() is True

    @patch.object(events_mod._random, "random", return_value=0.8)
    def test_should_not_sample_when_over_rate(self, mock_rand):
        set_event_sample_config(sample_rate=0.5)
        assert should_sample_event() is False

    @patch.object(events_mod._random, "random", return_value=0.5)
    def test_should_not_sample_at_boundary(self, mock_rand):
        """random() == sample_rate should be excluded (strict < comparison)."""
        set_event_sample_config(sample_rate=0.5)
        assert should_sample_event() is False


class TestTemplatize:
    def test_scalar_string(self):
        assert _templatize("hello") == "?"

    def test_scalar_number(self):
        assert _templatize(42) == "?"

    def test_scalar_bool(self):
        assert _templatize(True) == "?"

    def test_scalar_none(self):
        assert _templatize(None) == "?"

    def test_simple_dict(self):
        assert _templatize({"match": {"title": "laptop"}}) == {"match": {"title": "?"}}

    def test_nested_dict(self):
        obj = {"range": {"price": {"gte": 100, "lte": 500}}}
        assert _templatize(obj) == {"range": {"price": {"gte": "?", "lte": "?"}}}

    def test_list_of_scalars_collapses(self):
        obj = {"terms": {"tags": ["electronics", "laptop", "gaming"]}}
        assert _templatize(obj) == {"terms": {"tags": ["?"]}}

    def test_list_of_scalars_different_lengths_same_template(self):
        assert _templatize(["a", "b"]) == ["?"]
        assert _templatize(["a", "b", "c", "d"]) == ["?"]

    def test_list_of_dicts_preserves_structure(self):
        obj = {"bool": {"must": [
            {"term": {"status": "active"}},
            {"range": {"date": {"gte": "2024-01-01"}}},
        ]}}
        expected = {"bool": {"must": [
            {"term": {"status": "?"}},
            {"range": {"date": {"gte": "?"}}},
        ]}}
        assert _templatize(obj) == expected

    def test_empty_list_stays_empty(self):
        assert _templatize({"tags": []}) == {"tags": []}

    def test_empty_dict_stays_empty(self):
        assert _templatize({}) == {}

    def test_real_world_search_query(self):
        """A realistic Kibana-style search query."""
        query = {
            "query": {
                "bool": {
                    "must": [{"match": {"title": "laptop"}}],
                    "filter": [{"range": {"timestamp": {"gte": "now-24h"}}}],
                }
            },
            "size": 10,
            "sort": [{"timestamp": {"order": "desc"}}],
            "aggs": {
                "by_category": {"terms": {"field": "category", "size": 20}}
            },
        }
        result = _templatize(query)
        assert result["size"] == "?"
        assert result["query"]["bool"]["must"][0] == {"match": {"title": "?"}}
        assert result["query"]["bool"]["filter"][0] == {"range": {"timestamp": {"gte": "?"}}}
        assert result["sort"][0] == {"timestamp": {"order": "?"}}
        assert result["aggs"]["by_category"] == {"terms": {"field": "?", "size": "?"}}


class TestComputeTemplate:
    def test_same_structure_different_values_same_hash(self):
        body_a = b'{"match": {"title": "laptop"}}'
        body_b = b'{"match": {"title": "phone"}}'
        hash_a, _ = _compute_template(body_a)
        hash_b, _ = _compute_template(body_b)
        assert hash_a == hash_b
        assert hash_a is not None

    def test_different_structure_different_hash(self):
        body_a = b'{"match": {"title": "laptop"}}'
        body_b = b'{"term": {"category": "electronics"}}'
        hash_a, _ = _compute_template(body_a)
        hash_b, _ = _compute_template(body_b)
        assert hash_a != hash_b

    def test_key_order_irrelevant(self):
        body_a = b'{"a": 1, "b": 2}'
        body_b = b'{"b": 2, "a": 1}'
        hash_a, _ = _compute_template(body_a)
        hash_b, _ = _compute_template(body_b)
        assert hash_a == hash_b

    def test_list_values_collapse(self):
        body_a = b'{"terms": {"tags": ["a", "b", "c"]}}'
        body_b = b'{"terms": {"tags": ["x", "y"]}}'
        hash_a, _ = _compute_template(body_a)
        hash_b, _ = _compute_template(body_b)
        assert hash_a == hash_b

    def test_returns_hex_string(self):
        hash_val, _ = _compute_template(b'{"query": {"match_all": {}}}')
        assert hash_val is not None
        assert len(hash_val) == 64
        assert all(c in "0123456789abcdef" for c in hash_val)

    def test_text_contains_placeholders(self):
        _, text = _compute_template(b'{"match": {"title": "laptop"}}')
        assert text is not None
        assert '"?"' in text
        assert "laptop" not in text

    def test_text_readable_format(self):
        """Template text should use spaces for readability."""
        _, text = _compute_template(b'{"a": 1}')
        assert ": " in text  # separator with space

    def test_empty_body_returns_none(self):
        h, t = _compute_template(b"")
        assert h is None
        assert t is None

    def test_invalid_json_returns_none(self):
        h, t = _compute_template(b"not json")
        assert h is None
        assert t is None


class TestBuildEventTemplateFields:
    def test_template_fields_populated(self):
        refs = FieldRefs(queried={"title"})
        body = b'{"query": {"match": {"title": "laptop"}}}'
        event = build_event(
            index_name="products", operation="search", field_refs=refs,
            method="POST", path="/products/_search", response_status=200,
            elapsed_ms=42.0, body=body,
        )
        assert event["query_template_hash"] is not None
        assert len(event["query_template_hash"]) == 64
        assert event["query_template_text"] is not None
        assert '"?"' in event["query_template_text"]

    def test_template_fields_none_without_body(self):
        refs = FieldRefs()
        event = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="GET", path="/x/_search", response_status=200, elapsed_ms=0,
        )
        assert event["query_template_hash"] is None
        assert event["query_template_text"] is None

    def test_same_template_for_different_values(self):
        refs = FieldRefs(queried={"title"})
        event_a = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200,
            elapsed_ms=10, body=b'{"match": {"title": "laptop"}}',
        )
        event_b = build_event(
            index_name="x", operation="search", field_refs=refs,
            method="POST", path="/x/_search", response_status=200,
            elapsed_ms=10, body=b'{"match": {"title": "phone"}}',
        )
        assert event_a["query_template_hash"] == event_b["query_template_hash"]
