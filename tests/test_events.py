"""Tests for gateway.events — fingerprinting and event building."""

from gateway.events import _compute_fingerprint, build_event
from gateway.extractor import FieldRefs


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
