"""Tests for gateway.mapping_diff — mapping flattening, classification, and diff building."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.mapping_diff import (
    flatten_mapping,
    classify_field,
    build_diff_docs,
    build_usage_aggregation_query,
    _parse_usage_response,
    FIELD_CATEGORIES,
)
import gateway.mapping_diff as diff_mod
import gateway.metadata as metadata_mod


# ---------------------------------------------------------------------------
# TestFlattenMapping — pure function, no I/O
# ---------------------------------------------------------------------------

class TestFlattenMapping:
    """flatten_mapping walks ES mapping properties and produces flat field list."""

    def test_simple_fields(self):
        props = {
            "price": {"type": "float"},
            "category": {"type": "keyword"},
            "created_at": {"type": "date"},
        }
        result = flatten_mapping(props)
        names = {f["field_name"] for f in result}
        assert names == {"price", "category", "created_at"}

    def test_field_types_preserved(self):
        props = {"price": {"type": "float"}}
        result = flatten_mapping(props)
        assert result[0]["mapped_type"] == "float"

    def test_multi_fields(self):
        props = {
            "title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            }
        }
        result = flatten_mapping(props)
        names = {f["field_name"] for f in result}
        assert names == {"title", "title.keyword"}

    def test_nested_objects(self):
        props = {
            "metadata": {
                "properties": {
                    "created_at": {"type": "date"},
                    "author": {"type": "keyword"},
                }
            }
        }
        result = flatten_mapping(props)
        names = {f["field_name"] for f in result}
        assert names == {"metadata.created_at", "metadata.author"}

    def test_deeply_nested_objects(self):
        props = {
            "a": {
                "properties": {
                    "b": {
                        "properties": {
                            "c": {"type": "keyword"},
                        }
                    }
                }
            }
        }
        result = flatten_mapping(props)
        assert result[0]["field_name"] == "a.b.c"
        assert result[0]["mapped_type"] == "keyword"

    def test_nested_type_with_sub_properties(self):
        """An ES nested type has both a type and sub-properties."""
        props = {
            "address": {
                "type": "nested",
                "properties": {
                    "city": {"type": "keyword"},
                    "zip": {"type": "keyword"},
                }
            }
        }
        result = flatten_mapping(props)
        names = {f["field_name"] for f in result}
        assert "address" in names
        assert "address.city" in names
        assert "address.zip" in names

    def test_index_false_detected(self):
        props = {"notes": {"type": "text", "index": False}}
        result = flatten_mapping(props)
        assert result[0]["is_indexed"] is False

    def test_index_true_by_default(self):
        props = {"category": {"type": "keyword"}}
        result = flatten_mapping(props)
        assert result[0]["is_indexed"] is True

    def test_doc_values_false_for_text(self):
        props = {"description": {"type": "text"}}
        result = flatten_mapping(props)
        assert result[0]["has_doc_values"] is False

    def test_doc_values_true_for_keyword(self):
        props = {"category": {"type": "keyword"}}
        result = flatten_mapping(props)
        assert result[0]["has_doc_values"] is True

    def test_doc_values_true_for_numeric(self):
        props = {"price": {"type": "float"}}
        result = flatten_mapping(props)
        assert result[0]["has_doc_values"] is True

    def test_explicit_doc_values_override(self):
        props = {"tag": {"type": "keyword", "doc_values": False}}
        result = flatten_mapping(props)
        assert result[0]["has_doc_values"] is False

    def test_empty_properties(self):
        assert flatten_mapping({}) == []

    def test_multi_field_inherits_correct_defaults(self):
        props = {
            "title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            }
        }
        result = flatten_mapping(props)
        kw = next(f for f in result if f["field_name"] == "title.keyword")
        txt = next(f for f in result if f["field_name"] == "title")
        assert kw["has_doc_values"] is True
        assert txt["has_doc_values"] is False

    def test_products_mapping_realistic(self):
        """Full products mapping from seed.py."""
        props = {
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "description": {"type": "text"},
            "category": {"type": "keyword"},
            "brand": {"type": "keyword"},
            "price": {"type": "float"},
            "rating": {"type": "float"},
            "stock_count": {"type": "integer"},
            "created_at": {"type": "date"},
            "internal_sku": {"type": "keyword"},
            "legacy_supplier_code": {"type": "keyword"},
            "tags": {"type": "keyword"},
            "subcategory": {"type": "keyword"},
        }
        result = flatten_mapping(props)
        names = {f["field_name"] for f in result}
        # 12 top-level + 1 multi-field (title.keyword) = 13
        assert len(result) == 13
        assert "title" in names
        assert "title.keyword" in names
        assert "price" in names
        assert "legacy_supplier_code" in names

    def test_object_without_type(self):
        """Pure object (no type key) with nested properties."""
        props = {
            "geo": {
                "properties": {
                    "lat": {"type": "float"},
                    "lon": {"type": "float"},
                }
            }
        }
        result = flatten_mapping(props)
        names = {f["field_name"] for f in result}
        # "geo" itself should NOT appear (no type), only sub-fields
        assert "geo" not in names
        assert "geo.lat" in names
        assert "geo.lon" in names


# ---------------------------------------------------------------------------
# TestClassifyField — pure function
# ---------------------------------------------------------------------------

class TestClassifyField:
    """classify_field categorizes a field based on its usage pattern."""

    def _zero_usage(self) -> dict:
        usage = {f"count_{cat}": 0 for cat in FIELD_CATEGORIES}
        usage.update({f"last_seen_{cat}": None for cat in FIELD_CATEGORIES})
        return usage

    def test_unused_when_none(self):
        assert classify_field(None) == "unused"

    def test_unused_when_all_zero(self):
        assert classify_field(self._zero_usage()) == "unused"

    def test_active_when_queried(self):
        u = self._zero_usage()
        u["count_queried"] = 10
        assert classify_field(u) == "active"

    def test_active_when_filtered(self):
        u = self._zero_usage()
        u["count_filtered"] = 5
        assert classify_field(u) == "active"

    def test_active_when_aggregated(self):
        u = self._zero_usage()
        u["count_aggregated"] = 3
        assert classify_field(u) == "active"

    def test_active_when_sorted(self):
        u = self._zero_usage()
        u["count_sorted"] = 1
        assert classify_field(u) == "active"

    def test_sourced_only(self):
        u = self._zero_usage()
        u["count_sourced"] = 15
        assert classify_field(u) == "sourced_only"

    def test_write_only(self):
        u = self._zero_usage()
        u["count_written"] = 20
        assert classify_field(u) == "write_only"

    def test_active_beats_sourced(self):
        u = self._zero_usage()
        u["count_queried"] = 5
        u["count_sourced"] = 50
        assert classify_field(u) == "active"

    def test_active_beats_written(self):
        u = self._zero_usage()
        u["count_filtered"] = 3
        u["count_written"] = 100
        assert classify_field(u) == "active"

    def test_sourced_beats_written(self):
        u = self._zero_usage()
        u["count_sourced"] = 10
        u["count_written"] = 5
        assert classify_field(u) == "sourced_only"


# ---------------------------------------------------------------------------
# TestBuildUsageAggregationQuery — pure function
# ---------------------------------------------------------------------------

class TestBuildUsageAggregationQuery:
    """build_usage_aggregation_query generates correct ES query."""

    def test_has_six_aggs(self):
        query = build_usage_aggregation_query("products", 168)
        assert len(query["aggs"]) == 6
        for cat in FIELD_CATEGORIES:
            assert f"usage_{cat}" in query["aggs"]

    def test_filters_by_index_group(self):
        query = build_usage_aggregation_query("products", 168)
        filters = query["query"]["bool"]["filter"]
        assert {"term": {"index_group": "products"}} in filters

    def test_filters_by_lookback(self):
        query = build_usage_aggregation_query("logs", 24)
        filters = query["query"]["bool"]["filter"]
        assert {"range": {"timestamp": {"gte": "now-24h"}}} in filters

    def test_size_zero(self):
        query = build_usage_aggregation_query("products", 168)
        assert query["size"] == 0

    def test_each_agg_has_last_seen_sub_agg(self):
        query = build_usage_aggregation_query("products", 168)
        for cat in FIELD_CATEGORIES:
            agg = query["aggs"][f"usage_{cat}"]
            assert "last_seen" in agg["aggs"]
            assert agg["aggs"]["last_seen"] == {"max": {"field": "timestamp"}}

    def test_terms_size_is_generous(self):
        query = build_usage_aggregation_query("products", 168)
        for cat in FIELD_CATEGORIES:
            size = query["aggs"][f"usage_{cat}"]["terms"]["size"]
            assert size >= 200


# ---------------------------------------------------------------------------
# TestParseUsageResponse — pure function
# ---------------------------------------------------------------------------

class TestParseUsageResponse:
    """_parse_usage_response extracts per-field counts and timestamps."""

    def _empty_aggs(self) -> dict:
        return {
            "aggregations": {
                f"usage_{cat}": {"buckets": []}
                for cat in FIELD_CATEGORIES
            }
        }

    def test_empty_response(self):
        result = _parse_usage_response(self._empty_aggs())
        assert result == {}

    def test_single_field_single_category(self):
        resp = self._empty_aggs()
        resp["aggregations"]["usage_queried"] = {
            "buckets": [
                {"key": "title", "doc_count": 42,
                 "last_seen": {"value_as_string": "2026-02-13T10:00:00Z"}}
            ]
        }
        result = _parse_usage_response(resp)
        assert "title" in result
        assert result["title"]["count_queried"] == 42
        assert result["title"]["last_seen_queried"] == "2026-02-13T10:00:00Z"
        assert result["title"]["count_filtered"] == 0
        assert result["title"]["last_seen_filtered"] is None

    def test_field_across_multiple_categories(self):
        resp = self._empty_aggs()
        resp["aggregations"]["usage_queried"] = {
            "buckets": [
                {"key": "price", "doc_count": 10,
                 "last_seen": {"value_as_string": "2026-02-13T10:00:00Z"}}
            ]
        }
        resp["aggregations"]["usage_filtered"] = {
            "buckets": [
                {"key": "price", "doc_count": 25,
                 "last_seen": {"value_as_string": "2026-02-13T12:00:00Z"}}
            ]
        }
        resp["aggregations"]["usage_sorted"] = {
            "buckets": [
                {"key": "price", "doc_count": 5,
                 "last_seen": {"value_as_string": "2026-02-13T08:00:00Z"}}
            ]
        }
        result = _parse_usage_response(resp)
        assert result["price"]["count_queried"] == 10
        assert result["price"]["count_filtered"] == 25
        assert result["price"]["count_sorted"] == 5
        assert result["price"]["count_aggregated"] == 0

    def test_multiple_fields(self):
        resp = self._empty_aggs()
        resp["aggregations"]["usage_queried"] = {
            "buckets": [
                {"key": "title", "doc_count": 42,
                 "last_seen": {"value_as_string": "2026-02-13T10:00:00Z"}},
                {"key": "brand", "doc_count": 15,
                 "last_seen": {"value_as_string": "2026-02-13T09:00:00Z"}},
            ]
        }
        result = _parse_usage_response(resp)
        assert len(result) == 2
        assert result["title"]["count_queried"] == 42
        assert result["brand"]["count_queried"] == 15


# ---------------------------------------------------------------------------
# TestBuildDiffDocs — pure function
# ---------------------------------------------------------------------------

class TestBuildDiffDocs:
    """build_diff_docs merges mapping fields with usage data."""

    def test_unused_field(self):
        mapping_fields = [
            {"field_name": "legacy_sku", "mapped_type": "keyword",
             "is_indexed": True, "has_doc_values": True}
        ]
        docs = build_diff_docs("products", mapping_fields, {}, "2026-02-13T12:00:00Z")
        assert len(docs) == 1
        doc = docs[0]
        assert doc["field_name"] == "legacy_sku"
        assert doc["classification"] == "unused"
        assert doc["total_references"] == 0
        assert doc["last_seen"] is None
        assert doc["count_queried"] == 0
        assert doc["count_written"] == 0

    def test_active_field(self):
        mapping_fields = [
            {"field_name": "price", "mapped_type": "float",
             "is_indexed": True, "has_doc_values": True}
        ]
        field_usage = {
            "price": {
                "count_queried": 10, "last_seen_queried": "2026-02-13T10:00:00Z",
                "count_filtered": 25, "last_seen_filtered": "2026-02-13T12:00:00Z",
                "count_aggregated": 0, "last_seen_aggregated": None,
                "count_sorted": 5, "last_seen_sorted": "2026-02-13T08:00:00Z",
                "count_sourced": 50, "last_seen_sourced": "2026-02-13T11:00:00Z",
                "count_written": 0, "last_seen_written": None,
            }
        }
        docs = build_diff_docs("products", mapping_fields, field_usage, "2026-02-13T12:00:00Z")
        doc = docs[0]
        assert doc["classification"] == "active"
        assert doc["total_references"] == 90
        assert doc["last_seen"] == "2026-02-13T12:00:00Z"
        assert doc["count_queried"] == 10
        assert doc["count_filtered"] == 25
        assert doc["index_group"] == "products"

    def test_multiple_fields_mixed_classification(self):
        mapping_fields = [
            {"field_name": "title", "mapped_type": "text",
             "is_indexed": True, "has_doc_values": False},
            {"field_name": "internal_sku", "mapped_type": "keyword",
             "is_indexed": True, "has_doc_values": True},
        ]
        field_usage = {
            "title": {
                "count_queried": 100, "last_seen_queried": "2026-02-13T12:00:00Z",
                "count_filtered": 0, "last_seen_filtered": None,
                "count_aggregated": 0, "last_seen_aggregated": None,
                "count_sorted": 0, "last_seen_sorted": None,
                "count_sourced": 0, "last_seen_sourced": None,
                "count_written": 0, "last_seen_written": None,
            },
        }
        docs = build_diff_docs("products", mapping_fields, field_usage, "2026-02-13T12:00:00Z")
        assert len(docs) == 2
        title_doc = next(d for d in docs if d["field_name"] == "title")
        sku_doc = next(d for d in docs if d["field_name"] == "internal_sku")
        assert title_doc["classification"] == "active"
        assert sku_doc["classification"] == "unused"

    def test_timestamp_propagated(self):
        mapping_fields = [
            {"field_name": "x", "mapped_type": "keyword",
             "is_indexed": True, "has_doc_values": True}
        ]
        docs = build_diff_docs("test", mapping_fields, {}, "2026-02-13T15:30:00Z")
        assert docs[0]["timestamp"] == "2026-02-13T15:30:00Z"

    def test_empty_mapping(self):
        docs = build_diff_docs("empty", [], {}, "2026-02-13T12:00:00Z")
        assert docs == []

    def test_last_seen_is_max_across_categories(self):
        mapping_fields = [
            {"field_name": "f", "mapped_type": "keyword",
             "is_indexed": True, "has_doc_values": True}
        ]
        field_usage = {
            "f": {
                "count_queried": 1, "last_seen_queried": "2026-02-10T10:00:00Z",
                "count_filtered": 0, "last_seen_filtered": None,
                "count_aggregated": 0, "last_seen_aggregated": None,
                "count_sorted": 0, "last_seen_sorted": None,
                "count_sourced": 1, "last_seen_sourced": "2026-02-13T12:00:00Z",
                "count_written": 0, "last_seen_written": None,
            }
        }
        docs = build_diff_docs("test", mapping_fields, field_usage, "2026-02-13T12:00:00Z")
        assert docs[0]["last_seen"] == "2026-02-13T12:00:00Z"

    def test_write_only_field(self):
        mapping_fields = [
            {"field_name": "data", "mapped_type": "keyword",
             "is_indexed": True, "has_doc_values": True}
        ]
        field_usage = {
            "data": {
                "count_queried": 0, "last_seen_queried": None,
                "count_filtered": 0, "last_seen_filtered": None,
                "count_aggregated": 0, "last_seen_aggregated": None,
                "count_sorted": 0, "last_seen_sorted": None,
                "count_sourced": 0, "last_seen_sourced": None,
                "count_written": 50, "last_seen_written": "2026-02-13T12:00:00Z",
            }
        }
        docs = build_diff_docs("test", mapping_fields, field_usage, "2026-02-13T12:00:00Z")
        assert docs[0]["classification"] == "write_only"
        assert docs[0]["total_references"] == 50

    def test_sourced_only_field(self):
        mapping_fields = [
            {"field_name": "title", "mapped_type": "text",
             "is_indexed": True, "has_doc_values": False}
        ]
        field_usage = {
            "title": {
                "count_queried": 0, "last_seen_queried": None,
                "count_filtered": 0, "last_seen_filtered": None,
                "count_aggregated": 0, "last_seen_aggregated": None,
                "count_sorted": 0, "last_seen_sorted": None,
                "count_sourced": 30, "last_seen_sourced": "2026-02-12T10:00:00Z",
                "count_written": 0, "last_seen_written": None,
            }
        }
        docs = build_diff_docs("test", mapping_fields, field_usage, "2026-02-13T12:00:00Z")
        assert docs[0]["classification"] == "sourced_only"


# ---------------------------------------------------------------------------
# TestRefreshIntegration — mocked ES responses
# ---------------------------------------------------------------------------

class TestRefreshIntegration:
    """Integration tests: mock ES responses, verify full diff pipeline."""

    def setup_method(self):
        metadata_mod._index_to_group = {"products": "products"}
        metadata_mod._groups = {"products": {"products"}}

    def teardown_method(self):
        metadata_mod._index_to_group = {}
        metadata_mod._groups = {}

    @pytest.mark.asyncio
    async def test_refresh_completes_without_error(self):
        """Full pipeline: fetch mapping + fetch usage + write diff."""
        mapping_resp = MagicMock()
        mapping_resp.status_code = 200
        mapping_resp.json.return_value = {
            "products": {
                "mappings": {
                    "properties": {
                        "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "price": {"type": "float"},
                    }
                }
            }
        }

        usage_resp = MagicMock()
        usage_resp.status_code = 200
        usage_resp.json.return_value = {
            "aggregations": {
                f"usage_{cat}": {"buckets": []}
                for cat in FIELD_CATEGORIES
            }
        }
        usage_resp.json.return_value["aggregations"]["usage_queried"] = {
            "buckets": [
                {"key": "price", "doc_count": 10,
                 "last_seen": {"value_as_string": "2026-02-13T12:00:00Z"}}
            ]
        }

        delete_resp = MagicMock()
        delete_resp.status_code = 200

        bulk_resp = MagicMock()
        bulk_resp.status_code = 200
        bulk_resp.json.return_value = {"errors": False, "items": [
            {"index": {"status": 201}} for _ in range(3)
        ]}

        async def mock_get(url, **kwargs):
            return mapping_resp

        async def mock_post(url, **kwargs):
            if "_search" in url:
                return usage_resp
            if "_delete_by_query" in url:
                return delete_resp
            return bulk_resp

        with patch.object(diff_mod._client, "get", side_effect=mock_get):
            with patch.object(diff_mod._client, "post", side_effect=mock_post):
                # Should complete without raising
                await diff_mod.refresh()

    @pytest.mark.asyncio
    async def test_skips_system_indices(self):
        """Index groups starting with '.' are skipped."""
        metadata_mod._groups = {
            ".usage-events": {".usage-events"},
            "products": {"products"},
        }
        metadata_mod._index_to_group = {
            "products": "products",
            ".usage-events": ".usage-events",
        }

        mapping_resp = MagicMock()
        mapping_resp.status_code = 200
        mapping_resp.json.return_value = {
            "products": {
                "mappings": {
                    "properties": {"x": {"type": "keyword"}}
                }
            }
        }

        usage_resp = MagicMock()
        usage_resp.status_code = 200
        usage_resp.json.return_value = {
            "aggregations": {
                f"usage_{cat}": {"buckets": []}
                for cat in FIELD_CATEGORIES
            }
        }

        bulk_resp = MagicMock()
        bulk_resp.status_code = 200
        bulk_resp.json.return_value = {"errors": False, "items": [
            {"index": {"status": 201}}
        ]}

        get_calls = []

        async def mock_get(url, **kwargs):
            get_calls.append(url)
            return mapping_resp

        async def mock_post(url, **kwargs):
            if "_search" in url:
                return usage_resp
            return bulk_resp

        with patch.object(diff_mod._client, "get", side_effect=mock_get):
            with patch.object(diff_mod._client, "post", side_effect=mock_post):
                await diff_mod.refresh()

        # _mapping should only be fetched for products, never for .usage-events
        mapping_calls = [c for c in get_calls if "_mapping" in c]
        for call in mapping_calls:
            assert ".usage-events" not in call

    @pytest.mark.asyncio
    async def test_handles_mapping_fetch_failure(self):
        """If mapping fetch fails, group is skipped gracefully."""
        fail_resp = MagicMock()
        fail_resp.status_code = 404

        async def mock_get(url, **kwargs):
            return fail_resp

        with patch.object(diff_mod._client, "get", side_effect=mock_get):
            # Should not raise
            await diff_mod.refresh()
