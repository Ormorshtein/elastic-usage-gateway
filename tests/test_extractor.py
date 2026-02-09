"""Tests for gateway.extractor — path parsing, query DSL extraction, field context."""

import json
import pytest
from gateway.extractor import (
    parse_path,
    extract_from_request,
    extract_fields_from_search,
    extract_fields_from_document,
    FieldRefs,
    _extract_from_bulk,
    _extract_from_msearch,
)


# --- parse_path ---

class TestParsePath:
    def test_simple_search(self):
        indices, op = parse_path("/products/_search")
        assert indices == ["products"]
        assert op == "search"

    def test_multi_index(self):
        indices, op = parse_path("/products,orders/_search")
        assert indices == ["products", "orders"]
        assert op == "search"

    def test_multi_index_with_spaces(self):
        indices, op = parse_path("/products, orders /_search")
        assert indices == ["products", "orders"]
        assert op == "search"

    def test_doc_with_id(self):
        indices, op = parse_path("/products/_doc/abc123")
        assert indices == ["products"]
        assert op == "doc"

    def test_system_endpoint_bulk(self):
        indices, op = parse_path("/_bulk")
        assert indices is None
        assert op == "bulk"

    def test_system_endpoint_cluster(self):
        indices, op = parse_path("/_cluster/health")
        assert indices is None
        assert op == "cluster"

    def test_index_only(self):
        indices, op = parse_path("/products")
        assert indices == ["products"]
        assert op is None

    def test_empty_path(self):
        indices, op = parse_path("/")
        assert indices is None
        assert op is None

    def test_count(self):
        indices, op = parse_path("/logs/_count")
        assert indices == ["logs"]
        assert op == "count"

    def test_index_bulk(self):
        indices, op = parse_path("/products/_bulk")
        assert indices == ["products"]
        assert op == "bulk"


# --- extract_fields_from_search ---

class TestExtractFieldsFromSearch:
    def test_simple_match(self):
        body = {"query": {"match": {"title": "laptop"}}}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title"}
        assert refs.filtered == set()

    def test_bool_must_queried(self):
        body = {
            "query": {
                "bool": {
                    "must": [{"match": {"title": "laptop"}}],
                }
            }
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title"}
        assert refs.filtered == set()

    def test_bool_filter_goes_to_filtered(self):
        body = {
            "query": {
                "bool": {
                    "must": [{"match": {"title": "laptop"}}],
                    "filter": [
                        {"term": {"category": "Electronics"}},
                        {"range": {"price": {"gte": 100}}},
                    ],
                }
            }
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title"}
        assert refs.filtered == {"category", "price"}

    def test_nested_bool_in_filter_stays_filtered(self):
        body = {
            "query": {
                "bool": {
                    "filter": [
                        {"bool": {
                            "must": [{"term": {"status": "active"}}],
                            "should": [{"range": {"score": {"gte": 5}}}],
                        }},
                    ],
                }
            }
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == set()
        assert refs.filtered == {"status", "score"}

    def test_post_filter(self):
        body = {
            "query": {"match": {"title": "laptop"}},
            "post_filter": {"term": {"color": "red"}},
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title"}
        assert refs.filtered == {"color"}

    def test_multi_match(self):
        body = {
            "query": {
                "multi_match": {
                    "query": "laptop",
                    "fields": ["title^2", "description"],
                }
            }
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title", "description"}

    def test_aggregations(self):
        body = {
            "size": 0,
            "aggs": {
                "by_brand": {
                    "terms": {"field": "brand"},
                    "aggs": {
                        "avg_price": {"avg": {"field": "price"}},
                    },
                },
            },
        }
        refs = extract_fields_from_search(body)
        assert refs.aggregated == {"brand", "price"}

    def test_sort(self):
        body = {
            "query": {"match_all": {}},
            "sort": [{"price": "desc"}, {"rating": "asc"}],
        }
        refs = extract_fields_from_search(body)
        assert refs.sorted == {"price", "rating"}

    def test_sort_string(self):
        body = {
            "query": {"match_all": {}},
            "sort": ["price", "rating"],
        }
        refs = extract_fields_from_search(body)
        assert refs.sorted == {"price", "rating"}

    def test_source_list(self):
        body = {
            "query": {"match_all": {}},
            "_source": ["title", "price", "category"],
        }
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"title", "price", "category"}

    def test_source_includes_excludes(self):
        body = {
            "query": {"match_all": {}},
            "_source": {
                "includes": ["title", "price"],
                "excludes": ["internal_field"],
            },
        }
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"title", "price", "internal_field"}

    def test_internal_fields_excluded(self):
        body = {
            "query": {"match_all": {}},
            "sort": ["_score", "_doc", "price"],
        }
        refs = extract_fields_from_search(body)
        assert refs.sorted == {"price"}

    def test_exists_query(self):
        body = {"query": {"exists": {"field": "rating"}}}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"rating"}

    def test_nested_query(self):
        body = {
            "query": {
                "nested": {
                    "path": "reviews",
                    "query": {"match": {"reviews.text": "great"}},
                }
            }
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == {"reviews.text"}

    def test_empty_body(self):
        refs = extract_fields_from_search({})
        assert refs.all_fields == set()


# --- extract_fields_from_document ---

class TestExtractFieldsFromDocument:
    def test_simple_doc(self):
        body = {"title": "Laptop", "price": 999, "category": "Electronics"}
        refs = extract_fields_from_document(body)
        assert refs.written == {"title", "price", "category"}

    def test_ignores_internal(self):
        body = {"title": "Laptop", "_id": "abc"}
        refs = extract_fields_from_document(body)
        assert refs.written == {"title"}


# --- extract_from_request ---

class TestExtractFromRequest:
    def test_search_request(self):
        body = json.dumps({"query": {"match": {"title": "laptop"}}}).encode()
        indices, op, refs = extract_from_request("/products/_search", "POST", body)
        assert indices == ["products"]
        assert op == "search"
        assert refs.queried == {"title"}

    def test_doc_put(self):
        body = json.dumps({"title": "Laptop", "price": 999}).encode()
        indices, op, refs = extract_from_request("/products/_doc/1", "PUT", body)
        assert indices == ["products"]
        assert op == "doc_write"
        assert refs.written == {"title", "price"}

    def test_doc_get(self):
        indices, op, refs = extract_from_request("/products/_doc/1", "GET", b"")
        assert indices == ["products"]
        assert op == "doc_get"
        assert refs.written == set()

    def test_empty_body(self):
        indices, op, refs = extract_from_request("/products/_search", "POST", b"")
        assert indices == ["products"]
        assert op == "search"
        assert refs.all_fields == set()

    def test_invalid_json(self):
        indices, op, refs = extract_from_request("/products/_search", "POST", b"not json")
        assert indices == ["products"]
        assert op == "search"
        assert refs.all_fields == set()

    def test_multi_index_search(self):
        body = json.dumps({"query": {"match": {"title": "x"}}}).encode()
        indices, op, refs = extract_from_request("/a,b,c/_search", "POST", body)
        assert indices == ["a", "b", "c"]
        assert op == "search"

    def test_system_endpoint(self):
        indices, op, refs = extract_from_request("/_cluster/health", "GET", b"")
        assert indices is None
        assert op == "cluster"

    def test_unknown_operation(self):
        indices, op, refs = extract_from_request("/products", "GET", b"")
        assert indices == ["products"]
        assert op == "other"


# --- _extract_from_bulk ---

class TestExtractFromBulk:
    def test_simple_bulk(self):
        lines = [
            '{"index": {"_index": "products"}}',
            '{"title": "Laptop", "price": 999}',
            '{"index": {"_index": "products"}}',
            '{"title": "Phone", "brand": "Samsung"}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_bulk(body, "products")
        assert refs.written == {"title", "price", "brand"}

    def test_delete_action_no_body(self):
        lines = [
            '{"delete": {"_index": "products", "_id": "1"}}',
            '{"index": {"_index": "products"}}',
            '{"title": "Laptop"}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_bulk(body, "products")
        assert refs.written == {"title"}

    def test_empty_body(self):
        refs = _extract_from_bulk(b"", None)
        assert refs.written == set()


# --- FieldRefs ---

class TestFieldRefs:
    def test_to_dict_sorted(self):
        refs = FieldRefs(
            queried={"z_field", "a_field"},
            filtered={"m_field"},
        )
        d = refs.to_dict()
        assert d["queried"] == ["a_field", "z_field"]
        assert d["filtered"] == ["m_field"]

    def test_all_fields(self):
        refs = FieldRefs(
            queried={"a"},
            filtered={"b"},
            aggregated={"c"},
            sorted={"d"},
            sourced={"e"},
            written={"f"},
        )
        assert refs.all_fields == {"a", "b", "c", "d", "e", "f"}


# --- Lookback extraction ---

class TestLookbackExtraction:
    def test_range_with_now_hours(self):
        body = {"query": {"range": {"timestamp": {"gte": "now-24h"}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is not None
        assert refs.lookback.seconds == 86400
        assert refs.lookback.field == "timestamp"

    def test_range_with_now_days(self):
        body = {"query": {"range": {"order_date": {"gte": "now-30d"}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is not None
        assert refs.lookback.seconds == 2592000
        assert refs.lookback.field == "order_date"

    def test_range_with_now_minutes(self):
        body = {"query": {"range": {"timestamp": {"gte": "now-15m"}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback.seconds == 900

    def test_range_with_gt(self):
        body = {"query": {"range": {"timestamp": {"gt": "now-6h"}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback.seconds == 21600

    def test_numeric_range_no_lookback(self):
        body = {"query": {"range": {"price": {"gte": 100, "lte": 500}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is None

    def test_no_range_no_lookback(self):
        body = {"query": {"match": {"title": "laptop"}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is None

    def test_multiple_ranges_takes_max(self):
        body = {"query": {"bool": {"filter": [
            {"range": {"timestamp": {"gte": "now-1h"}}},
            {"range": {"created_at": {"gte": "now-7d"}}},
        ]}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is not None
        assert refs.lookback.seconds == 604800
        assert refs.lookback.field == "created_at"

    def test_empty_body_no_lookback(self):
        refs = extract_fields_from_search({})
        assert refs.lookback is None

    def test_absolute_date_not_parsed(self):
        body = {"query": {"range": {"timestamp": {"gte": "2026-01-01"}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is None

    def test_range_with_lte(self):
        body = {"query": {"range": {"timestamp": {"lte": "now-2h"}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is not None
        assert refs.lookback.seconds == 7200
        assert refs.lookback.field == "timestamp"

    def test_range_with_lt(self):
        body = {"query": {"range": {"timestamp": {"lt": "now-12h"}}}}
        refs = extract_fields_from_search(body)
        assert refs.lookback is not None
        assert refs.lookback.seconds == 43200


# --- Bulk update with doc wrapper ---

class TestBulkUpdateExtraction:
    def test_update_with_doc_wrapper(self):
        lines = [
            '{"update": {"_index": "products", "_id": "1"}}',
            '{"doc": {"title": "Updated Name", "price": 499}}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_bulk(body, "products")
        assert refs.written == {"title", "price"}

    def test_update_with_upsert_wrapper(self):
        lines = [
            '{"update": {"_index": "products", "_id": "1"}}',
            '{"doc": {"title": "Name"}, "upsert": {"title": "Name", "brand": "Acme"}}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_bulk(body, "products")
        assert "title" in refs.written
        assert "brand" in refs.written

    def test_mixed_index_and_update(self):
        lines = [
            '{"index": {"_index": "products"}}',
            '{"title": "Laptop", "price": 999}',
            '{"update": {"_index": "products", "_id": "2"}}',
            '{"doc": {"brand": "Dell"}}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_bulk(body, "products")
        assert refs.written == {"title", "price", "brand"}


# --- update extraction ---

class TestExtractFromUpdate:
    def test_update_with_doc(self):
        body = json.dumps({"doc": {"price": 499, "stock_count": 10}}).encode()
        indices, op, refs = extract_from_request("/products/_update/1", "POST", body)
        assert indices == ["products"]
        assert op == "update"
        assert refs.written == {"price", "stock_count"}

    def test_update_with_doc_and_upsert(self):
        body = json.dumps({
            "doc": {"price": 499},
            "upsert": {"title": "New Product", "price": 499, "category": "electronics"},
        }).encode()
        indices, op, refs = extract_from_request("/products/_update/1", "POST", body)
        assert op == "update"
        assert refs.written == {"price", "title", "category"}

    def test_update_get_method_ignored(self):
        """GET requests to _update path should not extract written fields."""
        body = json.dumps({"doc": {"price": 499}}).encode()
        indices, op, refs = extract_from_request("/products/_update/1", "GET", body)
        assert op == "update"
        assert refs.written == set()

    def test_update_empty_body(self):
        indices, op, refs = extract_from_request("/products/_update/1", "POST", b"")
        assert op == "update"
        assert refs.written == set()


# --- msearch extraction ---

class TestMsearchExtraction:
    def test_simple_msearch(self):
        lines = [
            '{"index": "products"}',
            '{"query": {"match": {"title": "laptop"}}}',
            '{"index": "logs"}',
            '{"query": {"term": {"level": "ERROR"}}}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_msearch(body)
        assert refs.queried == {"title", "level"}

    def test_msearch_with_aggs(self):
        lines = [
            '{"index": "products"}',
            '{"size": 0, "aggs": {"by_brand": {"terms": {"field": "brand"}}}}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_msearch(body)
        assert refs.aggregated == {"brand"}

    def test_msearch_empty_body(self):
        refs = _extract_from_msearch(b"")
        assert refs.all_fields == set()

    def test_msearch_via_extract_from_request(self):
        lines = [
            '{"index": "products"}',
            '{"query": {"match": {"title": "laptop"}}}',
        ]
        body = "\n".join(lines).encode()
        indices, op, refs = extract_from_request("/_msearch", "POST", body)
        assert op == "msearch"
        assert refs.queried == {"title"}
