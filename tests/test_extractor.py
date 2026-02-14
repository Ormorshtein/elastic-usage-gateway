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


# --- Gap 1: _async_search ---

class TestAsyncSearch:
    def test_async_search_extracts_like_search(self):
        body = json.dumps({
            "query": {"bool": {
                "must": [{"match": {"title": "laptop"}}],
                "filter": [{"term": {"category": "electronics"}}],
            }},
            "sort": [{"price": "desc"}],
        }).encode()
        indices, op, refs = extract_from_request("/products/_async_search", "POST", body)
        assert indices == ["products"]
        assert op == "async_search"
        assert refs.queried == {"title"}
        assert refs.filtered == {"category"}
        assert refs.sorted == {"price"}

    def test_async_search_empty_body(self):
        indices, op, refs = extract_from_request("/products/_async_search", "POST", b"")
        assert op == "async_search"
        assert refs.all_fields == set()


# --- Gap 2: docvalue_fields ---

class TestDocvalueFields:
    def test_string_format(self):
        body = {"docvalue_fields": ["timestamp", "status_code"]}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"timestamp", "status_code"}

    def test_object_format(self):
        body = {"docvalue_fields": [
            {"field": "timestamp", "format": "date_time"},
            {"field": "level", "format": "use_field_mapping"},
        ]}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"timestamp", "level"}

    def test_mixed_format(self):
        body = {"docvalue_fields": [
            "status_code",
            {"field": "timestamp", "format": "date_time"},
        ]}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"status_code", "timestamp"}


# --- Gap 3: highlight ---

class TestHighlight:
    def test_simple_highlight(self):
        body = {"highlight": {"fields": {"title": {}, "description": {}}}}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title", "description"}

    def test_highlight_with_config(self):
        body = {"highlight": {"fields": {
            "title": {"fragment_size": 100},
            "body": {"number_of_fragments": 3, "fragment_size": 200},
        }}}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title", "body"}

    def test_highlight_combined_with_query(self):
        body = {
            "query": {"match": {"title": "laptop"}},
            "highlight": {"fields": {"title": {}, "description": {}}},
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title", "description"}


# --- Gap 4: _update_by_query / _delete_by_query ---

class TestUpdateDeleteByQuery:
    def test_update_by_query(self):
        body = json.dumps({
            "query": {"term": {"status": "old"}},
        }).encode()
        indices, op, refs = extract_from_request("/products/_update_by_query", "POST", body)
        assert indices == ["products"]
        assert op == "update_by_query"
        assert refs.queried == {"status"}

    def test_delete_by_query(self):
        body = json.dumps({
            "query": {"range": {"created_at": {"lte": "now-90d"}}},
        }).encode()
        indices, op, refs = extract_from_request("/logs/_delete_by_query", "POST", body)
        assert indices == ["logs"]
        assert op == "delete_by_query"
        assert refs.queried == {"created_at"}

    def test_update_by_query_with_bool(self):
        body = json.dumps({
            "query": {"bool": {
                "filter": [{"term": {"status": "inactive"}}],
            }},
        }).encode()
        indices, op, refs = extract_from_request("/users/_update_by_query", "POST", body)
        assert refs.filtered == {"status"}
        assert refs.queried == set()


# --- Gap 5: stored_fields ---

class TestStoredFields:
    def test_stored_fields_list(self):
        body = {"stored_fields": ["title", "price", "category"]}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"title", "price", "category"}

    def test_stored_fields_ignores_internal(self):
        body = {"stored_fields": ["title", "_routing"]}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"title"}


# --- Gap 6: suggesters ---

class TestSuggesters:
    def test_completion_suggester(self):
        body = {"suggest": {
            "title-suggest": {
                "text": "lapt",
                "completion": {"field": "title.suggest", "fuzzy": {"fuzziness": "auto"}},
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title.suggest"}

    def test_term_suggester(self):
        body = {"suggest": {
            "spell-check": {
                "text": "laptpo",
                "term": {"field": "title"},
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title"}

    def test_phrase_suggester(self):
        body = {"suggest": {
            "phrase-suggest": {
                "text": "laptpo chargr",
                "phrase": {"field": "title"},
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title"}

    def test_multiple_suggesters(self):
        body = {"suggest": {
            "autocomplete": {
                "text": "lap",
                "completion": {"field": "title.suggest"},
            },
            "did-you-mean": {
                "text": "laptpo",
                "term": {"field": "title"},
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title.suggest", "title"}


# --- Gap 7: field collapsing ---

class TestFieldCollapsing:
    def test_simple_collapse(self):
        body = {"query": {"match_all": {}}, "collapse": {"field": "brand"}}
        refs = extract_fields_from_search(body)
        assert refs.filtered == {"brand"}

    def test_collapse_combined_with_filter(self):
        body = {
            "query": {"bool": {
                "filter": [{"term": {"category": "electronics"}}],
            }},
            "collapse": {"field": "brand"},
        }
        refs = extract_fields_from_search(body)
        assert refs.filtered == {"category", "brand"}


# --- Gap 8: composite agg sources bug ---

class TestCompositeAggSources:
    def test_composite_terms_and_date_histogram(self):
        body = {"aggs": {
            "my_composite": {
                "composite": {
                    "sources": [
                        {"category_src": {"terms": {"field": "category"}}},
                        {"date_bucket": {"date_histogram": {"field": "order_date", "calendar_interval": "month"}}},
                    ],
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.aggregated == {"category", "order_date"}

    def test_composite_with_sub_aggs(self):
        body = {"aggs": {
            "my_composite": {
                "composite": {
                    "sources": [
                        {"brand_src": {"terms": {"field": "brand"}}},
                    ],
                },
                "aggs": {
                    "avg_price": {"avg": {"field": "price"}},
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.aggregated == {"brand", "price"}


# --- Gap 9: filter/filters agg queries bug ---

class TestFilterAggQueries:
    def test_filter_agg_extracts_query_fields(self):
        body = {"aggs": {
            "active_products": {
                "filter": {"term": {"status": "active"}},
                "aggs": {
                    "avg_price": {"avg": {"field": "price"}},
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.filtered == {"status"}
        assert refs.aggregated == {"price"}

    def test_filters_agg_extracts_named_queries(self):
        body = {"aggs": {
            "messages": {
                "filters": {
                    "filters": {
                        "errors": {"term": {"level": "error"}},
                        "warnings": {"term": {"level": "warning"}},
                    },
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.filtered == {"level"}

    def test_filter_agg_with_bool_query(self):
        body = {"aggs": {
            "recent_active": {
                "filter": {"bool": {
                    "must": [{"term": {"status": "active"}}],
                    "filter": [{"range": {"created_at": {"gte": "now-7d"}}}],
                }},
                "aggs": {
                    "by_category": {"terms": {"field": "category"}},
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.filtered == {"status", "created_at"}
        assert refs.aggregated == {"category"}


# --- Deliverable 7: Painless script extraction ---

class TestScriptFields:
    """script_fields — computed columns via Painless scripts."""

    def test_single_quote_doc_access(self):
        body = {"script_fields": {
            "total": {"script": {"source": "doc['price'].value * doc['quantity'].value"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"price", "quantity"}

    def test_double_quote_doc_access(self):
        body = {"script_fields": {
            "total": {"script": {"source": 'doc["price"].value * doc["quantity"].value'}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"price", "quantity"}

    def test_short_form_string_script(self):
        body = {"script_fields": {
            "margin": {"script": "doc['revenue'].value - doc['cost'].value"},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"revenue", "cost"}

    def test_ctx_source_access(self):
        body = {"script_fields": {
            "full_name": {"script": {"source": "ctx._source.first_name + ' ' + ctx._source.last_name"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"first_name", "last_name"}

    def test_mustache_script_skipped(self):
        body = {"script_fields": {
            "computed": {"script": {"lang": "mustache", "source": "{{field}}"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == set()

    def test_explicit_painless_lang(self):
        body = {"script_fields": {
            "val": {"script": {"lang": "painless", "source": "doc['price'].value"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"price"}

    def test_combined_with_regular_source(self):
        body = {
            "_source": ["title"],
            "script_fields": {
                "margin": {"script": {"source": "doc['price'].value * 0.1"}},
            },
        }
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"title", "price"}


class TestRuntimeMappings:
    """runtime_mappings — virtual fields defined at query time."""

    def test_basic_runtime_mapping(self):
        body = {"runtime_mappings": {
            "day_of_week": {
                "type": "keyword",
                "script": {"source": "emit(doc['@timestamp'].value.dayOfWeekEnum.name())"},
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"@timestamp"}

    def test_multiple_fields_in_runtime_mapping(self):
        body = {"runtime_mappings": {
            "profit": {
                "type": "double",
                "script": {"source": "emit(doc['revenue'].value - doc['cost'].value)"},
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"revenue", "cost"}

    def test_runtime_mapping_string_script(self):
        body = {"runtime_mappings": {
            "upper_name": {
                "type": "keyword",
                "script": "emit(doc['name'].value.toUpperCase())",
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"name"}


class TestFunctionScore:
    """function_score — custom scoring with scripts and field references."""

    def test_script_score(self):
        body = {"query": {"function_score": {
            "query": {"match": {"title": "laptop"}},
            "functions": [
                {"script_score": {"script": {"source": "doc['popularity'].value * 2"}}},
            ],
        }}}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title", "popularity"}

    def test_field_value_factor(self):
        body = {"query": {"function_score": {
            "query": {"match_all": {}},
            "functions": [
                {"field_value_factor": {"field": "likes", "modifier": "log1p"}},
            ],
        }}}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"likes"}

    def test_decay_function(self):
        body = {"query": {"function_score": {
            "query": {"match_all": {}},
            "functions": [
                {"gauss": {"location": {"origin": "0,0", "scale": "5km"}}},
                {"exp": {"created_at": {"origin": "now", "scale": "10d"}}},
            ],
        }}}
        refs = extract_fields_from_search(body)
        assert refs.queried == {"location", "created_at"}

    def test_combined_functions(self):
        body = {"query": {"function_score": {
            "query": {"bool": {"filter": [{"term": {"status": "active"}}]}},
            "functions": [
                {"script_score": {"script": {"source": "doc['boost_factor'].value"}}},
                {"field_value_factor": {"field": "popularity"}},
            ],
        }}}
        refs = extract_fields_from_search(body)
        assert refs.filtered == {"status"}
        assert refs.queried == {"boost_factor", "popularity"}

    def test_function_score_in_filter_context(self):
        body = {"query": {"bool": {"filter": [
            {"function_score": {
                "query": {"match_all": {}},
                "functions": [{"field_value_factor": {"field": "rank"}}],
            }},
        ]}}}
        refs = extract_fields_from_search(body)
        assert refs.filtered == {"rank"}


class TestScriptedSort:
    """sort with _script — scripted sort expressions."""

    def test_scripted_sort_painless(self):
        body = {"sort": [
            {"_script": {
                "type": "number",
                "script": {"source": "doc['priority'].value * doc['weight'].value"},
                "order": "desc",
            }},
        ]}
        refs = extract_fields_from_search(body)
        assert refs.sorted == {"priority", "weight"}

    def test_scripted_sort_mixed_with_regular(self):
        body = {"sort": [
            {"timestamp": "desc"},
            {"_script": {
                "type": "number",
                "script": {"source": "doc['score'].value"},
                "order": "asc",
            }},
        ]}
        refs = extract_fields_from_search(body)
        assert refs.sorted == {"timestamp", "score"}

    def test_scripted_sort_string_script(self):
        body = {"sort": [
            {"_script": {
                "type": "number",
                "script": "doc['priority'].value",
                "order": "desc",
            }},
        ]}
        refs = extract_fields_from_search(body)
        assert refs.sorted == {"priority"}


class TestPipelineAggScripts:
    """bucket_script, bucket_selector, scripted_metric — script-based aggregations."""

    def test_bucket_script(self):
        body = {"aggs": {
            "sales_per_month": {
                "date_histogram": {"field": "date", "calendar_interval": "month"},
                "aggs": {
                    "total_sales": {"sum": {"field": "sales"}},
                    "profit": {
                        "bucket_script": {
                            "buckets_path": {"sales": "total_sales"},
                            "script": {"source": "doc['margin_pct'].value * params.sales"},
                        },
                    },
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert "date" in refs.aggregated
        assert "sales" in refs.aggregated
        assert "margin_pct" in refs.aggregated

    def test_bucket_selector(self):
        body = {"aggs": {
            "by_category": {
                "terms": {"field": "category"},
                "aggs": {
                    "avg_price": {"avg": {"field": "price"}},
                    "high_value_only": {
                        "bucket_selector": {
                            "buckets_path": {"avg": "avg_price"},
                            "script": {"source": "doc['min_threshold'].value < params.avg"},
                        },
                    },
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert "category" in refs.aggregated
        assert "price" in refs.aggregated
        assert "min_threshold" in refs.aggregated

    def test_scripted_metric(self):
        body = {"aggs": {
            "weighted_avg": {
                "scripted_metric": {
                    "init_script": {"source": "state.totals = []"},
                    "map_script": {"source": "state.totals.add(doc['price'].value * doc['quantity'].value)"},
                    "combine_script": {"source": "double total = 0; for (t in state.totals) { total += t } return total"},
                    "reduce_script": {"source": "double total = 0; for (s in states) { total += s } return total"},
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.aggregated == {"price", "quantity"}

    def test_scripted_metric_string_scripts(self):
        body = {"aggs": {
            "custom": {
                "scripted_metric": {
                    "init_script": "state.vals = []",
                    "map_script": "state.vals.add(doc['amount'].value)",
                    "combine_script": "return state.vals.sum()",
                    "reduce_script": "return states.sum()",
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.aggregated == {"amount"}


class TestScriptLangHandling:
    """Verify language detection: Painless default, explicit lang, mustache skip."""

    def test_default_lang_is_painless(self):
        """No lang specified → treated as Painless."""
        body = {"script_fields": {
            "val": {"script": {"source": "doc['price'].value"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"price"}

    def test_explicit_painless(self):
        body = {"script_fields": {
            "val": {"script": {"lang": "painless", "source": "doc['price'].value"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"price"}

    def test_mustache_skipped(self):
        body = {"script_fields": {
            "val": {"script": {"lang": "mustache", "source": "{{price}}"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == set()

    def test_inline_field_for_old_api(self):
        """ES 5.x used 'inline' instead of 'source'."""
        body = {"script_fields": {
            "val": {"script": {"inline": "doc['price'].value"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == {"price"}

    def test_no_source_returns_nothing(self):
        body = {"script_fields": {
            "val": {"script": {"id": "my_stored_script", "params": {"field": "price"}}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.sourced == set()


class TestHasPainlessDetection:
    """Verify has_painless flag is set when Painless scripts extract fields."""

    def test_no_scripts_is_false(self):
        body = {"query": {"match": {"title": "laptop"}}}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is False

    def test_script_fields_sets_flag(self):
        body = {"script_fields": {
            "total": {"script": {"source": "doc['price'].value * 2"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is True

    def test_runtime_mappings_sets_flag(self):
        body = {"runtime_mappings": {
            "day_of_week": {
                "type": "keyword",
                "script": {"source": "emit(doc['@timestamp'].value.dayOfWeekEnum.name())"},
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is True

    def test_function_score_script_sets_flag(self):
        body = {"query": {"function_score": {
            "query": {"match": {"title": "laptop"}},
            "functions": [
                {"script_score": {"script": {"source": "doc['popularity'].value * 2"}}},
            ],
        }}}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is True

    def test_scripted_sort_sets_flag(self):
        body = {"sort": [
            {"_script": {
                "type": "number",
                "script": {"source": "doc['priority'].value"},
                "order": "desc",
            }},
        ]}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is True

    def test_scripted_metric_sets_flag(self):
        body = {"aggs": {
            "custom": {
                "scripted_metric": {
                    "init_script": "state.vals = []",
                    "map_script": "state.vals.add(doc['amount'].value)",
                    "combine_script": "return state.vals.sum()",
                    "reduce_script": "return states.sum()",
                },
            },
        }}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is True

    def test_bucket_script_sets_flag(self):
        body = {"aggs": {
            "sales": {"sum": {"field": "amount"}},
            "ratio": {"bucket_script": {
                "buckets_path": {"sales": "sales"},
                "script": "doc['margin'].value / params.sales",
            }},
        }}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is True

    def test_mustache_script_does_not_set_flag(self):
        body = {"script_fields": {
            "computed": {"script": {"lang": "mustache", "source": "{{field}}"}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is False

    def test_stored_script_does_not_set_flag(self):
        body = {"script_fields": {
            "val": {"script": {"id": "my_stored_script", "params": {"field": "price"}}},
        }}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is False

    def test_mixed_dsl_and_painless(self):
        """A query with both regular DSL fields and a Painless script."""
        body = {
            "query": {"match": {"title": "laptop"}},
            "script_fields": {
                "margin": {"script": {"source": "doc['price'].value * 0.1"}},
            },
        }
        refs = extract_fields_from_search(body)
        assert refs.queried == {"title"}
        assert refs.sourced == {"price"}
        assert refs.has_painless is True

    def test_msearch_propagates_painless_flag(self):
        lines = [
            '{"index": "products"}',
            '{"query": {"match": {"title": "laptop"}}, "script_fields": {"x": {"script": {"source": "doc[\'price\'].value"}}}}',
        ]
        body = "\n".join(lines).encode()
        refs = _extract_from_msearch(body)
        assert refs.has_painless is True

    def test_runtime_mapping_string_script_sets_flag(self):
        body = {"runtime_mappings": {
            "total": {"type": "double", "script": "emit(doc['price'].value * doc['qty'].value)"},
        }}
        refs = extract_fields_from_search(body)
        assert refs.has_painless is True
