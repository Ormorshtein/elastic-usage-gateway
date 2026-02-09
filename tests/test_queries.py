"""Tests for generator.queries — lookback parameterization of query functions."""

import json
from generator.queries import (
    logs_filter_by_level,
    logs_agg_over_time,
    orders_date_range,
    search_by_title,
    products_index_single,
    products_bulk_index,
    products_update_price_stock,
    logs_bulk_ingest,
    orders_create_new,
    orders_update_status,
    SCENARIOS,
)


class TestLogsFilterByLevel:
    def test_custom_lookback(self):
        method, path, body = logs_filter_by_level(lookback="6h")
        assert method == "POST"
        assert path == "/logs/_search"
        parsed = json.loads(body)
        range_val = parsed["query"]["bool"]["filter"][1]["range"]["timestamp"]["gte"]
        assert range_val == "now-6h"

    def test_default_random(self):
        method, path, body = logs_filter_by_level()
        assert method == "POST"
        assert path == "/logs/_search"
        parsed = json.loads(body)
        range_val = parsed["query"]["bool"]["filter"][1]["range"]["timestamp"]["gte"]
        assert range_val.startswith("now-")
        # Should be one of: now-1h, now-6h, now-12h, now-24h, now-48h
        suffix = range_val.replace("now-", "")
        assert suffix in ("1h", "6h", "12h", "24h", "48h")

    def test_custom_lookback_days(self):
        method, path, body = logs_filter_by_level(lookback="30d")
        assert method == "POST"
        parsed = json.loads(body)
        range_val = parsed["query"]["bool"]["filter"][1]["range"]["timestamp"]["gte"]
        assert range_val == "now-30d"


class TestLogsAggOverTime:
    def test_custom_lookback(self):
        method, path, body = logs_agg_over_time(lookback="1h")
        assert method == "POST"
        assert path == "/logs/_search"
        parsed = json.loads(body)
        range_val = parsed["query"]["range"]["timestamp"]["gte"]
        assert range_val == "now-1h"

    def test_default_24h(self):
        method, path, body = logs_agg_over_time()
        assert method == "POST"
        parsed = json.loads(body)
        range_val = parsed["query"]["range"]["timestamp"]["gte"]
        assert range_val == "now-24h"


class TestOrdersDateRange:
    def test_custom_lookback(self):
        method, path, body = orders_date_range(lookback="30d")
        assert method == "POST"
        assert path == "/orders/_search"
        parsed = json.loads(body)
        range_val = parsed["query"]["range"]["order_date"]["gte"]
        assert range_val == "now-30d"

    def test_default_random(self):
        method, path, body = orders_date_range()
        assert method == "POST"
        assert path == "/orders/_search"
        parsed = json.loads(body)
        range_val = parsed["query"]["range"]["order_date"]["gte"]
        assert range_val.startswith("now-")
        suffix = range_val.replace("now-", "")
        assert suffix in ("7d", "14d", "30d", "60d")


class TestKwargsIgnored:
    def test_non_time_range_function_accepts_lookback(self):
        # Functions without time ranges should accept and ignore lookback
        method, path, body = search_by_title(lookback="6h")
        assert method == "POST"
        assert path == "/products/_search"


class TestProductsWriteTemplates:
    def test_index_single_returns_put_doc(self):
        method, path, body = products_index_single()
        assert method == "PUT"
        assert path.startswith("/products/_doc/")
        parsed = json.loads(body)
        assert "title" in parsed
        assert "price" in parsed
        assert "category" in parsed
        assert "brand" in parsed
        assert "tags" in parsed

    def test_bulk_index_returns_ndjson(self):
        method, path, body = products_bulk_index()
        assert method == "POST"
        assert path == "/products/_bulk"
        lines = body.strip().split("\n")
        assert len(lines) >= 6  # at least 3 action+doc pairs
        assert len(lines) % 2 == 0
        action = json.loads(lines[0])
        assert "index" in action
        doc = json.loads(lines[1])
        assert "title" in doc
        assert "price" in doc

    def test_update_price_stock_returns_post_update(self):
        method, path, body = products_update_price_stock()
        assert method == "POST"
        assert path.startswith("/products/_update/")
        parsed = json.loads(body)
        assert "doc" in parsed
        doc_fields = parsed["doc"]
        assert "price" in doc_fields or "stock_count" in doc_fields

    def test_write_functions_accept_kwargs(self):
        products_index_single(lookback="6h")
        products_bulk_index(lookback="6h")
        products_update_price_stock(lookback="6h")


class TestLogsWriteTemplates:
    def test_bulk_ingest_returns_ndjson(self):
        method, path, body = logs_bulk_ingest()
        assert method == "POST"
        assert path == "/logs-2026.02.06/_bulk"
        lines = body.strip().split("\n")
        assert len(lines) >= 10  # at least 5 action+doc pairs
        assert len(lines) % 2 == 0
        action = json.loads(lines[0])
        assert "index" in action
        doc = json.loads(lines[1])
        assert "timestamp" in doc
        assert "level" in doc
        assert "service" in doc
        assert "message" in doc

    def test_bulk_ingest_accepts_kwargs(self):
        logs_bulk_ingest(lookback="6h")


class TestOrdersWriteTemplates:
    def test_create_new_returns_put_doc(self):
        method, path, body = orders_create_new()
        assert method == "PUT"
        assert "/_doc/" in path
        assert path.startswith("/orders-us/") or path.startswith("/orders-eu/")
        parsed = json.loads(body)
        assert "order_id" in parsed
        assert "customer_name" in parsed
        assert parsed["status"] == "pending"
        assert "total_amount" in parsed

    def test_update_status_returns_post_update(self):
        method, path, body = orders_update_status()
        assert method == "POST"
        assert "/_update/" in path
        assert "orders-us" in path or "orders-eu" in path
        parsed = json.loads(body)
        assert "doc" in parsed
        assert "status" in parsed["doc"]

    def test_orders_write_functions_accept_kwargs(self):
        orders_create_new(lookback="30d")
        orders_update_status(lookback="30d")


class TestTimeRangeQueriesMetadata:
    def test_products_has_no_time_range_queries(self):
        assert SCENARIOS["products"]["time_range_queries"] == set()

    def test_logs_has_time_range_queries(self):
        assert SCENARIOS["logs"]["time_range_queries"] == {"logs_filter_by_level", "logs_agg_over_time"}

    def test_orders_has_time_range_queries(self):
        assert SCENARIOS["orders"]["time_range_queries"] == {"orders_date_range"}


# --- Scenario consistency ---

class TestScenariosConsistency:
    """Verify all scenarios have matching keys in queries, weights, and labels."""

    def test_all_scenarios_have_matching_keys(self):
        for name, scenario in SCENARIOS.items():
            query_keys = set(scenario["queries"].keys())
            weight_keys = set(scenario["weights"].keys())
            label_keys = set(scenario["labels"].keys())
            assert query_keys == weight_keys, (
                f"Scenario '{name}': queries keys != weights keys"
            )
            assert query_keys == label_keys, (
                f"Scenario '{name}': queries keys != labels keys"
            )

    def test_all_weights_positive(self):
        for name, scenario in SCENARIOS.items():
            for key, weight in scenario["weights"].items():
                assert weight > 0, f"Scenario '{name}', query '{key}' has weight {weight}"

    def test_time_range_queries_subset_of_queries(self):
        for name, scenario in SCENARIOS.items():
            trq = scenario.get("time_range_queries", set())
            query_keys = set(scenario["queries"].keys())
            assert trq.issubset(query_keys), (
                f"Scenario '{name}': time_range_queries not subset of query keys"
            )

    def test_all_query_functions_callable(self):
        for name, scenario in SCENARIOS.items():
            for key, func in scenario["queries"].items():
                assert callable(func), f"Scenario '{name}', query '{key}' is not callable"


# --- Write template extraction round-trip ---

class TestWriteTemplatesExtraction:
    """Verify that all write templates produce extractable field references."""

    def test_products_write_templates_produce_written_fields(self):
        from gateway.extractor import extract_from_request
        for name in ("index_single", "bulk_index", "update_price_stock"):
            func = SCENARIOS["products"]["queries"][name]
            method, path, body = func()
            body_bytes = body.encode() if body else b""
            indices, operation, refs = extract_from_request(path, method, body_bytes)
            assert len(refs.written) > 0, (
                f"Template '{name}' produced no written fields (op={operation})"
            )

    def test_logs_write_templates_produce_written_fields(self):
        from gateway.extractor import extract_from_request
        func = SCENARIOS["logs"]["queries"]["logs_bulk_ingest"]
        method, path, body = func()
        body_bytes = body.encode() if body else b""
        indices, operation, refs = extract_from_request(path, method, body_bytes)
        assert len(refs.written) > 0

    def test_orders_write_templates_produce_written_fields(self):
        from gateway.extractor import extract_from_request
        for name in ("orders_create_new", "orders_update_status"):
            func = SCENARIOS["orders"]["queries"][name]
            method, path, body = func()
            body_bytes = body.encode() if body else b""
            indices, operation, refs = extract_from_request(path, method, body_bytes)
            assert len(refs.written) > 0, (
                f"Template '{name}' produced no written fields (op={operation})"
            )
