"""Tests for generator.queries — lookback parameterization of query functions."""

import json
from generator.queries import (
    logs_filter_by_level,
    logs_agg_over_time,
    orders_date_range,
    search_by_title,
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


class TestTimeRangeQueriesMetadata:
    def test_products_has_no_time_range_queries(self):
        assert SCENARIOS["products"]["time_range_queries"] == set()

    def test_logs_has_time_range_queries(self):
        assert SCENARIOS["logs"]["time_range_queries"] == {"logs_filter_by_level", "logs_agg_over_time"}

    def test_orders_has_time_range_queries(self):
        assert SCENARIOS["orders"]["time_range_queries"] == {"orders_date_range"}
