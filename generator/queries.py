"""
Query templates with frequency weights for traffic generation.

Each template is a callable that returns a (method, path, body) tuple.
Weights control how often each query type is selected — this creates
the intentional skew needed to validate heat scores.

Organized by scenario (index):
  - products: e-commerce product search
  - logs: application log filtering/aggregation
  - orders: order analytics
"""

import random
import json


# ============================================================
# Products queries
# ============================================================

SEARCH_TERMS = [
    "wireless", "premium", "professional", "compact", "smart",
    "durable", "lightweight", "portable", "advanced", "eco-friendly",
]

PRODUCT_CATEGORIES = ["electronics", "clothing", "home", "sports", "books"]

PRODUCT_BRANDS = [
    "SoundMax", "TechPro", "EcoWear", "HomeStyle", "SportFit",
    "PageTurn", "BrightLife", "UrbanGear", "NaturePlus", "CoreTech",
]


def search_by_title(**kwargs):
    """Match query on title, with _source filtering."""
    term = random.choice(SEARCH_TERMS)
    body = {
        "query": {"match": {"title": term}},
        "_source": ["title", "price", "category"],
        "size": 10,
    }
    return "POST", "/products/_search", json.dumps(body)


def filter_by_category_sort_by_price(**kwargs):
    """Bool filter on category, sorted by price."""
    cat = random.choice(PRODUCT_CATEGORIES)
    body = {
        "query": {
            "bool": {
                "filter": [{"term": {"category": cat}}]
            }
        },
        "sort": [{"price": {"order": random.choice(["asc", "desc"])}}],
        "_source": ["title", "price", "category"],
        "size": 20,
    }
    return "POST", "/products/_search", json.dumps(body)


def aggregate_by_brand(**kwargs):
    """Terms aggregation on brand."""
    body = {
        "size": 0,
        "aggs": {
            "brands": {"terms": {"field": "brand", "size": 20}}
        },
    }
    return "POST", "/products/_search", json.dumps(body)


def range_on_rating(**kwargs):
    """Range query on rating."""
    min_rating = round(random.uniform(3.0, 4.5), 1)
    body = {
        "query": {"range": {"rating": {"gte": min_rating}}},
        "size": 10,
    }
    return "POST", "/products/_search", json.dumps(body)


def search_by_description(**kwargs):
    """Match query on description."""
    term = random.choice(SEARCH_TERMS)
    body = {
        "query": {"match": {"description": term}},
        "size": 5,
    }
    return "POST", "/products/_search", json.dumps(body)


def products_get_by_id(**kwargs):
    """GET a single document by ID."""
    doc_id = random.randint(1, 100)
    return "GET", f"/products/_doc/{doc_id}", None


def products_match_all(**kwargs):
    """match_all with no field references."""
    body = {"query": {"match_all": {}}, "size": 5}
    return "POST", "/products/_search", json.dumps(body)


# ============================================================
# Logs queries
# ============================================================

LOG_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
LOG_SERVICES = ["api-gateway", "auth-service", "payment-service", "inventory-service", "notification-service"]
LOG_MESSAGES_SEARCH = ["timeout", "error", "failed", "slow", "retry", "connection", "authenticated"]


def logs_filter_by_level(lookback=None, **kwargs):
    """Bool filter on level + range on timestamp, sort by timestamp."""
    level = random.choice(["ERROR", "WARN", "INFO"])
    time_expr = f"now-{lookback}" if lookback else f"now-{random.choice([1, 6, 12, 24, 48])}h"
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"level": level}},
                    {"range": {"timestamp": {"gte": time_expr}}},
                ]
            }
        },
        "sort": [{"timestamp": {"order": "desc"}}],
        "size": 50,
    }
    return "POST", "/logs/_search", json.dumps(body)


def logs_search_message(**kwargs):
    """Match query on message text."""
    term = random.choice(LOG_MESSAGES_SEARCH)
    body = {
        "query": {"match": {"message": term}},
        "_source": ["timestamp", "level", "service", "message"],
        "size": 20,
    }
    return "POST", "/logs/_search", json.dumps(body)


def logs_agg_by_service(**kwargs):
    """Terms agg on service with sub-agg avg on duration_ms."""
    body = {
        "size": 0,
        "aggs": {
            "by_service": {
                "terms": {"field": "service", "size": 20},
                "aggs": {
                    "avg_duration": {"avg": {"field": "duration_ms"}},
                },
            }
        },
    }
    return "POST", "/logs/_search", json.dumps(body)


def logs_agg_over_time(lookback=None, **kwargs):
    """Date histogram on timestamp."""
    interval = random.choice(["1h", "30m", "15m"])
    time_expr = f"now-{lookback}" if lookback else "now-24h"
    body = {
        "size": 0,
        "query": {"range": {"timestamp": {"gte": time_expr}}},
        "aggs": {
            "over_time": {
                "date_histogram": {"field": "timestamp", "fixed_interval": interval},
            }
        },
    }
    return "POST", "/logs/_search", json.dumps(body)


def logs_filter_by_service_and_status(**kwargs):
    """Bool filter on service + range on status_code."""
    service = random.choice(LOG_SERVICES)
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"service": service}},
                    {"range": {"status_code": {"gte": 400}}},
                ]
            }
        },
        "sort": [{"timestamp": {"order": "desc"}}],
        "_source": ["timestamp", "level", "service", "endpoint", "status_code", "message"],
        "size": 30,
    }
    return "POST", "/logs/_search", json.dumps(body)


# ============================================================
# Orders queries
# ============================================================

ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
ORDER_CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "food", "toys"]
ORDER_COUNTRIES = ["US", "UK", "DE", "FR", "JP", "CA", "AU"]
CUSTOMER_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]


def orders_agg_revenue_by_category(**kwargs):
    """Terms on category with sum on total_amount."""
    body = {
        "size": 0,
        "aggs": {
            "by_category": {
                "terms": {"field": "category", "size": 20},
                "aggs": {
                    "revenue": {"sum": {"field": "total_amount"}},
                },
            }
        },
    }
    return "POST", "/orders/_search", json.dumps(body)


def orders_filter_by_status(**kwargs):
    """Bool filter on status, sort by order_date."""
    status = random.choice(ORDER_STATUSES)
    body = {
        "query": {
            "bool": {
                "filter": [{"term": {"status": status}}]
            }
        },
        "sort": [{"order_date": {"order": "desc"}}],
        "_source": ["order_id", "customer_name", "status", "total_amount", "order_date"],
        "size": 20,
    }
    return "POST", "/orders/_search", json.dumps(body)


def orders_agg_by_payment_method(**kwargs):
    """Terms aggregation on payment_method."""
    body = {
        "size": 0,
        "aggs": {
            "by_payment": {"terms": {"field": "payment_method", "size": 10}},
        },
    }
    return "POST", "/orders/_search", json.dumps(body)


def orders_search_customer(**kwargs):
    """Match query on customer_name."""
    name = random.choice(CUSTOMER_NAMES)
    body = {
        "query": {"match": {"customer_name": name}},
        "_source": ["order_id", "customer_name", "total_amount", "status"],
        "size": 10,
    }
    return "POST", "/orders/_search", json.dumps(body)


def orders_date_range(lookback=None, **kwargs):
    """Range query on order_date."""
    time_expr = f"now-{lookback}" if lookback else f"now-{random.choice([7, 14, 30, 60])}d"
    body = {
        "query": {"range": {"order_date": {"gte": time_expr}}},
        "sort": [{"order_date": {"order": "desc"}}],
        "size": 20,
    }
    return "POST", "/orders/_search", json.dumps(body)


def orders_agg_by_country(**kwargs):
    """Terms aggregation on shipping_country."""
    body = {
        "size": 0,
        "aggs": {
            "by_country": {
                "terms": {"field": "shipping_country", "size": 20},
                "aggs": {
                    "revenue": {"sum": {"field": "total_amount"}},
                },
            },
        },
    }
    return "POST", "/orders/_search", json.dumps(body)


# ============================================================
# Scenario definitions
# ============================================================

SCENARIOS = {
    "products": {
        "label": "E-commerce Product Search",
        "index": "products",
        "queries": {
            "search_by_title": search_by_title,
            "filter_by_category_sort_by_price": filter_by_category_sort_by_price,
            "aggregate_by_brand": aggregate_by_brand,
            "range_on_rating": range_on_rating,
            "search_by_description": search_by_description,
            "get_by_id": products_get_by_id,
            "match_all": products_match_all,
        },
        "weights": {
            "search_by_title": 40,
            "filter_by_category_sort_by_price": 25,
            "aggregate_by_brand": 15,
            "range_on_rating": 10,
            "search_by_description": 5,
            "get_by_id": 3,
            "match_all": 2,
        },
        "labels": {
            "search_by_title": "Search by title",
            "filter_by_category_sort_by_price": "Filter category + sort price",
            "aggregate_by_brand": "Aggregate by brand",
            "range_on_rating": "Range on rating",
            "search_by_description": "Search by description",
            "get_by_id": "Get by ID",
            "match_all": "Match all",
        },
        "time_range_queries": set(),
    },
    "logs": {
        "label": "Application Logging",
        "index": "logs",
        "queries": {
            "logs_filter_by_level": logs_filter_by_level,
            "logs_search_message": logs_search_message,
            "logs_agg_by_service": logs_agg_by_service,
            "logs_agg_over_time": logs_agg_over_time,
            "logs_filter_by_service_and_status": logs_filter_by_service_and_status,
        },
        "weights": {
            "logs_filter_by_level": 35,
            "logs_search_message": 20,
            "logs_agg_by_service": 20,
            "logs_agg_over_time": 15,
            "logs_filter_by_service_and_status": 10,
        },
        "labels": {
            "logs_filter_by_level": "Filter by level + time",
            "logs_search_message": "Search message text",
            "logs_agg_by_service": "Aggregate by service",
            "logs_agg_over_time": "Histogram over time",
            "logs_filter_by_service_and_status": "Filter service + error status",
        },
        "time_range_queries": {"logs_filter_by_level", "logs_agg_over_time"},
    },
    "orders": {
        "label": "Order Analytics",
        "index": "orders",
        "queries": {
            "orders_agg_revenue_by_category": orders_agg_revenue_by_category,
            "orders_filter_by_status": orders_filter_by_status,
            "orders_agg_by_payment_method": orders_agg_by_payment_method,
            "orders_search_customer": orders_search_customer,
            "orders_date_range": orders_date_range,
            "orders_agg_by_country": orders_agg_by_country,
        },
        "weights": {
            "orders_agg_revenue_by_category": 25,
            "orders_filter_by_status": 20,
            "orders_agg_by_payment_method": 15,
            "orders_search_customer": 15,
            "orders_date_range": 15,
            "orders_agg_by_country": 10,
        },
        "labels": {
            "orders_agg_revenue_by_category": "Revenue by category",
            "orders_filter_by_status": "Filter by status",
            "orders_agg_by_payment_method": "Aggregate by payment",
            "orders_search_customer": "Search customer name",
            "orders_date_range": "Date range filter",
            "orders_agg_by_country": "Revenue by country",
        },
        "time_range_queries": {"orders_date_range"},
    },
}


# Backwards-compatible aliases — used by main.py and generate.py
QUERY_FUNCTIONS = SCENARIOS["products"]["queries"]
DEFAULT_WEIGHTS = SCENARIOS["products"]["weights"]


# Pre-compute for backwards compatibility
_weights = list(DEFAULT_WEIGHTS.values())
_funcs = list(QUERY_FUNCTIONS.values())


def random_query() -> tuple[str, str, str | None]:
    """Return a random (method, path, body) tuple from the products scenario."""
    func = random.choices(_funcs, weights=_weights, k=1)[0]
    return func()
