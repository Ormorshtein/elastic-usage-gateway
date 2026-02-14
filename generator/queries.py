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


def products_boosted_search(**kwargs):
    """function_score with script_score boosting by rating, plus field_value_factor on stock."""
    term = random.choice(SEARCH_TERMS)
    body = {
        "query": {
            "function_score": {
                "query": {"match": {"title": term}},
                "functions": [
                    {
                        "script_score": {
                            "script": {
                                "source": "doc['rating'].value * doc['price'].value / 100"
                            }
                        }
                    },
                    {
                        "field_value_factor": {
                            "field": "stock_count",
                            "modifier": "log1p",
                        }
                    },
                ],
                "boost_mode": "multiply",
            }
        },
        "_source": ["title", "price", "rating"],
        "size": 10,
    }
    return "POST", "/products/_search", json.dumps(body)


def products_discounted_price(**kwargs):
    """script_fields computing a discounted price from price field."""
    discount = round(random.uniform(0.05, 0.30), 2)
    body = {
        "query": {"bool": {"filter": [{"term": {"category": random.choice(PRODUCT_CATEGORIES)}}]}},
        "_source": ["title", "category"],
        "script_fields": {
            "discounted_price": {
                "script": {
                    "source": f"doc['price'].value * {1 - discount}"
                }
            }
        },
        "size": 10,
    }
    return "POST", "/products/_search", json.dumps(body)


# -- Products write operations --

PRODUCT_TAGS_POOL = ["wireless", "bluetooth", "premium", "eco-friendly", "bestseller", "new-arrival"]


def products_index_single(**kwargs):
    """Index (upsert) a single product document."""
    doc_id = random.randint(1, 100)
    body = {
        "title": f"{random.choice(SEARCH_TERMS).title()} {random.choice(PRODUCT_BRANDS)} Product",
        "category": random.choice(PRODUCT_CATEGORIES),
        "brand": random.choice(PRODUCT_BRANDS),
        "price": round(random.uniform(9.99, 499.99), 2),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "stock_count": random.randint(0, 500),
        "tags": random.sample(PRODUCT_TAGS_POOL, k=random.randint(1, 3)),
    }
    return "PUT", f"/products/_doc/{doc_id}", json.dumps(body)


def products_bulk_index(**kwargs):
    """Bulk index a batch of product documents."""
    batch_size = random.randint(3, 8)
    lines = []
    for _ in range(batch_size):
        doc_id = random.randint(1, 100)
        action = {"index": {"_index": "products", "_id": str(doc_id)}}
        doc = {
            "title": f"{random.choice(SEARCH_TERMS).title()} {random.choice(PRODUCT_BRANDS)} Product",
            "category": random.choice(PRODUCT_CATEGORIES),
            "brand": random.choice(PRODUCT_BRANDS),
            "price": round(random.uniform(9.99, 499.99), 2),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "stock_count": random.randint(0, 500),
        }
        lines.append(json.dumps(action))
        lines.append(json.dumps(doc))
    return "POST", "/products/_bulk", "\n".join(lines) + "\n"


def products_update_price_stock(**kwargs):
    """Partial update of price and/or stock_count for an existing product."""
    doc_id = random.randint(1, 100)
    updates = {}
    if random.random() < 0.7:
        updates["price"] = round(random.uniform(9.99, 499.99), 2)
    if random.random() < 0.7:
        updates["stock_count"] = random.randint(0, 500)
    if not updates:
        updates["price"] = round(random.uniform(9.99, 499.99), 2)
    body = {"doc": updates}
    return "POST", f"/products/_update/{doc_id}", json.dumps(body)


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


def logs_slow_request_score(**kwargs):
    """script_fields computing a severity score from duration_ms and status_code."""
    body = {
        "query": {"bool": {"filter": [{"range": {"duration_ms": {"gte": 100}}}]}},
        "_source": ["timestamp", "service", "endpoint", "duration_ms", "status_code"],
        "script_fields": {
            "severity_score": {
                "script": {
                    "source": (
                        "double score = doc['duration_ms'].value / 1000.0; "
                        "if (doc['status_code'].value >= 500) { score = score * 3; } "
                        "return score;"
                    )
                }
            }
        },
        "sort": [{"duration_ms": {"order": "desc"}}],
        "size": 20,
    }
    return "POST", "/logs/_search", json.dumps(body)


# -- Logs write operations --

LOG_INGEST_ENDPOINTS = ["/api/users", "/api/orders", "/api/search", "/api/products", "/api/auth/login"]


def logs_bulk_ingest(**kwargs):
    """Bulk ingest a batch of log entries (most common ES log pattern)."""
    batch_size = random.randint(5, 15)
    lines = []
    for _ in range(batch_size):
        action = {"index": {"_index": "logs-2026.02.06"}}
        doc = {
            "timestamp": f"2026-02-{random.randint(4, 8):02d}T{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}.000Z",
            "level": random.choice(LOG_LEVELS),
            "service": random.choice(LOG_SERVICES),
            "host": f"web-{random.randint(1, 3):02d}",
            "message": f"Generated log entry {random.randint(1000, 9999)}",
            "status_code": random.choice([200, 200, 200, 400, 500]),
            "duration_ms": round(random.uniform(1, 2000), 1),
            "trace_id": f"trace-{random.randint(100000, 999999)}",
            "endpoint": random.choice(LOG_INGEST_ENDPOINTS),
            "region": random.choice(["us-east-1", "eu-west-1"]),
        }
        lines.append(json.dumps(action))
        lines.append(json.dumps(doc))
    return "POST", "/logs-2026.02.06/_bulk", "\n".join(lines) + "\n"


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


def orders_runtime_day_of_week(**kwargs):
    """runtime_mappings extracting day of week from order_date."""
    body = {
        "size": 0,
        "runtime_mappings": {
            "day_of_week": {
                "type": "keyword",
                "script": {
                    "source": "emit(doc['order_date'].value.dayOfWeekEnum.name())"
                },
            }
        },
        "aggs": {
            "by_day": {"terms": {"field": "day_of_week", "size": 7}},
        },
    }
    return "POST", "/orders/_search", json.dumps(body)


def orders_scripted_sort_value(**kwargs):
    """Scripted sort by total_amount * item_count (order value density)."""
    body = {
        "query": {"bool": {"filter": [{"term": {"status": random.choice(["confirmed", "shipped"])}}]}},
        "sort": [
            {
                "_script": {
                    "type": "number",
                    "script": {
                        "source": "doc['total_amount'].value * doc['item_count'].value"
                    },
                    "order": "desc",
                }
            }
        ],
        "_source": ["order_id", "customer_name", "total_amount", "item_count"],
        "size": 20,
    }
    return "POST", "/orders/_search", json.dumps(body)


# -- Orders write operations --

ORDERS_CONCRETE_INDICES = ["orders-us", "orders-eu"]


def orders_create_new(**kwargs):
    """Create a new order document via single-doc index."""
    concrete_index = random.choice(ORDERS_CONCRETE_INDICES)
    order_id = f"ORD-{random.randint(10000, 99999)}"
    body = {
        "order_id": order_id,
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": f"{random.choice(CUSTOMER_NAMES)} Smith",
        "order_date": f"2026-02-{random.randint(1, 8):02d}T{random.randint(0, 23):02d}:00:00Z",
        "status": "pending",
        "total_amount": round(random.uniform(10.0, 1500.0), 2),
        "item_count": random.randint(1, 10),
        "payment_method": random.choice(["credit_card", "paypal", "debit_card"]),
        "shipping_country": random.choice(ORDER_COUNTRIES),
        "category": random.choice(ORDER_CATEGORIES),
        "channel": random.choice(["web", "mobile", "api"]),
    }
    return "PUT", f"/{concrete_index}/_doc/{order_id}", json.dumps(body)


def orders_update_status(**kwargs):
    """Partial update: change an order's status (common order lifecycle pattern)."""
    concrete_index = random.choice(ORDERS_CONCRETE_INDICES)
    doc_id = random.randint(1, 200)
    new_status = random.choice(["confirmed", "shipped", "delivered", "cancelled"])
    body = {"doc": {"status": new_status}}
    return "POST", f"/{concrete_index}/_update/{doc_id}", json.dumps(body)


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
            "boosted_search": products_boosted_search,
            "discounted_price": products_discounted_price,
            "index_single": products_index_single,
            "bulk_index": products_bulk_index,
            "update_price_stock": products_update_price_stock,
        },
        "weights": {
            "search_by_title": 40,
            "filter_by_category_sort_by_price": 25,
            "aggregate_by_brand": 15,
            "range_on_rating": 10,
            "search_by_description": 5,
            "get_by_id": 3,
            "match_all": 2,
            "boosted_search": 5,
            "discounted_price": 3,
            "index_single": 3,
            "bulk_index": 3,
            "update_price_stock": 5,
        },
        "labels": {
            "search_by_title": "Search by title",
            "filter_by_category_sort_by_price": "Filter category + sort price",
            "aggregate_by_brand": "Aggregate by brand",
            "range_on_rating": "Range on rating",
            "search_by_description": "Search by description",
            "get_by_id": "Get by ID",
            "match_all": "Match all",
            "boosted_search": "Boosted search (script_score)",
            "discounted_price": "Discounted price (script_fields)",
            "index_single": "Index single product",
            "bulk_index": "Bulk index products",
            "update_price_stock": "Update price/stock",
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
            "logs_slow_request_score": logs_slow_request_score,
            "logs_bulk_ingest": logs_bulk_ingest,
        },
        "weights": {
            "logs_filter_by_level": 35,
            "logs_search_message": 20,
            "logs_agg_by_service": 20,
            "logs_agg_over_time": 15,
            "logs_filter_by_service_and_status": 10,
            "logs_slow_request_score": 5,
            "logs_bulk_ingest": 8,
        },
        "labels": {
            "logs_filter_by_level": "Filter by level + time",
            "logs_search_message": "Search message text",
            "logs_agg_by_service": "Aggregate by service",
            "logs_agg_over_time": "Histogram over time",
            "logs_filter_by_service_and_status": "Filter service + error status",
            "logs_slow_request_score": "Severity score (script_fields)",
            "logs_bulk_ingest": "Bulk ingest logs",
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
            "orders_runtime_day_of_week": orders_runtime_day_of_week,
            "orders_scripted_sort_value": orders_scripted_sort_value,
            "orders_create_new": orders_create_new,
            "orders_update_status": orders_update_status,
        },
        "weights": {
            "orders_agg_revenue_by_category": 25,
            "orders_filter_by_status": 20,
            "orders_agg_by_payment_method": 15,
            "orders_search_customer": 15,
            "orders_date_range": 15,
            "orders_agg_by_country": 10,
            "orders_runtime_day_of_week": 5,
            "orders_scripted_sort_value": 3,
            "orders_create_new": 5,
            "orders_update_status": 5,
        },
        "labels": {
            "orders_agg_revenue_by_category": "Revenue by category",
            "orders_filter_by_status": "Filter by status",
            "orders_agg_by_payment_method": "Aggregate by payment",
            "orders_search_customer": "Search customer name",
            "orders_date_range": "Date range filter",
            "orders_agg_by_country": "Revenue by country",
            "orders_runtime_day_of_week": "Day of week (runtime_mappings)",
            "orders_scripted_sort_value": "Sort by order value (scripted sort)",
            "orders_create_new": "Create new order",
            "orders_update_status": "Update order status",
        },
        "time_range_queries": {"orders_date_range"},
    },
}
