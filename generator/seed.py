"""
Seed script — creates indices with explicit mappings and loads sample documents.

Supports three logical index groups:
  - products (100 docs) — single index, e-commerce product catalog
  - logs (500 docs) — 3 daily indices behind "logs" alias
  - orders (200 docs) — 2 regional indices behind "orders" alias

Usage:
    python -m generator.seed                     # seed all indices
    python -m generator.seed --index logs orders  # seed specific groups
    python -m generator.seed --gateway           # seed through the gateway (port 9201)
"""

import argparse
import json
import random
from datetime import datetime, timedelta

import requests

from config import ES_HOST, GATEWAY_PORT


# ============================================================
# Products index (100 docs) — single index, no alias
# ============================================================

PRODUCTS_MAPPING = {
    "mappings": {
        "properties": {
            "title":                {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "description":          {"type": "text"},
            "category":             {"type": "keyword"},
            "subcategory":          {"type": "keyword"},
            "brand":                {"type": "keyword"},
            "price":                {"type": "float"},
            "rating":               {"type": "float"},
            "stock_count":          {"type": "integer"},
            "created_at":           {"type": "date"},
            "internal_sku":         {"type": "keyword"},
            "legacy_supplier_code": {"type": "keyword"},
            "tags":                 {"type": "keyword"},
        }
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
}

CATEGORIES = {
    "electronics": ["audio", "cameras", "phones", "laptops", "accessories"],
    "clothing": ["shirts", "pants", "shoes", "jackets", "hats"],
    "home": ["kitchen", "bedroom", "bathroom", "garden", "lighting"],
    "sports": ["running", "cycling", "swimming", "fitness", "outdoor"],
    "books": ["fiction", "non-fiction", "technical", "comics", "education"],
}

BRANDS = [
    "SoundMax", "TechPro", "EcoWear", "HomeStyle", "SportFit",
    "PageTurn", "BrightLife", "UrbanGear", "NaturePlus", "CoreTech",
    "FlexBrand", "PeakEdge", "VividMark", "AquaLine", "SkyHigh",
]

ADJECTIVES = [
    "Premium", "Professional", "Lightweight", "Durable", "Compact",
    "Wireless", "Ergonomic", "Portable", "Advanced", "Ultra",
    "Smart", "Eco-Friendly", "Vintage", "Modern", "Classic",
]

NOUNS = {
    "electronics": ["Headphones", "Speaker", "Camera", "Laptop", "Charger", "Monitor", "Keyboard", "Mouse"],
    "clothing": ["T-Shirt", "Jacket", "Sneakers", "Hoodie", "Cap", "Polo", "Vest", "Shorts"],
    "home": ["Lamp", "Blender", "Pillow", "Rug", "Vase", "Shelf", "Clock", "Frame"],
    "sports": ["Shoes", "Bottle", "Mat", "Gloves", "Watch", "Bag", "Helmet", "Band"],
    "books": ["Guide", "Handbook", "Novel", "Collection", "Manual", "Anthology", "Workbook", "Atlas"],
}

TAG_POOL = [
    "wireless", "bluetooth", "noise-cancelling", "waterproof", "organic",
    "lightweight", "rechargeable", "eco-friendly", "premium", "budget",
    "bestseller", "new-arrival", "limited-edition", "sale", "trending",
]


def generate_product(doc_id: int) -> dict:
    category = random.choice(list(CATEGORIES.keys()))
    subcategory = random.choice(CATEGORIES[category])
    brand = random.choice(BRANDS)
    adj = random.choice(ADJECTIVES)
    noun = random.choice(NOUNS[category])
    title = f"{adj} {brand} {noun}"

    return {
        "title": title,
        "description": f"{title} — high-quality {subcategory} product from {brand}. "
                        f"Perfect for everyday use. Model #{doc_id}.",
        "category": category,
        "subcategory": subcategory,
        "brand": brand,
        "price": round(random.uniform(9.99, 499.99), 2),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "stock_count": random.randint(0, 500),
        "created_at": f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}T00:00:00Z",
        "internal_sku": f"{brand[:2].upper()}-{category[:3].upper()}-{doc_id:04d}",
        "legacy_supplier_code": f"SUP-{random.randint(10000, 99999)}",
        "tags": random.sample(TAG_POOL, k=random.randint(1, 4)),
    }


# ============================================================
# Logs index (500 docs total) — 3 daily indices behind "logs" alias
# ============================================================

LOGS_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp":           {"type": "date"},
            "level":               {"type": "keyword"},
            "service":             {"type": "keyword"},
            "host":                {"type": "keyword"},
            "message":             {"type": "text"},
            "status_code":         {"type": "integer"},
            "duration_ms":         {"type": "float"},
            "trace_id":            {"type": "keyword"},
            "user_agent":          {"type": "keyword"},
            "endpoint":            {"type": "keyword"},
            "region":              {"type": "keyword"},
            "internal_request_id": {"type": "keyword"},
        }
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
}

LOG_SERVICES = ["api-gateway", "auth-service", "payment-service", "inventory-service", "notification-service"]
LOG_HOSTS = ["web-01", "web-02", "web-03", "worker-01", "worker-02"]
LOG_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
LOG_LEVEL_WEIGHTS = [5, 60, 20, 15]
LOG_ENDPOINTS = ["/api/users", "/api/orders", "/api/products", "/api/auth/login", "/api/payments",
                 "/api/cart", "/api/search", "/api/notifications", "/healthz", "/metrics"]
LOG_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
LOG_USER_AGENTS = ["Mozilla/5.0", "PostmanRuntime/7.36", "python-requests/2.31",
                   "curl/8.4", "Go-http-client/2.0", "okhttp/4.12"]
LOG_MESSAGES = {
    "INFO": [
        "Request completed successfully",
        "User authenticated",
        "Cache hit for key",
        "Database query executed",
        "Background job completed",
        "Connection pool refreshed",
    ],
    "WARN": [
        "Slow query detected",
        "Rate limit approaching threshold",
        "Retry attempt for downstream service",
        "Deprecated API version used",
        "Memory usage above 80%",
    ],
    "ERROR": [
        "Connection timeout to database",
        "Authentication failed for user",
        "Payment processing error",
        "Null pointer in order validation",
        "Circuit breaker open for service",
    ],
    "DEBUG": [
        "Entering method processOrder",
        "Cache miss for session key",
        "Parsing request body",
    ],
}


def generate_log_entry(doc_id: int, target_date: datetime | None = None) -> dict:
    level = random.choices(LOG_LEVELS, weights=LOG_LEVEL_WEIGHTS, k=1)[0]
    service = random.choice(LOG_SERVICES)

    if target_date:
        # Spread within that day (0-24 hours)
        ts = target_date + timedelta(seconds=random.randint(0, 86399))
    else:
        now = datetime.utcnow()
        ts = now - timedelta(seconds=random.randint(0, 48 * 3600))

    status = 200
    if level == "ERROR":
        status = random.choice([500, 502, 503, 504, 400, 401, 403])
    elif level == "WARN":
        status = random.choice([200, 429, 408, 504])

    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "level": level,
        "service": service,
        "host": random.choice(LOG_HOSTS),
        "message": random.choice(LOG_MESSAGES[level]),
        "status_code": status,
        "duration_ms": round(random.uniform(1, 5000 if level == "ERROR" else 500), 1),
        "trace_id": f"trace-{random.randint(100000, 999999)}",
        "user_agent": random.choice(LOG_USER_AGENTS),
        "endpoint": random.choice(LOG_ENDPOINTS),
        "region": random.choice(LOG_REGIONS),
        "internal_request_id": f"req-{doc_id:06d}-{random.randint(1000, 9999)}",
    }


# ============================================================
# Orders index (200 docs total) — 2 regional indices behind "orders" alias
# ============================================================

ORDERS_MAPPING = {
    "mappings": {
        "properties": {
            "order_id":          {"type": "keyword"},
            "customer_id":       {"type": "keyword"},
            "customer_name":     {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "order_date":        {"type": "date"},
            "status":            {"type": "keyword"},
            "total_amount":      {"type": "float"},
            "item_count":        {"type": "integer"},
            "payment_method":    {"type": "keyword"},
            "shipping_country":  {"type": "keyword"},
            "category":          {"type": "keyword"},
            "channel":           {"type": "keyword"},
            "internal_notes":    {"type": "text"},
            "legacy_order_ref":  {"type": "keyword"},
        }
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
}

ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [10, 20, 25, 30, 10, 5]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
US_COUNTRIES = ["US", "CA", "MX"]
EU_COUNTRIES = ["UK", "DE", "FR", "IT", "ES", "NL"]
ORDER_CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "food", "toys"]
ORDER_CHANNELS = ["web", "mobile", "api", "in-store"]
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Moore"]


def generate_order(doc_id: int, region: str = "us") -> dict:
    now = datetime.utcnow()
    order_date = now - timedelta(days=random.randint(0, 90))
    status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    countries = US_COUNTRIES if region == "us" else EU_COUNTRIES

    return {
        "order_id": f"ORD-{doc_id:05d}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": f"{first} {last}",
        "order_date": order_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "total_amount": round(random.uniform(5.0, 2000.0), 2),
        "item_count": random.randint(1, 15),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_country": random.choice(countries),
        "category": random.choice(ORDER_CATEGORIES),
        "channel": random.choice(ORDER_CHANNELS),
        "internal_notes": f"Processed by batch-{random.randint(1, 50)}" if random.random() < 0.3 else "",
        "legacy_order_ref": f"LEG-{random.randint(100000, 999999)}",
    }


# ============================================================
# Index group configurations
# ============================================================

# Each group can have:
#   - "mapping": the ES mapping for all concrete indices
#   - "concrete_indices": list of {name, count, generator_kwargs} dicts
#   - "alias": optional alias name to create after seeding
#
# If no "concrete_indices", it's a single index (backwards compatible).

INDEX_CONFIGS = {
    "products": {
        "mapping": PRODUCTS_MAPPING,
        "concrete_indices": [
            {"name": "products", "count": 100, "generator": generate_product, "kwargs": {}},
        ],
        # No alias — single index
    },
    "logs": {
        "mapping": LOGS_MAPPING,
        "alias": "logs",
        "concrete_indices": [
            {
                "name": "logs-2026.02.04", "count": 200,
                "generator": generate_log_entry,
                "kwargs": {"target_date": datetime(2026, 2, 4)},
            },
            {
                "name": "logs-2026.02.05", "count": 200,
                "generator": generate_log_entry,
                "kwargs": {"target_date": datetime(2026, 2, 5)},
            },
            {
                "name": "logs-2026.02.06", "count": 100,
                "generator": generate_log_entry,
                "kwargs": {"target_date": datetime(2026, 2, 6)},
            },
        ],
    },
    "orders": {
        "mapping": ORDERS_MAPPING,
        "alias": "orders",
        "concrete_indices": [
            {
                "name": "orders-us", "count": 120,
                "generator": generate_order,
                "kwargs": {"region": "us"},
            },
            {
                "name": "orders-eu", "count": 80,
                "generator": generate_order,
                "kwargs": {"region": "eu"},
            },
        ],
    },
}


# ============================================================
# Generic seeding functions
# ============================================================

def seed_concrete_index(base_url: str, index_name: str, mapping: dict,
                        count: int, generator, kwargs: dict) -> None:
    """Create a single concrete index and load documents."""
    # Delete if exists
    requests.delete(f"{base_url}/{index_name}")

    # Create with mapping
    resp = requests.put(
        f"{base_url}/{index_name}",
        json=mapping,
        headers={"Content-Type": "application/json"},
    )
    print(f"    Create {index_name}: {resp.status_code} — {resp.json().get('acknowledged', resp.text[:100])}")

    # Bulk index
    bulk_lines = []
    for i in range(1, count + 1):
        action = json.dumps({"index": {"_index": index_name, "_id": str(i)}})
        doc = json.dumps(generator(i, **kwargs))
        bulk_lines.append(action)
        bulk_lines.append(doc)

    bulk_body = "\n".join(bulk_lines) + "\n"

    resp = requests.post(
        f"{base_url}/_bulk",
        data=bulk_body,
        headers={"Content-Type": "application/x-ndjson"},
    )
    result = resp.json()
    errors = result.get("errors", False)
    print(f"    Bulk index: {count} docs, errors={errors}")

    requests.post(f"{base_url}/{index_name}/_refresh")


def seed_group(base_url: str, group_name: str, config: dict) -> None:
    """Seed an index group (possibly multiple concrete indices with an alias)."""
    print(f"\nSeeding {group_name}...")
    mapping = config["mapping"]
    alias = config.get("alias")

    # Delete alias first if it exists (to avoid conflicts)
    if alias:
        requests.delete(f"{base_url}/{alias}")

    for ci in config["concrete_indices"]:
        seed_concrete_index(
            base_url=base_url,
            index_name=ci["name"],
            mapping=mapping,
            count=ci["count"],
            generator=ci["generator"],
            kwargs=ci.get("kwargs", {}),
        )

    # Create alias pointing to all concrete indices
    if alias:
        index_names = [ci["name"] for ci in config["concrete_indices"]]
        actions = [
            {"add": {"index": name, "alias": alias}}
            for name in index_names
        ]
        resp = requests.post(
            f"{base_url}/_aliases",
            json={"actions": actions},
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            print(f"  Created alias '{alias}' -> {index_names}")
        else:
            print(f"  WARNING: alias creation failed: {resp.status_code} {resp.text[:200]}")

    print(f"  {group_name} seeded.")


def seed(base_url: str, groups: list[str] | None = None) -> None:
    """Seed specified groups (or all if None)."""
    targets = groups or list(INDEX_CONFIGS.keys())
    for name in targets:
        if name not in INDEX_CONFIGS:
            print(f"  Unknown group: {name}, skipping")
            continue
        seed_group(base_url, name, INDEX_CONFIGS[name])

    print("\nSeeding complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed Elasticsearch indices")
    parser.add_argument("--gateway", action="store_true", help="Send through gateway (port 9201)")
    parser.add_argument("--index", nargs="*", help="Specific index groups to seed (default: all)")
    args = parser.parse_args()

    if args.gateway:
        url = f"http://localhost:{GATEWAY_PORT}"
        print(f"Seeding through gateway at {url}")
    else:
        url = ES_HOST
        print(f"Seeding directly to ES at {url}")

    seed(url, args.index)
