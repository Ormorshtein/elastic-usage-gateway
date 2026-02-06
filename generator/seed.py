"""
Seed script — creates the 'products' index with explicit mapping and loads
100 sample product documents.

Usage:
    python -m generator.seed
    python -m generator.seed --gateway   # seed through the gateway (port 9201)
"""

import argparse
import json
import random
import requests

from config import ES_HOST, GATEWAY_PORT

PRODUCTS_INDEX = "products"

MAPPING = {
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
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    }
}

# --- Sample data pools ---

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
    """Generate a single product document."""
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


def seed(base_url: str, count: int = 100) -> None:
    """Create index and load sample products."""
    # Delete if exists
    requests.delete(f"{base_url}/{PRODUCTS_INDEX}")

    # Create with mapping
    resp = requests.put(
        f"{base_url}/{PRODUCTS_INDEX}",
        json=MAPPING,
        headers={"Content-Type": "application/json"},
    )
    print(f"Create index: {resp.status_code} — {resp.json().get('acknowledged', resp.text[:100])}")

    # Bulk index
    bulk_lines = []
    for i in range(1, count + 1):
        action = json.dumps({"index": {"_index": PRODUCTS_INDEX, "_id": str(i)}})
        doc = json.dumps(generate_product(i))
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
    print(f"Bulk index: {count} docs, errors={errors}")

    # Refresh so docs are searchable
    requests.post(f"{base_url}/{PRODUCTS_INDEX}/_refresh")
    print("Index refreshed. Seeding complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed products index")
    parser.add_argument("--gateway", action="store_true", help="Send through gateway (port 9201)")
    parser.add_argument("--count", type=int, default=100, help="Number of products to seed")
    args = parser.parse_args()

    if args.gateway:
        url = f"http://localhost:{GATEWAY_PORT}"
        print(f"Seeding through gateway at {url}")
    else:
        url = ES_HOST
        print(f"Seeding directly to ES at {url}")

    seed(url, args.count)
