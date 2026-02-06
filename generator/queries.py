"""
Query templates with frequency weights for traffic generation.

Each template is a callable that returns a (method, path, body) tuple.
Weights control how often each query type is selected — this creates
the intentional skew needed to validate heat scores.

Expected heat outcome after running:
  Hot:    title, category, price
  Warm:   brand, rating, description
  Cold:   (none queried — stock_count, created_at, tags, subcategory are never queried)
  Unused: internal_sku, legacy_supplier_code
"""

import random
import json

INDEX = "products"

SEARCH_TERMS = [
    "wireless", "premium", "professional", "compact", "smart",
    "durable", "lightweight", "portable", "advanced", "eco-friendly",
]

CATEGORIES = ["electronics", "clothing", "home", "sports", "books"]

BRANDS = [
    "SoundMax", "TechPro", "EcoWear", "HomeStyle", "SportFit",
    "PageTurn", "BrightLife", "UrbanGear", "NaturePlus", "CoreTech",
]


def search_by_title():
    """40% — match query on title, with _source filtering."""
    term = random.choice(SEARCH_TERMS)
    body = {
        "query": {"match": {"title": term}},
        "_source": ["title", "price", "category"],
        "size": 10,
    }
    return "POST", f"/{INDEX}/_search", json.dumps(body)


def filter_by_category_sort_by_price():
    """25% — bool filter on category, sorted by price."""
    cat = random.choice(CATEGORIES)
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"category": cat}}
                ]
            }
        },
        "sort": [{"price": {"order": random.choice(["asc", "desc"])}}],
        "_source": ["title", "price", "category"],
        "size": 20,
    }
    return "POST", f"/{INDEX}/_search", json.dumps(body)


def aggregate_by_brand():
    """15% — terms aggregation on brand."""
    body = {
        "size": 0,
        "aggs": {
            "brands": {
                "terms": {"field": "brand", "size": 20}
            }
        }
    }
    return "POST", f"/{INDEX}/_search", json.dumps(body)


def range_on_rating():
    """10% — range query on rating."""
    min_rating = round(random.uniform(3.0, 4.5), 1)
    body = {
        "query": {
            "range": {
                "rating": {"gte": min_rating}
            }
        },
        "size": 10,
    }
    return "POST", f"/{INDEX}/_search", json.dumps(body)


def search_by_description():
    """5% — match query on description."""
    term = random.choice(SEARCH_TERMS)
    body = {
        "query": {"match": {"description": term}},
        "size": 5,
    }
    return "POST", f"/{INDEX}/_search", json.dumps(body)


def get_by_id():
    """3% — GET a single document by ID."""
    doc_id = random.randint(1, 100)
    return "GET", f"/{INDEX}/_doc/{doc_id}", None


def match_all():
    """2% — match_all with no field references."""
    body = {
        "query": {"match_all": {}},
        "size": 5,
    }
    return "POST", f"/{INDEX}/_search", json.dumps(body)


# Map of function names to functions — used by the UI for custom weights
QUERY_FUNCTIONS = {
    "search_by_title": search_by_title,
    "filter_by_category_sort_by_price": filter_by_category_sort_by_price,
    "aggregate_by_brand": aggregate_by_brand,
    "range_on_rating": range_on_rating,
    "search_by_description": search_by_description,
    "get_by_id": get_by_id,
    "match_all": match_all,
}

# Default weighted distribution
DEFAULT_WEIGHTS = {
    "search_by_title": 40,
    "filter_by_category_sort_by_price": 25,
    "aggregate_by_brand": 15,
    "range_on_rating": 10,
    "search_by_description": 5,
    "get_by_id": 3,
    "match_all": 2,
}

# Pre-compute for backwards compatibility
_weights = list(DEFAULT_WEIGHTS.values())
_funcs = list(QUERY_FUNCTIONS.values())


def random_query() -> tuple[str, str, str | None]:
    """
    Return a random (method, path, body) tuple according to the
    default weighted distribution.
    """
    func = random.choices(_funcs, weights=_weights, k=1)[0]
    return func()


def random_query_with_weights(weights: dict[str, int]) -> tuple[str, str, str | None]:
    """
    Return a random query using custom weights.

    Args:
        weights: dict mapping function names to integer weights.
                 e.g. {"search_by_title": 40, "match_all": 2}
    """
    funcs = []
    w = []
    for name, weight in weights.items():
        if name in QUERY_FUNCTIONS and weight > 0:
            funcs.append(QUERY_FUNCTIONS[name])
            w.append(weight)
    if not funcs:
        return random_query()
    func = random.choices(funcs, weights=w, k=1)[0]
    return func()
