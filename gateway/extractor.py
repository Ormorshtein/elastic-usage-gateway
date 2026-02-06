"""
DSL field extractor — walks Elasticsearch query DSL JSON to find field references.

This is NOT a full Query DSL parser. It handles the most common patterns:
- match, term, terms, range, exists, wildcard, prefix, fuzzy, match_phrase
- bool (must, should, must_not, filter)
- aggregations (terms, avg, sum, min, max, cardinality, value_count, stats,
  extended_stats, date_histogram, histogram, range)
- sort
- _source

Unknown structures are skipped silently. The extractor never raises — it
returns what it can find.
"""

from __future__ import annotations
import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ES internal fields we never report as user fields
_INTERNAL_FIELDS = {"_score", "_doc", "_id", "_index", "_type", "_source", "_version"}

# Leaf query types where the key under them is a field name
_LEAF_QUERY_TYPES = {
    "match", "match_phrase", "match_phrase_prefix",
    "term", "terms", "range", "exists",
    "wildcard", "prefix", "fuzzy", "regexp",
}

# Metric aggregation types that have a "field" key
_METRIC_AGG_TYPES = {
    "avg", "sum", "min", "max", "cardinality", "value_count",
    "stats", "extended_stats", "percentiles", "percentile_ranks",
    "median_absolute_deviation", "top_hits",
}

# Bucket aggregation types that have a "field" key
_BUCKET_AGG_TYPES = {
    "terms", "date_histogram", "histogram", "range",
    "date_range", "filter", "filters", "significant_terms",
    "composite", "auto_date_histogram",
}


@dataclass
class FieldRefs:
    """Collected field references categorized by usage type."""
    queried: set[str] = field(default_factory=set)
    filtered: set[str] = field(default_factory=set)
    aggregated: set[str] = field(default_factory=set)
    sorted: set[str] = field(default_factory=set)
    sourced: set[str] = field(default_factory=set)
    written: set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        return {
            "queried": sorted(self.queried),
            "filtered": sorted(self.filtered),
            "aggregated": sorted(self.aggregated),
            "sorted": sorted(self.sorted),
            "sourced": sorted(self.sourced),
            "written": sorted(self.written),
        }

    @property
    def all_fields(self) -> set[str]:
        return (
            self.queried | self.filtered | self.aggregated
            | self.sorted | self.sourced | self.written
        )


def _is_user_field(name: str) -> bool:
    """Return True if name looks like a user-defined field (not internal)."""
    return name not in _INTERNAL_FIELDS and not name.startswith("_")


def _extract_query_fields(query: dict, refs: FieldRefs, context: str = "queried") -> None:
    """
    Recursively extract field names from a query clause.

    Args:
        query: The query dict to walk.
        refs: FieldRefs to populate.
        context: "queried" or "filtered" — determines which set fields go into.
    """
    if not isinstance(query, dict):
        return

    for key, value in query.items():
        # Bool compound query — recurse into each clause list
        if key == "bool" and isinstance(value, dict):
            # must/should/must_not preserve current context
            for clause_type in ("must", "should", "must_not"):
                clauses = value.get(clause_type)
                if isinstance(clauses, list):
                    for clause in clauses:
                        _extract_query_fields(clause, refs, context)
                elif isinstance(clauses, dict):
                    _extract_query_fields(clauses, refs, context)
            # filter clause → fields go to "filtered" context
            filter_clauses = value.get("filter")
            if isinstance(filter_clauses, list):
                for clause in filter_clauses:
                    _extract_query_fields(clause, refs, "filtered")
            elif isinstance(filter_clauses, dict):
                _extract_query_fields(filter_clauses, refs, "filtered")
            continue

        # Nested query
        if key == "nested" and isinstance(value, dict):
            nested_query = value.get("query")
            if nested_query:
                _extract_query_fields(nested_query, refs, context)
            continue

        # Resolve target set based on context
        target = refs.filtered if context == "filtered" else refs.queried

        # Leaf query types — the key under them is the field name
        if key in _LEAF_QUERY_TYPES and isinstance(value, dict):
            for field_name in value:
                if _is_user_field(field_name):
                    target.add(field_name)
            continue

        # exists has a "field" key
        if key == "exists" and isinstance(value, dict):
            field_name = value.get("field")
            if field_name and _is_user_field(field_name):
                target.add(field_name)
            continue

        # multi_match has a "fields" list
        if key == "multi_match" and isinstance(value, dict):
            fields = value.get("fields", [])
            for f in fields:
                # Strip boost suffix like "title^2"
                clean = re.sub(r"\^[\d.]+$", "", f)
                if _is_user_field(clean):
                    target.add(clean)
            continue


def _extract_agg_fields(aggs: dict, target: set[str]) -> None:
    """Extract field names from aggregation definitions."""
    if not isinstance(aggs, dict):
        return

    for agg_name, agg_def in aggs.items():
        if not isinstance(agg_def, dict):
            continue

        for agg_type in (*_METRIC_AGG_TYPES, *_BUCKET_AGG_TYPES):
            if agg_type in agg_def:
                agg_body = agg_def[agg_type]
                if isinstance(agg_body, dict):
                    field_name = agg_body.get("field")
                    if field_name and _is_user_field(field_name):
                        target.add(field_name)

        # Recurse into sub-aggregations
        for sub_key in ("aggs", "aggregations"):
            sub_aggs = agg_def.get(sub_key)
            if sub_aggs:
                _extract_agg_fields(sub_aggs, target)


def _extract_sort_fields(sort: list | dict, target: set[str]) -> None:
    """Extract field names from sort clauses."""
    if isinstance(sort, dict):
        for field_name in sort:
            if _is_user_field(field_name):
                target.add(field_name)
    elif isinstance(sort, list):
        for item in sort:
            if isinstance(item, str) and _is_user_field(item):
                target.add(item)
            elif isinstance(item, dict):
                for field_name in item:
                    if _is_user_field(field_name):
                        target.add(field_name)


def _extract_source_fields(source, target: set[str]) -> None:
    """Extract field names from _source specification."""
    if isinstance(source, list):
        for f in source:
            if _is_user_field(f):
                target.add(f)
    elif isinstance(source, dict):
        for key in ("includes", "excludes"):
            fields = source.get(key, [])
            if isinstance(fields, list):
                for f in fields:
                    if _is_user_field(f):
                        target.add(f)


def extract_fields_from_search(body: dict) -> FieldRefs:
    """Extract field references from a _search request body."""
    refs = FieldRefs()

    query = body.get("query")
    if query:
        _extract_query_fields(query, refs, context="queried")

    # Post-filter is always filter context
    post_filter = body.get("post_filter")
    if post_filter:
        _extract_query_fields(post_filter, refs, context="filtered")

    for agg_key in ("aggs", "aggregations"):
        aggs = body.get(agg_key)
        if aggs:
            _extract_agg_fields(aggs, refs.aggregated)

    sort = body.get("sort")
    if sort:
        _extract_sort_fields(sort, refs.sorted)

    source = body.get("_source")
    if source is not None:
        _extract_source_fields(source, refs.sourced)

    return refs


def extract_fields_from_document(body: dict) -> FieldRefs:
    """Extract field names from an indexing request body (document fields)."""
    refs = FieldRefs()
    for key in body:
        if _is_user_field(key):
            refs.written.add(key)
    return refs


def parse_path(path: str) -> tuple[list[str] | None, str | None]:
    """
    Parse an ES URL path to extract index name(s) and operation.

    Returns:
        (indices, operation) — either can be None.
        indices is a list to support comma-separated multi-index paths.

    Examples:
        /products/_search          -> (["products"], "search")
        /products,orders/_search   -> (["products", "orders"], "search")
        /products/_doc/123         -> (["products"], "doc")
        /_bulk                     -> (None, "bulk")
        /_cluster/health           -> (None, "cluster")
        /products                  -> (["products"], None)
    """
    # Strip leading slash
    path = path.lstrip("/")
    if not path:
        return None, None

    parts = path.split("/")

    # System endpoints: _bulk, _cluster, _cat, _nodes, etc.
    if parts[0].startswith("_"):
        operation = parts[0].lstrip("_")
        return None, operation

    # Handle comma-separated multi-index: "products,orders"
    raw_index = parts[0]
    indices = [idx.strip() for idx in raw_index.split(",") if idx.strip()]
    if not indices:
        return None, None

    if len(parts) < 2:
        return indices, None

    operation = parts[1].lstrip("_")
    return indices, operation


def extract_from_request(
    path: str, method: str, body: bytes
) -> tuple[list[str] | None, str, FieldRefs]:
    """
    Top-level extraction: given a raw request, return indices, operation, and field refs.

    Returns:
        (indices, operation, FieldRefs)
        indices is a list of index names (supports multi-index paths like /a,b/_search).
    """
    indices, operation = parse_path(path)
    operation = operation or "other"
    refs = FieldRefs()

    # Map operation names to categories
    if operation in ("search", "msearch", "count"):
        try:
            parsed = json.loads(body) if body else {}
            if isinstance(parsed, dict):
                refs = extract_fields_from_search(parsed)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.debug("Could not parse body as JSON for %s", path)

    elif operation == "doc" and method.upper() in ("PUT", "POST"):
        try:
            parsed = json.loads(body) if body else {}
            if isinstance(parsed, dict):
                refs = extract_fields_from_document(parsed)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.debug("Could not parse body as JSON for %s", path)

    elif operation == "bulk":
        default_index = indices[0] if indices else None
        refs = _extract_from_bulk(body, default_index)

    return indices, operation, refs


def _extract_from_bulk(body: bytes, default_index: str | None) -> FieldRefs:
    """
    Extract field references from a _bulk request body (ndjson).

    Bulk format: alternating action/metadata lines and document lines.
    We extract written fields from the document lines of index/create/update actions.
    """
    refs = FieldRefs()
    if not body:
        return refs

    try:
        lines = body.decode("utf-8").strip().split("\n")
    except UnicodeDecodeError:
        return refs

    i = 0
    while i < len(lines):
        try:
            action_line = json.loads(lines[i])
        except json.JSONDecodeError:
            i += 1
            continue

        # Action is one of: index, create, update, delete
        action_type = next(iter(action_line), None)
        if action_type == "delete":
            # Delete has no document body
            i += 1
            continue

        # Next line is the document body
        i += 1
        if i < len(lines):
            try:
                doc = json.loads(lines[i])
                if isinstance(doc, dict):
                    for key in doc:
                        if _is_user_field(key):
                            refs.written.add(key)
            except json.JSONDecodeError:
                pass

        i += 1

    return refs
