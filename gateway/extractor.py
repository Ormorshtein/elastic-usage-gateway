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

# --- Lookback parsing for time-range queries ---

_LOOKBACK_RE = re.compile(r"^now-(\d+)([smhdw])$")
_TIME_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_BOOST_RE = re.compile(r"\^[\d.]+$")


@dataclass
class LookbackInfo:
    """Extracted time-range lookback from a query (e.g., now-24h → 86400s)."""
    seconds: float
    field: str
    label: str  # human-readable, e.g. "24h", "7d"


def _parse_lookback(value) -> tuple[float, str] | None:
    """Parse 'now-Xh' style values. Returns (seconds, label) or None."""
    if not isinstance(value, str):
        return None
    m = _LOOKBACK_RE.match(value.strip())
    if not m:
        return None
    num, unit = m.group(1), m.group(2)
    return int(num) * _TIME_UNITS[unit], f"{num}{unit}"


# ES internal fields we never report as user fields
_INTERNAL_FIELDS = {"_score", "_doc", "_id", "_index", "_type", "_source", "_version"}

# Leaf query types where the key under them is a field name
_LEAF_QUERY_TYPES = {
    "match", "match_phrase", "match_phrase_prefix",
    "term", "terms", "range",
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
    lookback: LookbackInfo | None = None

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


def _extract_query_fields(
    query: dict,
    refs: FieldRefs,
    context: str = "queried",
    lookbacks: list[LookbackInfo] | None = None,
) -> None:
    """
    Recursively extract field names from a query clause.

    Args:
        query: The query dict to walk.
        refs: FieldRefs to populate.
        context: "queried" or "filtered" — determines which set fields go into.
        lookbacks: Accumulator for time-range lookback candidates.
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
                        _extract_query_fields(clause, refs, context, lookbacks)
                elif isinstance(clauses, dict):
                    _extract_query_fields(clauses, refs, context, lookbacks)
            # filter clause → fields go to "filtered" context
            filter_clauses = value.get("filter")
            if isinstance(filter_clauses, list):
                for clause in filter_clauses:
                    _extract_query_fields(clause, refs, "filtered", lookbacks)
            elif isinstance(filter_clauses, dict):
                _extract_query_fields(filter_clauses, refs, "filtered", lookbacks)
            continue

        # Nested query
        if key == "nested" and isinstance(value, dict):
            nested_query = value.get("query")
            if nested_query:
                _extract_query_fields(nested_query, refs, context, lookbacks)
            continue

        # Resolve target set based on context
        target = refs.filtered if context == "filtered" else refs.queried

        # Leaf query types — the key under them is the field name
        if key in _LEAF_QUERY_TYPES and isinstance(value, dict):
            for field_name, field_body in value.items():
                if _is_user_field(field_name):
                    target.add(field_name)
                # Extract lookback from range queries (check all bound types)
                if key == "range" and lookbacks is not None and isinstance(field_body, dict):
                    for bound_key in ("gte", "gt", "lte", "lt"):
                        parsed = _parse_lookback(field_body.get(bound_key))
                        if parsed is not None:
                            secs, label = parsed
                            lookbacks.append(LookbackInfo(seconds=secs, field=field_name, label=label))
                            break
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
                clean = _BOOST_RE.sub("", f)
                if _is_user_field(clean):
                    target.add(clean)
            continue


def _extract_agg_fields(aggs: dict, target: set[str], refs: FieldRefs | None = None) -> None:
    """Extract field names from aggregation definitions."""
    if not isinstance(aggs, dict):
        return

    for agg_name, agg_def in aggs.items():
        if not isinstance(agg_def, dict):
            continue

        for agg_type in (*_METRIC_AGG_TYPES, *_BUCKET_AGG_TYPES):
            if agg_type not in agg_def:
                continue
            agg_body = agg_def[agg_type]
            if not isinstance(agg_body, dict):
                continue

            field_name = agg_body.get("field")
            if field_name and _is_user_field(field_name):
                target.add(field_name)

            # Composite agg: fields are nested inside sources array
            if agg_type == "composite":
                sources = agg_body.get("sources")
                if isinstance(sources, list):
                    for source_item in sources:
                        if not isinstance(source_item, dict):
                            continue
                        for _source_name, source_spec in source_item.items():
                            if not isinstance(source_spec, dict):
                                continue
                            for _inner_type, inner_body in source_spec.items():
                                if isinstance(inner_body, dict):
                                    f = inner_body.get("field")
                                    if f and _is_user_field(f):
                                        target.add(f)

            # Filter agg: body is a query, not {"field": ...}
            if agg_type == "filter" and refs is not None:
                _extract_query_fields(agg_body, refs, context="filtered")

            # Filters agg: named filter queries
            if agg_type == "filters" and refs is not None:
                filters_dict = agg_body.get("filters")
                if isinstance(filters_dict, dict):
                    for _filter_name, filter_query in filters_dict.items():
                        if isinstance(filter_query, dict):
                            _extract_query_fields(filter_query, refs, context="filtered")

        # Recurse into sub-aggregations
        for sub_key in ("aggs", "aggregations"):
            sub_aggs = agg_def.get(sub_key)
            if sub_aggs:
                _extract_agg_fields(sub_aggs, target, refs)


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
    lookbacks: list[LookbackInfo] = []

    query = body.get("query")
    if query:
        _extract_query_fields(query, refs, context="queried", lookbacks=lookbacks)

    # Post-filter is always filter context
    post_filter = body.get("post_filter")
    if post_filter:
        _extract_query_fields(post_filter, refs, context="filtered", lookbacks=lookbacks)

    for agg_key in ("aggs", "aggregations"):
        aggs = body.get(agg_key)
        if aggs:
            _extract_agg_fields(aggs, refs.aggregated, refs)

    sort = body.get("sort")
    if sort:
        _extract_sort_fields(sort, refs.sorted)

    source = body.get("_source")
    if source is not None:
        _extract_source_fields(source, refs.sourced)

    # docvalue_fields — Kibana Discover uses this heavily (string or {"field": ..., "format": ...})
    docvalue_fields = body.get("docvalue_fields")
    if isinstance(docvalue_fields, list):
        for item in docvalue_fields:
            if isinstance(item, str) and _is_user_field(item):
                refs.sourced.add(item)
            elif isinstance(item, dict):
                f = item.get("field")
                if f and _is_user_field(f):
                    refs.sourced.add(f)

    # stored_fields — explicit stored field retrieval
    stored_fields = body.get("stored_fields")
    if isinstance(stored_fields, list):
        for f in stored_fields:
            if _is_user_field(f):
                refs.sourced.add(f)

    # highlight — fields used for search result snippets
    highlight = body.get("highlight")
    if isinstance(highlight, dict):
        hl_fields = highlight.get("fields")
        if isinstance(hl_fields, dict):
            for field_name in hl_fields:
                if _is_user_field(field_name):
                    refs.queried.add(field_name)

    # suggest — completion, term, phrase suggesters
    suggest = body.get("suggest")
    if isinstance(suggest, dict):
        for suggest_name, suggest_def in suggest.items():
            if not isinstance(suggest_def, dict):
                continue
            for suggest_type in ("completion", "term", "phrase"):
                inner = suggest_def.get(suggest_type)
                if isinstance(inner, dict):
                    f = inner.get("field")
                    if f and _is_user_field(f):
                        refs.queried.add(f)

    # collapse — field collapsing (e.g., one result per brand)
    collapse = body.get("collapse")
    if isinstance(collapse, dict):
        f = collapse.get("field")
        if f and _is_user_field(f):
            refs.filtered.add(f)

    # Pick widest lookback window
    if lookbacks:
        refs.lookback = max(lookbacks, key=lambda lb: lb.seconds)

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
    if operation in ("search", "count", "async_search", "update_by_query", "delete_by_query"):
        try:
            parsed = json.loads(body) if body else {}
            if isinstance(parsed, dict):
                refs = extract_fields_from_search(parsed)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.debug("Could not parse body as JSON for %s", path)

    elif operation == "msearch":
        # msearch uses NDJSON: alternating header/query lines
        refs = _extract_from_msearch(body)

    elif operation == "doc":
        # Distinguish read vs write for single-doc operations
        if method.upper() in ("PUT", "POST"):
            operation = "doc_write"
            try:
                parsed = json.loads(body) if body else {}
                if isinstance(parsed, dict):
                    refs = extract_fields_from_document(parsed)
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.debug("Could not parse body as JSON for %s", path)
        else:
            operation = "doc_get"

    elif operation == "update" and method.upper() == "POST":
        # Single-doc update: POST /<index>/_update/<id>
        # Body is {"doc": {fields...}} and/or {"upsert": {fields...}}
        try:
            parsed = json.loads(body) if body else {}
            if isinstance(parsed, dict):
                refs = FieldRefs()
                for wrapper_key in ("doc", "upsert"):
                    inner = parsed.get(wrapper_key)
                    if isinstance(inner, dict):
                        for key in inner:
                            if _is_user_field(key):
                                refs.written.add(key)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.debug("Could not parse body as JSON for %s", path)

    elif operation == "bulk":
        default_index = indices[0] if indices else None
        refs = _extract_from_bulk(body, default_index)

    return indices, operation, refs


def _extract_from_bulk(body: bytes, default_index: str | None) -> FieldRefs:
    """
    Extract field references from a _bulk request body (NDJSON).

    Bulk format is alternating lines:
      Line 1: action/metadata  {"index": {"_index": "products", "_id": "1"}}
      Line 2: document body    {"title": "Laptop", "price": 999}
      Line 3: action/metadata  {"delete": {"_index": "products", "_id": "2"}}
      (no body for delete)
      Line 4: action/metadata  {"update": {"_index": "products", "_id": "3"}}
      Line 5: update body      {"doc": {"title": "Updated Name"}}

    For update actions, the actual document fields are nested inside "doc"
    or "upsert" wrappers — we unwrap those before extracting fields.
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
        # Parse the action/metadata line
        try:
            action_line = json.loads(lines[i])
        except json.JSONDecodeError:
            i += 1
            continue

        # Action is one of: index, create, update, delete
        action_type = next(iter(action_line), None)
        if action_type == "delete":
            # Delete has no document body — skip to next action
            i += 1
            continue

        # Next line is the document body
        i += 1
        if i < len(lines):
            try:
                doc = json.loads(lines[i])
                if isinstance(doc, dict):
                    if action_type == "update":
                        # Update wraps fields in {"doc": {...}} or {"upsert": {...}}
                        for wrapper_key in ("doc", "upsert"):
                            inner = doc.get(wrapper_key)
                            if isinstance(inner, dict):
                                for key in inner:
                                    if _is_user_field(key):
                                        refs.written.add(key)
                    else:
                        # index/create: fields are top-level
                        for key in doc:
                            if _is_user_field(key):
                                refs.written.add(key)
            except json.JSONDecodeError:
                pass

        i += 1

    return refs


def _extract_from_msearch(body: bytes) -> FieldRefs:
    """
    Extract field references from a _msearch request body (NDJSON).

    msearch format is alternating lines:
      Line 1: header  {"index": "products"}
      Line 2: query   {"query": {"match": {"title": "laptop"}}}
      Line 3: header  {"index": "logs"}
      Line 4: query   {"query": {"term": {"level": "ERROR"}}}

    We parse each query line as a search body and merge all field refs.
    """
    refs = FieldRefs()
    if not body:
        return refs

    try:
        lines = body.decode("utf-8").strip().split("\n")
    except UnicodeDecodeError:
        return refs

    # Process query lines (odd-indexed: 1, 3, 5, ...)
    for i in range(1, len(lines), 2):
        line = lines[i].strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                query_refs = extract_fields_from_search(parsed)
                refs.queried.update(query_refs.queried)
                refs.filtered.update(query_refs.filtered)
                refs.aggregated.update(query_refs.aggregated)
                refs.sorted.update(query_refs.sorted)
                refs.sourced.update(query_refs.sourced)
                # Keep widest lookback
                if query_refs.lookback:
                    if refs.lookback is None or query_refs.lookback.seconds > refs.lookback.seconds:
                        refs.lookback = query_refs.lookback
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    return refs
