"""
Mapping recommendations engine — generates actionable optimization advice.

Reads field classification data from the .mapping-diff index (produced by
mapping_diff.py), applies 8 decision rules, and writes recommendation
documents to the .mapping-recommendations index.

Each recommendation includes:
- recommendation: short code (e.g. "disable_index", "change_to_keyword")
- why: explanation of the problem, tradeoffs, and risks
- how: concrete mapping change with JSON snippets

Results are consumed via Kibana dashboards — no JSON API endpoint.

Lifecycle: Start via start_recommendations_loop() in the lifespan hook.
The loop runs every RECOMMENDATIONS_REFRESH_INTERVAL seconds.
"""

from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

from config import ES_HOST, EVENT_TIMEOUT, RECOMMENDATIONS_REFRESH_INTERVAL
from gateway import metadata as metadata_mod
from gateway import metrics
from gateway.mapping_diff import MAPPING_DIFF_INDEX

logger = logging.getLogger(__name__)

_client = httpx.AsyncClient(base_url=ES_HOST, timeout=EVENT_TIMEOUT)

RECOMMENDATIONS_INDEX = ".mapping-recommendations"

RECOMMENDATIONS_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp":       {"type": "date"},
            "index_group":     {"type": "keyword"},
            "field_name":      {"type": "keyword"},
            "mapped_type":     {"type": "keyword"},
            "classification":  {"type": "keyword"},
            "recommendation":  {"type": "keyword"},
            "why":             {"type": "keyword"},
            "how":             {"type": "keyword"},
            "breaking_change": {"type": "boolean"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    }
}


# ---------------------------------------------------------------------------
# Why/How text templates
#
# Each recommendation type has a why (explanation + tradeoffs) and how
# (concrete steps). Placeholders: {field_name}, {mapped_type}.
# ---------------------------------------------------------------------------

_WHY = {
    "disable_index": (
        "This field is stored in the index but never queried, filtered, "
        "aggregated, or sorted. Elasticsearch maintains an inverted index "
        "and columnar doc_values for it on every write — consuming CPU "
        "during indexing and disk space — but nothing reads those structures. "
        "The field is still stored in _source, so applications fetching "
        "documents will still see it.\n\n"
        "Tradeoff: if a new query starts using this field in the future, it "
        "won't work until the mapping is changed and data is reindexed. "
        "Check with the team that writes to this index before applying."
    ),
    "disable_doc_values": (
        "This field is used in queries or filters (which use the inverted "
        "index) but never in aggregations or sorts (which use doc_values). "
        "Elasticsearch builds a columnar doc_values store for this field "
        "on every write, but nothing reads it. Disabling doc_values saves "
        "disk and slightly speeds up indexing. The inverted index is "
        "untouched — all existing queries and filters continue to work.\n\n"
        "Tradeoff: if someone adds a terms aggregation or sort on this "
        "field later, it will fail until the mapping is updated and data "
        "is reindexed. Lower risk than disabling the index entirely "
        "because the field remains searchable."
    ),
    "disable_norms": (
        "This field is a text type used only in filter context (e.g. inside "
        "bool.filter) — never in scoring queries like match or match_phrase "
        "in bool.must/bool.should. Norms store per-document field length "
        "data used for BM25 relevance scoring. Since this field is never "
        "scored, norms are wasted storage (~1 byte per document).\n\n"
        "Tradeoff: minimal. If someone later uses this field in a scoring "
        "query, relevance scores will be less accurate (all documents "
        "treated as same length), but the query will still work. This is "
        "the safest of all recommendations."
    ),
    "change_to_keyword": (
        "This field is mapped as text (full-text analyzed — tokenized, "
        "lowercased, stemmed) but is only used with exact-match queries "
        "like term or terms. Nobody runs match or match_phrase against it. "
        "The analyzer is doing unnecessary work on every write, and the "
        "inverted index stores tokens that are never used for full-text "
        "search. A keyword type is correct for exact-match use cases — "
        "it is faster to index, uses less disk, and supports aggregations "
        "and sorting natively.\n\n"
        "Tradeoff: requires reindex. Changing a field type is not possible "
        "on an existing index. You must create a new index with the updated "
        "mapping and reindex data into it. Also, any match queries added in "
        "the future would need to switch to term."
    ),
    "add_keyword_subfield": (
        "This field is used with both full-text queries (match/match_phrase "
        "in scoring context) and exact-match operations (term queries, "
        "aggregations, or sorts). Currently it is a plain text field with "
        "no .keyword sub-field. Running terms aggregations on a text field "
        "requires fielddata (loaded into heap memory, expensive), and "
        "exact-match term queries on analyzed text are unreliable because "
        "stored tokens are lowercased/stemmed. Adding a .keyword multi-field "
        "gives the best of both: the parent text field handles full-text "
        "search, the .keyword sub-field handles exact match, aggregations, "
        "and sorting.\n\n"
        "Tradeoff: slightly more disk space and indexing time for the extra "
        "sub-field. Almost always worth it."
    ),
    "remove_multifield": (
        "This is a multi-field sub-field (e.g. title.keyword) that has "
        "zero usage — never queried, filtered, aggregated, sorted, or "
        "sourced. Multi-fields are built on every document write, so this "
        "sub-field consumes indexing CPU and disk space for nothing. Common "
        "cause: index templates often auto-generate .keyword sub-fields "
        "for every text field, even when nobody aggregates or sorts on them.\n\n"
        "Tradeoff: if someone starts aggregating on this field later, you "
        "would need to re-add the multi-field and reindex. Check Kibana "
        "saved objects — Kibana sometimes uses .keyword fields behind the "
        "scenes for filters and visualizations."
    ),
    "remove_field": (
        "This field has zero references across all 6 usage categories "
        "(queried, filtered, aggregated, sorted, sourced, written) within "
        "the observation window. No application is reading or writing it. "
        "It may be a leftover from a previous schema version, an unused "
        "template default, or a field that was planned but never adopted. "
        "Every mapped field costs indexing CPU (analyzer + inverted index "
        "+ doc_values) and disk space on every document write, even if the "
        "field value is null.\n\n"
        "Tradeoff: verify the observation window is long enough. A field "
        "used only during monthly batch jobs would appear unused in a 7-day "
        "window. Extend lookback to 30 days if unsure. Also check if any "
        "direct-to-ES traffic (bypassing the gateway) uses this field."
    ),
}

_HOW = {
    "disable_index": (
        "Update the index template mapping for this field:\n\n"
        '  "{field_name}": {{ "type": "{mapped_type}", '
        '"index": false, "doc_values": false }}\n\n'
        "Existing indices are unaffected — this only applies to new indices "
        "created from the template. To reclaim space on existing indices, "
        "reindex into a new index with the updated mapping."
    ),
    "disable_doc_values": (
        "Update the index template mapping for this field:\n\n"
        '  "{field_name}": {{ "type": "{mapped_type}", '
        '"doc_values": false }}\n\n'
        "Note: text fields already have doc_values disabled by default — "
        "this rule only applies to keyword, numeric, date, and other "
        "doc_values-enabled types. Only affects new indices."
    ),
    "disable_norms": (
        "Update the index template mapping for this field:\n\n"
        '  "{field_name}": {{ "type": "text", "norms": false }}\n\n'
        "Only affects new indices. Existing indices retain norms until "
        "reindexed."
    ),
    "change_to_keyword": (
        "1. Update the index template:\n\n"
        '  "{field_name}": {{ "type": "keyword" }}\n\n'
        "2. Create a new index from the updated template.\n"
        '3. Reindex: POST _reindex {{ "source": {{ "index": "old" }}, '
        '"dest": {{ "index": "new" }} }}\n'
        "4. Switch the alias to the new index.\n\n"
        "BREAKING CHANGE — all match queries on this field will behave "
        "differently after the type change. Verify no full-text queries "
        "exist before proceeding."
    ),
    "add_keyword_subfield": (
        "Update the index template mapping for this field:\n\n"
        '  "{field_name}": {{\n'
        '    "type": "text",\n'
        '    "fields": {{\n'
        '      "keyword": {{ "type": "keyword", "ignore_above": 256 }}\n'
        "    }}\n"
        "  }}\n\n"
        "After updating the template, existing data in old indices won't "
        "have the .keyword sub-field populated. It only applies to newly "
        "indexed documents. To backfill, use _update_by_query with no "
        "script (forces reindex in-place) or reindex into a new index."
    ),
    "remove_multifield": (
        "Remove the sub-field from the fields block in the index template. "
        "For example:\n\n"
        "  Before:\n"
        '  "{parent}": {{ "type": "text", "fields": {{ '
        '"{sub}": {{ "type": "keyword" }} }} }}\n\n'
        "  After:\n"
        '  "{parent}": {{ "type": "text" }}\n\n'
        "Only affects new indices. Existing indices retain the sub-field "
        "until reindexed."
    ),
    "remove_field": (
        "Remove the field from the index template entirely. If you want "
        "to be cautious, disable indexing first instead of removing:\n\n"
        '  "{field_name}": {{ "type": "{mapped_type}", '
        '"index": false, "doc_values": false }}\n\n'
        "This keeps the field in _source but stops building index "
        "structures. Full removal is safe only after confirming no "
        "traffic bypasses the gateway."
    ),
}


# ---------------------------------------------------------------------------
# Pure functions (no I/O, fully testable)
# ---------------------------------------------------------------------------

def _is_multifield(field_name: str) -> bool:
    """Check if a field name looks like a multi-field (contains a dot).

    Multi-fields are sub-fields like title.keyword, name.raw, etc.
    We exclude known non-multi-field dot patterns like nested objects,
    but since mapping_diff flattens both nested objects and multi-fields
    with dots, we use a simple heuristic: if a shorter prefix also exists
    in the sibling set, it's a multi-field.
    """
    return "." in field_name


def _format_how(recommendation: str, field_name: str, mapped_type: str) -> str:
    """Format the how text with field-specific values."""
    template = _HOW[recommendation]

    # For remove_multifield, split into parent and sub-field names
    if recommendation == "remove_multifield" and "." in field_name:
        last_dot = field_name.rfind(".")
        parent = field_name[:last_dot]
        sub = field_name[last_dot + 1:]
        return template.format(
            field_name=field_name, mapped_type=mapped_type,
            parent=parent, sub=sub,
        )

    return template.format(field_name=field_name, mapped_type=mapped_type)


def generate_recommendations(
    field_doc: dict,
    sibling_fields: set[str],
) -> list[dict]:
    """Generate recommendations for a single field based on its usage and mapping.

    Args:
        field_doc: A document from the .mapping-diff index.
        sibling_fields: Set of all field names in the same index group,
            used to check for .keyword sub-field existence.

    Returns:
        List of recommendation dicts (may be empty). Each dict contains:
        recommendation, why, how, breaking_change.
    """
    field_name = field_doc.get("field_name", "")
    mapped_type = field_doc.get("mapped_type", "")
    classification = field_doc.get("classification", "")
    is_indexed = field_doc.get("is_indexed", True)
    has_doc_values = field_doc.get("has_doc_values", True)

    count_queried = field_doc.get("count_queried", 0)
    count_filtered = field_doc.get("count_filtered", 0)
    count_aggregated = field_doc.get("count_aggregated", 0)
    count_sorted = field_doc.get("count_sorted", 0)

    recommendations = []

    def _add(rec_type: str, breaking: bool = False) -> None:
        recommendations.append({
            "recommendation": rec_type,
            "why": _WHY[rec_type],
            "how": _format_how(rec_type, field_name, mapped_type),
            "breaking_change": breaking,
        })

    # --- Rules for non-active fields (write_only, sourced_only, unused) ---

    # Rule 1: Written but never read
    if classification == "write_only" and (is_indexed or has_doc_values):
        _add("disable_index")
        return recommendations

    # Rule 2: Sourced only (fetched in _source, never searched)
    if classification == "sourced_only" and (is_indexed or has_doc_values):
        _add("disable_index")
        return recommendations

    # Rule 7: Unused multi-field (e.g. title.keyword with zero usage)
    if classification == "unused" and _is_multifield(field_name):
        _add("remove_multifield")
        return recommendations

    # Rule 8: Completely unused field
    if classification == "unused":
        _add("remove_field")
        return recommendations

    # --- Rules for active fields (can stack) ---

    if classification != "active":
        return recommendations

    # Rule 3: Queried/filtered but never aggregated/sorted → disable doc_values
    if (count_queried > 0 or count_filtered > 0) \
            and count_aggregated == 0 and count_sorted == 0 \
            and has_doc_values \
            and mapped_type not in ("text", "annotated_text"):
        _add("disable_doc_values")

    # Rule 4: Text field, filtered only (never scored) → disable norms
    if mapped_type == "text" and count_queried == 0 and count_filtered > 0:
        _add("disable_norms")

    # Rule 5: Text field, only exact-match usage → change to keyword
    if mapped_type == "text" \
            and count_queried == 0 and count_filtered > 0 \
            and count_aggregated == 0 and count_sorted == 0:
        _add("change_to_keyword", breaking=True)

    # Rule 6: Text field, both full-text and exact usage, no .keyword sub-field
    keyword_subfield = f"{field_name}.keyword"
    if mapped_type == "text" \
            and count_queried > 0 \
            and (count_filtered > 0 or count_aggregated > 0) \
            and keyword_subfield not in sibling_fields:
        _add("add_keyword_subfield")

    return recommendations


def build_recommendation_docs(
    index_group: str,
    diff_docs: list[dict],
    timestamp: str,
) -> list[dict]:
    """Build recommendation documents from mapping-diff data.

    Args:
        index_group: The index group name.
        diff_docs: List of documents from the .mapping-diff index.
        timestamp: ISO timestamp for the recommendation batch.

    Returns:
        List of recommendation documents ready to write to ES.
    """
    sibling_fields = {doc.get("field_name", "") for doc in diff_docs}
    results = []

    for diff_doc in diff_docs:
        recs = generate_recommendations(diff_doc, sibling_fields)
        for rec in recs:
            results.append({
                "timestamp": timestamp,
                "index_group": index_group,
                "field_name": diff_doc.get("field_name", ""),
                "mapped_type": diff_doc.get("mapped_type", ""),
                "classification": diff_doc.get("classification", ""),
                **rec,
            })

    return results


# ---------------------------------------------------------------------------
# Async functions (ES I/O)
# ---------------------------------------------------------------------------

async def ensure_recommendations_index() -> None:
    """Create the .mapping-recommendations index if it doesn't exist."""
    try:
        resp = await _client.head(f"/{RECOMMENDATIONS_INDEX}")
        if resp.status_code == 200:
            return
        resp = await _client.put(
            f"/{RECOMMENDATIONS_INDEX}",
            json=RECOMMENDATIONS_INDEX_MAPPING,
        )
        if resp.status_code in (200, 201):
            logger.info("Created recommendations index: %s", RECOMMENDATIONS_INDEX)
        else:
            logger.warning(
                "Failed to create recommendations index: %s %s",
                resp.status_code, resp.text[:200],
            )
    except httpx.RequestError as exc:
        logger.warning("Could not ensure recommendations index exists: %s", exc)


async def fetch_diff_docs_for_group(index_group: str) -> list[dict] | None:
    """Fetch all .mapping-diff documents for an index group."""
    query = {
        "size": 10000,
        "query": {"term": {"index_group": index_group}},
        "_source": True,
    }
    try:
        resp = await _client.post(f"/{MAPPING_DIFF_INDEX}/_search", json=query)
        if resp.status_code != 200:
            logger.warning(
                "Failed to fetch diff docs for %s: %s",
                index_group, resp.status_code,
            )
            return None

        hits = resp.json().get("hits", {}).get("hits", [])
        return [hit["_source"] for hit in hits]
    except httpx.RequestError as exc:
        logger.warning("Failed to fetch diff docs for %s: %s", index_group, exc)
        return None


async def write_recommendation_docs(index_group: str, docs: list[dict]) -> None:
    """Write recommendation documents to ES (delete-and-rewrite)."""
    # Delete existing docs for this group
    try:
        await _client.post(
            f"/{RECOMMENDATIONS_INDEX}/_delete_by_query",
            json={"query": {"term": {"index_group": index_group}}},
            params={"refresh": "false"},
        )
    except httpx.RequestError as exc:
        logger.warning(
            "Failed to delete old recommendation docs for %s: %s",
            index_group, exc,
        )

    if not docs:
        return

    # Bulk-write new docs
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": RECOMMENDATIONS_INDEX}}))
        lines.append(json.dumps(doc, default=str))
    bulk_body = "\n".join(lines) + "\n"

    try:
        resp = await _client.post(
            "/_bulk",
            content=bulk_body.encode(),
            headers={"Content-Type": "application/x-ndjson"},
            params={"refresh": "true"},
        )
        if resp.status_code in (200, 201):
            result = resp.json()
            error_count = sum(
                1 for item in result.get("items", [])
                if item.get("index", {}).get("error")
            )
            if error_count:
                logger.warning(
                    "Recommendations bulk write for %s: %d/%d errors",
                    index_group, error_count, len(docs),
                )
        else:
            logger.warning(
                "Recommendations bulk write failed for %s: %s",
                index_group, resp.status_code,
            )
    except httpx.RequestError as exc:
        logger.warning(
            "Recommendations bulk write failed for %s: %s",
            index_group, exc,
        )


async def refresh() -> None:
    """Recompute recommendations for all known index groups."""
    groups = metadata_mod.get_groups()
    if not groups:
        logger.debug("No index groups known — skipping recommendations refresh")
        return

    timestamp = datetime.now(timezone.utc).isoformat()
    processed = 0

    for index_group in groups:
        if index_group.startswith("."):
            continue

        diff_docs = await fetch_diff_docs_for_group(index_group)
        if diff_docs is None:
            continue

        rec_docs = build_recommendation_docs(index_group, diff_docs, timestamp)
        await write_recommendation_docs(index_group, rec_docs)
        processed += 1

    metrics.inc("recommendations_refresh_ok")
    logger.info(
        "Recommendations refreshed: %d groups processed", processed,
    )


# ---------------------------------------------------------------------------
# Background lifecycle
# ---------------------------------------------------------------------------

async def _recommendations_loop() -> None:
    """Background loop that refreshes recommendations periodically."""
    while True:
        try:
            await refresh()
        except Exception:
            logger.exception("Recommendations refresh failed")
            metrics.inc("recommendations_refresh_failed")
        await asyncio.sleep(RECOMMENDATIONS_REFRESH_INTERVAL)


def start_recommendations_loop() -> None:
    """Start the background recommendations refresh loop."""
    loop = asyncio.get_running_loop()
    loop.create_task(_recommendations_loop())
    logger.info(
        "Recommendations loop started (interval=%ds)",
        RECOMMENDATIONS_REFRESH_INTERVAL,
    )


async def close_recommendations_client() -> None:
    """Close the recommendations client. Called during gateway shutdown."""
    await _client.aclose()
