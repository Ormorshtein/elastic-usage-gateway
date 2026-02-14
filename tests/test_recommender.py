"""Tests for gateway.recommender — recommendation rules and doc building."""

import pytest

from gateway.recommender import (
    generate_recommendations,
    build_recommendation_docs,
    _is_multifield,
    _format_how,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _field_doc(
    field_name: str = "price",
    mapped_type: str = "keyword",
    classification: str = "active",
    is_indexed: bool = True,
    has_doc_values: bool = True,
    count_queried: int = 0,
    count_filtered: int = 0,
    count_aggregated: int = 0,
    count_sorted: int = 0,
    count_sourced: int = 0,
    count_written: int = 0,
) -> dict:
    """Build a minimal .mapping-diff document for testing."""
    return {
        "field_name": field_name,
        "mapped_type": mapped_type,
        "classification": classification,
        "is_indexed": is_indexed,
        "has_doc_values": has_doc_values,
        "count_queried": count_queried,
        "count_filtered": count_filtered,
        "count_aggregated": count_aggregated,
        "count_sorted": count_sorted,
        "count_sourced": count_sourced,
        "count_written": count_written,
    }


def _rec_types(recs: list[dict]) -> list[str]:
    """Extract recommendation type codes from a list of recommendations."""
    return [r["recommendation"] for r in recs]


# ---------------------------------------------------------------------------
# TestIsMultifield
# ---------------------------------------------------------------------------

class TestIsMultifield:

    def test_simple_field(self):
        assert _is_multifield("price") is False

    def test_keyword_subfield(self):
        assert _is_multifield("title.keyword") is True

    def test_nested_path(self):
        assert _is_multifield("metadata.author") is True

    def test_deeply_nested(self):
        assert _is_multifield("a.b.c") is True


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 1: write-only → disable_index
# ---------------------------------------------------------------------------

class TestRule1WriteOnly:

    def test_write_only_with_index_recommends_disable(self):
        doc = _field_doc(
            classification="write_only", is_indexed=True, has_doc_values=True,
            count_written=50,
        )
        recs = generate_recommendations(doc, set())
        assert _rec_types(recs) == ["disable_index"]
        assert recs[0]["breaking_change"] is False

    def test_write_only_already_disabled_no_recommendation(self):
        doc = _field_doc(
            classification="write_only", is_indexed=False, has_doc_values=False,
            count_written=50,
        )
        recs = generate_recommendations(doc, set())
        assert recs == []

    def test_write_only_only_doc_values_enabled(self):
        doc = _field_doc(
            classification="write_only", is_indexed=False, has_doc_values=True,
            count_written=50,
        )
        recs = generate_recommendations(doc, set())
        assert _rec_types(recs) == ["disable_index"]


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 2: sourced-only → disable_index
# ---------------------------------------------------------------------------

class TestRule2SourcedOnly:

    def test_sourced_only_recommends_disable(self):
        doc = _field_doc(
            classification="sourced_only", is_indexed=True, has_doc_values=True,
            count_sourced=30,
        )
        recs = generate_recommendations(doc, set())
        assert _rec_types(recs) == ["disable_index"]

    def test_sourced_only_already_disabled(self):
        doc = _field_doc(
            classification="sourced_only", is_indexed=False, has_doc_values=False,
            count_sourced=30,
        )
        recs = generate_recommendations(doc, set())
        assert recs == []


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 3: disable_doc_values
# ---------------------------------------------------------------------------

class TestRule3DisableDocValues:

    def test_queried_never_aggregated_recommends_disable_doc_values(self):
        doc = _field_doc(
            mapped_type="keyword",
            count_queried=10, count_filtered=5,
            count_aggregated=0, count_sorted=0,
            has_doc_values=True,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_doc_values" in _rec_types(recs)

    def test_aggregated_field_no_disable_doc_values(self):
        doc = _field_doc(
            count_queried=10, count_aggregated=5,
            has_doc_values=True,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_doc_values" not in _rec_types(recs)

    def test_sorted_field_no_disable_doc_values(self):
        doc = _field_doc(
            count_filtered=10, count_sorted=3,
            has_doc_values=True,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_doc_values" not in _rec_types(recs)

    def test_doc_values_already_false_no_recommendation(self):
        doc = _field_doc(
            count_queried=10, count_aggregated=0, count_sorted=0,
            has_doc_values=False,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_doc_values" not in _rec_types(recs)

    def test_text_field_skipped_for_doc_values(self):
        """Text fields have doc_values=false by default, so rule 3 should skip them."""
        doc = _field_doc(
            mapped_type="text",
            count_queried=10, count_aggregated=0, count_sorted=0,
            has_doc_values=True,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_doc_values" not in _rec_types(recs)


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 4: disable_norms
# ---------------------------------------------------------------------------

class TestRule4DisableNorms:

    def test_text_filtered_only_recommends_disable_norms(self):
        doc = _field_doc(
            mapped_type="text",
            count_queried=0, count_filtered=15,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_norms" in _rec_types(recs)

    def test_text_with_scoring_queries_no_disable_norms(self):
        doc = _field_doc(
            mapped_type="text",
            count_queried=10, count_filtered=5,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_norms" not in _rec_types(recs)

    def test_keyword_field_no_disable_norms(self):
        """Norms recommendation only applies to text fields."""
        doc = _field_doc(
            mapped_type="keyword",
            count_queried=0, count_filtered=15,
        )
        recs = generate_recommendations(doc, set())
        assert "disable_norms" not in _rec_types(recs)


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 5: change_to_keyword
# ---------------------------------------------------------------------------

class TestRule5ChangeToKeyword:

    def test_text_only_exact_match_recommends_keyword(self):
        doc = _field_doc(
            mapped_type="text",
            count_queried=0, count_filtered=20,
            count_aggregated=0, count_sorted=0,
        )
        recs = generate_recommendations(doc, set())
        assert "change_to_keyword" in _rec_types(recs)
        keyword_rec = next(r for r in recs if r["recommendation"] == "change_to_keyword")
        assert keyword_rec["breaking_change"] is True

    def test_text_with_scoring_queries_no_keyword(self):
        doc = _field_doc(
            mapped_type="text",
            count_queried=5, count_filtered=20,
        )
        recs = generate_recommendations(doc, set())
        assert "change_to_keyword" not in _rec_types(recs)

    def test_text_with_aggregation_no_keyword(self):
        doc = _field_doc(
            mapped_type="text",
            count_queried=0, count_filtered=20,
            count_aggregated=5,
        )
        recs = generate_recommendations(doc, set())
        assert "change_to_keyword" not in _rec_types(recs)

    def test_keyword_field_no_change_to_keyword(self):
        doc = _field_doc(
            mapped_type="keyword",
            count_queried=0, count_filtered=20,
        )
        recs = generate_recommendations(doc, set())
        assert "change_to_keyword" not in _rec_types(recs)


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 6: add_keyword_subfield
# ---------------------------------------------------------------------------

class TestRule6AddKeywordSubfield:

    def test_text_queried_and_filtered_no_keyword_sibling(self):
        doc = _field_doc(
            field_name="title", mapped_type="text",
            count_queried=10, count_filtered=5,
        )
        siblings = {"title", "price", "category"}
        recs = generate_recommendations(doc, siblings)
        assert "add_keyword_subfield" in _rec_types(recs)

    def test_text_queried_and_aggregated_no_keyword_sibling(self):
        doc = _field_doc(
            field_name="title", mapped_type="text",
            count_queried=10, count_aggregated=5,
        )
        siblings = {"title", "price"}
        recs = generate_recommendations(doc, siblings)
        assert "add_keyword_subfield" in _rec_types(recs)

    def test_text_with_keyword_sibling_no_recommendation(self):
        doc = _field_doc(
            field_name="title", mapped_type="text",
            count_queried=10, count_filtered=5,
        )
        siblings = {"title", "title.keyword", "price"}
        recs = generate_recommendations(doc, siblings)
        assert "add_keyword_subfield" not in _rec_types(recs)

    def test_text_only_queried_no_exact_match_no_recommendation(self):
        """Only scored queries, no filtering or aggregation — no need for .keyword."""
        doc = _field_doc(
            field_name="title", mapped_type="text",
            count_queried=10, count_filtered=0, count_aggregated=0,
        )
        siblings = {"title"}
        recs = generate_recommendations(doc, siblings)
        assert "add_keyword_subfield" not in _rec_types(recs)

    def test_keyword_field_no_add_subfield(self):
        doc = _field_doc(
            field_name="category", mapped_type="keyword",
            count_queried=10, count_filtered=5,
        )
        recs = generate_recommendations(doc, {"category"})
        assert "add_keyword_subfield" not in _rec_types(recs)


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 7: remove_multifield
# ---------------------------------------------------------------------------

class TestRule7RemoveMultifield:

    def test_unused_multifield_recommends_removal(self):
        doc = _field_doc(
            field_name="title.keyword", mapped_type="keyword",
            classification="unused",
        )
        recs = generate_recommendations(doc, {"title", "title.keyword"})
        assert _rec_types(recs) == ["remove_multifield"]

    def test_active_multifield_no_recommendation(self):
        doc = _field_doc(
            field_name="title.keyword", mapped_type="keyword",
            classification="active", count_aggregated=5,
        )
        recs = generate_recommendations(doc, {"title", "title.keyword"})
        assert "remove_multifield" not in _rec_types(recs)


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — Rule 8: remove_field
# ---------------------------------------------------------------------------

class TestRule8RemoveField:

    def test_unused_field_recommends_removal(self):
        doc = _field_doc(
            field_name="legacy_sku", classification="unused",
        )
        recs = generate_recommendations(doc, {"legacy_sku", "price"})
        assert _rec_types(recs) == ["remove_field"]

    def test_unused_multifield_gets_remove_multifield_not_remove_field(self):
        """Unused multi-fields should get remove_multifield (rule 7), not remove_field."""
        doc = _field_doc(
            field_name="title.keyword", classification="unused",
        )
        recs = generate_recommendations(doc, {"title", "title.keyword"})
        assert "remove_field" not in _rec_types(recs)
        assert "remove_multifield" in _rec_types(recs)


# ---------------------------------------------------------------------------
# TestGenerateRecommendations — stacking and edge cases
# ---------------------------------------------------------------------------

class TestRuleStacking:

    def test_rules_4_and_5_stack(self):
        """Text field with only filter usage gets both disable_norms and change_to_keyword."""
        doc = _field_doc(
            mapped_type="text",
            count_queried=0, count_filtered=20,
            count_aggregated=0, count_sorted=0,
        )
        recs = generate_recommendations(doc, set())
        types = _rec_types(recs)
        assert "disable_norms" in types
        assert "change_to_keyword" in types

    def test_active_field_all_structures_needed(self):
        """Active field using queries + aggs + sorts gets no recommendations."""
        doc = _field_doc(
            mapped_type="keyword",
            count_queried=10, count_filtered=5,
            count_aggregated=3, count_sorted=2,
            has_doc_values=True, is_indexed=True,
        )
        recs = generate_recommendations(doc, set())
        assert recs == []

    def test_non_active_classification_skips_active_rules(self):
        """A write_only field should not get active-field recommendations."""
        doc = _field_doc(
            classification="write_only", mapped_type="text",
            is_indexed=True, has_doc_values=False,
            count_written=50,
        )
        recs = generate_recommendations(doc, set())
        types = _rec_types(recs)
        assert "disable_index" in types
        # Should NOT get any of the active-field rules
        assert "disable_norms" not in types
        assert "change_to_keyword" not in types


# ---------------------------------------------------------------------------
# TestFormatHow
# ---------------------------------------------------------------------------

class TestFormatHow:

    def test_field_name_interpolated(self):
        result = _format_how("disable_doc_values", "price", "float")
        assert "price" in result
        assert "float" in result

    def test_remove_multifield_splits_parent_and_sub(self):
        result = _format_how("remove_multifield", "title.keyword", "keyword")
        assert "title" in result
        assert "keyword" in result


# ---------------------------------------------------------------------------
# TestBuildRecommendationDocs
# ---------------------------------------------------------------------------

class TestBuildRecommendationDocs:

    def test_builds_correct_structure(self):
        diff_docs = [
            _field_doc(field_name="legacy_sku", classification="unused"),
        ]
        docs = build_recommendation_docs("products", diff_docs, "2026-02-14T12:00:00Z")
        assert len(docs) == 1
        doc = docs[0]
        assert doc["timestamp"] == "2026-02-14T12:00:00Z"
        assert doc["index_group"] == "products"
        assert doc["field_name"] == "legacy_sku"
        assert doc["classification"] == "unused"
        assert doc["recommendation"] == "remove_field"
        assert "why" in doc
        assert "how" in doc
        assert "breaking_change" in doc

    def test_sibling_set_built_correctly(self):
        """Rule 6 should see sibling fields from the same diff batch."""
        diff_docs = [
            _field_doc(
                field_name="title", mapped_type="text",
                count_queried=10, count_filtered=5,
            ),
            _field_doc(
                field_name="title.keyword", mapped_type="keyword",
                count_aggregated=3,
            ),
        ]
        docs = build_recommendation_docs("products", diff_docs, "2026-02-14T12:00:00Z")
        rec_types = [d["recommendation"] for d in docs]
        # title.keyword exists as sibling → rule 6 should NOT fire
        assert "add_keyword_subfield" not in rec_types

    def test_empty_diff_docs(self):
        docs = build_recommendation_docs("products", [], "2026-02-14T12:00:00Z")
        assert docs == []

    def test_multiple_recommendations_per_field(self):
        """A text field with only filter usage gets multiple recommendations."""
        diff_docs = [
            _field_doc(
                field_name="status", mapped_type="text",
                count_queried=0, count_filtered=20,
                count_aggregated=0, count_sorted=0,
            ),
        ]
        docs = build_recommendation_docs("products", diff_docs, "2026-02-14T12:00:00Z")
        # Should get both disable_norms and change_to_keyword
        assert len(docs) >= 2
        types = [d["recommendation"] for d in docs]
        assert "disable_norms" in types
        assert "change_to_keyword" in types

    def test_no_recommendations_for_healthy_field(self):
        """An active keyword field with all structures in use generates nothing."""
        diff_docs = [
            _field_doc(
                field_name="category", mapped_type="keyword",
                count_queried=10, count_filtered=5,
                count_aggregated=3, count_sorted=2,
            ),
        ]
        docs = build_recommendation_docs("products", diff_docs, "2026-02-14T12:00:00Z")
        assert docs == []
