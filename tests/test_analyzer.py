"""Tests for gateway.analyzer — tier classification, recommendations, heat computation, and merge."""

from collections import defaultdict

from gateway.analyzer import (
    _index_tier, _field_tier, _recommend_index, _recommend_field,
    _compute_index_heat, merge_and_build_report, FIELD_CATEGORIES,
)


class TestIndexTier:
    """Index tiers based on ops/hour. Thresholds: hot>100, warm>10, cold>1."""

    def test_hot(self):
        assert _index_tier(150) == "hot"
        assert _index_tier(101) == "hot"

    def test_warm(self):
        assert _index_tier(50) == "warm"
        assert _index_tier(11) == "warm"

    def test_cold(self):
        assert _index_tier(5) == "cold"
        assert _index_tier(1.5) == "cold"

    def test_frozen(self):
        assert _index_tier(1) == "frozen"
        assert _index_tier(0.5) == "frozen"
        assert _index_tier(0) == "frozen"

    def test_boundary_values(self):
        # Boundaries are exclusive (>), not inclusive (>=)
        assert _index_tier(100) == "warm"   # not > 100
        assert _index_tier(10) == "cold"    # not > 10
        assert _index_tier(1) == "frozen"   # not > 1


class TestFieldTier:
    """Field tiers based on proportion. Thresholds: hot>=0.15, warm>=0.05, cold>=0.01."""

    def test_hot(self):
        assert _field_tier(0.30) == "hot"
        assert _field_tier(0.15) == "hot"

    def test_warm(self):
        assert _field_tier(0.10) == "warm"
        assert _field_tier(0.05) == "warm"

    def test_cold(self):
        assert _field_tier(0.03) == "cold"
        assert _field_tier(0.01) == "cold"

    def test_unused(self):
        assert _field_tier(0.009) == "unused"
        assert _field_tier(0) == "unused"

    def test_boundary_values(self):
        # Boundaries are inclusive (>=)
        assert _field_tier(0.15) == "hot"
        assert _field_tier(0.05) == "warm"
        assert _field_tier(0.01) == "cold"
        assert _field_tier(0.0099) == "unused"


class TestRecommendIndex:
    def test_frozen_recommends_freeze(self):
        recs = _recommend_index("frozen", 0.5)
        assert len(recs) == 1
        assert "freezing" in recs[0].lower() or "reducing replicas" in recs[0].lower()

    def test_cold_recommends_cold_tier(self):
        recs = _recommend_index("cold", 5)
        assert len(recs) == 1
        assert "cold" in recs[0].lower()

    def test_hot_recommends_replicas(self):
        recs = _recommend_index("hot", 150)
        assert len(recs) == 1
        assert "replicas" in recs[0].lower()

    def test_warm_no_recommendation(self):
        recs = _recommend_index("warm", 50)
        assert recs == []


class TestRecommendField:
    def test_unused_field(self):
        cats = {"queried": 0, "filtered": 0, "aggregated": 0, "sorted": 0, "sourced": 0, "written": 0}
        rec = _recommend_field("internal_sku", "unused", cats)
        assert rec is not None
        assert "index: false" in rec

    def test_cold_field(self):
        cats = {"queried": 1, "filtered": 0, "aggregated": 0, "sorted": 0, "sourced": 0, "written": 0}
        rec = _recommend_field("rare_field", "cold", cats)
        assert rec is not None
        assert "doc_values" in rec

    def test_only_sourced_field(self):
        cats = {"queried": 0, "filtered": 0, "aggregated": 0, "sorted": 0, "sourced": 10, "written": 0}
        rec = _recommend_field("display_field", "warm", cats)
        assert rec is not None
        assert "index: false" in rec

    def test_aggregated_field(self):
        cats = {"queried": 0, "filtered": 0, "aggregated": 5, "sorted": 0, "sourced": 0, "written": 0}
        rec = _recommend_field("brand", "hot", cats)
        assert rec is not None
        assert "doc_values" in rec

    def test_queried_hot_field_no_rec(self):
        cats = {"queried": 50, "filtered": 0, "aggregated": 0, "sorted": 0, "sourced": 0, "written": 0}
        rec = _recommend_field("title", "hot", cats)
        assert rec is None


class TestComputeIndexHeat:
    """_compute_index_heat processes a single ES aggregation bucket."""

    def test_basic_structure(self):
        bucket = {
            "doc_count": 240,
            "field_queried": {"buckets": [{"key": "title", "doc_count": 100}]},
            "field_filtered": {"buckets": [{"key": "category", "doc_count": 50}]},
            "field_aggregated": {"buckets": []},
            "field_sorted": {"buckets": [{"key": "price", "doc_count": 30}]},
            "field_sourced": {"buckets": [{"key": "title", "doc_count": 80}]},
            "field_written": {"buckets": []},
        }
        result = _compute_index_heat(bucket, time_window_hours=24.0)
        assert result["total_operations"] == 240
        assert result["heat_score"] == 10.0  # 240/24
        assert result["tier"] == "cold"  # 10.0 is not > 10
        assert "title" in result["fields"]
        assert "category" in result["fields"]
        assert "price" in result["fields"]
        assert isinstance(result["recommendations"], list)

    def test_hot_tier(self):
        bucket = {"doc_count": 2500}
        # No field aggs
        for cat in ("queried", "filtered", "aggregated", "sorted", "sourced", "written"):
            bucket[f"field_{cat}"] = {"buckets": []}
        result = _compute_index_heat(bucket, time_window_hours=24.0)
        assert result["tier"] == "hot"
        assert result["heat_score"] > 100

    def test_field_proportions(self):
        bucket = {
            "doc_count": 100,
            "field_queried": {"buckets": [
                {"key": "title", "doc_count": 80},
                {"key": "description", "doc_count": 10},
            ]},
            "field_filtered": {"buckets": []},
            "field_aggregated": {"buckets": []},
            "field_sorted": {"buckets": []},
            "field_sourced": {"buckets": [{"key": "title", "doc_count": 10}]},
            "field_written": {"buckets": []},
        }
        result = _compute_index_heat(bucket, time_window_hours=24.0)
        # total_field_refs = 80 + 10 + 10 = 100
        # title: (80+10)/100 = 0.9 → hot
        # description: 10/100 = 0.1 → warm
        assert result["fields"]["title"]["tier"] == "hot"
        assert result["fields"]["description"]["tier"] == "warm"


def _make_raw_agg_data(group_name, index_name, doc_count, field_buckets=None):
    """Helper to build a raw ES aggregation response for testing merge."""
    field_aggs = {}
    for cat in FIELD_CATEGORIES:
        field_aggs[f"field_{cat}"] = {"buckets": field_buckets.get(cat, []) if field_buckets else []}

    return {
        "aggregations": {
            "by_group": {
                "buckets": [{
                    "key": group_name,
                    "doc_count": doc_count,
                    "by_index": {
                        "buckets": [{
                            "key": index_name,
                            "doc_count": doc_count,
                            **field_aggs,
                        }],
                    },
                    "lookback_avg": {"value": None},
                    "lookback_max": {"value": None},
                    "lookback_percentiles": {"values": {"50.0": None}},
                    "lookback_count": {"value": 0},
                }],
            },
        },
    }


def _make_rollup_data(group_name, index_name, total_ops, field_counts=None):
    """Helper to build rollup aggregation data for testing merge."""
    fc = defaultdict(lambda: {cat: 0 for cat in FIELD_CATEGORIES})
    if field_counts:
        for fname, cats in field_counts.items():
            for cat, count in cats.items():
                fc[fname][cat] = count
    return {
        (group_name, index_name): {
            "total_operations": total_ops,
            "field_counts": fc,
            "lookback_sum": 0.0,
            "lookback_max": 0.0,
            "lookback_count": 0,
        },
    }


class TestMergeRawOnly:
    def test_raw_only_produces_valid_report(self):
        raw = _make_raw_agg_data("products", "products", 240, {
            "queried": [{"key": "title", "doc_count": 100}],
        })
        result = merge_and_build_report(raw, {}, 24.0)
        assert "groups" in result
        assert "products" in result["groups"]
        group = result["groups"]["products"]
        assert group["total_operations"] == 240
        assert group["heat_score"] == 10.0
        assert "title" in group["indices"]["products"]["fields"]


class TestMergeRollupOnly:
    def test_rollup_only_produces_valid_report(self):
        rollup = _make_rollup_data("logs", "logs", 500, {
            "message": {"queried": 200, "filtered": 0, "aggregated": 0, "sorted": 0, "sourced": 100, "written": 0},
        })
        result = merge_and_build_report(None, rollup, 24.0)
        assert "groups" in result
        assert "logs" in result["groups"]
        group = result["groups"]["logs"]
        assert group["total_operations"] == 500
        assert "message" in group["indices"]["logs"]["fields"]
        assert group["indices"]["logs"]["fields"]["message"]["queried"] == 200


class TestMergeBoth:
    def test_both_sources_summed(self):
        raw = _make_raw_agg_data("products", "products", 100, {
            "queried": [{"key": "title", "doc_count": 50}],
        })
        rollup = _make_rollup_data("products", "products", 200, {
            "title": {"queried": 80, "filtered": 0, "aggregated": 0, "sorted": 0, "sourced": 0, "written": 0},
        })
        result = merge_and_build_report(raw, rollup, 24.0)
        group = result["groups"]["products"]
        assert group["total_operations"] == 300  # 100 + 200
        idx = group["indices"]["products"]
        assert idx["total_operations"] == 300
        assert idx["fields"]["title"]["queried"] == 130  # 50 + 80


class TestMergeOverlappingFields:
    def test_same_field_both_sources_add(self):
        raw = _make_raw_agg_data("x", "x", 10, {
            "queried": [{"key": "f1", "doc_count": 5}],
            "filtered": [{"key": "f1", "doc_count": 3}],
        })
        rollup = _make_rollup_data("x", "x", 20, {
            "f1": {"queried": 10, "filtered": 7, "aggregated": 0, "sorted": 0, "sourced": 0, "written": 0},
        })
        result = merge_and_build_report(raw, rollup, 24.0)
        fields = result["groups"]["x"]["indices"]["x"]["fields"]
        assert fields["f1"]["queried"] == 15  # 5 + 10
        assert fields["f1"]["filtered"] == 10  # 3 + 7


class TestMergeMultipleGroups:
    def test_different_groups_from_different_sources(self):
        raw = _make_raw_agg_data("products", "products", 100)
        rollup = _make_rollup_data("logs", "logs", 200)
        result = merge_and_build_report(raw, rollup, 24.0)
        assert "products" in result["groups"]
        assert "logs" in result["groups"]
        assert result["summary"]["total_groups"] == 2


class TestHeatReportStructureUnchanged:
    def test_output_schema(self):
        raw = _make_raw_agg_data("test", "test", 500, {
            "queried": [{"key": "a", "doc_count": 100}],
        })
        result = merge_and_build_report(raw, {}, 24.0)
        # Top-level keys
        assert "time_window" in result
        assert "summary" in result
        assert "groups" in result
        assert result["time_window"] == "last_24h"
        assert "total_groups" in result["summary"]
        assert "by_tier" in result["summary"]

        # Group structure
        group = result["groups"]["test"]
        assert "heat_score" in group
        assert "tier" in group
        assert "total_operations" in group
        assert "indices" in group
        assert "lookback" in group
        assert "recommendations" in group

        # Index structure
        idx = group["indices"]["test"]
        assert "heat_score" in idx
        assert "tier" in idx
        assert "total_operations" in idx
        assert "fields" in idx
        assert "recommendations" in idx

        # Field structure
        field = idx["fields"]["a"]
        assert "heat" in field
        assert "tier" in field
        assert "queried" in field


class TestBackwardCompatNoTypeField:
    def test_raw_data_with_no_type_filter_still_works(self):
        """The merge function doesn't care about type filtering — that's in the query.
        Verify that the merge function handles raw data correctly regardless."""
        raw = _make_raw_agg_data("old", "old", 50)
        result = merge_and_build_report(raw, {}, 24.0)
        assert "old" in result["groups"]
        assert result["groups"]["old"]["total_operations"] == 50


class TestMergeEmptyBothSources:
    def test_empty_both(self):
        result = merge_and_build_report(None, {}, 24.0)
        assert result["summary"]["total_groups"] == 0
        assert result["groups"] == {}
