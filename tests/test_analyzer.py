"""Tests for gateway.analyzer — tier classification, recommendations, and heat computation."""

from gateway.analyzer import _index_tier, _field_tier, _recommend_index, _recommend_field, _compute_index_heat


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
