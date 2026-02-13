"""Tests for gateway.analyzer — tier classification, recommendations, heat computation, and query patterns."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.analyzer import _index_tier, _field_tier, _recommend_index, _recommend_field, _compute_index_heat, _compute_index_heat_weighted, compute_query_patterns


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


class TestComputeIndexHeatWeighted:
    """_compute_index_heat_weighted scores fields by total response_time_ms."""

    def _make_bucket(self, field_buckets_by_category: dict) -> dict:
        """Build a bucket with total_response_time sub-aggs per field."""
        bucket = {"doc_count": 100}
        for cat in ("queried", "filtered", "aggregated", "sorted", "sourced", "written"):
            raw = field_buckets_by_category.get(cat, [])
            bucket[f"field_{cat}"] = {"buckets": [
                {"key": name, "doc_count": count, "total_response_time": {"value": time_ms}}
                for name, count, time_ms in raw
            ]}
        return bucket

    def test_slow_field_ranks_higher_than_fast_field(self):
        """A field with high total response time outranks one with high count."""
        bucket = self._make_bucket({
            "queried": [
                ("slow_field", 10, 5000.0),   # 10 queries, 5000ms total
                ("fast_field", 1000, 1000.0),  # 1000 queries, 1000ms total
            ],
        })
        result = _compute_index_heat_weighted(bucket, time_window_hours=24.0)
        assert result["fields"]["slow_field"]["heat"] > result["fields"]["fast_field"]["heat"]
        # slow_field: 5000/6000 ≈ 0.833 → hot
        assert result["fields"]["slow_field"]["tier"] == "hot"

    def test_proportions_sum_to_one(self):
        bucket = self._make_bucket({
            "queried": [("a", 50, 300.0), ("b", 50, 200.0)],
            "filtered": [("c", 30, 500.0)],
        })
        result = _compute_index_heat_weighted(bucket, time_window_hours=24.0)
        total_heat = sum(f["heat"] for f in result["fields"].values())
        assert abs(total_heat - 1.0) < 0.01

    def test_empty_buckets(self):
        bucket = self._make_bucket({})
        result = _compute_index_heat_weighted(bucket, time_window_hours=24.0)
        assert result["fields"] == {}
        assert result["total_response_time_ms"] == 0.0

    def test_total_response_time_reported(self):
        bucket = self._make_bucket({
            "queried": [("title", 100, 4200.0)],
            "sorted": [("price", 30, 800.0)],
        })
        result = _compute_index_heat_weighted(bucket, time_window_hours=24.0)
        assert result["total_response_time_ms"] == 5000.0

    def test_multi_category_field_sums_time(self):
        """A field in multiple categories sums response time across all."""
        bucket = self._make_bucket({
            "queried": [("title", 80, 2000.0)],
            "sourced": [("title", 80, 1500.0)],
        })
        result = _compute_index_heat_weighted(bucket, time_window_hours=24.0)
        assert result["fields"]["title"]["heat"] == 1.0
        assert result["fields"]["title"]["total_time_ms"] == 3500.0

    def test_recommendations_generated(self):
        """Weighted panel generates recommendations like the count-based one."""
        bucket = self._make_bucket({
            "sourced": [("display_only", 100, 5000.0)],
        })
        result = _compute_index_heat_weighted(bucket, time_window_hours=24.0)
        # display_only is only sourced, never queried → should recommend index: false
        assert any("index: false" in r for r in result["recommendations"])

    def test_zero_response_time_no_crash(self):
        bucket = self._make_bucket({
            "queried": [("field_a", 50, 0.0)],
        })
        result = _compute_index_heat_weighted(bucket, time_window_hours=24.0)
        assert result["fields"]["field_a"]["heat"] == 0.0


class TestComputeQueryPatterns:
    """compute_query_patterns aggregates events by structural template."""

    def _mock_es_response(self, buckets):
        """Build a mock httpx response wrapping template aggregation buckets."""
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = {
            "aggregations": {"by_template": {"buckets": buckets}}
        }
        return mock

    @pytest.mark.asyncio
    async def test_parses_template_buckets(self):
        buckets = [
            {
                "key": "abc123",
                "doc_count": 150,
                "total_response_time": {"value": 7500.0},
                "avg_response_time": {"value": 50.0},
                "index_groups": {"buckets": [{"key": "products"}, {"key": "logs"}]},
                "sample": {"hits": {"hits": [
                    {"_source": {"query_template_text": '{"match": {"title": "?"}}', "operation": "search"}}
                ]}},
            },
            {
                "key": "def456",
                "doc_count": 80,
                "total_response_time": {"value": 2400.0},
                "avg_response_time": {"value": 30.0},
                "index_groups": {"buckets": [{"key": "orders"}]},
                "sample": {"hits": {"hits": []}},
            },
        ]
        mock_resp = self._mock_es_response(buckets)

        with patch("gateway.analyzer._client") as mock_client:
            mock_client.post = AsyncMock(return_value=mock_resp)
            result = await compute_query_patterns(time_window_hours=24.0)

        assert result["time_window"] == "last_24h"
        assert result["summary"]["unique_templates"] == 2
        assert result["summary"]["total_executions"] == 230
        assert result["summary"]["total_response_time_ms"] == 9900.0

        p = result["patterns"]
        assert len(p) == 2
        assert p[0]["template_hash"] == "abc123"
        assert p[0]["execution_count"] == 150
        assert p[0]["avg_response_time_ms"] == 50.0
        assert p[0]["index_groups"] == ["products", "logs"]
        assert p[0]["template_text"] == '{"match": {"title": "?"}}'
        assert p[0]["operation"] == "search"

        assert p[1]["template_hash"] == "def456"
        assert p[1]["template_text"] is None
        assert p[1]["operation"] is None

    @pytest.mark.asyncio
    async def test_index_group_filter_in_query(self):
        """Verify index_group filter is included in the ES query."""
        mock_resp = self._mock_es_response([])
        captured_query = None

        async def capture_post(url, json=None, **kwargs):
            nonlocal captured_query
            captured_query = json
            return mock_resp

        with patch("gateway.analyzer._client") as mock_client:
            mock_client.post = AsyncMock(side_effect=capture_post)
            await compute_query_patterns(time_window_hours=12.0, index_group="products")

        assert captured_query is not None
        must = captured_query["query"]["bool"]["must"]
        assert any(c.get("term", {}).get("index_group") == "products" for c in must)
        assert any("range" in c for c in must)
        assert any("exists" in c for c in must)

    @pytest.mark.asyncio
    async def test_empty_result(self):
        mock_resp = self._mock_es_response([])

        with patch("gateway.analyzer._client") as mock_client:
            mock_client.post = AsyncMock(return_value=mock_resp)
            result = await compute_query_patterns()

        assert result["summary"]["unique_templates"] == 0
        assert result["summary"]["total_executions"] == 0
        assert result["patterns"] == []
