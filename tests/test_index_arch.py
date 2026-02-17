"""Tests for gateway/index_arch.py — index architecture recommendation rules."""

import pytest
from gateway.index_arch import (
    check_shard_too_small,
    check_shard_too_large,
    check_replica_risk,
    check_replica_waste,
    check_codec_opportunity,
    check_field_count_near_limit,
    check_source_disabled,
    check_rollover_lookback_mismatch,
    check_index_sorting_opportunity,
    check_refresh_interval_opportunity,
    evaluate_all_rules,
    estimate_rollover_hours,
    parse_usage_stats_response,
    partition_by_group,
    pick_representative_index,
    build_usage_stats_query,
    build_group_profile,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _group_profile(
    index_group: str = "logs",
    primary_shard_count: int = 3,
    avg_primary_shard_size_bytes: int = 15_000_000_000,  # 15GB (healthy)
    total_primary_store_bytes: int = 45_000_000_000,
    number_of_replicas: int = 1,
    tier_preference: str | None = None,
    index_codec: str | None = None,
    blocks_write: bool = False,
    refresh_interval: str | None = None,
    index_sort_field: list[str] | None = None,
    field_count: int | None = 50,
    total_fields_limit: int = 1000,
    source_enabled: bool = True,
    creation_dates: list[str] | None = None,
    estimated_rollover_hours: float | None = 24.0,
    lookback_p50_seconds: float | None = 21600,   # 6h
    lookback_p95_seconds: float | None = 43200,    # 12h (fits in 24h rollover)
    dominant_sort_field: str | None = None,
    dominant_sort_pct: float | None = None,
    search_count: int = 100,
    write_count: int = 100,
) -> dict:
    """Build a minimal group profile with healthy defaults.

    Override specific fields to trigger rules under test.
    With these defaults, all 10 rules should return [].
    """
    return {
        "index_group": index_group,
        "indices": ["logs-000001"],
        "index_count": 1,
        "primary_shard_count": primary_shard_count,
        "avg_primary_shard_size_bytes": avg_primary_shard_size_bytes,
        "total_primary_store_bytes": total_primary_store_bytes,
        "number_of_replicas": number_of_replicas,
        "tier_preference": tier_preference,
        "index_codec": index_codec,
        "blocks_write": blocks_write,
        "refresh_interval": refresh_interval,
        "index_sort_field": index_sort_field,
        "field_count": field_count,
        "total_fields_limit": total_fields_limit,
        "source_enabled": source_enabled,
        "creation_dates": creation_dates or [],
        "estimated_rollover_hours": estimated_rollover_hours,
        "lookback_p50_seconds": lookback_p50_seconds,
        "lookback_p95_seconds": lookback_p95_seconds,
        "dominant_sort_field": dominant_sort_field,
        "dominant_sort_pct": dominant_sort_pct,
        "search_count": search_count,
        "write_count": write_count,
    }


def _rec_codes(recs: list[dict]) -> list[str]:
    """Extract recommendation codes from a list of recommendations."""
    return [r["recommendation"] for r in recs]


# ---------------------------------------------------------------------------
# Rule 1: shard_too_small
# ---------------------------------------------------------------------------

class TestShardTooSmall:
    def test_fires_when_avg_below_1gb(self):
        profile = _group_profile(
            primary_shard_count=5,
            avg_primary_shard_size_bytes=200_000_000,  # 200MB
            total_primary_store_bytes=1_000_000_000,
        )
        recs = check_shard_too_small(profile)
        assert _rec_codes(recs) == ["shard_too_small"]
        assert recs[0]["severity"] == "warning"
        assert recs[0]["category"] == "shard_sizing"
        assert recs[0]["breaking_change"] is False
        assert "200MB" in recs[0]["current_value"]

    def test_skips_single_shard(self):
        profile = _group_profile(
            primary_shard_count=1,
            avg_primary_shard_size_bytes=200_000_000,
        )
        assert check_shard_too_small(profile) == []

    def test_skips_when_above_1gb(self):
        profile = _group_profile(
            primary_shard_count=5,
            avg_primary_shard_size_bytes=5_000_000_000,
        )
        assert check_shard_too_small(profile) == []

    def test_skips_exactly_1gb(self):
        profile = _group_profile(
            primary_shard_count=5,
            avg_primary_shard_size_bytes=1_000_000_000,
        )
        assert check_shard_too_small(profile) == []


# ---------------------------------------------------------------------------
# Rule 2: shard_too_large
# ---------------------------------------------------------------------------

class TestShardTooLarge:
    def test_warning_above_50gb(self):
        profile = _group_profile(avg_primary_shard_size_bytes=60_000_000_000)
        recs = check_shard_too_large(profile)
        assert _rec_codes(recs) == ["shard_too_large"]
        assert recs[0]["severity"] == "warning"
        assert "60.0GB" in recs[0]["current_value"]

    def test_critical_above_100gb(self):
        profile = _group_profile(avg_primary_shard_size_bytes=120_000_000_000)
        recs = check_shard_too_large(profile)
        assert recs[0]["severity"] == "critical"

    def test_skips_below_50gb(self):
        profile = _group_profile(avg_primary_shard_size_bytes=30_000_000_000)
        assert check_shard_too_large(profile) == []

    def test_skips_zero_shards(self):
        profile = _group_profile(
            primary_shard_count=0,
            avg_primary_shard_size_bytes=0,
        )
        assert check_shard_too_large(profile) == []


# ---------------------------------------------------------------------------
# Rule 3: replica_risk
# ---------------------------------------------------------------------------

class TestReplicaRisk:
    def test_fires_zero_replicas(self):
        profile = _group_profile(number_of_replicas=0)
        recs = check_replica_risk(profile)
        assert _rec_codes(recs) == ["replica_risk"]
        assert recs[0]["severity"] == "warning"

    def test_skips_frozen_tier(self):
        profile = _group_profile(
            number_of_replicas=0,
            tier_preference="data_frozen",
        )
        assert check_replica_risk(profile) == []

    def test_skips_when_replicas_set(self):
        profile = _group_profile(number_of_replicas=1)
        assert check_replica_risk(profile) == []


# ---------------------------------------------------------------------------
# Rule 4: replica_waste
# ---------------------------------------------------------------------------

class TestReplicaWaste:
    def test_fires_cold_tier_with_replicas(self):
        profile = _group_profile(
            number_of_replicas=1,
            tier_preference="data_cold,data_warm",
        )
        recs = check_replica_waste(profile)
        assert _rec_codes(recs) == ["replica_waste"]
        assert recs[0]["severity"] == "info"

    def test_fires_frozen_tier_with_replicas(self):
        profile = _group_profile(
            number_of_replicas=1,
            tier_preference="data_frozen",
        )
        assert _rec_codes(check_replica_waste(profile)) == ["replica_waste"]

    def test_skips_hot_tier(self):
        profile = _group_profile(
            number_of_replicas=1,
            tier_preference="data_hot",
        )
        assert check_replica_waste(profile) == []

    def test_skips_zero_replicas(self):
        profile = _group_profile(
            number_of_replicas=0,
            tier_preference="data_cold",
        )
        assert check_replica_waste(profile) == []

    def test_skips_no_tier_preference(self):
        profile = _group_profile(number_of_replicas=1, tier_preference=None)
        assert check_replica_waste(profile) == []


# ---------------------------------------------------------------------------
# Rule 5: codec_opportunity
# ---------------------------------------------------------------------------

class TestCodecOpportunity:
    def test_fires_read_only_no_codec(self):
        profile = _group_profile(blocks_write=True, index_codec=None)
        recs = check_codec_opportunity(profile)
        assert _rec_codes(recs) == ["codec_opportunity"]
        assert recs[0]["severity"] == "info"
        assert "read-only" in recs[0]["current_value"]

    def test_fires_warm_tier_no_codec(self):
        profile = _group_profile(
            tier_preference="data_warm",
            index_codec=None,
        )
        recs = check_codec_opportunity(profile)
        assert _rec_codes(recs) == ["codec_opportunity"]
        assert "warm" in recs[0]["current_value"]

    def test_skips_when_codec_set(self):
        profile = _group_profile(
            blocks_write=True,
            index_codec="best_compression",
        )
        assert check_codec_opportunity(profile) == []

    def test_skips_hot_writable(self):
        profile = _group_profile(
            blocks_write=False,
            tier_preference=None,
            index_codec=None,
        )
        assert check_codec_opportunity(profile) == []


# ---------------------------------------------------------------------------
# Rule 6: field_count_near_limit
# ---------------------------------------------------------------------------

class TestFieldCountNearLimit:
    def test_warning_above_80_percent(self):
        profile = _group_profile(field_count=850, total_fields_limit=1000)
        recs = check_field_count_near_limit(profile)
        assert _rec_codes(recs) == ["field_count_near_limit"]
        assert recs[0]["severity"] == "warning"
        assert "850" in recs[0]["current_value"]
        assert "85%" in recs[0]["current_value"]

    def test_critical_above_95_percent(self):
        profile = _group_profile(field_count=960, total_fields_limit=1000)
        recs = check_field_count_near_limit(profile)
        assert recs[0]["severity"] == "critical"

    def test_skips_below_80_percent(self):
        profile = _group_profile(field_count=500, total_fields_limit=1000)
        assert check_field_count_near_limit(profile) == []

    def test_skips_when_field_count_unknown(self):
        profile = _group_profile(field_count=None)
        assert check_field_count_near_limit(profile) == []

    def test_exactly_80_percent(self):
        profile = _group_profile(field_count=800, total_fields_limit=1000)
        assert _rec_codes(check_field_count_near_limit(profile)) == ["field_count_near_limit"]

    def test_custom_limit(self):
        profile = _group_profile(field_count=450, total_fields_limit=500)
        recs = check_field_count_near_limit(profile)
        assert recs[0]["severity"] == "warning"
        assert "500" in recs[0]["current_value"]


# ---------------------------------------------------------------------------
# Rule 7: source_disabled
# ---------------------------------------------------------------------------

class TestSourceDisabled:
    def test_fires_when_disabled(self):
        profile = _group_profile(source_enabled=False)
        recs = check_source_disabled(profile)
        assert _rec_codes(recs) == ["source_disabled"]
        assert recs[0]["severity"] == "critical"
        assert recs[0]["breaking_change"] is True

    def test_skips_when_enabled(self):
        profile = _group_profile(source_enabled=True)
        assert check_source_disabled(profile) == []


# ---------------------------------------------------------------------------
# Rule 8: rollover_lookback_mismatch
# ---------------------------------------------------------------------------

class TestRolloverLookbackMismatch:
    def test_fires_when_lookback_exceeds_rollover(self):
        # Rollover=24h (daily), p95 lookback=259200s (72h = 3x) => clearly exceeds 2x
        profile = _group_profile(
            estimated_rollover_hours=24.0,
            lookback_p95_seconds=259200.0,
        )
        recs = check_rollover_lookback_mismatch(profile)
        assert _rec_codes(recs) == ["rollover_lookback_mismatch"]
        assert recs[0]["severity"] == "warning"
        assert recs[0]["category"] == "usage_based"

    def test_skips_when_lookback_fits(self):
        # Rollover=24h, p95 lookback=43200s (12h) => fits
        profile = _group_profile(
            estimated_rollover_hours=24.0,
            lookback_p95_seconds=43200.0,
        )
        assert check_rollover_lookback_mismatch(profile) == []

    def test_skips_exactly_2x(self):
        # Rollover=24h, p95 lookback=48h exactly => boundary, should not fire
        profile = _group_profile(
            estimated_rollover_hours=24.0,
            lookback_p95_seconds=172800.0,  # exactly 48h = 2x
        )
        # 48h > 24h * 2 is false (48 <= 48), so should not fire
        # Wait: 172800 / 3600 = 48, 24 * 2 = 48, 48 <= 48 so no fire
        assert check_rollover_lookback_mismatch(profile) == []

    def test_fires_just_above_2x(self):
        profile = _group_profile(
            estimated_rollover_hours=24.0,
            lookback_p95_seconds=173000.0,  # slightly over 48h
        )
        assert _rec_codes(check_rollover_lookback_mismatch(profile)) == [
            "rollover_lookback_mismatch"
        ]

    def test_skips_no_rollover_data(self):
        profile = _group_profile(estimated_rollover_hours=None)
        assert check_rollover_lookback_mismatch(profile) == []

    def test_skips_no_lookback_data(self):
        profile = _group_profile(lookback_p95_seconds=None)
        assert check_rollover_lookback_mismatch(profile) == []


# ---------------------------------------------------------------------------
# Rule 9: index_sorting_opportunity
# ---------------------------------------------------------------------------

class TestIndexSortingOpportunity:
    def test_fires_dominant_sort_field(self):
        profile = _group_profile(
            dominant_sort_field="@timestamp",
            dominant_sort_pct=0.85,
            index_sort_field=None,
        )
        recs = check_index_sorting_opportunity(profile)
        assert _rec_codes(recs) == ["index_sorting_opportunity"]
        assert recs[0]["severity"] == "info"
        assert "@timestamp" in recs[0]["current_value"]
        assert "85%" in recs[0]["current_value"]

    def test_skips_below_70_percent(self):
        profile = _group_profile(
            dominant_sort_field="@timestamp",
            dominant_sort_pct=0.5,
        )
        assert check_index_sorting_opportunity(profile) == []

    def test_skips_exactly_70_percent(self):
        profile = _group_profile(
            dominant_sort_field="@timestamp",
            dominant_sort_pct=0.7,
            index_sort_field=None,
        )
        assert _rec_codes(check_index_sorting_opportunity(profile)) == [
            "index_sorting_opportunity"
        ]

    def test_skips_already_sorted(self):
        profile = _group_profile(
            dominant_sort_field="@timestamp",
            dominant_sort_pct=0.9,
            index_sort_field=["@timestamp"],
        )
        assert check_index_sorting_opportunity(profile) == []

    def test_skips_no_sort_data(self):
        profile = _group_profile(dominant_sort_field=None)
        assert check_index_sorting_opportunity(profile) == []


# ---------------------------------------------------------------------------
# Rule 10: refresh_interval_opportunity
# ---------------------------------------------------------------------------

class TestRefreshIntervalOpportunity:
    def test_fires_high_write_low_search(self):
        profile = _group_profile(
            search_count=10,
            write_count=500,
            refresh_interval=None,  # default 1s
        )
        recs = check_refresh_interval_opportunity(profile)
        assert _rec_codes(recs) == ["refresh_interval_opportunity"]
        assert recs[0]["severity"] == "info"
        assert recs[0]["category"] == "usage_based"

    def test_fires_explicit_1s(self):
        profile = _group_profile(
            search_count=5,
            write_count=500,
            refresh_interval="1s",
        )
        assert _rec_codes(check_refresh_interval_opportunity(profile)) == [
            "refresh_interval_opportunity"
        ]

    def test_skips_when_already_customized(self):
        profile = _group_profile(
            search_count=10,
            write_count=500,
            refresh_interval="30s",
        )
        assert check_refresh_interval_opportunity(profile) == []

    def test_skips_balanced_traffic(self):
        profile = _group_profile(search_count=100, write_count=100)
        assert check_refresh_interval_opportunity(profile) == []

    def test_skips_zero_writes(self):
        profile = _group_profile(search_count=100, write_count=0)
        assert check_refresh_interval_opportunity(profile) == []

    def test_skips_moderate_ratio(self):
        # 5x write ratio, but threshold is 10x
        profile = _group_profile(search_count=100, write_count=500)
        assert check_refresh_interval_opportunity(profile) == []


# ---------------------------------------------------------------------------
# evaluate_all_rules
# ---------------------------------------------------------------------------

class TestEvaluateAllRules:
    def test_healthy_group_no_recommendations(self):
        """A well-configured group should produce zero recommendations."""
        profile = _group_profile()
        recs = evaluate_all_rules(profile)
        assert recs == []

    def test_multiple_rules_can_fire(self):
        """An unhealthy group can trigger multiple rules."""
        profile = _group_profile(
            number_of_replicas=0,
            source_enabled=False,
        )
        codes = _rec_codes(evaluate_all_rules(profile))
        assert "replica_risk" in codes
        assert "source_disabled" in codes

    def test_all_recommendations_have_required_fields(self):
        """Every recommendation must have all required fields."""
        # Trigger multiple rules
        profile = _group_profile(
            primary_shard_count=10,
            avg_primary_shard_size_bytes=100_000_000,  # 100MB
            total_primary_store_bytes=1_000_000_000,
            number_of_replicas=0,
            source_enabled=False,
            field_count=950,
        )
        recs = evaluate_all_rules(profile)
        assert len(recs) >= 3  # at least shard_too_small, replica_risk, source_disabled, field_count

        required_keys = {
            "category", "recommendation", "severity", "current_value",
            "why", "how", "reference_url", "breaking_change",
        }
        for rec in recs:
            missing = required_keys - set(rec.keys())
            assert not missing, f"Missing keys {missing} in {rec['recommendation']}"
            assert rec["severity"] in ("info", "warning", "critical")
            assert rec["reference_url"].startswith("https://")
            assert isinstance(rec["breaking_change"], bool)
            assert len(rec["why"]) > 50  # substantive explanation
            assert len(rec["how"]) > 50  # substantive steps


# ---------------------------------------------------------------------------
# estimate_rollover_hours
# ---------------------------------------------------------------------------

class TestEstimateRolloverHours:
    def test_daily_rollover(self):
        dates = [
            "2026-02-10T00:00:00.000Z",
            "2026-02-11T00:00:00.000Z",
            "2026-02-12T00:00:00.000Z",
        ]
        hours = estimate_rollover_hours(dates)
        assert hours == pytest.approx(24.0, abs=0.1)

    def test_weekly_rollover(self):
        dates = [
            "2026-02-01T00:00:00.000Z",
            "2026-02-08T00:00:00.000Z",
            "2026-02-15T00:00:00.000Z",
        ]
        hours = estimate_rollover_hours(dates)
        assert hours == pytest.approx(168.0, abs=0.1)

    def test_single_index_returns_none(self):
        assert estimate_rollover_hours(["2026-02-10T00:00:00.000Z"]) is None

    def test_empty_returns_none(self):
        assert estimate_rollover_hours([]) is None

    def test_handles_unordered_dates(self):
        dates = [
            "2026-02-12T00:00:00.000Z",
            "2026-02-10T00:00:00.000Z",
            "2026-02-11T00:00:00.000Z",
        ]
        hours = estimate_rollover_hours(dates)
        assert hours == pytest.approx(24.0, abs=0.1)

    def test_handles_invalid_dates(self):
        dates = ["not-a-date", "also-not-a-date"]
        assert estimate_rollover_hours(dates) is None


# ---------------------------------------------------------------------------
# parse_usage_stats_response
# ---------------------------------------------------------------------------

class TestParseUsageStatsResponse:
    def test_parses_all_fields(self):
        response = {
            "aggregations": {
                "lookback_percentiles": {
                    "values": {"50.0": 43200.0, "95.0": 86400.0}
                },
                "sorted_fields": {
                    "buckets": [
                        {"key": "timestamp", "doc_count": 80},
                        {"key": "price", "doc_count": 10},
                    ]
                },
                "total_sorted_queries": {"doc_count": 100},
                "operations": {
                    "buckets": [
                        {"key": "search", "doc_count": 500},
                        {"key": "bulk", "doc_count": 200},
                        {"key": "index", "doc_count": 50},
                    ]
                },
            }
        }
        stats = parse_usage_stats_response(response)
        assert stats["lookback_p50_seconds"] == 43200.0
        assert stats["lookback_p95_seconds"] == 86400.0
        assert stats["dominant_sort_field"] == "timestamp"
        assert stats["dominant_sort_pct"] == pytest.approx(0.8)
        assert stats["search_count"] == 500
        assert stats["write_count"] == 250  # 200 bulk + 50 index

    def test_handles_empty_aggregations(self):
        stats = parse_usage_stats_response({"aggregations": {}})
        assert stats["lookback_p50_seconds"] is None
        assert stats["lookback_p95_seconds"] is None
        assert stats["dominant_sort_field"] is None
        assert stats["dominant_sort_pct"] is None
        assert stats["search_count"] == 0
        assert stats["write_count"] == 0

    def test_handles_missing_aggregations(self):
        stats = parse_usage_stats_response({})
        assert stats["lookback_p50_seconds"] is None
        assert stats["search_count"] == 0

    def test_handles_no_sorted_queries(self):
        response = {
            "aggregations": {
                "sorted_fields": {"buckets": []},
                "total_sorted_queries": {"doc_count": 0},
            }
        }
        stats = parse_usage_stats_response(response)
        assert stats["dominant_sort_field"] is None
        assert stats["dominant_sort_pct"] is None

    def test_counts_async_search(self):
        response = {
            "aggregations": {
                "operations": {
                    "buckets": [
                        {"key": "async_search", "doc_count": 300},
                        {"key": "count", "doc_count": 100},
                    ]
                },
            }
        }
        stats = parse_usage_stats_response(response)
        assert stats["search_count"] == 400


# ---------------------------------------------------------------------------
# partition_by_group
# ---------------------------------------------------------------------------

class TestPartitionByGroup:
    def test_partitions_correctly(self):
        rows = [
            {"index": "logs-000001", "store": "100"},
            {"index": "logs-000002", "store": "200"},
            {"index": "products", "store": "50"},
        ]
        index_to_group = {
            "logs-000001": "logs",
            "logs-000002": "logs",
            "products": "products",
        }
        result = partition_by_group(rows, index_to_group)
        assert len(result["logs"]) == 2
        assert len(result["products"]) == 1

    def test_skips_unknown_indices(self):
        rows = [
            {"index": "logs-000001", "store": "100"},
            {"index": "unknown-index", "store": "50"},
        ]
        result = partition_by_group(rows, {"logs-000001": "logs"})
        assert len(result["logs"]) == 1
        assert "unknown-index" not in result

    def test_empty_rows(self):
        result = partition_by_group([], {"logs-000001": "logs"})
        assert result == {}


# ---------------------------------------------------------------------------
# pick_representative_index
# ---------------------------------------------------------------------------

class TestPickRepresentativeIndex:
    def test_picks_latest(self):
        indices = ["logs-000001", "logs-000003", "logs-000002"]
        assert pick_representative_index(indices) == "logs-000003"

    def test_single_index(self):
        assert pick_representative_index(["products"]) == "products"


# ---------------------------------------------------------------------------
# build_usage_stats_query
# ---------------------------------------------------------------------------

class TestBuildUsageStatsQuery:
    def test_query_structure(self):
        q = build_usage_stats_query("logs", 168)
        assert q["size"] == 0
        filters = q["query"]["bool"]["filter"]
        assert {"term": {"index_group": "logs"}} in filters
        assert "lookback_percentiles" in q["aggs"]
        assert "sorted_fields" in q["aggs"]
        assert "operations" in q["aggs"]
        assert "total_sorted_queries" in q["aggs"]


# ---------------------------------------------------------------------------
# build_group_profile
# ---------------------------------------------------------------------------

class TestBuildGroupProfile:
    def test_calculates_avg_shard_size(self):
        cat_indices = [
            {"index": "logs-000001", "creation.date.string": "2026-02-14T00:00:00.000Z"},
        ]
        cat_shards = [
            {"index": "logs-000001", "shard": "0", "prirep": "p",
             "state": "STARTED", "docs": "100", "store": "5000000000"},
            {"index": "logs-000001", "shard": "1", "prirep": "p",
             "state": "STARTED", "docs": "100", "store": "3000000000"},
            {"index": "logs-000001", "shard": "0", "prirep": "r",
             "state": "STARTED", "docs": "100", "store": "5000000000"},
        ]
        flat_settings = {"index.number_of_replicas": "1"}
        profile = build_group_profile(
            "logs", cat_indices, cat_shards, flat_settings,
            mapping_field_count=50, source_enabled=True, usage_stats=None,
        )
        # 2 primary shards: 5GB + 3GB = 8GB, avg = 4GB
        assert profile["primary_shard_count"] == 2
        assert profile["total_primary_store_bytes"] == 8_000_000_000
        assert profile["avg_primary_shard_size_bytes"] == 4_000_000_000

    def test_handles_string_numeric_values(self):
        """_cat API returns numbers as strings — profile builder must handle."""
        cat_indices = [
            {"index": "logs-000001", "creation.date.string": "2026-02-14T00:00:00.000Z"},
        ]
        cat_shards = [
            {"index": "logs-000001", "shard": "0", "prirep": "p",
             "state": "STARTED", "docs": "500", "store": "10000000000"},
        ]
        flat_settings = {
            "index.number_of_replicas": "2",
            "index.mapping.total_fields.limit": "2000",
        }
        profile = build_group_profile(
            "logs", cat_indices, cat_shards, flat_settings,
            mapping_field_count=100, source_enabled=True, usage_stats=None,
        )
        assert profile["number_of_replicas"] == 2
        assert profile["total_fields_limit"] == 2000

    def test_excludes_replica_and_non_started_shards(self):
        cat_shards = [
            {"index": "logs", "shard": "0", "prirep": "p",
             "state": "STARTED", "store": "1000"},
            {"index": "logs", "shard": "0", "prirep": "r",
             "state": "STARTED", "store": "1000"},
            {"index": "logs", "shard": "1", "prirep": "p",
             "state": "RELOCATING", "store": "2000"},  # not STARTED
        ]
        profile = build_group_profile(
            "logs", [{"index": "logs"}], cat_shards, {},
            mapping_field_count=10, source_enabled=True, usage_stats=None,
        )
        # Only 1 primary STARTED shard
        assert profile["primary_shard_count"] == 1

    def test_passes_usage_stats(self):
        usage = {
            "lookback_p50_seconds": 3600.0,
            "lookback_p95_seconds": 7200.0,
            "dominant_sort_field": "ts",
            "dominant_sort_pct": 0.9,
            "search_count": 50,
            "write_count": 200,
        }
        profile = build_group_profile(
            "logs", [{"index": "logs"}], [], {},
            mapping_field_count=10, source_enabled=True, usage_stats=usage,
        )
        assert profile["lookback_p50_seconds"] == 3600.0
        assert profile["dominant_sort_field"] == "ts"
        assert profile["write_count"] == 200

    def test_index_sort_field_parsing(self):
        flat_settings = {"index.sort.field": "@timestamp"}
        profile = build_group_profile(
            "logs", [{"index": "logs"}], [], flat_settings,
            mapping_field_count=10, source_enabled=True, usage_stats=None,
        )
        assert profile["index_sort_field"] == ["@timestamp"]

    def test_blocks_write_parsing(self):
        flat_settings = {"index.blocks.write": "true"}
        profile = build_group_profile(
            "logs", [{"index": "logs"}], [], flat_settings,
            mapping_field_count=10, source_enabled=True, usage_stats=None,
        )
        assert profile["blocks_write"] is True
