"""Tests for gateway.analyzer — tier classification logic."""

from gateway.analyzer import _index_tier, _field_tier


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
