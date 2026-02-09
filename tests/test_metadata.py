"""Tests for gateway.metadata — index group resolution."""

from gateway.metadata import resolve_group, _index_to_group, _groups
import gateway.metadata as metadata_mod


def _set_metadata(lookup: dict, groups: dict):
    """Helper to set internal metadata state for testing."""
    metadata_mod._index_to_group = lookup
    metadata_mod._groups = groups


def _clear_metadata():
    metadata_mod._index_to_group = {}
    metadata_mod._groups = {}


class TestResolveGroup:
    """resolve_group maps concrete index → logical group."""

    def setup_method(self):
        # Simulate: logs-2026.02.04, logs-2026.02.05, logs-2026.02.06 → "logs" alias
        #           orders-us, orders-eu → "orders" alias
        #           products → itself (no alias)
        _set_metadata(
            lookup={
                "logs-2026.02.04": "logs",
                "logs-2026.02.05": "logs",
                "logs-2026.02.06": "logs",
                "orders-us": "orders",
                "orders-eu": "orders",
                "products": "products",
            },
            groups={
                "logs": ["logs-2026.02.04", "logs-2026.02.05", "logs-2026.02.06"],
                "orders": ["orders-us", "orders-eu"],
                "products": ["products"],
            },
        )

    def teardown_method(self):
        _clear_metadata()

    def test_concrete_index_resolves_to_alias(self):
        assert resolve_group("logs-2026.02.04") == "logs"
        assert resolve_group("logs-2026.02.05") == "logs"
        assert resolve_group("logs-2026.02.06") == "logs"

    def test_regional_index_resolves_to_alias(self):
        assert resolve_group("orders-us") == "orders"
        assert resolve_group("orders-eu") == "orders"

    def test_single_index_resolves_to_itself(self):
        assert resolve_group("products") == "products"

    def test_alias_name_resolves_to_itself(self):
        """When a query targets the alias directly (e.g., /logs/_search)."""
        assert resolve_group("logs") == "logs"
        assert resolve_group("orders") == "orders"

    def test_unknown_index_returns_itself(self):
        assert resolve_group("mystery-index") == "mystery-index"
        assert resolve_group("foo") == "foo"

    def test_empty_metadata_returns_itself(self):
        _clear_metadata()
        assert resolve_group("anything") == "anything"



class TestGetGroups:
    def setup_method(self):
        _set_metadata(
            lookup={"logs-2026.02.04": "logs", "products": "products"},
            groups={"logs": ["logs-2026.02.04"], "products": ["products"]},
        )

    def teardown_method(self):
        _clear_metadata()

    def test_returns_groups_dict(self):
        groups = metadata_mod.get_groups()
        assert "logs" in groups
        assert "products" in groups
        assert "logs-2026.02.04" in groups["logs"]

    def test_returns_copy(self):
        """get_groups should return a copy, not the internal dict."""
        groups = metadata_mod.get_groups()
        groups["new_group"] = ["test"]
        assert "new_group" not in metadata_mod.get_groups()
