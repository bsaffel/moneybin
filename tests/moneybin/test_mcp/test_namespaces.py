# tests/moneybin/test_mcp/test_namespaces.py
"""Tests for namespace registry and progressive disclosure."""

import pytest

from moneybin.mcp.namespaces import (
    CORE_NAMESPACES_DEFAULT,
    EXTENDED_NAMESPACES,
    NamespaceRegistry,
    ToolDefinition,
)


def _make_tool(name: str, description: str = "A test tool") -> ToolDefinition:
    """Create a ToolDefinition for testing."""
    return ToolDefinition(
        name=name,
        description=description,
        fn=lambda: None,
    )


class TestToolDefinition:
    """Tests for ToolDefinition."""

    @pytest.mark.unit
    def test_namespace_extraction(self) -> None:
        tool = _make_tool("spending.summary")
        assert tool.namespace == "spending"

    @pytest.mark.unit
    def test_namespace_three_level(self) -> None:
        tool = _make_tool("transactions.matches.pending")
        assert tool.namespace == "transactions.matches"

    @pytest.mark.unit
    def test_no_namespace_raises(self) -> None:
        with pytest.raises(ValueError, match="must contain a dot"):
            _make_tool("badname")


class TestNamespaceRegistry:
    """Tests for the NamespaceRegistry."""

    @pytest.mark.unit
    def test_register_tool(self) -> None:
        registry = NamespaceRegistry()
        tool = _make_tool("spending.summary")
        registry.register(tool)
        assert "spending" in registry.all_namespaces()

    @pytest.mark.unit
    def test_get_namespace_tools(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("spending.by_category"))
        tools = registry.get_namespace_tools("spending")
        assert len(tools) == 2
        assert {t.name for t in tools} == {"spending.summary", "spending.by_category"}

    @pytest.mark.unit
    def test_core_tools(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("categorize.bulk"))

        core = registry.get_core_tools(core_namespaces={"spending"})
        names = {t.name for t in core}
        assert "spending.summary" in names
        assert "categorize.bulk" not in names

    @pytest.mark.unit
    def test_extended_tools(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("categorize.bulk"))

        extended = registry.get_extended_namespaces(core_namespaces={"spending"})
        assert "categorize" in extended
        assert "spending" not in extended

    @pytest.mark.unit
    def test_namespace_description(self) -> None:
        registry = NamespaceRegistry()
        registry.set_namespace_description("spending", "Expense analysis")
        assert registry.get_namespace_description("spending") == "Expense analysis"

    @pytest.mark.unit
    def test_loaded_tracking(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("categorize.bulk"))
        assert not registry.is_loaded("categorize")
        registry.mark_loaded("categorize")
        assert registry.is_loaded("categorize")

    @pytest.mark.unit
    def test_tools_resource_data(self) -> None:
        registry = NamespaceRegistry()
        registry.register(_make_tool("spending.summary"))
        registry.register(_make_tool("categorize.bulk"))
        registry.set_namespace_description("spending", "Expense analysis")
        registry.set_namespace_description("categorize", "Categorization pipeline")

        core_ns = {"spending"}
        registry.mark_loaded("spending")
        data = registry.tools_resource_data(core_ns)

        assert len(data["core"]) == 1
        assert data["core"][0]["namespace"] == "spending"
        assert data["core"][0]["loaded"] is True
        assert len(data["extended"]) == 1
        assert data["extended"][0]["namespace"] == "categorize"
        assert data["extended"][0]["loaded"] is False


class TestNamespaceConstants:
    """Tests for namespace constant definitions."""

    @pytest.mark.unit
    def test_core_namespaces_defined(self) -> None:
        assert "spending" in CORE_NAMESPACES_DEFAULT
        assert "accounts" in CORE_NAMESPACES_DEFAULT
        assert "transactions" in CORE_NAMESPACES_DEFAULT
        assert "overview" in CORE_NAMESPACES_DEFAULT
        assert "import" in CORE_NAMESPACES_DEFAULT

    @pytest.mark.unit
    def test_extended_namespaces_defined(self) -> None:
        assert "categorize" in EXTENDED_NAMESPACES
        assert "budget" in EXTENDED_NAMESPACES
        assert "tax" in EXTENDED_NAMESPACES
        assert "privacy" in EXTENDED_NAMESPACES

    @pytest.mark.unit
    def test_no_overlap(self) -> None:
        overlap = CORE_NAMESPACES_DEFAULT & EXTENDED_NAMESPACES
        assert overlap == set(), f"Overlapping namespaces: {overlap}"
