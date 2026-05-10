"""Tests for MoneyBin configuration system."""

import pytest

from moneybin.config import MCPConfig


@pytest.mark.unit
def test_mcp_tool_timeout_default() -> None:
    cfg = MCPConfig()
    assert cfg.tool_timeout_seconds == 30.0


@pytest.mark.unit
def test_mcp_tool_timeout_must_be_positive() -> None:
    with pytest.raises(ValueError):
        MCPConfig(tool_timeout_seconds=0.0)


@pytest.mark.unit
def test_mcp_max_items_default() -> None:
    """max_items defaults to 500 — collection-cap convention."""
    cfg = MCPConfig()
    assert cfg.max_items == 500


@pytest.mark.unit
def test_mcp_max_items_must_be_positive() -> None:
    with pytest.raises(ValueError):
        MCPConfig(max_items=0)


@pytest.mark.unit
def test_mcp_max_items_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """max_items honors env var override via MoneyBinSettings."""
    from moneybin.config import MoneyBinSettings, clear_settings_cache

    monkeypatch.setenv("MONEYBIN_MCP__MAX_ITEMS", "100")
    clear_settings_cache()
    s = MoneyBinSettings()
    assert s.mcp.max_items == 100


def test_categorization_settings_defaults() -> None:
    """Test CategorizationSettings default values."""
    from moneybin.config import CategorizationSettings

    s = CategorizationSettings()
    assert s.auto_rule_proposal_threshold == 1
    assert s.auto_rule_override_threshold == 2
    assert s.auto_rule_default_priority == 200


def test_categorization_settings_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test CategorizationSettings respects environment variable overrides."""
    from moneybin.config import MoneyBinSettings, clear_settings_cache

    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_PROPOSAL_THRESHOLD", "3")
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD", "5")
    clear_settings_cache()
    s = MoneyBinSettings()
    assert s.categorization.auto_rule_proposal_threshold == 3
    assert s.categorization.auto_rule_override_threshold == 5
