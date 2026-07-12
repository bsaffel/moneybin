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
def test_mcp_tool_timeout_below_write_lock_wait_rejected() -> None:
    """A tool_timeout under the write-lock wait reopens the late-write window.

    Below the wait, a write tool can time out while its uncancellable worker is
    still queued at the lock; the worker may later acquire and commit after the
    caller already received a timeout envelope. The validator forbids it.
    """
    from moneybin.config import DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS

    with pytest.raises(ValueError, match="write-lock wait"):
        MCPConfig(tool_timeout_seconds=DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS - 1.0)


@pytest.mark.unit
def test_mcp_tool_timeout_at_write_lock_wait_accepted() -> None:
    """tool_timeout equal to the write-lock wait is the boundary and allowed."""
    from moneybin.config import DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS

    cfg = MCPConfig(tool_timeout_seconds=DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS)
    assert cfg.tool_timeout_seconds == DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS


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


def test_ai_config_defaults() -> None:
    from moneybin.config import MoneyBinSettings

    settings = MoneyBinSettings()
    assert settings.ai.default_backend is None
    assert settings.ai.consent_policy == "standard"


def test_ai_config_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from moneybin.config import MoneyBinSettings

    monkeypatch.setenv("MONEYBIN_AI__DEFAULT_BACKEND", "anthropic")
    monkeypatch.setenv("MONEYBIN_AI__CONSENT_POLICY", "strict")
    settings = MoneyBinSettings()
    assert settings.ai.default_backend == "anthropic"
    assert settings.ai.consent_policy == "strict"


def test_source_priority_ranks_ofx_above_tabular_family() -> None:
    """RD-1: ofx must outrank the tabular family.

    So D4 COALESCE can't let a later CSV null an OFX account's routing_number.
    """
    from moneybin.config import MatchingSettings

    order = MatchingSettings().source_priority
    assert order.index("ofx") < order.index("csv")
    assert order.index("ofx") < order.index("plaid")
    assert order.index("manual") < order.index("ofx")
    assert order.index("gsheet") < order.index("ofx")


def test_auto_rule_guard_defaults() -> None:
    """The F17 guard ships with safe defaults: 4-char floor, 20-match floor, 10x factor."""
    from moneybin.config import CategorizationSettings

    s = CategorizationSettings()
    assert s.auto_rule_min_contains_length == 4
    assert s.auto_rule_broad_match_min == 20
    assert s.auto_rule_broad_match_factor == 10
