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


class TestConfidenceBands:
    """Validate confidence band thresholds and constraints."""

    def test_defaults_per_spec(self) -> None:
        from moneybin.config import ConfidenceBands

        b = ConfidenceBands()
        assert b.t_high == 0.90
        assert b.t_med == 0.70

    def test_rejects_inverted(self) -> None:
        from moneybin.config import ConfidenceBands

        with pytest.raises(ValueError, match="t_high must be >= t_med"):
            ConfidenceBands(t_high=0.5, t_med=0.7)

    def test_rejects_out_of_range(self) -> None:
        from moneybin.config import ConfidenceBands

        with pytest.raises(ValueError):
            ConfidenceBands(t_high=1.5, t_med=0.7)
        with pytest.raises(ValueError):
            ConfidenceBands(t_high=0.9, t_med=-0.1)

    def test_accepts_equal_bands(self) -> None:
        from moneybin.config import ConfidenceBands

        b = ConfidenceBands(t_high=0.8, t_med=0.8)
        assert b.t_high == b.t_med


class TestImportSettingsConfidence:
    """Validate ImportSettings carries confidence + self_accept_high."""

    def test_default_self_accept_high_false(self) -> None:
        from moneybin.config import ImportSettings

        s = ImportSettings()
        assert s.self_accept_high is False

    def test_self_accept_high_can_enable(self) -> None:
        from moneybin.config import ImportSettings

        s = ImportSettings(self_accept_high=True)
        assert s.self_accept_high is True

    def test_confidence_bands_default(self) -> None:
        from moneybin.config import ImportSettings

        s = ImportSettings()
        assert s.confidence.t_high == 0.90
        assert s.confidence.t_med == 0.70
