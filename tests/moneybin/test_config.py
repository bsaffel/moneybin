"""Tests for MoneyBin configuration system."""

import pytest

from moneybin.config import MCPConfig


def test_profile_exports_dir_is_profile_scoped() -> None:
    """Exports live beside the active profile's inbox rather than the shared root."""
    from moneybin.config import MoneyBinSettings

    settings = MoneyBinSettings(profile="alex")
    assert (
        settings.profile_exports_dir == settings.import_.inbox_root / "alex" / "exports"
    )


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


@pytest.mark.unit
def test_mcp_deprecated_compatibility_settings_remain_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy MCP settings load but advertise that they no longer take effect."""
    from moneybin.config import MoneyBinSettings, clear_settings_cache

    monkeypatch.setenv("MONEYBIN_MCP__MAX_CHARS", "123")
    monkeypatch.setenv("MONEYBIN_MCP__ALLOWED_TABLES", '["core.dim_accounts"]')
    clear_settings_cache()

    settings = MoneyBinSettings()

    compatibility_values = settings.mcp.model_dump()
    assert compatibility_values["max_chars"] == 123
    assert compatibility_values["allowed_tables"] == ["core.dim_accounts"]
    assert MCPConfig.model_fields["max_chars"].deprecated
    assert MCPConfig.model_fields["allowed_tables"].deprecated


@pytest.mark.unit
def test_mcp_elicitation_wait_default() -> None:
    """The default is read from the constant, not restated as a literal."""
    from moneybin.config import DEFAULT_ELICITATION_WAIT_SECONDS

    assert MCPConfig().elicitation_wait_seconds == DEFAULT_ELICITATION_WAIT_SECONDS


@pytest.mark.unit
@pytest.mark.parametrize("wait_seconds", [0.0, -1.0])
def test_mcp_elicitation_wait_must_be_positive(wait_seconds: float) -> None:
    """A non-positive window makes every confirmation prompt un-answerable.

    The bound is business logic, not decoration: at zero the wait expires before
    a human can read the prompt, so every destructive gate silently degrades to
    the opaque-token path and the human never sees the question.
    """
    with pytest.raises(ValueError):
        MCPConfig(elicitation_wait_seconds=wait_seconds)


@pytest.mark.unit
def test_mcp_confirmation_ttl_default() -> None:
    assert MCPConfig().confirmation_ttl_seconds == 300


@pytest.mark.unit
def test_mcp_confirmation_ttl_constants_define_inclusive_range() -> None:
    from moneybin import config

    assert config.MIN_CONFIRMATION_TTL_SECONDS == 30
    assert config.DEFAULT_CONFIRMATION_TTL_SECONDS == 300
    assert config.MAX_CONFIRMATION_TTL_SECONDS == 900


@pytest.mark.unit
@pytest.mark.parametrize("ttl_seconds", [30, 900])
def test_mcp_confirmation_ttl_accepts_inclusive_bounds(ttl_seconds: int) -> None:
    assert MCPConfig(confirmation_ttl_seconds=ttl_seconds).confirmation_ttl_seconds == (
        ttl_seconds
    )


@pytest.mark.unit
@pytest.mark.parametrize("ttl_seconds", [29, 901])
def test_mcp_confirmation_ttl_rejects_out_of_range(ttl_seconds: int) -> None:
    with pytest.raises(ValueError):
        MCPConfig(confirmation_ttl_seconds=ttl_seconds)


@pytest.mark.unit
def test_mcp_confirmation_ttl_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.config import MoneyBinSettings, clear_settings_cache

    monkeypatch.setenv("MONEYBIN_MCP__CONFIRMATION_TTL_SECONDS", "600")
    clear_settings_cache()
    settings = MoneyBinSettings()
    assert settings.mcp.confirmation_ttl_seconds == 600


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


@pytest.mark.unit
def test_gsheet_oauth_client_id_ships_embedded_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Sheets connector authorizes on a fresh install with no dotenv.

    connect-gsheet.md ships a public installed-app client ID so a pip user
    never touches Google Cloud Console. SyncConfig carries a same-named Auth0
    field; embedding must not land there.

    The env vars are cleared because this asserts the shipped default: a
    developer with either override exported would otherwise see a failure that
    reads like a regression in the embedded value.
    """
    from moneybin.config import GSHEET_PUBLIC_OAUTH_CLIENT_ID, MoneyBinSettings

    monkeypatch.delenv("MONEYBIN_GSHEET__OAUTH_CLIENT_ID", raising=False)
    monkeypatch.delenv("MONEYBIN_SYNC__OAUTH_CLIENT_ID", raising=False)

    settings = MoneyBinSettings()

    assert settings.gsheet.oauth_client_id == GSHEET_PUBLIC_OAUTH_CLIENT_ID
    assert GSHEET_PUBLIC_OAUTH_CLIENT_ID.endswith(".apps.googleusercontent.com")
    assert settings.sync.oauth_client_id is None


def test_gsheet_oauth_client_secret_defaults_to_the_shipped_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The secret ships beside the ID so a bare install can authorize.

    Google's Desktop clients require the secret in the code->token exchange, so
    a ``None`` default leaves every user who has not registered their own Cloud
    project unable to authorize at all.
    """
    from moneybin.config import GSHEET_PUBLIC_OAUTH_CLIENT_SECRET, MoneyBinSettings

    monkeypatch.delenv("MONEYBIN_GSHEET__OAUTH_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("MONEYBIN_SYNC__OAUTH_CLIENT_SECRET", raising=False)

    settings = MoneyBinSettings()

    secret = settings.gsheet.oauth_client_secret
    assert secret is not None
    assert secret.get_secret_value() == GSHEET_PUBLIC_OAUTH_CLIENT_SECRET
    assert settings.sync.oauth_client_secret is None


def test_gsheet_oauth_client_secret_override_lands_on_gsheet_not_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The override configures the Sheets desktop client, not Auth0.

    SyncConfig carries a same-named oauth_client_secret; a wrong-field edit
    would populate that instead and leave the Sheets exchange unauthenticated.
    """
    from moneybin.config import MoneyBinSettings

    expected = "desktop-secret"
    monkeypatch.setenv("MONEYBIN_GSHEET__OAUTH_CLIENT_SECRET", expected)
    monkeypatch.delenv("MONEYBIN_SYNC__OAUTH_CLIENT_SECRET", raising=False)

    settings = MoneyBinSettings()

    secret = settings.gsheet.oauth_client_secret
    assert secret is not None
    assert secret.get_secret_value() == expected
    assert settings.sync.oauth_client_secret is None
