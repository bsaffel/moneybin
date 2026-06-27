"""Centralized configuration management for MoneyBin application.

This module provides a modern Pydantic Settings-based configuration system
that consolidates all application settings with environment variable integration,
type validation, and clear error handling.
"""

import math
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

# Direct-path imports of provider configs. We import from the concrete
# config modules (not the package ``__init__``) to avoid triggering
# tabular/__init__.py's lazy ``__getattr__``, which gates polars and is
# load-bearing for the CLI cold-start path.
from moneybin.extractors.ofx.config import OFXProviderConfig
from moneybin.extractors.plaid.config import PlaidProviderConfig
from moneybin.extractors.tabular.config import TabularProviderConfig


def _is_moneybin_repo(path: Path) -> bool:
    """Check if path is a moneybin repo checkout.

    Detects the moneybin repository by checking for .git directory and
    pyproject.toml with name = "moneybin".

    Args:
        path: Directory to check.

    Returns:
        True if path appears to be a moneybin repo checkout.
    """
    if not (path / ".git").exists():
        return False
    try:
        content = (path / "pyproject.toml").read_text()
        return '\nname = "moneybin"' in content
    except OSError:
        return False


def find_repo_root() -> Path | None:
    """Return the moneybin repo root if CWD *is* the repo root, else None.

    Only the exact CWD is checked — no parent traversal. Calling from a
    subdirectory of the repo (e.g. ``src/``) returns None. Mirrors the
    contract used by ``get_base_dir()``'s repo-detection branch.
    """
    cwd = Path.cwd()
    if _is_moneybin_repo(cwd):
        return cwd.resolve()
    return None


def get_base_dir() -> Path:
    """Determine the base directory for MoneyBin data and configuration.

    Resolution order:
        1. MONEYBIN_HOME env var (explicit override, always wins)
        2. MONEYBIN_ENVIRONMENT=development: <cwd>/.moneybin
        3. Repo checkout detection (.git + pyproject.toml name=moneybin): <cwd>/.moneybin
        4. Default: ~/.moneybin/

    Returns:
        Path: Absolute base directory for the application.
    """
    # os.getenv used intentionally: this runs during MoneyBinSettings.__init__
    # to resolve paths, so get_settings() is not yet available.
    moneybin_home = os.getenv("MONEYBIN_HOME")
    if moneybin_home:
        return Path(moneybin_home).expanduser().resolve()

    environment = os.getenv("MONEYBIN_ENVIRONMENT")
    if environment == "development":
        return (Path.cwd() / ".moneybin").resolve()

    if _is_moneybin_repo(Path.cwd()):
        return (Path.cwd() / ".moneybin").resolve()

    return (Path.home() / ".moneybin").resolve()


# Retired tabular env-var namespace. ``MoneyBinSettings.__init__`` errors out
# if any key with this prefix appears in ``os.environ`` or the active dotenv
# file — the knobs moved to ``MONEYBIN_PROVIDERS__TABULAR__*`` in Plan 1 of
# the extension-contracts implementation and pydantic-settings would
# otherwise silently ignore the old names (``extra="ignore"``).
_LEGACY_TABULAR_PREFIX = "MONEYBIN_DATA__TABULAR__"


class DatabaseConfig(BaseModel):
    """Database configuration settings."""

    model_config = ConfigDict(frozen=True)

    path: Path = Field(
        default=Path("data/default/moneybin.duckdb"),
        description="Path to DuckDB database file",
    )
    backup_path: Path | None = Field(
        default=None, description="Path to database backup directory"
    )
    encryption_key_mode: Literal["auto", "passphrase"] = Field(
        default="auto",
        description="How the encryption key is managed: auto-generated or user passphrase",
    )
    temp_directory: Path | None = Field(
        default=None,
        description="DuckDB temp spill directory. Defaults to data/<profile>/temp/",
    )
    # Argon2id parameters for passphrase-based key derivation.
    # WARNING: changing these after a database is created locks you out —
    # the derived key will differ and the database will be unreadable.
    argon2_time_cost: int = Field(
        default=3, ge=1, description="Argon2id time cost (iterations)"
    )
    argon2_memory_cost: int = Field(
        default=65536, ge=8192, description="Argon2id memory cost in KiB"
    )
    argon2_parallelism: int = Field(
        default=4, ge=1, description="Argon2id degree of parallelism"
    )
    argon2_hash_len: int = Field(
        default=32, ge=16, description="Argon2id output hash length in bytes"
    )
    no_auto_upgrade: bool = Field(
        default=False,
        description="Skip versioned migrations and SQLMesh migrate on startup. "
        "Encryption and schema init still run.",
    )

    @field_validator("path")
    @classmethod
    def validate_database_path(cls, v: Path) -> Path:
        """Ensure database path has correct extension."""
        if not str(v).endswith((".db", ".duckdb")):
            raise ValueError("Database path must end with .db or .duckdb")
        return v


class DataConfig(BaseModel):
    """Data processing and storage configuration."""

    model_config = ConfigDict(frozen=True)

    raw_data_path: Path = Field(
        default=Path("data/raw"), description="Path to raw data directory"
    )
    temp_data_path: Path = Field(
        default=Path("data/temp"), description="Path to temporary data directory"
    )
    incremental_loading: bool = Field(
        default=True, description="Use incremental loading by default"
    )


class ProvidersSettings(BaseModel):
    """Per-provider configuration nested under MoneyBinSettings.providers.<name>.

    Each provider declares a ProviderConfig subclass in its package
    (``extractors/<name>/config.py``); the framework merges them here.
    Env-var override follows the standard nested-delimiter shape, e.g.
    ``MONEYBIN_PROVIDERS__TABULAR__TEXT_SIZE_LIMIT_MB=20``.
    """

    model_config = ConfigDict(frozen=True)

    ofx: OFXProviderConfig = Field(default_factory=OFXProviderConfig)
    plaid: PlaidProviderConfig = Field(default_factory=PlaidProviderConfig)
    tabular: TabularProviderConfig = Field(default_factory=TabularProviderConfig)


class PackagesSettings(BaseModel):
    """Per-package configuration nested under MoneyBinSettings.packages.<name>.

    Reference packages (assets, us_tax — Plan 4) declare fields here matching
    their Pydantic settings models. Empty by default — packages may be
    installed without any per-package overrides.

    Env-var override: until reference packages declare typed sub-fields
    (Plan 4), per-package values must use the JSON form, e.g.
    MONEYBIN_PACKAGES='{"assets": {"valuation_provider": "zillow"}}'.
    The nested-delimiter form (MONEYBIN_PACKAGES__ASSETS__...) works only
    once a package declares the field as a typed model attribute.

    Mirrors the ProvidersSettings pattern established by Plan 1. extra="allow"
    lets a runtime-installed package populate this without a Core schema
    change; reference packages add explicit typed fields when they land.
    """

    model_config = ConfigDict(frozen=True, extra="allow")


class LoggingConfig(BaseModel):
    """Logging configuration settings."""

    model_config = ConfigDict(frozen=True)

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Logging level"
    )
    log_to_file: bool = Field(default=True, description="Enable file logging")
    log_file_path: Path = Field(
        default=Path("logs/default/moneybin.log"), description="Path to log file"
    )
    format: Literal["human", "json"] = Field(
        default="human", description="Log output format: human-readable or JSON"
    )
    # PII sanitization (SanitizedLogFormatter) is always on and cannot be
    # disabled — it's a security invariant, not a tunable preference.


class MetricsConfig(BaseModel):
    """Metrics collection and persistence configuration."""

    model_config = ConfigDict(frozen=True)


# Default seconds get_database() blocks for the per-profile write lock before
# raising DatabaseLockError. Lives here (not database.py) so MCPConfig's
# tool-timeout validator can reference it without an import cycle — database
# imports config, never the reverse. get_database's max_wait default imports
# this value back. See database-writer-coordination.md.
DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS: float = 10.0


class MCPConfig(BaseModel):
    """MCP server runtime configuration."""

    model_config = ConfigDict(frozen=True)

    max_rows: int = Field(
        default=1000, ge=1, description="Maximum rows returned by any MCP query tool"
    )
    max_chars: int = Field(
        default=50000, ge=1, description="Maximum characters in any MCP tool response"
    )
    allowed_tables: list[str] | None = Field(
        default=None,
        description=(
            "Optional allowlist of fully-qualified table names the query tool may access "
            '(e.g. ["core.fct_transactions"]). None means all tables are permitted.'
        ),
    )
    max_items: int = Field(
        default=500,
        ge=1,
        description=(
            "Maximum length of any list-typed parameter accepted by an MCP tool. "
            "Validated at the decorator level for any tool whose signature includes "
            "list[X] / Sequence[X] / tuple[X, ...]. Exceeding returns a "
            "ResponseEnvelope.error with code='too_many_items'. Parallels max_rows "
            "for read responses. See docs/specs/moneybin-mcp.md §Collection size cap."
        ),
    )
    tool_timeout_seconds: float = Field(
        default=30.0,
        gt=0.0,
        description=(
            "Hard wall-clock cap for any single MCP tool dispatch. On timeout, "
            "the active DuckDB statement is interrupted and the connection is "
            "reset so subsequent calls aren't wedged behind a stale write lock. "
            "Must be >= the write-lock wait so a timed-out write tool's worker "
            "can never acquire the lock and commit after the caller gave up."
        ),
    )

    @field_validator("tool_timeout_seconds")
    @classmethod
    def _timeout_covers_write_lock_wait(cls, v: float) -> float:
        # A sync write tool runs in an uncancellable thread-pool worker. If the
        # tool timeout is shorter than the write-lock wait, the caller can time
        # out while the worker is still queued at the lock; the worker may then
        # acquire and commit after the timeout envelope was already returned.
        # Requiring timeout >= the wait guarantees the worker has stopped
        # queuing (acquired or errored) by the time the caller gives up.
        if v < DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS:
            raise ValueError(
                f"tool_timeout_seconds ({v}s) must be >= the write-lock wait "
                f"({DEFAULT_WRITE_LOCK_MAX_WAIT_SECONDS}s); a shorter cap lets a "
                f"timed-out write tool's worker commit after the caller gave up."
            )
        return v


class AIConfig(BaseModel):
    """AI backend and consent configuration.

    Minimal surface for the consent ledger. ``default_backend`` is the
    assumed consumer recorded on consent grants (the MCP/CLI surfaces
    cannot reliably introspect which AI host consumes their output —
    see privacy-and-ai-trust.md). ``consent_policy`` is recorded and
    surfaced today; its enforcement (re-prompt on every call) lands with
    the deferred consent-enforcement gate. (Distinct from a grant's
    per-grant ``consent_mode`` of persistent/one-time.) The per-provider
    API-key tree (Anthropic/OpenAI/Ollama configs) is deferred until an AI
    backend client exists.
    """

    model_config = ConfigDict(frozen=True)

    default_backend: str | None = Field(
        default=None,
        description="Default AI backend (e.g. anthropic, openai, ollama). "
        "None = no assumed backend; consent grants must name --backend explicitly.",
    )
    # Default is "standard" (non-disruptive). The field is inert until the
    # enforcement gate ships, so the default only takes effect then — at which
    # point the enforcement PR MUST make the secure-vs-ergonomic posture an
    # explicit decision rather than silently inheriting "standard". Tracked
    # with the deferred consent-enforcement gate (privacy-and-ai-trust.md).
    consent_policy: Literal["standard", "strict"] = Field(
        default="standard",
        description="standard: persistent consent grants persist. strict: "
        "(future) all AI calls re-prompt; recorded now, enforced with the consent gate.",
    )


class SyncConfig(BaseModel):
    """Configuration for MoneyBin Sync service (optional paid tier).

    The sync service provides automatic bank data synchronization through
    hosted connectors (Plaid, Yodlee, etc.) with E2E encryption.

    Security Model:
    - All bank access tokens stored server-side only
    - Client authenticates via OAuth2/Auth0 (future)
    - Client never handles or sees Plaid access tokens
    - All bank API communication happens server-side
    """

    model_config = ConfigDict(frozen=True)

    server_url: str | None = Field(
        default=None,
        description="MoneyBin Sync server URL (e.g., https://sync.moneybin.app)",
    )
    api_key: SecretStr | None = Field(
        default=None,
        description="API key for MoneyBin Sync service (legacy - prefer OAuth)",
    )
    use_local_server: bool = Field(
        default=True,
        description="Use local server code directly (development mode)",
    )

    # OAuth/Auth0 configuration (future)
    oauth_client_id: str | None = Field(
        default=None,
        description="OAuth2 client ID (for Auth0 integration)",
    )
    oauth_client_secret: SecretStr | None = Field(
        default=None,
        description="OAuth2 client secret (for Auth0 integration)",
    )
    oauth_domain: str | None = Field(
        default=None,
        description="OAuth2/Auth0 domain (e.g., moneybin.auth0.com)",
    )
    oauth_audience: str | None = Field(
        default=None,
        description="OAuth2 API audience/identifier",
    )


class GSheetSettings(BaseModel):
    """Configuration for the Google Sheets connector."""

    model_config = ConfigDict(frozen=True)

    oauth_client_id: str = Field(
        default="",
        description=(
            "OAuth 2.0 client ID for the installed-app Google OAuth flow. "
            "Empty disables the connector; set via "
            "MONEYBIN_GSHEET__OAUTH_CLIENT_ID."
        ),
    )
    api_timeout_seconds: float = Field(
        default=30.0,
        gt=0.0,
        description="HTTP timeout for Google Sheets API + token endpoint calls.",
    )


class ConfidenceBands(BaseModel):
    """Score thresholds used to band a detection into high/medium/low.

    Bands are calibration-tunable (see smart-import-confirmation.md Req 12);
    defaults match the brainstorm proposal (T_high 0.90 / T_med 0.70). PDF's
    inherited 0.70 reconciliation threshold maps to T_med.
    """

    model_config = ConfigDict(frozen=True)

    t_high: float = Field(default=0.90, ge=0.0, le=1.0)
    t_med: float = Field(default=0.70, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _check_ordering(self) -> "ConfidenceBands":
        if self.t_high < self.t_med:
            raise ValueError(
                f"t_high must be >= t_med (got t_high={self.t_high}, t_med={self.t_med})"
            )
        return self


class ImportSettings(BaseModel):
    """File-import related settings (inbox layout + confirmation gate)."""

    model_config = ConfigDict(frozen=True)

    inbox_root: Path = Field(
        default_factory=lambda: Path.home() / "Documents" / "MoneyBin",
        description=(
            "Parent directory for the user-facing import workspace. "
            "Per-profile subdirs (<inbox_root>/<profile>/{inbox,processed,failed}/) "
            "are created on first use. Defaults to ~/Documents/MoneyBin."
        ),
    )

    confidence: ConfidenceBands = Field(default_factory=ConfidenceBands)

    self_accept_high: bool = Field(
        default=False,
        description=(
            "When True, MCP-driven imports auto-accept a `high`-tier first "
            "encounter. Gated off until the calibration corpus proves the "
            "`high` band clears the precision bar (smart-import-confirmation.md "
            "Req 12). The CLI human path always prompts regardless."
        ),
    )


class DoctorSettings(BaseModel):
    """`moneybin doctor` integrity-check configuration.

    Tunes the per-table `app.*` audit-coverage invariants added by the
    repository layer (see `docs/specs/app-integrity-invariant.md` Req 9).
    """

    model_config = ConfigDict(frozen=True)

    audit_coverage_lookback_days: int = Field(
        default=7,
        ge=1,
        description=(
            "Audit-coverage checks only inspect protected app.* rows mutated "
            "within this many days. Bounds the cost on large profiles and lets "
            "users suppress pre-migration noise by lowering it after rollout."
        ),
    )
    audit_coverage_sample_cap: int = Field(
        default=1000,
        ge=1,
        description=(
            "Maximum rows sampled per table for the audit-coverage check. "
            "`moneybin doctor --full` bypasses the cap and scans every row."
        ),
    )


class MatchingSettings(BaseModel):
    """Transaction matching and dedup configuration."""

    model_config = ConfigDict(frozen=True)

    high_confidence_threshold: float = Field(
        default=0.95,
        ge=0.0,
        le=1.0,
        description="Auto-merge threshold (>= this score = accepted)",
    )
    review_threshold: float = Field(
        default=0.70,
        ge=0.0,
        le=1.0,
        description="Review queue threshold (>= this but < high = pending)",
    )
    date_window_days: int = Field(
        default=3,
        ge=0,
        description="Maximum days between transaction dates for candidate pairs",
    )
    source_priority: list[str] = Field(
        default=[
            "manual",
            "gsheet",
            "ofx",
            "plaid",
            "csv",
            "excel",
            "tsv",
            "parquet",
            "feather",
            "pipe",
        ],
        description="Source types in priority order (first = highest priority)",
    )
    transfer_review_threshold: float = Field(
        default=0.55,
        ge=0.0,
        le=1.0,
        description="Review queue threshold for transfer pairs",
    )
    transfer_signal_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "date_distance": 0.6,
            "keyword": 0.4,
        },
        description="Per-signal weights for transfer confidence scoring",
    )

    @field_validator("source_priority")
    @classmethod
    def validate_source_priority(cls, v: list[str]) -> list[str]:
        """Ensure source_priority is not empty."""
        if not v:
            raise ValueError("source_priority must not be empty")
        return v

    @field_validator("transfer_signal_weights")
    @classmethod
    def validate_transfer_weights(cls, v: dict[str, float]) -> dict[str, float]:
        """Ensure all required scoring keys are present and sum to 1.0."""
        required = {"date_distance", "keyword"}
        missing = required - v.keys()
        if missing:
            raise ValueError(f"transfer_signal_weights missing keys: {missing}")
        extra = v.keys() - required
        if extra:
            raise ValueError(f"transfer_signal_weights has unrecognised keys: {extra}")
        negative = {k: w for k, w in v.items() if w < 0}
        if negative:
            raise ValueError(f"transfer_signal_weights has negative values: {negative}")
        total = sum(v.values())
        if not math.isclose(total, 1.0, abs_tol=1e-6):
            raise ValueError(
                f"transfer_signal_weights must sum to 1.0, got {total:.6f}"
            )
        return v

    @model_validator(mode="after")
    def validate_threshold_ordering(self) -> "MatchingSettings":
        """Ensure review_threshold does not exceed high_confidence_threshold."""
        if self.review_threshold > self.high_confidence_threshold:
            raise ValueError(
                f"review_threshold ({self.review_threshold}) must be <= "
                f"high_confidence_threshold ({self.high_confidence_threshold})"
            )
        return self


class CategorizationSettings(BaseModel):
    """Auto-rule proposal and lifecycle configuration."""

    model_config = ConfigDict(frozen=True)

    auto_rule_proposal_threshold: int = Field(
        default=1,
        ge=1,
        description="Propose an auto-rule after N matching user categorizations",
    )
    auto_rule_override_threshold: int = Field(
        default=2,
        ge=1,
        description="Deactivate an auto-rule after N user overrides of its assignments",
    )
    auto_rule_default_priority: int = Field(
        default=200,
        ge=1,
        description="Priority assigned to promoted auto-rules (higher number = lower priority)",
    )
    auto_rule_sample_txn_cap: int = Field(
        default=5,
        ge=1,
        description="Maximum number of sample transaction IDs retained per proposal",
    )
    auto_rule_list_default_limit: int = Field(
        default=100,
        ge=1,
        description=(
            "Default LIMIT applied to auto-rule listing endpoints "
            "(categorize auto review, categorize auto rules) when no explicit "
            "--limit is given. "
            "Caps memory and response size for unbounded queues."
        ),
    )
    auto_rule_backfill_scan_cap: int = Field(
        default=50_000,
        ge=1,
        description=(
            "Maximum number of uncategorized transactions scanned when "
            "backfilling a newly-approved auto-rule. Caps memory of the "
            "in-Python match loop; transactions beyond the cap remain "
            "uncategorized until the next apply_rules run picks them up."
        ),
    )

    # Cold-start LLM-assist
    assist_offer_threshold: int = Field(
        default=10,
        ge=1,
        description="Min uncategorized count to surface assist hint in import summary",
    )
    assist_default_batch_size: int = Field(
        default=100,
        ge=1,
        le=500,
        description="Default txns per categorize_assist call",
    )
    assist_max_batch_size: int = Field(
        default=200,
        ge=1,
        le=500,
        description="Hard upper bound enforced server-side",
    )

    @model_validator(mode="after")
    def proposal_threshold_lte_override_threshold(self) -> "CategorizationSettings":
        """Ensure proposal_threshold <= override_threshold.

        The deactivation bar must not be lower than the creation bar. If
        ``override_threshold`` were less than ``proposal_threshold``, a
        pattern could accumulate enough user corrections to deactivate its
        rule (via ``check_overrides``) before enough auto-categorizations
        had ever occurred to propose the rule (via ``record_categorization``)
        — an obviously degenerate configuration.
        """
        if self.auto_rule_proposal_threshold > self.auto_rule_override_threshold:
            raise ValueError(
                f"auto_rule_proposal_threshold ({self.auto_rule_proposal_threshold}) "
                f"must be <= auto_rule_override_threshold "
                f"({self.auto_rule_override_threshold})"
            )
        return self


class MoneyBinSettings(BaseSettings):
    """Main application settings with environment variable integration.

    This class consolidates all configuration settings and provides
    automatic loading from environment variables with validation.

    Environment variables are loaded with the MONEYBIN_ prefix.
    For nested configs, use double underscores: MONEYBIN_DATABASE__PATH

    Profile Support:
    - Loads from .env.{profile} files (e.g., .env.dev, .env.prod)
    - Falls back to .env for backward compatibility
    - Profile defaults to "dev" for safety
    """

    # Core configuration sections
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    mcp: MCPConfig = Field(default_factory=MCPConfig)
    ai: AIConfig = Field(default_factory=AIConfig)
    sync: SyncConfig = Field(default_factory=SyncConfig)
    matching: MatchingSettings = Field(default_factory=MatchingSettings)
    doctor: DoctorSettings = Field(default_factory=DoctorSettings)
    categorization: CategorizationSettings = Field(
        default_factory=CategorizationSettings
    )
    import_: ImportSettings = Field(
        default_factory=ImportSettings,
        alias="import",
    )
    gsheet: GSheetSettings = Field(default_factory=GSheetSettings)
    providers: ProvidersSettings = Field(
        default_factory=ProvidersSettings,
        description="Per-provider configuration (OFX, Plaid, tabular).",
    )
    packages: PackagesSettings = Field(
        default_factory=PackagesSettings,
        description="Per-package configuration for installed analysis packages.",
    )

    # Application settings
    debug: bool = Field(default=False, description="Enable debug mode")
    environment: Literal["development", "staging", "production"] = Field(
        default="development", description="Application environment"
    )
    profile: str = Field(
        default="default",
        description="User profile name (e.g., alice, bob, household)",
    )

    @field_validator("profile")
    @classmethod
    def validate_profile_name(cls, v: str) -> str:
        """Ensure profile name is safe for use as a filename and normalize it."""
        from moneybin.utils.user_config import normalize_profile_name

        if not v:
            raise ValueError("Profile name cannot be empty")

        # Normalize profile name to lowercase with hyphens
        # This allows users to provide "John Smith", "Alice_Work", etc.
        # and converts them to "john-smith", "alice-work", etc.
        try:
            normalized = normalize_profile_name(v)
            return normalized
        except ValueError as e:
            raise ValueError(f"Invalid profile name: {e}") from e

    @staticmethod
    def _raise_on_deprecated_tabular_keys(profile: str) -> None:
        """Fail loudly if retired ``MONEYBIN_DATA__TABULAR__*`` keys are set.

        Scans both ``os.environ`` and the active dotenv file (the same
        lookup ``settings_customise_sources`` uses for the live load) so
        operators on either configuration path get a clear migration
        message rather than silently falling back to defaults.

        Prefix matching is case-insensitive to mirror pydantic-settings'
        ``case_sensitive=False`` config — lowercase or mixed-case legacy
        keys still get absorbed by the loader, so the guard must catch
        them too.
        """
        offenders: list[str] = sorted(
            k for k in os.environ if k.upper().startswith(_LEGACY_TABULAR_PREFIX)
        )

        base = get_base_dir()
        for env_file in (base / f".env.{profile}", base / ".env"):
            if not env_file.exists():
                continue
            for raw_line in env_file.read_text().splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key = line.split("=", 1)[0].strip()
                if key.upper().startswith(_LEGACY_TABULAR_PREFIX):
                    offenders.append(f"{env_file.name}:{key}")
            break  # Match settings_customise_sources: first existing file wins.

        if offenders:
            raise ValueError(
                "Deprecated env var(s) found: "
                f"{', '.join(offenders)}. Tabular provider settings moved "
                "to the MONEYBIN_PROVIDERS__TABULAR__* namespace. Rename "
                "each variable accordingly and re-run."
            )

    def __init__(self, **kwargs: Any):
        """Initialize settings with profile-based directory layout.

        Args:
            **kwargs: Additional configuration overrides
        """
        from moneybin.utils.user_config import normalize_profile_name

        # Get and normalize profile name BEFORE using it for paths
        # This ensures directories are created with normalized names
        raw_profile = kwargs.get("profile", "default")
        profile = normalize_profile_name(raw_profile)

        # Fail loudly on the retired tabular env-var namespace. Tabular
        # provider knobs moved from ``MONEYBIN_DATA__TABULAR__*`` to
        # ``MONEYBIN_PROVIDERS__TABULAR__*`` (Plan 1 of extension-contracts).
        # pydantic-settings silently ignores unknown env vars and unknown
        # dotenv keys (``extra="ignore"``), so this check scans both
        # ``os.environ`` and the active ``.env`` file (the same lookup the
        # settings_customise_sources hook uses) to catch operators on either
        # configuration path.
        self._raise_on_deprecated_tabular_keys(profile=profile)

        # Update kwargs with normalized profile name
        kwargs["profile"] = profile

        # Resolve all relative paths against the base directory so they work
        # regardless of the process's working directory (e.g. Claude Desktop MCP).
        base = get_base_dir()
        profile_dir = base / "profiles" / profile

        # Set database path if not explicitly provided
        if "database" not in kwargs:
            kwargs["database"] = DatabaseConfig(
                path=profile_dir / "moneybin.duckdb",
                backup_path=profile_dir / "backups",
                temp_directory=profile_dir / "temp",
            )

        if "data" not in kwargs:
            kwargs["data"] = DataConfig(
                raw_data_path=profile_dir / "raw",
                temp_data_path=profile_dir / "temp",
            )

        if "logging" not in kwargs:
            kwargs["logging"] = LoggingConfig(
                log_file_path=profile_dir / "logs" / "moneybin.log"
            )

        super().__init__(**kwargs)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        """Customize how settings are loaded to support profile-based env files.

        This method is called by Pydantic Settings to determine the order and
        sources of configuration loading.
        """
        # Get profile from init settings if provided
        # Pyright reports "Type of 'get' is partially unknown" because init_settings
        # from Pydantic doesn't have well-defined types in the stubs.
        init_dict = init_settings.init_kwargs if init_settings else {}
        profile = init_dict.get("profile", "dev")  # type: ignore[reportUnknownMemberType] — Pydantic init_settings has incomplete type stubs

        # Determine which env file to load based on profile
        base = get_base_dir()
        profile_env_file = base / f".env.{profile}"
        if profile_env_file.exists():
            env_file = str(profile_env_file)
        else:
            # Fall back to .env for backward compatibility
            env_file = str(base / ".env")

        # Create custom dotenv settings with the profile-specific file
        from pydantic_settings import DotEnvSettingsSource

        custom_dotenv = DotEnvSettingsSource(
            settings_cls,
            env_file=env_file,
            env_file_encoding="utf-8",
        )

        # Return sources in priority order (later sources override earlier ones)
        return (
            init_settings,
            env_settings,
            custom_dotenv,
            file_secret_settings,
        )

    model_config = SettingsConfigDict(
        # No default env_file: settings_customise_sources builds the real,
        # profile-aware dotenv source from get_base_dir(), so a CWD-relative
        # ".env" default is redundant — and pydantic stats/reads it eagerly at
        # construction, which breaks sandboxed runs that deny the repo-root .env
        # (PermissionError). See tests/moneybin/test_config_dotenv_isolation.py.
        env_file=None,
        env_file_encoding="utf-8",
        env_prefix="MONEYBIN_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # Allow extra env vars that don't match our schema
        frozen=True,
        populate_by_name=True,
    )

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate application environment."""
        if v == "production" and os.getenv("DEBUG", "").lower() in ("true", "1"):
            raise ValueError("DEBUG mode cannot be enabled in production")
        return v

    @property
    def profile_inbox_dir(self) -> Path:
        """Active profile's inbox parent: <inbox_root>/<profile>/."""
        return self.import_.inbox_root / self.profile


# Global settings - single instance for current profile
_current_settings: MoneyBinSettings | None = None


_current_profile: str | None = None


# Lazy profile resolver. Registered by the CLI entry point so that profile
# resolution (env → config.yaml → first-run wizard) happens only when a
# command actually needs settings — not eagerly in the parent callback.
# Without this, ``moneybin logs`` (a docker-style usage error) would fire
# the first-run wizard before Click ever surfaces the missing-arg error.
_profile_resolver: Callable[[], None] | None = None


def register_profile_resolver(fn: Callable[[], None] | None) -> None:
    """Register a callback that resolves and sets the current profile.

    The callback is invoked from ``get_settings()`` / ``get_current_profile()``
    when no profile is set yet. It must call ``set_current_profile(name)`` as
    a side effect (and may also do CLI-level setup like observability).
    Pass ``None`` to clear the registration (used by tests).
    """
    global _profile_resolver
    _profile_resolver = fn


def _maybe_resolve_profile() -> None:
    """Invoke the registered resolver if no profile is set."""
    if _current_profile is None and _profile_resolver is not None:
        _profile_resolver()


def get_settings() -> MoneyBinSettings:
    """Get the settings instance for the current user profile.

    This function provides a singleton pattern for accessing configuration
    throughout the application. Settings are loaded once and cached for the
    current profile.

    Returns:
        MoneyBinSettings: The configuration instance for the current profile

    Raises:
        ValueError: If required configuration is missing or invalid

    Note:
        The profile is determined by _current_profile, which can be changed
        via set_current_profile(). Changing the profile invalidates the cache.
    """
    global _current_settings, _current_profile

    # Return cached settings if available
    if _current_settings is not None:
        return _current_settings

    _maybe_resolve_profile()

    if _current_profile is None:
        raise RuntimeError(
            "No profile set. Call set_current_profile() before get_settings()."
        )

    # Load and cache new settings for current profile
    try:
        settings = MoneyBinSettings(profile=_current_profile)
        _current_settings = settings
        return settings

    except Exception as e:
        raise ValueError(
            f"Configuration error for profile '{_current_profile}': {e}"
        ) from e


def set_current_profile(profile: str) -> None:
    """Set the current active user profile.

    Changing the profile invalidates the cached settings, forcing a reload
    on the next call to get_settings().

    Args:
        profile: User profile name (e.g., 'alice', 'bob', 'household')
                 Will be normalized to lowercase with hyphens

    Raises:
        ValueError: If profile name contains invalid characters
    """
    from moneybin.utils.user_config import normalize_profile_name

    global _current_profile, _current_settings

    if not profile:
        raise ValueError("Profile name cannot be empty")

    # Normalize profile name
    try:
        normalized = normalize_profile_name(profile)

        # Only invalidate cache if profile actually changed
        if _current_profile is None or normalized != _current_profile:
            _current_profile = normalized
            _current_settings = None  # Invalidate settings cache
            # Each profile has its own encryption key; clear the process-level
            # key cache so the next Database() open fetches the correct key.
            from moneybin.database import (
                invalidate_encryption_key_cache,  # noqa: PLC0415
            )

            invalidate_encryption_key_cache()
    except ValueError as e:
        raise ValueError(f"Invalid profile name: {e}") from e


def get_current_profile(*, auto_resolve: bool = True) -> str:
    """Get the current active user profile.

    Args:
        auto_resolve: When True (default), invoke the registered profile
            resolver if no profile has been set yet. Profile-management
            commands (``profile show``, ``profile set``) pass False to
            avoid triggering the first-run wizard during recovery flows.

    Returns:
        str: The current profile name (e.g., 'alice', 'bob', 'default')

    Raises:
        RuntimeError: If no profile is set and the resolver could not set one.
    """
    if auto_resolve:
        _maybe_resolve_profile()
    if _current_profile is None:
        raise RuntimeError(
            "No profile set. Call set_current_profile() before get_current_profile()."
        )
    return _current_profile


def get_settings_for_profile(profile: str) -> MoneyBinSettings:
    """Get settings for a specific user profile.

    This is a convenience function that switches to the specified profile
    and returns its settings. The profile change persists after this call.

    Args:
        profile: User profile name (e.g., 'alice', 'bob', 'household')
                 Will be normalized to lowercase with hyphens

    Returns:
        MoneyBinSettings: The configuration instance for the specified profile

    Raises:
        ValueError: If required configuration is missing or invalid

    Note:
        This function changes the current profile via set_current_profile().
        If you need the current profile to remain unchanged, consider saving
        it first and restoring it after.
    """
    if not profile:
        raise ValueError("Profile name cannot be empty")

    # Switch to the requested profile
    try:
        set_current_profile(profile)
        return get_settings()
    except ValueError as e:
        raise ValueError(f"Invalid profile name: {e}") from e


def reload_settings(profile: str | None = None) -> MoneyBinSettings:
    """Reload settings from environment variables.

    This function forces a reload of the configuration, useful for testing
    or when environment variables change at runtime.

    Args:
        profile: Profile to reload. If None, reloads current profile.
                 If specified and different from current, switches to that profile.

    Returns:
        MoneyBinSettings: The reloaded configuration instance
    """
    global _current_settings, _current_profile

    # If profile specified, switch to it (which invalidates cache)
    if profile is not None and (
        _current_profile is None or profile != _current_profile
    ):
        set_current_profile(profile)
    else:
        # Just invalidate current cache to force reload
        _current_settings = None

    return get_settings()


def clear_settings_cache() -> None:
    """Clear cached settings and reset profile to None.

    This function is primarily for testing to ensure clean state between tests.
    It clears the cached settings and resets the current profile.
    """
    global _current_settings, _current_profile

    _current_settings = None
    _current_profile = None


# Convenience functions for common configuration access
def get_database_path() -> Path:
    """Get the configured database path for the current profile.

    Returns:
        Path: The database path
    """
    return get_settings().database.path


def get_raw_data_path() -> Path:
    """Get the configured raw data path for the current profile.

    Returns:
        Path: The raw data path
    """
    return get_settings().data.raw_data_path
