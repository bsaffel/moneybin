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

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    pyproject = path / "pyproject.toml"
    if not pyproject.exists():
        return False
    try:
        content = pyproject.read_text()
        return '\nname = "moneybin"' in content
    except OSError:
        return False


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


class TabularConfig(BaseModel):
    """Tabular import pipeline limits and thresholds."""

    model_config = ConfigDict(frozen=True)

    text_size_limit_mb: int = Field(
        default=25,
        description="Maximum file size (MB) for text formats (CSV/TSV)",
    )
    binary_size_limit_mb: int = Field(
        default=100,
        description="Maximum file size (MB) for binary formats (Excel/Parquet/Feather)",
    )
    row_warn_threshold: int = Field(
        default=10_000,
        description="Row count above which a warning is logged",
    )
    row_refuse_threshold: int = Field(
        default=50_000,
        description="Row count above which import is refused (use --no-row-limit to override)",
    )
    balance_pass_threshold: float = Field(
        default=0.90,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum fraction of balance deltas that must match "
            "for balance validation to pass"
        ),
    )
    balance_tolerance_cents: int = Field(
        default=1,
        ge=0,
        description="Per-delta tolerance in cents for balance validation",
    )
    account_match_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description=(
            "Fuzzy-match similarity threshold (difflib.SequenceMatcher.ratio) "
            "for account-name matching. Below this threshold, candidates are "
            "treated as 'no match'."
        ),
    )


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
    tabular: TabularConfig = Field(default_factory=TabularConfig)


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

    flush_interval_seconds: int = Field(
        default=300,
        ge=10,
        description="Periodic flush interval for MCP stream (seconds)",
    )


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

    enabled: bool = Field(
        default=False,
        description="Enable MoneyBin Sync service (paid tier)",
    )
    server_url: str | None = Field(
        default=None,
        description="MoneyBin Sync server URL (e.g., https://sync.moneybin.app)",
    )
    api_key: str | None = Field(
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
    oauth_client_secret: str | None = Field(
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
            "plaid",
            "csv",
            "excel",
            "tsv",
            "parquet",
            "feather",
            "pipe",
            "ofx",
        ],
        description="Source types in priority order (first = highest priority)",
    )
    transfer_review_threshold: float = Field(
        default=0.70,
        ge=0.0,
        le=1.0,
        description="Review queue threshold for transfer pairs",
    )
    transfer_signal_weights: dict[str, float] = Field(
        default={
            "date_distance": 0.4,
            "keyword": 0.3,
            "roundness": 0.15,
            "pair_frequency": 0.15,
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
        required = {"date_distance", "keyword", "roundness", "pair_frequency"}
        missing = required - v.keys()
        if missing:
            raise ValueError(f"transfer_signal_weights missing keys: {missing}")
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

    @model_validator(mode="after")
    def proposal_threshold_lte_override_threshold(self) -> "CategorizationSettings":
        """Ensure proposal_threshold <= override_threshold.

        If proposal > override, ``check_overrides`` deactivates a rule once
        override count reaches override_threshold but the re-proposal lands
        in ``tracking`` (count < proposal_threshold), hiding the corrected
        category from ``categorize auto review`` until further user categorizations.
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
    sync: SyncConfig = Field(default_factory=SyncConfig)
    matching: MatchingSettings = Field(default_factory=MatchingSettings)
    categorization: CategorizationSettings = Field(
        default_factory=CategorizationSettings
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
        env_file=".env",  # Default, but overridden by settings_customise_sources
        env_file_encoding="utf-8",
        env_prefix="MONEYBIN_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # Allow extra env vars that don't match our schema
        frozen=True,
    )

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate application environment."""
        if v == "production" and os.getenv("DEBUG", "").lower() in ("true", "1"):
            raise ValueError("DEBUG mode cannot be enabled in production")
        return v

    def validate_required_credentials(self) -> None:
        """Validate that required credentials are present.

        Note: Plaid credentials are now validated on the server side.
        This method is kept for potential future client-side validation needs.
        """
        # No client-side credentials to validate currently
        pass


# Global settings - single instance for current profile
_current_settings: MoneyBinSettings | None = None


_current_profile: str | None = None


# Lazy profile resolver. Registered by the CLI entry point so that profile
# resolution (env → config.yaml → first-run wizard) happens only when a
# command actually needs settings — not eagerly in the parent callback.
# Without this, ``moneybin logs`` (a docker-style usage error) would fire
# the first-run wizard before Click ever surfaces the missing-arg error.
_profile_resolver: Callable[[], None] | None = None
_resolver_in_progress: bool = False


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
    """Invoke the registered resolver if no profile is set.

    Guarded against re-entry so a resolver that itself reads settings
    before calling ``set_current_profile()`` fails fast with the normal
    "no profile set" error rather than recursing.
    """
    global _resolver_in_progress
    if (
        _current_profile is None
        and _profile_resolver is not None
        and not _resolver_in_progress
    ):
        _resolver_in_progress = True
        try:
            _profile_resolver()
        finally:
            _resolver_in_progress = False


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
        settings.validate_required_credentials()

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
            _current_settings = None  # Invalidate cache
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


def get_sync_config() -> SyncConfig:
    """Get the sync service configuration for the current profile.

    Returns:
        SyncConfig: The sync service configuration
    """
    return get_settings().sync
