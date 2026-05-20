"""Tabular provider configuration.

Merged into MoneyBinSettings.providers.tabular at framework startup.
"""

from pydantic import Field

from moneybin.extractors._types import ProviderConfig


class TabularProviderConfig(ProviderConfig):
    """Configuration for the tabular provider (CSV/TSV/Excel/Parquet/Feather).

    Tunables migrated from the historical ``MoneyBinSettings.data.tabular``
    (Task 5 of the provider-framework refactor). Read via
    ``get_settings().providers.tabular``. Env-var override follows the
    standard nested-delimiter shape, e.g.
    ``MONEYBIN_PROVIDERS__TABULAR__TEXT_SIZE_LIMIT_MB=20``.
    """

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
