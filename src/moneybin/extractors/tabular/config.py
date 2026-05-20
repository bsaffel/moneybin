"""Tabular provider configuration.

Merged into MoneyBinSettings.providers.tabular at framework startup
(wired in Task 5 of the provider-framework refactor).
"""

from moneybin.extractors._types import ProviderConfig


class TabularProviderConfig(ProviderConfig):
    """Configuration for the tabular provider (CSV/TSV/Excel/Parquet/Feather).

    Currently empty — the tabular pipeline's existing tunables
    (``text_size_limit_mb``, ``binary_size_limit_mb``, row thresholds,
    ``account_match_threshold``, balance-validation flags) still live on
    ``MoneyBinSettings.data.tabular`` and are read via
    ``get_settings().data.tabular``. Task 5 of the provider-framework
    refactor will migrate those fields here and wire
    ``MoneyBinSettings.providers.tabular`` end-to-end. For now this class
    satisfies the Protocol's requirement that every provider declare a
    config class.
    """
