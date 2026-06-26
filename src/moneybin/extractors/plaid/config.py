"""Plaid provider configuration.

Merged into MoneyBinSettings.providers.plaid at framework startup.

Not yet read by the extractor: ``PlaidExtractor`` instances are constructed
directly without consulting the singleton under ``settings.providers.plaid``.
Wiring lands in Plan 2 of the extension-contracts implementation.
"""

from moneybin.extractors._types import ProviderConfig


class PlaidProviderConfig(ProviderConfig):
    """Configuration for the Plaid provider.

    Plaid's auth lives in the SyncResponse payload (delivered by
    moneybin-sync's mediated Hosted Link flow); this config holds
    only client-side knobs.

    Currently empty — placeholder for future client-side options. The
    Protocol base requires every provider to declare a config class
    so the framework can merge it into MoneyBinSettings.providers.<name>.
    """
