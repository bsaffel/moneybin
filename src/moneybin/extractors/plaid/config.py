"""Plaid provider configuration.

Merged into MoneyBinSettings.providers.plaid at framework startup.
"""

from moneybin.extractors._types import ProviderConfig


class PlaidProviderConfig(ProviderConfig):
    """Configuration for the Plaid provider.

    Plaid's auth lives in the SyncResponse payload (delivered by
    moneybin-server's mediated Hosted Link flow); this config holds
    only client-side knobs.

    Currently empty — placeholder for future client-side options. The
    Protocol base requires every provider to declare a config class
    so the framework can merge it into MoneyBinSettings.providers.<name>.
    """
