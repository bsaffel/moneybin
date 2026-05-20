"""OFX provider configuration.

Merged into MoneyBinSettings.providers.ofx at framework startup.
"""

from pathlib import Path

from pydantic import ConfigDict

from moneybin.extractors._types import ProviderConfig


class OFXProviderConfig(ProviderConfig):
    """Configuration for the OFX provider.

    The extractor resolves a default for ``raw_data_path`` at construction
    time when None — that mutation is the reason this subclass overrides
    the base ``frozen=True``. Treat fields as effectively immutable except
    for that one initialization step.
    """

    # Override the base ProviderConfig's frozen=True: OFXExtractor.__init__
    # sets raw_data_path from get_raw_data_path() when None is passed.
    model_config = ConfigDict(extra="forbid", frozen=False)

    raw_data_path: Path | None = None
    """Where raw OFX files are staged. Resolved to ``<profile>/ofx`` when None."""

    preserve_source_files: bool = True
    """If True, keep the original source files after extraction."""

    validate_balances: bool = True
    """If True, validate balance fields during extraction."""
