"""OFX provider configuration.

Merged into MoneyBinSettings.providers.ofx at framework startup.

Not yet read by the extractor: ``ImportService._import_ofx()`` constructs
``OFXExtractor()`` with no argument today, so the singleton under
``settings.providers.ofx`` is ignored. Wiring lands in Plan 2 of the
extension-contracts implementation.
"""

from pathlib import Path

from moneybin.extractors._types import ProviderConfig


class OFXProviderConfig(ProviderConfig):
    """Configuration for the OFX provider.

    Inherits ``frozen=True`` from ``ProviderConfig`` — every field is
    immutable. The extractor resolves a default for ``raw_data_path`` as
    a local instance attribute rather than mutating the config; see
    ``OFXExtractor.__init__``.
    """

    raw_data_path: Path | None = None
    """Where raw OFX files are staged. Resolved to ``<profile>/ofx`` by
    ``OFXExtractor`` when None."""

    preserve_source_files: bool = True
    """If True, keep the original source files after extraction."""

    validate_balances: bool = True
    """If True, validate balance fields during extraction."""
