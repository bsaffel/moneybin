"""OFX provider configuration.

Merged into MoneyBinSettings.providers.ofx at framework startup.

Not yet read by the extractor: ``ImportService._import_ofx()`` constructs
``OFXExtractor()`` with no argument today, so the singleton under
``settings.providers.ofx`` is ignored. Wiring lands in Plan 2 of the
extension-contracts implementation.
"""

from pathlib import Path

from pydantic import Field

from moneybin.extractors._types import ProviderConfig


class OFXProviderConfig(ProviderConfig):
    """Configuration for the OFX provider.

    Inherits ``frozen=True`` from ``ProviderConfig`` — every field is
    immutable. The extractor resolves a default for ``raw_data_path`` as
    a local instance attribute rather than mutating the config; see
    ``OFXExtractor.__init__``.
    """

    raw_data_path: Path | None = Field(
        default=None,
        description="Where raw OFX files are staged; the extractor resolves "
        "`<profile dir>/ofx` when unset. Read from a config passed to "
        "`OFXExtractor`; this setting is not wired to it yet.",
    )
    preserve_source_files: bool = Field(
        default=True,
        description="Keep the original source files after extraction. Not read "
        "by the extractor yet.",
    )
    validate_balances: bool = Field(
        default=True,
        description="Validate balance fields during extraction. Not read by the "
        "extractor yet.",
    )
