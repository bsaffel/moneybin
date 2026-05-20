"""OFX provider — ingests OFX/QFX/QBO files into raw.ofx_* tables."""

from moneybin.extractors.ofx.config import OFXProviderConfig
from moneybin.extractors.ofx.extractor import OFXExtractor

__all__ = ["OFXExtractor", "OFXProviderConfig"]
