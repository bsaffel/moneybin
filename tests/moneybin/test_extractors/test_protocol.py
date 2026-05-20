"""Tests for the Provider Protocol contract.

The Protocol is the durable interface every in-tree provider must satisfy.
A minimal stub provider proves the Protocol is implementable; existing
providers conform to it (verified in their per-provider migration tasks).
"""

from pathlib import Path

import polars as pl

from moneybin.extractors._protocol import Provider
from moneybin.extractors._types import (
    ExtractionResult,
    FilePath,
    ProviderConfig,
    ProviderSource,
)


class _StubProviderConfig(ProviderConfig):
    """Minimal config for the stub provider."""

    encoding: str = "utf-8"


class _StubProvider:
    """Smallest implementation that satisfies the Protocol."""

    name = "stub"
    source_type = "stub"
    config = _StubProviderConfig()

    def extract(self, source: ProviderSource) -> ExtractionResult:
        # Stub returns one empty DataFrame keyed by its declared table name.
        return {"raw_stub_transactions": pl.DataFrame({"id": []})}

    def schema_files(self) -> list[Path]:
        return []


def test_stub_satisfies_protocol() -> None:
    """A minimal stub conforms to the Protocol at runtime."""
    stub = _StubProvider()
    assert isinstance(stub, Provider)


def test_file_path_is_a_provider_source() -> None:
    """FilePath is one of the accepted ProviderSource shapes."""
    source: ProviderSource = FilePath(Path("sample.ofx"))
    assert source.path.name == "sample.ofx"


def test_provider_config_is_pydantic() -> None:
    """ProviderConfig base supports Pydantic validation."""
    config = _StubProviderConfig(encoding="latin-1")
    assert config.encoding == "latin-1"
