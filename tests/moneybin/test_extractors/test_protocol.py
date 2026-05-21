"""Tests for the Provider Protocol contract.

The Protocol is the durable interface every in-tree provider must satisfy.
A minimal stub provider proves the Protocol is implementable; the
real-provider conformance suite below pins each in-tree extractor to the
same shape — pyright's structural check is helpful but doesn't catch a
contributor who removes an attribute and assumes ``isinstance`` will
notice (``@runtime_checkable`` only verifies methods).
"""

from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import polars as pl
import pytest

from moneybin.extractors._protocol import Provider
from moneybin.extractors._types import (
    ExtractionResult,
    FilePath,
    ProviderConfig,
    ProviderSource,
    SyncResponse,
)
from moneybin.extractors.ofx import OFXExtractor
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.extractors.tabular import TabularExtractor


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
    """A minimal stub conforms to the Protocol's runtime + attribute shape.

    @runtime_checkable Protocols verify method presence but NOT attribute
    presence — so attribute conformance is asserted explicitly here.
    """
    stub = _StubProvider()
    assert isinstance(stub, Provider)
    assert isinstance(stub.name, str)
    assert isinstance(stub.source_type, str)
    assert isinstance(stub.config, ProviderConfig)


def test_file_path_is_a_provider_source() -> None:
    """FilePath is one of the accepted ProviderSource shapes."""
    source: ProviderSource = FilePath(Path("sample.ofx"))
    assert source.path.name == "sample.ofx"


def test_provider_config_is_pydantic() -> None:
    """ProviderConfig base supports Pydantic validation."""
    config = _StubProviderConfig(encoding="latin-1")
    assert config.encoding == "latin-1"


def _build_extractor(cls: type) -> Provider:
    """Construct an in-tree extractor with a mock Database when required.

    Only ``OFXExtractor`` takes no ``db`` argument; Plaid and tabular
    accept one. The mock is never exercised — these tests only inspect
    Protocol shape and the ``extract()`` stub, both of which short-circuit
    before any DB call.
    """
    if cls is OFXExtractor:
        return cast(Provider, cls())  # type: ignore[call-arg]
    return cast(Provider, cls(MagicMock()))


@pytest.mark.parametrize("cls", [OFXExtractor, PlaidExtractor, TabularExtractor])
def test_in_tree_extractor_conforms_to_protocol(cls: type) -> None:
    """Every in-tree extractor satisfies the Provider Protocol shape.

    ``@runtime_checkable`` only verifies methods; the attribute assertions
    guard against silent regressions when a contributor removes a class
    attribute and assumes ``isinstance`` will fail.
    """
    extractor = _build_extractor(cls)
    assert isinstance(extractor, Provider)
    assert isinstance(extractor.name, str) and extractor.name  # noqa: PT018
    assert isinstance(extractor.source_type, str) and extractor.source_type  # noqa: PT018
    assert isinstance(extractor.config, ProviderConfig)


@pytest.mark.parametrize(
    "cls,source",
    [
        (OFXExtractor, FilePath(Path("sample.ofx"))),
        (PlaidExtractor, SyncResponse(payload=None, job_id=None)),
        (TabularExtractor, FilePath(Path("sample.csv"))),
    ],
)
def test_extract_stub_raises_not_implemented(cls: type, source: ProviderSource) -> None:
    """``extract()`` raises ``NotImplementedError`` until Plan 2 wires the framework.

    The Protocol's ``extract(source)`` signature has no carrier for
    ``import_id`` / ``source_origin``; until Plan 2's framework decoration
    supplies them, callers MUST use the per-provider live entry points
    (e.g. ``OFXExtractor.extract_from_file``, ``PlaidExtractor.load``,
    ``TabularExtractor.load_transactions``). This test makes premature
    ``provider.extract(source)`` calls a CI failure rather than a
    runtime surprise.
    """
    extractor = _build_extractor(cls)
    with pytest.raises(NotImplementedError, match="Plan 2|Task 5|framework"):
        extractor.extract(source)


def test_in_tree_extractor_schema_files_resolve() -> None:
    """Each in-tree extractor's schema_files() returns existing paths.

    Decentralized schema discovery (Task 6) shifts the source-of-truth
    for DDL discovery onto each provider; this guards against a provider
    package being reorganized without updating its ``schema/`` dir.
    """
    for cls in (OFXExtractor, PlaidExtractor, TabularExtractor):
        extractor = _build_extractor(cls)
        files = extractor.schema_files()
        assert files, f"{cls.__name__}.schema_files() returned empty"
        for path in files:
            assert path.exists(), f"{cls.__name__}: missing DDL {path}"
            assert path.name.startswith("raw_"), (
                f"{cls.__name__}: unexpected non-raw_ file {path.name}"
            )
