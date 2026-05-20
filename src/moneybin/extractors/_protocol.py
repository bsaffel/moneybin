"""The Provider Protocol — the durable interface every in-tree provider implements.

Providers are in-tree only. Third-party Provider packages are not supported
(see docs/specs/extension-contracts.md for the trust posture rationale).

This Protocol unifies the prior split between file-based extractors and
sync-response-based loaders. Every provider implements extract() and
schema_files() regardless of input shape; the input shape is expressed
via the ProviderSource union.
"""

from pathlib import Path
from typing import Protocol, runtime_checkable

from moneybin.extractors._types import (
    ExtractionResult,
    ProviderConfig,
    ProviderSource,
)


@runtime_checkable
class Provider(Protocol):
    """A data source that ingests external data into raw.<source>_* tables."""

    name: str
    """snake_case source identifier; matches raw.<name>_* prefix."""

    source_type: str
    """Written into source_type column on every row produced by this provider."""

    config: ProviderConfig
    """Pydantic config model declared by the provider module."""

    def extract(self, source: ProviderSource) -> ExtractionResult:
        """Extract data into per-table DataFrames keyed by raw table name."""
        ...

    def schema_files(self) -> list[Path]:
        """Return paths to SQL DDL files defining raw.<name>_* tables.

        Replaces src/moneybin/schema.py's hardcoded list. The framework
        enumerates schema_files() across all registered providers at init.
        Paths are absolute (typically resolved via importlib.resources or
        Path(__file__).parent / "schema" / "*.sql").
        """
        ...
