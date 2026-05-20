"""Shared types for the Provider Protocol.

ProviderSource is a typed union of input shapes — file path (most providers),
sync response (Plaid-style mediated providers), OAuth session (future direct-
connect providers). Each provider declares which shape(s) it accepts via its
extract() implementation.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel


@dataclass(frozen=True, slots=True)
class FilePath:
    """A file on disk the provider reads (OFX, CSV, Parquet, etc.)."""

    path: Path


@dataclass(frozen=True, slots=True)
class SyncResponse:
    """An already-fetched response payload from a mediated sync provider.

    Plaid Hosted Link delivers a SyncDataResponse via moneybin-server; the
    response payload is passed in directly rather than re-fetched.
    """

    payload: Any  # provider-specific: SyncDataResponse, dict, etc.
    job_id: str | None = None


@dataclass(frozen=True, slots=True)
class OAuthSession:
    """An authenticated OAuth session for direct-connect providers (future).

    Reserved for `connect-*` providers per docs/specs/connect-gsheet.md;
    no current provider uses this shape, but the type is declared so the
    Protocol's input union is complete.
    """

    access_token: str
    refresh_token: str | None = None
    expires_at: int | None = None  # epoch seconds


type ProviderSource = FilePath | SyncResponse | OAuthSession
"""Typed union of accepted provider input shapes."""

type ExtractionResult = dict[str, pl.DataFrame]
"""Output of Provider.extract(): {raw_table_name: DataFrame}.

The framework writes each DataFrame to its declared raw table via
Database.ingest_dataframe(), stamping every row with import_id,
source_type, source_origin, extracted_at, loaded_at.
"""


class ProviderConfig(BaseModel):
    """Base class for per-provider Pydantic config models.

    Each provider declares a subclass exposing its specific fields. The
    framework merges these into MoneyBinSettings.providers.<name>.
    """

    model_config = {"extra": "forbid", "frozen": True}
