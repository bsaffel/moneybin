"""Shared types for the Provider Protocol.

ProviderSource is a typed union of input shapes — file path (most providers),
sync response (Plaid-style mediated providers), OAuth session (future direct-
connect providers). Each provider declares which shape(s) it accepts via its
extract() implementation.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    import polars as pl


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

    # TODO(connect-providers): tokens must route through SecretStore before
    # the first `connect-*` provider lands — plain `str` violates the
    # project security rule on credential storage (CLAUDE.md / .claude/
    # rules/security.md). Either wrap as `SecretStr` (Pydantic) or have
    # OAuthSession reference a SecretStore key instead of holding the
    # token value. Decision deferred to the first connect provider PR.
    access_token: str
    refresh_token: str | None = None
    expires_at: int | None = None  # epoch seconds


ProviderSource: TypeAlias = FilePath | SyncResponse | OAuthSession  # noqa: UP040  # TypeAlias form for coherence (project has no PEP 695 usage)
"""Typed union of accepted provider input shapes."""

ExtractionResult: TypeAlias = "dict[str, pl.DataFrame]"  # noqa: UP040  # TypeAlias form for coherence (project has no PEP 695 usage); string form keeps polars out of cold-start import graph
"""Output of Provider.extract(): {raw_table_name: DataFrame}.

The framework writes each DataFrame to its declared raw table via
Database.ingest_dataframe(), stamping every row with import_id,
source_type, source_origin, extracted_at, loaded_at.

Quoted so polars is not pulled in at module-eval time — the cold-start
path (CLI main → config → provider configs → _types) must stay polars-free.
"""


class ProviderConfig(BaseModel):
    """Base class for per-provider Pydantic config models.

    Each provider declares a subclass exposing its specific fields. The
    framework merges these into MoneyBinSettings.providers.<name>.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
