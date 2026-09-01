"""GSheetAdapter Protocol and shared dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol

import polars as pl

from moneybin.connectors.gsheet.drift import DriftReport
from moneybin.database import Database

Confidence = Literal["high", "medium", "low"]

# Every row this connector writes carries it, and account identity is the
# (source_type, source_origin) pair — so the value has to be the same string
# where rows are written and where they are read back or deleted.
GSHEET_SOURCE_TYPE = "gsheet"


@dataclass(frozen=True)
class DetectionResult:
    """Result of sheet structure detection."""

    confidence: Confidence
    column_mapping: dict[str, str]  # source_header → dest_field
    header_signature: list[str]  # ordered source headers (post skip_rows)
    date_format: str | None = None
    sign_convention: str | None = None
    number_format: str | None = None
    skip_rows: int = 0
    skip_trailing_patterns: list[str] = field(default_factory=list)
    # Seed-adapter-only:
    typed_columns: dict[str, str] = field(default_factory=dict)  # header → SQL type
    notes: list[str] = field(default_factory=list)
    # Normalized confidence score in [0, 1]; 0.0 for adapters that don't compute one.
    score: float = 0.0


@dataclass(frozen=True)
class LoadResult:
    """Result of loading data into the database."""

    rows_inserted: int
    rows_soft_deleted: int
    rows_upserted: int
    rows_rejected: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GSheetConnection:
    """Represents a connection to a Google Sheet for data import."""

    connection_id: str
    spreadsheet_id: str
    sheet_gid: int
    sheet_name: str
    workbook_name: str
    adapter: str
    alias: str | None
    account_id: str | None
    account_name: str | None
    column_mapping: dict[str, str]
    header_signature: list[str]
    date_format: str | None
    sign_convention: str | None
    number_format: str | None
    skip_rows: int
    skip_trailing_patterns: list[str]
    status: str
    last_pull_at: str | None
    last_pull_import_id: str | None
    last_success_at: str | None
    last_status_reason: str | None
    consecutive_failure_count: int

    def to_dict(self) -> dict[str, object]:
        """Serialize for JSON/envelope output. Mapping + signature fields omitted."""
        return {
            "connection_id": self.connection_id,
            "spreadsheet_id": self.spreadsheet_id,
            "sheet_gid": self.sheet_gid,
            "sheet_name": self.sheet_name,
            "workbook_name": self.workbook_name,
            "adapter": self.adapter,
            "alias": self.alias,
            "account_id": self.account_id,
            "account_name": self.account_name,
            "status": self.status,
            "last_pull_at": self.last_pull_at,
            "last_success_at": self.last_success_at,
            "last_status_reason": self.last_status_reason,
            "consecutive_failure_count": self.consecutive_failure_count,
        }


class GSheetAdapter(Protocol):
    """Protocol for adapters that handle specific sheet types (transactions, seeds)."""

    name: str  # "transactions" | "seed"
    # Whether a column absent from this adapter's mapping still reaches the
    # target. Decides both what a duplicate-header rename costs the user and
    # whether a newly duplicated header has to stop a pull.
    imports_every_column: bool

    def detect(
        self,
        df: pl.DataFrame,
        *,
        account_name: str | None,
    ) -> DetectionResult:
        """Run column detection for this adapter's target shape."""
        ...

    def check_drift(
        self,
        connection: GSheetConnection,
        current_df: pl.DataFrame,
    ) -> DriftReport:
        """Compare current df against connection's pinned signature."""
        ...

    def transform(
        self,
        df: pl.DataFrame,
        connection: GSheetConnection,
        db: Database,
    ) -> pl.DataFrame:
        """Apply pinned mapping + typed transforms; produce load-ready frame.

        ``db`` carries the identities this connection has already registered.
        The transactions adapter keys an unbound sheet's rows by account, and
        ``transaction_id`` folds that key, so a key recomputed from the current
        label would rotate every id the account owns whenever the label changes.
        """
        ...

    def load(
        self,
        df: pl.DataFrame,
        connection: GSheetConnection,
        db: Database,
        import_id: str,
        source_df: pl.DataFrame | None = None,
    ) -> LoadResult:
        """Diff + soft-delete + upsert. Returns counts.

        ``source_df`` is the pre-transform frame. The transactions adapter
        needs it to register the accounts an unbound multi-account sheet
        names — the transform keeps only their slugified keys, and an account
        is named for a human by the label the sheet actually wrote.
        """
        ...
