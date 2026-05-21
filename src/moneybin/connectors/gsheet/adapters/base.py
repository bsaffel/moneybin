"""GSheetAdapter Protocol and shared dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol

import polars as pl

from moneybin.connectors.gsheet.drift import DriftReport
from moneybin.database import Database

Confidence = Literal["high", "medium", "low"]


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
    last_drift_reason: str | None
    consecutive_failure_count: int


class GSheetAdapter(Protocol):
    """Protocol for adapters that handle specific sheet types (transactions, seeds)."""

    name: str  # "transactions" | "seed"

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
    ) -> pl.DataFrame:
        """Apply pinned mapping + typed transforms; produce load-ready frame."""
        ...

    def load(
        self,
        df: pl.DataFrame,
        connection: GSheetConnection,
        db: Database,
        import_id: str,
    ) -> LoadResult:
        """Diff + soft-delete + upsert. Returns counts."""
        ...
