"""GSheetConnectionService — connect, list, get, disconnect, reconnect.

Orchestrates the connect flow: parse URL, fetch workbook metadata, run the
chosen adapter's detection, persist via ``GSheetConnectionsRepo``, then
optionally fire the initial pull (delegated to ``GSheetPullService`` via
late import to avoid the circular dependency).

Disconnect has two modes: soft (status='disconnected', raw rows retained
for analytics) and purge (drop seed view, wipe raw rows, hard-delete the
connection row).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import polars as pl
from sqlglot import exp

from moneybin.connectors.gsheet.adapters import ADAPTERS
from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetConnection,
    LoadResult,
)
from moneybin.connectors.gsheet.adapters.transactions import REQUIRED_DEST_FIELDS
from moneybin.connectors.gsheet.errors import GSheetError, GSheetUnreachableError
from moneybin.connectors.gsheet.sheets_api import SheetsAPI
from moneybin.connectors.gsheet.url_parser import parse_sheet_url
from moneybin.database import Database
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo
from moneybin.tables import GSHEET_SEEDS, TABULAR_TRANSACTIONS

logger = logging.getLogger(__name__)

# Mirrors ``view_generator._SAFE_ALIAS_RE`` so disconnect(purge=True) can
# re-validate the alias before string-interpolating it into DROP VIEW.
# Defense-in-depth: insert path already validates via the view_generator on
# load, but DROP VIEW happens outside the load path so we re-check here.
_SAFE_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class LowConfidenceError(GSheetError):
    """Transactions adapter returned low confidence and no override was given."""


class AmbiguousDetectionError(GSheetError):
    """Detection returned medium confidence and user has not accepted with --yes."""


@dataclass
class ConnectionRequest:
    """Inputs for ``GSheetConnectionService.connect``."""

    url: str
    adapter: str | None = None
    alias: str | None = None
    account_name: str | None = None
    account_id: str | None = None
    column_mapping: dict[str, str] | None = None
    yes: bool = False
    accept_seed_fallback: bool = False
    no_initial_pull: bool = False


@dataclass
class ConnectResult:
    """Outputs of ``GSheetConnectionService.connect``.

    ``initial_pull_status`` and ``initial_pull_error`` surface auto-pull
    failures that previously got swallowed when callers only retained
    ``.load_result``. The connection is persisted regardless — callers
    can inspect the pull state and decide whether to retry.
    """

    connection: GSheetConnection
    detection: DetectionResult
    initial_pull: LoadResult | None
    initial_pull_status: str | None = None
    initial_pull_error: str | None = None


class GSheetConnectionService:
    """Lifecycle owner for ``app.gsheet_connections`` rows."""

    def __init__(
        self,
        *,
        db: Database,
        sheets_client: SheetsAPI,
        oauth_client: Any,
    ) -> None:
        """Bind to a database, a SheetsAPI implementation, and an OAuth client."""
        self._db = db
        self._sheets = sheets_client
        self._oauth = oauth_client
        self._repo = GSheetConnectionsRepo(db)

    def connect(self, req: ConnectionRequest) -> ConnectResult:
        """Detect, persist, and optionally pull the initial snapshot."""
        if not self._oauth.is_authorized():
            self._oauth.authorize()

        try:
            spreadsheet_id, gid = parse_sheet_url(req.url)
        except ValueError as exc:
            raise GSheetError(f"Invalid Google Sheets URL: {exc}") from exc
        meta = self._sheets.get_workbook_metadata(spreadsheet_id)
        sheet = next((s for s in meta.sheets if s.gid == gid), None)
        if sheet is None:
            # Use the workbook title, not spreadsheet_id: the raw id uniquely
            # identifies a private document and aids phishing if it leaks to
            # the user-facing error / MCP envelope.
            raise GSheetUnreachableError(
                f"gid={gid} not found in workbook {meta.title!r}"
            )

        rows = self._sheets.read_sheet_values(spreadsheet_id, sheet.name)
        if not rows:
            raise GSheetError("Sheet has no data")

        df = rows_to_df(rows)

        # Adapter selection: explicit override wins; None tries transactions first.
        target_adapter = req.adapter or "transactions"
        if target_adapter not in ADAPTERS:
            raise GSheetError(
                f"Unknown adapter: {target_adapter!r}. "
                f"Valid options: {sorted(ADAPTERS)}"
            )
        adapter = ADAPTERS[target_adapter]
        detection = adapter.detect(df, account_name=req.account_name)

        # Fall-through: auto-detect → low-confidence transactions → maybe seed.
        if (
            target_adapter == "transactions"
            and detection.confidence == "low"
            and req.column_mapping is None
        ):
            if req.adapter is None and req.accept_seed_fallback:
                target_adapter = "seed"
                adapter = ADAPTERS["seed"]
                detection = adapter.detect(df, account_name=None)
            else:
                raise LowConfidenceError(
                    "Low-confidence transactions detection. "
                    "Provide --column-mapping or retry with "
                    "--adapter=seed --alias=<name>."
                )

        # Medium confidence: ambiguous column matches. Require explicit
        # acceptance (--yes) or an override (--column-mapping) before
        # persisting — otherwise wrong mappings can land silently and
        # corrupt the initial pull.
        if (
            target_adapter == "transactions"
            and detection.confidence == "medium"
            and req.column_mapping is None
            and not req.yes
        ):
            raise AmbiguousDetectionError(
                "Medium-confidence transactions detection. "
                "Re-run with --yes to accept the inferred mapping, "
                "or pass --column-mapping to override."
            )

        if target_adapter == "seed" and not req.alias:
            raise GSheetError(
                "--alias=<slug> is required when --adapter=seed. "
                "Pick a short identifier; it becomes the view name "
                "raw.gsheet_<alias>."
            )

        # TransactionsAdapter.transform requires account_id (see transactions.py).
        # Persisting without one creates a row that fails every pull. Accept
        # account_name as a free-text alias and resolve to the canonical id
        # at the service boundary (identifiers.md Guard 2 — bind filters to
        # the id; resolve free-text at the boundary).
        resolved_account_id: str | None = req.account_id
        if target_adapter == "transactions" and not resolved_account_id:
            if req.account_name:
                from moneybin.services.account_service import (  # noqa: PLC0415
                    AccountService,
                )

                # resolve_strict accepts an account_id or a display_name and
                # raises AccountNotFoundError / AmbiguousAccountError (both
                # UserError subclasses, surface cleanly via the MCP/CLI
                # boundary handlers).
                resolved_account_id = AccountService(self._db).resolve_strict(
                    req.account_name
                )
            else:
                raise GSheetError(
                    "--account-id or --account-name is required for the "
                    "transactions adapter. Pass --account-name=<display> "
                    "(resolved via dim_accounts) or "
                    "--account-id=<dim_accounts.account_id>."
                )

        # For seed adapter the column_mapping field holds inferred typed_columns
        # (the raw_seed adapter reuses the field for its typed view).
        # For transactions, a user-supplied override (req.column_mapping) wins
        # over detection — that's the whole point of letting the user pass it.
        if target_adapter == "seed":
            column_mapping = detection.typed_columns
        elif req.column_mapping is not None:
            # User override: schema-validate before persisting. Without this,
            # a missing or typo'd required dest (e.g. `amount`) creates a
            # "healthy" connection whose pulls fail with zero rows loaded —
            # silent ingestion failure indistinguishable from an empty sheet.
            _validate_transactions_column_mapping(
                user_mapping=req.column_mapping,
                sheet_headers=list(df.columns),
            )
            column_mapping = req.column_mapping
        else:
            column_mapping = detection.column_mapping

        connection_id = self._repo.insert(
            spreadsheet_id=spreadsheet_id,
            sheet_gid=gid,
            sheet_name=sheet.name,
            workbook_name=meta.title,
            adapter=target_adapter,
            alias=req.alias,
            account_id=resolved_account_id,
            account_name=req.account_name,
            column_mapping=column_mapping,
            header_signature=detection.header_signature,
            date_format=detection.date_format,
            sign_convention=detection.sign_convention,
            number_format=detection.number_format,
            skip_rows=detection.skip_rows,
            skip_trailing_patterns=detection.skip_trailing_patterns or None,
        )
        stored = self._repo.get(connection_id)
        if stored is None:
            raise RuntimeError(
                f"insert succeeded but get returned None: {connection_id}"
            )
        connection = row_to_connection(stored)

        initial_pull: LoadResult | None = None
        initial_pull_status: str | None = None
        initial_pull_error: str | None = None
        if not req.no_initial_pull:
            # Late import — Task 21 (pull_service) imports helpers from here.
            from moneybin.connectors.gsheet.pull_service import GSheetPullService

            pull_svc = GSheetPullService(
                db=self._db,
                sheets_client=self._sheets,
                oauth_client=self._oauth,
            )
            pull = pull_svc.pull_connection(connection_id)
            initial_pull = pull.load_result
            initial_pull_status = pull.status
            initial_pull_error = pull.error_message
            # Refresh the connection state after the pull updated counters.
            stored = self._repo.get(connection_id)
            if stored is None:
                raise RuntimeError(f"connection vanished mid-pull: {connection_id}")
            connection = row_to_connection(stored)

        logger.info(
            f"gsheet connect: connection_id={connection_id} "
            f"adapter={target_adapter} initial_pull_status={initial_pull_status}"
        )
        return ConnectResult(
            connection=connection,
            detection=detection,
            initial_pull=initial_pull,
            initial_pull_status=initial_pull_status,
            initial_pull_error=initial_pull_error,
        )

    def list_connections(self) -> list[GSheetConnection]:
        """Return every connection, audited reads."""
        return [row_to_connection(r) for r in self._repo.list_all()]

    def get(self, connection_id: str) -> GSheetConnection | None:
        """Return one connection by id, or None."""
        row = self._repo.get(connection_id)
        return row_to_connection(row) if row else None

    def disconnect(self, connection_id: str, *, purge: bool = False) -> None:
        """Soft-disconnect (default) or purge raw rows + delete row (purge=True)."""
        if not purge:
            self._repo.soft_disconnect(connection_id)
            return

        conn = self._repo.get(connection_id)
        if conn is None:
            raise GSheetError(f"Unknown connection: {connection_id}")

        # Atomic purge: DROP VIEW + raw DELETE + audited row DELETE all run
        # inside one transaction. A failure at any step rolls back the
        # whole purge so the connection row never desyncs from its raw
        # data. repo.delete cooperates via in_outer_txn=True.
        self._db.begin()
        try:
            if conn["adapter"] == "seed":
                alias = conn.get("alias")
                if alias:
                    # Defense-in-depth: re-validate the alias before
                    # interpolating into DROP VIEW. The insert path
                    # already validated via view_generator, but a
                    # malformed alias on disk would otherwise land here
                    # unchecked.
                    if not _SAFE_ALIAS_RE.fullmatch(alias):
                        raise GSheetError(
                            f"Refusing to DROP VIEW for unsafe alias: {alias!r}"
                        )
                    # security.md: quote dynamic identifiers via sqlglot
                    # even after regex validation — defense in depth, the
                    # rule is explicit.
                    safe_view = exp.to_identifier(f"gsheet_{alias}", quoted=True).sql(
                        "duckdb"
                    )
                    self._db.execute(f"DROP VIEW IF EXISTS raw.{safe_view};")  # noqa: S608  # alias regex-validated + sqlglot-quoted
                self._db.execute(
                    f"DELETE FROM {GSHEET_SEEDS.full_name} WHERE connection_id = ?",  # noqa: S608  # TableRef + parameterized value
                    [connection_id],
                )
            else:
                self._db.execute(
                    f"DELETE FROM {TABULAR_TRANSACTIONS.full_name} WHERE source_origin = ?",  # noqa: S608  # TableRef + parameterized value
                    [connection_id],
                )

            self._repo.delete(connection_id, in_outer_txn=True)
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def reconnect(self, connection_id: str, *, yes: bool = False) -> ConnectResult:
        """Re-detect against the current sheet, re-pin mapping, run a pull."""
        existing = self._repo.get(connection_id)
        if existing is None:
            raise GSheetError(f"Unknown connection: {connection_id}")

        # Resolve the current tab title by gid — sheet_name on the stored row
        # may be stale if the user renamed the tab between connect and reconnect.
        spreadsheet_id = existing["spreadsheet_id"]
        meta = self._sheets.get_workbook_metadata(spreadsheet_id)
        sheet = next((s for s in meta.sheets if s.gid == existing["sheet_gid"]), None)
        if sheet is None:
            # Workbook title, not spreadsheet_id — see connect() for why the
            # raw id must not surface in user-facing errors.
            raise GSheetUnreachableError(
                f"gid={existing['sheet_gid']} no longer present in workbook "
                f"{meta.title!r}; the tab was deleted"
            )
        rows = self._sheets.read_sheet_values(spreadsheet_id, sheet.name)
        if not rows:
            raise GSheetError("Sheet has no data")
        df = rows_to_df(rows)

        adapter = ADAPTERS[existing["adapter"]]
        detection = adapter.detect(df, account_name=existing.get("account_name"))

        if existing["adapter"] == "transactions" and detection.confidence == "low":
            raise LowConfidenceError(
                "Reconnect detection returned low confidence; "
                "the sheet structure may have changed substantially."
            )

        # Symmetric to connect(): a medium-confidence remap can silently
        # re-pin the wrong mapping, so require explicit acceptance via --yes.
        if (
            existing["adapter"] == "transactions"
            and detection.confidence == "medium"
            and not yes
        ):
            raise AmbiguousDetectionError(
                "Reconnect detection returned medium confidence. "
                "Re-run with --yes to accept the inferred mapping."
            )

        column_mapping = (
            detection.typed_columns
            if existing["adapter"] == "seed"
            else detection.column_mapping
        )

        self._repo.update_mapping(
            connection_id,
            column_mapping=column_mapping,
            header_signature=detection.header_signature,
            date_format=detection.date_format,
            sign_convention=detection.sign_convention,
            number_format=detection.number_format,
            skip_rows=detection.skip_rows,
            skip_trailing_patterns=detection.skip_trailing_patterns or None,
        )

        from moneybin.connectors.gsheet.pull_service import GSheetPullService

        pull_svc = GSheetPullService(
            db=self._db,
            sheets_client=self._sheets,
            oauth_client=self._oauth,
        )
        pull = pull_svc.pull_connection(connection_id)
        refreshed = self._repo.get(connection_id)
        if refreshed is None:
            raise RuntimeError(f"connection vanished mid-reconnect: {connection_id}")
        return ConnectResult(
            connection=row_to_connection(refreshed),
            detection=detection,
            initial_pull=pull.load_result,
            initial_pull_status=pull.status,
            initial_pull_error=pull.error_message,
        )


def _validate_transactions_column_mapping(
    *,
    user_mapping: dict[str, str],
    sheet_headers: list[str],
) -> None:
    """Verify a user-supplied --column-mapping is schema-complete.

    Mapping shape is ``{source_header: dest_field}``. Two failure modes:

    1. Required dest field missing — pulls will fail every run.
    2. Source header referenced but not present in the sheet — header
       lookup fails at transform-time, leaks Polars error to MCP.

    Raises GSheetError with a message that names the missing field /
    unknown header so the user can fix the mapping without a stack-
    trace dig.
    """
    dest_fields = set(user_mapping.values())
    missing = [f for f in REQUIRED_DEST_FIELDS if f not in dest_fields]
    if missing:
        raise GSheetError(
            f"--column-mapping is missing required dest field(s): {missing}. "
            f"The transactions adapter needs {list(REQUIRED_DEST_FIELDS)} "
            "to produce rows."
        )
    sheet_header_set = set(sheet_headers)
    unknown_sources = [src for src in user_mapping if src not in sheet_header_set]
    if unknown_sources:
        raise GSheetError(
            f"--column-mapping references header(s) not in the sheet: "
            f"{unknown_sources}. Available headers: {sheet_headers}."
        )


def rows_to_df(rows: list[list[str]]) -> pl.DataFrame:
    """Convert raw cell values (first row headers) into a Polars DataFrame.

    Ragged rows (Google Sheets trims trailing empty cells) are padded to the
    header width with ``None`` so polars receives uniform-length columns.

    Rejects duplicate header text — keying by header collapses duplicates
    into one dict entry and silently corrupts row cardinality.
    """
    if not rows:
        return pl.DataFrame()
    headers, *data = rows
    seen: set[str] = set()
    duplicates: list[str] = []
    for h in headers:
        if h in seen and h not in duplicates:
            duplicates.append(h)
        seen.add(h)
    if duplicates:
        raise GSheetError(
            f"Duplicate header(s) in sheet: {duplicates}. "
            "Rename to make headers unique before connecting."
        )
    columns: dict[str, list[str | None]] = {h: [] for h in headers}
    for row in data:
        for i, header in enumerate(headers):
            columns[header].append(row[i] if i < len(row) else None)
        # Extra columns past the header width have no header to bind to and
        # are dropped implicitly by the header-keyed loop above.
    return pl.DataFrame(columns)


def row_to_connection(row: dict[str, Any]) -> GSheetConnection:
    """Convert a ``GSheetConnectionsRepo.get`` row dict to a GSheetConnection.

    The repo decodes JSON columns and returns timestamps as ``datetime``
    objects; this helper stringifies the timestamps to match the
    ``GSheetConnection`` dataclass contract (``str | None``).
    """
    return GSheetConnection(
        connection_id=row["connection_id"],
        spreadsheet_id=row["spreadsheet_id"],
        sheet_gid=row["sheet_gid"],
        sheet_name=row["sheet_name"],
        workbook_name=row["workbook_name"],
        adapter=row["adapter"],
        alias=row.get("alias"),
        account_id=row.get("account_id"),
        account_name=row.get("account_name"),
        column_mapping=row.get("column_mapping") or {},
        header_signature=row.get("header_signature") or [],
        date_format=row.get("date_format"),
        sign_convention=row.get("sign_convention"),
        number_format=row.get("number_format"),
        skip_rows=row.get("skip_rows") or 0,
        skip_trailing_patterns=row.get("skip_trailing_patterns") or [],
        status=row["status"],
        last_pull_at=_to_iso(row.get("last_pull_at")),
        last_pull_import_id=row.get("last_pull_import_id"),
        last_success_at=_to_iso(row.get("last_success_at")),
        last_status_reason=row.get("last_status_reason"),
        consecutive_failure_count=row.get("consecutive_failure_count") or 0,
    )


def _to_iso(value: Any) -> str | None:
    """Stringify a datetime to ISO format; pass through None and strings."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
