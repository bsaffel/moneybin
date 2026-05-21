"""GSheetPullService — orchestrates per-connection pulls.

A single pull writes one ``raw.import_log`` row (lifecycle ``importing`` →
``complete`` | ``failed``), invokes the adapter's drift check / transform /
load, and updates ``app.gsheet_connections`` with the success/failure
outcome via ``GSheetConnectionsRepo.update_after_pull``.

Per-connection isolation: ``pull_all_healthy`` catches failures from one
connection so the rest of the batch can still run.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from moneybin.connectors.gsheet.adapters import ADAPTERS
from moneybin.connectors.gsheet.adapters.base import GSheetConnection, LoadResult
from moneybin.connectors.gsheet.connection_service import (
    row_to_connection,
    rows_to_df,
)
from moneybin.connectors.gsheet.errors import (
    GSheetAuthError,
    GSheetError,
    GSheetRateLimitError,
    GSheetUnreachableError,
)
from moneybin.connectors.gsheet.sheets_api import SheetsAPI
from moneybin.database import Database
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo

logger = logging.getLogger(__name__)

# Retry only on transient rate-limit errors; auth/unreachable failures
# fail-fast — retries can't fix a revoked token or a deleted sheet.
_RETRY_MAX = 3
_RETRY_BACKOFF_BASE = 1.5

PullStatus = Literal[
    "complete",
    "drift_detected",
    "auth_expired",
    "unreachable",
    "rate_limited",
    "failed",
]


@dataclass
class PullResult:
    """Outcome of a single ``pull_connection`` call."""

    connection_id: str
    status: PullStatus
    load_result: LoadResult | None = None
    drift_reason: str | None = None
    error_message: str | None = None


class GSheetPullService:
    """Per-connection pull orchestration with retry + isolation."""

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

    def pull_connection(self, connection_id: str) -> PullResult:
        """Pull one connection. Maps adapter exceptions to PullResult statuses."""
        conn_row = self._repo.get(connection_id)
        if conn_row is None:
            raise GSheetError(f"Unknown connection: {connection_id}")
        conn = row_to_connection(conn_row)
        adapter = ADAPTERS[conn.adapter]
        import_id = uuid.uuid4().hex[:16]

        self._open_import_log(import_id, conn)

        try:
            rows = self._fetch_with_retry(conn)
        except GSheetAuthError as exc:
            return self._record_failure(
                conn,
                import_id,
                status="auth_expired",
                reason=str(exc),
            )
        except GSheetUnreachableError as exc:
            return self._record_failure(
                conn,
                import_id,
                status="unreachable",
                reason=str(exc),
            )
        except GSheetRateLimitError as exc:
            return self._record_failure(
                conn,
                import_id,
                status="rate_limited",
                reason=str(exc),
            )

        df = rows_to_df(rows)
        drift = adapter.check_drift(conn, df)
        if drift.is_drift:
            self._close_import_log(import_id, status="failed", rows_imported=0)
            now = _utcnow_iso()
            self._repo.update_after_pull(
                connection_id,
                last_pull_at=now,
                last_pull_import_id=import_id,
                last_success_at=None,
                status="drift_detected",
                consecutive_failure_count=conn.consecutive_failure_count + 1,
            )
            # Carry the drift reason onto the connection row separately —
            # update_after_pull doesn't touch last_drift_reason.
            self._repo.update_status(
                connection_id,
                status="drift_detected",
                reason=drift.reason,
            )
            return PullResult(
                connection_id=connection_id,
                status="drift_detected",
                drift_reason=drift.reason,
            )

        try:
            transformed = adapter.transform(df, conn)
            load_result = adapter.load(transformed, conn, self._db, import_id)
        except Exception:
            # Without this guard the import_log row stays in "importing" forever
            # — transform/load failures escape pull_connection's existing branch
            # for fetch/drift errors and the outer pull_all_healthy catch never
            # closes the log.
            self._close_import_log(import_id, status="failed", rows_imported=0)
            raise

        self._close_import_log(
            import_id,
            status="complete",
            rows_imported=load_result.rows_inserted + load_result.rows_upserted,
        )
        now = _utcnow_iso()
        self._repo.update_after_pull(
            connection_id,
            last_pull_at=now,
            last_pull_import_id=import_id,
            last_success_at=now,
            status="healthy",
            consecutive_failure_count=0,
        )
        return PullResult(
            connection_id=connection_id,
            status="complete",
            load_result=load_result,
        )

    def pull_all_healthy(self) -> list[PullResult]:
        """Pull every healthy connection; isolate failures from siblings."""
        results: list[PullResult] = []
        for row in self._repo.list_healthy():
            connection_id = row["connection_id"]
            try:
                results.append(self.pull_connection(connection_id))
            except Exception:  # noqa: BLE001  # per-connection isolation
                # security.md: log full detail internally, return a generic
                # message to callers. str(exc) could surface DuckDB error
                # text, stack-trace fragments, or internal field values.
                logger.exception(
                    f"Pull failed unexpectedly for connection={connection_id}"
                )
                results.append(
                    PullResult(
                        connection_id=connection_id,
                        status="failed",
                        error_message="Unexpected pull failure; see application logs.",
                    )
                )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_with_retry(self, conn: GSheetConnection) -> list[list[str]]:
        """Read sheet values with exponential backoff on rate-limit errors only.

        Resolves the current sheet title by ``gid`` on each attempt — sheet
        titles are user-editable, but ``gid`` is the stable identifier
        Google issues at tab creation. This lets pulls survive a non-
        destructive tab rename instead of failing as unreachable.
        """
        last_exc: GSheetRateLimitError | None = None
        for attempt in range(_RETRY_MAX):
            try:
                meta = self._sheets.get_workbook_metadata(conn.spreadsheet_id)
                sheet = next((s for s in meta.sheets if s.gid == conn.sheet_gid), None)
                if sheet is None:
                    raise GSheetUnreachableError(
                        f"gid={conn.sheet_gid} no longer present in workbook "
                        f"{conn.spreadsheet_id}; the tab was deleted"
                    )
                return self._sheets.read_sheet_values(conn.spreadsheet_id, sheet.name)
            except GSheetRateLimitError as exc:
                last_exc = exc
                time.sleep(_RETRY_BACKOFF_BASE**attempt)
                continue
        # Loop invariant: we only reach here after _RETRY_MAX iterations all
        # raised, so last_exc is populated.
        if last_exc is None:  # pragma: no cover — defensive
            raise RuntimeError("retry loop exited without raising")
        raise last_exc

    def _open_import_log(self, import_id: str, conn: GSheetConnection) -> None:
        """Insert an ``importing`` row in raw.import_log scoped to this pull."""
        account_names = [] if conn.account_name is None else [conn.account_name]
        self._db.execute(
            """
            INSERT INTO raw.import_log (
                import_id, source_file, source_type, source_origin,
                format_name, format_source, account_names, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                import_id,
                f"gsheet://{conn.spreadsheet_id}/{conn.sheet_gid}",
                "gsheet",
                conn.connection_id,
                f"gsheet:{conn.workbook_name}/{conn.sheet_name}",
                "gsheet",
                json.dumps(account_names),
                "importing",
            ],
        )

    def _close_import_log(
        self,
        import_id: str,
        *,
        status: Literal["complete", "failed"],
        rows_imported: int,
    ) -> None:
        """Finalize the import_log row with terminal status."""
        self._db.execute(
            """
            UPDATE raw.import_log
               SET status = ?,
                   rows_imported = ?,
                   completed_at = CURRENT_TIMESTAMP
             WHERE import_id = ?
            """,
            [status, rows_imported, import_id],
        )

    def _record_failure(
        self,
        conn: GSheetConnection,
        import_id: str,
        *,
        status: Literal["auth_expired", "unreachable", "rate_limited"],
        reason: str,
    ) -> PullResult:
        """Close the import-log + update connection state for a pull failure."""
        self._close_import_log(import_id, status="failed", rows_imported=0)
        now = _utcnow_iso()
        # update_after_pull tracks counters; last_success_at unchanged (None
        # is treated as COALESCE-keep in the repo).
        self._repo.update_after_pull(
            conn.connection_id,
            last_pull_at=now,
            last_pull_import_id=import_id,
            last_success_at=None,
            status=status,
            consecutive_failure_count=conn.consecutive_failure_count + 1,
        )
        # Separate update_status call so last_drift_reason carries the human
        # explanation. update_after_pull does not touch last_drift_reason.
        self._repo.update_status(
            conn.connection_id,
            status=status,
            reason=reason,
        )
        return PullResult(
            connection_id=conn.connection_id,
            status=status,
            error_message=reason,
        )


def _utcnow_iso() -> str:
    """Return the current UTC timestamp as ISO 8601."""
    return datetime.now(UTC).isoformat()
