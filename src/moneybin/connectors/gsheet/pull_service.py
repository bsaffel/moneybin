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

# Sanitized messages surfaced via PullResult.error_message + last_drift_reason.
# Raw upstream exceptions (Google HttpError, etc.) include URLs, IDs, and
# quota text that must not flow to the MCP wire (see security.md "Minimize
# data in errors"). Full detail is logged internally via logger.exception.
_SANITIZED_FAILURE_MESSAGES: dict[str, str] = {
    "auth_expired": (
        "OAuth token revoked or expired. Re-authenticate with `moneybin gsheet auth`."
    ),
    "unreachable": (
        "Sheet unreachable. Verify the URL and that the sheet is shared "
        "with the authorized Google account."
    ),
    "rate_limited": (
        "Google Sheets API rate limit reached. Retry after a short delay."
    ),
}

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
        import_id = uuid.uuid4().hex[:12]

        self._open_import_log(import_id, conn)

        try:
            rows = self._fetch_with_retry(conn)
        except GSheetAuthError:
            # security.md: log full detail internally, return a generic
            # message to callers. str(exc) on Google HttpError leaks the
            # spreadsheet URL, response body, and quota details into the
            # MCP wire via PullResult.error_message and the
            # last_drift_reason column.
            logger.exception(f"gsheet auth failed for connection={conn.connection_id}")
            return self._record_failure(conn, import_id, status="auth_expired")
        except GSheetUnreachableError:
            logger.exception(f"gsheet unreachable for connection={conn.connection_id}")
            return self._record_failure(conn, import_id, status="unreachable")
        except GSheetRateLimitError:
            logger.exception(f"gsheet rate-limited for connection={conn.connection_id}")
            return self._record_failure(conn, import_id, status="rate_limited")

        try:
            df = rows_to_df(rows)
        except Exception:
            # rows_to_df raises GSheetError on duplicate headers (round-3
            # guard). Without this wrap the import_log stays in
            # "importing" — same bug class the transform/load guard below
            # closes; the symmetric fix lives here.
            logger.exception(f"rows_to_df failed for connection={conn.connection_id}")
            return self._record_unexpected_failure(conn, import_id)
        drift = adapter.check_drift(conn, df)
        if drift.is_drift:
            # Drift closes the import_log row as "failed" — import_log's status
            # enum is source-agnostic and has no "drift_detected" value, and we
            # don't want a gsheet-specific status on a shared audit table. The
            # real distinction lives on the connection row (status =
            # "drift_detected" + last_drift_reason); audit queries that need to
            # exclude drift should join app.gsheet_connections, not read
            # import_log.status alone.
            self._close_import_log(import_id, status="failed", rows_imported=0)
            now = _utcnow()
            self._repo.update_after_pull(
                connection_id,
                last_pull_at=now,
                last_pull_import_id=import_id,
                last_success_at=None,
                status="drift_detected",
                consecutive_failure_count=conn.consecutive_failure_count + 1,
                last_drift_reason=drift.reason,
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
            # Without _record_unexpected_failure the import_log row stays
            # in "importing" AND the connection row stays in "healthy"
            # forever — list_healthy() keeps re-scheduling it on every
            # refresh_run with no visible failure signal.
            logger.exception(
                f"transform/load failed for connection={conn.connection_id}"
            )
            return self._record_unexpected_failure(conn, import_id)

        self._close_import_log(
            import_id,
            status="complete",
            rows_imported=load_result.rows_inserted + load_result.rows_upserted,
        )
        now = _utcnow()
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
                    # str(exc) reaches logger.exception in pull_connection;
                    # connection_id alone is enough for correlation and
                    # keeps spreadsheet_id out of the log per security.md.
                    raise GSheetUnreachableError(
                        "Sheet tab is no longer present in the workbook "
                        "(deleted or renamed beyond gid resolution)."
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
    ) -> PullResult:
        """Close the import-log + update connection state for a pull failure.

        Surfaces a sanitized, actionable message in PullResult.error_message
        and on the connection row's last_drift_reason — never the raw
        upstream exception text, which can include URLs, IDs, and quota.
        """
        message = _SANITIZED_FAILURE_MESSAGES[status]
        self._close_import_log(import_id, status="failed", rows_imported=0)
        now = _utcnow()
        # Single repo write — last_drift_reason flows through
        # update_after_pull, avoiding the double-audit pattern (one row
        # for counter update, another for reason update).
        self._repo.update_after_pull(
            conn.connection_id,
            last_pull_at=now,
            last_pull_import_id=import_id,
            last_success_at=None,
            status=status,
            consecutive_failure_count=conn.consecutive_failure_count + 1,
            last_drift_reason=message,
        )
        return PullResult(
            connection_id=conn.connection_id,
            status=status,
            error_message=message,
        )

    def _record_unexpected_failure(
        self,
        conn: GSheetConnection,
        import_id: str,
    ) -> PullResult:
        """Mirror of _record_failure for non-classified exceptions.

        Closes the import_log row, increments consecutive_failure_count,
        and marks the connection status='failed'. Without this the
        connection would stay in 'healthy' across repeated transform/load
        failures and never surface as broken to gsheet_status.

        Caller logs the exception details internally; the message
        returned here is intentionally generic so internal state never
        leaks via MCP.
        """
        message = "Unexpected pull failure; see application logs."
        self._close_import_log(import_id, status="failed", rows_imported=0)
        now = _utcnow()
        self._repo.update_after_pull(
            conn.connection_id,
            last_pull_at=now,
            last_pull_import_id=import_id,
            last_success_at=None,
            status="failed",
            consecutive_failure_count=conn.consecutive_failure_count + 1,
            last_drift_reason=message,
        )
        return PullResult(
            connection_id=conn.connection_id,
            status="failed",
            error_message=message,
        )


def _utcnow() -> datetime:
    """Return the current UTC timestamp.

    A datetime (not an ISO string) so the repo write path matches the
    datetime objects DuckDB returns on read — one type for the column
    end to end.
    """
    return datetime.now(UTC)
