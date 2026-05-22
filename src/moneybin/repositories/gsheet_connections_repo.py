"""Audited writes to ``app.gsheet_connections``.

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation
of ``app.gsheet_connections`` flows through this repository, which pairs
the write with an ``app.audit_log`` row inside the same DuckDB transaction.
External callers must NOT issue raw ``INSERT``/``UPDATE``/``DELETE`` against
the table — the lint rule rejects that and the doctor verifies coverage.

Mutation mechanics (transaction handling, audit emission + metric, full-row
serialization) come from :class:`~moneybin.repositories.base.BaseRepo`. This
module owns only the table-specific SQL and column handling.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Literal

from moneybin.repositories.base import BaseRepo
from moneybin.tables import GSHEET_CONNECTIONS

logger = logging.getLogger(__name__)

# Audit target tuple shared across every mutation: (target_schema, target_table).
# Caller appends the row's connection_id to form the 3-tuple expected by
# AuditService.record_audit_event(target=...).
_AUDIT_TARGET = (GSHEET_CONNECTIONS.schema, GSHEET_CONNECTIONS.name)

# Columns selected for before_value / after_value capture. Per
# app-integrity-invariant.md Req 4, we capture the FULL pre-mutation row so
# Phase 2 undo can be additive without re-instrumentation.
_FULL_ROW_COLUMNS = (
    "connection_id",
    "spreadsheet_id",
    "sheet_gid",
    "sheet_name",
    "workbook_name",
    "adapter",
    "account_id",
    "account_name",
    "column_mapping",
    "header_signature",
    "date_format",
    "sign_convention",
    "number_format",
    "skip_rows",
    "skip_trailing_patterns",
    "status",
    "last_pull_at",
    "last_pull_import_id",
    "last_success_at",
    "last_status_reason",
    "consecutive_failure_count",
    "alias",
    "created_at",
    "updated_at",
)

# Columns that store JSON-encoded values. Reads decode these to Python
# objects so callers never see raw JSON strings; writes serialize via
# json.dumps once at the boundary.
_JSON_COLUMNS = frozenset({
    "column_mapping",
    "header_signature",
    "skip_trailing_patterns",
})

Status = Literal[
    "healthy",
    "auth_expired",
    "unreachable",
    "drift_detected",
    "rate_limited",
    "failed",
    "disconnected",
]


def _decode_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map a fetched row to a column → value dict, decoding JSON columns."""
    out: dict[str, Any] = {}
    for col, val in zip(_FULL_ROW_COLUMNS, row, strict=True):
        if col in _JSON_COLUMNS and isinstance(val, str):
            out[col] = json.loads(val)
        else:
            out[col] = val
    return out


class GSheetConnectionsRepo(BaseRepo):
    """Audited CRUD over ``app.gsheet_connections``.

    Every mutating method opens (or participates in) a transaction, captures
    the full pre-mutation row, performs the mutation, and emits a paired
    ``app.audit_log`` entry via :meth:`BaseRepo._emit_audit`. Reads decode the
    three JSON columns (``column_mapping``, ``header_signature``,
    ``skip_trailing_patterns``) into Python objects.

    Mutation methods return ``None`` / the generated id rather than the
    ``AuditEvent`` — these connections have no cascade callers, so the existing
    return shape is preserved for its callers. New repositories return the
    ``AuditEvent`` per the spec contract.
    """

    repository = "gsheet_connections"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_full_row(self, connection_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            GSHEET_CONNECTIONS,
            _FULL_ROW_COLUMNS,
            "connection_id",
            connection_id,
            decode=_decode_row,
        )

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def insert(
        self,
        *,
        spreadsheet_id: str,
        sheet_gid: int,
        sheet_name: str,
        workbook_name: str,
        adapter: str,
        alias: str | None,
        account_id: str | None,
        account_name: str | None,
        column_mapping: dict[str, str],
        header_signature: list[str],
        date_format: str | None,
        sign_convention: str | None,
        number_format: str | None,
        skip_rows: int,
        skip_trailing_patterns: list[str] | None,
        actor: str = "cli",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> str:
        """Insert a new connection row + audit. Returns the generated id."""
        connection_id = uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {GSHEET_CONNECTIONS.full_name} (
                    connection_id, spreadsheet_id, sheet_gid, sheet_name,
                    workbook_name, adapter, account_id, account_name,
                    column_mapping, header_signature,
                    date_format, sign_convention, number_format,
                    skip_rows, skip_trailing_patterns, alias
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    connection_id,
                    spreadsheet_id,
                    sheet_gid,
                    sheet_name,
                    workbook_name,
                    adapter,
                    account_id,
                    account_name,
                    json.dumps(column_mapping),
                    json.dumps(header_signature),
                    date_format,
                    sign_convention,
                    number_format,
                    skip_rows,
                    json.dumps(skip_trailing_patterns)
                    if skip_trailing_patterns is not None
                    else None,
                    alias,
                ],
            )
            after = self._fetch_full_row(connection_id)
            self._emit_audit(
                action="gsheet_connection.insert",
                target=(*_AUDIT_TARGET, connection_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        logger.info(
            f"gsheet_connection.insert connection_id={connection_id} actor={actor}"
        )
        return connection_id

    def update_status(
        self,
        connection_id: str,
        *,
        status: Status,
        reason: str | None = None,
        actor: str = "cli",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> None:
        """Update connection status + status reason; emit audit row."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_full_row(connection_id)
            if before is None:
                raise ValueError(f"connection_id={connection_id!r} not found")
            self._db.execute(
                f"""
                UPDATE {GSHEET_CONNECTIONS.full_name}
                   SET status = ?, last_status_reason = ?, updated_at = NOW()
                 WHERE connection_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [status, reason, connection_id],
            )
            after = self._fetch_full_row(connection_id)
            self._emit_audit(
                action="gsheet_connection.update_status",
                target=(*_AUDIT_TARGET, connection_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def update_after_pull(
        self,
        connection_id: str,
        *,
        last_pull_at: datetime,
        last_pull_import_id: str,
        last_success_at: datetime | None,
        status: Status,
        consecutive_failure_count: int,
        last_status_reason: str | None = None,
        actor: str = "system",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> None:
        """Persist pull-attempt results; emit audit row.

        ``last_status_reason`` is passed through inside the same UPDATE so
        callers don't need a paired ``update_status`` write for the
        reason column — that pattern previously emitted two audit rows
        per failed pull. None clears the column (intentional: a clean
        pull should clear any stale reason from the previous attempt).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_full_row(connection_id)
            if before is None:
                raise ValueError(f"connection_id={connection_id!r} not found")
            self._db.execute(
                f"""
                UPDATE {GSHEET_CONNECTIONS.full_name}
                   SET last_pull_at = ?,
                       last_pull_import_id = ?,
                       last_success_at = COALESCE(?, last_success_at),
                       status = ?,
                       consecutive_failure_count = ?,
                       last_status_reason = ?,
                       updated_at = NOW()
                 WHERE connection_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    last_pull_at,
                    last_pull_import_id,
                    last_success_at,
                    status,
                    consecutive_failure_count,
                    last_status_reason,
                    connection_id,
                ],
            )
            after = self._fetch_full_row(connection_id)
            self._emit_audit(
                action="gsheet_connection.update_after_pull",
                target=(*_AUDIT_TARGET, connection_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def update_mapping(
        self,
        connection_id: str,
        *,
        column_mapping: dict[str, str],
        header_signature: list[str],
        date_format: str | None,
        sign_convention: str | None,
        number_format: str | None,
        skip_rows: int,
        skip_trailing_patterns: list[str] | None = None,
        actor: str = "cli",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> None:
        """Re-pin mapping/signature after user reconnects to fix drift.

        Resets ``status`` to ``healthy`` and clears ``last_status_reason``.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_full_row(connection_id)
            if before is None:
                raise ValueError(f"connection_id={connection_id!r} not found")
            self._db.execute(
                f"""
                UPDATE {GSHEET_CONNECTIONS.full_name}
                   SET column_mapping = ?,
                       header_signature = ?,
                       date_format = ?,
                       sign_convention = ?,
                       number_format = ?,
                       skip_rows = ?,
                       skip_trailing_patterns = ?,
                       status = 'healthy',
                       last_status_reason = NULL,
                       updated_at = NOW()
                 WHERE connection_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    json.dumps(column_mapping),
                    json.dumps(header_signature),
                    date_format,
                    sign_convention,
                    number_format,
                    skip_rows,
                    json.dumps(skip_trailing_patterns)
                    if skip_trailing_patterns is not None
                    else None,
                    connection_id,
                ],
            )
            after = self._fetch_full_row(connection_id)
            self._emit_audit(
                action="gsheet_connection.reconnect",
                target=(*_AUDIT_TARGET, connection_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def soft_disconnect(
        self,
        connection_id: str,
        *,
        actor: str = "cli",
        parent_audit_id: str | None = None,
    ) -> None:
        """Mark the connection as disconnected (raw rows retained)."""
        self.update_status(
            connection_id,
            status="disconnected",
            reason=None,
            actor=actor,
            parent_audit_id=parent_audit_id,
        )

    def delete(
        self,
        connection_id: str,
        *,
        actor: str = "cli",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> None:
        """Hard-delete the connection row; emit audit row with full before.

        ``in_outer_txn=True`` skips begin/commit/rollback so a caller can
        wrap this with other side-effects in a single transaction
        (purge-with-raw-data is the canonical use case). The caller is
        then responsible for the transaction lifecycle.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_full_row(connection_id)
            if before is None:
                raise ValueError(f"connection_id={connection_id!r} not found")
            self._db.execute(
                f"DELETE FROM {GSHEET_CONNECTIONS.full_name} WHERE connection_id = ?",  # noqa: S608  # TableRef + parameterized value
                [connection_id],
            )
            self._emit_audit(
                action="gsheet_connection.delete",
                target=(*_AUDIT_TARGET, connection_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get(self, connection_id: str) -> dict[str, Any] | None:
        """Return one connection by id with JSON columns decoded, or None."""
        return self._fetch_full_row(connection_id)

    def list_all(self) -> list[dict[str, Any]]:
        """Return every connection row, ordered by ``created_at`` ascending."""
        cols = ", ".join(_FULL_ROW_COLUMNS)
        rows = self._db.execute(
            f"SELECT {cols} FROM {GSHEET_CONNECTIONS.full_name} ORDER BY created_at ASC, connection_id ASC"  # noqa: S608  # TableRef + allowlisted column list
        ).fetchall()
        return [_decode_row(r) for r in rows]

    def list_healthy(self) -> list[dict[str, Any]]:
        """Return connections eligible for an auto-pull on refresh_run.

        That's ``healthy`` plus the transient retryable statuses
        ``rate_limited`` and ``unreachable`` (a retry can clear those). Sticky
        states — auth_expired, drift_detected, disconnected, failed — need
        explicit operator action and are excluded. Name kept for caller
        stability; see the query comment for the per-status rationale.
        """
        cols = ", ".join(_FULL_ROW_COLUMNS)
        rows = self._db.execute(
            # Auto-retry transient statuses on each refresh_run. auth_expired
            # / drift_detected / disconnected / failed stay sticky — those
            # need explicit operator action; retrying would either fail in
            # an identical way or surprise the user.
            f"SELECT {cols} FROM {GSHEET_CONNECTIONS.full_name} "  # noqa: S608  # TableRef + allowlisted column list
            "WHERE status IN ('healthy', 'rate_limited', 'unreachable') "
            "ORDER BY created_at ASC, connection_id ASC"
        ).fetchall()
        return [_decode_row(r) for r in rows]
