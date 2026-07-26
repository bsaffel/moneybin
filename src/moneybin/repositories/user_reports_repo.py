"""Audited writes to ``app.user_reports``."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Final
from uuid import uuid4

from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import USER_REPORTS

logger = logging.getLogger(__name__)

_ID_NAMESPACE = "user"

_FULL_ROW_COLUMNS: Final = (
    "report_id",
    "name",
    "description",
    "query_sql",
    "params",
    "classes",
    "semantics",
    "class_downgrades",
    "class_fingerprint",
    "is_active",
    "created_at",
    "updated_at",
)

# Columns storing JSON-encoded values. Reads decode these so callers never see
# raw JSON strings; writes serialize via json.dumps once at the boundary.
_JSON_COLUMNS: Final = frozenset({
    "params",
    "classes",
    "semantics",
    "class_downgrades",
})

#: Columns a lifecycle update may set. ``report_id`` and ``created_at`` are
#: immutable, and ``updated_at`` is stamped by the mutation itself.
_SETTABLE_COLUMNS: Final = (
    "name",
    "description",
    "query_sql",
    "params",
    "classes",
    "semantics",
    "class_downgrades",
    "class_fingerprint",
    "is_active",
)


class _Unset:
    """Sentinel distinguishing an omitted update field from an explicit ``None``.

    ``description`` is nullable, so ``None`` is a value a caller may legitimately
    write. Without this, a partial update could not tell "leave the description
    alone" from "clear the description".
    """

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Final = _Unset()


def mint_user_report_id() -> str:
    """Mint the namespaced identity for one saved report.

    The ``r`` ahead of the hex is load-bearing, not styling: ``ReportSpec``
    requires both segments to match ``[a-z][a-z0-9_-]*``, and ``uuid4().hex``
    starts with a digit for 10 of its 16 possible leading values — so a bare
    truncated uuid would fail to construct a spec about 62% of the time.
    """
    return f"{_ID_NAMESPACE}:r{uuid4().hex[:12]}"


def _encode(column: str, value: object) -> object:
    """Serialize a JSON-backed column; pass every other value through."""
    if column in _JSON_COLUMNS:
        return json.dumps(value)
    return value


def _decode_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map a fetched row to a column → value dict, decoding JSON columns."""
    out: dict[str, Any] = {}
    for column, value in zip(_FULL_ROW_COLUMNS, row, strict=True):
        if column in _JSON_COLUMNS and isinstance(value, str):
            out[column] = json.loads(value)
        else:
            out[column] = value
    return out


class UserReportsRepo(BaseRepo):
    """Audited storage for user-created reports."""

    repository = "user_reports"
    table_ref = USER_REPORTS
    pk_columns = ("report_id",)

    def get(self, report_id: str) -> dict[str, Any] | None:
        """Read one saved report by its stable id, or ``None``."""
        return self._fetch_one(
            USER_REPORTS,
            _FULL_ROW_COLUMNS,
            "report_id",
            report_id,
            decode=_decode_row,
        )

    def find_by_name(self, name: str) -> dict[str, Any] | None:
        """Read one saved report by name — archived rows included.

        Archived reports keep their names, so a save onto a colliding archived
        name must be able to see it rather than reporting a bare conflict.
        """
        return self._fetch_one(
            USER_REPORTS, _FULL_ROW_COLUMNS, "name", name, decode=_decode_row
        )

    def create(
        self,
        *,
        name: str,
        query_sql: str,
        classes: Mapping[str, str],
        semantics: Mapping[str, Any],
        class_fingerprint: str,
        params: Sequence[Mapping[str, Any]] = (),
        description: str | None = None,
        class_downgrades: Mapping[str, Any] | None = None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Persist one new saved report and emit its paired audit row."""
        report_id = mint_user_report_id()
        values: dict[str, object] = {
            "report_id": report_id,
            "name": name,
            "description": description,
            "query_sql": query_sql,
            "params": list(params),
            "classes": dict(classes),
            "semantics": dict(semantics),
            "class_downgrades": dict(class_downgrades or {}),
            "class_fingerprint": class_fingerprint,
        }
        columns = tuple(values)

        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"INSERT INTO {USER_REPORTS.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted columns; values parameterized
                f"({', '.join(quote_ident(c) for c in columns)}) "
                f"VALUES ({', '.join('?' * len(columns))})",
                [_encode(column, values[column]) for column in columns],
            )
            after = self.get(report_id)
            event = self._emit_audit(
                action="user_report.create",
                target=(*self._audit_target, report_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        logger.info(f"user_report.create report_id={report_id} outcome=saved")
        return event

    def set(
        self,
        report_id: str,
        *,
        name: str | _Unset = UNSET,
        description: str | None | _Unset = UNSET,
        query_sql: str | _Unset = UNSET,
        params: Sequence[Mapping[str, Any]] | _Unset = UNSET,
        classes: Mapping[str, str] | _Unset = UNSET,
        semantics: Mapping[str, Any] | _Unset = UNSET,
        class_downgrades: Mapping[str, Any] | _Unset = UNSET,
        class_fingerprint: str | _Unset = UNSET,
        is_active: bool | _Unset = UNSET,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Apply a partial update to one saved report and audit the whole row.

        Only the fields the caller supplies are written. A metadata-only update
        deliberately leaves ``class_fingerprint`` alone: storing a current
        fingerprint beside a stale class map would put the next run on the
        no-re-resolution path and serve the stale, weaker class.
        """
        supplied = {
            column: value
            for column, value in (
                ("name", name),
                ("description", description),
                ("query_sql", query_sql),
                ("params", params),
                ("classes", classes),
                ("semantics", semantics),
                ("class_downgrades", class_downgrades),
                ("class_fingerprint", class_fingerprint),
                ("is_active", is_active),
            )
            if not isinstance(value, _Unset)
        }
        if not supplied:
            raise ValueError("set requires at least one field to update")
        assignments = [column for column in _SETTABLE_COLUMNS if column in supplied]

        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self.get(report_id), "report_id", report_id)
            set_sql = ", ".join(f"{quote_ident(c)} = ?" for c in assignments)
            self._db.execute(
                f"UPDATE {USER_REPORTS.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted columns; values parameterized
                f"SET {set_sql}, updated_at = CURRENT_TIMESTAMP "
                "WHERE report_id = ?",
                [_encode(c, supplied[c]) for c in assignments] + [report_id],
            )
            after = self.get(report_id)
            event = self._emit_audit(
                action="user_report.set",
                target=(*self._audit_target, report_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        logger.info(
            f"user_report.set report_id={report_id} "
            f"fields={len(assignments)} outcome=updated"
        )
        return event

    def delete(
        self,
        report_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Remove one saved report, capturing the full row the undo path needs."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self.get(report_id), "report_id", report_id)
            self._db.execute(
                f"DELETE FROM {USER_REPORTS.full_name} WHERE report_id = ?",  # noqa: S608  # TableRef + parameterized value
                [report_id],
            )
            event = self._emit_audit(
                action="user_report.delete",
                target=(*self._audit_target, report_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        logger.info(f"user_report.delete report_id={report_id} outcome=removed")
        return event
