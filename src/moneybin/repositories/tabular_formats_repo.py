"""Audited writes to ``app.tabular_formats`` (saved tabular import-format profiles).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. The format-persistence
helpers in ``extractors/tabular/formats.py`` compose this instead of issuing raw
mutation SQL — this is one of the non-service writers the spec covers (a loader
module, not a service): the protection boundary is the ``app.*`` table, not the
caller (Resolved Design Decision §5).

``name`` is the semantic-slug primary key (``identifiers.md`` strategy 4); there
is no shadow id, so ``target_id`` is the format ``name`` and ``set`` mirrors the
``INSERT OR REPLACE`` idempotency the writer relies on.
"""

from __future__ import annotations

import json
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import TABULAR_FORMATS

_TABULAR_FORMATS_COLUMNS = (
    "name",
    "institution_name",
    "file_type",
    "delimiter",
    "encoding",
    "skip_rows",
    "sheet",
    "header_signature",
    "field_mapping",
    "sign_convention",
    "date_format",
    "number_format",
    "skip_trailing_patterns",
    "multi_account",
    "source",
    "times_used",
    "last_used_at",
    "created_at",
    "updated_at",
)


class TabularFormatsRepo(BaseRepo):
    """Audited upsert/delete over ``app.tabular_formats``."""

    repository = "tabular_formats"

    _AUDIT_TARGET = (TABULAR_FORMATS.schema, TABULAR_FORMATS.name)

    def _fetch_row(self, name: str) -> dict[str, Any] | None:
        return self._fetch_one(TABULAR_FORMATS, _TABULAR_FORMATS_COLUMNS, "name", name)

    def set(
        self,
        *,
        name: str,
        institution_name: str,
        file_type: str,
        delimiter: str | None,
        encoding: str,
        skip_rows: int,
        sheet: str | None,
        header_signature: list[str],
        field_mapping: dict[str, str],
        sign_convention: str,
        date_format: str,
        number_format: str,
        skip_trailing_patterns: list[str] | None,
        multi_account: bool,
        source: str,
        times_used: int,
        last_used_at: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Upsert one format profile (INSERT OR REPLACE on ``name``) + audit.

        ``before`` is the prior row when the format already existed (re-save),
        else ``None``; ``after`` is the resulting row. ``target_id`` is ``name``.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(name)
            self._db.execute(
                f"""
                INSERT OR REPLACE INTO {TABULAR_FORMATS.full_name} (
                    name, institution_name, file_type, delimiter, encoding,
                    skip_rows, sheet, header_signature, field_mapping,
                    sign_convention, date_format, number_format,
                    skip_trailing_patterns, multi_account, source,
                    times_used, last_used_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, CURRENT_TIMESTAMP
                )
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    name,
                    institution_name,
                    file_type,
                    delimiter,
                    encoding,
                    skip_rows,
                    sheet,
                    json.dumps(header_signature),
                    json.dumps(field_mapping),
                    sign_convention,
                    date_format,
                    number_format,
                    json.dumps(skip_trailing_patterns)
                    if skip_trailing_patterns is not None
                    else None,
                    multi_account,
                    source,
                    times_used,
                    last_used_at,
                ],
            )
            after = self._fetch_row(name)
            return self._emit_audit(
                action="tabular_format.set",
                target=(*self._AUDIT_TARGET, name),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        name: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Delete a saved format; capture the full prior row in ``before``.

        Returns ``None`` (without emitting audit) when no format with this name
        exists — the writer contract is lookup-or-noop (mirrors the prior
        ``delete_format_from_db`` boolean), not assert-exists.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(name)
            if before is None:
                return None
            self._db.execute(
                f"DELETE FROM {TABULAR_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef + parameterized value
                [name],
            )
            return self._emit_audit(
                action="tabular_format.delete",
                target=(*self._AUDIT_TARGET, name),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
