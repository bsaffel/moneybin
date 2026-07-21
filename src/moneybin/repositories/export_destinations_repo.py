"""Audited writes to ``app.export_destinations``."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any, cast

from moneybin.exports.models import DestinationKind, ExportDestination
from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import EXPORT_DESTINATIONS

logger = logging.getLogger(__name__)

_FULL_ROW_COLUMNS = (
    "destination_id",
    "name",
    "kind",
    "local_path",
    "spreadsheet_id",
    "managed_tab_prefix",
    "created_at",
    "updated_at",
)


def _decode_destination(row: dict[str, Any]) -> ExportDestination:
    """Decode one complete database row into the shared destination contract."""
    return ExportDestination(
        destination_id=cast(str, row["destination_id"]),
        name=cast(str, row["name"]),
        kind=cast(DestinationKind, row["kind"]),
        local_path=Path(row["local_path"]) if row["local_path"] is not None else None,
        spreadsheet_id=cast(str | None, row["spreadsheet_id"]),
        managed_tab_prefix=cast(str | None, row["managed_tab_prefix"]),
    )


class ExportDestinationsRepo(BaseRepo):
    """Audited saved-destination configuration for export delivery."""

    repository = "export_destinations"
    table_ref = EXPORT_DESTINATIONS
    pk_columns = ("destination_id",)

    def _fetch_full_row(self, destination_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            EXPORT_DESTINATIONS,
            _FULL_ROW_COLUMNS,
            "destination_id",
            destination_id,
        )

    def _fetch_by_name(self, name: str) -> dict[str, Any] | None:
        return self._fetch_one(EXPORT_DESTINATIONS, _FULL_ROW_COLUMNS, "name", name)

    def list(self) -> list[ExportDestination]:
        """Return saved destinations in stable user-facing name order."""
        columns = ", ".join(quote_ident(column) for column in _FULL_ROW_COLUMNS)
        rows = self._db.execute(
            f"SELECT {columns} FROM {EXPORT_DESTINATIONS.full_name} "  # noqa: S608  # TableRef + allowlisted columns
            "ORDER BY name ASC, destination_id ASC"
        ).fetchall()
        return [
            _decode_destination(dict(zip(_FULL_ROW_COLUMNS, row, strict=True)))
            for row in rows
        ]

    def resolve(self, reference: str) -> ExportDestination | None:
        """Resolve an exact destination id or exact name; never fuzzy-match."""
        columns = ", ".join(quote_ident(column) for column in _FULL_ROW_COLUMNS)
        rows = self._db.execute(
            f"SELECT {columns} FROM {EXPORT_DESTINATIONS.full_name} "  # noqa: S608  # TableRef + allowlisted columns
            "WHERE destination_id = ? OR name = ?",
            [reference, reference],
        ).fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            raise ValueError("Destination reference is ambiguous")
        return _decode_destination(dict(zip(_FULL_ROW_COLUMNS, rows[0], strict=True)))

    def set_local(
        self,
        *,
        name: str,
        local_path: Path,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Create or replace a named local destination and emit one audit event."""
        return self._set(
            name=name,
            kind="local",
            local_path=local_path,
            spreadsheet_id=None,
            managed_tab_prefix=None,
            actor=actor,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

    def set_sheets(
        self,
        *,
        name: str,
        spreadsheet_id: str,
        managed_tab_prefix: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Create or replace a named Sheets destination and emit one audit event."""
        return self._set(
            name=name,
            kind="sheets",
            local_path=None,
            spreadsheet_id=spreadsheet_id,
            managed_tab_prefix=managed_tab_prefix,
            actor=actor,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

    def _set(
        self,
        *,
        name: str,
        kind: DestinationKind,
        local_path: Path | None,
        spreadsheet_id: str | None,
        managed_tab_prefix: str | None,
        actor: str,
        parent_audit_id: str | None,
        in_outer_txn: bool,
    ) -> AuditEvent:
        """Persist one complete kind-specific destination shape and audit it."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_by_name(name)
            destination_id = (
                cast(str, before["destination_id"])
                if before is not None
                else uuid.uuid4().hex[:12]
            )
            self._db.execute(
                f"""
                INSERT INTO {EXPORT_DESTINATIONS.full_name} (
                    destination_id, name, kind, local_path, spreadsheet_id,
                    managed_tab_prefix, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (name) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    local_path = EXCLUDED.local_path,
                    spreadsheet_id = EXCLUDED.spreadsheet_id,
                    managed_tab_prefix = EXCLUDED.managed_tab_prefix,
                    updated_at = EXCLUDED.updated_at
                """,  # noqa: S608  # TableRef + parameterized values; created_at is immutable
                [
                    destination_id,
                    name,
                    kind,
                    str(local_path) if local_path is not None else None,
                    spreadsheet_id,
                    managed_tab_prefix,
                ],
            )
            after = self._fetch_full_row(destination_id)
            event = self._emit_audit(
                action=f"export_destination.set_{kind}",
                target=(*self._audit_target, destination_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        logger.info(
            f"export_destination.set destination_id={destination_id} kind={kind} outcome=saved"
        )
        return event

    def remove(
        self,
        reference: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Remove saved configuration only; never delete destination content."""
        destination = self.resolve(reference)
        if destination is None:
            return None
        destination_id = destination.destination_id
        if destination_id is None:
            raise ValueError("Saved destination is missing destination_id")

        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_full_row(destination_id), "destination_id", destination_id
            )
            self._db.execute(
                f"DELETE FROM {EXPORT_DESTINATIONS.full_name} WHERE destination_id = ?",  # noqa: S608  # TableRef + parameterized value
                [destination_id],
            )
            event = self._emit_audit(
                action="export_destination.remove",
                target=(*self._audit_target, destination_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        logger.info(
            f"export_destination.remove destination_id={destination_id} outcome=removed"
        )
        return event
