"""Audited writes to ``app.export_destinations``."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any, cast

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.exports.models import DestinationKind, ExportDestination
from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.services.entity_reference import (
    AmbiguousEntity,
    EntityCandidate,
    MissingEntity,
    ResolvedEntity,
    resolve_entity_reference,
)
from moneybin.tables import EXPORT_DESTINATIONS, GSHEET_CONNECTIONS

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

type ExportDestinationResolution = ExportDestination | AmbiguousEntity | MissingEntity

_DERIVED_LOCAL_EXPORTS_CANDIDATE = EntityCandidate(
    entity_id="local:exports", display_name="local:exports"
)


class ExportDestinationSpreadsheetConflictError(UserError):
    """Raised when a workbook is already configured for inbound Sheets pulls."""

    def __init__(self) -> None:
        """Build a user-safe workbook-role conflict."""
        super().__init__(
            "This spreadsheet is already configured as an inbound connection and "
            "cannot also be an export destination.",
            code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
        )


class ReservedExportDestinationError(UserError):
    """Raised when callers try to persist the derived local:exports target."""

    def __init__(self) -> None:
        """Build a user-safe reserved-destination error."""
        super().__init__(
            "local:exports is a built-in derived destination and cannot be saved.",
            code=error_codes.MUTATION_INVALID_INPUT,
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

    def _list_full_rows(self) -> list[dict[str, Any]]:
        columns = ", ".join(quote_ident(column) for column in _FULL_ROW_COLUMNS)
        rows = self._db.execute(
            f"SELECT {columns} FROM {EXPORT_DESTINATIONS.full_name} "  # noqa: S608  # TableRef + allowlisted columns
            "ORDER BY name ASC, destination_id ASC"
        ).fetchall()
        return [dict(zip(_FULL_ROW_COLUMNS, row, strict=True)) for row in rows]

    def assert_not_inbound_connection(self, spreadsheet_id: str) -> None:
        """Reject a workbook already assigned to the inbound role."""
        conflict = self._db.execute(
            f"SELECT 1 FROM {GSHEET_CONNECTIONS.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE spreadsheet_id = ? LIMIT 1",
            [spreadsheet_id],
        ).fetchone()
        if conflict is not None:
            raise ExportDestinationSpreadsheetConflictError()

    def list(self) -> list[ExportDestination]:
        """Return saved destinations in stable user-facing name order."""
        return [_decode_destination(row) for row in self._list_full_rows()]

    def resolve(self, reference: str) -> ExportDestinationResolution:
        """Resolve by stable id, exact name, then unambiguous normalized name."""
        rows = self._list_full_rows()
        resolution = resolve_entity_reference(
            reference,
            (
                EntityCandidate(
                    entity_id=cast(str, row["destination_id"]),
                    display_name=cast(str, row["name"]),
                )
                for row in rows
            ),
        )
        if not isinstance(resolution, ResolvedEntity):
            return resolution
        return _decode_destination(
            next(row for row in rows if row["destination_id"] == resolution.entity_id)
        )

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
            self._validate_destination(
                name=name, kind=kind, spreadsheet_id=spreadsheet_id
            )
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

    def _validate_destination(
        self,
        *,
        name: str,
        kind: DestinationKind,
        spreadsheet_id: str | None,
    ) -> None:
        """Reject reserved targets and workbook role conflicts before mutation."""
        reservation = resolve_entity_reference(
            name, (_DERIVED_LOCAL_EXPORTS_CANDIDATE,)
        )
        if isinstance(reservation, ResolvedEntity):
            raise ReservedExportDestinationError()
        if kind != "sheets":
            return
        if spreadsheet_id is None:
            raise UserError(
                "A Sheets export destination requires a spreadsheet ID.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        self.assert_not_inbound_connection(spreadsheet_id)

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
        if not isinstance(destination, ExportDestination):
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
