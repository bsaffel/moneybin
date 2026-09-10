"""Manual Security routing and validated recovery of historical Raw repoints."""

from __future__ import annotations

from typing import Any, ClassVar

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.identity import manual_identity_sql
from moneybin.repositories.base import BaseRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import MANUAL_INVESTMENT_TRANSACTIONS, SECURITY_LINKS, TableRef

_MANUAL_INVESTMENT_COLUMNS = (
    "source_transaction_id",
    "source_type",
    "source_origin",
    "import_id",
    "account_id",
    "security_id",
    "security_ref",
    "type",
    "subtype",
    "event_group_id",
    "trade_date",
    "settlement_date",
    "original_acquisition_date",
    "quantity",
    "price",
    "amount",
    "fees",
    "currency_code",
    "description",
    "created_at",
    "created_by",
    "investment_transaction_id",
)


class ManualInvestmentTransactionsRepo(BaseRepo):
    """Route manual identities without modifying their frozen observations."""

    repository: ClassVar[str] = "manual_investment_transactions"
    table_ref: ClassVar[TableRef] = MANUAL_INVESTMENT_TRANSACTIONS
    pk_columns: ClassVar[tuple[str, ...]] = ("source_transaction_id",)

    def _fetch_row(self, source_transaction_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            MANUAL_INVESTMENT_TRANSACTIONS,
            _MANUAL_INVESTMENT_COLUMNS,
            "source_transaction_id",
            source_transaction_id,
        )

    def list_ids_for_security(self, security_id: str) -> list[str]:
        """Enumerate current identities, including routes from earlier merges."""
        rows = self._db.execute(
            f"""
            SELECT source_transaction_id FROM ({manual_identity_sql()}) AS identity
            WHERE security_id = ? ORDER BY source_transaction_id
            """,  # noqa: S608  # repository model query with parameterized identity
            [security_id],
        ).fetchall()
        return [str(row[0]) for row in rows]

    def _accepted_route(self, source_transaction_id: str) -> tuple[str, str] | None:
        rows = self._db.execute(
            f"""
            SELECT link_id, security_id FROM {SECURITY_LINKS.full_name}
            WHERE source_type = 'manual'
              AND ref_kind = 'manual_investment_transaction_id'
              AND ref_value = ? AND status = 'accepted'
            """,  # noqa: S608  # TableRef and parameterized source id
            [source_transaction_id],
        ).fetchall()
        if len(rows) > 1:
            raise UserError(
                "Manual identity has conflicting accepted Security Links.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        return (str(rows[0][0]), str(rows[0][1])) if rows else None

    def repoint_security(
        self,
        *,
        source_transaction_id: str,
        new_security_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
        undoes_operation_id: str | None = None,
    ) -> AuditEvent:
        """Write an audited Link; absent routing means the frozen Raw assignment."""
        with self._transaction(in_outer_txn=in_outer_txn):
            raw = self._require(
                self._fetch_row(source_transaction_id),
                "source_transaction_id",
                source_transaction_id,
            )
            route = self._accepted_route(source_transaction_id)
            if (route[1] if route else raw["security_id"]) == new_security_id:
                raise ValueError(
                    "manual investment already carries the target security"
                )
            links = SecurityLinksRepo(self._db, audit=self._audit)
            if route:
                return links.repoint(
                    link_id=route[0],
                    new_security_id=new_security_id,
                    decided_by="user",
                    actor=actor,
                    parent_audit_id=parent_audit_id,
                    in_outer_txn=True,
                    undoes_operation_id=undoes_operation_id,
                )
            return links.insert(
                security_id=new_security_id,
                ref_kind="manual_investment_transaction_id",
                ref_value=source_transaction_id,
                source_type="manual",
                decided_by="user",
                actor=actor,
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
                undoes_operation_id=undoes_operation_id,
            )

    def validate_legacy_event(self, event: AuditEvent) -> None:
        """Refuse incomplete or non-Security history before any cascade writes."""
        before, after = event.before_value, event.after_value
        action_suffix = event.action.removeprefix("manual_investment.repoint_security")
        valid_action = (
            event.action.startswith("manual_investment.repoint_security")
            and all(part == "undo" for part in action_suffix.split(".")[1:])
            and (not action_suffix or action_suffix.startswith("."))
        )
        if not valid_action or before is None or after is None:
            raise UserError(
                "Historical manual mutation has no supported identity recovery path.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        self._require_capture(before, _MANUAL_INVESTMENT_COLUMNS, event)
        self._require_capture(after, _MANUAL_INVESTMENT_COLUMNS, event)
        if (
            before.keys() != after.keys()
            or any(before[key] != after[key] for key in before if key != "security_id")
            or before["source_transaction_id"] != event.target_id
            or not isinstance(before["security_id"], str)
            or not isinstance(after["security_id"], str)
        ):
            raise UserError(
                "Historical manual mutation is not a complete Security-only repoint.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        raw = self._serialize_for_audit(self._fetch_row(str(event.target_id)))
        if raw is None or any(
            raw.get(key) != after[key] for key in after if key != "security_id"
        ):
            raise UserError(
                "Frozen manual observation no longer agrees with the captured history.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        route = self._accepted_route(str(event.target_id))
        if (route[1] if route else raw["security_id"]) != after["security_id"]:
            raise UserError(
                "Current manual identity no longer agrees with the captured history.",
                code=error_codes.RECOVERY_NO_PATH,
            )

    def undo_event(
        self,
        event: AuditEvent,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Translate legacy Raw inverses into actual, redoable Link audit events."""
        if event.target_table == SECURITY_LINKS.name:
            return SecurityLinksRepo(self._db, audit=self._audit).undo_event(
                event,
                actor=actor,
                in_outer_txn=in_outer_txn,
            )
        self.validate_legacy_event(event)
        if event.before_value == event.after_value:
            return None
        if event.before_value is None:
            raise UserError(
                "Historical manual mutation has no supported identity recovery path.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        return self.repoint_security(
            source_transaction_id=str(event.target_id),
            new_security_id=str(event.before_value["security_id"]),
            actor=actor,
            parent_audit_id=event.audit_id,
            in_outer_txn=in_outer_txn,
            undoes_operation_id=event.operation_id,
        )
