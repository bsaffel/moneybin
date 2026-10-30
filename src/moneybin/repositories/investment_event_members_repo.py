"""Audited owner of exact Golden investment membership transitions."""

from dataclasses import asdict
from typing import Any
from uuid import uuid4

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_membership import EventMember
from moneybin.repositories.base import BaseRepo
from moneybin.tables import INVESTMENT_EVENT_MEMBERS

_COLUMNS = (
    "membership_id",
    "event_group_id",
    "investment_transaction_id",
    "member_kind",
    "source_event_key",
    "source_type",
    "source_origin",
    "native_reference",
    "observation_version",
    "semantic_leg_role",
    "bound_account_id",
    "bound_security_id",
    "bound_effective_currency",
    "currency_source",
    "account_generation",
    "security_generation",
    "currency_generation",
    "decision_id",
    "reconstruction_evidence",
    "created_at",
    "retired_at",
    "projection_changed_at",
)


class InvestmentEventMembersRepo(BaseRepo):
    """Keep membership writes and their complete audit images atomic."""

    repository = "investment_event_members"
    table_ref = INVESTMENT_EVENT_MEMBERS
    pk_columns = ("membership_id",)

    def _fetch_row(self, membership_id: str) -> dict[str, Any] | None:
        return self._fetch_one(self.table_ref, _COLUMNS, "membership_id", membership_id)

    def history(self) -> list[dict[str, Any]]:
        """Read complete active and retired membership, ordered deterministically."""
        cursor = self._db.execute(
            f"""SELECT * FROM {INVESTMENT_EVENT_MEMBERS.full_name}
                ORDER BY created_at, membership_id"""  # noqa: S608  # fixed TableRef
        )
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def has_history(self) -> bool:
        """Whether this profile has crossed the membership-owned identity boundary."""
        present = self._db.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ?",
            [INVESTMENT_EVENT_MEMBERS.schema, INVESTMENT_EVENT_MEMBERS.name],
        ).fetchone()
        if present is None:
            return False
        return (
            self._db.execute(
                f"SELECT 1 FROM {INVESTMENT_EVENT_MEMBERS.full_name} LIMIT 1"  # noqa: S608  # fixed TableRef
            ).fetchone()
            is not None
        )  # fixed TableRef

    def active_members(self) -> list[dict[str, Any]]:
        """Read the sole active Golden membership authority."""
        return [row for row in self.history() if row["retired_at"] is None]

    def register(
        self,
        member: EventMember,
        *,
        actor: str,
        in_outer_txn: bool = False,
        parent_audit_id: str | None = None,
        membership_id: str | None = None,
    ) -> str:
        """Activate one exact member without replacing its historical evidence."""
        membership_id = membership_id or uuid4().hex[:12]
        values = asdict(member)
        binding = values.pop("binding")
        values.update({
            "bound_account_id": binding["account_id"],
            "bound_security_id": binding["security_id"],
            "bound_effective_currency": binding["effective_currency"],
            **{
                key: binding[key]
                for key in (
                    "currency_source",
                    "account_generation",
                    "security_generation",
                    "currency_generation",
                )
            },
        })
        columns = _COLUMNS[:19]
        with self._transaction(in_outer_txn=in_outer_txn):
            # The Database writer lock serializes this check and insert across processes.
            if self._db.execute(
                f"""SELECT 1 FROM {INVESTMENT_EVENT_MEMBERS.full_name}
                    WHERE member_kind = ? AND source_type = ? AND source_origin = ?
                      AND native_reference = ? AND retired_at IS NULL LIMIT 1""",  # noqa: S608  # fixed TableRef, bound identities
                [
                    member.member_kind,
                    member.source_type,
                    member.source_origin,
                    member.native_reference,
                ],
            ).fetchone():
                raise UserError(
                    "Source row already has active Golden membership.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            self._db.execute(
                f"""INSERT INTO {INVESTMENT_EVENT_MEMBERS.full_name}
                    ({", ".join(f'"{c}"' for c in columns)})
                    VALUES ({", ".join("?" for _ in columns)})""",  # noqa: S608  # fixed columns, bound values
                [membership_id, *(values[column] for column in columns[1:])],
            )
            self._emit_audit(
                action="investment_membership.register",
                target=(*self._audit_target, membership_id),
                before=None,
                after=self._serialize_for_audit(self._fetch_row(membership_id)),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        return membership_id

    def retire(
        self,
        membership_id: str,
        *,
        actor: str,
        in_outer_txn: bool = False,
        parent_audit_id: str | None = None,
    ) -> None:
        """Retire a member while preserving its immutable projection binding."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(membership_id)
            if before is None or before["retired_at"] is not None:
                raise UserError(
                    "Golden membership is no longer active.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            self._db.execute(
                f"""UPDATE {INVESTMENT_EVENT_MEMBERS.full_name}
                    SET retired_at = CURRENT_TIMESTAMP, projection_changed_at = CURRENT_TIMESTAMP
                    WHERE membership_id = ?""",  # noqa: S608  # fixed TableRef, bound identity
                [membership_id],
            )
            self._emit_audit(
                action="investment_membership.retire",
                target=(*self._audit_target, membership_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(self._fetch_row(membership_id)),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
