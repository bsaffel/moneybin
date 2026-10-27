"""Audited selected-field provenance for Golden investment Matches."""

from dataclasses import asdict
from typing import Any
from uuid import uuid4

from moneybin.investments.event_membership import FieldResolution
from moneybin.repositories.base import BaseRepo
from moneybin.tables import INVESTMENT_MATCH_FIELD_RESOLUTIONS

_COLUMNS = (
    "resolution_id",
    "decision_id",
    "investment_transaction_id",
    "field_name",
    "source_type",
    "source_origin",
    "native_reference",
    "observation_version",
    "created_at",
    "retired_at",
)


class InvestmentMatchFieldResolutionsRepo(BaseRepo):
    """Publish each explicit choice with the exact selected observation revision."""

    repository = "investment_match_field_resolutions"
    table_ref = INVESTMENT_MATCH_FIELD_RESOLUTIONS
    pk_columns = ("resolution_id",)

    def _fetch_row(self, resolution_id: str) -> dict[str, Any] | None:
        return self._fetch_one(self.table_ref, _COLUMNS, "resolution_id", resolution_id)

    def active_resolutions(self) -> list[dict[str, Any]]:
        """Read active explicit choices in stable order."""
        cursor = self._db.execute(
            f"""SELECT * FROM {INVESTMENT_MATCH_FIELD_RESOLUTIONS.full_name}
                WHERE retired_at IS NULL ORDER BY created_at, resolution_id"""  # noqa: S608  # fixed TableRef
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def record(
        self,
        resolution: FieldResolution,
        *,
        actor: str,
        in_outer_txn: bool = False,
        parent_audit_id: str | None = None,
    ) -> str:
        """Record a validated choice within the caller's atomic membership operation."""
        resolution_id = uuid4().hex[:12]
        columns = _COLUMNS[:8]
        values = asdict(resolution)
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""INSERT INTO {INVESTMENT_MATCH_FIELD_RESOLUTIONS.full_name}
                    ({", ".join(f'"{c}"' for c in columns)})
                    VALUES ({", ".join("?" for _ in columns)})""",  # noqa: S608  # fixed columns, bound values
                [resolution_id, *(values[column] for column in columns[1:])],
            )
            self._emit_audit(
                action="investment_resolution.select",
                target=(*self._audit_target, resolution_id),
                before=None,
                after=self._serialize_for_audit(self._fetch_row(resolution_id)),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        return resolution_id
