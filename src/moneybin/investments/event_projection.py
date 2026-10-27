"""Golden fields derive from exact source revisions and immutable bindings."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.event_choices import default_selection
from moneybin.tables import INVESTMENT_EVENT_LEGS, PLAID_INVESTMENT_REVISIONS

SourceRevision = tuple[str, str, str, str]


def source_revision(row: Mapping[str, Any]) -> SourceRevision:
    """Exact revision identity shared by membership and retained evidence."""
    return (
        row["source_type"],
        row["source_origin"],
        row["native_reference"],
        row["observation_version"],
    )


def read_rows(
    db: Database, query: str, parameters: list[Any] | None = None
) -> list[dict[str, Any]]:
    """Keep DuckDB values typed while converting a projection to named rows."""
    cursor = db.execute(query, parameters or [])
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def load_member_evidence(db: Database) -> dict[SourceRevision, dict[str, Any]]:
    """Read normalized immutable revisions, never an App observation snapshot."""
    manual = read_rows(
        db,
        f"""
        SELECT l.*, NULL::VARCHAR AS provider_type, NULL::VARCHAR AS provider_subtype
        FROM {INVESTMENT_EVENT_LEGS.full_name} AS l WHERE source_type = 'manual'
    """,  # noqa: S608  # fixed TableRef
    )
    plaid = read_rows(
        db,
        f"""
        SELECT p.*, investment_transaction_id AS native_reference
        FROM {PLAID_INVESTMENT_REVISIONS.full_name} AS p
    """,  # noqa: S608  # fixed TableRef
    )
    return {source_revision(row): row for row in [*manual, *plaid]}


def project_members(
    members: Sequence[Mapping[str, Any]],
    evidence: Mapping[SourceRevision, Mapping[str, Any]],
    resolutions: Sequence[Mapping[str, Any]] = (),
) -> tuple[dict[str, Any], ...]:
    """Collapse matching semantic legs without re-resolving canonical dependencies."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for member in members:
        if member.get("retired_at") is not None:
            continue
        revision = source_revision(member)
        observed = evidence.get(revision)
        if observed is None:
            raise UserError(
                "Exact investment membership evidence is unavailable.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        groups[member["investment_transaction_id"]].append({
            **observed,
            **{
                key: member[key]
                for key in (
                    "source_type",
                    "source_origin",
                    "native_reference",
                    "observation_version",
                )
            },
            "account_id": member["bound_account_id"],
            "security_id": member["bound_security_id"],
            "currency_code": member["bound_effective_currency"],
            "leg_role": member["semantic_leg_role"],
            "event_group_id": member["event_group_id"],
            "investment_transaction_id": member["investment_transaction_id"],
            "created_at": member["created_at"],
            "updated_at": member["projection_changed_at"],
        })
    result: list[dict[str, Any]] = []
    for leg_id, rows in sorted(groups.items()):
        selected = default_selection(rows)[0]
        for resolution in resolutions:
            if resolution["investment_transaction_id"] != leg_id:
                continue
            observed = evidence.get(source_revision(resolution))
            if observed is None:
                raise UserError(
                    "Selected investment field evidence is unavailable.",
                    code=error_codes.RECOVERY_NO_PATH,
                )
            selected[resolution["field_name"]] = observed[resolution["field_name"]]
            if resolution["field_name"] == "trade_date":
                selected["trade_date_basis"] = observed["trade_date_basis"]
        selected["created_at"] = min(row["created_at"] for row in rows)
        selected["updated_at"] = max(row["updated_at"] for row in rows)
        result.append(selected)
    return tuple(result)
