"""system_* tools — data status meta-view."""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.system import (
    InvariantResultPayload,
    SchemaDriftTable,
    SystemDoctorPayload,
    SystemStatusAccountsInfo,
    SystemStatusCategorizationInfo,
    SystemStatusMatchesInfo,
    SystemStatusPayload,
    SystemStatusSchemaDrift,
    SystemStatusTransactionsInfo,
    SystemStatusTransformsInfo,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@mcp_tool(sensitivity="low")
def system_status() -> ResponseEnvelope[SystemStatusPayload]:
    """Return data inventory, pending review queue counts, and transforms freshness.

    Use this tool to understand what data exists in MoneyBin and what
    needs user attention before suggesting any analytical query.
    """
    from moneybin.database import get_database
    from moneybin.services.system_service import SystemService

    with get_database(read_only=True) as db:
        status = SystemService(db).status()

    min_date, max_date = status.transactions_date_range

    schema_drift_payload: SystemStatusSchemaDrift | None = None
    actions = [
        "Use transactions_review for per-queue review counts",
        "Use reports_spending for a monthly spending trend snapshot",
    ]
    if status.schema_drift:
        schema_drift_payload = SystemStatusSchemaDrift(
            tables=[
                SchemaDriftTable(name=table, missing_columns=cols)
                for table, cols in sorted(status.schema_drift.items())
            ],
            remediation="moneybin refresh",
        )
        actions.append(
            "Run refresh_run to rebuild stale models — "
            f"{len(status.schema_drift)} core table(s) drifted"
        )

    if status.transforms_pending:
        actions.append(
            "Run refresh_run to refresh derived tables "
            "(raw imports are newer than the last refresh)"
        )

    return build_envelope(
        data=SystemStatusPayload(
            accounts=SystemStatusAccountsInfo(count=status.accounts_count),
            transactions=SystemStatusTransactionsInfo(
                count=status.transactions_count,
                date_range=[
                    min_date.isoformat() if min_date else None,
                    max_date.isoformat() if max_date else None,
                ],
                last_import_at=(
                    status.last_import_at.isoformat() if status.last_import_at else None
                ),
            ),
            matches=SystemStatusMatchesInfo(pending_review=status.matches_pending),
            categorization=SystemStatusCategorizationInfo(
                uncategorized=status.categorize_pending
            ),
            transforms=SystemStatusTransformsInfo(
                pending=status.transforms_pending,
                last_apply_at=(
                    status.transforms_last_apply_at.isoformat()
                    if status.transforms_last_apply_at
                    else None
                ),
            ),
            schema_drift=schema_drift_payload,
        ),
        sensitivity="low",
        actions=actions,
    )


@mcp_tool(sensitivity="low", read_only=False)
def system_doctor() -> ResponseEnvelope[SystemDoctorPayload]:
    """Run pipeline integrity checks across all SQLMesh named audits.

    Returns pass/fail/warn per invariant plus a transaction count.
    May write SQLMesh state tables on first Context init. Call before
    relying on analytical results to confirm the pipeline is self-consistent.
    """
    from moneybin.database import get_database
    from moneybin.services.doctor_service import DoctorService

    with get_database() as db:
        report = DoctorService(db).run_all(verbose=False)

    actions: list[str] = []
    if report.failing > 0:
        actions.append(
            "Run moneybin system doctor --verbose for affected transaction IDs"
        )

    return build_envelope(
        data=SystemDoctorPayload(
            passing=report.passing,
            failing=report.failing,
            warning=report.warning,
            skipped=report.skipped,
            transaction_count=report.transaction_count,
            invariants=[
                InvariantResultPayload(
                    name=r.name,
                    status=r.status,
                    detail=r.detail,
                    affected_ids=r.affected_ids,
                )
                for r in report.invariants
            ],
        ),
        sensitivity="low",
        actions=actions,
    )


def register_system_tools(mcp: FastMCP) -> None:
    """Register all system namespace tools with the FastMCP server."""
    register(
        mcp,
        system_status,
        "system_status",
        "Return data inventory (accounts, transactions, freshness), pending review queue counts, "
        "and a transforms-pending signal indicating whether derived tables need a refresh. "
        "Call this first to orient before suggesting analytical queries.",
    )
    register(
        mcp,
        system_doctor,
        "system_doctor",
        "Run pipeline integrity checks across all SQLMesh named audits. "
        "Returns pass/fail/warn per invariant plus transaction count. "
        "May write SQLMesh state tables on first call. Call before relying on analytical results to confirm the pipeline is self-consistent.",
    )
