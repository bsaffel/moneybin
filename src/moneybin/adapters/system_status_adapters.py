"""Build the system-status sections both surfaces emit.

``system_status(sections=["overview", "exports"])`` and ``moneybin system
status --output json`` return the same payload, so the overview and exports
sections are projected here once from ``SystemService`` reads. Lock handling
and ``actions`` stay with the caller: MCP degrades under a held lock and names
tools; the CLI reports the lock as an error and emits no actions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moneybin.build_info import get_build_info
from moneybin.privacy.payloads.system import (
    ExportsStatus,
    SchemaDriftTable,
    SystemStatusAccountLinksInfo,
    SystemStatusAccountsInfo,
    SystemStatusBuildInfo,
    SystemStatusCategorizationInfo,
    SystemStatusDatabaseConnectionsInfo,
    SystemStatusExportDestination,
    SystemStatusGsheetInfo,
    SystemStatusGsheetRow,
    SystemStatusMatchesInfo,
    SystemStatusMerchantLinksInfo,
    SystemStatusPayload,
    SystemStatusReader,
    SystemStatusSchemaDrift,
    SystemStatusSecurityLinksInfo,
    SystemStatusTransactionsInfo,
    SystemStatusTransformsInfo,
    SystemStatusWriter,
)

if TYPE_CHECKING:
    from moneybin.exports.service import ExportReadinessStatus
    from moneybin.services.system_service import SystemStatus

_HEALTHY_STATUSES = frozenset({"healthy"})
_DISCONNECTED_STATUSES = frozenset({"disconnected"})


def gsheet_info(connections: list[dict[str, Any]]) -> SystemStatusGsheetInfo:
    """Project connection rows into counts by status + per-attention rows.

    Healthy and disconnected connections are excluded from ``needs_attention``.
    """
    by_status: dict[str, int] = {}
    needs_attention: list[SystemStatusGsheetRow] = []
    for connection in connections:
        status = connection["status"]
        by_status[status] = by_status.get(status, 0) + 1
        if status in _HEALTHY_STATUSES or status in _DISCONNECTED_STATUSES:
            continue
        needs_attention.append(
            SystemStatusGsheetRow(
                connection_id=connection["connection_id"],
                workbook_name=connection["workbook_name"],
                sheet_name=connection["sheet_name"],
                status=status,
                reason=connection["last_status_reason"],
            )
        )

    return SystemStatusGsheetInfo(
        total_connections=len(connections),
        by_status=by_status,
        needs_attention=needs_attention,
    )


def database_connections_info(
    block: dict[str, list[dict[str, Any]]],
) -> SystemStatusDatabaseConnectionsInfo:
    """Project ``system_service.database_connections`` into its typed payload."""
    return SystemStatusDatabaseConnectionsInfo(
        writers=[SystemStatusWriter(**w) for w in block["writers"]],
        readers=[SystemStatusReader(**r) for r in block["readers"]],
    )


def build_info() -> SystemStatusBuildInfo:
    """Name the build this process is running.

    Derivation lives in ``moneybin.build_info`` — export manifest provenance
    (``exports/service.py``) reuses the same source rather than a second one.
    """
    build = get_build_info()
    return SystemStatusBuildInfo(version=build.version, revision=build.revision)


def overview_payload(
    status: SystemStatus,
    gsheet: SystemStatusGsheetInfo,
    db_connections: SystemStatusDatabaseConnectionsInfo,
) -> SystemStatusPayload:
    """Project one service status read into the overview section's payload."""
    min_date, max_date = status.transactions_date_range
    schema_drift: SystemStatusSchemaDrift | None = None
    if status.schema_drift:
        schema_drift = SystemStatusSchemaDrift(
            tables=[
                SchemaDriftTable(name=table, missing_columns=cols)
                for table, cols in sorted(status.schema_drift.items())
            ],
            remediation="moneybin refresh",
        )
    return SystemStatusPayload(
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
        account_links=SystemStatusAccountLinksInfo(
            pending_review=status.account_links_pending
        ),
        merchant_links=SystemStatusMerchantLinksInfo(
            pending_review=status.merchant_links_pending
        ),
        security_links=SystemStatusSecurityLinksInfo(
            pending_review=status.security_links_pending
        ),
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
            missing_models=list(status.transforms_missing_models),
        ),
        schema_drift=schema_drift,
        gsheet=gsheet,
        database_connections=db_connections,
        build=build_info(),
    )


def locked_overview_payload(
    db_connections: SystemStatusDatabaseConnectionsInfo,
) -> SystemStatusPayload:
    """Zero-filled overview for when a writer holds the database lock.

    Only ``database_connections`` and ``build`` are real: both need no DB
    connection, and the holder is exactly what a lock-recovery caller needs.
    """
    return SystemStatusPayload(
        accounts=SystemStatusAccountsInfo(count=0),
        transactions=SystemStatusTransactionsInfo(
            count=0, date_range=[None, None], last_import_at=None
        ),
        matches=SystemStatusMatchesInfo(pending_review=0),
        account_links=SystemStatusAccountLinksInfo(pending_review=0),
        merchant_links=SystemStatusMerchantLinksInfo(pending_review=0),
        security_links=SystemStatusSecurityLinksInfo(pending_review=0),
        categorization=SystemStatusCategorizationInfo(uncategorized=0),
        transforms=SystemStatusTransformsInfo(
            pending=False, last_apply_at=None, missing_models=[]
        ),
        schema_drift=None,
        gsheet=SystemStatusGsheetInfo(
            total_connections=0, by_status={}, needs_attention=[]
        ),
        database_connections=db_connections,
        build=build_info(),
    )


def exports_status(readiness: ExportReadinessStatus) -> ExportsStatus:
    """Project export readiness into the exports section."""
    return ExportsStatus(
        destinations=[
            SystemStatusExportDestination(
                name=item.name,
                kind=item.kind,
                ready=item.ready,
                write_capable=item.write_capable,
                reasons=list(item.reasons),
            )
            for item in readiness.destinations
        ]
    )
