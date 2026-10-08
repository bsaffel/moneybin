"""Build the system-status sections both surfaces emit.

``system_status(sections=["overview", "exports"])`` and ``moneybin system
status --output json`` return the same payload, so the overview and exports
sections are assembled here once. Lock handling and ``actions`` stay with the
caller: MCP degrades under a held lock and names tools, the CLI names commands.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from moneybin.build_info import get_build_info
from moneybin.db_lock import live_writer
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
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo
from moneybin.utils.db_processes import describe_process, find_blocking_processes

if TYPE_CHECKING:
    from moneybin.exports.service import ExportReadinessStatus
    from moneybin.services.system_service import SystemStatus

_HEALTHY_STATUSES = frozenset({"healthy"})
_DISCONNECTED_STATUSES = frozenset({"disconnected"})


def gsheet_info(db: Any) -> SystemStatusGsheetInfo:
    """Build the gsheet block: counts by status + per-attention rows.

    Returns the zero-connections shape when the table is empty or absent;
    healthy and disconnected connections are excluded from ``needs_attention``.
    """
    try:
        connections = GSheetConnectionsRepo(db).list_all()
    except duckdb.CatalogException:
        # Table absent on bare DBs before init_schemas — report empty rather
        # than error. Narrowed from a blanket except so real DB/query problems
        # (corruption, permission, a broken schema) surface instead of being
        # masked as total_connections=0 and suppressing recovery hints.
        return SystemStatusGsheetInfo(
            total_connections=0, by_status={}, needs_attention=[]
        )

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


def database_connections_info(db_path: Path) -> SystemStatusDatabaseConnectionsInfo:
    """Build the typed database_connections payload from the lock + lsof view."""
    block = _database_connections_block(db_path)
    return SystemStatusDatabaseConnectionsInfo(
        writers=[SystemStatusWriter(**w) for w in block["writers"]],
        readers=[SystemStatusReader(**r) for r in block["readers"]],
    )


def _database_connections_block(db_path: Path) -> dict[str, Any]:
    """Merge file-lock writer metadata with lsof-derived reader enumeration.

    Returns the empty-shape ``{"writers": [], "readers": []}`` when neither
    source reports anything. A writer is reported only when a process actually
    holds the file lock (``live_writer``) — the persisted metadata file
    alone is not enough, since it outlives the holder. Tolerates a corrupted
    lock file by treating it as no-writer-info — the lock-file payload is
    best-effort observability, not a correctness contract. The writer's pid is
    filtered out of the reader list to avoid double-listing the writer process.
    """
    writers: list[dict[str, Any]] = []
    writer_pid: int | None = None
    # live_writer resolves db_path itself for the lock file; resolving here
    # too keeps the lsof reader scan on the same inode for a symlinked path.
    resolved = db_path.resolve()
    metadata = live_writer(db_path)
    if metadata is not None:
        writer_pid = metadata["pid"]
        writers.append(metadata)

    # A writer can time out and release the advisory lock while a DuckDB reader
    # still blocks the next write, so recovery diagnostics must enumerate
    # readers independently of the current writer-lock state.
    readers: list[dict[str, Any]] = []
    processes = find_blocking_processes(resolved)
    for proc in processes:
        if writer_pid is not None and proc["pid"] == writer_pid:
            continue  # Avoid double-listing the writer as a reader
        readers.append({
            "pid": int(proc["pid"]),
            "command": describe_process(
                str(proc.get("cmdline") or proc.get("command", ""))
            ),
        })

    return {"writers": writers, "readers": readers}


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
