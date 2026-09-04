"""Adapters that turn Google Sheets service results into typed payload rows.

The CLI and the `gsheet_*` MCP tools project the same connection registry, the
same detection result, and the same per-connection pull outcome. Written once
here so a column added to `app.gsheet_connections` reaches both, and so the
drift hint an agent is handed says the same thing on both.

Pure: no I/O, no side-effects. `actions` stay with the caller — MCP names tools,
the CLI names commands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from moneybin.privacy.payloads.gsheet import (
    GsheetConnectionRow,
    GsheetConnectPayload,
    GsheetDetection,
    GsheetInitialPull,
    GsheetPullRow,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from moneybin.connectors.gsheet.adapters.base import GSheetConnection
    from moneybin.connectors.gsheet.connection_service import ConnectResult
    from moneybin.connectors.gsheet.pull_service import PullResult


def gsheet_connection_row(conn: GSheetConnection) -> GsheetConnectionRow:
    """Project one stored connection (mirrors ``GSheetConnection.to_dict()``)."""
    return GsheetConnectionRow(
        connection_id=conn.connection_id,
        spreadsheet_id=conn.spreadsheet_id,
        sheet_gid=conn.sheet_gid,
        sheet_name=conn.sheet_name,
        workbook_name=conn.workbook_name,
        adapter=conn.adapter,
        alias=conn.alias,
        account_id=conn.account_id,
        account_name=conn.account_name,
        status=conn.status,
        last_pull_at=conn.last_pull_at,
        last_success_at=conn.last_success_at,
        last_status_reason=conn.last_status_reason,
        consecutive_failure_count=conn.consecutive_failure_count,
    )


def gsheet_initial_pull(result: ConnectResult) -> GsheetInitialPull | None:
    """Project the initial-pull outcome of a connect or reconnect.

    Rows on success; status plus reason on a pull that ran and failed; None only
    when no pull ran at all (``--no-initial-pull``). Collapsing the middle case
    into None would make a drift-detected connect indistinguishable from one the
    caller asked not to pull.
    """
    if result.initial_pull is not None:
        return GsheetInitialPull(
            status=result.initial_pull_status,
            rows_inserted=result.initial_pull.rows_inserted,
            rows_upserted=result.initial_pull.rows_upserted,
            rows_soft_deleted=result.initial_pull.rows_soft_deleted,
        )
    if result.initial_pull_status is not None:
        return GsheetInitialPull(
            status=result.initial_pull_status,
            error=result.initial_pull_error,
        )
    return None


def gsheet_connect_payload(result: ConnectResult) -> GsheetConnectPayload:
    """Project one connect or reconnect result."""
    return GsheetConnectPayload(
        connection=gsheet_connection_row(result.connection),
        detection=GsheetDetection(
            confidence=result.detection.confidence,
            column_mapping=result.detection.column_mapping,
            detection_notes=result.detection.notes,
        ),
        initial_pull=gsheet_initial_pull(result),
    )


def gsheet_pull_rows(results: Sequence[PullResult]) -> list[GsheetPullRow]:
    """Project each per-connection pull outcome.

    Row counts fall back to 0 when the pull produced no load result: the
    connection's ``status`` is what says why, and a null count there would read
    as "unknown" for an outcome that is fully known.
    """
    return [
        GsheetPullRow(
            connection_id=r.connection_id,
            status=r.status,
            rows_inserted=r.load_result.rows_inserted if r.load_result else 0,
            rows_upserted=r.load_result.rows_upserted if r.load_result else 0,
            rows_soft_deleted=r.load_result.rows_soft_deleted if r.load_result else 0,
            drift_reason=r.drift_reason,
            error_message=r.error_message,
        )
        for r in results
    ]
