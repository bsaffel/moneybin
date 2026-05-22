"""Envelope shape tests for the gsheet block on ``system_status``."""

from __future__ import annotations

import json
from typing import Any

import pytest

from moneybin.database import get_database
from moneybin.mcp.tools.system import system_status


def _insert_connection(
    *,
    connection_id: str,
    status: str,
    adapter: str = "transactions",
    alias: str | None = None,
    workbook: str = "Test Workbook",
    sheet: str = "Sheet1",
    drift_reason: str | None = None,
    sheet_gid: int = 0,
    spreadsheet_id: str | None = None,
) -> None:
    """Insert a connection row directly (bypasses audited repo for test speed)."""
    spreadsheet_id = spreadsheet_id or f"ss_{connection_id}"
    with get_database() as db:
        db.execute(
            """
            INSERT INTO app.gsheet_connections (
                connection_id, spreadsheet_id, sheet_gid, sheet_name,
                workbook_name, adapter, account_id, account_name,
                column_mapping, header_signature, skip_rows, alias,
                status, last_status_reason
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 0, ?, ?, ?)
            """,
            [
                connection_id,
                spreadsheet_id,
                sheet_gid,
                sheet,
                workbook,
                adapter,
                json.dumps({"Date": "transaction_date"}),
                json.dumps(["Date"]),
                alias,
                status,
                drift_reason,
            ],
        )


@pytest.mark.unit
async def test_system_status_gsheet_block_present_when_no_connections(
    mcp_db: object,
) -> None:
    """With zero connections the gsheet block exists in zero-shape."""
    env = await system_status()
    data = env.to_dict()["data"]
    assert "gsheet" in data
    block = data["gsheet"]
    assert block["total_connections"] == 0
    assert block["by_status"] == {}
    assert block["needs_attention"] == []


@pytest.mark.unit
async def test_system_status_groups_connections_by_status(mcp_db: object) -> None:
    """Mixed-status connections produce correct by_status counts."""
    _insert_connection(connection_id="c_h", status="healthy")
    _insert_connection(
        connection_id="c_d", status="drift_detected", drift_reason="header_added"
    )
    _insert_connection(connection_id="c_a", status="auth_expired", sheet_gid=1)
    env = await system_status()
    block = env.to_dict()["data"]["gsheet"]
    assert block["total_connections"] == 3
    assert block["by_status"] == {
        "healthy": 1,
        "drift_detected": 1,
        "auth_expired": 1,
    }


@pytest.mark.unit
async def test_system_status_needs_attention_omits_healthy(mcp_db: object) -> None:
    """Healthy connections never appear in needs_attention."""
    _insert_connection(connection_id="c_h", status="healthy")
    _insert_connection(
        connection_id="c_d", status="drift_detected", drift_reason="reordered"
    )
    env = await system_status()
    block = env.to_dict()["data"]["gsheet"]
    ids = [row["connection_id"] for row in block["needs_attention"]]
    assert "c_h" not in ids
    assert "c_d" in ids


@pytest.mark.unit
async def test_system_status_needs_attention_omits_disconnected(
    mcp_db: object,
) -> None:
    """Soft-disconnected connections are not flagged as needing attention."""
    _insert_connection(connection_id="c_off", status="disconnected")
    env = await system_status()
    block = env.to_dict()["data"]["gsheet"]
    assert block["by_status"] == {"disconnected": 1}
    assert block["needs_attention"] == []


@pytest.mark.unit
async def test_system_status_drift_action_hint(mcp_db: object) -> None:
    """drift_detected connections surface a gsheet_reconnect actions[] hint."""
    _insert_connection(
        connection_id="c_drift",
        status="drift_detected",
        drift_reason="header_added",
    )
    env = await system_status()
    actions = env.to_dict()["actions"]
    assert any("gsheet_reconnect" in a and "c_drift" in a for a in actions), actions


@pytest.mark.unit
async def test_system_status_auth_expired_action_hint(mcp_db: object) -> None:
    """auth_expired connections surface a CLI re-auth message."""
    _insert_connection(connection_id="c_auth", status="auth_expired")
    env = await system_status()
    actions = env.to_dict()["actions"]
    assert any("moneybin gsheet auth" in a for a in actions), actions


@pytest.mark.unit
async def test_system_status_needs_attention_row_shape(mcp_db: object) -> None:
    """needs_attention rows carry workbook, sheet, status, and reason."""
    _insert_connection(
        connection_id="c_d",
        status="drift_detected",
        workbook="Budget 2025",
        sheet="Transactions",
        drift_reason="header_added: Notes",
    )
    env = await system_status()
    row: dict[str, Any] = env.to_dict()["data"]["gsheet"]["needs_attention"][0]
    assert row["connection_id"] == "c_d"
    assert row["workbook_name"] == "Budget 2025"
    assert row["sheet_name"] == "Transactions"
    assert row["status"] == "drift_detected"
    assert row["reason"] == "header_added: Notes"
