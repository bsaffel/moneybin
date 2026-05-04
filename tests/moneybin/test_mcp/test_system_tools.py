"""Tests for system_* MCP tools."""

from __future__ import annotations

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.system import register_system_tools, system_status

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.unit
async def test_system_status_returns_response_envelope(mcp_db: object) -> None:
    """system_status returns a valid ResponseEnvelope."""
    result = await system_status()
    parsed = result.to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_system_status_data_keys(mcp_db: object) -> None:
    """system_status data dict has all required domain keys."""
    result = await system_status()
    parsed = result.to_dict()
    data = parsed["data"]
    assert "accounts" in data
    assert "transactions" in data
    assert "matches" in data
    assert "categorization" in data


@pytest.mark.unit
async def test_system_status_accounts_count(mcp_db: object) -> None:
    """Accounts count reflects the mcp_db fixture's 2 accounts."""
    result = await system_status()
    parsed = result.to_dict()
    assert parsed["data"]["accounts"]["count"] == 2


@pytest.mark.unit
async def test_system_status_transactions_empty(mcp_db: object) -> None:
    """Transactions count is 0 when no transactions are inserted."""
    result = await system_status()
    parsed = result.to_dict()
    txn = parsed["data"]["transactions"]
    assert txn["count"] == 0
    assert txn["date_range"] == [None, None]
    assert txn["last_import_at"] is None


@pytest.mark.unit
async def test_system_status_queue_counts_are_integers(mcp_db: object) -> None:
    """matches.pending_review and categorization.uncategorized are integers."""
    result = await system_status()
    parsed = result.to_dict()
    assert isinstance(parsed["data"]["matches"]["pending_review"], int)
    assert isinstance(parsed["data"]["categorization"]["uncategorized"], int)


@pytest.mark.unit
async def test_system_status_actions_non_empty(mcp_db: object) -> None:
    """system_status provides at least one action hint."""
    result = await system_status()
    parsed = result.to_dict()
    assert len(parsed["actions"]) >= 1


@pytest.mark.unit
async def test_register_system_tools() -> None:
    """register_system_tools registers system_status with a FastMCP server."""
    srv = FastMCP("test")
    register_system_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "system_status" in names
