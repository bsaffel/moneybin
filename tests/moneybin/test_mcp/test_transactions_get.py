"""Tests for the transactions_get MCP tool."""

from __future__ import annotations

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.transactions import (
    register_transactions_tools,
    transactions_get,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.unit
async def test_transactions_get_returns_envelope(mcp_db: object) -> None:
    """transactions_get returns a valid ResponseEnvelope."""
    result = await transactions_get()
    d = result.to_dict()
    assert "summary" in d
    assert "data" in d
    assert "actions" in d
    assert d["summary"]["sensitivity"] == "medium"


@pytest.mark.unit
async def test_transactions_get_data_is_list(mcp_db: object) -> None:
    """Data field is a list (may be empty on fresh DB)."""
    result = await transactions_get()
    d = result.to_dict()
    assert isinstance(d["data"], list)


@pytest.mark.unit
async def test_transactions_get_no_cursor_when_empty(mcp_db: object) -> None:
    """next_cursor absent when all results fit in one page."""
    result = await transactions_get(limit=50)
    d = result.to_dict()
    # Fresh MCP DB has no transactions — no cursor expected
    assert "next_cursor" not in d or d.get("next_cursor") is None


@pytest.mark.unit
async def test_register_includes_transactions_get() -> None:
    """register_transactions_tools registers transactions_get."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "transactions_get" in names
    assert "transactions_search" not in names
    assert "transactions_review" in names
    assert "transactions_recurring_list" in names
