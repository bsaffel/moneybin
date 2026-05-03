"""Tests for transactions_* MCP tools."""

from __future__ import annotations

import asyncio

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.transactions import (
    register_transactions_tools,
    transactions_review_status,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.unit
def test_review_status_returns_envelope(mcp_db: object) -> None:
    """transactions_review_status returns a valid ResponseEnvelope."""
    parsed = transactions_review_status().to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
def test_review_status_data_shape(mcp_db: object) -> None:
    """Data dict carries matches_pending, categorize_pending, and total."""
    data = transactions_review_status().to_dict()["data"]
    assert "matches_pending" in data
    assert "categorize_pending" in data
    assert "total" in data
    assert isinstance(data["matches_pending"], int)
    assert isinstance(data["categorize_pending"], int)
    assert data["total"] == data["matches_pending"] + data["categorize_pending"]


@pytest.mark.unit
def test_review_status_actions_non_empty(mcp_db: object) -> None:
    """Tool provides next-step action hints."""
    parsed = transactions_review_status().to_dict()
    assert len(parsed["actions"]) >= 1


@pytest.mark.unit
def test_register_includes_review_status() -> None:
    """register_transactions_tools registers transactions_review_status."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in asyncio.run(srv._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "transactions_review_status" in names
    assert "transactions_search" in names
    assert "transactions_recurring_list" in names
