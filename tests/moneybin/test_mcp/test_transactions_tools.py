"""Tests for transactions_* MCP tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.transactions import (
    register_transactions_tools,
    transactions_matches_run,
    transactions_review,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.unit
async def test_review_status_returns_envelope(mcp_db: object) -> None:
    """transactions_review returns a valid ResponseEnvelope."""
    parsed = (await transactions_review()).to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_review_status_data_shape(mcp_db: object) -> None:
    """Data dict carries matches_pending, categorize_pending, and total."""
    data = (await transactions_review()).to_dict()["data"]
    assert "matches_pending" in data
    assert "categorize_pending" in data
    assert "total" in data
    assert isinstance(data["matches_pending"], int)
    assert isinstance(data["categorize_pending"], int)
    assert data["total"] == data["matches_pending"] + data["categorize_pending"]


@pytest.mark.unit
async def test_review_status_actions_non_empty(mcp_db: object) -> None:
    """Tool provides next-step action hints."""
    parsed = (await transactions_review()).to_dict()
    assert len(parsed["actions"]) >= 1


@pytest.mark.unit
@patch("moneybin.mcp.tools.transactions.get_database")
@patch("moneybin.services.matching_service.MatchingService.run")
async def test_matches_run_threads_mcp_actor(
    mock_run: MagicMock, mock_get_db: MagicMock
) -> None:
    """transactions_matches_run audits its writes as actor="mcp", not "system"."""
    from moneybin.matching.engine import MatchResult

    mock_run.return_value = MatchResult(auto_merged=2, pending_review=1)

    await transactions_matches_run()

    mock_run.assert_called_once_with(actor="mcp")


@pytest.mark.unit
async def test_register_includes_review_status() -> None:
    """register_transactions_tools registers transactions_review."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "transactions_review" in names
    assert "transactions_recurring_list" not in names
