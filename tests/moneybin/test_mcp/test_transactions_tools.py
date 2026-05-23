"""Tests for transactions_* MCP tools."""

from __future__ import annotations

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.transactions import (
    register_transactions_tools,
    transactions_matches_pending,
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
async def test_register_includes_review_status() -> None:
    """register_transactions_tools registers transactions_review."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "transactions_review" in names
    assert "transactions_recurring_list" not in names


@pytest.mark.unit
async def test_matches_pending_component_key_present(mcp_db: object) -> None:
    """Each pending dedup row carries a component_key field."""
    # Seed two edges forming a 3-copy cluster and one unrelated edge
    import json
    from datetime import UTC, datetime

    with get_database() as db:
        for match_id, stid_a, stype_a, stid_b, stype_b, acct in [
            ("mc_ab", "t1", "csv", "t2", "ofx", "ACC001"),
            ("mc_bc", "t2", "ofx", "t3", "tiller", "ACC001"),
            ("mc_zz", "x1", "csv", "x2", "ofx", "ACC002"),
        ]:
            db.execute(
                """
                INSERT INTO app.match_decisions (
                    match_id, source_transaction_id_a, source_type_a,
                    source_origin_a, source_transaction_id_b, source_type_b,
                    source_origin_b, account_id, confidence_score, match_signals,
                    match_type, match_tier, account_id_b, match_status,
                    match_reason, decided_by, decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,  # noqa: S608  # test input, not executing SQL
                [
                    match_id,
                    stid_a,
                    stype_a,
                    "origin_a",
                    stid_b,
                    stype_b,
                    "origin_b",
                    acct,
                    0.9,
                    json.dumps({}),
                    "dedup",
                    "3",
                    None,
                    "pending",
                    None,
                    "matcher",
                    datetime.now(tz=UTC).isoformat(),
                ],
            )

    result = (await transactions_matches_pending(match_type="dedup")).to_dict()
    matches = result["data"]["matches"]
    keys = {m["match_id"]: m["component_key"] for m in matches}

    # All rows carry component_key
    assert all("component_key" in m for m in matches)
    # Same cluster shares one key
    assert keys["mc_ab"] == keys["mc_bc"]
    # Different account is its own cluster
    assert keys["mc_zz"] != keys["mc_ab"]


@pytest.mark.unit
async def test_matches_pending_summary_hint_present(mcp_db: object) -> None:
    """actions[] includes the N-pending-edges-across-M-groups summary hint."""
    result = (await transactions_matches_pending()).to_dict()
    # When queue is empty the hint still appears
    assert any("pending dedup edge" in a for a in result["actions"])
