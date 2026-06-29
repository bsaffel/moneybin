"""Tests for the top-level `review` MCP tool and deprecated `transactions_review` alias."""

from __future__ import annotations

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.transactions import (
    register_transactions_tools,
    review,
    transactions_review,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


@pytest.mark.unit
async def test_review_returns_envelope(mcp_db: object) -> None:
    """`review` returns a valid ResponseEnvelope."""
    parsed = (await review()).to_dict()
    assert "summary" in parsed
    assert "data" in parsed
    assert "actions" in parsed
    assert parsed["summary"]["sensitivity"] == "low"


@pytest.mark.unit
async def test_review_data_shape(mcp_db: object) -> None:
    """Data carries the four queue counts and a total equal to their sum."""
    data = (await review()).to_dict()["data"]
    assert "matches_pending" in data
    assert "categorize_pending" in data
    assert "account_links_pending" in data
    assert "merchant_links_pending" in data
    assert "total" in data
    assert isinstance(data["account_links_pending"], int)
    assert isinstance(data["merchant_links_pending"], int)
    assert data["total"] == (
        data["matches_pending"]
        + data["categorize_pending"]
        + data["account_links_pending"]
        + data["merchant_links_pending"]
    )


@pytest.mark.unit
async def test_review_actions_mention_drill_down_queues(mcp_db: object) -> None:
    """actions[] guides the agent to the three queues that have drill-down tools.

    Merchant-links is counted in the review sweep but has no dedicated
    drill-down tool yet (arrives in a later increment), so it has no action.
    """
    parsed = (await review()).to_dict()
    actions_text = " ".join(parsed["actions"])
    assert "transactions_matches_pending" in actions_text
    assert "transactions_categorize_pending" in actions_text
    assert "accounts_links_pending" in actions_text


@pytest.mark.unit
async def test_transactions_review_alias_returns_same_shape(mcp_db: object) -> None:
    """`transactions_review` is a deprecated alias with the same data shape."""
    data = (await transactions_review()).to_dict()["data"]
    assert "matches_pending" in data
    assert "categorize_pending" in data
    assert "account_links_pending" in data
    assert "merchant_links_pending" in data
    assert "total" in data


@pytest.mark.unit
async def test_register_includes_review_and_alias() -> None:
    """register_transactions_tools registers both `review` and `transactions_review`."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "review" in names
    assert "transactions_review" in names


@pytest.mark.unit
async def test_transactions_review_description_starts_with_deprecated() -> None:
    """`transactions_review` description must start with 'DEPRECATED:'."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    tools = {t.name: t for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    desc = tools["transactions_review"].description or ""
    assert desc.startswith("DEPRECATED:"), (
        f"transactions_review description must start with 'DEPRECATED:' but got: {desc[:80]!r}"
    )
