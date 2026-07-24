"""Unit tests for the refresh_run MCP tool."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.mcp.adapters.refresh_adapters import REFRESH_CATEGORIZE_FOLLOWUP_HINT
from moneybin.mcp.tools.refresh import refresh_run, register_refresh_tools
from moneybin.services.refresh import RefreshResult


@pytest.mark.unit
async def test_refresh_run_is_registered() -> None:
    mcp = FastMCP("test")
    register_refresh_tools(mcp)
    tools = await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    names = {tool.name for tool in tools}
    assert names == {"refresh_run"}
    description = next(tool.description for tool in tools if tool.name == "refresh_run")
    assert description is not None
    assert "canonical order" in description
    assert "No revert path" in description


@pytest.mark.unit
async def test_refresh_run_returns_envelope_on_success() -> None:
    fake_result = RefreshResult(applied=True, duration_seconds=4.2, error=None)
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run()
    assert envelope.data.applied is True
    assert envelope.data.duration_seconds == 4.2
    assert envelope.data.error is None


@pytest.mark.unit
async def test_refresh_run_surfaces_apply_error() -> None:
    fake_result = RefreshResult(applied=False, duration_seconds=1.1, error="model boom")
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run()
    assert envelope.data.applied is False
    assert envelope.data.error == "model boom"
    # The recovery hint must reference tools that are actually registered.
    # moneybin_discover was retired in #161; any reference to it would point
    # an agent at an unknown tool exactly when guidance matters most.
    assert envelope.actions, "apply failure must emit a recovery hint"
    assert all("moneybin_discover" not in a for a in envelope.actions)
    assert any("moneybin transform plan" in a for a in envelope.actions)


@pytest.mark.unit
async def test_refresh_run_apply_failure_skips_identity_review_hints() -> None:
    """Apply failure stops identity, so full-refresh actions do not advertise reviews."""
    fake_result = RefreshResult(
        applied=False,
        duration_seconds=1.1,
        error="model boom",
    )
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run()

    actions = " ".join(envelope.actions)
    assert 'reviews(kind="account_links")' not in actions
    assert 'reviews(kind="merchant_links")' not in actions


@pytest.mark.unit
async def test_refresh_run_steps_pass_through() -> None:
    """``refresh_run(steps=[...])`` forwards the list verbatim to refresh()."""
    fake_result = RefreshResult(applied=True, duration_seconds=1.5, error=None)
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result) as svc,
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run(steps=["transform"])
    assert envelope.data.applied is True
    _db_arg, kwargs = svc.call_args[0], svc.call_args[1]
    assert kwargs == {"steps": ["transform"]}


@pytest.mark.unit
async def test_refresh_run_steps_none_calls_service_with_none() -> None:
    """No ``steps`` argument means service receives ``steps=None`` (default cascade)."""
    fake_result = RefreshResult(applied=True, duration_seconds=4.2, error=None)
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result) as svc,
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        await refresh_run()
    assert svc.call_args.kwargs == {"steps": None}


@pytest.mark.unit
async def test_refresh_run_identity_errors_are_typed_and_point_to_reviews() -> None:
    """Identity backfill exposes only failed domains and review next steps."""
    fake_result = RefreshResult(
        applied=False,
        duration_seconds=None,
        identity_errors=(),
    )
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run(steps=["identity"])

    assert envelope.data.identity_errors == []
    actions = " ".join(envelope.actions)
    assert 'reviews(kind="account_links")' in actions
    assert 'reviews(kind="merchant_links")' in actions


@pytest.mark.unit
async def test_refresh_run_identity_account_failure_keeps_merchant_review_hint() -> (
    None
):
    """A failed account backfill hides only its own review queue hint."""
    fake_result = RefreshResult(
        applied=False,
        duration_seconds=None,
        identity_errors=("accounts",),
    )
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run(steps=["identity"])

    assert envelope.data.identity_errors == ["accounts"]
    actions = " ".join(envelope.actions)
    assert 'reviews(kind="account_links")' not in actions
    assert 'reviews(kind="merchant_links")' in actions


@pytest.mark.unit
async def test_refresh_run_emits_followup_hint_when_match_without_categorize() -> None:
    """When match is requested but categorize is omitted, actions[] hints at categorize."""
    fake_result = RefreshResult(applied=True, duration_seconds=1.0, error=None)
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run(steps=["match", "transform"])
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT in envelope.actions


@pytest.mark.unit
async def test_refresh_run_suppresses_followup_hint_on_transform_failure() -> None:
    """Suppress the categorize follow-up hint when transform fails.

    When transform was requested and failed, categorize would run against
    stale outputs — the agent should resolve the apply failure first.
    """
    fake_result = RefreshResult(applied=False, duration_seconds=0.5, error="model boom")
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run(steps=["match", "transform"])
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT not in envelope.actions
    # Apply-failed hint should still fire.
    assert any("moneybin transform plan" in a for a in envelope.actions), (
        envelope.actions
    )


@pytest.mark.unit
async def test_refresh_run_no_followup_hint_when_categorize_included() -> None:
    """Default-cascade or explicit categorize → no follow-up hint."""
    fake_result = RefreshResult(applied=True, duration_seconds=1.0, error=None)
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run()
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT not in envelope.actions
