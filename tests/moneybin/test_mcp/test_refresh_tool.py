"""Unit tests for the refresh_run MCP tool."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.refresh import refresh_run, register_refresh_tools


@pytest.mark.unit
async def test_refresh_run_is_registered() -> None:
    mcp = FastMCP("test")
    register_refresh_tools(mcp)
    names = {tool.name for tool in await mcp._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "refresh_run" in names


@pytest.mark.unit
async def test_refresh_run_returns_envelope_on_success() -> None:
    fake_result = MagicMock(applied=True, duration_seconds=4.2, error=None)
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run()
    assert envelope.data["applied"] is True
    assert envelope.data["duration_seconds"] == 4.2
    assert envelope.data.get("error") is None


@pytest.mark.unit
async def test_refresh_run_surfaces_apply_error() -> None:
    fake_result = MagicMock(applied=False, duration_seconds=1.1, error="model boom")
    with (
        patch("moneybin.mcp.tools.refresh.refresh", return_value=fake_result),
        patch("moneybin.mcp.tools.refresh.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        envelope = await refresh_run()
    assert envelope.data["applied"] is False
    assert envelope.data["error"] == "model boom"
