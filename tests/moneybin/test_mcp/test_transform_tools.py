"""Tests for transform_* MCP tools.

Transform is exposed as a taxonomy stub surface — the CLI works but
SQLMesh output is unstructured, so MCP wrappers return not_implemented
envelopes pending a TransformService layer. These tests verify wiring
and envelope shape, not real SQLMesh behavior.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.transform import (
    register_transform_tools,
    transform_apply,
    transform_audit,
    transform_plan,
    transform_status,
    transform_validate,
)
from moneybin.protocol.envelope import ResponseEnvelope

_EXPECTED_TOOLS = {
    "transform_status",
    "transform_plan",
    "transform_validate",
    "transform_audit",
    "transform_apply",
}


@pytest.mark.unit
def test_register_transform_tools_registers_all_five() -> None:
    """All 5 transform tools register; restate is excluded by design."""
    srv = FastMCP("test")
    register_transform_tools(srv)
    names = {t.name for t in asyncio.run(srv._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert _EXPECTED_TOOLS <= names
    assert "transform_restate" not in names


@pytest.mark.unit
@pytest.mark.parametrize(
    "fn",
    [
        lambda: transform_status(),
        lambda: transform_plan(),
        lambda: transform_validate(),
        lambda: transform_audit(start="2026-01-01", end="2026-04-30"),
        lambda: transform_apply(),
    ],
)
def test_transform_tool_returns_not_implemented_envelope(
    fn: Callable[[], ResponseEnvelope],
) -> None:
    """Every transform tool returns a stub envelope referencing the spec."""
    parsed = fn().to_dict()
    assert parsed["summary"]["sensitivity"] == "low"
    assert parsed["data"]["status"] == "not_implemented"
    assert parsed["data"]["spec"] == "docs/specs/mcp-tool-surface.md"
