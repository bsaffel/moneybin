"""transform_* tools — SQLMesh pipeline operations exposed via MCP.

v2 MCP taxonomy stubs. The CLI surface (`moneybin transform plan/apply/
audit/status/validate`) is implemented and exercises SQLMesh directly,
but its operations print to stdout rather than returning structured
data. Wrapping them for MCP envelope shape requires a TransformService
that captures and parses SQLMesh output — out of scope for the
restructure PR. These stubs complete the v2 taxonomy and become real in
a follow-up that introduces the service layer.

Excludes from MCP exposure:
    - transform_restate — operator-territory destructive op preceded by
      code changes the AI doesn't drive (per .claude/rules/mcp-server.md
      "When CLI-only is justified").
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

_SPEC = "docs/specs/mcp-tool-surface.md"


def _stub_envelope(action: str) -> ResponseEnvelope:
    """Return a uniform not_implemented envelope for transform stubs."""
    return build_envelope(
        data={"status": "not_implemented", "action": action, "spec": _SPEC},
        sensitivity="low",
        actions=[
            f"Use the CLI: moneybin transform {action.removeprefix('transform_')}",
            f"See {_SPEC} §transform_* for the planned MCP surface",
        ],
    )


@mcp_tool(sensitivity="low")
def transform_status() -> ResponseEnvelope:
    """Current SQLMesh model state and environment."""
    return _stub_envelope("transform_status")


@mcp_tool(sensitivity="low")
def transform_plan() -> ResponseEnvelope:
    """Preview pending SQLMesh changes."""
    return _stub_envelope("transform_plan")


@mcp_tool(sensitivity="low")
def transform_validate() -> ResponseEnvelope:
    """Check that model SQL parses and resolves."""
    return _stub_envelope("transform_validate")


@mcp_tool(sensitivity="low")
def transform_audit(start: str, end: str) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Run SQLMesh data-quality audits over a date window."""
    return _stub_envelope("transform_audit")


@mcp_tool(sensitivity="low")
def transform_apply() -> ResponseEnvelope:
    """Apply pending SQLMesh changes."""
    return _stub_envelope("transform_apply")


def register_transform_tools(mcp: FastMCP) -> None:
    """Register all transform namespace tools with the FastMCP server."""
    for fn, desc in [
        (transform_status, "Show current SQLMesh model state and environment."),
        (transform_plan, "Preview pending SQLMesh changes without applying."),
        (transform_validate, "Check that all model SQL parses and resolves."),
        (transform_audit, "Run SQLMesh data-quality audits over a date window."),
        (transform_apply, "Apply pending SQLMesh changes to rebuild affected models."),
    ]:
        register(mcp, fn, fn.__name__, desc)
