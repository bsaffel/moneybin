"""transform_* stubs — SQLMesh wrappers pending a TransformService.

The CLI surface works (it drives SQLMesh directly), but SQLMesh prints
to stdout rather than returning structured data; capturing that for MCP
envelope shape requires a service layer that's out of scope here.
Excludes transform_restate (operator territory: destructive force-recompute).
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, not_implemented_envelope

_SPEC = "docs/specs/mcp-tool-surface.md"


def _stub(action: str) -> ResponseEnvelope:
    cli_verb = action.removeprefix("transform_")
    return not_implemented_envelope(
        action=action,
        spec=_SPEC,
        actions=[
            f"Use the CLI: moneybin transform {cli_verb}",
            f"See {_SPEC} §transform_* for the planned MCP surface",
        ],
    )


@mcp_tool(sensitivity="low")
def transform_status() -> ResponseEnvelope:
    """Current SQLMesh model state and environment."""
    return _stub("transform_status")


@mcp_tool(sensitivity="low")
def transform_plan() -> ResponseEnvelope:
    """Preview pending SQLMesh changes."""
    return _stub("transform_plan")


@mcp_tool(sensitivity="low")
def transform_validate() -> ResponseEnvelope:
    """Check that model SQL parses and resolves."""
    return _stub("transform_validate")


@mcp_tool(sensitivity="low")
def transform_audit(start: str, end: str) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Run SQLMesh data-quality audits over a date window."""
    return _stub("transform_audit")


@mcp_tool(sensitivity="low", read_only=False)
def transform_apply() -> ResponseEnvelope:
    """Apply pending SQLMesh changes."""
    return _stub("transform_apply")


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
