"""transform_* tools — thin wrappers over ``TransformService``.

Excludes ``transform_restate`` (operator territory: destructive force-recompute,
CLI-only).
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@mcp_tool(sensitivity="low")
def transform_status() -> ResponseEnvelope:
    """Current SQLMesh model state and environment."""
    from moneybin.services.transform_service import TransformService

    with get_database(read_only=True) as db:
        status = TransformService(db).status()
    actions: list[str] = []
    if status.pending:
        actions.append("Run transform_apply to refresh derived tables")
    return build_envelope(
        data={
            "environment": status.environment,
            "initialized": status.initialized,
            "last_apply_at": (
                status.last_apply_at.isoformat()
                if status.last_apply_at is not None
                else None
            ),
            "pending": status.pending,
            "latest_import_at": (
                status.latest_import_at.isoformat()
                if status.latest_import_at is not None
                else None
            ),
        },
        sensitivity="low",
        actions=actions,
    )


@mcp_tool(sensitivity="low")
def transform_plan() -> ResponseEnvelope:
    """Preview pending SQLMesh changes."""
    from moneybin.services.transform_service import TransformService

    with get_database(read_only=True) as db:
        plan = TransformService(db).plan()
    return build_envelope(
        data={
            "has_changes": plan.has_changes,
            "directly_modified": plan.directly_modified,
            "indirectly_modified": plan.indirectly_modified,
            "added": plan.added,
            "removed": plan.removed,
        },
        sensitivity="low",
    )


@mcp_tool(sensitivity="low")
def transform_validate() -> ResponseEnvelope:
    """Check that model SQL parses and resolves."""
    from moneybin.services.transform_service import TransformService

    with get_database(read_only=True) as db:
        result = TransformService(db).validate()
    return build_envelope(
        data={"valid": result.valid, "errors": result.errors},
        sensitivity="low",
    )


@mcp_tool(sensitivity="low")
def transform_audit(start: str, end: str) -> ResponseEnvelope:
    """Run SQLMesh data-quality audits over a date window."""
    from moneybin.services.transform_service import TransformService

    with get_database() as db:
        result = TransformService(db).audit(start, end)
    return build_envelope(
        data={
            "passed": result.passed,
            "failed": result.failed,
            "audits": result.audits,
        },
        sensitivity="low",
    )


@mcp_tool(sensitivity="low", read_only=False)
def transform_apply() -> ResponseEnvelope:
    """Apply pending SQLMesh changes.

    Mutation surface: rebuilds ``core.*`` and ``reports.*`` from raw/prep
    inputs. No revert — re-run after fixing inputs.
    """
    from moneybin.services.transform_service import TransformService

    with get_database() as db:
        result = TransformService(db).apply()
    data: dict[str, object] = {
        "applied": result.applied,
        "duration_seconds": result.duration_seconds,
    }
    if result.error is not None:
        data["error"] = result.error
    return build_envelope(data=data, sensitivity="low")


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
