"""transform_* tools — thin wrappers over ``TransformService``.

CLI-only (operator territory, category 2). ``transform_apply`` was folded
into ``refresh_run(steps=["transform"])`` per the refresh umbrella; see
``src/moneybin/mcp/tools/refresh.py``. ``transform_restate`` is destructive
force-recompute, CLI-only. The four introspection tools (status, plan,
validate, audit) remain CLI-accessible via the same service layer but are
not registered on the MCP surface.
"""

from __future__ import annotations

from moneybin.database import get_database
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
        actions.append(
            "Run refresh_run with steps=['transform'] to refresh derived tables"
        )
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


@mcp_tool(sensitivity="low", read_only=False)
def transform_audit(start: str, end: str) -> ResponseEnvelope:
    """Run SQLMesh data-quality audits over a date window.

    May write SQLMesh state tables on first Context init.
    """
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
