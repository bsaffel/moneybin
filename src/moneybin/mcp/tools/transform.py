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
from moneybin.privacy.payloads.system import (
    TransformAuditPayload,
    TransformAuditRow,
    TransformPlanPayload,
    TransformStatusPayload,
    TransformValidatePayload,
    TransformValidationError,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@mcp_tool()
def transform_status() -> ResponseEnvelope[TransformStatusPayload]:
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
        data=TransformStatusPayload(
            environment=status.environment,
            initialized=status.initialized,
            last_apply_at=(
                status.last_apply_at.isoformat()
                if status.last_apply_at is not None
                else None
            ),
            pending=status.pending,
            latest_import_at=(
                status.latest_import_at.isoformat()
                if status.latest_import_at is not None
                else None
            ),
        ),
        actions=actions,
    )


@mcp_tool()
def transform_plan() -> ResponseEnvelope[TransformPlanPayload]:
    """Preview pending SQLMesh changes."""
    from moneybin.services.transform_service import TransformService

    with get_database(read_only=True) as db:
        plan = TransformService(db).plan()
    return build_envelope(
        data=TransformPlanPayload(
            has_changes=plan.has_changes,
            directly_modified=plan.directly_modified,
            indirectly_modified=plan.indirectly_modified,
            added=plan.added,
            removed=plan.removed,
        )
    )


@mcp_tool()
def transform_validate() -> ResponseEnvelope[TransformValidatePayload]:
    """Check that model SQL parses and resolves."""
    from moneybin.services.transform_service import TransformService

    with get_database(read_only=True) as db:
        result = TransformService(db).validate()
    return build_envelope(
        data=TransformValidatePayload(
            valid=result.valid,
            errors=[
                TransformValidationError(
                    model=e.get("model", "<unknown>"), message=e.get("message", "")
                )
                for e in result.errors
            ],
        )
    )


@mcp_tool(read_only=False)
def transform_audit(start: str, end: str) -> ResponseEnvelope[TransformAuditPayload]:
    """Run SQLMesh data-quality audits over a date window.

    May write SQLMesh state tables on first Context init.
    """
    from moneybin.services.transform_service import TransformService

    with get_database(read_only=False) as db:
        result = TransformService(db).audit(start, end)
    return build_envelope(
        data=TransformAuditPayload(
            passed=result.passed,
            failed=result.failed,
            audits=[
                TransformAuditRow(
                    name=a.get("name") or "",
                    status=a.get("status") or "",
                    detail=a.get("detail"),
                )
                for a in result.audits
            ],
        )
    )
