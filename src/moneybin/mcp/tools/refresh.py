"""refresh_* tools — the always-visible umbrella over the refresh domain.

Tool:
    - refresh_run — match + SQLMesh apply + categorization (low sensitivity)

Wraps :func:`moneybin.services.refresh.refresh`. Operators needing
SQLMesh-step granularity (plan, validate, audit, status) reach those via
``moneybin_discover(domain='admin')``.
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.refresh import refresh


@mcp_tool(sensitivity="low", read_only=False)
def refresh_run() -> ResponseEnvelope:
    """Run the post-load refresh pipeline: matching → SQLMesh apply → categorization.

    The single user-facing entry point for refreshing derived state from raw
    inputs. Idempotent; safe to retry after a failure. Matching and
    categorization steps are best-effort and log-only on failure — only
    SQLMesh apply errors surface in the response envelope.

    For SQLMesh-step granularity (plan, validate, audit, per-step status),
    call ``moneybin_discover(domain='admin')`` to reveal the ``transform_*``
    tools.
    """
    with get_database() as db:
        result = refresh(db)
    data: dict[str, object] = {
        "applied": result.applied,
        "duration_seconds": result.duration_seconds,
    }
    if result.error is not None:
        data["error"] = result.error
    actions: list[str] = []
    if not result.applied and result.error is not None:
        actions.append(
            "SQLMesh apply failed — call moneybin_discover(domain='admin') "
            "then transform_plan to inspect, or refresh_run to retry."
        )
    return build_envelope(data=data, sensitivity="low", actions=actions)


def register_refresh_tools(mcp: FastMCP) -> None:
    """Register the refresh namespace tools with the FastMCP server."""
    register(
        mcp,
        refresh_run,
        "refresh_run",
        "Run the post-load refresh pipeline: cross-source matching, "
        "SQLMesh apply, deterministic categorization. The single "
        "always-visible entry point for refreshing derived tables (core.* "
        "and reports.*) from raw inputs. Idempotent — safe to retry. "
        "Mutation surface: rebuilds core.* and reports.* views via SQLMesh "
        "and writes app.transaction_categories for newly-matched rules. "
        "No revert path; re-run after fixing inputs. "
        "For SQLMesh-step granularity, call moneybin_discover(domain='admin').",
    )
