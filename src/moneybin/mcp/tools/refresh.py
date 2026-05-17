"""refresh_* tools — the always-visible umbrella over the refresh domain.

Tool:
    - refresh_run — match + SQLMesh apply + categorization (low sensitivity)

Wraps :func:`moneybin.services.refresh.refresh`. Operators needing
SQLMesh-step granularity can pass ``steps=["transform"]`` (the granular
form formerly exposed as ``transform_apply``) or reach the dedicated
read tools :func:`transform_plan`, :func:`transform_validate`,
:func:`transform_audit`, :func:`transform_status` directly — they're
registered as infrastructure verbs per the carve-out in
``.claude/rules/mcp-server.md``.
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.adapters.refresh_adapters import refresh_envelope
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.refresh import RefreshStep, expand_steps, refresh


@mcp_tool(sensitivity="low", read_only=False)
def refresh_run(
    steps: list[RefreshStep] | None = None,
) -> ResponseEnvelope:
    """Run the post-load refresh pipeline: matching → SQLMesh apply → categorization.

    The single user-facing entry point for refreshing derived state from raw
    inputs. Idempotent; safe to retry after a failure. Matching and
    categorization steps are best-effort and log-only on failure — only
    SQLMesh apply errors surface in the response envelope.

    Args:
        steps: Subset of ``["match", "transform", "categorize"]`` to run.
            Defaults to None (full cascade). Steps execute in canonical
            order (match → transform → categorize) regardless of input
            order; dependencies enforce it (categorize reads SQLMesh-built
            views). Pass ``["transform"]`` to run only SQLMesh apply.

    For SQLMesh-step granularity beyond apply (plan, validate, audit,
    per-step status), call ``transform_plan``, ``transform_validate``,
    ``transform_audit``, or ``transform_status`` directly.

    This umbrella is symmetric with ``transactions_categorize_run(methods=...)``:
    both accept a list parameter to scope which sub-operations execute,
    both default to the full set, both raise on unknown member names.
    """
    with get_database() as db:
        result = refresh(db, steps=list(steps) if steps is not None else None)
    return refresh_envelope(result, requested=expand_steps(steps))


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
        "Accepts optional steps (list of 'match', 'transform', 'categorize') "
        "to scope which sub-operations execute; defaults to the full cascade. "
        "Steps execute in canonical order (match → transform → categorize) "
        "regardless of input order. "
        "Mutation surface: rebuilds core.* and reports.* views via SQLMesh "
        "and writes app.transaction_categories for newly-matched rules. "
        "No revert path; re-run after fixing inputs. "
        "Symmetric with transactions_categorize_run(methods=...). "
        "For SQLMesh-step granularity beyond apply, call transform_plan, "
        "transform_validate, transform_audit, or transform_status directly.",
    )
