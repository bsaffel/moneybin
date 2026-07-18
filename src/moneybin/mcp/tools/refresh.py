"""refresh_* tools — the always-visible umbrella over the refresh domain.

Tool:
    - refresh_run — match + SQLMesh apply + categorization + identity backfill

Wraps :func:`moneybin.services.refresh.refresh`. Operators needing
SQLMesh-step granularity can pass ``steps=["transform"]`` (the granular
form formerly exposed as ``transform_apply``), or use the CLI for
read-only introspection: ``moneybin transform plan|validate|audit|status``
(operator territory, not MCP-registered; see mcp.md category 2).
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.adapters.refresh_adapters import refresh_envelope
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.system import RefreshRunPayload
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.refresh import RefreshStep, expand_steps, refresh


@mcp_tool(read_only=False)
def refresh_run(
    steps: list[RefreshStep] | None = None,
) -> ResponseEnvelope[RefreshRunPayload]:
    """Run refresh: matching → SQLMesh apply → categorization → identity backfill.

    The single user-facing entry point for refreshing derived state from raw
    inputs. Idempotent; safe to retry after a failure. Matching and
    categorization are best-effort. Identity backfill is also best-effort per
    domain: ``identity_errors`` contains only ``accounts`` and/or ``merchants``
    when proposal generation fails, while successful domains point to their
    ``reviews(kind=...)`` queue. Only a SQLMesh apply error sets the top-level
    ``error``. (A first-load missing-view precondition is not a crash and
    leaves matching/categorization fields unset.)

    Args:
        steps: Subset of ``["gsheet", "match", "transform", "categorize",
            "identity"]``
            to run. Defaults to None (full cascade). Steps execute in
            canonical order (gsheet → match → transform → categorize → identity)
            regardless of input order; dependencies enforce it (categorize
            reads SQLMesh-built views). Pass ``["transform"]`` to run only
            SQLMesh apply.

    For SQLMesh-step granularity beyond apply (plan, validate, audit,
    per-step status), use the CLI: ``moneybin transform plan|validate|
    audit|status`` (operator tools, CLI-only).

    This umbrella is symmetric with ``transactions_categorize_run(methods=...)``:
    both accept a list parameter to scope which sub-operations execute,
    both default to the full set, both raise on unknown member names.
    """
    with get_database(read_only=False, operation_type="transform_apply") as db:
        result = refresh(db, steps=list(steps) if steps is not None else None)
    return refresh_envelope(result, requested=expand_steps(steps))


def register_refresh_tools(mcp: FastMCP) -> None:
    """Register the refresh namespace tools with the FastMCP server."""
    register(
        mcp,
        refresh_run,
        "refresh_run",
        "Run the post-load refresh pipeline: cross-source matching, "
        "SQLMesh apply, deterministic categorization, and account/merchant "
        "identity proposal backfill. The single "
        "always-visible entry point for refreshing derived tables (core.* "
        "and reports.*) from raw inputs. Idempotent — safe to retry. "
        "Accepts optional steps (list of 'gsheet', 'match', 'transform', "
        "'categorize', 'identity') to scope which sub-operations execute; defaults to "
        "the full cascade. "
        "Steps execute in canonical order (gsheet → match → transform → "
        "categorize → identity) regardless of input order. "
        "Best-effort steps (match, categorize, identity) don't fail the call: a real "
        "crash is surfaced as matching_error/categorization_error/identity_errors plus "
        "recovery_actions (a targeted refresh_run retry and system_doctor). "
        "Only SQLMesh apply errors set the top-level error. "
        "Mutation surface: rebuilds core.* and reports.* views via SQLMesh "
        "and writes app.transaction_categories plus reviewable app account/merchant "
        "identity proposal state. "
        "No revert path; re-run after fixing inputs. "
        "Symmetric with transactions_categorize_run(methods=...). "
        "For SQLMesh-step granularity beyond apply, use the CLI: "
        "`moneybin transform plan|validate|audit|status` (CLI-only operator tools).",
    )
