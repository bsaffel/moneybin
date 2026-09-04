"""refresh_* tools — the always-visible umbrella over the refresh domain.

Tool:
    - refresh_run — gsheet pull + match + SQLMesh apply + categorization +
      identity backfill + exchange-rate gather

Wraps :func:`moneybin.orchestration.refresh.refresh`. Operators needing
SQLMesh-step granularity can pass ``steps=["transform"]`` (the granular
form formerly exposed as ``transform_apply``), or use the CLI for
read-only introspection: ``moneybin transform plan|validate|audit|status``
(operator territory, not MCP-registered; see mcp.md category 2).
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.adapters.refresh_adapters import refresh_envelope
from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.orchestration.refresh import RefreshStep, expand_steps, refresh
from moneybin.privacy.payloads.system import RefreshRunPayload
from moneybin.protocol.envelope import ResponseEnvelope


@mcp_tool(read_only=False)
def refresh_run(
    steps: list[RefreshStep] | None = None,
) -> ResponseEnvelope[RefreshRunPayload]:
    """Run refresh: match → SQLMesh apply → categorize → identity → rate gather.

    The single user-facing entry point for refreshing derived state from raw
    inputs. Idempotent; safe to retry after a failure. Matching and
    categorization are best-effort. Identity backfill is also best-effort per
    domain: ``identity_errors`` contains only ``accounts`` and/or ``merchants``
    when proposal generation fails, while successful domains point to their
    ``reviews(kind=...)`` queue. Only a SQLMesh apply error sets the top-level
    ``error``. (A first-load missing-view precondition is not a crash and
    leaves matching/categorization fields unset.)

    The match step acts without asking, so the response says what it decided:
    ``matches_auto_merged`` (duplicates folded above the confidence threshold),
    ``matches_pending_review`` / ``matches_pending_transfers`` (queued for the
    user), and ``transfers_retired`` (transfers the user had accepted that a
    dedup collapse invalidated, reversed — report these and point at
    ``system_audit_undo``, since they undo a decision of the user's). Read all
    four against ``matching_skipped``: when it is true the step could not run and
    the counts are zero because nothing was examined, so "no duplicates found"
    is not a claim they support.

    Args:
        steps: Subset of ``["gsheet", "match", "transform", "categorize",
            "identity", "rates"]``
            to run. Defaults to None (full cascade). Steps execute in canonical
            order (gsheet → match → transform → categorize → identity → rates)
            regardless of input order; dependencies enforce it (categorize
            reads SQLMesh-built views, and rates derives the currency pairs it
            fetches from core.*). Pass ``["transform"]`` to run only
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
        # 880 of the 900-char cap, first sentence 35 of 120. Both halves are
        # load-bearing and the verbatim union of the two branches was 1049, so
        # the rate detail is compressed to parenthetical remedies while the
        # transfer-reversal warning keeps its full wording — it is the one that
        # undoes a decision the user made. The opening two sentences stay split
        # rather than joined by a colon: `FIRST_SENTENCE_CHARACTER_LIMIT` is 120
        # and the whole enumeration does not fit in one.
        "Run the post-load refresh pipeline. By default it performs Google "
        "Sheets pull, matching, SQLMesh apply, deterministic categorization, "
        "identity proposal backfill, and an exchange-rate gather in canonical "
        "order. Pass steps to select from gsheet, match, transform, categorize, "
        "identity, and rates. The match step acts without asking, and folding a "
        "duplicate can reverse a transfer the user already accepted: "
        "`transfers_retired` counts those, and system_audit_undo(operation_id=...) "
        "restores "
        "them. The rates step caches the rates this profile's own rows imply so "
        "reports convert offline; an unfilled pair lands in rate_pairs_failed "
        "(retried), rate_pairs_unsupported (needs `moneybin fx set`), or "
        "rate_pairs_discarded (partly unusable, so gaps). Rebuilds core.* and "
        "reports.* and may write app categorization or identity-review state, "
        "plus the raw exchange-rate cache. No revert; fix inputs and rerun.",
    )
