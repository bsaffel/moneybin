"""Merchants namespace tools — merchant name mapping reference data and link review.

Tools:
    - merchants — List all merchant name mappings (low sensitivity)
    - merchants_create — Create merchant name mappings (low sensitivity)
    - merchants_links_pending — Pending merchant-link decisions (medium sensitivity)
    - merchants_links_set — Accept or reject one pending decision (low sensitivity)
    - merchants_links_history — Recent merchant-link decisions (medium sensitivity)
    - merchants_links_run — Harvest pending proposals from existing data (low sensitivity)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.elicitation import confirm_or_raise
from moneybin.privacy.payloads.categories import (
    MerchantsCreatePayload,
    MerchantsPayload,
)
from moneybin.privacy.payloads.merchants import (
    MerchantLinksHistoryPayload,
    MerchantLinksPendingPayload,
    MerchantLinksRunPayload,
    MerchantLinksSetPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization import CategorizationService, validate_match_type
from moneybin.services.merchant_links_service import MerchantLinksService

logger = logging.getLogger(__name__)


@mcp_tool()
def merchants() -> ResponseEnvelope[MerchantsPayload]:
    """List all merchant name mappings.

    Returns merchant ID, raw pattern, match type, canonical name,
    and associated category. Merchant mappings normalize transaction
    descriptions and provide default categories.
    """
    with get_database(read_only=True) as db:
        payload = CategorizationService(db).list_merchants()
    return build_envelope(
        data=payload,
        actions=[
            "Use merchants_create to add new merchant mappings",
        ],
    )


@mcp_tool(read_only=False, idempotent=False)
def merchants_create(
    merchants: list[dict[str, str | None]],
) -> ResponseEnvelope[MerchantsCreatePayload]:
    """Create multiple merchant name mappings in one call.

    Each merchant dict should have ``raw_pattern`` and ``canonical_name``.
    Optional fields: ``match_type`` (default 'contains'), ``category``,
    ``subcategory``.

    Args:
        merchants: List of merchant mapping dicts.
    """
    if not merchants:
        return build_envelope(
            data=MerchantsCreatePayload(created=0, skipped=0, error_details=[])
        )

    created = 0
    skipped = 0
    error_details: list[dict[str, str]] = []

    with get_database(read_only=False) as db:
        service = CategorizationService(db)
        for item in merchants:
            raw_pattern = str(item.get("raw_pattern", "")).strip()
            canonical_name = str(item.get("canonical_name", "")).strip()
            if not raw_pattern or not canonical_name:
                skipped += 1
                error_details.append({
                    "canonical_name": canonical_name or "(missing)",
                    "reason": "Missing raw_pattern or canonical_name",
                })
                continue

            raw_match_type = str(item.get("match_type", "contains")).strip()
            try:
                match_type = validate_match_type(raw_match_type)
            except ValueError:
                skipped += 1
                error_details.append({
                    "canonical_name": canonical_name,
                    "reason": f"Invalid match_type: {raw_match_type}",
                })
                continue

            category = str(item.get("category", "")).strip() or None
            subcategory = str(item.get("subcategory", "")).strip() or None

            try:
                service.create_merchant(
                    raw_pattern,
                    canonical_name,
                    match_type=match_type,
                    category=category,
                    subcategory=subcategory,
                    created_by="ai",
                    actor="mcp",
                )
                created += 1
            except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
                skipped += 1
                logger.exception("create_merchants failed")
                error_details.append({
                    "canonical_name": canonical_name,
                    "reason": "Failed to create merchant — check logs for details.",
                })

    return build_envelope(
        data=MerchantsCreatePayload(
            created=created, skipped=skipped, error_details=error_details
        ),
        total_count=len(merchants),
        actions=[
            "Use merchants to review all merchant mappings",
        ],
    )


# ─── Review tools (links) ──────────────────────────────────────────────────


@mcp_tool(domain="links")
def merchants_links_pending() -> ResponseEnvelope[MerchantLinksPendingPayload]:
    """List pending merchant-link decisions, grouped by provider entity id.

    Returns the review queue of provider entity ids with candidate merge
    proposals. Each group represents one provider entity id (a Plaid
    ``merchant_entity_id`` or equivalent) and the candidate canonical
    merchants it may represent.

    For each candidate: decision_id, candidate_merchant_id, canonical name,
    and confidence score. ref_value (the raw provider entity id) is an
    opaque RECORD_ID (low sensitivity); provider_merchant_name and
    candidate_canonical_name are MERCHANT_NAME (medium sensitivity).

    Decide each group via merchants_links_set; run merchants_links_run to
    backfill bindings from existing categorizations first.
    """
    with get_database(read_only=True) as db:
        svc = MerchantLinksService(db, actor="mcp")
        groups = svc.pending()
        n_pending = svc.count_pending()
    payload = MerchantLinksPendingPayload.from_service(groups, n_pending)
    return build_envelope(
        data=payload,
        total_count=n_pending,
        actions=[
            "Use merchants_links_set(decision_id, action='accept', "
            "target_merchant_id=<candidate_merchant_id>) to bind — the user is "
            "prompted to confirm the binding before anything is written",
            "Use merchants_links_set(decision_id, action='reject') to leave the "
            "provider entity id unbound",
        ],
    )


@dataclass(frozen=True, slots=True)
class _MerchantBindProposal:
    """One pending merchant-link decision, flattened for the confirmation prompt."""

    decision_id: str
    ref_value: str
    source_type: str
    provider_merchant_name: str | None
    candidate_merchant_id: str
    candidate_canonical_name: str
    confidence: float | None


def _merchant_cli_equivalent(decision_id: str, target_merchant_id: str) -> str:
    return f"moneybin merchants links set {decision_id} --into {target_merchant_id}"


def _load_pending_merchant_proposal(decision_id: str) -> _MerchantBindProposal:
    """Read the decision out of the live review queue, or raise if it isn't there."""
    with get_database(read_only=True) as db:
        groups = MerchantLinksService(db, actor="mcp").pending()
    for group in groups:
        for candidate in group.candidates:
            if candidate.decision_id == decision_id:
                return _MerchantBindProposal(
                    decision_id=decision_id,
                    ref_value=group.ref_value,
                    source_type=group.source_type,
                    provider_merchant_name=group.provider_merchant_name,
                    candidate_merchant_id=candidate.candidate_merchant_id,
                    candidate_canonical_name=candidate.candidate_canonical_name,
                    confidence=candidate.confidence,
                )
    raise UserError(
        f"No pending merchant-link decision '{decision_id}'.",
        code=error_codes.MUTATION_NOT_FOUND,
        hint="List open decisions with merchants_links_pending.",
    )


def _merchant_confirm_message(p: _MerchantBindProposal) -> str:
    """Prompt text a human reads before a provider entity id is bound to a merchant.

    Names BOTH sides of the binding and the confidence the resolver could not
    clear — a decision only reaches this queue because the resolver saw one
    provider id pointing at more than one canonical merchant.
    """
    confidence = "unscored" if p.confidence is None else f"{p.confidence:.2f}"
    return (
        "Confirm a merchant binding (this attributes every transaction carrying "
        "the provider's entity id to one canonical merchant).\n\n"
        f"BIND — provider entity id {p.ref_value} (source {p.source_type}):\n"
        f"  provider name {p.provider_merchant_name or '(none)'}\n\n"
        f"TO — canonical merchant_id {p.candidate_merchant_id}:\n"
        f"  name {p.candidate_canonical_name or '(none)'}\n\n"
        f"Proposed at confidence {confidence}. The resolver queues a binding for "
        "review ONLY when it cannot decide on its own — this is an ambiguous "
        "match, not a certain one.\n\n"
        "Accepting writes the binding and rejects every other candidate for this "
        "entity id. If this is not the right merchant, future transactions "
        "carrying the id are attributed — and categorized — under the wrong one. "
        "Reversible via system_audit_undo(operation_id).\n\n"
        "Accept this binding?"
    )


def _apply_merchant_accept(decision_id: str, target_merchant_id: str) -> None:
    # decided_by="user" is truthful only on this path: a human just ratified the
    # binding through the elicitation gate above.
    with get_database(read_only=False) as db:
        MerchantLinksService(db, actor="mcp").set(
            decision_id, target_merchant_id=target_merchant_id, decided_by="user"
        )


def _apply_merchant_reject(decision_id: str) -> None:
    # decided_by="auto": no human ratified this reject — the agent called it.
    # The column's CHECK admits only 'auto' | 'user', and recording 'user' for a
    # decision no human made is precisely the falsehood the accept gate exists to
    # prevent. The MCP channel itself is preserved in app.audit_log (actor='mcp').
    with get_database(read_only=False) as db:
        MerchantLinksService(db, actor="mcp").set(
            decision_id, target_merchant_id=None, decided_by="auto"
        )


@mcp_tool(
    domain="links",
    read_only=False,
    destructive=True,
    idempotent=False,
    # The accept path blocks on a human reading a binding confirmation (the
    # provider entity + the merchant + the reason they're ambiguous). The 30s
    # default would routinely fire first — and a cap that expires mid-decision
    # means the user "accepts" into a coroutine that was already cancelled. Same
    # headroom as investments_securities_links_set. Timing out is still safe
    # (nothing is written), just confusing.
    timeout_seconds=180.0,
)
async def merchants_links_set(
    decision_id: str,
    action: str,
    target_merchant_id: str | None = None,
) -> ResponseEnvelope[MerchantLinksSetPayload]:
    """Accept (bind) or reject one pending merchant-link decision.

    `action` is explicit — accept vs reject is never inferred from whether
    `target_merchant_id` has a value:

    - `action="accept"` + `target_merchant_id=<the decision's own
      candidate_merchant_id>` BINDS the provider entity id to that merchant.
      This REQUIRES explicit human confirmation: the tool prompts the user
      through an MCP elicitation naming both sides and the confidence, and binds
      only if they agree. On a client that cannot prompt (no elicitation
      capability), accept HARD-FAILS with mutation_confirmation_required and
      points at the CLI — an agent cannot accept a binding on its own, at any
      confidence. `target_merchant_id` is also a confirming safety check: it
      must equal the decision's own candidate. Mismatched, empty, or missing
      `target_merchant_id` raises mutation_invalid_input — it is never treated
      as a reject.
    - `action="reject"` (pass no `target_merchant_id`) REJECTS this decision and
      every pending sibling candidate for the same provider entity id. The id
      stays unbound; the declined pairing is not re-proposed, and the resolver
      mints a new merchant for the id on its next categorization pass.

    Accepting attributes every transaction carrying the provider entity id to
    the chosen canonical merchant, which also drives their categorization.

    Mutation surface: writes app.merchant_link_decisions + app.merchant_links.
    Reverse with system_audit_undo(operation_id) — find the operation_id via
    system_audit. Find pending decisions with merchants_links_pending.

    Args:
        decision_id: The decision id to act on (from merchants_links_pending).
        action: "accept" (bind, requires `target_merchant_id` + human
            confirmation) or "reject" (leave the entity id unbound; pass no
            `target_merchant_id`).
        target_merchant_id: With action="accept", the candidate merchant_id to
            bind — must equal the decision's own candidate_merchant_id. Invalid
            with action="reject".
    """
    if action not in ("accept", "reject"):
        raise UserError(
            f"action must be 'accept' or 'reject' (got {action!r}).",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if action == "reject":
        if target_merchant_id is not None:
            raise UserError(
                "'target_merchant_id' is only valid with action='accept'. To "
                "reject, pass action='reject' with no 'target_merchant_id'.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        # DB work off the event loop: this tool is a coroutine (it awaits the
        # elicitation), so a blocking DuckDB write here would stall the server.
        await asyncio.to_thread(_apply_merchant_reject, decision_id)
        status = "rejected"
    else:
        if not target_merchant_id:
            raise UserError(
                "action='accept' requires 'target_merchant_id' = the decision's "
                "own candidate_merchant_id (see merchants_links_pending). An "
                "empty 'target_merchant_id' is not a reject — pass "
                "action='reject' for that.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        proposal = await asyncio.to_thread(_load_pending_merchant_proposal, decision_id)
        if target_merchant_id != proposal.candidate_merchant_id:
            # Refuse BEFORE prompting: a doomed binding must not cost the user a
            # confirmation. The service re-checks this; this is the boundary copy.
            raise UserError(
                f"'target_merchant_id' does not match decision '{decision_id}' — "
                "it must be that decision's own candidate_merchant_id.",
                code=error_codes.MUTATION_INVALID_INPUT,
                hint="Re-read the decision with merchants_links_pending.",
            )
        await confirm_or_raise(
            _merchant_confirm_message(proposal),
            subject="This binding",
            unchanged=f"decision '{decision_id}' is still pending",
            cli_equivalent=_merchant_cli_equivalent(decision_id, target_merchant_id),
            details={"decision_id": decision_id},
        )
        await asyncio.to_thread(_apply_merchant_accept, decision_id, target_merchant_id)
        status = "accepted"
    return build_envelope(
        data=MerchantLinksSetPayload(decision_id=decision_id, status=status),
        actions=[
            "Use merchants_links_pending to review remaining pending decisions",
            "Reverse this decision with system_audit_undo(operation_id) — find "
            "the operation_id with system_audit",
        ],
    )


@mcp_tool(domain="links")
def merchants_links_history(
    limit: int = 50,
) -> ResponseEnvelope[MerchantLinksHistoryPayload]:
    """Recent merchant-link decisions (all statuses), newest first.

    Args:
        limit: Maximum rows (default 50).
    """
    with get_database(read_only=True) as db:
        rows = MerchantLinksService(db, actor="mcp").history(limit=limit)
    payload = MerchantLinksHistoryPayload.from_rows(rows)
    return build_envelope(
        data=payload,
        actions=["Use merchants_links_pending for the active review queue"],
    )


@mcp_tool(domain="links", read_only=False)
def merchants_links_run() -> ResponseEnvelope[MerchantLinksRunPayload]:
    """Harvest merchant-link proposals from existing categorization facts.

    Binds provider entity ids that point unambiguously to a single canonical
    merchant, and routes conflicts to the pending review queue. Writes
    accepted bindings to ``app.merchant_links`` and conflict decisions to
    ``app.merchant_link_decisions`` — the same shape the import-time resolver
    writes.

    Skips provider entity ids that already have an accepted or rejected
    binding and avoids double-proposing.

    Mutation surface: writes ``app.merchant_links`` + ``app.merchant_link_decisions``.
    Revert is via the audit log in ``app.audit_log`` (no undo tool yet; deferred to M1L).
    Review queued conflicts with ``merchants_links_pending``.

    Returns:
        Envelope with ``data.bound`` (entity ids silently bound to a single
        merchant) and ``data.conflicts`` (one-id-many-merchant cases queued for
        review). Bound bindings are NOT pending — only conflicts need review.
    """
    with get_database(read_only=False) as db:
        result = MerchantLinksService(db, actor="mcp").run()
    return build_envelope(
        data=MerchantLinksRunPayload(bound=result.bound, conflicts=result.conflicts),
        actions=["Use merchants_links_pending to review the queued conflicts"],
    )


def register_merchants_tools(mcp: FastMCP) -> None:
    """Register all merchants namespace tools with the FastMCP server."""
    register(mcp, merchants, "merchants", "List all merchant name mappings.")
    register(
        mcp,
        merchants_create,
        "merchants_create",
        "Create multiple merchant name mappings for description "
        "normalization and auto-categorization. "
        "Writes app.user_merchants; no built-in delete tool — revert by editing or repointing the row directly via SQL.",
    )
    register(
        mcp,
        merchants_links_pending,
        "merchants_links_pending",
        "List pending merchant-link decisions grouped by provider entity id "
        "(e.g. Plaid merchant_entity_id). Returns the review queue of provider "
        "entity ids with candidate canonical merchant proposals. Each candidate "
        "carries decision_id, merchant_id, canonical name, and confidence score. "
        "Sensitivity: low for ids (ref_value, candidate_merchant_id = RECORD_ID); "
        "medium for names (provider_merchant_name, candidate_canonical_name = MERCHANT_NAME). "
        "Use merchants_links_set to bind or reject each decision. "
        "Run merchants_links_run first to backfill proposals for pre-existing data.",
    )
    register(
        mcp,
        merchants_links_run,
        "merchants_links_run",
        "Harvest merchant-link proposals from existing categorization facts. "
        "Binds unambiguous provider entity id → merchant pairings and routes "
        "conflicts to the pending review queue. Writes app.merchant_links + "
        "app.merchant_link_decisions; skips pairs already proposed or decided. "
        "Returns data.bound (entity ids silently bound to one merchant) and "
        "data.conflicts (one-id-many-merchant cases queued for review) — bound "
        "bindings are NOT pending. "
        "Mutation surface: writes app.merchant_links + app.merchant_link_decisions; "
        "revert via app.audit_log (no undo tool yet). "
        "Review queued conflicts with merchants_links_pending.",
    )
    register(
        mcp,
        merchants_links_set,
        "merchants_links_set",
        "Accept (bind) or reject one pending merchant-link decision. "
        "action='accept' + target_merchant_id=<the decision's own "
        "candidate_merchant_id> BINDS the provider entity id to that canonical "
        "merchant: it prompts the user to confirm (MCP elicitation naming both "
        "sides and the confidence) and binds only on their explicit agreement — "
        "the binding attributes every transaction carrying that entity id to the "
        "merchant, so the agent cannot accept one on its own. On a client without "
        "elicitation, accept fails with mutation_confirmation_required and names "
        "the CLI equivalent. target_merchant_id must equal the decision's own "
        "candidate (mismatched, empty, or missing target_merchant_id raises "
        "mutation_invalid_input — it is NEVER read as a reject). action='reject' "
        "(no target_merchant_id) rejects ALL pending candidates for this provider "
        "entity id; the id stays unbound, the declined pairings are not "
        "re-proposed, and the resolver mints a new merchant for the id on its next "
        "categorization pass. Writes app.merchant_link_decisions + "
        "app.merchant_links; reverse with system_audit_undo(operation_id). "
        "Discover pending decisions with merchants_links_pending.",
    )
    register(
        mcp,
        merchants_links_history,
        "merchants_links_history",
        "Recent merchant-link decisions (all statuses), newest first. "
        "Read-only. Filter by limit. Use merchants_links_pending for the "
        "active review queue.",
    )
