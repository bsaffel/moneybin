"""Merchants namespace tools — merchant name mapping reference data and link review.

Tools:
    - merchants — List all merchant name mappings (low sensitivity)
    - merchants_create — Create merchant name mappings (low sensitivity)
    - merchants_links_pending — Pending merchant-link decisions (medium sensitivity)
    - merchants_links_set — Accept or reject one pending decision (low sensitivity)
    - merchants_links_history — Recent merchant-link decisions (low sensitivity)
    - merchants_links_run — Harvest pending proposals from existing data (low sensitivity)
"""

from __future__ import annotations

import logging

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
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
            "Use merchants_links_set to bind (pass candidate_merchant_id as "
            "target_merchant_id) or reject (pass null) each decision",
        ],
    )


@mcp_tool(domain="links", read_only=False)
def merchants_links_set(
    decision_id: str,
    target_merchant_id: str | None,
) -> ResponseEnvelope[MerchantLinksSetPayload]:
    """Accept (bind) or reject one pending merchant-link decision.

    Mutates app.merchant_link_decisions (sets status) and, on accept,
    writes an accepted binding in app.merchant_links from the provider
    entity id onto target_merchant_id. On reject (target_merchant_id
    = null), marks only this decision rejected.

    target_merchant_id MUST be passed explicitly — there is no default:
    - Pass the decision's own candidate_merchant_id to BIND (confirming
      safety check: target must equal the decision's candidate).
    - Pass null to REJECT (the provider entity id stays unbound; the declined
      pairing is not re-proposed, and the resolver mints a new merchant for the
      id on its next categorization pass).

    Mutation surface: writes app.merchant_link_decisions + app.merchant_links.
    Revert is via the audit log in app.audit_log (no undo tool yet; undo
    is deferred to the M1L audit-undo consumer). Find pending decisions
    with merchants_links_pending.

    Args:
        decision_id: The decision id to act on (from merchants_links_pending).
        target_merchant_id: The candidate merchant_id to bind, or null to reject.
    """
    with get_database(read_only=False) as db:
        MerchantLinksService(db, actor="mcp").set(
            decision_id, target_merchant_id=target_merchant_id, decided_by="user"
        )
    status = "accepted" if target_merchant_id is not None else "rejected"
    return build_envelope(
        data=MerchantLinksSetPayload(decision_id=decision_id, status=status),
        actions=[
            "Use merchants_links_pending to review remaining pending decisions",
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
        "Pass target_merchant_id = candidate_merchant_id to BIND the provider "
        "entity id to that canonical merchant (target must equal the decision's "
        "own candidate_merchant_id — a confirming safety check). Pass null to "
        "REJECT — the provider entity id stays unbound; the declined pairing is "
        "not re-proposed, and the resolver mints a new merchant for the id on "
        "its next categorization pass. target_merchant_id has no default: pass "
        "it explicitly to avoid accidental rejection. "
        "Writes app.merchant_link_decisions + app.merchant_links; revert via "
        "app.audit_log (no undo tool yet). Discover pending decisions with "
        "merchants_links_pending.",
    )
    register(
        mcp,
        merchants_links_history,
        "merchants_links_history",
        "Recent merchant-link decisions (all statuses), newest first. "
        "Read-only. Filter by limit. Use merchants_links_pending for the "
        "active review queue.",
    )
