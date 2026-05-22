"""Privacy namespace tools — AI consent ledger.

Tools:
    - privacy_consent_grant  — record consent to share an AI feature category
    - privacy_consent_revoke — revoke consent
    - privacy_status         — active grants + configured backend
    - privacy_log            — recent privacy log events

The consent enforcement gate (degrading responses without consent) is
deferred — these tools record and report consent only.
"""

from __future__ import annotations

from typing import Literal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.consent import ConsentMode
from moneybin.privacy.log import read_privacy_events
from moneybin.privacy.payloads.consent import (
    ConsentGrantRow,
    ConsentMutationPayload,
    PrivacyLogPayload,
    PrivacyLogRow,
    PrivacyStatusPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.consent_service import ConsentService


@mcp_tool(domain="privacy", read_only=False)
def privacy_consent_grant(
    category: str,
    backend: str | None = None,
    mode: Literal["persistent", "one-time"] = "persistent",
) -> ResponseEnvelope[ConsentMutationPayload]:
    """Record consent to share a category of data with your AI backend.

    Granting this means MoneyBin may return real <category> data (e.g.
    transaction descriptions, amounts, dates) to the configured AI
    backend when you ask questions that need it. Account numbers, routing
    numbers, and other CRITICAL fields ALWAYS remain masked — consent
    never unmasks them. Consent is per (category, backend).

    Args:
        category: One of mcp-data-sharing, smart-import-parsing,
            ml-categorization, matching-overview.
        backend: AI backend; defaults to the configured default backend.
        mode: persistent (default) or one-time.
    """
    with get_database() as db:
        result = ConsentService(db).grant_consent(
            feature_category=category,
            backend=backend,
            consent_mode=ConsentMode(mode),
            actor="mcp.privacy_consent_grant",
        )
    grant = result.grant
    return build_envelope(
        data=ConsentMutationPayload(
            feature_category=grant.feature_category,
            backend=grant.backend,
            consent_mode=grant.consent_mode.value,
            action="granted" if result.created else "noop",
        ),
        actions=["Use privacy_status to see all active grants"],
    )


@mcp_tool(domain="privacy", read_only=False)
def privacy_consent_revoke(
    category: str, backend: str | None = None
) -> ResponseEnvelope[ConsentMutationPayload]:
    """Revoke consent for a category; takes effect immediately.

    Args:
        category: The feature category to revoke.
        backend: AI backend; defaults to the configured default backend.
    """
    with get_database() as db:
        result = ConsentService(db).revoke_consent(
            feature_category=category,
            backend=backend,
            actor="mcp.privacy_consent_revoke",
        )
    return build_envelope(
        data=ConsentMutationPayload(
            feature_category=category,
            backend=result.backend,
            consent_mode=None,
            action="revoked" if result.count else "noop",
        ),
        actions=["Use privacy_status to confirm"],
    )


@mcp_tool(domain="privacy", read_only=True)
def privacy_status() -> ResponseEnvelope[PrivacyStatusPayload]:
    """Show active AI consent grants, the configured backend, and consent policy."""
    with get_database(read_only=True) as db:
        status = ConsentService(db).status()
    return build_envelope(
        data=PrivacyStatusPayload(
            default_backend=status.default_backend,
            consent_policy=status.consent_policy,
            active_grants=[
                ConsentGrantRow(
                    feature_category=g.feature_category,
                    backend=g.backend,
                    consent_mode=g.consent_mode.value,
                    granted_at=str(g.granted_at),
                )
                for g in status.active_grants
            ],
        ),
        actions=["Use privacy_consent_grant to add consent"],
    )


@mcp_tool(domain="privacy", read_only=True)
def privacy_log(
    last_n: int = 50, actor: str | None = None
) -> ResponseEnvelope[PrivacyLogPayload]:
    """Return recent privacy log events (consent grants/revokes + tool calls).

    Args:
        last_n: Maximum number of events to return.
        actor: Optional exact-match actor filter.
    """
    filters: dict[str, object] = {}
    if actor:
        filters["actor"] = actor
    events = read_privacy_events(filters, max_rows=last_n)
    return build_envelope(
        data=PrivacyLogPayload(
            events=[
                PrivacyLogRow(
                    ts=str(e.get("ts", "")),
                    action=str(e.get("action", "")),
                    actor=str(e.get("actor", "")),
                    feature_category=str(e.get("feature_category", "")),
                    backend=str(e.get("backend", "")),
                )
                for e in events
            ]
        ),
    )


def register_privacy_tools(mcp: FastMCP) -> None:
    """Register privacy namespace tools with the FastMCP server."""
    register(
        mcp,
        privacy_consent_grant,
        "privacy_consent_grant",
        "Record consent to share a data category (mcp-data-sharing, "
        "smart-import-parsing, ml-categorization, matching-overview) with your "
        "AI backend. CRITICAL fields (account/routing numbers) always stay "
        "masked. Writes app.ai_consent_grants (one active grant per "
        "category+backend; idempotent); revert with privacy_consent_revoke.",
    )
    register(
        mcp,
        privacy_consent_revoke,
        "privacy_consent_revoke",
        "Revoke consent for a data category; effective immediately. Writes "
        "app.ai_consent_grants (sets revoked_at; row retained for audit); "
        "revert by calling privacy_consent_grant again.",
    )
    register(
        mcp,
        privacy_status,
        "privacy_status",
        "Show active AI consent grants, the configured backend, and consent "
        "policy (standard/strict). Read-only.",
    )
    register(
        mcp,
        privacy_log,
        "privacy_log",
        "Return recent privacy log events (consent grants/revokes and tool "
        "calls). Read-only metadata; no financial data.",
    )
