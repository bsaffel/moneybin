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

import asyncio
import base64
import binascii
import json
from dataclasses import replace
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, JsonValue

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.privacy.consent import ConsentMode
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.log import (
    MAX_LOG_ROWS,
    read_privacy_events,
    read_privacy_events_page,
)
from moneybin.privacy.payloads.consent import (
    ConsentGrantRow,
    ConsentMutationPayload,
    ConsentSetPayload,
    PrivacyCoarsePayload,
    PrivacyLogPayload,
    PrivacyLogRow,
    PrivacyLogView,
    PrivacyStatusPayload,
    PrivacyStatusView,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.consent_service import (
    ConsentService,
    ConsentTargetPlan,
    ConsentTargetResult,
)
from moneybin.services.mutation_context import current_operation_id
from moneybin.vocabulary import ConsentFeatureCategory


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
        mode: persistent (default) or one-time. NOTE: one-time enforcement is
            pending — a one-time grant currently persists until revoked.
            Re-granting an existing active category+backend is a no-op and does
            NOT change the mode; revoke then grant again to change it.
    """
    with get_database(read_only=False) as db:
        result = ConsentService(db).grant_consent(
            feature_category=category,
            backend=backend,
            consent_mode=ConsentMode(mode),
            actor="mcp.privacy_consent_set",
        )
    grant = result.grant
    return build_envelope(
        data=ConsentMutationPayload(
            feature_category=grant.feature_category,
            backend=grant.backend,
            consent_mode=grant.consent_mode.value,
            action="granted" if result.created else "noop",
        ),
        actions=["Use privacy(view='status') to see all active grants"],
    )


def privacy_consent_revoke(
    category: str, backend: str | None = None
) -> ResponseEnvelope[ConsentMutationPayload]:
    """Revoke consent for a category; takes effect immediately.

    Args:
        category: The feature category to revoke.
        backend: AI backend; defaults to the configured default backend.
    """
    with get_database(read_only=False) as db:
        result = ConsentService(db).revoke_consent(
            feature_category=category,
            backend=backend,
            actor="mcp.privacy_consent_set",
        )
    return build_envelope(
        data=ConsentMutationPayload(
            feature_category=category,
            backend=result.backend,
            consent_mode=None,
            action="revoked" if result.count else "noop",
        ),
        actions=["Use privacy(view='status') to confirm"],
    )


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
        actions=["Use privacy_consent_set(state='granted') to add consent"],
    )


def privacy_log(
    last: int = 50, actor: str | None = None
) -> ResponseEnvelope[PrivacyLogPayload]:
    """Return recent privacy log events (consent grants/revokes + tool calls).

    Args:
        last: Maximum number of events to return (matches the CLI --last flag;
            capped at 1000 server-side).
        actor: Optional exact-match actor filter.
    """
    filters: dict[str, object] = {}
    if actor is not None:
        filters["actor"] = actor
    effective = min(last, MAX_LOG_ROWS)
    events = read_privacy_events(filters, max_rows=effective)
    # A full page means more may exist — whether the caller's `last` or the
    # server cap (MAX_LOG_ROWS) was the limiter. Over-signalling "more" beats a
    # silent truncation; total_count is a floor (len+1), not an exact count
    # (an exact count would require a second full scan of the log).
    has_more = effective > 0 and len(events) >= effective
    return build_envelope(
        data=PrivacyLogPayload(
            events=[PrivacyLogRow.from_event(e) for e in events],
        ),
        total_count=len(events) + 1 if has_more else None,
    )


def _privacy_cursor(offset: int, *, snapshot_total: int) -> str:
    """Encode a cursor bound to the consolidated privacy log query."""
    raw = json.dumps(
        {
            "filters": {},
            "offset": offset,
            "snapshot_total": snapshot_total,
            "tool": "privacy",
            "view": "log",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _privacy_page_state(cursor: str | None) -> tuple[int, int | None]:
    """Decode a privacy cursor and reject malformed or cross-query reuse."""
    if cursor is None:
        return 0, None
    try:
        decoded = base64.b64decode(cursor.encode(), altchars=b"-_", validate=True)
        value = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UserError(
            "Invalid privacy pagination cursor.",
            code="PRIVACY_CURSOR_INVALID",
        ) from exc
    if not isinstance(value, dict):
        raise UserError(
            "Invalid privacy pagination cursor.",
            code="PRIVACY_CURSOR_INVALID",
        )
    payload = cast(dict[str, Any], value)
    offset = payload.get("offset")
    snapshot_total = payload.get("snapshot_total")
    if (
        set(payload) != {"filters", "offset", "snapshot_total", "tool", "view"}
        or payload.get("filters") != {}
        or payload.get("tool") != "privacy"
        or payload.get("view") != "log"
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or isinstance(snapshot_total, bool)
        or not isinstance(snapshot_total, int)
        or snapshot_total < 0
    ):
        raise UserError(
            "Invalid privacy pagination cursor.",
            code="PRIVACY_CURSOR_INVALID",
        )
    return offset, snapshot_total


def _privacy_coarse_envelope(
    data: PrivacyStatusView | PrivacyLogView,
    *,
    total_count: int,
    returned_count: int,
    next_cursor: str | None = None,
    actions: list[str] | None = None,
) -> ResponseEnvelope[PrivacyCoarsePayload]:
    """Build and redact one dynamically classified privacy view."""
    contract_type = type(data)
    classes = extract_data_classes(contract_type)
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(PrivacyStatusView | PrivacyLogView, redact_typed(data, None))
    envelope = cast(
        ResponseEnvelope[PrivacyCoarsePayload],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            total_count=total_count,
            returned_count=returned_count,
            next_cursor=next_cursor,
            actions=actions,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )
    return replace(
        envelope,
        summary=replace(envelope.summary, has_more=next_cursor is not None),
    )


@mcp_tool(
    domain="privacy",
    dynamic_classification=True,
    maximum_sensitivity=Sensitivity.LOW,
)
def privacy_coarse(
    view: Literal["status", "log"] = "status",
    limit: Annotated[int, Field(strict=True, ge=1, le=MAX_LOG_ROWS)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[PrivacyCoarsePayload]:
    """Read consent status or a deterministic page of privacy-log events."""
    if view == "status":
        if limit != 100 or cursor is not None:
            raise UserError(
                "Privacy status does not accept pagination overrides.",
                code="PRIVACY_PAGINATION_NOT_ALLOWED",
            )
        with get_database(read_only=True) as db:
            status = ConsentService(db).status()
        data = PrivacyStatusView(
            default_backend=status.default_backend,
            consent_policy=status.consent_policy,
            active_grants=[
                ConsentGrantRow(
                    feature_category=grant.feature_category,
                    backend=grant.backend,
                    consent_mode=grant.consent_mode.value,
                    granted_at=str(grant.granted_at),
                )
                for grant in status.active_grants
            ],
        )
        return _privacy_coarse_envelope(
            data,
            total_count=len(data.active_grants),
            returned_count=len(data.active_grants),
            actions=["Use privacy_consent_set(state='granted') to add consent"],
        )

    offset, snapshot_total = _privacy_page_state(cursor)
    try:
        events, total_count = read_privacy_events_page(
            {},
            limit=limit,
            offset=offset,
            snapshot_total=snapshot_total,
        )
    except ValueError as exc:
        raise UserError(
            "Invalid privacy pagination cursor.",
            code="PRIVACY_CURSOR_INVALID",
        ) from exc
    rows = [PrivacyLogRow.from_event(event) for event in events]
    next_cursor = (
        _privacy_cursor(
            offset + len(rows),
            snapshot_total=total_count,
        )
        if offset + len(rows) < total_count
        else None
    )
    actions = (
        [f"Continue with privacy(view='log', limit={limit}, cursor={next_cursor!r})"]
        if next_cursor is not None
        else []
    )
    return _privacy_coarse_envelope(
        PrivacyLogView(events=rows),
        total_count=total_count,
        returned_count=len(rows),
        next_cursor=next_cursor,
        actions=actions,
    )


def register_privacy_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard privacy read."""
    register(
        mcp,
        privacy_coarse,
        "privacy",
        "Read active AI consent status or exact, cursor-paginated privacy log "
        "events. Privacy status does not accept pagination arguments.",
        privacy_actor="privacy",
    )


def _consent_binding(plan: ConsentTargetPlan) -> ConfirmationBinding:
    """Bind exact effective consent categories, backend, and before-state."""
    before = [
        {
            "grant_id": grant.grant_id,
            "feature_category": grant.feature_category,
            "backend": grant.backend,
            "consent_mode": grant.consent_mode.value,
            "granted_at": grant.granted_at.isoformat(),
            "revoked_at": (
                grant.revoked_at.isoformat() if grant.revoked_at is not None else None
            ),
        }
        for grant in plan.before
    ]
    arguments: dict[str, JsonValue] = {
        "categories": list(plan.categories),
        "state": plan.state,
        "backend": plan.backend,
        "mode": plan.mode.value if plan.mode is not None else None,
        "before_state": cast(JsonValue, before),
    }
    return ConfirmationBinding(
        arguments=arguments,
        resolved_ids=tuple(grant.grant_id for grant in plan.before),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="privacy_consent_set",
        blast_radius={
            "categories": len(plan.categories),
            "changed_grants": len(plan.changed_categories),
            "revoked_grants": (
                len(plan.changed_categories) if plan.state == "revoked" else 0
            ),
        },
    )


def _preview_consent_targets(
    categories: list[str],
    *,
    state: Literal["granted", "revoked"],
    backend: str | None,
    mode: ConsentMode,
) -> ConsentTargetPlan:
    """Preflight one consent batch on a read-only connection."""
    with get_database(read_only=True) as db:
        return ConsentService(db).plan_targets(
            categories,
            state=state,
            backend=backend,
            mode=mode,
        )


def _apply_consent_targets(
    plan: ConsentTargetPlan,
    *,
    operation_id: str,
    grant: ConfirmationGrant | None,
    expected_binding: ConfirmationBinding,
) -> ConsentTargetResult:
    """Revalidate and apply one consent batch through the shared service."""
    with get_database(read_only=False) as db:
        service = ConsentService(db)

        def verify(live: ConsentTargetPlan) -> None:
            binding = _consent_binding(live)
            if grant is not None:
                grant.verify(binding)
            elif binding.canonical_bytes() != expected_binding.canonical_bytes():
                raise UserError(
                    "Consent state changed after preflight.",
                    code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                )

        return service.apply_targets(
            plan,
            actor="mcp.privacy_consent_set",
            operation_id=operation_id,
            verify=verify,
        )


@mcp_tool(
    domain="privacy",
    read_only=False,
    destructive=True,
    idempotent=True,
)
async def privacy_consent_set_coarse(
    categories: list[ConsentFeatureCategory],
    state: Literal["granted", "revoked"],
    backend: str | None = None,
    mode: Literal["persistent", "one-time"] = "persistent",
    confirmation_token: str | None = None,
) -> ResponseEnvelope[ConsentSetPayload]:
    """Atomically declare consent granted or revoked for feature categories."""
    if state == "revoked" and mode != "persistent":
        raise UserError(
            "mode is only valid when granting consent.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    operation_id = current_operation_id()
    plan = await asyncio.to_thread(
        _preview_consent_targets,
        list(categories),
        state=state,
        backend=backend,
        mode=ConsentMode(mode),
    )
    if confirmation_token is not None and state != "revoked":
        raise UserError(
            "confirmation_token is only valid for consent revocation.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if not plan.changed and confirmation_token is None:
        raise UserError(
            "Every consent category already has its requested state.",
            code=error_codes.MUTATION_NOTHING_TO_DO,
        )
    binding = _consent_binding(plan)
    grant: ConfirmationGrant | None = None
    if state == "revoked":
        grant = await grant_confirmation_or_raise(
            binding=binding if confirmation_token is None else None,
            message=(
                "Revoke consent for these exact feature categories and backend? "
                "The retained audit ledger can be inspected with system_audit."
            ),
            confirmation_token=confirmation_token,
        )
    result = await asyncio.to_thread(
        _apply_consent_targets,
        plan,
        operation_id=operation_id,
        grant=grant,
        expected_binding=binding,
    )
    return build_envelope(
        data=ConsentSetPayload(
            categories=list(result.plan.categories),
            state=result.plan.state,
            backend=result.plan.backend,
            consent_mode=(
                result.plan.mode.value if result.plan.mode is not None else None
            ),
            effective_categories=list(result.effective_categories),
            operation_id=operation_id,
        ),
        actions=["Use privacy(view='status') to inspect active consent"],
    )


def register_privacy_coarse_writes(mcp: FastMCP) -> None:
    """Register the standard declarative consent batch."""
    register(
        mcp,
        privacy_consent_set_coarse,
        "privacy_consent_set",
        "Atomically grant or revoke consent for one or more feature categories. "
        "Grant is runtime non-destructive and idempotent; revoke confirms the "
        "exact normalized category set, backend, actor, profile, and live grants.",
        privacy_actor="privacy_consent_set",
        input_schema_extra={
            "allOf": [
                {
                    "if": {
                        "properties": {"state": {"const": "revoked"}},
                        "required": ["state"],
                    },
                    "then": {
                        "not": {"anyOf": [{"required": ["mode"]}]},
                    },
                },
                {
                    "if": {
                        "properties": {"state": {"const": "granted"}},
                        "required": ["state"],
                    },
                    "then": {
                        "not": {
                            "anyOf": [{"required": ["confirmation_token"]}],
                        },
                    },
                },
            ]
        },
    )


def register_privacy_tools(mcp: FastMCP) -> None:
    """Register the standard privacy read and consent boundaries."""
    register_privacy_coarse_reads(mcp)
    register_privacy_coarse_writes(mcp)
