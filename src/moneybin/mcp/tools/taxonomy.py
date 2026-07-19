"""Normalized category and merchant taxonomy boundaries."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
from dataclasses import replace
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, JsonValue, StrictBool

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.errors import RecoveryAction, UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.mcp.write_contracts import (
    CategoryStateRequest,
    TaxonomyStateRequest,
)
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.categories import CategoryRow, MerchantRow
from moneybin.privacy.payloads.taxonomy import (
    TaxonomyCategoriesView,
    TaxonomyCoarsePayload,
    TaxonomyMerchantsView,
    TaxonomySetPayload,
    TaxonomyStateResult,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization import (
    CategorizationService,
    CategoryStateTarget,
    MerchantStateTarget,
    TaxonomyStateTarget,
    TaxonomyTargetPlan,
)
from moneybin.services.mutation_context import current_operation_id

_TAXONOMY_TOOL = "taxonomy"


def _taxonomy_cursor(
    view: str,
    offset: int,
    filters: dict[str, object],
) -> str:
    """Encode a cursor bound to the complete taxonomy query."""
    raw = json.dumps(
        {
            "filters": filters,
            "offset": offset,
            "tool": _TAXONOMY_TOOL,
            "view": view,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _taxonomy_offset(
    cursor: str | None,
    *,
    view: str,
    filters: dict[str, object],
) -> int:
    """Decode a cursor and reject cross-filter reuse."""
    if cursor is None:
        return 0
    try:
        decoded = base64.b64decode(cursor.encode(), altchars=b"-_", validate=True)
        value = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UserError(
            "Invalid taxonomy pagination cursor.",
            code="TAXONOMY_CURSOR_INVALID",
        ) from exc
    if not isinstance(value, dict):
        raise UserError(
            "Invalid taxonomy pagination cursor.",
            code="TAXONOMY_CURSOR_INVALID",
        )
    payload = cast(dict[str, Any], value)
    offset = payload.get("offset")
    if (
        set(payload) != {"filters", "offset", "tool", "view"}
        or payload.get("filters") != filters
        or payload.get("tool") != _TAXONOMY_TOOL
        or payload.get("view") != view
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
    ):
        raise UserError(
            "Invalid taxonomy pagination cursor.",
            code="TAXONOMY_CURSOR_INVALID",
        )
    return offset


def _taxonomy_envelope[T](
    data: T,
    *,
    contract_type: type[Any],
    total_count: int,
    returned_count: int,
    next_cursor: str | None,
    actions: list[str],
) -> ResponseEnvelope[T]:
    """Build one dynamically classified taxonomy envelope."""
    classes = extract_data_classes(contract_type)
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(T, redact_typed(data, None))
    envelope = cast(
        ResponseEnvelope[T],
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


def _contains_query(values: tuple[str | None, ...], query: str | None) -> bool:
    """Return whether any projected field contains the normalized query."""
    if query is None:
        return True
    return any(query in value.casefold() for value in values if value is not None)


def _category_rows(
    service: CategorizationService,
    *,
    include_inactive: bool,
    query: str | None,
) -> list[CategoryRow]:
    """Return deterministically filtered category rows."""
    rows = service.get_all_categories(include_inactive=include_inactive).categories
    selected = [
        row
        for row in rows
        if _contains_query(
            (
                row.category_id,
                row.category,
                row.subcategory,
                row.description,
            ),
            query,
        )
    ]
    return sorted(
        selected,
        key=lambda row: (
            (row.category or "").casefold(),
            (row.subcategory or "").casefold(),
            row.category_id,
        ),
    )


def _merchant_rows(
    service: CategorizationService,
    *,
    query: str | None,
) -> list[MerchantRow]:
    """Return deterministically filtered merchant rows."""
    rows = service.list_merchants().merchants
    selected = [
        row
        for row in rows
        if _contains_query(
            (
                row.merchant_id,
                row.raw_pattern,
                row.match_type,
                row.canonical_name,
                row.category,
                row.subcategory,
            ),
            query,
        )
    ]
    return sorted(
        selected,
        key=lambda row: (
            row.canonical_name.casefold(),
            row.merchant_id,
            (row.raw_pattern or "").casefold(),
        ),
    )


def _taxonomy_actions(
    *,
    view: Literal["categories", "merchants"],
    include_inactive: bool,
    query: str | None,
    limit: int,
    next_cursor: str | None,
) -> list[str]:
    """Return replacement-native navigation and continuation actions."""
    kind = {
        "categories": "category",
        "merchants": "merchant",
    }[view]
    actions = [f"Use taxonomy_set with kind={kind!r} to maintain this taxonomy"]
    if next_cursor is not None:
        actions.append(
            f"Continue with taxonomy(view={view!r}, "
            f"include_inactive={include_inactive!r}, query={query!r}, "
            f"limit={limit}, cursor='{next_cursor}')"
        )
    return actions


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.MEDIUM)
def taxonomy_coarse(
    view: Literal["categories", "merchants"] = "categories",
    include_inactive: StrictBool = False,
    query: str | None = None,
    limit: Annotated[int, Field(strict=True, ge=1)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[TaxonomyCoarsePayload]:
    """Read category taxonomy or merchant mappings with exact pagination."""
    if view == "merchants" and include_inactive:
        raise UserError(
            "include_inactive is only valid for the categories view.",
            code="TAXONOMY_INCLUDE_INACTIVE_NOT_ALLOWED",
        )
    canonical_query = query.casefold().strip() if query is not None else None
    filters: dict[str, object] = {
        "include_inactive": bool(include_inactive),
        "query": canonical_query,
    }
    offset = _taxonomy_offset(cursor, view=view, filters=filters)

    with get_database(read_only=True) as db:
        service = CategorizationService(db)
        if view == "categories":
            rows = _category_rows(
                service,
                include_inactive=bool(include_inactive),
                query=canonical_query,
            )
            page = rows[offset : offset + limit]
            payload: TaxonomyCoarsePayload = TaxonomyCategoriesView(rows=page)
            contract_type = TaxonomyCategoriesView
        else:
            rows = _merchant_rows(service, query=canonical_query)
            page = rows[offset : offset + limit]
            payload = TaxonomyMerchantsView(rows=page)
            contract_type = TaxonomyMerchantsView

    next_cursor = (
        _taxonomy_cursor(view, offset + limit, filters)
        if len(rows) > offset + limit
        else None
    )
    return _taxonomy_envelope(
        payload,
        contract_type=contract_type,
        total_count=len(rows),
        returned_count=len(page),
        next_cursor=next_cursor,
        actions=_taxonomy_actions(
            view=view,
            include_inactive=bool(include_inactive),
            query=query,
            limit=limit,
            next_cursor=next_cursor,
        ),
    )


def register_taxonomy_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard normalized taxonomy read."""
    register(
        mcp,
        taxonomy_coarse,
        "taxonomy",
        "Read categories or merchant mappings with deterministic filtering and "
        "exact cursor pagination.",
        privacy_actor="taxonomy",
    )


def _to_taxonomy_target(request: TaxonomyStateRequest) -> TaxonomyStateTarget:
    """Translate one strict transport request to the service target."""
    if isinstance(request, CategoryStateRequest):
        return CategoryStateTarget(
            state=request.state,
            category_id=request.category_id,
            category=request.category,
            subcategory=request.subcategory,
            description=request.description,
            force=bool(request.force),
        )
    return MerchantStateTarget(
        state=request.state,
        merchant_id=request.merchant_id,
        raw_pattern=request.raw_pattern,
        canonical_name=request.canonical_name,
        match_type=request.match_type,
        category=request.category,
        subcategory=request.subcategory,
    )


def _taxonomy_binding(
    items: list[TaxonomyStateRequest],
    plan: TaxonomyTargetPlan,
) -> ConfirmationBinding:
    """Bind the exact ordered request and complete resolved before-state."""
    before_state = json.loads(
        json.dumps(
            [
                {
                    "row": item.before_state,
                    "usage": item.usage,
                    "effective_usage": item.effective_usage,
                    "cascade_before": (
                        [
                            {
                                "store": group.store,
                                "rows": [
                                    {
                                        "target_id": row.target_id,
                                        "before_state": row.before_state,
                                    }
                                    for row in group.rows
                                ],
                            }
                            for group in item.category_delete.references
                        ]
                        if item.category_delete is not None
                        else []
                    ),
                }
                for item in plan.items
            ],
            default=str,
            separators=(",", ":"),
        )
    )
    arguments: dict[str, JsonValue] = {
        "items": [cast(JsonValue, item.model_dump(mode="json")) for item in items],
        "before_state": cast(JsonValue, before_state),
    }
    resolved_ids: list[str] = []
    for request, planned in zip(items, plan.items, strict=True):
        for value in (
            planned.target_id,
            request.category_id
            if isinstance(request, CategoryStateRequest)
            else request.merchant_id,
        ):
            if value is not None and value not in resolved_ids:
                resolved_ids.append(value)
    return ConfirmationBinding(
        arguments=arguments,
        resolved_ids=tuple(resolved_ids),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="taxonomy_set",
        blast_radius={
            "targets": len(items),
            "changed_targets": len(plan.changed),
            "explicit_hard_deletes": plan.explicit_hard_deletes,
            "cascade_hard_deletes": plan.cascade_hard_deletes,
            "hard_deletes": (plan.explicit_hard_deletes + plan.cascade_hard_deletes),
        },
    )


def _preview_taxonomy_targets(
    targets: list[TaxonomyStateTarget],
) -> TaxonomyTargetPlan:
    """Preflight one taxonomy batch on a read-only connection."""
    with get_database(read_only=True) as db:
        return CategorizationService(db).plan_taxonomy_targets(targets)


def _apply_taxonomy_targets(
    items: list[TaxonomyStateRequest],
    targets: list[TaxonomyStateTarget],
    *,
    grant: ConfirmationGrant | None,
    expected_binding: ConfirmationBinding,
) -> list[TaxonomyStateResult]:
    """Revalidate and apply taxonomy targets on one write connection."""
    with get_database(read_only=False) as db:
        service = CategorizationService(db)

        def verify(plan: TaxonomyTargetPlan) -> None:
            binding = _taxonomy_binding(items, plan)
            if grant is not None:
                grant.verify(binding)
            elif binding.canonical_bytes() != expected_binding.canonical_bytes():
                raise UserError(
                    "Taxonomy state changed after preflight.",
                    code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                )

        results = service.apply_taxonomy_targets(
            targets,
            actor="mcp",
            verify=verify,
        )
    return [
        TaxonomyStateResult(
            kind=result.kind,
            target_id=result.target_id,
            state=result.state,
            changed=result.changed,
        )
        for result in results
    ]


@mcp_tool(read_only=False, destructive=True, idempotent=True)
async def taxonomy_set_coarse(
    items: list[TaxonomyStateRequest],
    confirmation_token: str | None = None,
) -> ResponseEnvelope[TaxonomySetPayload]:
    """Atomically declare category and merchant mapping target states."""
    if not items:
        raise UserError(
            "items must contain at least one taxonomy target.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    targets = [_to_taxonomy_target(item) for item in items]
    plan = await asyncio.to_thread(_preview_taxonomy_targets, targets)
    binding = _taxonomy_binding(items, plan)
    if not plan.changed and confirmation_token is None:
        raise UserError(
            "Every taxonomy target already has its requested state.",
            code=error_codes.MUTATION_NOTHING_TO_DO,
        )
    grant: ConfirmationGrant | None = None
    if plan.destructive or confirmation_token is not None:
        grant = await grant_confirmation_or_raise(
            binding=binding if confirmation_token is None else None,
            message=(
                "Confirm this complete taxonomy batch. Hard-deleted category "
                "and merchant rows retain audit before-state and can be restored "
                "with system_audit_undo(operation_id)."
            ),
            confirmation_token=confirmation_token,
        )
    results = await asyncio.to_thread(
        _apply_taxonomy_targets,
        items,
        targets,
        grant=grant,
        expected_binding=binding,
    )
    operation_id = current_operation_id()
    return build_envelope(
        data=TaxonomySetPayload(results=results, operation_id=operation_id),
        recovery_actions=[
            RecoveryAction(
                tool="system_audit_undo",
                arguments={"operation_id": operation_id},
                rationale="Restore the audited taxonomy mutation.",
                confidence="certain",
                idempotent=False,
            )
        ],
    )


def register_taxonomy_coarse_writes(mcp: FastMCP) -> None:
    """Register the standard taxonomy target-state batch."""
    register(
        mcp,
        taxonomy_set_coarse,
        "taxonomy_set",
        "Atomically declare categories present, inactive, or absent and merchant "
        "mappings present or absent. The tool advertises maximum destructive "
        "risk and confirms only batches containing a resolved hard delete.",
        privacy_actor="taxonomy_set",
    )


def register_taxonomy_tools(mcp: FastMCP) -> None:
    """Register the standard taxonomy read and target-state boundaries."""
    register_taxonomy_coarse_reads(mcp)
    register_taxonomy_coarse_writes(mcp)
