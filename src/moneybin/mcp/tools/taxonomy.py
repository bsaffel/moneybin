"""Dormant normalized category and merchant taxonomy read."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import replace
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, StrictBool

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.categories import CategoryRow, MerchantRow
from moneybin.privacy.payloads.taxonomy import (
    TaxonomyCategoriesView,
    TaxonomyCoarsePayload,
    TaxonomyMerchantsView,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization import CategorizationService

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
    actions = (
        ["Use categories_create or categories_set to maintain the category taxonomy"]
        if view == "categories"
        else ["Use merchants_create to add merchant mappings"]
    )
    if next_cursor is not None:
        actions.append(
            f"Continue with taxonomy(view={view!r}, "
            f"include_inactive={include_inactive!r}, query={query!r}, "
            f"limit={limit}, cursor='{next_cursor}')"
        )
    return actions


@mcp_tool(dynamic_classification=True)
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
    """Register the dormant Plan 6 normalized taxonomy read."""
    register(
        mcp,
        taxonomy_coarse,
        "taxonomy",
        "Read categories or merchant mappings with deterministic filtering and "
        "exact cursor pagination.",
        privacy_actor="taxonomy",
    )
