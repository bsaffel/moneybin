"""Merchants namespace tools — merchant name mapping reference data.

Tools:
    - merchants_list — List all merchant name mappings (low sensitivity)
    - merchants_create — Create merchant name mappings (low sensitivity)
"""

from __future__ import annotations

import logging

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization_service import (
    CategorizationService,
    validate_match_type,
)

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low")
def merchants_list() -> ResponseEnvelope:
    """List all merchant name mappings.

    Returns merchant ID, raw pattern, match type, canonical name,
    and associated category. Merchant mappings normalize transaction
    descriptions and provide default categories.
    """
    data = CategorizationService(get_database()).list_merchants()
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use merchants_create to add new merchant mappings",
        ],
    )


@mcp_tool(sensitivity="low", read_only=False, idempotent=False)
def merchants_create(
    merchants: list[dict[str, str | None]],
) -> ResponseEnvelope:
    """Create multiple merchant name mappings in one call.

    Each merchant dict should have ``raw_pattern`` and ``canonical_name``.
    Optional fields: ``match_type`` (default 'contains'), ``category``,
    ``subcategory``.

    Args:
        merchants: List of merchant mapping dicts.
    """
    if not merchants:
        return build_envelope(
            data={"created": 0, "skipped": 0, "error_details": []},
            sensitivity="low",
        )

    service = CategorizationService(get_database())
    created = 0
    skipped = 0
    error_details: list[dict[str, str]] = []

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
        data={
            "created": created,
            "skipped": skipped,
            "error_details": error_details,
        },
        sensitivity="low",
        total_count=len(merchants),
        actions=[
            "Use merchants_list to review all merchant mappings",
        ],
    )


def register_merchants_tools(mcp: FastMCP) -> None:
    """Register all merchants namespace tools with the FastMCP server."""
    register(
        mcp,
        merchants_list,
        "merchants_list",
        "List all merchant name mappings.",
    )
    register(
        mcp,
        merchants_create,
        "merchants_create",
        "Create multiple merchant name mappings for description "
        "normalization and auto-categorization. "
        "Writes app.user_merchants; no built-in delete tool — revert by editing or repointing the row directly via SQL.",
    )
