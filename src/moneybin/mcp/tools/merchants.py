"""Merchants namespace tools — merchant name mapping reference data.

Tools:
    - merchants_list — List all merchant name mappings (low sensitivity)
    - merchants_create — Create merchant name mappings in bulk (low sensitivity)
"""

from __future__ import annotations

import logging
import typing

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization_service import CategorizationService, MatchType
from moneybin.tables import MERCHANTS

logger = logging.getLogger(__name__)

_VALID_MATCH_TYPES: frozenset[MatchType] = frozenset(typing.get_args(MatchType))


def _validate_match_type(match_type: str) -> MatchType:
    """Validate and narrow a match_type string at the MCP boundary."""
    if match_type not in _VALID_MATCH_TYPES:
        raise ValueError(
            f"Invalid match_type: '{match_type}'. "
            f"Must be one of: {', '.join(sorted(_VALID_MATCH_TYPES))}"
        )
    return match_type  # type: ignore[return-value]  # validated above


@mcp_tool(sensitivity="low", domain="categorize")
def merchants_list() -> ResponseEnvelope:
    """List all merchant name mappings.

    Returns merchant ID, raw pattern, match type, canonical name,
    and associated category. Merchant mappings normalize transaction
    descriptions and provide default categories.
    """
    import duckdb

    db = get_database()
    try:
        rows = db.execute(
            f"""
            SELECT merchant_id, raw_pattern, match_type,
                   canonical_name, category, subcategory
            FROM {MERCHANTS.full_name}
            ORDER BY canonical_name
            """
        ).fetchall()
    except duckdb.CatalogException:
        rows = []

    data = [
        {
            "merchant_id": r[0],
            "raw_pattern": r[1],
            "match_type": r[2],
            "canonical_name": r[3],
            "category": r[4],
            "subcategory": r[5],
        }
        for r in rows
    ]
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use merchants_create to add new merchant mappings",
        ],
    )


@mcp_tool(sensitivity="low", domain="categorize")
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
            match_type = _validate_match_type(raw_match_type)
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
            logger.exception(f"create_merchants failed for {canonical_name!r}")
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
        "normalization and auto-categorization.",
    )
