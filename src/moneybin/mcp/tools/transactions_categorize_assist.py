"""MCP tool: transactions_categorize_assist — redacted batch for LLM categorization."""

from __future__ import annotations

import logging

from fastmcp import FastMCP

from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import audit_log
from moneybin.metrics.registry import CATEGORIZE_ASSIST_CALLS_TOTAL
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization_service import CategorizationService

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="medium", domain="categorize")
def transactions_categorize_assist(
    limit: int | None = None,
    account_filter: list[str] | None = None,
    date_range: dict[str, str] | None = None,
) -> ResponseEnvelope:
    """Return uncategorized transactions as redacted records for LLM categorization.

    The LLM proposes (category, subcategory, canonical_merchant_name) for each.
    The user reviews; the LLM commits via transactions_categorize_apply with
    categorized_by='ai'.

    Privacy: descriptions pass through redact_for_llm() before transmission.
    No amounts, dates, or account references are ever sent.

    Args:
        limit: Max records to return. Defaults to assist_default_batch_size (100).
        account_filter: Restrict to specific account IDs.
        date_range: Dict with "start" and "end" keys (ISO date strings).
    """
    settings = get_settings().categorization
    effective_limit = limit if limit is not None else settings.assist_default_batch_size

    date_tuple = None
    if date_range:
        if "start" not in date_range or "end" not in date_range:
            raise ValueError(
                "date_range must include both 'start' and 'end' keys "
                "(ISO date strings)."
            )
        date_tuple = (date_range["start"], date_range["end"])

    svc = CategorizationService(get_database())
    redacted = svc.categorize_assist(
        limit=effective_limit,
        account_filter=account_filter,
        date_range=date_tuple,
    )

    CATEGORIZE_ASSIST_CALLS_TOTAL.labels(surface="mcp").inc()

    audit_log(
        tool="transactions_categorize_assist",
        sensitivity="medium",
        metadata={
            "txn_count": len(redacted),
            "account_filter": account_filter,
        },
    )

    return build_envelope(
        data=[
            {
                "opaque_id": r.opaque_id,
                "description_redacted": r.description_redacted,
                "memo_redacted": r.memo_redacted,
                "source_type": r.source_type,
                "transaction_type": r.transaction_type,
                "check_number": r.check_number,
                "is_transfer": r.is_transfer,
                "transfer_pair_id": r.transfer_pair_id,
                "payment_channel": r.payment_channel,
                "amount_sign": r.amount_sign,
            }
            for r in redacted
        ],
        sensitivity="medium",
        actions=[
            "Propose (category, subcategory, canonical_merchant_name) per item",
            "Use transactions_categorize_apply to commit user-accepted proposals",
            "Redaction: description + memo redacted; structural fields exposed for matcher and LLM signal",
        ],
    )


def register_transactions_categorize_assist_tools(mcp: FastMCP) -> None:
    """Register transactions_categorize_assist with the FastMCP server."""
    register(
        mcp,
        transactions_categorize_assist,
        "transactions_categorize_assist",
        "Fetch uncategorized transactions as redacted records for LLM-assisted "
        "categorization. Descriptions are redacted; no amounts or account IDs sent.",
    )
