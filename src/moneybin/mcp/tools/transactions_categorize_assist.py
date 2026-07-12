"""MCP tool: transactions_categorize_assist — PII-scrubbed batch for LLM categorization."""

from __future__ import annotations

import logging

from fastmcp import FastMCP

from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import audit_log
from moneybin.metrics.registry import CATEGORIZE_ASSIST_CALLS_TOTAL
from moneybin.privacy.payloads.categorize import AssistRow, CatAssistPayload
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization import CategorizationService

logger = logging.getLogger(__name__)


@mcp_tool(domain="categorize")
def transactions_categorize_assist(
    limit: int | None = None,
    account_filter: list[str] | None = None,
    date_range: dict[str, str] | None = None,
) -> ResponseEnvelope[CatAssistPayload]:
    """Return uncategorized transactions as PII-scrubbed rows for LLM categorization.

    Merchant text is PRESERVED and sent to the model — it is the categorization
    signal, and stripping it would make the task impossible. What is scrubbed is
    embedded PII: account numbers in the memo are masked (e.g.
    "ON-LINE xxxxxxxxx5648"). No amounts, dates, or account identifiers are sent;
    only an amount SIGN.

    The LLM proposes (category, subcategory, canonical_merchant_name) for each.
    The user reviews; the LLM commits via transactions_categorize_commit with
    categorized_by='ai'.

    Sensitivity: medium — descriptions leave the machine.

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

    with get_database(read_only=True) as db:
        svc = CategorizationService(db)
        redacted = svc.categorize_assist(
            limit=effective_limit, account_filter=account_filter, date_range=date_tuple
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

    payload = CatAssistPayload(
        transactions=[
            AssistRow(
                transaction_id=r.transaction_id,
                description_scrubbed=r.description_scrubbed,
                memo_scrubbed=r.memo_scrubbed,
                source_type=r.source_type,
                transaction_type=r.transaction_type,
                check_number=r.check_number,
                is_transfer=r.is_transfer,
                transfer_pair_id=r.transfer_pair_id,
                payment_channel=r.payment_channel,
                amount_sign=r.amount_sign,
            )
            for r in redacted
        ]
    )
    return build_envelope(
        data=payload,
        actions=[
            "Propose (category, subcategory, canonical_merchant_name) per item",
            "Use transactions_categorize_commit to commit user-accepted proposals",
            "Scrubbing: description + memo have embedded PII masked, merchant text preserved; structural fields exposed for matcher and LLM signal",
        ],
    )


def register_transactions_categorize_assist_tools(mcp: FastMCP) -> None:
    """Register transactions_categorize_assist with the FastMCP server."""
    register(
        mcp,
        transactions_categorize_assist,
        "transactions_categorize_assist",
        "Fetch uncategorized transactions as PII-scrubbed records for LLM-assisted "
        "categorization. Merchant text (description/memo) IS sent in full — it's "
        "the categorization signal; only embedded PII (e.g. account numbers in "
        "the memo) is masked. No amounts, dates, or account IDs sent.",
    )
