"""Export uncategorized transactions for LLM-assisted categorization."""

import json
import logging
import sys
from pathlib import Path

import typer

from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database

logger = logging.getLogger(__name__)


def categorize_export_uncategorized(
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Write JSON to this file path instead of stdout.",
    ),
    account_filter: list[str] | None = typer.Option(
        None,
        "--account-filter",
        help="Restrict to transactions in these account IDs (repeatable).",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum transactions to export (defaults to configured assist_default_batch_size).",
    ),
) -> None:
    """Export uncategorized transactions as redacted JSON for LLM review.

    Output is a JSON array of objects with the full ``RedactedTransaction``
    shape — ``transaction_id``, ``description_redacted``, ``memo_redacted``,
    ``source_type``, plus structural signals (``transaction_type``,
    ``check_number``, ``is_transfer``, ``transfer_pair_id``,
    ``payment_channel``, ``amount_sign``). No amounts, dates, or account
    identifiers. Feed the output to an LLM, fill in category/subcategory,
    then pipe back through ``moneybin transactions categorize apply-from-file``
    (extra export keys are stripped at the apply boundary).
    """
    from moneybin.config import get_settings
    from moneybin.mcp.privacy import audit_log
    from moneybin.metrics.registry import CATEGORIZE_ASSIST_CALLS_TOTAL
    from moneybin.services.categorization_service import CategorizationService

    with handle_cli_errors():
        with get_database() as db:
            svc = CategorizationService(db)
            effective_limit = (
                limit
                if limit is not None
                else get_settings().categorization.assist_default_batch_size
            )
            rows = svc.categorize_assist(
                limit=effective_limit,
                account_filter=account_filter or None,
            )

    CATEGORIZE_ASSIST_CALLS_TOTAL.labels(surface="cli").inc()

    audit_log(
        tool="categorize_export_uncategorized_cli",
        sensitivity="medium",
        metadata={
            "txn_count": len(rows),
            "account_filter": list(account_filter) if account_filter else None,
        },
    )

    payload = [
        {
            "transaction_id": row.transaction_id,
            "description_redacted": row.description_redacted,
            "memo_redacted": row.memo_redacted,
            "source_type": row.source_type,
            "transaction_type": row.transaction_type,
            "check_number": row.check_number,
            "is_transfer": row.is_transfer,
            "transfer_pair_id": row.transfer_pair_id,
            "payment_channel": row.payment_channel,
            "amount_sign": row.amount_sign,
        }
        for row in rows
    ]
    json_text = json.dumps(payload, indent=2)

    if output is not None:
        output.write_text(json_text, encoding="utf-8")
        logger.info(
            f"✅ Exported {len(payload)} uncategorized transactions to {output}"
        )
    else:
        sys.stdout.write(json_text + "\n")
