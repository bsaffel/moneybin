"""LLM-assist bridge: redacted projection of uncategorized transactions.

Read-only against ``core.fct_transactions`` and ``app.transaction_categories``.
The :class:`RedactedTransaction` dataclass type-enforces the privacy contract
(no full amount, no date, no account ID); see
``docs/specs/categorization-cold-start.md`` §"Solver 3: LLM-assist" for the
end-to-end workflow.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Literal

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.metrics.registry import (
    CATEGORIZE_ASSIST_DURATION_SECONDS,
    CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL,
)
from moneybin.services._text import redact_for_llm
from moneybin.tables import FCT_TRANSACTIONS, TRANSACTION_CATEGORIES

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RedactedTransaction:
    """LLM-safe view of an uncategorized transaction.

    Type-enforces the redaction contract: no full amount, no date, no account ID.
    The v2 contract (per categorization-matching-mechanics.md §Match input) adds
    memo and structural-field signals. Adding any new field requires conscious
    code review — accidental PII leakage is a compile-time impossibility enforced
    by the frozen dataclass shape.
    """

    transaction_id: str
    description_redacted: str
    memo_redacted: str
    source_type: str
    transaction_type: str | None
    check_number: str | None
    is_transfer: bool
    transfer_pair_id: str | None
    payment_channel: str | None
    amount_sign: Literal["+", "-", "0"]


def _amount_sign_label(amount: float | None) -> Literal["+", "-", "0"]:
    """Map a raw amount to the LLM-facing sign signal.

    ``"0"`` covers both ``NULL`` (defective import) and zero amount (balance
    adjustments, voided rows). Mapping both to ``"+"`` biases the LLM toward
    income-side categories on rows that are neither income nor expense.
    """
    if amount is None or amount == 0:
        return "0"
    return "-" if amount < 0 else "+"


class AssistBridge:
    """Shapes uncategorized transactions into LLM-safe ``RedactedTransaction``s.

    Self-contained: does not call the matcher or applier collaborators. The
    facade owns construction and exposes ``categorize_assist`` as a thin
    pass-through. The redaction contract is owned by :class:`RedactedTransaction`.
    """

    def __init__(self, db: Database) -> None:
        """Bind the bridge to a database connection."""
        self._db = db

    def categorize_assist(
        self,
        limit: int = 100,
        account_filter: list[str] | None = None,
        date_range: tuple[str, str] | None = None,
    ) -> list[RedactedTransaction]:
        """Return uncategorized transactions as redacted records for LLM review.

        Sensitivity: medium. Output is sent to the user's LLM via MCP or
        written to disk via the CLI bridge. The redaction contract is enforced
        by RedactedTransaction's frozen dataclass shape (v2: description + memo
        redacted; structural fields exposed unredacted).
        """
        settings = get_settings().categorization
        effective_limit = min(limit, settings.assist_max_batch_size)

        where_clauses = ["tc.transaction_id IS NULL"]
        params: list[object] = []
        if account_filter:
            where_clauses.append(
                f"t.account_id IN ({','.join('?' * len(account_filter))})"
            )
            params.extend(account_filter)
        if date_range:
            where_clauses.append("t.transaction_date BETWEEN ? AND ?")
            params.extend(date_range)
        where_sql = " AND ".join(where_clauses)

        start = time.monotonic()
        result: list[RedactedTransaction] = []
        try:
            rows = self._db.execute(
                f"""
                SELECT t.transaction_id,
                       t.description,
                       t.memo,
                       t.source_type,
                       t.transaction_type,
                       t.check_number,
                       t.is_transfer,
                       t.transfer_pair_id,
                       t.payment_channel,
                       t.amount
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} tc USING (transaction_id)
                WHERE {where_sql}
                LIMIT ?
                """,  # noqa: S608  # where_sql composed from constants and parameter placeholders
                params + [effective_limit],
            ).fetchall()

            result = [
                RedactedTransaction(
                    transaction_id=row[0],
                    description_redacted=redact_for_llm(row[1] or ""),
                    memo_redacted=redact_for_llm(row[2] or ""),
                    source_type=row[3] or "",
                    transaction_type=row[4],
                    check_number=row[5],
                    is_transfer=bool(row[6]),
                    transfer_pair_id=row[7],
                    payment_channel=row[8],
                    amount_sign=_amount_sign_label(row[9]),
                )
                for row in rows
            ]
            return result
        finally:
            CATEGORIZE_ASSIST_DURATION_SECONDS.observe(time.monotonic() - start)
            CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL.inc(len(result))
