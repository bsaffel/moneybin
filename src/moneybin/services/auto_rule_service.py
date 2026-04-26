"""Auto-rule proposal lifecycle: pattern extraction, dedup, promotion, override detection.

Hooks into CategorizationService.bulk_categorize() to capture user categorization
patterns, stage them as proposals in app.proposed_rules, and promote approved
proposals into active rules in app.categorization_rules with created_by='auto_rule'.
"""

# ruff: noqa: F401
import logging
from typing import Literal

import duckdb

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.services.categorization_service import normalize_description
from moneybin.tables import (
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    PROPOSED_RULES,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)

ProposalStatus = Literal["pending", "approved", "rejected", "superseded", "tracking"]
SAMPLE_TXN_CAP = 5


def extract_pattern(db: Database, transaction_id: str) -> str | None:
    """Extract a merchant-first pattern for the given transaction.

    Returns the canonical merchant name if a merchant_id is recorded on the
    transaction_categories row; otherwise falls back to a normalized description.
    Returns None if neither is available.
    """
    row = db.execute(
        f"SELECT merchant_id FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    merchant_id = row[0] if row else None
    if merchant_id:
        m = db.execute(
            f"SELECT canonical_name FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        if m and m[0]:
            return str(m[0])

    desc_row = db.execute(
        f"SELECT description FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    if not desc_row or not desc_row[0]:
        return None
    cleaned = normalize_description(str(desc_row[0]))
    return cleaned or None
