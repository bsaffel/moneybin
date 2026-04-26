"""Auto-rule proposal lifecycle: pattern extraction, dedup, promotion, override detection.

Hooks into CategorizationService.bulk_categorize() to capture user categorization
patterns, stage them as proposals in app.proposed_rules, and promote approved
proposals into active rules in app.categorization_rules with created_by='auto_rule'.
"""

import logging
import uuid
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


def _active_rule_covers(db: Database, pattern: str) -> bool:
    """True when an active categorization rule already matches this pattern (case-insensitive exact pattern compare)."""
    row = db.execute(
        f"""
        SELECT 1 FROM {CATEGORIZATION_RULES.full_name}
        WHERE is_active = true AND LOWER(merchant_pattern) = LOWER(?)
        LIMIT 1
        """,
        [pattern],
    ).fetchone()
    return row is not None


def _merchant_mapping_covers(db: Database, pattern: str, category: str) -> bool:
    """True when a merchant mapping already produces this category for this pattern."""
    try:
        row = db.execute(
            f"""
            SELECT 1 FROM {MERCHANTS.full_name}
            WHERE LOWER(canonical_name) = LOWER(?) AND category = ?
            LIMIT 1
            """,
            [pattern, category],
        ).fetchone()
    except duckdb.CatalogException:
        return False
    return row is not None


def _find_pending_proposal(
    db: Database, pattern: str
) -> tuple[str, str, str | None, int, list[str]] | None:
    row = db.execute(
        f"""
        SELECT proposed_rule_id, category, subcategory, trigger_count, sample_txn_ids
        FROM {PROPOSED_RULES.full_name}
        WHERE LOWER(merchant_pattern) = LOWER(?) AND status IN ('pending', 'tracking')
        ORDER BY proposed_at DESC LIMIT 1
        """,
        [pattern],
    ).fetchone()
    if not row:
        return None
    return row[0], row[1], row[2], int(row[3]), list(row[4] or [])


def record_categorization(
    db: Database,
    transaction_id: str,
    category: str,
    *,
    subcategory: str | None = None,
) -> str | None:
    """Record a categorization event for auto-rule learning.

    Returns the proposed_rule_id if a proposal was created or updated,
    None if the categorization was filtered out (covered by existing rule/merchant
    or pattern unavailable).
    """
    pattern = extract_pattern(db, transaction_id)
    if not pattern:
        return None

    if _active_rule_covers(db, pattern):
        return None
    if _merchant_mapping_covers(db, pattern, category):
        return None

    threshold = get_settings().categorization.auto_rule_proposal_threshold
    existing = _find_pending_proposal(db, pattern)

    if existing is not None:
        proposed_rule_id, existing_category, existing_subcategory, count, samples = (
            existing
        )
        if existing_category == category and existing_subcategory == subcategory:
            new_samples = (
                samples + [transaction_id] if transaction_id not in samples else samples
            )
            new_samples = new_samples[:SAMPLE_TXN_CAP]
            new_count = count + 1
            new_status = "pending" if new_count >= threshold else "tracking"
            db.execute(
                f"""
                UPDATE {PROPOSED_RULES.full_name}
                SET trigger_count = ?, sample_txn_ids = ?, status = ?
                WHERE proposed_rule_id = ?
                """,
                [new_count, new_samples, new_status, proposed_rule_id],
            )
            return proposed_rule_id
        # Different category: supersede the old proposal, fall through to create a new one
        db.execute(
            f"UPDATE {PROPOSED_RULES.full_name} SET status = 'superseded' WHERE proposed_rule_id = ?",
            [proposed_rule_id],
        )

    proposed_rule_id = uuid.uuid4().hex[:12]
    initial_status = "pending" if threshold <= 1 else "tracking"
    db.execute(
        f"""
        INSERT INTO {PROPOSED_RULES.full_name}
        (proposed_rule_id, merchant_pattern, match_type, category, subcategory,
         status, trigger_count, source, sample_txn_ids)
        VALUES (?, ?, 'contains', ?, ?, ?, 1, 'pattern_detection', ?)
        """,
        [
            proposed_rule_id,
            pattern,
            category,
            subcategory,
            initial_status,
            [transaction_id],
        ],
    )
    return proposed_rule_id
