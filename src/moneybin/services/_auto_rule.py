"""Auto-rule proposal lifecycle: pattern extraction, dedup, promotion, override detection.

Hooks into CategorizationService.bulk_categorize() to capture user categorization
patterns, stage them as proposals in app.proposed_rules, and promote approved
proposals into active rules in app.categorization_rules with created_by='auto_rule'.
"""

import logging
import uuid
from dataclasses import dataclass, field
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


def extract_pattern(db: Database, transaction_id: str) -> str | None:
    """Extract a merchant-first pattern for the given transaction.

    When the transaction has a resolved merchant_id, returns the merchant's
    ``raw_pattern`` — the substring that actually matches statement descriptions
    (e.g., 'AMZN'), not the canonical display name (e.g., 'Amazon'). This guarantees
    that the resulting auto-rule's `contains` match will fire against future imports.
    Falls back to a normalized description when no merchant is associated.
    Returns None when neither is available.
    """
    row = db.execute(
        f"SELECT merchant_id FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    merchant_id = row[0] if row else None
    if merchant_id:
        m = db.execute(
            f"SELECT raw_pattern FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
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
    """True when a merchant mapping already produces this category for this pattern.

    Compares ``raw_pattern`` (the field actually used to match descriptions)
    rather than ``canonical_name`` (a display label). ``extract_pattern``
    returns ``raw_pattern`` when a merchant exists, so this comparison is
    direct; for description-derived patterns we additionally check whether
    the pattern contains the merchant's raw_pattern as a substring.
    """
    try:
        row = db.execute(
            f"""
            SELECT 1 FROM {MERCHANTS.full_name}
            WHERE category = ?
              AND (
                LOWER(raw_pattern) = LOWER(?)
                OR POSITION(LOWER(raw_pattern) IN LOWER(?)) > 0
              )
            LIMIT 1
            """,
            [category, pattern, pattern],
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

    settings = get_settings().categorization
    threshold = settings.auto_rule_proposal_threshold
    sample_cap = settings.auto_rule_sample_txn_cap
    existing = _find_pending_proposal(db, pattern)

    # Merchant coverage is only a reason to skip when there is no
    # in-progress proposal for this pattern. Otherwise tracking proposals
    # could be permanently stuck below threshold once bulk_categorize
    # creates the merchant mapping during the first categorization.
    if existing is None and _merchant_mapping_covers(db, pattern, category):
        return None

    if existing is not None:
        proposed_rule_id, existing_category, existing_subcategory, count, samples = (
            existing
        )
        if existing_category == category and existing_subcategory == subcategory:
            new_samples = (
                samples + [transaction_id] if transaction_id not in samples else samples
            )
            new_samples = new_samples[:sample_cap]
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


@dataclass(slots=True)
class ApproveResult:
    """Result of an approve() call: counts of approved, skipped, and newly categorized transactions."""

    approved: int = 0
    skipped: int = 0
    newly_categorized: int = 0
    rule_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RejectResult:
    """Result of a reject() call: counts of rejected and skipped proposals."""

    rejected: int = 0
    skipped: int = 0


def _categorize_existing_with_rule(
    db: Database, rule_id: str, pattern: str, category: str, subcategory: str | None
) -> int:
    """Run the new rule against currently-uncategorized matching transactions. Returns count categorized."""
    rows = db.execute(
        f"""
        SELECT t.transaction_id
        FROM {FCT_TRANSACTIONS.full_name} t
        LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c ON t.transaction_id = c.transaction_id
        WHERE c.transaction_id IS NULL
          AND t.description IS NOT NULL
          AND POSITION(LOWER(?) IN LOWER(t.description)) > 0
        """,
        [pattern],
    ).fetchall()
    if not rows:
        return 0
    db.executemany(
        f"""
        INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
        (transaction_id, category, subcategory, categorized_at, categorized_by, rule_id, confidence)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'auto_rule', ?, 1.0)
        """,
        [[r[0], category, subcategory, rule_id] for r in rows],
    )
    return len(rows)


def approve(db: Database, proposed_rule_ids: list[str]) -> ApproveResult:
    """Promote pending proposals to active rules and immediately categorize matching transactions."""
    settings = get_settings().categorization
    result = ApproveResult()

    for pid in proposed_rule_ids:
        row = db.execute(
            f"""
            SELECT merchant_pattern, match_type, category, subcategory, status
            FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?
            """,
            [pid],
        ).fetchone()
        if not row or row[4] != "pending":
            result.skipped += 1
            continue

        pattern, match_type, category, subcategory, _status = row
        rule_id = uuid.uuid4().hex[:12]
        db.execute(
            f"""
            INSERT INTO {CATEGORIZATION_RULES.full_name}
            (rule_id, name, merchant_pattern, match_type, category, subcategory,
             priority, is_active, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, true, 'auto_rule', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
                rule_id,
                f"auto: {pattern}",
                pattern,
                match_type,
                category,
                subcategory,
                settings.auto_rule_default_priority,
            ],
        )
        db.execute(
            f"""
            UPDATE {PROPOSED_RULES.full_name}
            SET status = 'approved', decided_at = CURRENT_TIMESTAMP, decided_by = 'user'
            WHERE proposed_rule_id = ?
            """,
            [pid],
        )
        newly = _categorize_existing_with_rule(
            db, rule_id, pattern, category, subcategory
        )
        result.approved += 1
        result.rule_ids.append(rule_id)
        result.newly_categorized += newly

    if result.approved:
        logger.info(
            f"Approved {result.approved} auto-rule proposal(s); "
            f"{result.newly_categorized} existing transaction(s) categorized"
        )
    return result


def check_overrides(db: Database) -> int:
    """Deactivate auto-rules with override count >= configured threshold; return number deactivated.

    An override = a transaction whose description matches the auto-rule's pattern
    but is currently categorized by 'user' with a different category. When the
    threshold is reached we deactivate the rule, mark its source proposal superseded,
    and create a new pending proposal with the most common override category.
    """
    settings = get_settings().categorization
    threshold = settings.auto_rule_override_threshold

    rules = db.execute(
        f"""
        SELECT rule_id, merchant_pattern, category, created_at
        FROM {CATEGORIZATION_RULES.full_name}
        WHERE is_active = true AND created_by = 'auto_rule'
        """
    ).fetchall()
    deactivated = 0

    for rule_id, pattern, rule_category, rule_created_at in rules:
        # An override is any non-auto_rule categorization recorded after the
        # rule was created whose category disagrees with the rule. This
        # captures both direct user edits ('user') and AI-applied corrections
        # via bulk_categorize ('ai'), and excludes legacy categorizations
        # that predate the rule entirely.
        rows = db.execute(
            f"""
            SELECT c.category, c.subcategory, COUNT(*) AS n
            FROM {TRANSACTION_CATEGORIES.full_name} c
            JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
            WHERE c.categorized_by != 'auto_rule'
              AND c.categorized_at > ?
              AND c.category != ?
              AND POSITION(LOWER(?) IN LOWER(t.description)) > 0
            GROUP BY c.category, c.subcategory
            ORDER BY n DESC
            """,
            [rule_created_at, rule_category, pattern],
        ).fetchall()
        total_overrides = sum(r[2] for r in rows)
        if total_overrides < threshold:
            continue

        db.execute(
            f"UPDATE {CATEGORIZATION_RULES.full_name} SET is_active = false, updated_at = CURRENT_TIMESTAMP WHERE rule_id = ?",
            [rule_id],
        )
        db.execute(
            f"""
            UPDATE {PROPOSED_RULES.full_name}
            SET status = 'superseded'
            WHERE LOWER(merchant_pattern) = LOWER(?) AND status = 'approved'
            """,
            [pattern],
        )
        new_category = rows[0][0]
        new_subcategory = rows[0][1]
        new_pid = uuid.uuid4().hex[:12]
        db.execute(
            f"""
            INSERT INTO {PROPOSED_RULES.full_name}
            (proposed_rule_id, merchant_pattern, match_type, category, subcategory,
             status, trigger_count, source, sample_txn_ids)
            VALUES (?, ?, 'contains', ?, ?, 'pending', ?, 'pattern_detection', ?)
            """,
            [new_pid, pattern, new_category, new_subcategory, total_overrides, []],
        )
        deactivated += 1

    if deactivated:
        logger.info(f"Deactivated {deactivated} auto-rule(s) due to user overrides")
    return deactivated


def reject(db: Database, proposed_rule_ids: list[str]) -> RejectResult:
    """Mark pending proposals as rejected. No rule is created."""
    result = RejectResult()
    for pid in proposed_rule_ids:
        row = db.execute(
            f"SELECT status FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?",
            [pid],
        ).fetchone()
        if not row or row[0] != "pending":
            result.skipped += 1
            continue
        db.execute(
            f"""
            UPDATE {PROPOSED_RULES.full_name}
            SET status = 'rejected', decided_at = CURRENT_TIMESTAMP, decided_by = 'user'
            WHERE proposed_rule_id = ?
            """,
            [pid],
        )
        result.rejected += 1
    return result
