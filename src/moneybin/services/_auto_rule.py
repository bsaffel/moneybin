"""Auto-rule proposal lifecycle: pattern extraction, dedup, promotion, override detection.

Hooks into CategorizationService.bulk_categorize() to capture user categorization
patterns, stage them as proposals in app.proposed_rules, and promote approved
proposals into active rules in app.categorization_rules with created_by='auto_rule'.
"""

import logging
import uuid
from dataclasses import dataclass, field

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


def extract_pattern(db: Database, transaction_id: str) -> tuple[str, str] | None:
    """Extract a (pattern, match_type) tuple for the given transaction.

    When the transaction has a resolved merchant_id, returns the merchant's
    ``(raw_pattern, match_type)`` — the substring that actually matches statement
    descriptions (e.g., 'AMZN'), not the canonical display name (e.g., 'Amazon'),
    paired with the merchant's declared match_type so non-``contains`` merchants
    (e.g., regex) propagate into proposed rules with correct semantics.
    Falls back to a normalized description with ``match_type='contains'`` when no
    merchant is associated. Returns None when neither is available.
    """
    row = db.execute(
        f"SELECT merchant_id FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    merchant_id = row[0] if row else None
    if merchant_id:
        m = db.execute(
            f"SELECT raw_pattern, match_type FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
            [merchant_id],
        ).fetchone()
        if m and m[0]:
            return str(m[0]), str(m[1] or "contains")

    desc_row = db.execute(
        f"SELECT description FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    if not desc_row or not desc_row[0]:
        return None
    cleaned = normalize_description(str(desc_row[0]))
    if not cleaned:
        return None
    return cleaned, "contains"


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
    extracted = extract_pattern(db, transaction_id)
    if not extracted:
        return None
    pattern, match_type = extracted

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
        VALUES (?, ?, ?, ?, ?, ?, 1, 'pattern_detection', ?)
        """,
        [
            proposed_rule_id,
            pattern,
            match_type,
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


def _description_match_sql(match_type: str) -> str:
    """SQL fragment matching ``?`` against ``t.description`` per match_type.

    The pattern is supplied as a ``?`` placeholder; the caller binds it.
    'exact' compares case-insensitively; 'regex' uses DuckDB ``regexp_matches``;
    anything else falls back to case-insensitive substring (``contains``).
    """
    if match_type == "exact":
        return "LOWER(t.description) = LOWER(?)"
    if match_type == "regex":
        return "regexp_matches(t.description, ?)"
    return "POSITION(LOWER(?) IN LOWER(t.description)) > 0"


def _categorize_existing_with_rule(
    db: Database,
    rule_id: str,
    pattern: str,
    match_type: str,
    category: str,
    subcategory: str | None,
) -> int:
    """Run the new rule against currently-uncategorized matching transactions. Returns count categorized.

    Dispatches the description match on the rule's declared ``match_type``
    (contains/exact/regex) so back-fill semantics match how the rule engine
    will subsequently apply the rule.
    """
    match_sql = _description_match_sql(match_type)
    rows = db.execute(
        f"""
        SELECT t.transaction_id
        FROM {FCT_TRANSACTIONS.full_name} t
        LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c ON t.transaction_id = c.transaction_id
        WHERE c.transaction_id IS NULL
          AND t.description IS NOT NULL
          AND {match_sql}
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
        # Wrap rule INSERT, proposal UPDATE, and back-fill INSERT in a single
        # transaction so a partial failure (e.g., interrupt between steps)
        # cannot leave an active rule whose source proposal is still 'pending'
        # — which would let approve() create a duplicate rule on retry.
        db.begin()
        try:
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
                db, rule_id, pattern, match_type, category, subcategory
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
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
    proposal_threshold = settings.auto_rule_proposal_threshold
    sample_cap = settings.auto_rule_sample_txn_cap

    # Fast-path: skip the override scan entirely when no auto-rules exist.
    # bulk_categorize calls this on every batch, so a one-row probe avoids
    # an unnecessary aggregate scan in the common pre-promotion case.
    if not db.execute(
        f"""
        SELECT 1 FROM {CATEGORIZATION_RULES.full_name}
        WHERE created_by = 'auto_rule' AND is_active = true LIMIT 1
        """
    ).fetchone():
        return 0

    rules = db.execute(
        f"""
        SELECT rule_id, merchant_pattern, match_type, category, subcategory, created_at
        FROM {CATEGORIZATION_RULES.full_name}
        WHERE is_active = true AND created_by = 'auto_rule'
        """
    ).fetchall()
    deactivated = 0

    for (
        rule_id,
        pattern,
        rule_match_type,
        rule_category,
        rule_subcategory,
        rule_created_at,
    ) in rules:
        # An override is any human-driven correction recorded after the rule
        # was created whose (category, subcategory) disagrees with the rule.
        # Excludes 'rule' and 'auto_rule' (machine-applied; counting them
        # would deactivate auto-rules due to overlapping rule engine output)
        # and predates legacy categorizations via the created_at filter.
        # Description match honors the rule's own match_type so override
        # counting matches how the rule engine actually selects rows.
        match_sql = _description_match_sql(rule_match_type)
        rows = db.execute(
            f"""
            SELECT c.category, c.subcategory, COUNT(*) AS n
            FROM {TRANSACTION_CATEGORIES.full_name} c
            JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
            WHERE c.categorized_by IN ('user', 'ai')
              AND c.categorized_at > ?
              AND (
                c.category != ?
                OR COALESCE(c.subcategory, '') != COALESCE(?, '')
              )
              AND {match_sql}
            GROUP BY c.category, c.subcategory
            ORDER BY n DESC
            """,
            [rule_created_at, rule_category, rule_subcategory, pattern],
        ).fetchall()
        total_overrides = sum(r[2] for r in rows)
        if total_overrides < threshold:
            continue

        new_category = rows[0][0]
        new_subcategory = rows[0][1]
        # Capture up to sample_cap transaction IDs that drove the most
        # common override category, so the re-proposal surfaces concrete
        # examples in auto-review instead of an empty list.
        sample_rows = db.execute(
            f"""
            SELECT c.transaction_id
            FROM {TRANSACTION_CATEGORIES.full_name} c
            JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
            WHERE c.categorized_by IN ('user', 'ai')
              AND c.categorized_at > ?
              AND c.category = ?
              AND COALESCE(c.subcategory, '') = COALESCE(?, '')
              AND {match_sql}
            ORDER BY c.categorized_at DESC
            LIMIT ?
            """,
            [
                rule_created_at,
                new_category,
                new_subcategory,
                pattern,
                sample_cap,
            ],
        ).fetchall()
        sample_ids = [r[0] for r in sample_rows]
        new_status = "pending" if total_overrides >= proposal_threshold else "tracking"
        new_pid = uuid.uuid4().hex[:12]
        # Wrap deactivate + supersede + re-propose in a single transaction so
        # a failure between steps cannot leave the rule deactivated with no
        # replacement proposal — an unrecoverable state for the override loop.
        db.begin()
        try:
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
            db.execute(
                f"""
                INSERT INTO {PROPOSED_RULES.full_name}
                (proposed_rule_id, merchant_pattern, match_type, category, subcategory,
                 status, trigger_count, source, sample_txn_ids)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pattern_detection', ?)
                """,
                [
                    new_pid,
                    pattern,
                    rule_match_type,
                    new_category,
                    new_subcategory,
                    new_status,
                    total_overrides,
                    sample_ids,
                ],
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
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
