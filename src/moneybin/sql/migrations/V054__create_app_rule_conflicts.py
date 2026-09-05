"""V054: create app.rule_conflicts.

A conflict is user state, not a derivable artifact: it records that rule
creation refused a proposal because an active rule already owns the same
canonical matcher and assigns a different category, and it holds that proposal
until the user decides. Nothing in raw or core can reconstruct a proposal that
was never written.

The row binds to ``(existing_rule_id, existing_rule_updated_at)``. Editing the
existing rule moves its ``updated_at``, so the recorded conflict stops
describing live state — the review queue joins on both columns and drops it,
and a resolution quoting it is refused as stale.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.rule_conflicts (
    conflict_id VARCHAR PRIMARY KEY,
    matcher_digest VARCHAR NOT NULL,
    existing_rule_id VARCHAR NOT NULL,
    existing_rule_updated_at TIMESTAMP NOT NULL,
    existing_name VARCHAR NOT NULL,
    existing_category VARCHAR NOT NULL,
    existing_subcategory VARCHAR,
    existing_priority INTEGER NOT NULL,
    proposed_name VARCHAR NOT NULL,
    proposed_merchant_pattern VARCHAR NOT NULL,
    proposed_match_type VARCHAR NOT NULL,
    proposed_min_amount DECIMAL(18, 2),
    proposed_max_amount DECIMAL(18, 2),
    proposed_account_id VARCHAR,
    proposed_category VARCHAR NOT NULL,
    proposed_subcategory VARCHAR,
    proposed_priority INTEGER NOT NULL,
    proposed_created_by VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'pending',
    resolution VARCHAR,
    resolved_rule_id VARCHAR,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
)
"""

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "conflict_id",
        "Content hash over the conflict's identity ('conf_' + 16 hex of the existing "
        "rule, its updated_at, the matcher digest, and the proposed category) so "
        "re-detecting the same conflict updates one row",
    ),
    (
        "matcher_digest",
        "SHA-256 over the canonical matcher key (pattern, match type, amount bounds, "
        "account) shared by both rules; see canonical_matcher_key",
    ),
    (
        "existing_rule_id",
        "The active app.categorization_rules row that already owns this matcher",
    ),
    (
        "existing_rule_updated_at",
        "The existing rule's updated_at when the conflict was detected; a mismatch "
        "against live state means the conflict is stale",
    ),
    (
        "existing_name",
        "The existing rule's label, so review can name it without a second read",
    ),
    ("existing_category", "Category the existing rule assigns"),
    (
        "existing_subcategory",
        "Subcategory the existing rule assigns; NULL when it assigns none",
    ),
    (
        "existing_priority",
        "The existing rule's evaluation priority; lower wins",
    ),
    ("proposed_name", "Label of the rule that was refused"),
    (
        "proposed_merchant_pattern",
        "Pattern of the refused rule, stored verbatim as authored",
    ),
    (
        "proposed_match_type",
        "How the refused rule's pattern is applied: contains, exact, or regex",
    ),
    (
        "proposed_min_amount",
        "Lower bound of the refused rule; NULL means no lower bound",
    ),
    (
        "proposed_max_amount",
        "Upper bound of the refused rule; NULL means no upper bound",
    ),
    (
        "proposed_account_id",
        "Account the refused rule was scoped to; NULL means all accounts",
    ),
    ("proposed_category", "Category the refused rule would have assigned"),
    (
        "proposed_subcategory",
        "Subcategory the refused rule would have assigned; NULL when it assigns none",
    ),
    ("proposed_priority", "Evaluation priority the refused rule asked for"),
    ("proposed_created_by", "Who authored the refused rule: user or ai"),
    (
        "status",
        "pending until decided; resolved once the user replaced, reprioritized, or "
        "cancelled it",
    ),
    (
        "resolution",
        "The decision taken: replace, reprioritize, or cancel; NULL while pending",
    ),
    (
        "resolved_rule_id",
        "Rule the resolution activated; NULL for cancel and while pending",
    ),
    (
        "detected_at",
        "When rule creation refused the proposal and recorded this conflict",
    ),
    ("resolved_at", "When the user decided; NULL while pending"),
]


def migrate(conn: object) -> None:
    """Create app.rule_conflicts and apply catalog comments."""
    logger.debug("V054: CREATE TABLE IF NOT EXISTS app.rule_conflicts")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]

    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.rule_conflicts.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.debug("V054: app.rule_conflicts ready")
