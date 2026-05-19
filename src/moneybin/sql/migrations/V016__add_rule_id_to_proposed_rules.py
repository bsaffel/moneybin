"""V016: add rule_id FK column to app.proposed_rules and backfill approved rows.

Backfill rule: for each ``status = 'approved'`` proposal whose
``merchant_pattern`` matches exactly one ACTIVE auto_rule
categorization_rule, copy that rule's ``rule_id``. Inactive duplicates
(deactivated rules from a prior override cycle) are excluded so the
common post-override state — inactive original + active replacement
sharing a pattern — backfills to the active rule. Orphans (no active
match) and genuinely ambiguous matches (two active rules share a
pattern) stay NULL; the approve flow normally maintains the 1:1
invariant so ambiguity is a sign of hand-edited state.

Three-step ADD then UPDATE then CREATE INDEX with an interim COMMIT
between the UPDATE and CREATE INDEX — DuckDB rejects index creation
on a column with outstanding updates in the same transaction. See
V010 for the same pattern and the recovery branch reasoning.

Idempotent: ``ADD COLUMN IF NOT EXISTS`` and ``CREATE INDEX IF NOT
EXISTS`` are no-ops on replay; the backfill ``UPDATE`` only touches
rows where ``rule_id IS NULL``.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add app.proposed_rules.rule_id, backfill, and create supporting index."""
    logger.info("V016: ADD COLUMN IF NOT EXISTS app.proposed_rules.rule_id")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE app.proposed_rules ADD COLUMN IF NOT EXISTS rule_id VARCHAR"
    )

    # Filter to active rules only so a post-override-cycle state
    # (inactive original + active replacement sharing a pattern) still
    # backfills to the active rule. HAVING COUNT(*) = 1 then filters
    # the residual case of two ACTIVE rules colliding on the same
    # pattern — those stay NULL rather than guess. MIN over the
    # singleton group is just a placeholder aggregator.
    conn.execute(  # type: ignore[union-attr]
        """
        UPDATE app.proposed_rules AS p
        SET rule_id = sub.rule_id
        FROM (
            SELECT pr.proposed_rule_id, MIN(cr.rule_id) AS rule_id
            FROM app.proposed_rules AS pr
            JOIN app.categorization_rules AS cr
              ON LOWER(pr.merchant_pattern) = LOWER(cr.merchant_pattern)
            WHERE pr.status = 'approved'
              AND cr.created_by = 'auto_rule'
              AND cr.is_active = true
            GROUP BY pr.proposed_rule_id
            HAVING COUNT(*) = 1
        ) AS sub
        WHERE p.proposed_rule_id = sub.proposed_rule_id
          AND p.rule_id IS NULL
        """
    )

    # Commit the backfill before CREATE INDEX. DuckDB raises
    # "Cannot create index with outstanding updates" when the UPDATE
    # above and the CREATE INDEX below share a transaction. A failure
    # between this COMMIT and CREATE INDEX leaves the column added and
    # backfilled but without the supporting index; the next migration
    # run hits ADD COLUMN IF NOT EXISTS (no-op), UPDATE (touches no
    # rows since rule_id is set), and CREATE INDEX IF NOT EXISTS
    # finishes the job.
    conn.execute("COMMIT")  # type: ignore[union-attr]
    conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]

    logger.info("V016: CREATE INDEX IF NOT EXISTS idx_proposed_rules_rule_id")
    conn.execute(  # type: ignore[union-attr]
        "CREATE INDEX IF NOT EXISTS idx_proposed_rules_rule_id "
        "ON app.proposed_rules (rule_id)"
    )
