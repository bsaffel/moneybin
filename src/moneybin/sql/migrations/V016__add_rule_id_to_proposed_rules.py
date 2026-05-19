"""V016: add rule_id FK column to app.proposed_rules and backfill approved rows.

Backfill rule: for each ``status = 'approved'`` proposal whose
``merchant_pattern`` matches exactly one auto_rule categorization_rule,
copy that rule's ``rule_id``. Orphans (no match) and ambiguous matches
(multiple rules share the pattern) intentionally stay NULL — the
approve flow has historically maintained a 1:1 proposal->rule
invariant, so any ambiguity is a sign the upgrade is operating on
hand-edited state and the caller should triage it explicitly.

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

    # HAVING COUNT(*) = 1 filters out proposals whose pattern matches
    # multiple auto_rules — those stay NULL rather than guess. MIN over
    # the singleton group is just a placeholder aggregator.
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
            GROUP BY pr.proposed_rule_id
            HAVING COUNT(*) = 1
        ) AS sub
        WHERE p.proposed_rule_id = sub.proposed_rule_id
          AND p.rule_id IS NULL
        """
    )

    logger.info("V016: CREATE INDEX IF NOT EXISTS idx_proposed_rules_rule_id")
    conn.execute(  # type: ignore[union-attr]
        "CREATE INDEX IF NOT EXISTS idx_proposed_rules_rule_id "
        "ON app.proposed_rules (rule_id)"
    )
