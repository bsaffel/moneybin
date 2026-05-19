"""V016: add rule_id FK column to app.proposed_rules and backfill approved rows.

PR #174 lit up the storage-layer text-keyed reference pattern; the
2026-05-18 identifier-hygiene audit found that ``approve()`` and
``check_overrides()`` reconstruct the proposal->rule link from
``merchant_pattern`` text because ``app.proposed_rules`` has no
``rule_id`` column.

This migration adds the column and backfills it for approved proposals
that have exactly one matching active-rule row. Orphans (no matching
rule) and ambiguous backfills (multiple rules share the pattern) stay
NULL — we don't guess. The auto-rule approve flow has historically
maintained the 1:1 invariant, so the expected backfill outcome is
one-rule-per-approved-proposal; orphans are documented but not coerced.

Idempotent: ``ADD COLUMN IF NOT EXISTS`` and ``CREATE INDEX IF NOT
EXISTS`` are no-ops on replay; the backfill ``UPDATE`` only touches rows
where ``rule_id IS NULL``.
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

    # Backfill: for each approved proposal whose merchant_pattern matches
    # exactly one auto_rule categorization_rules row, copy that rule's id.
    # GROUP BY + HAVING COUNT(*) = 1 filters out proposals whose pattern
    # matches multiple rules — we don't guess which one is the source.
    # MIN(cr.rule_id) is just an aggregator over the singleton group.
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
