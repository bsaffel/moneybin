"""V016: add rule_id FK column to app.proposed_rules with backfill.

Seeds three proposed_rules rows (linked+approved, orphaned+approved,
pending) plus one matching categorization_rules row, runs the migration
inside a BEGIN/COMMIT wrap mirroring ``MigrationRunner``, and verifies
the backfill rule: only proposals with a 1:1 active-rule match receive
a rule_id; orphans and pending rows stay NULL.

Populated-fixture pattern per ``.claude/rules/database.md`` — V016
touches existing data (ADD COLUMN + UPDATE backfill).
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V016__add_rule_id_to_proposed_rules import migrate
from tests.moneybin.migration_helpers import run_migration

LINKED_PATTERN = "STARBUCKS"
ORPHAN_PATTERN = "WHOLE_FOODS"
PENDING_PATTERN = "TARGET"
LINKED_RULE_ID = "rul-linked01"
LINKED_PROPOSAL_ID = "prop-linked1"
ORPHAN_PROPOSAL_ID = "prop-orphan1"
PENDING_PROPOSAL_ID = "prop-pending"


@pytest.fixture()
def v016_db(db: Database) -> Database:
    """Database with three proposed_rules rows + one matching active rule.

    Seeded shape:
      - LINKED_PROPOSAL_ID: status='approved'; LINKED_RULE_ID exists in
        categorization_rules with the same merchant_pattern.
      - ORPHAN_PROPOSAL_ID: status='approved'; no matching active rule.
      - PENDING_PROPOSAL_ID: status='pending'; no matching active rule.

    rule_id is intentionally NOT set on any proposed_rules row so the
    backfill UPDATE has work to do. The schema DDL declares rule_id
    NULL, so fresh installs match this initial state.
    """
    db.execute(
        """
        INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, category, subcategory,
             priority, is_active, created_by, created_at, updated_at)
        VALUES (?, ?, ?, 'contains', 'Food & Drink', 'Coffee',
                500, true, 'auto_rule', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [LINKED_RULE_ID, f"auto: {LINKED_PATTERN}", LINKED_PATTERN],
    )
    for pid, pattern, status in [
        (LINKED_PROPOSAL_ID, LINKED_PATTERN, "approved"),
        (ORPHAN_PROPOSAL_ID, ORPHAN_PATTERN, "approved"),
        (PENDING_PROPOSAL_ID, PENDING_PATTERN, "pending"),
    ]:
        db.execute(
            """
            INSERT INTO app.proposed_rules
                (proposed_rule_id, merchant_pattern, match_type, category,
                 subcategory, status, trigger_count, source, proposed_at)
            VALUES (?, ?, 'contains', 'Food & Drink', 'Coffee', ?, 3,
                    'pattern_detection', CURRENT_TIMESTAMP)
            """,
            [pid, pattern, status],
        )
    db.execute(
        "UPDATE app.proposed_rules SET rule_id = NULL WHERE proposed_rule_id IN (?, ?, ?)",
        [LINKED_PROPOSAL_ID, ORPHAN_PROPOSAL_ID, PENDING_PROPOSAL_ID],
    )
    return db


class TestV016AddRuleId:
    """V016 adds rule_id to app.proposed_rules and backfills approved rows."""

    def test_column_exists_after_migration(self, v016_db: Database) -> None:
        run_migration(v016_db, migrate)
        cols = {
            r[0]
            for r in v016_db.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE schema_name = 'app' AND table_name = 'proposed_rules'"
            ).fetchall()
        }
        assert "rule_id" in cols

    def test_backfills_approved_proposal_with_matching_rule(
        self, v016_db: Database
    ) -> None:
        run_migration(v016_db, migrate)
        row = v016_db.execute(
            "SELECT rule_id FROM app.proposed_rules WHERE proposed_rule_id = ?",
            [LINKED_PROPOSAL_ID],
        ).fetchone()
        assert row == (LINKED_RULE_ID,)

    def test_orphan_approved_proposal_rule_id_stays_null(
        self, v016_db: Database
    ) -> None:
        run_migration(v016_db, migrate)
        row = v016_db.execute(
            "SELECT rule_id FROM app.proposed_rules WHERE proposed_rule_id = ?",
            [ORPHAN_PROPOSAL_ID],
        ).fetchone()
        assert row == (None,)

    def test_pending_proposal_rule_id_stays_null(self, v016_db: Database) -> None:
        run_migration(v016_db, migrate)
        row = v016_db.execute(
            "SELECT rule_id FROM app.proposed_rules WHERE proposed_rule_id = ?",
            [PENDING_PROPOSAL_ID],
        ).fetchone()
        assert row == (None,)

    def test_does_not_backfill_when_two_rules_share_a_pattern(
        self, v016_db: Database
    ) -> None:
        """Ambiguous backfill stays NULL — we don't guess which rule wins."""
        v016_db.execute(
            """
            INSERT INTO app.categorization_rules
                (rule_id, name, merchant_pattern, match_type, category, subcategory,
                 priority, is_active, created_by, created_at, updated_at)
            VALUES ('rul-dup00001', 'auto: STARBUCKS DUP', ?, 'contains',
                    'Food & Drink', 'Coffee',
                    500, true, 'auto_rule', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [LINKED_PATTERN],
        )
        run_migration(v016_db, migrate)
        row = v016_db.execute(
            "SELECT rule_id FROM app.proposed_rules WHERE proposed_rule_id = ?",
            [LINKED_PROPOSAL_ID],
        ).fetchone()
        assert row == (None,)

    def test_index_exists_after_migration(self, v016_db: Database) -> None:
        run_migration(v016_db, migrate)
        indexes = {
            r[0]
            for r in v016_db.execute(
                "SELECT index_name FROM duckdb_indexes() "
                "WHERE schema_name = 'app' AND table_name = 'proposed_rules'"
            ).fetchall()
        }
        assert "idx_proposed_rules_rule_id" in indexes

    def test_idempotent(self, v016_db: Database) -> None:
        """Re-running the migration on an already-migrated DB is harmless."""
        run_migration(v016_db, migrate)
        run_migration(v016_db, migrate)
        row = v016_db.execute(
            "SELECT rule_id FROM app.proposed_rules WHERE proposed_rule_id = ?",
            [LINKED_PROPOSAL_ID],
        ).fetchone()
        assert row == (LINKED_RULE_ID,)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
