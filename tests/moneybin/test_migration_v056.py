"""V056: clear canonical references to a blank taxonomy row.

V054 and V055 null the *deprecated* text snapshot and leave ``category_id``,
which every consumer prefers, still pointing at a blank ``app.user_categories``
row. These tests pin the half that makes those two migrations actually take
effect.

Populated-fixture pattern per ``.claude/rules/database.md`` — V056 touches
existing data (UPDATE backfill, no schema change).
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V056__clear_blank_taxonomy_references import migrate
from tests.moneybin.migration_helpers import insert_rows, run_migration

BLANK_CATEGORY_ID = "cat-blank0001"
BLANK_SUBCATEGORY_ID = "cat-blanksub1"
NORMAL_CATEGORY_ID = "cat-normal001"

MERCHANT_ON_BLANK = "mrc-onblank001"
MERCHANT_ON_BLANK_SUB = "mrc-onblanksub"
MERCHANT_ON_NORMAL = "mrc-onnormal01"
MERCHANT_UNRESOLVED = "mrc-noneat0001"

SPLIT_ON_BLANK = "spl-onblank001"
SPLIT_ON_NORMAL = "spl-onnormal01"

TXNCAT_ON_BLANK_SUB = "txn-onblanksub"
TXNCAT_ON_NORMAL = "txn-onnormal01"

BUDGET_BLANK_TEXT = "bgt-blanktext"
BUDGET_ON_BLANK = "bgt-onblank001"
BUDGET_NORMAL = "bgt-normal0001"

RULE_BLANK_TEXT = "rul-blanktext1"
RULE_BLANK_SUB = "rul-blanksub01"
RULE_ON_BLANK = "rul-onblank001"
RULE_NORMAL = "rul-normal0001"

PROPOSED_BLANK_TEXT = "prp-blanktext1"
PROPOSED_ON_BLANK = "prp-onblank001"
PROPOSED_NORMAL = "prp-normal0001"

SOURCE_MAP_ON_BLANK = ("plaid", "BLANK_CODE")
SOURCE_MAP_NORMAL = ("plaid", "FOOD_AND_DRINK")

_CATEGORY_COLUMNS = ("category_id", "category", "subcategory")
_MERCHANT_COLUMNS = (
    "merchant_id",
    "canonical_name",
    "category",
    "subcategory",
    "category_id",
    "created_by",
)
_SPLIT_COLUMNS = (
    "split_id",
    "transaction_id",
    "amount",
    "category",
    "category_id",
    "created_by",
)
_TXNCAT_COLUMNS = (
    "transaction_id",
    "category",
    "subcategory",
    "category_id",
    "categorized_by",
)
_BUDGET_COLUMNS = (
    "budget_id",
    "category",
    "category_id",
    "monthly_amount",
    "start_month",
)
_RULE_COLUMNS = (
    "rule_id",
    "name",
    "merchant_pattern",
    "category",
    "subcategory",
    "category_id",
)
_PROPOSED_COLUMNS = (
    "proposed_rule_id",
    "merchant_pattern",
    "category",
    "subcategory",
    "category_id",
)
_SOURCE_MAP_COLUMNS = ("source_type", "source_category_code", "category_id")


@pytest.fixture()
def v056_db(db: Database) -> Database:
    """A blank taxonomy row plus the rows whose canonical FK points at it."""
    insert_rows(
        db,
        "app",
        "user_categories",
        _CATEGORY_COLUMNS,
        [
            # Reachable before this PR: `categories create` had no text rule.
            (BLANK_CATEGORY_ID, "   ", None),
            (BLANK_SUBCATEGORY_ID, "Food", "\t\t"),
            (NORMAL_CATEGORY_ID, "Food & Drink", "Coffee"),
        ],
    )
    insert_rows(
        db,
        "app",
        "user_merchants",
        _MERCHANT_COLUMNS,
        [
            (MERCHANT_ON_BLANK, "On Blank", None, None, BLANK_CATEGORY_ID, "user"),
            (
                MERCHANT_ON_BLANK_SUB,
                "On Blank Sub",
                "Food",
                None,
                BLANK_SUBCATEGORY_ID,
                "user",
            ),
            (
                MERCHANT_ON_NORMAL,
                "On Normal",
                "Food & Drink",
                "Coffee",
                NORMAL_CATEGORY_ID,
                "user",
            ),
            (MERCHANT_UNRESOLVED, "Unresolved", "Groceries", None, None, "user"),
        ],
    )
    insert_rows(
        db,
        "app",
        "transaction_splits",
        _SPLIT_COLUMNS,
        [
            (
                SPLIT_ON_BLANK,
                "txn-parent0001",
                "-10.00",
                None,
                BLANK_CATEGORY_ID,
                "cli",
            ),
            (
                SPLIT_ON_NORMAL,
                "txn-parent0001",
                "-5.00",
                "Food & Drink",
                NORMAL_CATEGORY_ID,
                "cli",
            ),
        ],
    )
    insert_rows(
        db,
        "app",
        "transaction_categories",
        _TXNCAT_COLUMNS,
        [
            (TXNCAT_ON_BLANK_SUB, "Food", None, BLANK_SUBCATEGORY_ID, "rule"),
            (TXNCAT_ON_NORMAL, "Food & Drink", "Coffee", NORMAL_CATEGORY_ID, "user"),
        ],
    )
    insert_rows(
        db,
        "app",
        "budgets",
        _BUDGET_COLUMNS,
        [
            # NOT NULL category holding a blank: no UPDATE repairs this row.
            (BUDGET_BLANK_TEXT, "   ", BLANK_CATEGORY_ID, "500.00", "2026-01"),
            (BUDGET_ON_BLANK, "Food", BLANK_SUBCATEGORY_ID, "300.00", "2026-01"),
            (BUDGET_NORMAL, "Food & Drink", NORMAL_CATEGORY_ID, "250.00", "2026-01"),
        ],
    )
    insert_rows(
        db,
        "app",
        "categorization_rules",
        _RULE_COLUMNS,
        [
            (RULE_BLANK_TEXT, "Blank", "BLANKCO", "   ", None, BLANK_CATEGORY_ID),
            (RULE_BLANK_SUB, "Blank Sub", "SUBCO", "Food", "\t\t", None),
            (RULE_ON_BLANK, "On Blank", "ONBLANK", "Food", None, BLANK_SUBCATEGORY_ID),
            (
                RULE_NORMAL,
                "Normal",
                "NORMALCO",
                "Food & Drink",
                "Coffee",
                NORMAL_CATEGORY_ID,
            ),
        ],
    )
    insert_rows(
        db,
        "app",
        "proposed_rules",
        _PROPOSED_COLUMNS,
        [
            (PROPOSED_BLANK_TEXT, "BLANKCO", "   ", None, BLANK_CATEGORY_ID),
            (PROPOSED_ON_BLANK, "ONBLANK", "Food", None, BLANK_SUBCATEGORY_ID),
            (PROPOSED_NORMAL, "NORMALCO", "Food & Drink", "Coffee", NORMAL_CATEGORY_ID),
        ],
    )
    insert_rows(
        db,
        "app",
        "category_source_map",
        _SOURCE_MAP_COLUMNS,
        [
            # category_id is NOT NULL here, so a reference cannot be nulled.
            (*SOURCE_MAP_ON_BLANK, BLANK_CATEGORY_ID),
            (*SOURCE_MAP_NORMAL, NORMAL_CATEGORY_ID),
        ],
    )
    return db


def _merchant_category_id(db: Database, merchant_id: str) -> str | None:
    row = db.execute(
        "SELECT category_id FROM app.user_merchants WHERE merchant_id = ?",
        [merchant_id],
    ).fetchone()
    assert row is not None
    return row[0]


def _split_category_id(db: Database, split_id: str) -> str | None:
    row = db.execute(
        "SELECT category_id FROM app.transaction_splits WHERE split_id = ?",
        [split_id],
    ).fetchone()
    assert row is not None
    return row[0]


def _txncat_category_id(db: Database, transaction_id: str) -> str | None:
    row = db.execute(
        "SELECT category_id FROM app.transaction_categories WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    assert row is not None
    return row[0]


class TestV056ClearBlankTaxonomyReferences:
    """A pointer at a blank taxonomy row is cleared; a good pointer is kept."""

    def test_merchant_reference_to_a_blank_category_is_cleared(
        self, v056_db: Database
    ) -> None:
        """Without this, V055's nulling never reaches the reader.

        ``core.dim_merchants`` projects
        ``COALESCE(dc.category, um.category)`` — the FK-resolved value wins, so
        a merchant still pointing at a blank taxonomy row renders the blank
        even though V055 nulled its snapshot.
        """
        run_migration(v056_db, migrate)
        assert _merchant_category_id(v056_db, MERCHANT_ON_BLANK) is None

    def test_merchant_reference_to_a_blank_subcategory_is_cleared(
        self, v056_db: Database
    ) -> None:
        """A blank on either axis makes the whole taxonomy row unusable."""
        run_migration(v056_db, migrate)
        assert _merchant_category_id(v056_db, MERCHANT_ON_BLANK_SUB) is None

    def test_split_reference_to_a_blank_category_is_cleared(
        self, v056_db: Database
    ) -> None:
        """Same defeat, on V054's table.

        ``fct_transactions`` builds its splits struct from
        ``COALESCE(sdc.category, s.category)``.
        """
        run_migration(v056_db, migrate)
        assert _split_category_id(v056_db, SPLIT_ON_BLANK) is None

    def test_transaction_category_reference_to_a_blank_row_is_cleared(
        self, v056_db: Database
    ) -> None:
        """V055 nulls a blank subcategory snapshot; the FK still carries it."""
        run_migration(v056_db, migrate)
        assert _txncat_category_id(v056_db, TXNCAT_ON_BLANK_SUB) is None

    def test_merchant_reference_to_a_real_category_is_kept(
        self, v056_db: Database
    ) -> None:
        run_migration(v056_db, migrate)
        assert _merchant_category_id(v056_db, MERCHANT_ON_NORMAL) == NORMAL_CATEGORY_ID

    def test_split_reference_to_a_real_category_is_kept(
        self, v056_db: Database
    ) -> None:
        run_migration(v056_db, migrate)
        assert _split_category_id(v056_db, SPLIT_ON_NORMAL) == NORMAL_CATEGORY_ID

    def test_transaction_category_reference_to_a_real_category_is_kept(
        self, v056_db: Database
    ) -> None:
        run_migration(v056_db, migrate)
        assert _txncat_category_id(v056_db, TXNCAT_ON_NORMAL) == NORMAL_CATEGORY_ID

    def test_already_unresolved_merchant_is_untouched(self, v056_db: Database) -> None:
        """An orphaned text row stays orphaned — this migration adds no FKs."""
        run_migration(v056_db, migrate)
        assert _merchant_category_id(v056_db, MERCHANT_UNRESOLVED) is None

    def test_touched_merchant_gets_a_fresh_updated_at(self, v056_db: Database) -> None:
        """``core.dim_merchants`` reads ``updated_at`` straight through."""
        before = v056_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [MERCHANT_ON_BLANK],
        ).fetchone()
        assert before is not None

        run_migration(v056_db, migrate)

        after = v056_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [MERCHANT_ON_BLANK],
        ).fetchone()
        assert after is not None
        assert after[0] > before[0]

    def test_untouched_merchant_keeps_its_updated_at(self, v056_db: Database) -> None:
        """The bump rides the same ``WHERE``, so a good row is not restamped."""
        before = v056_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [MERCHANT_ON_NORMAL],
        ).fetchone()
        assert before is not None

        run_migration(v056_db, migrate)

        after = v056_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [MERCHANT_ON_NORMAL],
        ).fetchone()
        assert after is not None
        assert after[0] == before[0]

    def test_idempotent(self, v056_db: Database) -> None:
        """Re-running the migration on an already-migrated DB is harmless."""
        run_migration(v056_db, migrate)
        run_migration(v056_db, migrate)
        assert _merchant_category_id(v056_db, MERCHANT_ON_BLANK) is None
        assert _merchant_category_id(v056_db, MERCHANT_ON_NORMAL) == NORMAL_CATEGORY_ID
        assert _split_category_id(v056_db, SPLIT_ON_NORMAL) == NORMAL_CATEGORY_ID


def _exists(db: Database, table: str, key_column: str, key: str) -> bool:
    row = db.execute(
        f"SELECT 1 FROM app.{table} WHERE {key_column} = ?",  # noqa: S608  # test input, not executing user SQL
        [key],
    ).fetchone()
    return row is not None


class TestV056RemovesTheBlankTaxonomyRows:
    """The taxonomy row goes, and every reference to it goes with it."""

    def test_blank_category_row_is_deleted(self, v056_db: Database) -> None:
        """A category nothing can resolve to should not be in the taxonomy.

        It renders as an empty name in ``categories list`` and gives
        ``resolve_category_id`` a row nothing usefully matches. It is only
        deletable once every reference is cleared, which is why this runs last.
        """
        run_migration(v056_db, migrate)
        assert not _exists(v056_db, "user_categories", "category_id", BLANK_CATEGORY_ID)

    def test_blank_subcategory_row_is_deleted(self, v056_db: Database) -> None:
        """Unusable on either axis is unusable."""
        run_migration(v056_db, migrate)
        assert not _exists(
            v056_db, "user_categories", "category_id", BLANK_SUBCATEGORY_ID
        )

    def test_real_taxonomy_row_survives(self, v056_db: Database) -> None:
        run_migration(v056_db, migrate)
        assert _exists(v056_db, "user_categories", "category_id", NORMAL_CATEGORY_ID)

    def test_budget_with_unrepairable_blank_text_is_deleted(
        self, v056_db: Database
    ) -> None:
        """``budgets.category`` is ``NOT NULL``, so no UPDATE repairs it.

        The row names nothing and reports against nothing; deletion is the only
        available repair, which is why this one case deletes user state.
        """
        run_migration(v056_db, migrate)
        assert not _exists(v056_db, "budgets", "budget_id", BUDGET_BLANK_TEXT)

    def test_rule_with_unrepairable_blank_text_is_deleted(
        self, v056_db: Database
    ) -> None:
        run_migration(v056_db, migrate)
        assert not _exists(v056_db, "categorization_rules", "rule_id", RULE_BLANK_TEXT)

    def test_proposed_rule_with_unrepairable_blank_text_is_deleted(
        self, v056_db: Database
    ) -> None:
        run_migration(v056_db, migrate)
        assert not _exists(
            v056_db, "proposed_rules", "proposed_rule_id", PROPOSED_BLANK_TEXT
        )

    def test_budget_naming_a_real_category_keeps_its_row_and_loses_the_pointer(
        self, v056_db: Database
    ) -> None:
        """A repairable row is repaired, never deleted.

        This is where V056 deliberately departs from ``plan_category_delete``,
        whose ``force`` path cascade-*deletes* every referencing row. That is
        right for a user saying "remove this category and everything using it";
        it is wrong for a migration repairing an invalid row, which must not
        destroy a budget the user still wants.
        """
        run_migration(v056_db, migrate)
        row = v056_db.execute(
            "SELECT category, category_id FROM app.budgets WHERE budget_id = ?",
            [BUDGET_ON_BLANK],
        ).fetchone()
        assert row == ("Food", None)

    def test_rule_naming_a_real_category_keeps_its_row_and_loses_the_pointer(
        self, v056_db: Database
    ) -> None:
        run_migration(v056_db, migrate)
        row = v056_db.execute(
            "SELECT category, category_id FROM app.categorization_rules "
            "WHERE rule_id = ?",
            [RULE_ON_BLANK],
        ).fetchone()
        assert row == ("Food", None)

    def test_proposed_rule_naming_a_real_category_keeps_its_row(
        self, v056_db: Database
    ) -> None:
        run_migration(v056_db, migrate)
        row = v056_db.execute(
            "SELECT category, category_id FROM app.proposed_rules "
            "WHERE proposed_rule_id = ?",
            [PROPOSED_ON_BLANK],
        ).fetchone()
        assert row == ("Food", None)

    def test_blank_rule_subcategory_is_nulled(self, v056_db: Database) -> None:
        """Same blank-means-absent rule V054 and V055 apply to their tables."""
        run_migration(v056_db, migrate)
        row = v056_db.execute(
            "SELECT subcategory FROM app.categorization_rules WHERE rule_id = ?",
            [RULE_BLANK_SUB],
        ).fetchone()
        assert row == (None,)

    def test_source_mapping_to_a_blank_category_is_deleted(
        self, v056_db: Database
    ) -> None:
        """``category_source_map.category_id`` is ``NOT NULL``.

        A provider code mapped to a category that no longer exists resolves to
        nothing, and the column cannot hold the absent value, so the mapping
        goes rather than becoming a dangling id.
        """
        run_migration(v056_db, migrate)
        row = v056_db.execute(
            "SELECT 1 FROM app.category_source_map "
            "WHERE source_type = ? AND source_category_code = ?",
            list(SOURCE_MAP_ON_BLANK),
        ).fetchone()
        assert row is None

    def test_real_source_mapping_survives(self, v056_db: Database) -> None:
        run_migration(v056_db, migrate)
        row = v056_db.execute(
            "SELECT 1 FROM app.category_source_map "
            "WHERE source_type = ? AND source_category_code = ?",
            list(SOURCE_MAP_NORMAL),
        ).fetchone()
        assert row is not None

    def test_real_rows_on_every_swept_table_survive(self, v056_db: Database) -> None:
        """The restraint half, across the whole sweep."""
        run_migration(v056_db, migrate)
        assert _exists(v056_db, "budgets", "budget_id", BUDGET_NORMAL)
        assert _exists(v056_db, "categorization_rules", "rule_id", RULE_NORMAL)
        assert _exists(v056_db, "proposed_rules", "proposed_rule_id", PROPOSED_NORMAL)
        assert _exists(v056_db, "user_merchants", "merchant_id", MERCHANT_ON_NORMAL)
        assert _exists(v056_db, "transaction_splits", "split_id", SPLIT_ON_NORMAL)

    def test_no_reference_is_left_dangling(self, v056_db: Database) -> None:
        """The property the whole sweep exists to establish.

        Every ``category_id`` still present in an ``app`` table resolves to a
        surviving taxonomy row. This is what ``doctor``'s
        ``budgets.category_id`` check would otherwise report after the deletes.
        """
        run_migration(v056_db, migrate)
        dangling = v056_db.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT category_id FROM app.user_merchants
                UNION ALL SELECT category_id FROM app.transaction_splits
                UNION ALL SELECT category_id FROM app.transaction_categories
                UNION ALL SELECT category_id FROM app.budgets
                UNION ALL SELECT category_id FROM app.categorization_rules
                UNION ALL SELECT category_id FROM app.proposed_rules
                UNION ALL SELECT category_id FROM app.category_source_map
            ) refs
            WHERE refs.category_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM app.user_categories uc
                  WHERE uc.category_id = refs.category_id
              )
            """
        ).fetchone()
        assert dangling == (0,)

    def test_sweep_is_idempotent(self, v056_db: Database) -> None:
        run_migration(v056_db, migrate)
        run_migration(v056_db, migrate)
        assert not _exists(v056_db, "user_categories", "category_id", BLANK_CATEGORY_ID)
        assert _exists(v056_db, "user_categories", "category_id", NORMAL_CATEGORY_ID)
        assert _exists(v056_db, "budgets", "budget_id", BUDGET_NORMAL)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
