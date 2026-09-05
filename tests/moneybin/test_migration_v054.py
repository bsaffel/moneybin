"""V054: null out whitespace-only category/subcategory in app.transaction_splits.

Seeds five splits — blank category, blank subcategory, a normal row, a
padded-but-non-blank category, and an all-NULL row — runs the migration
inside a BEGIN/COMMIT wrap mirroring ``MigrationRunner``, and verifies only
the whitespace-only values are nulled while a merely padded value is left
untouched.

Populated-fixture pattern per ``.claude/rules/database.md`` — V054 touches
existing data (UPDATE backfill, no schema change).
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V054__backfill_blank_split_categories import migrate
from tests.moneybin.migration_helpers import insert_rows, run_migration

BLANK_CATEGORY_SPLIT = "spl-blankcat01"
BLANK_SUBCATEGORY_SPLIT = "spl-blanksub01"
NORMAL_SPLIT = "spl-normal0001"
PADDED_SPLIT = "spl-padded0001"
NULL_SPLIT = "spl-nullboth01"
UNICODE_BLANK_SPLIT = "spl-nbspcat001"
ORPHAN_SUBCATEGORY_SPLIT = "spl-orphansub1"

_SPLIT_COLUMNS = (
    "split_id",
    "transaction_id",
    "amount",
    "category",
    "subcategory",
    "created_by",
)


@pytest.fixture()
def v054_db(db: Database) -> Database:
    """Five app.transaction_splits rows covering the backfill's cases."""
    insert_rows(
        db,
        "app",
        "transaction_splits",
        _SPLIT_COLUMNS,
        [
            (BLANK_CATEGORY_SPLIT, "txn-parent0001", "10.00", "   ", "Coffee", "cli"),
            (BLANK_SUBCATEGORY_SPLIT, "txn-parent0002", "20.00", "Food", "\t\t", "cli"),
            (NORMAL_SPLIT, "txn-parent0003", "30.00", "Food & Drink", "Coffee", "cli"),
            (PADDED_SPLIT, "txn-parent0004", "40.00", "  Gas  ", None, "cli"),
            (NULL_SPLIT, "txn-parent0005", "50.00", None, None, "cli"),
            # A non-breaking space is what a spreadsheet paste produces, and an
            # ideographic space is ordinary in CJK input. Python's `str.strip()`
            # calls both blank, so the backfill has to agree or the two halves
            # of this rule part company on the most likely real input.
            (
                UNICODE_BLANK_SPLIT,
                "txn-parent0006",
                "60.00",
                "\xa0\xa0",
                "　",
                "cli",
            ),
            # Never blank, and so invisible to a predicate written only around
            # blankness: the old permissive `set_splits({"subcategory": ...})`
            # accepted this shape outright, and a row carrying it is the same
            # orphan the blank-category cascade exists to prevent.
            (
                ORPHAN_SUBCATEGORY_SPLIT,
                "txn-parent0007",
                "70.00",
                None,
                "Coffee",
                "cli",
            ),
        ],
    )
    return db


def _category_pair(db: Database, split_id: str) -> tuple[str | None, str | None]:
    row = db.execute(
        "SELECT category, subcategory FROM app.transaction_splits WHERE split_id = ?",
        [split_id],
    ).fetchone()
    assert row is not None
    return row


class TestV054BackfillBlankSplitCategories:
    """V054 nulls out whitespace-only category/subcategory, leaves the rest."""

    def test_whitespace_only_category_takes_its_subcategory_with_it(
        self, v054_db: Database
    ) -> None:
        """A subcategory cannot outlive the category it hangs off.

        Nulling only the category would leave ``(NULL, 'Coffee')``, and
        ``core.fct_transaction_lines`` coalesces each field independently — so
        the rendered line would pair the *parent's* category with this split's
        leftover subcategory, a combination nobody chose. `SplitTarget` refuses
        that same shape on MCP writes, so producing one here would have the
        migration manufacture a state the write path forbids.
        """
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, BLANK_CATEGORY_SPLIT) == (None, None)

    def test_orphaned_subcategory_under_a_null_category_becomes_null(
        self, v054_db: Database
    ) -> None:
        """The orphan the blank predicate cannot see, on three-valued logic.

        `REGEXP_FULL_MATCH(NULL, pattern)` is `NULL`, and `NULL OR FALSE` is
        `NULL` rather than `TRUE`, so a row that was *born* `(NULL, 'Coffee')`
        — the shape the old permissive `set_splits` accepted — is excluded by
        a `WHERE` written only around blankness. The migration's postcondition
        is the write path's invariant, so it has to be stated as "no split has
        a subcategory without a category", not "no split has a blank
        category".
        """
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, ORPHAN_SUBCATEGORY_SPLIT) == (None, None)

    def test_whitespace_only_subcategory_becomes_null(self, v054_db: Database) -> None:
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, BLANK_SUBCATEGORY_SPLIT) == ("Food", None)

    def test_normal_row_is_untouched(self, v054_db: Database) -> None:
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, NORMAL_SPLIT) == ("Food & Drink", "Coffee")

    def test_padded_non_blank_category_is_not_trimmed(self, v054_db: Database) -> None:
        """Padding survives — this PR trims blank, never pads, anywhere."""
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, PADDED_SPLIT) == ("  Gas  ", None)

    def test_already_null_row_is_untouched(self, v054_db: Database) -> None:
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, NULL_SPLIT) == (None, None)

    def test_unicode_whitespace_only_becomes_null(self, v054_db: Database) -> None:
        """Blank means what `str.strip()` means, not what ASCII space means.

        A character-list `TRIM` has to enumerate every character it strips, so
        it silently keeps whichever ones nobody thought of — a non-breaking
        space from a spreadsheet, an ideographic space from CJK input. Those
        rows would stay non-NULL here while the service refuses the identical
        value on write.
        """
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, UNICODE_BLANK_SPLIT) == (None, None)

    def test_idempotent(self, v054_db: Database) -> None:
        """Re-running the migration on an already-migrated DB is harmless."""
        run_migration(v054_db, migrate)
        run_migration(v054_db, migrate)
        assert _category_pair(v054_db, BLANK_CATEGORY_SPLIT) == (None, None)
        assert _category_pair(v054_db, PADDED_SPLIT) == ("  Gas  ", None)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
