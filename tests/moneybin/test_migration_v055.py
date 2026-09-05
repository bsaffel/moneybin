"""V055: blank merchant category defaults, and the categories they propagated.

Two tables, one rule. ``app.user_merchants`` takes the same nulling V054 gives
``app.transaction_splits``; ``app.transaction_categories`` takes a delete,
because its ``category`` is ``NOT NULL`` and so has no absent value to write.

Populated-fixture pattern per ``.claude/rules/database.md`` — V055 touches
existing data (UPDATE/DELETE backfill, no schema change).
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V055__backfill_blank_merchant_categories import migrate
from tests.moneybin.migration_helpers import insert_rows, run_migration

BLANK_CATEGORY_MERCHANT = "mrc-blankcat01"
BLANK_SUBCATEGORY_MERCHANT = "mrc-blanksub01"
NORMAL_MERCHANT = "mrc-normal0001"
PADDED_MERCHANT = "mrc-padded0001"
NULL_MERCHANT = "mrc-nullboth01"
UNICODE_BLANK_MERCHANT = "mrc-nbspcat001"
ORPHAN_SUBCATEGORY_MERCHANT = "mrc-orphansub1"

BLANK_CATEGORY_TXN = "txn-blankcat001"
BLANK_SUBCATEGORY_TXN = "txn-blanksub001"
NORMAL_TXN = "txn-normal00001"
PADDED_TXN = "txn-padded00001"

_MERCHANT_COLUMNS = (
    "merchant_id",
    "canonical_name",
    "category",
    "subcategory",
    "created_by",
)

_CATEGORY_COLUMNS = ("transaction_id", "category", "subcategory", "categorized_by")


@pytest.fixture()
def v055_db(db: Database) -> Database:
    """Merchant defaults and propagated categories covering the backfill."""
    insert_rows(
        db,
        "app",
        "user_merchants",
        _MERCHANT_COLUMNS,
        [
            (BLANK_CATEGORY_MERCHANT, "Blank Cat", "   ", "Coffee", "user"),
            (BLANK_SUBCATEGORY_MERCHANT, "Blank Sub", "Food", "\t\t", "user"),
            (NORMAL_MERCHANT, "Normal", "Food & Drink", "Coffee", "user"),
            (PADDED_MERCHANT, "Padded", "  Gas  ", None, "user"),
            (NULL_MERCHANT, "Null Both", None, None, "plaid"),
            # A non-breaking space is what a spreadsheet paste produces, and an
            # ideographic space is ordinary in CJK input.
            (UNICODE_BLANK_MERCHANT, "Unicode Blank", "\xa0\xa0", "　", "user"),
            # Never blank, so invisible to a predicate written around
            # blankness alone — the same orphan V054 covers on splits.
            (ORPHAN_SUBCATEGORY_MERCHANT, "Orphan Sub", None, "Coffee", "user"),
        ],
    )
    insert_rows(
        db,
        "app",
        "transaction_categories",
        _CATEGORY_COLUMNS,
        [
            (BLANK_CATEGORY_TXN, "   ", "Coffee", "rule"),
            (BLANK_SUBCATEGORY_TXN, "Food", "\t\t", "rule"),
            (NORMAL_TXN, "Food & Drink", "Coffee", "user"),
            (PADDED_TXN, "  Gas  ", None, "user"),
        ],
    )
    return db


def _merchant_pair(db: Database, merchant_id: str) -> tuple[str | None, str | None]:
    row = db.execute(
        "SELECT category, subcategory FROM app.user_merchants WHERE merchant_id = ?",
        [merchant_id],
    ).fetchone()
    assert row is not None
    return row


def _category_row(db: Database, transaction_id: str) -> tuple[str, str | None] | None:
    return db.execute(
        "SELECT category, subcategory FROM app.transaction_categories "
        "WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()


class TestV055BackfillBlankMerchantCategories:
    """A blank merchant default is nulled; what it already wrote is cleared."""

    def test_whitespace_only_category_takes_its_subcategory_with_it(
        self, v055_db: Database
    ) -> None:
        """Same cascade V054 applies to a split, for the same reason.

        A subcategory is a child of a category in this taxonomy, so leaving
        ``(NULL, 'Coffee')`` behind would have the migration manufacture the
        exact pair ``create_merchant_core`` now refuses on write.
        """
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, BLANK_CATEGORY_MERCHANT) == (None, None)

    def test_orphaned_subcategory_under_a_null_category_becomes_null(
        self, v055_db: Database
    ) -> None:
        """The orphan a blankness-only predicate cannot see.

        ``REGEXP_FULL_MATCH(NULL, pattern)`` is ``NULL`` and ``NULL OR FALSE``
        is ``NULL`` rather than ``TRUE``, so this row is excluded unless the
        ``WHERE`` names the already-NULL category outright.
        """
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, ORPHAN_SUBCATEGORY_MERCHANT) == (None, None)

    def test_whitespace_only_subcategory_becomes_null(self, v055_db: Database) -> None:
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, BLANK_SUBCATEGORY_MERCHANT) == ("Food", None)

    def test_normal_merchant_is_untouched(self, v055_db: Database) -> None:
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, NORMAL_MERCHANT) == ("Food & Drink", "Coffee")

    def test_padded_non_blank_category_is_not_trimmed(self, v055_db: Database) -> None:
        """Padding survives — this PR trims blank, never pads, anywhere."""
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, PADDED_MERCHANT) == ("  Gas  ", None)

    def test_already_null_merchant_is_untouched(self, v055_db: Database) -> None:
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, NULL_MERCHANT) == (None, None)

    def test_unicode_whitespace_only_becomes_null(self, v055_db: Database) -> None:
        """Blank means what ``str.strip()`` means, not what ASCII space means."""
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, UNICODE_BLANK_MERCHANT) == (None, None)

    def test_propagated_blank_category_row_is_removed(self, v055_db: Database) -> None:
        """Deleted, not nulled — the column is ``NOT NULL``.

        Row absence is the only way this table spells "uncategorized", and it
        is the state the transaction should have been in all along: the row is
        what hid it from ``core.uncategorized_queue`` while rendering an empty
        cell. The next sweep re-derives the merchant match against the now-null
        default, so nothing is lost that was worth keeping.
        """
        run_migration(v055_db, migrate)
        assert _category_row(v055_db, BLANK_CATEGORY_TXN) is None

    def test_propagated_blank_subcategory_is_nulled_not_deleted(
        self, v055_db: Database
    ) -> None:
        """The category is real, so only the subcategory goes."""
        run_migration(v055_db, migrate)
        assert _category_row(v055_db, BLANK_SUBCATEGORY_TXN) == ("Food", None)

    def test_normal_category_row_is_untouched(self, v055_db: Database) -> None:
        run_migration(v055_db, migrate)
        assert _category_row(v055_db, NORMAL_TXN) == ("Food & Drink", "Coffee")

    def test_padded_category_row_is_kept(self, v055_db: Database) -> None:
        run_migration(v055_db, migrate)
        assert _category_row(v055_db, PADDED_TXN) == ("  Gas  ", None)

    def test_repaired_merchant_keeps_its_updated_at(self, v055_db: Database) -> None:
        """A migration must not restamp the rows it repairs.

        ``updated_at`` means "set on UPDATE by service writes", and
        ``DoctorService._run_app_audit_coverage`` flags a row whose watermark is
        recent with no paired ``app.audit_log`` row — so a bump here would make
        ``doctor`` report every row this migration fixes as an unaudited
        mutation. No migration before this pair bumps it either, and nothing
        reads the column for staleness: ``dim_merchants`` only projects it.
        """
        before = v055_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [BLANK_CATEGORY_MERCHANT],
        ).fetchone()
        assert before is not None

        run_migration(v055_db, migrate)

        after = v055_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [BLANK_CATEGORY_MERCHANT],
        ).fetchone()
        assert after is not None
        assert after[0] == before[0]
        assert _merchant_pair(v055_db, BLANK_CATEGORY_MERCHANT) == (None, None)

    def test_untouched_merchant_keeps_its_updated_at(self, v055_db: Database) -> None:
        """A row the migration never matched is likewise left alone."""
        before = v055_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [NORMAL_MERCHANT],
        ).fetchone()
        assert before is not None

        run_migration(v055_db, migrate)

        after = v055_db.execute(
            "SELECT updated_at FROM app.user_merchants WHERE merchant_id = ?",
            [NORMAL_MERCHANT],
        ).fetchone()
        assert after is not None
        assert after[0] == before[0]

    def test_idempotent(self, v055_db: Database) -> None:
        """Re-running the migration on an already-migrated DB is harmless."""
        run_migration(v055_db, migrate)
        run_migration(v055_db, migrate)
        assert _merchant_pair(v055_db, BLANK_CATEGORY_MERCHANT) == (None, None)
        assert _merchant_pair(v055_db, PADDED_MERCHANT) == ("  Gas  ", None)
        assert _category_row(v055_db, BLANK_CATEGORY_TXN) is None
        assert _category_row(v055_db, PADDED_TXN) == ("  Gas  ", None)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
