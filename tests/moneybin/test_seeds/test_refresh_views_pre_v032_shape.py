"""Regression: refresh_views must tolerate a pre-V032 DB shape (review P2).

A pre-V032 existing database opened with ``no_auto_upgrade=True`` (migrations
skipped) keeps ``seeds.categories`` and ``app.user_categories`` in their
pre-V032 shape — no ``class`` column. ``Database.__init__`` still calls
``refresh_views`` unconditionally, whose ``core.dim_categories`` body selects
``s.class`` (seed arm) and ``class`` (user arm) -> ``BinderException`` without
a guard. ``refresh_views`` must add the missing column (backfilling it) before
building the view, exactly as V032 would once it runs.
"""

from __future__ import annotations

from moneybin.database import Database
from moneybin.seeds import refresh_views
from tests.moneybin.migration_helpers import column_exists


def _recreate_pre_v032_shape(db: Database) -> None:
    """Drop `class` from both tables and seed representative pre-migration rows."""
    db.execute("ALTER TABLE seeds.categories DROP COLUMN class")
    db.execute(
        "INSERT INTO seeds.categories "
        "(category_id, category, subcategory, description) VALUES "
        "('INC-TST', 'Income', 'Test', ''), "
        "('TRN-TST', 'Transfer', 'Test', ''), "
        "('LNP-TST', 'Loan Payments', 'Test', ''), "
        "('FND-TST', 'Food & Drink', 'Test', '')"
    )
    db.execute("ALTER TABLE app.user_categories DROP COLUMN class")
    db.execute(
        "INSERT INTO app.user_categories "
        "(category_id, category, subcategory, description, is_active) VALUES "
        "('u_a1b2c3d4e5f6', 'Side Gig', 'Consulting', '', true), "
        "('u_b2c3d4e5f6a1', 'Hobby', 'Models', '', true), "
        "('u_c3d4e5f6a1b2', 'Gifts', 'Given', '', false)"
    )


def test_refresh_views_tolerates_pre_v032_shape(db: Database) -> None:
    _recreate_pre_v032_shape(db)
    assert not column_exists(db, "seeds", "categories", "class")
    assert not column_exists(db, "app", "user_categories", "class")

    refresh_views(db)  # must not raise BinderException on the missing columns

    assert column_exists(db, "seeds", "categories", "class")
    assert column_exists(db, "app", "user_categories", "class")

    classes = dict(
        db.execute(
            "SELECT category_id, class FROM seeds.categories "
            "WHERE category_id LIKE '%-TST' ORDER BY category_id"
        ).fetchall()
    )
    assert classes == {
        "INC-TST": "income",
        "TRN-TST": "transfer",
        "LNP-TST": "debt",
        "FND-TST": "expense",
    }

    # core.dim_categories must actually build (the view whose SELECT was
    # raising BinderException before the fix).
    row_count = db.execute(
        "SELECT COUNT(*) FROM core.dim_categories WHERE category_id LIKE '%-TST' "
        "OR category_id LIKE 'u_%'"
    ).fetchone()
    assert row_count == (7,)
