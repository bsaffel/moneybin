"""Regression: refresh_views must tolerate a pre-V032 DB shape (review P2).

A pre-V032 existing database opened with ``no_auto_upgrade=True`` (migrations
skipped) keeps ``seeds.categories`` and ``app.user_categories`` in their
pre-V032 shape — no ``class`` column. ``Database.__init__`` still calls
``refresh_views`` unconditionally, whose ``core.dim_categories`` body selects
``s.class`` (seed arm) and ``class`` (user arm) -> ``BinderException`` without
a guard.

The tolerance is a VIEW-level projection (the prefix-derived CASE expression
substituted in for the missing column), never a table ALTER. An earlier
version of this fix pre-added the column via ``ALTER TABLE`` — that broke
V015's ``CREATE TABLE tmp AS SELECT *`` rebuild, which assumes the historical
7-column shape and fails on the resulting column-count mismatch. This test
asserts both that the view tolerates the missing column AND that the base
tables are left untouched.
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


def test_refresh_views_tolerates_pre_v032_shape_without_mutating_tables(
    db: Database,
) -> None:
    _recreate_pre_v032_shape(db)
    assert not column_exists(db, "seeds", "categories", "class")
    assert not column_exists(db, "app", "user_categories", "class")

    refresh_views(db)  # must not raise BinderException on the missing columns

    # V015-safety guard: refresh_views must never ALTER these tables — the
    # view tolerates the missing column via a projected CASE expression, not
    # by pre-adding the column.
    assert not column_exists(db, "seeds", "categories", "class")
    assert not column_exists(db, "app", "user_categories", "class")

    seed_classes = dict(
        db.execute(
            "SELECT category_id, class FROM core.dim_categories "
            "WHERE category_id LIKE '%-TST' ORDER BY category_id"
        ).fetchall()
    )
    assert seed_classes == {
        "INC-TST": "income",
        "TRN-TST": "transfer",
        "LNP-TST": "debt",
        "FND-TST": "expense",
    }

    # User-created category_ids never carry the INC/TRN/LNP prefixes, so the
    # same CASE expression resolves them all to 'expense' — matching the old
    # flat-default backfill behavior for user rows.
    user_classes = dict(
        db.execute(
            "SELECT category_id, class FROM core.dim_categories "
            "WHERE category_id LIKE 'u\\_%' ESCAPE '\\'"
            "ORDER BY category_id"
        ).fetchall()
    )
    assert user_classes == {
        "u_a1b2c3d4e5f6": "expense",
        "u_b2c3d4e5f6a1": "expense",
        "u_c3d4e5f6a1b2": "expense",
    }

    # core.dim_categories must actually build (the view whose SELECT was
    # raising BinderException before the fix).
    row_count = db.execute(
        "SELECT COUNT(*) FROM core.dim_categories WHERE category_id LIKE '%-TST' "
        "OR category_id LIKE 'u_%'"
    ).fetchone()
    assert row_count == (7,)
