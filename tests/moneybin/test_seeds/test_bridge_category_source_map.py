"""Tests for core.bridge_category_source_map — the two-tier, user-over-seed bridge.

Exercises the real anti-join + UNION ALL logic via representative rows rather
than the full 91-row seed (that completeness is Task 2's job — see
test_category_source_map_seed.py).
"""

from __future__ import annotations

from moneybin.database import Database
from moneybin.seeds import refresh_views


def _insert_seed_row(
    db: Database,
    source_category_code: str,
    code_level: str,
    category_id: str,
) -> None:
    # source_subcategory_code is explicit '' for readability, not because it
    # is load-bearing: the view's anti-join applies
    # COALESCE(s.source_subcategory_code, '') to the seed side before
    # comparing, so a NULL here would normalize to '' the same way and still
    # match an app-table override keyed on ''.
    db.execute(
        "INSERT INTO seeds.category_source_map "
        "(source_type, source_category_code, source_subcategory_code, "
        "code_level, category_id, source_taxonomy_version) "
        "VALUES ('plaid', ?, '', ?, ?, 'plaid_pfc_v2')",
        [source_category_code, code_level, category_id],
    )


def test_detailed_lookup_returns_one(db: Database) -> None:
    refresh_views(db)  # ensures seeds.category_source_map exists
    _insert_seed_row(db, "MEDICAL_DENTAL_CARE", "detailed", "HLC-DNT")
    _insert_seed_row(db, "MEDICAL", "primary", "HLC")

    rows = db.execute(
        """
        SELECT category_id FROM core.bridge_category_source_map
        WHERE source_type = 'plaid'
        AND source_category_code IN ('MEDICAL_DENTAL_CARE', 'MEDICAL')
        ORDER BY code_level = 'detailed' DESC LIMIT 1
        """
    ).fetchall()

    assert rows == [("HLC-DNT",)]


def test_primary_fallback_when_detailed_unmapped(db: Database) -> None:
    refresh_views(db)  # ensures seeds.category_source_map exists
    # TRANSPORTATION_BIKES_AND_SCOOTERS is intentionally NOT mapped here;
    # only the primary-level TRANSPORTATION code is seeded.
    _insert_seed_row(db, "TRANSPORTATION", "primary", "TRP")

    rows = db.execute(
        """
        SELECT category_id FROM core.bridge_category_source_map
        WHERE source_type = 'plaid'
        AND source_category_code IN
            ('TRANSPORTATION_BIKES_AND_SCOOTERS', 'TRANSPORTATION')
        ORDER BY code_level = 'detailed' DESC LIMIT 1
        """
    ).fetchall()

    assert rows == [("TRP",)]


def test_user_override_wins(db: Database) -> None:
    refresh_views(db)  # ensures seeds.category_source_map exists
    _insert_seed_row(db, "FOOD_AND_DRINK_FAST_FOOD", "detailed", "FND-FST")

    db.execute(
        "INSERT INTO app.category_source_map "
        "(source_type, source_category_code, code_level, category_id, "
        "source_taxonomy_version) "
        "VALUES ('plaid', 'FOOD_AND_DRINK_FAST_FOOD', 'detailed', "
        "'u_custom0001', 'plaid_pfc_v2')"
    )
    # The bridge is a live view — no refresh_views() re-run needed for the
    # new app.category_source_map row to appear.

    rows = db.execute(
        """
        SELECT category_id, is_default FROM core.bridge_category_source_map
        WHERE source_category_code = 'FOOD_AND_DRINK_FAST_FOOD'
        """
    ).fetchall()

    assert rows == [("u_custom0001", False)]


def test_refresh_views_survives_a_seed_table_without_the_subcategory_column(
    db: Database,
) -> None:
    """A SQLMesh-managed seed table can predate source_subcategory_code.

    ``_ensure_seed_tables_exist``'s CREATE TABLE IF NOT EXISTS is a no-op on a
    database SQLMesh has already built, so the new column only lands once
    ``transform apply`` runs. ``Database.__init__`` calls ``refresh_views`` on
    every open, long before that — so referencing the column unconditionally
    raised BinderException on the first open after upgrading and bricked every
    entry point, with no self-heal path (opening the database is itself a
    prerequisite for running ``transform apply``).
    """
    refresh_views(db)  # ensures seeds.category_source_map exists
    db.execute("DROP TABLE seeds.category_source_map")
    db.execute(
        "CREATE TABLE seeds.category_source_map ("
        "source_type VARCHAR, source_category_code VARCHAR, "
        "code_level VARCHAR, category_id VARCHAR, "
        "source_taxonomy_version VARCHAR)"
    )
    db.execute(
        "INSERT INTO seeds.category_source_map VALUES "
        "('plaid', 'INCOME', 'primary', 'INC', 'plaid_pfc_v2')"
    )

    refresh_views(db)

    rows = db.execute(
        "SELECT source_subcategory_code, category_id "
        "FROM core.bridge_category_source_map "
        "WHERE source_type = 'plaid' AND source_category_code = 'INCOME'"
    ).fetchall()

    assert rows == [("", "INC")]
