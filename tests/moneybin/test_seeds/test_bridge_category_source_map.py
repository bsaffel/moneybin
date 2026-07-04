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
    db.execute(
        "INSERT INTO seeds.category_source_map "
        "(source_type, source_category_code, code_level, category_id, "
        "source_taxonomy_version) VALUES ('plaid', ?, ?, ?, 'plaid_pfc_v2')",
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
