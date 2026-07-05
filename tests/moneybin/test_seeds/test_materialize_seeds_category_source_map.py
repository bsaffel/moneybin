"""Regression: materialize_seeds must load ALL seed models (review P1).

``_SEED_MODELS`` previously listed only ``seeds.categories``, so
``seeds.category_source_map`` — a real SQLMesh SEED model with 97 rows — was
never materialized on the seed path (``db init`` / ``transform seed``).
``core.bridge_category_source_map``'s seed side was silently empty on every
fresh install. Drives the real mechanism (``materialize_seeds`` -> real
``sqlmesh_context`` -> real SQLMesh seed load) per
``.claude/rules/testing.md`` "No Shortcuts" rather than inserting rows
directly.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.seeds import materialize_seeds

pytestmark = pytest.mark.integration


@pytest.mark.slow
def test_materialize_seeds_loads_category_source_map(db: Database) -> None:
    materialize_seeds(db)

    count = db.execute("SELECT COUNT(*) FROM seeds.category_source_map").fetchone()
    assert count == (97,), f"expected all 97 seed rows materialized, got {count}"

    # Reverse lookup through the resolved bridge view proves the seed rows are
    # actually wired into core.bridge_category_source_map, not just present
    # in the seeds schema.
    rows = db.execute(
        """
        SELECT category_id FROM core.bridge_category_source_map
        WHERE source_type = 'plaid'
        AND source_category_code IN ('MEDICAL_DENTAL_CARE', 'MEDICAL')
        ORDER BY code_level = 'detailed' DESC LIMIT 1
        """
    ).fetchall()
    assert rows == [("HLC-DNT",)]

    # A newly-mapped M1W code resolves to its new finer category through the
    # bridge (proves the M1W additions are wired, not just present in the CSV).
    new_code = db.execute(
        """
        SELECT category_id FROM core.bridge_category_source_map
        WHERE source_type = 'plaid'
        AND source_category_code = 'ENTERTAINMENT_CASINOS_AND_GAMBLING'
        """
    ).fetchall()
    assert new_code == [("ENT-GMB",)]
