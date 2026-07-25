"""Tests for the registered-vs-built SQLMesh model comparison."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.seeds import INIT_CREATED_MODELS
from moneybin.sqlmesh_registry import model_presence, registered_model_names


@pytest.mark.unit
@pytest.mark.fresh_db
def test_init_created_models_matches_what_db_init_actually_builds(
    db: Database,
) -> None:
    """The declared init set must equal the observed one, or `never_built` lies.

    ``never_built`` subtracts :data:`INIT_CREATED_MODELS` from the built set, so
    a stale declaration breaks it silently in both directions: a name that
    ``db init`` stopped creating makes a fresh profile look built, and a new
    init-created model makes a built warehouse look never-built. Neither shows
    up as a failure anywhere else — this is the only thing watching.
    """
    built = {
        row[0]
        for row in db.execute(
            "SELECT LOWER(schema_name || '.' || table_name) FROM duckdb_tables() "
            "UNION "
            "SELECT LOWER(schema_name || '.' || view_name) FROM duckdb_views()"
        ).fetchall()
    }

    assert registered_model_names() & built == INIT_CREATED_MODELS


@pytest.mark.unit
@pytest.mark.fresh_db
def test_a_freshly_initialized_profile_reads_as_never_built(db: Database) -> None:
    """`db init` alone must not count as a build.

    It creates five registered relations of its own (the ``core`` dim views and
    the seed tables), so "anything registered exists" is true before the first
    refresh ever runs.
    """
    assert model_presence(db).never_built is True


@pytest.mark.unit
def test_a_wiped_staging_layer_does_not_read_as_never_built(db: Database) -> None:
    """A built warehouse that lost its `prep` layer is broken, not brand new.

    ``never_built`` keyed on an empty ``prep`` schema conflates the two: the
    doctor skips its invariant and freshness reports ``pending=False``, so the
    one signal that watches for absent models goes quiet exactly when a model
    is absent. A ``core`` model no ``db init`` creates is the evidence that a
    real apply once ran.
    """
    db.execute(
        "CREATE TABLE IF NOT EXISTS core.fct_transactions (transaction_id VARCHAR)"
    )

    presence = model_presence(db)

    assert presence.never_built is False
    assert "prep.int_transactions__merged" in presence.missing
