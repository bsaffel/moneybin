"""Tests for the registered-vs-built SQLMesh model comparison."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError, classify_user_error
from moneybin.seeds import INIT_CREATED_MODELS
from moneybin.sqlmesh_registry import model_presence, registered_model_names


def _built_relations(db: Database) -> set[str]:
    return {
        row[0]
        for row in db.execute(
            "SELECT LOWER(schema_name || '.' || table_name) FROM duckdb_tables() "
            "UNION "
            "SELECT LOWER(schema_name || '.' || view_name) FROM duckdb_views()"
        ).fetchall()
    }


@pytest.mark.integration
def test_init_created_models_matches_what_a_real_db_init_builds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The declared init set must equal the observed one, or `never_built` lies.

    Runs the real :func:`init_db`, not the ``db`` fixture. Only ``init_db``
    calls ``materialize_seeds``, whose SQLMesh plan materializes every model in
    ``_SEED_MODELS`` — opening a ``Database`` runs ``refresh_views`` alone and
    builds a strictly smaller set. A declaration checked against the fixture
    therefore passes while omitting a seed the real path creates, which is
    exactly how ``seeds.exchange_mic_map`` went missing from the baseline and
    flipped ``never_built`` to False on a healthy freshly-initialized profile.

    ``never_built`` subtracts this set from the built set, so a stale
    declaration breaks it silently in both directions: a name ``db init``
    stopped creating makes a fresh profile look built, and a new init-created
    model makes a built warehouse look never-built. Nothing else is watching.
    """
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    from moneybin.database import Database as Db
    from moneybin.database import init_db
    from moneybin.secrets import SecretStore

    db_path = tmp_path / "profiles" / "probe" / "moneybin.duckdb"
    db_path.parent.mkdir(parents=True)
    init_db(db_path, profile="probe")

    with Db(
        db_path,
        read_only=True,
        secret_store=SecretStore(profile="probe"),
        no_auto_upgrade=True,
    ) as db:
        assert registered_model_names() & _built_relations(db) == INIT_CREATED_MODELS
        assert model_presence(db).never_built is True


@pytest.mark.unit
@pytest.mark.fresh_db
def test_opening_a_database_reads_as_never_built(db: Database) -> None:
    """Opening a database must not count as a build either.

    ``refresh_views`` runs on every open and creates registered relations of
    its own, so "anything registered exists" is true before a refresh ever
    runs. This is the subset of the real init path, checked cheaply.
    """
    assert registered_model_names() & _built_relations(db) <= INIT_CREATED_MODELS
    assert model_presence(db).never_built is True


@pytest.mark.unit
def test_an_unreadable_catalog_is_not_reported_as_never_built(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A catalog that cannot be read is unknown — not a fresh profile.

    Swallowing the error returned ``built_beyond_init_count=0``, the exact
    value ``never_built`` keys on, so an unreadable catalog took the healthy
    first-run branch in *both* consumers: the doctor reported ``skipped`` with
    "run refresh_run" as the remedy, and ``freshness()`` dropped the missing
    set and reported ``pending=False``.

    It propagates *classified*, not raw. ``handle_cli_errors`` re-raises
    whatever ``classify_user_error`` does not recognize, so a bare DuckDB
    error would turn `moneybin system status` and `moneybin transform status`
    into tracebacks — trading a wrong answer for a crash.
    """

    def _raise(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("catalog unreadable")

    monkeypatch.setattr(db, "execute", _raise)

    with pytest.raises(UserError) as excinfo:
        model_presence(db)

    assert excinfo.value.code == error_codes.INFRA_CATALOG_UNAVAILABLE
    assert classify_user_error(excinfo.value) is not None


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
