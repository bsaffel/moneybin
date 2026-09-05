"""Tests for the registered-vs-built SQLMesh model comparison."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError, classify_user_error
from moneybin.seeds import INIT_CREATED_MODELS
from moneybin.sqlmesh_registry import (
    materialized_model_names,
    model_presence,
    registered_model_names,
    relations_downstream_of,
    relations_upstream_of,
)


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
def test_a_failed_catalog_read_does_not_log_the_exception_message(
    db: Database, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The debug log gets where it broke, never what the exception said.

    ``exc_info=True`` appends ``<Type>: <str(exc)>`` plus the full traceback,
    and a DuckDB catalog error carries the database file path.
    ``SanitizedLogFormatter`` masks SSNs, long digit runs, and dollar amounts —
    not filesystem paths — so the raw message defeats the generic ``UserError``
    raised right below it. Same reason the MCP decorator logs
    ``exception_origin`` instead of calling ``logger.exception``.
    """

    def _raise(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("/Users/someone/Documents/MoneyBin/private.duckdb is locked")

    monkeypatch.setattr(db, "execute", _raise)

    with caplog.at_level("DEBUG"), pytest.raises(UserError):
        model_presence(db)

    assert "/Users/someone" not in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError" in caplog.text


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


def test_relations_downstream_of_follows_model_reads_transitively() -> None:
    """A report over daily balances is fed by transactions two hops away."""
    downstream = relations_downstream_of("core.fct_transactions")

    assert "core.fct_transactions" in downstream
    assert "reports.spending_trend" in downstream  # reads the fact directly
    assert "reports.net_worth" in downstream  # via core.fct_balances_daily
    assert "raw.plaid_transactions" not in downstream  # upstream of the fact
    assert "reports.no_such_model" not in downstream


def test_relations_downstream_of_ignores_a_schema_reference_in_prose() -> None:
    """A `schema.table` named only in a comment is documentation, not a read.

    ``core/bridge_merchant_entities.sql`` carries ``/* FK to
    core.fct_transactions.transaction_id */`` while its real ``FROM`` is
    ``prep.int_transactions__merged``; ``core/dim_accounts.sql`` names its
    downstream neighbours the same way. A text scan turned both into reverse
    edges, which marked a report reading either one provisional whenever any
    dedup was undecided.
    """
    downstream = relations_downstream_of("core.fct_transactions")

    assert "core.bridge_merchant_entities" not in downstream
    assert "core.dim_accounts" not in downstream
    assert "prep.int_transactions__merged" not in downstream  # upstream of the fact


def test_relations_upstream_of_walks_the_other_direction() -> None:
    """The report side of the same graph: what a relation is built from."""
    upstream = relations_upstream_of("reports.net_worth")

    assert "reports.net_worth" in upstream
    assert "core.fct_balances_daily" in upstream  # read directly
    assert "core.fct_transactions" in upstream  # via daily balances
    assert "reports.cash_flow" not in upstream


def test_materialized_model_names_names_the_tables_a_refresh_rebuilds() -> None:
    """A VIEW recomputes on read; a FULL model holds its rows until a refresh."""
    materialized = materialized_model_names()

    assert "core.fct_balances_daily" in materialized  # kind="FULL"
    assert "core.fct_transactions" not in materialized  # kind VIEW
    assert "reports.net_worth" not in materialized  # kind VIEW


def test_model_reads_match_the_dependencies_sqlmesh_parses() -> None:
    """The scan is pinned to SQLMesh's own answer, not to its own regex.

    ``relations_downstream_of`` feeds a user-facing ``degraded_reason`` with no
    review step in between, so an over-inclusive edge is a wrong caveat and a
    missing one is a silent stale total. SQLMesh resolves the same files
    connectionlessly (the ``report_class_derivation`` precedent), so CI can
    hold the cheap runtime scan to the expensive parser's answer.
    """
    from sqlmesh.core.dialect import parse as sqlmesh_parse
    from sqlmesh.core.model import load_sql_based_model

    from moneybin.sqlmesh_registry import (
        _MODELS_DIR,  # pyright: ignore[reportPrivateUsage]  # the scan under test
        _relations_read_by_model,  # pyright: ignore[reportPrivateUsage]
    )

    scanned = _relations_read_by_model()
    for path in sorted(_MODELS_DIR.rglob("*.sql")):
        model = load_sql_based_model(
            sqlmesh_parse(path.read_text(), default_dialect="duckdb"),
            path=path,
            dialect="duckdb",
        )
        name = model.name.lower()
        parsed = {
            dep.replace('"', "").lower()
            for dep in model.depends_on
            # SQLMesh's own state schema backs meta.model_freshness; it is not a
            # project relation and no report is ever downstream of it.
            if not dep.replace('"', "").lower().startswith("sqlmesh.")
        }
        assert scanned.get(name, frozenset()) == parsed, (
            f"{name}: scan says {sorted(scanned.get(name, frozenset()))}, "
            f"SQLMesh parses {sorted(parsed)}"
        )


def test_every_python_model_declares_what_it_reads() -> None:
    """A Python model's ``depends_on`` is the only read set anything can see.

    Its SQL lives in ``context.fetchdf()`` strings SQLMesh cannot parse, so an
    omitted declaration silently *shrinks* the dependency graph — the direction
    that drops a caveat rather than over-warning, and the one the SQL pinning
    test above cannot reach.
    """
    from moneybin.sqlmesh_registry import (
        _MODELS_DIR,  # pyright: ignore[reportPrivateUsage]  # the scan under test
        _python_model_node,  # pyright: ignore[reportPrivateUsage]
    )

    parsed = [
        node
        for path in sorted(_MODELS_DIR.rglob("*.py"))
        for node in [_python_model_node(path.read_text())]
        if node is not None
    ]
    assert parsed, "no Python models resolved; the scan found nothing to check"
    assert [name for name, node in parsed if not node.reads] == []
