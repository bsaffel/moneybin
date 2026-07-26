"""The user tier: lifecycle service, catalog membership, and R5's name rules.

Covers R5 (one access path, three tiers), R8 (by-name binding through the
synthesized runner), and the classification-downgrade capability from
``docs/specs/reports-dynamic.md``. The save pipeline itself is covered by
``test_dynamic.py``; these tests are about the surface over it.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.redaction import mask_strength
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import get_report_catalog, report_tier
from moneybin.reports._framework.contract import ParamSpec, ReportSpec
from moneybin.reports._framework.dynamic import spec_from_row, user_report_specs
from moneybin.repositories.user_reports_repo import UserReportsRepo
from moneybin.services.user_reports_service import (
    UserReportsService,
    is_weaker_class,
)

_ACCOUNTS_SQL = "SELECT account_id, routing_number FROM core.dim_accounts"


@pytest.fixture
def service(saved_db: Database) -> UserReportsService:
    return UserReportsService(saved_db)


def _param(name: str, annotation: type = str, **overrides: Any) -> ParamSpec:
    """One declared parameter; ``data_class`` is derived, never read from here."""
    declared: dict[str, Any] = {
        "name": name,
        "annotation": annotation,
        "default": None,
        "required": True,
        "help": "",
        "data_class": DataClass.UNRESOLVED,
    }
    return ParamSpec(**(declared | overrides))


def _create(service: UserReportsService, **overrides: Any) -> str:
    """Save one report through the service and return its minted ``report_id``."""
    kwargs: dict[str, Any] = {
        "name": "my_accounts",
        "query_sql": _ACCOUNTS_SQL,
        "description": "Accounts and their routing numbers.",
        "actor": "cli",
    }
    return service.create(**(kwargs | overrides)).report_id


# ---------------------------------------------------------------------------
# Saving — R2's pipeline reached through the lifecycle surface
# ---------------------------------------------------------------------------


def test_create_stores_the_derived_class_map_the_user_never_supplied(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(service)

    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    assert row["classes"] == {
        "account_id": DataClass.RECORD_ID.value,
        "routing_number": DataClass.ROUTING_NUMBER.value,
    }


def test_create_notes_unresolved_columns_without_blocking_the_save(
    service: UserReportsService,
) -> None:
    """R3: an unresolvable projection produces a note, never a gate."""
    outcome = service.create(
        name="opaque",
        query_sql="SELECT account_id, amount FROM reports.test_summary",
        actor="cli",
    )

    assert outcome.report_id.startswith("user:")
    assert outcome.unresolved_columns == ("account_id", "amount")


def test_create_notes_nothing_for_a_fully_resolved_query(
    service: UserReportsService,
) -> None:
    """The benign twin: a resolvable projection must not produce a note.

    Required by the fail-closed lesson from M2P.1 — no privacy test in this
    repo fails on *over*-reporting, so the quiet path needs its own assertion.
    """
    outcome = service.create(name="plain", query_sql=_ACCOUNTS_SQL, actor="cli")

    assert outcome.unresolved_columns == ()


def test_create_derives_a_parameter_class_from_its_comparison(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(
        service,
        query_sql="SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct",
        params=(_param("acct"),),
    )

    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    assert row["params"] == [
        {
            "name": "acct",
            "annotation": "str",
            "data_class": DataClass.ROUTING_NUMBER.value,
        }
    ]


def test_create_refuses_a_name_a_builtin_report_already_holds(
    service: UserReportsService,
) -> None:
    """R5: names are unique across the whole registry, not just this tier."""
    with pytest.raises(UserError) as raised:
        _create(service, name="spending")

    assert raised.value.code == "report_name_taken"
    assert "spending" in str(raised.value)


def test_create_refuses_a_name_another_saved_report_holds(
    service: UserReportsService,
) -> None:
    _create(service)

    with pytest.raises(UserError) as raised:
        _create(service, query_sql="SELECT account_id FROM core.dim_accounts")

    assert raised.value.code == "report_name_taken"


def test_create_names_both_exits_when_the_colliding_report_is_archived(
    service: UserReportsService,
) -> None:
    """An archived name stays taken, so a bare 'already exists' would mislead."""
    report_id = _create(service)
    service.update(report_id, is_active=False, actor="cli")

    with pytest.raises(UserError) as raised:
        _create(service, query_sql="SELECT account_id FROM core.dim_accounts")

    assert raised.value.code == "report_name_archived"
    message = f"{raised.value} {raised.value.hint}"
    assert "restore" in message.lower()
    assert "delete" in message.lower()


def test_create_refuses_a_name_that_is_not_a_slug(service: UserReportsService) -> None:
    with pytest.raises(UserError) as raised:
        _create(service, name="My Accounts")

    assert raised.value.code == "report_name_invalid"


# ---------------------------------------------------------------------------
# Resolution — the shared reference contract (R6)
# ---------------------------------------------------------------------------


def test_resolve_accepts_a_name(service: UserReportsService) -> None:
    report_id = _create(service)

    assert service.resolve("my_accounts")["report_id"] == report_id


def test_resolve_prefers_an_exact_report_id_over_a_matching_name(
    service: UserReportsService, saved_db: Database
) -> None:
    """A contested name must not shadow the id escape hatch R6 promises.

    The colliding row is written through the repo rather than the service
    because the service's own slug rule forbids it — the point here is
    ``resolve``'s ordering, which must hold for rows however they arrived.
    """
    target = _create(service)
    UserReportsRepo(saved_db).create(
        name=target,
        query_sql="SELECT 1 AS n FROM core.dim_accounts",
        classes={"n": DataClass.AGGREGATE.value},
        semantics={"kind": "unknown"},
        class_fingerprint="unused",
        actor="test",
    )

    assert service.resolve(target)["report_id"] == target


def test_resolve_raises_not_found_for_an_unknown_handle(
    service: UserReportsService,
) -> None:
    with pytest.raises(UserError) as raised:
        service.resolve("nope")

    assert raised.value.code == "report_id_not_found"


# ---------------------------------------------------------------------------
# Catalog membership — R5's one access path
# ---------------------------------------------------------------------------


def test_catalog_resolves_a_saved_report_when_given_a_database(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(service)

    resolved = get_report_catalog(saved_db).resolve(report_id)

    assert isinstance(resolved, ReportSpec)
    assert resolved.name == "my_accounts"
    assert resolved.view is None


def test_catalog_without_a_database_serves_only_the_packaged_tiers(
    service: UserReportsService,
) -> None:
    """The db-less catalog is the registration-time view, not a user-facing one."""
    _create(service)

    ids = {report.report_id for report in get_report_catalog().list()}

    assert not any(report_id.startswith("user:") for report_id in ids)


def test_catalog_hides_an_archived_report_but_can_be_asked_for_it(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(service)
    service.update(report_id, is_active=False, actor="cli")

    default_ids = {r.report_id for r in get_report_catalog(saved_db).list()}
    widened_ids = {
        r.report_id for r in get_report_catalog(saved_db, include_archived=True).list()
    }

    assert report_id not in default_ids
    assert report_id in widened_ids


def test_catalog_entries_carry_their_tier(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(service)
    catalog = get_report_catalog(saved_db)

    assert report_tier(catalog.resolve(report_id)) == "user"
    assert report_tier(catalog.resolve("core:spending")) == "builtin"


def test_catalog_surfaces_a_contested_name_without_choosing_a_winner(
    saved_db: Database,
) -> None:
    """A collision can arrive without a mutation — an upgrade adds a built-in.

    Written through the repo because the lifecycle service refuses the name at
    save time; the row models one that predates the built-in it now collides
    with. The user's report must stay reachable by ``report_id``.
    """
    event = UserReportsRepo(saved_db).create(
        name="spending",
        query_sql=_ACCOUNTS_SQL,
        classes={
            "account_id": DataClass.RECORD_ID.value,
            "routing_number": DataClass.ROUTING_NUMBER.value,
        },
        semantics={"kind": "unknown"},
        class_fingerprint="stale",
        actor="test",
    )
    report_id = event.target_id
    assert report_id is not None

    catalog = get_report_catalog(saved_db)

    assert catalog.name_collisions() == {
        "spending": ("core:spending", report_id),
    }
    assert catalog.resolve(report_id).report_id == report_id
    with pytest.raises(UserError) as raised:
        catalog.resolve("spending")
    assert raised.value.code == "report_id_ambiguous"


def test_a_saved_report_whose_table_vanished_stays_listed_and_masks_wholly(
    service: UserReportsService, saved_db: Database
) -> None:
    """One unresolvable row must not take the whole catalog down with it.

    Derivation raises when the query's table is gone. Dropping the report from
    the catalog would hide the user's work behind an upstream change they did
    not make, so the spec is served the other way: it stays listed, wholly
    masked, and says why.
    """
    report_id = _create(service)
    saved_db.execute("DROP TABLE core.dim_accounts")

    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    report = spec_from_row(saved_db, row)

    assert report.degraded is True
    assert set(report.spec.classes.values()) == {DataClass.UNRESOLVED}
    assert report_id in {spec.report_id for spec in user_report_specs(saved_db)}


# ---------------------------------------------------------------------------
# Running — R8's by-name binding through the shared path
# ---------------------------------------------------------------------------


def test_a_saved_report_runs_its_stored_sql_through_the_catalog(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(
        service,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE routing_number = $acct ORDER BY account_id"
        ),
        params=(_param("acct"),),
    )

    result = get_report_catalog(saved_db).execute(
        saved_db,
        report_id=report_id,
        parameters={"acct": "021000021"},
        limit=10,
    )

    assert [row["account_id"] for row in result.records] == ["acct_11112222"]


def test_a_saved_report_masks_a_critical_column_like_a_builtin_does(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(service)

    result = get_report_catalog(saved_db).execute(
        saved_db, report_id=report_id, parameters={}, limit=10
    )

    assert {row["routing_number"] for row in result.records} == {"*****"}
    assert {row["account_id"] for row in result.records} == {
        "acct_11112222",
        "acct_99998888",
    }


#: R7's benign shapes. Expected values are counted by hand from the two rows
#: ``saved_db`` inserts, never read back from a run. Each isolates one mechanism
#: — a fixture that could be masked by two of them would prove neither:
#:
#: 1-2. The result name differs from the projection name (sqlglot calls the
#:      first projection ``*``; DuckDB calls the column ``count_star()``), so
#:      these two exercise the name bridge and nothing else.
#: 3.   Aliased, so the name bridge is trivially satisfied — what must hold is
#:      that lineage reaches *through* a scalar subquery to classify it.
#: 4.   Also aliased, and the count sits inside a derived table: the
#:      counting-aggregate rule must still fire there. ``_within_subquery``'s
#:      documented misfire suppressed exactly this, leaving nothing to classify.
_BENIGN_SHAPES = [
    ("SELECT COUNT(*) FROM core.fct_transactions", "count_star()", 2),
    ("SELECT MIN(amount) FROM core.fct_transactions", "min(amount)", Decimal("-30.00")),
    ("SELECT (SELECT COUNT(*) FROM core.dim_accounts) AS n", "n", 2),
    (
        "SELECT n FROM (SELECT COUNT(*) AS n FROM core.fct_transactions) sub",
        "n",
        2,
    ),
]


@pytest.mark.parametrize(("query_sql", "column", "expected"), _BENIGN_SHAPES)
def test_a_saved_report_returns_a_real_value_for_an_unaliased_aggregate(
    service: UserReportsService,
    saved_db: Database,
    query_sql: str,
    column: str,
    expected: object,
) -> None:
    """R7's over-masking twins: none of these three may come back ``'*****'``.

    Each shape reaches ``redact_records`` under a *result* column name that
    differs from the one lineage classified, and ``classify_columns`` fails
    closed on a name it cannot find — so a broken name bridge does not raise, it
    silently masks a count. No privacy test in this repo fails on over-masking,
    which is why these are written deliberately rather than left implied by the
    masking tests above.
    """
    report_id = _create(service, query_sql=query_sql)

    result = get_report_catalog(saved_db).execute(
        saved_db, report_id=report_id, parameters={}, limit=10
    )

    assert [row[column] for row in result.records] == [expected]
    # Nothing masked, so R3's inspection hint must stay off the response too.
    assert result.actions == []


# ---------------------------------------------------------------------------
# The downgrade capability — D5 / R5's strictly-weaker rule
# ---------------------------------------------------------------------------


def test_reclassify_lowers_the_stored_class_and_records_the_approval(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )

    service.reclassify(
        report_id,
        column="spend",
        to_class=DataClass.AGGREGATE,
        reason="A single total reveals no transaction amount.",
        confirmed=True,
        actor="cli",
    )

    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    assert row["classes"]["spend"] == DataClass.AGGREGATE.value
    assert row["class_downgrades"]["spend"] == {
        "from": DataClass.TXN_AMOUNT.value,
        "to": DataClass.AGGREGATE.value,
        "reason": "A single total reveals no transaction amount.",
    }


def test_reclassify_leaves_the_report_on_the_fingerprint_match_path(
    service: UserReportsService,
    saved_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A downgrade must not send every later run back through derivation.

    ``degraded`` alone cannot see this: a mismatched key re-resolves, reapplies
    the downgrade, agrees with the stored map, and reports no drift — so the
    report looks healthy while paying a full re-derivation on every read. The
    only direct observation is that derivation is never called, which is what
    proves the write and the read compute one key from the same values.
    """
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )
    service.reclassify(
        report_id,
        column="spend",
        to_class=DataClass.AGGREGATE,
        reason="A single total reveals no transaction amount.",
        confirmed=True,
        actor="cli",
    )
    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None

    def _fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("the stored fingerprint should have matched")

    monkeypatch.setattr(
        "moneybin.reports._framework.dynamic.derive_classification", _fail
    )
    report = spec_from_row(saved_db, row)

    assert report.degraded is False
    assert report.spec.classes["spend"] is DataClass.AGGREGATE


def test_reclassify_refuses_an_equal_tier_weakening(
    service: UserReportsService, saved_db: Database
) -> None:
    """The dangerous case a pair-ordering check admits.

    ``ROUTING_NUMBER → ACCOUNT_IDENTIFIER`` holds CRITICAL and drops masking
    from whole to partial, publishing the real last four digits.
    """
    report_id = _create(service)

    with pytest.raises(UserError) as raised:
        service.reclassify(
            report_id,
            column="routing_number",
            to_class=DataClass.ACCOUNT_IDENTIFIER,
            reason="Only the last four are needed.",
            confirmed=True,
            actor="cli",
        )

    assert raised.value.code == "report_class_not_weaker"
    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    assert row["class_downgrades"] == {}


def test_reclassify_refuses_a_class_that_raises_the_tier(
    service: UserReportsService,
) -> None:
    report_id = _create(
        service, query_sql="SELECT COUNT(*) AS n FROM core.dim_accounts"
    )

    with pytest.raises(UserError) as raised:
        service.reclassify(
            report_id,
            column="n",
            to_class=DataClass.BALANCE,
            reason="Wrong direction.",
            confirmed=True,
            actor="cli",
        )

    assert raised.value.code == "report_class_not_weaker"


def test_reclassify_refuses_an_unconfirmed_downgrade(
    service: UserReportsService, saved_db: Database
) -> None:
    """D5: the only path that durably lowers a masking floor needs a human."""
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )

    with pytest.raises(UserError) as raised:
        service.reclassify(
            report_id,
            column="spend",
            to_class=DataClass.AGGREGATE,
            reason="A single total reveals no transaction amount.",
            confirmed=False,
            actor="cli",
        )

    assert raised.value.code == "report_class_confirm_required"
    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    assert row["class_downgrades"] == {}


def test_reclassify_refuses_a_column_the_report_does_not_return(
    service: UserReportsService,
) -> None:
    report_id = _create(service)

    with pytest.raises(UserError) as raised:
        service.reclassify(
            report_id,
            column="nonexistent",
            to_class=DataClass.AGGREGATE,
            reason="No such column.",
            confirmed=True,
            actor="cli",
        )

    assert raised.value.code == "report_column_unknown"


def test_every_admitted_downgrade_drops_the_tier_and_never_masks_more_weakly() -> None:
    """The rule as a property over the whole enum, not one fixture pair.

    A tier drop that *raises* mask strength is not constructible today — below
    CRITICAL every transform is passthrough — so a fixture-based test of the
    mask-strength half would be vacuous. Sweeping every pair keeps the guard
    honest if a future ``DataClass`` changes that, and asserting the admitted
    set is non-empty is what keeps the sweep itself from passing vacuously.
    """
    admitted = [
        (from_class, to_class)
        for from_class in DataClass
        for to_class in DataClass
        if is_weaker_class(from_class, to_class)
    ]

    assert admitted
    for from_class, to_class in admitted:
        assert to_class.tier < from_class.tier
        assert mask_strength(to_class) <= mask_strength(from_class)


# ---------------------------------------------------------------------------
# Observability — the spec's counters, asserted by the label they emit
# ---------------------------------------------------------------------------


def _counter(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(f"moneybin_{name}", labels or None) or 0.0


def test_a_save_counts_itself_and_its_unresolved_columns(
    service: UserReportsService,
) -> None:
    """The two counters the spec calls load-bearing, on one save.

    Together they answer whether invisible classification is invisible *in
    practice* or whether users are quietly accumulating masked columns — so the
    unresolved counter must move by the column count, not by one per save.
    """
    before_saves = _counter("user_report_saves_total", outcome="saved")
    before_columns = _counter("user_report_unresolved_columns_total")

    service.create(
        name="opaque_pair",
        query_sql="SELECT account_id, amount FROM reports.test_summary",
        actor="cli",
    )

    assert _counter("user_report_saves_total", outcome="saved") == before_saves + 1
    assert _counter("user_report_unresolved_columns_total") == before_columns + 2


def test_a_rejected_save_counts_as_rejected(service: UserReportsService) -> None:
    before = _counter("user_report_saves_total", outcome="rejected")

    with pytest.raises(UserError):
        service.create(
            name="bad", query_sql="DELETE FROM core.dim_accounts", actor="cli"
        )

    assert _counter("user_report_saves_total", outcome="rejected") == before + 1


def test_running_a_saved_report_counts_against_the_user_tier(
    service: UserReportsService, saved_db: Database
) -> None:
    """The ``tier`` label is what separates user reports from shipped ones."""
    report_id = _create(service)
    before = _counter("user_report_runs_total", tier="user", outcome="ok")

    get_report_catalog(saved_db).execute(
        saved_db, report_id=report_id, parameters={}, limit=10
    )

    assert _counter("user_report_runs_total", tier="user", outcome="ok") == before + 1


def test_an_unknown_column_is_counted_apart_from_a_refused_downgrade(
    service: UserReportsService,
) -> None:
    """A typo and an illegitimate downgrade are different signals.

    ``refused_not_weaker`` is the abuse signal — someone trying to publish a
    value everyone agrees is sensitive. Counting a misspelled column under it
    inflates exactly the number that is supposed to mean something, and the
    comparison ``is_weaker_class`` would make cannot even be evaluated for a
    column that does not exist.
    """
    report_id = _create(service)
    before_unknown = _counter(
        "user_report_reclassify_total", outcome="refused_unknown_column"
    )
    before_weaker = _counter(
        "user_report_reclassify_total", outcome="refused_not_weaker"
    )

    with pytest.raises(UserError):
        service.reclassify(
            report_id,
            column="nonexistent",
            to_class=DataClass.AGGREGATE,
            reason="No such column.",
            confirmed=True,
            actor="cli",
        )

    assert (
        _counter("user_report_reclassify_total", outcome="refused_unknown_column")
        == before_unknown + 1
    )
    assert (
        _counter("user_report_reclassify_total", outcome="refused_not_weaker")
        == before_weaker
    )


def test_a_surface_that_could_not_ask_is_counted_apart_from_a_decline(
    service: UserReportsService,
) -> None:
    """``no_elicitation`` vs ``declined`` — the spec's reason for both.

    Conflating them hides a surface refusing every downgrade for mechanical
    reasons (no prompt available) behind what looks like users saying no. Both
    still refuse; only the recorded reason differs.
    """
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )
    before_declined = _counter("user_report_reclassify_total", outcome="declined")
    before_silent = _counter("user_report_reclassify_total", outcome="no_elicitation")

    for confirmed in (False, None):
        with pytest.raises(UserError) as raised:
            service.reclassify(
                report_id,
                column="spend",
                to_class=DataClass.AGGREGATE,
                reason="A single total reveals no transaction amount.",
                confirmed=confirmed,
                actor="cli",
            )
        assert raised.value.code == "report_class_confirm_required"

    assert _counter("user_report_reclassify_total", outcome="declined") == (
        before_declined + 1
    )
    assert _counter("user_report_reclassify_total", outcome="no_elicitation") == (
        before_silent + 1
    )


# ---------------------------------------------------------------------------
# Updates — R2's "every SQL or parameter change re-runs the pipeline"
# ---------------------------------------------------------------------------


def test_update_rederives_the_class_map_when_the_sql_changes(
    service: UserReportsService, saved_db: Database
) -> None:
    """The stale-authority bug: an alias reused over a sensitive column.

    ``run_report`` treats the stored map as authoritative, so re-aliasing an
    ``AGGREGATE`` projection onto ``routing_number`` under the same name would
    serve a routing number at the old LOW class.
    """
    report_id = _create(
        service, query_sql="SELECT COUNT(*) AS x FROM core.dim_accounts"
    )

    service.update(
        report_id,
        query_sql="SELECT routing_number AS x FROM core.dim_accounts",
        actor="cli",
    )

    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    assert row["classes"] == {"x": DataClass.ROUTING_NUMBER.value}


def test_update_rederives_when_only_the_parameters_change(
    service: UserReportsService, saved_db: Database
) -> None:
    """A parameter's class comes from its comparison, so a redeclaration re-runs.

    Retyping ``$acct`` changes the DESCRIBE binding, and a stale parameter class
    renders a CRITICAL literal into the provenance view under the old class.
    """
    query = "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
    report_id = _create(service, query_sql=query, params=(_param("acct"),))
    before = UserReportsRepo(saved_db).get(report_id)
    assert before is not None

    service.update(
        report_id,
        params=(_param("acct", str, help="The routing number to filter on."),),
        actor="cli",
    )

    after = UserReportsRepo(saved_db).get(report_id)
    assert after is not None
    assert after["params"] == [
        {
            "name": "acct",
            "annotation": "str",
            "help": "The routing number to filter on.",
            "data_class": DataClass.ROUTING_NUMBER.value,
        }
    ]
    assert after["class_fingerprint"] == before["class_fingerprint"]


def test_update_clears_class_downgrades_when_the_sql_changes(
    service: UserReportsService, saved_db: Database
) -> None:
    """A downgrade is a judgment about one column of one query, not a setting."""
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )
    service.reclassify(
        report_id,
        column="spend",
        to_class=DataClass.AGGREGATE,
        reason="A single total reveals no transaction amount.",
        confirmed=True,
        actor="cli",
    )

    outcome = service.update(
        report_id,
        query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions WHERE amount < 0",
        actor="cli",
    )

    assert outcome.cleared_downgrades == ("spend",)
    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    assert row["class_downgrades"] == {}
    assert row["classes"]["spend"] == DataClass.TXN_AMOUNT.value


def test_update_of_metadata_alone_leaves_the_classification_untouched(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(service)
    before = UserReportsRepo(saved_db).get(report_id)
    assert before is not None

    service.update(report_id, description="A new description.", actor="cli")

    after = UserReportsRepo(saved_db).get(report_id)
    assert after is not None
    assert after["description"] == "A new description."
    assert after["classes"] == before["classes"]
    assert after["class_fingerprint"] == before["class_fingerprint"]


def test_update_refuses_a_rename_onto_a_name_the_registry_holds(
    service: UserReportsService,
) -> None:
    """R5: every path that can set a name runs the collision check."""
    report_id = _create(service)

    with pytest.raises(UserError) as raised:
        service.update(report_id, name="spending", actor="cli")

    assert raised.value.code == "report_name_taken"


def test_update_accepts_a_rename_to_the_reports_own_current_name(
    service: UserReportsService,
) -> None:
    """The self-collision a naive check would reject."""
    report_id = _create(service)

    service.update(report_id, name="my_accounts", description="Same name.", actor="cli")


def test_delete_removes_the_row(
    service: UserReportsService, saved_db: Database
) -> None:
    report_id = _create(service)

    service.delete(report_id, actor="cli")

    assert UserReportsRepo(saved_db).get(report_id) is None
