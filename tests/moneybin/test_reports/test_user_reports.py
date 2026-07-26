"""The user tier: lifecycle service, catalog membership, and R5's name rules.

Covers R5 (one access path, three tiers), R8 (by-name binding through the
synthesized runner), and the classification-downgrade capability from
``docs/specs/reports-dynamic.md``. The save pipeline itself is covered by
``test_dynamic.py``; these tests are about the surface over it.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from prometheus_client import REGISTRY
from pydantic import JsonValue

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy import sql_lineage
from moneybin.privacy.redaction import mask_strength
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass
from moneybin.reports._framework.catalog import (
    catalog_to_payload,
    get_report_catalog,
    report_tier,
)
from moneybin.reports._framework.contract import ParamSpec, ReportSpec
from moneybin.reports._framework.dynamic import spec_from_row, user_report_specs
from moneybin.repositories.user_reports_repo import UserReportsRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.user_reports_service import (
    ConfirmedVia,
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

    catalog = get_report_catalog(saved_db)
    default_ids = {report.report_id for report in catalog.list()}
    every_id = {report.report_id for report in catalog.list(archived=None)}

    assert report_id not in default_ids
    assert report_id in every_id


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
    assert report_id in {
        report.spec.report_id for report in user_report_specs(saved_db)
    }


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


def test_reclassify_refuses_an_approval_whose_revision_moved(
    service: UserReportsService, saved_db: Database
) -> None:
    """The approval names a column of the query the human read, not the row's future.

    ``reports reclassify`` resolves the row read-only, prompts, then re-resolves in
    a write connection — the writer lock must not be held across an interactive
    prompt. That window is seconds-to-minutes wide, and a concurrent
    ``reports set --sql`` inside it changes what the approved column *is*. The
    strictly-weaker rule cannot catch it: it only asks that the tier drop, and
    CRITICAL → LOW drops one, so an approval given for ``SUM(amount) AS spend``
    would durably unmask a ``spend`` that has become a routing number.

    Same guard, same reasoning, as ``import_confirm``'s digest re-check — there an
    approval could transfer to a statement nobody reviewed. Refusing costs a
    re-run; not refusing costs a permanently lowered floor on unread SQL.
    """
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )
    shown = service.resolve(report_id)["class_fingerprint"]

    # The concurrent writer, landing while the prompt is open.
    service.update(
        report_id,
        query_sql="SELECT routing_number AS spend FROM core.dim_accounts",
        actor="cli",
    )

    with pytest.raises(UserError) as raised:
        service.reclassify(
            report_id,
            column="spend",
            to_class=DataClass.AGGREGATE,
            reason="A single total reveals no transaction amount.",
            confirmed=True,
            confirmed_via="prompt",
            expected_fingerprint=shown,
            actor="cli",
        )

    assert raised.value.code == error_codes.REPORT_CHANGED_DURING_CONFIRMATION
    row = UserReportsRepo(saved_db).get(report_id)
    assert row is not None
    # The floor is what matters: refusing loudly but writing anyway would be worse
    # than not guarding at all.
    assert row["class_downgrades"] == {}
    assert row["classes"]["spend"] == DataClass.ROUTING_NUMBER.value


def test_reclassify_lowers_the_stored_class_and_records_the_approval(
    service: UserReportsService, saved_db: Database
) -> None:
    """Also the benign twin of the revision guard: an unmoved row still downgrades."""
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )

    service.reclassify(
        report_id,
        column="spend",
        to_class=DataClass.AGGREGATE,
        reason="A single total reveals no transaction amount.",
        confirmed=True,
        confirmed_via="prompt",
        expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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


@pytest.mark.parametrize("confirmed_via", ["prompt", "flag"])
def test_reclassify_records_which_path_supplied_the_confirmation(
    service: UserReportsService, saved_db: Database, confirmed_via: ConfirmedVia
) -> None:
    """``actor="cli"`` is the same on both paths, so it cannot answer this.

    A human answering the prompt and an assistant passing ``--yes`` produce
    byte-identical audit rows today. That is the one distinction this row has to
    carry: ``design-principles.md`` puts a durable masking downgrade outside
    agent self-accept entirely, and an audit that cannot tell the two apart
    cannot show the rule was kept. Both values are asserted because a
    provenance field that only ever records one of them is not provenance.
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
        confirmed_via=confirmed_via,
        expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
        actor="cli",
    )

    events = AuditService(saved_db).list_events(
        target_id=report_id, action_pattern="user_report.set"
    )
    assert [event.context_json for event in events] == [
        {"confirmed_via": confirmed_via}
    ]


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
        confirmed_via="prompt",
        expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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
            confirmed_via="prompt",
            expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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
            confirmed_via="prompt",
            expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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
            confirmed_via="prompt",
            expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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
            confirmed_via="prompt",
            expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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
            confirmed_via="prompt",
            expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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
                confirmed_via="prompt",
                expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
                actor="cli",
            )
        assert raised.value.code == "report_class_confirm_required"

    assert _counter("user_report_reclassify_total", outcome="declined") == (
        before_declined + 1
    )
    assert _counter("user_report_reclassify_total", outcome="no_elicitation") == (
        before_silent + 1
    )


def test_a_flag_confirmation_is_counted_apart_from_a_prompted_one(
    service: UserReportsService,
) -> None:
    """Without the split, this counter is blind to the case it watches for.

    Its stated read is that a rising confirm rate against a flat ``declined``
    rate means the confirm has become a formality people click through. ``--yes``
    never touches ``declined`` — so an assistant supplying it unasked produces
    exactly that pattern and is indistinguishable from a human clicking through.
    """
    report_id = _create(
        service, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )
    before_prompt = _counter("user_report_reclassify_total", outcome="confirmed_prompt")
    before_flag = _counter("user_report_reclassify_total", outcome="confirmed_flag")

    service.reclassify(
        report_id,
        column="spend",
        to_class=DataClass.AGGREGATE,
        reason="A single total reveals no transaction amount.",
        confirmed=True,
        confirmed_via="flag",
        expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
        actor="cli",
    )

    assert _counter("user_report_reclassify_total", outcome="confirmed_flag") == (
        before_flag + 1
    )
    assert _counter("user_report_reclassify_total", outcome="confirmed_prompt") == (
        before_prompt
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
        confirmed_via="prompt",
        expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
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


# ---------------------------------------------------------------------------
# What a caller of the catalog actually receives
# ---------------------------------------------------------------------------


def test_an_archived_report_still_runs_by_report_id(
    service: UserReportsService, saved_db: Database
) -> None:
    """R5: archiving hides a report from the catalog; it does not revoke access.

    The `app.user_reports` DDL comment, the spec, and ``user_report_specs``' own
    docstring all promise this. Nothing archived a report and then ran it, so
    three of the four catalog call sites omitted ``include_archived`` and an
    archived report was unreachable by any surface but ``explain``.
    """
    report_id = _create(service)
    service.update(report_id, is_active=False, actor="cli")

    result = get_report_catalog(saved_db).execute(
        saved_db, report_id=report_id, parameters={}, limit=10
    )

    assert result.report_id == report_id
    assert len(result.records) == 2


def test_the_default_listing_hides_an_archived_report(
    service: UserReportsService, saved_db: Database
) -> None:
    """The benign twin: resolvable must not mean listed.

    Building the catalog over every row and then listing every row would satisfy
    the test above while making archiving do nothing at all.
    """
    report_id = _create(service)
    service.update(report_id, is_active=False, actor="cli")
    catalog = get_report_catalog(saved_db)

    assert report_id not in {report.report_id for report in catalog.list()}
    assert report_id in {report.report_id for report in catalog.list(archived=True)}


def test_the_archived_only_selector_excludes_active_reports(
    service: UserReportsService, saved_db: Database
) -> None:
    """``list(archived=True)`` is the archived-only arm of the three-state selector.

    The *listing surfaces* widen instead (``include_archived``), but the selector
    underneath must still be able to name one view exactly, or a caller wanting
    only the archived rows would have to filter them itself.
    """
    archived_id = _create(service)
    service.update(archived_id, is_active=False, actor="cli")
    active_id = _create(service, name="still_active")

    listed = {
        report.report_id for report in get_report_catalog(saved_db).list(archived=True)
    }

    assert listed == {archived_id}
    assert active_id not in listed


def test_a_widened_listing_says_which_entries_are_archived(
    service: UserReportsService, saved_db: Database
) -> None:
    """Including a hidden row obliges the payload to mark it, per ``accounts list``.

    A combined listing that cannot distinguish an archived report from an active
    one is worse than the archived-only view it replaces: the caller sees a
    report it can run, with nothing saying the user had hidden it. The flag and
    the field ship together or neither ships.
    """
    archived_id = _create(service)
    service.update(archived_id, is_active=False, actor="cli")
    active_id = _create(service, name="still_active")

    payload = catalog_to_payload(get_report_catalog(saved_db), include_archived=True)
    archived_by_id = {entry.report_id: entry.archived for entry in payload.reports}

    assert archived_by_id[archived_id] is True
    assert archived_by_id[active_id] is False


def test_the_default_listing_omits_archived_rows_and_marks_none(
    service: UserReportsService, saved_db: Database
) -> None:
    """The benign twin: widening is opt-in, and the field is not merely constant.

    A payload hard-coding ``archived=False`` would satisfy the test above for the
    active row and pass here too, so this asserts the archived row is *absent*
    rather than present-and-false.
    """
    archived_id = _create(service)
    service.update(archived_id, is_active=False, actor="cli")
    _create(service, name="still_active")

    payload = catalog_to_payload(get_report_catalog(saved_db))

    assert archived_id not in {entry.report_id for entry in payload.reports}
    assert all(entry.archived is False for entry in payload.reports)


def test_the_listing_carries_the_name_every_operation_resolves(
    service: UserReportsService, saved_db: Database
) -> None:
    """``report_id`` is not the handle — ``name`` is, and for every tier.

    ``reports-dynamic.md`` makes ``name`` "the handle every operation in R5
    takes", and ``resolve()`` accepts it. But the id diverges from it everywhere:
    a built-in registers ``report_id="core:networth", name="networth"``, and a
    saved report's id is an opaque ``user:r…`` its owner never chose and cannot
    retype. Publishing only the id leaves the one string the surface accepts
    undiscoverable the moment the create response scrolls away.
    """
    report_id = _create(service, name="monthly_spend")

    payload = catalog_to_payload(get_report_catalog(saved_db), include_archived=True)
    names_by_id = {entry.report_id: entry.name for entry in payload.reports}

    assert names_by_id[report_id] == "monthly_spend"
    # A built-in too, so the field cannot be satisfied by echoing `report_id`:
    # every entry's name is the bare handle, never the namespaced identifier.
    assert names_by_id["core:networth"] == "networth"


def test_the_run_envelope_reports_that_a_report_degraded(
    service: UserReportsService, saved_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R4 requires the *response* to say it degraded, not an intermediate object.

    ``DynamicReport.degraded`` was asserted by every drift test and read by
    nobody: the catalog dropped it on the way to the envelope, so a caller
    receiving masked rows had no machine-readable reason for them.
    """
    report_id = _create(service)

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "account_id": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    result = get_report_catalog(saved_db).execute(
        saved_db, report_id=report_id, parameters={}, limit=10
    )
    summary = result.to_envelope().to_dict()["summary"]

    assert summary["degraded"] is True
    assert "account_id" in summary["degraded_reason"]


def test_an_undegraded_run_leaves_the_envelope_undegraded(
    service: UserReportsService, saved_db: Database
) -> None:
    """The benign twin: a stamped-degraded envelope would be worthless."""
    report_id = _create(service)

    result = get_report_catalog(saved_db).execute(
        saved_db, report_id=report_id, parameters={}, limit=10
    )

    assert "degraded" not in result.to_envelope().to_dict()["summary"]


def test_a_builtin_run_carries_no_degraded_flag(saved_db: Database) -> None:
    """A tier with no stored row has no drift state to report."""
    result = get_report_catalog(saved_db).execute(
        saved_db, report_id="core:networth", parameters={}, limit=10
    )

    assert "degraded" not in result.to_envelope().to_dict()["summary"]


@pytest.mark.parametrize(
    ("token", "supplied", "expected"),
    [
        ("date", "2026-01-01", date(2026, 1, 1)),
        ("decimal", "10.50", Decimal("10.50")),
        ("decimal", 10, Decimal(10)),
    ],
    ids=["date-from-iso-string", "decimal-from-string", "decimal-from-int"],
)
def test_a_json_caller_can_bind_a_type_json_cannot_represent(
    service: UserReportsService,
    saved_db: Database,
    token: str,
    supplied: JsonValue,
    expected: object,
) -> None:
    """Two of the six declarable types were reachable only from the CLI.

    The CLI's binder coerces ``--param since=2026-01-01`` into a real ``date``
    before validation; an MCP ``parameters`` object carries JSON, which the
    shared type check refused outright. The parity test exercised only ``str``,
    which is why nothing caught it.
    """
    column = {"date": "transaction_date", "decimal": "amount"}[token]
    report_id = _create(
        service,
        query_sql=(
            f"SELECT transaction_id FROM core.fct_transactions WHERE {column} > $bound"  # noqa: S608  # parametrized column name from this test's own table, not user input
        ),
        params=[_param("bound", {"date": date, "decimal": Decimal}[token])],
    )

    spec, validated = get_report_catalog(saved_db).resolve_request(
        report_id=report_id, parameters={"bound": supplied}, limit=1
    )

    assert spec.report_id == report_id
    assert validated == {"bound": expected}


def test_a_value_that_is_not_the_declared_type_is_still_refused(
    service: UserReportsService, saved_db: Database
) -> None:
    """The benign twin: coercion must not become "accept anything".

    Returning the value untouched when it cannot be read as the declared type is
    what keeps the existing type error — with the parameter name and the expected
    type — as what the caller sees.
    """
    report_id = _create(
        service,
        query_sql=(
            "SELECT transaction_id FROM core.fct_transactions "
            "WHERE transaction_date > $bound"
        ),
        params=[_param("bound", date)],
    )

    with pytest.raises(UserError) as caught:
        get_report_catalog(saved_db).resolve_request(
            report_id=report_id, parameters={"bound": "not-a-date"}, limit=1
        )

    assert caught.value.code == error_codes.REPORT_PARAMETER_INVALID_TYPE


def test_a_date_default_survives_the_save_and_reaches_the_catalog(
    service: UserReportsService, saved_db: Database
) -> None:
    """``params`` is a JSON column; a ``date`` default crashed ``json.dumps``.

    The published schema carries the default verbatim, so the round trip has to
    end in something JSON can hold *and* something the runner can bind.
    """
    report_id = _create(
        service,
        query_sql="SELECT account_id FROM core.dim_accounts LIMIT $top",
        params=[_param("top", int, default=25, required=False)],
    )

    spec = get_report_catalog(saved_db).resolve(report_id)
    assert isinstance(spec, ReportSpec)

    assert [(p.name, p.default, p.required) for p in spec.params] == [
        ("top", 25, False)
    ]


def test_reclassify_ignores_a_stored_default_it_is_not_touching(
    service: UserReportsService, saved_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default gate is a *write* gate, and reclassify stores no parameters.

    ``_refuse_sensitive_defaults`` exists so an above-LOW default cannot be
    written — the catalog publishes defaults unmasked. Re-deriving with the stored
    defaults still attached let an upstream reclassification of an unrelated
    filter's column refuse a downgrade whose caller never mentioned that
    parameter, with an error about a default they did not supply.
    """
    report_id = _create(
        service,
        query_sql=(
            "SELECT account_id, SUM(amount) AS spend FROM core.fct_transactions "
            "WHERE account_id = $acct GROUP BY account_id"
        ),
        params=[_param("acct", str, default="acct_11112222", required=False)],
    )

    # `account_id` is RECORD_ID (LOW), so the default was legal at save time.
    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "fct_transactions")] = {
        **CLASSIFICATION[("core", "fct_transactions")],
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    outcome = service.reclassify(
        report_id,
        column="spend",
        to_class=DataClass.AGGREGATE,
        reason="a monthly total reveals no single amount",
        confirmed=True,
        confirmed_via="prompt",
        expected_fingerprint=service.resolve(report_id)["class_fingerprint"],
        actor="cli",
    )

    assert outcome.column == "spend"
    assert outcome.to_class is DataClass.AGGREGATE


def test_saving_an_above_low_default_is_still_refused(
    service: UserReportsService,
) -> None:
    """The benign twin: stripping defaults must not disarm the write gate.

    Stripping them in ``derive_classification`` itself — rather than in the one
    caller that stores no parameters — would let a routing number pasted as a
    filter's default be returned in the clear by a bare catalog listing.
    """
    with pytest.raises(UserError) as caught:
        _create(
            service,
            query_sql=(
                "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
            ),
            params=[_param("acct", str, default="021000021", required=False)],
        )

    assert caught.value.code == error_codes.REPORT_PARAMETER_DEFAULT_NOT_ALLOWED


def test_building_the_catalog_reads_the_live_schema_once(
    service: UserReportsService, saved_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Schema reads must not scale with the number of saved reports.

    ``get_current_schema_snapshot`` memoises its expensive ``MappingSchema`` build
    but still issues two catalog queries per call, and every saved row needed two
    of them — one for its fingerprint, one for its provenance — on every catalog
    build, including a build serving a request for a built-in.
    """
    for index in range(3):
        _create(service, name=f"saved_{index}")

    calls: list[int] = []
    real = sql_lineage.get_current_schema_snapshot

    def _counted(db: Database) -> Any:
        calls.append(1)
        return real(db)

    monkeypatch.setattr(
        "moneybin.reports._framework.dynamic.get_current_schema_snapshot", _counted
    )
    monkeypatch.setattr(
        "moneybin.reports._framework.derive.get_current_schema_snapshot", _counted
    )

    catalog = get_report_catalog(saved_db)

    assert len({report.report_id for report in catalog.list()}) > 3
    assert sum(calls) == 1
