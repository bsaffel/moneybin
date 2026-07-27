"""R6's verify surface and R9's two SQL forms, across all three tiers.

``docs/specs/reports-dynamic.md`` R6 (the verify surface) and R9 (provenance
renders identically across tiers). The rendering rule these tests pin down is
the one that keeps the provenance surface inside the same classification
pipeline as the data surface: a parameter classed above LOW keeps its
placeholder, because rendering is not execution and never passes through
``redact_records``.
"""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.contract import (
    Binding,
    ParamSpec,
    ReportQuery,
    ReportSpec,
)
from moneybin.reports._framework.explain import explain_report, explain_spec
from moneybin.reports._framework.introspect import build_spec
from moneybin.reports._framework.provenance import render_sql_forms
from moneybin.repositories.user_reports_repo import UserReportsRepo
from moneybin.services.user_reports_service import UserReportsService
from moneybin.tables import TableRef
from tests.moneybin.db_helpers import create_core_tables_raw
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns


@pytest.fixture
def saved_db(db: Database) -> Database:
    """The shared schema (including ``app.user_reports``) plus classified rows."""
    create_core_tables_raw(db.conn)
    db.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, routing_number, institution_name, display_name)
        VALUES ('acct_11112222', '021000021', 'Test Bank', 'Checking'),
               ('acct_99998888', '026009593', 'Other Bank', 'Savings')
        """
    )
    db.execute(
        """
        INSERT INTO core.fct_transactions (transaction_id, account_id, amount)
        VALUES ('t1', 'acct_11112222', -30.00),
               ('t2', 'acct_99998888', 100.00)
        """
    )
    # A `reports.*` view no `@report` declares, so its columns are the honest
    # unresolvable case — the same answer a package-contributed view gets today.
    db.execute(
        """
        CREATE OR REPLACE VIEW reports.test_summary AS
        SELECT account_id, SUM(amount) AS amount, COUNT(*) AS txn_count
        FROM core.fct_transactions
        GROUP BY account_id
        """
    )
    return db


@pytest.fixture
def service(saved_db: Database) -> UserReportsService:
    return UserReportsService(saved_db)


def _declare(name: str, annotation: type = str, **overrides: Any) -> ParamSpec:
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


def _explain_saved(
    db: Database, service: UserReportsService, *, sql: str, **kwargs: Any
) -> Any:
    """Save one report and explain it, so the explanation covers a real row."""
    params = kwargs.pop("params", ())
    parameters = kwargs.pop("parameters", {})
    report_id = service.create(
        name=kwargs.pop("name", "my_report"),
        query_sql=sql,
        params=params,
        actor="test",
        **kwargs,
    ).report_id
    return explain_report(db, handle=report_id, parameters=parameters)


# ---------------------------------------------------------------------------
# R9 — the two SQL forms
# ---------------------------------------------------------------------------


def test_a_low_classed_parameter_renders_as_a_literal_in_the_executed_form(
    saved_db: Database, service: UserReportsService
) -> None:
    """The benign twin of the withholding rule: a LOW binding IS rendered.

    ``account_id`` is a minted opaque surrogate (RECORD_ID, LOW), so there is
    nothing to withhold and the executed form must be pasteable as-is.
    """
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id FROM core.dim_accounts WHERE account_id = $acct",
        params=[_declare("acct")],
        parameters={"acct": "acct_11112222"},
    )

    assert explanation.sql is not None
    assert "'acct_11112222'" in explanation.sql
    assert "$acct" not in explanation.sql
    assert "$acct" in (explanation.sql_template or "")
    assert explanation.withheld_parameters == ()


def test_an_above_low_parameter_keeps_its_placeholder_in_the_executed_form(
    saved_db: Database, service: UserReportsService
) -> None:
    """R9: rendering is not execution, so it never passes through redaction.

    A routing number bound as a filter would be returned verbatim by report
    inspection while the same value is masked in every row of the result it
    explains. The placeholder is retained instead — honest about what was
    withheld, and still pasteable into the SQL console.
    """
    explanation = _explain_saved(
        saved_db,
        service,
        sql=("SELECT account_id FROM core.dim_accounts WHERE routing_number = $rn"),
        params=[_declare("rn")],
        parameters={"rn": "021000021"},
    )

    assert explanation.sql is not None
    assert "021000021" not in explanation.sql
    assert "$rn" in explanation.sql
    assert explanation.withheld_parameters == ("rn",)


def test_a_withheld_parameter_is_never_masked_into_the_rendered_sql(
    saved_db: Database, service: UserReportsService
) -> None:
    """Masking the value would be invalid SQL for the column it filters.

    ``'****0021'`` is not a routing number, and printing it invites the reader
    to believe the string is the query that ran.
    """
    explanation = _explain_saved(
        saved_db,
        service,
        sql=("SELECT account_id FROM core.dim_accounts WHERE routing_number = $rn"),
        params=[_declare("rn")],
        parameters={"rn": "021000021"},
    )

    assert explanation.sql is not None
    assert "****" not in explanation.sql


def test_a_missing_required_parameter_suppresses_the_executed_form(
    saved_db: Database, service: UserReportsService
) -> None:
    """User tier: the stored template survives, the executed form does not."""
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id FROM core.dim_accounts WHERE account_id = $acct",
        params=[_declare("acct")],
        parameters={},
    )

    assert explanation.sql is None
    assert explanation.sql_suppressed_by == ("acct",)
    assert "$acct" in (explanation.sql_template or "")


def test_a_typed_parameter_renders_with_its_type_intact(
    saved_db: Database, service: UserReportsService
) -> None:
    """R9 asks for sqlglot literal construction, not naive quoting."""
    forms = render_sql_forms(
        ReportQuery(
            "SELECT amount FROM core.fct_transactions WHERE txn_date >= $since",
            {"since": Binding("2026-01-02", DataClass.AGGREGATE)},
        )
    )

    assert forms.sql is not None
    assert "'2026-01-02'" in forms.sql


def test_an_unclassed_binding_keeps_its_placeholder(
    saved_db: Database,
) -> None:
    """An extension runner that binds a bare value fails closed, not open.

    The runtime tolerates it so a third-party runner keeps working; what it must
    not do is render an unclassified value in the clear.
    """
    forms = render_sql_forms(
        ReportQuery(
            "SELECT account_id FROM core.dim_accounts WHERE routing_number = ?",
            # An extension outside this repo is not pyright-checked against the
            # Binding contract, so the runtime must answer for a bare value.
            ["021000021"],  # type: ignore[list-item]  # the untyped extension path
        )
    )

    assert forms.sql is not None
    assert "021000021" not in forms.sql
    assert forms.withheld_parameters == ("?1",)


def test_every_placeholder_is_rendered_not_only_the_first(
    saved_db: Database,
) -> None:
    """Two placeholders, both LOW: replacing one must not skip its successors.

    A one-placeholder fixture cannot distinguish this — substituting a node while
    walking the same generator silently drops whatever came after it.
    """
    forms = render_sql_forms(
        ReportQuery(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE account_id = ? OR account_id = ?",
            [
                Binding("acct_11112222", DataClass.RECORD_ID),
                Binding("acct_99998888", DataClass.RECORD_ID),
            ],
        )
    )

    assert forms.sql is not None
    assert "'acct_11112222'" in forms.sql
    assert "'acct_99998888'" in forms.sql
    assert "?" not in forms.sql


def test_positional_bindings_render_in_the_order_the_text_presents_them(
    saved_db: Database,
) -> None:
    """DuckDB binds ``?`` in source order; the renderer must agree with it.

    Three slots across a ``WHERE`` and a ``LIMIT`` is the shape that separates
    source order from every tree walk: ``find_all`` is breadth-first, and
    depth-first follows sqlglot's arg order, which visits a ``Select``'s ``limit``
    before its ``where``. Either would print each value in a slot it was never
    bound to — a query claiming numbers it did not run with.
    """
    forms = render_sql_forms(
        ReportQuery(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE account_id >= ? AND institution_name = ? LIMIT ?",
            [
                Binding("acct_first", DataClass.RECORD_ID),
                Binding("second_bank", DataClass.CATEGORY),
                Binding(33, DataClass.AGGREGATE),
            ],
        )
    )

    assert forms.sql is not None
    assert "account_id >= 'acct_first'" in forms.sql
    assert "institution_name = 'second_bank'" in forms.sql
    assert "LIMIT 33" in forms.sql


def test_the_rendered_form_is_valid_read_only_sql(saved_db: Database) -> None:
    """R9's purpose: the rendered form is pasted into the SQL console.

    There it re-enters through ``validate_read_only_query`` and normal
    parameterization, so a form that will not survive that gate is not a
    provenance artifact — it is a string that looks like one. This is what makes
    a mis-spliced span a failure rather than a cosmetic defect.
    """
    from moneybin.privacy.sql_query import validate_read_only_query

    forms = render_sql_forms(
        ReportQuery(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE account_id = $acct AND institution_name = $bank LIMIT $top",
            {
                "acct": Binding("acct_11112222", DataClass.RECORD_ID),
                "bank": Binding("Test Bank", DataClass.CATEGORY),
                "top": Binding(5, DataClass.AGGREGATE),
            },
        )
    )

    assert forms.sql is not None
    assert validate_read_only_query(forms.sql) is None
    assert saved_db.execute(forms.sql).fetchall() == [("acct_11112222",)]


def test_a_question_mark_inside_a_string_literal_is_not_a_placeholder(
    saved_db: Database,
) -> None:
    """A textual scan for ``?`` would splice a literal into quoted prose."""
    forms = render_sql_forms(
        ReportQuery(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE display_name = 'who? me' AND account_id = ?",
            [Binding("acct_11112222", DataClass.RECORD_ID)],
        )
    )

    assert forms.sql is not None
    assert "'who? me'" in forms.sql
    assert "account_id = 'acct_11112222'" in forms.sql


def test_rendering_one_binding_does_not_change_what_the_next_render_sees(
    saved_db: Database,
) -> None:
    """The parsed tree is cached process-wide; rendering must not mutate it.

    ``parse_cached`` is memoised on the SQL text, so substituting literals into
    the tree it returns would leave the *next* reader of that same query looking
    at the previous caller's values — a cross-request leak that no single-render
    assertion can see.
    """
    sql = "SELECT account_id FROM core.dim_accounts WHERE account_id = $acct"

    first = render_sql_forms(
        ReportQuery(sql, {"acct": Binding("acct_11112222", DataClass.RECORD_ID)})
    )
    second = render_sql_forms(
        ReportQuery(sql, {"acct": Binding("acct_99998888", DataClass.RECORD_ID)})
    )

    assert first.sql is not None
    assert second.sql is not None
    assert "'acct_11112222'" in first.sql
    assert "'acct_99998888'" in second.sql
    assert "acct_11112222" not in second.sql


# ---------------------------------------------------------------------------
# R6 — per-column provenance
# ---------------------------------------------------------------------------


def test_a_passthrough_column_names_the_upstream_column_it_descends_from(
    saved_db: Database, service: UserReportsService
) -> None:
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id, routing_number FROM core.dim_accounts",
    )

    by_column = {column.column: column for column in explanation.columns}
    assert by_column["routing_number"].origin == "upstream"
    assert by_column["routing_number"].upstream == "core.dim_accounts.routing_number"
    assert by_column["routing_number"].data_class is DataClass.ROUTING_NUMBER


def test_a_computed_column_is_reported_as_computed(
    saved_db: Database, service: UserReportsService
) -> None:
    """No single upstream column describes an aggregate."""
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT SUM(amount) AS total FROM core.fct_transactions",
    )

    by_column = {column.column: column for column in explanation.columns}
    assert by_column["total"].origin == "computed"
    assert by_column["total"].upstream is None


def test_an_unclassifiable_column_is_reported_as_unresolved(
    saved_db: Database, service: UserReportsService
) -> None:
    """The masked column self-explains: R3's whole reason for this surface."""
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id, amount FROM reports.test_summary",
    )

    origins = {column.column: column.origin for column in explanation.columns}
    assert origins["amount"] == "unresolved"


def test_a_union_naming_two_different_upstreams_names_neither(
    saved_db: Database, service: UserReportsService
) -> None:
    """Both branches pass a column through, so it stays a passthrough — of what?

    Nothing nameable. The two facts collapse independently: ``passthrough`` holds
    because neither branch computes, and ``upstream`` is dropped because the
    branches disagree. Keeping the first branch's answer would attribute a value
    to a column half the rows never touched.
    """
    explanation = _explain_saved(
        saved_db,
        service,
        sql=(
            "SELECT account_id AS ident FROM core.dim_accounts "
            "UNION ALL "
            "SELECT routing_number AS ident FROM core.dim_accounts"
        ),
    )

    by_column = {column.column: column for column in explanation.columns}
    assert by_column["ident"].origin == "upstream"
    assert by_column["ident"].upstream is None


# ---------------------------------------------------------------------------
# R6 — freshness
# ---------------------------------------------------------------------------


def test_freshness_reports_the_stored_fingerprint_with_no_drift(
    saved_db: Database, service: UserReportsService
) -> None:
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id FROM core.dim_accounts",
    )

    row = UserReportsRepo(saved_db).get(explanation.report_id)
    assert row is not None
    assert explanation.class_fingerprint == row["class_fingerprint"]
    assert explanation.drift_detected is False
    assert explanation.drift_reason is None
    assert explanation.updated_at is not None


def test_freshness_reports_drift_when_an_upstream_column_is_reclassified(
    saved_db: Database,
    service: UserReportsService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R4's stale-map state has to be visible on the verify surface, not only felt."""
    report_id = service.create(
        name="drifting",
        query_sql="SELECT account_id FROM core.dim_accounts",
        actor="test",
    ).report_id

    reclassified = dict(CLASSIFICATION[("core", "dim_accounts")])
    reclassified["account_id"] = DataClass.INSTITUTION_ACCOUNT_NUMBER
    monkeypatch.setitem(CLASSIFICATION, ("core", "dim_accounts"), reclassified)

    explanation = explain_report(saved_db, handle=report_id, parameters={})

    assert explanation.drift_detected is True
    assert explanation.drift_reason is not None
    assert "account_id" in explanation.drift_reason


# ---------------------------------------------------------------------------
# R6 — graduation eligibility (R2's M2P.3 constraint, stated honestly)
# ---------------------------------------------------------------------------


def test_graduation_is_eligible_for_a_core_only_named_projection(
    saved_db: Database, service: UserReportsService
) -> None:
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id, routing_number FROM core.dim_accounts",
    )

    assert explanation.graduation == "eligible"
    assert explanation.graduation_blockers == ()


def test_graduation_case_folds_the_schema_before_deciding(
    saved_db: Database, service: UserReportsService
) -> None:
    """DuckDB resolves a schema case-insensitively; the verdict must agree.

    ``FROM CORE.dim_accounts`` saves, classifies, and runs correctly — the save
    path qualifies the tree first, and ``qualify`` folds identifiers for a
    case-insensitive dialect. ``_graduation`` instead bare-parses, so the user's
    spelling survives into a membership test against lowercase ``{"core",
    "app"}`` and a materializable report is told it is blocked. ``assert_acyclic``
    already folds ``table.name`` for its CTE check; the schema is the half that
    was left unfolded.
    """
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id, routing_number FROM CORE.dim_accounts",
    )

    assert explanation.graduation == "eligible"
    assert explanation.graduation_blockers == ()


def test_graduation_still_blocks_a_mixed_case_reports_read(
    saved_db: Database, service: UserReportsService
) -> None:
    """Case-folding must not turn the acyclic refusal into a default-open one.

    The benign-input partner to the test above: folding the schema so ``CORE``
    matches must also make ``REPORTS`` match the schema it is forbidden to read,
    or the fix would let the spelling ``REPORTS.test_summary`` slip past the one
    check that keeps a derived class map from becoming self-referential.
    """
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id, amount FROM REPORTS.test_summary",
    )

    assert explanation.graduation == "blocked"
    assert any("test_summary" in blocker for blocker in explanation.graduation_blockers)


def test_graduation_is_blocked_by_a_reports_read(
    saved_db: Database, service: UserReportsService
) -> None:
    """The save allowlist admits ``reports.*``; materialization never will."""
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id, amount FROM reports.test_summary",
    )

    assert explanation.graduation == "blocked"
    assert any(
        "reports.test_summary" in blocker for blocker in explanation.graduation_blockers
    )


def test_graduation_survives_a_cte_whose_name_has_no_schema(
    saved_db: Database, service: UserReportsService
) -> None:
    """``assert_acyclic`` now refuses a schema-less table; a CTE is not one.

    It skipped *every* empty-schema table, which made it blind to an unqualified
    read of a real table — see
    ``test_derivation_rejects_an_unqualified_upstream_read`` for that half, which
    only the build-time caller can reach (a saved report never gets there: DuckDB
    fails the save-time ``DESCRIBE`` on a bare name first). This is the benign
    twin the narrowed skip has to keep passing, at the boundary where a
    regression would block every saved report that names an intermediate result.
    """
    explanation = _explain_saved(
        saved_db,
        service,
        sql=(
            "WITH mine AS (SELECT account_id FROM core.dim_accounts) "
            "SELECT account_id FROM mine"
        ),
    )

    assert explanation.graduation == "eligible"
    assert explanation.graduation_blockers == ()


def test_graduation_is_blocked_by_a_star_projection(
    saved_db: Database, service: UserReportsService
) -> None:
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT * FROM core.dim_accounts",
    )

    assert explanation.graduation == "blocked"
    assert any("SELECT *" in blocker for blocker in explanation.graduation_blockers)


def test_a_runner_backed_built_in_is_already_materialized(saved_db: Database) -> None:
    """A runner-backed built-in really is a ``reports.*`` model already.

    The exemplar has to be runner-backed. This test named ``core:networth``
    before, which is service-backed and has no model at all — so it pinned the
    wrong verdict in place instead of proving this one.
    """
    explanation = explain_report(saved_db, handle="core:merchants", parameters={})

    assert explanation.graduation == "already_materialized"
    assert explanation.graduation_blockers == ()


def test_a_service_backed_report_is_not_called_materialized(
    saved_db: Database,
) -> None:
    """A service report has no model of its own, so it never "already" is one.

    ``already_materialized`` is portability evidence: it tells the reader this
    report is a relation in the graph, so it is schedulable, exportable, and
    dependable. A ``ServiceReportSpec`` runs an executor and owns no
    ``reports.*`` model, so that is the one answer this field must not give —
    the distinction ``sql_unavailable`` and ``ColumnOrigin.undetermined``
    already draw one field over.
    """
    explanation = explain_report(saved_db, handle="core:networth", parameters={})

    assert explanation.graduation == "service_backed"
    assert explanation.graduation_blockers == ()


# ---------------------------------------------------------------------------
# R6 — the three kinds, and handle resolution
# ---------------------------------------------------------------------------


def test_a_service_backed_report_names_why_it_has_no_sql(
    saved_db: Database,
) -> None:
    """R9's bound, stated plainly: no query exists anywhere in that path.

    A chip that renders "derived by NetworthService from reports.net_worth"
    tells the truth; one that fabricates a plausible SELECT does not.
    """
    explanation = explain_report(saved_db, handle="core:networth", parameters={})

    assert explanation.sql is None
    assert explanation.sql_template is None
    assert explanation.sql_unavailable is not None
    assert "service" in explanation.sql_unavailable
    # The declared read set, which is what stands in for a query here. Three
    # entries, not one: `reports-dynamic.md` R6 says `("reports.net_worth",)`,
    # which is the *history* report's provenance — a spec drift, fixed in the
    # doc pass rather than by weakening this assertion.
    assert explanation.lineage == (
        "reports.net_worth",
        "core.fct_balances_daily",
        "core.dim_accounts",
    )


def test_a_runner_backed_report_requires_every_required_parameter(
    saved_db: Database,
) -> None:
    """No template exists for a runner, so a placeholder cannot stand in.

    ``spec.runner(db, **params)`` raises on a missing keyword argument before a
    query exists, and a sentinel would fail the runner's own validation
    instead — so report inspection validates first and names what is missing.
    """
    classes = {"account_id": DataClass.RECORD_ID}

    def _needs_a_month(db: Database, *, month: str) -> ReportQuery:
        """Rows for one month.

        Args:
            db: Open read-only database connection.
            month: The month to report on.
        """
        return ReportQuery(
            "SELECT account_id FROM core.dim_accounts WHERE ? = ?",
            [Binding(month, DataClass.AGGREGATE), Binding(month, DataClass.AGGREGATE)],
        )

    spec = build_spec(
        _needs_a_month,
        report_id="test:needs_a_month",
        name="needs_a_month",
        view=TableRef("reports", "test_summary"),
        classes=classes,
        parameter_classes={"month": DataClass.AGGREGATE},
        columns=output_columns(classes),
        semantics=TEST_SEMANTICS,
    )

    with pytest.raises(UserError) as raised:
        explain_spec(saved_db, spec, parameters={})

    assert "month" in str(raised.value.details)


def test_explain_resolves_an_archived_saved_report(
    saved_db: Database, service: UserReportsService
) -> None:
    """Archiving suppresses catalog noise; it never revokes inspectability."""
    report_id = service.create(
        name="hidden",
        query_sql="SELECT account_id FROM core.dim_accounts",
        actor="test",
    ).report_id
    service.update(report_id, is_active=False, actor="test")

    explanation = explain_report(saved_db, handle=report_id, parameters={})

    assert explanation.report_id == report_id
    assert explanation.tier == "user"


def test_a_contested_name_stays_inspectable_by_report_id(
    saved_db: Database,
) -> None:
    """R6's collision-recovery promise: a name a user cannot type still has an id.

    Written through the repo because the lifecycle service refuses the name at
    save time; the row models one that predates the built-in it now collides
    with.
    """
    event = UserReportsRepo(saved_db).create(
        name="spending",
        query_sql="SELECT account_id FROM core.dim_accounts",
        classes={"account_id": DataClass.RECORD_ID.value},
        semantics={"kind": "unknown"},
        class_fingerprint="stale",
        actor="test",
    )
    report_id = event.target_id
    assert report_id is not None

    explanation = explain_report(saved_db, handle=report_id, parameters={})
    assert explanation.report_id == report_id

    with pytest.raises(UserError):
        explain_report(saved_db, handle="spending", parameters={})


def test_the_explanation_reports_the_tier_the_report_came_from(
    saved_db: Database, service: UserReportsService
) -> None:
    saved = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id FROM core.dim_accounts",
    )
    built_in = explain_report(saved_db, handle="core:spending", parameters={})

    assert saved.tier == "user"
    assert built_in.tier == "builtin"


def test_a_built_in_report_carries_no_fingerprint_or_update_time(
    saved_db: Database,
) -> None:
    """Freshness is a property of a stored row; a repo file has neither field."""
    explanation = explain_report(saved_db, handle="core:spending", parameters={})

    assert explanation.class_fingerprint is None
    assert explanation.updated_at is None
    assert explanation.drift_detected is False


def test_the_catalog_and_the_explanation_agree_on_the_class_map(
    saved_db: Database, service: UserReportsService
) -> None:
    """One class map, two projections of it — never two derivations."""
    explanation = _explain_saved(
        saved_db,
        service,
        sql="SELECT account_id, routing_number FROM core.dim_accounts",
    )
    spec = get_report_catalog(saved_db).resolve(explanation.report_id)
    assert isinstance(spec, ReportSpec)

    assert {column.column: column.data_class for column in explanation.columns} == dict(
        spec.classes
    )
