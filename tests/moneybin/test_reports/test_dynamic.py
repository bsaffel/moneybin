"""The dynamic-report second constructor and its save-time derivation pipeline.

Covers R2 (save-time classification), R4 (fingerprint drift), and the stored
row → ``ReportSpec`` constructor from ``docs/specs/reports-dynamic.md``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.redaction import MaskStrength
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass
from moneybin.reports._framework.contract import ParamSpec
from moneybin.reports._framework.derive import (
    DERIVATION_VERSION,
    class_fingerprint,
    derive_classification,
)
from moneybin.reports._framework.dynamic import (
    DEGRADED_STALE_CLASSIFICATION,
    DEGRADED_UNREADABLE_ROW,
    declared_params,
    spec_from_row,
    stored_params,
    unknown_semantics,
)
from moneybin.reports._framework.execute import run_report


@pytest.fixture
def dynamic_db(reports_db: Database) -> Database:
    """``reports_db`` plus account rows, so a saved report returns real rows.

    Local rather than folded into ``reports_db``: the shared fixture backs the
    classify and catalog tests, which count rows.
    """
    reports_db.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, routing_number, institution_name, display_name)
        VALUES ('acct_11112222', '021000021', 'Test Bank', 'Checking'),
               ('acct_99998888', '026009593', 'Other Bank', 'Savings')
        """
    )
    return reports_db


def _param(
    name: str,
    annotation: type = str,
    *,
    default: Any = None,
    required: bool = True,
) -> ParamSpec:
    """One declared parameter; ``data_class`` is derived, never read from here."""
    return ParamSpec(
        name=name,
        annotation=annotation,
        default=default,
        required=required,
        help="",
        data_class=DataClass.UNRESOLVED,
    )


def _row(**overrides: Any) -> dict[str, Any]:
    """A stored ``app.user_reports`` row, JSON columns already decoded."""
    row: dict[str, Any] = {
        "report_id": "user:rab12cd34ef56",
        "name": "my_accounts",
        "description": "Saved report.",
        "query_sql": "SELECT account_id FROM core.dim_accounts",
        "params": [],
        "classes": {"account_id": "record_id"},
        "semantics": {"kind": "unknown"},
        "class_downgrades": {},
        "class_fingerprint": "",
        "is_active": True,
        "created_at": None,
        "updated_at": None,
    }
    row.update(overrides)
    return row


_ANNOTATIONS: dict[str, type] = {"str": str, "int": int, "date": date}


def _saved(db: Database, **overrides: Any) -> dict[str, Any]:
    """A stored row whose class map and fingerprint come from real derivation.

    Building the row through the save pipeline rather than hand-writing a
    fingerprint is what keeps these tests on the mechanism: a hand-written one
    would land on the Mismatch branch and never exercise Match.
    """
    row = _row(**overrides)
    declared = tuple(
        _param(
            entry["name"],
            _ANNOTATIONS[entry.get("annotation", "str")],
            default=entry.get("default"),
            required="default" not in entry,
        )
        for entry in row["params"]
    )
    derived = derive_classification(db, query_sql=row["query_sql"], params=declared)
    row["classes"] = {name: dc.value for name, dc in derived.classes.items()}
    row["params"] = stored_params(declared, derived.parameter_classes)
    row["class_fingerprint"] = derived.fingerprint
    return row


# ---------------------------------------------------------------------------
# R2 — the save pipeline
# ---------------------------------------------------------------------------


def test_derivation_classes_a_projection_from_its_upstream_column(
    dynamic_db: Database,
) -> None:
    derived = derive_classification(
        dynamic_db,
        query_sql="SELECT routing_number, account_id FROM core.dim_accounts",
        params=(),
    )

    assert dict(derived.classes) == {
        "routing_number": DataClass.ROUTING_NUMBER,
        "account_id": DataClass.RECORD_ID,
    }
    assert derived.unresolved_columns == ()


def test_derivation_keys_the_map_by_duckdb_result_name_not_projection_name(
    dynamic_db: Database,
) -> None:
    """The benign twin for step 6 — an unaliased aggregate must not over-mask.

    sqlglot names this projection ``*`` and DuckDB names it ``count_star()``.
    Persisting the unbridged map is the M2P.1 over-redaction bug written to
    disk: every run of every report containing a bare ``COUNT(*)`` would mask
    it. No privacy test fails on over-masking, so this one is deliberate.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql="SELECT COUNT(*) FROM core.dim_accounts",
        params=(),
    )

    assert dict(derived.classes) == {"count_star()": DataClass.AGGREGATE}
    assert derived.classes["count_star()"].tier is DataClass.AGGREGATE.tier


def test_derivation_rejects_a_statement_that_returns_no_rows(
    dynamic_db: Database,
) -> None:
    """``validate_read_only_query`` admits DESCRIBE; a durable report must not."""
    with pytest.raises(UserError, match="row-returning"):
        derive_classification(
            dynamic_db, query_sql="DESCRIBE core.dim_accounts", params=()
        )


def test_derivation_rejects_a_write(dynamic_db: Database) -> None:
    with pytest.raises(UserError, match="read-only"):
        derive_classification(
            dynamic_db, query_sql="DELETE FROM core.dim_accounts", params=()
        )


def test_derivation_rejects_a_table_outside_the_classified_schemas(
    dynamic_db: Database,
) -> None:
    with pytest.raises(UserError, match="app, core, reports"):
        derive_classification(
            dynamic_db, query_sql="SELECT * FROM raw.plaid_accounts", params=()
        )


def test_derivation_rejects_duplicate_result_column_names(
    dynamic_db: Database,
) -> None:
    """Two result columns named ``x`` leave the class map unable to address one.

    ``redact_records`` masks by name, so one entry survives holding whichever
    class resolved last — and it would govern whichever value survives.
    """
    with pytest.raises(UserError, match="'x'"):
        derive_classification(
            dynamic_db,
            query_sql="SELECT 0 AS x, routing_number AS x FROM core.dim_accounts",
            params=(),
        )


def test_derivation_describes_an_overloaded_builtin_over_a_typed_parameter(
    dynamic_db: Database,
) -> None:
    """``date_part`` cannot resolve its overload against an untyped NULL.

    DuckDB 1.5.4 raises ``BinderException`` for ``date_part('year', $d)`` with
    ``$d`` bound to ``None``, so the DESCRIBE binds a typed sentinel of the
    parameter's declared annotation instead. The filter this exercises is as
    ordinary as they come, so a crash here would be a hard failure on a valid
    query rather than one of R2's soft-fail paths.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT COUNT(*) AS n FROM core.fct_transactions "
            "WHERE date_part('year', transaction_date) = date_part('year', $d)"
        ),
        params=(_param("d", date),),
    )

    assert dict(derived.classes) == {"n": DataClass.AGGREGATE}


def test_describe_names_match_a_value_bound_run_for_a_bare_placeholder(
    dynamic_db: Database,
) -> None:
    """Step 6's column names must be the names the run actually returns.

    This is why the DESCRIBE binds a typed sentinel *value* rather than
    substituting ``CAST(NULL AS <t>)`` for the placeholder. DuckDB names
    ``SELECT $x`` as ``$x`` but names ``SELECT CAST(NULL AS BIGINT)`` after the
    cast text, so the substitution would key the stored map by a name no run
    ever produces — and ``classify_columns`` fails closed on a name it cannot
    find, masking the column on every future run. Leaving the SQL byte-identical
    is what keeps the two in agreement.
    """
    sql = "SELECT $x, account_id FROM core.dim_accounts"

    derived = derive_classification(
        dynamic_db, query_sql=sql, params=(_param("x", int),)
    )
    executed = dynamic_db.execute(sql, {"x": 7})

    assert [column[0] for column in executed.description or ()] == list(derived.classes)


def test_derivation_rejects_a_placeholder_with_no_consistent_type(
    dynamic_db: Database,
) -> None:
    """One placeholder in two positions demanding incompatible types."""
    with pytest.raises(UserError, match="d"):
        derive_classification(
            dynamic_db,
            query_sql="SELECT date_part('year', $d) + $d AS y",
            params=(_param("d", date),),
        )


def test_derivation_classes_a_parameter_from_the_column_it_filters(
    dynamic_db: Database,
) -> None:
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
        ),
        params=(_param("acct", str),),
    )

    assert dict(derived.parameter_classes) == {"acct": DataClass.ROUTING_NUMBER}


def test_derivation_leaves_a_parameter_compared_against_nothing_unresolved(
    dynamic_db: Database,
) -> None:
    """A value in a position this module has not reasoned about fails closed.

    ``$prefix`` is a function argument, not the operand of a comparison, so
    nothing here can say which column it describes — and it plainly *could*
    carry one's data. The row-count exemption below is the only position exempt.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE starts_with(routing_number, $prefix)"
        ),
        params=(_param("prefix", str),),
    )

    assert dict(derived.parameter_classes) == {"prefix": DataClass.UNRESOLVED}


def test_derivation_classes_a_row_count_parameter_as_an_aggregate(
    dynamic_db: Database,
) -> None:
    """``LIMIT $n`` bounds rows returned, so it may carry a default.

    The class governs two things — whether a default may be stored, and whether
    the value renders as a literal — and a page size is safe for both. Every
    built-in already declares its own ``LIMIT ?`` parameter ``AGGREGATE``.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql="SELECT account_id FROM core.dim_accounts LIMIT $n",
        params=(_param("n", int, default=50, required=False),),
    )

    assert dict(derived.parameter_classes) == {"n": DataClass.AGGREGATE}


def test_derivation_leaves_a_parameter_compared_against_an_expression_unresolved(
    dynamic_db: Database,
) -> None:
    """The other side must be a bare column, not an expression holding one.

    ``$year`` is a year, not a transaction date. Reading the class off
    ``transaction_date`` because it appears somewhere in the comparison would
    attach a class to a value it does not describe — and the provenance renderer
    emits a literal for anything it believes is LOW.

    Distinct from the ``LIMIT $n`` case above, which never reaches this rule: its
    placeholder has no comparison at all, so that fixture leaves this one
    untested.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.fct_transactions "
            "WHERE date_part('year', transaction_date) = $year"
        ),
        params=(_param("year", int),),
    )

    assert dict(derived.parameter_classes) == {"year": DataClass.UNRESOLVED}
    assert (
        CLASSIFICATION[("core", "fct_transactions")]["transaction_date"]
        is not DataClass.UNRESOLVED
    ), "the fixture only isolates the rule while transaction_date has a real class"


def test_derivation_refuses_a_stored_default_on_an_above_low_parameter(
    dynamic_db: Database,
) -> None:
    """A CRITICAL default would be published unmasked by a bare catalog listing.

    ``_parameter_schema`` copies a non-required parameter's default verbatim
    into the published schema, and the catalog entry classes that whole schema
    ``AGGREGATE`` — LOW, unmasked. So the value never gets to be a default.
    """
    with pytest.raises(UserError, match="acct"):
        derive_classification(
            dynamic_db,
            query_sql=(
                "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
            ),
            params=(_param("acct", str, default="021000021", required=False),),
        )


def test_derivation_allows_a_stored_default_on_a_low_parameter(
    dynamic_db: Database,
) -> None:
    """The benign twin: a LOW-classed parameter keeps its default."""
    derived = derive_classification(
        dynamic_db,
        query_sql="SELECT routing_number FROM core.dim_accounts WHERE account_id = $a",
        params=(_param("a", str, default="acct_11112222", required=False),),
    )

    assert derived.parameter_classes["a"] is DataClass.RECORD_ID
    assert derived.parameter_classes["a"].tier is DataClass.RECORD_ID.tier


def test_derivation_reports_an_unresolvable_column_without_refusing_the_save(
    dynamic_db: Database,
) -> None:
    """R3: unresolvable columns produce a note, not a gate. The report saves.

    ``reports.test_summary`` is deployed but undeclared, so every column it
    exposes is a coverage gap. ``strict=False`` is what keeps that a note.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql="SELECT account_id, amount FROM reports.test_summary",
        params=(),
    )

    assert derived.unresolved_columns
    assert all(
        derived.classes[column] is DataClass.UNRESOLVED
        for column in derived.unresolved_columns
    )


# ---------------------------------------------------------------------------
# R4 — the fingerprint
# ---------------------------------------------------------------------------


def _fingerprint(db: Database, sql: str, **kwargs: Any) -> str:
    return class_fingerprint(
        db,
        query_sql=sql,
        classes=kwargs.get("classes", {"account_id": DataClass.RECORD_ID}),
        parameter_classes=kwargs.get("parameter_classes", {}),
        class_downgrades=kwargs.get("class_downgrades", {}),
    )


def test_fingerprint_is_stable_across_calls(dynamic_db: Database) -> None:
    sql = "SELECT account_id FROM core.dim_accounts"

    assert _fingerprint(dynamic_db, sql) == _fingerprint(dynamic_db, sql)


def test_fingerprint_moves_when_an_upstream_column_is_reclassified(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The dangerous direction: a column raised to CRITICAL after the save."""
    sql = "SELECT account_id FROM core.dim_accounts"
    before = _fingerprint(dynamic_db, sql)

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "display_name": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    assert _fingerprint(dynamic_db, sql) != before


def test_fingerprint_moves_when_the_derivation_version_changes(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A classifier change moves no input tuple, so the version term carries it."""
    sql = "SELECT account_id FROM core.dim_accounts"
    before = _fingerprint(dynamic_db, sql)

    monkeypatch.setattr(
        "moneybin.reports._framework.derive.DERIVATION_VERSION",
        DERIVATION_VERSION + 1,
    )

    assert _fingerprint(dynamic_db, sql) != before


def test_fingerprint_moves_when_a_class_mask_policy_changes(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A downgrade is an assertion about a tier and a transform, not a name.

    If ``RECORD_ID`` began masking under an unchanged classification, every
    stored map would keep its ``DataClass`` names and every input tuple would
    hold — so without the policy triples the fingerprint would not move and an
    approval granted against the old, weaker policy would keep applying.
    """
    sql = "SELECT account_id FROM core.dim_accounts"
    before = _fingerprint(dynamic_db, sql)

    # RECORD_ID's transform is passthrough today; this is the "it began masking
    # under an unchanged classification" shift the triples exist to catch.
    def _now_masks_wholly(_data_class: DataClass) -> MaskStrength:
        return MaskStrength.WHOLE

    monkeypatch.setattr(
        "moneybin.reports._framework.derive.mask_strength", _now_masks_wholly
    )

    assert _fingerprint(dynamic_db, sql) != before


def test_fingerprint_covers_the_downgrade_map(dynamic_db: Database) -> None:
    sql = "SELECT account_id FROM core.dim_accounts"
    plain = _fingerprint(dynamic_db, sql)
    downgraded = _fingerprint(
        dynamic_db,
        sql,
        class_downgrades={
            "account_id": {
                "from": DataClass.TXN_AMOUNT.value,
                "to": DataClass.AGGREGATE.value,
                "reason": "z-score reveals no amount",
            }
        },
    )

    assert plain != downgraded


# ---------------------------------------------------------------------------
# The second constructor
# ---------------------------------------------------------------------------


def test_spec_from_row_builds_a_spec_the_shared_runner_executes(
    dynamic_db: Database,
) -> None:
    """Everything downstream of ``ReportSpec`` must run unchanged."""
    dynamic = spec_from_row(dynamic_db, _saved(dynamic_db))

    result = run_report(dynamic.spec, dynamic_db, max_rows=10)

    assert dynamic.spec.view is None
    assert dynamic.spec.report_id == "user:rab12cd34ef56"
    assert result.columns == ["account_id"]
    assert {record["account_id"] for record in result.records} == {
        "acct_11112222",
        "acct_99998888",
    }
    assert not dynamic.degraded


def test_spec_from_row_synthesizes_columns_from_the_class_map(
    dynamic_db: Database,
) -> None:
    dynamic = spec_from_row(dynamic_db, _saved(dynamic_db))

    assert [column.name for column in dynamic.spec.columns] == ["account_id"]
    assert dynamic.spec.columns[0].data_class is DataClass.RECORD_ID


def test_spec_from_row_derives_provenance_from_the_tables_the_query_reads(
    dynamic_db: Database,
) -> None:
    """``provenance`` feeds the export receipt's ``lineage``.

    A dynamic report's export manifest writes ``"source": null`` because it has
    no single backing view, so the receipt's lineage is the only place the read
    set appears. Deriving it from the SQL is what keeps it from going stale.
    """
    dynamic = spec_from_row(
        dynamic_db,
        _saved(
            dynamic_db,
            query_sql=(
                "SELECT a.account_id FROM core.dim_accounts AS a "
                "JOIN core.fct_transactions AS t USING (account_id)"
            ),
            classes={"account_id": "record_id"},
        ),
    )

    assert dynamic.spec.semantics.provenance == (
        "core.dim_accounts",
        "core.fct_transactions",
    )


def test_spec_from_row_states_that_its_financial_semantics_are_unknown(
    dynamic_db: Database,
) -> None:
    semantics = spec_from_row(dynamic_db, _saved(dynamic_db)).spec.semantics

    assert semantics.kind == "unknown"
    assert semantics.unit is None
    assert semantics.sign is None
    assert semantics.time_basis is None


def test_spec_from_row_serves_the_stored_map_without_re_resolving(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A matching fingerprint means no lineage work — the stored map governs.

    Asserting the *class* would not prove this: derivation returns the same
    answer here, so a match and a re-resolution are indistinguishable by their
    result. Making re-derivation raise is what separates them.

    Note the fingerprint covers the classes in the stored map itself, so a
    hand-edited map cannot reach this branch — tampering invalidates its own key.
    """
    row = _saved(dynamic_db)

    def _explode(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the match branch must not re-derive")

    monkeypatch.setattr(
        "moneybin.reports._framework.dynamic.derive_classification", _explode
    )

    dynamic = spec_from_row(dynamic_db, row)

    assert dict(dynamic.spec.classes) == {"account_id": DataClass.RECORD_ID}
    assert not dynamic.degraded


def test_spec_from_row_fails_closed_when_a_column_is_reclassified_upward(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The #330 shape persisted in a durable artifact.

    A stale stored map keeps serving a now-sensitive column at its old weaker
    class. Re-resolution must win, and the response must say it degraded.
    """
    row = _saved(dynamic_db)
    assert row["classes"] == {"account_id": DataClass.RECORD_ID.value}

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "account_id": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.spec.classes["account_id"] is DataClass.UNRESOLVED
    assert dynamic.degraded
    assert dynamic.degraded_reason is not None
    assert dynamic.degraded_reason.startswith(DEGRADED_STALE_CLASSIFICATION)
    assert "account_id" in dynamic.degraded_reason

    masked = run_report(dynamic.spec, dynamic_db, max_rows=10)
    assert {record["account_id"] for record in masked.records} == {"*****"}


def test_spec_from_row_serves_normally_when_drift_leaves_the_map_unchanged(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fingerprint mismatch costs work, never correctness.

    Reclassifying a column the query does not project moves the fingerprint but
    not the derived map, so the run proceeds undegraded.
    """
    row = _saved(dynamic_db)

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "display_name": DataClass.MERCHANT_NAME,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.spec.classes["account_id"] is DataClass.RECORD_ID
    assert not dynamic.degraded


def test_spec_from_row_reapplies_a_downgrade_whose_premise_still_holds(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A legitimately downgraded report must not degrade forever.

    Its stored map differs from raw derivation *by design*, so comparing
    derivation against the stored map would report a change on every run — and
    since reads never refresh the fingerprint, it would stay degraded from the
    first unrelated classification change onward.
    """
    row = _saved(
        dynamic_db, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )
    assert row["classes"] == {"spend": DataClass.TXN_AMOUNT.value}
    row["classes"] = {"spend": DataClass.AGGREGATE.value}
    row["class_downgrades"] = {
        "spend": {
            "from": DataClass.TXN_AMOUNT.value,
            "to": DataClass.AGGREGATE.value,
            "reason": "monthly total reveals no single amount",
        }
    }
    row["class_fingerprint"] = "stale-forces-the-mismatch-branch"

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.spec.classes["spend"] is DataClass.AGGREGATE
    assert not dynamic.degraded


def test_spec_from_row_drops_a_downgrade_whose_derived_class_moved(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A downgrade is an assertion about one class on one column, not a licence.

    Approved for ``TXN_AMOUNT → AGGREGATE``; if derivation now yields
    ``ROUTING_NUMBER`` there, reapplying by column name alone would let an
    approval collected against a weak class suppress a stronger one.
    """
    row = _saved(
        dynamic_db, query_sql="SELECT SUM(amount) AS spend FROM core.fct_transactions"
    )
    row["classes"] = {"spend": DataClass.AGGREGATE.value}
    row["class_downgrades"] = {
        "spend": {
            "from": DataClass.TXN_AMOUNT.value,
            "to": DataClass.AGGREGATE.value,
            "reason": "monthly total reveals no single amount",
        }
    }
    row["class_fingerprint"] = "stale-forces-the-mismatch-branch"

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "fct_transactions")] = {
        **CLASSIFICATION[("core", "fct_transactions")],
        "amount": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.spec.classes["spend"] is DataClass.UNRESOLVED
    assert dynamic.degraded


def test_spec_from_row_drops_a_default_reclassification_made_sensitive(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The save gate was right when it ran; drift can still outdate it.

    ``account_id`` is ``RECORD_ID`` (LOW), so a default on a filter against it
    saves legally. Raising it to CRITICAL afterwards must not make a *read*
    raise — R4's answer to drift is failing closed and degrading, not taking the
    report down — and the now-sensitive default must not reach the catalog,
    which publishes defaults unmasked.
    """
    row = _saved(
        dynamic_db,
        query_sql=(
            "SELECT institution_name FROM core.dim_accounts WHERE account_id = $who"
        ),
        params=[{"name": "who", "annotation": "str", "default": "acct_11112222"}],
    )
    assert row["params"][0]["data_class"] == DataClass.RECORD_ID.value

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "account_id": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.spec.params[0].default is None
    assert dynamic.spec.params[0].required
    assert dynamic.degraded


def test_spec_from_row_re_resolves_stored_parameter_classes(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parameter classes go stale by the same mechanism the columns do.

    Report inspection renders a LOW-classed binding as a literal, so a stale
    parameter class would print the filter that selected the rows in the clear
    while the rows themselves were masked.
    """
    row = _saved(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts WHERE display_name = $who"
        ),
        params=[{"name": "who", "annotation": "str"}],
    )
    assert row["params"] == [
        {"name": "who", "annotation": "str", "data_class": DataClass.USER_NOTE.value}
    ]
    row["class_fingerprint"] = "stale-forces-the-mismatch-branch"

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "display_name": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.spec.params[0].data_class is DataClass.UNRESOLVED
    assert dynamic.degraded
    assert "who" in (dynamic.degraded_reason or "")


# ---------------------------------------------------------------------------
# R8 — the synthesized runner binds by name
# ---------------------------------------------------------------------------


def test_synthesized_runner_binds_a_parameter_by_name(dynamic_db: Database) -> None:
    dynamic = spec_from_row(
        dynamic_db,
        _saved(
            dynamic_db,
            query_sql=(
                "SELECT account_id FROM core.dim_accounts WHERE account_id = $who"
            ),
            params=[{"name": "who", "annotation": "str"}],
        ),
    )

    result = run_report(dynamic.spec, dynamic_db, max_rows=10, who="acct_99998888")

    assert [record["account_id"] for record in result.records] == ["acct_99998888"]


def test_synthesized_runner_fills_a_declared_default(dynamic_db: Database) -> None:
    dynamic = spec_from_row(
        dynamic_db,
        _saved(
            dynamic_db,
            query_sql=(
                "SELECT account_id FROM core.dim_accounts WHERE account_id = $who"
            ),
            params=[{"name": "who", "annotation": "str", "default": "acct_11112222"}],
        ),
    )

    result = run_report(dynamic.spec, dynamic_db, max_rows=10)

    assert [record["account_id"] for record in result.records] == ["acct_11112222"]


def test_synthesized_runner_rejects_an_unknown_parameter_name(
    dynamic_db: Database,
) -> None:
    """R8's deciding property: an unknown name raises rather than mis-binding."""
    dynamic = spec_from_row(dynamic_db, _saved(dynamic_db))

    with pytest.raises(UserError, match="nope"):
        run_report(dynamic.spec, dynamic_db, max_rows=10, nope="x")


def test_synthesized_runner_requires_a_parameter_with_no_default(
    dynamic_db: Database,
) -> None:
    dynamic = spec_from_row(
        dynamic_db,
        _saved(
            dynamic_db,
            query_sql=(
                "SELECT account_id FROM core.dim_accounts WHERE account_id = $who"
            ),
            params=[{"name": "who", "annotation": "str"}],
        ),
    )

    with pytest.raises(UserError, match="who"):
        run_report(dynamic.spec, dynamic_db, max_rows=10)


def test_stored_params_omit_an_unresolved_class_rather_than_declaring_it() -> None:
    """``data_class`` is derived, never declared — an absent one means unresolved.

    ``taxonomy.py`` notes that *declaring* a column unresolved defeats the
    completeness tests that exist to find gaps; the same holds for a parameter.
    """
    declared = (_param("since", date), _param("top", int, default=5, required=False))
    entries = stored_params(
        declared, {"since": DataClass.TXN_DATE, "top": DataClass.UNRESOLVED}
    )

    assert entries == [
        {"name": "since", "annotation": "date", "data_class": "txn_date"},
        {"name": "top", "annotation": "int", "default": 5},
    ]


def test_unknown_semantics_states_every_field_it_cannot_derive() -> None:
    semantics = unknown_semantics(provenance=("core.dim_accounts",))

    assert semantics.kind == "unknown"
    assert semantics.provenance == ("core.dim_accounts",)
    assert semantics.exclusions == ()


# ---------------------------------------------------------------------------
# What the row hands to the runner, and what an undecodable row costs
# ---------------------------------------------------------------------------


def test_the_runner_binds_the_same_parameter_class_the_spec_publishes(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two readers of one fact must not disagree on the drift path.

    ``spec.params`` carries the re-resolved class and governs redaction; the
    binding the runner returns carries the class the provenance renderer reads to
    decide whether a filter value may be printed as a literal. A runner closed
    over the *stored* parameters keeps the pre-drift class, so ``explain`` prints
    the value in the clear inside the same response that calls the parameter
    unresolved and masks every row.
    """
    row = _saved(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
        ),
        params=[{"name": "acct", "annotation": "str"}],
    )
    assert row["params"] == [
        {"name": "acct", "annotation": "str", "data_class": "routing_number"}
    ]

    # Move the filtered column's class so re-resolution fails the parameter
    # closed — the stored `routing_number` is now stale.
    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "routing_number": DataClass.TXN_AMOUNT,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    dynamic = spec_from_row(dynamic_db, row)
    published = {
        parameter.name: parameter.data_class for parameter in dynamic.spec.params
    }
    query = dynamic.spec.runner(dynamic_db, acct="021000021")
    # R8 binds by name, so the runner always returns a mapping.
    assert isinstance(query.params, Mapping)

    assert published == {"acct": DataClass.UNRESOLVED}
    assert query.params["acct"].data_class is published["acct"]


def test_the_runner_binds_the_stored_class_when_nothing_drifted(
    dynamic_db: Database,
) -> None:
    """The benign twin: the binding still carries the real class on a match.

    Binding ``FAIL_CLOSED_CLASS`` unconditionally would satisfy the drift test
    above while withholding every filter value from every explanation.
    """
    row = _saved(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
        ),
        params=[{"name": "acct", "annotation": "str"}],
    )

    dynamic = spec_from_row(dynamic_db, row)
    query = dynamic.spec.runner(dynamic_db, acct="021000021")
    assert isinstance(query.params, Mapping)

    assert query.params["acct"].data_class is DataClass.ROUTING_NUMBER


@pytest.mark.parametrize(
    ("overrides", "detail"),
    [
        ({"params": [{"name": "x", "annotation": "complex"}]}, "annotation token"),
        ({"classes": {"account_id": "not_a_data_class"}}, "class value"),
    ],
    ids=["unknown-annotation-token", "unknown-class-value"],
)
def test_a_row_whose_stored_tokens_no_longer_decode_stays_listed_and_masked(
    dynamic_db: Database, overrides: dict[str, Any], detail: str
) -> None:
    """One bad row must not take down every tier's catalog.

    Stored tokens are written from an allowlist, so this becomes reachable the
    moment a release renames a ``DataClass`` or drops a parameter type — and then
    it is every saved row at once. The adjacent unresolvable-query branch already
    decided the answer for a row that cannot be classified: it stays listed and
    wholly masked. Raising out of ``spec_from_row`` instead makes a *built-in*
    unreachable, which is the one thing archiving and drift both refuse to do.
    """
    dynamic = spec_from_row(dynamic_db, _row(**overrides))

    assert set(dynamic.spec.classes.values()) <= {DataClass.UNRESOLVED}
    assert dynamic.degraded
    assert dynamic.degraded_reason is not None
    assert dynamic.degraded_reason.startswith(DEGRADED_UNREADABLE_ROW)


def test_spec_from_row_reports_whether_the_stored_row_is_archived(
    dynamic_db: Database,
) -> None:
    """Archived is a fact about the row, so it travels with the spec built from it.

    Without it the catalog cannot both *resolve* an archived report (R5 promises
    it stays runnable) and *hide* it from a listing — the two needs read the same
    ``is_active`` column, and only one of them can win a repo-level filter.
    """
    assert spec_from_row(dynamic_db, _row(is_active=True)).archived is False
    assert spec_from_row(dynamic_db, _row(is_active=False)).archived is True


def test_a_date_or_decimal_default_is_stored_as_a_json_scalar() -> None:
    """``params`` is a JSON column, and two declarable types are not JSON-native.

    ``json.dumps`` raises ``TypeError`` on a ``date`` or ``Decimal``, and
    ``classify_user_error`` does not classify it — so the save reaches the user as
    a bare traceback with no code and no envelope.
    """
    declared = (
        _param("since", date, default=date(2026, 1, 1), required=False),
        _param("floor", Decimal, default=Decimal("10.50"), required=False),
    )
    entries = stored_params(
        declared, {"since": DataClass.AGGREGATE, "floor": DataClass.AGGREGATE}
    )

    assert json.loads(json.dumps(entries)) == entries
    assert [entry["default"] for entry in entries] == ["2026-01-01", "10.50"]


def test_a_stored_json_default_is_read_back_as_its_declared_type() -> None:
    """The other half of the round trip: the runner must bind a real date.

    Storing an ISO string and handing that string back would bind a VARCHAR where
    the report declared a DATE, leaving DuckDB's implicit cast to decide what the
    filter means.
    """
    declared = declared_params([
        {"name": "since", "annotation": "date", "default": "2026-01-01"},
        {"name": "floor", "annotation": "decimal", "default": "10.50"},
    ])

    assert [parameter.default for parameter in declared] == [
        date(2026, 1, 1),
        Decimal("10.50"),
    ]
    assert [parameter.required for parameter in declared] == [False, False]
