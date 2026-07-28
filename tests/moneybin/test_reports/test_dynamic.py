"""The dynamic-report second constructor and its save-time derivation pipeline.

Covers R2 (save-time classification), R4 (fingerprint drift), and the stored
row → ``ReportSpec`` constructor from ``docs/specs/reports-dynamic.md``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.redaction import MaskStrength, mask_strength
from moneybin.privacy.sql_lineage import FAIL_CLOSED_CLASS, SqlSchemaError
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
    DEGRADED_UNRESOLVABLE_QUERY,
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


def test_derivation_names_a_declared_parameter_the_query_never_references(
    dynamic_db: Database,
) -> None:
    """DuckDB refuses the excess binding; the author must learn why.

    Every declared parameter is bound on the DESCRIBE, and DuckDB errors outright
    on an excess named binding — so this save cannot succeed either way. It failed
    through ``_describe_result_columns``'s ``duckdb.Error`` handler, whose message
    ("every column must exist, and each parameter's declared type must fit every
    position it is used in") is false for this case and sends the author hunting a
    column and a type that are both fine.
    """
    with pytest.raises(UserError) as raised:
        derive_classification(
            dynamic_db,
            query_sql="SELECT account_id FROM core.dim_accounts",
            params=(_param("unused", str),),
        )

    # The code, not the prose: the DESCRIBE handler *also* names ``$unused``, in
    # its "(declared $unused: str)" tail, so matching on the name alone passes
    # either way and isolates neither refusal.
    assert raised.value.code == error_codes.REPORT_QUERY_INVALID
    assert "never references" in str(raised.value)


def test_derivation_classes_a_parameter_from_the_column_it_filters(
    dynamic_db: Database,
) -> None:
    """The benign twin of the refusal above: a referenced parameter is fine."""
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
        ),
        params=(_param("acct", str),),
    )

    assert dict(derived.parameter_classes) == {"acct": DataClass.ROUTING_NUMBER}


def test_derivation_refuses_two_declarations_of_one_parameter_name(
    dynamic_db: Database,
) -> None:
    """One name, two contracts — the published schema cannot express both.

    Every map keyed by parameter name collapses the pair (``parameter_classes``
    here, ``bindings`` on the DESCRIBE), but ``params`` stores both entries, and
    ``_parameter_schema`` reads that list: it appends ``n`` to ``required`` from
    the first entry, then *overwrites* ``properties["n"]`` from the second. The
    catalog then publishes ``n`` as required **and** carrying a default, under
    ``additionalProperties: false`` — a caller trusting the default is rejected
    for omitting it.

    ``LIMIT $n`` is the fixture because it classes AGGREGATE, so the default is
    one ``_refuse_sensitive_defaults`` permits; the name is referenced, so
    ``_refuse_unused_parameters`` passes; and the bindings collapse, so DESCRIBE
    succeeds. No other guard can claim this refusal.
    """
    with pytest.raises(UserError) as raised:
        derive_classification(
            dynamic_db,
            query_sql="SELECT account_id FROM core.dim_accounts LIMIT $n",
            params=(_param("n", int), _param("n", int, default=50, required=False)),
        )

    assert raised.value.code == error_codes.REPORT_PARAMETER_DUPLICATE
    assert "declared more than once" in str(raised.value)


def test_derivation_accepts_two_parameters_with_distinct_names(
    dynamic_db: Database,
) -> None:
    """The benign twin: a guard on *repeated* names must not refuse two names."""
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE routing_number = $acct LIMIT $n"
        ),
        params=(_param("acct", str), _param("n", int, default=50, required=False)),
    )

    assert dict(derived.parameter_classes) == {
        "acct": DataClass.ROUTING_NUMBER,
        "n": DataClass.AGGREGATE,
    }


# A free-text merchant name no `SanitizedLogFormatter` pattern can recognise —
# it masks SSNs, runs of 8+ digits, and dollar amounts. A saved query may hold
# one as an inline literal, and DuckDB and sqlglot both quote the statement they
# failed on.
_MERCHANT = "ACME PLUMBING"


def _assert_log_names_the_failure_without_quoting_it(
    caplog: pytest.LogCaptureFixture, cause: BaseException, prefix: str
) -> None:
    """The log named which save step failed, by type and query digest only."""
    assert prefix in caplog.text
    assert type(cause).__name__ in caplog.text
    assert "sql sha256=" in caplog.text
    assert str(cause) not in caplog.text
    assert _MERCHANT not in caplog.text


def test_a_failed_qualification_logs_no_query_text(
    dynamic_db: Database,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The qualify step's own handler must not log the query either.

    The user-facing error is already generic. The log record is the other, more
    durable boundary: the statement being saved is user-authored text that may
    carry an inline merchant name or description, and
    ``.claude/rules/security.md`` forbids either in a log file.

    ``expand_star`` is forced to raise rather than provoked into it, because on
    this path nothing provokes it: ``expand_star`` qualifies with
    ``validate_qualify_columns=False``, so an unknown table, an unknown column,
    and an ambiguous one all survive it and are rejected by DuckDB at DESCRIBE
    (the next test's site). sqlglot still owns the arm — a future release that
    validates more eagerly would route real saves through it — and the arm's
    message is what this asserts.
    """
    from moneybin.reports._framework import derive as derive_module

    def _raise(*_: object, **__: object) -> None:
        raise SqlSchemaError(f"cannot resolve: SELECT x WHERE note = '{_MERCHANT}'")

    monkeypatch.setattr(derive_module, "expand_star", _raise)

    with caplog.at_level(logging.WARNING, logger="moneybin.reports._framework.derive"):
        with pytest.raises(UserError) as raised:
            derive_classification(
                dynamic_db,
                query_sql="SELECT account_id FROM core.dim_accounts",
                params=(),
            )
    assert raised.value.code == error_codes.SQL_UNKNOWN_TABLE
    cause = raised.value.__cause__
    assert cause is not None
    _assert_log_names_the_failure_without_quoting_it(
        caplog, cause, "user report save: unknown table/column"
    )


def test_a_failed_describe_logs_no_query_text(
    dynamic_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The DESCRIBE step has its own handler, and DuckDB echoes the statement.

    Reached by a parameter whose declared type does not fit the position it is
    used in — the one failure that survives qualification and lineage and shows
    up only when DuckDB binds the sentinel.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.reports._framework.derive"):
        with pytest.raises(UserError) as raised:
            derive_classification(
                dynamic_db,
                query_sql=(
                    "SELECT account_id FROM core.dim_accounts "  # noqa: S608  # `_MERCHANT` is a test constant; the query is meant to fail
                    f"WHERE display_name = '{_MERCHANT}' "
                    "AND date_part($n, CURRENT_DATE) = 1"
                ),
                params=(_param("n", int),),
            )
    assert raised.value.code == error_codes.REPORT_QUERY_UNRESOLVABLE
    cause = raised.value.__cause__
    assert cause is not None
    _assert_log_names_the_failure_without_quoting_it(
        caplog, cause, "user report save: could not describe result columns"
    )


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

    Matched on the code and ``details`` rather than on the name in the message: the
    name is the author's own text and the message reaches ``logger.error``, so
    asserting it appeared there pinned the leak in place.
    """
    with pytest.raises(UserError) as caught:
        derive_classification(
            dynamic_db,
            query_sql=(
                "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
            ),
            params=(_param("acct", str, default="021000021", required=False),),
        )

    assert caught.value.code == error_codes.REPORT_PARAMETER_DEFAULT_NOT_ALLOWED
    assert caught.value.details is not None
    assert caught.value.details["parameter"] == "acct"


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


def test_fingerprint_moves_when_an_unselected_input_class_changes_policy(
    dynamic_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The policy triples must cover what the query *reads*, not what it stores.

    A derived output takes the strongest class among its inputs, so an input
    that lost that contest still decides the answer the moment its own tier or
    transform is raised. ``INSTITUTION`` is a column of ``core.dim_accounts``
    and nothing this query stores — not an output class, not a parameter, not a
    downgrade — so only the read set can carry it into the key.
    """
    sql = "SELECT account_id FROM core.dim_accounts"
    before = _fingerprint(dynamic_db, sql)

    def _institution_now_masks_wholly(data_class: DataClass) -> MaskStrength:
        if data_class is DataClass.INSTITUTION:
            return MaskStrength.WHOLE
        return mask_strength(data_class)

    monkeypatch.setattr(
        "moneybin.reports._framework.derive.mask_strength",
        _institution_now_masks_wholly,
    )

    assert _fingerprint(dynamic_db, sql) != before


def test_fingerprint_moves_when_the_query_text_changes(dynamic_db: Database) -> None:
    """Two projections over one table share every other term in the key.

    ``read_column_classes`` is deliberately *table*-scoped, so swapping which
    column a query selects moves neither it nor the policy triples: without the
    query term the fingerprint matched and ``spec_from_row`` served the stored
    LOW map for a CRITICAL value. ``UserReportsService.update`` re-derives on any
    SQL change, so this is defence in depth for ``UserReportsRepo.set``, which
    accepts ``query_sql`` on its own.
    """
    low = _fingerprint(dynamic_db, "SELECT account_type AS v FROM core.dim_accounts")
    critical = _fingerprint(
        dynamic_db, "SELECT routing_number AS v FROM core.dim_accounts"
    )

    assert low != critical


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


def test_the_reclassified_parameter_warning_withholds_the_parameter_name(
    dynamic_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A declared parameter's name is user-authored text, like a column alias.

    ``--param amazon_spend:str=...`` is a name a user chooses, and
    ``SanitizedLogFormatter`` cannot tell it from a merchant label. The record
    keeps the count and the new class; the response already names the parameter
    it made required.
    """
    row = _saved(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts WHERE account_id = $amazon_spend"
        ),
        params=[
            {
                "name": "amazon_spend",
                "annotation": "str",
                "default": "acct_11112222",
            }
        ],
    )

    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "account_id": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    with caplog.at_level(logging.WARNING):
        dynamic = spec_from_row(dynamic_db, row)

    # The invariant the warning exists to report still holds.
    (parameter,) = dynamic.spec.params
    assert parameter.required is True
    assert parameter.default is None

    logged = [record.getMessage() for record in caplog.records]
    assert logged, "expected a record about the dropped default"
    assert not any("amazon_spend" in message for message in logged)


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
    assert dynamic.degraded_code == DEGRADED_STALE_CLASSIFICATION

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


def test_spec_from_row_masks_a_row_whose_downgrade_entry_is_half_written(
    dynamic_db: Database,
) -> None:
    """A downgrade entry missing a side degrades its own row, not the catalog.

    ``class_fingerprint`` validated each side only ``if entry.get(side)``, so an
    entry whose ``to`` is empty keyed a fingerprint without complaint and then
    reached ``DataClass("")`` in the reapplication loop. That ``ValueError`` is
    raised outside every handler on this path — the inner one catches
    ``UserError`` alone — so one corrupt row failed ``user_report_specs`` for
    every tier at once, built-ins included. The row is what is broken, so the
    row is what degrades.

    The stale fingerprint is load-bearing: an entry naming a class the stored map
    already holds moves no policy triple, so without it the key still matches and
    the reapplication loop never runs.
    """
    row = _saved(dynamic_db)
    row["class_downgrades"] = {"account_id": {"from": "record_id", "to": ""}}
    row["class_fingerprint"] = "stale-forces-the-mismatch-branch"

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.degraded
    assert DEGRADED_UNREADABLE_ROW in (dynamic.degraded_reason or "")
    # The reason embeds the raw exception, which quotes the stored row back; the
    # code is what a redacted export publishes in its place.
    assert dynamic.degraded_code == DEGRADED_UNREADABLE_ROW
    assert dict(dynamic.spec.classes) == {"account_id": FAIL_CLOSED_CLASS}


def test_derivation_masks_a_projected_parameter_carrying_a_critical_value(
    dynamic_db: Database,
) -> None:
    """A projected ``$param`` returns a value lineage cannot see.

    ``SELECT $acct AS acct … WHERE routing_number = $acct`` puts no *column* in
    the projection, so ``resolve_output_classes`` had nothing to trace and landed
    on ``AGGREGATE`` — passthrough. The report then returned the supplied routing
    number in its own rows, in the clear, with the parameter beside it masked.

    The projection now takes the parameter's class. For any *projected*
    placeholder that is ``FAIL_CLOSED_CLASS`` today, because
    ``resolve_placeholder_classes`` resolves a placeholder from the column it is
    compared against and one appearing in both positions "identifies neither" —
    so the run masks wholly, which is the answer this leak needed.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT $acct AS acct FROM core.dim_accounts WHERE routing_number = $acct"
        ),
        params=(_param("acct"),),
    )

    assert derived.classes["acct"] is FAIL_CLOSED_CLASS


def test_derivation_leaves_an_ordinary_projection_untouched(
    dynamic_db: Database,
) -> None:
    """The benign twin: only projections that return a parameter are affected.

    Without this, raising every projection to a fail-closed floor would pass the
    test above while masking every saved report in the catalog.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
        ),
        params=(_param("acct"),),
    )

    assert derived.classes["account_id"] is DataClass.RECORD_ID


def test_derivation_masks_a_parameter_projected_by_a_later_union_branch(
    dynamic_db: Database,
) -> None:
    """A set operation names its output from branch 0 and draws values from all.

    The first fix for the projected-parameter leak keyed the inherited class by
    each ``SELECT``'s own local alias. A later branch's alias names no output
    column — DuckDB calls this result ``value``, from branch 0 — so the entry
    landed under ``other``, a key nothing reads, and the routing number came back
    in the clear anyway. Classification per output *position* is what the
    combining rule already does for columns; a parameter is not exempt.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT 'x' AS value "
            "UNION ALL "
            "SELECT $acct AS other FROM core.dim_accounts WHERE routing_number = $acct"
        ),
        params=(_param("acct"),),
    )

    assert derived.classes["value"] is FAIL_CLOSED_CLASS


def test_derivation_masks_a_parameter_projected_through_a_derived_table(
    dynamic_db: Database,
) -> None:
    """The value reaches the output through a source, not through the projection.

    The outer projection is a bare column reference, so walking the outermost
    projection for placeholders finds none — the parameter is projected one scope
    down. Resolving it needs the same scope walk that classifies a derived
    table's columns, which is why this belongs to the one classifier rather than
    to a second rule that has to re-implement the walk.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT t.v AS acct FROM ("
            "SELECT $acct AS v FROM core.dim_accounts WHERE routing_number = $acct"
            ") t"
        ),
        params=(_param("acct"),),
    )

    assert derived.classes["acct"] is FAIL_CLOSED_CLASS


def test_derivation_leaves_a_union_of_ordinary_columns_untouched(
    dynamic_db: Database,
) -> None:
    """The set-operation benign twin.

    A parameter used only as a filter leaves both branches classified from their
    own columns, so the fix above cannot be a blanket floor over every position
    a set operation returns.
    """
    derived = derive_classification(
        dynamic_db,
        query_sql=(
            "SELECT account_id AS value FROM core.dim_accounts "
            "UNION ALL "
            "SELECT account_id AS other FROM core.dim_accounts "
            "WHERE routing_number = $acct"
        ),
        params=(_param("acct"),),
    )

    assert derived.classes["value"] is DataClass.RECORD_ID


def test_spec_from_row_degrades_when_a_stored_projection_disappears(
    dynamic_db: Database,
) -> None:
    """Drift is a difference between two maps, and one direction went unread.

    The comparison walked the freshly derived map and looked each name up in the
    stored one, so a column present in the stored map and absent from the new one
    was never visited: ``changed_columns`` came back empty and the report served
    as healthy while its output contract had silently narrowed. A saved
    ``SELECT *`` reaches this the moment an upstream column is retired.

    Nothing can be masked for a column that no longer exists — the point is that
    the response says the contract moved rather than quietly returning less.
    """
    row = _saved(dynamic_db, query_sql="SELECT * FROM core.dim_accounts")
    assert "institution_name" in row["classes"]
    dynamic_db.execute("ALTER TABLE core.dim_accounts DROP COLUMN institution_name")

    dynamic = spec_from_row(dynamic_db, row)

    assert "institution_name" not in dynamic.spec.classes
    assert dynamic.degraded
    assert "institution_name" in (dynamic.degraded_reason or "")


def test_spec_from_row_drops_a_downgrade_the_current_policy_would_refuse(
    dynamic_db: Database,
) -> None:
    """Moving the fingerprint is only half of what a policy change has to buy.

    The triples exist so a tier or transform change forces re-derivation — but
    re-derivation reapplied the approval on class *identity* alone, so a pair
    the current ``is_weaker_class`` refuses went on applying. This is the exact
    pair that function's docstring names: ``ROUTING_NUMBER → ACCOUNT_IDENTIFIER``
    holds CRITICAL and drops masking from whole to partial, so serving it
    publishes the real last four digits where every row showed ``'*****'``.

    A stored entry can only be this shape if the policy moved under it — an
    older build's approval, or one granted before either class was retiered —
    which is precisely the case the fingerprint forces this branch for. And
    because the stored map already holds ``ACCOUNT_IDENTIFIER``, reapplying left
    the maps identical and the report served normally, undegraded.
    """
    row = _saved(
        dynamic_db, query_sql="SELECT routing_number AS rn FROM core.dim_accounts"
    )
    row["classes"] = {"rn": DataClass.ACCOUNT_IDENTIFIER.value}
    row["class_downgrades"] = {
        "rn": {
            "from": DataClass.ROUTING_NUMBER.value,
            "to": DataClass.ACCOUNT_IDENTIFIER.value,
            "reason": "approved under a policy that no longer holds",
        }
    }
    row["class_fingerprint"] = "stale-forces-the-mismatch-branch"

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.spec.classes["rn"] is FAIL_CLOSED_CLASS
    assert dynamic.degraded

    masked = run_report(dynamic.spec, dynamic_db, max_rows=10)
    assert {record["rn"] for record in masked.records} == {"*****"}


def test_spec_from_row_keeps_a_downgrade_the_current_policy_still_allows(
    dynamic_db: Database,
) -> None:
    """The benign twin: a legal approval must still survive the same branch.

    ``TXN_AMOUNT (HIGH) → AGGREGATE (LOW)`` drops a tier and does not strengthen
    masking, so the recheck has nothing to say about it. Without this, a recheck
    that simply refused to reapply anything would pass the test above while
    masking every legitimately downgraded column on the next unrelated drift.
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
    "overrides",
    [
        {"params": [{"name": "x", "annotation": "complex"}]},
        {"classes": {"account_id": "not_a_data_class"}},
        {"params": [{"annotation": "str"}]},
        {"query_sql": "SELECT FROM WHERE"},
    ],
    ids=[
        "unknown-annotation-token",
        "unknown-class-value",
        "parameter-entry-missing-its-name",
        "unparseable-stored-sql",
    ],
)
def test_a_row_whose_stored_text_no_longer_decodes_stays_listed_and_masked(
    dynamic_db: Database, overrides: dict[str, Any]
) -> None:
    """One bad row must not take down every tier's catalog.

    Every stored *text* this row is rebuilt from is covered, not only the two
    token columns: the parameter entries, the class values, and the SQL that
    ``class_fingerprint`` and ``read_tables`` both parse. All are written from
    allowlists, so this becomes reachable the moment a release renames a
    ``DataClass``, drops a parameter type, or ships a parser that no longer
    accepts text an earlier one wrote — and then it is every saved row at once.
    The adjacent unresolvable-query branch already decided the answer for a row
    that cannot be classified: it stays listed and wholly masked. Raising out of
    ``spec_from_row`` instead makes a *built-in* unreachable, which is the one
    thing archiving and drift both refuse to do.

    The last two cases raise ``KeyError`` and ``SqlParseError`` — neither a
    ``UserError`` nor a ``ValueError``, so each escaped the original guard and
    took ``reports list`` / ``run`` / ``export report`` and the ``reports`` MCP
    tool down with it for every tier.
    """
    dynamic = spec_from_row(dynamic_db, _row(**overrides))

    assert set(dynamic.spec.classes.values()) <= {DataClass.UNRESOLVED}
    assert dynamic.degraded
    assert dynamic.degraded_reason is not None
    assert dynamic.degraded_reason.startswith(DEGRADED_UNREADABLE_ROW)


def test_a_row_whose_query_can_no_longer_be_classified_stays_listed_and_masked(
    dynamic_db: Database,
) -> None:
    """The third degraded branch: the SQL parses, but the table it reads is gone.

    Retiring a ``core`` table is a release doing it, not the author, so the row
    keeps its place in the catalog and every column masks. Its reason embeds the
    derivation error, which quotes the query back — hence the code, on the same
    terms as the two branches beside it. A retired *column* is not this branch:
    lineage fails that one closed to ``UNRESOLVED`` and it degrades as stale, so
    the table has to actually leave the catalog to reach this one.
    """
    row = _saved(dynamic_db)
    dynamic_db.execute("DROP TABLE core.dim_accounts")

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.degraded
    assert dynamic.degraded_reason is not None
    assert dynamic.degraded_reason.startswith(DEGRADED_UNRESOLVABLE_QUERY)
    assert dynamic.degraded_code == DEGRADED_UNRESOLVABLE_QUERY
    assert set(dynamic.spec.classes.values()) == {FAIL_CLOSED_CLASS}


def test_an_unreadable_row_reports_its_cause_without_quoting_the_statement(
    dynamic_db: Database,
) -> None:
    """A parser error quotes the SQL it choked on, and that reaches every surface.

    ``degraded_reason`` rides the envelope to MCP and JSON callers and prints on
    the CLI, so interpolating the exception republished the offending fragment —
    inline literals included — beside a result whose rows are masked. It is the
    same disclosure the export receipt's withheld ``sql`` exists to prevent, one
    surface over.

    The exception *type* stays: it is what distinguishes a parse failure from a
    decode failure for whoever has to fix it, and it names no content.
    """
    row = _row(
        query_sql=(
            "SELECT routing_number FROM core.dim_accounts "
            "WHERE account_id = '021000021' AND"
        )
    )

    dynamic = spec_from_row(dynamic_db, row)

    assert dynamic.degraded
    assert dynamic.degraded_code == DEGRADED_UNREADABLE_ROW
    reason = dynamic.degraded_reason or ""
    assert reason.startswith(DEGRADED_UNREADABLE_ROW)
    assert "021000021" not in reason
    assert "SELECT" not in reason
    assert "SqlParseError" in reason


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
