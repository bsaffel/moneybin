"""Tests for the runner-first report contract and signature introspection.

A report is a decorated runner ``(db, **params) -> ReportQuery``. ``build_spec``
turns that runner into a ``ReportSpec`` by reading its signature (params,
types, defaults) and Google-style docstring (summary, Args, Examples). The
``@report`` decorator attaches the spec to the function for later discovery.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import replace

import pytest

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    ReportSpec,
    report,
)
from moneybin.reports._framework.introspect import build_spec
from moneybin.tables import REPORTS_MERCHANT_ACTIVITY, TableRef

# Placeholder column map for introspection tests that don't assert on classes.
_CLASSES = {"value": DataClass.AGGREGATE}
_PARAMETER_CLASSES = {
    "month": DataClass.TXN_DATE,
    "top": DataClass.AGGREGATE,
    "by": DataClass.TXN_TYPE,
}
_COLUMNS = (
    OutputColumn(
        name="value",
        description="Aggregate value.",
        data_class=DataClass.AGGREGATE,
    ),
)
_SEMANTICS = ReportSemantics(
    unit="count",
    currency=None,
    sign="non-negative",
    kind="count",
    valuation_basis=None,
    fx_basis=None,
    time_basis="point-in-time query result",
    denominator=None,
    comparison_window=None,
    exclusions=(),
    provenance=("reports.merchant_activity",),
)


def _sample(
    db: Database,
    *,
    month: str | None = None,
    top: int = 25,
    by: str = "account",
) -> ReportQuery:
    """Sample rollup, grouped per ``by``.

    Args:
        db: Open read-only database connection.
        month: Inclusive month filter (YYYY-MM).
        top: Maximum rows to return.
        by: Grouping dimension.

    Examples:
        reports_sample(top=5)
        reports_sample(by="category", month="2024-01")
    """
    return ReportQuery("SELECT 1", [])


def _build_spec(
    runner: object = _sample,
    *,
    report_id: str = "test:sample",
    name: str = "sample",
    view: TableRef = REPORTS_MERCHANT_ACTIVITY,
    classes: dict[str, DataClass] | None = None,
    parameter_classes: Mapping[str, DataClass] = _PARAMETER_CLASSES,
    columns: tuple[OutputColumn, ...] = _COLUMNS,
    semantics: ReportSemantics = _SEMANTICS,
    domain: str | None = None,
) -> ReportSpec:
    return build_spec(
        runner,  # type: ignore[arg-type]
        report_id=report_id,
        name=name,
        view=view,
        classes=_CLASSES if classes is None else classes,
        parameter_classes=parameter_classes,
        columns=columns,
        semantics=semantics,
        domain=domain,
    )


def test_report_query_is_frozen_with_sql_and_params() -> None:
    rq = ReportQuery("SELECT ?", [3])
    assert rq.sql == "SELECT ?"
    assert list(rq.params) == [3]
    with pytest.raises(AttributeError):
        rq.sql = "x"  # type: ignore[misc]  # frozen


def test_build_spec_reads_name_view_and_description() -> None:
    spec = _build_spec()
    assert spec.name == "sample"
    assert spec.view == REPORTS_MERCHANT_ACTIVITY
    assert spec.description == "Sample rollup, grouped per ``by``."


def test_build_spec_derives_surface_names() -> None:
    spec = _build_spec(
        name="large_transactions",
    )
    assert not hasattr(spec, "mcp_tool_name")
    assert spec.cli_name == "large-transactions"


def test_build_spec_excludes_db_and_reads_params() -> None:
    spec = _build_spec()
    by_name = {p.name: p for p in spec.params}
    assert set(by_name) == {"month", "top", "by"}  # db excluded

    assert by_name["month"].default is None
    assert by_name["month"].required is False
    assert by_name["month"].help == "Inclusive month filter (YYYY-MM)."
    assert by_name["month"].data_class is DataClass.TXN_DATE

    assert by_name["top"].default == 25
    assert by_name["top"].annotation is int
    assert by_name["top"].data_class is DataClass.AGGREGATE
    assert by_name["by"].default == "account"
    assert by_name["by"].data_class is DataClass.TXN_TYPE


def test_build_spec_arg_continuation_line_is_not_parsed_as_new_param() -> None:
    # A continuation line shaped like "word: text" must append to the current
    # param's help, not start a phantom entry that truncates it. The deeper
    # indent (not the colon) is what distinguishes a continuation from an entry.
    def runner(db: Database, *, fmt: str = "iso") -> ReportQuery:
        """Summary.

        Args:
            db: Open read-only database connection.
            fmt: Output date format.
                default: today when omitted.
        """
        return ReportQuery("SELECT 1", [])

    spec = _build_spec(
        runner,
        name="cont",
        parameter_classes={"fmt": DataClass.TXN_TYPE},
    )
    by_name = {p.name: p for p in spec.params}
    assert set(by_name) == {"fmt"}
    assert by_name["fmt"].help == "Output date format. default: today when omitted."


def test_build_spec_description_includes_body_prose_not_args() -> None:
    # The MCP tool description is spec.description; for amount-bearing reports it
    # MUST carry the sign-convention paragraph (mcp.md). Description = all prose
    # before Args:, so the body reaches the agent while the Args block (which
    # names the non-passable `db`) does not.
    def runner(db: Database, *, top: int = 10) -> ReportQuery:
        """Spending rollup by category.

        Amounts use the accounting convention (negative = expense, positive =
        income) in the currency named by summary.display_currency.

        Args:
            db: Open read-only database connection.
            top: Maximum rows to return.
        """
        return ReportQuery("SELECT 1", [])

    spec = _build_spec(
        runner,
        name="r",
        parameter_classes={"top": DataClass.AGGREGATE},
    )
    assert spec.description.startswith("Spending rollup by category.")
    assert "accounting convention" in spec.description
    assert "summary.display_currency" in spec.description
    assert "Args:" not in spec.description
    assert "Open read-only database connection" not in spec.description


def test_build_spec_bare_word_colon_in_description_is_not_a_section_header() -> None:
    # A bare "<word>:" line that is not a known Google section (e.g. "Options:")
    # must not be mistaken for a section header — that would truncate the
    # description there and drop the prose after it. Only known sections
    # (Args/Examples/...) terminate the description.
    def runner(db: Database, *, top: int = 10) -> ReportQuery:
        """Spending rollup.

        Options:
        grouping is by account then category.

        Args:
            db: Open read-only database connection.
            top: Maximum rows to return.
        """
        return ReportQuery("SELECT 1", [])

    spec = _build_spec(
        runner,
        name="r",
        parameter_classes={"top": DataClass.AGGREGATE},
    )
    assert "grouping is by account then category." in spec.description
    by_name = {p.name: p for p in spec.params}
    assert by_name["top"].help == "Maximum rows to return."  # Args still parsed


def test_build_spec_reads_examples() -> None:
    spec = _build_spec()
    assert spec.examples == (
        "reports_sample(top=5)",
        'reports_sample(by="category", month="2024-01")',
    )


def test_report_decorator_attaches_spec_and_returns_function() -> None:
    @report(
        report_id="test:sample",
        name="sample",
        view=REPORTS_MERCHANT_ACTIVITY,
        classes=_CLASSES,
        parameter_classes={"top": DataClass.AGGREGATE},
        columns=_COLUMNS,
        semantics=_SEMANTICS,
    )
    def runner(db: Database, *, top: int = 10) -> ReportQuery:
        """One-line summary."""
        return ReportQuery("SELECT 1", [])

    assert runner._report_spec.name == "sample"  # type: ignore[attr-defined]
    assert isinstance(runner._report_spec, ReportSpec)  # type: ignore[attr-defined]
    # decorator returns the original callable unchanged
    assert runner(Database.__new__(Database), top=1).sql == "SELECT 1"


def test_build_spec_requires_docstring() -> None:
    def no_doc(db: Database, *, x: int = 1) -> ReportQuery:
        return ReportQuery("SELECT 1", [])

    with pytest.raises(ValueError, match="docstring"):
        _build_spec(no_doc, name="nodoc")


def test_build_spec_requires_db_first_param() -> None:
    def wrong_first(conn: Database, *, x: int = 1) -> ReportQuery:
        """Summary."""
        return ReportQuery("SELECT 1", [])

    with pytest.raises(ValueError, match="db"):
        _build_spec(wrong_first, name="wrong")


def test_build_spec_requires_keyword_only_params() -> None:
    def positional(db: Database, top: int = 1) -> ReportQuery:
        """Summary."""
        return ReportQuery("SELECT 1", [])

    with pytest.raises(ValueError, match="keyword-only"):
        _build_spec(positional, name="pos")


def test_build_spec_rejects_non_reports_view() -> None:
    with pytest.raises(ValueError, match="reports"):
        _build_spec(
            _sample,
            name="bad",
            view=TableRef("core", "fct_transactions"),
        )


def test_build_spec_accepts_custom_table_ref() -> None:
    view = TableRef("reports", "sample")
    spec = _build_spec(view=view, domain="merchants")
    assert spec.view.full_name == "reports.sample"
    assert spec.domain == "merchants"


def test_build_spec_rejects_empty_classes() -> None:
    # Every report must declare its column privacy contract (ADR-013).
    with pytest.raises(ValueError, match="classes"):
        _build_spec(classes={})


def test_build_spec_requires_exact_parameter_class_coverage() -> None:
    with pytest.raises(ValueError, match="parameter_classes"):
        _build_spec(parameter_classes={"month": DataClass.TXN_DATE})

    with pytest.raises(ValueError, match="parameter_classes"):
        _build_spec(
            parameter_classes={
                **_PARAMETER_CLASSES,
                "unknown": DataClass.AGGREGATE,
            }
        )


def test_build_spec_rejects_output_reserved_param() -> None:
    # `output` collides with the shared --output CLI option the registrar
    # injects; without this guard the collision surfaces as a cryptic duplicate-
    # parameter error that crashes the whole reports command group at build.
    def runner(db: Database, *, output: str = "x") -> ReportQuery:
        """Summary."""
        return ReportQuery("SELECT 1", [])

    with pytest.raises(ValueError, match="output"):
        _build_spec(
            runner,
            name="r",
            parameter_classes={"output": DataClass.TXN_TYPE},
        )


def test_build_spec_rejects_quiet_reserved_param() -> None:
    def runner(db: Database, *, quiet: bool = False) -> ReportQuery:
        """Summary."""
        return ReportQuery("SELECT 1", [])

    with pytest.raises(ValueError, match="quiet"):
        _build_spec(
            runner,
            name="r",
            parameter_classes={"quiet": DataClass.TXN_TYPE},
        )


def test_report_spec_requires_namespaced_id_and_metric_semantics() -> None:
    spec = _build_spec(report_id="core:spending")

    assert spec.report_id == "core:spending"
    assert spec.semantics.kind == "count"


@pytest.mark.parametrize(
    "field",
    ["report_id", "columns", "semantics", "parameter_classes"],
)
def test_build_spec_requires_financial_metadata(field: str) -> None:
    assert (
        inspect.signature(build_spec).parameters[field].default
        is inspect.Parameter.empty
    )
    assert (
        inspect.signature(report).parameters[field].default is inspect.Parameter.empty
    )


def test_invalid_report_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="namespace:name"):
        replace(_build_spec(), report_id="sample")


def test_columns_and_classes_must_name_the_same_output_fields() -> None:
    with pytest.raises(ValueError, match="columns and classes"):
        replace(_build_spec(), columns=())


def test_columns_and_classes_must_use_the_same_privacy_class() -> None:
    mismatched = (
        OutputColumn(
            name="value",
            description="Aggregate value.",
            data_class=DataClass.TXN_AMOUNT,
        ),
    )
    with pytest.raises(ValueError, match="columns and classes"):
        replace(_build_spec(), columns=mismatched)


def test_duplicate_output_column_names_are_rejected() -> None:
    with pytest.raises(ValueError, match="columns and classes"):
        replace(_build_spec(), columns=(_COLUMNS[0], _COLUMNS[0]))


def test_report_spec_defensively_freezes_classes() -> None:
    classes = dict(_CLASSES)
    spec = _build_spec(classes=classes)

    classes["value"] = DataClass.TXN_AMOUNT

    assert spec.classes["value"] is DataClass.AGGREGATE
    with pytest.raises(TypeError):
        spec.classes["value"] = DataClass.TXN_AMOUNT  # type: ignore[index]  # immutable
