"""Tests for the runner-first report contract and signature introspection.

A report is a decorated runner ``(db, **params) -> ReportQuery``. ``build_spec``
turns that runner into a ``ReportSpec`` by reading its signature (params,
types, defaults) and Google-style docstring (summary, Args, Examples). The
``@report`` decorator attaches the spec to the function for later discovery.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.reports._framework.contract import (
    ReportQuery,
    ReportSpec,
    report,
)
from moneybin.reports._framework.introspect import build_spec
from moneybin.tables import REPORTS_MERCHANT_ACTIVITY, TableRef


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


def test_report_query_is_frozen_with_sql_and_params() -> None:
    rq = ReportQuery("SELECT ?", [3])
    assert rq.sql == "SELECT ?"
    assert list(rq.params) == [3]
    with pytest.raises(AttributeError):
        rq.sql = "x"  # type: ignore[misc]  # frozen


def test_build_spec_reads_name_view_and_description() -> None:
    spec = build_spec(_sample, name="sample", view=REPORTS_MERCHANT_ACTIVITY)
    assert spec.name == "sample"
    assert spec.view == REPORTS_MERCHANT_ACTIVITY
    assert spec.description == "Sample rollup, grouped per ``by``."


def test_build_spec_derives_surface_names() -> None:
    spec = build_spec(
        _sample, name="large_transactions", view=REPORTS_MERCHANT_ACTIVITY
    )
    assert spec.mcp_tool_name == "reports_large_transactions"
    assert spec.cli_name == "large-transactions"


def test_build_spec_excludes_db_and_reads_params() -> None:
    spec = build_spec(_sample, name="sample", view=REPORTS_MERCHANT_ACTIVITY)
    by_name = {p.name: p for p in spec.params}
    assert set(by_name) == {"month", "top", "by"}  # db excluded

    assert by_name["month"].default is None
    assert by_name["month"].required is False
    assert by_name["month"].help == "Inclusive month filter (YYYY-MM)."

    assert by_name["top"].default == 25
    assert by_name["top"].annotation is int
    assert by_name["by"].default == "account"


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

    spec = build_spec(runner, name="cont", view=REPORTS_MERCHANT_ACTIVITY)
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

    spec = build_spec(runner, name="r", view=REPORTS_MERCHANT_ACTIVITY)
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

    spec = build_spec(runner, name="r", view=REPORTS_MERCHANT_ACTIVITY)
    assert "grouping is by account then category." in spec.description
    by_name = {p.name: p for p in spec.params}
    assert by_name["top"].help == "Maximum rows to return."  # Args still parsed


def test_build_spec_reads_examples() -> None:
    spec = build_spec(_sample, name="sample", view=REPORTS_MERCHANT_ACTIVITY)
    assert spec.examples == (
        "reports_sample(top=5)",
        'reports_sample(by="category", month="2024-01")',
    )


def test_report_decorator_attaches_spec_and_returns_function() -> None:
    @report(name="sample", view=REPORTS_MERCHANT_ACTIVITY)
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
        build_spec(no_doc, name="nodoc", view=REPORTS_MERCHANT_ACTIVITY)


def test_build_spec_requires_db_first_param() -> None:
    def wrong_first(conn: Database, *, x: int = 1) -> ReportQuery:
        """Summary."""
        return ReportQuery("SELECT 1", [])

    with pytest.raises(ValueError, match="db"):
        build_spec(wrong_first, name="wrong", view=REPORTS_MERCHANT_ACTIVITY)


def test_build_spec_requires_keyword_only_params() -> None:
    def positional(db: Database, top: int = 1) -> ReportQuery:
        """Summary."""
        return ReportQuery("SELECT 1", [])

    with pytest.raises(ValueError, match="keyword-only"):
        build_spec(positional, name="pos", view=REPORTS_MERCHANT_ACTIVITY)


def test_build_spec_rejects_non_reports_view() -> None:
    with pytest.raises(ValueError, match="reports"):
        build_spec(_sample, name="bad", view=TableRef("core", "fct_transactions"))


def test_build_spec_accepts_custom_table_ref() -> None:
    view = TableRef("reports", "sample")
    spec = build_spec(_sample, name="sample", view=view, domain="merchants")
    assert spec.view.full_name == "reports.sample"
    assert spec.domain == "merchants"
