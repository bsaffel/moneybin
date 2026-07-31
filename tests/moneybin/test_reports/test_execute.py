"""Tests for run_report — execute, classify, redact, envelope fields."""

from __future__ import annotations

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.contract import Binding, ReportQuery, ReportSpec
from moneybin.reports._framework.execute import (
    ReportResult,
    execute_catalog_report,
    run_report,
)
from moneybin.reports._framework.introspect import build_spec
from moneybin.tables import TableRef
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

_VIEW = TableRef("reports", "test_summary")


def _summary(db: Database, *, top: int = 50) -> ReportQuery:
    """Per-account amount + count summary.

    Args:
        db: Open read-only database connection.
        top: Maximum rows to return.
    """
    return ReportQuery(
        "SELECT account_id, amount, txn_count FROM reports.test_summary "
        "ORDER BY account_id LIMIT ?",
        [Binding(top, DataClass.AGGREGATE)],
        actions=("reports.next",),
        period="all time",
    )


def _spec(
    classes: dict[str, DataClass] | None = None, *, name: str = "summary"
) -> ReportSpec:
    declared = classes or {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
    }
    return build_spec(
        _summary,
        report_id=f"test:{name}",
        name=name,
        view=_VIEW,
        classes=declared,
        parameter_classes={"top": DataClass.AGGREGATE},
        columns=output_columns(declared),
        semantics=TEST_SEMANTICS,
    )


def test_run_report_masks_critical_and_sets_tier(reports_db: Database) -> None:
    result = run_report(_spec(), reports_db, max_rows=50)
    assert isinstance(result, ReportResult)
    # CRITICAL account_id masked to ****<last4>; tier is the max over columns.
    by_acct = {r["account_id"]: r for r in result.records}
    assert set(by_acct) == {"****2222", "****8888"}
    assert result.tier is Tier.CRITICAL
    # Non-critical columns pass through in the clear.
    assert by_acct["****2222"]["txn_count"] == 2
    assert by_acct["****8888"]["txn_count"] == 1
    assert set(result.classes_returned) == {
        "account_identifier",
        "txn_amount",
        "aggregate",
    }


def test_run_report_truncates(reports_db: Database) -> None:
    result = run_report(_spec(), reports_db, max_rows=1)
    assert result.truncated is True
    assert len(result.records) == 1
    assert result.total_count == 2  # max_rows + 1 signals "at least one more"


def test_run_report_passes_params_to_runner(reports_db: Database) -> None:
    result = run_report(_spec(), reports_db, max_rows=50, top=1)
    assert len(result.records) == 1  # runner bound LIMIT 1
    assert result.truncated is False


def test_execute_catalog_report_exposes_raw_execution_before_public_redaction(
    reports_db: Database,
) -> None:
    spec = _spec()

    raw = execute_catalog_report(spec, reports_db, max_rows=10, top=1)
    public = run_report(spec, reports_db, max_rows=10, top=1)

    assert raw.report_id == "test:summary"
    assert raw.parameters == {"top": 1}
    assert raw.sql is not None
    assert raw.sql.startswith("SELECT account_id, amount, txn_count")
    assert raw.columns == ["account_id", "amount", "txn_count"]
    assert raw.column_types == ["VARCHAR", "DECIMAL(38,2)", "BIGINT"]
    assert raw.output_classes == public.output_classes
    assert raw.records[0]["account_id"] == "acct_11112222"
    assert public.records[0]["account_id"] == "****2222"
    assert raw.actions == ["reports.next"]
    assert raw.period == "all time"
    assert raw.semantics is spec.semantics
    assert raw.provenance == ("reports.test_summary",)


def test_a_stale_saved_query_fails_without_echoing_its_sql(
    reports_db: Database,
) -> None:
    """An upstream change must not turn a saved report into a disclosure.

    A saved report's SQL is user-authored and can carry an inline literal. When an
    upstream rename invalidates it, DuckDB's binder error echoes the statement it
    failed to bind, and ``handle_cli_errors`` re-raises an *unclassified*
    exception — so ``reports run`` and ``export report`` would print a traceback
    carrying that literal. The MCP wrapper masks unclassified failures; the CLI
    does not, so the boundary itself has to classify the failure and drop the SQL.
    """

    def stale(db: Database) -> ReportQuery:  # noqa: ARG001  # report contract handle
        """Saved query naming a column no longer present upstream.

        Args:
            db: Open read-only database connection.
        """
        return ReportQuery(
            "SELECT account_id FROM reports.test_summary "
            "WHERE routing_number = '021000021'",
            [],
            actions=("reports.next",),
            period="all time",
        )

    classes = {"account_id": DataClass.ACCOUNT_IDENTIFIER}
    spec = build_spec(
        stale,
        report_id="test:stale",
        name="stale",
        view=_VIEW,
        classes=classes,
        parameter_classes={},
        columns=output_columns(classes),
        semantics=TEST_SEMANTICS,
    )

    with pytest.raises(UserError) as exc_info:
        execute_catalog_report(spec, reports_db, max_rows=10)

    assert exc_info.value.code == error_codes.REPORT_QUERY_EXECUTION_FAILED
    message = str(exc_info.value)
    assert "021000021" not in message
    assert "routing_number" not in message


def test_masked_output_carries_the_inspection_hint(reports_db: Database) -> None:
    """R3: a ``'*****'`` with no explanation is a two-call fix.

    The hint names the CLI command because the verify surface has no MCP
    identity — R3 requires it bind only to an admitted surface.
    """
    result = run_report(_spec(), reports_db, max_rows=50)

    hints = [action for action in result.actions if "reports explain" in action]
    assert len(hints) == 1
    assert "test:summary" in hints[0]
    assert "account_id" in hints[0]
    # Names what actually masked, not what is merely sensitive: `amount` is
    # TXN_AMOUNT (HIGH) and today's transform table passes HIGH through, so it
    # comes back in the clear and there is nothing to explain about it. When
    # HIGH transforms are wired, `mask_strength` starts reporting it here with no
    # edit to this code — the hint is measured from the transform, not the tier.
    assert "amount" not in hints[0]
    # The runner's own hint keeps its position; the framework's is appended.
    assert result.actions[0] == "reports.next"


def test_unmasked_output_carries_no_inspection_hint(reports_db: Database) -> None:
    """The over-explaining twin: no column masks, so there is nothing to explain.

    Written deliberately because no test in this repo fails on *over*-hinting,
    the same asymmetry that let three over-masking regressions ship in #340.
    """
    unmasked = {
        "account_id": DataClass.RECORD_ID,
        "amount": DataClass.AGGREGATE,
        "txn_count": DataClass.AGGREGATE,
    }
    result = run_report(_spec(unmasked, name="open"), reports_db, max_rows=50)

    assert result.records[0]["account_id"] == "acct_11112222"
    assert result.actions == ["reports.next"]
