"""Tests for run_report — execute, classify, redact, envelope fields."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.contract import ReportQuery, ReportSpec
from moneybin.reports._framework.execute import ReportResult, run_report
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
        [top],
    )


def _spec() -> ReportSpec:
    classes = {
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
    }
    return build_spec(
        _summary,
        report_id="test:summary",
        name="summary",
        view=_VIEW,
        classes=classes,
        parameter_classes={"top": DataClass.AGGREGATE},
        columns=output_columns(classes),
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
