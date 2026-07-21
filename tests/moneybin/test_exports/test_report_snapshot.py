"""Catalog report snapshot contract tests."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest
from pytest_mock import MockerFixture

import moneybin.exports.service as export_service
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.exports.service import ExportService
from moneybin.exports.snapshot import PreparedExport
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import ReportCatalog
from moneybin.reports._framework.contract import ReportQuery, ReportSpec
from moneybin.reports._framework.introspect import build_spec
from moneybin.tables import TableRef
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

_VIEW = TableRef("reports", "test_export")


def _report(
    db: Database,
    *,
    top: int = 2,
    account_number: str = "acct_11112222",
) -> ReportQuery:
    """Account totals for export.

    Args:
        db: Open database connection.
        top: Maximum rows to return.
        account_number: Institution account number to include.
    """
    return ReportQuery(
        "SELECT account_number, amount FROM reports.test_export "
        "WHERE account_number = ? ORDER BY account_number LIMIT ?",
        [account_number, top],
        actions=("reports.inspect",),
        period="all time",
    )


def _spec() -> ReportSpec:
    classes = {
        "account_number": DataClass.ACCOUNT_IDENTIFIER,
        "amount": DataClass.TXN_AMOUNT,
    }
    return build_spec(
        _report,
        report_id="test:export",
        name="test_export",
        view=_VIEW,
        classes=classes,
        parameter_classes={
            "top": DataClass.AGGREGATE,
            "account_number": DataClass.ACCOUNT_IDENTIFIER,
        },
        columns=output_columns(classes),
        semantics=TEST_SEMANTICS,
    )


def _service(db: Database) -> ExportService:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(
        """
        CREATE TABLE reports.test_export (
            account_number VARCHAR,
            amount DECIMAL(18, 2)
        )
        """
    )
    db.execute(
        """
        INSERT INTO reports.test_export VALUES
            ('acct_11112222', -30.00),
            ('acct_99998888', 100.00)
        """
    )
    return ExportService(db, report_catalog=ReportCatalog((_spec(),)))


def _first_row(snapshot: PreparedExport) -> dict[str, object]:
    table = snapshot.tables[0]
    return dict(
        zip(
            (column.name for column in table.columns),
            table.rows[0],
            strict=True,
        )
    )


def test_prepare_report_executes_once_and_preserves_the_report_receipt(
    db: Database,
    mocker: MockerFixture,
) -> None:
    service = _service(db)
    execute_spy = mocker.spy(export_service, "execute_catalog_report")

    snapshot = service.prepare_report(
        profile="test",
        report_id="test:export",
        report_parameters={},
        max_rows=10,
        redaction_mode="unredacted",
    )

    assert execute_spy.call_count == 1
    assert snapshot.subject.as_manifest() == {
        "kind": "report",
        "report_id": "test:export",
        "parameters": {"top": 2, "account_number": "acct_11112222"},
    }
    assert len(snapshot.tables) == 1
    table = snapshot.tables[0]
    assert table.name == "test:export"
    assert table.source == _VIEW
    assert [
        (column.name, column.duckdb_type, column.data_class) for column in table.columns
    ] == [
        ("account_number", "VARCHAR", DataClass.ACCOUNT_IDENTIFIER),
        ("amount", "DECIMAL(18,2)", DataClass.TXN_AMOUNT),
    ]
    assert _first_row(snapshot) == {
        "account_number": "acct_11112222",
        "amount": Decimal("-30.00"),
    }

    assert snapshot.provenance is not None
    assert snapshot.provenance.report_id == "test:export"
    assert snapshot.provenance.receipt == {
        "report_id": "test:export",
        "parameters": {"top": 2, "account_number": "acct_11112222"},
        "parameter_classes": {
            "top": "aggregate",
            "account_number": "account_identifier",
        },
        "sql": (
            "SELECT account_number, amount FROM reports.test_export "
            "WHERE account_number = ? ORDER BY account_number LIMIT ?"
        ),
        "lineage": ("reports.test_summary",),
        "output_classes": {
            "account_number": "account_identifier",
            "amount": "txn_amount",
        },
        "freshness": None,
        "graduation_eligibility": None,
        "semantics": {
            "unit": "count",
            "currency": None,
            "sign": "non-negative",
            "kind": "count",
            "valuation_basis": None,
            "fx_basis": None,
            "time_basis": "point-in-time query result",
            "denominator": None,
            "comparison_window": None,
            "exclusions": (),
            "provenance": ("reports.test_summary",),
        },
    }
    manifest_provenance = snapshot.manifest["provenance"]
    assert manifest_provenance is not None
    assert manifest_provenance["report_id"] == "test:export"  # type: ignore[index]
    manifest_receipt = manifest_provenance["receipt"]  # type: ignore[index]
    assert manifest_receipt["lineage"] == ["reports.test_summary"]  # type: ignore[index]
    assert manifest_receipt["semantics"]["provenance"] == [  # type: ignore[index]
        "reports.test_summary"
    ]
    json.dumps(snapshot.manifest)


def test_prepare_report_applies_redaction_after_raw_execution(db: Database) -> None:
    snapshot = _service(db).prepare_report(
        profile="test",
        report_id="test:export",
        report_parameters={},
        max_rows=10,
    )

    assert snapshot.redaction_mode == "redacted"
    assert _first_row(snapshot)["account_number"] == "****2222"
    assert snapshot.subject.as_manifest()["parameters"] == {
        "top": 2,
        "account_number": "****2222",
    }
    assert snapshot.manifest["provenance"]["receipt"]["parameters"] == {  # type: ignore[index]
        "top": 2,
        "account_number": "****2222",
    }


@pytest.mark.parametrize(
    ("report_id", "parameters", "code"),
    [
        ("missing:report", {}, "REPORT_ID_NOT_FOUND"),
        ("test:export,test:other", {}, "REPORT_ID_NOT_FOUND"),
        ("SELECT * FROM reports.test_export", {}, "REPORT_ID_NOT_FOUND"),
        ("test:export", {"unknown": 1}, "REPORT_PARAMETER_UNKNOWN"),
        ("test:export", {"top": "two"}, "REPORT_PARAMETER_INVALID_TYPE"),
    ],
)
def test_prepare_report_uses_catalog_errors_for_invalid_subjects_and_parameters(
    db: Database,
    report_id: str,
    parameters: dict[str, object],
    code: str,
) -> None:
    service = _service(db)

    with pytest.raises(UserError) as exc_info:
        service.prepare_report(
            profile="test",
            report_id=report_id,
            report_parameters=parameters,  # type: ignore[arg-type]  # invalid runtime input under test
            max_rows=10,
        )

    assert exc_info.value.code == code


def test_prepare_report_uses_catalog_error_for_invalid_limit(db: Database) -> None:
    service = _service(db)

    with pytest.raises(UserError) as exc_info:
        service.prepare_report(
            profile="test",
            report_id="test:export",
            report_parameters={},
            max_rows=-1,
        )

    assert exc_info.value.code == "REPORT_LIMIT_INVALID"
