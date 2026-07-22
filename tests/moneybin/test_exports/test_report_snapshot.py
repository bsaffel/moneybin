"""Catalog report snapshot contract tests."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from decimal import Decimal

import pytest
from pydantic import JsonValue
from pytest_mock import MockerFixture

import moneybin.reports._framework.catalog as report_catalog
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.exports.service import ExportService
from moneybin.exports.snapshot import PreparedExport
from moneybin.privacy.payloads.networth import (
    NetWorthHistoryPayload,
    NetWorthHistoryPoint,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import ReportCatalog, ServiceReportSpec
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportQuery,
    ReportSpec,
)
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    build_catalog_execution,
)
from moneybin.reports._framework.introspect import build_spec
from moneybin.reports.service_reports import NETWORTH_HISTORY_REPORT
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
    execute_spy = mocker.spy(report_catalog, "execute_catalog_report")

    snapshot = service.prepare_report(
        profile="test",
        report_id="test:export",
        report_parameters={},
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


def test_prepare_report_exports_every_row_without_the_mcp_response_cap(
    db: Database,
) -> None:
    """Artifact completeness is independent from interactive response limits."""
    rows = [{"value": value} for value in range(5)]

    def executor(
        database: Database,  # noqa: ARG001  # service contract handle
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> CatalogReportExecution:
        assert limit is None
        return build_catalog_execution(
            spec,
            parameters=parameters,
            records=rows,
            columns=["value"],
            column_types=["BIGINT"],
            max_rows=limit,
            sql=None,
        )

    spec = ServiceReportSpec(
        report_id="test:complete_export",
        name="complete_export",
        description="Synthetic complete report export.",
        parameters=(),
        columns=(OutputColumn("value", "Value.", DataClass.AGGREGATE),),
        semantics=TEST_SEMANTICS,
        classes={"value": DataClass.AGGREGATE},
        examples=(),
        executor=executor,
    )

    snapshot = ExportService(
        db,
        report_catalog=ReportCatalog((spec,)),
    ).prepare_report(
        profile="test",
        report_id="test:complete_export",
        report_parameters={},
        redaction_mode="redacted",
    )

    assert snapshot.tables[0].rows == tuple((value,) for value in range(5))
    assert snapshot.manifest["tables"][0]["row_count"] == 5  # type: ignore[index]


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
        )

    assert exc_info.value.code == code


def test_prepare_service_report_uses_one_raw_execution_for_each_output_policy(
    db: Database,
) -> None:
    calls = 0

    def executor(
        database: Database,  # noqa: ARG001  # service contract handle
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> CatalogReportExecution:
        nonlocal calls
        calls += 1
        return build_catalog_execution(
            spec,
            parameters=parameters,
            records=[{"account_number": parameters["account_number"]}],
            columns=["account_number"],
            column_types=["VARCHAR"],
            max_rows=limit,
            actions=["reports.inspect"],
            period="all time",
            sql=None,
        )

    spec = ServiceReportSpec(
        report_id="test:service_export",
        name="service_export",
        description="Synthetic service-backed export.",
        parameters=(
            ParamSpec(
                "account_number",
                str,
                "acct_11112222",
                False,
                "Institution account number.",
                DataClass.ACCOUNT_IDENTIFIER,
            ),
        ),
        columns=(
            OutputColumn(
                "account_number",
                "Institution account number.",
                DataClass.ACCOUNT_IDENTIFIER,
            ),
        ),
        semantics=TEST_SEMANTICS,
        classes={"account_number": DataClass.ACCOUNT_IDENTIFIER},
        examples=(),
        executor=executor,
    )
    service = ExportService(db, report_catalog=ReportCatalog((spec,)))

    redacted = service.prepare_report(
        profile="test",
        report_id="test:service_export",
        report_parameters={},
    )
    assert calls == 1
    assert _first_row(redacted)["account_number"] == "****2222"
    assert redacted.manifest["provenance"]["receipt"]["sql"] is None  # type: ignore[index]

    unredacted = service.prepare_report(
        profile="test",
        report_id="test:service_export",
        report_parameters={},
        redaction_mode="unredacted",
    )
    assert calls == 2
    assert _first_row(unredacted)["account_number"] == "acct_11112222"

    with pytest.raises(UserError) as exc_info:
        service.prepare_report(
            profile="test",
            report_id="test:service_export",
            report_parameters={"unknown": 1},
        )
    assert exc_info.value.code == "REPORT_PARAMETER_UNKNOWN"
    assert calls == 2


def test_networth_history_export_retains_native_values_with_truthful_types(
    db: Database,
    mocker: MockerFixture,
) -> None:
    history = mocker.patch(
        "moneybin.reports.service_reports.NetworthService.history",
        return_value=NetWorthHistoryPayload(
            points=[
                NetWorthHistoryPoint(
                    period="2026-07-01",
                    net_worth=Decimal("1000.12345678"),
                    change_abs=Decimal("100.75308643"),
                    change_pct=Decimal("0.100740651234567890"),
                )
            ]
        ),
    )

    snapshot = ExportService(
        db,
        report_catalog=ReportCatalog((NETWORTH_HISTORY_REPORT,)),
    ).prepare_report(
        profile="test",
        report_id="core:networth_history",
        report_parameters={
            "from_date": "2026-07-01",
            "to_date": "2026-07-31",
        },
        redaction_mode="unredacted",
    )

    history.assert_called_once_with(
        date(2026, 7, 1),
        date(2026, 7, 31),
        interval="monthly",
    )
    table = snapshot.tables[0]
    assert [(column.name, column.duckdb_type) for column in table.columns] == [
        ("period", "VARCHAR"),
        ("net_worth", "DECIMAL(12,8)"),
        ("change_abs", "DECIMAL(11,8)"),
        ("change_pct", "DECIMAL(18,18)"),
    ]
    assert table.rows == (
        (
            "2026-07-01",
            Decimal("1000.12345678"),
            Decimal("100.75308643"),
            Decimal("0.100740651234567890"),
        ),
    )
    manifest_columns = snapshot.manifest["tables"][0]["columns"]  # type: ignore[index]
    assert [column["duckdb_type"] for column in manifest_columns] == [  # type: ignore[index]
        "VARCHAR",
        "DECIMAL(12,8)",
        "DECIMAL(11,8)",
        "DECIMAL(18,18)",
    ]
