"""Unified catalog behavior for SQL-backed and service-backed reports."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import cast
from unittest.mock import MagicMock

import pytest
import typer
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.payloads.networth import (
    NetWorthAccountRow,
    NetWorthHistoryPayload,
    NetWorthHistoryPoint,
    NetWorthSnapshotPayload,
)
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework import registry
from moneybin.reports._framework.catalog import (
    ReportCatalog,
    ServiceReportSpec,
    get_report_catalog,
)
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportQuery,
    ReportSemantics,
    ReportSpec,
)
from moneybin.reports._framework.execute import CatalogReportResult
from moneybin.reports._framework.registry import (
    extension_report_specs,
    register_extension_report,
    register_reports_cli,
)
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.reports.service_reports import (
    NETWORTH_HISTORY_REPORT,
    NETWORTH_REPORT,
)
from moneybin.tables import TableRef

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
    provenance=("reports.test_summary",),
)
_COLUMNS = (OutputColumn("value", "Aggregate value.", DataClass.AGGREGATE),)
_CLASSES = {"value": DataClass.AGGREGATE}


def _sql_runner(
    db: Database,  # noqa: ARG001  # contract handle
    *,
    count: int,
    label: str | None = None,
) -> ReportQuery:
    """Test SQL report."""
    return ReportQuery(
        "SELECT ? AS value",
        [count],
        actions=[f"Label present: {label is not None}"],
        period="test period",
    )


def _sql_report(
    *,
    report_id: str = "core:summary",
    name: str = "summary",
) -> ReportSpec:
    return ReportSpec(
        report_id=report_id,
        name=name,
        description="Test SQL report.",
        view=TableRef("reports", "test_summary"),
        runner=_sql_runner,
        classes=_CLASSES,
        columns=_COLUMNS,
        semantics=_SEMANTICS,
        params=(
            ParamSpec("count", int, None, True, "Required count."),
            ParamSpec("label", str | None, None, False, "Optional label."),
        ),
        examples=(),
    )


def _service_report(
    executor: MagicMock,
    *,
    report_id: str = "retirement:summary",
    name: str = "summary",
) -> ServiceReportSpec:
    return ServiceReportSpec(
        report_id=report_id,
        name=name,
        description="Test service report.",
        parameters=(ParamSpec("year", int, None, True, "Tax year."),),
        columns=_COLUMNS,
        semantics=_SEMANTICS,
        classes=_CLASSES,
        examples=(),
        executor=executor,
    )


def _db_with_rows(*rows: tuple[object, ...]) -> Database:
    cursor = MagicMock()
    cursor.description = [("value",)]
    cursor.fetchmany.return_value = list(rows)
    db = MagicMock(spec=Database)
    db.execute.return_value = cursor
    return cast(Database, db)


def test_catalog_lists_reports_in_full_id_order() -> None:
    catalog = ReportCatalog((NETWORTH_REPORT, _sql_report()))

    assert tuple(report.report_id for report in catalog.list()) == (
        "core:networth",
        "core:summary",
    )


def test_catalog_resolves_namespaced_and_unique_short_ids() -> None:
    sql_report = _sql_report()
    catalog = ReportCatalog((sql_report, NETWORTH_REPORT))

    assert catalog.resolve("core:summary").report_id == "core:summary"
    assert catalog.resolve("summary").report_id == "core:summary"


def test_exact_namespaced_id_wins_over_matching_short_name() -> None:
    exact = _sql_report(report_id="core:summary", name="core_summary")
    alias_collision = _sql_report(
        report_id="retirement:projection",
        name="core:summary",
    )
    catalog = ReportCatalog((alias_collision, exact))

    assert catalog.resolve("core:summary") is exact


def test_ambiguous_short_id_lists_sorted_namespaced_candidates() -> None:
    executor = MagicMock()
    catalog = ReportCatalog((_sql_report(), _service_report(executor)))

    with pytest.raises(UserError) as raised:
        catalog.resolve("summary")

    assert raised.value.code == "REPORT_ID_AMBIGUOUS"
    assert raised.value.details == {
        "report_id": "summary",
        "candidates": ["core:summary", "retirement:summary"],
    }


def test_missing_report_id_is_structured_and_sanitized() -> None:
    catalog = ReportCatalog((_sql_report(),))

    with pytest.raises(UserError) as raised:
        catalog.resolve("missing")

    assert raised.value.code == "REPORT_ID_NOT_FOUND"
    assert raised.value.details == {"report_id": "missing"}
    assert "missing" not in raised.value.message


def test_duplicate_full_report_ids_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate report_id: core:summary"):
        ReportCatalog((_sql_report(), _sql_report()))


@pytest.mark.parametrize(
    ("parameters", "code", "details"),
    [
        (
            {"year": 2026, "account_number": "sensitive"},
            "REPORT_PARAMETER_UNKNOWN",
            {
                "report_id": "retirement:summary",
                "parameters": ["account_number"],
            },
        ),
        (
            {},
            "REPORT_PARAMETER_MISSING",
            {"report_id": "retirement:summary", "parameters": ["year"]},
        ),
        (
            {"year": "2026"},
            "REPORT_PARAMETER_INVALID_TYPE",
            {
                "report_id": "retirement:summary",
                "parameter": "year",
                "expected": "int",
            },
        ),
    ],
)
def test_service_parameters_are_rejected_before_executor_dispatch(
    parameters: dict[str, object],
    code: str,
    details: dict[str, object],
) -> None:
    executor = MagicMock()
    catalog = ReportCatalog((_service_report(executor),))
    db = MagicMock(spec=Database)

    with pytest.raises(UserError) as raised:
        catalog.execute(
            cast(Database, db),
            report_id="retirement:summary",
            parameters=parameters,  # type: ignore[arg-type]  # invalid JSON types under test
            limit=100,
        )

    assert raised.value.code == code
    assert raised.value.details == details
    assert "sensitive" not in raised.value.message
    executor.assert_not_called()
    db.execute.assert_not_called()


def test_sql_parameters_are_rejected_before_query_dispatch() -> None:
    catalog = ReportCatalog((_sql_report(),))
    db = MagicMock(spec=Database)

    with pytest.raises(UserError, match="invalid type") as raised:
        catalog.execute(
            cast(Database, db),
            report_id="core:summary",
            parameters={"count": True},
            limit=100,
        )

    assert raised.value.code == "REPORT_PARAMETER_INVALID_TYPE"
    db.execute.assert_not_called()


def test_sql_report_dispatch_returns_catalog_result_with_defaults() -> None:
    catalog = ReportCatalog((_sql_report(),))
    db = _db_with_rows((7,))

    result = catalog.execute(
        db,
        report_id="core:summary",
        parameters={"count": 7},
        limit=100,
    )

    assert isinstance(result, CatalogReportResult)
    assert result.report_id == "core:summary"
    assert result.parameters == {"count": 7, "label": None}
    assert result.semantics is _SEMANTICS
    assert result.provenance == ("reports.test_summary",)
    assert result.records == [{"value": 7}]
    assert result.columns == ["value"]
    assert result.period == "test period"
    cast(MagicMock, db.execute).assert_called_once_with(
        "SELECT ? AS value",
        [7],
    )


def test_service_report_dispatch_uses_same_result_contract() -> None:
    expected = cast(CatalogReportResult, object())
    executor = MagicMock(return_value=expected)
    service_report = _service_report(executor)
    catalog = ReportCatalog((service_report,))
    db = MagicMock(spec=Database)

    result = catalog.execute(
        cast(Database, db),
        report_id="retirement:summary",
        parameters={"year": 2026},
        limit=25,
    )

    assert result is expected
    executor.assert_called_once_with(
        cast(Database, db),
        {"year": 2026},
        25,
    )
    db.execute.assert_not_called()


def test_networth_service_report_is_tabular_redacted_and_truncated(
    mocker: MockerFixture,
) -> None:
    current = mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=date(2026, 7, 1),
            net_worth=Decimal("1234.56000000"),
            total_assets=Decimal("1500.12000000"),
            total_liabilities=Decimal("-265.56000000"),
            account_count=2,
            per_account=[
                NetWorthAccountRow(
                    account_id="acct_11112222",
                    display_name="Checking",
                    balance=Decimal("500.12000000"),
                    observation_source="asserted",
                ),
                NetWorthAccountRow(
                    account_id="acct_99998888",
                    display_name="Brokerage",
                    balance=Decimal("1000.00000000"),
                    observation_source="derived",
                ),
            ],
        ),
    )
    db = MagicMock(spec=Database)

    result = ReportCatalog((NETWORTH_REPORT,)).execute(
        cast(Database, db),
        report_id="core:networth",
        parameters={"as_of": "2026-07-02"},
        limit=1,
    )

    current.assert_called_once_with(
        as_of_date=date(2026, 7, 2),
        account_ids=None,
    )
    assert result.report_id == "core:networth"
    assert result.semantics.kind == "position"
    assert result.semantics.valuation_basis == (
        "latest resolved daily balance, observed or carried forward, on or "
        "before the resolved balance_date"
    )
    assert result.semantics.fx_basis == (
        "no FX conversion in v1; assumes single-currency inputs"
    )
    assert result.parameters == {"as_of": "2026-07-02", "account_ids": None}
    assert result.records == [
        {
            "balance_date": date(2026, 7, 1),
            "net_worth": Decimal("1234.56000000"),
            "total_assets": Decimal("1500.12000000"),
            "total_liabilities": Decimal("-265.56000000"),
            "account_count": 2,
            "account_id": "****2222",
            "account_name": "Checking",
            "account_balance": Decimal("500.12000000"),
            "observation_source": "asserted",
        }
    ]
    assert result.tier is Tier.CRITICAL
    assert result.truncated is True
    assert result.total_count == 2
    envelope = result.to_envelope().to_dict()
    assert envelope["summary"]["display_currency"] == "USD"
    db.execute.assert_not_called()


def test_networth_service_report_keeps_summary_when_no_accounts(
    mocker: MockerFixture,
) -> None:
    mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=date(2026, 7, 1),
            net_worth=Decimal("0"),
            total_assets=Decimal("0"),
            total_liabilities=Decimal("0"),
            account_count=0,
            per_account=[],
        ),
    )

    result = ReportCatalog((NETWORTH_REPORT,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="networth",
        parameters={},
        limit=100,
    )

    assert len(result.records) == 1
    assert result.records[0]["account_id"] is None
    assert result.records[0]["net_worth"] == Decimal("0")
    assert result.total_count == 1
    assert result.truncated is False
    assert "zero position dated current local date" in result.semantics.time_basis


def test_networth_history_service_report_preserves_numeric_fidelity(
    mocker: MockerFixture,
) -> None:
    history = mocker.patch(
        "moneybin.reports.service_reports.NetworthService.history",
        return_value=NetWorthHistoryPayload(
            points=[
                NetWorthHistoryPoint(
                    period="2026-06-01",
                    net_worth=Decimal("1000.12345678"),
                    change_abs=None,
                    change_pct=None,
                ),
                NetWorthHistoryPoint(
                    period="2026-07-01",
                    net_worth=Decimal("1100.87654321"),
                    change_abs=Decimal("100.75308643"),
                    change_pct=Decimal("0.10074065"),
                ),
            ]
        ),
    )

    result = ReportCatalog((NETWORTH_HISTORY_REPORT,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="core:networth_history",
        parameters={
            "from_date": "2026-06-01",
            "to_date": "2026-07-31",
            "interval": "monthly",
        },
        limit=1,
    )

    history.assert_called_once_with(
        date(2026, 6, 1),
        date(2026, 7, 31),
        interval="monthly",
    )
    assert result.semantics.kind == "position"
    assert result.records == [
        {
            "period": "2026-06-01",
            "net_worth": Decimal("1000.12345678"),
            "change_abs": None,
            "change_pct": None,
        }
    ]
    assert isinstance(result.records[0]["net_worth"], Decimal)
    assert result.truncated is True
    assert result.total_count == 2


def test_service_report_metadata_is_frozen() -> None:
    with pytest.raises(AttributeError):
        NETWORTH_REPORT.name = "changed"  # type: ignore[misc]  # frozen contract


def test_extension_reports_join_fresh_catalog_without_surface_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry, "_extension_reports", {})
    before = get_report_catalog()
    extension = _sql_report(
        report_id="retirement:summary",
        name="retirement_summary",
    )

    register_extension_report(extension)
    after = get_report_catalog()

    with pytest.raises(UserError) as raised:
        before.resolve("retirement_summary")
    assert raised.value.code == "REPORT_ID_NOT_FOUND"
    assert after.resolve("retirement_summary") is extension
    assert extension_report_specs() == (extension,)


def test_transitional_core_cli_registration_does_not_populate_extensions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry, "_extension_reports", {})

    register_reports_cli(ALL_REPORTS, typer.Typer())

    assert extension_report_specs() == ()


def test_duplicate_extension_report_id_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry, "_extension_reports", {})
    report = _sql_report(report_id="retirement:summary")
    register_extension_report(report)

    with pytest.raises(ValueError, match="duplicate extension report_id"):
        register_extension_report(_sql_report(report_id="retirement:summary"))
