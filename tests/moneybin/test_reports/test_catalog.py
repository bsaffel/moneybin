"""Unified catalog behavior for SQL-backed and service-backed reports."""

from __future__ import annotations

import json
import typing
from collections.abc import Mapping
from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import cast
from unittest.mock import MagicMock

import pytest
import typer
from pydantic import JsonValue
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
from moneybin.protocol.envelope import PayloadEncoder
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
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    CatalogReportResult,
    build_catalog_execution,
    build_catalog_result,
)
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
            ParamSpec(
                "count",
                int,
                None,
                True,
                "Required count.",
                DataClass.AGGREGATE,
            ),
            ParamSpec(
                "label",
                str | None,
                None,
                False,
                "Optional label.",
                DataClass.USER_NOTE,
            ),
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
        parameters=(
            ParamSpec(
                "year",
                int,
                None,
                True,
                "Tax year.",
                DataClass.TXN_DATE,
            ),
        ),
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


def test_registered_account_id_metadata_uses_opaque_record_id_class() -> None:
    """Exact account-id fields stay unmasked across both report kinds."""
    problems: list[str] = []
    for report in get_report_catalog().list():
        if report.classes.get("account_id") is not None and (
            report.classes["account_id"] is not DataClass.RECORD_ID
        ):
            problems.append(f"{report.report_id}.account_id output")
        parameters = (
            report.params if isinstance(report, ReportSpec) else report.parameters
        )
        for parameter in parameters:
            if parameter.name in {"account_id", "account_ids"} and (
                parameter.data_class is not DataClass.RECORD_ID
            ):
                problems.append(f"{report.report_id}.{parameter.name} parameter")

    assert problems == []


def test_service_report_privacy_maps_match_independent_contract() -> None:
    """Every service-backed report has an explicit, independently reviewed map."""
    expected = {
        "core:networth": {
            "columns": {
                "balance_date": DataClass.TXN_DATE,
                "net_worth": DataClass.BALANCE,
                "total_assets": DataClass.BALANCE,
                "total_liabilities": DataClass.BALANCE,
                "account_count": DataClass.AGGREGATE,
                "account_id": DataClass.RECORD_ID,
                "account_name": DataClass.USER_NOTE,
                "account_balance": DataClass.BALANCE,
                "observation_source": DataClass.TXN_TYPE,
            },
            "parameters": {
                "as_of": DataClass.TXN_DATE,
                "account_ids": DataClass.RECORD_ID,
            },
        },
        "core:networth_history": {
            "columns": {
                "period": DataClass.TXN_DATE,
                "net_worth": DataClass.BALANCE,
                "change_abs": DataClass.BALANCE,
                "change_pct": DataClass.AGGREGATE,
            },
            "parameters": {
                "from_date": DataClass.TXN_DATE,
                "to_date": DataClass.TXN_DATE,
                "interval": DataClass.TXN_TYPE,
            },
        },
    }
    service_reports = {
        report.report_id: report
        for report in get_report_catalog().list()
        if isinstance(report, ServiceReportSpec)
    }

    assert set(service_reports) == set(expected)
    for report_id, contract in expected.items():
        report = service_reports[report_id]
        assert report.classes == contract["columns"]
        assert {
            parameter.name: parameter.data_class for parameter in report.parameters
        } == contract["parameters"]


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


def test_legacy_typing_optional_is_validated_like_pep604_union() -> None:
    report = replace(
        _sql_report(),
        params=(
            ParamSpec(
                "count",
                typing.Optional[int],  # noqa: UP045  # legacy union form under test
                None,
                False,
                "Optional count.",
                DataClass.AGGREGATE,
            ),
            ParamSpec(
                "label",
                str | None,
                None,
                False,
                "Optional label.",
                DataClass.USER_NOTE,
            ),
        ),
    )
    db = _db_with_rows((None,))

    result = ReportCatalog((report,)).execute(
        db,
        report_id="core:summary",
        parameters={},
        limit=100,
    )

    assert result.records == [{"value": None}]
    cast(MagicMock, db.execute).assert_called_once_with(
        "SELECT ? AS value",
        [None],
    )


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
    executor = MagicMock()
    service_report = _service_report(executor)
    execution = build_catalog_execution(
        service_report,
        parameters={"year": 2026},
        records=[{"value": 7}],
        columns=["value"],
        column_types=["BIGINT"],
        max_rows=25,
        sql=None,
    )
    executor.return_value = execution
    catalog = ReportCatalog((service_report,))
    db = MagicMock(spec=Database)

    result = catalog.execute(
        cast(Database, db),
        report_id="retirement:summary",
        parameters={"year": 2026},
        limit=25,
    )

    assert result.records == [{"value": 7}]
    assert result.report_id == "retirement:summary"
    executor.assert_called_once_with(
        cast(Database, db),
        {"year": 2026},
        25,
    )
    db.execute.assert_not_called()


def test_catalog_resolve_request_validates_service_parameters_without_execution() -> (
    None
):
    executor = MagicMock()
    validator = MagicMock()
    report = replace(_service_report(executor), validator=validator)
    catalog = ReportCatalog((report,))

    resolved, parameters = catalog.resolve_request(
        report_id="summary",
        parameters={"year": 2026},
        limit=None,
    )

    assert resolved is report
    assert parameters == {"year": 2026}
    validator.assert_called_once_with({"year": 2026})
    executor.assert_not_called()


def test_catalog_execute_raw_returns_unredacted_execution() -> None:
    report = _sql_report()
    catalog = ReportCatalog((report,))
    db = _db_with_rows((7,))

    resolved, execution = catalog.execute_raw(
        db,
        report_id="summary",
        parameters={"count": 7, "label": "private label"},
        limit=100,
    )

    assert resolved is report
    assert isinstance(execution, CatalogReportExecution)
    assert execution.parameters == {"count": 7, "label": "private label"}
    assert execution.records == [{"value": 7}]
    assert execution.columns == ["value"]
    cast(MagicMock, db.execute).assert_called_once_with("SELECT ? AS value", [7])


@pytest.mark.parametrize(
    "sensitive_class",
    [
        DataClass.USER_NOTE,
        DataClass.BALANCE,
        DataClass.ACCOUNT_IDENTIFIER,
    ],
)
def test_sensitive_mapping_parameter_metadata_is_summarized_without_keys(
    sensitive_class: DataClass,
) -> None:
    dispatched: dict[str, JsonValue] = {}

    def executor(
        db: Database,  # noqa: ARG001  # contract handle
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> CatalogReportExecution:
        dispatched.update(parameters)
        return build_catalog_execution(
            spec,
            parameters=parameters,
            records=[{"value": 1}],
            columns=["value"],
            column_types=["BIGINT"],
            max_rows=limit,
            sql=None,
        )

    spec = ServiceReportSpec(
        report_id="test:nested",
        name="nested",
        description="Nested parameter report.",
        parameters=(
            ParamSpec(
                "accounts",
                dict[str, str],
                None,
                True,
                "Account-reference mapping.",
                sensitive_class,
            ),
        ),
        columns=_COLUMNS,
        semantics=_SEMANTICS,
        classes=_CLASSES,
        examples=(),
        executor=executor,
    )
    raw_accounts: dict[str, JsonValue] = {
        "acct_key_11112222": "acct_value_99998888",
    }

    result = ReportCatalog((spec,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="test:nested",
        parameters={"accounts": raw_accounts},
        limit=100,
    )

    assert dispatched["accounts"] == raw_accounts
    assert result.parameters == {
        "accounts": {"entry_count": 1, "redacted": True},
    }
    with pytest.raises(TypeError):
        result.parameters["accounts"] = {}  # type: ignore[index]  # immutable
    nested = cast(Mapping[str, object], result.parameters["accounts"])
    with pytest.raises(TypeError):
        nested["entry_count"] = 2  # type: ignore[index]  # immutable
    normalized = json.loads(json.dumps(result.parameters, cls=PayloadEncoder))
    assert normalized == {
        "accounts": {"entry_count": 1, "redacted": True},
    }
    assert "acct_key_11112222" not in json.dumps(normalized)
    assert "acct_value_99998888" not in json.dumps(normalized)


def test_low_mapping_parameter_metadata_retains_frozen_json_shape() -> None:
    executor = MagicMock()
    spec = ServiceReportSpec(
        report_id="test:low_mapping",
        name="low_mapping",
        description="Low-safe mapping report.",
        parameters=(
            ParamSpec(
                "categories",
                dict[str, list[str]],
                None,
                True,
                "Category mapping.",
                DataClass.CATEGORY,
            ),
        ),
        columns=_COLUMNS,
        semantics=_SEMANTICS,
        classes=_CLASSES,
        examples=(),
        executor=executor,
    )

    result = build_catalog_result(
        spec,
        parameters={"categories": {"food": ["groceries", "dining"]}},
        records=[{"value": 1}],
        columns=["value"],
        max_rows=100,
    )

    assert result.parameters == {
        "categories": {"food": ("groceries", "dining")},
    }


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
        "resolved transaction-adjusted daily positions on or before the "
        "resolved balance_date"
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
            "account_id": "acct_11112222",
            "account_name": "Checking",
            "account_balance": Decimal("500.12000000"),
            "observation_source": "asserted",
        }
    ]
    assert result.output_classes["account_id"] is DataClass.RECORD_ID
    assert result.tier is Tier.HIGH
    assert result.truncated is True
    assert result.total_count == 2
    envelope = result.to_envelope().to_dict()
    assert envelope["summary"]["display_currency"] == "USD"
    db.execute.assert_not_called()


def test_networth_account_id_parameter_metadata_preserves_opaque_ids(
    mocker: MockerFixture,
) -> None:
    current = mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=0,
            per_account=[],
        ),
    )

    result = ReportCatalog((NETWORTH_REPORT,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="core:networth",
        parameters={"account_ids": ["acct_11112222"]},
        limit=100,
    )

    current.assert_called_once_with(
        as_of_date=None,
        account_ids=["acct_11112222"],
    )
    assert result.parameters == {
        "as_of": None,
        "account_ids": ("acct_11112222",),
    }


def test_networth_service_report_preserves_explicit_no_data(
    mocker: MockerFixture,
) -> None:
    mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
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
    assert result.records[0]["balance_date"] is None
    assert result.records[0]["net_worth"] is None
    assert result.records[0]["total_assets"] is None
    assert result.records[0]["total_liabilities"] is None
    assert result.total_count == 1
    assert result.truncated is False
    assert result.period is None


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
    assert result.semantics.valuation_basis == (
        "last resolved transaction-adjusted daily position in each selected period"
    )
    columns = {column.name: column for column in NETWORTH_HISTORY_REPORT.columns}
    assert columns["net_worth"].description == (
        "Resolved transaction-adjusted period-end position."
    )
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


@pytest.mark.parametrize("kind", ["sql", "service"])
def test_negative_limit_is_rejected_before_dispatch(kind: str) -> None:
    executor = MagicMock()
    report: ReportSpec | ServiceReportSpec
    report = _sql_report() if kind == "sql" else _service_report(executor)
    catalog = ReportCatalog((report,))
    db = MagicMock(spec=Database)

    with pytest.raises(UserError) as raised:
        catalog.execute(
            cast(Database, db),
            report_id=report.report_id,
            parameters={"count": 1} if kind == "sql" else {"year": 2026},
            limit=-1,
        )

    assert raised.value.code == "REPORT_LIMIT_INVALID"
    assert raised.value.details == {"minimum": 0}
    executor.assert_not_called()
    db.execute.assert_not_called()


def test_zero_limit_is_valid_and_reports_truncation() -> None:
    result = ReportCatalog((_sql_report(),)).execute(
        _db_with_rows((7,)),
        report_id="core:summary",
        parameters={"count": 7},
        limit=0,
    )

    assert result.records == []
    assert result.truncated is True
    assert result.total_count == 1


@pytest.mark.parametrize(
    ("spec", "parameters", "code", "details"),
    [
        (
            NETWORTH_REPORT,
            {"as_of": "not-a-date"},
            "REPORT_PARAMETER_INVALID_VALUE",
            {
                "report_id": "core:networth",
                "parameter": "as_of",
                "expected": "ISO date (YYYY-MM-DD)",
            },
        ),
        (
            NETWORTH_REPORT,
            {"as_of": "20260702"},
            "REPORT_PARAMETER_INVALID_VALUE",
            {
                "report_id": "core:networth",
                "parameter": "as_of",
                "expected": "ISO date (YYYY-MM-DD)",
            },
        ),
        (
            NETWORTH_REPORT,
            {"as_of": "2026-W27-4"},
            "REPORT_PARAMETER_INVALID_VALUE",
            {
                "report_id": "core:networth",
                "parameter": "as_of",
                "expected": "ISO date (YYYY-MM-DD)",
            },
        ),
        (
            NETWORTH_REPORT,
            {"as_of": "2026-02-30"},
            "REPORT_PARAMETER_INVALID_VALUE",
            {
                "report_id": "core:networth",
                "parameter": "as_of",
                "expected": "ISO date (YYYY-MM-DD)",
            },
        ),
        (
            NETWORTH_HISTORY_REPORT,
            {
                "from_date": "2026-07-02",
                "to_date": "2026-07-01",
            },
            "REPORT_PARAMETER_INVALID_RANGE",
            {
                "report_id": "core:networth_history",
                "parameters": ["from_date", "to_date"],
                "relation": "from_date <= to_date",
            },
        ),
    ],
)
def test_service_value_validation_runs_before_executor(
    spec: ServiceReportSpec,
    parameters: dict[str, object],
    code: str,
    details: dict[str, object],
) -> None:
    executor = MagicMock()
    guarded = replace(spec, executor=executor)

    with pytest.raises(UserError) as raised:
        ReportCatalog((guarded,)).execute(
            cast(Database, MagicMock(spec=Database)),
            report_id=guarded.report_id,
            parameters=parameters,  # type: ignore[arg-type]  # invalid values under test
            limit=100,
        )

    assert raised.value.code == code
    assert raised.value.details == details
    serialized_error = json.dumps({
        "message": raised.value.message,
        "details": raised.value.details,
    })
    for value in parameters.values():
        if isinstance(value, str):
            assert value not in serialized_error
    executor.assert_not_called()


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
