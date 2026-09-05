"""Unified catalog behavior for SQL-backed and service-backed reports."""

from __future__ import annotations

import dataclasses
import json
import logging
import re
import shlex
import typing
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Literal, cast
from unittest.mock import MagicMock, patch

import click
import pytest
import typer
from pydantic import JsonValue
from pytest_mock import MockerFixture

from moneybin.cli.main import app as cli_app
from moneybin.database import (
    Database,
    DatabaseKeyError,
    DatabaseNotInitializedError,
)
from moneybin.errors import UserError
from moneybin.matching.persistence import count_pending_matches
from moneybin.privacy.payloads.networth import (
    NetWorthAccountRow,
    NetWorthCurrencySegment,
    NetWorthHistoryPayload,
    NetWorthHistoryPoint,
    NetWorthSnapshotPayload,
)
from moneybin.privacy.payloads.reports import (
    ReportCatalogEntry,
    ReportOutputColumn,
    ReportSemanticsPayload,
)
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.protocol.envelope import PayloadEncoder
from moneybin.reports._framework import registry
from moneybin.reports._framework.catalog import (
    DEGRADED_PENDING_DEDUP,
    STALE_DEDUP_HINT,
    ReportCatalog,
    ServiceReportSpec,
    catalog_classes_returned,
    catalog_sensitivity,
    get_report_catalog,
    open_report_catalog,
)
from moneybin.reports._framework.contract import (
    USER_NAMESPACE,
    Binding,
    OutputColumn,
    ParamSpec,
    ReportQuery,
    ReportSemantics,
    ReportSpec,
)
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    CatalogReportResult,
    ReportResult,
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
from moneybin.services.matching_service import (
    PENDING_MATCHES_HINT,
    MatchingService,
)
from moneybin.tables import (
    FCT_TRANSACTIONS,
    MATCH_DECISIONS,
    MODEL_FRESHNESS,
    TableRef,
)
from tests.database_mocks import without_a_profile
from tests.moneybin.db_helpers import record_model_execution, seed_pending_dedup_pair

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
        [Binding(count, DataClass.AGGREGATE)],
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


def test_every_money_bearing_report_projects_the_currency_it_is_denominated_in() -> (
    None
):
    """No registered report emits an amount without naming its currency.

    multi-currency.md Requirements 5 and 6 — the report path that "would
    violate Requirement 5" is one that sums money and cannot tell two
    currencies apart. Enumerating the live catalog (rather than a hand-kept
    list) is what makes a future report unable to ship unsegmented.
    """
    monetary = {DataClass.TXN_AMOUNT, DataClass.BALANCE}
    unsegmented = [
        report.report_id
        for report in get_report_catalog().list()
        if monetary.intersection(report.classes.values())
        and report.classes.get("currency_code") is not DataClass.CURRENCY
    ]

    assert unsegmented == []


def test_only_reports_whose_rows_price_exactly_declare_an_fx_date() -> None:
    """Set equality, because both halves of this membership are load-bearing.

    Declaring `fx_date` opts a report's rows into display conversion, which is
    defensible only where one row holds one amount and one date to price it on.
    Five of the eight packaged reports put `currency_code` in their GROUP BY,
    so pricing each row into one display currency would return several rows
    sharing a grain key — one month and category in two currencies, both
    relabelled USD, with nothing left to tell them apart or add them up.

    Adding an id here without checking that report's grain reintroduces exactly
    that, silently. Dropping one stops converting a report that used to. A
    subset check or a count would catch neither.
    """
    declared = {
        report.report_id
        for report in get_report_catalog().list()
        if report.semantics.fx_date is not None
    }

    assert declared == {
        "core:balance_drift",
        "core:large_transactions",
        "core:networth",
    }


def test_service_report_privacy_maps_match_independent_contract() -> None:
    """Every service-backed report has an explicit, independently reviewed map."""
    expected = {
        "core:networth": {
            "columns": {
                "balance_date": DataClass.TXN_DATE,
                "currency_code": DataClass.CURRENCY,
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
                "currency_code": DataClass.CURRENCY,
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


def test_the_collision_warning_names_the_reports_and_not_the_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A colliding report name may be a merchant name, so it cannot be logged.

    A saved report is named by its user, and ``amazon-spend`` is both a
    plausible name and a merchant name ``.claude/rules/security.md`` forbids in
    a log file — ``SanitizedLogFormatter`` masks digit runs and dollar amounts,
    never free text. The report IDs carry everything an operator can act on.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.reports._framework.catalog"):
        ReportCatalog((
            _sql_report(report_id="core:summary", name="amazon-spend"),
            _sql_report(report_id="user:rab12cd34ef56", name="amazon-spend"),
        ))

    assert "amazon-spend" not in caplog.text
    assert "core:summary" in caplog.text
    assert "user:rab12cd34ef56" in caplog.text


def test_ambiguous_short_id_lists_sorted_namespaced_candidates() -> None:
    executor = MagicMock()
    catalog = ReportCatalog((_sql_report(), _service_report(executor)))

    with pytest.raises(UserError) as raised:
        catalog.resolve("summary")

    assert raised.value.code == "report_id_ambiguous"
    assert raised.value.details == {
        "report_id": "summary",
        "candidates": ["core:summary", "retirement:summary"],
    }


def test_missing_report_id_is_structured_and_sanitized() -> None:
    catalog = ReportCatalog((_sql_report(),))

    with pytest.raises(UserError) as raised:
        catalog.resolve("missing")

    assert raised.value.code == "report_id_not_found"
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
            "report_parameter_unknown",
            {
                "report_id": "retirement:summary",
                "parameters": ["account_number"],
            },
        ),
        (
            {},
            "report_parameter_missing",
            {"report_id": "retirement:summary", "parameters": ["year"]},
        ),
        (
            {"year": "2026"},
            "report_parameter_invalid_type",
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

    assert raised.value.code == "report_parameter_invalid_type"
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


def test_display_currency_sees_every_row_not_just_the_returned_page() -> None:
    """Truncation must not turn a mixed-currency result into a confident one.

    The page returned here is entirely USD; the rows past `max_rows` are EUR.
    Resolving over the truncated slice would advertise display_currency "USD"
    for a result that is not USD — the Requirement 5 blend re-entering at the
    pagination boundary, where it is hardest to notice.
    """
    execution = build_catalog_execution(
        _sql_report(),
        parameters={"count": 3},
        records=[
            {"value": 1, "currency_code": "USD"},
            {"value": 2, "currency_code": "USD"},
            {"value": 3, "currency_code": "EUR"},
        ],
        columns=["value", "currency_code"],
        column_types=["BIGINT", "VARCHAR"],
        max_rows=2,
        sql=None,
    )

    assert execution.truncated
    assert [row["currency_code"] for row in execution.records] == ["USD", "USD"]
    assert execution.display_currency is None


def test_display_currency_describes_the_returned_page_when_truncated() -> None:
    """A truncated but uniform result still names its currency, correctly.

    The field describes the rows in this response — `has_more` is what says
    more exist. `records` is what the cursor fetched, max_rows + 1, so
    agreement across it is always true of the returned rows. Withholding here
    would cost every large single-currency report a correct label and buy no
    safety, because the page-scoped claim was never wrong.
    """
    execution = build_catalog_execution(
        _sql_report(),
        parameters={"count": 3},
        records=[{"value": n, "currency_code": "USD"} for n in range(3)],
        columns=["value", "currency_code"],
        column_types=["BIGINT", "VARCHAR"],
        max_rows=2,
        sql=None,
    )

    assert execution.truncated
    assert execution.display_currency == "USD"


def test_report_without_a_currency_column_states_no_currency() -> None:
    """A report that never mentions currency has not been told one.

    ``build_catalog_execution`` only resolves a currency when the result
    declares a ``currency_code`` column; everything else falls through to the
    dataclass default. Every report that counts, ranks, or ratios — no currency
    column anywhere — therefore shipped a confident label its rows never
    supported.
    """
    execution = build_catalog_execution(
        _sql_report(),
        parameters={"count": 1},
        records=[{"value": 1}],
        columns=["value"],
        column_types=["BIGINT"],
        max_rows=None,
        sql=None,
    )

    assert "currency_code" not in execution.columns
    assert execution.display_currency is None


@pytest.mark.parametrize("cls", [ReportResult, CatalogReportExecution])
def test_report_result_currency_default_is_not_a_currency_literal(
    cls: type,
) -> None:
    """Pin both defaults: no future edit may restore a hardcoded currency.

    The behavioural test above passes just as well if someone re-adds ``"USD"``
    and updates that one assertion, and it only reaches one of the two classes.
    ``build_envelope`` carries the same pin
    (``test_build_envelope_default_is_not_a_currency_literal``) — these are the
    remaining places one default speaks for every caller at once.
    """
    default = next(
        f for f in dataclasses.fields(cls) if f.name == "display_currency"
    ).default

    assert default is None


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
            currency_code="USD",
            net_worth=Decimal("1234.56000000"),
            total_assets=Decimal("1500.12000000"),
            total_liabilities=Decimal("-265.56000000"),
            account_count=2,
            per_currency=[
                NetWorthCurrencySegment(
                    currency_code="USD",
                    net_worth=Decimal("1234.56000000"),
                    total_assets=Decimal("1500.12000000"),
                    total_liabilities=Decimal("-265.56000000"),
                    account_count=2,
                ),
            ],
            per_account=[
                NetWorthAccountRow(
                    account_id="acct_11112222",
                    display_name="Checking",
                    balance=Decimal("500.12000000"),
                    observation_source="asserted",
                    currency_code="USD",
                ),
                NetWorthAccountRow(
                    account_id="acct_99998888",
                    display_name="Brokerage",
                    balance=Decimal("1000.00000000"),
                    observation_source="derived",
                    currency_code="USD",
                ),
            ],
        ),
    )
    db = without_a_profile(MagicMock(spec=Database))

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
    assert result.semantics.fx_date == "balance_date"
    assert result.parameters == {"as_of": "2026-07-02", "account_ids": None}
    # limit=1 keeps the currency's position and drops the breakdown, because
    # totals lead: a page capped below the row count still answers "what am I
    # worth" rather than showing one account and calling it the snapshot.
    assert result.records == [
        {
            "balance_date": date(2026, 7, 1),
            "currency_code": "USD",
            "net_worth": Decimal("1234.56000000"),
            "total_assets": Decimal("1500.12000000"),
            "total_liabilities": Decimal("-265.56000000"),
            "account_count": 2,
            "account_id": None,
            "account_name": None,
            "account_balance": None,
            "observation_source": None,
        }
    ]
    assert result.output_classes["account_id"] is DataClass.RECORD_ID
    assert result.tier is Tier.HIGH
    assert result.truncated is True
    # `max_rows + 1`, the deliberate lower bound a truncated execution reports —
    # three rows exist here (one totals, two accounts).
    assert result.total_count == 2
    envelope = result.to_envelope().to_dict()
    assert envelope["summary"]["display_currency"] == "USD"
    # Net worth is downstream of the transactions fact and reads it through a
    # materialized model, so the framework's own reads run: the pending count,
    # the model's rebuild stamp, and the decided-since count. `without_a_profile`
    # answers each with no row. The report's rows still never come from SQL here.
    pending_read, freshness_read, settled_read = db.execute.call_args_list
    assert MATCH_DECISIONS.full_name in pending_read.args[0]
    assert pending_read.args[1] == ["dedup"]
    assert MODEL_FRESHNESS.full_name in freshness_read.args[0]
    assert freshness_read.args[1] == ["core.fct_balances_daily"]
    assert MATCH_DECISIONS.full_name in settled_read.args[0]
    assert settled_read.args[1] == ["dedup"]


def test_networth_account_id_parameter_metadata_preserves_opaque_ids(
    mocker: MockerFixture,
) -> None:
    current = mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=None,
            currency_code=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=0,
            per_currency=[],
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
            currency_code=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=0,
            per_currency=[],
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
                    currency_code="USD",
                    net_worth=Decimal("1000.12345678"),
                    change_abs=None,
                    change_pct=None,
                ),
                NetWorthHistoryPoint(
                    period="2026-07-01",
                    currency_code="USD",
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
        "Resolved transaction-adjusted period-end position in currency_code."
    )
    assert result.records == [
        {
            "period": "2026-06-01",
            "currency_code": "USD",
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

    assert raised.value.code == "report_limit_invalid"
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
            "report_parameter_invalid_value",
            {
                "report_id": "core:networth",
                "parameter": "as_of",
                "expected": "ISO date (YYYY-MM-DD)",
            },
        ),
        (
            NETWORTH_REPORT,
            {"as_of": "20260702"},
            "report_parameter_invalid_value",
            {
                "report_id": "core:networth",
                "parameter": "as_of",
                "expected": "ISO date (YYYY-MM-DD)",
            },
        ),
        (
            NETWORTH_REPORT,
            {"as_of": "2026-W27-4"},
            "report_parameter_invalid_value",
            {
                "report_id": "core:networth",
                "parameter": "as_of",
                "expected": "ISO date (YYYY-MM-DD)",
            },
        ),
        (
            NETWORTH_REPORT,
            {"as_of": "2026-02-30"},
            "report_parameter_invalid_value",
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
            "report_parameter_invalid_range",
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
    assert raised.value.code == "report_id_not_found"
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


def test_an_extension_may_not_claim_the_user_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``user:`` means "derived at save time from a row", and code cannot be that.

    ``report_tier`` reads the namespace, so an extension shipping ``user:x``
    would be presented on every surface as the caller's own saved report: listed
    under ``--tier user``, explained at MEDIUM as user-authored text, and — the
    part that matters — believed to carry a class map *derived* from its SQL. Its
    ``classes={...}`` is declared by the package author instead, which is exactly
    the "user-supplied class in the map" that `.claude/rules/reports.md` forbids.
    """
    monkeypatch.setattr(registry, "_extension_reports", {})

    with pytest.raises(ValueError, match=f"{USER_NAMESPACE}:"):
        register_extension_report(
            _sql_report(report_id=f"{USER_NAMESPACE}:retirement", name="retirement")
        )

    assert extension_report_specs() == ()


def test_an_extension_namespace_that_merely_begins_with_user_registers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reserved thing is the namespace, not the four characters.

    ``user_notes`` is a package name a real extension could pick, and a check
    written as "starts with ``user``" would lock it out. The refusal above must
    fail closed on the namespace and stay open here.
    """
    monkeypatch.setattr(registry, "_extension_reports", {})
    report = _sql_report(report_id="user_notes:summary", name="user_notes_summary")

    register_extension_report(report)

    assert extension_report_specs() == (report,)


# ---------------------------------------------------------------------------
# Browsing without a database
# ---------------------------------------------------------------------------


def test_the_catalog_serves_the_packaged_tiers_when_no_database_exists() -> None:
    """Browsing must not require ``db init``: two of three tiers are repo files.

    An agent calling ``reports()`` with no arguments to orient itself used to
    receive the built-in catalog with no database touched. Adding the user tier
    turned that into a database-not-initialized error on a fresh profile, and
    turned a mistyped ``export report`` id into advice to run ``db unlock``.
    """
    with patch(
        "moneybin.reports._framework.catalog.get_database",
        side_effect=DatabaseNotInitializedError("no database yet"),
    ):
        with open_report_catalog() as (catalog, db):
            report_ids = {report.report_id for report in catalog.list()}

    assert db is None
    assert "core:networth" in report_ids


def test_a_locked_database_is_still_an_error_when_browsing() -> None:
    """The benign twin: only "never initialized" degrades.

    A locked or wrong-key database is a real failure with a real fix. Swallowing
    it would hand back a catalog silently missing the user's own reports.
    """
    with patch(
        "moneybin.reports._framework.catalog.get_database",
        side_effect=DatabaseKeyError("wrong key"),
    ):
        with pytest.raises(DatabaseKeyError):
            with open_report_catalog():
                pass  # pragma: no cover — the open raises before the body runs


def _listing_entry(
    *, tier: Literal["builtin", "extension", "user"], report_id: str = "core:spending"
) -> ReportCatalogEntry:
    """One catalog listing row, varying only the field ``catalog_sensitivity`` reads."""
    return ReportCatalogEntry(
        report_id=report_id,
        name=report_id.split(":")[-1],
        tier=tier,
        description="A listing row.",
        parameter_schema={},
        parameter_classes={},
        examples=[],
        columns=[ReportOutputColumn(name="total", data_class="aggregate")],
        output_classes={"total": "aggregate"},
        semantics=ReportSemanticsPayload(
            unit=None,
            currency=None,
            sign=None,
            kind="unknown",
            valuation_basis=None,
            fx_basis=None,
            time_basis=None,
            denominator=None,
            comparison_window=None,
            exclusions=(),
            provenance=(),
        ),
    )


@pytest.mark.parametrize(
    ("tiers", "expected"),
    [
        (("builtin",), "low"),
        (("builtin", "extension"), "low"),
        (("user",), "medium"),
        (("builtin", "user"), "medium"),
        (("builtin", "extension", "user"), "medium"),
        ((), "low"),
    ],
)
def test_catalog_sensitivity_elevates_on_any_user_tier_row(
    tiers: tuple[Literal["builtin", "extension", "user"], ...],
    expected: Literal["low", "medium"],
) -> None:
    """A listing holding one user-authored name is a MEDIUM response.

    Tested directly on the mixed-tier rows rather than through a surface that
    opens a database. Every caller-side test reached this via
    ``open_report_catalog()``, which degrades to the packaged tiers alone on a
    profile with no database — so the user arm was never actually taken and the
    ``low`` assertions passed for the wrong reason.
    """
    entries = [
        _listing_entry(tier=tier, report_id=f"{tier}:r{index}")
        for index, tier in enumerate(tiers)
    ]

    assert catalog_sensitivity(entries) == expected


def test_catalog_classes_returned_follows_the_elevated_sensitivity() -> None:
    """The audit event's class list is the envelope tier's other half.

    Pinned beside ``catalog_sensitivity`` because the privacy event and the
    envelope must never disagree about what a listing published.
    """
    assert catalog_classes_returned(catalog_sensitivity([])) == ["aggregate"]
    assert catalog_classes_returned(
        catalog_sensitivity([_listing_entry(tier="user")])
    ) == ["aggregate", "user_note"]


def test_report_envelope_names_the_currency_its_rows_are_denominated_in(
    mocker: MockerFixture,
) -> None:
    """summary.display_currency follows the rows, instead of asserting USD."""
    mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=date(2026, 7, 1),
            currency_code="GBP",
            net_worth=Decimal("1000.00"),
            total_assets=Decimal("1000.00"),
            total_liabilities=Decimal("0.00"),
            account_count=1,
            per_currency=[
                NetWorthCurrencySegment(
                    currency_code="GBP",
                    net_worth=Decimal("1000.00"),
                    total_assets=Decimal("1000.00"),
                    total_liabilities=Decimal("0.00"),
                    account_count=1,
                ),
            ],
            per_account=[
                NetWorthAccountRow(
                    account_id="acct_11112222",
                    display_name="Current",
                    balance=Decimal("1000.00"),
                    observation_source="asserted",
                    currency_code="GBP",
                ),
            ],
        ),
    )

    result = ReportCatalog((NETWORTH_REPORT,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="core:networth",
        parameters={},
        limit=100,
    )

    assert result.to_envelope().to_dict()["summary"]["display_currency"] == "GBP"


@pytest.mark.parametrize(
    ("second_currency", "case"),
    [
        ("USD", "two known currencies"),
        (None, "one known currency plus an unknown one"),
    ],
)
def test_report_envelope_names_no_currency_when_its_rows_disagree(
    mocker: MockerFixture, second_currency: str | None, case: str
) -> None:
    """Rows in more than one currency leave summary.display_currency null.

    The envelope default is "USD", so a resolver that declines to answer here
    silently labels the whole response USD — the same blend Requirement 5
    forbids in the rows, moved up into the summary. The unknown-currency case
    is the sharper one: it must not resolve to the one currency it *does* know.
    """
    segment = partial(
        NetWorthCurrencySegment,
        net_worth=Decimal("1000.00"),
        total_assets=Decimal("1000.00"),
        total_liabilities=Decimal("0.00"),
        account_count=1,
    )
    mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=date(2026, 7, 1),
            currency_code=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=2,
            per_currency=[
                segment(currency_code="GBP"),
                segment(currency_code=second_currency),
            ],
            per_account=[
                NetWorthAccountRow(
                    account_id="acct_11112222",
                    display_name="Current",
                    balance=Decimal("1000.00"),
                    observation_source="asserted",
                    currency_code="GBP",
                ),
                NetWorthAccountRow(
                    account_id="acct_33334444",
                    display_name="Other",
                    balance=Decimal("1000.00"),
                    observation_source="asserted",
                    currency_code=second_currency,
                ),
            ],
        ),
    )

    result = ReportCatalog((NETWORTH_REPORT,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="core:networth",
        parameters={},
        limit=100,
    )

    assert result.to_envelope().to_dict()["summary"]["display_currency"] is None, case


def test_networth_keeps_every_currency_within_the_returned_page(
    mocker: MockerFixture,
) -> None:
    """Truncation must not be able to drop a whole currency.

    Rows are one per account, so a profile with two dollar accounts and one
    euro account pushes the euro row third. Any limit below that returns a
    response that looks single-currency — blend by omission, the same failure
    the segmentation prevents inside a row. Ordering one representative per
    currency first makes the guarantee "every currency survives any limit at
    least as large as the currency count."
    """
    segment = partial(
        NetWorthCurrencySegment,
        total_assets=Decimal("1000.00"),
        total_liabilities=Decimal("0.00"),
        account_count=1,
    )
    account = partial(
        NetWorthAccountRow,
        balance=Decimal("1000.00"),
        observation_source="asserted",
    )
    mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=date(2026, 7, 1),
            currency_code=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=3,
            per_currency=[
                segment(currency_code="USD", net_worth=Decimal("2000.00")),
                segment(currency_code="EUR", net_worth=Decimal("1000.00")),
            ],
            per_account=[
                account(
                    account_id="acct_usd00001",
                    display_name="Checking",
                    currency_code="USD",
                ),
                account(
                    account_id="acct_usd00002",
                    display_name="Savings",
                    currency_code="USD",
                ),
                account(
                    account_id="acct_eur00001",
                    display_name="Euro",
                    currency_code="EUR",
                ),
            ],
        ),
    )

    result = ReportCatalog((NETWORTH_REPORT,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="core:networth",
        parameters={},
        limit=2,
    )

    assert {row["currency_code"] for row in result.records} == {"USD", "EUR"}


def test_networth_keeps_every_currency_when_the_breakdown_is_filtered(
    mocker: MockerFixture,
) -> None:
    """An account_ids filter narrows the breakdown, not the reported position."""
    mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=date(2026, 7, 1),
            currency_code=None,
            net_worth=None,
            total_assets=None,
            total_liabilities=None,
            account_count=2,
            per_currency=[
                NetWorthCurrencySegment(
                    currency_code="EUR",
                    net_worth=Decimal("800.00"),
                    total_assets=Decimal("800.00"),
                    total_liabilities=Decimal("0.00"),
                    account_count=1,
                ),
                NetWorthCurrencySegment(
                    currency_code="USD",
                    net_worth=Decimal("500.00"),
                    total_assets=Decimal("500.00"),
                    total_liabilities=Decimal("0.00"),
                    account_count=1,
                ),
            ],
            # Only the USD account survived the filter.
            per_account=[
                NetWorthAccountRow(
                    account_id="acct_usd",
                    display_name="Checking",
                    balance=Decimal("500.00"),
                    observation_source="asserted",
                    currency_code="USD",
                ),
            ],
        ),
    )

    result = ReportCatalog((NETWORTH_REPORT,)).execute(
        cast(Database, MagicMock(spec=Database)),
        report_id="core:networth",
        parameters={"account_ids": ["acct_usd"]},
        limit=100,
    )

    by_currency = {
        record["currency_code"]: record["net_worth"]
        for record in result.records
        if record["account_id"] is None
    }
    assert by_currency == {"USD": Decimal("500.00"), "EUR": Decimal("800.00")}


def test_networth_separates_currency_totals_from_account_balances(
    mocker: MockerFixture,
) -> None:
    """A currency's position is one row; each account's balance is another.

    Fusing the headline onto every account row makes two dollar accounts two
    rows that each claim the same $2,000 position. Display conversion prices
    rows one at a time, so once both are relabelled into the display currency
    nothing distinguishes them from two separate positions, and anything that
    adds them up double-counts. Separate rows keep the grain conversion has to
    preserve.
    """
    account = partial(
        NetWorthAccountRow,
        observation_source="asserted",
        currency_code="USD",
    )
    mocker.patch(
        "moneybin.reports.service_reports.NetworthService.current",
        return_value=NetWorthSnapshotPayload(
            balance_date=date(2026, 7, 1),
            currency_code="USD",
            net_worth=Decimal("2000.00"),
            total_assets=Decimal("2000.00"),
            total_liabilities=Decimal("0.00"),
            account_count=2,
            per_currency=[
                NetWorthCurrencySegment(
                    currency_code="USD",
                    net_worth=Decimal("2000.00"),
                    total_assets=Decimal("2000.00"),
                    total_liabilities=Decimal("0.00"),
                    account_count=2,
                )
            ],
            per_account=[
                account(
                    account_id="acct_usd00001",
                    display_name="Checking",
                    balance=Decimal("1200.00"),
                ),
                account(
                    account_id="acct_usd00002",
                    display_name="Savings",
                    balance=Decimal("800.00"),
                ),
            ],
        ),
    )

    records = (
        ReportCatalog((NETWORTH_REPORT,))
        .execute(
            cast(Database, MagicMock(spec=Database)),
            report_id="core:networth",
            parameters={},
            limit=100,
        )
        .records
    )

    totals = [row for row in records if row["account_id"] is None]
    accounts = [row for row in records if row["account_id"] is not None]
    assert [row["net_worth"] for row in totals] == [Decimal("2000.00")]
    assert [row["account_count"] for row in totals] == [2]
    # The position is stated once. An account row repeating it would be counted
    # again by anything summing the column.
    assert [row["net_worth"] for row in accounts] == [None, None]
    assert [row["account_count"] for row in accounts] == [None, None]
    assert [row["account_balance"] for row in accounts] == [
        Decimal("1200.00"),
        Decimal("800.00"),
    ]
    # Both kinds still say what they hold and when: conversion prices every row
    # it is handed, and segments the whole result if one cannot answer either.
    assert {row["currency_code"] for row in records} == {"USD"}
    assert {row["balance_date"] for row in records} == {date(2026, 7, 1)}
    # Totals lead, so a limit eats breakdown rows before it eats a position.
    assert records[0]["account_id"] is None

    # Requirement 6, and the reason this report's default set spans both row
    # shapes: every row has to render something. An account-only set turns the
    # leading position row into a blank line, and a profile holding no accounts
    # into a table of one empty row — the headline figure absent from the
    # default text view of a report whose whole subject is that figure.
    # Measured against each shape's *exclusive* columns, not against anything
    # it populates: `currency_code` and `balance_date` are filled on both, so a
    # set naming only those passes a weaker check while every totals row still
    # renders as a label with no figure beside it.
    declared_columns = NETWORTH_REPORT.default_columns
    assert declared_columns is not None and not callable(declared_columns), (
        "this report declares a static set"
    )
    declared = set(declared_columns)
    totals_filled = {
        name for row in totals for name, value in row.items() if value is not None
    }
    accounts_filled = {
        name for row in accounts for name, value in row.items() if value is not None
    }
    assert declared & (totals_filled - accounts_filled), (
        f"default columns {sorted(declared)} name nothing a totals row fills, "
        "so every position renders as a blank line"
    )
    assert declared & (accounts_filled - totals_filled), (
        f"default columns {sorted(declared)} name nothing an account row fills, "
        "so every breakdown row renders as a blank line"
    )


def _transaction_total_runner(db: Database) -> ReportQuery:  # noqa: ARG001  # contract handle
    """Total the very rows an undecided duplicate pair leaves doubled."""
    return ReportQuery("SELECT SUM(amount) AS value FROM core.fct_transactions")


def _transaction_total_report(provenance: tuple[str, ...]) -> ReportSpec:
    return dataclasses.replace(
        _sql_report(),
        runner=_transaction_total_runner,
        params=(),
        semantics=dataclasses.replace(_SEMANTICS, provenance=provenance),
    )


def test_a_total_over_an_undecided_duplicate_pair_is_marked_provisional(
    saved_db: Database,
) -> None:
    """Issue #409: both rows of a pending pair inflate the total, and it says so."""
    seed_pending_dedup_pair(saved_db)

    result = ReportCatalog((
        _transaction_total_report((FCT_TRANSACTIONS.full_name,)),
    )).execute(saved_db, report_id="core:summary", parameters={}, limit=100)

    assert result.degraded
    assert result.degraded_reason is not None
    assert result.degraded_reason.startswith(f"{DEGRADED_PENDING_DEDUP}: 1 ")
    assert PENDING_MATCHES_HINT in result.actions
    assert [action.tool for action in result.recovery_actions] == ["reviews"]


def test_a_total_with_nothing_pending_carries_no_provisional_marking(
    saved_db: Database,
) -> None:
    """The disclosure is evidence of a real queue, not decoration on every read."""
    result = ReportCatalog((
        _transaction_total_report((FCT_TRANSACTIONS.full_name,)),
    )).execute(saved_db, report_id="core:summary", parameters={}, limit=100)

    assert not result.degraded
    assert result.degraded_reason is None
    assert result.actions == []
    assert result.recovery_actions == ()


def test_a_report_reading_nothing_downstream_of_transactions_is_not_marked(
    saved_db: Database,
) -> None:
    """A pending pair cannot inflate a total that never reads a transaction."""
    seed_pending_dedup_pair(saved_db)

    result = ReportCatalog((
        _transaction_total_report(("raw.plaid_transactions",)),
    )).execute(saved_db, report_id="core:summary", parameters={}, limit=100)

    assert not result.degraded
    assert result.actions == []


def _naive_utc(offset: timedelta) -> datetime:
    """``meta.model_freshness`` stores naive UTC; fixtures must match it."""
    return datetime.now(UTC).replace(tzinfo=None) + offset


def test_a_total_stays_provisional_until_the_materialization_rebuilds(
    saved_db: Database,
) -> None:
    """A decided duplicate is still doubled inside a FULL model built before it.

    ``core.fct_balances_daily`` is ``kind="FULL"``, so accepting a dedup rewrites
    ``app.match_decisions`` and nothing else — ``reports.net_worth`` keeps
    serving the pre-decision balance until the next transform. Drives the real
    sequence: decide through ``MatchingService.set_status`` (what
    ``reviews_decide`` calls), then read the report with no rebuild in between.
    """
    seed_pending_dedup_pair(saved_db)
    record_model_execution(
        saved_db, "core.fct_balances_daily", _naive_utc(-timedelta(hours=1))
    )
    MatchingService(saved_db).set_status(
        "match00000001", status="accepted", decided_by="user", actor="test"
    )

    result = ReportCatalog((
        _transaction_total_report(("reports.net_worth",)),
    )).execute(saved_db, report_id="core:summary", parameters={}, limit=100)

    assert count_pending_matches(saved_db, match_type="dedup") == 0, (
        "the decision must actually have left the pending queue, or this test "
        "passes on the caveat the pending count already produced"
    )
    assert result.degraded
    assert result.degraded_reason is not None
    assert result.degraded_reason.startswith(f"{DEGRADED_PENDING_DEDUP}: ")
    assert [action.tool for action in result.recovery_actions] == ["refresh_run"]


def test_a_rebuilt_materialization_clears_the_provisional_marking(
    saved_db: Database,
) -> None:
    """Once the FULL model is rebuilt past the decision, the number is final."""
    seed_pending_dedup_pair(saved_db)
    MatchingService(saved_db).set_status(
        "match00000001", status="accepted", decided_by="user", actor="test"
    )
    record_model_execution(
        saved_db, "core.fct_balances_daily", _naive_utc(timedelta(hours=1))
    )

    result = ReportCatalog((
        _transaction_total_report(("reports.net_worth",)),
    )).execute(saved_db, report_id="core:summary", parameters={}, limit=100)

    assert not result.degraded
    assert result.degraded_reason is None
    assert result.recovery_actions == ()


def test_a_decision_clears_the_marking_at_once_on_a_view_only_report(
    saved_db: Database,
) -> None:
    """Nothing between ``app.match_decisions`` and this report holds rows.

    ``prep.int_transactions__matched`` through ``core.fct_transactions`` is
    every-hop ``kind VIEW``, so the collapse resolves on read and a decided pair
    owes no caveat here — the precision that keeps the disclosure meaningful
    where it does apply.
    """
    seed_pending_dedup_pair(saved_db)
    record_model_execution(
        saved_db, "core.fct_balances_daily", _naive_utc(-timedelta(hours=1))
    )
    MatchingService(saved_db).set_status(
        "match00000001", status="accepted", decided_by="user", actor="test"
    )

    result = ReportCatalog((
        _transaction_total_report((FCT_TRANSACTIONS.full_name,)),
    )).execute(saved_db, report_id="core:summary", parameters={}, limit=100)

    assert not result.degraded
    assert result.degraded_reason is None
    assert result.actions == []


def test_stale_dedup_hint_names_a_runnable_command() -> None:
    """A caveat printing an unrunnable command is worse than no caveat.

    Extracted from the constant rather than restated, so editing the hint fails
    here instead of in a user's terminal — the same guard
    ``PENDING_MATCHES_HINT`` carries after three surfaces once published one
    invalid invocation between them. ``moneybin refresh`` is a leaf command, so
    a stale ``refresh run`` spelling exits 2 on the extra argument.
    """
    invocation = re.search(r"'moneybin ([^']+)'", STALE_DEDUP_HINT)
    assert invocation, f"no `moneybin` command found in {STALE_DEDUP_HINT!r}"

    # Resolved through the command tree rather than invoked: appending `--help`
    # would short-circuit before argument parsing, so `refresh run --help`
    # exits 0 on a leaf command that accepts no `run`.
    command: click.Command = typer.main.get_command(cli_app)
    context = click.Context(command)
    for token in shlex.split(invocation.group(1)):
        assert isinstance(command, click.Group), (
            f"the hint prints `moneybin {invocation.group(1)}`, which passes "
            f"{token!r} to a leaf command that takes no argument"
        )
        resolved = command.get_command(context, token)
        assert resolved is not None, (
            f"the hint prints `moneybin {invocation.group(1)}`, and {token!r} "
            "is not a registered command"
        )
        command = resolved
