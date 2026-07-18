"""Typed payload contracts for the internal report catalog runner."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import Any, cast, get_args, get_origin, get_type_hints
from unittest.mock import MagicMock

from pydantic import JsonValue, TypeAdapter

from moneybin.privacy.introspection import derive_tier
from moneybin.privacy.payloads.reports import (
    ReportCatalogPayload,
    ReportResultPayload,
    ReportsPayload,
)
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.protocol.envelope import build_envelope
from moneybin.reports._framework.catalog import (
    ReportCatalog,
    ServiceReportSpec,
    catalog_to_payload,
    result_to_payload,
)
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportSemantics,
)
from moneybin.reports._framework.execute import (
    CatalogReportResult,
    build_catalog_result,
)

_SEMANTICS = ReportSemantics(
    unit="currency",
    currency="summary.display_currency",
    sign="positive absolute outflow",
    kind="flow",
    valuation_basis="transaction amount",
    fx_basis="no FX conversion",
    time_basis="calendar month",
    denominator=None,
    comparison_window="prior calendar month",
    exclusions=("transfers",),
    provenance=("reports.spending",),
)
_COLUMNS = (
    OutputColumn("date", "Calendar date.", DataClass.TXN_DATE),
    OutputColumn("amount", "Signed money amount.", DataClass.TXN_AMOUNT),
)
_SPEC = ServiceReportSpec(
    report_id="core:spending",
    name="spending",
    description="Monthly spending totals.",
    parameters=(
        ParamSpec(
            "from_month",
            str | None,
            None,
            False,
            "Inclusive lower month bound.",
            DataClass.TXN_DATE,
        ),
        ParamSpec(
            "category",
            str | None,
            None,
            False,
            "Optional category filter.",
            DataClass.CATEGORY,
        ),
    ),
    columns=_COLUMNS,
    semantics=_SEMANTICS,
    classes={column.name: column.data_class for column in _COLUMNS},
    examples=('reports(report_id="core:spending")',),
    executor=MagicMock(),
)
_CATALOG_RESULT = CatalogReportResult(
    report_id="core:spending",
    parameters=MappingProxyType({
        "from_month": "2026-07",
        "categories": MappingProxyType({"food": ("groceries", "dining")}),
        "account_ids": ("****2222",),
    }),
    semantics=_SEMANTICS,
    provenance=_SEMANTICS.provenance,
    records=[{"date": date(2026, 7, 1), "amount": Decimal("42.50")}],
    columns=["date", "amount"],
    output_classes={"date": DataClass.TXN_DATE, "amount": DataClass.TXN_AMOUNT},
    tier=Tier.HIGH,
    total_count=1,
    truncated=False,
    period="2026-07",
)


def test_reports_payload_is_a_tagged_union() -> None:
    schema = TypeAdapter(ReportsPayload).json_schema()

    discriminator = schema["discriminator"]
    assert discriminator["propertyName"] == "kind"
    assert set(discriminator["mapping"]) == {"catalog", "result"}


def test_catalog_entry_includes_complete_static_metadata() -> None:
    payload = catalog_to_payload(ReportCatalog((_SPEC,)))

    assert isinstance(payload, ReportCatalogPayload)
    entry = payload.reports[0]
    assert entry.report_id == "core:spending"
    assert entry.description == "Monthly spending totals."
    assert entry.parameter_schema == {
        "additionalProperties": False,
        "properties": {
            "category": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "default": None,
                "description": "Optional category filter.",
            },
            "from_month": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "default": None,
                "description": "Inclusive lower month bound.",
            },
        },
        "type": "object",
    }
    assert entry.parameter_classes == {
        "from_month": "txn_date",
        "category": "category",
    }
    assert entry.examples == ['reports(report_id="core:spending")']
    assert [(column.name, column.description) for column in entry.columns] == [
        ("date", "Calendar date."),
        ("amount", "Signed money amount."),
    ]
    assert entry.output_classes == {"date": "txn_date", "amount": "txn_amount"}
    assert entry.semantics.provenance == ("reports.spending",)


def test_result_repeats_semantics_provenance_and_runtime_classification() -> None:
    payload = result_to_payload(_CATALOG_RESULT)

    assert isinstance(payload, ReportResultPayload)
    assert payload.kind == "result"
    assert payload.report_id == "core:spending"
    assert payload.semantics.provenance == ("reports.spending",)
    assert [(column.name, column.data_class) for column in payload.columns] == [
        ("date", "txn_date"),
        ("amount", "txn_amount"),
    ]
    assert payload.sensitivity == "high"
    assert payload.count == 1
    assert payload.truncated is False


def test_result_parameters_thaw_only_safe_frozen_json_shapes() -> None:
    payload = result_to_payload(_CATALOG_RESULT)

    assert payload.parameters == {
        "from_month": "2026-07",
        "categories": {"food": ["groceries", "dining"]},
        "account_ids": ["****2222"],
    }
    assert isinstance(payload.parameters["categories"], dict)
    assert isinstance(payload.parameters["account_ids"], list)


def test_result_payload_preserves_runtime_numeric_and_date_types() -> None:
    payload = result_to_payload(_CATALOG_RESULT)
    envelope = build_envelope(data=payload)
    encoded = json.loads(envelope.to_json())

    assert isinstance(encoded["data"]["rows"][0]["amount"], float)
    assert encoded["data"]["rows"][0]["amount"] == 42.5
    assert encoded["data"]["rows"][0]["date"] == "2026-07-01"


def test_result_row_contract_is_not_falsely_annotated_as_aggregate() -> None:
    hints = get_type_hints(ReportResultPayload, include_extras=True)
    rows_type = hints["rows"]

    assert get_origin(rows_type) is list
    assert get_args(rows_type) == (dict[str, Any],)
    assert derive_tier(ReportResultPayload) is Tier.LOW


def test_result_payload_never_recovers_raw_account_parameter_values() -> None:
    raw_account_id = "acct-raw-123456789012"
    spec = ServiceReportSpec(
        report_id="test:accounts",
        name="accounts",
        description="Account-scoped report.",
        parameters=(
            ParamSpec(
                "account_ids",
                list[str],
                None,
                True,
                "Account IDs.",
                DataClass.ACCOUNT_IDENTIFIER,
            ),
            ParamSpec(
                "account_refs",
                dict[str, str],
                None,
                True,
                "Account reference mapping.",
                DataClass.ACCOUNT_IDENTIFIER,
            ),
        ),
        columns=(OutputColumn("value", "Aggregate value.", DataClass.AGGREGATE),),
        semantics=_SEMANTICS,
        classes={"value": DataClass.AGGREGATE},
        examples=(),
        executor=MagicMock(),
    )
    result = build_catalog_result(
        spec,
        parameters=cast(
            Mapping[str, JsonValue],
            {
                "account_ids": [raw_account_id],
                "account_refs": {raw_account_id: "source-account-99990000"},
            },
        ),
        records=[{"value": 1}],
        columns=["value"],
        max_rows=100,
    )

    payload = result_to_payload(result)
    serialized = build_envelope(data=payload).to_json()

    assert payload.parameters == {
        "account_ids": ["****9012"],
        "account_refs": {"entry_count": 1, "redacted": True},
    }
    assert raw_account_id not in serialized
    assert "source-account-99990000" not in serialized
    assert payload.sensitivity == "low"
