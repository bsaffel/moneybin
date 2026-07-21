"""One internal catalog for SQL-backed and service-backed reports."""

from __future__ import annotations

import re
import types
import typing
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, cast, get_args, get_origin

from pydantic import JsonValue, TypeAdapter

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.payloads.reports import (
    ReportCatalogEntry,
    ReportCatalogPayload,
    ReportOutputColumn,
    ReportResultPayload,
    ReportSemanticsPayload,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportSemantics,
    ReportSpec,
)
from moneybin.reports._framework.execute import CatalogReportResult, run_report

_REPORT_ID = re.compile(r"[a-z][a-z0-9_-]*:[a-z][a-z0-9_-]*")


@dataclass(frozen=True, slots=True)
class ServiceReportSpec:
    """Immutable catalog metadata and executor for one service-backed report."""

    report_id: str
    name: str
    description: str
    parameters: tuple[ParamSpec, ...]
    columns: tuple[OutputColumn, ...]
    semantics: ReportSemantics
    classes: Mapping[str, DataClass]
    examples: tuple[str, ...]
    executor: Callable[[Database, Mapping[str, JsonValue], int], CatalogReportResult]
    validator: Callable[[Mapping[str, JsonValue]], None] | None = None

    def __post_init__(self) -> None:
        if _REPORT_ID.fullmatch(self.report_id) is None:
            raise ValueError("report_id must use namespace:name")
        declared = {column.name: column.data_class for column in self.columns}
        if len(declared) != len(self.columns) or declared != dict(self.classes):
            raise ValueError(
                "columns and classes must declare the same output fields "
                "with identical privacy classes"
            )
        object.__setattr__(self, "classes", MappingProxyType(dict(self.classes)))


type RegisteredReport = ReportSpec | ServiceReportSpec


class ReportCatalog:
    """Deterministic resolver and dispatcher for registered reports."""

    def __init__(self, reports: Iterable[RegisteredReport]) -> None:
        ordered = tuple(sorted(reports, key=lambda report: report.report_id))
        duplicate_ids = sorted(
            report_id
            for report_id in {report.report_id for report in ordered}
            if sum(report.report_id == report_id for report in ordered) > 1
        )
        if duplicate_ids:
            raise ValueError(f"duplicate report_id: {', '.join(duplicate_ids)}")
        self._reports = ordered

    def list(self) -> tuple[RegisteredReport, ...]:
        """Return all reports ordered by stable full ID."""
        return self._reports

    def resolve(self, report_id: str) -> RegisteredReport:
        """Resolve an exact full ID or an unambiguous short report name."""
        exact = [report for report in self._reports if report.report_id == report_id]
        if exact:
            return exact[0]

        short = [report for report in self._reports if report.name == report_id]
        if len(short) == 1:
            return short[0]
        if len(short) > 1:
            raise UserError(
                "Report ID is ambiguous.",
                code="REPORT_ID_AMBIGUOUS",
                details={
                    "report_id": report_id,
                    "candidates": sorted(report.report_id for report in short),
                },
            )
        raise UserError(
            "Report not found.",
            code="REPORT_ID_NOT_FOUND",
            details={"report_id": report_id},
        )

    def execute(
        self,
        db: Database,
        *,
        report_id: str,
        parameters: Mapping[str, JsonValue],
        limit: int,
    ) -> CatalogReportResult:
        """Validate parameters, then dispatch through the selected report kind."""
        if limit < 0:
            raise UserError(
                "Report limit must be non-negative.",
                code="REPORT_LIMIT_INVALID",
                details={"minimum": 0},
            )
        spec = self.resolve(report_id)
        validated = _validate_parameters(spec, parameters)
        if isinstance(spec, ReportSpec):
            return run_report(spec, db, max_rows=limit, **validated)
        if spec.validator is not None:
            spec.validator(validated)
        return spec.executor(db, validated, limit)


def _parameter_specs(spec: RegisteredReport) -> tuple[ParamSpec, ...]:
    if isinstance(spec, ReportSpec):
        return spec.params
    return spec.parameters


def _validate_parameters(
    spec: RegisteredReport,
    supplied: Mapping[str, JsonValue],
) -> dict[str, JsonValue]:
    declared = _parameter_specs(spec)
    declared_by_name = {parameter.name: parameter for parameter in declared}

    unknown = sorted(set(supplied) - set(declared_by_name))
    if unknown:
        raise UserError(
            "Unknown report parameter.",
            code="REPORT_PARAMETER_UNKNOWN",
            details={"report_id": spec.report_id, "parameters": unknown},
        )

    missing = sorted(
        parameter.name
        for parameter in declared
        if parameter.required and parameter.name not in supplied
    )
    if missing:
        raise UserError(
            "Required report parameter is missing.",
            code="REPORT_PARAMETER_MISSING",
            details={"report_id": spec.report_id, "parameters": missing},
        )

    validated: dict[str, JsonValue] = {}
    for parameter in declared:
        value = (
            supplied[parameter.name]
            if parameter.name in supplied
            else parameter.default
        )
        if not _matches_annotation(value, parameter.annotation):
            raise UserError(
                "Report parameter has an invalid type.",
                code="REPORT_PARAMETER_INVALID_TYPE",
                details={
                    "report_id": spec.report_id,
                    "parameter": parameter.name,
                    "expected": _annotation_name(parameter.annotation),
                },
            )
        validated[parameter.name] = value
    return validated


def _matches_annotation(value: object, annotation: object) -> bool:
    """Strictly match JSON-native values against one introspected annotation."""
    if annotation is None or annotation is Any:
        return True
    if annotation is type(None):
        return value is None

    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (types.UnionType, typing.Union):
        return any(_matches_annotation(value, arg) for arg in args)
    if origin is Literal:
        return value in args and any(type(value) is type(arg) for arg in args)
    if origin is list:
        item_type = args[0] if args else Any
        return isinstance(value, list) and all(
            _matches_annotation(item, item_type) for item in cast(list[object], value)
        )
    if origin is dict:
        key_type, value_type = args if args else (Any, Any)
        return isinstance(value, dict) and all(
            _matches_annotation(key, key_type) and _matches_annotation(item, value_type)
            for key, item in cast(dict[object, object], value).items()
        )
    if annotation is bool:
        return type(value) is bool
    if annotation is int:
        return type(value) is int
    if annotation is float:
        return type(value) in (int, float)
    if annotation is str:
        return isinstance(value, str)
    if isinstance(annotation, type):
        return isinstance(value, annotation)
    return False


def _annotation_name(annotation: object) -> str:
    if annotation is None or annotation is Any:
        return "any"
    if annotation is type(None):
        return "None"
    if isinstance(annotation, type):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def get_report_catalog() -> ReportCatalog:
    """Build the current core, service, and explicitly registered extension union."""
    from moneybin.reports._framework.registry import (
        extension_report_specs,
        spec_of,
    )
    from moneybin.reports.definitions import ALL_REPORTS
    from moneybin.reports.service_reports import SERVICE_REPORTS

    core = (spec_of(runner) for runner in ALL_REPORTS)
    return ReportCatalog((*core, *SERVICE_REPORTS, *extension_report_specs()))


def catalog_to_payload(catalog: ReportCatalog) -> ReportCatalogPayload:
    """Expose the catalog's static, aggregate-only metadata."""
    return ReportCatalogPayload(
        reports=[_catalog_entry_to_payload(report) for report in catalog.list()]
    )


def result_to_payload(result: CatalogReportResult) -> ReportResultPayload:
    """Expose an already-redacted catalog result without touching executor inputs."""
    return ReportResultPayload(
        report_id=result.report_id,
        parameters={
            name: _thaw_parameter_metadata(value)
            for name, value in result.parameters.items()
        },
        columns=[
            ReportOutputColumn(
                name=name,
                data_class=result.output_classes[name].value,
            )
            for name in result.columns
        ],
        rows=result.records,
        semantics=_semantics_to_payload(result.semantics),
        period=result.period,
        sensitivity=tier_to_sensitivity(result.tier).value,
        count=result.total_count,
        truncated=result.truncated,
    )


def _catalog_entry_to_payload(report: RegisteredReport) -> ReportCatalogEntry:
    return ReportCatalogEntry(
        report_id=report.report_id,
        description=report.description,
        parameter_schema=_parameter_schema(report),
        parameter_classes={
            parameter.name: parameter.data_class.value
            for parameter in _parameter_specs(report)
        },
        examples=list(report.examples),
        columns=[
            ReportOutputColumn(
                name=column.name,
                description=column.description,
                data_class=column.data_class.value,
            )
            for column in report.columns
        ],
        output_classes={
            name: data_class.value for name, data_class in report.classes.items()
        },
        semantics=_semantics_to_payload(report.semantics),
    )


def _parameter_schema(report: RegisteredReport) -> dict[str, JsonValue]:
    """Build the strict object schema published for one report's parameters."""
    properties: dict[str, JsonValue] = {}
    required: list[JsonValue] = []
    for parameter in _parameter_specs(report):
        annotation = Any if parameter.annotation is None else parameter.annotation
        property_schema = TypeAdapter(annotation).json_schema()
        property_schema["description"] = parameter.help
        if parameter.required:
            required.append(parameter.name)
        else:
            property_schema["default"] = parameter.default
        properties[parameter.name] = property_schema

    schema: dict[str, JsonValue] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema


def _semantics_to_payload(semantics: ReportSemantics) -> ReportSemanticsPayload:
    return ReportSemanticsPayload(
        unit=semantics.unit,
        currency=semantics.currency,
        sign=semantics.sign,
        kind=semantics.kind,
        valuation_basis=semantics.valuation_basis,
        fx_basis=semantics.fx_basis,
        time_basis=semantics.time_basis,
        denominator=semantics.denominator,
        comparison_window=semantics.comparison_window,
        exclusions=semantics.exclusions,
        provenance=semantics.provenance,
    )


def _thaw_parameter_metadata(value: object) -> JsonValue:
    """Convert only frozen JSON containers from safe result metadata to JSON shapes."""
    if isinstance(value, Mapping):
        return {
            name: _thaw_parameter_metadata(item)
            for name, item in cast(Mapping[str, object], value).items()
        }
    if isinstance(value, tuple):
        return [
            _thaw_parameter_metadata(item) for item in cast(tuple[object, ...], value)
        ]
    return cast(JsonValue, value)
