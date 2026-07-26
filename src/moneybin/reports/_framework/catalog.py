"""One internal catalog for SQL-backed and service-backed reports."""

from __future__ import annotations

import logging
import re
import types
import typing
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, cast, get_args, get_origin

from pydantic import JsonValue, TypeAdapter

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.metrics.registry import USER_REPORT_RUNS_TOTAL
from moneybin.privacy.payloads.reports import (
    ReportCatalogEntry,
    ReportCatalogPayload,
    ReportOutputColumn,
    ReportResultPayload,
    ReportSemanticsPayload,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    USER_NAMESPACE,
    OutputColumn,
    ParamSpec,
    ReportSemantics,
    ReportSpec,
)
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    CatalogReportResult,
    execute_catalog_report,
    redact_catalog_execution,
)

logger = logging.getLogger(__name__)

_REPORT_ID = re.compile(r"[a-z][a-z0-9_-]*:[a-z][a-z0-9_-]*")

type ReportTier = Literal["builtin", "extension", "user"]


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
    executor: Callable[
        [Database, Mapping[str, JsonValue], int | None], CatalogReportExecution
    ]
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
        self._name_collisions = _name_collisions(ordered)
        for name, report_ids in self._name_collisions.items():
            logger.warning(
                f"report name {name!r} is claimed by {len(report_ids)} reports "
                f"({', '.join(report_ids)}); each stays runnable by report_id."
            )

    def list(self) -> tuple[RegisteredReport, ...]:
        """Return all reports ordered by stable full ID."""
        return self._reports

    def name_collisions(self) -> Mapping[str, tuple[str, ...]]:
        """Names claimed by more than one report, mapped to the claiming IDs.

        R5's mutation-time checks cannot cover every collision: upgrading
        MoneyBin can add a built-in whose name a user already took, and neither
        path calls a lifecycle mutation. So the assembled registry is validated
        here rather than trusted, and a collision is surfaced instead of
        silently resolved — shadowing the user's report hides their work behind
        an upgrade they did not ask for, and shadowing the built-in makes a
        shipped report vanish with no visible cause. Both stay resolvable by
        ``report_id``; only the contested *name* stops resolving.
        """
        return self._name_collisions

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
                code=error_codes.REPORT_ID_AMBIGUOUS,
                details={
                    "report_id": report_id,
                    "candidates": sorted(report.report_id for report in short),
                },
            )
        raise UserError(
            "Report not found.",
            code=error_codes.REPORT_ID_NOT_FOUND,
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
        spec, execution = self.execute_raw(
            db,
            report_id=report_id,
            parameters=parameters,
            limit=limit,
        )
        return redact_catalog_execution(spec, execution)

    def execute_raw(
        self,
        db: Database,
        *,
        report_id: str,
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> tuple[RegisteredReport, CatalogReportExecution]:
        """Validate and execute one report without terminal redaction."""
        spec, validated = self.resolve_request(
            report_id=report_id,
            parameters=parameters,
            limit=limit,
        )
        tier = report_tier(spec)
        try:
            if isinstance(spec, ReportSpec):
                execution = execute_catalog_report(
                    spec,
                    db,
                    max_rows=limit,
                    **validated,
                )
            else:
                execution = spec.executor(db, validated, limit)
        except Exception:
            USER_REPORT_RUNS_TOTAL.labels(tier=tier, outcome="error").inc()
            raise
        USER_REPORT_RUNS_TOTAL.labels(tier=tier, outcome="ok").inc()
        return spec, execution

    def resolve_request(
        self,
        *,
        report_id: str,
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> tuple[RegisteredReport, dict[str, JsonValue]]:
        """Resolve and validate one request without executing its report."""
        if limit is not None and limit < 0:
            raise UserError(
                "Report limit must be non-negative.",
                code=error_codes.REPORT_LIMIT_INVALID,
                details={"minimum": 0},
            )
        spec = self.resolve(report_id)
        validated = _validate_parameters(spec, parameters)
        if isinstance(spec, ServiceReportSpec) and spec.validator is not None:
            spec.validator(validated)
        return spec, validated


def _name_collisions(
    reports: Sequence[RegisteredReport],
) -> Mapping[str, tuple[str, ...]]:
    """Group report IDs by any name more than one of them claims."""
    by_name: dict[str, list[str]] = defaultdict(list)
    for report in reports:
        by_name[report.name].append(report.report_id)
    return MappingProxyType({
        name: tuple(report_ids)
        for name, report_ids in sorted(by_name.items())
        if len(report_ids) > 1
    })


def report_tier(report: RegisteredReport) -> ReportTier:
    """Which of R5's three tiers ``report`` belongs to.

    Keyed on the ``report_id`` namespace for the user tier and on the extension
    registry for the rest, because a spec carries no tier field — and adding one
    would let a spec claim a tier its provenance contradicts.
    """
    from moneybin.reports._framework.registry import extension_report_specs

    if report.report_id.startswith(f"{USER_NAMESPACE}:"):
        return "user"
    if any(
        extension.report_id == report.report_id
        for extension in extension_report_specs()
    ):
        return "extension"
    return "builtin"


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
            code=error_codes.REPORT_PARAMETER_UNKNOWN,
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
            code=error_codes.REPORT_PARAMETER_MISSING,
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
                code=error_codes.REPORT_PARAMETER_INVALID_TYPE,
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


def get_report_catalog(
    db: Database | None = None, *, include_archived: bool = False
) -> ReportCatalog:
    """Build the union of every registered report across R5's three tiers.

    ``db`` adds the user tier — one ``ReportSpec`` per ``app.user_reports`` row.
    Pass it on any path that resolves a **caller-supplied** report reference, so
    a saved report is reachable by the same call a built-in is. Omitting it
    yields the packaged tiers alone, which is what extension registration needs
    (it runs before any database is open) and what a built-in's own generated
    CLI command needs (it resolves one fixed ID it already holds).
    """
    from moneybin.reports._framework.dynamic import user_report_specs
    from moneybin.reports._framework.registry import (
        extension_report_specs,
        spec_of,
    )
    from moneybin.reports.definitions import ALL_REPORTS
    from moneybin.reports.service_reports import SERVICE_REPORTS

    core = (spec_of(runner) for runner in ALL_REPORTS)
    user = (
        user_report_specs(db, include_archived=include_archived)
        if db is not None
        else ()
    )
    return ReportCatalog((*core, *SERVICE_REPORTS, *extension_report_specs(), *user))


def catalog_to_payload(catalog: ReportCatalog) -> ReportCatalogPayload:
    """Expose the catalog's static metadata."""
    return ReportCatalogPayload(
        reports=[_catalog_entry_to_payload(report) for report in catalog.list()]
    )


def catalog_sensitivity(catalog: ReportCatalog) -> Literal["low", "medium"]:
    """The envelope sensitivity a listing of ``catalog`` actually carries.

    A built-in's name and description are authored in the repo and reviewed, so
    the entry fields are annotated ``AGGREGATE``. A **user** report's name and
    description are user-authored free text — ``USER_NOTE``, MEDIUM, the same
    class the stored columns carry — so a listing that includes one is not a LOW
    response. The annotations stay AGGREGATE deliberately (masking a user's own
    report name would make the catalog unusable); what has to be honest is the
    tier the envelope reports.
    """
    if any(report_tier(report) == "user" for report in catalog.list()):
        return "medium"
    return "low"


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
        tier=report_tier(report),
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
