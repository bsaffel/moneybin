"""Run a report: execute the runner's query, classify, redact, summarize.

The generic execution path shared by the generated MCP tool and CLI command.
It mirrors ``execute_sql_query`` — same ``redact_records`` /
``derive_query_tier`` bottleneck — but the SQL comes from a report runner and
the per-column classes come from the report's view (see ``classify``) rather
than live lineage on a user query.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol, cast

from pydantic import JsonValue

from moneybin.database import Database
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.redaction import redact_records
from moneybin.privacy.sql_lineage import derive_query_tier
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.reports._framework.classify import classify_columns
from moneybin.reports._framework.contract import ParamSpec, ReportSemantics, ReportSpec

type FrozenJsonValue = (
    None
    | bool
    | int
    | float
    | str
    | tuple["FrozenJsonValue", ...]
    | Mapping[str, "FrozenJsonValue"]
)


@dataclass(frozen=True)
class ReportResult:
    """Redacted rows plus the envelope-relevant metadata for one report call.

    Mirrors the envelope-facing fields of ``SqlQueryResult`` so the MCP and CLI
    registrars build identical envelopes to the SQL surface.
    """

    records: list[dict[str, Any]]
    columns: list[str]
    output_classes: dict[str, DataClass]
    tier: Tier
    total_count: int
    truncated: bool
    actions: list[str] = field(default_factory=list)
    period: str | None = None
    display_currency: str = "USD"

    @property
    def classes_returned(self) -> list[str]:
        """Sorted data-class values for the envelope/audit."""
        if not self.output_classes:
            return ["aggregate"]
        return sorted({c.value for c in self.output_classes.values()})

    def to_envelope(self) -> ResponseEnvelope[Any]:
        """Build the standard response envelope from this result.

        The ReportResult→envelope mapping is identical for both surfaces (only
        what each does with the envelope differs), so it lives here next to the
        fields it reads.
        """
        return build_envelope(
            data=self.records,
            sensitivity=tier_to_sensitivity(self.tier).value,
            total_count=self.total_count,
            classes_returned=self.classes_returned,
            actions=self.actions or None,
            period=self.period,
            display_currency=self.display_currency,
        )


@dataclass(frozen=True, kw_only=True)
class CatalogReportResult(ReportResult):
    """A report result tagged with its catalog identity and financial meaning."""

    report_id: str
    parameters: Mapping[str, FrozenJsonValue]
    semantics: ReportSemantics
    provenance: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class CatalogReportExecution:
    """One raw catalog-runner execution before terminal redaction."""

    report_id: str
    parameters: Mapping[str, JsonValue]
    sql: str
    records: list[dict[str, Any]]
    columns: list[str]
    column_types: list[str]
    output_classes: dict[str, DataClass]
    tier: Tier
    total_count: int
    truncated: bool
    actions: list[str]
    period: str | None
    semantics: ReportSemantics
    provenance: tuple[str, ...]


class _CatalogSpec(Protocol):
    """The result-building fields shared by SQL and service report specs."""

    @property
    def report_id(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def classes(self) -> Mapping[str, DataClass]: ...

    @property
    def semantics(self) -> ReportSemantics: ...

    @property
    def parameters(self) -> tuple[ParamSpec, ...]: ...


def _redact_and_freeze_parameter(
    value: JsonValue,
    data_class: DataClass,
) -> FrozenJsonValue:
    """Redact parameter leaves and recursively freeze JSON containers."""
    if isinstance(value, dict):
        if data_class.tier >= Tier.MEDIUM:
            return MappingProxyType({
                "entry_count": len(value),
                "redacted": True,
            })
        return MappingProxyType({
            key: _redact_and_freeze_parameter(item, data_class)
            for key, item in value.items()
        })
    if isinstance(value, list):
        return tuple(_redact_and_freeze_parameter(item, data_class) for item in value)
    redacted = redact_records(
        [{"value": value}],
        {"value": data_class},
        consent=None,
    )[0]["value"]
    return cast(FrozenJsonValue, redacted)


def redact_report_parameters(
    spec: _CatalogSpec,
    parameters: Mapping[str, JsonValue],
) -> Mapping[str, FrozenJsonValue]:
    """Build immutable, redacted effective-parameter metadata."""
    classes = {parameter.name: parameter.data_class for parameter in spec.parameters}
    return MappingProxyType({
        name: _redact_and_freeze_parameter(value, classes[name])
        for name, value in parameters.items()
    })


def build_catalog_result(
    spec: _CatalogSpec,
    *,
    parameters: Mapping[str, JsonValue],
    records: list[dict[str, Any]],
    columns: list[str],
    max_rows: int,
    actions: list[str] | None = None,
    period: str | None = None,
) -> CatalogReportResult:
    """Redact and truncate tabular rows using the shared report rules."""
    execution = _build_catalog_execution(
        spec,
        parameters=parameters,
        sql="",
        records=records,
        columns=columns,
        column_types=[],
        max_rows=max_rows,
        actions=actions,
        period=period,
    )
    return _redact_catalog_execution(spec, execution)


def _build_catalog_execution(
    spec: _CatalogSpec,
    *,
    parameters: Mapping[str, JsonValue],
    sql: str,
    records: list[dict[str, Any]],
    columns: list[str],
    column_types: list[str],
    max_rows: int,
    actions: list[str] | None = None,
    period: str | None = None,
) -> CatalogReportExecution:
    """Build one raw, classified execution from already-fetched rows."""
    truncated = len(records) > max_rows
    limited = records[:max_rows]

    # ServiceReportSpec intentionally matches the classification-facing subset
    # of ReportSpec. The cast keeps classify_columns' existing public signature
    # stable while both kinds use its fail-closed undeclared-column behavior.
    col_classes = classify_columns(cast(ReportSpec, spec), columns)
    return CatalogReportExecution(
        report_id=spec.report_id,
        parameters=MappingProxyType(dict(parameters)),
        sql=sql,
        records=limited,
        columns=columns,
        column_types=column_types,
        output_classes=col_classes,
        tier=derive_query_tier(col_classes),
        total_count=max_rows + 1 if truncated else len(limited),
        truncated=truncated,
        actions=actions or [],
        period=period,
        semantics=spec.semantics,
        provenance=spec.semantics.provenance,
    )


def _redact_catalog_execution(
    spec: _CatalogSpec,
    execution: CatalogReportExecution,
) -> CatalogReportResult:
    """Apply the existing terminal report redaction to a raw execution."""
    redacted = redact_records(
        execution.records,
        execution.output_classes,
        consent=None,
    )

    return CatalogReportResult(
        report_id=execution.report_id,
        parameters=redact_report_parameters(spec, execution.parameters),
        semantics=execution.semantics,
        provenance=execution.provenance,
        records=redacted,
        columns=execution.columns,
        output_classes=execution.output_classes,
        tier=execution.tier,
        total_count=execution.total_count,
        truncated=execution.truncated,
        actions=execution.actions,
        period=execution.period,
    )


def execute_catalog_report(
    spec: ReportSpec, db: Database, *, max_rows: int, **params: Any
) -> CatalogReportExecution:
    """Execute a catalog runner once; do not apply terminal redaction."""
    rq = spec.runner(db, **params)
    cursor = db.execute(rq.sql, list(rq.params))
    descriptions = cursor.description or []
    columns = [str(description[0]) for description in descriptions]
    column_types = [
        str(description[1]) if len(description) > 1 else "UNKNOWN"
        for description in descriptions
    ]
    rows = cursor.fetchmany(max_rows + 1)
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return _build_catalog_execution(
        spec,
        parameters=cast(Mapping[str, JsonValue], params),
        sql=rq.sql,
        records=records,
        columns=columns,
        column_types=column_types,
        actions=list(rq.actions),
        period=rq.period,
        max_rows=max_rows,
    )


def run_report(
    spec: ReportSpec, db: Database, *, max_rows: int, **params: Any
) -> CatalogReportResult:
    """Execute ``spec``'s runner with ``params`` and return redacted results.

    Fetches one extra row to detect truncation, classifies each output column
    via the report's view, and masks CRITICAL columns before returning.
    """
    execution = execute_catalog_report(spec, db, max_rows=max_rows, **params)
    return _redact_catalog_execution(spec, execution)
