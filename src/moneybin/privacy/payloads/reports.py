"""Typed payloads for the internal report catalog and runner."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from moneybin.privacy.taxonomy import DataClass


class ReportSemanticsPayload(BaseModel):
    """Financial interpretation metadata repeated by catalog and result."""

    unit: Annotated[str, DataClass.AGGREGATE]
    currency: Annotated[str | None, DataClass.AGGREGATE]
    sign: Annotated[str, DataClass.AGGREGATE]
    kind: Annotated[Literal["position", "flow", "ratio", "count"], DataClass.AGGREGATE]
    valuation_basis: Annotated[str | None, DataClass.AGGREGATE]
    fx_basis: Annotated[str | None, DataClass.AGGREGATE]
    time_basis: Annotated[str, DataClass.AGGREGATE]
    denominator: Annotated[str | None, DataClass.AGGREGATE]
    comparison_window: Annotated[str | None, DataClass.AGGREGATE]
    exclusions: Annotated[tuple[str, ...], DataClass.AGGREGATE]
    provenance: Annotated[tuple[str, ...], DataClass.AGGREGATE]


class ReportOutputColumn(BaseModel):
    """One ordered report-output field and its declared privacy class."""

    name: Annotated[str, DataClass.AGGREGATE]
    description: Annotated[str | None, DataClass.AGGREGATE] = None
    data_class: Annotated[str, DataClass.AGGREGATE]


class ReportCatalogEntry(BaseModel):
    """Complete static metadata for one registered report."""

    report_id: Annotated[str, DataClass.AGGREGATE]
    description: Annotated[str, DataClass.AGGREGATE]
    parameter_schema: Annotated[dict[str, JsonValue], DataClass.AGGREGATE]
    parameter_classes: Annotated[dict[str, str], DataClass.AGGREGATE]
    examples: Annotated[list[str], DataClass.AGGREGATE]
    columns: Annotated[list[ReportOutputColumn], DataClass.AGGREGATE]
    output_classes: Annotated[dict[str, str], DataClass.AGGREGATE]
    semantics: Annotated[ReportSemanticsPayload, DataClass.AGGREGATE]


class ReportCatalogPayload(BaseModel):
    """The aggregate-only listing of every registered report."""

    kind: Literal["catalog"] = "catalog"
    reports: Annotated[list[ReportCatalogEntry], DataClass.AGGREGATE]


class ReportResultPayload(BaseModel):
    """One redacted report result with its actual runtime classification."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    kind: Literal["result"] = "result"
    report_id: Annotated[str, DataClass.AGGREGATE]
    parameters: Annotated[dict[str, JsonValue], DataClass.AGGREGATE]
    columns: Annotated[list[ReportOutputColumn], DataClass.AGGREGATE]
    # Rows are already redacted by the report executor. Their classes vary by
    # report and are carried by ``columns`` plus the actual ``sensitivity``;
    # marking a dynamic row as AGGREGATE would incorrectly downgrade it.
    rows: list[dict[str, Any]]
    semantics: Annotated[ReportSemanticsPayload, DataClass.AGGREGATE]
    period: Annotated[str | None, DataClass.AGGREGATE]
    sensitivity: Annotated[str, DataClass.AGGREGATE]
    count: Annotated[int, DataClass.AGGREGATE]
    truncated: Annotated[bool, DataClass.AGGREGATE]


ReportsPayload = Annotated[
    ReportCatalogPayload | ReportResultPayload,
    Field(discriminator="kind"),
]
