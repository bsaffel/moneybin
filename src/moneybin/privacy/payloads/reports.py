"""Typed payloads for the internal report catalog and runner."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from moneybin.privacy.taxonomy import DataClass


class ReportSemanticsPayload(BaseModel):
    """Financial interpretation metadata repeated by catalog and result.

    Mirrors ``ReportSemantics`` field-for-field, including its
    ``"unknown"`` / ``None`` arms for user-created reports. Keep the two in
    lockstep: this is a Pydantic model, so ``test_annotated_registry_sync``
    (which gates on ``is_dataclass``) does not cover it — the only guard against
    divergence is ``_semantics_to_payload`` failing to type-check.
    """

    unit: Annotated[str | None, DataClass.AGGREGATE]
    currency: Annotated[str | None, DataClass.AGGREGATE]
    sign: Annotated[str | None, DataClass.AGGREGATE]
    kind: Annotated[
        Literal["position", "flow", "ratio", "count", "unknown"], DataClass.AGGREGATE
    ]
    valuation_basis: Annotated[str | None, DataClass.AGGREGATE]
    fx_basis: Annotated[str | None, DataClass.AGGREGATE]
    time_basis: Annotated[str | None, DataClass.AGGREGATE]
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
    name: Annotated[str, DataClass.AGGREGATE]
    """The handle every operation accepts, and the one a human can retype.

    ``report_id`` is namespaced and, for the user tier, minted — ``user:r`` plus
    twelve hex characters nobody chose. ``name`` is what the CLI command is
    called and what ``resolve()`` matches, so a listing without it publishes an
    identity its reader cannot act on from memory. ``AGGREGATE`` like
    ``description``, for the reason ``catalog_sensitivity`` gives: a user's own
    report name masked to ``'*****'`` would make the catalog unusable, so the
    envelope's *tier* carries the honesty instead of the annotation."""
    tier: Annotated[Literal["builtin", "extension", "user"], DataClass.AGGREGATE]
    """Which registry tier served this entry. A user-created report is a row in
    one local database rather than a reviewed, installable artifact, so a caller
    reading the catalog is owed that distinction without having to parse an ID."""
    description: Annotated[str, DataClass.AGGREGATE]
    archived: Annotated[bool, DataClass.AGGREGATE] = False
    """Whether the user has hidden this report from the default listing.

    Always ``False`` for the builtin and extension tiers, which have no archived
    state. It rides on the shared entry rather than on a user-tier-only payload
    because a widened listing must be able to mark the rows it widened to
    include — a caller cannot otherwise tell a report it may run from one the
    user had put away."""
    parameter_schema: Annotated[dict[str, JsonValue], DataClass.AGGREGATE]
    parameter_classes: Annotated[dict[str, str], DataClass.AGGREGATE]
    examples: Annotated[list[str], DataClass.AGGREGATE]
    columns: Annotated[list[ReportOutputColumn], DataClass.AGGREGATE]
    output_classes: Annotated[dict[str, str], DataClass.AGGREGATE]
    semantics: Annotated[ReportSemanticsPayload, DataClass.AGGREGATE]

    @model_validator(mode="after")
    def validate_output_columns(self) -> Self:
        """Require one exact privacy-class declaration per output column."""
        declared = {column.name: column.data_class for column in self.columns}
        if len(declared) != len(self.columns):
            raise ValueError("duplicate output column")
        if declared != self.output_classes:
            raise ValueError(
                "columns and output_classes must declare the same output fields "
                "with identical privacy classes"
            )
        return self


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
