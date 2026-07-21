"""Prepared export orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from typing import cast

from pydantic import JsonValue

from moneybin.database import Database
from moneybin.exports.models import RedactionMode, ReportExportReceipt
from moneybin.exports.redaction import apply_export_redaction
from moneybin.exports.snapshot import (
    ARTIFACT_VERSION,
    ExportSubject,
    PreparedColumn,
    PreparedExport,
    PreparedTable,
    ReportExportProvenance,
    build_bundle_snapshot,
    build_data_dictionary,
    prepared_table_checksum,
)
from moneybin.reports._framework.catalog import (
    ReportCatalog,
    get_report_catalog,
)
from moneybin.reports._framework.contract import ReportSpec
from moneybin.reports._framework.execute import redact_report_parameters
from moneybin.tables import TableRef


class ExportService:
    """Prepare format-neutral exports from trusted semantic sources."""

    def __init__(
        self,
        db: Database,
        *,
        report_catalog: ReportCatalog | None = None,
    ) -> None:
        """Bind the database used for canonical snapshot reads."""
        self._db = db
        self._report_catalog = report_catalog

    def prepare_bundle(
        self,
        *,
        profile: str,
        redaction_mode: RedactionMode = "redacted",
        report_id: str | None = None,
        report_parameters: Mapping[str, JsonValue] | None = None,
    ) -> PreparedExport:
        """Prepare the closed canonical bundle under one per-run output policy."""
        if report_id is not None:
            raise ValueError("bundle exports cannot include a report id")
        if report_parameters is not None:
            raise ValueError("bundle exports cannot include report parameters")
        snapshot = build_bundle_snapshot(
            self._db,
            profile=profile,
            created_at=datetime.now(UTC),
        )
        return apply_export_redaction(snapshot, redaction_mode)

    def prepare_report(
        self,
        *,
        profile: str,
        report_id: str,
        report_parameters: Mapping[str, JsonValue] | None = None,
        max_rows: int,
        redaction_mode: RedactionMode = "redacted",
    ) -> PreparedExport:
        """Prepare exactly one catalog report under one output policy."""
        catalog = self._report_catalog or get_report_catalog()
        spec, execution = catalog.execute_raw(
            self._db,
            report_id=report_id,
            parameters=report_parameters or {},
            limit=max_rows,
        )
        columns = tuple(
            PreparedColumn(
                name=name,
                duckdb_type=duckdb_type,
                data_class=execution.output_classes[name],
            )
            for name, duckdb_type in zip(
                execution.columns,
                execution.column_types,
                strict=True,
            )
        )
        rows = tuple(
            tuple(record[name] for name in execution.columns)
            for record in execution.records
        )
        source = (
            spec.view
            if isinstance(spec, ReportSpec)
            else _service_report_source(spec.name, execution.provenance)
        )
        table = PreparedTable(
            name=execution.report_id,
            source=source,
            columns=columns,
            rows=rows,
            checksum_sha256=prepared_table_checksum(columns, rows),
        )
        parameters = spec.params if isinstance(spec, ReportSpec) else spec.parameters
        parameter_classes = {
            parameter.name: parameter.data_class.value for parameter in parameters
        }
        snapshot_parameters: Mapping[str, object]
        if redaction_mode == "redacted":
            snapshot_parameters = redact_report_parameters(
                spec,
                execution.parameters,
            )
        else:
            snapshot_parameters = execution.parameters
        receipt = ReportExportReceipt(
            report_id=execution.report_id,
            parameters=snapshot_parameters,
            parameter_classes=parameter_classes,
            sql=execution.sql,
            lineage=execution.provenance,
            output_classes={
                name: data_class.value
                for name, data_class in execution.output_classes.items()
            },
            # The current ReportSpec exposes neither field. Keep that absence
            # explicit instead of inferring verification state from provenance.
            freshness=None,
            graduation_eligibility=None,
            semantics=cast(dict[str, object], asdict(execution.semantics)),
        )
        tables = (table,)
        snapshot = PreparedExport(
            artifact_version=ARTIFACT_VERSION,
            profile=profile,
            created_at=datetime.now(UTC),
            subject=ExportSubject(
                kind="report",
                report_id=execution.report_id,
                parameters=snapshot_parameters,
            ),
            redaction_mode="unredacted",
            tables=tables,
            data_dictionary=build_data_dictionary(tables),
            provenance=ReportExportProvenance(
                report_id=execution.report_id,
                receipt=receipt.as_mapping(),
            ),
        )
        return apply_export_redaction(snapshot, redaction_mode)


def _service_report_source(name: str, provenance: tuple[str, ...]) -> TableRef:
    """Return the service report's declared report-level provenance source."""
    if provenance:
        parts = provenance[0].split(".", maxsplit=1)
        if len(parts) == 2:
            return TableRef(parts[0], parts[1])
    return TableRef("reports", name)
