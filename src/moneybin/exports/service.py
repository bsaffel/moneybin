"""Prepared export orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from time import perf_counter
from typing import TYPE_CHECKING, Protocol, cast

from pydantic import JsonValue

from moneybin.database import Database
from moneybin.exports.models import (
    DestinationKind,
    ExportDestination,
    ExportReceipt,
    ExportRequest,
    RedactionMode,
    ReportExportReceipt,
)
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
from moneybin.metrics.registry import EXPORT_DURATION_SECONDS, EXPORT_RUNS_TOTAL
from moneybin.reports._framework.catalog import (
    ReportCatalog,
    get_report_catalog,
)
from moneybin.reports._framework.contract import ReportSpec
from moneybin.reports._framework.execute import redact_report_parameters
from moneybin.tables import TableRef

if TYPE_CHECKING:
    from pathlib import Path

    from moneybin.exports.manifest import LocalExportFormat
    from moneybin.exports.sheets import SheetsAuthorization
    from moneybin.services.audit_service import AuditEvent


class _SheetsPublisher(Protocol):
    """Small publication boundary injected by Sheets-facing adapters and tests."""

    def publish(
        self,
        snapshot: PreparedExport,
        destination: ExportDestination,
    ) -> ExportReceipt:
        """Publish one prepared snapshot."""
        ...


class _SheetsReadiness(Protocol):
    """Read-only OAuth capability needed by export readiness status."""

    def is_authorized(self, *, require_write: bool = False) -> bool:
        """Return whether the persisted grant permits the requested capability."""
        ...


@dataclass(frozen=True, slots=True)
class ExportDestinationReadiness:
    """Privacy-safe readiness for one derived or stored export destination."""

    name: str
    kind: DestinationKind
    ready: bool
    write_capable: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExportReadinessStatus:
    """Shared CLI/MCP projection of configured export destinations."""

    destinations: tuple[ExportDestinationReadiness, ...]


_SUBJECT_KINDS = frozenset({"bundle", "report"})
_FORMATS = frozenset({"csv", "parquet", "xlsx", "sheets"})
_DESTINATION_KINDS = frozenset({"local", "sheets"})
_REDACTION_MODES = frozenset({"redacted", "unredacted"})


class ExportService:
    """Prepare format-neutral exports from trusted semantic sources."""

    def __init__(
        self,
        db: Database,
        *,
        report_catalog: ReportCatalog | None = None,
        sheets_publisher: _SheetsPublisher | None = None,
    ) -> None:
        """Bind the database used for canonical snapshot reads."""
        self._db = db
        self._report_catalog = report_catalog
        self._sheets_publisher_override = sheets_publisher

    def run(self, request: ExportRequest, *, actor: str) -> ExportReceipt:
        """Validate, prepare, and publish exactly one export snapshot."""
        _ = actor
        labels = {
            "subject_kind": _bounded_label(request.subject_kind, _SUBJECT_KINDS),
            "format": _bounded_label(request.format, _FORMATS),
            "destination_kind": _bounded_label(
                request.destination.kind, _DESTINATION_KINDS
            ),
            "redaction_mode": _bounded_label(request.redaction_mode, _REDACTION_MODES),
        }
        started_at = perf_counter()
        try:
            self._validate_request(request)
            from moneybin.config import get_settings  # noqa: PLC0415

            settings = get_settings()
            if request.subject_kind == "bundle":
                snapshot = self.prepare_bundle(
                    profile=settings.profile,
                    redaction_mode=request.redaction_mode,
                )
            else:
                report_id = cast(str, request.report_id)
                snapshot = self.prepare_report(
                    profile=settings.profile,
                    report_id=report_id,
                    report_parameters=request.report_parameters,
                    max_rows=settings.mcp.max_rows,
                    redaction_mode=request.redaction_mode,
                )

            if request.destination.kind == "local":
                from moneybin.exports.local import (  # noqa: PLC0415
                    LocalExportPublisher,
                )

                publisher = LocalExportPublisher(
                    cast("Path", request.destination.local_path),
                    destination_name=request.destination.name,
                )
                receipt = publisher.publish(
                    snapshot,
                    format=cast("LocalExportFormat", request.format),
                    compress_zip=request.compress_zip,
                )
                receipt = replace(receipt, destination=request.destination)
            else:
                receipt = self._sheets_publisher().publish(
                    snapshot,
                    request.destination,
                )
        except Exception:
            EXPORT_RUNS_TOTAL.labels(**labels, outcome="failed").inc()
            raise
        else:
            EXPORT_RUNS_TOTAL.labels(**labels, outcome="success").inc()
            return receipt
        finally:
            EXPORT_DURATION_SECONDS.labels(**labels).observe(
                perf_counter() - started_at
            )

    @staticmethod
    def _validate_request(request: ExportRequest) -> None:
        """Reject impossible typed-contract combinations before preparation."""
        if request.subject_kind not in _SUBJECT_KINDS:
            raise ValueError("Unsupported export subject kind")
        if request.format not in _FORMATS:
            raise ValueError("Unsupported export format")
        if request.destination.kind not in _DESTINATION_KINDS:
            raise ValueError("Unsupported export destination kind")
        if request.redaction_mode not in _REDACTION_MODES:
            raise ValueError("Unsupported export redaction mode")
        if request.subject_kind == "bundle":
            if request.report_id is not None:
                raise ValueError("bundle exports cannot include a report id")
            if request.report_parameters:
                raise ValueError("bundle exports cannot include report parameters")
        elif not request.report_id:
            raise ValueError("report exports require a report id")

        destination = request.destination
        if destination.kind == "local":
            if (
                destination.local_path is None
                or destination.spreadsheet_id is not None
                or destination.managed_tab_prefix is not None
            ):
                raise ValueError("Invalid local export destination")
            if request.format == "sheets":
                raise ValueError("Local destinations do not support Sheets format")
        else:
            if (
                destination.local_path is not None
                or destination.spreadsheet_id is None
                or destination.managed_tab_prefix is None
            ):
                raise ValueError("Invalid Sheets export destination")
            if request.format != "sheets":
                raise ValueError("Sheets destinations use the native Sheets format")
            if request.compress_zip:
                raise ValueError("Sheets exports do not support compression")
        if request.format == "xlsx" and request.compress_zip:
            raise ValueError("XLSX is already compressed and rejects ZIP compression")

    def _sheets_publisher(self) -> _SheetsPublisher:
        if self._sheets_publisher_override is not None:
            return self._sheets_publisher_override
        from moneybin.connectors.gsheet.service_factory import (  # noqa: PLC0415
            build_oauth_client,
        )
        from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
        from moneybin.exports.sheets import SheetsExportPublisher  # noqa: PLC0415

        return SheetsExportPublisher(
            db=self._db,
            sheets_client=SheetsClient(oauth=build_oauth_client()),
        )

    def status(
        self,
        *,
        sheets_authorization: _SheetsReadiness | None = None,
    ) -> ExportReadinessStatus:
        """Return destination readiness without target identities or locations."""
        from moneybin.exports.sheets import validate_managed_tab_prefix  # noqa: PLC0415
        from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
            ExportDestinationSpreadsheetConflictError,
            ExportDestinationsRepo,
        )

        repo = ExportDestinationsRepo(self._db)
        stored = repo.list()
        sheets_write_capable = False
        if any(destination.kind == "sheets" for destination in stored):
            if sheets_authorization is None:
                from moneybin.connectors.gsheet.service_factory import (  # noqa: PLC0415
                    build_oauth_client,
                )

                sheets_authorization = cast(
                    "_SheetsReadiness",
                    build_oauth_client(),
                )
            sheets_write_capable = sheets_authorization.is_authorized(
                require_write=True
            )

        results = [
            ExportDestinationReadiness(
                name="local:exports",
                kind="local",
                ready=True,
                write_capable=True,
                reasons=(),
            )
        ]
        for destination in stored:
            reasons: list[str] = []
            if not destination.name.strip():
                reasons.append("invalid_destination_name")
            if destination.kind == "local":
                if (
                    destination.local_path is None
                    or destination.spreadsheet_id is not None
                    or destination.managed_tab_prefix is not None
                ):
                    reasons.append("invalid_destination_configuration")
                write_capable = not reasons
            else:
                if (
                    destination.local_path is not None
                    or not destination.spreadsheet_id
                    or not destination.managed_tab_prefix
                ):
                    reasons.append("invalid_destination_configuration")
                else:
                    try:
                        validate_managed_tab_prefix(destination.managed_tab_prefix)
                    except ValueError:
                        reasons.append("invalid_managed_tab_prefix")
                if destination.spreadsheet_id:
                    try:
                        repo.assert_not_inbound_connection(destination.spreadsheet_id)
                    except ExportDestinationSpreadsheetConflictError:
                        reasons.append("inbound_connection_collision")
                if not sheets_write_capable:
                    reasons.append("sheets_write_authorization_required")
                write_capable = sheets_write_capable
            results.append(
                ExportDestinationReadiness(
                    name=destination.name,
                    kind=destination.kind,
                    ready=not reasons,
                    write_capable=write_capable,
                    reasons=tuple(reasons),
                )
            )
        return ExportReadinessStatus(destinations=tuple(results))

    def set_sheets_destination(
        self,
        *,
        name: str,
        spreadsheet_id: str,
        managed_tab_prefix: str,
        actor: str,
        oauth_client: SheetsAuthorization,
    ) -> AuditEvent:
        """Validate, authorize, and persist one Sheets output destination."""
        from moneybin.connectors.gsheet.errors import GSheetAuthError  # noqa: PLC0415
        from moneybin.exports.sheets import validate_managed_tab_prefix  # noqa: PLC0415
        from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
            ExportDestinationsRepo,
        )

        prefix = validate_managed_tab_prefix(managed_tab_prefix)
        repo = ExportDestinationsRepo(self._db)
        repo.assert_not_inbound_connection(spreadsheet_id)
        grant = oauth_client.authorize(require_write=True)
        if not grant.can_write:
            raise GSheetAuthError("Google Sheets write authorization was not granted")
        return repo.set_sheets(
            name=name,
            spreadsheet_id=spreadsheet_id,
            managed_tab_prefix=prefix,
            actor=actor,
        )

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


def _bounded_label(value: object, allowed: frozenset[str]) -> str:
    """Map malformed runtime values to one fixed low-cardinality label."""
    return value if isinstance(value, str) and value in allowed else "invalid"
