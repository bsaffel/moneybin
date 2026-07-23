"""Prepared export orchestration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from time import perf_counter
from typing import TYPE_CHECKING, Protocol, cast

from pydantic import JsonValue

from moneybin.database import Database
from moneybin.exports.models import (
    DestinationKind,
    ExportCommand,
    ExportDestination,
    ExportReceipt,
    ExportRequest,
    RedactionMode,
    ReportExportReceipt,
    normalize_export_destination_name,
    validate_export_destination_name,
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
from moneybin.services.request_lifetime import (
    RequestLifetime,
    current_request_lifetime,
)
from moneybin.tables import TableRef

if TYPE_CHECKING:
    from pathlib import Path

    from moneybin.exports.manifest import LocalExportFormat
    from moneybin.exports.sheets import SheetsAuthorization
    from moneybin.exports.workbook_roles import WorkbookRolePermit
    from moneybin.services.audit_service import AuditEvent


class _SheetsPublisher(Protocol):
    """Small publication boundary injected by Sheets-facing adapters and tests."""

    def publish(
        self,
        snapshot: PreparedExport,
        destination: ExportDestination,
        *,
        role_permit: WorkbookRolePermit,
        publication_lifetime: RequestLifetime | None,
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
    ) -> None:
        """Bind the database used for canonical snapshot reads."""
        self._db = db
        self._report_catalog = report_catalog

    @classmethod
    def run(
        cls,
        command: ExportCommand,
        *,
        actor: str,
        report_catalog: ReportCatalog | None = None,
        sheets_publisher: _SheetsPublisher | None = None,
        publication_lifetime: RequestLifetime | None = None,
        on_destination_resolved: Callable[[ExportDestination], None] | None = None,
    ) -> ExportReceipt:
        """Snapshot under a read lease, then publish after releasing DuckDB."""
        _ = actor
        requested_destination_kind = command.destination_reference.partition(":")[0]
        labels = {
            "subject_kind": _bounded_label(command.subject_kind, _SUBJECT_KINDS),
            "format": _bounded_label(command.format, _FORMATS),
            "destination_kind": _bounded_label(
                requested_destination_kind, _DESTINATION_KINDS
            ),
            "redaction_mode": _bounded_label(command.redaction_mode, _REDACTION_MODES),
        }
        started_at = perf_counter()
        try:
            from moneybin.config import get_settings  # noqa: PLC0415
            from moneybin.database import get_database  # noqa: PLC0415
            from moneybin.exports.workbook_roles import (  # noqa: PLC0415
                workbook_role_lease,
            )
            from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
                ExportDestinationsRepo,
            )

            settings = get_settings()
            lifetime = publication_lifetime or current_request_lifetime()
            with ExitStack() as role_stack:
                role_permit: WorkbookRolePermit | None = None
                with get_database(read_only=True) as db:
                    service = cls(db, report_catalog=report_catalog)
                    destination = service.resolve_destination(
                        command.destination_reference
                    )
                    request = command.resolve(destination)
                    service._validate_request(request)
                    if on_destination_resolved is not None:
                        on_destination_resolved(destination)
                    if request.subject_kind == "bundle":
                        snapshot = service.prepare_bundle(
                            profile=settings.profile,
                            redaction_mode=request.redaction_mode,
                        )
                    else:
                        report_id = cast(str, request.report_id)
                        snapshot = service.prepare_report(
                            profile=settings.profile,
                            report_id=report_id,
                            report_parameters=request.report_parameters,
                            redaction_mode=request.redaction_mode,
                        )

                    if destination.kind == "sheets":
                        spreadsheet_id = cast(str, destination.spreadsheet_id)
                        role_permit = role_stack.enter_context(
                            workbook_role_lease(
                                db.path,
                                spreadsheet_id,
                                lifetime=lifetime,
                            )
                        )
                        ExportDestinationsRepo(db).assert_current_for_publication(
                            destination
                        )
                    elif destination.destination_id is not None:
                        ExportDestinationsRepo(db).assert_current_for_publication(
                            destination
                        )

                if lifetime is not None:
                    lifetime.raise_if_cancelled()
                if destination.kind == "local":
                    from moneybin.exports.local import (  # noqa: PLC0415
                        LocalExportPublisher,
                    )

                    publisher = LocalExportPublisher(
                        cast("Path", destination.local_path),
                        destination_name=destination.name,
                    )
                    receipt = publisher.publish(
                        snapshot,
                        format=cast("LocalExportFormat", request.format),
                        compress_zip=request.compress_zip,
                        publication_lifetime=lifetime,
                    )
                    receipt = replace(receipt, destination=destination)
                else:
                    if role_permit is None:
                        raise RuntimeError("Sheets publication requires a role permit")
                    selected_publisher = sheets_publisher or cls._sheets_publisher()
                    receipt = selected_publisher.publish(
                        snapshot,
                        destination,
                        role_permit=role_permit,
                        publication_lifetime=lifetime,
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

    def resolve_destination(self, reference: str) -> ExportDestination:
        """Resolve one explicit kind:name reference without accepting a path."""
        from moneybin import error_codes  # noqa: PLC0415
        from moneybin.config import get_settings  # noqa: PLC0415
        from moneybin.errors import UserError  # noqa: PLC0415
        from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
            ExportDestinationsRepo,
        )
        from moneybin.services.entity_reference import (  # noqa: PLC0415
            AmbiguousEntity,
            MissingEntity,
        )

        kind, separator, name = reference.partition(":")
        if separator != ":" or kind not in _DESTINATION_KINDS:
            raise UserError(
                "Destination must be local:<name> or sheets:<name>.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        destination_kind = cast(DestinationKind, kind)
        validate_export_destination_name(
            name,
            kind=destination_kind,
            allow_builtin_local=True,
        )
        if (
            destination_kind == "local"
            and normalize_export_destination_name(name) == "exports"
        ):
            return ExportDestination(
                destination_id=None,
                name="local:exports",
                kind="local",
                local_path=get_settings().profile_exports_dir.expanduser().resolve(),
                spreadsheet_id=None,
                managed_tab_prefix=None,
            )

        resolved = ExportDestinationsRepo(self._db).resolve(name)
        if isinstance(resolved, MissingEntity):
            raise UserError(
                "Export destination not found.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        if isinstance(resolved, AmbiguousEntity):
            raise UserError(
                "Export destination reference is ambiguous.",
                code=error_codes.MUTATION_AMBIGUOUS,
                details={"candidate_ids": list(resolved.candidate_ids)},
            )
        if resolved.kind != destination_kind:
            raise UserError(
                f"Export destination is configured as {resolved.kind}, "
                f"not {destination_kind}.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if resolved.kind == "local" and resolved.local_path is not None:
            return replace(
                resolved,
                local_path=resolved.local_path.expanduser().resolve(),
            )
        return resolved

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
        destination_reasons = _destination_validation_reasons(request.destination)
        if destination_reasons:
            raise ValueError(
                "Invalid export destination: " + ", ".join(destination_reasons)
            )
        if request.subject_kind == "bundle":
            if request.report_id is not None:
                raise ValueError("bundle exports cannot include a report id")
            if request.report_parameters:
                raise ValueError("bundle exports cannot include report parameters")
        elif not request.report_id:
            raise ValueError("report exports require a report id")

        destination = request.destination
        if destination.kind == "local":
            if request.format == "sheets":
                raise ValueError("Local destinations do not support Sheets format")
        else:
            if request.format != "sheets":
                raise ValueError("Sheets destinations use the native Sheets format")
            if request.compress_zip:
                raise ValueError("Sheets exports do not support compression")
        if request.format == "xlsx" and request.compress_zip:
            raise ValueError("XLSX is already compressed and rejects ZIP compression")

    @staticmethod
    def _sheets_publisher() -> _SheetsPublisher:
        from moneybin.connectors.gsheet.service_factory import (  # noqa: PLC0415
            build_oauth_client,
        )
        from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
        from moneybin.exports.sheets import SheetsExportPublisher  # noqa: PLC0415

        return SheetsExportPublisher(
            sheets_client=SheetsClient(oauth=build_oauth_client()),
        )

    def status(
        self,
        *,
        sheets_authorization: _SheetsReadiness | None = None,
    ) -> ExportReadinessStatus:
        """Return destination readiness without target identities or locations."""
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

        from moneybin.config import get_settings  # noqa: PLC0415

        default_path = get_settings().profile_exports_dir.expanduser().resolve()
        default_reasons = (
            ("local_path_not_directory",)
            if default_path.exists() and not default_path.is_dir()
            else ()
        )
        results = [
            ExportDestinationReadiness(
                name="local:exports",
                kind="local",
                ready=not default_reasons,
                write_capable=not default_reasons,
                reasons=default_reasons,
            )
        ]
        for destination in stored:
            reasons = list(_destination_validation_reasons(destination))
            if destination.kind == "local":
                write_capable = not reasons
            else:
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

    @staticmethod
    def set_sheets_destination(
        *,
        name: str,
        spreadsheet_id: str,
        managed_tab_prefix: str,
        actor: str,
        oauth_client: SheetsAuthorization | None = None,
    ) -> AuditEvent:
        """Validate, authorize without DuckDB, then persist one Sheets target."""
        from moneybin.connectors.gsheet.errors import GSheetAuthError  # noqa: PLC0415
        from moneybin.database import get_database  # noqa: PLC0415
        from moneybin.exports.sheets import validate_managed_tab_prefix  # noqa: PLC0415
        from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
            ExportDestinationsRepo,
        )

        client: SheetsAuthorization
        if oauth_client is None:
            from moneybin.connectors.gsheet.service_factory import (  # noqa: PLC0415
                build_oauth_client,
            )

            client = build_oauth_client()
        else:
            client = oauth_client
        validate_export_destination_name(name, kind="sheets")
        prefix = validate_managed_tab_prefix(managed_tab_prefix)
        with get_database(read_only=True) as db:
            ExportDestinationsRepo(db).assert_not_inbound_connection(spreadsheet_id)
        grant = client.authorize(require_write=True)
        if not grant.can_write:
            raise GSheetAuthError("Google Sheets write authorization was not granted")
        with get_database(read_only=False) as db:
            return ExportDestinationsRepo(db).set_sheets(
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
        redaction_mode: RedactionMode = "redacted",
    ) -> PreparedExport:
        """Prepare exactly one catalog report under one output policy."""
        catalog = self._report_catalog or get_report_catalog()
        spec, execution = catalog.execute_raw(
            self._db,
            report_id=report_id,
            parameters=report_parameters or {},
            # Artifact exports are complete-or-fail. Interactive MCP response
            # caps never limit durable export contents.
            limit=None,
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


def _destination_validation_reasons(
    destination: ExportDestination,
) -> tuple[str, ...]:
    """Return fixed structural reason codes shared by run and status."""
    from moneybin.exports.sheets import validate_managed_tab_prefix  # noqa: PLC0415

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
        elif destination.local_path.exists() and not destination.local_path.is_dir():
            reasons.append("local_path_not_directory")
    elif destination.kind == "sheets":
        if destination.local_path is not None or not destination.spreadsheet_id:
            reasons.append("invalid_destination_configuration")
        if destination.managed_tab_prefix is None:
            reasons.append("invalid_destination_configuration")
        else:
            try:
                validate_managed_tab_prefix(destination.managed_tab_prefix)
            except ValueError:
                reasons.append("invalid_managed_tab_prefix")
    else:
        reasons.append("invalid_destination_configuration")
    return tuple(reasons)


def _bounded_label(value: object, allowed: frozenset[str]) -> str:
    """Map malformed runtime values to one fixed low-cardinality label."""
    return value if isinstance(value, str) and value in allowed else "invalid"
