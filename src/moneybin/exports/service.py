"""Prepared export orchestration."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from time import perf_counter
from typing import TYPE_CHECKING, Final, Protocol, cast

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
from moneybin.metrics.registry import (
    EXPORT_DURATION_SECONDS,
    EXPORT_RECEIPT_FAILURES_TOTAL,
    EXPORT_RUNS_TOTAL,
)
from moneybin.reports._framework.catalog import (
    ReportCatalog,
    get_report_catalog,
    merged_degraded_reason,
    report_tier,
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

logger = logging.getLogger(__name__)


#: Stems of the positional names a redacted export publishes in place of a
#: user-authored column alias or parameter name. Positional rather than
#: class-derived so no two can collide on one name.
_REDACTED_COLUMN_NAME: Final = "redacted_column"
_REDACTED_PARAMETER_NAME: Final = "redacted_parameter"


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
            cls._record_receipt(receipt, actor=actor, lifetime=lifetime)
            return receipt
        finally:
            EXPORT_DURATION_SECONDS.labels(**labels).observe(
                perf_counter() - started_at
            )

    @classmethod
    def _record_receipt(
        cls,
        receipt: ExportReceipt,
        *,
        actor: str,
        lifetime: RequestLifetime | None,
    ) -> None:
        """Persist one receipt so a later turn or session can still find it.

        Opens its own short write transaction *after* publication returns. The
        read-only snapshot lease is already released by this point, so the
        writer lock is never held across filesystem or Sheets I/O — the
        property #349 exists to protect. Because it opens late, it runs inside
        the request's publication barrier: an already-ended request skips the
        write entirely, and a started one holds the timeout handler until it
        finishes, so the tool's response and this write can never diverge.

        Never raises, so recording is best-effort rather than guaranteed — the
        tool description says so, because an agent that promised recovery on a
        run whose receipt silently failed to record would be wrong. By the time
        this runs the artifact is already published and cannot be withdrawn, so
        letting a write failure escape would report an irreversible success as
        an error and discard the caller's only copy of the receipt — prompting
        a re-run that publishes a *second* artifact. The receipt is lost from
        the audit log either way; this keeps the loss to the thing that
        actually failed, logs it, and counts it in
        ``EXPORT_RECEIPT_FAILURES_TOTAL``.
        """
        from moneybin.database import get_database  # noqa: PLC0415
        from moneybin.errors import exception_origin  # noqa: PLC0415
        from moneybin.services.audit_service import AuditService  # noqa: PLC0415
        from moneybin.services.request_lifetime import (  # noqa: PLC0415
            publication_barrier,
        )

        destination = receipt.destination
        try:
            # The whole open-and-write sits inside the barrier, the same one
            # local.py and sheets.py put their publish step in. Every other
            # write in a tool body proves it cannot commit late via
            # tool_timeout_seconds >= the write-lock wait; this one opens
            # after publication, whose duration is unbounded, so that proof
            # does not reach it. Nor would bounding the lock wait alone:
            # write_lock shells out to `ps` before its first attempt, and a
            # successful open runs migrations before the connection registers
            # for interrupt — both outside max_wait. The barrier covers all of
            # it, refusing to start once the request has ended and holding the
            # timeout handler until a started write finishes.
            #
            # run() resolved this lifetime once and gave it to both publish
            # steps; re-deriving the ambient one here would ignore an explicit
            # publication_lifetime and bind this write to a no-op barrier.
            with (
                publication_barrier(lifetime),
                get_database(read_only=False) as db,
            ):
                AuditService(db).record_audit_event(
                    action="export.run",
                    # No app.* row backs an export, so the target names the
                    # export itself. undo_dispatch refuses targets outside the
                    # repo-owned app.* surface, which is what keeps a published
                    # artifact from ever appearing undoable via
                    # system_audit_undo.
                    #
                    # A real schema/table pair rather than nulls, matching
                    # import_service's ("raw", "pdf_seeds"): UndoService builds
                    # its refusal from this pair, and a null one renders as a
                    # bare "." (_row_targets coalesces null to ""), so the
                    # refusal would be correct but unreadable.
                    target=("export", "run", receipt.export_id),
                    before=None,
                    after=None,
                    actor=actor,
                    context={
                        # The id as well as the name: a destination can be
                        # renamed, so the name alone stops identifying the
                        # target it had at publication time.
                        "destination_id": destination.destination_id,
                        "destination_name": destination.name,
                        "destination_kind": destination.kind,
                        "format": receipt.format,
                        "redaction_mode": receipt.redaction_mode,
                        # What was exported, not just where it went — but the
                        # report's identity only, never its binding or any
                        # derivative of one. An earlier revision hashed the
                        # parameters to tell two runs apart; for a redacted
                        # export that hash covered already-masked values, so
                        # two bindings sharing a mask collapsed into one, and
                        # for an unredacted one it was an unkeyed digest of a
                        # low-entropy binding — a verifier for guessing it.
                        # export_id and checksums already discriminate runs.
                        "subject_kind": receipt.subject.get("kind"),
                        "report_id": receipt.subject.get("report_id"),
                        # File name, never the full path: R9 forbids
                        # persisting local paths, and a real export directory
                        # embeds the OS username. The row identifies what an
                        # export produced; it does not promise to re-locate
                        # the file, because a destination root can be
                        # repointed or removed after publication. The
                        # checksums are what confirm a candidate file is this
                        # artifact.
                        "artifact_name": (
                            receipt.artifact_path.name
                            if receipt.artifact_path
                            else None
                        ),
                        "compressed_artifact_name": (
                            receipt.compressed_artifact_path.name
                            if receipt.compressed_artifact_path
                            else None
                        ),
                        "sheets_identity": receipt.sheets_identity,
                        "row_counts": dict(receipt.row_counts),
                        "checksums": dict(receipt.checksums),
                    },
                )
        except Exception as exc:  # noqa: BLE001  # never fail a published export
            # A swallowed failure still has to be countable: the run itself
            # reports outcome="success" (correctly — the artifact is
            # published), so without this the only signal is a log line.
            EXPORT_RECEIPT_FAILURES_TOTAL.labels(
                destination_kind=destination.kind,
                reason=type(exc).__name__,
            ).inc()
            # Type and origin, never the message or traceback: a lock or
            # attach failure carries the database path, and
            # SanitizedLogFormatter masks amounts and account numbers, not
            # filesystem paths. Same boundary rule as mcp/decorator.py.
            logger.error(
                f"Export {receipt.export_id} published successfully but its "
                f"receipt could not be recorded to the audit log "
                f"({type(exc).__name__} at {exception_origin(exc)}); recover "
                f"it from the returned receipt or the artifact itself."
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
        default_reasons = _local_path_validation_reasons(default_path)
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
        if not client.is_authorized(require_write=True):
            grant = client.authorize(require_write=True)
            if not grant.can_write:
                raise GSheetAuthError(
                    "Google Sheets write authorization was not granted"
                )
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
        # `db` spans the user tier: a saved report is exportable by construction.
        catalog = self._report_catalog or get_report_catalog(self._db)
        spec, execution = catalog.execute_raw(
            self._db,
            report_id=report_id,
            parameters=report_parameters or {},
            # Artifact exports are complete-or-fail. Interactive MCP response
            # caps never limit durable export contents.
            limit=None,
        )
        # Only a saved report's names and SQL are the author's own text, and only a
        # redacted artifact has to withhold them. One predicate for both: they are
        # withheld for the same reason, so they cannot disagree about the tier.
        withhold_authored = redaction_mode == "redacted" and report_tier(spec) == "user"
        published = _published_names(
            execution.columns, stem=_REDACTED_COLUMN_NAME, withhold=withhold_authored
        )
        columns = tuple(
            PreparedColumn(
                name=published[name],
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
            _report_spec_source(spec)
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
        status = catalog.status(execution.report_id)
        parameters = spec.params if isinstance(spec, ReportSpec) else spec.parameters
        parameter_classes_by_name = {
            parameter.name: parameter.data_class for parameter in parameters
        }
        published_parameters = _published_names(
            tuple(parameter_classes_by_name),
            stem=_REDACTED_PARAMETER_NAME,
            withhold=withhold_authored,
        )
        parameter_classes = {
            published_parameters[name]: data_class.value
            for name, data_class in parameter_classes_by_name.items()
        }
        snapshot_parameters: Mapping[str, object]
        receipt_sql: str | None
        if redaction_mode == "redacted":
            # Keyed by declared name, exactly as `redact_report_parameters` reads
            # its own class map, so an undeclared key is an invariant violation
            # here rather than a name that quietly keeps itself.
            snapshot_parameters = {
                published_parameters[name]: value
                for name, value in redact_report_parameters(
                    spec,
                    execution.parameters,
                ).items()
            }
            # A saved report's SQL is user-authored, so a critical literal can sit
            # inline in the statement (`WHERE routing_number = '021000021'`) rather
            # than in a parameter this redacts. `apply_export_redaction` transforms
            # table rows only, so a verbatim receipt would republish in the manifest
            # exactly what the redacted policy was chosen to withhold.
            #
            # The user tier only, on the same reasoning as the names above: a
            # built-in's SQL is repo-authored and reviewed, keeps its values in
            # bindings the receipt redacts separately, and is already public in the
            # repo — so withholding it discloses nothing and costs the receipt the
            # one field that makes the artifact reproducible.
            receipt_sql = None if withhold_authored else execution.sql
        else:
            snapshot_parameters = execution.parameters
            receipt_sql = execution.sql
        receipt = ReportExportReceipt(
            report_id=execution.report_id,
            parameters=snapshot_parameters,
            parameter_classes=parameter_classes,
            sql=receipt_sql,
            lineage=execution.provenance,
            output_classes={
                published[name]: data_class.value
                for name, data_class in execution.output_classes.items()
            },
            # The current ReportSpec exposes neither field, and both want the
            # stored row rather than the catalog. Keep that absence explicit
            # instead of inferring verification state from provenance.
            freshness=None,
            graduation_eligibility=None,
            semantics=cast(dict[str, object], asdict(execution.semantics)),
            # Drift, unlike freshness, is already in hand: the catalog carries
            # R4's verdict for every user-tier row it built.
            degraded=status.degraded or execution.degraded_reason is not None,
            # The drift sentence names the columns that moved, and those are the
            # author's own — the same text the header rename and the withheld SQL
            # keep out of a redacted artifact. The code says a stale class map
            # from an unreadable row without repeating any of them. The caveat
            # the catalog attached beneath this call (#409) names no author text,
            # so a durable artifact of an inflated total carries it either way.
            degraded_reason=merged_degraded_reason(
                status.degraded_code if withhold_authored else status.degraded_reason,
                execution.degraded_reason,
            ),
        )
        tables = (table,)
        snapshot = PreparedExport(
            artifact_version=ARTIFACT_VERSION,
            export_id=None,
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


def _published_names(
    names: Sequence[str], *, stem: str, withhold: bool
) -> dict[str, str]:
    """Map each user-authored name to the one a redacted artifact may publish.

    A saved report's names are its author's text, so
    ``routing_number AS "021000021"`` puts a critical literal in the artifact
    header, the data-dictionary entry, and the receipt's class-map key, and
    ``WHERE routing_number = $acct_021000021`` puts one in the receipt's parameter
    keys and the subject. Redaction transforms *values*, so every one of them
    survives — the same disclosure the withheld receipt SQL exists to prevent.

    **Every** authored name is withheld, not only those whose own value is
    masked. Keying on the value was the obvious rule and it leaks:
    ``SELECT 1 AS "021000021"`` carries the literal beside a published ``1``, so
    the name's sensitivity is plainly not a function of the column it labels. A
    name is arbitrary user text, MoneyBin cannot classify arbitrary text — the
    reason ``catalog.py`` withholds a report's name wholesale from its collision
    warning rather than judging it — and a redacted artifact outlives the session
    that produced it.

    ``withhold`` is false for anything but a redacted user-tier export: a
    ``builtin`` or ``extension`` name is repo-authored and describes the column or
    filter rather than a value, and an unredacted artifact publishes the values
    anyway. Renaming is positional, so the result is unique by construction —
    these names are dict keys downstream, in ``redact_records``, the receipt, and
    the subject, and two sharing one would collapse into a single entry.
    """
    if not withhold:
        return {name: name for name in names}
    return {name: f"{stem}_{position}" for position, name in enumerate(names, start=1)}


def _report_spec_source(spec: ReportSpec) -> TableRef | None:
    """Return the ``reports.*`` view a report reads, or ``None`` if it has none.

    A dynamic report's ``view`` is ``None``: it is evaluated at query time over
    whatever ``core``/``app`` tables its SQL names, so no single source view
    exists. Pass that through rather than synthesizing one — the pre-existing
    ``_service_report_source`` fallback ends at ``TableRef("reports", name)``,
    which here would write a view that does not exist into the manifest, and
    provenance that cannot be checked is worse than none. Nothing is lost: the
    complete read-table set is carried by the receipt's ``lineage``.
    """
    return spec.view


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
        else:
            reasons.extend(_local_path_validation_reasons(destination.local_path))
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


def _local_path_validation_reasons(path: Path) -> tuple[str, ...]:
    """Return readiness reasons before a local publisher creates its root."""
    candidate = path
    if path.exists():
        if not path.is_dir():
            return ("local_path_not_directory",)
    else:
        while not candidate.exists():
            candidate = candidate.parent
        if not candidate.is_dir():
            return ("local_path_not_directory",)
    if not os.access(candidate, os.W_OK | os.X_OK):
        return ("local_path_not_writable",)
    return ()


def _bounded_label(value: object, allowed: frozenset[str]) -> str:
    """Map malformed runtime values to one fixed low-cardinality label."""
    return value if isinstance(value, str) and value in allowed else "invalid"
