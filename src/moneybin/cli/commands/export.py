"""Export delivery and saved-destination CLI commands."""

from __future__ import annotations

import json
import sys
import types
import typing
from enum import StrEnum
from pathlib import Path
from typing import Any, cast, get_args, get_origin
from urllib.parse import urlparse

import typer
from pydantic import JsonValue, TypeAdapter, ValidationError

from moneybin.cli.output import (
    ExportDestinationsOutput,
    ExportDestinationStatusOutput,
    ExportReceiptOutput,
    OutputFormat,
    output_option,
    quiet_option,
    render_export_receipt,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.exports.models import (
    ExportCommand,
    ExportDestination,
    ExportFormat,
    RedactionMode,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

_ACTOR = "cli"
_SHEETS_TAB_PREFIX = "MoneyBin"


class LocalExportFormat(StrEnum):
    """Formats published as local artifacts."""

    CSV = "csv"
    PARQUET = "parquet"
    XLSX = "xlsx"


class CompressionFormat(StrEnum):
    """Supported local export compression."""

    ZIP = "zip"


export_app = typer.Typer(
    help="Export canonical bundles or catalog reports",
    no_args_is_help=True,
)
destination_app = typer.Typer(
    help="List and manage saved export destinations",
    no_args_is_help=True,
)
destination_add_app = typer.Typer(
    help="Add or replace a saved export destination",
    no_args_is_help=True,
)
export_app.add_typer(destination_app, name="destination")
destination_app.add_typer(destination_add_app, name="add")


def _destination_reference(value: str) -> tuple[str, str]:
    """Parse only explicit ``local:<name>`` or ``sheets:<name>`` references."""
    kind, separator, name = value.partition(":")
    if separator != ":" or kind not in {"local", "sheets"} or not name or ":" in name:
        raise typer.BadParameter(
            "destination must be local:<name> or sheets:<name>",
            param_hint="--to",
        )
    return kind, name


def _is_interactive_terminal() -> bool:
    """Return whether the command can safely elicit a privacy choice."""
    return sys.stdin.isatty()


def _redaction_mode(*, unredacted: bool, yes: bool) -> RedactionMode:
    """Choose a per-run mode without ever inferring unredacted output."""
    if unredacted:
        return "unredacted"
    if yes or not _is_interactive_terminal():
        return "redacted"
    if typer.confirm("Export redacted output?", default=True, err=True):
        return "redacted"
    typer.echo(
        "Export cancelled. Re-run with --unredacted to request unredacted output.",
        err=True,
    )
    raise typer.Exit(1)


def _annotation_accepts_container(annotation: object) -> bool:
    """Return whether JSON container syntax is meaningful for a parameter."""
    origin = get_origin(annotation)
    if origin in (list, dict):
        return True
    if origin in (typing.Union, types.UnionType):
        return any(_annotation_accepts_container(arm) for arm in get_args(annotation))
    return False


def _annotation_accepts_none(annotation: object) -> bool:
    """Return whether the declared report parameter accepts JSON null."""
    origin = get_origin(annotation)
    return annotation is type(None) or (
        origin in (typing.Union, types.UnionType) and type(None) in get_args(annotation)
    )


def _parse_parameter_value(raw: str, annotation: object) -> JsonValue:
    """Apply the report parameter's declared type to one CLI string value."""
    adapter = TypeAdapter(Any if annotation is None else annotation)
    try:
        if _annotation_accepts_container(annotation):
            parsed = json.loads(raw)
            return cast(JsonValue, adapter.validate_python(parsed))
        if raw == "null" and _annotation_accepts_none(annotation):
            return cast(JsonValue, adapter.validate_python(None))
        return cast(JsonValue, adapter.validate_strings(raw))
    except (json.JSONDecodeError, ValidationError) as exc:
        raise typer.BadParameter(
            f"report parameter value {raw!r} does not match {annotation}",
            param_hint="--param",
        ) from exc


def _parse_report_parameters(
    report_id: str,
    raw_parameters: list[str] | None,
) -> dict[str, JsonValue]:
    """Parse repeated key=value options through catalog parameter annotations."""
    from moneybin.reports._framework.catalog import (  # noqa: PLC0415
        ServiceReportSpec,
        get_report_catalog,
    )

    catalog = get_report_catalog()
    spec = catalog.resolve(report_id)
    declared = spec.parameters if isinstance(spec, ServiceReportSpec) else spec.params
    annotations = {parameter.name: parameter.annotation for parameter in declared}
    supplied: dict[str, JsonValue] = {}
    for raw in raw_parameters or []:
        name, separator, value = raw.partition("=")
        if separator != "=" or not name:
            raise typer.BadParameter(
                "report parameters must use key=value",
                param_hint="--param",
            )
        if name in supplied:
            raise typer.BadParameter(
                f"report parameter {name!r} was supplied more than once",
                param_hint="--param",
            )
        annotation = annotations.get(name, str)
        supplied[name] = _parse_parameter_value(value, annotation)

    _, validated = catalog.resolve_request(
        report_id=report_id,
        parameters=supplied,
        limit=0,
    )
    return validated


def _validate_delivery_options(
    *,
    destination_reference: str,
    format_: LocalExportFormat | None,
    compress: CompressionFormat | None,
) -> tuple[ExportFormat, bool]:
    """Reject transport-incompatible flags before opening the database."""
    kind, _ = _destination_reference(destination_reference)
    if kind == "sheets":
        if format_ is not None:
            raise typer.BadParameter(
                "Sheets destinations do not accept --format",
                param_hint="--format",
            )
        if compress is not None:
            raise typer.BadParameter(
                "Sheets destinations do not accept --compress",
                param_hint="--compress",
            )
        return "sheets", False
    selected_format = format_ or LocalExportFormat.CSV
    if selected_format == LocalExportFormat.XLSX and compress is not None:
        raise typer.BadParameter(
            "XLSX is already compressed and does not accept --compress",
            param_hint="--compress",
        )
    return cast(ExportFormat, selected_format.value), compress is not None


def _parse_sheets_workbook_id(url: str) -> str:
    """Validate a Sheets URL and return its workbook identity without needing a tab."""
    from moneybin.connectors.gsheet.url_parser import parse_sheet_url  # noqa: PLC0415

    workbook_url = urlparse(url)._replace(fragment="gid=0").geturl()
    spreadsheet_id, _ = parse_sheet_url(workbook_url)
    return spreadsheet_id


def _run_export(
    *,
    subject_kind: typing.Literal["bundle", "report"],
    report_id: str | None,
    report_parameters: dict[str, JsonValue],
    destination_reference: str,
    format_: LocalExportFormat | None,
    compress: CompressionFormat | None,
    unredacted: bool,
    yes: bool,
    output: OutputFormat,
) -> None:
    """Build one typed request and delegate all orchestration to ExportService."""
    selected_format, compress_zip = _validate_delivery_options(
        destination_reference=destination_reference,
        format_=format_,
        compress=compress,
    )
    redaction_mode = _redaction_mode(unredacted=unredacted, yes=yes)

    from moneybin.exports.models import local_export_publish_error  # noqa: PLC0415
    from moneybin.exports.service import ExportService  # noqa: PLC0415

    cli_actor = f"export_{subject_kind}"

    def disclose_destination(destination: ExportDestination) -> None:
        if destination.kind == "local":
            typer.echo(
                f"Exporting to {cast(Path, destination.local_path).resolve()}",
                err=True,
            )
        else:
            typer.echo(
                f"Exporting to sheets:{destination.name} "
                f"(destination_id={destination.destination_id})",
                err=True,
            )

    with handle_cli_errors(cli_actor=cli_actor, payload_type=ExportReceiptOutput):
        try:
            receipt = ExportService.run(
                ExportCommand(
                    subject_kind=subject_kind,
                    report_id=report_id,
                    report_parameters=report_parameters,
                    destination_reference=destination_reference,
                    format=selected_format,
                    redaction_mode=redaction_mode,
                    compress_zip=compress_zip,
                ),
                actor=_ACTOR,
                on_destination_resolved=disclose_destination,
            )
        except OSError as exc:
            destination_kind, _ = _destination_reference(destination_reference)
            if destination_kind != "local":
                raise
            raise local_export_publish_error() from exc
    render_export_receipt(receipt, output, cli_actor=cli_actor)


@export_app.command("bundle")
def export_bundle(
    format_: LocalExportFormat | None = typer.Option(
        None,
        "--format",
        help="Local artifact format: csv, parquet, or xlsx. Default: csv.",
    ),
    destination_reference: str = typer.Option(
        "local:exports",
        "--to",
        help="Explicit destination: local:<name> or sheets:<name>.",
    ),
    compress: CompressionFormat | None = typer.Option(
        None,
        "--compress",
        help="Compress a CSV or Parquet local bundle as zip.",
    ),
    unredacted: bool = typer.Option(
        False,
        "--unredacted",
        help="Explicitly export unredacted data for this run only.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Accept the redacted default without prompting.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Export the canonical portability bundle."""
    _run_export(
        subject_kind="bundle",
        report_id=None,
        report_parameters={},
        destination_reference=destination_reference,
        format_=format_,
        compress=compress,
        unredacted=unredacted,
        yes=yes,
        output=output,
    )


@export_app.command("report")
def export_report(
    report_id: str = typer.Argument(..., help="Stable catalog report ID."),
    parameter: list[str] | None = typer.Option(
        None,
        "--param",
        help="Typed report parameter in key=value form; repeat for multiple values.",
    ),
    format_: LocalExportFormat | None = typer.Option(
        None,
        "--format",
        help="Local artifact format: csv, parquet, or xlsx. Default: csv.",
    ),
    destination_reference: str = typer.Option(
        "local:exports",
        "--to",
        help="Explicit destination: local:<name> or sheets:<name>.",
    ),
    compress: CompressionFormat | None = typer.Option(
        None,
        "--compress",
        help="Compress a CSV or Parquet local bundle as zip.",
    ),
    unredacted: bool = typer.Option(
        False,
        "--unredacted",
        help="Explicitly export unredacted data for this run only.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Accept the redacted default without prompting.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Export one catalog report and typed parameter binding."""
    with handle_cli_errors(
        cli_actor="export_report",
        payload_type=ExportReceiptOutput,
    ):
        parameters = _parse_report_parameters(report_id, parameter)
    _run_export(
        subject_kind="report",
        report_id=report_id,
        report_parameters=parameters,
        destination_reference=destination_reference,
        format_=format_,
        compress=compress,
        unredacted=unredacted,
        yes=yes,
        output=output,
    )


@destination_app.command("list")
def destination_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001  # list output is data-only
) -> None:
    """List derived and saved export destinations with readiness."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.exports.service import ExportService  # noqa: PLC0415
    from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
        ExportDestinationsRepo,
    )

    with handle_cli_errors(cli_actor="export_destination_list"):
        with get_database(read_only=True) as db:
            stored = ExportDestinationsRepo(db).list()
            readiness = ExportService(db).status()
        derived_path = get_settings().profile_exports_dir.expanduser().resolve()

    readiness_by_key = {(item.name, item.kind): item for item in readiness.destinations}
    rows = [
        ExportDestinationStatusOutput(
            destination_id=None,
            name="local:exports",
            kind="local",
            local_path=str(derived_path),
            ready=readiness_by_key[("local:exports", "local")].ready,
            write_capable=readiness_by_key[("local:exports", "local")].write_capable,
            reasons=list(readiness_by_key[("local:exports", "local")].reasons),
        )
    ]
    for destination in stored:
        state = readiness_by_key[(destination.name, destination.kind)]
        rows.append(
            ExportDestinationStatusOutput(
                destination_id=destination.destination_id,
                name=destination.name,
                kind=destination.kind,
                local_path=(
                    str(destination.local_path.expanduser().resolve())
                    if destination.local_path is not None
                    else None
                ),
                ready=state.ready,
                write_capable=state.write_capable,
                reasons=list(state.reasons),
            )
        )

    payload = ExportDestinationsOutput(destinations=rows)

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        for row in rows:
            state = "ready" if row.ready else "not ready"
            identity = (
                row.local_path
                if row.kind == "local"
                else f"destination_id={row.destination_id}"
            )
            line = f"{row.name} ({row.kind}, {state}): {identity}"
            if row.reasons:
                line = f"{line}; reasons: {', '.join(row.reasons)}"
            typer.echo(line)

    render_or_json(
        build_envelope(data=payload, total_count=len(rows), returned_count=len(rows)),
        output,
        render_fn=_render_text,
        cli_actor="export_destination_list",
    )


@destination_add_app.command("local")
def destination_add_local(
    name: str = typer.Argument(..., help="Unique saved destination name."),
    path: Path = typer.Argument(..., help="Local directory used for new artifacts."),
) -> None:
    """Add or replace a local artifact destination."""
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
        ExportDestinationsRepo,
    )

    resolved_path = path.expanduser().resolve()
    with handle_cli_errors(cli_actor="export_destination_add_local"):
        with get_database(read_only=False) as db:
            ExportDestinationsRepo(db).set_local(
                name=name,
                local_path=resolved_path,
                actor=_ACTOR,
            )
    typer.echo(f"Saved local destination {name}: {resolved_path}")
    typer.echo("✅ Destination saved.")


@destination_add_app.command("sheets")
def destination_add_sheets(
    name: str = typer.Argument(..., help="Unique saved destination name."),
    url: str = typer.Argument(..., help="Google Sheets workbook URL."),
) -> None:
    """Authorize and add or replace a Google Sheets destination."""
    from moneybin.connectors.gsheet.service_factory import (  # noqa: PLC0415
        build_oauth_client,
    )
    from moneybin.exports.service import ExportService  # noqa: PLC0415

    with handle_cli_errors(cli_actor="export_destination_add_sheets"):
        spreadsheet_id = _parse_sheets_workbook_id(url)
        ExportService.set_sheets_destination(
            name=name,
            spreadsheet_id=spreadsheet_id,
            managed_tab_prefix=_SHEETS_TAB_PREFIX,
            actor=_ACTOR,
            oauth_client=build_oauth_client(),
        )
    typer.echo(f"Saved Sheets destination {name}.")
    typer.echo("✅ Destination saved.")


@destination_app.command("remove")
def destination_remove(
    name: str = typer.Argument(..., help="Saved destination name or ID."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation."),
) -> None:
    """Remove MoneyBin configuration without deleting destination content."""
    from moneybin import error_codes  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.errors import UserError  # noqa: PLC0415
    from moneybin.repositories.export_destinations_repo import (  # noqa: PLC0415
        ExportDestinationsRepo,
    )

    if not yes and not typer.confirm(f"Remove destination configuration {name!r}?"):
        raise typer.Exit(0)

    with handle_cli_errors(cli_actor="export_destination_remove"):
        with get_database(read_only=False) as db:
            event = ExportDestinationsRepo(db).remove(name, actor=_ACTOR)
            if event is None:
                raise UserError(
                    "Export destination not found.",
                    code=error_codes.MUTATION_NOT_FOUND,
                )
    typer.echo(f"Removed destination configuration for {name}.")
    typer.echo("✅ Destination removed.")
