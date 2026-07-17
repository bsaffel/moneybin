# ruff: noqa: A001 — "import" shadows builtin, but it's a Typer subcommand name
"""Import commands for MoneyBin CLI.

This module provides the user-facing import workflow: auto-detect file type,
extract, load into DuckDB, and optionally run SQLMesh transforms.
Also provides history, revert, preview, and format management subcommands.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import typer

from moneybin.cli.commands import import_inbox, import_labels
from moneybin.cli.output import (
    OutputFormat,
    emit_json_error,
    output_option,
    quiet_option,
)
from moneybin.cli.utils import emit_json
from moneybin.errors import UserError
from moneybin.extractors.tabular.formats import NumberFormatType, SignConventionType

if TYPE_CHECKING:
    from moneybin.database import Database
    from moneybin.extractors.tabular.formats import TabularFormat
    from moneybin.repositories.pdf_formats_repo import PdfFormat
    from moneybin.services.import_confirmation import (
        ConfirmationRequired,
        SignConventionProposal,
    )
    from moneybin.services.import_service import ImportResult


class _FormatTypeFilter(StrEnum):
    """Projection filter for `import formats list`."""

    tabular = "tabular"
    pdf = "pdf"
    all = "all"


app = typer.Typer(
    help=("Import financial files (OFX/QFX, CSV/TSV/Excel/Parquet) into MoneyBin"),
    no_args_is_help=True,
)
formats_app = typer.Typer(
    help="Manage tabular import format definitions",
    no_args_is_help=True,
)
app.add_typer(formats_app, name="formats")
app.add_typer(import_inbox.app, name="inbox", help="Drain the watched import inbox")
app.add_typer(import_labels.app, name="labels", help="Manage labels on imports")
logger = logging.getLogger(__name__)

# Shown whenever a saved --sign override replays onto a new statement. The
# override disarms the credit-card detector for that format on every future
# import, so the decision is restated at the moment it acts — one message, one
# definition, both the single-file and batch paths echo it.
_SIGN_OVERRIDE_REPLAYED_NOTE = (
    "⚠️  Sign convention taken from your saved --sign override for this "
    "statement format — the credit-card detector was not consulted. Re-run "
    "with --sign to change it."
)


def _parse_kv(
    values: list[str] | None, *, flag: str, fmt: str
) -> dict[str, str] | None:
    """Parse repeatable ``KEY=VALUE`` CLI options into a stripped dict.

    ``flag`` and ``fmt`` shape only the error message (e.g. ``flag="--override"``,
    ``fmt="field=column"``). Returns ``None`` for empty input.
    """
    if not values:
        return None
    result: dict[str, str] = {}
    for raw in values:
        if "=" not in raw:
            logger.error(f"❌ Invalid {flag} format (expected {fmt}): {raw!r}")
            raise typer.Exit(1)
        key, _, value = raw.partition("=")
        result[key.strip()] = value.strip()
    return result


def _parse_overrides(override: list[str] | None) -> dict[str, str] | None:
    """Parse and validate --override field=column values."""
    return _parse_kv(override, flag="--override", fmt="field=column")


def _parse_account_bindings(binding: list[str] | None) -> dict[str, str] | None:
    """Parse --account-binding source_key=ACCOUNT_ID|new values."""
    return _parse_kv(binding, flag="--account-binding", fmt="source_key=ACCOUNT_ID|new")


def _parse_account_metadata(
    meta: list[str] | None,
) -> dict[str, dict[str, str]] | None:
    """Parse --account-meta source_key:field=value into a nested map."""
    if not meta:
        return None
    result: dict[str, dict[str, str]] = {}
    for raw in meta:
        if ":" not in raw or "=" not in raw.split(":", 1)[1]:
            logger.error(
                "❌ Invalid --account-meta format "
                f"(expected source_key:field=value): {raw!r}"
            )
            raise typer.Exit(1)
        key, _, field_value = raw.partition(":")
        field, _, value = field_value.partition("=")
        result.setdefault(key.strip(), {})[field.strip()] = value.strip()
    return result


def _load_all_formats(
    db: Database | None = None,
) -> tuple[dict[str, TabularFormat], dict[str, TabularFormat]]:
    """Load built-in + user-saved formats, returning (all_formats, builtin).

    Falls back to built-in only if DB is unavailable.
    """
    from moneybin.extractors.tabular.formats import (
        load_builtin_formats,
        load_formats_from_db,
        merge_formats,
    )

    builtin = load_builtin_formats()
    user_formats: dict[str, TabularFormat] = {}
    if db is not None:
        try:
            user_formats = load_formats_from_db(db)
        except Exception:  # noqa: BLE001 — DB table may not exist yet
            logger.debug("Could not load user formats from DB, using built-in only")
    all_formats = merge_formats(builtin, user_formats)
    return all_formats, builtin


def _load_pdf_formats(db: Database | None) -> list[PdfFormat]:
    """Load saved PDF format profiles from the DB, or return [] on miss."""
    if db is None:
        return []
    try:
        from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo

        return PdfFormatsRepo(db).list_all()
    except Exception:  # noqa: BLE001 — app.pdf_formats may not exist yet
        logger.debug("Could not load PDF formats from DB")
        return []


@app.command("files")
def import_files_command(
    file_paths: list[Path] = typer.Argument(
        ..., help="One or more financial data files to import"
    ),
    refresh: bool = typer.Option(
        True,
        "--refresh/--no-refresh",
        help=(
            "Run the post-load refresh pipeline (matching + SQLMesh apply + "
            "categorization) once after the batch completes. Pass --no-refresh "
            "to defer; system_status will show transforms_pending and a later "
            "'transform apply' or refresh will catch up."
        ),
    ),
    institution: str | None = typer.Option(
        None,
        "--institution",
        "-i",
        help=(
            "Institution override for OFX/QFX/QBO files. Consulted only when "
            "the file's <FI><ORG>, FID lookup, and filename heuristic all "
            "yield nothing. For CSV/tabular files, selects the format profile. "
            "Single-file mode only."
        ),
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-F",
        help="Re-import a file already in the import log (creates a new batch).",
    ),
    account_id: str | None = typer.Option(
        None,
        "--account-id",
        "-a",
        help="Account identifier (bypasses name matching). Single-file mode only.",
    ),
    account_name: str | None = typer.Option(
        None,
        "--account-name",
        "-n",
        help="Account name for single-account tabular files. Single-file mode only.",
    ),
    format_name: str | None = typer.Option(
        None,
        "--format",
        "-f",
        help=(
            "Use a specific named format (bypass auto-detection). "
            "Single-file mode only."
        ),
    ),
    override: list[str] = typer.Option(
        None,
        "--override",
        help=(
            "Field→column override, repeatable (e.g. --override date=Date "
            "--override amount=Amount). Single-file mode only."
        ),
    ),
    mapping: list[str] = typer.Option(
        None,
        "--mapping",
        help=(
            "Field→column override, repeatable (alias for --override). "
            "e.g. --mapping description=Memo. Single-file mode only."
        ),
    ),
    confirm: bool = typer.Option(
        False,
        "--confirm/--no-confirm",
        help=(
            "Accept the proposed column mapping without prompting. "
            "Use when a previous import returned confirmation_required. "
            "Single-file mode only."
        ),
    ),
    confirm_sign: bool = typer.Option(
        False,
        "--confirm-sign",
        help=(
            "Explicitly approve an inferred tabular sign inversion. "
            "Single-file mode only."
        ),
    ),
    sign: SignConventionType | None = typer.Option(
        None,
        "--sign",
        help="Sign convention override. Single-file mode only.",
    ),
    date_format: str | None = typer.Option(
        None,
        "--date-format",
        help=(
            "Date format override (strptime format string, e.g. %%Y-%%m-%%d). "
            "Single-file mode only."
        ),
    ),
    number_format: NumberFormatType | None = typer.Option(
        None,
        "--number-format",
        help="Number format override. Single-file mode only.",
    ),
    sheet: str | None = typer.Option(
        None,
        "--sheet",
        help="Excel sheet name (default: auto-select largest). Single-file mode only.",
    ),
    delimiter: str | None = typer.Option(
        None,
        "--delimiter",
        help="Explicit delimiter for text formats. Single-file mode only.",
    ),
    encoding: str | None = typer.Option(
        None,
        "--encoding",
        help=("Explicit file encoding (e.g. utf-8, latin-1). Single-file mode only."),
    ),
    no_row_limit: bool = typer.Option(
        False, "--no-row-limit", help="Override row count limit"
    ),
    no_size_limit: bool = typer.Option(
        False, "--no-size-limit", help="Override file size limit"
    ),
    save_format: bool = typer.Option(
        True,
        "--save-format/--no-save-format",
        help="Auto-save detected format for future imports (default: save)",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Auto-accept the top fuzzy account match without prompting",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Import one or more financial data files into MoneyBin.

    Supported file types:
      - OFX/QFX/QBO: Bank and credit card statements
      - CSV/TSV/Excel: Bank transaction exports (Chase, Citi, etc.)
      - Parquet/Feather: Data warehouse exports
      - PDF: Native-text bank statements (lands as queryable seed in raw.pdf_seeds)

    Per-file failures do not abort the batch. The refresh pipeline runs
    once at end of the batch by default; pass --no-refresh to defer.

    Per-file overrides (--institution, --account-name, --format, --override,
    etc.) apply only when a single path is supplied. Pass one file per
    command when per-file overrides are required.

    Examples:
        moneybin import files ~/Downloads/WellsFargo_2025.qfx
        moneybin import files ~/Downloads/*.ofx
        moneybin import files ~/Downloads/chase_activity.csv --account-name "Chase Checking"
        moneybin import files statement.ofx --output json
    """
    from moneybin.cli.output import render_or_json
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.import_service import ImportService

    # Single-file invocations keep fast-fail on missing paths (typo
    # detection). Multi-file batches defer to ImportService.import_files()
    # which records per-file FileNotFoundError as PerFileResult so the
    # batch contract ("per-file failures do not abort the batch") holds.
    if len(file_paths) == 1 and not file_paths[0].exists():
        logger.error(f"❌ File not found: {file_paths[0]}")
        raise typer.Exit(1)

    # --mapping is an alias for --override; merge both into one dict.
    combined_override = list(override or []) + list(mapping or [])
    overrides = _parse_overrides(combined_override or None)
    interactive = not yes and sys.stdin.isatty()

    # Single-file mode (`len(file_paths) == 1`) always uses import_file
    # directly so ImportConfirmationRequiredError can bubble. This variable
    # only drives the warning at line ~289 for multi-file invocations: any
    # per-file flag silently ignored by the batch path warrants a warning.
    # NOTE: ``confirm`` is NOT in this set because the batch path forwards
    # it (see svc.import_files call below). ``overrides`` IS — the batch
    # method doesn't accept it, so multi-file + --mapping silently ignores
    # the override.
    has_single_file_knobs = (
        any(
            v is not None
            for v in (
                institution,
                account_id,
                account_name,
                format_name,
                sign,
                date_format,
                number_format,
                sheet,
                delimiter,
                encoding,
            )
        )
        or overrides is not None
        or yes
        or no_row_limit
        or no_size_limit
        or not save_format
    )

    if len(file_paths) > 1 and has_single_file_knobs:
        logger.warning(
            "⚠️  Per-file flags only apply in single-file mode and will be "
            "ignored. Use one file per command for per-file overrides."
        )
    if len(file_paths) > 1 and (confirm or confirm_sign):
        # --confirm with multiple files would silently auto-accept every
        # first-encounter layout in the batch sight-unseen. Each layout is a
        # separate trust decision; refuse the batch and require per-file
        # invocations or use `moneybin import confirm <file>` after the
        # confirmation_required envelopes surface.
        raise typer.BadParameter(
            "--confirm and --confirm-sign cannot be combined with multiple files. Each first-"
            "encounter layout requires its own confirmation. Re-run per-file "
            "or import without --confirm to surface confirmation_required "
            "envelopes, then ratify with `moneybin import confirm <file>`."
        )

    from moneybin.database import get_database  # noqa: PLC0415 — deferred import

    files_list: list[dict[str, Any]] = []
    data: dict[str, Any] = {}
    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                svc = ImportService(db)
                # Single-path invocations always use import_file directly so
                # ImportConfirmationRequiredError can bubble to the CLI handler.
                # Multi-path stays on import_files (batch contract).
                if len(file_paths) == 1:
                    import_kwargs: dict[str, Any] = {
                        "file_path": file_paths[0],
                        "refresh": refresh,
                        "institution": institution,
                        "force": force,
                        "interactive": interactive,
                        "account_id": account_id,
                        "account_name": account_name,
                        "format_name": format_name,
                        "overrides": overrides,
                        "sign": sign,
                        "date_format": date_format or None,
                        "number_format": number_format,
                        "save_format": save_format,
                        "sheet": sheet,
                        "delimiter": delimiter,
                        "encoding": encoding,
                        "no_row_limit": no_row_limit,
                        "no_size_limit": no_size_limit,
                        "auto_accept": yes,
                        "confirm": confirm,
                        "actor_kind": "human",
                    }
                    if confirm_sign:
                        import_kwargs["human_sign_confirmation"] = True
                    result = svc.import_file(**import_kwargs)
                    if result.sign_correction_suggested:
                        typer.echo(
                            "⚠️  Sign convention may be inverted (running balance "
                            "suggests negation). If amounts look wrong, re-run "
                            "with --sign to override.",
                            err=True,
                        )
                    if result.sign_override_replayed:
                        typer.echo(_SIGN_OVERRIDE_REPLAYED_NOTE, err=True)
                    files_list = [
                        {
                            "path": str(file_paths[0]),
                            "status": "imported",
                            "source_type": result.file_type,
                            "rows_loaded": result.rows_loaded,
                            "import_id": result.import_id,
                            # Mirror the batch path: JSON-output agents need
                            # the structured signal regardless of single vs
                            # multi-file invocation.
                            "sign_correction_suggested": result.sign_correction_suggested,
                            "sign_override_replayed": result.sign_override_replayed,
                        }
                    ]
                    data = {
                        "imported_count": 1,
                        "failed_count": 0,
                        "total_count": 1,
                        "transforms_applied": refresh and result.core_tables_rebuilt,
                        "transforms_duration_seconds": None,
                        "files": files_list,
                    }
                else:
                    batch = svc.import_files(
                        [str(p) for p in file_paths],
                        refresh=refresh,
                        force=force,
                        interactive=interactive,
                        confirm=confirm,
                        actor_kind="human",
                    )
                    if any(r.sign_correction_suggested for r in batch.per_file):
                        typer.echo(
                            "⚠️  Sign convention may be inverted for one or "
                            "more imports (running balance suggests negation). "
                            "If amounts look wrong, re-run with --sign to "
                            "override.",
                            err=True,
                        )
                    if any(r.sign_override_replayed for r in batch.per_file):
                        typer.echo(_SIGN_OVERRIDE_REPLAYED_NOTE, err=True)
                    files_list = [
                        {
                            "path": r.path,
                            "status": r.status,
                            "source_type": r.source_type,
                            "rows_loaded": r.rows_loaded,
                            "import_id": r.import_id,
                            # Always include sign_correction_suggested so
                            # JSON-output agents see a structured signal that
                            # amounts may need re-import with --sign — the TTY
                            # path already warns to stderr; this closes the
                            # gap for scripted callers.
                            "sign_correction_suggested": r.sign_correction_suggested,
                            "sign_override_replayed": r.sign_override_replayed,
                            **({"error": r.error} if r.error else {}),
                            **(
                                {"confirmation_payload": r.confirmation_payload}
                                if r.confirmation_payload
                                else {}
                            ),
                        }
                        for r in batch.per_file
                    ]
                    data = {
                        "imported_count": batch.imported_count,
                        "failed_count": batch.failed_count,
                        "total_count": batch.total_count,
                        "transforms_applied": batch.transforms_applied,
                        "transforms_duration_seconds": batch.transforms_duration_seconds,
                        "files": files_list,
                    }
                    if batch.transforms_error:
                        data["transforms_error"] = batch.transforms_error
    except Exception as _exc:  # noqa: BLE001 — dispatch on type below
        from moneybin.services.import_confirmation import (  # noqa: PLC0415
            ImportConfirmationRequiredError,
        )

        if isinstance(_exc, ImportConfirmationRequiredError):
            # Surface the confirmation_required envelope.  Non-TTY / --output
            # json callers get JSON directly; interactive callers see a
            # human-readable summary with re-run instructions.
            #
            # TODO(v1-edit): Full interactive field-walk (prompt per flagged
            # field) is deferred.  The interactive path below directs the user
            # to re-run with --confirm or --mapping instead.
            outcome = _exc.outcome
            file_path_str = str(file_paths[0]) if len(file_paths) == 1 else ""
            envelope_data = _confirmation_envelope_data(outcome)
            confirm_actions: list[str] = []
            if outcome.reason == "sign_convention":
                # A card statement proposes inverting every amount. The agent must
                # NOT blind-accept it, and there's no column mapping to preview —
                # so name the two honest recoveries, not the tabular
                # accept/mapping/preview hints. error_message is self-contained
                # (it already names the commands) and leads instead of the generic
                # "Validation failed" prefix (this is a proposal, not a failure).
                if outcome.error_message:
                    confirm_actions.append(outcome.error_message)
                confirm_actions.extend(
                    _sign_recovery_commands(
                        file_path_str,
                        channel=outcome.channel,
                        accept=confirm or overrides is None,
                        mapping=overrides,
                        save_format=save_format,
                        account_id=account_id,
                        account_name=account_name,
                    )
                )
            else:
                if outcome.error_message:
                    confirm_actions.append(
                        f"Validation failed: {outcome.error_message}"
                    )
                if outcome.reason == "account_confirmation":
                    # The layout is settled; only the account identity needs
                    # ratifying. Replay the current confirmation inputs because
                    # retries persist no partial state, and add the missing binding.
                    # The generic alternate mapping hints below remain irrelevant.
                    confirm_actions.append(
                        f"Run `{_account_recovery_command(file_path_str, outcome, accept=confirm or overrides is None, mapping=overrides, save_format=save_format, account_id=account_id, account_name=account_name, confirm_sign=confirm_sign)}` "
                        "to bind each proposed account (adopt an existing id, or "
                        "'new' to keep distinct)."
                    )
                else:
                    # resolve_or_confirm refuses Accept on low-tier proposals (the
                    # detector couldn't form a complete one); suggesting --confirm
                    # there would just bounce back with the same outcome. Only
                    # surface the accept hint when the tier permits acceptance.
                    if outcome.confidence.tier != "low":
                        confirm_actions.append(
                            "Re-run with --confirm to accept the proposed mapping "
                            "as-is."
                        )
                    confirm_actions.append(
                        "Re-run with --mapping <field>=<column> to override specific "
                        "fields."
                    )
                    if outcome.confidence.tier != "low":
                        confirm_actions.append(
                            f"Run 'moneybin import confirm {file_path_str} --accept' "
                            "as a subcommand."
                        )
                confirm_actions.append(
                    f"Run 'moneybin import preview {file_path_str}' to inspect the "
                    "proposal."
                )
            if output == OutputFormat.JSON or not sys.stdout.isatty():
                # Non-TTY / --output json: emit the full ResponseEnvelope so
                # CLI --output json matches the MCP envelope shape (same
                # top-level status/summary/data/actions wrapper).
                # Exit 0 so scripted consumers receive the envelope cleanly.
                confirm_envelope = build_envelope(
                    data=envelope_data,
                    sensitivity="medium",
                    actions=confirm_actions,
                )
                render_or_json(
                    confirm_envelope,
                    OutputFormat.JSON,
                    cli_actor="import_files_command",
                )
                raise typer.Exit(0) from _exc
            # Interactive human path: render a human-readable summary and exit
            # 1 so pipelines halt cleanly (unlike the non-TTY path which exits
            # 0 so scripted consumers can parse the envelope).
            _render_confirmation_prompt(
                outcome,
                file_path_str,
                accept=confirm or overrides is None,
                mapping=overrides,
                save_format=save_format,
                account_id=account_id,
                account_name=account_name,
                confirm_sign=confirm_sign,
            )
            raise typer.Exit(1) from _exc

        if not isinstance(_exc, (ValueError, PermissionError)):
            raise

        # ValueError / PermissionError: surface as a structured failed-file
        # envelope so --output json stays consistent with the batch contract.
        error_type = type(_exc).__name__
        files_list = [
            {
                "path": str(file_paths[0]) if len(file_paths) == 1 else "",
                "status": "failed",
                "source_type": None,
                "rows_loaded": 0,
                "import_id": None,
                "error": error_type,
            }
        ]
        data = {
            "imported_count": 0,
            "failed_count": 1,
            "total_count": 1,
            "transforms_applied": False,
            "transforms_duration_seconds": None,
            "files": files_list,
        }
        envelope = build_envelope(data=data, sensitivity="low")
        if output == OutputFormat.JSON:
            render_or_json(envelope, output, cli_actor="import_files_command")
        else:
            logger.error(f"❌ {_exc}")
        raise typer.Exit(1) from _exc

    # Bump sensitivity to "medium" when any per-file entry carries a
    # confirmation_payload — those payloads include detector samples
    # (description / merchant cells) and must match the single-file
    # confirmation_required envelope's medium tier so agents apply the
    # same consent gate to batch proposals.
    batch_sensitivity = (
        "medium" if any(f.get("confirmation_payload") for f in files_list) else "low"
    )
    envelope = build_envelope(data=data, sensitivity=batch_sensitivity)
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="import_files_command")
    elif not quiet:
        for f in files_list:
            icon = "✅" if f["status"] == "imported" else "❌"
            label = f["source_type"] or "?"
            rows = f.get("rows_loaded") or 0
            logger.info(f"{icon} {f['path']} [{label}] — {rows} rows")
        if data["transforms_applied"]:
            duration = data["transforms_duration_seconds"]
            if duration is not None:
                logger.info(f"✅ Core tables rebuilt in {duration:.1f}s")
            else:
                logger.info("✅ Core tables rebuilt")
        if data.get("transforms_error"):
            logger.warning(f"⚠️  Transform apply failed: {data['transforms_error']}")

    # Batch import succeeds file-by-file but the post-import SQLMesh apply is
    # a separate failure surface. Exit non-zero so scripts and agents detect
    # that core tables were not refreshed even when every file imported.
    # Mirrors the fail-loud single-file path that raises on refresh() error.
    if data.get("transforms_error"):
        raise typer.Exit(1)


def _confirmation_envelope_data(outcome: ConfirmationRequired) -> dict[str, Any]:
    """Build the ``confirmation_required`` envelope ``data`` dict from an outcome.

    Shared by ``import files`` and ``import confirm`` so the JSON shape cannot
    drift between the two surfaces. Delegates to the canonical
    ``confirmation_payload_dict`` — the single source MCP and the batch service
    also use — so a new channel field (e.g. ``bridge_payload``) lands in one
    place; this wrapper only prepends the CLI-envelope ``status`` field. The
    per-command ``actions[]`` hints differ (files-level vs confirm-subcommand
    context) and stay in the callers.
    """
    from moneybin.services.import_confirmation import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
        confirmation_payload_dict,
    )

    return {"status": "confirmation_required", **confirmation_payload_dict(outcome)}


def _echo_account_proposals(outcome: ConfirmationRequired, *, err: bool) -> None:
    """Print the source keys + candidate accounts for an account_confirmation.

    Shared by the interactive `import files` prompt (stdout) and the `import
    confirm` error path (stderr) so the binding info a user must reference never
    diverges between the two surfaces.
    """
    if not outcome.account_proposals:
        return
    typer.echo("\n   Account binding required:", err=err)
    for p in outcome.account_proposals:
        typer.echo(f"     source key: {p['source_account_key']}", err=err)
        for c in p["candidates"]:
            typer.echo(
                f"       candidate: {c['account_id']}  "
                f"({c['display_name']}, {c['signal']})",
                err=err,
            )


def _tabular_recovery_args(
    *,
    mapping: dict[str, str] | None,
    account_bindings: dict[str, str] | None,
    account_metadata: dict[str, dict[str, str]] | None,
) -> list[str]:
    """Serialize repeatable tabular mapping and account options."""
    args: list[str] = []
    for field, source in (mapping or {}).items():
        args.extend(("--mapping", f"{field}={source}"))
    for source_key, account_id in (account_bindings or {}).items():
        args.extend(("--account-binding", f"{source_key}={account_id}"))
    for source_key, metadata in (account_metadata or {}).items():
        for field, value in metadata.items():
            args.extend(("--account-meta", f"{source_key}:{field}={value}"))
    return args


def _tabular_confirmation_command(
    file_path_str: str,
    *,
    accept: bool,
    confirm_sign: bool,
    mapping: dict[str, str] | None,
    save_format: bool,
    account_id: str | None,
    account_name: str | None,
    account_bindings: dict[str, str] | None,
    account_metadata: dict[str, dict[str, str]] | None,
) -> str:
    """Serialize one public tabular confirmation request losslessly."""
    import shlex  # noqa: PLC0415

    parts = ["moneybin", "import", "confirm", file_path_str]
    if accept:
        parts.append("--accept")
    if confirm_sign:
        parts.append("--confirm-sign")
    if account_id is not None:
        parts.extend(("--account-id", account_id))
    if account_name is not None:
        parts.extend(("--account-name", account_name))
    parts.extend(
        _tabular_recovery_args(
            mapping=mapping,
            account_bindings=account_bindings,
            account_metadata=account_metadata,
        )
    )
    if not save_format:
        parts.append("--no-save-format")
    return shlex.join(parts)


def _account_recovery_command(
    file_path_str: str,
    outcome: ConfirmationRequired,
    *,
    accept: bool = True,
    mapping: dict[str, str] | None = None,
    save_format: bool = True,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
    confirm_sign: bool = False,
) -> str:
    """Reproduce a tabular confirmation while adding unresolved account bindings."""
    bindings = dict(account_bindings or {})
    for proposal in outcome.account_proposals:
        source_key = str(proposal["source_account_key"])
        bindings.setdefault(source_key, "<account_id|new>")
    if not bindings:
        bindings["<source_key>"] = "<account_id|new>"

    return _tabular_confirmation_command(
        file_path_str,
        accept=accept,
        confirm_sign=confirm_sign,
        mapping=mapping,
        save_format=save_format,
        account_id=account_id,
        account_name=account_name,
        account_bindings=bindings,
        account_metadata=account_metadata,
    )


def _sign_recovery_commands(
    file_path_str: str,
    *,
    channel: str,
    accept: bool = True,
    mapping: dict[str, str] | None = None,
    save_format: bool = True,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
) -> list[str]:
    """The two honest recoveries for a card sign-convention confirmation.

    A card statement proposes inverting every amount (charges → expenses,
    payments → credits). The user decides by re-running with the convention they
    intend, never by blind-accepting a proposed mapping.
    Shared by the JSON ``actions[]`` and the interactive prompt so the CLI never
    drifts from the terminal command the gate's ``error_message`` already names.
    Mirrors the MCP ``_sign_confirm_actions`` recovery.
    """
    import shlex  # noqa: PLC0415

    quoted = shlex.quote(file_path_str)
    tabular_command = _tabular_confirmation_command(
        file_path_str,
        accept=accept,
        confirm_sign=True,
        mapping=mapping,
        save_format=save_format,
        account_id=account_id,
        account_name=account_name,
        account_bindings=account_bindings,
        account_metadata=account_metadata,
    )
    return [
        (
            f"Confirm the tabular mapping, then approve the sign: {tabular_command}"
            if channel == "tabular"
            else f"If it IS a credit card: moneybin import files {quoted} --confirm "
            "(records charges as expenses, payments as credits)."
        ),
        f"If it is NOT a credit card: moneybin import files {quoted} "
        "--sign negative_is_expense (records amounts exactly as printed).",
    ]


def _render_sign_convention_prompt(
    proposed: SignConventionProposal,
    file_path_str: str,
    *,
    channel: str,
    accept: bool = True,
    mapping: dict[str, str] | None = None,
    save_format: bool = True,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
) -> None:
    """Print the interactive prompt for a sign-convention confirmation.

    "magic stays visible": a whole-ledger sign inversion the user can't see the
    evidence for must never be applied. Show the matched card disclosures and the
    printed-vs-recorded sample rows so the flip is concrete, then name the two
    honest recoveries — never "Validation failed" (this is a proposal, not a
    failure) or the --mapping hint (a dead-end loop for a PDF).
    """
    typer.echo("\n👀  Sign convention confirmation required")
    typer.echo(f"   File: {file_path_str}")
    typer.echo(
        "   Recording it with this convention inverts every amount's sign — "
        "negative values become income and positive values become expenses."
    )
    if proposed.evidence:
        typer.echo(f"\n   Inference evidence: {', '.join(proposed.evidence)}")
    if proposed.sample_rows:
        typer.echo("\n   Printed on statement → recorded by MoneyBin:")
        for row in proposed.sample_rows:
            desc = row.get("description", "")
            printed = row.get("as_printed", "")
            recorded = row.get("as_recorded", "")
            label = f"{desc}: " if desc else ""
            typer.echo(f"     {label}{printed} → {recorded}")
    typer.echo("\n   To proceed:")
    for line in _sign_recovery_commands(
        file_path_str,
        channel=channel,
        accept=accept,
        mapping=mapping,
        save_format=save_format,
        account_id=account_id,
        account_name=account_name,
        account_bindings=account_bindings,
        account_metadata=account_metadata,
    ):
        typer.echo(f"     {line}")
    typer.echo()


def _render_confirmation_prompt(
    outcome: ConfirmationRequired,
    file_path_str: str,
    *,
    accept: bool = True,
    mapping: dict[str, str] | None = None,
    save_format: bool = True,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
    confirm_sign: bool = False,
) -> None:
    """Print a human-readable confirmation summary for an unknown-layout encounter.

    Interactive edit-flow (walking each flagged field one at a time) is deferred
    to a future task.  This v1 implementation shows the proposal and instructs
    the user to re-run with the appropriate flags.
    """
    import shlex  # noqa: PLC0415

    from moneybin.services.import_confirmation import (  # noqa: PLC0415
        ProposedMapping,
        SignConventionProposal,
    )

    # A card sign-convention proposal is not an unknown-layout / validation
    # encounter — it has its own honest rendering (evidence + printed-vs-recorded
    # rows + --confirm/--sign recovery), so short-circuit before the tabular
    # mapping/validation prose below.
    if outcome.reason == "sign_convention" and isinstance(
        outcome.proposed, SignConventionProposal
    ):
        _render_sign_convention_prompt(
            outcome.proposed,
            file_path_str,
            channel=outcome.channel,
            accept=accept,
            mapping=mapping,
            save_format=save_format,
            account_id=account_id,
            account_name=account_name,
            account_bindings=account_bindings,
            account_metadata=account_metadata,
        )
        return

    quoted_path = shlex.quote(file_path_str)
    tier = outcome.confidence.tier
    tier_icon = {"high": "✅", "medium": "⚠️", "low": "❓"}.get(tier, "❓")

    typer.echo(f"\n{tier_icon}  Confirmation required ({tier} confidence)")
    typer.echo(f"   File: {file_path_str}")
    typer.echo(f"   Reason: {outcome.reason}")
    if outcome.error_message:
        typer.echo(f"   ❌ Validation failed: {outcome.error_message}")

    if isinstance(outcome.proposed, ProposedMapping):
        typer.echo("\n   Proposed column mapping:")
        for dest, src in outcome.proposed.field_mapping.items():
            samples = outcome.samples.get(dest, [])[:3]
            sample_str = (
                f"  (e.g. {', '.join(str(s) for s in samples)})" if samples else ""
            )
            typer.echo(f"     {dest} ← {src}{sample_str}")

        if outcome.confidence.flagged:
            typer.echo(
                f"\n   ⚠️  Flagged fields: {', '.join(outcome.confidence.flagged)}"
            )
        if outcome.confidence.missing_required:
            typer.echo(
                f"   ❌ Missing required fields: "
                f"{', '.join(outcome.confidence.missing_required)}"
            )
        if outcome.proposed.unmapped_columns:
            typer.echo(
                f"   Unmapped source columns: "
                f"{', '.join(outcome.proposed.unmapped_columns)}"
            )

    # account_confirmation: the layout is settled; show the source keys +
    # candidate accounts the user must reference in --account-binding (without
    # this, an interactive user has no visible path to complete the binding).
    if outcome.reason == "account_confirmation":
        _echo_account_proposals(outcome, err=False)

    typer.echo("\n   To proceed:")
    # Suggested commands shlex-quoted so paths with spaces survive copy-paste.
    if outcome.reason == "account_confirmation":
        # Replay prior confirmation inputs because retries persist no partial state.
        typer.echo(
            "     "
            + _account_recovery_command(
                file_path_str,
                outcome,
                accept=accept,
                mapping=mapping,
                save_format=save_format,
                account_id=account_id,
                account_name=account_name,
                account_bindings=account_bindings,
                account_metadata=account_metadata,
                confirm_sign=confirm_sign,
            )
        )
    else:
        # Accept hint is gated on tier — resolve_or_confirm refuses Accept at
        # the low-tier gate, so suggesting --confirm there would loop.
        if tier != "low":
            typer.echo(f"     moneybin import files {quoted_path} --confirm")
        typer.echo(
            f"     moneybin import files {quoted_path} --mapping description=<column>"
        )
        if tier != "low":
            typer.echo(
                f"     moneybin import confirm {quoted_path} --accept   "
                "(dedicated confirm subcommand)"
            )
    typer.echo(
        f"     moneybin import preview {quoted_path}   (inspect proposal in detail)"
    )
    typer.echo()


@app.command("confirm")
def import_confirm_command(
    file_path: Path = typer.Argument(..., help="Path to the file to confirm."),
    accept: bool = typer.Option(
        False,
        "--accept",
        help="Accept the detected mapping as-is.",
    ),
    mapping: list[str] = typer.Option(
        None,
        "--mapping",
        help="Partial-merge override (repeatable): --mapping field=column.",
    ),
    bridge_response: Path | None = typer.Option(
        None,
        "--bridge-response",
        help="JSON file containing a PDF bridge {recipe, rows} response.",
    ),
    confirm: bool = typer.Option(
        False,
        "--confirm",
        help="Confirm a PDF bridge recipe's ledger-wide sign inversion.",
    ),
    confirm_sign: bool = typer.Option(
        False,
        "--confirm-sign",
        help="Explicitly approve an inferred tabular sign inversion.",
    ),
    account_id: str | None = typer.Option(
        None,
        "--account-id",
        help="Account ID to associate with imported transactions.",
    ),
    account_name: str | None = typer.Option(
        None,
        "--account-name",
        help="Account name to associate with imported transactions.",
    ),
    account_binding: list[str] = typer.Option(
        None,
        "--account-binding",
        help=(
            "Ratify an account_confirmation (repeatable): "
            "--account-binding source_key=ACCOUNT_ID to adopt an existing "
            "account, or source_key=new to mint a distinct new account. "
            "Keys come from confirmation_required account_proposals. On retry, "
            "re-supply ALL bindings — no partial state persists between calls."
        ),
    ),
    account_meta: list[str] = typer.Option(
        None,
        "--account-meta",
        help=(
            "Metadata for a 'new' account (repeatable): "
            "--account-meta source_key:field=value, where field is one of "
            "display_name, account_subtype, last_four, iso_currency_code."
        ),
    ),
    save_format: bool = typer.Option(
        True,
        "--save-format/--no-save-format",
        help="Auto-save the confirmed mapping as a named format for future imports.",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Accept or override the proposed mapping for a file awaiting confirmation.

    Use after 'import files' returns confirmation_required.  Pass --accept to
    ratify the detected mapping as-is, or supply --mapping field=column (repeatable)
    to override specific destination fields.

    Examples:
        moneybin import confirm ~/Downloads/statement.csv --accept
        moneybin import confirm ~/Downloads/statement.csv --mapping description=Memo
        moneybin import confirm ~/Downloads/statement.csv --mapping date=Date --mapping amount=Amount
        moneybin import confirm ~/Downloads/statement.csv --accept --output json
        moneybin import confirm ~/Downloads/statement.csv --accept --account-name "Chase Checking"
        moneybin import confirm ~/Downloads/card.csv --accept --confirm-sign
        moneybin import confirm ~/Downloads/card.pdf --bridge-response response.json --confirm
    """
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.cli.utils import handle_cli_errors  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.import_service import ImportService  # noqa: PLC0415

    if bridge_response is not None:
        if accept or mapping or confirm_sign:
            raise typer.BadParameter(
                "--bridge-response cannot be combined with --accept, --mapping, or "
                "--confirm-sign.",
                param_hint="'--bridge-response'",
            )
        if account_name or account_binding or account_meta:
            raise typer.BadParameter(
                "--bridge-response supports --account-id only; PDF rows do not use "
                "--account-name, --account-binding, or --account-meta.",
                param_hint="'--bridge-response'",
            )
        if not confirm:
            raise typer.BadParameter(
                "--bridge-response requires --confirm because its recipe may invert "
                "every amount in the statement.",
                param_hint="'--confirm'",
            )
    elif confirm:
        raise typer.BadParameter(
            "--confirm is only valid with --bridge-response; use --accept for a "
            "tabular mapping.",
            param_hint="'--confirm'",
        )
    elif not accept and not mapping:
        raise typer.BadParameter(
            "Pass --accept to ratify the proposed mapping, or at least one "
            "--mapping field=column to override specific fields.",
            param_hint="'--accept' or '--mapping'",
        )

    if not file_path.exists():
        logger.error(f"❌ File not found: {file_path}")
        raise typer.Exit(1)

    bridge_response_data: dict[str, Any] | None = None
    if bridge_response is not None:
        try:
            parsed_response = json.loads(bridge_response.read_text(encoding="utf-8"))
        except OSError as e:
            raise typer.BadParameter(
                f"Could not read bridge response: {e}",
                param_hint="'--bridge-response'",
            ) from e
        except json.JSONDecodeError as e:
            raise typer.BadParameter(
                f"Bridge response must be valid JSON: {e.msg}",
                param_hint="'--bridge-response'",
            ) from e
        if not isinstance(parsed_response, dict):
            raise typer.BadParameter(
                "Bridge response JSON must be an object with recipe and rows keys.",
                param_hint="'--bridge-response'",
            )
        bridge_response_data = parsed_response

    parsed_mapping = _parse_overrides(list(mapping)) if mapping else None
    parsed_bindings = (
        _parse_account_bindings(list(account_binding)) if account_binding else None
    )
    parsed_metadata = (
        _parse_account_metadata(list(account_meta)) if account_meta else None
    )

    from moneybin.services.import_confirmation import (
        ImportConfirmationRequiredError,
    )

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                service = ImportService(db)
                if bridge_response_data is not None:
                    bridge_result = service.apply_pdf_bridge_response(
                        file_path,
                        bridge_response_data,
                        save_format=save_format,
                        account_id=account_id,
                        confirm=True,
                    )
                    result = None
                else:
                    confirm_kwargs: dict[str, Any] = {
                        "file_path": file_path,
                        "confirm": accept,
                        "overrides": parsed_mapping,
                        "account_id": account_id,
                        "account_name": account_name,
                        "account_bindings": parsed_bindings,
                        "account_metadata": parsed_metadata,
                        "save_format": save_format,
                        "actor_kind": "human",
                        "refresh": False,
                    }
                    if confirm_sign:
                        confirm_kwargs["human_sign_confirmation"] = True
                    result = service.import_file(**confirm_kwargs)
                    bridge_result = None
    except ImportConfirmationRequiredError as e:
        # The confirm attempt itself can re-surface ConfirmationRequired —
        # e.g. an override that names an unknown source column, or a
        # low-tier proposal where the user-supplied mapping still leaves
        # required fields missing. For --output json / non-TTY callers
        # emit the same envelope shape import_files uses so agents see
        # a structured payload instead of an unparseable stderr message.
        # Exit code stays 1: the confirm action did not succeed.
        outcome = e.outcome
        envelope_data = _confirmation_envelope_data(outcome)
        confirm_actions: list[str] = []
        if outcome.error_message:
            confirm_actions.append(f"Validation failed: {outcome.error_message}")
        if outcome.reason == "sign_convention":
            confirm_actions.extend(
                _sign_recovery_commands(
                    str(file_path),
                    channel=outcome.channel,
                    accept=accept,
                    mapping=parsed_mapping,
                    save_format=save_format,
                    account_id=account_id,
                    account_name=account_name,
                    account_bindings=parsed_bindings,
                    account_metadata=parsed_metadata,
                )
            )
        elif outcome.reason == "account_confirmation":
            # The layout is settled; only the account identity needs ratifying.
            # Replay the current confirmation inputs because retries persist no
            # partial state, and add the missing binding. Generic alternate
            # mapping hints remain irrelevant here.
            confirm_actions.append(
                f"Re-run `{_account_recovery_command(str(file_path), outcome, accept=accept, mapping=parsed_mapping, save_format=save_format, account_id=account_id, account_name=account_name, account_bindings=parsed_bindings, account_metadata=parsed_metadata, confirm_sign=confirm_sign)}` "
                "to bind each proposed account (adopt an existing id, or 'new' "
                "to keep distinct)."
            )
        else:
            confirm_actions.append(
                "Re-run with --mapping <field>=<column> to override specific fields."
            )
            if outcome.confidence.tier != "low":
                confirm_actions.append(
                    f"Re-run 'moneybin import confirm {file_path} --accept' "
                    "to accept the proposed mapping as-is."
                )
        confirm_actions.append(
            f"Run 'moneybin import preview {file_path}' to inspect the proposal."
        )
        if output == OutputFormat.JSON or not sys.stdout.isatty():
            envelope = build_envelope(
                data=envelope_data,
                sensitivity="medium",
                actions=confirm_actions,
            )
            render_or_json(
                envelope, OutputFormat.JSON, cli_actor="import_confirm_command"
            )
            # Exit 0 to mirror `moneybin import files` JSON-mode behavior on
            # confirmation_required (data.status is the discriminant).
            # Scripted propose→review→confirm loops branch on the body, not
            # exit code — a non-zero exit would abort the loop on every
            # partial-override iteration.
            return
        # Interactive path: human-readable summary + exit code 1.
        if outcome.reason == "sign_convention":
            _render_confirmation_prompt(
                outcome,
                str(file_path),
                accept=accept,
                mapping=parsed_mapping,
                save_format=save_format,
                account_id=account_id,
                account_name=account_name,
                account_bindings=parsed_bindings,
                account_metadata=parsed_metadata,
                confirm_sign=confirm_sign,
            )
        elif outcome.reason == "account_confirmation":
            # The layout is settled; replay the current inputs and add the
            # bindings still required to finish this independent call.
            logger.error("❌ Account identity must be confirmed before import.")
            _echo_account_proposals(outcome, err=True)
            logger.info(
                "💡 Re-run `"
                + _account_recovery_command(
                    str(file_path),
                    outcome,
                    accept=accept,
                    mapping=parsed_mapping,
                    save_format=save_format,
                    account_id=account_id,
                    account_name=account_name,
                    account_bindings=parsed_bindings,
                    account_metadata=parsed_metadata,
                    confirm_sign=confirm_sign,
                )
                + "`."
            )
        else:
            msg = f"❌ Confirmation failed: {outcome.reason}" + (
                f" — {outcome.error_message}" if outcome.error_message else ""
            )
            logger.error(msg)
            logger.info(
                "💡 Inspect the proposal with 'moneybin import preview "
                f"{file_path}' and re-run with a corrected --mapping."
            )
        raise typer.Exit(1) from e

    if bridge_result is not None:
        if bridge_result.outcome == "invalid":
            data = {
                "status": "invalid",
                "reject_reason": bridge_result.reject_reason,
                "expected_row_count": bridge_result.expected_row_count,
                "actual_row_count": bridge_result.actual_row_count,
                "rows_diverged": bridge_result.rows_diverged,
            }
            envelope = build_envelope(data=data, sensitivity="medium", actions=[])
            render_or_json(envelope, output, cli_actor="import_confirm_command")
            if output != OutputFormat.JSON:
                logger.error(
                    "❌ PDF bridge response did not reconcile; nothing was imported."
                )
            raise typer.Exit(1)

        from moneybin.services.inbox_service import InboxService  # noqa: PLC0415

        InboxService.for_active_profile_no_db().archive_confirmed_file(file_path)
        data = {
            "status": "applied",
            "import_id": bridge_result.import_id,
            "rows_loaded": bridge_result.rows_loaded,
            "format_name": bridge_result.format_name,
            "expected_row_count": bridge_result.expected_row_count,
            "actual_row_count": bridge_result.actual_row_count,
            "rows_diverged": bridge_result.rows_diverged,
        }
        actions = [
            f"Use 'moneybin import revert {bridge_result.import_id}' to undo this import.",
            "Run 'moneybin transform apply' to rebuild derived tables.",
            "Run 'moneybin import status' to confirm imported counts.",
        ]
        envelope = build_envelope(data=data, sensitivity="medium", actions=actions)
        render_or_json(envelope, output, cli_actor="import_confirm_command")
        if not quiet and output != OutputFormat.JSON:
            logger.info(
                f"✅ Imported {file_path.name}: {bridge_result.rows_loaded} rows "
                f"(import_id: {bridge_result.import_id})"
            )
            logger.info("💡 Run 'moneybin transform apply' to rebuild derived tables.")
        return

    result = cast("ImportResult", result)

    # Confirmed out of the inbox's pending/ bucket → archive to processed/ and
    # drop the .pending.yml sidecar (no-op for a path that never entered the
    # inbox, e.g. a file passed directly to `import files`).
    from moneybin.services.inbox_service import (
        InboxService,  # noqa: PLC0415 — defer import
    )

    InboxService.for_active_profile_no_db().archive_confirmed_file(file_path)

    if output == OutputFormat.JSON:
        data: dict[str, Any] = {
            # Mirror the confirmation_required envelope's top-level status
            # field so scripted propose→review→confirm loops branch on a
            # single discriminant (`data.status`) regardless of outcome.
            "status": "imported",
            "import_id": result.import_id,
            "rows_loaded": result.rows_loaded,
            "file_type": result.file_type,
            "sign_correction_suggested": result.sign_correction_suggested,
            # merged_mapping is authoritative (threaded from
            # ImportResult.field_mapping); agents need it to verify which
            # column mapping was actually applied without re-detecting.
            "merged_mapping": dict(result.field_mapping or {}),
        }
        actions = [
            f"Use 'moneybin import revert {result.import_id}' to undo this import.",
            "Run 'moneybin transform apply' to rebuild derived tables.",
            "Run 'moneybin import status' to confirm imported counts.",
        ]
        if result.sign_correction_suggested:
            actions.insert(
                0,
                "⚠️  Sign convention may be inverted — inspect amounts and re-import "
                "with --mapping corrected if needed.",
            )
        envelope = build_envelope(data=data, sensitivity="medium", actions=actions)
        render_or_json(envelope, output, cli_actor="import_confirm_command")
        return

    if not quiet:
        logger.info(
            f"✅ Imported {file_path.name}: {result.rows_loaded} rows "
            f"(import_id: {result.import_id})"
        )
        if result.sign_correction_suggested:
            typer.echo(
                "⚠️  Sign convention may be inverted (running balance suggests "
                "negation). If amounts look wrong, re-run with --mapping corrected.",
                err=True,
            )
        logger.info("💡 Run 'moneybin transform apply' to rebuild derived tables.")


@app.command("history")
def import_history(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
    import_id: str | None = typer.Option(
        None, "--import-id", help="Show details for a specific import"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List recent imports with batch details.

    Shows import ID, source file, status, row counts, and detection confidence
    for each completed import batch.

    Examples:
        moneybin import history
        moneybin import history --limit 50
        moneybin import history --import-id abc123
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import get_database  # noqa: PLC0415 — deferred import
    from moneybin.extractors.tabular import TabularExtractor

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            extractor = TabularExtractor(db)
            records = extractor.get_import_history(limit=limit, import_id=import_id)

    if output == OutputFormat.JSON:
        emit_json("imports", records)
        return

    if not records:
        if not quiet:
            if import_id:
                logger.warning(f"⚠️  No import found with ID: {import_id}")
            else:
                logger.warning("⚠️  No import history found")
        return

    typer.echo(
        f"\n{'Import ID':<38} {'Status':<10} {'Imported':>8} {'Rejected':>8}  {'Source File'}"
    )
    typer.echo("-" * 100)
    for rec in records:
        imp_id = str(rec.get("import_id", ""))
        status = str(rec.get("status", ""))
        rows_imported = rec.get("rows_imported") or 0
        rows_rejected = rec.get("rows_rejected") or 0
        source_file = str(rec.get("source_file", ""))
        # Truncate source file path for display
        display_path = Path(source_file).name if source_file else ""
        typer.echo(
            f"{imp_id:<38} {status:<10} {rows_imported:>8} {rows_rejected:>8}  "
            f"{display_path}"
        )

    if import_id and records:
        rec = records[0]
        typer.echo("\nDetails:")
        for key, value in rec.items():
            if value is not None:
                typer.echo(f"  {key}: {value}")
    typer.echo()


@app.command("revert")
def import_revert(
    import_id: str = typer.Argument(..., help="Import batch ID to revert"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Revert an import — deletes all rows from that batch.

    Removes all transactions and accounts loaded in the specified import batch,
    and marks the batch as reverted in the import log.

    Examples:
        moneybin import revert abc123-...
        moneybin import revert abc123-... --yes
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import get_database  # noqa: PLC0415 — deferred import
    from moneybin.services.import_service import ImportService

    if not yes:
        confirmed = typer.confirm(
            f"Revert import {import_id[:8]}...? This will delete all rows from "
            f"this batch and cannot be undone."
        )
        if not confirmed:
            logger.info("Revert cancelled")
            raise typer.Exit(0)

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            result = ImportService(db).revert(import_id)

    status = result.get("status")
    if status == "not_found":
        logger.error(f"❌ {result.get('reason', 'Import not found')}")
        raise typer.Exit(1)
    elif status == "superseded":
        logger.error(f"❌ {result.get('reason', 'Import was superseded')}")
        raise typer.Exit(1)
    elif status == "already_reverted":
        logger.warning(f"⚠️  Import {import_id[:8]}... was already reverted")
    else:
        rows_deleted = result.get("rows_deleted", 0)
        logger.info(
            f"✅ Reverted import {import_id[:8]}...: {rows_deleted} rows deleted"
        )


@app.command("preview")
def import_preview(
    file_path: str = typer.Argument(..., help="File to preview"),
    format_name: str | None = typer.Option(
        None,
        "--format",
        "-f",
        help="Use a specific named format (bypass auto-detection)",
    ),
    sheet: str | None = typer.Option(
        None, "--sheet", help="Excel sheet name (default: auto-select largest)"
    ),
    delimiter: str | None = typer.Option(
        None, "--delimiter", help="Explicit delimiter for text formats"
    ),
    encoding: str | None = typer.Option(
        None, "--encoding", help="Explicit file encoding (e.g. utf-8, latin-1)"
    ),
    override: list[str] = typer.Option(
        None,
        "--override",
        help="Field→column override, repeatable (e.g. --override date=Date)",
    ),
) -> None:
    """Preview file structure without importing.

    Runs detection and column-mapping stages without loading any data into
    the database. Shows detected format, column mapping, and sample rows.

    Examples:
        moneybin import preview ~/Downloads/chase_activity.csv
        moneybin import preview ~/Downloads/transactions.xlsx --sheet Sheet1
    """
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import read_file

    source = Path(file_path)

    if not source.exists():
        logger.error(f"❌ File not found: {source}")
        raise typer.Exit(1)

    overrides = _parse_overrides(override)

    try:
        # Stage 1: Detect format
        format_info = detect_format(
            source,
            delimiter_override=delimiter,
            encoding_override=encoding,
        )

        # Stage 2: Read file
        read_result = read_file(source, format_info, sheet=sheet)
        df = read_result.df

        if len(df) == 0:
            logger.warning(f"⚠️  No data rows found in {source.name}")
            return

        # Stage 3: Column mapping — load built-in + user-saved formats
        matched_format = None
        from moneybin.database import (  # noqa: PLC0415
            DatabaseKeyError,
            DatabaseNotInitializedError,
            get_database,
        )

        try:
            with get_database(read_only=True) as preview_db:
                all_formats, _ = _load_all_formats(preview_db)
        except (DatabaseNotInitializedError, DatabaseKeyError):
            all_formats, _ = _load_all_formats(None)
        if format_name:
            matched_format = all_formats.get(format_name)
            if matched_format is None:
                logger.warning(
                    f"⚠️  Format {format_name!r} not found in available formats"
                )
        else:
            headers = list(df.columns)
            for fmt in all_formats.values():
                if fmt.matches_headers(headers):
                    matched_format = fmt
                    break

        typer.echo(f"\nFile: {source.name}")
        typer.echo(f"Type: {format_info.file_type}")
        if format_info.delimiter:
            typer.echo(f"Delimiter: {format_info.delimiter!r}")
        typer.echo(f"Encoding: {format_info.encoding}")
        typer.echo(f"Rows: {len(df):,}")
        if read_result.rows_skipped_trailing:
            typer.echo(f"Trailing rows skipped: {read_result.rows_skipped_trailing}")
        typer.echo(f"Header row detected: {read_result.has_header}")
        typer.echo(
            f"Row reconciliation: {read_result.rows_in_file:,} in file = "
            f"{read_result.skip_rows:,} skipped + "
            f"{1 if read_result.has_header else 0} header + "
            f"{len(df):,} read + "
            f"{read_result.rows_skipped_trailing:,} trailing"
        )
        if read_result.header_row_looks_like_data:
            # A warning (diagnostic) → stderr via logger, not stdout, per
            # cli.md; the ⚠️ icon is reserved for logger.warning messages.
            logger.warning(
                "⚠️  The row consumed as the header also parses as a transaction "
                "(date + amount) — this may be a headerless file misread as having "
                "a header. Re-run with a corrected --format or check the source file."
            )
        typer.echo(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")

        if matched_format:
            typer.echo(
                f"\nMatched format: {matched_format.name} ({matched_format.institution_name})"
            )
            typer.echo(f"Sign convention: {matched_format.sign_convention}")
            typer.echo(f"Date format: {matched_format.date_format}")
            typer.echo(f"Number format: {matched_format.number_format}")
            typer.echo("\nColumn mapping:")
            for field, col in matched_format.field_mapping.items():
                typer.echo(f"  {field} ← {col}")
        else:
            from moneybin.config import get_settings  # noqa: PLC0415

            bands = get_settings().import_.confidence
            mapping_result = map_columns(
                df,
                overrides=overrides,
                t_high=bands.t_high,
                t_med=bands.t_med,
                structural_red_flag=read_result.header_row_looks_like_data,
            )
            typer.echo(f"\nDetected mapping (confidence: {mapping_result.confidence}):")
            for field, col in mapping_result.field_mapping.items():
                typer.echo(f"  {field} ← {col}")
            if mapping_result.sign_convention:
                typer.echo(f"Sign convention: {mapping_result.sign_convention}")
            if mapping_result.date_format:
                typer.echo(f"Date format: {mapping_result.date_format}")
            if mapping_result.number_format:
                typer.echo(f"Number format: {mapping_result.number_format}")

        # Show sample rows
        sample_n = min(5, len(df))
        typer.echo(f"\nSample ({sample_n} rows):")
        typer.echo(df.head(sample_n))
        typer.echo()

    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@formats_app.command("list")
def formats_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    # _type shadows the builtin `type` — Typer CLI name remains --type (A001).
    _type: _FormatTypeFilter = typer.Option(  # noqa: A002
        _FormatTypeFilter.all,
        "--type",
        help=(
            "Filter by format type: tabular (CSV/Excel/etc.), pdf, or all (default). "
            "JSON output uses a uniform list; each row carries a 'type' field. "
            "Example: --type=pdf"
        ),
    ),
) -> None:
    """List all formats (built-in and user-saved).

    Displays format name, institution, sign convention, and date format
    for tabular formats, and name, institution, routing, front-end, version,
    times-used, and last-used for PDF formats.

    Example:
        moneybin import formats list
        moneybin import formats list --type=pdf
        moneybin import formats list --type=tabular --output json
    """
    from moneybin.database import get_database

    try:
        with get_database(read_only=True) as db:
            all_formats, builtin = _load_all_formats(db)
            pdf_formats = _load_pdf_formats(db)
    except Exception:  # noqa: BLE001 — DB may not exist yet; show built-in / empty PDF
        all_formats, builtin = _load_all_formats(None)
        pdf_formats = _load_pdf_formats(None)

    show_tabular = _type in (_FormatTypeFilter.tabular, _FormatTypeFilter.all)
    show_pdf = _type in (_FormatTypeFilter.pdf, _FormatTypeFilter.all)

    if output == OutputFormat.JSON:
        # Uniform list with a 'type' discriminator per row — agents can filter
        # via jq '.formats | map(select(.type=="pdf"))' with no flag required.
        formats_payload: list[dict[str, Any]] = []
        if show_tabular:
            for fmt in sorted(all_formats.values(), key=lambda f: f.name):
                formats_payload.append({
                    "type": "tabular",
                    "name": fmt.name,
                    "institution": fmt.institution_name,
                    "sign_convention": fmt.sign_convention,
                    "date_format": fmt.date_format,
                    "source": "builtin" if fmt.name in builtin else "user",
                })
        if show_pdf:
            for pf in pdf_formats:
                last_used = (
                    pf.last_used_at.date().isoformat()
                    if pf.last_used_at is not None
                    else None
                )
                formats_payload.append({
                    "type": "pdf",
                    "name": pf.name,
                    "institution": pf.institution_name,
                    "routing": pf.routing,
                    "front_end": pf.front_end,
                    "version": pf.version,
                    "times_used": pf.times_used,
                    "last_used": last_used,
                })
        emit_json("formats", formats_payload)
        return

    # ---- Text output -------------------------------------------------------

    if show_tabular:
        if not all_formats:
            if not quiet:
                logger.warning("⚠️  No tabular formats found")
        else:
            n_builtin = len(builtin)
            n_user = len(all_formats) - len(builtin)
            section_hdr = f"Tabular formats ({n_builtin} built-in, {n_user} user-saved)"
            typer.echo(f"\n{section_hdr}")
            typer.echo(
                f"\n{'Name':<24} {'Institution':<28} {'Sign Convention':<24} "
                f"{'Date Format'}"
            )
            typer.echo("-" * 100)
            for fmt in sorted(all_formats.values(), key=lambda f: f.name):
                source_tag = " (user)" if fmt.name not in builtin else ""
                typer.echo(
                    f"{fmt.name:<24} {fmt.institution_name:<28} "
                    f"{fmt.sign_convention:<24} {fmt.date_format}{source_tag}"
                )

    if show_pdf:
        if not pdf_formats:
            if not quiet:
                if show_tabular:
                    typer.echo("")
                logger.warning("⚠️  No PDF formats found")
        else:
            if show_tabular:
                typer.echo("")
            typer.echo(f"PDF formats ({len(pdf_formats)})")
            typer.echo(
                f"\n{'Name':<28} {'Institution':<20} {'Routing':<16} "
                f"{'Front-end':<14} {'Ver':<5} {'Used':<6} {'Last used'}"
            )
            typer.echo("-" * 104)
            for pf in pdf_formats:
                last_used = (
                    pf.last_used_at.date().isoformat()
                    if pf.last_used_at is not None
                    else "—"
                )
                typer.echo(
                    f"{pf.name:<28} {pf.institution_name:<20} {pf.routing:<16} "
                    f"{pf.front_end:<14} {pf.version:<5} {pf.times_used:<6} "
                    f"{last_used}"
                )

    typer.echo("")


@formats_app.command("show")
def formats_show(
    name: str = typer.Argument(..., help="Format name to show"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — show has no info chatter; only data lines
) -> None:
    """Show details for a specific format.

    Displays the full configuration for a built-in or user-saved format,
    including column mappings, detection signature, and format options.
    If the name does not match a tabular format, the command falls through
    to the PDF format namespace before reporting not-found.

    Example:
        moneybin import formats show tiller
        moneybin import formats show chase_a1b2c3d4e5f6
    """
    from moneybin.database import get_database

    try:
        with get_database(read_only=True) as db:
            all_formats, _ = _load_all_formats(db)
            pdf_formats_list = _load_pdf_formats(db)
    except Exception:  # noqa: BLE001 — DB may not exist yet; show built-in / empty PDF
        all_formats, _ = _load_all_formats(None)
        pdf_formats_list = _load_pdf_formats(None)

    fmt = all_formats.get(name)

    # Fall through to PDF namespace if name not in tabular formats.
    pdf_fmt = next((pf for pf in pdf_formats_list if pf.name == name), None)

    if fmt is None and pdf_fmt is None:
        tabular_names = sorted(all_formats.keys())
        pdf_names = sorted(pf.name for pf in pdf_formats_list)
        all_names = tabular_names + pdf_names
        available = ", ".join(all_names) or "(none)"
        if output == OutputFormat.JSON:
            emit_json_error(
                UserError(
                    f"Format not found: {name!r}",
                    code="not_found",
                    hint=f"Available formats: {available}",
                )
            )
        else:
            logger.error(f"❌ Format not found: {name!r}")
            logger.info(f"💡 Available formats: {available}")
        raise typer.Exit(1)

    # ---- Tabular format ----
    if fmt is not None:
        if output == OutputFormat.JSON:
            payload = {
                "type": "tabular",
                "name": fmt.name,
                "institution": fmt.institution_name,
                "file_type": fmt.file_type,
                "delimiter": fmt.delimiter,
                "encoding": fmt.encoding,
                "skip_rows": fmt.skip_rows,
                "sheet": fmt.sheet,
                "sign_convention": fmt.sign_convention,
                "date_format": fmt.date_format,
                "number_format": fmt.number_format,
                "multi_account": fmt.multi_account,
                "header_signature": fmt.header_signature,
                "field_mapping": dict(fmt.field_mapping),
                "skip_trailing_patterns": fmt.skip_trailing_patterns,
            }
            emit_json("format", payload)
            return

        typer.echo(f"\nFormat: {fmt.name}")
        typer.echo(f"Institution: {fmt.institution_name}")
        typer.echo(f"File type: {fmt.file_type}")
        if fmt.delimiter:
            typer.echo(f"Delimiter: {fmt.delimiter!r}")
        typer.echo(f"Encoding: {fmt.encoding}")
        if fmt.skip_rows:
            typer.echo(f"Skip rows: {fmt.skip_rows}")
        if fmt.sheet:
            typer.echo(f"Sheet: {fmt.sheet}")
        typer.echo(f"Sign convention: {fmt.sign_convention}")
        typer.echo(f"Date format: {fmt.date_format}")
        typer.echo(f"Number format: {fmt.number_format}")
        typer.echo(f"Multi-account: {fmt.multi_account}")
        typer.echo(f"\nHeader signature: {fmt.header_signature}")
        typer.echo("\nField mapping:")
        for field, col in fmt.field_mapping.items():
            typer.echo(f"  {field} ← {col}")
        if fmt.skip_trailing_patterns:
            typer.echo(f"\nSkip trailing patterns: {fmt.skip_trailing_patterns}")
        typer.echo()
        return

    # ---- PDF format ----
    # pdf_fmt is not None — if both were None we raised above; only PDF path remains.
    if pdf_fmt is None:  # pragma: no cover — defensive; unreachable by logic above
        raise RuntimeError("pdf_fmt is None after not-found guard — logic error")
    last_used = (
        pdf_fmt.last_used_at.date().isoformat()
        if pdf_fmt.last_used_at is not None
        else None
    )
    if output == OutputFormat.JSON:
        payload_pdf: dict[str, Any] = {
            "type": "pdf",
            "name": pdf_fmt.name,
            "institution": pdf_fmt.institution_name,
            "document_kind": pdf_fmt.document_kind,
            "routing": pdf_fmt.routing,
            "front_end": pdf_fmt.front_end,
            "sign_convention": pdf_fmt.sign_convention,
            "date_format": pdf_fmt.date_format,
            "number_format": pdf_fmt.number_format,
            "version": pdf_fmt.version,
            "times_used": pdf_fmt.times_used,
            "last_used": last_used,
            "source": pdf_fmt.source,
            "extraction_recipe": pdf_fmt.extraction_recipe,
        }
        emit_json("format", payload_pdf)
        return

    typer.echo(f"\nFormat: {pdf_fmt.name}")
    typer.echo("Type: pdf")
    typer.echo(f"Institution: {pdf_fmt.institution_name}")
    typer.echo(f"Document kind: {pdf_fmt.document_kind}")
    typer.echo(f"Routing: {pdf_fmt.routing}")
    typer.echo(f"Front-end: {pdf_fmt.front_end}")
    if pdf_fmt.sign_convention:
        typer.echo(f"Sign convention: {pdf_fmt.sign_convention}")
    if pdf_fmt.date_format:
        typer.echo(f"Date format: {pdf_fmt.date_format}")
    typer.echo(f"Number format: {pdf_fmt.number_format}")
    typer.echo(f"Version: {pdf_fmt.version}  Times used: {pdf_fmt.times_used}")
    if last_used:
        typer.echo(f"Last used: {last_used}")
    typer.echo(f"Source: {pdf_fmt.source}")
    typer.echo(
        f"\nExtraction recipe:\n{json.dumps(pdf_fmt.extraction_recipe, indent=2)}"
    )
    typer.echo()


@formats_app.command("delete")
def formats_delete(
    name: str = typer.Argument(..., help="Format name to delete"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Delete a user-saved format.

    Removes a user-saved format from the database. Built-in formats cannot
    be deleted.

    Example:
        moneybin import formats delete my_custom_format
        moneybin import formats delete my_custom_format --yes
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import get_database  # noqa: PLC0415 — deferred import
    from moneybin.extractors.tabular.formats import (
        delete_format_from_db,
        load_builtin_formats,
    )

    # Check if it's a built-in format
    builtin = load_builtin_formats()
    if name in builtin:
        logger.error(f"❌ {name!r} is a built-in format and cannot be deleted")
        raise typer.Exit(1)

    if not yes:
        confirmed = typer.confirm(f"Delete format {name!r}?")
        if not confirmed:
            logger.info("Delete cancelled")
            raise typer.Exit(0)

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            deleted = delete_format_from_db(db, name, actor="cli")

    if not deleted:
        logger.error(f"❌ Format {name!r} not found")
        raise typer.Exit(1)
    logger.info(f"✅ Deleted format {name!r}")


@app.command("status")
def import_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show a summary of all imported data: row counts, date ranges, and sources.

    Queries raw tables in DuckDB to display what has been imported so far.

    Example:
        moneybin import status
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_settings
    from moneybin.database import get_database  # noqa: PLC0415 — deferred import

    db_path = get_settings().database.path

    if not db_path.exists():
        if output == OutputFormat.JSON:
            typer.echo(
                json.dumps(
                    {
                        "database": str(db_path),
                        "tables": [],
                        "exists": False,
                        "error": "database not found",
                    },
                    indent=2,
                    default=str,
                )
            )
        elif not quiet:
            logger.warning(f"Database not found: {db_path}")
            logger.info("Run 'moneybin import files <path>' to import data first.")
        # Both modes exit non-zero so machine consumers can detect missing/
        # uninitialized state. The JSON payload carries the same signal as
        # the human warning; the exit code carries it for scripts.
        raise typer.Exit(1)

    try:
        with handle_cli_errors():
            with get_database(read_only=True) as db:
                rows = _collect_import_status(db)
    except Exception as e:  # noqa: BLE001 — surface connection errors generically
        logger.error(f"❌ Could not open database: {e}")
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        typer.echo(
            json.dumps(
                {
                    "database": str(db_path),
                    "tables": [asdict(r) for r in rows],
                    "exists": True,
                },
                indent=2,
                default=str,
            )
        )
        return

    if not rows:
        if not quiet:
            typer.echo("\nNo imported data found.")
            typer.echo("   Run 'moneybin import files <path>' to get started.")
        return

    if not quiet:
        typer.echo("\nImported Data Summary")
        typer.echo("=" * 60)

    for row in rows:
        date_info = ""
        if row.date_min is not None:
            date_info = f"  ({row.date_min} to {row.date_max})"
        typer.echo(f"  {row.schema}.{row.table}: {row.rows:,} rows{date_info}")

    if not quiet:
        typer.echo()


@dataclass(frozen=True, slots=True)
class _ImportStatusRow:
    schema: str
    table: str
    rows: int
    date_min: date | None
    date_max: date | None


def _collect_import_status(db: Database) -> list[_ImportStatusRow]:
    """Query raw tables and return per-table row counts and date ranges."""
    tables = db.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
        ORDER BY table_name
    """).fetchall()

    from sqlglot import exp

    results: list[_ImportStatusRow] = []
    for schema, table in tables:
        safe_schema = exp.to_identifier(schema, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
        safe_table = exp.to_identifier(table, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
        row_count = db.execute(
            f"SELECT COUNT(*) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers
        ).fetchone()
        count = row_count[0] if row_count else 0

        date_min: date | None = None
        date_max: date | None = None
        if "transaction" in table:
            date_col = "date_posted" if "ofx" in table else "transaction_date"
            safe_date_col = exp.to_identifier(date_col, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
            try:
                dates = db.execute(
                    f"SELECT MIN(CAST({safe_date_col} AS DATE)), MAX(CAST({safe_date_col} AS DATE)) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers; date_col from hardcoded map
                ).fetchone()
                if dates and dates[0]:
                    date_min, date_max = dates[0], dates[1]
            except Exception:  # noqa: BLE001 — column may not exist in all tables
                logger.debug(f"Could not get date range for {schema}.{table}")

        results.append(
            _ImportStatusRow(
                schema=schema,
                table=table,
                rows=count,
                date_min=date_min,
                date_max=date_max,
            )
        )
    return results
