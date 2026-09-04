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
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import typer

from moneybin import error_codes
from moneybin.cli.commands import import_inbox, import_labels
from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    wide_option,
)
from moneybin.cli.render import column_view, render_rows, render_summary
from moneybin.cli.utils import (
    handle_cli_errors,
    warn_refresh_steps,
    warn_transfers_retired,
)
from moneybin.errors import UserError
from moneybin.extractors.tabular.formats import NumberFormatType, SignConventionType
from moneybin.matching.reconciliation import RETIRED_SIDES_COLLAPSED
from moneybin.services.refresh_outcome import (
    RefreshStepOutcome,
    refresh_steps_fields,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from moneybin.database import Database
    from moneybin.extractors.tabular.formats import TabularFormat
    from moneybin.privacy.payloads.imports import CLIConfirmationRequiredPayload
    from moneybin.repositories.pdf_formats_repo import PdfFormat
    from moneybin.services.import_confirmation import (
        ConfirmationRequired,
        SignConventionProposal,
    )
    from moneybin.services.import_service import (
        BatchImportResult,
        CreatedAccount,
        ImportResult,
    )


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

    Refuses one key given two different values. A dict silently kept the last,
    so the caller's earlier answer vanished with nothing said and no layer below
    ever saw a conflict to refuse — on ``--account-binding`` that settles which
    account a file attaches to by argument order, which is precisely the
    silent-attachment case the gate exists to prevent. Restating the *same*
    value is agreement, not conflict, matching the rule
    ``_apply_account_bindings`` already applies to a re-sent id.
    """
    if not values:
        return None
    result: dict[str, str] = {}
    # Masked before logging, on both refusals: --account-binding and
    # --account-meta accept a raw source key, which on OFX is the institution's
    # <ACCTID>, and logger.error persists it to a file that outlives the session
    # (.claude/rules/security.md). SanitizedLogFormatter masks recognized shapes,
    # not every issuer's numbering. Harmless for --override, whose keys are field
    # names with no digit run to find. Same mask the service-layer refusals use,
    # so a key is never disclosed to two different depths.
    from moneybin.services.import_service import (  # noqa: PLC0415 — defer import
        mask_embedded_account_number,
    )

    for raw in values:
        if "=" not in raw:
            masked = mask_embedded_account_number(raw)
            logger.error(f"❌ Invalid {flag} format (expected {fmt}): {masked!r}")
            raise typer.Exit(1)
        key, _, value = raw.partition("=")
        key, value = key.strip(), value.strip()
        if key in result and result[key] != value:
            logger.error(
                f"❌ {flag} {mask_embedded_account_number(key)!r} was given twice "
                f"with different values ({result[key]!r} and {value!r}). Send one."
            )
            raise typer.Exit(1)
        result[key] = value
    return result


def _parse_overrides(override: list[str] | None) -> dict[str, str] | None:
    """Parse and validate --override field=column values."""
    return _parse_kv(override, flag="--override", fmt="field=column")


def _parse_account_bindings(binding: list[str] | None) -> dict[str, str] | None:
    """Parse --account-binding REF=ACCOUNT_ID|new values (REF is @0 or a source key)."""
    return _parse_kv(binding, flag="--account-binding", fmt="REF=ACCOUNT_ID|new")


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
            "Run the post-load refresh pipeline (matching + transforms + "
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
            "yield nothing. Ignored for tabular and PDF files, which resolve "
            "their institution from the matched format and filename. "
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
        help=(
            "Account identifier (bypasses name matching). Single-file mode only. "
            "Not honored for OFX/QFX/QBO, which name their own accounts and can "
            "carry several — use --account-binding there; supplying it is "
            "refused rather than ignored."
        ),
    ),
    account_name: str | None = typer.Option(
        None,
        "--account-name",
        "-n",
        help="Account name for single-account tabular files. Single-file mode only.",
    ),
    account_binding: list[str] = typer.Option(
        None,
        "--account-binding",
        help=(
            "Answer an account confirmation: REF=ACCOUNT_ID|new, repeatable. "
            "REF is the proposal_ref the gate printed (@0 is the file's first "
            "source account) or that proposal's source_account_key. This is how "
            "OFX and PDF imports ratify an account identity; a tabular file "
            "answers through 'import confirm' instead, because its confirmation "
            "also stages a column mapping. For a file already sitting in the "
            "inbox's pending/, answer with 'import confirm' too: only that "
            "command archives the file afterward, so answering here imports the "
            "data but leaves the file pending and the next 'import inbox' "
            "re-offers it. Single-file mode only."
        ),
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
    from moneybin.protocol.import_envelope import mark_total_failure
    from moneybin.services.import_service import ImportService

    # --mapping is an alias for --override; merge both into one dict.
    combined_override = list(override or []) + list(mapping or [])
    overrides = _parse_overrides(combined_override or None)
    account_bindings = _parse_account_bindings(list(account_binding or []) or None)
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
    if len(file_paths) > 1 and account_bindings is not None:
        # Deliberately NOT in has_single_file_knobs above. Everything in that
        # warn-and-drop set is an override; this is an ANSWER to a gate, and a
        # dropped answer returns the identical account_confirmation on every
        # re-run. A source key is also only unambiguous within one file, and the
        # batch path can't route bindings per-file. `import_files` refuses the
        # same input for the same reason.
        raise typer.BadParameter(
            "--account-binding answers one file's account confirmation; run it "
            "with a single file. Each file's source keys are its own.",
            param_hint="'--account-binding'",
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
    # Declared out here, not in the batch branch, so the total-failure gate
    # after the try block can reach it. The single-file path also sets it, from
    # a synthesized one-file batch, when the file itself fails — that is what
    # gives both paths the same counts, files[] rows, status, and exit code.
    batch_result: BatchImportResult | None = None
    # Tracked separately from `batch_result`, which the single-file *success*
    # path deliberately never assigns (it would drag `mark_total_failure` onto a
    # successful import). That is the same trap `transfers_retired` sidesteps by
    # reading from `data`, and the most common invocation is the one it drops.
    refresh_steps: RefreshStepOutcome | None = None
    try:
        with handle_cli_errors(cli_actor="import_files_command"):
            # Single-file invocations keep fast-fail on missing paths (typo
            # detection), before the database is opened. Multi-file batches
            # defer to ImportService.import_files(), which records a per-file
            # FileNotFoundError as a PerFileResult so the batch contract
            # ("per-file failures do not abort the batch") holds.
            #
            # `Path.exists()` is itself a classified-error site: pathlib only
            # swallows ENOENT/ENOTDIR/EBADF/ELOOP, so a macOS TCC denial
            # (EPERM) propagates out of `.exists()` rather than returning
            # False. That makes THIS the line the headline single-file TCC
            # case reaches — not `svc.import_file` below — so it has to
            # produce the same one-file batch, or the one scenario this
            # affordance exists for is the only one answering `data: []`
            # while every sibling answers `data.files[]`.
            if len(file_paths) == 1:
                try:
                    missing = not file_paths[0].exists()
                except PermissionError as preflight_exc:
                    batch_result = _single_file_failure(file_paths[0], preflight_exc)
                    files_list, data = _batch_payload(batch_result)
                else:
                    if missing:
                        # A path that does not exist has no import outcome to
                        # report, so this stays a bare error rather than a
                        # failed row: there is nothing actionable in
                        # `data.files[]` that the message does not already say.
                        logger.error(f"❌ File not found: {file_paths[0]}")
                        raise typer.Exit(1)

            # Skipped when the preflight already failed: the file was never
            # opened, so there is nothing to import and no reason to take a
            # write connection.
            if batch_result is None:
                with get_database(read_only=False) as db:
                    svc = ImportService(db)
                    # Single-path invocations always use import_file directly
                    # so ImportConfirmationRequiredError can bubble to the CLI
                    # handler. Multi-path stays on import_files (batch
                    # contract).
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
                            "account_bindings": account_bindings,
                        }
                        if confirm_sign:
                            import_kwargs["human_sign_confirmation"] = True
                        try:
                            result = svc.import_file(**import_kwargs)
                        except (ValueError, PermissionError) as file_exc:
                            # A failure attributable to THIS FILE is part of the
                            # import contract, not a command-level error: the batch
                            # path and the MCP tool both answer with counts plus a
                            # data.files[] row. Letting handle_cli_errors claim it
                            # answers `data: []` instead, so the per-file error,
                            # code, and hint reach scripted callers on every path
                            # but the single-file one — the most common invocation,
                            # and the one this change's recovery advice targets.
                            #
                            # Deliberately narrow: a locked or uninitialised
                            # database is not a failed file, and reporting it as
                            # `failed_count: 1` would blame a file that was never
                            # read. Those stay with handle_cli_errors, which is
                            # also where anything unclassified still surfaces.
                            batch_result = _single_file_failure(file_paths[0], file_exc)
                            files_list, data = _batch_payload(batch_result)
                        else:
                            if result.sign_correction_suggested:
                                typer.echo(
                                    "⚠️  Sign convention may be inverted (running "
                                    "balance suggests negation). If amounts look "
                                    "wrong, re-run with --sign to override.",
                                    err=True,
                                )
                            if result.sign_override_replayed:
                                typer.echo(_SIGN_OVERRIDE_REPLAYED_NOTE, err=True)
                            # Through the batch projector rather than an inline
                            # dict: the per-file row is a public contract agents
                            # branch on, and a second copy of it is how
                            # `error`/`hint` once reached the batch path only.
                            synthesized = _single_file_success(
                                file_paths[0], result, refresh
                            )
                            files_list, data = _batch_payload(synthesized)
                            refresh_steps = synthesized.refresh_steps
                    else:
                        batch_result = svc.import_files(
                            [str(p) for p in file_paths],
                            refresh=refresh,
                            force=force,
                            interactive=interactive,
                            confirm=confirm,
                            actor_kind="human",
                        )
                        if any(
                            r.sign_correction_suggested for r in batch_result.per_file
                        ):
                            typer.echo(
                                "⚠️  Sign convention may be inverted for one or "
                                "more imports (running balance suggests negation). "
                                "If amounts look wrong, re-run with --sign to "
                                "override.",
                                err=True,
                            )
                        if any(r.sign_override_replayed for r in batch_result.per_file):
                            typer.echo(_SIGN_OVERRIDE_REPLAYED_NOTE, err=True)
                        files_list, data = _batch_payload(batch_result)
                        refresh_steps = batch_result.refresh_steps
    except Exception as _exc:  # noqa: BLE001 — dispatch on type below
        from moneybin.services.import_confirmation import (  # noqa: PLC0415
            ImportConfirmationRequiredError,
            header_row_consumed_recovery,
            unreadable_date_recovery,
        )
        from moneybin.services.import_service import (  # noqa: PLC0415 — defer import
            ImportRefreshError,
        )

        # Ahead of every dispatch below, because none of them reach the
        # success-path warning further down: the refresh reconciled inside its
        # match step and committed there, so these reversals outlived the
        # transform failure that raised. A decision the *user* made being undone
        # is disclosed whether or not what followed it succeeded.
        if isinstance(_exc, ImportRefreshError):
            warn_transfers_retired(
                _exc.transfers_retired, cause=RETIRED_SIDES_COLLAPSED
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
                proposed_sign, prior_sign = _sign_direction(outcome)
                confirm_actions.extend(
                    _sign_recovery_commands(
                        file_path_str,
                        channel=outcome.channel,
                        accept=confirm or overrides is None,
                        mapping=overrides,
                        save_format=save_format,
                        institution=institution,
                        account_id=account_id,
                        account_name=account_name,
                        account_bindings=account_bindings,
                        proposed_sign=proposed_sign,
                        prior_sign=prior_sign,
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
                    # account_bindings rides along: retries persist no partial
                    # state, so a two-account file answered one at a time needs
                    # every binding re-sent together or the printed command
                    # drops the answer already given and never converges.
                    confirm_actions.append(
                        f"Run `{_account_recovery_command(file_path_str, outcome, accept=confirm or overrides is None, mapping=overrides, save_format=save_format, institution=institution, account_id=account_id, account_name=account_name, confirm_sign=confirm_sign, sign=sign)}` "
                        "to bind each proposed account (adopt an existing id, or "
                        "'new' to keep distinct)."
                    )
                elif outcome.reason == "header_row_consumed":
                    confirm_actions.append(header_row_consumed_recovery())
                elif outcome.reason == "unreadable_date":
                    confirm_actions.append(unreadable_date_recovery(file_path_str))
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
                # Same rule as the inbox subfolder recovery: an action is only
                # worth printing on a channel that can run it.
                if _can_preview(outcome):
                    confirm_actions.append(
                        f"Run 'moneybin import preview {file_path_str}' to inspect "
                        "the proposal."
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
                institution=institution,
                account_id=account_id,
                account_name=account_name,
                # Same reason the non-TTY branch above replays them: retries
                # persist no partial state, so a command that omits the answers
                # already given re-raises the gate they satisfied.
                account_bindings=account_bindings,
                confirm_sign=confirm_sign,
                sign=sign,
            )
            raise typer.Exit(1) from _exc

        # Everything else belongs to handle_cli_errors: it classifies every
        # exception it recognizes (the Database*Errors, and any ValueError or
        # PermissionError the single-file branch did not already claim as a
        # per-file failure) into a structured error envelope and a typer.Exit,
        # and re-raises what it does not. Only ImportConfirmationRequiredError
        # — unclassified by design, so it can carry its proposal — reaches the
        # branch above.
        raise

    # Bump sensitivity to "medium" when any per-file entry carries a
    # confirmation_payload — those payloads include detector samples
    # (description / merchant cells) and must match the single-file
    # confirmation_required envelope's medium tier so agents apply the
    # same consent gate to batch proposals.
    # `error` and `hint` are DESCRIPTION-tier in `ImportPerFileRow`, which is
    # what the MCP path derives its tier from. They are prose that can name the
    # failing path, so a batch carrying either is medium on this surface too —
    # otherwise the same bytes ship as `low` from the CLI and `medium` from
    # MCP, and the privacy-audit row inherits the under-declaration.
    # `accounts_created` is here for the same reason: its `display_name` is
    # USER_NOTE in `ImportCreatedAccount`, but this path builds a bare dict, so
    # nothing downstream can re-derive that tier from the annotation.
    batch_sensitivity = (
        "medium"
        if any(
            f.get("confirmation_payload")
            or f.get("error")
            or f.get("hint")
            or f.get("accounts_created")
            for f in files_list
        )
        else "low"
    )

    # An account gate outranks all of the above: `source_account_key` is
    # ACCOUNT_IDENTIFIER, and on OFX it is the <ACCTID> the institution issued.
    # Masking it (above) is only half the contract — a bare dict lets
    # `render_or_json` derive neither the tier nor `classes_returned`, so the
    # same bytes MCP's typed payload calls critical would ship as medium here
    # and the privacy-audit row would inherit the under-declaration (cli.md:
    # the redaction contract is the same on both surfaces). Both values come
    # from `ImportConfirmationPayload` itself, so a change to its declarations
    # moves this surface with it rather than leaving a literal behind.
    def _gates_an_account(row: dict[str, Any]) -> bool:
        payload = row.get("confirmation_payload")
        if not isinstance(payload, dict):
            return False
        return bool(cast("dict[str, Any]", payload).get("account_proposals"))

    classes_returned: list[str] | None = None
    if any(_gates_an_account(f) for f in files_list):
        from moneybin.privacy.classified_envelope import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
            classify,
        )
        from moneybin.privacy.payloads.imports import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
            ImportConfirmationPayload,
        )

        # The shared classification primitive, not a second inline derivation:
        # one function answers "what tier and classes does this payload carry"
        # on every surface, so a change to the declarations moves them together.
        classification = classify(ImportConfirmationPayload)
        batch_sensitivity = classification.sensitivity
        classes_returned = classification.classes_returned
    envelope = build_envelope(data=data, sensitivity=batch_sensitivity)
    if batch_result is not None:
        # Same gate the MCP import_files tool applies, from the same function:
        # "every file failed" must read as a failure on both surfaces, or a
        # script checking .status proceeds as though the data landed.
        envelope = mark_total_failure(envelope, batch_result)
    if output == OutputFormat.JSON:
        render_or_json(
            envelope,
            output,
            cli_actor="import_files_command",
            classes_returned=classes_returned,
        )
    elif not quiet:
        for f in files_list:
            icon = "✅" if f["status"] == "imported" else "❌"
            label = f["source_type"] or "?"
            rows = f.get("rows_loaded") or 0
            logger.info(f"{icon} {f['path']} [{label}] — {rows} rows")
            # A failed row's whole value is why it failed and how to fix it.
            # Text mode is the CLI default, so leaving these to the JSON branch
            # made the recovery advice invisible to anyone running the bare
            # command — the exact scenario this classification exists for.
            if error := f.get("error"):
                logger.error(f"   {error}")
            if hint := f.get("hint"):
                logger.info(f"   {hint}")
            echo_accounts_created(f.get("accounts_created") or [])
        if data["transforms_applied"]:
            duration = data["transforms_duration_seconds"]
            if duration is not None:
                logger.info(f"✅ Core tables rebuilt in {duration:.1f}s")
            else:
                logger.info("✅ Core tables rebuilt")
        if data.get("transforms_error"):
            logger.warning(f"⚠️  Transform apply failed: {data['transforms_error']}")

    # The import's refresh runs the matcher, so folding a duplicate can reverse
    # a transfer the user accepted. Same helper and sentence as the matcher
    # commands: the event is identical, only the surface differs. Outside both
    # branches above for the reason `refresh.py` states — this is a decision the
    # *user* made being undone, not a status line, so it survives --quiet and is
    # emitted alongside JSON (where the count is in the payload too).
    warn_transfers_retired(
        int(data.get("transfers_retired") or 0), cause=RETIRED_SIDES_COLLAPSED
    )
    # Outside both branches for the same reason: the import reached the network
    # for exchange rates and ran three other best-effort steps on the user's
    # behalf, and each carries a remedy the count above does not.
    warn_refresh_steps(refresh_steps)

    # Batch import succeeds file-by-file but the post-import SQLMesh apply is
    # a separate failure surface. Exit non-zero so scripts and agents detect
    # that core tables were not refreshed even when every file imported.
    # Mirrors the fail-loud single-file path that raises on refresh() error.
    #
    # The envelope's own status covers the other batch-level failure: an
    # all-failed batch. Reading it (rather than re-deriving the condition)
    # keeps the exit code and the reported status from disagreeing.
    if data.get("transforms_error") or envelope.status == "error":
        raise typer.Exit(1)


def _batch_payload(
    batch: BatchImportResult,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Project a batch result into the ``files[]`` rows and ``data`` dict.

    One builder for every path that reports a batch — the multi-file import and
    the synthesized single-file failure — because the per-file row shape is a
    public contract that agents branch on. Two inline copies is how `error` and
    `hint` reached one surface and not the other.

    The rows stay a bare dict rather than becoming the typed payload MCP uses,
    because the omitted keys below are themselves the contract: a dataclass
    emits every field, so `error`, `hint`, `accounts_created`, and
    `confirmation_payload` would start arriving as `null` on every row. That is
    why the one field carrying a real account number is masked explicitly.
    """
    from moneybin.privacy.payloads.imports import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
        ImportConfirmationPayload,
    )
    from moneybin.privacy.redaction import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
        redact_typed,
    )

    files_list = [
        {
            "path": r.path,
            "status": r.status,
            "source_type": r.source_type,
            "rows_loaded": r.rows_loaded,
            "import_id": r.import_id,
            # Always include sign_correction_suggested so JSON-output agents
            # see a structured signal that amounts may need re-import with
            # --sign — the TTY path already warns to stderr; this closes the
            # gap for scripted callers.
            "sign_correction_suggested": r.sign_correction_suggested,
            "sign_override_replayed": r.sign_override_replayed,
            # Omitted rather than emitted empty: this key means "an account you
            # have never seen now exists", and a key present on every row stops
            # reading as news. Same reason `error` and `hint` are conditional.
            **(
                {
                    "accounts_created": [
                        {"account_id": a.account_id, "display_name": a.display_name}
                        for a in r.accounts_created
                    ]
                }
                if r.accounts_created
                else {}
            ),
            **({"error": r.error} if r.error else {}),
            # Paired with "error" so scripted/agent callers get the same stable
            # code the MCP files[] rows carry.
            **({"error_code": r.error_code} if r.error_code else {}),
            # The recovery advice travels with the code — a scripted caller
            # hitting a TCC block needs the fix, not just the classification.
            **({"hint": r.hint} if r.hint else {}),
            # The structured facts behind the code (errno, platform,
            # protected_root). `hint` says it in prose; this is what a script
            # branches on without matching that prose.
            **({"details": r.details} if r.details else {}),
            # Masked here rather than by the envelope walk, which never reaches
            # it: `render_or_json` starts that walk only when
            # `type(envelope.data)` declares a transform, and this payload is a
            # bare dict that declares nothing. On the OFX channel
            # `account_proposals[].source_account_key` is the <ACCTID> the
            # institution issued, so skipping the walk shipped a real account
            # number. Masking by the shared `ImportConfirmationPayload`
            # declarations — the same ones MCP's typed row uses — rather than by
            # a field list here, so one declaration still governs both surfaces.
            **(
                {
                    "confirmation_payload": redact_typed(
                        r.confirmation_payload,
                        consent=None,
                        declared_type=ImportConfirmationPayload,
                    )
                }
                if r.confirmation_payload
                else {}
            ),
        }
        for r in batch.per_file
    ]
    data: dict[str, Any] = {
        "imported_count": batch.imported_count,
        "failed_count": batch.failed_count,
        "total_count": batch.total_count,
        "transforms_applied": batch.transforms_applied,
        "transforms_duration_seconds": batch.transforms_duration_seconds,
        "transfers_retired": batch.transfers_retired,
        "files": files_list,
        # Flat, and named as every other surface names them: the import ran a
        # matcher, a categorizer, an identity pass and a network rate backfill,
        # and `transforms_error` below reports only the SQLMesh apply.
        **refresh_steps_fields(batch.refresh_steps),
    }
    if batch.transforms_error:
        data["transforms_error"] = batch.transforms_error
    return files_list, data


def _accounts_created_payload(
    accounts: Sequence[CreatedAccount],
) -> list[dict[str, str]]:
    """Project minted accounts for a JSON envelope."""
    return [
        {"account_id": a.account_id, "display_name": a.display_name} for a in accounts
    ]


def echo_accounts_created(accounts: Sequence[dict[str, str]]) -> None:
    """Name the accounts an import created, and how to correct one.

    This is the visible half of "gate the merge, not the mint": a first-contact
    mint no longer stops the import, so it has to announce itself instead. Both
    recoveries are named because they are different commands — a wrong *name* is
    a rename, a wrong *identity* is a merge — and neither is guessable.

    ``typer.echo``, not ``logger.info``: a display_name is the resolved account
    label ``core.dim_accounts`` stores, which a user override can turn into
    anything they typed — including the holder's name — so it must not reach the
    log pipeline.
    """
    if not accounts:
        return
    for account in accounts:
        typer.echo(
            f"👀 Created account: {account['display_name']} ({account['account_id']})",
            err=True,
        )
    typer.echo(
        # --display-name takes a value; ending the hint at the flag prints a
        # command that exits on a missing option value.
        "   Rename with 'moneybin accounts set <account_id> --display-name <name>'; "
        "if it duplicates an account you already have, "
        "'moneybin accounts links run' proposes the merge — and if that "
        "proposes nothing, the pair shares no signal, so name it yourself with "
        "'moneybin accounts links run <account_id> <candidate_account_id>'.",
        err=True,
    )


def _single_file_success(
    file_path: Path, result: ImportResult, refresh: bool
) -> BatchImportResult:
    """Wrap a successful single-file import as the one-file batch it really is.

    Twin of ``_single_file_failure``, and for the same reason: both feed
    ``_batch_payload`` so `moneybin import files a.csv` and
    `moneybin import files a.csv b.csv` describe a file identically.
    """
    from moneybin.services.import_service import (  # noqa: PLC0415 — defer import
        BatchImportResult,
        PerFileResult,
    )

    return BatchImportResult(
        per_file=[
            PerFileResult(
                path=str(file_path),
                status="imported",
                source_type=result.file_type,
                rows_loaded=result.rows_loaded,
                import_id=result.import_id,
                sign_correction_suggested=result.sign_correction_suggested,
                sign_override_replayed=result.sign_override_replayed,
                accounts_created=result.accounts_created,
            )
        ],
        transforms_applied=refresh and result.core_tables_rebuilt,
        transforms_duration_seconds=None,
        # The single-file refresh reaches the same reconciliation the batch one
        # does, so the count has to ride onto the batch this synthesizes —
        # everything downstream reads it from here, not from ImportResult.
        transfers_retired=result.transfers_retired,
        # And the rest of that refresh, for the same reason.
        refresh_steps=result.refresh_steps,
    )


def _single_file_failure(file_path: Path, exc: Exception) -> BatchImportResult:
    """Wrap a file-attributable failure as the one-file batch it really is.

    Mirrors the MCP ``import_files`` single-file branch, down to sharing
    ``per_file_failure`` — the classified message, code, and hint must be
    identical whichever surface asked. ``per_file_failure`` is also what keeps
    raw ``str(e)`` off the wire for anything it cannot classify.
    """
    from moneybin.services.import_service import (  # noqa: PLC0415 — defer import
        BatchImportResult,
        PerFileResult,
        per_file_failure,
    )

    error_message, error_code, error_hint, error_details = per_file_failure(exc)
    # The class name, never the message: a classified message is user-safe but
    # still names the path, and logs stay PII-free.
    logger.warning(f"Import failed for one file: {type(exc).__name__}")
    return BatchImportResult(
        per_file=[
            PerFileResult(
                path=str(file_path),
                status="failed",
                source_type=None,
                rows_loaded=0,
                import_id=None,
                error=error_message,
                error_code=error_code,
                hint=error_hint,
                details=error_details,
            )
        ],
        transforms_applied=False,
        transforms_duration_seconds=None,
    )


def _confirmation_envelope_data(
    outcome: ConfirmationRequired,
) -> CLIConfirmationRequiredPayload:
    """Build the ``confirmation_required`` envelope ``data`` from an outcome.

    Shared by ``import files`` and ``import confirm`` so the JSON shape cannot
    drift between the two surfaces. Delegates to the canonical
    ``confirmation_payload_dict`` — the single source MCP and the batch service
    also use — so a new channel field (e.g. ``bridge_payload``) lands in one
    place; this wrapper only prepends the CLI-envelope ``status`` field. The
    per-command ``actions[]`` hints differ (files-level vs confirm-subcommand
    context) and stay in the callers.

    Typed, not the bare dict it used to be: ``render_or_json`` derives both the
    redaction walk and the audit log's ``classes_returned`` from
    ``type(envelope.data)``, and a bare dict declares nothing. That shipped the
    institution's own account number — the OFX ``<ACCTID>`` behind
    ``account_proposals[].source_account_key`` — unmasked, and recorded no
    classes for it. MoneyBin's own minted account ids stay readable through the
    same walk: they are RECORD_ID, and they are what an answer names.
    """
    from moneybin.privacy.payloads.imports import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
        CLIConfirmationRequiredPayload,
    )
    from moneybin.services.import_confirmation import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
        confirmation_payload_dict,
    )

    return CLIConfirmationRequiredPayload(
        status="confirmation_required",
        **cast(Any, confirmation_payload_dict(outcome)),
    )


def format_account_candidate(candidate: Mapping[str, object]) -> str:
    """Format one account candidate with any available ledger evidence."""
    account_id = str(candidate.get("account_id", ""))
    display_name = str(candidate.get("display_name", ""))
    signal = str(candidate.get("signal", ""))
    rendered = f"{account_id}  ({display_name}, {signal})"

    matched = candidate.get("overlap_matched")
    comparable = candidate.get("overlap_comparable")
    if type(matched) is not int or type(comparable) is not int:
        return rendered
    if comparable == 0:
        return f"{rendered} · ledger overlap: no comparable transactions"

    overlap = f"ledger overlap: {matched}/{comparable} matched"
    # The posting-lag tolerance, when the payload states it: "2/2 matched" reads
    # as exact-date agreement otherwise, which is a stronger claim than the
    # probe makes.
    window_days = candidate.get("overlap_window_days")
    if type(window_days) is int:
        overlap += f" within ±{window_days}d"
    window_start = candidate.get("overlap_window_start")
    window_end = candidate.get("overlap_window_end")
    if isinstance(window_start, str) and isinstance(window_end, str):
        overlap += f" ({window_start} to {window_end})"
    return f"{rendered} · {overlap}"


def _echo_account_proposals(outcome: ConfirmationRequired, *, err: bool) -> None:
    """Print the source keys + candidate accounts for an account_confirmation.

    Shared by the interactive `import files` prompt (stdout) and the `import
    confirm` error path (stderr) so the binding info a user must reference never
    diverges between the two surfaces.

    The terminal gets the same masking the JSON envelope gets. It does not reach
    ``render_or_json``, so no redaction walk runs here on its own — and on the
    OFX channel ``source_account_key`` is the ``<ACCTID>`` the institution
    issued, which this gate is what first routes through this function.
    `.claude/rules/cli.md` allows no CLI exemption: "never assume CLI users are
    'trusted enough to skip redaction'".

    The masked key is printed as context, never as an answer — it is a
    disambiguator when one file proposes several accounts, and it is not
    typeable. ``proposal_ref`` is the answer, on every channel.
    """
    from moneybin.privacy.payloads.imports import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
        ImportConfirmationAccountProposal,
    )
    from moneybin.privacy.redaction import (  # noqa: PLC0415 — defer import to keep CLI cold-start light
        redact_typed,
    )

    if not outcome.account_proposals:
        return
    typer.echo("\n   Account binding required:", err=err)
    for p in outcome.account_proposals:
        masked = redact_typed(
            p, consent=None, declared_type=ImportConfirmationAccountProposal
        )
        # Lead with the ref: it is what --account-binding takes on both
        # surfaces, and the only half of this line an MCP caller can read.
        typer.echo(
            f"     {p['proposal_ref']}  account: {masked['source_account_key']}",
            err=err,
        )
        for c in p["candidates"]:
            typer.echo(
                f"       candidate: {format_account_candidate(c)}",
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


def _import_files_account_args(
    *,
    institution: str | None,
    account_id: str | None,
    account_name: str | None,
    account_bindings: dict[str, str] | None,
    account_metadata: dict[str, dict[str, str]] | None,
) -> str:
    """Serialize the account-identity options onto an ``import files`` recovery.

    The `import confirm` twin of this lives in ``_import_confirm_command``; this
    one exists because the sign recoveries re-run ``import files`` instead. Both
    gates can fire on one file, and the sign gate goes first on PDF — so a sign
    recovery is routinely the command a caller re-runs while still holding an
    account pin they must not lose.

    Returns a leading-space-prefixed fragment (or ``""``) so callers can splice
    it into a sentence without emitting a double space when nothing is set.
    """
    import shlex  # noqa: PLC0415

    parts: list[str] = []
    if institution is not None:
        parts.extend(("--institution", institution))
    if account_id is not None:
        parts.extend(("--account-id", account_id))
    if account_name is not None:
        parts.extend(("--account-name", account_name))
    # mapping=None: a mapping override is tabular-only, and this fragment serves
    # the channels that have none. The binding/metadata serialization is shared.
    # Ref-keyed answers only. The account-gate recoveries re-key to @N off the
    # gate's own proposals, but the sign gate fires BEFORE the account gate on
    # PDF, so there are no proposals here to re-key from — and passing the
    # caller's key through would put a raw source_account_key (an OFX <ACCTID>,
    # or an opaque PDF document digest) into `actions[]`, which sits outside the
    # redaction walk. Dropping it is the honest half: a ref answers the same
    # account and discloses nothing, a raw key cannot be printed, and
    # `_sign_recovery_note` tells the caller what to re-supply rather than
    # letting the pin vanish.
    from moneybin.services.import_service import (  # noqa: PLC0415 — defer import
        is_proposal_ref,
    )

    ref_bindings = {
        k: v for k, v in (account_bindings or {}).items() if is_proposal_ref(k)
    }
    ref_metadata = {
        k: v for k, v in (account_metadata or {}).items() if is_proposal_ref(k)
    }
    parts.extend(
        _tabular_recovery_args(
            mapping=None,
            account_bindings=ref_bindings,
            account_metadata=ref_metadata,
        )
    )
    return f" {shlex.join(parts)}" if parts else ""


def _sign_recovery_note(
    account_bindings: dict[str, str] | None,
    account_metadata: dict[str, dict[str, str]] | None,
) -> str:
    """Tell the caller about any answer the recovery command could not carry.

    Silence here would be the failure this gate exists to prevent: the caller
    pastes a command that looks complete, the raw-keyed pin is gone, and the
    import binds whatever matching infers.
    """
    from moneybin.services.import_service import (  # noqa: PLC0415 — defer import
        is_proposal_ref,
    )

    dropped = sum(1 for k in (account_bindings or {}) if not is_proposal_ref(k)) + sum(
        1 for k in (account_metadata or {}) if not is_proposal_ref(k)
    )
    if not dropped:
        return ""
    return (
        f" (re-supply your {dropped} source-key-keyed account answer(s) — they "
        "are omitted here because this surface masks source keys; @N refs carry "
        "through)"
    )


def _import_confirm_command(
    file_path_str: str,
    *,
    accept: bool,
    confirm_sign: bool,
    sign: SignConventionType | None,
    mapping: dict[str, str] | None,
    save_format: bool,
    institution: str | None,
    account_id: str | None,
    account_name: str | None,
    account_bindings: dict[str, str] | None,
    account_metadata: dict[str, dict[str, str]] | None,
    bridge_response: Path | None = None,
) -> str:
    """Serialize one public `import confirm` request losslessly.

    Every channel's account and sign recoveries route through here, so the
    command MoneyBin prints is always the command it would accept back.

    ``institution`` is here because a file can need it to reach the gate at
    all: ``resolve_institution`` raises earlier in the import, so an OFX whose
    issuer is underivable arrives at an account confirmation only on a re-run
    that already carries the override. Dropping it printed a command that
    failed the check ahead of the one it was answering.

    ``bridge_response`` is the same argument one step further: the bridge raises
    the account gate like every other channel, but its recipe is agent-authored
    and lives only in that file. Dropping it printed a command that re-ran the
    deterministic path instead — and paired ``--accept`` with a flag this
    command refuses alongside it. The bridge takes ``--confirm``, not
    ``--accept``, so the two are mutually exclusive here as well.
    """
    import shlex  # noqa: PLC0415

    parts = ["moneybin", "import", "confirm", file_path_str]
    if bridge_response is not None:
        parts.extend(("--bridge-response", str(bridge_response), "--confirm"))
    elif accept:
        parts.append("--accept")
    if confirm_sign:
        parts.append("--confirm-sign")
    if sign is not None:
        parts.extend(("--sign", sign))
    if institution is not None:
        parts.extend(("--institution", institution))
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
    institution: str | None = None,
    account_id: str | None = None,
    account_name: str | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
    confirm_sign: bool = False,
    sign: SignConventionType | None = None,
    bridge_response: Path | None = None,
) -> str:
    """Name the command that answers this account confirmation — one, for every channel.

    ``import confirm`` on all three. It takes a file path, not a staged preview
    id (that is the MCP tool), so nothing about OFX or PDF makes it bounce, and
    ``InboxService`` has always emitted exactly this form for an account gate
    regardless of channel. ``--accept`` ratifies nothing on a channel with no
    column mapping; it satisfies the command's require-an-action guard, which is
    why the inbox's version carries it too.

    OFX and PDF used to be sent to ``import files`` here — a second vocabulary
    for one question, and worse than cosmetic: only ``import confirm`` calls
    ``archive_confirmed_file``, so a pending inbox file answered through
    ``import files`` stayed in ``pending/`` and was offered again on every sync.
    """
    # Every key this command names is a proposal_ref, never a source key.
    # `actions[]` sits outside the envelope's redaction walk — render_or_json
    # applies redact_typed to `data` alone — so a source key here hands an OFX
    # <ACCTID> to whatever reads the JSON, and the CLI carries MCP's redaction
    # contract unchanged (cli.md). A ref names the same account and discloses
    # nothing, which is what it exists for.
    #
    # The caller's own `account_bindings` is deliberately NOT read here. The
    # replay exists for the two-account file answered one at a time, and an
    # answered account is skipped by the gate — so it is absent from
    # `account_proposals`, and re-keying against those alone would find no
    # entry and fall back to echoing the caller's raw <ACCTID>. The gate hands
    # the same answers over already keyed by ref (`ratified_bindings`), which
    # is the only layer that still knows each account's position. Nothing is
    # lost by ignoring the caller's dict: a key naming no account in the file
    # raises upstream, so every answer they sent is represented there.
    bindings = dict(outcome.ratified_bindings)
    for proposal in outcome.account_proposals:
        bindings.setdefault(str(proposal["proposal_ref"]), "<account_id|new>")
    if not bindings:
        bindings["<proposal_ref>"] = "<account_id|new>"

    return _import_confirm_command(
        file_path_str,
        # A channel with no mapping has nothing to ratify, but the command
        # requires an action; `accept` is False only where a --mapping override
        # supplies one, which is tabular-only. A bridge answer satisfies that
        # guard with --confirm instead, and _import_confirm_command drops
        # --accept when it sees one.
        accept=accept or outcome.channel in ("ofx", "pdf"),
        confirm_sign=confirm_sign,
        sign=sign,
        mapping=mapping,
        save_format=save_format,
        institution=institution,
        account_id=account_id,
        account_name=account_name,
        account_bindings=bindings,
        account_metadata=account_metadata,
        bridge_response=bridge_response,
    )


def _sign_recovery_commands(
    file_path_str: str,
    *,
    channel: str,
    accept: bool = True,
    mapping: dict[str, str] | None = None,
    save_format: bool = True,
    institution: str | None = None,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
    proposed_sign: str | None = None,
    prior_sign: str | None = None,
) -> list[str]:
    """The two honest recoveries for a sign-convention confirmation.

    The user decides by re-running with the convention they intend, never by
    blind-accepting a proposed mapping. Shared by the JSON ``actions[]`` and the
    interactive prompt so the CLI never drifts from the terminal command the
    gate's ``error_message`` already names. Mirrors the MCP
    ``_sign_confirm_actions`` recovery.

    ``prior_sign`` decides the framing, because the two cases pose different
    questions. Without one this is a first-contact card inference: the proposal
    is always ``negative_is_income`` and the alternative always
    ``negative_is_expense``, so "is this a credit card?" is both accurate and the
    only question the user can actually answer. With one, a self-healed recipe
    re-derived to the opposite polarity — which can run *either* direction, so
    the card question may be exactly backwards. There the honest framing names
    both conventions and what each does.
    """
    if channel == "tabular":
        approve_command = _import_confirm_command(
            file_path_str,
            accept=accept,
            confirm_sign=True,
            sign=None,
            mapping=mapping,
            save_format=save_format,
            institution=institution,
            account_id=account_id,
            account_name=account_name,
            account_bindings=account_bindings,
            account_metadata=account_metadata,
        )
        native_command = _import_confirm_command(
            file_path_str,
            accept=accept,
            confirm_sign=False,
            sign="negative_is_expense",
            mapping=mapping,
            save_format=save_format,
            institution=institution,
            account_id=account_id,
            account_name=account_name,
            account_bindings=account_bindings,
            account_metadata=account_metadata,
        )
        return [
            f"Approve the inferred credit-card inversion: {approve_command}",
            f"Keep amounts exactly as printed: {native_command}",
        ]

    import shlex  # noqa: PLC0415

    from moneybin.services.import_confirmation import (  # noqa: PLC0415
        sign_convention_effect,
    )

    quoted = shlex.quote(file_path_str)
    # The account-identity options ride along on every one of these, exactly as
    # the tabular branch above threads them through _import_confirm_command. On
    # PDF the sign gate raises BEFORE the account gate, so this is the command a
    # caller who already passed --account-id gets handed; without the pin, the
    # command MoneyBin prints re-imports under auto-matching and binds something
    # the caller never chose.
    acct = _import_files_account_args(
        institution=institution,
        account_id=account_id,
        account_name=account_name,
        account_bindings=account_bindings,
        account_metadata=account_metadata,
    )
    note = _sign_recovery_note(account_bindings, account_metadata)
    if prior_sign is None:
        return [
            f"If it IS a credit card: moneybin import files {quoted} "
            f"--confirm{acct} (records charges as expenses, payments as "
            f"credits).{note}",
            f"If it is NOT a credit card: moneybin import files {quoted} "
            f"--sign negative_is_expense{acct} "
            f"(records amounts exactly as printed).{note}",
        ]

    accepted = proposed_sign or "the re-derived convention"
    # No sentence-ending period: the command runs to the end of the line, so a
    # period would glue onto the final argument and a pasted command would carry
    # it (`--confirm.`, or a metadata value ending in `.`). The branch above
    # closes with a parenthetical instead, which is why it can keep its period.
    return [
        f"Accept the change — {sign_convention_effect(accepted)}: "
        f"moneybin import files {quoted} --confirm{acct}{note}",
        f"Keep the previous convention — {sign_convention_effect(prior_sign)}: "
        f"moneybin import files {quoted} --sign {prior_sign}{acct}{note}",
    ]


def _can_preview(outcome: ConfirmationRequired) -> bool:
    """Whether ``moneybin import preview`` can actually inspect this file.

    ``import preview`` runs tabular format detection with one special route for
    PDF; it has no OFX path at all, so offering it there names a command that
    fails instead of inspecting anything.

    A predicate rather than the fourth copy of ``outcome.channel != "ofx"``:
    this hint is printed from four places (the ``import files`` envelope, the
    text-mode prompt, and both of ``import confirm``'s recovery paths), the
    first fix reached only one of them, and a reviewer found each survivor in a
    separate round. One definition is what stops that.
    """
    return outcome.channel != "ofx"


def _sign_direction(
    outcome: ConfirmationRequired,
) -> tuple[str | None, str | None]:
    """The (proposed, prior) conventions for the recovery renderers.

    ``(None, None)`` when the outcome isn't a sign proposal, which keeps the
    default first-contact framing.
    """
    from moneybin.services.import_confirmation import (  # noqa: PLC0415  # module-scope import is TYPE_CHECKING-only (cold-start hygiene)
        SignConventionProposal,
    )

    proposed = outcome.proposed
    if not isinstance(proposed, SignConventionProposal):
        return (None, None)
    return (proposed.sign_convention, proposed.prior_sign_convention)


def _render_sign_convention_prompt(
    proposed: SignConventionProposal,
    file_path_str: str,
    *,
    channel: str,
    accept: bool = True,
    mapping: dict[str, str] | None = None,
    save_format: bool = True,
    institution: str | None = None,
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
    if proposed.prior_sign_convention is None:
        typer.echo(
            "   Recording it with this convention inverts every amount's sign — "
            "negative values become income and positive values become expenses."
        )
    else:
        # A repaired recipe can flip EITHER way, so the fixed sentence above
        # describes the wrong direction half the time. Name both conventions.
        typer.echo(
            f"   This layout recorded amounts as "
            f"{proposed.prior_sign_convention!r} before; the re-derived version "
            f"records them as {proposed.sign_convention!r}. Every amount's sign "
            f"flips relative to earlier imports of this format."
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
        institution=institution,
        account_id=account_id,
        account_name=account_name,
        account_bindings=account_bindings,
        account_metadata=account_metadata,
        proposed_sign=proposed.sign_convention,
        prior_sign=proposed.prior_sign_convention,
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
    institution: str | None = None,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
    confirm_sign: bool = False,
    sign: SignConventionType | None = None,
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
            institution=institution,
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
                institution=institution,
                account_id=account_id,
                account_name=account_name,
                account_metadata=account_metadata,
                confirm_sign=confirm_sign,
                sign=sign,
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
    if _can_preview(outcome):
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
        help=(
            "Explicitly approve an inferred tabular sign inversion (pair with "
            "--accept). For a PDF statement use `import files <path> --confirm`; "
            "the MCP equivalent is import_confirm(preview_id=...) on a sign "
            "preview, which asks the human rather than asserting their approval."
        ),
    ),
    sign: SignConventionType | None = typer.Option(
        None,
        "--sign",
        help=(
            "Explicit tabular sign-convention override. Use "
            "negative_is_expense to keep amounts as printed."
        ),
    ),
    institution: str | None = typer.Option(
        None,
        "--institution",
        "-i",
        help=(
            "Institution override, carried over from the 'import files' call "
            "that raised this confirmation. Same meaning as on 'import files': "
            "consulted for OFX/QFX/QBO only when the file's <FI><ORG>, FID "
            "lookup, and filename heuristic all yield nothing. Ignored for "
            "tabular and PDF files, which resolve their institution from the "
            "matched format and filename."
        ),
    ),
    account_id: str | None = typer.Option(
        None,
        "--account-id",
        help=(
            "Account ID to associate with imported transactions. Not honored "
            "for OFX/QFX/QBO, which name their own accounts and can carry "
            "several — use --account-binding there; supplying it is refused "
            "rather than ignored."
        ),
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
            "--account-binding REF=ACCOUNT_ID to adopt an existing account, or "
            "REF=new to mint a distinct new account. REF is the proposal_ref "
            "the gate printed (@0 is the file's first source account) or that "
            "proposal's source_account_key; both name the same account and "
            "supplying two different answers for one is an error. On retry, "
            "re-supply ALL bindings — no partial state persists between calls."
        ),
    ),
    account_meta: list[str] = typer.Option(
        None,
        "--account-meta",
        help=(
            "Metadata for a 'new' account (repeatable): "
            "--account-meta REF:field=value, where REF is the @0/@1 ref the "
            "confirmation showed (or the source key), and field is one of "
            "display_name, account_subtype, last_four, currency_code."
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
        moneybin import confirm ~/Downloads/card.csv --accept --sign negative_is_expense
        moneybin import confirm ~/Downloads/card.pdf --bridge-response response.json --confirm
    """
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.import_service import ImportService  # noqa: PLC0415

    if bridge_response is not None:
        if accept or mapping or confirm_sign or sign:
            raise typer.BadParameter(
                "--bridge-response cannot be combined with --accept, --mapping, "
                "--confirm-sign, or --sign.",
                param_hint="'--bridge-response'",
            )
        # --account-binding is deliberately absent from this refusal: the bridge
        # raises the same account gate every other channel does, and this is the
        # command a human answers it with. Refusing it made that gate
        # unanswerable — the same re-run raised the same question.
        # --institution joins the refusal rather than the forward list:
        # apply_pdf_bridge_response has no institution parameter (the recipe
        # carries the format), so accepting it would silently discard it.
        if account_name or account_meta or institution:
            raise typer.BadParameter(
                "--bridge-response supports --account-id and --account-binding "
                "only; PDF rows do not use --account-name, --account-meta, or "
                "--institution.",
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
    elif confirm_sign and sign is not None:
        raise typer.BadParameter(
            "--confirm-sign and --sign are alternate sign decisions; choose one.",
            param_hint="'--confirm-sign' or '--sign'",
        )
    elif not accept and not mapping:
        raise typer.BadParameter(
            "Pass --accept to ratify the proposed mapping, or at least one "
            "--mapping field=column to override specific fields.",
            param_hint="'--accept' or '--mapping'",
        )

    # Third site with this shape, wrapped for the same reason as the `import
    # files` and `import preview` preflights: `Path.exists()` raises under a
    # macOS TCC denial instead of returning False, so an unwrapped check
    # tracebacks rather than classifying.
    with handle_cli_errors(cli_actor="import_confirm_command"):
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
        header_row_consumed_recovery,
        unreadable_date_recovery,
    )

    try:
        with handle_cli_errors(cli_actor="import_confirm_command"):
            with get_database(read_only=False) as db:
                service = ImportService(db)
                if bridge_response_data is not None:
                    bridge_result = service.apply_pdf_bridge_response(
                        file_path,
                        bridge_response_data,
                        save_format=save_format,
                        account_id=account_id,
                        account_bindings=parsed_bindings,
                        confirm=True,
                    )
                    result = None
                else:
                    confirm_kwargs: dict[str, Any] = {
                        "file_path": file_path,
                        "confirm": accept,
                        "overrides": parsed_mapping,
                        "institution": institution,
                        "account_id": account_id,
                        "account_name": account_name,
                        "account_bindings": parsed_bindings,
                        "account_metadata": parsed_metadata,
                        "save_format": save_format,
                        "sign": sign,
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
            proposed_sign, prior_sign = _sign_direction(outcome)
            confirm_actions.extend(
                _sign_recovery_commands(
                    str(file_path),
                    channel=outcome.channel,
                    accept=accept,
                    mapping=parsed_mapping,
                    save_format=save_format,
                    institution=institution,
                    account_id=account_id,
                    account_name=account_name,
                    account_bindings=parsed_bindings,
                    account_metadata=parsed_metadata,
                    proposed_sign=proposed_sign,
                    prior_sign=prior_sign,
                )
            )
        elif outcome.reason == "account_confirmation":
            # The layout is settled; only the account identity needs ratifying.
            # Replay the current confirmation inputs because retries persist no
            # partial state, and add the missing binding. Generic alternate
            # mapping hints remain irrelevant here.
            confirm_actions.append(
                f"Re-run `{_account_recovery_command(str(file_path), outcome, accept=accept, mapping=parsed_mapping, save_format=save_format, institution=institution, account_id=account_id, account_name=account_name, account_metadata=parsed_metadata, confirm_sign=confirm_sign, sign=sign, bridge_response=bridge_response)}` "
                "to bind each proposed account (adopt an existing id, or 'new' "
                "to keep distinct)."
            )
        elif outcome.reason == "header_row_consumed":
            confirm_actions.append(header_row_consumed_recovery())
        elif outcome.reason == "unreadable_date":
            # `import confirm` carries no --date-format, so the recovery is a
            # different command, not a different flag on this one.
            confirm_actions.append(unreadable_date_recovery(str(file_path)))
        else:
            confirm_actions.append(
                "Re-run with --mapping <field>=<column> to override specific fields."
            )
            if outcome.confidence.tier != "low":
                confirm_actions.append(
                    f"Re-run 'moneybin import confirm {file_path} --accept' "
                    "to accept the proposed mapping as-is."
                )
        if _can_preview(outcome):
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
                institution=institution,
                account_id=account_id,
                account_name=account_name,
                account_bindings=parsed_bindings,
                account_metadata=parsed_metadata,
                confirm_sign=confirm_sign,
                sign=sign,
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
                    institution=institution,
                    account_id=account_id,
                    account_name=account_name,
                    account_metadata=parsed_metadata,
                    confirm_sign=confirm_sign,
                    sign=sign,
                    # Forwarded for the same reason the JSON branch above does:
                    # without it the printed line loses --bridge-response and
                    # --confirm, gains an --accept this command refuses beside a
                    # bridge response, and so cannot finish the agent-authored
                    # import the user was answering the gate for.
                    bridge_response=bridge_response,
                )
                + "`."
            )
        elif outcome.reason == "header_row_consumed":
            logger.error("❌ A transaction row was consumed as the header.")
            logger.info(f"💡 {header_row_consumed_recovery()}")
        elif outcome.reason == "unreadable_date":
            logger.error("❌ No date format could be read from the date column.")
            logger.info(f"💡 {unreadable_date_recovery(str(file_path))}")
        else:
            msg = f"❌ Confirmation failed: {outcome.reason}" + (
                f" — {outcome.error_message}" if outcome.error_message else ""
            )
            logger.error(msg)
            if _can_preview(outcome):
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
        if bridge_result.accounts_created:
            data["accounts_created"] = _accounts_created_payload(
                bridge_result.accounts_created
            )
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
            echo_accounts_created(
                _accounts_created_payload(bridge_result.accounts_created)
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
        # Same omit-when-empty rule as the files[] rows: present means news.
        if result.accounts_created:
            data["accounts_created"] = _accounts_created_payload(
                result.accounts_created
            )
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
        echo_accounts_created(_accounts_created_payload(result.accounts_created))
        if result.sign_correction_suggested:
            typer.echo(
                "⚠️  Sign convention may be inverted (running balance suggests "
                "negation). If amounts look wrong, re-run with --mapping corrected.",
                err=True,
            )
        logger.info("💡 Run 'moneybin transform apply' to rebuild derived tables.")


_HISTORY_COLUMNS: tuple[
    tuple[str, Callable[[dict[str, str | int | None]], object]], ...
] = (
    ("import", lambda rec: str(rec.get("import_id", ""))),
    ("status", lambda rec: str(rec.get("status", ""))),
    ("imported", lambda rec: rec.get("rows_imported") or 0),
    ("rejected", lambda rec: rec.get("rows_rejected") or 0),
    # The whole path, not the basename: `source_file` is part of the dedup key
    # on `raw.tabular_transactions`, so the same name under two directories is
    # two imports and the basename answers which one wrong. `render_rows` folds
    # text rather than truncating it, so width is the renderer's problem here.
    ("source file", lambda rec: str(rec.get("source_file") or "")),
)

_HISTORY_DEFAULT = ("import", "status", "imported", "rejected")
"""The id is what `import revert` takes, so it is never the column dropped.

A full UUID is 36 characters on its own, which leaves room for the outcome and
the two counts and no more; the source file follows under `--wide`.
"""


@app.command("history")
def import_history(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
    import_id: str | None = typer.Option(
        None, "--import-id", help="Show details for a specific import"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    wide: bool = wide_option,
) -> None:
    """List recent imports with batch details.

    Shows the import ID — what ``import revert`` takes — plus status and row
    counts for each completed batch. ``--wide`` adds the source file.

    Examples:
        moneybin import history
        moneybin import history --limit 50
        moneybin import history --import-id abc123
    """
    from moneybin.cli.output import render_or_json
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import get_database  # noqa: PLC0415 — deferred import
    from moneybin.extractors.tabular import TabularExtractor
    from moneybin.protocol.envelope import build_envelope

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            extractor = TabularExtractor(db)
            records = extractor.get_import_history(limit=limit, import_id=import_id)

    if output == OutputFormat.JSON:
        from moneybin.privacy.payloads.imports import ImportStatusPayload

        # The same payload the MCP `import_status` tool returns: one query,
        # one shape. `records` keeps the rows opaque by declaration — see that
        # payload's docstring for why.
        render_or_json(
            build_envelope(
                data=ImportStatusPayload(records=records),
                actions=[
                    "Use 'moneybin import revert <import_id>' to undo one batch",
                ],
            ),
            output,
            cli_actor="import_history",
        )
        return

    if not records:
        if not quiet:
            if import_id:
                logger.warning(f"⚠️  No import found with ID: {import_id}")
            else:
                logger.warning("⚠️  No import history found")
        return

    view = column_view(_HISTORY_COLUMNS, records, default=_HISTORY_DEFAULT, wide=wide)
    render_rows(
        view.names,
        view.rows,
        numeric=("imported", "rejected"),
        total_columns=view.total,
    )

    if import_id and records:
        render_summary(
            [
                (key, str(value))
                for key, value in records[0].items()
                if value is not None
            ],
            title="\nDetails:",
        )


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
    from moneybin.services.import_service import ImportRevertPlan, ImportService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            plan = ImportService(db).plan_revert(import_id)

    if plan.revertable and not yes:
        confirmed = typer.confirm(
            f"Revert import {import_id[:8]}...? This permanently deletes "
            f"{plan.rows_to_delete} row(s) from this batch and cannot be undone."
        )
        if not confirmed:
            logger.info("Revert cancelled")
            raise typer.Exit(0)

    def _verify(live: ImportRevertPlan) -> None:
        """Refuse if the batch changed between the prompt and the delete."""
        if live != plan:
            raise UserError(
                "Import state changed while the confirmation was pending; "
                "nothing was deleted. Re-run to see the current plan.",
                code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
            )

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            result = ImportService(db).revert_confirmed(import_id, verify=_verify)

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


def _preview_pdf(source: Path) -> None:
    """Inspect a PDF statement without importing it.

    PDFs never reach the tabular detector's format/read/column-map stages — a
    statement's structure is derived by the recipe rung, not by column mapping —
    so preview routes them to the same ``ImportService.pdf_preview`` the MCP
    ``import_preview`` tool uses. Without this branch the detector rejected
    ``.pdf`` outright and the whole PDF debug loop was MCP-only.

    ``read_only=False`` matches the MCP path: a bridge escalation writes the
    Req 14 egress audit row before raising.
    """
    from moneybin.database import (  # noqa: PLC0415
        DatabaseKeyError,
        database_key_error_hint,
        get_database,
    )
    from moneybin.services.import_confirmation import (  # noqa: PLC0415
        ImportConfirmationRequiredError,
        SignConventionProposal,
    )
    from moneybin.services.import_service import ImportService  # noqa: PLC0415

    try:
        with get_database(read_only=False) as db:
            preview = ImportService(db).pdf_preview(source)
    except DatabaseKeyError as e:
        # The tabular branch degrades to built-in formats when there's no
        # database; a PDF cannot — the recipe rung reads app.pdf_formats. Say so
        # rather than dumping a traceback on a fresh install.
        #
        # This one exception covers both "never initialized" and "locked":
        # read_only=False never raises DatabaseNotInitializedError (that path is
        # read_only=True only), so a fresh install arrives here via
        # SecretNotFoundError. database_key_error_hint() is what picks the right
        # recovery — a hardcoded "db unlock" strands a fresh install on the one
        # command that cannot work, since there is no salt to re-derive from.
        logger.error(f"❌ Can't open the database, so {source.name} wasn't read: {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e
    except ImportConfirmationRequiredError as e:
        # pdf_preview signals both the sign gate and a bridge escalation by
        # raising. Preview's job is to report what is pending, not to resolve
        # it, so this is a successful inspection — not an error exit.
        outcome = e.outcome
        proposed = outcome.proposed
        if isinstance(proposed, SignConventionProposal):
            # Reuses the shared renderer rather than logging the proposal:
            # sample rows carry merchant descriptions and bare amounts, and
            # SanitizedLogFormatter masks neither (_DOLLAR_PATTERN requires a
            # literal "$"; nothing matches descriptions). Logging them would
            # persist transaction detail to the session log, which
            # `.claude/rules/security.md` forbids. typer.echo is the correct
            # channel for user-facing proposal output, and the shared renderer
            # also emits the real recovery commands.
            _render_sign_convention_prompt(proposed, str(source), channel="pdf")
        else:
            logger.info(
                f"Deterministic extraction escalated to the assisted reader: "
                f"{source.name} (reason: {outcome.reason})"
            )
            logger.info(
                "💡 The assisted-reader path runs through an AI agent driving the "
                "MCP server; from the CLI, apply its result with "
                "'moneybin import confirm <file> --bridge-response <file>.json'."
            )
        return

    verdict = "deterministic" if preview.deterministic else "NOT deterministic"
    logger.info(f"PDF preview: {source.name}")
    logger.info(f"  Extraction: {verdict} (reason: {preview.decision_reason})")
    logger.info(f"  Rows:       {preview.row_count}")
    logger.info(f"  Confidence: {preview.confidence:.2f}")
    if preview.fingerprint:
        issuer = preview.fingerprint.get("issuer", "unknown")
        logger.info(f"  Layout:     issuer={issuer}")
    if not preview.deterministic:
        logger.info(
            "💡 This statement would be stored as an unparsed seed rather than "
            "transactions."
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

    Tabular files (CSV/Excel/Parquet): runs detection and column-mapping
    stages without loading any data. Shows detected format, column mapping,
    and sample rows.

    PDF statements: runs the deterministic recipe rung and reports whether
    the statement extracts cleanly, how many rows it would yield, and any
    pending sign-convention confirmation. Nothing is imported either way.

    Examples:
        moneybin import preview ~/Downloads/chase_activity.csv
        moneybin import preview ~/Downloads/transactions.xlsx --sheet Sheet1
        moneybin import preview ~/Downloads/chase_statement.pdf
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import read_file

    source = Path(file_path)

    # Wrapped for the same reason as the `import files` preflight: `Path.exists()`
    # raises rather than returning False under a macOS TCC denial, so an
    # unwrapped check turns the documented `import preview ~/Documents/...`
    # scenario into a traceback instead of the Full Disk Access guidance.
    with handle_cli_errors():
        if not source.exists():
            logger.error(f"❌ File not found: {source}")
            raise typer.Exit(1)

    if source.suffix.lower() == ".pdf":
        # Routed before the tabular stages below: none of format detection,
        # read_file, or column mapping apply to a statement PDF.
        ignored = [
            flag
            for flag, value in (
                ("--format", format_name),
                ("--sheet", sheet),
                ("--delimiter", delimiter),
                ("--encoding", encoding),
                ("--override", override),
            )
            if value
        ]
        if ignored:
            # Say so rather than no-op silently: an agent that passed --format
            # and got a clean report would otherwise conclude the flag was
            # honoured, and repeat it on the import that follows.
            logger.warning(
                f"⚠️  Ignored for a PDF (tabular-only): {', '.join(ignored)}. "
                f"A statement's structure comes from its recipe, not a column "
                f"mapping."
            )
        # Same handler the tabular stages below use, for the same reason: a
        # PermissionError here (statements live under ~/Documents, where macOS
        # TCC denies reads) must reach `permission_advice` so the remedy matches
        # the errno and platform. This branch used to hardcode one macOS pane
        # for every PermissionError, which named the wrong pane for a TCC block
        # and offered the macOS fix on Linux and for mode denials chmod solves.
        with handle_cli_errors():
            _preview_pdf(source)
        return

    overrides = _parse_overrides(override)

    with handle_cli_errors():
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
            # Say "not detected" rather than dropping the line: a missing row
            # reads as "nothing to report", when it is the one fact that blocks
            # the import. Name both fixes — a status column can claim the date
            # alias while the real dates sit unmapped, and --date-format aimed
            # at that wrong column is refused.
            if mapping_result.date_format:
                typer.echo(f"Date format: {mapping_result.date_format}")
            else:
                # Name only what THIS command accepts: preview takes
                # --override, not --mapping, and no --date-format at all.
                # The other half of the recovery therefore has to name the
                # command that does carry it.
                typer.echo(
                    "Date format: not detected — re-run with `--override "
                    "transaction_date=<column>` if the wrong column matched; "
                    "if the mapped column is right, its format is unrecognized "
                    "and only `moneybin import files <file> --confirm "
                    "--date-format <strptime>` can read it"
                )
            if mapping_result.number_format:
                typer.echo(f"Number format: {mapping_result.number_format}")

        # Show sample rows
        sample_n = min(5, len(df))
        typer.echo(f"\nSample ({sample_n} rows):")
        typer.echo(df.head(sample_n))
        typer.echo()


_PDF_FORMAT_COLUMNS: tuple[tuple[str, Callable[[PdfFormat], object]], ...] = (
    ("name", lambda pf: pf.name),
    ("institution", lambda pf: pf.institution_name),
    ("routing", lambda pf: pf.routing),
    ("front-end", lambda pf: pf.front_end),
    ("version", lambda pf: pf.version),
    ("used", lambda pf: pf.times_used),
    (
        "last used",
        lambda pf: (
            pf.last_used_at.date().isoformat()
            if pf.last_used_at is not None
            else "\u2014"
        ),
    ),
)

_PDF_FORMAT_DEFAULT = ("name", "institution", "routing", "last used")
"""Which format, whose statements it reads, what it routes to, and whether it
is still in use. The front end, version, and use count are provenance for a
format that misbehaves, and follow under `--wide`."""


@formats_app.command("list")
def formats_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    wide: bool = wide_option,
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

    Displays format name, institution, sign convention, date format, and
    source for tabular formats, and name, institution, routing, and last-used
    for PDF formats. ``--wide`` adds the PDF front-end, version, and use count.

    Example:
        moneybin import formats list
        moneybin import formats list --type=pdf
        moneybin import formats list --type=tabular --output json
    """
    from moneybin.database import get_database
    from moneybin.services.import_service import ImportService

    try:
        with get_database(read_only=True) as db:
            all_formats, builtin, pdf_formats = ImportService(db).list_formats()
    except Exception:  # noqa: BLE001 — DB may not exist yet; show built-in / empty PDF
        all_formats, builtin = _load_all_formats(None)
        pdf_formats = _load_pdf_formats(None)

    show_tabular = _type in (_FormatTypeFilter.tabular, _FormatTypeFilter.all)
    show_pdf = _type in (_FormatTypeFilter.pdf, _FormatTypeFilter.all)

    if output == OutputFormat.JSON:
        from moneybin.adapters.imports_adapters import (
            pdf_format_row,
            tabular_format_row,
        )
        from moneybin.cli.output import render_or_json
        from moneybin.privacy.payloads.imports import (
            ImportFormatEntry,
            ImportFormatsPayload,
        )
        from moneybin.protocol.envelope import build_envelope

        # One list with a 'type' discriminator per row — `--type` narrows it,
        # and an agent filters the unnarrowed answer with
        # jq '.data.formats | map(select(.type == "pdf"))'.
        rows: list[ImportFormatEntry] = []
        if show_tabular:
            rows.extend(
                tabular_format_row(fmt, builtin=builtin)
                for fmt in sorted(all_formats.values(), key=lambda f: f.name)
            )
        if show_pdf:
            rows.extend(pdf_format_row(pf) for pf in pdf_formats)
        render_or_json(
            build_envelope(data=ImportFormatsPayload(formats=rows)),
            output,
            cli_actor="import_formats_list",
        )
        return

    # ---- Text output -------------------------------------------------------

    if show_tabular:
        if not all_formats:
            if not quiet:
                logger.warning("⚠️  No tabular formats found")
        else:
            # Count only, like the PDF header below: the per-format split now
            # lives in the `source` column, and spelling it "built-in" here
            # beside a cell reading `builtin` made one value look like two.
            typer.echo(f"\nTabular formats ({len(all_formats)})")
            render_rows(
                ["name", "institution", "sign convention", "date format", "source"],
                [
                    (
                        fmt.name,
                        fmt.institution_name,
                        fmt.sign_convention,
                        fmt.date_format,
                        # Same spelling as the `source` field in the JSON
                        # branch above: one field, one value, whichever
                        # surface a caller reads it from.
                        "user" if fmt.name not in builtin else "builtin",
                    )
                    for fmt in sorted(all_formats.values(), key=lambda f: f.name)
                ],
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
            pdf_view = column_view(
                _PDF_FORMAT_COLUMNS,
                pdf_formats,
                default=_PDF_FORMAT_DEFAULT,
                wide=wide,
            )
            render_rows(
                pdf_view.names,
                pdf_view.rows,
                numeric=("version", "used"),
                total_columns=pdf_view.total,
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
    from moneybin.privacy.payloads.imports import (  # noqa: PLC0415 — defer import
        ImportPdfFormatDetail,
    )
    from moneybin.services.import_service import ImportService

    try:
        with get_database(read_only=True) as db:
            all_formats, _, pdf_formats_list = ImportService(db).list_formats()
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
        # Raised rather than emitted: `handle_cli_errors` owns both the JSON
        # error envelope and the text ❌/💡 pair, so the two branches cannot
        # drift and the JSON failure gets the audit row every other one gets.
        # `ImportPdfFormatDetail` of the two payloads this command can return:
        # both classify `medium`, and it is the superset by one class, so a
        # not-found row that names neither over-declares rather than under-.
        with handle_cli_errors(
            cli_actor="import_formats_show", payload_type=ImportPdfFormatDetail
        ):
            raise UserError(
                f"Format not found: {name!r}",
                code=error_codes.IMPORT_SAVED_FORMAT_NOT_FOUND,
                hint=f"Available formats: {available}",
            )

    # ---- Tabular format ----
    if fmt is not None:
        if output == OutputFormat.JSON:
            from moneybin.adapters.imports_adapters import (
                tabular_format_detail,
            )
            from moneybin.cli.output import render_or_json
            from moneybin.protocol.envelope import build_envelope

            render_or_json(
                # One format is one row. Stated rather than inferred: the
                # payload's `header_signature` is a list of column names, and
                # with `skip_trailing_patterns` unset it is the only list on
                # the payload, so the sole-collection rule would count the
                # signature's columns — 9, 8 and 11 for the shipped formats.
                build_envelope(data=tabular_format_detail(fmt), returned_count=1),
                output,
                cli_actor="import_formats_show",
            )
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
        from moneybin.adapters.imports_adapters import pdf_format_detail
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        render_or_json(
            # One format is one row, stated for the same reason as the tabular
            # branch above: this payload carries no list today, so the rule
            # happens to agree, and a field added later must not change it.
            build_envelope(data=pdf_format_detail(pdf_fmt), returned_count=1),
            output,
            cli_actor="import_formats_show",
        )
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
    from moneybin import error_codes
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import get_database  # noqa: PLC0415 — deferred import
    from moneybin.errors import UserError
    from moneybin.extractors.tabular.formats import load_builtin_formats
    from moneybin.services.import_service import ImportService

    if name in load_builtin_formats():
        logger.error(f"❌ {name!r} is a built-in format and cannot be deleted")
        raise typer.Exit(1)

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            reviewed_plan = ImportService(db).plan_saved_format_delete(name)

        if not yes:
            confirmed = typer.confirm(f"Delete format {name!r}?")
            if not confirmed:
                logger.info("Delete cancelled")
                raise typer.Exit(0)

        def verify(live_plan: object) -> None:
            if live_plan != reviewed_plan:
                raise UserError(
                    "Saved format changed after confirmation; review and retry.",
                    code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                )

        with get_database(read_only=False) as db:
            ImportService(db).delete_saved_format_confirmed(
                name,
                actor="cli",
                verify=verify,
            )

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
    from moneybin.cli.output import render_or_json
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_settings
    from moneybin.database import get_database  # noqa: PLC0415 — deferred import
    from moneybin.privacy.payloads.imports import (
        ImportRawSummaryPayload,
        ImportRawTableRow,
    )
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.import_service import ImportService  # noqa: PLC0415

    db_path = get_settings().database.path

    if not db_path.exists():
        # Raised rather than echoed per branch: `handle_cli_errors` owns the ❌
        # line and the hint in text mode and the error envelope in JSON mode,
        # so the two cannot drift and the JSON failure gets the audit row every
        # other one gets. Both modes still exit non-zero, which is how a script
        # detects uninitialized state.
        with handle_cli_errors(
            cli_actor="import_status", payload_type=ImportRawSummaryPayload
        ):
            raise UserError(
                f"No MoneyBin database at {db_path}.",
                code=error_codes.INFRA_DATABASE_NOT_INITIALIZED,
                hint="Run 'moneybin import files <path>' to import data first.",
            )

    try:
        with handle_cli_errors():
            with get_database(read_only=True) as db:
                rows = ImportService(db).raw_data_summary()
    except Exception as e:  # noqa: BLE001 — surface connection errors generically
        logger.error(f"❌ Could not open database: {e}")
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=ImportRawSummaryPayload(
                    database=str(db_path),
                    tables=[
                        ImportRawTableRow(
                            schema=row.schema,
                            table=row.table,
                            rows=row.rows,
                            date_min=None
                            if row.date_min is None
                            else str(row.date_min),
                            date_max=None
                            if row.date_max is None
                            else str(row.date_max),
                        )
                        for row in rows
                    ],
                    exists=True,
                ),
            ),
            output,
            cli_actor="import_status",
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
