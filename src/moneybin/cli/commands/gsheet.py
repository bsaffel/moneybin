"""Google Sheets connector CLI commands.

User-controlled-storage connect-* family (see `.claude/rules/surface-design.md`
verb vocabulary). Mirrors the `sync` subgroup's shape — thin Typer wrappers
that build a service inside a context manager and delegate. Heavy imports
defer to inside command bodies per the cold-start hygiene rule.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import typer

from moneybin import error_codes
from moneybin.adapters.gsheet_adapters import (
    gsheet_connect_payload,
    gsheet_connection_row,
    gsheet_pull_rows,
)
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.progress import operation_progress
from moneybin.cli.render import build_summary, compose_human_result
from moneybin.cli.utils import (
    emit_json_failure,
    generated_cli_command,
    get_terminal_policy,
    handle_cli_errors,
    warn_refresh_steps,
    warn_transfers_retired,
)
from moneybin.connectors.gsheet.service_factory import (
    build_connection_service as _build_connection_service,
)
from moneybin.connectors.gsheet.service_factory import (
    build_oauth_client as _build_oauth_client,
)
from moneybin.connectors.gsheet.service_factory import (
    build_pull_service_with_db as _build_pull_service,
)
from moneybin.errors import UserError
from moneybin.extractors.tabular.formats import SignConventionType
from moneybin.matching.reconciliation import RETIRED_SIDES_COLLAPSED
from moneybin.privacy.payloads.gsheet import (
    GsheetAuthPayload,
    GsheetConnectionsPayload,
    GsheetDisconnectPayload,
    GsheetPullPayload,
)
from moneybin.progress import ProgressEvent
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

if TYPE_CHECKING:
    from collections.abc import Sequence

    from moneybin.connectors.gsheet.adapters.base import GSheetConnection
    from moneybin.services.refresh_outcome import RefreshStepOutcome

logger = logging.getLogger(__name__)


def _emit_receipt(lines: list[str]) -> None:
    """Render a stateful Sheets receipt through the shared terminal policy."""
    emit_human_result(
        "\n".join(lines),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


def _connections_envelope(
    connections: Sequence[GSheetConnection],
) -> ResponseEnvelope[GsheetConnectionsPayload]:
    """Wrap a connection list, with a reconnect hint per drifted binding.

    `gsheet list` and `gsheet status` answer the same question at different
    scopes, so they build one payload — the difference is which connections the
    caller asked for, not what a connection looks like.
    """
    rows = [gsheet_connection_row(c) for c in connections]
    return build_envelope(
        data=GsheetConnectionsPayload(connections=rows),
        actions=[
            f"Run '{generated_cli_command('gsheet', 'reconnect', row.connection_id)}' to re-detect "
            "this sheet's structure"
            for row in rows
            if row.status == "drift_detected"
        ],
    )


app = typer.Typer(
    help="Connect Google Sheets workbooks as transaction sources or raw seed data",
    no_args_is_help=True,
)


def _echo_detection_notes(notes: list[str]) -> None:
    """Print structural detection notes on the default text surface.

    These carry the cost of an inference the user did not make — a renamed
    duplicate header, say. Emitting them only under ``--output json`` hides
    them from every human who does not ask for machine output.

    They go to stderr per cli.md: a warning on stdout is captured by a redirect
    or pipeline as though it were part of the data the command was asked for.
    """
    for note in notes:
        typer.echo(f"{get_terminal_policy().symbols.attention} {note}", err=True)


def _parse_column_mapping(raw: str | None) -> dict[str, str] | None:
    """Parse a ``--column-mapping`` CLI argument into a dict.

    Accepts JSON (``{"Date":"date","Amount":"amount"}``) or a comma-separated
    ``key=value`` list (``Date=date,Amount=amount``). Returns None when no
    mapping was given.
    """
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith("{"):
        try:
            parsed: object = json.loads(raw)
        except json.JSONDecodeError as exc:
            # Without this guard, malformed JSON raises before
            # handle_cli_errors() wraps the call — automation sees a raw
            # JSONDecodeError traceback instead of the documented exit-2
            # validation error contract.
            raise typer.BadParameter(
                f"--column-mapping JSON is malformed: {exc.msg} "
                f"(line {exc.lineno}, column {exc.colno})"
            ) from exc
        if not isinstance(parsed, dict):
            raise typer.BadParameter("--column-mapping JSON must be an object")
        return {str(k): str(v) for k, v in parsed.items()}  # type: ignore[reportUnknownVariableType]
    mapping: dict[str, str] = {}
    for pair in raw.split(","):
        if "=" not in pair:
            raise typer.BadParameter(
                f"--column-mapping pair {pair!r} must be in key=value form"
            )
        key, value = pair.split("=", 1)
        mapping[key.strip()] = value.strip()
    return mapping


@app.command("auth")
def gsheet_auth(
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-authenticate even if a refresh token is already on file.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Run the Google OAuth installed-app flow and persist tokens.

    Opens a browser window for the user to authorize MoneyBin to read
    Google Sheets. Tokens are stored in the platform keychain via
    ``SecretStore``. Subsequent ``gsheet connect`` and ``gsheet pull``
    calls reuse the persisted refresh token automatically.

    Short-circuits when a refresh token is already on file unless
    ``--force`` is passed — mirrors ``force_reauth`` on the ``gsheet_connect``
    MCP tool.
    """
    with handle_cli_errors():
        client = _build_oauth_client()
        if client.is_authorized() and not force:
            status = "already_authorized"
        else:
            client.authorize()
            status = "authorized"
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=GsheetAuthPayload(status=status)),
            output,
            cli_actor="gsheet_auth",
        )
    elif status == "already_authorized":
        _emit_receipt([
            f"{get_terminal_policy().symbols.success} Already authorized. "
            "Pass --force to re-authenticate."
        ])
    else:
        _emit_receipt([
            f"{get_terminal_policy().symbols.success} Google Sheets authorized."
        ])


@app.command("connect")
def gsheet_connect(
    url: str = typer.Argument(..., help="Google Sheet URL (must include #gid=...)."),
    adapter: str | None = typer.Option(
        None,
        "--adapter",
        help="Force adapter selection ('transactions' or 'seed'). "
        "Default: auto-detect transactions, fall through to seed when "
        "--accept-seed-fallback is set.",
    ),
    alias: str | None = typer.Option(
        None,
        "--alias",
        help="Short identifier for seed adapter — becomes raw.gsheet_<alias>. "
        "Required when --adapter=seed.",
    ),
    account_name: str | None = typer.Option(
        None,
        "--account-name",
        help="Account name to attribute every imported transaction to. "
        "Omit for a sheet with its own account column — each row is then "
        "attributed to the account it names.",
    ),
    account_id: str | None = typer.Option(
        None,
        "--account-id",
        help="Canonical account_id to attribute every imported transaction "
        "to. Omit for a sheet with its own account column.",
    ),
    column_mapping: str | None = typer.Option(
        None,
        "--column-mapping",
        help='Override auto-detected mapping. JSON ({"Date":"date",...}) '
        "or comma-separated key=value pairs.",
    ),
    sign: SignConventionType | None = typer.Option(
        None,
        "--sign",
        help="Sign-convention override for the saved connection. Required "
        "when MoneyBin cannot derive polarity from the selected source. "
        "Split-to-single mappings derive polarity from the selected "
        "detected debit or credit role; replacing a detected single "
        "amount source requires an explicit sign. Choices: negative_is_expense, "
        "negative_is_income, split_debit_credit.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip any interactive confirmation prompts.",
    ),
    no_initial_pull: bool = typer.Option(
        False,
        "--no-initial-pull",
        help="Skip the auto-pull after the connection is recorded.",
    ),
    accept_seed_fallback: bool = typer.Option(
        False,
        "--accept-seed-fallback",
        help="Allow falling back to the seed adapter when transactions "
        "detection returns low confidence.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Bind a Google Sheet to MoneyBin via direct OAuth (user-controlled storage).

    Detects sheet structure, persists the column mapping + header
    signature, and (by default) runs the initial pull. Use --adapter=seed
    --alias=<name> to land arbitrary tabular data into raw.gsheet_<alias>.
    """
    from moneybin.connectors.gsheet.connection_service import (
        ConnectionRequest,
    )

    parsed_mapping = _parse_column_mapping(column_mapping)

    with handle_cli_errors():
        with _build_connection_service() as service:
            req = ConnectionRequest(
                url=url,
                adapter=adapter,
                alias=alias,
                account_name=account_name,
                account_id=account_id,
                column_mapping=parsed_mapping,
                sign=sign,
                yes=yes,
                no_initial_pull=no_initial_pull,
                accept_seed_fallback=accept_seed_fallback,
            )
            result = service.connect(req, actor="cli")

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=gsheet_connect_payload(result),
                actions=[
                    f"Run '{generated_cli_command('gsheet', 'pull')}' to refresh this connection",
                ],
            ),
            output,
            cli_actor="gsheet_connect",
        )
        return

    conn = result.connection
    receipt_lines = [
        f"{get_terminal_policy().symbols.success} Connected "
        f"{conn.workbook_name}/{conn.sheet_name} "
        f"(adapter={conn.adapter}, connection_id={conn.connection_id})"
    ]
    _echo_detection_notes(result.detection.notes)
    if result.initial_pull is not None:
        p = result.initial_pull
        receipt_lines.append(
            f"   Pulled {p.rows_inserted + p.rows_upserted} rows "
            f"({p.rows_inserted} new, {p.rows_upserted} updated, "
            f"{p.rows_soft_deleted} soft-deleted)"
        )
    elif result.initial_pull_status not in (None, "complete"):
        receipt_lines.append(
            f"{get_terminal_policy().symbols.attention} Initial pull returned status={result.initial_pull_status}"
            + (f" — {result.initial_pull_error}" if result.initial_pull_error else "")
            + ". Run 'moneybin gsheet status' for detail."
        )
    _emit_receipt(receipt_lines)


@app.command("pull")
def gsheet_pull(
    connection_id: str | None = typer.Argument(
        None,
        help="Connection ID to pull. Omit to pull every healthy connection.",
    ),
    refresh: bool = typer.Option(
        True,
        "--refresh/--no-refresh",
        help="Run the refresh pipeline (match → transform → categorize → rates) "
        "after the pull. Default: on. Pass --no-refresh to defer.",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pull a single connection by ID, or every healthy connection."""
    from moneybin.adapters.refresh_adapters import (
        refresh_steps_fields,
    )
    from moneybin.connectors.gsheet.pull_service import PullResult
    from moneybin.orchestration.refresh import refresh as run_refresh
    from moneybin.orchestration.refresh import step_outcome

    refresh_error: str | None = None
    transfers_retired = 0
    steps_outcome: RefreshStepOutcome | None = None
    results: list[PullResult] | None = None
    terminal = get_terminal_policy()
    try:
        with handle_cli_errors():
            with _build_pull_service() as (service, db):
                with operation_progress(terminal, quiet=quiet) as report:
                    report(ProgressEvent("Pulling Google Sheets"))
                    if connection_id is None:
                        results = service.pull_all_healthy()
                    else:
                        results = [service.pull_connection(connection_id)]

                    if refresh:
                        report(ProgressEvent("Refreshing derived data"))
                        # Skip the "gsheet" step — we just ran the pull directly.
                        # run_refresh soft-fails by returning a RefreshResult with
                        # applied=False + error set, instead of raising. Capture
                        # the error so the CLI can surface a non-zero exit + a
                        # warning line; agents parsing --output json see it on the
                        # envelope too.
                        # An explicit list is never widened by a later canonical step,
                        # so every stage a pulled row needs is named here. `rates` is
                        # named because a sheet can carry foreign-currency rows: without
                        # it the pull rebuilds core.* against an empty rate cache and
                        # reports cannot convert offline until some unrelated refresh
                        # happens to fill it.
                        refresh_result = run_refresh(
                            db,
                            steps=["match", "transform", "categorize", "rates"],
                            progress=report,
                        )
                        if (
                            not refresh_result.applied
                            and refresh_result.error is not None
                        ):
                            refresh_error = refresh_result.error
                        # The `match` step above reconciles, so a pull can reverse a
                        # transfer the user accepted — reported even when the apply
                        # failed, because the retirement commits before it.
                        transfers_retired = refresh_result.transfers_retired
                        # Read for the same reason, and outside the `applied` check
                        # above for a sharper one: every step named above except the
                        # apply is best-effort, so any of them can crash or come back
                        # short while SQLMesh applies cleanly. Neither `applied` nor
                        # `error` moves in that case, and without this the work this
                        # command just did on the user's behalf would report nothing.
                        steps_outcome = step_outcome(refresh_result)
    except KeyboardInterrupt:
        known_pulls = (
            []
            if results is None
            else [
                {
                    "connection_id": row.connection_id,
                    "status": row.status,
                    "rows_inserted": row.load_result.rows_inserted
                    if row.load_result is not None
                    else 0,
                    "rows_upserted": row.load_result.rows_upserted
                    if row.load_result is not None
                    else 0,
                    "rows_soft_deleted": row.load_result.rows_soft_deleted
                    if row.load_result is not None
                    else 0,
                }
                for row in results
            ]
        )
        if output == OutputFormat.JSON:
            details = (
                {"saved_scope": "unknown", "outcome": "cancelled"}
                if results is None
                else {
                    "pulls": known_pulls,
                    "freshness": "unknown",
                    "outcome": "cancelled",
                }
            )
            emit_json_failure(
                UserError(
                    "Google Sheets pull cancelled; saved scope is unknown"
                    if results is None
                    else "Google Sheets pull cancelled after saved pull results",
                    code=error_codes.REFRESH_MODEL_FAILED,
                    hint="Run 'moneybin gsheet status' to inspect connections.",
                    details=details,
                ),
                cli_actor="gsheet_pull",
                payload_type=GsheetPullPayload,
            )
        else:
            emit_human_result(
                compose_human_result([
                    build_summary(
                        (
                            [
                                ("Saved state", "Saved scope is unknown"),
                                ("Next step", "`moneybin gsheet status`"),
                            ]
                            if results is None
                            else [
                                (
                                    "Saved pulls",
                                    "; ".join(
                                        f"{row['connection_id']}: "
                                        f"{row['rows_inserted']} new, "
                                        f"{row['rows_upserted']} updated, "
                                        f"{row['rows_soft_deleted']} soft-deleted"
                                        for row in known_pulls
                                    ),
                                ),
                                ("Freshness", "Refresh completion is unknown"),
                                ("Next step", "`moneybin gsheet status`"),
                            ]
                        ),
                        title="! Google Sheets pull cancelled",
                    )
                ]),
                policy=terminal,
                finite_read=False,
                receipt=True,
            )
        raise typer.Exit(130) from None

    # Requested work that cannot load due to a recoverable drift still exits
    # non-zero: reconnect is the recovery action, but no import took place.
    failure_statuses = {
        "auth_expired",
        "unreachable",
        "rate_limited",
        "failed",
        "drift_detected",
    }
    pull_failed = any(r.status in failure_statuses for r in results)
    refresh_partial = steps_outcome is not None and steps_outcome.has_failure

    # Ahead of both output branches, like `moneybin refresh`: a reversal of the
    # user's own decision is not informational output, so it survives --quiet
    # and is said aloud next to the JSON that also carries the count.
    warn_transfers_retired(transfers_retired, cause=RETIRED_SIDES_COLLAPSED)
    warn_refresh_steps(steps_outcome)

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=GsheetPullPayload(
                    pulls=gsheet_pull_rows(results),
                    refresh_error=refresh_error,
                    transfers_retired=transfers_retired,
                    **refresh_steps_fields(steps_outcome),
                )
            ),
            output,
            cli_actor="gsheet_pull",
        )
        if refresh_error is not None or pull_failed or refresh_partial:
            raise typer.Exit(1)
        return

    for r in results:
        if r.status == "complete" and r.load_result is not None:
            lr = r.load_result
            typer.echo(
                f"{terminal.symbols.success} {r.connection_id}: "
                f"{lr.rows_inserted} new, {lr.rows_upserted} updated, "
                f"{lr.rows_soft_deleted} soft-deleted"
            )
        elif r.status == "drift_detected":
            typer.echo(
                f"{terminal.symbols.attention} {r.connection_id}: drift detected — {r.drift_reason}"
            )
        else:
            typer.echo(
                f"{terminal.symbols.failure} {r.connection_id}: {r.status}"
                + (f" — {r.error_message}" if r.error_message else "")
            )

    if refresh_error is not None:
        typer.echo(
            f"{terminal.symbols.failure} Pull completed but refresh pipeline failed: {refresh_error}",
            err=True,
        )
    if refresh_partial:
        typer.echo(
            "! Pull partially completed; requested refresh results may be incomplete.",
            err=True,
        )
    if refresh_error is not None or pull_failed or refresh_partial:
        raise typer.Exit(1)


@app.command("list")
def gsheet_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List every Google Sheets connection."""
    with handle_cli_errors():
        with _build_connection_service() as service:
            connections = service.list_connections()

    if output == OutputFormat.JSON:
        render_or_json(
            _connections_envelope(connections),
            output,
            cli_actor="gsheet_list",
        )
        return

    if not connections:
        typer.echo(
            "No Google Sheets connections. Run `moneybin gsheet connect <url>` "
            "to add one."
        )
        return
    lines: list[str] = []
    for c in connections:
        last = c.last_success_at or "never"
        lines.append(
            f"{c.connection_id}  {c.workbook_name}/{c.sheet_name}  "
            f"adapter={c.adapter}  status={c.status}  last_success={last}"
        )
    emit_human_result(
        "\n".join(lines),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("status")
def gsheet_status(
    connection_id: str | None = typer.Argument(
        None,
        help="Connection ID to inspect. Omit for a full summary.",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show status for one connection, or a summary of all of them."""
    with handle_cli_errors(
        cli_actor="gsheet_status", payload_type=GsheetConnectionsPayload
    ):
        with _build_connection_service() as service:
            if connection_id is None:
                connections = service.list_connections()
            else:
                conn = service.get(connection_id)
                if conn is None:
                    # Raised, not echoed: `handle_cli_errors` owns both the
                    # JSON error envelope and the text failure marker, so the branches
                    # cannot drift and the failure gets its audit row.
                    raise UserError(
                        f"Unknown connection: {connection_id}",
                        code=error_codes.INFRA_NOT_FOUND,
                        hint="Run 'moneybin gsheet list' to see every connection.",
                    )
                connections = [conn]

    if output == OutputFormat.JSON:
        render_or_json(
            _connections_envelope(connections),
            output,
            cli_actor="gsheet_status",
        )
        return

    if not connections:
        typer.echo("No Google Sheets connections.")
        return
    lines: list[str] = []
    for c in connections:
        last = c.last_success_at or "never"
        lines.append(
            f"{c.connection_id}  status={c.status}  "
            f"adapter={c.adapter}  last_success={last}  "
            f"failures={c.consecutive_failure_count}"
        )
        if c.last_status_reason:
            lines.append(
                f"   {get_terminal_policy(no_pager=no_pager).symbols.attention} "
                f"{c.last_status_reason}"
            )
    emit_human_result(
        "\n".join(lines),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("reconnect")
def gsheet_reconnect(
    connection_id: str = typer.Argument(..., help="Connection ID to reconnect."),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip any interactive confirmation prompts.",
    ),
    sign: SignConventionType | None = typer.Option(
        None,
        "--sign",
        help="Sign-convention override for the re-pinned mapping. Use when "
        "the source sheet shape implies a different convention than the "
        "saved connection (e.g., a credit-card export now using "
        "positive_is_expense). Choices: negative_is_expense, "
        "negative_is_income, split_debit_credit.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Re-detect the sheet structure, re-pin the mapping, and run a pull.

    Use after the source sheet changes shape (column added, header reworded)
    and drift_detected status appears.
    """
    with handle_cli_errors():
        with _build_connection_service() as service:
            result = service.reconnect(connection_id, yes=yes, sign=sign, actor="cli")

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=gsheet_connect_payload(result),
                actions=[
                    f"Run '{generated_cli_command('gsheet', 'pull')}' to refresh this connection"
                ],
            ),
            output,
            cli_actor="gsheet_reconnect",
        )
        return

    receipt_lines = [
        f"{get_terminal_policy().symbols.success} Reconnected {connection_id} "
        f"(status={result.connection.status})"
    ]
    _echo_detection_notes(result.detection.notes)
    if result.initial_pull is not None:
        p = result.initial_pull
        receipt_lines.append(
            f"   Pulled {p.rows_inserted + p.rows_upserted} rows "
            f"({p.rows_inserted} new, {p.rows_upserted} updated, "
            f"{p.rows_soft_deleted} soft-deleted)"
        )
    _emit_receipt(receipt_lines)


@app.command("disconnect")
def gsheet_disconnect(
    connection_id: str = typer.Argument(..., help="Connection ID to disconnect."),
    purge: bool = typer.Option(
        False,
        "--purge",
        help="Also drop the seed view (if any) and delete raw rows. "
        "Without --purge, the connection is soft-disconnected and raw "
        "rows are retained for analytics.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip the destructive-action confirmation prompt (required for --purge).",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Soft-disconnect (default) or purge a Google Sheets connection."""
    with handle_cli_errors():
        with _build_connection_service() as service:
            if not purge:
                service.disconnect(connection_id, purge=False, actor="cli")
            else:
                planned = service.plan_purge(connection_id)
                typer.echo(
                    f"Purge {planned.connection_id}: {planned.rows_to_delete} raw row(s), "
                    f"{planned.blast_radius['views']} view(s), and the saved connection.",
                    err=True,
                )
                if not yes:
                    if not get_terminal_policy().interactive:
                        raise UserError(
                            "--purge requires --yes outside an interactive terminal.",
                            code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                            hint="Re-run with --yes after reviewing the purge preview.",
                        )
                    if not typer.confirm("Apply this purge?", default=False, err=True):
                        typer.echo("Cancelled. No purge was applied.", err=True)
                        raise typer.Exit(0)

                def verify(live: object) -> None:
                    if live != planned:
                        raise UserError(
                            "Purge target changed after the preview; no rows were deleted.",
                            code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                            hint="Review the new purge preview and confirm again.",
                        )

                service.purge_confirmed(connection_id, verify=verify, actor="cli")

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=GsheetDisconnectPayload(
                    connection_id=connection_id,
                    status="purged" if purge else "disconnected",
                    purged=purge,
                )
            ),
            output,
            cli_actor="gsheet_disconnect",
        )
    else:
        verb = "Purged" if purge else "Disconnected"
        _emit_receipt([
            f"{get_terminal_policy().symbols.success} {verb} {connection_id}"
        ])
