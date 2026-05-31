"""Google Sheets connector CLI commands.

User-controlled-storage connect-* family (see `.claude/rules/surface-design.md`
verb vocabulary). Mirrors the `sync` subgroup's shape — thin Typer wrappers
that build a service inside a context manager and delegate. Heavy imports
defer to inside command bodies per the cold-start hygiene rule.
"""

from __future__ import annotations

import json
import logging
import sys

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.connectors.gsheet.service_factory import (
    build_connection_service as _build_connection_service,
)
from moneybin.connectors.gsheet.service_factory import (
    build_oauth_client as _build_oauth_client,
)
from moneybin.connectors.gsheet.service_factory import (
    build_pull_service_with_db as _build_pull_service,
)
from moneybin.extractors.tabular.formats import SignConventionType

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Connect Google Sheets workbooks as transaction sources or raw seed data",
    no_args_is_help=True,
)


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
    ``--force`` is passed — mirrors the ``gsheet_auth`` MCP tool.
    """
    with handle_cli_errors():
        client = _build_oauth_client()
        if client.is_authorized() and not force:
            status = "already_authorized"
        else:
            client.authorize()
            status = "authorized"
    if output == OutputFormat.JSON:
        typer.echo(json.dumps({"status": status}))
    elif status == "already_authorized":
        typer.echo("✅ Already authorized. Pass --force to re-authenticate.")
    else:
        typer.echo("✅ Google Sheets authorized.")


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
        help="Account name to attribute imported transactions to.",
    ),
    account_id: str | None = typer.Option(
        None,
        "--account-id",
        help="Canonical account_id to attribute imported transactions to.",
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
        "when --column-mapping changes a split debit/credit detection "
        "into a single 'amount' column and the export uses "
        "positive_is_expense (credit-card style); otherwise the saved "
        "sign defaults to negative_is_expense and amounts persist with "
        "inverted polarity. Choices: negative_is_expense, "
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
    from moneybin.connectors.gsheet.connection_service import (  # noqa: PLC0415
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
        payload = {
            "connection": result.connection.to_dict(),
            "detection": {
                "confidence": result.detection.confidence,
                "column_mapping": result.detection.column_mapping,
                "notes": result.detection.notes,
            },
            # Mirror the MCP shape: a failed/empty pull still reports its
            # status + error so scripts distinguish "pull ran and failed"
            # from "pull skipped by --no-initial-pull" (both else-None
            # otherwise).
            "initial_pull": (
                {
                    "status": result.initial_pull_status,
                    "rows_inserted": result.initial_pull.rows_inserted,
                    "rows_upserted": result.initial_pull.rows_upserted,
                    "rows_soft_deleted": result.initial_pull.rows_soft_deleted,
                }
                if result.initial_pull
                else (
                    {
                        "status": result.initial_pull_status,
                        "error": result.initial_pull_error,
                    }
                    if result.initial_pull_status is not None
                    else None
                )
            ),
        }
        typer.echo(json.dumps(payload, indent=2))
        return

    conn = result.connection
    typer.echo(
        f"✅ Connected {conn.workbook_name}/{conn.sheet_name} "
        f"(adapter={conn.adapter}, connection_id={conn.connection_id})"
    )
    if result.initial_pull is not None:
        p = result.initial_pull
        typer.echo(
            f"   Pulled {p.rows_inserted + p.rows_upserted} rows "
            f"({p.rows_inserted} new, {p.rows_upserted} updated, "
            f"{p.rows_soft_deleted} soft-deleted)"
        )
    elif result.initial_pull_status not in (None, "complete"):
        typer.echo(
            f"⚠️  Initial pull returned status={result.initial_pull_status}"
            + (f" — {result.initial_pull_error}" if result.initial_pull_error else "")
            + ". Run 'moneybin gsheet status' for detail."
        )


@app.command("pull")
def gsheet_pull(
    connection_id: str | None = typer.Argument(
        None,
        help="Connection ID to pull. Omit to pull every healthy connection.",
    ),
    refresh: bool = typer.Option(
        True,
        "--refresh/--no-refresh",
        help="Run the refresh pipeline (match → transform → categorize) after "
        "the pull. Default: on. Pass --no-refresh to defer.",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pull a single connection by ID, or every healthy connection."""
    from moneybin.services.refresh import refresh as run_refresh  # noqa: PLC0415

    refresh_error: str | None = None
    with handle_cli_errors():
        with _build_pull_service() as (service, db):
            if not quiet and output == OutputFormat.TEXT:
                typer.echo("⚙️  Pulling Google Sheets…")
            if connection_id is None:
                results = service.pull_all_healthy()
            else:
                results = [service.pull_connection(connection_id)]

            if refresh:
                # Skip the "gsheet" step — we just ran the pull directly.
                # run_refresh soft-fails by returning a RefreshResult with
                # applied=False + error set, instead of raising. Capture
                # the error so the CLI can surface a non-zero exit + a
                # warning line; agents parsing --output json see it on the
                # envelope too.
                refresh_result = run_refresh(
                    db, steps=["match", "transform", "categorize"]
                )
                if not refresh_result.applied and refresh_result.error is not None:
                    refresh_error = refresh_result.error

    # Hard-failure statuses (auth_expired, unreachable, rate_limited, failed)
    # exit non-zero so CI/agents detect them without parsing output. drift_detected
    # is surfaced as a ⚠️ warning, not a ❌ error — the command ran and reported a
    # recoverable state (reconnect), so it stays exit 0, matching the ⚠️/❌ split
    # in the text output below.
    failure_statuses = {"auth_expired", "unreachable", "rate_limited", "failed"}
    pull_failed = any(r.status in failure_statuses for r in results)

    if output == OutputFormat.JSON:
        typer.echo(
            json.dumps(
                {
                    "pulls": [
                        {
                            "connection_id": r.connection_id,
                            "status": r.status,
                            "rows_inserted": (
                                r.load_result.rows_inserted if r.load_result else 0
                            ),
                            "rows_upserted": (
                                r.load_result.rows_upserted if r.load_result else 0
                            ),
                            "rows_soft_deleted": (
                                r.load_result.rows_soft_deleted if r.load_result else 0
                            ),
                            "drift_reason": r.drift_reason,
                            "error_message": r.error_message,
                        }
                        for r in results
                    ],
                    "refresh_error": refresh_error,
                },
                indent=2,
            )
        )
        if refresh_error is not None or pull_failed:
            raise typer.Exit(1)
        return

    for r in results:
        if r.status == "complete" and r.load_result is not None:
            lr = r.load_result
            typer.echo(
                f"✅ {r.connection_id}: "
                f"{lr.rows_inserted} new, {lr.rows_upserted} updated, "
                f"{lr.rows_soft_deleted} soft-deleted"
            )
        elif r.status == "drift_detected":
            typer.echo(f"⚠️  {r.connection_id}: drift detected — {r.drift_reason}")
        else:
            typer.echo(
                f"❌ {r.connection_id}: {r.status}"
                + (f" — {r.error_message}" if r.error_message else "")
            )

    if refresh_error is not None:
        typer.echo(
            f"❌ Pull completed but refresh pipeline failed: {refresh_error}",
            err=True,
        )
    if refresh_error is not None or pull_failed:
        raise typer.Exit(1)


@app.command("list")
def gsheet_list(
    output: OutputFormat = output_option,
) -> None:
    """List every Google Sheets connection."""
    with handle_cli_errors():
        with _build_connection_service() as service:
            connections = service.list_connections()

    if output == OutputFormat.JSON:
        typer.echo(json.dumps([c.to_dict() for c in connections], indent=2))
        return

    if not connections:
        typer.echo(
            "No Google Sheets connections. Run `moneybin gsheet connect <url>` "
            "to add one."
        )
        return
    for c in connections:
        last = c.last_success_at or "never"
        typer.echo(
            f"{c.connection_id}  {c.workbook_name}/{c.sheet_name}  "
            f"adapter={c.adapter}  status={c.status}  last_success={last}"
        )


@app.command("status")
def gsheet_status(
    connection_id: str | None = typer.Argument(
        None,
        help="Connection ID to inspect. Omit for a full summary.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Show status for one connection, or a summary of all of them."""
    with handle_cli_errors():
        with _build_connection_service() as service:
            if connection_id is None:
                connections = service.list_connections()
            else:
                conn = service.get(connection_id)
                if conn is None:
                    if output == OutputFormat.JSON:
                        typer.echo(json.dumps({"error": "not_found"}))
                    else:
                        typer.echo(f"❌ Unknown connection: {connection_id}", err=True)
                    raise typer.Exit(1)
                connections = [conn]

    if output == OutputFormat.JSON:
        typer.echo(json.dumps([c.to_dict() for c in connections], indent=2))
        return

    if not connections:
        typer.echo("No Google Sheets connections.")
        return
    for c in connections:
        last = c.last_success_at or "never"
        typer.echo(
            f"{c.connection_id}  status={c.status}  "
            f"adapter={c.adapter}  last_success={last}  "
            f"failures={c.consecutive_failure_count}"
        )
        if c.last_status_reason:
            typer.echo(f"   ⚠️  {c.last_status_reason}")


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
        typer.echo(
            json.dumps(
                {
                    "connection": result.connection.to_dict(),
                    "detection": {
                        "confidence": result.detection.confidence,
                        "column_mapping": result.detection.column_mapping,
                    },
                    "initial_pull": (
                        {
                            "rows_inserted": result.initial_pull.rows_inserted,
                            "rows_upserted": result.initial_pull.rows_upserted,
                            "rows_soft_deleted": result.initial_pull.rows_soft_deleted,
                        }
                        if result.initial_pull
                        else None
                    ),
                },
                indent=2,
            )
        )
        return

    typer.echo(f"✅ Reconnected {connection_id} (status={result.connection.status})")
    if result.initial_pull is not None:
        p = result.initial_pull
        typer.echo(
            f"   Pulled {p.rows_inserted + p.rows_upserted} rows "
            f"({p.rows_inserted} new, {p.rows_upserted} updated, "
            f"{p.rows_soft_deleted} soft-deleted)"
        )


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
    if purge and not yes:
        if not sys.stdin.isatty():
            typer.echo(
                "❌ --purge requires --yes when stdin is not a TTY "
                "(non-interactive contexts cannot show the confirmation prompt).",
                err=True,
            )
            raise typer.Exit(2)
        if not typer.confirm(
            f"Purge {connection_id} (drops raw rows + view)?",
            default=False,
        ):
            typer.echo("Cancelled.", err=True)
            raise typer.Exit(0)

    with handle_cli_errors():
        with _build_connection_service() as service:
            service.disconnect(connection_id, purge=purge, actor="cli")

    if output == OutputFormat.JSON:
        typer.echo(
            json.dumps({
                "status": "purged" if purge else "disconnected",
                "connection_id": connection_id,
            })
        )
    else:
        verb = "Purged" if purge else "Disconnected"
        typer.echo(f"✅ {verb} {connection_id}")
