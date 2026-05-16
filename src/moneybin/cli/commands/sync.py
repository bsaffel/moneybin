"""Data synchronization commands for MoneyBin CLI."""

import json
import logging
import sys
import webbrowser
from contextlib import contextmanager

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.connectors.sync_models import ConnectInitiateResponse

from .stubs import _not_implemented

app = typer.Typer(
    help="Sync financial data from external services",
    no_args_is_help=True,
)
key_app = typer.Typer(
    help="Manage the sync server's encryption key",
    no_args_is_help=True,
)
app.add_typer(key_app, name="key")
logger = logging.getLogger(__name__)


def _build_sync_client():
    """Construct a SyncClient from current settings. Extracted for test mocking."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.sync_client import SyncClient  # noqa: PLC0415

    settings = get_settings()
    if settings.sync.server_url is None:
        raise ValueError(
            "sync.server_url is not configured. "
            "Set MONEYBIN_SYNC__SERVER_URL in your environment."
        )
    return SyncClient(server_url=str(settings.sync.server_url))


@contextmanager
def _build_sync_service():
    """Yield a SyncService with an active Database connection (per ADR-010)."""
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.loaders.plaid_loader import PlaidLoader  # noqa: PLC0415
    from moneybin.services.sync_service import SyncService  # noqa: PLC0415

    client = _build_sync_client()
    with get_database(read_only=False) as db:
        loader = PlaidLoader(db)
        yield SyncService(client=client, db=db, loader=loader)


@app.command("login")
def sync_login(
    no_browser: bool = typer.Option(
        False, "--no-browser", help="Print URL only; don't try to open a browser."
    ),
) -> None:
    """Authenticate with moneybin-server via Device Authorization Flow."""
    with handle_cli_errors():
        client = _build_sync_client()
        client.login(open_browser=not no_browser)
        typer.echo("✅ Logged in.")


@app.command("logout")
def sync_logout() -> None:
    """Clear stored JWT from keychain (or fallback file)."""
    with handle_cli_errors():
        client = _build_sync_client()
        client.logout()
        typer.echo("✅ Logged out.")


def _surface_connect_link(
    initiate: ConnectInitiateResponse, *, open_browser: bool
) -> None:
    """Print the Plaid Hosted Link URL to stderr and optionally open the browser.

    Always prints to stderr so headless users can copy the URL even when
    `webbrowser.open()` falsely reports success (common on Linux without a
    display server). Called by SyncService.connect via the on_initiate hook
    before it begins polling.
    """
    typer.echo("⚙️  To complete authentication, open this URL:", err=True)
    typer.echo(f"   {initiate.link_url}", err=True)
    if open_browser:
        try:
            webbrowser.open(initiate.link_url)
        except webbrowser.Error:
            pass  # URL already printed; user can copy manually


@app.command("connect")
def sync_connect(
    institution: str | None = typer.Option(
        None,
        "--institution",
        help="Re-authenticate this connected institution, or a label for a new one.",
    ),
    no_pull: bool = typer.Option(
        False,
        "--no-pull",
        help="Skip the auto-pull after connecting.",
    ),
    no_browser: bool = typer.Option(
        False,
        "--no-browser",
        help="Print URL only; don't try to open a browser.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip re-auth confirmation prompt.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Connect a bank account (new) or re-authenticate one in error state.

    JSON output mode is event-driven: returns the initiate response immediately
    so an agent can present the link to the user and verify completion with
    `sync connect-status` later. Text mode blocks until the user finishes the
    Plaid flow in their browser and returns the auto-pull summary.
    """
    with handle_cli_errors():
        with _build_sync_service() as service:
            if institution is None:
                connections = service.list_connections()
                error_state = [c for c in connections if c.status == "error"]
                if len(error_state) == 1:
                    target = error_state[0].institution_name
                    if yes:
                        institution = target
                    elif sys.stdin.isatty():
                        confirmed = typer.confirm(
                            f"Re-authenticate {target}?",
                            default=True,
                        )
                        if not confirmed:
                            typer.echo("Cancelled.", err=True)
                            raise typer.Exit(0)
                        institution = target
                    else:
                        typer.echo(
                            f"❌ Found 1 institution needing re-auth ({target}). "
                            f"Pass `--institution {target}` to re-authenticate, or "
                            f"`--institution <new-bank-name>` to add a different "
                            "institution. Bare invocation is ambiguous in non-"
                            "interactive mode.",
                            err=True,
                        )
                        raise typer.Exit(2)
                elif len(error_state) > 1:
                    typer.echo(
                        "❌ Multiple institutions need re-auth. Pass `--institution NAME`:",
                        err=True,
                    )
                    for c in error_state:
                        typer.echo(f"   - {c.institution_name}", err=True)
                    raise typer.Exit(2)
                # else: no error-state institutions → new connection flow

            if output == OutputFormat.JSON:
                # Event-driven: emit initiate response and exit. Agent verifies
                # completion via `sync connect-status` after the user finishes
                # the Plaid Hosted Link flow out-of-band.
                initiate = service.initiate_connect(institution=institution)
                typer.echo(initiate.model_dump_json(indent=2))
                return

            def _on_initiate(init: ConnectInitiateResponse) -> None:
                _surface_connect_link(init, open_browser=not no_browser)

            result = service.connect(
                institution=institution,
                auto_pull=not no_pull,
                on_initiate=_on_initiate,
            )

    typer.echo(f"✅ Connected {result.institution_name}")
    if result.pull_result is not None:
        typer.echo(f"   Pulled {result.pull_result.transactions_loaded} transactions")
        if result.pull_result.transforms_error:
            # Same fail-loud contract as `sync pull`: connect's auto-pull is
            # the common path, so a silent transforms failure here would
            # leave the success-looking ✅ line hiding stale core.* tables.
            logger.warning(
                f"⚠️  transforms failed ({result.pull_result.transforms_error}); "
                f"raw rows landed. Retry with `moneybin transform apply`."
            )
            raise typer.Exit(1)


@app.command("connect-status")
def sync_connect_status(
    session_id: str = typer.Option(
        ..., "--session-id", help="Session ID from connect."
    ),
    output: OutputFormat = output_option,
) -> None:
    """Verify a pending connect session completed (CLI mirror of MCP sync_connect_status).

    Single-shot — returns whatever state the server holds for `session_id`
    (pending, connected, or failed). Does not poll; the caller decides when
    to check again.
    """
    with handle_cli_errors():
        client = _build_sync_client()
        result = client.get_connect_status(session_id)

    if output == OutputFormat.JSON:
        typer.echo(result.model_dump_json(indent=2))
        return
    typer.echo(f"✅ {result.status}: {result.institution_name or '(no name)'}")


@app.command("disconnect")
def sync_disconnect(
    institution: str = typer.Option(
        ...,
        "--institution",
        help="Institution name to disconnect.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompt.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Remove a bank connection."""
    if not yes and sys.stdin.isatty():
        if not typer.confirm(f"Disconnect {institution}?", default=False):
            typer.echo("Cancelled.", err=True)
            raise typer.Exit(0)
    with handle_cli_errors():
        with _build_sync_service() as service:
            service.disconnect(institution=institution)
    if output == OutputFormat.JSON:
        typer.echo(json.dumps({"status": "disconnected", "institution": institution}))
    else:
        typer.echo(f"✅ Disconnected {institution}")


@app.command("pull")
def sync_pull(
    institution: str | None = typer.Option(
        None, "--institution", help="Sync specific institution by name."
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Reset cursor and re-fetch full history."
    ),
    refresh: bool = typer.Option(
        True,
        "--refresh/--no-refresh",
        help=(
            "Run the post-load refresh pipeline (matching + SQLMesh apply + "
            "categorization) after a successful pull so core.* models "
            "(dim_accounts, etc.) reflect the new data before this command "
            "returns. Default: on. Pass --no-refresh to defer; SQLMesh apply "
            "dominates pull latency, so high-frequency callers should defer "
            "and run refresh on a separate schedule."
        ),
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pull data from connected institutions."""
    with handle_cli_errors():
        with _build_sync_service() as service:
            if not quiet and output == OutputFormat.TEXT:
                typer.echo("⚙️  Syncing… (this may take up to 2 minutes)")
            result = service.pull(
                institution=institution,
                force=force,
                refresh=refresh,
            )

    if output == OutputFormat.JSON:
        typer.echo(result.model_dump_json(indent=2))
        return

    for inst in result.institutions:
        icon = "✅" if inst.status == "completed" else "❌"
        count = inst.transaction_count or 0
        typer.echo(f"{icon} {inst.institution_name}: {count} transactions")
        if inst.status == "failed" and inst.error_code:
            typer.echo(f"   💡 error: {inst.error_code}")
    completed = sum(1 for i in result.institutions if i.status == "completed")
    typer.echo(
        f"✅ Loaded {result.transactions_loaded} transactions from "
        f"{completed} institutions."
    )
    if result.transactions_removed:
        typer.echo(f"   Removed {result.transactions_removed} stale transactions.")
    if result.transforms_error:
        # Mirror import_cmd.py: route the warning to stderr via the project
        # logger and exit non-zero so scripts and agents detect that core
        # tables are stale even though raw rows landed.
        logger.warning(
            f"⚠️  transforms failed ({result.transforms_error}); "
            f"raw rows landed. Retry with `moneybin transform apply`."
        )
        raise typer.Exit(1)


@app.command("status")
def sync_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — nothing to suppress yet
    json_fields: str | None = typer.Option(
        None,
        "--json-fields",
        help=(
            "Comma-separated field projection (json output only). Available: "
            "id, provider_item_id, institution_name, provider, status, last_sync, "
            "error_code, guidance"
        ),
    ),
) -> None:
    """Show connected institutions, last sync times, and health."""
    with handle_cli_errors():
        with _build_sync_service() as service:
            connections = service.list_connections()

    if output == OutputFormat.JSON:
        rows = [c.model_dump(mode="json") for c in connections]
        if json_fields:
            keep = {f.strip() for f in json_fields.split(",")}
            rows = [{k: v for k, v in r.items() if k in keep} for r in rows]
        typer.echo(json.dumps(rows, indent=2))
        return

    if not connections:
        typer.echo("No connected institutions. Run `moneybin sync connect` to add one.")
        return
    for c in connections:
        last = c.last_sync.strftime("%Y-%m-%d %H:%M UTC") if c.last_sync else "never"
        line = f"{c.institution_name} — status: {c.status}, last sync: {last}"
        typer.echo(line)
        if c.error_code:
            typer.echo(f"   ⚠️  error: {c.error_code}")
        if c.guidance:
            typer.echo(f"   💡 {c.guidance}")


@key_app.command("rotate")
def sync_key_rotate() -> None:
    """Rotate E2E encryption key pair."""
    _not_implemented("sync-overview.md")


# sync schedule subgroup
schedule_app = typer.Typer(help="Manage scheduled sync jobs")
app.add_typer(schedule_app, name="schedule")


@schedule_app.command("set")
def sync_schedule_set() -> None:
    """Install daily sync schedule."""
    _not_implemented("sync-overview.md")


@schedule_app.command("show")
def sync_schedule_show() -> None:
    """Show current schedule details."""
    _not_implemented("sync-overview.md")


@schedule_app.command("remove")
def sync_schedule_remove() -> None:
    """Uninstall scheduled sync job."""
    _not_implemented("sync-overview.md")
