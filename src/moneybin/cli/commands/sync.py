"""Data synchronization commands for MoneyBin CLI."""

import logging
import webbrowser
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
    wide_option,
)
from moneybin.cli.progress import operation_progress
from moneybin.cli.render import (
    build_rows,
    build_summary,
    column_view,
    compose_human_result,
    render_rows,
    render_summary,
)
from moneybin.cli.terminal import TerminalPolicy
from moneybin.cli.utils import (
    emit_json_failure,
    get_terminal_policy,
    handle_cli_errors,
    warn_refresh_steps,
    warn_transfers_retired,
)
from moneybin.connectors.sync_models import LinkInitiateResponse, PullResult
from moneybin.errors import UserError
from moneybin.matching.reconciliation import RETIRED_SIDES_COLLAPSED
from moneybin.progress import ProgressEvent

from .stubs import _not_implemented

if TYPE_CHECKING:
    from moneybin.connectors.sync_models import SyncConnectionView

app = typer.Typer(
    help="Sync financial data from external services",
    no_args_is_help=True,
)
key_app = typer.Typer(
    help="Manage the sync server's encryption key",
    no_args_is_help=True,
)
app.add_typer(key_app, name="key", hidden=True)
logger = logging.getLogger(__name__)


def _sync_pull_has_incomplete_work(result: PullResult) -> bool:
    """Whether the receipt must not describe the requested pull as complete."""
    return bool(
        any(inst.status == "failed" for inst in result.institutions)
        or result.transforms_error
        or result.security_resolution_error
        or (result.refresh_steps is not None and result.refresh_steps.has_failure)
    )


def _render_sync_pull_receipt(
    result: PullResult,
    *,
    terminal: TerminalPolicy,
    title: str | None = None,
) -> None:
    """Render the human result from the service's already-established facts."""
    completed = [inst for inst in result.institutions if inst.status == "completed"]
    failed = [inst for inst in result.institutions if inst.status == "failed"]
    incomplete = _sync_pull_has_incomplete_work(result)
    if title is None:
        if incomplete and completed:
            title = f"{terminal.symbols.attention} Sync partially completed"
        elif incomplete:
            title = f"{terminal.symbols.failure} Sync failed"
        else:
            title = f"{terminal.symbols.success} Sync complete"
    typer.echo(title)

    institution_word = "institution" if len(completed) == 1 else "institutions"
    render_summary([
        (
            "Loaded",
            f"{result.transactions_loaded:,} transactions loaded from "
            f"{len(completed):,} {institution_word}",
        )
    ])
    render_rows(
        ["Institution", "Transactions", "Status"],
        [
            (
                inst.institution_name or "Unnamed institution",
                inst.transaction_count
                if inst.transaction_count is not None
                else "Unknown",
                inst.status,
            )
            for inst in result.institutions
        ],
        numeric=["Transactions"],
        terminal=terminal,
    )

    changes: list[tuple[str, str]] = []
    if result.transactions_removed:
        changes.append((
            "Removed",
            f"{result.transactions_removed:,} stale transactions",
        ))
    for count, label in (
        (result.securities_loaded, "new securities"),
        (result.investment_transactions_loaded, "investment transactions"),
        (result.holdings_loaded, "holdings snapshots"),
        (result.security_prices_loaded, "new price closes"),
    ):
        if count:
            changes.append(("Changed", f"{count:,} {label}"))
    if result.opening_bootstrap_rows:
        changes.append((
            "Opening lots",
            f"{result.opening_bootstrap_rows:,} cumulative lots seeded for "
            "pre-window positions",
        ))
    if changes:
        render_summary(changes, title="Other changes")

    attention: list[tuple[str, str]] = []
    for inst in failed:
        detail = inst.error or inst.error_code or "refresh failed"
        attention.append((inst.institution_name or "Unnamed institution", detail))
        attention.append((
            "Remaining data",
            f"{inst.institution_name or 'This institution'} was not refreshed; "
            "previously available data may be stale",
        ))
    if result.transforms_error:
        attention.append(("Refresh", f"did not finish: {result.transforms_error}"))
        attention.append((
            "Derived data",
            "Core tables and reports may still reflect data before this pull",
        ))
    if result.security_resolution_error:
        attention.append((
            "Investment identity",
            f"did not finish: {result.security_resolution_error}",
        ))
        attention.append((
            "Investment data",
            "Investment transactions from this pull are not attributed to securities; "
            "cost basis may be incomplete",
        ))
    if result.refresh_steps is not None and result.refresh_steps.has_failure:
        attention.append((
            "Post-load refresh",
            "One or more requested stages did not finish",
        ))
        attention.append((
            "Derived results",
            "Derived matching, categorization, identity, or exchange-rate results "
            "may be incomplete",
        ))
    awaiting_identity = result.security_resolution.get(
        "proposed", 0
    ) + result.security_resolution.get("pending", 0)
    if awaiting_identity:
        attention.append((
            "Needs review",
            f"{awaiting_identity:,} securities awaiting identity review",
        ))
    if result.investment_source_overlap_accounts:
        attention.append((
            "Investment sources",
            f"{len(result.investment_source_overlap_accounts):,} accounts have "
            "both manual and Plaid history",
        ))
    if attention:
        render_summary(attention, title="! Needs attention")
    if _sync_pull_has_incomplete_work(result):
        typer.echo("Loaded transactions were saved.")
    if failed:
        typer.echo(f"{terminal.symbols.action} moneybin sync status")
    if result.transforms_error:
        typer.echo(f"{terminal.symbols.action} moneybin transform apply")
    if result.security_resolution_error:
        typer.echo(f"{terminal.symbols.action} moneybin sync pull")
    if awaiting_identity:
        typer.echo(
            f"{terminal.symbols.action} moneybin investments securities links pending"
        )
    if result.investment_source_overlap_accounts:
        typer.echo(f"{terminal.symbols.action} moneybin doctor")


def _build_sync_client():
    """Construct a SyncClient from current settings. Extracted for test mocking."""
    from moneybin.config import get_settings
    from moneybin.connectors.sync_client import SyncClient
    from moneybin.utils.user_config import get_or_create_profile_id

    settings = get_settings()
    if settings.sync.server_url is None:
        raise ValueError(
            "sync.server_url is not configured. "
            "Set MONEYBIN_SYNC__SERVER_URL in your environment."
        )
    # Scope the broker identity to the active profile so each profile
    # authenticates as a distinct user.
    profile_id = get_or_create_profile_id(settings.profile_dir)
    return SyncClient(server_url=str(settings.sync.server_url), profile_id=profile_id)


def _emit_sync_receipt(title: str, pairs: list[tuple[str, str]]) -> None:
    """Render an outcome-first, unpaged receipt for one sync mutation."""
    emit_human_result(
        compose_human_result([build_summary(pairs, title=title)]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@contextmanager
def _build_sync_service():
    """Yield a SyncService with an active Database connection (per ADR-010)."""
    from moneybin.database import get_database
    from moneybin.extractors.plaid import PlaidExtractor
    from moneybin.services.sync_service import SyncService

    client = _build_sync_client()
    with get_database(read_only=False) as db:
        loader = PlaidExtractor(db)
        yield SyncService(client=client, db=db, loader=loader)


@app.command("login")
def sync_login(
    no_browser: bool = typer.Option(
        False, "--no-browser", help="Print URL only; don't try to open a browser."
    ),
) -> None:
    """Authenticate with moneybin-sync via Device Authorization Flow."""
    with handle_cli_errors():
        client = _build_sync_client()
        client.login(open_browser=not no_browser)
    _emit_sync_receipt("Login complete", [("Outcome", "Logged in")])


@app.command("logout")
def sync_logout() -> None:
    """Clear stored JWTs and any in-progress device authorization sessions."""
    from moneybin.connectors.sync_auth import SyncAuthService

    with handle_cli_errors():
        client = _build_sync_client()
        SyncAuthService(client=client).logout()
    _emit_sync_receipt("Logout complete", [("Outcome", "Logged out")])


def _surface_link(initiate: LinkInitiateResponse, *, open_browser: bool) -> None:
    """Print the Plaid Hosted Link URL to stderr and optionally open the browser.

    Always prints to stderr so headless users can copy the URL even when
    `webbrowser.open()` falsely reports success (common on Linux without a
    display server). Called by SyncService.link via the on_initiate hook
    before it begins polling.
    """
    typer.echo(
        f"{get_terminal_policy().symbols.action} To complete authentication, open this URL:",
        err=True,
    )
    typer.echo(f"   {initiate.link_url}", err=True)
    if open_browser:
        try:
            webbrowser.open(initiate.link_url)
        except webbrowser.Error:
            pass  # URL already printed; user can copy manually


@app.command("link")
def sync_link(
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
    """Link a bank account via Plaid (formerly: sync connect).

    Establishes a new Plaid-mediated link to an institution, or
    re-authenticates one in error state. JSON output mode is event-driven:
    returns the initiate response immediately so an agent can present the
    link to the user and verify completion with `sync link-status` later.
    Text mode blocks until the user finishes the Plaid flow in their
    browser and returns the auto-pull summary.
    """
    try:
        with handle_cli_errors(cli_actor="sync_link"):
            with _build_sync_service() as service:
                if institution is None:
                    connections = service.list_connections()
                    error_state = [c for c in connections if c.status == "error"]
                    if len(error_state) == 1:
                        target = error_state[0].institution_name
                        if yes:
                            institution = target
                        elif (
                            output == OutputFormat.JSON
                            or not get_terminal_policy().interactive
                        ):
                            message = (
                                "A connected institution needs re-authentication. "
                                f"Pass --institution {target} with --yes to re-authenticate, "
                                "or pass --institution <new-bank-name> to add a different "
                                "institution."
                            )
                            if output == OutputFormat.JSON:
                                emit_json_failure(
                                    UserError(
                                        "Re-authentication selection is required",
                                        code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                                        hint=message,
                                    ),
                                    cli_actor="sync_link",
                                )
                            else:
                                typer.echo(message, err=True)
                            raise typer.Exit(2)
                        elif not typer.confirm(
                            f"Re-authenticate {target}?", default=True
                        ):
                            _emit_sync_receipt(
                                "Link cancelled",
                                [
                                    ("Institution", target or "(no name)"),
                                    ("Outcome", "No link was started"),
                                ],
                            )
                            raise typer.Exit(0)
                        else:
                            institution = target
                    elif len(error_state) > 1:
                        message = (
                            "Multiple institutions need re-authentication. "
                            "Pass --institution NAME with --yes."
                        )
                        if output == OutputFormat.JSON:
                            emit_json_failure(
                                UserError(
                                    "Re-authentication selection is required",
                                    code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                                    hint=message,
                                ),
                                cli_actor="sync_link",
                            )
                        else:
                            typer.echo(message, err=True)
                            for c in error_state:
                                typer.echo(f"  {c.institution_name}", err=True)
                        raise typer.Exit(2)
                    # else: no error-state institutions → new connection flow

                if output == OutputFormat.JSON:
                    # Event-driven: emit initiate response and exit. Agent verifies
                    # completion via `sync link-status` after the user finishes
                    # the Plaid Hosted Link flow out-of-band.
                    from moneybin.adapters.sync_adapters import (
                        sync_link_envelope,
                    )

                    initiate = service.initiate_link(institution=institution)
                    render_or_json(
                        sync_link_envelope(
                            initiate,
                            actions=[
                                "Open link_url in a browser to complete the connection",
                                "Then run 'moneybin sync link-status --session-id "
                                "<session_id>' to verify",
                            ],
                        ),
                        output,
                        cli_actor="sync_link",
                    )
                    return

                terminal = get_terminal_policy()
                with operation_progress(terminal) as report:

                    def _on_initiate(init: LinkInitiateResponse) -> None:
                        _surface_link(init, open_browser=not no_browser)
                        report(ProgressEvent("Waiting for link completion"))

                    result = service.link(
                        institution=institution,
                        auto_pull=not no_pull,
                        on_initiate=_on_initiate,
                    )
    except KeyboardInterrupt:
        if output == OutputFormat.JSON:
            details = {
                "saved_link_state": "unknown",
                "outcome": "cancelled",
            }
            if not no_pull:
                details.update({
                    "auto_pull_data": "unknown",
                    "refresh_freshness": "unconfirmed",
                })
            emit_json_failure(
                UserError(
                    "Link cancelled; saved state is unknown",
                    code=error_codes.SYNC_ERROR,
                    hint="Run 'moneybin sync status' to inspect connection state.",
                    details=details,
                ),
                cli_actor="sync_link",
            )
        else:
            state = (
                "Unknown — link connection may have changed"
                if no_pull
                else "Unknown — link and any auto-pull data may have changed"
            )
            details = [
                ("Saved state", state),
                ("Next action", "moneybin sync status"),
            ]
            if not no_pull:
                details.insert(
                    1,
                    (
                        "Data freshness",
                        "Refresh and report freshness are not confirmed",
                    ),
                )
            _emit_sync_receipt(
                "Link cancelled",
                details,
            )
        raise typer.Exit(130) from None

    if result.pull_result is not None:
        pr = result.pull_result
        incomplete = _sync_pull_has_incomplete_work(pr)
        _render_sync_pull_receipt(
            pr,
            terminal=terminal,
            title=(
                "! Link partially completed"
                if incomplete
                else f"{terminal.symbols.success} Link complete: "
                f"{result.institution_name}"
            ),
        )
        # connect's auto-pull reaches the same reconciliation `sync pull` does.
        warn_transfers_retired(pr.transfers_retired, cause=RETIRED_SIDES_COLLAPSED)
        # And the same best-effort steps, for the same reason.
        warn_refresh_steps(pr.refresh_steps)
        if incomplete:
            raise typer.Exit(1)
    else:
        if no_pull:
            _emit_sync_receipt(
                "Link complete",
                [
                    ("Institution", result.institution_name or "(no name)"),
                    ("Outcome", "Connected"),
                ],
            )
        else:
            _emit_sync_receipt(
                "! Link partially completed",
                [
                    ("Institution", result.institution_name or "(no name)"),
                    ("Outcome", "Connected; auto-pull failed"),
                    (
                        "Data freshness",
                        "Core tables and reports may still reflect data before this pull",
                    ),
                    ("Recovery", "moneybin sync pull"),
                ],
            )
            raise typer.Exit(1)


@app.command("link-status")
def sync_link_status(
    session_id: str = typer.Option(..., "--session-id", help="Session ID from link."),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # result framing remains essential under quiet
    no_pager: bool = no_pager_option,
) -> None:
    """Poll a sync link session for completion (formerly: sync connect-status).

    CLI mirror of MCP sync_link_status. Single-shot — returns whatever state
    the server holds for `session_id` (pending, connected, or failed). Does
    not poll; the caller decides when to check again.
    """
    with handle_cli_errors(cli_actor="sync_link_status"):
        client = _build_sync_client()
        result = client.get_link_status(session_id)

    if output == OutputFormat.JSON:
        from moneybin.adapters.sync_adapters import (
            sync_link_status_envelope,
        )

        render_or_json(
            sync_link_status_envelope(
                result,
                actions=["Run 'moneybin sync pull' once the status is 'linked'"],
            ),
            output,
            cli_actor="sync_link_status",
        )
        return
    policy = get_terminal_policy(no_pager=no_pager)
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Status", result.status),
                    ("Institution", result.institution_name or "(no name)"),
                ],
                title="Link status",
            )
        ]),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("connect", hidden=True)
def sync_connect_alias(  # Typer-registered alias; referenced by decorator
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
    """Deprecated alias for `sync link`. Will be removed in the next minor release."""
    logger.warning(
        "`moneybin sync connect` is deprecated; use `moneybin sync link`. "
        "The alias will be removed in the next minor release."
    )
    sync_link(
        institution=institution,
        no_pull=no_pull,
        no_browser=no_browser,
        yes=yes,
        output=output,
    )


@app.command("connect-status", hidden=True)
def sync_connect_status_alias(  # Typer-registered alias; referenced by decorator
    session_id: str = typer.Option(..., "--session-id", help="Session ID from link."),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Deprecated alias for `sync link-status`. Will be removed in the next minor release."""
    logger.warning(
        "`moneybin sync connect-status` is deprecated; use `moneybin sync link-status`. "
        "The alias will be removed in the next minor release."
    )
    sync_link_status(
        session_id=session_id,
        output=output,
        quiet=quiet,
        no_pager=no_pager,
    )


@app.command("disconnect")
def sync_disconnect(
    institution: str | None = typer.Option(
        None,
        "--institution",
        help=(
            "Institution name to disconnect. Ambiguous when it has more "
            "than one connection (e.g. after a relink) — use "
            "--provider-item-id instead."
        ),
    ),
    provider_item_id: str | None = typer.Option(
        None,
        "--provider-item-id",
        help=(
            "Exact connection to disconnect, from `moneybin sync status`. "
            "Mutually exclusive with --institution."
        ),
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
    if institution is not None and provider_item_id is not None:
        message = (
            "--institution and --provider-item-id are mutually exclusive — "
            "pass exactly one."
        )
        if output == OutputFormat.JSON:
            emit_json_failure(
                UserError(
                    "institution and provider_item_id are mutually exclusive",
                    code=error_codes.MUTATION_INVALID_INPUT,
                    hint=message,
                ),
                cli_actor="sync_disconnect",
            )
        else:
            typer.echo(message, err=True)
        raise typer.Exit(2)
    if institution is None and provider_item_id is None:
        message = (
            "One of --institution or --provider-item-id is required to disconnect."
        )
        if output == OutputFormat.JSON:
            emit_json_failure(
                UserError(
                    "institution or provider_item_id is required",
                    code=error_codes.SYNC_INSTITUTION_REQUIRED,
                    hint=message,
                ),
                cli_actor="sync_disconnect",
            )
        else:
            typer.echo(message, err=True)
        raise typer.Exit(2)
    if not yes and (
        output == OutputFormat.JSON or not get_terminal_policy().interactive
    ):
        message = "Disconnect requires explicit confirmation. Re-run with --yes."
        if output == OutputFormat.JSON:
            emit_json_failure(
                UserError(
                    "Disconnect confirmation is required",
                    code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                    hint=message,
                ),
                cli_actor="sync_disconnect",
            )
        else:
            typer.echo(message, err=True)
        raise typer.Exit(2)
    with handle_cli_errors():
        with _build_sync_service() as service:
            if yes:
                # No prompt to diverge from — resolving directly here is safe.
                disconnected = service.disconnect(
                    institution=institution, provider_item_id=provider_item_id
                )
            else:
                plan = service.plan_disconnect(
                    institution=institution, provider_item_id=provider_item_id
                )
                target = plan.institution_name or plan.provider_item_id
                if not typer.confirm(
                    f"Disconnect {target} (provider_item_id={plan.provider_item_id})?",
                    default=False,
                ):
                    _emit_sync_receipt(
                        "Disconnect cancelled",
                        [
                            ("Institution", plan.institution_name or "-"),
                            ("Provider item ID", plan.provider_item_id),
                            ("Outcome", "No connection was removed"),
                        ],
                    )
                    raise typer.Exit(0)
                # Delete exactly the connection the prompt named, not whatever
                # `institution` now resolves to — it may have been removed or
                # relinked while the prompt was open.
                disconnected = service.disconnect(
                    provider_item_id=plan.provider_item_id
                )
    resolved = disconnected.institution_name or disconnected.provider_item_id
    if output == OutputFormat.JSON:
        from moneybin.adapters.sync_adapters import (
            sync_disconnect_envelope,
        )

        render_or_json(
            sync_disconnect_envelope(
                institution=resolved,
                provider_item_id=disconnected.provider_item_id,
                actions=["Use 'moneybin sync link' to reconnect an institution"],
            ),
            output,
            cli_actor="sync_disconnect",
        )
    else:
        _emit_sync_receipt(
            "Disconnect complete",
            [
                ("Institution", disconnected.institution_name or "-"),
                ("Provider item ID", disconnected.provider_item_id),
                ("Outcome", "Disconnected"),
            ],
        )


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
            "Run the post-load refresh pipeline (matching + transforms + "
            "categorization) after a successful pull so core.* models "
            "(dim_accounts, etc.) reflect the new data before this command "
            "returns. Default: on. Pass --no-refresh to defer; transforms "
            "dominates pull latency, so high-frequency callers should defer "
            "and run refresh on a separate schedule."
        ),
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pull data from connected institutions."""
    terminal = get_terminal_policy()
    try:
        with handle_cli_errors():
            with _build_sync_service() as service:
                with operation_progress(terminal, quiet=quiet) as report:
                    result = service.pull(
                        institution=institution,
                        force=force,
                        refresh=refresh,
                        progress=report,
                    )
    except KeyboardInterrupt:
        if output == OutputFormat.TEXT:
            typer.echo("! Sync cancelled")
            typer.echo("Saved scope is unknown; inspect the current connection state.")
            typer.echo(f"{terminal.symbols.action} moneybin sync status")
        else:
            emit_json_failure(
                UserError(
                    "Sync cancelled; saved scope is unknown",
                    code=error_codes.SYNC_ERROR,
                    hint="Run 'moneybin sync status' to inspect connection state.",
                    details={"saved_scope": "unknown", "outcome": "cancelled"},
                ),
                cli_actor="sync_pull",
            )
        raise typer.Exit(130) from None

    # Ahead of both output branches, like `moneybin refresh` and `gsheet pull`:
    # a pull runs the full refresh, whose match step can reverse a transfer the
    # user accepted. The count rides in the JSON body either way, but only this
    # line names the way back, and an agent driving --output json is the caller
    # least able to notice a reversal on its own.
    warn_transfers_retired(result.transfers_retired, cause=RETIRED_SIDES_COLLAPSED)
    # Ahead of both branches for the same reason as the line above: this pull
    # ran a matcher, a categorizer, an identity pass and a network rate
    # backfill, and only these lines carry each one's remedy.
    warn_refresh_steps(result.refresh_steps)

    if output == OutputFormat.JSON:
        from moneybin.adapters.sync_adapters import (
            sync_pull_envelope,
        )

        render_or_json(
            sync_pull_envelope(
                result,
                actions=[
                    "Run 'moneybin sync status' to see connection health going forward",
                ],
            ),
            output,
            cli_actor="sync_pull",
        )
        # Fall through to the shared exit-code check so JSON-mode agents
        # gating on process status see the same signal as text-mode users.
    else:
        _render_sync_pull_receipt(result, terminal=terminal)

    # Non-zero exit on refresh failure applies to BOTH output modes — agents
    # gating on process status need the signal whether they parse JSON or
    # scrape text. Mirrors import_cmd.py:406. security_resolution_error joins
    # the same gate — a swallowed resolution failure is exactly as silent a
    # cost-basis corruption as a swallowed transform failure.
    if _sync_pull_has_incomplete_work(result):
        raise typer.Exit(1)


_STATUS_COLUMNS: tuple[tuple[str, Callable[["SyncConnectionView"], object]], ...] = (
    ("Institution", lambda c: c.institution_name),
    ("Status", lambda c: c.status),
    (
        "Last sync",
        lambda c: (
            c.last_sync.strftime("%Y-%m-%d %H:%M UTC") if c.last_sync else "never"
        ),
    ),
    ("Linked", lambda c: c.created_at.strftime("%Y-%m-%d %H:%M UTC")),
    ("Error", lambda c: c.error_code or "-"),
    ("Item ID", lambda c: c.provider_item_id),
)
_STATUS_DEFAULT = ("Institution", "Status", "Last sync", "Linked", "Error")
"""`created_at` (Linked) tells two identically named items apart by default;
`provider_item_id` — what `sync disconnect --provider-item-id` takes — follows
under `--wide` and in JSON (issue #408)."""


@app.command("status")
def sync_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # nothing to suppress yet
    wide: bool = wide_option,
    no_pager: bool = no_pager_option,
    json_fields: str | None = typer.Option(
        None,
        "--json-fields",
        help=(
            "Comma-separated field projection (json output only). Available: "
            "id, provider_item_id, institution_name, provider, status, last_sync, "
            "created_at, error_code, guidance"
        ),
    ),
) -> None:
    """Show connected institutions, last sync times, and health."""
    with handle_cli_errors():
        with _build_sync_service() as service:
            connections = service.list_connections()

    if output == OutputFormat.JSON:
        from moneybin.adapters.sync_adapters import (
            sync_status_envelope,
        )

        # The projection is `render_or_json`'s now, not this command's: it
        # descends into the payload's single list field and applies the same
        # filter, after the redaction walk rather than instead of it.
        render_or_json(
            sync_status_envelope(
                connections,
                actions=["Use 'moneybin sync pull' to fetch new transactions"],
            ),
            output,
            json_fields=json_fields,
            cli_actor="sync_status",
        )
        return

    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary(
            [("Scope", "connected institutions")], title="Connected institutions"
        )
    ]
    if not connections:
        parts.append(
            build_summary([
                (
                    "Result",
                    "No connected institutions. Run `moneybin sync link` to add one.",
                )
            ])
        )
    else:
        view = column_view(
            _STATUS_COLUMNS, connections, default=_STATUS_DEFAULT, wide=wide
        )
        parts.append(
            build_rows(
                view.names,
                view.rows,
                total_columns=view.total,
                terminal=policy,
            )
        )
        guidance = [
            (c.institution_name or "Institution", c.guidance)
            for c in connections
            if c.guidance
        ]
        if guidance:
            parts.append(build_summary(guidance, title="Needs attention"))
    emit_human_result(
        compose_human_result(parts),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@key_app.command("rotate", hidden=True)
def sync_key_rotate() -> None:
    """Rotate E2E encryption key pair."""
    _not_implemented("sync key rotation")


# sync schedule subgroup
schedule_app = typer.Typer(help="Manage scheduled sync jobs")
app.add_typer(schedule_app, name="schedule", hidden=True)


@schedule_app.command("set", hidden=True)
def sync_schedule_set() -> None:
    """Install daily sync schedule."""
    _not_implemented("scheduled sync")


@schedule_app.command("show", hidden=True)
def sync_schedule_show() -> None:
    """Show current schedule details."""
    _not_implemented("scheduled sync")


@schedule_app.command("remove", hidden=True)
def sync_schedule_remove() -> None:
    """Uninstall scheduled sync job."""
    _not_implemented("scheduled sync")
