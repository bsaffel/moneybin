"""accounts links — review-queue commands for account identity binding.

Subcommands: pending, set, history, run.
Mirrors `transactions matches` — thin wrappers over AccountLinksService.

`accounts links undo` is deliberately NOT YET registered:
deferred to the M1L audit-undo consumer.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.accounts import (
    AccountLinksHistoryPayload,
    AccountLinksPendingPayload,
    AccountLinksRunPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.account_links_service import (
    AccountLinksService,
)

app = typer.Typer(
    help="Review and manage account-link binding decisions",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("pending")
def links_pending(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List pending account-link decisions, grouped by provisional account.

    Shows provisional accounts with candidate merge proposals. Each group
    lists the candidate decision_id, account_id, display name, confidence,
    and match signal. Use `accounts links set` to decide each group.
    """
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            svc = AccountLinksService(db, actor="cli")
            groups = svc.pending()
            n_pending = svc.count_pending()

    payload = AccountLinksPendingPayload.from_service(groups, n_pending)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="accounts_links_pending",
        )
        return

    if not groups:
        if not quiet:
            logger.info("No pending account-link decisions")
        return

    for group in groups:
        typer.echo(
            f"\n── provisional {group.provisional_account_id} "
            f"({group.provisional_display_name or '-'}) "
            f"— {len(group.candidates)} candidate(s) ──"
        )
        typer.echo(
            f"  {'Decision ID':<14} {'Candidate ID':<14} {'Signal':<18} "
            f"{'Conf':>5}  {'Display Name'}"
        )
        for c in group.candidates:
            conf_str = f"{c.confidence:.2f}" if c.confidence is not None else "  -  "
            typer.echo(
                f"  {c.decision_id[:12]:<14} "
                f"{c.candidate_account_id[:12]:<14} "
                f"{c.signal:<18} "
                f"{conf_str:>5}  "
                f"{c.candidate_display_name or '-'}"
            )
    typer.echo()


@app.command("set")
def links_set(
    decision_id: str = typer.Argument(
        ..., help="Decision ID to act on (from `accounts links pending`)"
    ),
    into: str | None = typer.Option(
        None,
        "--into",
        help="Merge: the candidate account_id to adopt (from the pending group)",
    ),
    standalone: bool = typer.Option(
        False,
        "--standalone",
        help="Standalone-reject: keep the provisional account as its own canonical entity",
    ),
) -> None:
    """Accept (merge) or standalone-reject a pending account-link decision.

    Pass exactly one of:
      --into <candidate_account_id>   merge the provisional into the candidate
      --standalone                    reject all candidates; provisional stays standalone

    Examples:
      accounts links set dec001 --into ACC002
      accounts links set dec001 --standalone
    """
    if into is not None and standalone:
        logger.error("❌ --into and --standalone are mutually exclusive")
        raise typer.Exit(2)
    if into is None and not standalone:
        logger.error("❌ Specify either --into <account_id> or --standalone")
        raise typer.Exit(2)

    target_account_id: str | None = into if not standalone else None

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            AccountLinksService(db, actor="cli").set(
                decision_id, target_account_id=target_account_id, decided_by="user"
            )

    action = (
        f"merged into {target_account_id}"
        if target_account_id
        else "standalone (rejected)"
    )
    logger.info(f"✅ Decision {decision_id[:12]}... → {action}")


@app.command("history")
def links_history(
    limit: int = typer.Option(50, "--limit", "-n", help="Max records to show"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show recent account-link decisions (all statuses), newest first."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            rows = AccountLinksService(db, actor="cli").history(limit=limit)

    payload = AccountLinksHistoryPayload.from_rows(rows)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="accounts_links_history",
        )
        return

    if not rows:
        if not quiet:
            logger.info("No account-link decisions found")
        return

    typer.echo(
        f"\n{'Decision ID':<14} {'Provisional':<14} {'Candidate':<14} "
        f"{'Status':<10} {'Decided By':<10} {'Signal':<18} {'Conf':>5}"
    )
    typer.echo("-" * 90)
    for d in payload.decisions:
        conf_str = f"{d.confidence:.2f}" if d.confidence is not None else "  -  "
        typer.echo(
            f"{d.decision_id[:12]:<14} "
            f"{d.provisional_account_id[:12]:<14} "
            f"{d.candidate_account_id[:12]:<14} "
            f"{d.status:<10} "
            f"{d.decided_by:<10} "
            f"{d.signal:<18} "
            f"{conf_str:>5}"
        )
    typer.echo()


@app.command("run")
def links_run(
    output: OutputFormat = output_option,
) -> None:
    """Backfill pending account-link proposals for existing accounts.

    Finds weak-signal candidate pairs for every account in core.dim_accounts
    that has no pending proposal yet and writes pending decisions for review.

    Run this after importing accounts from multiple sources to surface
    cross-source twins for review.
    """
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            new_proposals = AccountLinksService(db, actor="cli").run()

    payload = AccountLinksRunPayload(new_proposals=new_proposals)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="accounts_links_run",
        )
        return

    if new_proposals == 0:
        typer.echo("No new account-link proposals written.")
    else:
        typer.echo(f"✅ Wrote {new_proposals} new pending account-link proposal(s).")
    typer.echo("Run `accounts links pending` to review.")
