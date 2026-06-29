"""merchants links — review-queue commands for merchant entity id binding.

Subcommands: pending, set, history, run.
Mirrors `accounts links` — thin wrappers over MerchantLinksService.

`merchants links undo` is deliberately NOT registered: deferred to the
M1L audit-undo consumer, same as `accounts links undo`.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.merchants import (
    MerchantLinksHistoryPayload,
    MerchantLinksPendingPayload,
    MerchantLinksRunPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.merchant_links_service import MerchantLinksService

app = typer.Typer(
    help="Review and manage merchant-link binding decisions",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("pending")
def links_pending(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List pending merchant-link decisions, grouped by provider entity id.

    Shows provider entity ids with candidate merchant proposals. Each group
    lists the candidate decision_id, merchant_id, canonical name, and
    confidence. Use `merchants links set` to decide each group.
    """
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            svc = MerchantLinksService(db, actor="cli")
            groups = svc.pending()
            n_pending = svc.count_pending()

    payload = MerchantLinksPendingPayload.from_service(groups, n_pending)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="merchants_links_pending",
        )
        return

    if not groups:
        if not quiet:
            logger.info("No pending merchant-link decisions")
        return

    for group in groups:
        typer.echo(
            f"\n── entity {group.ref_value[:20]} "
            f"({group.provider_merchant_name or '-'}) "
            f"[{group.source_type}] "
            f"— {len(group.candidates)} candidate(s) ──"
        )
        typer.echo(
            f"  {'Decision ID':<14} {'Merchant ID':<14} {'Conf':>5}  {'Canonical Name'}"
        )
        for c in group.candidates:
            conf_str = f"{c.confidence:.2f}" if c.confidence is not None else "  -  "
            typer.echo(
                f"  {c.decision_id[:12]:<14} "
                f"{c.candidate_merchant_id[:12]:<14} "
                f"{conf_str:>5}  "
                f"{c.candidate_canonical_name or '-'}"
            )
    typer.echo()


@app.command("set")
def links_set(
    decision_id: str = typer.Argument(
        ..., help="Decision ID to act on (from `merchants links pending`)"
    ),
    into: str | None = typer.Option(
        None,
        "--into",
        help="Accept: bind this provider entity id to the decision's candidate merchant_id (confirming safety check)",
    ),
    new: bool = typer.Option(
        False,
        "--new",
        help="Reject all candidates; resolver mints a new merchant on its next categorization pass",
    ),
) -> None:
    """Accept (bind) or reject a pending merchant-link decision.

    Pass exactly one of:
      --into <candidate_merchant_id>   bind the provider entity id to this merchant
      --new                            reject; resolver mints a new merchant on next run

    Examples:
      merchants links set dec001 --into merch0001aa
      merchants links set dec001 --new
    """
    if into is not None and new:
        logger.error("❌ --into and --new are mutually exclusive")
        raise typer.Exit(2)
    # Truthiness, not `is None`: an empty `--into ""` is not a valid merchant id
    # and must not silently fall through to the bind path.
    if not into and not new:
        logger.error("❌ Specify either --into <merchant_id> or --new")
        raise typer.Exit(2)

    target_merchant_id: str | None = into if not new else None

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            MerchantLinksService(db, actor="cli").set(
                decision_id, target_merchant_id=target_merchant_id, decided_by="user"
            )

    action = (
        f"bound to {target_merchant_id}"
        if target_merchant_id
        else "new merchant (rejected)"
    )
    logger.info(f"✅ Decision {decision_id[:12]}... → {action}")


@app.command("history")
def links_history(
    limit: int = typer.Option(50, "--limit", "-n", min=0, help="Max records to show"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show recent merchant-link decisions (all statuses), newest first."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            rows = MerchantLinksService(db, actor="cli").history(limit=limit)

    payload = MerchantLinksHistoryPayload.from_rows(rows)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="merchants_links_history",
        )
        return

    if not rows:
        if not quiet:
            logger.info("No merchant-link decisions found")
        return

    typer.echo(
        f"\n{'Decision ID':<14} {'Ref Value':<22} {'Candidate':<14} "
        f"{'Status':<10} {'Decided By':<10} {'Conf':>5}"
    )
    typer.echo("-" * 80)
    for d in payload.decisions:
        conf_str = f"{d.confidence:.2f}" if d.confidence is not None else "  -  "
        typer.echo(
            f"{d.decision_id[:12]:<14} "
            f"{d.ref_value[:20]:<22} "
            f"{d.candidate_merchant_id[:12]:<14} "
            f"{d.status:<10} "
            f"{d.decided_by:<10} "
            f"{conf_str:>5}"
        )
    typer.echo()


@app.command("run")
def links_run(
    output: OutputFormat = output_option,
) -> None:
    """Harvest existing categorization facts into merchant-link bindings.

    Binds provider entity ids that point unambiguously to a single canonical
    merchant (recorded immediately, no review), and routes one-id-many-merchant
    conflicts to the pending review queue. Reports the two outcomes distinctly.

    Run this after importing transactions with merchant_entity_id data to
    surface binding decisions for review.
    """
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            result = MerchantLinksService(db, actor="cli").run()

    payload = MerchantLinksRunPayload(bound=result.bound, conflicts=result.conflicts)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="merchants_links_run",
        )
        return

    if result.bound == 0 and result.conflicts == 0:
        typer.echo("No merchant-link bindings or conflicts found.")
    else:
        typer.echo(
            f"✅ Recorded {result.bound} merchant binding(s); "
            f"queued {result.conflicts} conflict(s) for review."
        )
    typer.echo("Run `merchants links pending` to review.")
