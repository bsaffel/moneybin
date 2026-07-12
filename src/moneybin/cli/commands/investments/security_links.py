"""investments securities links — review-queue commands for security identity merges.

Subcommands: pending, set, history.
Mirrors `merchants links` (M1T) — thin wrappers over SecurityLinksService.
Unlike merchants links, there is no `run` subcommand: merge proposals are
filed by SecurityResolver during `sync pull`, not by a CLI-invoked harvest.

`investments securities links undo` is deliberately NOT registered: deferred
to the M1L audit-undo consumer, same as `merchants links undo`.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.investments import (
    SecurityLinksHistoryPayload,
    SecurityLinksPendingPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.security_links_service import SecurityLinksService

app = typer.Typer(
    help="Review security identity merge proposals",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("pending")
def links_pending(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List pending security merge decisions, grouped by provider ref.

    Shows the provider's ref (plaid_security_id or institution_security_id)
    with candidate merge-survivor proposals. The group header shows BOTH
    sides of the proposed merge: the provider's own ticker/name (what's
    being merged) next to each candidate's ticker/name (what it would merge
    into) — this matters most for a fuzzy_name proposal, where name
    similarity is the entire basis. Each candidate also lists its Reason
    (identifier_tie, exchange_contradiction, fuzzy_name, ...) — the field
    that conveys HOW risky accepting is; an identifier_tie is a much safer
    accept than an exchange_contradiction, which is a signal the two
    instruments are probably NOT the same. Use
    `investments securities links set` to decide each group.
    """
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            svc = SecurityLinksService(db, actor="cli")
            groups = svc.pending()
            n_pending = svc.count_pending()

    payload = SecurityLinksPendingPayload.from_service(groups, n_pending)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="investments_securities_links_pending",
        )
        return

    if not groups:
        if not quiet:
            logger.info("No pending security-link decisions")
        return

    for group in groups:
        typer.echo(
            f"\n── {group.ref_kind}:{group.ref_value[:20]} "
            f"provider=({group.provider_ticker or '-'} / "
            f"{group.provider_name or '-'}) "
            f"[{group.source_type}] "
            f"— {len(group.candidates)} candidate(s) ──"
        )
        typer.echo(
            f"  {'Decision ID':<14} {'Candidate ID':<14} {'Ticker':<8} "
            f"{'Conf':>5}  {'Reason':<20} {'Name'}"
        )
        for c in group.candidates:
            conf_str = f"{c.confidence:.2f}" if c.confidence is not None else "  -  "
            typer.echo(
                f"  {c.decision_id[:12]:<14} "
                f"{c.candidate_security_id[:12]:<14} "
                f"{(c.candidate_ticker or '-'):<8} "
                f"{conf_str:>5}  "
                f"{(c.match_reason or '-'):<20} "
                f"{c.candidate_name or '-'}"
            )
    typer.echo()


@app.command("set")
def links_set(
    decision_id: str = typer.Argument(
        ...,
        help="Decision ID to act on (from `investments securities links pending`)",
    ),
    accept: bool = typer.Option(
        False,
        "--accept",
        help="Accept: merge the provisional security into the decision's candidate",
    ),
    reject: bool = typer.Option(
        False,
        "--reject",
        help="Reject: keep the provisional security as its own distinct instrument",
    ),
    into: str | None = typer.Option(
        None,
        "--into",
        help=(
            "Required with --accept: the decision's own candidate_security_id "
            "(confirming safety check — must match, not just be A candidate)"
        ),
    ),
) -> None:
    """Accept (merge) or reject a pending security merge decision.

    Pass exactly one of:
      --accept --into <candidate_security_id>   merge into this candidate
      --reject                                  keep the provisional security;
                                                 this pairing is not re-proposed

    A merge re-points every accepted provider ref and tax lot onto the
    candidate in one transaction — review the candidate's ticker, name, and
    Reason in `investments securities links pending` before accepting.
    `--into` must equal the decision's own candidate_security_id: on a tied
    group the resolver files one decision per candidate, so this is the
    confirming check that stops a mistyped or stale decision_id from
    merging into the wrong security.

    Examples:
      investments securities links set dec001 --accept --into sec001aabbcc
      investments securities links set dec001 --reject
    """
    if accept and reject:
        logger.error("❌ --accept and --reject are mutually exclusive")
        raise typer.Exit(2)
    if not accept and not reject:
        logger.error("❌ Specify either --accept or --reject")
        raise typer.Exit(2)
    if reject and into is not None:
        logger.error("❌ --into is only valid with --accept")
        raise typer.Exit(2)
    if accept and not into:
        logger.error("❌ --accept requires --into <candidate_security_id>")
        raise typer.Exit(2)

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            svc = SecurityLinksService(db, actor="cli")
            if accept:
                svc.accept_merge(decision_id, into=into or "", decided_by="user")
            else:
                svc.reject_merge(decision_id, decided_by="user")

    action = f"merged into {into}" if accept else "rejected"
    logger.info(f"✅ Decision {decision_id[:12]}... → {action}")


@app.command("history")
def links_history(
    limit: int = typer.Option(50, "--limit", "-n", min=1, help="Max records to show"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show recent security-link decisions (all statuses), newest first."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            rows = SecurityLinksService(db, actor="cli").history(limit=limit)

    payload = SecurityLinksHistoryPayload.from_rows(rows)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="investments_securities_links_history",
        )
        return

    if not rows:
        if not quiet:
            logger.info("No security-link decisions found")
        return

    typer.echo(
        f"\n{'Decision ID':<14} {'Ref Value':<22} {'Candidate':<14} "
        f"{'Status':<10} {'Decided By':<10} {'Reason':<20} {'Conf':>5}"
    )
    typer.echo("-" * 100)
    for d in payload.decisions:
        conf_str = f"{d.confidence:.2f}" if d.confidence is not None else "  -  "
        typer.echo(
            f"{d.decision_id[:12]:<14} "
            f"{d.ref_value[:20]:<22} "
            f"{d.candidate_security_id[:12]:<14} "
            f"{d.status:<10} "
            f"{d.decided_by:<10} "
            f"{(d.match_reason or '-'):<20} "
            f"{conf_str:>5}"
        )
    typer.echo()
