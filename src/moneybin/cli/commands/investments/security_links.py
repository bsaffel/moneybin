"""investments securities links — review-queue commands for security ref decisions.

The queue holds two kinds of question that look alike to a reviewer and resolve
oppositely. An **identity** ref (``plaid_security_id``) asks whether two catalog
rows are one instrument; accepting MERGES and deletes the provisional. A
**feed-key** ref (``tiingo_ticker``, ``coingecko_slug``) asks whether a
market-data symbol names this security; accepting BINDS and deletes nothing.
`set --accept` routes on ref_kind and reports which one ran.

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
from moneybin.cli.render import render_rows
from moneybin.cli.utils import confidence_cell, handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.investments import (
    SecurityLinksHistoryPayload,
    SecurityLinksPendingPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.security_links_service import SecurityLinksService

app = typer.Typer(
    help="Review security identity merges and price-feed key proposals",
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
        render_rows(
            ["decision id", "candidate id", "ticker", "conf", "reason", "name"],
            [
                (
                    c.decision_id[:12],
                    c.candidate_security_id[:12],
                    c.candidate_ticker or "-",
                    confidence_cell(c.confidence),
                    c.match_reason or "-",
                    c.candidate_name or "-",
                )
                for c in group.candidates
            ],
            numeric=("conf",),
        )


@app.command("set")
def links_set(
    decision_id: str = typer.Argument(
        ...,
        help="Decision ID to act on (from `investments securities links pending`)",
    ),
    accept: bool = typer.Option(
        False,
        "--accept",
        help=(
            "Accept: an identity ref merges into the candidate, "
            "a feed-key ref only binds the symbol to it"
        ),
    ),
    reject: bool = typer.Option(
        False,
        "--reject",
        help="Reject: no merge and no binding; the pairing is not re-proposed",
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
    """Accept or reject a pending security-link decision.

    Pass exactly one of:
      --accept --into <candidate_security_id>   act on this candidate
      --reject                                  refuse the pairing;
                                                 it is not re-proposed

    What accepting does depends on the decision's ref_kind, shown in the group
    header of `investments securities links pending`:

    An identity ref (plaid_security_id) MERGES. It re-points every accepted
    provider ref, tax lot, manual investment ledger row, and price mark you set
    by hand onto the candidate in one transaction, then deletes the provisional
    catalog row.

    A feed-key ref (tiingo_ticker, coingecko_slug) BINDS. It records that the
    symbol prices this security, so later pulls fetch under that key. Nothing
    is re-pointed and nothing is deleted.

    Review the candidate's ticker, name, and Reason in
    `investments securities links pending` before accepting. There is no
    interactive prompt: `--into` is the confirmation, so this help text is the
    only place the blast radius is stated.
    `--into` must equal the decision's own candidate_security_id: on a tied
    group the resolver files one decision per candidate, so this is the
    confirming check that stops a mistyped or stale decision_id from acting on
    the wrong security.

    Examples:
      moneybin investments securities links set dec001 --accept --into sec001aabbcc
      moneybin investments securities links set dec001 --reject
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
                outcome = svc.accept(decision_id, into=into or "", decided_by="user")
            else:
                svc.reject_merge(decision_id, decided_by="user")
                outcome = None

    # The queue mixes two kinds of question and the service picks the mechanism,
    # so the word for it comes back from there. A fixed "merged" told the user
    # two securities had been combined when accepting a feed key only creates a
    # link and deletes nothing.
    if outcome is None:
        action = "rejected"
    else:
        action = f"{'bound to' if outcome == 'bound' else 'merged into'} {into}"
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

    render_rows(
        [
            "decision id",
            "ref value",
            "candidate",
            "status",
            "decided by",
            "reason",
            "conf",
        ],
        [
            (
                d.decision_id[:12],
                d.ref_value[:20],
                d.candidate_security_id[:12],
                d.status,
                d.decided_by,
                d.match_reason or "-",
                confidence_cell(d.confidence),
            )
            for d in payload.decisions
        ],
        numeric=("conf",),
    )
