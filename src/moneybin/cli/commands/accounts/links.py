"""accounts links — review-queue commands for account identity binding.

Subcommands: pending, set, history, run.
Mirrors `transactions matches` — thin wrappers over AccountLinksService.

`accounts links undo` is deliberately NOT YET registered:
deferred to the M1L audit-undo consumer.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import typer

from moneybin import error_codes
from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.privacy.payloads.accounts import (
    AccountLinksHistoryPayload,
    AccountLinksPendingPayload,
    AccountLinksRunPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.account_links_service import (
    AccountLinkAcceptImpact,
    AccountLinksService,
)
from moneybin.services.identity_confirmation import identity_confirm_message

if TYPE_CHECKING:
    from collections.abc import Callable

    from moneybin.database import Database
    from moneybin.services.review_decisions_service import IdentityDecisionPlan

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
            f"  {'Decision ID':<14} {'Candidate ID':<14} {'Signal':<20} "
            f"{'Conf':>5}  {'Display Name'}"
        )
        for c in group.candidates:
            conf_str = f"{c.confidence:.2f}" if c.confidence is not None else "  -  "
            typer.echo(
                f"  {c.decision_id[:12]:<14} "
                f"{c.candidate_account_id[:12]:<14} "
                f"{c.signal:<20} "
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
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip the merge confirmation prompt (--into only; --standalone never asks)",
    ),
) -> None:
    """Accept (merge) or standalone-reject a pending account-link decision.

    Pass exactly one of:
      --into <candidate_account_id>   merge the provisional into the candidate
      --standalone                    reject all candidates; provisional stays standalone

    A merge shows what it moves and asks before committing; pass --yes to answer
    it in advance. Rejecting is never gated — the provisional account stays where
    it is.

    Examples:
      accounts links set dec001 --into ACC002
      accounts links set dec001 --into ACC002 --yes
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
        approved: dict[str, int] | None = None
        if target_account_id is not None and not yes:
            approved = _confirm_merge(decision_id, target_account_id)
        with get_database(read_only=False) as db:
            AccountLinksService(db, actor="cli").set(
                decision_id,
                target_account_id=target_account_id,
                decided_by="user",
                verify_accept=_drift_check(db, decision_id, approved),
            )

    action = (
        f"merged into {target_account_id}"
        if target_account_id
        else "standalone (rejected)"
    )
    logger.info(f"✅ Decision {decision_id[:12]}... → {action}")


def _plan_merge(
    db: Database, decision_id: str, target_account_id: str
) -> IdentityDecisionPlan:
    """Resolve the merge against ``db``, the way MCP plans the same decision."""
    # Deferred: the identity contracts and decision service are not worth loading
    # on every CLI invocation to gate one subcommand.
    from moneybin.mcp.write_contracts import (  # noqa: PLC0415 — keep off the cold-start path
        AccountLinkDecisionRequest,
    )
    from moneybin.services.review_decisions_service import (  # noqa: PLC0415 — keep off the cold-start path
        ReviewDecisionsService,
    )

    try:
        return ReviewDecisionsService(db, actor="cli").plan_identity([
            AccountLinkDecisionRequest(
                kind="account_link",
                decision_id=decision_id,
                decision="accept",
                target_id=target_account_id,
            )
        ])
    except UserError as exc:
        raise _preflight_reason(exc) from exc


def _merge_preview(decision_id: str, target_account_id: str) -> IdentityDecisionPlan:
    """Resolve the merge read-only, the way MCP previews the same decision."""
    with get_database(read_only=True) as db:
        return _plan_merge(db, decision_id, target_account_id)


def _drift_check(
    db: Database, decision_id: str, approved: dict[str, int] | None
) -> Callable[[AccountLinkAcceptImpact], None] | None:
    """Refuse the write if the merge stopped matching the sentence the operator read.

    The prompt reads on its own read-only connection and closes it, so the plan
    shown and the plan committed are two separate reads with a human in between
    — long enough for a concurrent import or sync to widen the merge. The
    service calls this back inside its write transaction immediately before the
    first mutation, which is the only place the comparison is worth anything;
    ``mcp/tools/accounts.py`` verifies its own grant through the same hook.

    ``None`` when nothing was approved: ``--yes`` and ``--standalone`` have no
    displayed radius to hold the commit to, and inventing one would refuse a
    write on a comparison the operator never saw.
    """
    if approved is None:
        return None

    def verify(impact: AccountLinkAcceptImpact) -> None:
        # Re-plans rather than reading impact.blast_radius: that one counts link
        # rows, while the sentence the operator answered counts transactions and
        # accounts. Only the plan's own arithmetic can contradict what was shown.
        current = _plan_merge(db, decision_id, impact.candidate_account_id).blast_radius
        if current != approved:
            raise UserError(
                "This merge changed while the confirmation was open, so nothing "
                "was written. Re-run the command to see what it moves now.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )

    return verify


#: The per-decision codes ``plan_identity`` can attach, mapped to themselves.
#: The lookup is what keeps the unwrapped code declared: ``--output json`` puts
#: it in the envelope an agent reads, and a code assembled from a dict is
#: invisible to the literal scan that proves every wire code was declared. An
#: unrecognized value degrades to the batch code rather than reaching the wire
#: unannounced.
_PREFLIGHT_CODES = {
    code: code
    for code in (
        error_codes.MUTATION_INVALID_INPUT,
        error_codes.MUTATION_NOT_FOUND,
        error_codes.MUTATION_CONSTRAINT_VIOLATION,
        error_codes.MUTATION_NOTHING_TO_DO,
    )
}


def _preflight_reason(exc: UserError) -> UserError:
    """Unwrap a one-decision preflight failure back into its own message.

    ``plan_identity`` batches per-decision reasons into ``details["errors"]``,
    which the MCP envelope carries but the CLI's text mode never prints — it
    shows ``message`` and ``hint`` only. A mistyped decision id would otherwise
    read "Identity decision preflight failed." and nothing else, where the
    ungated command used to name the id it could not find. One request can
    produce at most one reason, so surface it directly.
    """
    errors: list[dict[str, object]] = (exc.details or {}).get("errors") or []
    if len(errors) != 1:
        return exc
    (error,) = errors
    return UserError(
        str(error["reason"]),
        code=_PREFLIGHT_CODES.get(str(error["code"]), exc.code),
    )


def _confirm_merge(decision_id: str, target_account_id: str) -> dict[str, int] | None:
    """Show what the merge moves, require a yes, and return what was approved.

    Renders the same sentence the MCP elicitation shows, from the same preflight,
    because the decision does not change with the surface driving it: an accepted
    link folds one account's whole history into another and no command splits it
    back apart. This was the last accept path that committed unasked.

    Returns the blast radius the operator actually read, so the write can hold
    itself to it. A plan that turns out not to be destructive — the decision is
    already accepted, so nothing moves — returns ``None`` and asks nothing.
    """
    plan = _merge_preview(decision_id, target_account_id)
    if not plan.destructive:
        return None
    approved = plan.blast_radius
    typer.echo(identity_confirm_message(approved), err=True)
    if not typer.confirm("Merge these accounts?", default=False, err=True):
        typer.echo("Cancelled — nothing was merged.", err=True)
        raise typer.Exit(0)
    return approved


@app.command("history")
def links_history(
    limit: int = typer.Option(50, "--limit", "-n", min=0, help="Max records to show"),
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
        f"{'Status':<10} {'Decided By':<10} {'Signal':<20} {'Conf':>5}"
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
            f"{d.signal:<20} "
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
