"""accounts links — review-queue commands for account identity binding.

Subcommands: pending, set, history, run.
Mirrors `transactions matches` — thin wrappers over AccountLinksService.

`accounts links undo` is deliberately NOT YET registered:
deferred to the M1L audit-undo consumer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import typer

from moneybin import error_codes
from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.render import render_rows
from moneybin.cli.utils import (
    handle_cli_errors,
    warn_transfers_retired,
)
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.matching.reconciliation import RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED
from moneybin.privacy.payloads.accounts import (
    AccountLinksHistoryPayload,
    AccountLinksPendingPayload,
    AccountLinksRunPayload,
    LinkHistoryRow,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.account_links_service import (
    AccountLinkAcceptImpact,
    AccountLinksService,
)
from moneybin.services.account_resolution_types import UNNAMED_ACCOUNT_LABEL
from moneybin.services.identity_confirmation import identity_confirm_message
from moneybin.services.ledger_overlap import LedgerOverlap

if TYPE_CHECKING:
    from collections.abc import Callable

    from moneybin.database import Database
    from moneybin.orchestration.refresh import RefreshResult
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
    leads with the account's name and lists, per candidate, its name, match
    signal, ledger overlap, and the ids to act on. Use `accounts links set` to
    decide each group.
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
            f"\n── {group.provisional_display_name or UNNAMED_ACCOUNT_LABEL} "
            f"[{group.provisional_account_id}] "
            f"— {group.transactions:,} transactions move "
            f"— {len(group.candidates)} candidate(s) ──"
        )
        # Never truncated. A resolved display name ends in the masked last
        # four, so clipping it to the column width would cut off the digits
        # that tell two candidates at one institution apart — the exact
        # confusion this ordering exists to remove. `render_rows` folds a wide
        # value rather than eliding it, so a long name pushes the row ragged
        # instead of losing characters.
        render_rows(
            ["candidate", "signal", "ledger overlap", "decision id", "candidate id"],
            [
                (
                    c.candidate_display_name or "-",
                    c.signal,
                    _overlap_cell(c.overlap),
                    c.decision_id[:12],
                    c.candidate_account_id[:12],
                )
                for c in group.candidates
            ],
        )


def _overlap_cell(overlap: LedgerOverlap) -> str:
    """One cell of measured evidence, or an honest dash when there is none.

    "0 of 0" would read as two ledgers with nothing in common — evidence against
    the merge — where no comparable period means the probe could not look.

    The tolerance rides along because the ratio does not mean what it looks
    like without it: two sources date the same purchase differently, so these
    are amounts agreeing within a posting-lag window, not on an exact date. Read
    from the measurement rather than from the module default, so a caller that
    widened the window cannot render a number under a width it never used.
    """
    if not overlap.measurable:
        return "no shared period"
    return f"{overlap.matched:,} of {overlap.comparable:,} ±{overlap.window_days}d"


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
      moneybin accounts links set dec001 --into ACC002
      moneybin accounts links set dec001 --into ACC002 --yes
      moneybin accounts links set dec001 --standalone
    """
    if into is not None and standalone:
        logger.error("❌ --into and --standalone are mutually exclusive")
        raise typer.Exit(2)
    if into is None and not standalone:
        logger.error("❌ Specify either --into <account_id> or --standalone")
        raise typer.Exit(2)

    target_account_id: str | None = into if not standalone else None

    rematch: RefreshResult | None = None
    with handle_cli_errors():
        approved: _ApprovedMerge | None = None
        if target_account_id is not None and not yes:
            approved = _confirm_merge(decision_id, target_account_id)
        with get_database(read_only=False) as db:
            rematch = AccountLinksService(db, actor="cli").set(
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
    _report_rematch(rematch)
    if rematch is not None and rematch.error is not None:
        # `refresh_command` exits 1 on this identical RefreshResult.error, and
        # cli.md reads 1 as "operation ran and failed". The merge did commit,
        # but core.dim_accounts is kind FULL: until an apply lands it still
        # lists both accounts, so a script or agent gating on status would
        # record a collapse that is nowhere in the user's ledger.
        raise typer.Exit(1)


def _report_rematch(rematch: RefreshResult | None) -> None:
    """Say what the post-merge match pass did — it may have merged rows unasked.

    ``None`` means no pass ran (a standalone reject repoints nothing), and
    printing nothing is then the honest output.
    """
    if rematch is None:
        return
    # Deferred, and below the guard: matching_service pulls duckdb and the match
    # engine, this module is on the CLI cold-start path (.claude/rules/cli.md),
    # and the reject path reaches here needing none of it.
    from moneybin.services.matching_service import (  # noqa: PLC0415
        PENDING_MATCHES_HINT,
    )

    # The two failures are independent — refresh() attempts the transform step
    # whether or not the match step raised, so one call can carry both — and
    # they say different things: nothing was proposed, *and* the merge itself
    # is not visible yet. Neither may short-circuit the other.
    if rematch.matching_skipped:
        # Zero counts here mean nothing was examined, not that nothing was
        # found — so the clean-run line below would be inventing a result.
        logger.warning(
            "⚠️  Re-match after the merge could not run — its matching views "
            "were missing or stale, so the newly co-resident rows were never "
            "examined; re-run 'moneybin refresh'"
        )
    elif rematch.matching_error is not None:
        # Not "still unproposed": the matcher commits each edge as it goes and
        # wraps no transaction around the run, so a crash mid-pass leaves
        # earlier tiers' decisions durable. `MatchRunError` carries those counts
        # and `refresh` copies them onto the result, so this branch can name
        # them exactly instead of hedging — and zero is trustworthy too, because
        # a run that committed nothing raises unwrapped and never reaches the
        # branch that would populate them.
        landed = ", ".join(
            f"{count} {noun}"
            for count, noun in (
                (rematch.matches_auto_merged, "auto-merged"),
                (rematch.matches_pending_review, "new proposal(s)"),
                (rematch.matches_pending_transfers, "possible transfer(s)"),
            )
            if count
        )
        committed = (
            f"after committing {landed}, which are durable"
            if landed
            else "before it had committed anything"
        )
        logger.warning(
            f"⚠️  Re-match after the merge stopped partway {committed}; its "
            "remaining counts are incomplete — re-run 'moneybin refresh', then "
            "check 'moneybin review --type matches' and "
            "'moneybin system audit list'"
        )
    else:
        merged = rematch.matches_auto_merged
        pending = rematch.matches_pending_review
        # The pass is a full match run, so it raises Tier 4 transfer candidates
        # too. Judging it clean on the dedup counters alone would report "no new
        # duplicates" over a merge that just queued transfers for review.
        transfers = rematch.matches_pending_transfers
        if not merged and not pending and not transfers:
            logger.info("Re-matched after the merge: no new duplicates found")
        else:
            logger.info(
                f"👀 Re-matched after the merge: {merged} auto-merged, "
                f"{pending} new proposal(s) to review, "
                f"{transfers} possible transfer(s)"
            )
            if pending:
                logger.info(f"  💡 {PENDING_MATCHES_HINT}")
            if transfers:
                logger.info(
                    "  💡 Review possible transfers with "
                    "'moneybin review --type matches'"
                )
    # Independent of every branch above: this is a decision the *user* made being
    # undone, so it must be stated whether or not the pass was otherwise clean,
    # and it must name the way back.
    warn_transfers_retired(
        rematch.transfers_retired, cause=RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED
    )
    if rematch.error is not None:
        # The counts above are true — match decisions were written — but the
        # SQLMesh apply that follows them is what rebuilds core.dim_accounts,
        # a kind FULL model. Without it the merge is invisible: the accounts
        # dimension still lists both, which reads as a merge that never
        # happened. Reporting the counts and stopping would describe a
        # collapse the user cannot find anywhere.
        logger.warning(
            "⚠️  The rebuild after the merge failed, so the merge is not "
            "reflected in your accounts or totals yet; re-run "
            "'moneybin refresh'"
        )


def _plan_merge(
    db: Database, decision_id: str, target_account_id: str
) -> IdentityDecisionPlan:
    """Resolve the merge against ``db``, the way MCP plans the same decision."""
    # Deferred: the identity contracts and decision service are not worth loading
    # on every CLI invocation to gate one subcommand.
    from moneybin.protocol.write_contracts import (  # noqa: PLC0415 — keep off the cold-start path
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


@dataclass(frozen=True)
class _ApprovedMerge:
    """Everything the operator's yes was bound to.

    ``sentence`` is what the prompt printed — accounts, transactions, merchants
    — and moves when a concurrent import brings new history into either side.
    ``links`` and ``decisions`` are the rows the write physically touches: the
    ``account_links`` it repoints and the ``account_link_decisions`` it accepts
    and auto-rejects. Neither half covers the other. A concurrent
    ``accounts links run`` proposes one more candidate for the same provisional
    and moves only the rows, so holding just the sentence would commit a merge
    that silently rejects a decision nobody read.

    The rows are identities rather than counts because a swap keeps every count
    intact: one pending sibling resolved elsewhere while another arrives leaves
    the same number of decisions in play and still changes which one dies.

    The surviving account's own ledger size and the overlap ratio are rendered
    in the prompt but deliberately not held here. Neither is a consequence of
    the accept — the write touches the absorbed account's rows, which
    ``sentence`` already counts — and holding them by equality would refuse a
    correct merge whenever a concurrent import moved either one harmlessly.

    **Known accepted gap**, in both of them. Equality is the wrong test, but no
    test at all is not the right one, because each can also move the *unsafe*
    way between prompt and commit:

    - The empty-survivor warning ("the surviving account has no transactions of
      its own — check the direction"): a second accept can absorb the survivor's
      own history elsewhere and leave it an empty placeholder, so a warning that
      should have fired never does.
    - The overlap ratio: the probe's comparison window is ``MIN``/``MAX`` over
      the *survivor's* dates, so one survivor-side row arriving outside it
      widens the window and admits absorbed rows that match nothing. A ratified
      "40 of 40" commits as "40 of 400" with the sentence unchanged. Pinned by
      ``test_a_wider_survivor_span_pulls_more_rows_into_comparable``.

    Both want the same missing mechanism: re-verify asymmetrically inside the
    write transaction — refuse when the evidence got worse, never when it got
    better — on this path *and* on the MCP grant, whose binding is a frozen
    symmetric digest and would need a transported-but-undigested baseline to
    express the asymmetry. That is a change to a primitive every destructive
    MCP tool shares, so both halves land together in their own change or the
    two surfaces disagree about what a confirmation covers.
    """

    sentence: dict[str, int]
    links: tuple[str, ...]
    decisions: tuple[str, ...]


def _merge_preview(
    decision_id: str, target_account_id: str
) -> tuple[_ApprovedMerge, str] | None:
    """Resolve both radii and the prompt text read-only, the way MCP previews it.

    ``None`` when the accept changes nothing — a decision already settled onto
    this same candidate. The destructive check has to come first: ``accept_impact``
    refuses a non-pending decision, so asking it about that legitimate re-run
    would raise where the command should simply pass through.
    """
    with get_database(read_only=True) as db:
        plan = _plan_merge(db, decision_id, target_account_id)
        if not plan.destructive:
            return None
        service = AccountLinksService(db, actor="cli")
        impact = service.accept_impact(decision_id, target_account_id=target_account_id)
        facts = service.merge_facts(
            absorbed_account_id=impact.provisional_account_id,
            survivor_account_id=target_account_id,
        )
    approved = _ApprovedMerge(
        sentence=plan.blast_radius,
        links=impact.link_ids,
        decisions=impact.decision_ids,
    )
    return approved, identity_confirm_message(
        plan.blast_radius, surface="cli", merges=[facts], kinds=["account_link"]
    )


def _drift_check(
    db: Database, decision_id: str, approved: _ApprovedMerge | None
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
        # Both halves, because a merge can move in either one alone. Re-planning
        # catches history that arrived on either account; ``impact`` — which the
        # service already recomputed on this transaction's own read — names the
        # exact links this write repoints and the exact decisions it settles,
        # none of which the plan's arithmetic counts.
        current = _plan_merge(db, decision_id, impact.candidate_account_id).blast_radius
        if (
            current != approved.sentence
            or impact.link_ids != approved.links
            or impact.decision_ids != approved.decisions
        ):
            raise UserError(
                "This merge changed while the confirmation was open, so nothing "
                "was written. Re-run the command to see what it moves now.",
                # Not MUTATION_CONSTRAINT_VIOLATION, which this file already uses
                # for a structurally invalid decision. A stale approval is the
                # retriable failure, and an agent reading --output json has to
                # tell the two apart; import_cmd.py's delete_saved_format raises
                # this same code from the same preview → confirm → verify shape.
                code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
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


def _confirm_merge(decision_id: str, target_account_id: str) -> _ApprovedMerge | None:
    """Show what the merge moves, require a yes, and return what was approved.

    Renders the same sentence the MCP elicitation shows, from the same preflight,
    because the decision does not change with the surface driving it: an accepted
    link folds one account's whole history into another and no command splits it
    back apart. This was the last accept path that committed unasked.

    Returns both radii the operator's yes was bound to, so the write can hold
    itself to them. A merge that turns out to move nothing — the decision is
    already settled — returns ``None`` and asks nothing.
    """
    preview = _merge_preview(decision_id, target_account_id)
    if preview is None:
        return None
    approved, message = preview
    typer.echo(message, err=True)
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

    def _merged(d: LinkHistoryRow) -> str:
        """Both sides of one merge, named rather than identified.

        A frozen "" means the freeze looked and declined to record a raw-only
        name. Falling back to a truncated id puts an id where a name goes — the
        defect this whole surface exists to fix — and a 12-char prefix is not
        even usable as a handle. The shared constant is the same string
        core.dim_accounts puts in this column for a row it could not name, so
        the two cannot render as two spellings in one table.
        """
        return (
            f"{d.provisional_display_name or UNNAMED_ACCOUNT_LABEL} → "
            f"{d.candidate_display_name or UNNAMED_ACCOUNT_LABEL}"
        )

    # `merged` holds two resolved names and the arrow between them, which
    # dim_accounts builds as institution + subtype + masked last four — about
    # 55 characters each. The renderer sizes the column to the widest value
    # rather than to a guessed width, which is what the hand-built version
    # could not do: a column sized for one name left every named row ragged and
    # aligned only the rows that had fallen back to ids.
    render_rows(
        ["merged", "status", "decided by", "signal", "decision id"],
        [
            (_merged(d), d.status, d.decided_by, d.signal, d.decision_id[:12])
            for d in payload.decisions
        ],
    )


@app.command("run")
def links_run(
    account_id: str | None = typer.Argument(
        None, help="Account absorbed on accept (requires CANDIDATE_ACCOUNT_ID)"
    ),
    candidate_account_id: str | None = typer.Argument(
        None, help="Account kept on accept (requires ACCOUNT_ID)"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Propose account merges: sweep for duplicates, or link two accounts by id.

    With no arguments, finds weak-signal candidate pairs for every account in
    core.dim_accounts that has no pending proposal yet. Run this after importing
    accounts from multiple sources to surface cross-source twins.

    With two account ids, proposes exactly that pair — the escape hatch for a
    duplicate no signal reaches, where nothing matches but you know it is one
    account. Order is direction: the first id is absorbed and the second kept,
    whenever both are absorbable. Neither form merges: both write proposals for
    `accounts links set`.
    """
    if (account_id is None) != (candidate_account_id is None):
        logger.error(
            "❌ Naming one account is ambiguous. Pass both ids to propose that "
            "pair, or neither to sweep every account for duplicates."
        )
        raise typer.Exit(2)

    decision_id: str | None = None
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            svc = AccountLinksService(db, actor="cli")
            if account_id is None or candidate_account_id is None:
                new_proposals = svc.run()
            else:
                decision_id = svc.propose_pair(account_id, candidate_account_id)
                new_proposals = 1

    payload = AccountLinksRunPayload(
        new_proposals=new_proposals, decision_id=decision_id
    )

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json  # noqa: PLC0415 — defer import

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="accounts_links_run",
        )
        return

    if decision_id is not None:
        typer.echo(f"✅ Proposed the pair as decision {decision_id}.")
    elif new_proposals == 0:
        typer.echo("No new account-link proposals written.")
    else:
        typer.echo(f"✅ Wrote {new_proposals} new pending account-link proposal(s).")
    typer.echo("Run `accounts links pending` to review.")
