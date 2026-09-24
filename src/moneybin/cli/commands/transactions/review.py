"""Unified review queue: walks pending matches + uncategorized transactions + account-links.

CLI-only collapse (per moneybin-cli.md v2). MCP keeps separate
``transactions_matches_pending`` and ``transactions_categorize_pending``
tools because their result shapes differ; the orientation tool
``review`` returns the counts.

Counts are the default. The item-by-item walk is stubbed for v2 and reachable
only via ``--interactive``, so no invocation a user is likely to type lands on
the stub.

``transactions_review`` is a **deprecated alias** for the top-level
``review_command`` (``moneybin review``). It emits a deprecation warning
then delegates to ``review_impl``.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.prompts import Choice, choose_required
from moneybin.cli.render import build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope

from ..stubs import _not_implemented

logger = logging.getLogger(__name__)

_VALID_TYPES = {
    "all",
    "matches",
    "categorize",
    "account-links",
    "merchant-links",
    "security-links",
}


def review_impl(
    type_: str | None,
    status: bool,
    interactive: bool,
    confirm_id: str | None,
    reject_id: str | None,
    confirm_all: bool,
    limit: int,
    no_pager: bool,
    output: OutputFormat,
    quiet: bool,  # the status path emits data only; nothing to suppress
) -> None:
    """Shared impl for `moneybin review` and its deprecated `transactions review` alias.

    Extracted so both the top-level leaf and the deprecated alias can call it
    without duplicating logic.
    """
    # Counts have a meaningful all-queues view; a mutation does not.  Ask the
    # human to name its only supported queue instead of treating the first
    # available queue as an implicit target.
    decides = bool(confirm_id or reject_id or confirm_all)
    if type_ is None:
        if decides:
            type_ = choose_required(
                None,
                choices=(Choice("matches", "Matches"),),
                flag="--type",
                policy=get_terminal_policy(),
            )
        else:
            type_ = "all"
    if type_ not in _VALID_TYPES:
        raise typer.BadParameter(
            f"--type must be one of {sorted(_VALID_TYPES)}, got {type_!r}"
        )

    # Three mutually exclusive modes. Silently letting one win would either drop
    # a requested mutation or perform an unrequested one, so say so instead —
    # the same call the two guards in `_review_matches_noninteractive` make.
    if sum((status, interactive, decides)) > 1:
        logger.error(
            "--status, --interactive, and --confirm/--reject/--confirm-all "
            "select different modes; pass only one"
        )
        raise typer.Exit(2)

    if decides:
        # `--type matches` stays explicit rather than being inferred from the
        # decide flags. They are match-only today, but the queued work extends
        # them to categorize and the three link queues — at which point a bare
        # `--confirm <id>` names an id whose queue we would have to guess, and
        # guessing wrong accepts the wrong decision silently.
        if type_ != "matches":
            logger.error(
                "--confirm/--reject/--confirm-all require --type matches "
                "(categorize non-interactive review is not yet supported)"
            )
            raise typer.Exit(2)
        _review_matches_noninteractive(
            confirm_id=confirm_id,
            reject_id=reject_id,
            confirm_all=confirm_all,
            limit=limit,
            output=output,
        )
        return

    if interactive:
        # Not a whole-command stub: `review` works, and only this mode is
        # unfinished. `stubs.py` explains what that excludes it from.
        _not_implemented("the interactive review loop", whole_command=False)
        return

    # Counts are the default: they are what `--help` describes, and the only
    # thing the command can do for every queue today.
    _print_status(type_, output, no_pager=no_pager)


def transactions_review(
    type_: str | None = typer.Option(
        None,
        "--type",
        help="all | matches | categorize | account-links | merchant-links | security-links",
    ),
    status: bool = typer.Option(
        False, "--status", help="Show queue counts (the default)"
    ),
    interactive: bool = typer.Option(
        False, "--interactive", help="Walk the queue item by item (not yet built)"
    ),
    confirm_id: str | None = typer.Option(
        None, "--confirm", help="Non-interactive: confirm one item by ID"
    ),
    reject_id: str | None = typer.Option(
        None, "--reject", help="Non-interactive: reject one item by ID"
    ),
    confirm_all: bool = typer.Option(
        False, "--confirm-all", help="Non-interactive: confirm all items in scope"
    ),
    limit: int = typer.Option(50, "--limit", help="Cap items per session"),
    no_pager: bool = no_pager_option,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pending counts across all review queues.

    DEPRECATED: use `moneybin review` instead. Removed after one minor release.
    """
    typer.echo(
        "`moneybin transactions review` is deprecated. "
        "Use `moneybin review` instead. Removed after one minor release.",
        err=True,
    )
    review_impl(
        type_=type_,
        status=status,
        interactive=interactive,
        confirm_id=confirm_id,
        reject_id=reject_id,
        confirm_all=confirm_all,
        limit=limit,
        no_pager=no_pager,
        output=output,
        quiet=quiet,
    )


def _review_matches_noninteractive(
    *,
    confirm_id: str | None,
    reject_id: str | None,
    confirm_all: bool,
    limit: int,
    output: OutputFormat,
) -> None:
    from moneybin.cli.utils import (
        handle_cli_errors,
        warn_transfers_retired,
    )
    from moneybin.matching.reconciliation import RETIRED_SIDES_COLLAPSED
    from moneybin.services.matching_service import (
        MatchDecisionOutcome,
        MatchingService,
    )

    # --confirm-all bulk-accepts the whole queue; pairing it with a targeted
    # --confirm/--reject is ambiguous (the targeted id would be silently
    # dropped), so reject the combination as a usage error rather than run a
    # partial action.
    if confirm_all and (confirm_id or reject_id):
        logger.error("--confirm-all cannot be combined with --confirm or --reject")
        raise typer.Exit(2)

    # Same id to both flags would accept then immediately fail the reject (the
    # match is no longer pending), leaving the accept silently committed behind
    # an error exit. Reject the contradiction up front, like the guard above.
    if confirm_id is not None and confirm_id == reject_id:
        logger.error("--confirm and --reject cannot target the same match_id")
        raise typer.Exit(2)

    with handle_cli_errors(cli_actor="review"):
        with get_database(read_only=False) as db:
            svc = MatchingService(db)
            if confirm_all:
                selection = svc.preview_pending(limit=limit)
                if output == OutputFormat.TEXT:
                    from moneybin.cli.render import render_summary
                    from moneybin.cli.utils import confidence_cell

                    render_summary(
                        [
                            ("Scope", f"{len(selection.ids)} pending match(es)"),
                            ("Effect", "Accept the exact matches below"),
                            *[
                                (
                                    item.match_id,
                                    f"{item.match_type}: "
                                    f"{item.source_transaction_id_a} ↔ "
                                    f"{item.source_transaction_id_b}; "
                                    f"confidence {confidence_cell(item.confidence_score)}",
                                )
                                for item in selection.items
                            ],
                        ],
                        title="Confirm match decisions",
                    )
                bulk = svc.accept_previewed(selection, actor="cli")
                if output == OutputFormat.JSON:
                    from moneybin import error_codes
                    from moneybin.errors import ErrorDetail
                    from moneybin.privacy.payloads.transactions import (
                        MatchBulkSetPayload,
                    )

                    envelope = build_envelope(
                        data=MatchBulkSetPayload(
                            requested=len(selection.ids),
                            accepted=bulk.accepted,
                            reversed_by_reconciliation=(
                                bulk.reversed_by_reconciliation
                            ),
                            transfers_retired=bulk.transfers_retired,
                            accounting_stale=bulk.accounting_stale,
                            accounting_hint=bulk.accounting_hint,
                        ),
                        sensitivity="low",
                    )
                    if bulk.reversed_by_reconciliation or bulk.accounting_stale:
                        envelope = envelope.with_error(
                            ErrorDetail(
                                message=(
                                    f"{bulk.reversed_by_reconciliation} requested "
                                    "match decision(s) were reversed by reconciliation."
                                    if bulk.reversed_by_reconciliation
                                    else "Match decisions were saved, but FX accounting is stale."
                                ),
                                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                            )
                        )
                    render_or_json(envelope, output, cli_actor="review")
                else:
                    stale_disclosures = ()
                    if bulk.accounting_stale:
                        stale_disclosures = (
                            "! Match decisions were saved, but FX accounting is stale. "
                            f"{bulk.accounting_hint or 'Refresh before relying on FX reports.'}",
                        )
                    emit_human_result(
                        compose_human_result(
                            [
                                build_summary(
                                    [
                                        ("Requested", str(len(selection.ids))),
                                        ("Accepted", str(bulk.accepted)),
                                        (
                                            "Review scope",
                                            f"First {selection.limit} in review order",
                                        ),
                                    ],
                                    title="Match decisions saved",
                                )
                            ],
                            disclosures=stale_disclosures,
                        ),
                        policy=get_terminal_policy(),
                        finite_read=False,
                        receipt=True,
                    )
                if bulk.reversed_by_reconciliation:
                    # Named separately from the retirement warning below: that
                    # one counts every transfer this call reversed, most of
                    # which the user accepted in some earlier session. This
                    # counts the rows they just asked for and did not get.
                    logger.warning(
                        f"{bulk.reversed_by_reconciliation} of them did not "
                        "stand: an accepted transfer already claims the merged "
                        "pair, and the earlier decision stands"
                    )
                warn_transfers_retired(
                    bulk.transfers_retired,
                    cause=RETIRED_SIDES_COLLAPSED,
                    rematch_follow_up=True,
                )
                # Part of what the caller asked for did not commit, which
                # cli.md reads as a failed operation. --confirm-all is the
                # surface most likely to run unattended, so the status is the
                # only signal some callers will ever check.
                if bulk.reversed_by_reconciliation or bulk.accounting_stale:
                    raise typer.Exit(1)
                return
            # Independent ifs (not elif): `--confirm X --reject Y` targets two
            # different matches in one invocation.
            refused = False
            outcomes: list[tuple[str, MatchDecisionOutcome]] = []
            if confirm_id:
                outcome = svc.set_status(confirm_id, status="accepted", actor="cli")
                outcomes.append((confirm_id, outcome))
                if outcome.match_status == "accepted":
                    if output == OutputFormat.TEXT:
                        emit_human_result(
                            compose_human_result([
                                build_summary(
                                    [("Match", confirm_id), ("Decision", "accepted")],
                                    title="Match decision saved",
                                )
                            ]),
                            policy=get_terminal_policy(),
                            finite_read=False,
                            receipt=True,
                        )
                else:
                    # Same refusal `matches set` can hit: the reconciliation this
                    # accept triggers walks every accepted transfer, this row
                    # included, and the earliest-decided one keeps the component.
                    logger.warning(
                        f"Match {confirm_id} was not accepted: it is "
                        f"{outcome.match_status} — an accepted transfer already "
                        "claims the merged pair, and the earlier decision stands"
                    )
                    refused = True
                warn_transfers_retired(
                    outcome.transfers_retired,
                    cause=RETIRED_SIDES_COLLAPSED,
                    rematch_follow_up=True,
                )
            if reject_id:
                try:
                    outcome = svc.set_status(reject_id, status="rejected", actor="cli")
                except Exception as exc:
                    from moneybin.errors import ErrorDetail, classify_user_error
                    from moneybin.privacy.payloads.transactions import (
                        MatchReviewDecisionPayload,
                        MatchSetPayload,
                    )

                    user_error = classify_user_error(exc)
                    if (
                        output != OutputFormat.JSON
                        or not outcomes
                        or user_error is None
                    ):
                        raise
                    payload = MatchReviewDecisionPayload(
                        outcomes=[
                            MatchSetPayload(
                                match_id=match_id,
                                match_status=saved_outcome.match_status,
                                transfers_retired=saved_outcome.transfers_retired,
                            )
                            for match_id, saved_outcome in outcomes
                        ]
                    )
                    envelope = build_envelope(
                        data=payload,
                        sensitivity="low",
                        recovery_actions=user_error.recovery_actions,
                    ).with_error(ErrorDetail.from_user_error(user_error))
                    render_or_json(envelope, output, cli_actor="review")
                    raise typer.Exit(1) from exc
                outcomes.append((reject_id, outcome))
                if output == OutputFormat.TEXT:
                    emit_human_result(
                        compose_human_result([
                            build_summary(
                                [("Match", reject_id), ("Decision", "rejected")],
                                title="Match decision saved",
                            )
                        ]),
                        policy=get_terminal_policy(),
                        finite_read=False,
                        receipt=True,
                    )
            if output == OutputFormat.JSON:
                from moneybin.privacy.payloads.transactions import (
                    MatchReviewDecisionPayload,
                    MatchSetPayload,
                )

                decision_payloads = [
                    MatchSetPayload(
                        match_id=match_id,
                        match_status=outcome.match_status,
                        transfers_retired=outcome.transfers_retired,
                    )
                    for match_id, outcome in outcomes
                ]
                payload = (
                    decision_payloads[0]
                    if len(decision_payloads) == 1
                    else MatchReviewDecisionPayload(outcomes=decision_payloads)
                )
                envelope = build_envelope(data=payload, sensitivity="low")
                if refused:
                    from moneybin import error_codes
                    from moneybin.errors import ErrorDetail

                    envelope = envelope.with_error(
                        ErrorDetail(
                            message=(
                                "A requested match acceptance was reversed by "
                                "reconciliation."
                            ),
                            code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                        )
                    )
                render_or_json(envelope, output, cli_actor="review")
            # After both, for the same reason they are independent ifs: a
            # refused confirm must not skip the reject the caller also asked
            # for. Same exit code as `matches set` on the identical refusal.
            if refused:
                raise typer.Exit(1)


def _print_status(type_: str, output: OutputFormat, *, no_pager: bool) -> None:
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_settings
    from moneybin.services.account_links_service import AccountLinksService
    from moneybin.services.categorization import CategorizationService
    from moneybin.services.matching_service import MatchingService
    from moneybin.services.merchant_links_service import MerchantLinksService
    from moneybin.services.review_service import ReviewService
    from moneybin.services.security_links_service import SecurityLinksService

    with handle_cli_errors(cli_actor="review"):
        with get_database(read_only=True) as db:
            review_svc = ReviewService(
                match_service=MatchingService(db, get_settings().matching),
                categorize_service=CategorizationService(db),
                account_links_service=AccountLinksService(db),
                merchant_links_service=MerchantLinksService(db),
                security_links_service=SecurityLinksService(db),
            )
            s = review_svc.status()

    if output == OutputFormat.JSON:
        from moneybin.privacy.payloads.transactions import ReviewStatusPayload

        # Subset the typed payload per --type filter so the JSON shape
        # matches the documented per-type contract.
        if type_ == "all":
            data: object = ReviewStatusPayload(
                matches_pending=s.matches_pending,
                categorize_pending=s.categorize_pending,
                account_links_pending=s.account_links_pending,
                merchant_links_pending=s.merchant_links_pending,
                security_links_pending=s.security_links_pending,
                total=s.total,
            )
        elif type_ == "matches":
            data = {"matches_pending": s.matches_pending}
        elif type_ == "categorize":
            data = {"categorize_pending": s.categorize_pending}
        elif type_ == "account-links":
            data = {"account_links_pending": s.account_links_pending}
        elif type_ == "merchant-links":
            data = {"merchant_links_pending": s.merchant_links_pending}
        else:  # type_ == "security-links"
            data = {"security_links_pending": s.security_links_pending}
        render_or_json(
            build_envelope(data=data, sensitivity="low"),
            output,
            cli_actor="review",
        )
        return

    labels = {
        "matches": ("Matches pending", s.matches_pending),
        "categorize": ("Uncategorized transactions", s.categorize_pending),
        "account-links": ("Account-link decisions pending", s.account_links_pending),
        "merchant-links": ("Merchant-link decisions pending", s.merchant_links_pending),
        "security-links": ("Security-link decisions pending", s.security_links_pending),
    }
    selected = labels.items() if type_ == "all" else ((type_, labels[type_]),)
    pairs = [(label, str(count)) for _, (label, count) in selected]
    if type_ == "all":
        pairs.append(("Total", str(s.total)))
    if type_ == "matches" and s.matches_pending == 0:
        action = "Run 'moneybin transactions matches run' to find new matches."
    elif type_ == "all" and s.total == 0:
        action = "No review items are pending. Run 'moneybin review --status' after your next import."
    else:
        action = "Use 'moneybin review --status' to refresh these counts."
    emit_human_result(
        compose_human_result(
            [build_summary(pairs, title="Review queue")], disclosures=[action]
        ),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )
