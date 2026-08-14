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
    output_option,
    quiet_option,
    render_or_json,
)
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
    type_: str,
    status: bool,
    interactive: bool,
    confirm_id: str | None,
    reject_id: str | None,
    confirm_all: bool,
    limit: int,
    output: OutputFormat,
    quiet: bool,  # noqa: ARG001 — the status path emits data only; nothing to suppress
) -> None:
    """Shared impl for `moneybin review` and its deprecated `transactions review` alias.

    Extracted so both the top-level leaf and the deprecated alias can call it
    without duplicating logic.
    """
    if type_ not in _VALID_TYPES:
        raise typer.BadParameter(
            f"--type must be one of {sorted(_VALID_TYPES)}, got {type_!r}"
        )

    # Three mutually exclusive modes. Silently letting one win would either drop
    # a requested mutation or perform an unrequested one, so say so instead —
    # the same call the two guards in `_review_matches_noninteractive` make.
    decides = bool(confirm_id or reject_id or confirm_all)
    if sum((status, interactive, decides)) > 1:
        logger.error(
            "❌ --status, --interactive, and --confirm/--reject/--confirm-all "
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
                "❌ --confirm/--reject/--confirm-all require --type matches "
                "(categorize non-interactive review is not yet supported)"
            )
            raise typer.Exit(2)
        _review_matches_noninteractive(
            confirm_id=confirm_id, reject_id=reject_id, confirm_all=confirm_all
        )
        return

    if interactive:
        _not_implemented("moneybin-cli.md (review collapse — interactive loop pending)")
        return

    # Counts are the default: they are what `--help` describes, and the only
    # thing the command can do for every queue today.
    _print_status(type_, output)


def transactions_review(
    type_: str = typer.Option(
        "all",
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
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pending counts across all review queues.

    DEPRECATED: use `moneybin review` instead. Removed after one minor release.
    """
    typer.echo(
        "⚠️  `moneybin transactions review` is deprecated. "
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
        output=output,
        quiet=quiet,
    )


def _review_matches_noninteractive(
    *, confirm_id: str | None, reject_id: str | None, confirm_all: bool
) -> None:
    from moneybin.cli.utils import (
        RETIRED_SIDES_COLLAPSED,
        handle_cli_errors,
        warn_transfers_retired,
    )
    from moneybin.services.matching_service import MatchingService

    # --confirm-all bulk-accepts the whole queue; pairing it with a targeted
    # --confirm/--reject is ambiguous (the targeted id would be silently
    # dropped), so reject the combination as a usage error rather than run a
    # partial action.
    if confirm_all and (confirm_id or reject_id):
        logger.error("❌ --confirm-all cannot be combined with --confirm or --reject")
        raise typer.Exit(2)

    # Same id to both flags would accept then immediately fail the reject (the
    # match is no longer pending), leaving the accept silently committed behind
    # an error exit. Reject the contradiction up front, like the guard above.
    if confirm_id is not None and confirm_id == reject_id:
        logger.error("❌ --confirm and --reject cannot target the same match_id")
        raise typer.Exit(2)

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            svc = MatchingService(db)
            if confirm_all:
                bulk = svc.accept_all_pending(actor="cli")
                logger.info(f"✅ Accepted {bulk.accepted} pending match(es)")
                if bulk.reversed_by_reconciliation:
                    # Named separately from the retirement warning below: that
                    # one counts every transfer this call reversed, most of
                    # which the user accepted in some earlier session. This
                    # counts the rows they just asked for and did not get.
                    logger.warning(
                        f"⚠️  {bulk.reversed_by_reconciliation} of them did not "
                        "stand: an accepted transfer already claims the merged "
                        "pair, and the earlier decision stands"
                    )
                warn_transfers_retired(
                    bulk.transfers_retired, cause=RETIRED_SIDES_COLLAPSED
                )
                return
            # Independent ifs (not elif): `--confirm X --reject Y` targets two
            # different matches in one invocation.
            if confirm_id:
                outcome = svc.set_status(confirm_id, status="accepted", actor="cli")
                if outcome.match_status == "accepted":
                    logger.info(f"✅ Accepted match {confirm_id[:8]}...")
                else:
                    # Same refusal `matches set` can hit: the reconciliation this
                    # accept triggers walks every accepted transfer, this row
                    # included, and the earliest-decided one keeps the component.
                    logger.warning(
                        f"⚠️  Match {confirm_id[:8]}... was not accepted: it is "
                        f"{outcome.match_status} — an accepted transfer already "
                        "claims the merged pair, and the earlier decision stands"
                    )
                warn_transfers_retired(
                    outcome.transfers_retired, cause=RETIRED_SIDES_COLLAPSED
                )
            if reject_id:
                svc.set_status(reject_id, status="rejected", actor="cli")
                logger.info(f"✅ Rejected match {reject_id[:8]}...")


def _print_status(type_: str, output: OutputFormat) -> None:
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_settings
    from moneybin.services.account_links_service import AccountLinksService
    from moneybin.services.categorization import CategorizationService
    from moneybin.services.matching_service import MatchingService
    from moneybin.services.merchant_links_service import MerchantLinksService
    from moneybin.services.review_service import ReviewService
    from moneybin.services.security_links_service import SecurityLinksService

    with handle_cli_errors():
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

    if type_ == "matches":
        typer.echo(f"Matches pending: {s.matches_pending}")
    elif type_ == "categorize":
        typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
    elif type_ == "account-links":
        typer.echo(f"Account-link decisions pending: {s.account_links_pending}")
    elif type_ == "merchant-links":
        typer.echo(f"Merchant-link decisions pending: {s.merchant_links_pending}")
    elif type_ == "security-links":
        typer.echo(f"Security-link decisions pending: {s.security_links_pending}")
    else:
        typer.echo(f"Matches pending: {s.matches_pending}")
        typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
        typer.echo(f"Account-link decisions pending: {s.account_links_pending}")
        typer.echo(f"Merchant-link decisions pending: {s.merchant_links_pending}")
        typer.echo(f"Security-link decisions pending: {s.security_links_pending}")
        typer.echo(f"Total: {s.total}")
