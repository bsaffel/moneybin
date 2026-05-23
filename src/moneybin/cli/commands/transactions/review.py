"""Unified review queue: walks pending matches + uncategorized transactions.

CLI-only collapse (per moneybin-cli.md v2). MCP keeps separate
``transactions_matches_pending`` and ``transactions_categorize_pending``
tools because their result shapes differ; the orientation tool
``transactions_review`` returns the counts.

Interactive loop UX is stubbed for v2; --status works end-to-end.
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

_VALID_TYPES = {"all", "matches", "categorize"}


def transactions_review(
    type_: str = typer.Option("all", "--type", help="all | matches | categorize"),
    status: bool = typer.Option(
        False, "--status", help="Show queue counts only, no interactive loop"
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
    limit: int = typer.Option(50, "--limit", help="Cap items per session"),  # noqa: ARG001 — placeholder; interactive loop pending
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — --status emits data only; nothing to suppress
) -> None:
    """Walk pending matches first, then uncategorized transactions."""
    if type_ not in _VALID_TYPES:
        raise typer.BadParameter(
            f"--type must be one of {sorted(_VALID_TYPES)}, got {type_!r}"
        )

    if status:
        _print_status(type_, output)
        return

    if confirm_id or reject_id or confirm_all:
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

    _not_implemented("moneybin-cli.md (review collapse — interactive loop pending)")


def _review_matches_noninteractive(
    *, confirm_id: str | None, reject_id: str | None, confirm_all: bool
) -> None:
    from moneybin.cli.utils import handle_cli_errors
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
        with get_database() as db:
            svc = MatchingService(db)
            if confirm_all:
                n = svc.accept_all_pending(actor="cli")
                logger.info(f"✅ Accepted {n} pending match(es)")
                return
            # Independent ifs (not elif): `--confirm X --reject Y` targets two
            # different matches in one invocation.
            if confirm_id:
                svc.set_status(confirm_id, status="accepted", actor="cli")
                logger.info(f"✅ Accepted match {confirm_id[:8]}...")
            if reject_id:
                svc.set_status(reject_id, status="rejected", actor="cli")
                logger.info(f"✅ Rejected match {reject_id[:8]}...")


def _print_status(type_: str, output: OutputFormat) -> None:
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_settings
    from moneybin.services.categorization import CategorizationService
    from moneybin.services.matching_service import MatchingService
    from moneybin.services.review_service import ReviewService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            review_svc = ReviewService(
                match_service=MatchingService(db, get_settings().matching),
                categorize_service=CategorizationService(db),
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
                total=s.total,
            )
        elif type_ == "matches":
            data = {"matches_pending": s.matches_pending}
        else:  # type_ == "categorize"
            data = {"categorize_pending": s.categorize_pending}
        render_or_json(
            build_envelope(data=data, sensitivity="low"),
            output,
            cli_actor="transactions_review",
        )
        return

    if type_ == "matches":
        typer.echo(f"Matches pending: {s.matches_pending}")
    elif type_ == "categorize":
        typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
    else:
        typer.echo(f"Matches pending: {s.matches_pending}")
        typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
        typer.echo(f"Total: {s.total}")
