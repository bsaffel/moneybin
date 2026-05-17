"""Unified review queue: walks pending matches + uncategorized transactions.

CLI-only collapse (per moneybin-cli.md v2). MCP keeps separate
``transactions_matches_pending`` and ``transactions_categorize_pending_list``
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
        _not_implemented(
            "moneybin-cli.md (review collapse — non-interactive flags pending)"
        )
        return

    _not_implemented("moneybin-cli.md (review collapse — interactive loop pending)")


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
        payload: dict[str, int] = {}
        if type_ in ("matches", "all"):
            payload["matches_pending"] = s.matches_pending
        if type_ in ("categorize", "all"):
            payload["categorize_pending"] = s.categorize_pending
        if type_ == "all":
            payload["total"] = s.total
        render_or_json(build_envelope(data=payload, sensitivity="low"), output)
        return

    if type_ == "matches":
        typer.echo(f"Matches pending: {s.matches_pending}")
    elif type_ == "categorize":
        typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
    else:
        typer.echo(f"Matches pending: {s.matches_pending}")
        typer.echo(f"Uncategorized transactions: {s.categorize_pending}")
        typer.echo(f"Total: {s.total}")
