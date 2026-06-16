"""Top-level `moneybin review` command — domain-neutral orientation sweep.

Aggregates pending counts from all three review queues:
  - Matches (transaction dedup / transfer pairs)
  - Uncategorized transactions
  - Account-link decisions

Use `moneybin review --status` to see counts; interactive walk is
stubbed pending the v2 review-loop UX.

See also: `moneybin transactions review` (deprecated alias — removed after
one minor release; points here).
"""

from __future__ import annotations

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
)

from .transactions.review import review_impl


def review_command(
    type_: str = typer.Option("all", "--type", help="all | matches | categorize"),
    status: bool = typer.Option(
        False, "--status", help="Show queue counts only, no interactive loop"
    ),
    confirm_id: str | None = typer.Option(
        None, "--confirm", help="Non-interactive: confirm one match by ID"
    ),
    reject_id: str | None = typer.Option(
        None, "--reject", help="Non-interactive: reject one match by ID"
    ),
    confirm_all: bool = typer.Option(
        False, "--confirm-all", help="Non-interactive: confirm all items in scope"
    ),
    limit: int = typer.Option(50, "--limit", help="Cap items per session"),  # noqa: ARG001 — placeholder; interactive loop pending
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Walk all pending review queues: matches, uncategorized, and account-links.

    One sweep answers "what needs my attention?" across all review domains.
    Use --status for counts only; --type to filter to a specific queue.
    """
    review_impl(
        type_=type_,
        status=status,
        confirm_id=confirm_id,
        reject_id=reject_id,
        confirm_all=confirm_all,
        limit=limit,
        output=output,
        quiet=quiet,
    )
