"""Top-level `moneybin review` command — domain-neutral orientation sweep.

Aggregates pending counts from six review queues:
  - Matches (transaction dedup / transfer pairs)
  - Investment-match Proposals
  - Uncategorized transactions
  - Account-link decisions
  - Merchant-link decisions
  - Security-link decisions

Counts are what a bare `moneybin review` prints; `--interactive` walks a bounded
selection with explicit decisions, skip, and quit.

See also: `moneybin transactions review` (deprecated alias — removed after
one minor release; points here).
"""

from __future__ import annotations

import typer

from moneybin.cli.output import (
    OutputFormat,
    no_pager_option,
    output_option,
    quiet_option,
)

from .transactions.review import review_impl


def review_command(
    type_: str | None = typer.Option(
        None,
        "--type",
        help="all | matches | investment-matches | categorize | account-links | merchant-links | security-links",
    ),
    status: bool = typer.Option(
        False, "--status", help="Show queue counts (the default)"
    ),
    interactive: bool = typer.Option(
        False,
        "--interactive",
        help="Review pending items with explicit decisions, skip, or quit",
    ),
    confirm_id: list[str] | None = typer.Option(
        None, "--confirm", help="Confirm exactly one item by ID; repeats are invalid"
    ),
    reject_id: str | None = typer.Option(
        None,
        "--reject",
        help="Non-interactive: reject one match or categorization decision by ID",
    ),
    confirm_all: bool = typer.Option(
        False, "--confirm-all", help="Non-interactive: confirm all items in scope"
    ),
    field_choice: list[str] | None = typer.Option(
        None,
        "--field-choice",
        help="Investment confirmation: conflict-id=choice-id (repeatable)",
    ),
    limit: int = typer.Option(50, "--limit", help="Cap items per session"),
    no_pager: bool = no_pager_option,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pending counts across matches, investment matches, categorization, and identity links.

    One sweep answers "what needs my attention?" across all review domains.
    Use --type to filter to a specific queue, then --confirm/--reject/
    --confirm-all to decide matches, or --type categorize --reject <decision-id>
    to reject a categorization attempt without prompting. Use --type investment-matches
    --confirm or --reject to decide one investment Proposal; --field-choice resolves material conflicts.
    """
    review_impl(
        type_=type_,
        status=status,
        interactive=interactive,
        confirm_id=confirm_id[0] if confirm_id else None,
        confirm_count=len(confirm_id or []),
        field_choices=field_choice,
        reject_id=reject_id,
        confirm_all=confirm_all,
        limit=limit,
        no_pager=no_pager,
        output=output,
        quiet=quiet,
    )
