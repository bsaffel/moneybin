"""Match review and management commands."""

import logging
from typing import Any

import duckdb as duckdb_mod
import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import render_rows
from moneybin.cli.utils import (
    confidence_cell,
    handle_cli_errors,
    warn_match_decisions_committed,
    warn_transfers_retired,
)
from moneybin.database import get_database
from moneybin.errors import exception_origin
from moneybin.matching.engine import MatchRunError
from moneybin.matching.persistence import VALID_MATCH_TYPES
from moneybin.matching.reconciliation import RETIRED_SIDES_COLLAPSED
from moneybin.services.matching_service import PENDING_MATCHES_HINT, MatchingService
from moneybin.tables import INT_TRANSACTIONS_UNIONED

app = typer.Typer(
    help="Review and manage transaction matches (dedup, transfers)",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


def _score(row: dict[str, Any]) -> float | None:
    """This match's confidence, keeping "no score recorded" distinct from zero.

    The two hand-built tables this replaced coerced a missing score to ``0.00``,
    which reads as the engine having compared the pair and found nothing in
    common — the opposite of what an exact-id match, which records no score at
    all, actually means. Its sibling queues already printed a dash here.
    """
    value = row.get("confidence_score")
    return None if value is None else float(value)


_NO_TRANSFORMS_MSG = (
    "❌ No transaction data available — run 'moneybin transform apply' first"
)


@app.command("pending")
def matches_pending(
    match_type: str | None = typer.Option(
        None, "--type", help="Filter by match type: dedup or transfer"
    ),
    limit: int = typer.Option(50, "--limit", "-n", help="Max records to show"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List pending matches, grouped by component (copies of the same transaction cluster together)."""
    if match_type and match_type not in VALID_MATCH_TYPES:
        logger.error("❌ --type must be 'dedup' or 'transfer'")
        raise typer.Exit(2)

    with handle_cli_errors(cli_actor="matches_pending"):
        with get_database(read_only=True) as db:
            service = MatchingService(db)
            rows = service.get_pending(match_type=match_type, limit=limit)

            if output == OutputFormat.JSON:
                from moneybin.adapters.matching_adapters import (  # noqa: PLC0415 — defer import
                    matches_pending_envelope,
                )

                # Both counts span the whole queue, not this page: an agent
                # that sees `has_more` needs to know how much is behind it,
                # and a page-local group count would understate the review
                # still to do. Computed inside the JSON branch because the
                # text branch below renders neither, and
                # `count_pending_dedup_groups` reloads the entire pending
                # queue and rebuilds the component graph `get_pending` just
                # walked.
                render_or_json(
                    matches_pending_envelope(
                        rows,
                        total_count=service.count_pending(match_type=match_type),
                        n_dedup_groups=service.count_pending_dedup_groups(
                            match_type=match_type
                        ),
                        actions=[
                            "Use 'moneybin transactions matches set <id> --status "
                            "accepted|rejected' to decide one match",
                            "Rows sharing a component_key are copies of one "
                            "transaction — decide the whole cluster together with "
                            "'moneybin transactions matches set'",
                        ],
                    ),
                    output,
                    cli_actor="matches_pending",
                )
                return

        if not rows:
            if not quiet:
                logger.info("No pending matches")
            return

        # Group by component_key so N-way clusters surface as one block.
        # Insertion order in dict preserves first-seen component ordering.
        # get_pending always sets component_key on every row.
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            groups.setdefault(str(row["component_key"]), []).append(row)

        for ck, group_rows in groups.items():
            typer.echo(f"\n── component {ck} ({len(group_rows)} edge(s)) ──")
            render_rows(
                ["match id", "type", "tier", "score", "type a", "type b"],
                [
                    (
                        str(row["match_id"])[:12],
                        str(row.get("match_type", "dedup")),
                        str(row.get("match_tier") or "-"),
                        confidence_cell(_score(row)),
                        str(row["source_type_a"]),
                        str(row["source_type_b"]),
                    )
                    for row in group_rows
                ],
                numeric=("score",),
            )


@app.command("run")
def matches_run(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip transforms after matching"
    ),
    auto_accept_transfers: bool = typer.Option(
        False,
        "--auto-accept-transfers",
        help="Auto-accept transfer matches (skip interactive review)",
    ),
) -> None:
    """Run matcher against existing transactions."""
    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                try:
                    result = MatchingService(db).run(
                        auto_accept_transfers=auto_accept_transfers, actor="cli"
                    )
                except MatchRunError as exc:
                    # The decisions and the reversals both committed before
                    # whatever failed; this exception is the last thing that
                    # knows either count. Warn, then re-raise so the failure
                    # keeps its own presentation — `classify_user_error`
                    # answers this type with a message that withholds the
                    # cause, which is why the frames are logged here.
                    warn_match_decisions_committed(exc.partial)
                    warn_transfers_retired(
                        exc.partial.transfers_retired, cause=RETIRED_SIDES_COLLAPSED
                    )
                    logger.error(
                        f"Matching failed during 'matches run' at "
                        f"{exception_origin(exc.__cause__ or exc)}"
                    )
                    raise
                if result.has_matches:
                    logger.info(f"Matching: {result.summary()}")
                    if result.has_pending:
                        logger.info(PENDING_MATCHES_HINT)
                else:
                    logger.info("No new matches found")
                # Outside the branch above: the reconciliation runs inside
                # `run()` whatever the tiers find, so "No new matches found" is
                # the very case where a silent retirement reads as "nothing
                # changed".
                warn_transfers_retired(
                    result.transfers_retired, cause=RETIRED_SIDES_COLLAPSED
                )

                if not skip_transform and result.auto_merged:
                    from moneybin.services.import_service import ImportService

                    ImportService(db).run_transforms()
    except duckdb_mod.CatalogException:
        logger.error(_NO_TRANSFORMS_MSG)
        raise typer.Exit(1) from None


@app.command("history")
def matches_history(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
    match_type: str | None = typer.Option(
        None, "--type", help="Filter by match type: dedup or transfer"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show recent match decisions."""
    if match_type and match_type not in VALID_MATCH_TYPES:
        logger.error("❌ --type must be 'dedup' or 'transfer'")
        raise typer.Exit(2)

    with handle_cli_errors(cli_actor="matches_history"):
        with get_database(read_only=True) as db:
            entries = MatchingService(db).get_log(limit=limit, match_type=match_type)

            if output == OutputFormat.JSON:
                from moneybin.adapters.matching_adapters import (  # noqa: PLC0415 — defer import
                    matches_history_envelope,
                )

                render_or_json(
                    matches_history_envelope(
                        entries,
                        actions=[
                            "Use 'moneybin transactions matches pending' for "
                            "the active queue",
                            "Use 'moneybin transactions matches undo <id>' to "
                            "reverse an accepted decision",
                        ],
                    ),
                    output,
                    cli_actor="matches_history",
                )
                return

            if not entries:
                if not quiet:
                    logger.info("No match decisions found")
                return

            render_rows(
                [
                    "match id",
                    "type",
                    "status",
                    "tier",
                    "score",
                    "decided by",
                    "type a",
                    "type b",
                ],
                [
                    (
                        entry["match_id"][:12],
                        entry.get("match_type", "dedup"),
                        entry["match_status"],
                        entry.get("match_tier") or "-",
                        confidence_cell(_score(entry)),
                        entry["decided_by"],
                        entry["source_type_a"],
                        entry["source_type_b"],
                    )
                    for entry in entries
                ],
                numeric=("score",),
            )


@app.command("undo")
def matches_undo(
    match_id: str = typer.Argument(..., help="Match ID to reverse"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Reverse a match decision."""
    if not yes:
        confirmed = typer.confirm(f"Undo match {match_id[:8]}...?")
        if not confirmed:
            logger.info("Undo cancelled")
            raise typer.Exit(0)

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                MatchingService(db).undo(match_id, reversed_by="user", actor="cli")
                logger.info(f"Reversed match {match_id[:8]}...")
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("set")
def matches_set(
    match_id: str = typer.Argument(..., help="Match ID to accept or reject"),
    status: str = typer.Option(..., "--status", help="accepted or rejected"),
) -> None:
    """Accept or reject one pending match by id."""
    if status not in {"accepted", "rejected"}:
        logger.error("❌ --status must be 'accepted' or 'rejected'")
        raise typer.Exit(2)
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            outcome = MatchingService(db).set_status(
                match_id, status=status, actor="cli"
            )
    if outcome.match_status == status:
        logger.info(f"✅ Set match {match_id[:8]}... to {status}")
    else:
        # The reconciliation this accept triggered reversed this very row. A ✅
        # here would report the opposite of what committed, and the count-shaped
        # warning below would not contradict it.
        logger.warning(
            f"⚠️  Match {match_id[:8]}... was not {status}: it is "
            f"{outcome.match_status} — an accepted transfer already claims the "
            "merged pair, and the earlier decision stands"
        )
    warn_transfers_retired(
        outcome.transfers_retired,
        cause=RETIRED_SIDES_COLLAPSED,
        rematch_follow_up=True,
    )
    if outcome.match_status != status:
        # The status the caller asked for is not the one that committed, which
        # cli.md calls a failed operation. Warning alone leaves an agent gating
        # on exit status recording an accept the reconciliation refused.
        raise typer.Exit(1)


@app.command("backfill")
def matches_backfill(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip transforms after matching"
    ),
    auto_accept_transfers: bool = typer.Option(
        False,
        "--auto-accept-transfers",
        help="Auto-accept transfer matches (skip interactive review)",
    ),
) -> None:
    """One-time scan of all existing transactions for latent duplicates."""
    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                count = db.execute(
                    f"SELECT COUNT(*) FROM {INT_TRANSACTIONS_UNIONED.full_name}"  # noqa: S608 — TableRef constant
                ).fetchone()
                total = count[0] if count else 0
                logger.info(
                    f"Scanning {total:,} existing transactions for duplicates and transfers..."
                )

                try:
                    result = MatchingService(db).run(
                        auto_accept_transfers=auto_accept_transfers, actor="cli"
                    )
                except MatchRunError as exc:
                    # Same guard as `run` above, repeated rather than shared:
                    # the two commands own their own summaries, and a helper
                    # here would hide which one lost the disclosure.
                    warn_match_decisions_committed(exc.partial)
                    warn_transfers_retired(
                        exc.partial.transfers_retired, cause=RETIRED_SIDES_COLLAPSED
                    )
                    logger.error(
                        f"Matching failed during 'matches backfill' at "
                        f"{exception_origin(exc.__cause__ or exc)}"
                    )
                    raise

                logger.info(f"Backfill complete: {result.summary()}")
                if result.has_pending:
                    logger.info(PENDING_MATCHES_HINT)
                warn_transfers_retired(
                    result.transfers_retired, cause=RETIRED_SIDES_COLLAPSED
                )

                if not skip_transform and result.auto_merged:
                    from moneybin.services.import_service import ImportService

                    ImportService(db).run_transforms()
    except duckdb_mod.CatalogException:
        logger.error(_NO_TRANSFORMS_MSG)
        raise typer.Exit(1) from None
