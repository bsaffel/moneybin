"""Match review and management commands."""

import json
import logging
from typing import Any

import duckdb as duckdb_mod
import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import VALID_MATCH_TYPES, get_match_log, undo_match

app = typer.Typer(
    help="Review and manage transaction matches (dedup, transfers)",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)

_NO_TRANSFORMS_MSG = (
    "❌ No transaction data available — run 'moneybin transform apply' first"
)


@app.command("run")
def matches_run(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip SQLMesh transforms after matching"
    ),
    auto_accept_transfers: bool = typer.Option(
        False,
        "--auto-accept-transfers",
        help="Auto-accept transfer matches (skip interactive review)",
    ),
) -> None:
    """Run matcher against existing transactions."""
    from moneybin.config import get_settings
    from moneybin.matching.priority import seed_source_priority

    try:
        with handle_cli_errors() as db:
            settings = get_settings().matching
            seed_source_priority(db, settings)
            matcher = TransactionMatcher(db, settings)
            result = matcher.run(auto_accept_transfers=auto_accept_transfers)
            if result.has_matches:
                logger.info(f"Matching: {result.summary()}")
                if result.has_pending:
                    logger.info("Run 'moneybin matches review' when ready")
            else:
                logger.info("No new matches found")

            if not skip_transform and result.auto_merged:
                from moneybin.services.import_service import run_transforms

                run_transforms()
    except duckdb_mod.CatalogException:
        logger.error(_NO_TRANSFORMS_MSG)
        raise typer.Exit(1) from None


def _display_dedup_match(match: dict[str, Any]) -> None:
    """Display a dedup match for interactive review."""
    typer.echo(
        f"  Match {match['match_id'][:8]}... "
        f"(confidence: {match['confidence_score']:.2f})"
    )
    typer.echo(
        f"    A: [{match['source_type_a']}] {match['source_transaction_id_a'][:20]}"
    )
    typer.echo(
        f"    B: [{match['source_type_b']}] {match['source_transaction_id_b'][:20]}"
    )
    if match.get("match_reason"):
        typer.echo(f"    Reason: {match['match_reason']}")


def _display_transfer_match(match: dict[str, Any]) -> None:
    """Display a transfer match for interactive review."""
    typer.echo(f"  Transfer pair (confidence: {match['confidence_score']:.2f})")
    typer.echo(
        f"    DEBIT:  [{match['source_type_a']}] "
        f"{match['source_transaction_id_a'][:16]}  "
        f"acct:{match['account_id'][:8]}"
    )
    typer.echo(
        f"    CREDIT: [{match['source_type_b']}] "
        f"{match['source_transaction_id_b'][:16]}  "
        f"acct:{(match.get('account_id_b') or '?')[:8]}"
    )
    signals = match.get("match_signals")
    if signals:
        if isinstance(signals, str):
            signals = json.loads(signals)
        parts = [f"{k}={v:.1f}" for k, v in signals.items()]
        typer.echo(f"    Signals: {'  '.join(parts)}")
    if match.get("match_reason"):
        typer.echo(f"    Reason: {match['match_reason']}")


@app.command("review")
def matches_review(
    match_type: str | None = typer.Option(
        None, "--type", help="Filter by match type: dedup or transfer"
    ),
    accept_all: bool = typer.Option(
        False, "--accept-all", help="Accept all pending matches without prompting"
    ),
    match_id: str | None = typer.Option(
        None, "--match-id", help="Specific match ID to act on (use with --decision)"
    ),
    decision: str | None = typer.Option(
        None,
        "--decision",
        help="accept or reject (use with --match-id)",
    ),
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip SQLMesh transforms after review"
    ),
) -> None:
    """Review pending match proposals. Interactive by default."""
    from moneybin.matching.persistence import get_pending_matches, update_match_status

    if decision and not match_id:
        logger.error("❌ --decision requires --match-id")
        raise typer.Exit(2)

    if match_id and decision and decision not in ("accept", "reject"):
        logger.error("❌ --decision must be 'accept' or 'reject'")
        raise typer.Exit(2)

    if match_type and match_type not in VALID_MATCH_TYPES:
        logger.error("❌ --type must be 'dedup' or 'transfer'")
        raise typer.Exit(2)

    with handle_cli_errors() as db:
        accepted_any = False

        # Non-interactive: single match decision
        if match_id and decision:
            if match_type:
                row = db.execute(
                    "SELECT match_type FROM app.match_decisions WHERE match_id = ?",
                    [match_id],
                ).fetchone()
                if row and row[0] != match_type:
                    logger.error(
                        f"❌ Match {match_id[:8]} is type '{row[0]}', "
                        f"not '{match_type}'"
                    )
                    raise typer.Exit(2)
            status = "accepted" if decision == "accept" else "rejected"
            update_match_status(db, match_id, status=status, decided_by="user")
            logger.info(f"{status.capitalize()} {match_id[:8]}")
            accepted_any = status == "accepted"

        # Non-interactive: accept all
        elif accept_all:
            pending = get_pending_matches(db, match_type=match_type)
            if not pending:
                logger.info("No pending matches to review")
                return
            for match in pending:
                update_match_status(
                    db, match["match_id"], status="accepted", decided_by="user"
                )
            logger.info(f"Accepted {len(pending)} pending match(es)")
            accepted_any = True

        # Interactive review
        else:
            pending = get_pending_matches(db, match_type=match_type)
            if not pending:
                logger.info("No pending matches to review")
                return

            logger.info(f"{len(pending)} match(es) to review\n")
            for match in pending:
                if match.get("match_type") == "transfer":
                    _display_transfer_match(match)
                else:
                    _display_dedup_match(match)

                action = typer.prompt(
                    "  [a]ccept / [r]eject / [s]kip / [q]uit", default="s"
                )
                if action.lower().startswith("a"):
                    update_match_status(
                        db,
                        match["match_id"],
                        status="accepted",
                        decided_by="user",
                    )
                    logger.info(f"Accepted {match['match_id'][:8]}")
                    accepted_any = True
                elif action.lower().startswith("r"):
                    update_match_status(
                        db,
                        match["match_id"],
                        status="rejected",
                        decided_by="user",
                    )
                    logger.info(f"Rejected {match['match_id'][:8]}")
                elif action.lower().startswith("q"):
                    break

        if accepted_any and not skip_transform:
            from moneybin.services.import_service import run_transforms

            run_transforms()


@app.command("history")
def matches_history_cmd(
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

    with handle_cli_errors() as db:
        entries = get_match_log(db, limit=limit, match_type=match_type)

        if output == "json":
            emit_json("matches", entries)
            return

        if not entries:
            if not quiet:
                logger.info("No match decisions found")
            return

        typer.echo(
            f"\n{'Match ID':<14} {'Type':<9} {'Status':<10} {'Tier':<5} {'Score':>6} "
            f"{'Decided By':<10} {'Type A':<6} {'Type B':<6}"
        )
        typer.echo("-" * 80)
        for entry in entries:
            typer.echo(
                f"{entry['match_id'][:12]:<14} "
                f"{entry.get('match_type', 'dedup'):<9} "
                f"{entry['match_status']:<10} "
                f"{(entry.get('match_tier') or '-'):<5} "
                f"{float(entry.get('confidence_score') or 0):>6.2f} "
                f"{entry['decided_by']:<10} "
                f"{entry['source_type_a']:<6} "
                f"{entry['source_type_b']:<6}"
            )
        typer.echo()


@app.command("undo")
def matches_undo_cmd(
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
        with handle_cli_errors() as db:
            undo_match(db, match_id, reversed_by="user")
            logger.info(f"Reversed match {match_id[:8]}...")
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("backfill")
def matches_backfill(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip SQLMesh transforms after matching"
    ),
    auto_accept_transfers: bool = typer.Option(
        False,
        "--auto-accept-transfers",
        help="Auto-accept transfer matches (skip interactive review)",
    ),
) -> None:
    """One-time scan of all existing transactions for latent duplicates."""
    from moneybin.config import get_settings
    from moneybin.matching.priority import seed_source_priority

    try:
        with handle_cli_errors() as db:
            settings = get_settings().matching

            count = db.execute(
                "SELECT COUNT(*) FROM prep.int_transactions__unioned"
            ).fetchone()
            total = count[0] if count else 0
            logger.info(
                f"Scanning {total:,} existing transactions for duplicates and transfers..."
            )

            seed_source_priority(db, settings)
            matcher = TransactionMatcher(db, settings)
            result = matcher.run(auto_accept_transfers=auto_accept_transfers)

            logger.info(f"Backfill complete: {result.summary()}")
            if result.has_pending:
                logger.info("Run 'moneybin matches review' when ready")

            if not skip_transform and result.auto_merged:
                from moneybin.services.import_service import run_transforms

                run_transforms()
    except duckdb_mod.CatalogException:
        logger.error(_NO_TRANSFORMS_MSG)
        raise typer.Exit(1) from None
