"""Match review and management commands."""

import logging

import typer

from moneybin.database import DatabaseKeyError, get_database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import get_match_log, undo_match

app = typer.Typer(
    help="Review and manage transaction matches (dedup, transfers)",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("run")
def matches_run(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip SQLMesh transforms after matching"
    ),
) -> None:
    """Run matcher against existing transactions."""
    from moneybin.config import get_settings
    from moneybin.matching.priority import seed_source_priority

    try:
        db = get_database()
        settings = get_settings().matching
        seed_source_priority(db, settings)
        matcher = TransactionMatcher(db, settings)
        result = matcher.run()
        if result.auto_merged or result.pending_review:
            logger.info(f"Matching: {result.summary()}")
            if result.pending_review:
                logger.info("Run 'moneybin matches review' when ready")
        else:
            logger.info("No new matches found")

        if not skip_transform and (result.auto_merged or result.pending_review):
            from moneybin.services.import_service import run_transforms

            db.close()
            run_transforms(get_settings().database.path)
    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


@app.command("review")
def matches_review(
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
) -> None:
    """Review pending match proposals. Interactive by default."""
    from moneybin.matching.persistence import get_pending_matches, update_match_status

    if decision and not match_id:
        logger.error("❌ --decision requires --match-id")
        raise typer.Exit(2)

    if match_id and decision and decision not in ("accept", "reject"):
        logger.error("❌ --decision must be 'accept' or 'reject'")
        raise typer.Exit(2)

    try:
        db = get_database()

        # Non-interactive: single match decision
        if match_id and decision:
            status = "accepted" if decision == "accept" else "rejected"
            update_match_status(db, match_id, status=status, decided_by="user")
            logger.info(f"{status.capitalize()} {match_id[:8]}")
            return

        pending = get_pending_matches(db)

        if not pending:
            logger.info("No pending matches to review")
            return

        # Non-interactive: accept all
        if accept_all:
            for match in pending:
                update_match_status(
                    db, match["match_id"], status="accepted", decided_by="user"
                )
            logger.info(f"Accepted {len(pending)} pending match(es)")
            return

        # Interactive review
        logger.info(f"{len(pending)} match(es) to review\n")
        for match in pending:
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

            action = typer.prompt(
                "  [a]ccept / [r]eject / [s]kip / [q]uit", default="s"
            )
            if action.lower().startswith("a"):
                update_match_status(
                    db, match["match_id"], status="accepted", decided_by="user"
                )
                logger.info(f"Accepted {match['match_id'][:8]}")
            elif action.lower().startswith("r"):
                update_match_status(
                    db, match["match_id"], status="rejected", decided_by="user"
                )
                logger.info(f"Rejected {match['match_id'][:8]}")
            elif action.lower().startswith("q"):
                break

    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


@app.command("history")
def matches_history_cmd(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
) -> None:
    """Show recent match decisions."""
    try:
        db = get_database()
        entries = get_match_log(db, limit=limit, match_type="dedup")

        if not entries:
            logger.info("No match decisions found")
            return

        typer.echo(
            f"\n{'Match ID':<14} {'Status':<10} {'Tier':<5} {'Score':>6} "
            f"{'Decided By':<10} {'Type A':<6} {'Type B':<6}"
        )
        typer.echo("-" * 70)
        for entry in entries:
            typer.echo(
                f"{entry['match_id'][:12]:<14} "
                f"{entry['match_status']:<10} "
                f"{(entry.get('match_tier') or '-'):<5} "
                f"{float(entry.get('confidence_score') or 0):>6.2f} "
                f"{entry['decided_by']:<10} "
                f"{entry['source_type_a']:<6} "
                f"{entry['source_type_b']:<6}"
            )
        typer.echo()

    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


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
        db = get_database()
        undo_match(db, match_id, reversed_by="user")
        logger.info(f"Reversed match {match_id[:8]}...")
    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


@app.command("backfill")
def matches_backfill(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip SQLMesh transforms after matching"
    ),
) -> None:
    """One-time scan of all existing transactions for latent duplicates."""
    from moneybin.config import get_settings
    from moneybin.matching.priority import seed_source_priority

    try:
        db = get_database()
        settings = get_settings().matching

        count = db.execute(
            "SELECT COUNT(*) FROM prep.int_transactions__unioned"
        ).fetchone()
        total = count[0] if count else 0
        logger.info(f"Scanning {total:,} existing transactions for duplicates...")

        seed_source_priority(db, settings)
        matcher = TransactionMatcher(db, settings)
        result = matcher.run()

        logger.info(f"Backfill complete: {result.summary()}")
        if result.pending_review:
            logger.info("Run 'moneybin matches review' when ready")

        if not skip_transform and result.auto_merged:
            from moneybin.services.import_service import run_transforms

            db.close()
            run_transforms(get_settings().database.path)

    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e
