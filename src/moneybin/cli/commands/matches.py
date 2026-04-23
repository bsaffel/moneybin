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
def matches_review() -> None:
    """Interactive: accept/reject/skip/quit match proposals."""
    from moneybin.matching.persistence import get_pending_matches, update_match_status

    try:
        db = get_database()
        pending = get_pending_matches(db)

        if not pending:
            logger.info("No pending matches to review")
            return

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


@app.command("log")
def matches_log_cmd(
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
