"""Match review and management commands."""

import logging

import duckdb as duckdb_mod
import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import VALID_MATCH_TYPES, get_match_log, undo_match
from moneybin.protocol.envelope import build_envelope

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
                    logger.info(
                        "Run 'moneybin transactions review --type matches' when ready"
                    )
            else:
                logger.info("No new matches found")

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

    with handle_cli_errors(output=output) as db:
        entries = get_match_log(db, limit=limit, match_type=match_type)

        if output == OutputFormat.JSON:
            render_or_json(build_envelope(data=entries, sensitivity="low"), output)
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
                logger.info(
                    "Run 'moneybin transactions review --type matches' when ready"
                )

            if not skip_transform and result.auto_merged:
                from moneybin.services.import_service import ImportService

                ImportService(db).run_transforms()
    except duckdb_mod.CatalogException:
        logger.error(_NO_TRANSFORMS_MSG)
        raise typer.Exit(1) from None
