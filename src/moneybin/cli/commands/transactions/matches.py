"""Match review and management commands."""

import logging
from typing import Any

import duckdb as duckdb_mod
import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.database import get_database
from moneybin.matching.persistence import VALID_MATCH_TYPES
from moneybin.services.matching_service import MatchingService
from moneybin.tables import INT_TRANSACTIONS_UNIONED

app = typer.Typer(
    help="Review and manage transaction matches (dedup, transfers)",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)

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

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            rows = MatchingService(db).get_pending(match_type=match_type, limit=limit)

        if output == OutputFormat.JSON:
            emit_json("matches", rows)
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
            typer.echo(
                f"  {'Match ID':<14} {'Type':<9} {'Tier':<5} {'Score':>6} "
                f"{'Type A':<8} {'Type B':<8}"
            )
            for row in group_rows:
                score = float(row.get("confidence_score") or 0)
                typer.echo(
                    f"  {str(row['match_id'])[:12]:<14} "
                    f"{str(row.get('match_type', 'dedup')):<9} "
                    f"{str(row.get('match_tier') or '-'):<5} "
                    f"{score:>6.2f} "
                    f"{str(row['source_type_a']):<8} "
                    f"{str(row['source_type_b']):<8}"
                )
        typer.echo()


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
    try:
        with handle_cli_errors():
            with get_database() as db:
                result = MatchingService(db).run(
                    auto_accept_transfers=auto_accept_transfers
                )
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

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            entries = MatchingService(db).get_log(limit=limit, match_type=match_type)

            if output == OutputFormat.JSON:
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
            with get_database() as db:
                MatchingService(db).undo(match_id, reversed_by="user")
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
        with get_database() as db:
            MatchingService(db).set_status(match_id, status=status)
    logger.info(f"✅ Set match {match_id[:8]}... to {status}")


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
    try:
        with handle_cli_errors():
            with get_database() as db:
                count = db.execute(
                    f"SELECT COUNT(*) FROM {INT_TRANSACTIONS_UNIONED.full_name}"  # noqa: S608 — TableRef constant
                ).fetchone()
                total = count[0] if count else 0
                logger.info(
                    f"Scanning {total:,} existing transactions for duplicates and transfers..."
                )

                result = MatchingService(db).run(
                    auto_accept_transfers=auto_accept_transfers
                )

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
