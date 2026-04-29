"""CLI commands for database migration management.

Power-user commands for explicit migration control. Most users never
need these — auto-upgrade in Database.__init__() handles everything
transparently.
"""

import logging
from typing import Annotated

import typer

from moneybin.cli.utils import handle_cli_errors
from moneybin.migrations import MigrationRunner, get_current_versions

logger = logging.getLogger(__name__)

app = typer.Typer(help="Database migration management", no_args_is_help=True)


@app.command("apply")
def migrate_apply(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List pending migrations without executing"),
    ] = False,
) -> None:
    """Apply pending database migrations."""
    with handle_cli_errors() as db:
        runner = MigrationRunner(db)

        if dry_run:
            pending = runner.pending()
            if not pending:
                logger.info("No pending migrations")
                raise typer.Exit(0) from None
            logger.info(f"{len(pending)} pending migration(s):")
            for m in pending:
                logger.info(f"  {m.filename} ({m.file_type})")
            raise typer.Exit(0) from None

        result = runner.apply_all()

        # Show drift warnings
        for warning in runner.check_drift():
            logger.warning(f"⚠️  {warning.reason}")

        if result.failed:
            result.log_failure()
            raise typer.Exit(1) from None

        if result.applied_count > 0:
            logger.info(f"✅ {result.applied_count} migration(s) applied")
        else:
            logger.info("No pending migrations")


@app.command("status")
def migrate_status() -> None:
    """Show migration state — applied, pending, and drift warnings."""
    with handle_cli_errors() as db:
        runner = MigrationRunner(db)

        # Applied migrations
        applied = runner.applied_details()

        if applied:
            logger.info("Applied migrations:")
            for m in applied:
                status = "✅" if m.success else "❌"
                time_str = (
                    f" ({m.execution_ms}ms)" if m.execution_ms is not None else ""
                )
                logger.info(
                    f"  {status} V{m.version:03d} {m.filename}{time_str} — {m.applied_at}"
                )
        else:
            logger.info("No applied migrations")

        # Pending
        pending = runner.pending()
        if pending:
            logger.info(f"\nPending migrations ({len(pending)}):")
            for m in pending:
                logger.info(f"  ⚙️  {m.filename}")
        else:
            logger.info("\nNo pending migrations")

        # Drift warnings
        drift = runner.check_drift()
        if drift:
            logger.info("\nDrift warnings:")
            for w in drift:
                logger.warning(f"  ⚠️  {w.reason}")

        # Version state
        versions = get_current_versions(db)
        if versions:
            logger.info("\nComponent versions:")
            for component, version in sorted(versions.items()):
                logger.info(f"  {component}: {version}")
