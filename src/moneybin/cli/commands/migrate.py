"""CLI commands for database migration management.

Power-user commands for explicit migration control. Most users never
need these — auto-upgrade in Database.__init__() handles everything
transparently.
"""

import json
import logging
from typing import Annotated

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.migrations import (
    MigrationRunner,
    get_current_versions,
    sqlmesh_state_assessment,
    sqlmesh_state_drift,
)

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
    with handle_cli_errors():
        with get_database(read_only=False, operation_type="migration") as db:
            runner = MigrationRunner(db)

            if dry_run:
                pending = runner.pending()
                sqlmesh_drift = sqlmesh_state_drift(db)
                if not pending and sqlmesh_drift is None:
                    logger.info("No pending migrations")
                    raise typer.Exit(0) from None
                if pending:
                    logger.info(f"{len(pending)} pending migration(s):")
                    for m in pending:
                        logger.info(f"  {m.filename} ({m.file_type})")
                if sqlmesh_drift is not None:
                    logger.warning(f"⚠️  {sqlmesh_drift}")
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

            # Advance SQLMesh's internal state when the installed package has
            # moved past the schema version recorded in the database. This is the
            # explicit recovery path for the case auto-migrate on open can't
            # reach (e.g. a database whose state drifted under an earlier bug).
            drift, needs_migration = sqlmesh_state_assessment(db)
            if needs_migration:
                logger.info(
                    "⚙️  Migrating SQLMesh state to the installed package version..."
                )
                if db.repair_sqlmesh_state():
                    logger.info("✅ SQLMesh state migrated")
                else:
                    logger.error(
                        "❌ SQLMesh state migration did not advance the state — see logs"
                    )
                    raise typer.Exit(1) from None
            elif drift is not None:
                # State ahead of the installed package can't be repaired by
                # migrating — surface it rather than forcing a doomed migrate.
                logger.warning(f"⚠️  {drift}")


@app.command("status")
def migrate_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show migration state — applied, pending, and drift warnings."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            runner = MigrationRunner(db)

            applied = runner.applied_details()
            pending = runner.pending()
            drift = runner.check_drift()
            versions = get_current_versions(db)
            sqlmesh_drift = sqlmesh_state_drift(db)

            if output == OutputFormat.JSON:
                payload = {
                    "applied": [
                        {
                            "version": m.version,
                            "filename": m.filename,
                            "success": m.success,
                            "execution_ms": m.execution_ms,
                            "applied_at": m.applied_at,
                        }
                        for m in applied
                    ],
                    "pending": [
                        {"filename": m.filename, "file_type": m.file_type}
                        for m in pending
                    ],
                    "drift": [{"reason": w.reason} for w in drift],
                    "sqlmesh_state_drift": sqlmesh_drift,
                    "versions": versions,
                }
                typer.echo(json.dumps(payload, indent=2, default=str))
                return

            if applied:
                if not quiet:
                    logger.info("Applied migrations:")
                for m in applied:
                    status = "✅" if m.success else "❌"
                    time_str = (
                        f" ({m.execution_ms}ms)" if m.execution_ms is not None else ""
                    )
                    logger.info(
                        f"  {status} V{m.version:03d} {m.filename}{time_str} — {m.applied_at}"
                    )
            elif not quiet:
                logger.info("No applied migrations")

            if pending:
                if not quiet:
                    logger.info(f"\nPending migrations ({len(pending)}):")
                for m in pending:
                    logger.info(f"  ⚙️  {m.filename}")
            elif not quiet:
                logger.info("\nNo pending migrations")

            if drift:
                if not quiet:
                    logger.info("\nDrift warnings:")
                for w in drift:
                    logger.warning(f"  ⚠️  {w.reason}")

            if sqlmesh_drift is not None:
                logger.warning(f"\n⚠️  {sqlmesh_drift}")

            if versions:
                if not quiet:
                    logger.info("\nComponent versions:")
                for component, version in sorted(versions.items()):
                    logger.info(f"  {component}: {version}")
