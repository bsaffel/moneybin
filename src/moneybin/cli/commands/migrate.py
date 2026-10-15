"""CLI commands for database migration management."""

import json
from typing import Annotated

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
)
from moneybin.cli.progress import operation_progress
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import classify_user_error
from moneybin.migrations import (
    MigrationResult,
    MigrationRunner,
    get_current_versions,
    sqlmesh_state_assessment,
    sqlmesh_state_drift,
)
from moneybin.progress import ProgressEvent

app = typer.Typer(help="Database migration management", no_args_is_help=True)


def _emit_migration_text(
    title: str,
    pairs: list[tuple[str, object]],
    *,
    finite_read: bool,
    no_pager: bool = False,
) -> None:
    """Render one migration answer or receipt through the shared boundary."""
    policy = get_terminal_policy(no_pager=no_pager)
    emit_human_result(
        compose_human_result([
            build_summary(
                [(key, str(value)) for key, value in pairs],
                title=title,
                terminal=policy,
            )
        ]),
        policy=policy,
        finite_read=finite_read,
        no_pager=no_pager,
        receipt=not finite_read,
    )


@app.command("apply")
def migrate_apply(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List pending migrations without executing"),
    ] = False,
) -> None:
    """Apply pending database migrations."""
    terminal = get_terminal_policy()
    result: MigrationResult | None = None
    drift_warnings = []
    drift: str | None = None
    needs_migration = False
    repaired = True
    post_apply_error = None
    is_preview = dry_run
    try:
        with (
            handle_cli_errors(),
            get_database(read_only=False, operation_type="migration") as db,
        ):
            runner = MigrationRunner(db)
            if dry_run:
                pending = runner.pending()
                sqlmesh_drift = sqlmesh_state_drift(db)
                pairs: list[tuple[str, object]] = []
                if pending:
                    pairs.extend(
                        ("Migration", f"{migration.filename} ({migration.file_type})")
                        for migration in pending
                    )
                else:
                    pairs.append(("Result", "No pending migrations"))
                if sqlmesh_drift is not None:
                    pairs.append(("Transform state", sqlmesh_drift))
                _emit_migration_text(
                    "Planned migrations" if pending else "No pending migrations",
                    pairs,
                    finite_read=False,
                )
                return

            with operation_progress(terminal) as report:
                report(ProgressEvent("Applying migrations"))
                result = runner.apply_all()
                try:
                    drift_warnings = runner.check_drift()
                    if not result.failed:
                        report(ProgressEvent("Assessing transform state"))
                        drift, needs_migration = sqlmesh_state_assessment(db)
                        repaired = not needs_migration or db.repair_sqlmesh_state()
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    post_apply_error = classify_user_error(exc)
                    if post_apply_error is None:
                        raise
    except KeyboardInterrupt:
        if is_preview:
            _emit_migration_text(
                "Migration preview cancelled",
                [
                    ("Applied migrations", "No migrations were applied"),
                    ("Next step", "moneybin db migrate apply --dry-run"),
                ],
                finite_read=False,
            )
            raise typer.Exit(130) from None
        if result is None:
            pairs = [("Saved state", "Saved scope is unknown")]
        else:
            pairs = [
                ("Applied migrations", f"{result.applied_count} migration(s) applied"),
                (
                    "Remaining state",
                    "Unknown; migration and transform-state freshness is unknown",
                ),
            ]
        pairs.append(("Next step", "moneybin db migrate status"))
        _emit_migration_text("Migration apply cancelled", pairs, finite_read=False)
        raise typer.Exit(130) from None

    if post_apply_error is not None and not result.failed:
        pairs = [
            ("Applied migrations", f"{result.applied_count} migration(s) applied"),
            ("Failure", post_apply_error.message),
            (
                "Remaining state",
                "Unknown; migration and transform-state freshness is unknown",
            ),
        ]
        if post_apply_error.hint is not None:
            pairs.append(("Recovery", post_apply_error.hint))
        _emit_migration_text(
            "Migration apply partially completed", pairs, finite_read=False
        )
        raise typer.Exit(1)

    if result.failed:
        pairs = [
            ("Applied migrations", f"{result.applied_count} migration(s) applied"),
            ("Failed migration", result.failed_migration or "Unknown"),
            ("Failure", result.error_message or "Unknown failure"),
        ]
        if post_apply_error is not None:
            pairs.append(("Diagnostic failure", post_apply_error.message))
            if post_apply_error.hint is not None:
                pairs.append(("Recovery", post_apply_error.hint))
        pairs.append((
            "Remaining state",
            "Unknown; inspect migration status before retrying",
        ))
        _emit_migration_text(
            "Migration apply failed",
            pairs,
            finite_read=False,
        )
        raise typer.Exit(1)

    warning_pairs = [("Migration drift", warning.reason) for warning in drift_warnings]
    if needs_migration and not repaired:
        _emit_migration_text(
            "Migration apply partially completed",
            [
                ("Applied migrations", f"{result.applied_count} migration(s) applied"),
                ("Transform state repair", "Failed"),
                ("Remaining state", "Transform state remains stale"),
                ("Next step", "moneybin db migrate status"),
                *warning_pairs,
            ],
            finite_read=False,
        )
        raise typer.Exit(1)

    pairs = [
        (
            "Applied migrations",
            f"{result.applied_count} migration(s) applied"
            if result.applied_count
            else "No pending migrations",
        )
    ]
    if needs_migration:
        pairs.append(("Transform state", "Migrated to the installed package version"))
    elif drift is not None:
        pairs.append(("Transform state", drift))
    pairs.extend(warning_pairs)
    _emit_migration_text("Migration apply complete", pairs, finite_read=False)


@app.command("status")
def migrate_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
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
                            "version": migration.version,
                            "filename": migration.filename,
                            "success": migration.success,
                            "execution_ms": migration.execution_ms,
                            "applied_at": migration.applied_at,
                        }
                        for migration in applied
                    ],
                    "pending": [
                        {
                            "filename": migration.filename,
                            "file_type": migration.file_type,
                        }
                        for migration in pending
                    ],
                    "drift": [{"reason": warning.reason} for warning in drift],
                    "sqlmesh_state_drift": sqlmesh_drift,
                    "versions": versions,
                }
                typer.echo(json.dumps(payload, indent=2, default=str))
                return

            pairs: list[tuple[str, object]] = [
                ("Applied migrations", len(applied)),
                ("Pending migrations", len(pending)),
            ]
            pairs.extend(
                ("Pending", f"{migration.filename} ({migration.file_type})")
                for migration in pending
            )
            pairs.extend(("Migration drift", warning.reason) for warning in drift)
            if sqlmesh_drift is not None:
                pairs.append(("Transform state", sqlmesh_drift))
            pairs.extend(
                (f"Version {component}", version)
                for component, version in sorted(versions.items())
            )
            policy = get_terminal_policy(no_pager=no_pager)
            parts: list[object] = [
                build_summary(
                    [(key, str(value)) for key, value in pairs],
                    title="Migration status",
                    terminal=policy,
                )
            ]
            if applied:
                # A table, not one `Applied:` summary pair per migration: the
                # pair form printed an unbounded `applied_at` timestamp beside
                # a long filename, wrapping mid-token at 100 columns.
                # `nowrap=` keeps the timestamp whole; a table also stops the
                # per-row label repeating "Applied" N times.
                parts.append(
                    build_rows(
                        ["id", "applied at", "name"],
                        [
                            (
                                f"V{migration.version:03d}",
                                str(migration.applied_at),
                                (
                                    f"{migration.filename} "
                                    f"({'success' if migration.success else 'failed'})"
                                    + (
                                        f", {migration.execution_ms}ms"
                                        if migration.execution_ms is not None
                                        else ""
                                    )
                                ),
                            )
                            for migration in applied
                        ],
                        nowrap=("applied at",),
                        terminal=policy,
                    )
                )
            emit_human_result(
                compose_human_result(parts),
                policy=policy,
                finite_read=True,
                no_pager=no_pager,
            )
