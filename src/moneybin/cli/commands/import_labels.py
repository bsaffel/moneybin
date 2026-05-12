"""Import labels subgroup: add, remove, list.

Thin wrappers over ``ImportService.{add_labels,remove_labels,list_labels,
list_distinct_labels}``. Labels are slug-flavored markers attached to a single
``app.imports`` row.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.database import get_database

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage labels on import_log rows",
    no_args_is_help=True,
)


@app.command("add")
def import_labels_add(
    import_id: str = typer.Argument(..., help="Import ID"),
    labels: list[str] = typer.Argument(..., help="One or more labels"),
    output: OutputFormat = output_option,
) -> None:
    """Add one or more labels to an import."""
    from moneybin.services.import_service import ImportService

    try:
        with handle_cli_errors():
            with get_database() as db:
                updated = ImportService(db).add_labels(import_id, labels, actor="cli")
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        emit_json("import_labels", {"import_id": import_id, "labels": updated})
        return
    logger.info(f"✅ Labels on {import_id}: {', '.join(updated) if updated else '-'}")


@app.command("remove")
def import_labels_remove(
    import_id: str = typer.Argument(..., help="Import ID"),
    labels: list[str] = typer.Argument(..., help="One or more labels"),
    output: OutputFormat = output_option,
) -> None:
    """Remove one or more labels from an import."""
    from moneybin.services.import_service import ImportService

    try:
        with handle_cli_errors():
            with get_database() as db:
                updated = ImportService(db).remove_labels(
                    import_id, labels, actor="cli"
                )
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        emit_json("import_labels", {"import_id": import_id, "labels": updated})
        return
    logger.info(f"✅ Labels on {import_id}: {', '.join(updated) if updated else '-'}")


@app.command("list")
def import_labels_list(
    import_id: str | None = typer.Option(
        None, "--import-id", help="Filter to one import (omit for distinct counts)"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List labels for one import, or all distinct labels with usage counts."""
    from moneybin.services.import_service import ImportService

    with handle_cli_errors():
        with get_database() as db:
            svc = ImportService(db)
            if import_id is not None:
                labels = svc.list_labels(import_id)
                if output == OutputFormat.JSON:
                    emit_json(
                        "import_labels", {"import_id": import_id, "labels": labels}
                    )
                    return
                if not labels:
                    if not quiet:
                        logger.info(f"No labels on {import_id}")
                    return
                for label in labels:
                    typer.echo(label)
                return

            rows = svc.list_distinct_labels()
            if output == OutputFormat.JSON:
                emit_json(
                    "import_labels",
                    [{"label": label, "usage_count": n} for label, n in rows],
                )
                return
            if not rows:
                if not quiet:
                    logger.info("No labels in use")
                return
            for label, count in rows:
                typer.echo(f"  {label}\t{count}")
