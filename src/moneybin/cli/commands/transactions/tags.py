"""Transaction tags subgroup: add, remove, list, rename.

Thin wrappers over ``TransactionService`` tag methods. Tags are slug-flavored
labels (per ``_validators.validate_slug``).
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.database import get_database

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Tags applied to transactions (slug-flavored labels)",
    no_args_is_help=True,
)


@app.command("add")
def transactions_tags_add(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    tags: list[str] = typer.Argument(..., help="One or more tags"),
    output: OutputFormat = output_option,
) -> None:
    """Apply one or more tags to a transaction."""
    from moneybin.services.transaction_service import TransactionService

    try:
        with handle_cli_errors():
            with get_database() as db:
                added = TransactionService(db).add_tags(
                    transaction_id, tags, actor="cli"
                )
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        emit_json("tags_added", {"transaction_id": transaction_id, "added": added})
        return
    if added:
        logger.info(f"✅ Added tags to {transaction_id}: {', '.join(added)}")
    else:
        logger.info(f"No new tags applied (all already present) on {transaction_id}")


@app.command("remove")
def transactions_tags_remove(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    tags: list[str] = typer.Argument(..., help="One or more tags to remove"),
    output: OutputFormat = output_option,
) -> None:
    """Remove one or more tags from a transaction."""
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors():
        with get_database() as db:
            removed = TransactionService(db).remove_tags(
                transaction_id, tags, actor="cli"
            )

    if output == OutputFormat.JSON:
        emit_json(
            "tags_removed", {"transaction_id": transaction_id, "removed": removed}
        )
        return
    if removed:
        logger.info(f"✅ Removed tags from {transaction_id}: {', '.join(removed)}")
    else:
        logger.info(f"No tags removed (none matched) on {transaction_id}")


@app.command("list")
def transactions_tags_list(
    transaction_id: str | None = typer.Argument(
        None,
        help="Transaction ID (omit to list all distinct tags with usage counts)",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List tags on a transaction, or all distinct tags with usage counts."""
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors():
        with get_database() as db:
            svc = TransactionService(db)
            if transaction_id is not None:
                tags = svc.list_tags(transaction_id)
                if output == OutputFormat.JSON:
                    emit_json("tags", {"transaction_id": transaction_id, "tags": tags})
                    return
                if not tags:
                    if not quiet:
                        logger.info(f"No tags on {transaction_id}")
                    return
                for t in tags:
                    typer.echo(t)
                return

            rows = svc.list_distinct_tags()

    if output == OutputFormat.JSON:
        emit_json("tags", [{"tag": t, "usage_count": n} for t, n in rows])
        return
    if not rows:
        if not quiet:
            logger.info("No tags in use")
        return
    for tag, count in rows:
        typer.echo(f"  {tag}\t{count}")


@app.command("rename")
def transactions_tags_rename(
    old: str = typer.Argument(..., help="Existing tag"),
    new: str = typer.Argument(..., help="Replacement tag"),
    output: OutputFormat = output_option,
) -> None:
    """Rename a tag globally (all transactions). Emits a parent audit event."""
    from moneybin.services.transaction_service import TransactionService

    try:
        with handle_cli_errors():
            with get_database() as db:
                result = TransactionService(db).rename_tag(old, new, actor="cli")
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        emit_json(
            "tag_rename",
            {
                "old": old,
                "new": new,
                "row_count": result.row_count,
                "parent_audit_id": result.parent_audit_id,
            },
        )
        return
    logger.info(
        f"✅ Renamed tag {old!r} -> {new!r}: {result.row_count} rows updated "
        f"(parent_audit_id={result.parent_audit_id})"
    )
    logger.info(
        f"💡 Use 'moneybin system audit show {result.parent_audit_id}' to inspect"
    )
