"""Transaction tags subgroup: add, remove, list, rename.

Thin wrappers over ``TransactionService`` tag methods. Tags are slug-flavored
labels (per ``_validators.validate_slug``).
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import abort_cli_error, get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.transactions import TagRenamePayload, TagsPayload
from moneybin.protocol.envelope import build_envelope

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
            with get_database(read_only=False) as db:
                added = TransactionService(db).add_tags(
                    transaction_id, tags, actor="cli"
                )
    except ValueError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_tags_add"
        )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=TagsPayload(transaction_id=transaction_id, tags=added),
                sensitivity="low",
            ),
            output,
            cli_actor="transactions_tags_add",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Transaction", transaction_id),
                    (
                        "Tags added",
                        ", ".join(added) if added else "none (already present)",
                    ),
                ],
                title="Tags updated",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("remove")
def transactions_tags_remove(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    tags: list[str] = typer.Argument(..., help="One or more tags to remove"),
    output: OutputFormat = output_option,
) -> None:
    """Remove one or more tags from a transaction."""
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            removed = TransactionService(db).remove_tags(
                transaction_id, tags, actor="cli"
            )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=TagsPayload(transaction_id=transaction_id, tags=removed),
                sensitivity="low",
            ),
            output,
            cli_actor="transactions_tags_remove",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Transaction", transaction_id),
                    ("Tags removed", ", ".join(removed) if removed else "none matched"),
                ],
                title="Tags updated",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("list")
def transactions_tags_list(
    transaction_id: str | None = typer.Argument(
        None,
        help="Transaction ID (omit to list all distinct tags with usage counts)",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List tags on a transaction, or all distinct tags with usage counts."""
    from moneybin.services.transaction_service import TransactionService

    tags: list[str] = []
    rows: list[tuple[str, int]] = []
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            svc = TransactionService(db)
            if transaction_id is not None:
                tags = svc.list_tags(transaction_id)
                if output == OutputFormat.JSON:
                    render_or_json(
                        build_envelope(
                            data=TagsPayload(transaction_id=transaction_id, tags=tags),
                            sensitivity="low",
                        ),
                        output,
                        cli_actor="transactions_tags_list",
                    )
                    return
            else:
                rows = svc.list_distinct_tags()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=[{"tag": t, "usage_count": n} for t, n in rows],
                sensitivity="low",
            ),
            output,
            cli_actor="transactions_tags_list",
        )
        return
    policy = get_terminal_policy(no_pager=no_pager)
    if transaction_id is not None:
        parts: list[object] = [
            build_summary([("Transaction", transaction_id)], title="Tags")
        ]
        if tags:
            parts.append(build_rows(["tag"], [(tag,) for tag in tags], terminal=policy))
        else:
            parts.append(build_summary([("Result", "No tags on this transaction.")]))
    else:
        parts = [build_summary([], title="Tags")]
        if rows:
            parts.append(
                build_rows(
                    ["tag", "transactions"],
                    list(rows),
                    numeric=("transactions",),
                    terminal=policy,
                )
            )
        else:
            parts.append(build_summary([("Result", "No tags in use.")]))
    emit_human_result(
        compose_human_result(
            parts,
            disclosures=(
                ()
                if (tags if transaction_id is not None else rows)
                else ("Next: moneybin transactions tags add --help",)
            ),
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


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
            with get_database(read_only=False) as db:
                result = TransactionService(db).rename_tag(old, new, actor="cli")
    except ValueError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_tags_rename"
        )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=TagRenamePayload(
                    old_tag=old,
                    new_tag=new,
                    row_count=result.row_count,
                    parent_audit_id=result.parent_audit_id,
                ),
                sensitivity="low",
            ),
            output,
            cli_actor="transactions_tags_rename",
        )
        return
    emit_human_result(
        compose_human_result(
            [
                build_summary(
                    [
                        ("Tag", old),
                        ("Renamed to", new),
                        ("Transactions updated", str(result.row_count)),
                        ("Audit ID", result.parent_audit_id),
                    ],
                    title="Tag renamed",
                )
            ],
            disclosures=(f"Next: moneybin system audit show {result.parent_audit_id}",),
        ),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
