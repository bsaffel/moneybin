"""Transaction splits subgroup: add, list, remove, clear.

Thin wrappers over ``TransactionService`` split methods. After ``add`` and
``remove`` we report the parent's residual balance so users see at a glance
whether children sum to the parent. Non-zero residual is a warning, not an
error (per spec — splits are warn-not-block).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import (
    UNCATEGORIZED_LABEL,
    Money,
    build_rows,
    build_summary,
    compose_human_result,
)
from moneybin.cli.utils import abort_cli_error, get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.transactions import (
    SplitAddPayload,
    SplitRemovePayload,
    SplitRow,
    SplitsPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.transaction_service import Split

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Splits: allocate one transaction across categories",
    no_args_is_help=True,
)


def _split_row(s: Split) -> SplitRow:
    return SplitRow(
        split_id=s.split_id,
        transaction_id=s.transaction_id,
        amount=str(s.amount),
        category=s.category,
        subcategory=s.subcategory,
        note=s.note,
        ord=s.ord,
        created_at=s.created_at,
        created_by=s.created_by,
    )


@app.command("add")
def transactions_splits_add(
    transaction_id: str = typer.Argument(..., help="Parent transaction ID"),
    amount: str = typer.Argument(..., help="Signed decimal amount"),
    category: str | None = typer.Option(None, "--category", help="Category"),
    subcategory: str | None = typer.Option(None, "--subcategory", help="Subcategory"),
    note: str | None = typer.Option(None, "--note", help="Optional split note"),
    output: OutputFormat = output_option,
) -> None:
    """Append a split to a transaction."""
    from moneybin.services.transaction_service import TransactionService

    try:
        amount_dec = Decimal(amount)
    except InvalidOperation as e:
        abort_cli_error(
            e,
            output=output,
            exit_code=2,
            cli_actor="transactions_splits_add",
            message=f"Invalid amount {amount!r}",
        )

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                svc = TransactionService(db)
                split = svc.add_split(
                    transaction_id,
                    amount_dec,
                    category=category,
                    subcategory=subcategory,
                    note=note,
                    actor="cli",
                )
                residual = svc.splits_balance(transaction_id)
    except LookupError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_splits_add"
        )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=SplitAddPayload(split=_split_row(split), residual=str(residual)),
            ),
            output,
            cli_actor="transactions_splits_add",
        )
        return
    _emit_split_receipt(
        "Split added", split.split_id, transaction_id, split.amount, residual
    )


@app.command("list")
def transactions_splits_list(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List splits on a transaction."""
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            splits = TransactionService(db).list_splits(transaction_id)

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=SplitsPayload(splits=[_split_row(s) for s in splits]),
            ),
            output,
            cli_actor="transactions_splits_list",
        )
        return
    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary([("Transaction", transaction_id)], title="Transaction splits")
    ]
    if splits:
        parts.append(
            build_rows(
                ["split id", "amount", "category", "subcategory", "note"],
                [
                    (
                        split.split_id,
                        split.amount,
                        UNCATEGORIZED_LABEL
                        if split.category is None
                        else split.category,
                        split.subcategory or "-",
                        split.note or "-",
                    )
                    for split in splits
                ],
                money={"amount": Money("flow")},
                terminal=policy,
            )
        )
    else:
        parts.append(build_summary([("Result", "No splits on this transaction.")]))
    emit_human_result(
        compose_human_result(
            parts,
            disclosures=(
                () if splits else ("Next: moneybin transactions splits add --help",)
            ),
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("remove")
def transactions_splits_remove(
    split_id: str = typer.Argument(..., help="Split ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Remove a single split."""
    from moneybin.services.transaction_service import TransactionService

    if not yes:
        if not typer.confirm(f"Remove split {split_id}?"):
            _emit_split_cancellation("Split removal cancelled", "No split was removed")
            raise typer.Exit(0)

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                svc = TransactionService(db)
                # Look up parent before delete so we can report residual after.
                existing = svc.get_split(split_id)
                if existing is None:
                    abort_cli_error(
                        LookupError(f"split_id={split_id} not found"),
                        output=output,
                        exit_code=1,
                        cli_actor="transactions_splits_remove",
                    )
                transaction_id = existing.transaction_id
                svc.remove_split(split_id, actor="cli")
                residual = svc.splits_balance(transaction_id)
    except LookupError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_splits_remove"
        )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=SplitRemovePayload(
                    split_id=split_id,
                    transaction_id=transaction_id,
                    residual=str(residual),
                ),
            ),
            output,
            cli_actor="transactions_splits_remove",
        )
        return
    _emit_split_receipt("Split removed", split_id, transaction_id, None, residual)


@app.command("clear")
def transactions_splits_clear(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Delete all splits on a transaction."""
    from moneybin.services.transaction_service import TransactionService

    if not yes:
        if not typer.confirm(f"Clear all splits on {transaction_id}?"):
            _emit_split_cancellation("Split clear cancelled", "No splits were removed")
            raise typer.Exit(0)

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            cleared = TransactionService(db).clear_splits(transaction_id, actor="cli")

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={"transaction_id": transaction_id, "cleared": True},
                sensitivity="low",
            ),
            output,
            cli_actor="transactions_splits_clear",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Transaction", cleared.transaction_id),
                    ("Splits cleared", str(cleared.cleared_count)),
                ],
                title="Splits cleared",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


def _emit_split_receipt(
    title: str,
    split_id: str,
    transaction_id: str,
    amount: Decimal | None,
    residual: Decimal,
) -> None:
    """Render one unpaged split-mutation receipt from service-returned state."""
    policy = get_terminal_policy()
    rows: list[tuple[object, ...]] = [(split_id, transaction_id, residual)]
    columns = ["split id", "transaction", "residual"]
    money = {"residual": Money("flow")}
    if amount is not None:
        columns.insert(2, "amount")
        rows = [(split_id, transaction_id, amount, residual)]
        money["amount"] = Money("flow")
    emit_human_result(
        compose_human_result(
            [
                build_summary([("Result", title)]),
                build_rows(columns, rows, money=money, terminal=policy),
            ],
            disclosures=(
                () if residual == Decimal("0") else ("Warning: Splits do not balance.",)
            ),
        ),
        policy=policy,
        finite_read=False,
        receipt=True,
    )


def _emit_split_cancellation(title: str, saved_state: str) -> None:
    """Present a declined split mutation as a terminal outcome."""
    emit_human_result(
        compose_human_result([
            build_summary([("Saved state", saved_state)], title=title)
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
