"""Merchant mapping management (list, create) and link-review subgroup."""

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
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.categories import MerchantCreatePayload
from moneybin.protocol.envelope import build_envelope

from . import links

app = typer.Typer(
    help="Merchant mappings management",
    no_args_is_help=True,
)
app.add_typer(links.app, name="links")


@app.command("list")
def merchants_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # list emits result rows only
    no_pager: bool = no_pager_option,
) -> None:
    """List all merchant mappings."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = CategorizationService(db).list_merchants()

    envelope = build_envelope(data=payload, sensitivity="medium")
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="merchants_list")
        return
    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary(
            [("Mappings", f"{len(payload.merchants):,}")], title="Merchant mappings"
        )
    ]
    if payload.merchants:
        parts.append(
            build_rows(
                ["merchant id", "canonical name", "pattern", "category"],
                [
                    (
                        row.merchant_id,
                        row.canonical_name,
                        row.raw_pattern or "-",
                        " / ".join(
                            part for part in (row.category, row.subcategory) if part
                        )
                        or "-",
                    )
                    for row in payload.merchants
                ],
                terminal=policy,
            )
        )
    else:
        parts.append(build_summary([("Result", "No merchant mappings found.")]))
    disclosures = (
        ()
        if payload.merchants
        else ("Next: moneybin transactions categorize run --methods merchants",)
    )
    emit_human_result(
        compose_human_result(parts, disclosures=disclosures),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("create")
def merchants_create(
    pattern: str = typer.Argument(..., help="Merchant name pattern"),
    canonical_name: str = typer.Argument(..., help="Canonical merchant name"),
    default_category: str | None = typer.Option(
        None, "--default-category", help="Default category for this merchant"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Create a merchant mapping."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            merchant_id = CategorizationService(db).create_merchant(
                pattern,
                canonical_name,
                match_type="contains",
                category=default_category,
                created_by="user",
                actor="cli",
            )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=MerchantCreatePayload(
                    merchant_id=merchant_id,
                    action="created",
                )
            ),
            output,
            cli_actor="merchants_create",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Merchant ID", merchant_id),
                    ("Pattern", pattern),
                    ("Canonical name", canonical_name),
                    ("Default category", default_category or "-"),
                ],
                title="Merchant mapping created",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
