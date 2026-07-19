"""Merchant mapping management (list, create) and link-review subgroup."""

import typer

from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope

from . import links

app = typer.Typer(
    help="Merchant mappings management",
    no_args_is_help=True,
)
app.add_typer(links.app, name="links")


@app.command("list")
def merchants_list(output: OutputFormat = output_option) -> None:
    """List all merchant mappings."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = CategorizationService(db).list_merchants()

    envelope = build_envelope(data=payload, sensitivity="medium")
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="merchants_list")
        return
    for row in payload.merchants:
        pattern = f"  {row.raw_pattern}" if row.raw_pattern else ""
        typer.echo(f"{row.merchant_id}  {row.canonical_name}{pattern}")


@app.command("create")
def merchants_create(
    pattern: str = typer.Argument(..., help="Merchant name pattern"),
    canonical_name: str = typer.Argument(..., help="Canonical merchant name"),
    default_category: str | None = typer.Option(
        None, "--default-category", help="Default category for this merchant"
    ),
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
    typer.echo(merchant_id)
