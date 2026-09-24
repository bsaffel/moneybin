"""categories mappings — curation commands for imported category-source text.

Subcommands: pending, set. Mirrors `merchants links` — thin wrappers over
CategorizationService. Imported category text is never displayed as a
category and never passed through; it is only ever an input the curator
matches against an existing MoneyBin category or uses to mint a new one
(the owner's ruling on this surface). `pending` enumerates the distinct
unmapped vocabulary terms (not one row per affected transaction — the
decision unit is the term); `set` resolves one term.
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
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import (
    format_cli_failure,
    get_terminal_policy,
    handle_cli_errors,
)
from moneybin.database import get_database
from moneybin.privacy.payloads.category_mappings import (
    CategoryMappingSetPayload,
    CategoryMappingsPendingPayload,
)
from moneybin.protocol.envelope import build_envelope

app = typer.Typer(
    help="Curate imported category text mappings",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("pending")
def mappings_pending(
    namespace: str | None = typer.Option(
        None,
        "--namespace",
        help="Filter to one source_origin (e.g. an exporter slug)",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List imported category-vocabulary terms with no curated mapping.

    Each term is a distinct (namespace, category, subcategory) triple pulled
    from imported transaction data — the decision unit is the term, not the
    transaction, so a handful of terms can stand behind many transactions.
    Shows how many uncategorized transactions mapping each term would
    categorize, and up to 3 suggested MoneyBin categories.
    Use `categories mappings set` to resolve each term.
    """
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            terms = CategorizationService(db).list_unmapped_source_terms(
                namespace=namespace
            )

    payload = CategoryMappingsPendingPayload.from_domain(terms)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json

        render_or_json(
            build_envelope(data=payload, sensitivity="low"),
            output,
            cli_actor="categories_mappings_pending",
        )
        return

    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary(
            [("Unmapped terms", str(len(payload.terms)))],
            title="Imported category terms",
        )
    ]
    if payload.terms:
        parts.append(
            build_rows(
                [
                    "namespace",
                    "category",
                    "subcategory",
                    "transactions",
                    "suggestions",
                ],
                [
                    (
                        t.source_origin,
                        t.category,
                        t.subcategory or "-",
                        t.transaction_count,
                        ", ".join(t.suggestions) or "-",
                    )
                    for t in payload.terms
                ],
                numeric=("transactions",),
                terminal=policy,
            )
        )
    else:
        parts.append(build_summary([("Result", "No unmapped terms.")]))
    disclosures: tuple[str, ...] = (
        (
            "Next: moneybin categories mappings set --namespace <namespace> "
            "--category <text> --into <category-id>",
        )
        if payload.terms and not quiet
        else ()
    )
    emit_human_result(
        compose_human_result(parts, disclosures=disclosures),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("set")
def mappings_set(
    namespace: str = typer.Option(
        ..., "--namespace", help="Term's source_origin (e.g. an exporter slug)"
    ),
    category: str = typer.Option(
        ..., "--category", help="Term's imported category text"
    ),
    subcategory: str | None = typer.Option(
        None, "--subcategory", help="Term's imported subcategory text, if any"
    ),
    into: str | None = typer.Option(
        None,
        "--into",
        help="Map the term to this existing category_id",
    ),
    new: str | None = typer.Option(
        None,
        "--new",
        help="Create a new category with this name, then map the term to it",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Map one imported category-vocabulary term to a MoneyBin category.

    Identify the term with --namespace, --category, and (if applicable)
    --subcategory — the exact triple `categories mappings pending` reported.
    Pass exactly one of:
      --into <category_id>   map to this existing category
      --new <name>           create a new category, then map to it

    Examples:
      moneybin categories mappings set --namespace chase_credit --category Groceries --into cat-food
      moneybin categories mappings set --namespace mint --category "Home Improvement" --new "Housing"
    """
    if into is not None and new is not None:
        logger.error(format_cli_failure("--into and --new are mutually exclusive"))
        raise typer.Exit(2)
    if not into and not new:
        logger.error(
            format_cli_failure("Specify either --into <category_id> or --new <name>")
        )
        raise typer.Exit(2)

    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            category_id = CategorizationService(db).resolve_source_term(
                source_origin=namespace,
                category=category,
                subcategory=subcategory,
                category_id=into,
                new_category=new,
                actor="cli",
            )

    payload = CategoryMappingSetPayload(
        source_origin=namespace,
        category=category,
        subcategory=subcategory,
        category_id=category_id,
        action="mapped",
    )
    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json

        render_or_json(
            build_envelope(data=payload, sensitivity="low"),
            output,
            cli_actor="categories_mappings_set",
        )
        return
    receipt = [("Namespace", namespace), ("Category", category)]
    if subcategory:
        receipt.append(("Subcategory", subcategory))
    receipt.append(("Mapped to", category_id))
    emit_human_result(
        compose_human_result([
            build_summary(receipt, title="Category mapping recorded")
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
