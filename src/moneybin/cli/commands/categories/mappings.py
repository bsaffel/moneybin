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

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import (
    abort_cli_error,
    get_terminal_policy,
    handle_cli_errors,
)
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.privacy.payloads.category_mappings import (
    CategoryMappingSetPayload,
    CategoryMappingsPendingPayload,
)
from moneybin.protocol.envelope import build_envelope

app = typer.Typer(
    help="Curate imported category text mappings",
    no_args_is_help=True,
)


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
    A term no imported transaction carries, and that has no mapping yet, is
    refused. Pass exactly one of:
      --into <category_id>   map to this existing category
      --new <name>           create a new category, then map to it

    Examples:
      moneybin categories mappings set --namespace chase_credit --category Groceries --into cat-food
      moneybin categories mappings set --namespace mint --category "Home Improvement" --new "Housing"
    """
    # Usage errors go through the output-aware seam so --output json still
    # returns an error envelope, while keeping the usage exit code.
    usage_error: str | None = None
    if into is not None and new is not None:
        usage_error = "--into and --new are mutually exclusive"
    elif not into and not new:
        usage_error = "Specify either --into <category_id> or --new <name>"
    if usage_error is not None:
        abort_cli_error(
            UserError(usage_error, code=error_codes.MUTATION_INVALID_INPUT),
            output=output,
            exit_code=2,
            cli_actor="categories_mappings_set",
        )

    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            mapping = CategorizationService(db).resolve_source_term(
                source_origin=namespace,
                category=category,
                subcategory=subcategory,
                category_id=into,
                new_category=new,
                actor="cli",
            )

    # Echo the term as stored (trimmed), not as typed, so the receipt names
    # the row a later `set` on the same term would address.
    payload = CategoryMappingSetPayload(
        source_origin=mapping.source_origin,
        category=mapping.category,
        subcategory=mapping.subcategory,
        category_id=mapping.category_id,
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
    receipt = [("Namespace", payload.source_origin), ("Category", payload.category)]
    if payload.subcategory:
        receipt.append(("Subcategory", payload.subcategory))
    receipt.append(("Mapped to", payload.category_id))
    emit_human_result(
        compose_human_result([
            build_summary(receipt, title="Category mapping recorded")
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
