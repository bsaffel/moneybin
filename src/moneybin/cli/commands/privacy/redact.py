"""Redact a transaction description the same way the LLM export does."""

import logging
import sys

import typer

logger = logging.getLogger(__name__)


def privacy_redact(
    description: str | None = typer.Argument(
        None,
        help="Description to redact, or '-' to read from stdin.",
    ),
) -> None:
    """Apply the same PII-stripping used by export-uncategorized to any text.

    Useful for verifying what an LLM will see before exporting a full batch.

    Examples:
      moneybin privacy redact "VENMO PAYMENT TO J SMITH"

      echo "STARBUCKS #1234 SEATTLE WA" | moneybin privacy redact -
    """
    from moneybin.services._text import redact_for_llm

    use_stdin = description == "-"

    if description is None:
        typer.echo(
            "Provide a description string or '-' to read from stdin.",
            err=True,
        )
        raise typer.Exit(2)

    if use_stdin:
        raw = sys.stdin.read().strip()
    else:
        raw = description

    typer.echo(redact_for_llm(raw))
