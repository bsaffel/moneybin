"""Transactions top-level command group.

Owns transaction entity operations and workflows on transactions
(matches, categorize, review) per cli-restructure.md v2.
"""

import typer

from . import categorize, matches
from .review import transactions_review

app = typer.Typer(
    help="Transactions and workflows on them (matches, categorize, review)",
    no_args_is_help=True,
)

app.add_typer(categorize.app, name="categorize")
app.add_typer(matches.app, name="matches")
app.command("review")(transactions_review)
