"""Transactions top-level command group.

Owns transaction entity operations and workflows on transactions
(matches, categorize, review) per cli-restructure.md v2, plus curation
primitives (create, notes, tags, splits, audit) per
``docs/specs/transaction-curation.md``.
"""

import typer

from . import categorize, matches, notes, splits, tags
from .audit import transactions_audit
from .create import transactions_create
from .list_ import transactions_list
from .review import transactions_review

app = typer.Typer(
    help=(
        "Transactions and workflows on them "
        "(matches, categorize, review, notes, tags, splits, audit)"
    ),
    no_args_is_help=True,
)

app.add_typer(categorize.app, name="categorize")
app.add_typer(matches.app, name="matches")
app.add_typer(notes.app, name="notes")
app.add_typer(tags.app, name="tags")
app.add_typer(splits.app, name="splits")
app.command("review")(transactions_review)
app.command("create")(transactions_create)
app.command("audit")(transactions_audit)
app.command("list")(transactions_list)
