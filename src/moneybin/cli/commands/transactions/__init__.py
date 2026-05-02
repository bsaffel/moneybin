"""Transactions top-level command group.

Owns transaction operations (list, show, edit, bulk-edit) and
per-transaction workflows (categorize, review matches) per cli-restructure.md v2.
"""

import typer

app = typer.Typer(
    help="Transactions and workflows on them (matches, categorize, review)",
    no_args_is_help=True,
)
