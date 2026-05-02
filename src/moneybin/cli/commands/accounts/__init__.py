"""Accounts top-level command group.

Owns account entity operations (list, show, rename, include) and
per-account workflows (balance) per cli-restructure.md v2.
"""

import typer

app = typer.Typer(
    help="Accounts and per-account workflows (balance, investments)",
    no_args_is_help=True,
)
