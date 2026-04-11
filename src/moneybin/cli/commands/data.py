"""Data pipeline commands for MoneyBin CLI.

This module groups fine-grained pipeline operations (extract, transform)
under a single ``data`` command group for power users.
"""

import typer

from . import categorize, extract, transform

app = typer.Typer(
    help="Fine-grained data pipeline: extract and transform steps individually",
    no_args_is_help=True,
)

app.add_typer(
    extract.app,
    name="extract",
    help="Parse local files (OFX, W-2) into DuckDB raw tables",
)
app.add_typer(
    transform.app,
    name="transform",
    help="Run SQLMesh models to rebuild staging and core tables",
)
app.add_typer(
    categorize.app,
    name="categorize",
    help="Manage transaction categories, rules, and merchants",
)
