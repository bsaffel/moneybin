"""Data pipeline commands for MoneyBin CLI.

This module groups fine-grained pipeline operations (extract, load, transform)
under a single ``data`` command group for power users.
"""

import typer

from . import extract, load, transform

app = typer.Typer(
    help="Fine-grained data pipeline: extract, load, and transform steps individually",
    no_args_is_help=True,
)

app.add_typer(
    extract.app,
    name="extract",
    help="Parse local files (OFX, W-2) into structured data and Parquet",
)
app.add_typer(
    load.app,
    name="load",
    help="Load extracted Parquet files into DuckDB raw tables",
)
app.add_typer(
    transform.app,
    name="transform",
    help="Run SQLMesh models to rebuild staging and core tables",
)
