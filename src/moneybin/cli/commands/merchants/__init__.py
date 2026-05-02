"""Merchants top-level command group.

Owns merchant mapping management operations.
"""

import typer

app = typer.Typer(
    help="Merchant mappings management",
    no_args_is_help=True,
)
