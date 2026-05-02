"""System top-level command group.

Owns system and data status operations.
"""

import typer

app = typer.Typer(
    help="System and data status",
    no_args_is_help=True,
)
