"""Categories top-level command group.

Owns category taxonomy management operations.
"""

import typer

app = typer.Typer(
    help="Category taxonomy management",
    no_args_is_help=True,
)
