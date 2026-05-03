"""Assets top-level command group.

Owns physical asset operations (real estate, vehicles, valuables).
"""

import typer

app = typer.Typer(
    help="Physical assets (real estate, vehicles, valuables)",
    no_args_is_help=True,
)
