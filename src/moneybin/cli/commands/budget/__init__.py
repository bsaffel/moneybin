"""Budget top-level command group.

Owns budget target management operations. Vs-actual report lives in `reports budget`.
"""

import typer

app = typer.Typer(
    help="Budget target management (vs-actual report lives in `reports budget`)",
    no_args_is_help=True,
)
