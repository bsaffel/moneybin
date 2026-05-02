"""Reports top-level command group.

Owns cross-domain analytical and aggregation view operations.
"""

import typer

app = typer.Typer(
    help="Cross-domain analytical and aggregation views",
    no_args_is_help=True,
)
