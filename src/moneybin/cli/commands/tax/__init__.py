"""Tax top-level command group.

Owns tax forms, deductions, and tax-prep utility operations.
"""

import typer

app = typer.Typer(
    help="Tax forms, deductions, and tax-prep utilities",
    no_args_is_help=True,
)
