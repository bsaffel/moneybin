"""Reports top-level command group.

Owns cross-domain analytical and aggregation view operations. Per
cli-restructure.md v2: cross-cutting read-only views (networth,
spending, cashflow, financial health, budget vs actual).
"""

import typer

from ..stubs import _not_implemented
from . import networth

app = typer.Typer(
    help="Cross-domain analytical and aggregation views",
    no_args_is_help=True,
)

app.add_typer(networth.app, name="networth")


@app.command("spending")
def reports_spending() -> None:
    """Spending analysis report."""
    _not_implemented("cli-restructure.md")


@app.command("cashflow")
def reports_cashflow() -> None:
    """Cash flow report."""
    _not_implemented("cli-restructure.md")


@app.command("budget")
def reports_budget() -> None:
    """Budget vs actual report."""
    _not_implemented("budget-tracking.md")


@app.command("health")
def reports_health(months: int = typer.Option(1, "--months")) -> None:
    """Cross-domain financial health snapshot."""
    _not_implemented("net-worth.md")
