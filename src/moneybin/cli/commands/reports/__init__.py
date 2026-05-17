"""Reports top-level command group — cross-domain read-only views."""

from __future__ import annotations

import logging

import typer

from ..stubs import _not_implemented
from .balance_drift import reports_balance_drift
from .cashflow import reports_cashflow
from .large_transactions import reports_large_transactions
from .merchants import reports_merchants
from .networth import reports_networth, reports_networth_history
from .recurring import reports_recurring
from .spending import reports_spending
from .uncategorized import reports_uncategorized

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Cross-domain analytical and aggregation views",
    no_args_is_help=True,
)

app.command("networth")(reports_networth)
app.command("networth-history")(reports_networth_history)
app.command("cashflow")(reports_cashflow)
app.command("spending")(reports_spending)
app.command("recurring")(reports_recurring)
app.command("merchants")(reports_merchants)
app.command("uncategorized")(reports_uncategorized)
app.command("large-transactions")(reports_large_transactions)
app.command("balance-drift")(reports_balance_drift)


@app.command("budget")
def reports_budget() -> None:
    """Budget vs actual report."""
    _not_implemented("budget-tracking.md")


@app.command("health")
def reports_health(months: int = typer.Option(1, "--months")) -> None:
    """Cross-domain financial health snapshot."""
    _not_implemented("net-worth.md")
