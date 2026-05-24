"""Reports top-level command group — cross-domain read-only views.

The view-backed reports (cashflow, spending, recurring, merchants,
large-transactions, balance-drift) are generated from ``@report`` runners in
``moneybin.reports.definitions`` and registered via ``register_reports_cli``.
``networth`` / ``networth-history`` are NetworthService-backed and stay
hand-written.
"""

from __future__ import annotations

import logging

import typer

from moneybin.reports._framework.registry import register_reports_cli
from moneybin.reports.definitions import ALL_REPORTS

from ..stubs import _not_implemented
from .networth import reports_networth, reports_networth_history

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Cross-domain analytical and aggregation views",
    no_args_is_help=True,
)

app.command("networth")(reports_networth)
app.command("networth-history")(reports_networth_history)
register_reports_cli(ALL_REPORTS, app)


@app.command("budget")
def reports_budget() -> None:
    """Budget vs actual report."""
    _not_implemented("budget-tracking.md")


@app.command("health")
def reports_health(months: int = typer.Option(1, "--months")) -> None:
    """Cross-domain financial health snapshot."""
    _not_implemented("reports-net-worth.md")
