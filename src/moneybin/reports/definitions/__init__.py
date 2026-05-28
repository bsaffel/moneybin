"""In-tree report runners — the reference set every report (incl. packages) mirrors.

Each module declares one ``@report`` runner; ``ALL_REPORTS`` is the explicit
registration list the application wires via ``register_reports``. ``networth`` /
``networth_history`` are NetworthService-backed (not single reports.* view reads)
and stay hand-written — a documented exception, not part of this list.
"""

from __future__ import annotations

from moneybin.reports._framework.contract import Runner
from moneybin.reports.definitions.balance_drift import balance_drift
from moneybin.reports.definitions.cash_flow import cash_flow
from moneybin.reports.definitions.large_transactions import large_transactions
from moneybin.reports.definitions.merchant_activity import merchant_activity
from moneybin.reports.definitions.recurring_subscriptions import recurring_subscriptions
from moneybin.reports.definitions.spending_trend import spending_trend

ALL_REPORTS: list[Runner] = [
    spending_trend,
    cash_flow,
    recurring_subscriptions,
    merchant_activity,
    large_transactions,
    balance_drift,
]

__all__ = ["ALL_REPORTS"]
