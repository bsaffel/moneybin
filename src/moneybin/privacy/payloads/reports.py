"""Typed payload dataclasses for the reports surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Payloads with ``ACCOUNT_IDENTIFIER`` (CRITICAL) resolve to ``Tier.CRITICAL``:
  - ``CashFlowPayload`` (account_id present when by='account' or 'account-and-category')
  - ``UncategorizedQueuePayload`` (account_id always present)
  - ``BalanceDriftPayload`` (account_id always present)

Remaining payloads carry ``TXN_AMOUNT`` (HIGH) as their strongest class and
resolve to ``Tier.HIGH``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class SpendingTrendRow:
    """One row of reports.spending_trend."""

    year_month: Annotated[str, DataClass.TXN_DATE]
    category: Annotated[str | None, DataClass.CATEGORY]
    total_spend: Annotated[Decimal, DataClass.TXN_AMOUNT]
    txn_count: Annotated[int, DataClass.AGGREGATE]
    prev_month_spend: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    mom_delta: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    mom_pct: Annotated[float | None, DataClass.AGGREGATE]
    prev_year_spend: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    yoy_delta: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    yoy_pct: Annotated[float | None, DataClass.AGGREGATE]
    trailing_3mo_avg: Annotated[Decimal | None, DataClass.TXN_AMOUNT]


@dataclass(frozen=True, slots=True)
class SpendingTrendPayload:
    """Payload for reports_spending."""

    rows: list[SpendingTrendRow]


@dataclass(frozen=True, slots=True)
class CashFlowRow:
    """One row of cash-flow rollup.

    Account / category columns are optional based on the ``by`` grouping:
    ``by='account'`` omits ``category``; ``by='category'`` omits ``account_id``
    and ``account_name``; ``by='account-and-category'`` populates all three.
    ``None`` means the column was not in the grouping for this query.
    """

    year_month: Annotated[str, DataClass.TXN_DATE]
    # CRITICAL — present when by includes 'account'; None otherwise.
    account_id: Annotated[str | None, DataClass.ACCOUNT_IDENTIFIER]
    account_name: Annotated[str | None, DataClass.INSTITUTION]
    category: Annotated[str | None, DataClass.CATEGORY]
    inflow: Annotated[Decimal, DataClass.TXN_AMOUNT]
    outflow: Annotated[Decimal, DataClass.TXN_AMOUNT]
    net: Annotated[Decimal, DataClass.TXN_AMOUNT]
    txn_count: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class CashFlowPayload:
    """Payload for reports_cashflow."""

    rows: list[CashFlowRow]


@dataclass(frozen=True, slots=True)
class RecurringSubscriptionRow:
    """One row of reports.recurring_subscriptions."""

    merchant_normalized: Annotated[str | None, DataClass.MERCHANT_NAME]
    cadence: Annotated[str | None, DataClass.TXN_TYPE]
    avg_amount: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    occurrence_count: Annotated[int, DataClass.AGGREGATE]
    first_seen: Annotated[date | None, DataClass.TXN_DATE]
    last_seen: Annotated[date | None, DataClass.TXN_DATE]
    status: Annotated[str | None, DataClass.TXN_TYPE]
    annualized_cost: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    confidence: Annotated[float, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class RecurringSubscriptionsPayload:
    """Payload for reports_recurring."""

    rows: list[RecurringSubscriptionRow]


@dataclass(frozen=True, slots=True)
class MerchantActivityRow:
    """One row of reports.merchant_activity."""

    merchant_normalized: Annotated[str | None, DataClass.MERCHANT_NAME]
    total_spend: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    total_inflow: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    total_outflow: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    txn_count: Annotated[int, DataClass.AGGREGATE]
    avg_amount: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    median_amount: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    first_seen: Annotated[date | None, DataClass.TXN_DATE]
    last_seen: Annotated[date | None, DataClass.TXN_DATE]
    active_months: Annotated[int | None, DataClass.AGGREGATE]
    top_category: Annotated[str | None, DataClass.CATEGORY]
    account_count: Annotated[int | None, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class MerchantActivityPayload:
    """Payload for reports_merchants."""

    rows: list[MerchantActivityRow]


@dataclass(frozen=True, slots=True)
class UncategorizedQueueRow:
    """One row of reports.uncategorized_queue."""

    transaction_id: Annotated[str, DataClass.RECORD_ID]
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    account_name: Annotated[str | None, DataClass.INSTITUTION]
    txn_date: Annotated[date, DataClass.TXN_DATE]
    amount: Annotated[Decimal, DataClass.TXN_AMOUNT]
    description: Annotated[str | None, DataClass.DESCRIPTION]
    merchant_normalized: Annotated[str | None, DataClass.MERCHANT_NAME]
    age_days: Annotated[int | None, DataClass.AGGREGATE]
    priority_score: Annotated[float | None, DataClass.AGGREGATE]
    source_type: Annotated[str | None, DataClass.TXN_TYPE]
    source_id: Annotated[str | None, DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class UncategorizedQueuePayload:
    """Payload for reports_uncategorized."""

    rows: list[UncategorizedQueueRow]


@dataclass(frozen=True, slots=True)
class LargeTransactionRow:
    """One row of reports.large_transactions."""

    transaction_id: Annotated[str, DataClass.RECORD_ID]
    account_name: Annotated[str | None, DataClass.INSTITUTION]
    txn_date: Annotated[date, DataClass.TXN_DATE]
    amount: Annotated[Decimal, DataClass.TXN_AMOUNT]
    description: Annotated[str | None, DataClass.DESCRIPTION]
    merchant_normalized: Annotated[str | None, DataClass.MERCHANT_NAME]
    category: Annotated[str | None, DataClass.CATEGORY]
    amount_zscore_account: Annotated[float | None, DataClass.AGGREGATE]
    amount_zscore_category: Annotated[float | None, DataClass.AGGREGATE]
    is_top_100: Annotated[bool, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class LargeTransactionsPayload:
    """Payload for reports_large_transactions."""

    rows: list[LargeTransactionRow]


@dataclass(frozen=True, slots=True)
class BalanceDriftRow:
    """One row of reports.balance_drift."""

    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    account_name: Annotated[str | None, DataClass.INSTITUTION]
    assertion_date: Annotated[date, DataClass.TXN_DATE]
    asserted_balance: Annotated[Decimal | None, DataClass.BALANCE]
    computed_balance: Annotated[Decimal | None, DataClass.BALANCE]
    drift: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    drift_abs: Annotated[Decimal | None, DataClass.TXN_AMOUNT]
    drift_pct: Annotated[float | None, DataClass.AGGREGATE]
    days_since_assertion: Annotated[int | None, DataClass.AGGREGATE]
    status: Annotated[str | None, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class BalanceDriftPayload:
    """Payload for reports_balance_drift."""

    rows: list[BalanceDriftRow]
