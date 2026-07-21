"""Closed portability catalog for canonical bundle exports."""

from __future__ import annotations

from dataclasses import dataclass

from moneybin.tables import (
    BRIDGE_TRANSFERS,
    CATEGORIES,
    DIM_ACCOUNTS,
    DIM_HOLDINGS,
    DIM_SECURITIES,
    FCT_BALANCES,
    FCT_BALANCES_DAILY,
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_REALIZED_GAINS,
    FCT_TRANSACTION_LINES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    TableRef,
)


@dataclass(frozen=True, slots=True)
class BundleTable:
    """One public artifact table and its fixed canonical query order."""

    name: str
    source: TableRef
    order_by: tuple[str, ...]


BUNDLE_TABLES: tuple[BundleTable, ...] = (
    BundleTable("accounts", DIM_ACCOUNTS, ("account_id",)),
    BundleTable(
        "transactions", FCT_TRANSACTIONS, ("transaction_date", "transaction_id")
    ),
    BundleTable(
        "transaction_lines",
        FCT_TRANSACTION_LINES,
        ("transaction_date", "transaction_id", "line_id"),
    ),
    BundleTable("transfers", BRIDGE_TRANSFERS, ("transfer_id",)),
    BundleTable(
        "balances",
        FCT_BALANCES,
        ("balance_date", "account_id", "source_type", "source_ref"),
    ),
    BundleTable("balances_daily", FCT_BALANCES_DAILY, ("balance_date", "account_id")),
    BundleTable("categories", CATEGORIES, ("category_id",)),
    BundleTable("merchants", MERCHANTS, ("merchant_id",)),
    BundleTable("securities", DIM_SECURITIES, ("security_id",)),
    BundleTable(
        "investment_transactions",
        FCT_INVESTMENT_TRANSACTIONS,
        ("trade_date", "investment_transaction_id"),
    ),
    BundleTable("investment_lots", FCT_INVESTMENT_LOTS, ("acquisition_date", "lot_id")),
    BundleTable(
        "realized_gains", FCT_REALIZED_GAINS, ("disposal_date", "realized_gain_id")
    ),
    BundleTable("holdings", DIM_HOLDINGS, ("account_id", "security_id")),
)
