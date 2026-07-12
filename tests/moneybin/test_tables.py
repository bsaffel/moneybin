"""Tests for the TableRef registry and INTERFACE_TABLES derivation."""

from __future__ import annotations

import pytest

from moneybin.tables import (
    ACCOUNT_SETTINGS,
    AUDIT_LOG,
    BALANCE_ASSERTIONS,
    BRIDGE_CATEGORY_SOURCE_MAP,
    BRIDGE_TRANSFERS,
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
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
    IMPORTS,
    INTERFACE_TABLES,
    LOT_SELECTIONS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    MERCHANTS,
    OFX_TRANSACTIONS,
    PLAID_INVESTMENT_HOLDING_LOTS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS,
    PLAID_INVESTMENT_TRANSACTIONS,
    PLAID_SECURITIES,
    REPORTS_BALANCE_DRIFT,
    REPORTS_CASH_FLOW,
    REPORTS_LARGE_TRANSACTIONS,
    REPORTS_MERCHANT_ACTIVITY,
    REPORTS_NET_WORTH,
    REPORTS_RECURRING_SUBSCRIPTIONS,
    REPORTS_SPENDING_TREND,
    REPORTS_UNCATEGORIZED_QUEUE,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
    SEED_EXCHANGE_MIC_MAP,
    TRANSACTION_CATEGORIES,
    TRANSACTION_NOTES,
    TRANSACTION_SPLITS,
    TRANSACTION_TAGS,
    TableRef,
)

pytestmark = pytest.mark.unit

EXPECTED_INTERFACE = {
    FCT_TRANSACTIONS.full_name,
    FCT_TRANSACTION_LINES.full_name,
    DIM_ACCOUNTS.full_name,
    BRIDGE_TRANSFERS.full_name,
    BRIDGE_CATEGORY_SOURCE_MAP.full_name,
    CATEGORIES.full_name,
    BUDGETS.full_name,
    TRANSACTION_NOTES.full_name,
    MERCHANTS.full_name,
    CATEGORIZATION_RULES.full_name,
    TRANSACTION_CATEGORIES.full_name,
    FCT_BALANCES.full_name,
    FCT_BALANCES_DAILY.full_name,
    ACCOUNT_SETTINGS.full_name,
    BALANCE_ASSERTIONS.full_name,
    TRANSACTION_TAGS.full_name,
    TRANSACTION_SPLITS.full_name,
    IMPORTS.full_name,
    AUDIT_LOG.full_name,
    REPORTS_NET_WORTH.full_name,
    REPORTS_CASH_FLOW.full_name,
    REPORTS_SPENDING_TREND.full_name,
    REPORTS_RECURRING_SUBSCRIPTIONS.full_name,
    REPORTS_UNCATEGORIZED_QUEUE.full_name,
    REPORTS_MERCHANT_ACTIVITY.full_name,
    REPORTS_LARGE_TRANSACTIONS.full_name,
    REPORTS_BALANCE_DRIFT.full_name,
    DIM_SECURITIES.full_name,
    FCT_INVESTMENT_TRANSACTIONS.full_name,
    FCT_INVESTMENT_LOTS.full_name,
    FCT_REALIZED_GAINS.full_name,
    DIM_HOLDINGS.full_name,
}


def test_audience_defaults_to_internal() -> None:
    """Audience field defaults to "internal" when not specified."""
    t = TableRef("foo", "bar")
    assert t.audience == "internal"


def test_interface_tables_set_matches_expected() -> None:
    """INTERFACE_TABLES contains exactly the expected set of full names."""
    full_names = {t.full_name for t in INTERFACE_TABLES}
    assert full_names == EXPECTED_INTERFACE


def test_interface_tables_all_carry_interface_audience() -> None:
    """Every entry in INTERFACE_TABLES has audience="interface"."""
    for t in INTERFACE_TABLES:
        assert t.audience == "interface"


def test_internal_tables_excluded_from_interface() -> None:
    """Raw tables are not present in INTERFACE_TABLES."""
    assert OFX_TRANSACTIONS not in INTERFACE_TABLES
    assert OFX_TRANSACTIONS.audience == "internal"


def test_full_name_unchanged() -> None:
    """full_name property returns schema.name as before."""
    assert FCT_TRANSACTIONS.full_name == "core.fct_transactions"


def test_investment_table_refs() -> None:
    """M1J.1 investment constants resolve to the spec'd tables."""
    assert SECURITIES.full_name == "app.securities"
    assert LOT_SELECTIONS.full_name == "app.lot_selections"
    assert (
        MANUAL_INVESTMENT_TRANSACTIONS.full_name == "raw.manual_investment_transactions"
    )
    assert DIM_SECURITIES.full_name == "core.dim_securities"
    assert FCT_INVESTMENT_TRANSACTIONS.full_name == "core.fct_investment_transactions"
    assert FCT_INVESTMENT_LOTS.full_name == "core.fct_investment_lots"
    assert FCT_REALIZED_GAINS.full_name == "core.fct_realized_gains"
    assert DIM_HOLDINGS.full_name == "core.dim_holdings"
    # M1G.4 Plaid investment sync raw tables
    assert PLAID_SECURITIES.full_name == "raw.plaid_securities"
    assert (
        PLAID_INVESTMENT_TRANSACTIONS.full_name == "raw.plaid_investment_transactions"
    )
    assert PLAID_INVESTMENT_HOLDINGS.full_name == "raw.plaid_investment_holdings"
    assert (
        PLAID_INVESTMENT_HOLDING_LOTS.full_name == "raw.plaid_investment_holding_lots"
    )
    assert (
        PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS.full_name
        == "raw.plaid_investment_holdings_snapshots"
    )
    # M1G.4 Plaid investment sync app tables (placeholders for later schema creation)
    assert SECURITY_LINKS.full_name == "app.security_links"
    assert SECURITY_LINK_DECISIONS.full_name == "app.security_link_decisions"
    # M1G.4 Seed table for MIC normalization
    assert SEED_EXCHANGE_MIC_MAP.full_name == "seeds.exchange_mic_map"
    # The five core investment models are audience="interface" (SQLMesh models +
    # schema-catalog examples have landed, satisfying the INTERFACE_TABLES live
    # catalog contract). app.* and raw.* investment tables stay internal —
    # they're application-managed / ingest-only, not curated query surfaces.
    assert DIM_HOLDINGS.audience == "interface"
    assert DIM_SECURITIES.audience == "interface"
    assert FCT_INVESTMENT_TRANSACTIONS.audience == "interface"
    assert FCT_INVESTMENT_LOTS.audience == "interface"
    assert FCT_REALIZED_GAINS.audience == "interface"
    assert SECURITIES.audience == "internal"
    assert MANUAL_INVESTMENT_TRANSACTIONS.audience == "internal"
