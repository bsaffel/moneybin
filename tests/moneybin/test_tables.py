"""Tests for the TableRef registry and INTERFACE_TABLES derivation."""

from __future__ import annotations

from moneybin.tables import (
    BRIDGE_TRANSFERS,
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    INTERFACE_TABLES,
    MERCHANTS,
    OFX_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
    TRANSACTION_NOTES,
    TableRef,
)

EXPECTED_INTERFACE = {
    FCT_TRANSACTIONS.full_name,
    DIM_ACCOUNTS.full_name,
    BRIDGE_TRANSFERS.full_name,
    CATEGORIES.full_name,
    BUDGETS.full_name,
    TRANSACTION_NOTES.full_name,
    MERCHANTS.full_name,
    CATEGORIZATION_RULES.full_name,
    TRANSACTION_CATEGORIES.full_name,
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
