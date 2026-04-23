"""Verify tabular table registry constants exist and are well-formed."""

from moneybin.tables import (
    IMPORT_LOG,
    TABULAR_ACCOUNTS,
    TABULAR_FORMATS,
    TABULAR_TRANSACTIONS,
)


def test_tabular_transactions_ref() -> None:
    """Verify TABULAR_TRANSACTIONS constant has correct schema and name."""
    assert TABULAR_TRANSACTIONS.schema == "raw"
    assert TABULAR_TRANSACTIONS.name == "tabular_transactions"
    assert TABULAR_TRANSACTIONS.full_name == "raw.tabular_transactions"


def test_tabular_accounts_ref() -> None:
    """Verify TABULAR_ACCOUNTS constant has correct schema and name."""
    assert TABULAR_ACCOUNTS.schema == "raw"
    assert TABULAR_ACCOUNTS.name == "tabular_accounts"
    assert TABULAR_ACCOUNTS.full_name == "raw.tabular_accounts"


def test_import_log_ref() -> None:
    """Verify IMPORT_LOG constant has correct schema and name."""
    assert IMPORT_LOG.schema == "raw"
    assert IMPORT_LOG.name == "import_log"
    assert IMPORT_LOG.full_name == "raw.import_log"


def test_tabular_formats_ref() -> None:
    """Verify TABULAR_FORMATS constant has correct schema and name."""
    assert TABULAR_FORMATS.schema == "app"
    assert TABULAR_FORMATS.name == "tabular_formats"
    assert TABULAR_FORMATS.full_name == "app.tabular_formats"
