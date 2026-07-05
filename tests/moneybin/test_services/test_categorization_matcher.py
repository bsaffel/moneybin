"""Tests for CategorizationMatcher's named-row projection (UncategorizedRow).

PR1 Task 2 makes CategorizationMatcher the single owner of the
``merchant_entity_id``-carrying ``prep.int_transactions__merged`` read and
returns a named, frozen dataclass instead of positional tuples so column
order stops being load-bearing at any consumer.
"""

import pytest

from moneybin.database import Database
from moneybin.services.categorization.matcher import (
    CategorizationMatcher,
    UncategorizedRow,
)
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture(autouse=True)
def _core_tables(db: Database) -> None:  # pyright: ignore[reportUnusedFunction]
    create_core_tables(db)


def _insert_plaid_txn(
    db: Database,
    *,
    txn_id: str,
    merchant_entity_id: str,
    description: str,
    account_id: str = "ACC1",
) -> None:
    """Insert a transaction plus its prep.int_transactions__merged entity carry.

    ``CREATE TABLE IF NOT EXISTS`` (rather than drop-and-recreate) so multiple
    calls within one test accumulate rows instead of wiping prior inserts.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__merged ("
        "  transaction_id VARCHAR PRIMARY KEY, "
        "  merchant_entity_id VARCHAR, "
        "  merchant_entity_source_type VARCHAR, "
        "  merchant_name VARCHAR"
        ")"
    )
    db.execute(
        "INSERT INTO prep.int_transactions__merged VALUES (?, ?, 'plaid', NULL)",
        [txn_id, merchant_entity_id],
    )
    db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES (?, ?, '2025-06-01', -5.00, ?, 'plaid')",
        [txn_id, account_id, description],
    )


@pytest.mark.unit
def test_fetch_uncategorized_rows_returns_named_rows(db: Database) -> None:
    """fetch_uncategorized_rows returns UncategorizedRow objects, not bare tuples."""
    _insert_plaid_txn(db, txn_id="t1", merchant_entity_id="ent1", description="COFFEE")

    rows = CategorizationMatcher(db).fetch_uncategorized_rows()

    assert rows is not None
    row = next(r for r in rows if r.transaction_id == "t1")
    assert isinstance(row, UncategorizedRow)
    assert row.merchant_entity_id == "ent1"
    assert row.description == "COFFEE"


@pytest.mark.unit
def test_fetch_rows_for_ids_filters_and_projects_the_same_row_shape(
    db: Database,
) -> None:
    """fetch_rows_for_ids returns only the requested ids, in the same UncategorizedRow shape."""
    _insert_plaid_txn(db, txn_id="t1", merchant_entity_id="ent1", description="COFFEE")
    _insert_plaid_txn(db, txn_id="t2", merchant_entity_id="ent2", description="GROCERY")

    rows = CategorizationMatcher(db).fetch_rows_for_ids(["t1"])

    assert rows is not None
    assert [r.transaction_id for r in rows] == ["t1"]
    assert rows[0].merchant_entity_id == "ent1"
    assert rows[0].description == "COFFEE"


@pytest.mark.unit
def test_fetch_rows_for_ids_empty_input_short_circuits(db: Database) -> None:
    """An empty id list returns an empty list without issuing a query."""
    assert CategorizationMatcher(db).fetch_rows_for_ids([]) == []
