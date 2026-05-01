"""Tests for validation.expectations.transactions predicates."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.expectations import SourceTransactionRef
from moneybin.validation.expectations.transactions import (
    verify_category_for_transaction,
    verify_gold_record_count,
    verify_provenance_for_transaction,
)
from moneybin.validation.result import ExpectationResult


def _make_db(tmp_path: Path, mock_secret_store: MagicMock, name: str) -> Database:
    # init_schemas creates app.* tables; core/meta schemas are present but empty.
    return Database(
        tmp_path / name, secret_store=mock_secret_store, no_auto_upgrade=True
    )


@pytest.fixture()
def populated_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """DB with two gold rows and provenance linking source IDs to each."""
    db = _make_db(tmp_path, mock_secret_store, "populated.duckdb")
    db.execute(
        "CREATE TABLE IF NOT EXISTS core.fct_transactions ("
        "  transaction_id VARCHAR PRIMARY KEY,"
        "  category VARCHAR,"
        "  categorized_by VARCHAR"
        ")"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS meta.fct_transaction_provenance ("
        "  source_transaction_id VARCHAR,"
        "  source_type VARCHAR,"
        "  transaction_id VARCHAR,"
        "  match_id VARCHAR"
        ")"
    )
    db.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('txn_abc', 'Groceries', 'rule'),"
        "('txn_def', 'Dining', 'ai')"
    )
    db.execute(
        "INSERT INTO meta.fct_transaction_provenance VALUES "
        "('csv_a', 'csv', 'txn_abc', NULL),"
        "('csv_b', 'csv', 'txn_def', NULL)"
    )
    return db


@pytest.fixture()
def categorized_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """DB with a single categorized transaction for category predicate tests."""
    db = _make_db(tmp_path, mock_secret_store, "categorized.duckdb")
    db.execute(
        "CREATE TABLE IF NOT EXISTS core.fct_transactions ("
        "  transaction_id VARCHAR PRIMARY KEY,"
        "  category VARCHAR,"
        "  categorized_by VARCHAR"
        ")"
    )
    db.execute(
        "INSERT INTO core.fct_transactions VALUES ('txn_abc', 'Groceries', 'rule')"
    )
    return db


def test_verify_gold_record_count_total(populated_db: Database) -> None:
    """Total gold count matches the number of rows in fct_transactions."""
    result = verify_gold_record_count(populated_db, expected_collapsed_count=2)
    assert isinstance(result, ExpectationResult)
    assert result.kind == "gold_record_count"
    assert result.passed is True


def test_verify_gold_record_count_total_wrong_expected(populated_db: Database) -> None:
    """Passing the wrong expected count produces passed=False."""
    result = verify_gold_record_count(populated_db, expected_collapsed_count=99)
    assert result.kind == "gold_record_count"
    assert result.passed is False


def test_verify_gold_record_count_scoped_by_fixture_ids(populated_db: Database) -> None:
    """Scoped count via fixture_source_ids counts distinct gold rows for those sources."""
    # csv_a maps to txn_abc only → expected_collapsed_count=1
    result = verify_gold_record_count(
        populated_db,
        expected_collapsed_count=1,
        fixture_source_ids=["csv_a"],
    )
    assert result.kind == "gold_record_count"
    assert result.passed is True


def test_verify_category_for_transaction_passes(categorized_db: Database) -> None:
    """Category and categorized_by both match → passed=True."""
    result = verify_category_for_transaction(
        categorized_db,
        transaction_id="txn_abc",
        expected_category="Groceries",
        expected_categorized_by="rule",
    )
    assert isinstance(result, ExpectationResult)
    assert result.kind == "category_for_transaction"
    assert result.passed is True


def test_verify_category_for_transaction_wrong_category(
    categorized_db: Database,
) -> None:
    """Wrong expected category → passed=False."""
    result = verify_category_for_transaction(
        categorized_db,
        transaction_id="txn_abc",
        expected_category="Dining",
    )
    assert result.kind == "category_for_transaction"
    assert result.passed is False


def test_verify_category_for_transaction_missing(categorized_db: Database) -> None:
    """Transaction not found → passed=False with reason detail."""
    result = verify_category_for_transaction(
        categorized_db,
        transaction_id="txn_missing",
        expected_category="Groceries",
    )
    assert result.kind == "category_for_transaction"
    assert result.passed is False
    assert result.details.get("reason") == "transaction not found"


def test_verify_provenance_for_transaction(populated_db: Database) -> None:
    """Provenance source rows match expected list → passed=True."""
    result = verify_provenance_for_transaction(
        populated_db,
        transaction_id="txn_abc",
        expected_sources=[
            SourceTransactionRef(source_transaction_id="csv_a", source_type="csv"),
        ],
    )
    assert isinstance(result, ExpectationResult)
    assert result.kind == "provenance_for_transaction"
    assert result.passed is True


def test_verify_provenance_for_transaction_mismatch(populated_db: Database) -> None:
    """Wrong expected provenance → passed=False."""
    result = verify_provenance_for_transaction(
        populated_db,
        transaction_id="txn_abc",
        expected_sources=[
            SourceTransactionRef(source_transaction_id="csv_x", source_type="csv"),
        ],
    )
    assert result.kind == "provenance_for_transaction"
    assert result.passed is False
