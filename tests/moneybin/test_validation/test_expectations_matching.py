"""Tests for validation.expectations.matching predicates."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.expectations import SourceTransactionRef
from moneybin.validation.expectations.matching import (
    verify_match_decision,
    verify_transfers_match_ground_truth,
)
from moneybin.validation.result import ExpectationResult


def _make_db(tmp_path: Path, mock_secret_store: MagicMock, name: str) -> Database:
    # init_schemas runs automatically and creates app.* tables; meta/core/prep
    # schemas are created but have no tables yet — we add them below.
    return Database(
        tmp_path / name, secret_store=mock_secret_store, no_auto_upgrade=True
    )


@pytest.fixture()
def matched_dedup_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """DB where csv_a and ofx_b both point to the same gold row (dedup match)."""
    db = _make_db(tmp_path, mock_secret_store, "matched.duckdb")
    # meta.fct_transaction_provenance is a SQLMesh view in production; create
    # a minimal table here since SQLMesh transforms are skipped in unit tests.
    db.execute(
        "CREATE TABLE IF NOT EXISTS meta.fct_transaction_provenance ("
        "  source_transaction_id VARCHAR,"
        "  source_type VARCHAR,"
        "  transaction_id VARCHAR,"
        "  match_id VARCHAR"
        ")"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS core.fct_transactions ("
        "  transaction_id VARCHAR PRIMARY KEY,"
        "  match_confidence DOUBLE"
        ")"
    )
    # app.match_decisions is created by init_schemas with its full schema;
    # INSERT must supply all NOT NULL columns.
    db.execute(
        "INSERT INTO app.match_decisions "
        "(match_id, source_transaction_id_a, source_type_a, source_origin_a,"
        " source_transaction_id_b, source_type_b, source_origin_b, account_id,"
        " match_type, match_status, decided_by, decided_at) VALUES "
        "('m1', 'csv_a', 'csv', 'bank', 'ofx_b', 'ofx', 'bank', 'acct1',"
        " 'dedup', 'accepted', 'auto', '2024-01-01 00:00:00')"
    )
    db.execute(
        "INSERT INTO meta.fct_transaction_provenance VALUES "
        "('csv_a', 'csv', 'gold_1', 'm1'),"
        "('ofx_b', 'ofx', 'gold_1', 'm1')"
    )
    db.execute("INSERT INTO core.fct_transactions VALUES ('gold_1', 0.9)")
    return db


@pytest.fixture()
def not_matched_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """DB where csv_x and ofx_y each map to a distinct gold row (not matched)."""
    db = _make_db(tmp_path, mock_secret_store, "not_matched.duckdb")
    db.execute(
        "CREATE TABLE IF NOT EXISTS meta.fct_transaction_provenance ("
        "  source_transaction_id VARCHAR,"
        "  source_type VARCHAR,"
        "  transaction_id VARCHAR,"
        "  match_id VARCHAR"
        ")"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS core.fct_transactions ("
        "  transaction_id VARCHAR PRIMARY KEY,"
        "  match_confidence DOUBLE"
        ")"
    )
    db.execute(
        "INSERT INTO meta.fct_transaction_provenance VALUES "
        "('csv_x', 'csv', 'gold_x', NULL),"
        "('ofx_y', 'ofx', 'gold_y', NULL)"
    )
    db.execute(
        "INSERT INTO core.fct_transactions VALUES ('gold_x', 0.0), ('gold_y', 0.0)"
    )
    return db


@pytest.fixture()
def transfer_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """DB with a labeled transfer pair that maps to a single transfer_pair_id."""
    db = _make_db(tmp_path, mock_secret_store, "transfer.duckdb")
    # synthetic schema is "on demand" — not created by init_schemas.
    # prep schema belongs to SQLMesh — also not in init_schemas.
    # Create both schemas and their minimal stub tables here.
    db.execute("CREATE SCHEMA IF NOT EXISTS synthetic")
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE IF NOT EXISTS synthetic.ground_truth ("
        "  source_transaction_id VARCHAR NOT NULL PRIMARY KEY,"
        "  account_id VARCHAR NOT NULL,"
        "  transfer_pair_id VARCHAR,"
        "  persona VARCHAR NOT NULL,"
        "  seed INTEGER NOT NULL,"
        "  generated_at TIMESTAMP NOT NULL"
        ")"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS prep.int_transactions__matched ("
        "  source_transaction_id VARCHAR,"
        "  transaction_id VARCHAR"
        ")"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS core.fct_transactions ("
        "  transaction_id VARCHAR PRIMARY KEY,"
        "  transfer_pair_id VARCHAR,"
        "  match_confidence DOUBLE"
        ")"
    )
    # synthetic.ground_truth requires all NOT NULL columns (see synthetic_ground_truth.sql).
    db.execute(
        "INSERT INTO synthetic.ground_truth "
        "(source_transaction_id, account_id, transfer_pair_id, persona, seed, generated_at)"
        " VALUES "
        "('src_from', 'acct1', 'tp_1', 'test', 42, '2024-01-01 00:00:00'),"
        "('src_to', 'acct2', 'tp_1', 'test', 42, '2024-01-01 00:00:00')"
    )
    db.execute(
        "INSERT INTO prep.int_transactions__matched VALUES "
        "('src_from', 'txn_from'), ('src_to', 'txn_to')"
    )
    db.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('txn_from', 'tp_1', NULL), ('txn_to', 'tp_1', NULL)"
    )
    return db


def test_verify_match_decision_returns_expectation_result(
    matched_dedup_db: Database,
) -> None:
    """All listed sources collapse to one gold row → passed=True."""
    result = verify_match_decision(
        matched_dedup_db,
        transactions=[
            SourceTransactionRef(source_transaction_id="csv_a", source_type="csv"),
            SourceTransactionRef(source_transaction_id="ofx_b", source_type="ofx"),
        ],
        expected="matched",
        expected_match_type="dedup",
        expected_confidence_min=0.5,
    )
    assert isinstance(result, ExpectationResult)
    assert result.kind == "match_decision"
    assert result.passed is True


def test_verify_match_decision_not_matched_passes_when_distinct(
    not_matched_db: Database,
) -> None:
    """Distinct gold rows → expected="not_matched" passes."""
    result = verify_match_decision(
        not_matched_db,
        transactions=[
            SourceTransactionRef(source_transaction_id="csv_x", source_type="csv"),
            SourceTransactionRef(source_transaction_id="ofx_y", source_type="ofx"),
        ],
        expected="not_matched",
    )
    assert result.passed is True


def test_verify_transfers_match_ground_truth_returns_expectation_result(
    transfer_db: Database,
) -> None:
    """Labeled transfer pairs that match a single transfer_pair_id pass."""
    result = verify_transfers_match_ground_truth(transfer_db)
    assert isinstance(result, ExpectationResult)
    assert result.kind == "transfers_match_ground_truth"
    assert result.passed is True
