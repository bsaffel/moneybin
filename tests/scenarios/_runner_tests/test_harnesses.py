"""Tests for pipeline-execution harness primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from tests.scenarios._harnesses import (
    assert_empty_input_safe,
    assert_idempotent,
    assert_incremental_safe,
    assert_malformed_input_rejected,
    assert_subprocess_parity,
)


@pytest.fixture()
def mock_secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = "test-encryption-key-for-unit-tests"
    return store


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    return Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )


def test_idempotent_passes_when_counts_unchanged(db: Database) -> None:
    db.execute("CREATE TABLE t (id INT)")
    db.execute("INSERT INTO t VALUES (1), (2), (3)")
    r = assert_idempotent(
        db,
        tables=["t"],
        rerun=lambda: None,  # noop rerun — counts won't change
    )
    assert r.passed
    assert r.details["before"] == r.details["after"]


def test_idempotent_fails_when_rerun_adds_rows(db: Database) -> None:
    db.execute("CREATE TABLE t (id INT)")
    db.execute("INSERT INTO t VALUES (1)")

    def add_row() -> None:
        db.execute("INSERT INTO t VALUES (2)")

    r = assert_idempotent(db, tables=["t"], rerun=add_row)
    assert not r.passed


def test_empty_input_safe_passes_when_no_crash_and_tables_empty(
    db: Database,
) -> None:
    db.execute("CREATE TABLE t (id INT)")
    r = assert_empty_input_safe(
        db,
        run=lambda: None,  # noop — represents a run on empty input
        tables=["t"],
    )
    assert r.passed
    assert r.details["row_counts"]["t"] == 0


def test_malformed_input_rejected_passes_on_expected_exception() -> None:
    def bad_run() -> None:
        raise ValueError("missing required column 'amount'")

    r = assert_malformed_input_rejected(
        run=bad_run, expected_message_substring="missing required column"
    )
    assert r.passed


def test_malformed_input_rejected_fails_when_no_exception_raised() -> None:
    r = assert_malformed_input_rejected(
        run=lambda: None, expected_message_substring="anything"
    )
    assert not r.passed
    assert "no exception" in r.details["reason"].lower()


def test_malformed_input_rejected_fails_on_wrong_message() -> None:
    def bad_run() -> None:
        raise ValueError("disk full")

    r = assert_malformed_input_rejected(
        run=bad_run, expected_message_substring="missing column"
    )
    assert not r.passed


def test_incremental_safe_passes_when_counts_match_expected(db: Database) -> None:
    """Load A → 2 rows; load B → 3 rows total (1 net-new). Both expectations met."""
    db.execute("CREATE TABLE t (id INT)")

    def load_a() -> None:
        db.execute("INSERT INTO t VALUES (1), (2)")

    def load_b() -> None:
        db.execute("INSERT INTO t VALUES (3)")

    r = assert_incremental_safe(
        db,
        tables=["t"],
        load_a=load_a,
        load_b=load_b,
        expected_a_count={"t": 2},
        expected_b_count={"t": 3},
    )
    assert r.passed, r.details


def test_incremental_safe_fails_when_load_b_exceeds_expected(db: Database) -> None:
    db.execute("CREATE TABLE t (id INT)")

    def load_a() -> None:
        db.execute("INSERT INTO t VALUES (1)")

    def load_b() -> None:
        db.execute("INSERT INTO t VALUES (2), (3), (4)")

    r = assert_incremental_safe(
        db,
        tables=["t"],
        load_a=load_a,
        load_b=load_b,
        expected_a_count={"t": 1},
        expected_b_count={"t": 2},  # expect only 1 net-new, got 3
    )
    assert not r.passed
    assert any("after-B" in f for f in r.details["failures"])


def test_incremental_safe_reports_missing_expected_key_without_keyerror(
    db: Database,
) -> None:
    """Missing key in expected_*_count must yield a failure result, not raise."""
    db.execute("CREATE TABLE t (id INT)")
    db.execute("INSERT INTO t VALUES (1)")

    r = assert_incremental_safe(
        db,
        tables=["t"],
        load_a=lambda: None,
        load_b=lambda: None,
        expected_a_count={},  # 't' missing
        expected_b_count={},
    )
    assert not r.passed
    assert any("None" in f for f in r.details["failures"])


def test_subprocess_parity_passes_when_outputs_match() -> None:
    r = assert_subprocess_parity(
        in_process_outputs={"raw.t": 10, "core.t": 8},
        subprocess_outputs={"raw.t": 10, "core.t": 8},
    )
    assert r.passed
    assert r.details["diff"] == {}


def test_subprocess_parity_fails_when_counts_diverge() -> None:
    r = assert_subprocess_parity(
        in_process_outputs={"raw.t": 10},
        subprocess_outputs={"raw.t": 9},
    )
    assert not r.passed
    assert r.details["diff"]["raw.t"] == {"in_process": 10, "subprocess": 9}
