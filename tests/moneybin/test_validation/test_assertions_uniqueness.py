"""Tests for uniqueness assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.uniqueness import assert_no_duplicates


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with parent/child tables."""
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE TABLE parent (id INT)")
    database.execute("INSERT INTO parent VALUES (1), (2), (3)")
    database.execute("CREATE TABLE child (id INT, parent_id INT)")
    return database


def test_no_duplicates_detects_repeats(db: Database) -> None:
    """Duplicate rows in a column set are detected."""
    db.execute("INSERT INTO child VALUES (10, 1), (10, 1)")
    r = assert_no_duplicates(db, table="child", columns=["id"])
    assert not r.passed
    assert r.details["duplicate_groups"] == 1
