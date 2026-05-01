"""Tests for referential-integrity assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.integrity import (
    assert_no_orphans,
    assert_valid_foreign_keys,
)


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


def test_valid_foreign_keys_passes_when_all_children_resolve(db: Database) -> None:
    """All child rows reference existing parent rows."""
    db.execute("INSERT INTO child VALUES (10, 1), (11, 2)")
    r = assert_valid_foreign_keys(
        db, child="child", column="parent_id", parent="parent", parent_column="id"
    )
    assert r.passed
    assert r.details == {"checked_rows": 2, "violations": 0}


def test_valid_foreign_keys_fails_with_violation_count(db: Database) -> None:
    """Child row with missing parent is counted as a violation."""
    db.execute("INSERT INTO child VALUES (10, 1), (11, 99)")
    r = assert_valid_foreign_keys(
        db, child="child", column="parent_id", parent="parent", parent_column="id"
    )
    assert not r.passed
    assert r.details["violations"] == 1


def test_no_orphans_passes_when_every_parent_has_child(db: Database) -> None:
    """Every parent row has at least one matching child row."""
    db.execute("INSERT INTO child VALUES (1, 1), (2, 2), (3, 3)")
    r = assert_no_orphans(
        db, parent="parent", parent_column="id", child="child", child_column="parent_id"
    )
    assert r.passed


def test_no_orphans_fails_when_parent_has_no_child(db: Database) -> None:
    """Parent rows with no matching child rows are counted as orphans."""
    r = assert_no_orphans(
        db, parent="parent", parent_column="id", child="child", child_column="parent_id"
    )
    assert not r.passed
    assert r.details["orphan_count"] == 3
