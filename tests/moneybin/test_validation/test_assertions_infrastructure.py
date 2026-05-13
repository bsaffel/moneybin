"""Unit tests for infrastructure assertions."""

from __future__ import annotations

from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import Database
from moneybin.migrations import MigrationRunner
from moneybin.validation.assertions.infrastructure import (
    assert_migrations_at_head,
    assert_min_rows,
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)

# `assert_no_unencrypted_db_files` ignores its db argument — it only inspects the
# tmpdir for leaked files — so a mock suffices in unit tests.
_DUMMY_DB = cast("Database", MagicMock())


def test_no_unencrypted_db_files_passes_for_empty_dir(tmp_path: Path) -> None:
    """An empty directory contains no leaked unencrypted DuckDB files."""
    result = assert_no_unencrypted_db_files(_DUMMY_DB, tmpdir=tmp_path)
    assert result.passed
    assert result.details["files"] == []


def _write_plaintext_duckdb(path: Path) -> None:
    """Create a real (unencrypted) DuckDB file.

    The assertion's probe tries to open each ``*.duckdb`` file without a
    key — only a real plaintext file opens successfully, so test fixtures
    must be real DBs.
    """
    with duckdb.connect(str(path)) as conn:
        conn.execute("SELECT 1").fetchone()


def test_no_unencrypted_db_files_flags_bare_duckdb(tmp_path: Path) -> None:
    """A bare .duckdb file in the directory must be flagged."""
    _write_plaintext_duckdb(tmp_path / "leak.duckdb")
    result = assert_no_unencrypted_db_files(_DUMMY_DB, tmpdir=tmp_path)
    assert not result.passed
    assert "leak.duckdb" in str(result.details["files"])


def test_no_unencrypted_db_files_finds_nested(tmp_path: Path) -> None:
    """Recursive search must catch leaked files in subdirectories."""
    nested = tmp_path / "sub" / "deeper"
    nested.mkdir(parents=True)
    _write_plaintext_duckdb(nested / "buried.duckdb")
    result = assert_no_unencrypted_db_files(_DUMMY_DB, tmpdir=tmp_path)
    assert not result.passed
    assert "buried.duckdb" in str(result.details["files"])


def test_migrations_at_head_passes_when_runner_has_no_pending(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No pending migrations means we are at head."""

    def _empty_pending(self: MigrationRunner) -> list[object]:
        return []

    monkeypatch.setattr(MigrationRunner, "pending", _empty_pending)
    result = assert_migrations_at_head(db)
    assert result.passed
    assert result.details["pending_count"] == 0


def test_migrations_at_head_fails_when_pending_exist(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pending migrations on disk mean we're behind head."""

    class _Stub:
        version = 99
        filename = "V099__future.sql"

    def _one_pending(self: MigrationRunner) -> list[object]:
        return [_Stub()]

    monkeypatch.setattr(MigrationRunner, "pending", _one_pending)
    result = assert_migrations_at_head(db)
    assert not result.passed
    assert result.details["pending_count"] == 1
    assert "V099__future.sql" in str(result.details["pending"])


def test_min_rows_passes_when_table_meets_threshold(db: Database) -> None:
    """A table with enough rows passes the min-rows check."""
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute("CREATE TABLE core.t (id INT)")
    db.execute("INSERT INTO core.t VALUES (1), (2), (3)")
    result = assert_min_rows(db, table_min_rows={"core.t": 2})
    assert result.passed
    assert result.details["counts"] == {"core.t": 3}
    assert result.details["failures"] == {}


def test_min_rows_fails_when_table_below_threshold(db: Database) -> None:
    """Tables below the required row count are reported as failures."""
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute("CREATE TABLE core.t (id INT)")
    db.execute("INSERT INTO core.t VALUES (1)")
    result = assert_min_rows(db, table_min_rows={"core.t": 5})
    assert not result.passed
    assert result.details["failures"]["core.t"] == {"min_required": 5, "actual": 1}


def test_min_rows_treats_missing_table_as_zero_rows(db: Database) -> None:
    """A table that does not exist contributes 0 rows rather than erroring."""
    result = assert_min_rows(db, table_min_rows={"core.does_not_exist": 1})
    assert not result.passed
    assert result.details["counts"]["core.does_not_exist"] == 0


@pytest.mark.integration
def test_sqlmesh_catalog_matches_against_real_db(db: Database) -> None:
    """SQLMesh's bound adapter path must equal db.path.

    sqlmesh_context() takes db directly — no singleton needed.
    """
    result = assert_sqlmesh_catalog_matches(db)
    assert result.passed, result.details
