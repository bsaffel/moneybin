"""Unit tests for infrastructure assertions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import moneybin.database as database_module
from moneybin.migrations import MigrationRunner
from moneybin.validation.assertions.infrastructure import (
    assert_migrations_at_head,
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)

if TYPE_CHECKING:
    from moneybin.database import Database


def test_no_unencrypted_db_files_passes_for_empty_dir(tmp_path: Path) -> None:
    """An empty directory contains no leaked unencrypted DuckDB files."""
    result = assert_no_unencrypted_db_files(tmpdir=tmp_path)
    assert result.passed
    assert result.details["files"] == []


def test_no_unencrypted_db_files_flags_bare_duckdb(tmp_path: Path) -> None:
    """A bare .duckdb file in the directory must be flagged."""
    (tmp_path / "leak.duckdb").write_bytes(b"\x00" * 16)
    result = assert_no_unencrypted_db_files(tmpdir=tmp_path)
    assert not result.passed
    assert "leak.duckdb" in str(result.details["files"])


def test_no_unencrypted_db_files_finds_nested(tmp_path: Path) -> None:
    """Recursive search must catch leaked files in subdirectories."""
    nested = tmp_path / "sub" / "deeper"
    nested.mkdir(parents=True)
    (nested / "buried.duckdb").write_bytes(b"\x00")
    result = assert_no_unencrypted_db_files(tmpdir=tmp_path)
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


@pytest.mark.integration
def test_sqlmesh_catalog_matches_against_real_db(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SQLMesh's bound adapter path must equal db.path.

    sqlmesh_context() reads the module-level _database_instance singleton —
    monkeypatch it to point at the test fixture's database.
    """
    monkeypatch.setattr(database_module, "_database_instance", db)
    result = assert_sqlmesh_catalog_matches(db)
    assert result.passed, result.details
