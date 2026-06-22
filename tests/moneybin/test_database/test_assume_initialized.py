"""Tests for Database(assume_initialized=True): the test-fixture skip-init path."""

import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.schema as schema_mod
from moneybin.database import Database


def _store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = "test-encryption-key-for-unit-tests"
    return store


def _build_template(path: Path) -> None:
    """A real, fully-initialized DB closed so it is safe to copy."""
    Database(path, secret_store=_store(), no_auto_upgrade=True, read_only=False).close()


def test_assume_initialized_skips_init_schemas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template = tmp_path / "template.duckdb"
    _build_template(template)
    copy = tmp_path / "copy.duckdb"
    shutil.copy(template, copy)
    copy.chmod(0o600)

    calls: list[int] = []

    def _noop_init_schemas(*_args: object, **_kwargs: object) -> None:
        calls.append(1)

    monkeypatch.setattr(schema_mod, "init_schemas", _noop_init_schemas)

    db = Database(
        copy,
        secret_store=_store(),
        no_auto_upgrade=True,
        assume_initialized=True,
        read_only=False,
    )
    try:
        assert calls == []  # init_schemas was NOT called
        # ...and the DB is a usable read-write current-schema DB:
        db.execute(
            "INSERT INTO app.transaction_notes (note_id, transaction_id, text, author)"
            " VALUES (?, ?, ?, ?)",
            ["n1", "t1", "x", "cli"],
        )
        row = db.execute(
            "SELECT text FROM app.transaction_notes WHERE transaction_id = ?",
            ["t1"],
        ).fetchone()
        assert row is not None and row[0] == "x"
    finally:
        db.close()


def test_assume_initialized_rejects_read_only(tmp_path: Path) -> None:
    template = tmp_path / "template.duckdb"
    _build_template(template)
    with pytest.raises(ValueError, match="incompatible with read_only"):
        Database(
            template,
            secret_store=_store(),
            assume_initialized=True,
            read_only=True,
        )


def test_assume_initialized_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires an existing"):
        Database(
            tmp_path / "does_not_exist.duckdb",
            secret_store=_store(),
            no_auto_upgrade=True,
            assume_initialized=True,
            read_only=False,
        )


def _catalog(db: Database) -> dict[str, object]:
    """Full schema fingerprint: tables, columns (+types), views, and comments."""
    tables = db.execute(
        "SELECT schema_name, table_name, comment FROM duckdb_tables() "
        "ORDER BY schema_name, table_name"
    ).fetchall()
    columns = db.execute(
        "SELECT schema_name, table_name, column_name, data_type, comment "
        "FROM duckdb_columns() ORDER BY schema_name, table_name, column_name"
    ).fetchall()
    views = db.execute(
        "SELECT schema_name, view_name FROM duckdb_views() "
        "ORDER BY schema_name, view_name"
    ).fetchall()
    return {"tables": tables, "columns": columns, "views": views}


def test_template_copy_matches_fresh_build(tmp_path: Path) -> None:
    # A fresh, real init build:
    fresh_path = tmp_path / "fresh.duckdb"
    fresh = Database(
        fresh_path, secret_store=_store(), no_auto_upgrade=True, read_only=False
    )

    # The template-copy path the fast `db` fixture uses:
    template = tmp_path / "template.duckdb"
    _build_template(template)
    copy = tmp_path / "copy.duckdb"
    shutil.copy(template, copy)
    copy.chmod(0o600)
    copied = Database(
        copy,
        secret_store=_store(),
        no_auto_upgrade=True,
        assume_initialized=True,
        read_only=False,
    )
    try:
        assert _catalog(copied) == _catalog(fresh)
    finally:
        copied.close()
        fresh.close()
