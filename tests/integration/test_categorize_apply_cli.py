"""Integration tests for `moneybin transactions categorize apply` CLI command."""

# ruff: noqa: S101

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.database import Database

pytestmark = pytest.mark.integration

runner = CliRunner()

_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def _make_secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _ENCRYPTION_KEY
    return store


def _seed_one_transaction(db: Database) -> str:
    """Create a minimal fct_transactions table and insert one row; return its id.

    core.fct_transactions is a SQLMesh VIEW in production, but tests don't run
    SQLMesh, so we create a bare table with the columns the service reads.
    """
    txn_id = "csv_apply_cli_test_001"
    db.execute(  # noqa: S608  # test input, not executing SQL
        """
        CREATE TABLE IF NOT EXISTS core.fct_transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            account_id      VARCHAR,
            transaction_date DATE,
            description     VARCHAR,
            memo            VARCHAR,
            amount          DECIMAL(18,2),
            source_type     VARCHAR
        )
        """
    )
    db.execute(
        """
        INSERT OR REPLACE INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, description, amount, source_type)
        VALUES (?, 'acct_1', '2026-01-01', 'STARBUCKS COFFEE', -5.00, 'csv')
        """,
        [txn_id],
    )
    return txn_id


def _make_db(tmp_path: Path) -> tuple[Database, MagicMock]:
    """Create a seeded test database and return (db, secret_store)."""
    store = _make_secret_store()
    db = Database(tmp_path / "test.duckdb", secret_store=store)
    return db, store


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    db: Database,
    store: MagicMock,
    args: list[str],
    **kwargs: object,
) -> object:
    """Invoke the categorize subcommand app with the database singleton pre-wired."""
    monkeypatch.setattr("moneybin.database._database_instance", db)
    monkeypatch.setattr("moneybin.secrets.SecretStore", lambda: store)
    return runner.invoke(app, args, **kwargs)  # type: ignore[call-overload]


class TestCategorizeApplyCLI:
    """Integration tests for the 'moneybin categorize apply' command."""

    def test_file_input_applies_categorizations(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--input <file> applies categories and exits 0 on full success."""
        db, store = _make_db(tmp_path)
        txn_id = _seed_one_transaction(db)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(
            json.dumps([
                {"transaction_id": txn_id, "category": "Food", "subcategory": "Coffee"}
            ])
        )

        result = _invoke(monkeypatch, db, store, ["apply", "--input", str(cats_file)])
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]

    def test_stdin_sentinel_reads_json_from_stdin(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Passing '-' as positional argument reads JSON from stdin."""
        db, store = _make_db(tmp_path)
        txn_id = _seed_one_transaction(db)

        payload = json.dumps([{"transaction_id": txn_id, "category": "Food"}])
        result = _invoke(monkeypatch, db, store, ["apply", "-"], input=payload)
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]

    def test_json_output_returns_envelope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--output json prints a valid response envelope with applied count."""
        db, store = _make_db(tmp_path)
        txn_id = _seed_one_transaction(db)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(
            json.dumps([{"transaction_id": txn_id, "category": "Food"}])
        )

        result = _invoke(
            monkeypatch,
            db,
            store,
            ["apply", "--input", str(cats_file), "--output", "json"],
        )
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert envelope["data"]["applied"] == 1
        assert envelope["data"]["error_details"] == []

    def test_partial_failure_exits_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When some items fail validation the exit code is 1 and errors surface."""
        db, store = _make_db(tmp_path)
        txn_id = _seed_one_transaction(db)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(
            json.dumps([
                {"transaction_id": txn_id, "category": "Food"},
                {"transaction_id": "", "category": "X"},  # empty id — validation error
            ])
        )

        result = _invoke(
            monkeypatch,
            db,
            store,
            ["apply", "--input", str(cats_file), "--output", "json"],
        )
        assert result.exit_code == 1  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert any(
            "transaction_id" in e["reason"] for e in envelope["data"]["error_details"]
        )

    def test_malformed_top_level_exits_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A JSON object (not array) at the top level exits 1."""
        db, store = _make_db(tmp_path)
        _seed_one_transaction(db)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text(json.dumps({"items": []}))  # dict, not list

        result = _invoke(monkeypatch, db, store, ["apply", "--input", str(cats_file)])
        assert result.exit_code == 1  # type: ignore[union-attr]

    def test_missing_file_exits_two(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A path that doesn't exist exits 2."""
        db, store = _make_db(tmp_path)
        _seed_one_transaction(db)

        result = _invoke(
            monkeypatch,
            db,
            store,
            ["apply", "--input", str(tmp_path / "missing.json")],
        )
        assert result.exit_code == 2  # type: ignore[union-attr]

    def test_empty_array_exits_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty JSON array succeeds with applied=0."""
        db, store = _make_db(tmp_path)
        _seed_one_transaction(db)

        cats_file = tmp_path / "cats.json"
        cats_file.write_text("[]")

        result = _invoke(
            monkeypatch,
            db,
            store,
            ["apply", "--input", str(cats_file), "--output", "json"],
        )
        assert result.exit_code == 0  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert envelope["data"]["applied"] == 0
