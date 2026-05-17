"""Integration tests for `moneybin transactions categorize run` CLI command."""

# ruff: noqa: S101

from __future__ import annotations

import json
from contextlib import contextmanager
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


def _make_db(tmp_path: Path) -> tuple[Database, MagicMock]:
    """Create a test database.

    core.fct_transactions is a SQLMesh VIEW in production but tests create
    the bare table the service reads from.
    """
    store = _make_secret_store()
    db = Database(tmp_path / "test.duckdb", secret_store=store)
    db.execute(
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
    return db, store


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    db: Database,
    store: MagicMock,
    args: list[str],
    **kwargs: object,
) -> object:
    import moneybin.cli.commands.transactions.categorize as _categorize_mod

    @contextmanager
    def _db_ctx(*_a: object, **_kw: object):
        yield db

    monkeypatch.setattr(_categorize_mod, "get_database", _db_ctx)
    monkeypatch.setattr("moneybin.secrets.SecretStore", lambda: store)
    return runner.invoke(app, args, **kwargs)  # type: ignore[call-overload]


class TestCategorizeRunCLI:
    """Integration tests for the 'moneybin transactions categorize run' command."""

    def test_run_default_methods_returns_json_envelope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Default methods returns JSON envelope with applied_by_method breakdown."""
        db, store = _make_db(tmp_path)
        result = _invoke(monkeypatch, db, store, ["run", "--output", "json"])
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert "applied_by_method" in envelope["data"]
        assert "rules" in envelope["data"]["applied_by_method"]
        assert "merchants" in envelope["data"]["applied_by_method"]
        assert envelope["data"]["total_applied"] == sum(
            envelope["data"]["applied_by_method"].values()
        )

    def test_run_rules_only(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--methods rules runs only the rules engine."""
        db, store = _make_db(tmp_path)
        result = _invoke(
            monkeypatch, db, store, ["run", "--methods", "rules", "--output", "json"]
        )
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert "rules" in envelope["data"]["applied_by_method"]
        assert "merchants" not in envelope["data"]["applied_by_method"]

    def test_run_unknown_method_exits_two(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unknown method name exits with code 2 (usage error)."""
        db, store = _make_db(tmp_path)
        result = _invoke(monkeypatch, db, store, ["run", "--methods", "bogus"])
        assert result.exit_code == 2  # type: ignore[union-attr]
