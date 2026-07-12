"""Integration tests for `moneybin transactions categorize assist` CLI command."""

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


def _make_db_with_uncategorized(tmp_path: Path) -> tuple[Database, MagicMock]:
    """Test DB with two uncategorized transactions, no categorizations recorded."""
    store = _make_secret_store()
    db = Database(
        tmp_path / "test.duckdb",
        secret_store=store,
        no_auto_upgrade=True,
        read_only=False,
    )
    db.execute(  # noqa: S608  # test input, not executing SQL
        """
        CREATE TABLE IF NOT EXISTS core.fct_transactions (
            transaction_id   VARCHAR PRIMARY KEY,
            account_id       VARCHAR,
            transaction_date DATE,
            description      VARCHAR,
            memo             VARCHAR,
            amount           DECIMAL(18,2),
            source_type      VARCHAR,
            transaction_type VARCHAR,
            check_number     VARCHAR,
            is_transfer      BOOLEAN,
            transfer_pair_id VARCHAR,
            payment_channel  VARCHAR
        )
        """
    )
    # app.transaction_categories is the LEFT JOIN target in AssistBridge
    db.execute(  # noqa: S608  # test input, not executing SQL
        """
        CREATE TABLE IF NOT EXISTS app.transaction_categories (
            transaction_id  VARCHAR PRIMARY KEY,
            category        VARCHAR,
            subcategory     VARCHAR,
            categorized_by  VARCHAR
        )
        """
    )
    db.execute(
        """
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, description, amount,
         source_type, is_transfer)
        VALUES
        ('txn-assist-1', 'acct_a', '2026-01-15', 'STARBUCKS COFFEE', -5.00, 'csv', false),
        ('txn-assist-2', 'acct_b', '2026-02-01', 'CHASE DEPOSIT', 100.00, 'csv', false)
        """
    )
    return db, store


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    db: Database,
    store: MagicMock,
    args: list[str],
) -> object:
    import moneybin.cli.commands.transactions.categorize as _categorize_mod

    @contextmanager
    def _db_ctx(*_a: object, **_kw: object):
        yield db

    monkeypatch.setattr(_categorize_mod, "get_database", _db_ctx)
    monkeypatch.setattr("moneybin.secrets.SecretStore", lambda: store)
    return runner.invoke(app, args)  # type: ignore[call-overload]


class TestCategorizeAssistCLI:
    """Integration tests for the 'moneybin transactions categorize assist' command."""

    def test_returns_redacted_rows_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """JSON output has redacted fields, no amount/date/account."""
        db, store = _make_db_with_uncategorized(tmp_path)
        result = _invoke(
            monkeypatch, db, store, ["assist", "--limit", "10", "--output", "json"]
        )
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        # CatAssistPayload serializes to {"transactions": [...]}
        rows = envelope["data"]["transactions"]
        assert len(rows) == 2
        first = rows[0]
        # Privacy contract: scrubbed fields present, raw fields absent.
        assert "description_scrubbed" in first
        assert "memo_scrubbed" in first
        assert "amount" not in first
        assert "transaction_date" not in first
        assert "account_id" not in first
        # Structural signal preserved.
        assert "amount_sign" in first
        assert first["amount_sign"] in {"+", "-", "0"}

    def test_account_filter_scopes_results(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--account-filter restricts results to specified accounts."""
        db, store = _make_db_with_uncategorized(tmp_path)
        result = _invoke(
            monkeypatch,
            db,
            store,
            ["assist", "--account-filter", "acct_a", "--output", "json"],
        )
        assert result.exit_code == 0, result.stderr  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        rows = envelope["data"]["transactions"]
        assert len(rows) == 1
        assert rows[0]["transaction_id"] == "txn-assist-1"

    def test_invalid_date_range_exits_two(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--date-range without comma exits with usage error (code 2)."""
        db, store = _make_db_with_uncategorized(tmp_path)
        result = _invoke(
            monkeypatch, db, store, ["assist", "--date-range", "2026-01-01"]
        )
        assert result.exit_code == 2  # type: ignore[union-attr]
