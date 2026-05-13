"""Round-trip integration tests: export-uncategorized → apply-from-file.

Tests the full CLI bridge workflow:
  export-uncategorized → simulate LLM decisions → apply-from-file
"""

# ruff: noqa: S101

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app as categorize_app
from moneybin.cli.main import app as main_app
from moneybin.database import Database

pytestmark = pytest.mark.integration

runner = CliRunner()

_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def _make_secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _ENCRYPTION_KEY
    return store


def _make_db_with_uncategorized(
    tmp_path: Path, count: int = 3
) -> tuple[Database, MagicMock]:
    """Create a seeded test database with uncategorized transactions."""
    store = _make_secret_store()
    db = Database(tmp_path / "test.duckdb", secret_store=store)

    db.execute(  # noqa: S608  # test input, not executing SQL
        """
        CREATE TABLE IF NOT EXISTS core.fct_transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            account_id      VARCHAR,
            transaction_date DATE,
            description     VARCHAR,
            memo            VARCHAR,
            amount          DECIMAL(18,2),
            source_type     VARCHAR,
            transaction_type VARCHAR,
            check_number    VARCHAR,
            payment_channel VARCHAR,
            is_transfer     BOOLEAN,
            transfer_pair_id VARCHAR
        )
        """
    )
    db.execute(  # noqa: S608  # test input, not executing SQL
        """
        CREATE TABLE IF NOT EXISTS app.transaction_categories (
            transaction_id  VARCHAR PRIMARY KEY,
            category        VARCHAR,
            subcategory     VARCHAR,
            categorized_at  TIMESTAMP,
            categorized_by  VARCHAR,
            merchant_id     VARCHAR
        )
        """
    )
    for i in range(count):
        db.execute(
            """
            INSERT OR REPLACE INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, description, amount, source_type)
            VALUES (?, 'acct_test', '2026-01-01', ?, -10.00, 'csv')
            """,
            [f"txn_export_{i:03d}", f"STARBUCKS COFFEE {i}"],
        )

    return db, store


def _invoke_categorize(
    monkeypatch: pytest.MonkeyPatch,
    db: Database,
    store: MagicMock,
    args: list[str],
    **kwargs: object,
) -> object:
    """Invoke the categorize subcommand app with the database pre-wired."""
    import moneybin.cli.commands.transactions.categorize as _categorize_mod
    import moneybin.cli.commands.transactions.categorize.apply_from_file as _apply_mod
    import moneybin.cli.commands.transactions.categorize.export as _export_mod

    @contextmanager
    def _db_ctx(*_a: object, **_kw: object):
        yield db  # noqa: B023 — db is loop-invariant; test owns lifecycle

    monkeypatch.setattr(_categorize_mod, "get_database", _db_ctx)
    monkeypatch.setattr(_apply_mod, "get_database", _db_ctx)
    monkeypatch.setattr(_export_mod, "get_database", _db_ctx)
    monkeypatch.setattr("moneybin.secrets.SecretStore", lambda: store)
    return runner.invoke(categorize_app, args, **kwargs)  # type: ignore[call-overload]


class TestExportApplyRoundTrip:
    """Export uncategorized → simulate LLM → apply-from-file round trip."""

    def test_export_apply_round_trip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Export uncategorized txns, simulate LLM decisions, apply; verify applied=3."""
        db, store = _make_db_with_uncategorized(tmp_path, count=3)

        # Step 1: export uncategorized to a file
        export_file = tmp_path / "uncategorized.json"
        result = _invoke_categorize(
            monkeypatch,
            db,
            store,
            ["export-uncategorized", "--output", str(export_file)],
        )
        assert result.exit_code == 0, result.output  # type: ignore[union-attr]
        assert export_file.exists()

        exported = json.loads(export_file.read_text())
        assert len(exported) == 3
        # Verify redacted shape: transaction_id, description_redacted, source_type present
        for item in exported:
            assert "transaction_id" in item
            assert "description_redacted" in item
            assert "source_type" in item
            # No PII fields
            assert "amount" not in item
            assert "account_id" not in item

        # Step 2: simulate LLM — add category/subcategory to each item
        for item in exported:
            item["category"] = "Food"
            item["subcategory"] = "Coffee"
        apply_file = tmp_path / "categorized.json"
        apply_file.write_text(json.dumps(exported))

        # Step 3: apply from file
        result = _invoke_categorize(
            monkeypatch,
            db,
            store,
            ["apply-from-file", str(apply_file), "--output", "json"],
        )
        assert result.exit_code == 0, result.output  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert envelope["data"]["applied"] == 3
        assert envelope["data"]["errors"] == 0

    def test_apply_from_stdin(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pipe JSON through stdin using '-' sentinel applies categorizations."""
        db, store = _make_db_with_uncategorized(tmp_path, count=2)

        payload = json.dumps([
            {
                "transaction_id": "txn_export_000",
                "description_redacted": "STARBUCKS",
                "source_type": "csv",
                "category": "Food",
                "subcategory": "Coffee",
            },
            {
                "transaction_id": "txn_export_001",
                "description_redacted": "STARBUCKS",
                "source_type": "csv",
                "category": "Food",
            },
        ])

        result = _invoke_categorize(
            monkeypatch,
            db,
            store,
            ["apply-from-file", "-", "--output", "json"],
            input=payload,
        )
        assert result.exit_code == 0, result.output  # type: ignore[union-attr]
        envelope = json.loads(result.output)  # type: ignore[union-attr]
        assert envelope["data"]["applied"] == 2

    def test_export_empty_when_all_categorized(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Export returns empty JSON array when all transactions are categorized."""
        db, store = _make_db_with_uncategorized(tmp_path, count=2)

        # Mark all as categorized
        db.execute(
            """
            INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_at, categorized_by)
            VALUES ('txn_export_000', 'Food', 'Coffee', CURRENT_TIMESTAMP, 'user'),
                   ('txn_export_001', 'Food', 'Coffee', CURRENT_TIMESTAMP, 'user')
            """
        )

        result = _invoke_categorize(
            monkeypatch,
            db,
            store,
            ["export-uncategorized"],
        )
        assert result.exit_code == 0, result.output  # type: ignore[union-attr]
        exported = json.loads(result.output)  # type: ignore[union-attr]
        assert exported == []


class TestPrivacyRedact:
    """Tests for `moneybin privacy redact`."""

    def test_redact_strips_p2p_recipient(self) -> None:
        """VENMO PAYMENT TO <name> strips the name."""
        result = runner.invoke(
            main_app, ["privacy", "redact", "VENMO PAYMENT TO J SMITH"]
        )
        assert result.exit_code == 0, result.output
        assert "VENMO PAYMENT TO" in result.output
        assert "J SMITH" not in result.output

    def test_redact_strips_store_number(self) -> None:
        """Store numbers like #1234 are removed."""
        result = runner.invoke(main_app, ["privacy", "redact", "STARBUCKS #1234"])
        assert result.exit_code == 0, result.output
        assert "STARBUCKS" in result.output
        assert "#1234" not in result.output

    def test_redact_stdin(self) -> None:
        """Passing '-' reads from stdin."""
        result = runner.invoke(
            main_app, ["privacy", "redact", "-"], input="STARBUCKS #9876 SEATTLE WA\n"
        )
        assert result.exit_code == 0, result.output
        assert "STARBUCKS" in result.output
        assert "#9876" not in result.output
