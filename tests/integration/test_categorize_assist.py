"""Integration tests for transactions_categorize_assist MCP tool."""

# ruff: noqa: S101

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.mcp.tools.transactions_categorize_assist import (
    transactions_categorize_assist,
)

pytestmark = pytest.mark.integration

_ENCRYPTION_KEY = "integration-test-key-0123456789abcdef"


def _make_secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _ENCRYPTION_KEY
    return store


def _make_db(tmp_path: Path) -> tuple[Database, MagicMock]:
    store = _make_secret_store()
    db = Database(tmp_path / "test.duckdb", secret_store=store)
    return db, store


def _seed_uncategorized_transactions(db: Database, count: int = 3) -> None:
    """Create core.fct_transactions with uncategorized rows.

    core.fct_transactions is a SQLMesh VIEW in production, but tests don't run
    SQLMesh, so we create a bare table with the columns the service reads.
    """
    db.execute(  # noqa: S608  # test input, not executing SQL
        """
        CREATE TABLE IF NOT EXISTS core.fct_transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            account_id      VARCHAR,
            transaction_date DATE,
            description     VARCHAR,
            amount          DECIMAL(18,2),
            source_type     VARCHAR
        )
        """
    )
    # app.transaction_categories is the LEFT JOIN target; ensure it exists empty
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
    for i in range(count):
        db.execute(
            """
            INSERT OR REPLACE INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, description, amount, source_type)
            VALUES (?, 'acct_test', '2026-01-01', ?, -10.00, 'csv')
            """,
            [f"txn_assist_{i:03d}", f"MERCHANT STORE {i}"],
        )


class TestCategorizeAssistMCPTool:
    """Integration tests for the transactions_categorize_assist MCP tool."""

    async def test_returns_envelope_with_redacted_descriptions(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Tool returns envelope; descriptions pass through redact_for_llm."""
        db, _store = _make_db(tmp_path)
        _seed_uncategorized_transactions(db, count=3)
        monkeypatch.setattr("moneybin.database._database_instance", db)

        response = await transactions_categorize_assist(limit=10)

        assert response.summary.sensitivity == "medium"
        assert isinstance(response.data, list)
        for item in response.data:
            assert "opaque_id" in item
            assert "description_redacted" in item
            assert "source_type" in item
            assert "redaction_version" in item
            # Confirm no amount/date/account fields leaked
            assert "amount" not in item
            assert "date" not in item
            assert "account_id" not in item

    async def test_action_hints_point_at_apply_tool(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Response actions reference transactions_categorize_apply for commit."""
        db, _store = _make_db(tmp_path)
        _seed_uncategorized_transactions(db, count=2)
        monkeypatch.setattr("moneybin.database._database_instance", db)

        response = await transactions_categorize_assist(limit=10)

        assert any("transactions_categorize_apply" in a for a in response.actions)

    async def test_account_filter_does_not_crash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Passing account_filter scopes results without raising."""
        db, _store = _make_db(tmp_path)
        _seed_uncategorized_transactions(db, count=5)
        monkeypatch.setattr("moneybin.database._database_instance", db)

        response = await transactions_categorize_assist(
            limit=10, account_filter=["acct_test"]
        )

        # All seeded rows belong to acct_test — full set should be returned
        assert isinstance(response.data, list)
        assert len(response.data) > 0

    async def test_empty_result_when_no_uncategorized(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns an empty data list when all transactions are categorized."""
        db, _store = _make_db(tmp_path)
        _seed_uncategorized_transactions(db, count=2)
        # Mark all as categorized
        db.execute(
            """
            INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_by)
            VALUES ('txn_assist_000', 'Food', 'Coffee', 'user'),
                   ('txn_assist_001', 'Food', 'Coffee', 'user')
            """
        )
        monkeypatch.setattr("moneybin.database._database_instance", db)

        response = await transactions_categorize_assist(limit=10)

        assert response.data == []
        assert response.summary.total_count == 0
