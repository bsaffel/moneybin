# tests/moneybin/test_mcp/test_categorization_tools.py
"""Tests for v1 categorization MCP tools.

Individual categorization logic is tested in test_categorization_service.py.
These tests verify the MCP tool wiring, envelope format, and basic end-to-end.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.categories import (
    categories,
    categories_delete,
    categories_set,
    register_categories_tools,
)
from moneybin.mcp.tools.merchants import register_merchants_tools
from moneybin.mcp.tools.transactions_categorize import (
    register_transactions_categorize_tools,
    transactions_categorize_commit,
    transactions_categorize_stats,
)
from moneybin.mcp.tools.transactions_categorize_assist import (
    register_transactions_categorize_assist_tools,
)
from tests.moneybin.db_helpers import seed_categories_view

pytestmark = pytest.mark.usefixtures("mcp_db")


async def _registered_names() -> set[str]:
    srv = FastMCP("test")
    register_categories_tools(srv)
    register_merchants_tools(srv)
    register_transactions_categorize_tools(srv)
    register_transactions_categorize_assist_tools(srv)
    return {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


class TestCategorizeToolRegistration:
    """Verify categorize tools register and return envelopes."""

    @pytest.mark.unit
    async def test_all_categorize_tools_register(self) -> None:
        names = await _registered_names()
        assert "categories" in names
        assert "transactions_categorize_rules" in names
        assert "merchants" in names
        assert "transactions_categorize_stats" in names
        assert "transactions_categorize_pending" in names
        assert "transactions_categorize_commit" in names
        assert "transactions_categorize_rules_create" in names
        assert "transactions_categorize_rules_delete" in names
        assert "merchants_create" in names
        assert "categories_create" in names
        assert "categories_set" in names
        assert "categories_delete" in names
        assert "transactions_categorize_assist" in names
        assert "transactions_categorize_run" in names

    @pytest.mark.unit
    async def test_categorize_stats_returns_envelope(self, mcp_db: object) -> None:
        parsed = (await transactions_categorize_stats()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    async def test_categorize_categories_returns_envelope(self, mcp_db: object) -> None:
        """List categories returns a valid envelope (empty when no data)."""
        cat_result = (await categories()).to_dict()
        assert "summary" in cat_result
        assert "data" in cat_result
        # data is now a typed CategoriesPayload dict with a "categories" list field
        assert isinstance(cat_result["data"]["categories"], list)

    @pytest.mark.unit
    async def test_register_includes_auto_rule_tools(self) -> None:
        names = await _registered_names()
        assert {
            "transactions_categorize_auto_review",
            "transactions_categorize_auto_accept",
            "transactions_categorize_auto_stats",
        } <= names


class TestCategorySetWritePath:
    """categories_set routes writes to the right backing table."""

    @pytest.mark.unit
    async def test_set_default_category_writes_override(self, mcp_db: Path) -> None:
        with get_database() as db:
            seed_categories_view(db)

        await categories_set(category_id="FND", is_active=False)

        with get_database() as db:
            rows = db.execute(
                "SELECT category_id, is_active FROM app.category_overrides"
            ).fetchall()
        assert rows == [("FND", False)]

    @pytest.mark.unit
    async def test_set_user_category_updates_user_categories(
        self, mcp_db: Path
    ) -> None:
        with get_database() as db:
            seed_categories_view(db)
            db.execute("""
                INSERT INTO app.user_categories
                (category_id, category, subcategory, is_active)
                VALUES ('CUSTOM1', 'Childcare', 'Daycare', true)
            """)

        await categories_set(category_id="CUSTOM1", is_active=False)

        with get_database() as db:
            rows = db.execute(
                "SELECT is_active FROM app.user_categories WHERE category_id = ?",
                ["CUSTOM1"],
            ).fetchall()
            # Override table is for defaults only — user category updates must not write here.
            override_count = db.execute(
                "SELECT COUNT(*) FROM app.category_overrides"
            ).fetchone()
        assert rows == [(False,)]
        assert override_count == (0,)


class TestCategoriesDeleteTool:
    """categories_delete envelopes, error mapping, and force semantics."""

    @pytest.mark.unit
    async def test_deletes_unreferenced_user_category(self, mcp_db: Path) -> None:
        with get_database() as db:
            db.execute(
                "INSERT INTO app.user_categories "
                "(category_id, category, subcategory, is_active) "
                "VALUES ('USERCAT1', 'TestCat', NULL, true)"
            )

        envelope = (await categories_delete(category_id="USERCAT1")).to_dict()

        assert envelope["data"]["action"] == "deleted"
        assert envelope["data"]["category_id"] == "USERCAT1"
        assert envelope["data"]["force"] is False
        with get_database(read_only=True) as db:
            rows = db.execute(
                "SELECT 1 FROM app.user_categories WHERE category_id = ?",
                ["USERCAT1"],
            ).fetchall()
        assert rows == []

    @pytest.mark.unit
    async def test_default_category_returns_error_envelope(self, mcp_db: Path) -> None:
        with get_database() as db:
            seed_categories_view(db)
        envelope = (await categories_delete(category_id="FND")).to_dict()
        assert envelope["status"] == "error"
        assert envelope["error"]["code"] == "CATEGORY_IS_DEFAULT"

    @pytest.mark.unit
    async def test_force_cascade_clears_transaction_reference(
        self, mcp_db: Path
    ) -> None:
        with get_database() as db:
            db.execute(
                "INSERT INTO app.user_categories "
                "(category_id, category, subcategory, is_active) "
                "VALUES ('USERCAT2', 'Linked', NULL, true)"
            )
            db.execute(
                "INSERT INTO app.transaction_categories "
                "(transaction_id, category, category_id, categorized_by) "
                "VALUES ('txn-forced', 'Linked', 'USERCAT2', 'user')"
            )

        envelope = (
            await categories_delete(category_id="USERCAT2", force=True)
        ).to_dict()
        assert envelope["data"]["force"] is True
        with get_database(read_only=True) as db:
            txn_rows = db.execute(
                "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?",
                ["txn-forced"],
            ).fetchall()
        assert txn_rows == []


class TestTransactionsCategorizeRun:
    """transactions_categorize_run tool wiring and response envelope."""

    @pytest.mark.unit
    async def test_default_methods_runs_rules_and_merchants(self, mcp_db: Path) -> None:
        """Default methods cascade runs rules then merchants."""
        from moneybin.mcp.tools.transactions_categorize import (
            transactions_categorize_run,
        )

        result = (await transactions_categorize_run()).to_dict()
        assert result["summary"]["sensitivity"] == "medium"
        assert "applied_by_method" in result["data"]
        assert "rules" in result["data"]["applied_by_method"]
        assert "merchants" in result["data"]["applied_by_method"]
        assert result["data"]["total_applied"] == sum(
            result["data"]["applied_by_method"].values()
        )

    @pytest.mark.unit
    async def test_with_explicit_methods_rules_only(self, mcp_db: Path) -> None:
        """methods=['rules'] runs only the rules engine."""
        from moneybin.mcp.tools.transactions_categorize import (
            transactions_categorize_run,
        )

        result = (await transactions_categorize_run(methods=["rules"])).to_dict()
        assert "rules" in result["data"]["applied_by_method"]
        assert "merchants" not in result["data"]["applied_by_method"]


class TestTransactionsCategorizeCommit:
    """transactions_categorize_commit tool wiring and response envelope."""

    @pytest.mark.unit
    async def test_transactions_categorize_commit_writes_categorization(
        self, mcp_db: Path
    ) -> None:
        """Commit tool accepts items and writes categorizations."""
        with get_database() as db:
            db.execute(
                """
                INSERT INTO core.fct_transactions
                (transaction_id, account_id, authorized_date, amount, description)
                VALUES (?, ?, ?, ?, ?)
            """,
                ["txn-123", "acct-1", "2026-05-17", "-50.00", "Test purchase"],
            )

        result = (
            await transactions_categorize_commit(
                items=[
                    {
                        "transaction_id": "txn-123",
                        "category": "Groceries",
                        "subcategory": None,
                        "canonical_merchant_name": None,
                    }
                ]
            )
        ).to_dict()

        assert result["summary"]["returned_count"] == 1
        assert result["data"]["applied"] == 1
        with get_database(read_only=True) as db:
            rows = db.execute(
                "SELECT category FROM app.transaction_categories WHERE transaction_id = ?",
                ["txn-123"],
            ).fetchall()
        assert rows == [("Groceries",)]
