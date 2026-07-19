# tests/moneybin/test_mcp/test_categorization_tools.py
"""Tests for v1 categorization MCP tools.

Individual categorization logic is tested in test_categorization_service.py.
These tests verify the MCP tool wiring, envelope format, and basic end-to-end.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

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
    transactions_categorize_auto_accept,
    transactions_categorize_commit,
    transactions_categorize_pending,
    transactions_categorize_rules_create,
    transactions_categorize_stats,
)
from moneybin.mcp.tools.transactions_categorize_assist import (
    register_transactions_categorize_assist_tools,
)
from moneybin.services.auto_rule_service import AutoConfirmResult
from moneybin.services.categorization import CategorizationRuleInput
from moneybin.services.categorization.applier import RuleCreationResult
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
        assert "transactions_categorize_improve_ai" in names

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
        # transactions_categorize_auto_stats was removed — its functionality
        # is now available via transactions_categorize_stats(include_auto=True).
        assert {
            "transactions_categorize_auto_review",
            "transactions_categorize_auto_accept",
        } <= names
        assert "transactions_categorize_auto_stats" not in names


class TestCategorySetWritePath:
    """categories_set routes writes to the right backing table."""

    @pytest.mark.unit
    async def test_set_default_category_writes_override(self, mcp_db: Path) -> None:
        with get_database(read_only=False) as db:
            seed_categories_view(db)

        await categories_set(category_id="FND", is_active=False)

        with get_database(read_only=False) as db:
            rows = db.execute(
                "SELECT category_id, is_active FROM app.category_overrides"
            ).fetchall()
        assert rows == [("FND", False)]

    @pytest.mark.unit
    async def test_set_user_category_updates_user_categories(
        self, mcp_db: Path
    ) -> None:
        with get_database(read_only=False) as db:
            seed_categories_view(db)
            db.execute("""
                INSERT INTO app.user_categories
                (category_id, category, subcategory, is_active)
                VALUES ('CUSTOM1', 'Childcare', 'Daycare', true)
            """)

        await categories_set(category_id="CUSTOM1", is_active=False)

        with get_database(read_only=False) as db:
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
        with get_database(read_only=False) as db:
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
        with get_database(read_only=False) as db:
            seed_categories_view(db)
        envelope = (await categories_delete(category_id="FND")).to_dict()
        assert envelope["status"] == "error"
        assert envelope["error"]["code"] == "CATEGORY_IS_DEFAULT"

    @pytest.mark.unit
    async def test_force_cascade_clears_transaction_reference(
        self, mcp_db: Path
    ) -> None:
        with get_database(read_only=False) as db:
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
        # CategorizeRunPayload has only AGGREGATE fields → Tier.LOW derived sensitivity
        assert result["summary"]["sensitivity"] == "low"
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


class TestTransactionsCategorizeImproveAi:
    """transactions_categorize_improve_ai tool wiring and response envelope."""

    @pytest.mark.unit
    async def test_upgrades_confident_ai_row_to_provider_native(
        self, mcp_db: Path
    ) -> None:
        """An ai-guessed row whose Plaid category bridges confidently is upgraded.

        Mirrors the CLI integration test's seeding
        (``tests/integration/test_categorize_improve_ai_cli.py``): a confident
        (HIGH) Plaid bridge mapping plus a pre-existing ``ai``-guessed
        categorization that the upgrade pass should overwrite.
        """
        from moneybin.mcp.tools.transactions_categorize import (
            transactions_categorize_improve_ai,
        )

        with get_database(read_only=False) as db:
            db.execute(
                "INSERT INTO seeds.category_source_map "
                "(source_type, source_category_code, code_level, category_id, "
                "source_taxonomy_version) VALUES "
                "('plaid', 'FOOD_AND_DRINK_GROCERIES', 'detailed', 'FND-GRO', 'plaid_pfc_v2')"
            )
            db.execute(
                "INSERT INTO seeds.categories "
                "(category_id, category, subcategory, description) "
                "VALUES ('FND-GRO', 'Groceries', NULL, 'test category')"
            )
            db.execute("CREATE SCHEMA IF NOT EXISTS prep")
            db.execute(
                "CREATE TABLE IF NOT EXISTS prep.int_transactions__merged ("
                "  transaction_id VARCHAR PRIMARY KEY, "
                "  category_detailed VARCHAR, "
                "  plaid_category VARCHAR, "
                "  category_confidence VARCHAR"
                ")"
            )
            db.execute(
                "INSERT INTO prep.int_transactions__merged "
                "(transaction_id, category_detailed, plaid_category, category_confidence) "
                "VALUES ('t1', 'FOOD_AND_DRINK_GROCERIES', 'FOOD_AND_DRINK', 'HIGH')"
            )
            db.execute(
                "INSERT INTO app.transaction_categories "
                "(transaction_id, category, categorized_by) "
                "VALUES ('t1', 'Shopping', 'ai')"
            )

        result = (await transactions_categorize_improve_ai()).to_dict()
        # ImproveAiPayload has only an AGGREGATE field → Tier.LOW derived sensitivity
        assert result["summary"]["sensitivity"] == "low"
        assert result["data"]["upgraded_count"] == 1

        with get_database(read_only=True) as db:
            row = db.execute(
                "SELECT category, categorized_by FROM app.transaction_categories "
                "WHERE transaction_id = 't1'"
            ).fetchone()
        assert row == ("Groceries", "provider_native")

    @pytest.mark.unit
    async def test_no_upgradeable_rows_returns_zero(self, mcp_db: Path) -> None:
        """With no ai-guessed rows to upgrade, the count is zero."""
        from moneybin.mcp.tools.transactions_categorize import (
            transactions_categorize_improve_ai,
        )

        result = (await transactions_categorize_improve_ai()).to_dict()
        assert result["data"]["upgraded_count"] == 0


class TestTransactionsCategorizeCommit:
    """transactions_categorize_commit tool wiring and response envelope."""

    @pytest.mark.unit
    async def test_transactions_categorize_commit_writes_categorization(
        self, mcp_db: Path
    ) -> None:
        """Commit tool accepts items and writes categorizations."""
        with get_database(read_only=False) as db:
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


class TestCategorizationStatsIncludeAuto:
    """transactions_categorize_stats with include_auto=True returns both scopes."""

    @pytest.mark.unit
    async def test_categorize_stats_include_auto_returns_both_scopes(
        self, mcp_db: object
    ) -> None:
        result = (await transactions_categorize_stats(include_auto=True)).to_dict()
        assert "overall" in result["data"]
        assert "auto" in result["data"]

    @pytest.mark.unit
    async def test_categorize_stats_default_no_auto(self, mcp_db: object) -> None:
        result = (await transactions_categorize_stats()).to_dict()
        # Default (include_auto=False) returns flat stats, not nested overall/auto.
        assert "total_transactions" in result["data"]
        assert "auto" not in result["data"]


class TestCategorizePendingSortParam:
    """transactions_categorize_pending sort parameter."""

    @staticmethod
    def _install_view_with_transactions() -> None:
        with get_database(read_only=False) as db:
            db.execute("""
                CREATE OR REPLACE VIEW core.uncategorized_queue AS
                SELECT
                    'T1' AS transaction_id, 'ACC001' AS account_id,
                    'Test Bank Checking' AS account_name,
                    DATE '2026-04-01' AS txn_date,
                    CAST(-25.00 AS DECIMAL(18,2)) AS amount,
                    'COFFEE SHOP' AS description,
                    CAST(NULL AS VARCHAR) AS merchant_id,
                    'Coffee Shop' AS merchant_normalized,
                    CAST(39 AS INTEGER) AS age_days,
                    CAST(975.0 AS DOUBLE) AS priority_score,
                    'ofx' AS source_type,
                    CAST(NULL AS VARCHAR) AS source_id
                UNION ALL SELECT
                    'T2', 'ACC001', 'Test Bank Checking',
                    DATE '2026-04-10', CAST(-500.00 AS DECIMAL(18,2)),
                    'BIG EXPENSE', NULL, 'Big Expense',
                    CAST(30 AS INTEGER), 15000.0, 'ofx', NULL
                UNION ALL SELECT
                    'T3', 'ACC002', 'Other Bank Savings',
                    DATE '2026-04-15', CAST(-5.00 AS DECIMAL(18,2)),
                    'TINY', NULL, 'Tiny',
                    CAST(25 AS INTEGER), 125.0, 'ofx', NULL
            """)  # noqa: S608  # test input, not executing dynamic SQL

    @pytest.mark.unit
    async def test_categorize_pending_sort_impact(self, mcp_db: object) -> None:
        self._install_view_with_transactions()
        result = (
            await transactions_categorize_pending(sort="impact", limit=10)
        ).to_dict()
        amounts = [r["amount"] for r in result["data"]["transactions"]]
        ages = [r["age_days"] for r in result["data"]["transactions"]]
        impacts = [abs(float(a)) * d for a, d in zip(amounts, ages, strict=True)]
        assert impacts == sorted(impacts, reverse=True)

    @pytest.mark.unit
    async def test_categorize_pending_sort_date_default(self, mcp_db: object) -> None:
        self._install_view_with_transactions()
        result = (
            await transactions_categorize_pending(sort="date", limit=10)
        ).to_dict()
        dates = [r["transaction_date"] for r in result["data"]["transactions"]]
        assert dates == sorted(dates, reverse=True)


class TestAllowBroadWiring:
    """MCP-level wiring tests for allow_broad forwarding.

    Mirrors the CLI wiring tests (test_categorize_rules_commands.py::
    test_rules_create_allow_broad_forwards_true,
    test_categorize_auto_commands.py::test_auto_accept_allow_broad_forwards_true)
    — the CLI got these; the MCP tools did not.
    """

    _RULE_DICT = {
        "name": "Transfer TO",
        "merchant_pattern": "TO",
        "category": "Transfer",
        "subcategory": "Internal Transfer",
        "match_type": "contains",
    }
    _EXPECTED_ITEM = CategorizationRuleInput(
        name="Transfer TO",
        merchant_pattern="TO",
        category="Transfer",
        subcategory="Internal Transfer",
        match_type="contains",
    )

    @staticmethod
    def _rule_result() -> RuleCreationResult:
        return RuleCreationResult(
            created=1, existing=0, skipped=0, error_details=[], rule_ids=["r1"]
        )

    @pytest.mark.unit
    @patch("moneybin.mcp.tools.transactions_categorize.get_database")
    @patch("moneybin.services.categorization.CategorizationService.create_rules")
    async def test_rules_create_forwards_allow_broad_true(
        self, mock_create_rules: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """allow_broad=True forwards to CategorizationService.create_rules()."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_create_rules.return_value = self._rule_result()

        await transactions_categorize_rules_create(
            rules=[self._RULE_DICT], allow_broad=True
        )

        mock_create_rules.assert_called_once_with(
            [self._EXPECTED_ITEM], reapply=False, actor="mcp", allow_broad=True
        )

    @pytest.mark.unit
    @patch("moneybin.mcp.tools.transactions_categorize.get_database")
    @patch("moneybin.services.categorization.CategorizationService.create_rules")
    async def test_rules_create_allow_broad_defaults_to_false(
        self, mock_create_rules: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Without allow_broad, create_rules() is called with allow_broad=False."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_create_rules.return_value = self._rule_result()

        await transactions_categorize_rules_create(rules=[self._RULE_DICT])

        mock_create_rules.assert_called_once_with(
            [self._EXPECTED_ITEM], reapply=False, actor="mcp", allow_broad=False
        )

    @pytest.mark.unit
    @patch("moneybin.mcp.tools.transactions_categorize.get_database")
    @patch("moneybin.services.auto_rule_service.AutoRuleService.accept")
    async def test_auto_accept_forwards_allow_broad_true(
        self, mock_accept: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """allow_broad=True forwards to AutoRuleService.accept() (F17 Layer 3 override)."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_accept.return_value = AutoConfirmResult(
            approved=1, rejected=0, skipped=0, newly_categorized=0, rule_ids=["r1"]
        )

        await transactions_categorize_auto_accept(accept=["a1"], allow_broad=True)

        mock_accept.assert_called_once_with(
            accept=["a1"], reject=[], actor="mcp", allow_broad=True
        )

    @pytest.mark.unit
    @patch("moneybin.mcp.tools.transactions_categorize.get_database")
    @patch("moneybin.services.auto_rule_service.AutoRuleService.accept")
    async def test_auto_accept_allow_broad_defaults_to_false(
        self, mock_accept: MagicMock, mock_get_db: MagicMock
    ) -> None:
        """Without allow_broad, accept() is called with allow_broad=False."""
        mock_get_db.return_value.__enter__.return_value = MagicMock()
        mock_accept.return_value = AutoConfirmResult(
            approved=1, rejected=0, skipped=0, newly_categorized=0, rule_ids=["r1"]
        )

        await transactions_categorize_auto_accept(accept=["a1"])

        mock_accept.assert_called_once_with(
            accept=["a1"], reject=[], actor="mcp", allow_broad=False
        )
