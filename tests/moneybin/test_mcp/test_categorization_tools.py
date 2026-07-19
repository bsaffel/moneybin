# tests/moneybin/test_mcp/test_categorization_tools.py
"""Tests for v1 categorization MCP tools.

Individual categorization logic is tested in test_categorization_service.py.
These tests verify the MCP tool wiring, envelope format, and basic end-to-end.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any
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
    register_categorization_coarse_reads,
    register_categorization_coarse_writes,
    register_transactions_categorize_tools,
    transactions_categorize_auto_accept,
    transactions_categorize_commit,
    transactions_categorize_pending,
    transactions_categorize_rules_coarse,
    transactions_categorize_rules_create,
    transactions_categorize_rules_set_coarse,
    transactions_categorize_stats,
)
from moneybin.mcp.tools.transactions_categorize_assist import (
    register_transactions_categorize_assist_tools,
)
from moneybin.mcp.write_contracts import (
    CategorizationRuleMatch,
    CategorizationRuleTarget,
)
from moneybin.privacy.introspection import derive_tier
from moneybin.privacy.payloads.categorize import CategorizationRulesCoarsePayload
from moneybin.privacy.taxonomy import Tier
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo
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

    @pytest.mark.unit
    async def test_force_cascade_is_fully_audited_and_undoable(
        self, mcp_db: Path
    ) -> None:
        with get_database(read_only=False) as db:
            db.execute(
                "INSERT INTO app.user_categories "
                "(category_id, category, subcategory, is_active) "
                "VALUES ('USERCAT3', 'Undo Linked', NULL, true)"
            )
            db.execute(
                "INSERT INTO app.transaction_categories "
                "(transaction_id, category, category_id, categorized_by) "
                "VALUES ('txn-undo-forced', 'Undo Linked', 'USERCAT3', 'user')"
            )

        envelope = (
            await categories_delete(category_id="USERCAT3", force=True)
        ).to_dict()
        assert envelope["data"] == {
            "category_id": "USERCAT3",
            "action": "deleted",
            "force": True,
        }
        with get_database(read_only=True) as db:
            audit_rows = db.execute(
                "SELECT operation_id, action FROM app.audit_log "
                "WHERE target_id IN ('USERCAT3', 'txn-undo-forced') "
                "ORDER BY occurred_at, audit_id"
            ).fetchall()
        operation_ids = {row[0] for row in audit_rows}
        assert len(operation_ids) == 1
        assert {row[1] for row in audit_rows} == {
            "category.clear",
            "user_category.delete",
        }

        from moneybin.mcp.tools.system import system_audit_undo

        undo = await system_audit_undo(operation_ids.pop())
        assert undo.error is None
        with get_database(read_only=True) as db:
            assert db.execute(
                "SELECT category_id FROM app.user_categories "
                "WHERE category_id = 'USERCAT3'"
            ).fetchall() == [("USERCAT3",)]
            assert db.execute(
                "SELECT transaction_id FROM app.transaction_categories "
                "WHERE transaction_id = 'txn-undo-forced'"
            ).fetchall() == [("txn-undo-forced",)]


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
            db.execute("CREATE SCHEMA IF NOT EXISTS reports")
            db.execute("""
                CREATE OR REPLACE VIEW reports.uncategorized_queue AS
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


def _rule_target(
    *,
    state: str,
    rule_id: str | None = None,
    value: str = "COFFEE",
    category: str = "Food",
    min_amount: Decimal | None = None,
    max_amount: Decimal | None = None,
) -> CategorizationRuleTarget:
    """Build one strict target-state request with concise test defaults."""
    if state == "present":
        return CategorizationRuleTarget(
            kind="rule",
            rule_id=rule_id,
            state="present",
            matcher=CategorizationRuleMatch(
                type="contains",
                value=value,
                min_amount=min_amount,
                max_amount=max_amount,
            ),
            category=category,
            priority=10,
        )
    return CategorizationRuleTarget(
        kind="rule",
        rule_id=rule_id or "rule_target_1",
        state=state,  # type: ignore[arg-type]  # parametrized literal states
    )


class TestCategorizationRulesTargetState:
    """The dormant coarse write makes each rule's desired state explicit."""

    @pytest.mark.unit
    async def test_present_creates_then_updates_a_rule(self, mcp_db: Path) -> None:
        created = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="present")]
        )
        rule_id = created.data.results[0].rule_id

        updated = await transactions_categorize_rules_set_coarse(
            rules=[
                CategorizationRuleTarget(
                    kind="rule",
                    rule_id=rule_id,
                    state="present",
                    matcher=CategorizationRuleMatch(type="exact", value="COFFEE SHOP"),
                    category="Dining",
                    priority=20,
                )
            ]
        )

        assert created.data.results[0].state == "present"
        assert updated.data.results[0].state == "present"
        with get_database(read_only=True) as db:
            row = db.execute(
                "SELECT merchant_pattern, match_type, category, priority, is_active "
                "FROM app.categorization_rules WHERE rule_id = ?",
                [rule_id],
            ).fetchone()
        assert row == ("COFFEE SHOP", "exact", "Dining", 20, True)

    @pytest.mark.unit
    async def test_inactive_is_an_idempotent_target_state(self, mcp_db: Path) -> None:
        created = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="present")]
        )
        rule_id = created.data.results[0].rule_id

        response = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="inactive", rule_id=rule_id)]
        )

        assert response.data.results[0].state == "inactive"
        with get_database(read_only=True) as db:
            assert db.execute(
                "SELECT is_active FROM app.categorization_rules WHERE rule_id = ?",
                [rule_id],
            ).fetchone() == (False,)

        repeated = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="inactive", rule_id=rule_id)]
        )
        assert repeated.to_dict()["error"]["code"] == "mutation_nothing_to_do"

    @pytest.mark.unit
    async def test_absent_requires_confirmation_for_a_present_rule(
        self, mcp_db: Path
    ) -> None:
        created = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="present")]
        )
        rule_id = created.data.results[0].rule_id

        first = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="absent", rule_id=rule_id)]
        )
        token = first.error.details["confirmation_token"]
        removed = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="absent", rule_id=rule_id)],
            confirmation_token=token,
        )

        assert removed.data.results[0].state == "absent"
        with get_database(read_only=True) as db:
            assert (
                db.execute(
                    "SELECT 1 FROM app.categorization_rules WHERE rule_id = ?",
                    [rule_id],
                ).fetchone()
                is None
            )

    @pytest.mark.unit
    async def test_batch_validates_and_resolves_before_writing(
        self, mcp_db: Path
    ) -> None:
        valid = _rule_target(state="present")
        invalid = _rule_target(state="inactive", rule_id="missing_rule")

        response = await transactions_categorize_rules_set_coarse(
            rules=[valid, invalid]
        )

        assert response.to_dict()["status"] == "error"
        with get_database(read_only=True) as db:
            assert (
                db.execute(
                    "SELECT 1 FROM app.categorization_rules WHERE merchant_pattern = ?",
                    ["COFFEE"],
                ).fetchone()
                is None
            )

    @pytest.mark.unit
    async def test_coarse_registrar_keeps_the_live_rule_tools_unchanged(
        self, mcp_db: Path
    ) -> None:
        server = FastMCP("coarse-rules")
        register_categorization_coarse_writes(server)

        names = {tool.name for tool in await server._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert names == {"transactions_categorize_rules_set"}

    @pytest.mark.unit
    async def test_bounded_decimal_target_retries_by_natural_key_and_rule_id(
        self, mcp_db: Path
    ) -> None:
        target = _rule_target(
            state="present",
            min_amount=Decimal("-19.99"),
            max_amount=Decimal("-0.10"),
        )
        created = await transactions_categorize_rules_set_coarse(rules=[target])
        rule_id = created.data.results[0].rule_id

        natural_retry = await transactions_categorize_rules_set_coarse(rules=[target])
        id_retry = await transactions_categorize_rules_set_coarse(
            rules=[
                _rule_target(
                    state="present",
                    rule_id=rule_id,
                    min_amount=Decimal("-19.99"),
                    max_amount=Decimal("-0.10"),
                )
            ]
        )
        active = await transactions_categorize_rules_coarse(view="active")

        assert natural_retry.to_dict()["error"]["code"] == "mutation_nothing_to_do"
        assert id_retry.to_dict()["error"]["code"] == "mutation_nothing_to_do"
        assert active.data.rules[0].min_amount == Decimal("-19.99")
        assert active.data.rules[0].max_amount == Decimal("-0.10")
        with get_database(read_only=True) as db:
            assert db.execute(
                "SELECT COUNT(*) FROM app.categorization_rules "
                "WHERE merchant_pattern = 'COFFEE'"
            ).fetchone() == (1,)

    @pytest.mark.unit
    async def test_duplicate_new_natural_keys_are_rejected_before_writing(
        self, mcp_db: Path
    ) -> None:
        target = _rule_target(state="present")

        response = await transactions_categorize_rules_set_coarse(
            rules=[target, target]
        )

        assert response.to_dict()["error"]["code"] == "mutation_invalid_input"
        with get_database(read_only=True) as db:
            assert db.execute(
                "SELECT COUNT(*) FROM app.categorization_rules "
                "WHERE merchant_pattern = 'COFFEE'"
            ).fetchone() == (0,)

    @pytest.mark.unit
    async def test_unsafe_short_contains_target_is_rejected(self, mcp_db: Path) -> None:
        response = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="present", value="TO")]
        )

        assert response.to_dict()["error"]["code"] == "mutation_invalid_input"
        with get_database(read_only=True) as db:
            assert db.execute(
                "SELECT COUNT(*) FROM app.categorization_rules "
                "WHERE merchant_pattern = 'TO'"
            ).fetchone() == (0,)

    @pytest.mark.unit
    async def test_write_failure_rolls_back_rule_and_audit_rows(
        self, mcp_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        original_insert = CategorizationRulesRepo.insert
        calls = 0

        def fail_second_insert(repo: CategorizationRulesRepo, **kwargs: Any) -> object:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected second write failure")
            return original_insert(repo, **kwargs)

        monkeypatch.setattr(CategorizationRulesRepo, "insert", fail_second_insert)

        with pytest.raises(RuntimeError, match="injected second write failure"):
            await transactions_categorize_rules_set_coarse(
                rules=[
                    _rule_target(state="present", value="COFFEE"),
                    _rule_target(state="present", value="MARKET"),
                ]
            )

        with get_database(read_only=True) as db:
            assert db.execute(
                "SELECT COUNT(*) FROM app.categorization_rules "
                "WHERE merchant_pattern IN ('COFFEE', 'MARKET')"
            ).fetchone() == (0,)
            assert db.execute(
                "SELECT COUNT(*) FROM app.audit_log "
                "WHERE target_table = 'categorization_rules'"
            ).fetchone() == (0,)

    @pytest.mark.unit
    async def test_confirmation_rejects_changed_live_rule_state(
        self, mcp_db: Path
    ) -> None:
        created = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="present")]
        )
        rule_id = created.data.results[0].rule_id
        first = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="absent", rule_id=rule_id)]
        )
        token = first.error.details["confirmation_token"]
        with get_database(read_only=False) as db:
            assert (
                CategorizationRulesRepo(db).deactivate(rule_id, actor="test")
                is not None
            )

        response = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="absent", rule_id=rule_id)],
            confirmation_token=token,
        )

        assert response.to_dict()["error"]["code"] == "mutation_confirmation_mismatch"
        with get_database(read_only=True) as db:
            assert db.execute(
                "SELECT is_active FROM app.categorization_rules WHERE rule_id = ?",
                [rule_id],
            ).fetchone() == (False,)


class TestCategorizationRulesCoarseReads:
    """Dormant rule reads expose stable current views and real audit history."""

    @pytest.mark.unit
    async def test_active_inactive_and_history_include_deleted_states(
        self, mcp_db: Path
    ) -> None:
        created_ids: list[str] = []
        for value in ("ALPHA", "BRAVO", "CHARLIE", "DELTA"):
            response = await transactions_categorize_rules_set_coarse(
                rules=[_rule_target(state="present", value=value)]
            )
            rule_id = response.data.results[0].rule_id
            assert rule_id is not None
            created_ids.append(rule_id)
        for rule_id in created_ids[2:]:
            await transactions_categorize_rules_set_coarse(
                rules=[_rule_target(state="inactive", rule_id=rule_id)]
            )
        deletion = await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="absent", rule_id=created_ids[3])]
        )
        token = deletion.error.details["confirmation_token"]
        await transactions_categorize_rules_set_coarse(
            rules=[_rule_target(state="absent", rule_id=created_ids[3])],
            confirmation_token=token,
        )
        with get_database(read_only=False) as db:
            db.execute(
                "UPDATE app.categorization_rules "
                "SET priority = 10, created_at = TIMESTAMP '2026-01-01'"
            )
            db.execute(
                "UPDATE app.audit_log SET occurred_at = TIMESTAMP '2026-01-01' "
                "WHERE target_table = 'categorization_rules'"
            )

        active = await transactions_categorize_rules_coarse(view="active")
        inactive = await transactions_categorize_rules_coarse(view="inactive")
        history = await transactions_categorize_rules_coarse(view="history")

        assert active.data.kind == "active"
        assert [row.rule_id for row in active.data.rules] == sorted(created_ids[:2])
        assert inactive.data.kind == "inactive"
        assert [row.rule_id for row in inactive.data.rules] == [created_ids[2]]
        assert history.data.kind == "history"
        deleted = [
            event
            for event in history.data.events
            if event.rule_id == created_ids[3]
            and event.action == "categorization_rule.delete"
        ]
        assert len(deleted) == 1
        assert deleted[0].prior is not None
        assert deleted[0].current is None
        keys = [(event.rule_id, event.event_id) for event in history.data.events]
        expected = sorted(keys, key=lambda item: item[1], reverse=True)
        expected = sorted(expected, key=lambda item: item[0])
        assert keys == expected

    @pytest.mark.unit
    async def test_coarse_read_registrar_is_dormant_and_exact(self) -> None:
        server = FastMCP("coarse-rule-reads")
        register_categorization_coarse_reads(server)

        names = {tool.name for tool in await server._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert names == {"transactions_categorize_rules"}
        assert derive_tier(CategorizationRulesCoarsePayload) is Tier.HIGH


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
