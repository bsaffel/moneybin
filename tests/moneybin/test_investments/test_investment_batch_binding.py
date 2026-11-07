"""Human approval binds resolved effects of every item in a mixed batch."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.server.elicitation import AcceptedElicitation

from moneybin import error_codes
from moneybin.database import Database
from moneybin.mcp.tools.reviews import reviews_decide_coarse
from moneybin.protocol.write_contracts import CategorizationDecisionRequest
from moneybin.repositories.categorization_decisions_repo import (
    categorization_decision_id,
)
from moneybin.repositories.user_merchants_repo import UserMerchantsRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.review_decisions_service import ReviewDecisionsService
from tests.moneybin.db_helpers import CORE_FCT_TRANSACTIONS_DDL, seed_categories_view
from tests.moneybin.test_investments.test_investment_decision_contract import (
    investment_request,
)
from tests.moneybin.test_investments.test_investment_decision_routes import (
    use_decision_database,
)
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)


@pytest.mark.parametrize("drift", ["new_merchant", "existing_exemplar"])
@pytest.mark.asyncio
async def test_mixed_batch_refuses_resolved_merchant_drift_during_elicitation(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    investment_service, investment_id = planned_investment(comparison_db)
    use_decision_database(monkeypatch, comparison_db)
    comparison_db.execute(CORE_FCT_TRANSACTIONS_DDL)
    comparison_db.execute(
        """INSERT INTO core.fct_transactions
            (transaction_id, description, memo, account_id, amount)
            VALUES ('category_target', 'Synthetic Purchase', NULL, 'account', -10)"""
    )
    seed_categories_view(comparison_db)
    merchants = UserMerchantsRepo(comparison_db)

    def insert_merchant() -> str:
        event = merchants.insert(
            raw_pattern=None,
            match_type="oneOf",
            canonical_name="Synthetic Merchant",
            category="Food & Drink",
            subcategory=None,
            category_id="FND",
            created_by="user",
            exemplars=[],
            actor="test",
        )
        assert event.target_id is not None
        return event.target_id

    merchant_id = insert_merchant() if drift == "existing_exemplar" else None
    category_id = categorization_decision_id("category_target")
    decisions = [
        investment_request(investment_id),
        CategorizationDecisionRequest(
            kind="categorization",
            decision_id=category_id,
            decision="accept",
            category="Food & Drink",
            canonical_merchant_name="Synthetic Merchant",
        ),
    ]
    planner = ReviewDecisionsService(comparison_db, actor="mcp")
    before = planner.plan_ordinary(decisions)
    assert before[1].merchant_changed

    async def elicit(message: str, **kwargs: Any) -> Any:
        assert investment_id in message
        assert category_id in message
        # This concurrent write is independently committed while the prompt is open.
        with operation():
            if merchant_id is None:
                insert_merchant()
            else:
                merchants.append_exemplar(
                    merchant_id, "Synthetic Purchase", actor="test"
                )
        after = planner.plan_ordinary(decisions)
        assert before[1].before_state == after[1].before_state
        assert (
            before[1].merchant_id != after[1].merchant_id
            or before[1].merchant_changed != after[1].merchant_changed
        )
        return AcceptedElicitation(data=True)

    context = MagicMock()
    context.elicit = AsyncMock(side_effect=elicit)

    def supports_elicitation(_context: Any) -> bool:
        return True

    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: context)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", supports_elicitation
    )
    with operation() as decision_operation:
        result = await reviews_decide_coarse(decisions)
    assert result.error is not None
    assert result.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert investment_service.pending()[0]["proposal_id"] == investment_id
    assert (
        comparison_db.execute(
            "SELECT * FROM app.transaction_categories WHERE transaction_id = ?",
            ["category_target"],
        ).fetchall()
        == []
    )
    assert AuditService(comparison_db).events_for_operation(decision_operation) == []
