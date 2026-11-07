"""Mixed human confirmations bind cash reconciliation and forwarded curation."""

from typing import Any, Literal
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.server.elicitation import AcceptedElicitation

from moneybin import error_codes
from moneybin.database import Database
from moneybin.matching.persistence import get_match_decision
from moneybin.mcp.tools.reviews import reviews_decide_coarse
from moneybin.protocol.write_contracts import (
    MatchDecisionRequest,
    OrdinaryReviewDecisionRequest,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.transaction_id_aliases_repo import TransactionIdAliasesRepo
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import ApplyResult, TransformService
from tests.moneybin.db_helpers import CORE_FCT_TRANSACTIONS_DDL
from tests.moneybin.matching.test_transaction_id_aliasing import (
    _UNIONED_STUB_DDL,  # pyright: ignore[reportPrivateUsage]  # Reuse the complete real-model fixture.
    _matched_view_sql,  # pyright: ignore[reportPrivateUsage]  # Exercise the shipped anchor derivation.
)
from tests.moneybin.test_investments.test_investment_decision_contract import (
    investment_request,
)
from tests.moneybin.test_investments.test_investment_decision_routes import (
    use_decision_database,
)
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)


@pytest.fixture
def no_sqlmesh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep real committed cash effects while excluding SQLMesh materialization."""
    monkeypatch.setattr(
        TransformService,
        "restate_models",
        MagicMock(return_value=ApplyResult(applied=True, duration_seconds=0)),
    )


def _transfer(
    db: Database, match_id: str, *, accepted: bool = False, dedup: bool = False
) -> None:
    MatchDecisionsRepo(db).insert(
        match_id=match_id,
        source_transaction_id_a="shared-leg",
        source_type_a="manual",
        source_origin_a="user",
        source_transaction_id_b=match_id,
        source_type_b="manual",
        source_origin_b="user",
        account_id="cash-a",
        account_id_b=None if dedup else "cash-b",
        confidence_score=1,
        match_signals={},
        match_type="dedup" if dedup else "transfer",
        match_status="accepted" if accepted else "pending",
        decided_by="user",
        actor="test",
    )


def _alias_fixture(db: Database, *, real_anchor: bool = False) -> None:
    db.execute(CORE_FCT_TRANSACTIONS_DDL)
    db.execute(
        """INSERT INTO core.fct_transactions (transaction_id, amount)
           VALUES ('live-cash', 10)"""
    )
    db.execute(_UNIONED_STUB_DDL)
    db.execute(_matched_view_sql())
    if real_anchor:
        db.execute(
            """INSERT INTO prep.int_transactions__unioned
               (source_type, source_origin, source_account_key,
                source_transaction_id, account_id, loaded_at)
               VALUES ('manual', 'user', 'source-account', 'shared-leg',
                       'cash-a', '2026-01-01'),
                      ('manual', 'user', 'source-account', 'requested-transfer',
                       'cash-a', '2026-02-01')"""
        )
    TransactionIdAliasesRepo(db).insert(
        old_transaction_id="old-cash", new_transaction_id="live-cash", actor="test"
    )


@pytest.mark.parametrize("drift", ["transfer", "curation", "anchor_order", "amount"])
@pytest.mark.parametrize("decision", ["accept", "reject"])
@pytest.mark.asyncio
async def test_mixed_batch_binds_only_effect_dependencies(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
    decision: Literal["accept", "reject"],
    no_sqlmesh: None,
) -> None:
    service, investment_id = planned_investment(comparison_db)
    use_decision_database(monkeypatch, comparison_db)
    _transfer(comparison_db, "requested-transfer", dedup=drift == "anchor_order")
    _alias_fixture(comparison_db, real_anchor=drift == "anchor_order")
    original = get_match_decision(comparison_db, "requested-transfer")
    identities = comparison_db.execute(
        "SELECT transaction_id FROM prep.int_transactions__matched ORDER BY 1"
    ).fetchall()
    requests: list[OrdinaryReviewDecisionRequest] = [
        investment_request(investment_id),
        MatchDecisionRequest(
            kind="match", decision_id="requested-transfer", decision=decision
        ),
    ]

    async def elicit(message: str, **kwargs: Any) -> Any:
        assert investment_id in message
        assert "requested-transfer" in message
        with operation():
            if drift == "transfer":
                _transfer(comparison_db, "earlier-transfer", accepted=True)
            elif drift == "curation":
                TransactionNotesRepo(comparison_db).add(
                    transaction_id="old-cash",
                    note_id="concurrent-note",
                    text="Synthetic note",
                    actor="test",
                )
            elif drift == "anchor_order":
                comparison_db.execute(
                    """UPDATE prep.int_transactions__unioned
                       SET loaded_at = '2026-03-01'
                       WHERE source_transaction_id = 'shared-leg'"""
                )
                assert (
                    comparison_db.execute(
                        "SELECT transaction_id FROM prep.int_transactions__matched ORDER BY 1"
                    ).fetchall()
                    == identities
                )
            else:
                comparison_db.execute("UPDATE core.fct_transactions SET amount = 20")
        assert get_match_decision(comparison_db, "requested-transfer") == original
        return AcceptedElicitation(data=True)

    context = MagicMock()
    context.elicit = AsyncMock(side_effect=elicit)

    def supports(_context: Any) -> bool:
        return True

    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: context)
    monkeypatch.setattr("moneybin.mcp.confirmation.supports_elicitation", supports)
    with operation() as op:
        result = await reviews_decide_coarse(requests)
    if decision == "accept" and drift != "amount":
        assert result.error is not None
        assert result.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
        assert service.pending()[0]["proposal_id"] == investment_id
        assert get_match_decision(comparison_db, "requested-transfer") == original
        assert AuditService(comparison_db).events_for_operation(op) == []
        if drift == "curation":
            assert comparison_db.execute(
                "SELECT transaction_id FROM app.transaction_notes WHERE note_id = ?",
                ["concurrent-note"],
            ).fetchone() == ("old-cash",)
    else:
        assert result.error is None
        assert service.history()[0]["status"] == "rejected"
        row = get_match_decision(comparison_db, "requested-transfer")
        assert row is not None
        assert row["match_status"] == (
            "accepted" if decision == "accept" else "rejected"
        )


@pytest.mark.asyncio
async def test_ordinary_cash_accept_keeps_existing_confirmation_behavior(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, no_sqlmesh: None
) -> None:
    use_decision_database(monkeypatch, comparison_db)
    _transfer(comparison_db, "ordinary-transfer")
    result = await reviews_decide_coarse([
        MatchDecisionRequest(
            kind="match", decision_id="ordinary-transfer", decision="accept"
        )
    ])
    assert result.error is None
    row = get_match_decision(comparison_db, "ordinary-transfer")
    assert row is not None
    assert row["match_status"] == "accepted"
