"""Undo discovery and recovery respect the shipped investment inverse boundary."""

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)


def _rejected_and_undone(db: Database) -> tuple[str, str]:
    matching, proposal_id = planned_investment(db)
    with operation() as rejected:
        matching.decide(proposal_id, decision="reject", actor="test")
    undo = UndoService(db)
    assert undo.get(rejected).can_undo
    inverse = undo.undo(rejected, actor="test")
    return rejected, inverse.undo_operation_id


@pytest.mark.parametrize("mixed", [False, True])
def test_investment_inverse_is_not_advertised_as_undoable(
    comparison_db: Database, mixed: bool
) -> None:
    _, inverse = _rejected_and_undone(comparison_db)
    if mixed:
        with operation(inverse):
            TransactionTagsRepo(comparison_db).add(
                transaction_id="synthetic", tag="keep", actor="test"
            )
    undo = UndoService(comparison_db)
    detail = undo.get(inverse)
    assert not detail.can_undo
    assert detail.undo_refusal_code == error_codes.RECOVERY_NO_PATH
    assert not detail.undo_recovery_actions
    summary = next(
        row for row in undo.history(include_undone=True) if row.operation_id == inverse
    )
    assert not summary.can_undo
    assert not summary.recovery_actions


def test_already_undone_rejection_never_recommends_unsupported_inverse(
    comparison_db: Database,
) -> None:
    rejected, _ = _rejected_and_undone(comparison_db)
    undo = UndoService(comparison_db)
    detail = undo.get(rejected)
    assert detail.undo_refusal_code == error_codes.UNDO_ALREADY_UNDONE
    assert not detail.undo_recovery_actions
    summary = next(row for row in undo.history() if row.operation_id == rejected)
    assert not summary.recovery_actions
    with pytest.raises(UserError) as raised:
        undo.undo(rejected, actor="test")
    assert raised.value.code == error_codes.UNDO_ALREADY_UNDONE
    assert not raised.value.recovery_actions


def test_mixed_unsupported_inverse_refuses_without_reversing_ordinary_rows(
    comparison_db: Database,
) -> None:
    _, inverse = _rejected_and_undone(comparison_db)
    with operation(inverse):
        TransactionTagsRepo(comparison_db).add(
            transaction_id="synthetic", tag="keep", actor="test"
        )
    audit = AuditService(comparison_db)
    before = audit.events_for_operation(inverse)
    audit_count = comparison_db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with operation() as refused:
        with pytest.raises(UserError) as raised:
            UndoService(comparison_db).undo(inverse, actor="test")
    assert raised.value.code == error_codes.RECOVERY_NO_PATH
    assert comparison_db.execute(
        "SELECT tag FROM app.transaction_tags WHERE transaction_id = ?",
        ["synthetic"],
    ).fetchall() == [("keep",)]
    assert audit.events_for_operation(inverse) == before
    assert audit.events_for_operation(refused) == []
    assert (
        comparison_db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        == audit_count
    )


def test_ordinary_undo_retains_inverse_recovery(comparison_db: Database) -> None:
    with operation() as original:
        TransactionTagsRepo(comparison_db).add(
            transaction_id="synthetic", tag="restore", actor="test"
        )
    undo = UndoService(comparison_db)
    inverse = undo.undo(original, actor="test").undo_operation_id
    assert undo.get(inverse).can_undo
    recovery = undo.get(original).undo_recovery_actions
    assert recovery is not None
    assert [action.arguments for action in recovery] == [{"operation_id": inverse}]
    undo.undo(inverse, actor="test")
    assert undo.get(original).can_undo
    assert comparison_db.execute(
        "SELECT tag FROM app.transaction_tags WHERE transaction_id = ?",
        ["synthetic"],
    ).fetchall() == [("restore",)]
