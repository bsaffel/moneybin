"""Audit actions respect the undo service's current inverse capability."""

import pytest
from pytest_mock import MockerFixture

from moneybin import error_codes
from moneybin.mcp.tools.system import system_audit_get, system_audit_undo
from moneybin.services.undo_service import OperationDetail, UndoResult


def _inverse_detail(*, can_undo: bool) -> OperationDetail:
    return OperationDetail(
        operation_id="inverse-op",
        events=[],
        can_undo=can_undo,
        undo_blocked_by=None,
        unresolvable=None,
        reversible_source_event_count=1,
        source_tables=["app.match_decisions"],
        undo_refusal_code=None if can_undo else error_codes.RECOVERY_NO_PATH,
        undo_refusal_message=None
        if can_undo
        else "Investment rejection redo is unavailable.",
        undo_recovery_actions=None,
    )


@pytest.mark.parametrize("can_redo", [False, True])
async def test_undo_actions_use_returned_operation_capability(
    mcp_db: object, mocker: MockerFixture, can_redo: bool
) -> None:
    mocker.patch(
        "moneybin.orchestration.account_mutations.undo_operation",
        return_value=UndoResult("inverse-op", "original-op", 1, ["match_decisions"]),
    )
    inspect_operation = mocker.patch(
        "moneybin.services.undo_service.UndoService.get",
        return_value=_inverse_detail(can_undo=can_redo),
    )

    result = await system_audit_undo("original-op")

    assert result.status == "ok"
    assert result.data is not None
    assert result.data.undo_operation_id == "inverse-op"
    assert result.data.reversed_row_count == 1
    assert bool(result.actions) is can_redo
    inspect_operation.assert_called_once_with("inverse-op")
    if can_redo:
        assert "system_audit_undo(operation_id='inverse-op')" in result.actions[0]


def test_operation_detail_reports_domain_refusal(
    mcp_db: object, mocker: MockerFixture
) -> None:
    detail = _inverse_detail(can_undo=False)
    mocker.patch("moneybin.services.undo_service.UndoService.get", return_value=detail)

    result = system_audit_get("inverse-op")

    assert result.data is not None
    assert result.data.can_undo is False
    assert result.actions == [detail.undo_refusal_message]
