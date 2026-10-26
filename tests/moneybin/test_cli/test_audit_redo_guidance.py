"""Undo receipts offer redo only when the returned operation supports it."""

from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.commands.system.audit import app
from moneybin.services.undo_service import OperationDetail, UndoResult


@pytest.mark.parametrize("can_redo", [False, True])
def test_undo_receipt_uses_returned_operation_capability(
    mocker: MockerFixture, can_redo: bool
) -> None:
    mocker.patch(
        "moneybin.cli.commands.system.audit.get_database", return_value=MagicMock()
    )
    mocker.patch(
        "moneybin.cli.commands.system.audit.undo_operation",
        return_value=UndoResult("inverse-op", "original-op", 1, ["match_decisions"]),
    )
    inspect_operation = mocker.patch(
        "moneybin.services.undo_service.UndoService.get",
        side_effect=[
            OperationDetail(
                operation_id=operation_id,
                events=[],
                can_undo=can_undo,
                undo_blocked_by=None,
                unresolvable=None,
                reversible_source_event_count=1,
                source_tables=["app.match_decisions"],
                undo_refusal_code=None,
                undo_refusal_message=None,
                undo_recovery_actions=None,
            )
            for operation_id, can_undo in (
                ("original-op", True),
                ("inverse-op", can_redo),
            )
        ],
    )

    result = CliRunner().invoke(app, ["undo", "original-op", "--yes"])

    assert result.exit_code == 0, result.output
    assert "Reversed operation original-op" in result.stdout
    assert "Rows reversed: 1" in result.stdout
    assert ("Undo this undo:" in result.stdout) is can_redo
    assert inspect_operation.call_args.args == ("inverse-op",)
    if can_redo:
        assert "moneybin system audit undo inverse-op" in result.stdout
