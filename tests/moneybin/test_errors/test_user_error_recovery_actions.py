"""UserError with recovery_actions."""

from __future__ import annotations

from moneybin import error_codes
from moneybin.errors import RecoveryAction, UserError


def _sample_action(tool: str = "system_audit_undo") -> RecoveryAction:
    return RecoveryAction(
        tool=tool,
        arguments={"operation_id": "op_test"},
        rationale="Restore pre-mutation state",
        confidence="certain",
        idempotent=True,
    )


class TestUserErrorRecoveryActions:
    """Tests for UserError.recovery_actions field."""

    def test_user_error_accepts_recovery_actions(self):
        err = UserError(
            "Something broke",
            code=error_codes.MUTATION_NOT_FOUND,
            recovery_actions=[_sample_action()],
        )
        assert err.recovery_actions is not None
        assert len(err.recovery_actions) == 1
        assert err.recovery_actions[0].tool == "system_audit_undo"

    def test_user_error_recovery_actions_defaults_to_none(self):
        err = UserError("Something broke", code=error_codes.MUTATION_NOT_FOUND)
        assert err.recovery_actions is None

    def test_user_error_accepts_empty_list(self):
        """Empty list = explicit 'nothing to recover'. Different from None."""
        err = UserError(
            "Something broke",
            code=error_codes.RECOVERY_NO_PATH,
            recovery_actions=[],
        )
        assert err.recovery_actions == []

    def test_user_error_accepts_multiple_actions(self):
        actions = [
            _sample_action("accounts_get"),
            _sample_action("import_revert"),
        ]
        err = UserError(
            "Something broke",
            code=error_codes.AUDIT_FK_VIOLATION,
            recovery_actions=actions,
        )
        assert err.recovery_actions is not None
        assert len(err.recovery_actions) == 2
        assert [a.tool for a in err.recovery_actions] == [
            "accounts_get",
            "import_revert",
        ]
