"""Verify classify_user_error injects the system_status recovery action."""

from __future__ import annotations

from moneybin.database import DatabaseLockError
from moneybin.errors import classify_user_error

_FAKE_DB_PATH = "/tmp/x.duckdb"  # noqa: S108  # static test fixture string, not a real file


def test_database_lock_error_classified_with_system_status_recovery_action() -> None:
    err = DatabaseLockError(
        f"Could not acquire write lock for {_FAKE_DB_PATH} after 10s "
        "(operation_type=interactive)."
    )
    user_err = classify_user_error(err)
    assert user_err is not None
    assert user_err.recovery_actions is not None
    assert len(user_err.recovery_actions) == 1
    action = user_err.recovery_actions[0]
    assert action.tool == "system_status"
    # system_status takes no parameters — the action carries no arguments and
    # the agent reads the holder from the always-present database_connections
    # block of the full payload.
    assert action.arguments == {}
    # "suggested", not "certain": system_status diagnoses the contention but
    # does not resolve it.
    assert action.confidence == "suggested"
    assert action.idempotent is True


def test_database_lock_error_classified_message_preserved() -> None:
    err = DatabaseLockError(f"Could not acquire write lock for {_FAKE_DB_PATH}")
    user_err = classify_user_error(err)
    assert user_err is not None
    assert _FAKE_DB_PATH in user_err.message
    # The existing hint string is preserved alongside the new structured
    # recovery_actions — both surfaces want both.
    assert user_err.hint is not None
    assert "db ps" in user_err.hint
