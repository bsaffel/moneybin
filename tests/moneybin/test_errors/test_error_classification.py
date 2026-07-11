"""Tests for the cross-cutting user-error classifier."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from moneybin import error_codes
from moneybin.database import DatabaseKeyError
from moneybin.errors import UserError, classify_user_error


def test_classify_database_key_error_returns_user_error() -> None:
    """DatabaseKeyError maps to a UserError carrying the recovery hint."""
    with patch(
        "moneybin.errors.database_key_error_hint",
        return_value="Run: moneybin db unlock",
    ):
        result = classify_user_error(DatabaseKeyError("locked"))
    assert result is not None
    assert result.code == error_codes.INFRA_WRONG_KEY
    assert "locked" in result.message
    assert result.hint == "Run: moneybin db unlock"


def test_classify_file_not_found_returns_user_error() -> None:
    """FileNotFoundError maps to a UserError with no hint."""
    result = classify_user_error(FileNotFoundError("missing.csv"))
    assert result is not None
    assert result.code == error_codes.INFRA_FILE_NOT_FOUND
    assert "missing.csv" in result.message
    assert result.hint is None


def test_classify_lookup_error_returns_not_found() -> None:
    """Plain LookupError maps to a UserError with code infra_not_found.

    Use INFRA_NOT_FOUND (prefix-neutral) rather than MUTATION_NOT_FOUND
    because the classifier fires on read paths too (account/category/note
    lookups). MUTATION_NOT_FOUND would mis-signal "write attempt" to
    agents branching on the prefix.
    """
    result = classify_user_error(LookupError("note abc not found"))
    assert result is not None
    assert result.code == error_codes.INFRA_NOT_FOUND
    assert "not found" in result.message


def test_classify_key_error_returns_none() -> None:
    """KeyError (LookupError subclass) propagates as internal error, not not_found."""
    assert classify_user_error(KeyError("bad_key")) is None


def test_classify_index_error_returns_none() -> None:
    """IndexError (LookupError subclass) propagates as internal error, not not_found."""
    assert classify_user_error(IndexError(0)) is None


def test_classify_unknown_exception_returns_none() -> None:
    """Unrecognized exceptions return None so callers can re-raise."""
    assert classify_user_error(RuntimeError("internal bug")) is None


def test_classify_value_error_returns_user_error() -> None:
    """ValueError maps to a UserError with code infra_invalid_input.

    Use INFRA_INVALID_INPUT (prefix-neutral) rather than MUTATION_INVALID_INPUT
    because ValueError fires on read paths too (date/decimal parsing in
    reports, query filters). MUTATION_INVALID_INPUT would mis-signal
    "write attempt" to agents branching on the prefix.
    """
    result = classify_user_error(ValueError("bad input"))
    assert result is not None
    assert result.code == error_codes.INFRA_INVALID_INPUT
    assert "bad input" in result.message


def test_user_error_to_dict_omits_none_hint() -> None:
    """UserError.to_dict drops the hint field when not set."""
    err = UserError("m", code="c")
    assert err.to_dict() == {"message": "m", "code": "c"}


def test_user_error_to_dict_serializes_recovery_actions() -> None:
    """UserError.to_dict includes recovery_actions when populated."""
    from moneybin.errors import RecoveryAction

    err = UserError(
        "m",
        code=error_codes.MUTATION_NOT_FOUND,
        recovery_actions=[
            RecoveryAction(
                tool="system_audit_undo",
                arguments={"operation_id": "op_test"},
                rationale="Restore pre-mutation state",
                confidence="certain",
                idempotent=True,
            )
        ],
    )
    d = err.to_dict()
    assert "recovery_actions" in d
    assert d["recovery_actions"][0]["tool"] == "system_audit_undo"
    assert d["recovery_actions"][0]["confidence"] == "certain"


def test_user_error_to_dict_omits_recovery_actions_when_none() -> None:
    """UserError.to_dict omits recovery_actions when not set.

    Preserves backward compat — to_dict shape unchanged for callers that
    aren't aware of recovery_actions.
    """
    err = UserError("m", code=error_codes.MUTATION_NOT_FOUND)
    assert "recovery_actions" not in err.to_dict()


def test_user_error_to_dict_includes_hint() -> None:
    """UserError.to_dict includes the hint when populated."""
    err = UserError("m", code="c", hint="h")
    assert err.to_dict() == {"message": "m", "code": "c", "hint": "h"}


def test_classify_database_not_initialized_error() -> None:
    from moneybin.database import DatabaseNotInitializedError
    from moneybin.errors import classify_user_error

    err = DatabaseNotInitializedError("db missing")
    result = classify_user_error(err)
    assert result is not None
    assert "db init" in (result.message + (result.hint or "")).lower()
    assert result.code == error_codes.INFRA_DATABASE_NOT_INITIALIZED


def test_classify_database_lock_error() -> None:
    from moneybin.database import DatabaseLockError
    from moneybin.errors import classify_user_error

    err = DatabaseLockError("Could not acquire write lock after 5s")
    result = classify_user_error(err)
    assert result is not None
    assert result.code == error_codes.INFRA_DATABASE_LOCKED


@pytest.fixture(autouse=True)
def _clean_active_profile() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Reset the process-wide active profile so DB-not-init guidance is deterministic."""
    from moneybin import config

    original = config._current_profile  # pyright: ignore[reportPrivateUsage]
    config._current_profile = None  # pyright: ignore[reportPrivateUsage]
    try:
        yield
    finally:
        config._current_profile = original  # pyright: ignore[reportPrivateUsage]


def test_db_not_initialized_unregistered_points_at_profile_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.config import set_current_profile
    from moneybin.database import DatabaseNotInitializedError

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    set_current_profile("ghost")  # active profile, but no config.yaml under tmp_path

    result = classify_user_error(DatabaseNotInitializedError("missing"))
    assert result is not None
    assert "profile create" in result.message
    assert result.code == error_codes.INFRA_DATABASE_NOT_INITIALIZED


def test_db_not_initialized_bare_directory_does_not_point_at_profile_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A directory that exists but was never registered: `profile create` refuses on
    # the directory alone, so pointing there would dead-end the user. `db init`
    # works, so that is what the message must say.
    from moneybin.config import set_current_profile
    from moneybin.database import DatabaseNotInitializedError

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    (tmp_path / "profiles" / "bare").mkdir(parents=True)  # no config.yaml, no db
    set_current_profile("bare")

    result = classify_user_error(DatabaseNotInitializedError("missing"))
    assert result is not None
    assert "profile create" not in result.message
    assert "db init" in result.message.lower()


def test_db_not_initialized_registered_points_at_db_init(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from unittest.mock import patch as _patch

    from moneybin.config import set_current_profile
    from moneybin.database import DatabaseNotInitializedError
    from moneybin.services.profile_service import ProfileService

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    with _patch.object(ProfileService, "_init_database"):
        ProfileService().create("real")  # registered: config.yaml written
    set_current_profile("real")

    result = classify_user_error(DatabaseNotInitializedError("missing"))
    assert result is not None
    assert "db init" in result.message.lower()
