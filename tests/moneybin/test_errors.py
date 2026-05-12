"""Tests for the cross-cutting user-error classifier."""

from unittest.mock import patch

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
    assert result.code == "database_locked"
    assert "locked" in result.message
    assert result.hint == "Run: moneybin db unlock"


def test_classify_file_not_found_returns_user_error() -> None:
    """FileNotFoundError maps to a UserError with no hint."""
    result = classify_user_error(FileNotFoundError("missing.csv"))
    assert result is not None
    assert result.code == "file_not_found"
    assert "missing.csv" in result.message
    assert result.hint is None


def test_classify_lookup_error_returns_not_found() -> None:
    """LookupError maps to a UserError with code not_found."""
    result = classify_user_error(LookupError("note abc not found"))
    assert result is not None
    assert result.code == "not_found"
    assert "not found" in result.message


def test_classify_unknown_exception_returns_none() -> None:
    """Unrecognized exceptions return None so callers can re-raise."""
    assert classify_user_error(RuntimeError("internal bug")) is None


def test_classify_value_error_returns_user_error() -> None:
    """ValueError maps to a UserError so CLI date/decimal parse errors surface cleanly."""
    result = classify_user_error(ValueError("bad input"))
    assert result is not None
    assert result.code == "invalid_input"
    assert "bad input" in result.message


def test_user_error_to_dict_omits_none_hint() -> None:
    """UserError.to_dict drops the hint field when not set."""
    err = UserError("m", code="c")
    assert err.to_dict() == {"message": "m", "code": "c"}


def test_user_error_to_dict_includes_hint() -> None:
    """UserError.to_dict includes the hint when populated."""
    err = UserError("m", code="c", hint="h")
    assert err.to_dict() == {"message": "m", "code": "c", "hint": "h"}
