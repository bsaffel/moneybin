"""Tests for the MCP-side error handler decorator and error envelope."""

import json

import pytest

from moneybin.database import DatabaseKeyError
from moneybin.errors import UserError
from moneybin.mcp.error_handler import handle_mcp_errors
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    SummaryMeta,
    build_envelope,
    build_error_envelope,
)


def test_build_error_envelope_carries_user_error() -> None:
    """build_error_envelope returns an envelope with empty data and the error set."""
    err = UserError(message="bad", code="database_locked", hint="unlock it")
    env = build_error_envelope(error=err)
    assert isinstance(env, ResponseEnvelope)
    assert env.error is err
    assert env.data == []
    assert env.summary.total_count == 0
    assert env.summary.sensitivity == "low"


def test_envelope_to_dict_includes_error_section() -> None:
    """Serialized envelope surfaces the error fields under an `error` key."""
    err = UserError(message="bad", code="x", hint="y")
    env = build_error_envelope(error=err)
    d = env.to_dict()
    assert d["error"] == {"message": "bad", "code": "x", "hint": "y"}
    assert d["data"] == []


def test_envelope_to_dict_omits_error_when_none() -> None:
    """Successful envelopes do not carry an `error` key in their dict."""
    env = build_envelope(data=[{"x": 1}], sensitivity="low")
    assert "error" not in env.to_dict()


def test_handle_mcp_errors_returns_envelope_on_classified_exception() -> None:
    """A classified exception becomes an error envelope."""

    @handle_mcp_errors
    def tool() -> ResponseEnvelope:
        raise FileNotFoundError("missing.csv")

    result = tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "file_not_found"
    assert "missing.csv" in result.error.message


def test_handle_mcp_errors_passes_through_success() -> None:
    """Successful returns are not modified by the decorator."""
    success = ResponseEnvelope(
        summary=SummaryMeta(total_count=1, returned_count=1),
        data=[{"k": "v"}],
    )

    @handle_mcp_errors
    def tool() -> ResponseEnvelope:
        return success

    assert tool() is success


def test_handle_mcp_errors_reraises_unknown_exceptions() -> None:
    """Unclassified exceptions propagate so the framework can return 500-equivalents."""

    @handle_mcp_errors
    def tool() -> ResponseEnvelope:
        raise RuntimeError("internal bug")

    with pytest.raises(RuntimeError, match="internal bug"):
        tool()


def test_handle_mcp_errors_classifies_database_key_error() -> None:
    """DatabaseKeyError produces an envelope with the database_locked code."""

    @handle_mcp_errors
    def tool() -> ResponseEnvelope:
        raise DatabaseKeyError("locked")

    result = tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "database_locked"


def test_error_envelope_round_trips_through_json() -> None:
    """to_json + json.loads recovers the error section structure."""
    err = UserError(message="m", code="c", hint="h")
    parsed = json.loads(build_error_envelope(error=err).to_json())
    assert parsed["error"] == {"message": "m", "code": "c", "hint": "h"}
    assert parsed["data"] == []
