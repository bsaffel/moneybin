"""Tests for error envelope building (formerly tested handle_mcp_errors decorator).

The handle_mcp_errors decorator was deleted in the fastmcp 3.x migration.
Error handling is now folded into the mcp_tool decorator. Tests for the new
mcp_tool error-handling behavior live in tests/mcp/test_error_handling.py.

This file retains envelope construction and serialization tests that belong
in the moneybin unit test tree.
"""

import json

from moneybin.errors import UserError
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)


def test_build_error_envelope_carries_user_error() -> None:
    """build_error_envelope returns an envelope with empty data and the error set."""
    err = UserError("bad", code="database_locked", hint="unlock it")
    env = build_error_envelope(error=err)
    assert isinstance(env, ResponseEnvelope)
    assert env.error is err
    assert env.data == []
    assert env.summary.total_count == 0
    assert env.summary.sensitivity == "low"


def test_envelope_to_dict_includes_error_section() -> None:
    """Serialized envelope surfaces the error fields under an `error` key."""
    err = UserError("bad", code="x", hint="y")
    env = build_error_envelope(error=err)
    d = env.to_dict()
    assert d["error"] == {"message": "bad", "code": "x", "hint": "y"}
    assert d["data"] == []


def test_envelope_to_dict_omits_error_when_none() -> None:
    """Successful envelopes do not carry an `error` key in their dict."""
    env = build_envelope(data=[{"x": 1}], sensitivity="low")
    assert "error" not in env.to_dict()


def test_error_envelope_round_trips_through_json() -> None:
    """to_json + json.loads recovers the error section structure."""
    err = UserError("m", code="c", hint="h")
    parsed = json.loads(build_error_envelope(error=err).to_json())
    assert parsed["error"] == {"message": "m", "code": "c", "hint": "h"}
    assert parsed["data"] == []
