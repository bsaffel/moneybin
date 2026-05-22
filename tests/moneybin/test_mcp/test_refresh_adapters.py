"""Unit tests for refresh_envelope error/recovery-action surfacing."""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.mcp.adapters.refresh_adapters import refresh_envelope
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.refresh import RefreshResult, SelfHealRecord, expand_steps


def _data(env: ResponseEnvelope) -> dict[str, Any]:
    """Narrow the envelope payload to the dict refresh always returns."""
    assert isinstance(env.data, dict)
    return env.data


@pytest.mark.unit
def test_envelope_includes_self_heal_actions_empty_by_default() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0), requested=expand_steps(None)
    )
    assert _data(env)["self_heal_actions"] == []
    assert env.recovery_actions is None


@pytest.mark.unit
def test_envelope_serializes_self_heal_records() -> None:
    rec = SelfHealRecord(
        recipe_id="orphan_categorizations_cleanup",
        rows_affected=2,
        operation_id="op_self_heal_orphan_categorizations_cleanup_x",
        timestamp="2026-05-22T00:00:00Z",
    )
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, self_heal_actions=(rec,)),
        requested=expand_steps(None),
    )
    assert _data(env)["self_heal_actions"][0]["recipe_id"] == (
        "orphan_categorizations_cleanup"
    )
    assert _data(env)["self_heal_actions"][0]["rows_affected"] == 2


@pytest.mark.unit
def test_matching_error_yields_match_retry_and_doctor() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, matching_error="boom"),
        requested=expand_steps(None),
    )
    assert _data(env)["matching_error"] == "boom"
    tools = [(ra.tool, ra.arguments) for ra in env.recovery_actions or []]
    assert ("refresh_run", {"steps": ["match"]}) in tools
    # system_doctor takes no MCP parameters — args must be empty to stay executable.
    assert ("system_doctor", {}) in tools


@pytest.mark.unit
def test_categorization_error_yields_categorize_retry_and_doctor() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, categorization_error="bang"),
        requested=expand_steps(None),
    )
    assert _data(env)["categorization_error"] == "bang"
    tools = [(ra.tool, ra.arguments) for ra in env.recovery_actions or []]
    assert ("refresh_run", {"steps": ["categorize"]}) in tools
    assert ("system_doctor", {}) in tools


@pytest.mark.unit
def test_both_errors_emit_single_doctor_action() -> None:
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            matching_error="boom",
            categorization_error="bang",
        ),
        requested=expand_steps(None),
    )
    actions = env.recovery_actions or []
    doctor = [ra for ra in actions if ra.tool == "system_doctor"]
    assert len(doctor) == 1
    # Match-retry first, categorize-retry second, doctor last (most-likely first).
    assert [ra.tool for ra in actions] == [
        "refresh_run",
        "refresh_run",
        "system_doctor",
    ]
    assert all(ra.confidence == "suggested" for ra in actions)


@pytest.mark.unit
def test_categorize_followup_suppressed_when_matcher_crashed() -> None:
    """A matcher crash suppresses the 'run categorize' hint (recovery says retry match)."""
    from moneybin.mcp.adapters.refresh_adapters import REFRESH_CATEGORIZE_FOLLOWUP_HINT

    env = refresh_envelope(
        RefreshResult(applied=False, duration_seconds=None, matching_error="boom"),
        requested=expand_steps(["match"]),
    )
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT not in env.actions


@pytest.mark.unit
def test_categorize_followup_still_fires_on_clean_match_only() -> None:
    """A clean match-only run still emits the categorize follow-up hint."""
    from moneybin.mcp.adapters.refresh_adapters import REFRESH_CATEGORIZE_FOLLOWUP_HINT

    env = refresh_envelope(
        RefreshResult(applied=False, duration_seconds=None),
        requested=expand_steps(["match"]),
    )
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT in env.actions


@pytest.mark.unit
def test_apply_failure_suppresses_step_recovery_actions() -> None:
    """When apply failed, the apply error is the blocker — don't emit step retries.

    A matcher crash can co-occur with an apply failure (match runs before
    transform). Surfacing 'retry match' would misdirect the agent from the
    blocking apply error, which is carried by error + the apply-failed hint.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=False,
            duration_seconds=1.0,
            error="model boom",
            matching_error="matcher boom",
        ),
        requested=expand_steps(None),
    )
    assert env.recovery_actions is None
    assert _data(env)["matching_error"] == "matcher boom"  # still surfaced in data


@pytest.mark.unit
def test_recovery_actions_are_idempotent() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, matching_error="boom"),
        requested=expand_steps(None),
    )
    assert all(ra.idempotent for ra in env.recovery_actions or [])
