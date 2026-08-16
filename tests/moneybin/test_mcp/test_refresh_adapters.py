"""Unit tests for refresh_envelope error/recovery-action surfacing."""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.mcp.adapters.refresh_adapters import refresh_envelope
from moneybin.privacy.payloads.system import RefreshRunPayload
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.rate_backfill import RateBackfillResult
from moneybin.services.refresh import RefreshResult, SelfHealRecord, expand_steps
from tests.moneybin.test_mcp.schema_assertions import (
    assert_recovery_actions_executable,
)


def _payload(env: ResponseEnvelope[Any]) -> RefreshRunPayload:
    """Narrow the envelope payload to the typed RefreshRunPayload refresh returns."""
    assert isinstance(env.data, RefreshRunPayload)
    return env.data


@pytest.mark.unit
def test_rate_fields_are_absent_when_the_step_did_not_run() -> None:
    """``rates_written`` is the only did-it-run signal the envelope carries.

    ``None`` and ``0`` mean different things — the step was skipped versus it
    ran and found nothing to fetch — and neither pair list can tell them apart,
    since both are empty either way.
    """
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0), requested=expand_steps(None)
    )

    assert _payload(env).rates_written is None
    assert _payload(env).rate_pairs_failed == []
    assert _payload(env).rate_pairs_unsupported == []


@pytest.mark.unit
def test_rate_backfill_counts_and_pairs_reach_the_envelope() -> None:
    """The two pair lists stay separate all the way to the agent.

    An agent reading this envelope decides what to tell the user, and the two
    lists carry opposite instructions: wait for the next refresh, or record the
    rate by hand. Collapsing them here would erase that distinction after the
    service went to the trouble of making it.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(
                rates_written=7,
                pairs_failed=("EUR/USD",),
                pairs_unsupported=("JPY/USD",),
            ),
        ),
        requested=expand_steps(None),
    )

    assert _payload(env).rates_written == 7
    assert _payload(env).rate_pairs_failed == ["EUR/USD"]
    assert _payload(env).rate_pairs_unsupported == ["JPY/USD"]


@pytest.mark.unit
def test_a_rates_step_that_found_nothing_is_not_a_skipped_step() -> None:
    """Zero written with no failures still proves the step ran."""
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(rates_written=0, pairs_failed=()),
        ),
        requested=expand_steps(None),
    )

    assert _payload(env).rates_written == 0
    assert _payload(env).rate_pairs_failed == []


@pytest.mark.unit
def test_envelope_includes_self_heal_actions_empty_by_default() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0), requested=expand_steps(None)
    )
    assert _payload(env).self_heal_actions == []
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
    assert _payload(env).self_heal_actions[0].recipe_id == (
        "orphan_categorizations_cleanup"
    )
    assert _payload(env).self_heal_actions[0].rows_affected == 2


@pytest.mark.unit
async def test_matching_error_yields_match_retry_and_doctor() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, matching_error="boom"),
        requested=expand_steps(None),
    )
    assert _payload(env).matching_error == "boom"
    actions = env.recovery_actions or []
    await assert_recovery_actions_executable(actions)
    tools = [(ra.tool, ra.arguments) for ra in actions]
    assert ("refresh_run", {"steps": ["match"]}) in tools
    assert (
        "system_status",
        {"sections": ["doctor"], "detail": "full"},
    ) in tools


@pytest.mark.unit
async def test_categorization_error_yields_categorize_retry_and_doctor() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, categorization_error="bang"),
        requested=expand_steps(None),
    )
    assert _payload(env).categorization_error == "bang"
    actions = env.recovery_actions or []
    await assert_recovery_actions_executable(actions)
    tools = [(ra.tool, ra.arguments) for ra in actions]
    assert ("refresh_run", {"steps": ["categorize"]}) in tools
    assert (
        "system_status",
        {"sections": ["doctor"], "detail": "full"},
    ) in tools


@pytest.mark.unit
async def test_both_errors_emit_single_doctor_action() -> None:
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
    await assert_recovery_actions_executable(actions)
    doctor = [
        ra
        for ra in actions
        if ra.tool == "system_status" and ra.arguments.get("sections") == ["doctor"]
    ]
    assert len(doctor) == 1
    # Match-retry first, categorize-retry second, doctor last (most-likely first).
    assert [ra.tool for ra in actions] == [
        "refresh_run",
        "refresh_run",
        "system_status",
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
    assert _payload(env).matching_error == "matcher boom"  # still surfaced in data


@pytest.mark.unit
def test_recovery_actions_are_idempotent() -> None:
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, matching_error="boom"),
        requested=expand_steps(None),
    )
    assert all(ra.idempotent for ra in env.recovery_actions or [])
