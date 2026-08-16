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
    assert _payload(env).rate_pairs_discarded == []


@pytest.mark.unit
def test_rate_backfill_counts_and_pairs_reach_the_envelope() -> None:
    """The three pair lists stay separate all the way to the agent.

    An agent reading this envelope decides what to tell the user, and the lists
    carry different instructions: wait for the next refresh, record the rate by
    hand, or expect gaps on some dates of an otherwise-filled pair. Collapsing
    any two here would erase a distinction the service went to the trouble of
    making.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(
                rates_written=7,
                pairs_failed=("EUR/USD",),
                pairs_unsupported=("JPY/USD",),
                pairs_discarded=("GBP/USD",),
            ),
        ),
        requested=expand_steps(None),
    )

    assert _payload(env).rates_written == 7
    assert _payload(env).rate_pairs_failed == ["EUR/USD"]
    assert _payload(env).rate_pairs_unsupported == ["JPY/USD"]
    assert _payload(env).rate_pairs_discarded == ["GBP/USD"]


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
async def test_a_failed_rate_pair_offers_the_rates_retry() -> None:
    """A transient rate failure earns the same retry the sibling steps get.

    ``matching_error`` and ``categorization_error`` each hand the agent a
    ``refresh_run(steps=[...])`` it can execute, and the CLI already prints the
    equivalent hint for a failed pair because ``retryable_error`` counts it. An
    MCP-driven agent was the only caller left to infer the next step, which is
    the CLI/MCP parity gap this closes.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(
                rates_written=0, pairs_failed=("EUR/USD",)
            ),
        ),
        requested=expand_steps(None),
    )

    actions = env.recovery_actions or []
    await assert_recovery_actions_executable(actions)
    tools = [(ra.tool, ra.arguments) for ra in actions]
    assert ("refresh_run", {"steps": ["rates"]}) in tools
    assert ("system_status", {"sections": ["doctor"], "detail": "full"}) in tools


@pytest.mark.unit
async def test_a_crashed_rates_step_reaches_the_envelope_with_a_retry() -> None:
    """A crash is a distinct signal from an empty result, and earns the retry.

    ``rate_backfill=None`` is what the payload defines as "the step did not
    run", so a crash reported only that way is indistinguishable from a profile
    with no home currency — the agent sees ``rates_written=null``, three empty
    pair lists, and no reason to act. ``rate_backfill_error`` is the field that
    separates them, and it earns a retry for the same reason
    ``matching_error`` and ``categorization_error`` do.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=None,
            rate_backfill_error="Rate backfill failed — the cause is in the local log",
        ),
        requested=expand_steps(None),
    )

    assert env.data.rate_backfill_error is not None
    assert env.data.rates_written is None
    actions = env.recovery_actions or []
    await assert_recovery_actions_executable(actions)
    tools = [(ra.tool, ra.arguments) for ra in actions]
    assert ("refresh_run", {"steps": ["rates"]}) in tools
    assert ("system_status", {"sections": ["doctor"], "detail": "full"}) in tools


@pytest.mark.unit
def test_a_crashed_rates_step_offers_the_retry_exactly_once() -> None:
    """A crash that also left failed pairs must not queue two identical retries.

    Both conditions select the same ``refresh_run(steps=["rates"])``; emitting
    it twice would make the agent run the step a second time for no reason.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(
                rates_written=0, pairs_failed=("EUR/USD",)
            ),
            rate_backfill_error="Rate backfill failed — the cause is in the local log",
        ),
        requested=expand_steps(None),
    )

    actions = env.recovery_actions or []
    rate_retries = [
        ra
        for ra in actions
        if (ra.tool, ra.arguments) == ("refresh_run", {"steps": ["rates"]})
    ]
    assert len(rate_retries) == 1


@pytest.mark.unit
def test_a_clean_rates_step_reports_no_error() -> None:
    """Negative twin: the new field stays absent on the paths that did not fail."""
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(rates_written=3, pairs_failed=()),
        ),
        requested=expand_steps(None),
    )

    assert env.data.rate_backfill_error is None
    assert env.recovery_actions is None


@pytest.mark.unit
def test_an_unsupported_pair_is_offered_no_retry() -> None:
    """Negative twin: retrying never fills a pair the provider does not publish.

    The CLI keeps this out of ``retryable_error`` for exactly this reason, and
    its remedy (``moneybin fx set``) already rode the warning. An executable
    retry here would send the agent around a loop that cannot terminate.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(
                rates_written=0, pairs_failed=(), pairs_unsupported=("JPY/USD",)
            ),
        ),
        requested=expand_steps(None),
    )

    assert env.recovery_actions is None


@pytest.mark.unit
def test_a_discarded_pair_is_offered_no_retry() -> None:
    """Negative twin: the provider answered, so the same request returns the same.

    A discarded rate was unusable on arrival — dated outside the window, or too
    small for the column — so re-sending produces the identical unusable value.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(
                rates_written=3, pairs_failed=(), pairs_discarded=("GBP/USD",)
            ),
        ),
        requested=expand_steps(None),
    )

    assert env.recovery_actions is None


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


@pytest.mark.unit
def test_envelope_discloses_what_the_match_step_decided() -> None:
    """A refresh that merges rows or undoes a transfer must say so.

    The match step auto-merges above the confidence threshold without asking
    and reverses transfers a dedup collapse invalidated. Both are decisions the
    user did not make, and until these keys existed only the two merge-accept
    tools reported them — a plain ``refresh_run`` after an import returned an
    ordinary success.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            matches_auto_merged=3,
            matches_pending_review=2,
            matches_pending_transfers=1,
            transfers_retired=4,
        ),
        requested=expand_steps(None),
    )
    payload = _payload(env)
    assert payload.matches_auto_merged == 3
    assert payload.matches_pending_review == 2
    assert payload.matches_pending_transfers == 1
    assert payload.transfers_retired == 4
    assert payload.matching_skipped is False


@pytest.mark.unit
def test_envelope_routes_a_retirement_to_the_way_back() -> None:
    """The count is not the disclosure; the route back is.

    ``transactions_matches_run`` already pairs its count with the audit/undo
    route, and refresh is the surface most users reach the reconciliation
    through — it runs the matcher on every import. Reporting the number here
    with no action left the primary path stating that an accepted transfer was
    undone while saying nothing about how to restore it.
    """
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, transfers_retired=4),
        requested=expand_steps(None),
    )
    retirement = [a for a in env.actions if "4" in a and "system_audit_undo" in a]
    assert retirement, f"no action names the retirement or its way back: {env.actions}"


@pytest.mark.unit
def test_envelope_stays_quiet_about_undo_when_nothing_was_retired() -> None:
    """Negative twin: an ordinary refresh must not imply a decision was undone."""
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, transfers_retired=0),
        requested=expand_steps(None),
    )
    assert not [a for a in env.actions if "system_audit_undo" in a]


@pytest.mark.unit
def test_envelope_marks_zero_counts_as_unexamined_when_match_was_skipped() -> None:
    """``matching_skipped`` is what separates an honest zero from an invented one.

    The counts are zero on a skipped step because nothing was examined, not
    because nothing was found. Without this key an agent reads the same payload
    as "no duplicates" — which is the claim the flag exists to refuse.
    """
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, matching_skipped=True),
        requested=expand_steps(None),
    )
    payload = _payload(env)
    assert payload.matching_skipped is True
    assert payload.matches_auto_merged == 0
