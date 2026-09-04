"""Unit tests for refresh_envelope error/recovery-action surfacing."""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.adapters.refresh_adapters import (
    refresh_envelope,
    refresh_rate_gap_hints,
    refresh_step_actions,
)
from moneybin.orchestration.refresh import RefreshResult, SelfHealRecord, expand_steps
from moneybin.privacy.payloads.system import RefreshRunPayload
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.rate_backfill import RateBackfillResult
from moneybin.services.refresh_outcome import RefreshStepOutcome
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
    from moneybin.adapters.refresh_adapters import (
        REFRESH_CATEGORIZE_FOLLOWUP_HINT,
    )

    env = refresh_envelope(
        RefreshResult(applied=False, duration_seconds=None, matching_error="boom"),
        requested=expand_steps(["match"]),
    )
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT not in env.actions


@pytest.mark.unit
def test_categorize_followup_still_fires_on_clean_match_only() -> None:
    """A clean match-only run still emits the categorize follow-up hint."""
    from moneybin.adapters.refresh_adapters import (
        REFRESH_CATEGORIZE_FOLLOWUP_HINT,
    )

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


@pytest.mark.unit
def test_a_clean_step_outcome_earns_no_recovery_actions() -> None:
    """Silent when nothing broke, so an action keeps the meaning of an action."""
    assert refresh_step_actions(None, apply_failed=False) == []
    assert (
        refresh_step_actions(RefreshStepOutcome(rates_written=0), apply_failed=False)
        == []
    )


@pytest.mark.unit
async def test_each_crashed_step_is_offered_the_retry_that_fits_it() -> None:
    """Each channel routes somewhere different, so each gets its own retry.

    The pairing is the point, not the presence: one "the refresh had problems"
    action would satisfy a naive check while sending an agent to re-run
    matching over a provider outage.
    """
    actions = refresh_step_actions(
        RefreshStepOutcome(
            matching_error="matcher blew up",
            categorization_error="categorizer blew up",
            rates_written=0,
            rate_backfill_error="rates blew up",
        ),
        apply_failed=False,
    )

    await assert_recovery_actions_executable(actions)
    assert [action.arguments.get("steps") for action in actions[:3]] == [
        ["match"],
        ["categorize"],
        ["rates"],
    ]
    # The diagnostic closes the list rather than competing with the retries.
    assert actions[-1].tool == "system_status"


@pytest.mark.unit
async def test_a_crashed_identity_pass_is_offered_its_own_retry() -> None:
    """The fourth channel `RefreshStepOutcome` carries, and the one that had no retry.

    ``identity`` is already a `RefreshStep`, so ``refresh_run(steps=["identity"])``
    needs nothing new to be executable — the omission was the builder's, not the
    contract's. An identity-only crash was the one failure that reached an agent
    with a populated error field and an empty action list.
    """
    actions = refresh_step_actions(
        RefreshStepOutcome(identity_errors=("account identity pass blew up",)),
        apply_failed=False,
    )

    await assert_recovery_actions_executable(actions)
    assert [action.arguments.get("steps") for action in actions] == [
        ["identity"],
        None,
    ]


@pytest.mark.unit
async def test_retries_are_offered_in_the_order_refresh_runs_them() -> None:
    """Identity sits between categorize and rates, as it does in `CANONICAL_STEPS`.

    Ordering is load-bearing: the list is "most-likely-correct first", and an
    agent that re-runs them top-down must not run a later step before an earlier
    one it depends on.
    """
    actions = refresh_step_actions(
        RefreshStepOutcome(
            matching_error="matcher blew up",
            categorization_error="categorizer blew up",
            identity_errors=("identity blew up",),
            rate_backfill_error="rates blew up",
        ),
        apply_failed=False,
    )

    await assert_recovery_actions_executable(actions)
    assert [action.arguments.get("steps") for action in actions[:4]] == [
        ["match"],
        ["categorize"],
        ["identity"],
        ["rates"],
    ]


@pytest.mark.unit
def test_a_pair_no_retry_can_fill_still_names_its_remedy() -> None:
    """Withholding the futile retry must not leave the gap with no next step.

    These two pair lists earn no executable action on purpose — re-running the
    step returns the identical unusable answer. But `moneybin fx set` is a CLI
    command, so it can never be a `RecoveryAction`, and without an ordinary hint
    the agent receives a populated field naming a permanent gap and nothing at
    all about how the user closes it. The CLI has warned this all along.
    """
    unsupported = refresh_rate_gap_hints(
        RefreshStepOutcome(rate_pairs_unsupported=("XBT/USD",))
    )
    assert any("fx set" in hint for hint in unsupported)

    discarded = refresh_rate_gap_hints(
        RefreshStepOutcome(rate_pairs_discarded=("JPY/USD",))
    )
    assert discarded, "short coverage is hedged, but it is still not nothing"
    assert not any("fx set" in hint for hint in discarded), (
        "a discarded pair mostly stored; pointing at a manual override overstates it"
    )


@pytest.mark.unit
def test_pairs_that_are_merely_retryable_earn_no_manual_remedy() -> None:
    """Negative twin: a failed pair's remedy is the retry it already gets."""
    assert (
        refresh_rate_gap_hints(RefreshStepOutcome(rate_pairs_failed=("EUR/USD",))) == []
    )
    assert refresh_rate_gap_hints(RefreshStepOutcome(rates_written=3)) == []
    assert refresh_rate_gap_hints(None) == []


@pytest.mark.unit
def test_the_unpublished_remedy_reaches_the_refresh_envelope() -> None:
    """`refresh_run`'s own envelope carried the field but never the remedy.

    Its tool *description* explains `rate_pairs_unsupported`, which is static
    prose the agent read at connect; the response that actually names the pair
    said nothing about what to do next.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(
                rates_written=0, pairs_failed=(), pairs_unsupported=("XBT/USD",)
            ),
        ),
        requested=expand_steps(None),
    )

    assert any("fx set" in action for action in (env.actions or []))


@pytest.mark.unit
def test_a_failed_apply_withholds_every_step_retry() -> None:
    """The blocker outranks the steps it took down with it.

    A step retry offered beside a failed SQLMesh apply sends the agent to chase
    the secondary crash while the thing that caused it stays unfixed — and the
    retry would run against the same broken warehouse and fail the same way.
    `refresh_envelope` has always suppressed this; the parameter exists so the
    surfaces that embed a refresh cannot answer it differently.
    """
    crashed = RefreshStepOutcome(
        matching_error="matcher blew up",
        identity_errors=("identity blew up",),
        rate_backfill_error="rates blew up",
    )

    assert refresh_step_actions(crashed, apply_failed=True) == []
    assert refresh_step_actions(crashed, apply_failed=False), (
        "the same outcome must still earn retries when the apply survived"
    )


@pytest.mark.unit
def test_a_pair_the_provider_never_answered_is_offered_a_retry() -> None:
    """``rate_pairs_failed`` is retryable even with no step crash beside it.

    The rates step can return without raising and still have left a pair
    unfetched, so gating the retry on ``rate_backfill_error`` alone would drop
    the action in a case a later run does fix.
    """
    actions = refresh_step_actions(
        RefreshStepOutcome(rate_pairs_failed=("EUR/USD",)), apply_failed=False
    )

    assert [action.arguments.get("steps") for action in actions] == [["rates"], None]


@pytest.mark.unit
def test_pairs_a_retry_cannot_fill_are_offered_no_retry() -> None:
    """Unsupported and short-coverage pairs have remedies a re-run is not.

    The provider answered for both, so re-sending returns the identical
    unusable value — an executable retry here is a loop with no terminating
    condition. They ride the payload fields, which is why those exist.
    """
    assert (
        refresh_step_actions(
            RefreshStepOutcome(
                rates_written=3,
                rate_pairs_unsupported=("XBT/USD",),
                rate_pairs_discarded=("JPY/USD",),
            ),
            apply_failed=False,
        )
        == []
    )
