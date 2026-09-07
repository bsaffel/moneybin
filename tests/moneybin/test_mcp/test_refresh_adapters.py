"""Unit tests for refresh_envelope error/recovery-action surfacing."""

from __future__ import annotations

import json
from typing import Any

import pytest

from moneybin.adapters.refresh_adapters import (
    refresh_envelope,
    refresh_rate_gap_hints,
    refresh_step_actions,
    refresh_steps_fields,
)
from moneybin.orchestration.refresh import RefreshResult, SelfHealRecord, expand_steps
from moneybin.privacy.payloads.system import RefreshRunPayload, RefreshStageRow
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.rate_backfill import RateBackfillResult
from moneybin.services.refresh_outcome import RefreshStepOutcome, StageOutcome
from tests.moneybin.test_mcp.schema_assertions import (
    assert_recovery_actions_executable,
)


def _payload(env: ResponseEnvelope[Any]) -> RefreshRunPayload:
    """Narrow the envelope payload to the typed RefreshRunPayload refresh returns."""
    assert isinstance(env.data, RefreshRunPayload)
    return env.data


def _stage(env: ResponseEnvelope[Any], step: str) -> RefreshStageRow | None:
    """The payload row for one step, or None when the step has no entry."""
    return next((s for s in _payload(env).stages if s.step == step), None)


def _rates_stage(written: int | None = None, error: str | None = None) -> StageOutcome:
    """A rates step, in the shape ``refresh`` builds it.

    ``written=None`` is the step that declined to run — it needs a home currency
    and built views — which comes back ``ran=False`` carrying no count at all.
    """
    if written is None:
        return StageOutcome(step="rates", ran=False, error=error)
    return StageOutcome(
        step="rates", ran=True, counts={"rates_written": written}, error=error
    )


def _match_stage(
    ran: bool = True, error: str | None = None, **counts: int
) -> StageOutcome:
    """A match step that ran (or declined), with the counts it reported."""
    return StageOutcome(
        step="match", ran=ran, counts={} if not ran else counts, error=error
    )


@pytest.mark.unit
def test_rate_fields_are_absent_when_the_step_did_not_run() -> None:
    """A step with no entry never ran, and reports no count at all.

    An absent stage and a stage reporting zero mean different things — the step
    was never asked for versus it ran and found nothing to fetch — and neither
    pair list can tell them apart, since both are empty either way.
    """
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0), requested=expand_steps(None)
    )

    assert _stage(env, "rates") is None
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
            stages=(_rates_stage(7),),
        ),
        requested=expand_steps(None),
    )

    rates = _stage(env, "rates")
    assert rates is not None and rates.counts["rates_written"] == 7
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
            stages=(_rates_stage(0),),
        ),
        requested=expand_steps(None),
    )

    rates = _stage(env, "rates")
    assert rates is not None
    assert rates.ran is True
    assert rates.counts["rates_written"] == 0
    assert _payload(env).rate_pairs_failed == []


@pytest.mark.unit
async def test_a_failed_rate_pair_offers_the_rates_retry() -> None:
    """A transient rate failure earns the same retry the sibling steps get.

    A crashed match or categorize step each hands the agent a
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
            stages=(_rates_stage(0),),
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

    A null backfill is what "the step did not run" looks like, so a crash
    reported only that way is indistinguishable from a profile with no home
    currency — the agent sees no count, three empty pair lists, and no reason to
    act. The stage's own ``error`` is what separates them, and it earns a retry
    for the same reason a crashed match or categorize step does.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=None,
            stages=(
                _rates_stage(
                    error="Rate backfill failed — the cause is in the local log"
                ),
            ),
        ),
        requested=expand_steps(None),
    )

    rates = _stage(env, "rates")
    assert rates is not None
    assert rates.error is not None
    assert rates.counts == {}
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
            stages=(
                _rates_stage(
                    0, error="Rate backfill failed — the cause is in the local log"
                ),
            ),
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
    """Negative twin: the stage's error stays absent on paths that did not fail."""
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=RateBackfillResult(rates_written=3, pairs_failed=()),
            stages=(_rates_stage(3),),
        ),
        requested=expand_steps(None),
    )

    rates = _stage(env, "rates")
    assert rates is not None and rates.error is None
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
            stages=(_rates_stage(0),),
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
            stages=(_rates_stage(3),),
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
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            stages=(_match_stage(error="boom"),),
        ),
        requested=expand_steps(None),
    )
    match = _stage(env, "match")
    assert match is not None and match.error == "boom"
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
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            stages=(StageOutcome(step="categorize", ran=True, error="bang"),),
        ),
        requested=expand_steps(None),
    )
    categorize = _stage(env, "categorize")
    assert categorize is not None and categorize.error == "bang"
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
            stages=(
                _match_stage(error="boom"),
                StageOutcome(step="categorize", ran=True, error="bang"),
            ),
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
        RefreshResult(
            applied=False,
            duration_seconds=None,
            stages=(_match_stage(error="boom"),),
        ),
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
            stages=(_match_stage(error="matcher boom"),),
        ),
        requested=expand_steps(None),
    )
    assert env.recovery_actions is None
    match = _stage(env, "match")
    # Still surfaced in data — withholding the retry is not withholding the fact.
    assert match is not None and match.error == "matcher boom"


@pytest.mark.unit
def test_recovery_actions_are_idempotent() -> None:
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            stages=(_match_stage(error="boom"),),
        ),
        requested=expand_steps(None),
    )
    assert all(ra.idempotent for ra in env.recovery_actions or [])


@pytest.mark.unit
def test_envelope_discloses_what_the_match_step_decided() -> None:
    """A refresh that merges rows or undoes a transfer must say so.

    The match step auto-merges above the confidence threshold without asking
    and reverses transfers a dedup collapse invalidated. Both are decisions the
    user did not make, and until the step reported them only the two
    merge-accept tools did — a plain ``refresh_run`` after an import returned an
    ordinary success.

    ``transfers_retired`` is asserted on the payload rather than in the match
    stage's counts because it is an operation total: ``accounts_links_set`` adds
    retirements the matcher never saw, so the stage must not claim it produced
    the summed number.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            stages=(
                _match_stage(auto_merged=3, pending_review=2, pending_transfers=1),
            ),
            transfers_retired=4,
        ),
        requested=expand_steps(None),
    )
    match = _stage(env, "match")
    assert match is not None
    assert match.ran is True
    assert match.counts == {
        "auto_merged": 3,
        "pending_review": 2,
        "pending_transfers": 1,
    }
    assert "transfers_retired" not in match.counts
    assert _payload(env).transfers_retired == 4


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
    """``ran`` is what separates an honest zero from an invented one.

    A step that declined to run reports no counts at all rather than a set of
    zeros, because the zeros would say nothing was found when nothing was
    examined — the claim ``ran=False`` exists to refuse. An agent reading the
    counts alone would report "no duplicates" over rows the matcher never saw.
    """
    env = refresh_envelope(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            stages=(_match_stage(ran=False),),
        ),
        requested=expand_steps(None),
    )
    match = _stage(env, "match")
    assert match is not None
    assert match.ran is False
    assert match.counts == {}


@pytest.mark.unit
def test_a_clean_step_outcome_earns_no_recovery_actions() -> None:
    """Silent when nothing broke, so an action keeps the meaning of an action."""
    assert refresh_step_actions(None, apply_failed=False) == []
    assert (
        refresh_step_actions(
            RefreshStepOutcome(stages=(_rates_stage(0),)), apply_failed=False
        )
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
            stages=(
                _match_stage(error="matcher blew up"),
                StageOutcome(step="categorize", ran=True, error="categorizer blew up"),
                _rates_stage(0, error="rates blew up"),
            )
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
            stages=(
                _match_stage(error="matcher blew up"),
                StageOutcome(step="categorize", ran=True, error="categorizer blew up"),
                _rates_stage(error="rates blew up"),
            ),
            identity_errors=("identity blew up",),
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
    assert refresh_rate_gap_hints(RefreshStepOutcome(stages=(_rates_stage(3),))) == []
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
            stages=(_rates_stage(0),),
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
        stages=(
            _match_stage(error="matcher blew up"),
            _rates_stage(error="rates blew up"),
        ),
        identity_errors=("identity blew up",),
    )

    assert refresh_step_actions(crashed, apply_failed=True) == []
    assert refresh_step_actions(crashed, apply_failed=False), (
        "the same outcome must still earn retries when the apply survived"
    )


@pytest.mark.unit
def test_a_pair_the_provider_never_answered_is_offered_a_retry() -> None:
    """``rate_pairs_failed`` is retryable even with no step crash beside it.

    The rates step can return without raising and still have left a pair
    unfetched, so gating the retry on the stage's own error alone would drop the
    action in a case a later run does fix.
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
                stages=(_rates_stage(3),),
                rate_pairs_unsupported=("XBT/USD",),
                rate_pairs_discarded=("JPY/USD",),
            ),
            apply_failed=False,
        )
        == []
    )


@pytest.mark.unit
def test_a_refresh_counts_as_one_outcome_however_many_stages_ran() -> None:
    """``returned_count`` counts rows returned, and a refresh returns no rows.

    ``stages`` is a diagnostic list like ``self_heal_actions`` beside it: it
    reports what the run did, not a second set of rows. Left out of
    ``AUXILIARY_LIST_FIELDS`` it becomes the payload's only primary list, and
    the row-count heuristic reports one "row" per pipeline step — a six-step
    refresh claiming it returned six of something.
    """
    stages = tuple(
        StageOutcome(step=step, ran=True) for step in expand_steps(None) or ()
    )
    env = refresh_envelope(
        RefreshResult(applied=True, duration_seconds=1.0, stages=stages),
        requested=expand_steps(None),
    )

    assert len(stages) == 6
    assert env.to_dict()["summary"]["returned_count"] == 1


@pytest.mark.unit
def test_the_flattened_field_names_are_pinned() -> None:
    """Four public envelopes are built by splatting this dict; nothing checks it.

    ``**refresh_steps_fields(...)`` spreads into a typed payload, so a key that
    stops being emitted is a field that stops being populated — and the payload
    still constructs, because every one of these carries a default. All four
    surfaces would drop the same key silently and stay green. Pin the set here
    so removing one is a decision rather than an accident.
    """
    assert set(refresh_steps_fields(None)) == {
        "identity_errors",
        "rate_pairs_failed",
        "rate_pairs_unsupported",
        "rate_pairs_discarded",
        "stages",
    }


@pytest.mark.unit
def test_a_skipped_step_survives_the_flattening_as_a_skip() -> None:
    """An embedded caller is owed the same three states the refresh carrier has.

    Never requested, requested-and-declined, and ran-and-found-nothing are
    three different facts, and the flattened shape has to keep them apart: a
    match step that declined for missing views leaves the same zero counts as
    one that examined every row and found no duplicates. Reporting the first as
    the second claims there are no duplicates among rows nothing read.
    """
    declined = refresh_steps_fields(
        RefreshStepOutcome(stages=(StageOutcome(step="match", ran=False),))
    )
    clean = refresh_steps_fields(
        RefreshStepOutcome(
            stages=(StageOutcome(step="match", ran=True, counts={"auto_merged": 0}),)
        )
    )

    assert declined["stages"] == [
        RefreshStageRow(step="match", ran=False, counts={}, error=None)
    ]
    assert clean["stages"] == [
        RefreshStageRow(step="match", ran=True, counts={"auto_merged": 0}, error=None)
    ]
    assert refresh_steps_fields(None)["stages"] == []


@pytest.mark.unit
def test_a_stage_survives_the_pydantic_carrier_it_rides_in() -> None:
    """``PullResult`` embeds this carrier, so a stage has to reach JSON intact.

    ``StageOutcome.counts`` defaulted to a ``MappingProxyType`` — safe for a
    plain dataclass, and a live break once ``stages`` joined the carrier a
    Pydantic model holds: ``model_dump`` passes the proxy straight through with
    only a warning, and ``json.dumps`` then refuses it outright. The failure is
    on the sync surface rather than in the adapter, which copies to a plain
    dict, so no test of the payload shape would have found it.
    """
    from moneybin.connectors.sync_models import PullResult

    pull = PullResult(
        job_id="job-1",
        transactions_loaded=0,
        accounts_loaded=0,
        balances_loaded=0,
        transactions_removed=0,
        institutions=[],
        refresh_steps=RefreshStepOutcome(
            stages=(StageOutcome(step="match", ran=False),)
        ),
    )

    assert json.loads(pull.model_dump_json())["refresh_steps"]["stages"] == [
        {"step": "match", "ran": False, "counts": {}, "error": None}
    ]
    # The proxy also guarded against a shared mutable default; `default_factory`
    # is what actually holds that line, so prove it still does.
    first, second = StageOutcome(step="a", ran=True), StageOutcome(step="b", ran=True)
    assert first.counts is not second.counts
