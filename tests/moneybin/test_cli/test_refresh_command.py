"""Unit tests for the `moneybin refresh` CLI command."""

from __future__ import annotations

import json
import logging
from types import MappingProxyType
from unittest.mock import MagicMock, patch

import pytest
from click.testing import Result
from typer.testing import CliRunner

from moneybin.adapters.refresh_adapters import REFRESH_CATEGORIZE_FOLLOWUP_HINT
from moneybin.cli.main import app
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services.rate_backfill import RateBackfillResult
from moneybin.services.refresh_outcome import StageOutcome


def test_refresh_json_success(runner: CliRunner) -> None:
    """Full-refresh JSON success exposes identity review next steps."""
    fake_result = RefreshResult(applied=True, duration_seconds=4.2, error=None)
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["applied"] is True
    assert payload["data"]["duration_seconds"] == 4.2
    assert payload["data"].get("error") is None
    # self_heal_actions is always emitted (empty until the safelist lands) so
    # agents see a stable key — guard that "always-present" contract.
    assert payload["data"]["self_heal_actions"] == []
    assert payload["data"]["identity_errors"] == []
    actions = " ".join(payload["actions"])
    assert 'reviews(kind="account_links")' in actions
    assert 'reviews(kind="merchant_links")' in actions


def test_refresh_json_failure_includes_action_hint(runner: CliRunner) -> None:
    """JSON output on apply failure must mirror the MCP tool's recovery hint."""
    fake_result = RefreshResult(applied=False, duration_seconds=1.1, error="model boom")
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--output", "json"])

    assert result.exit_code == 1, "apply failure must exit non-zero"
    payload = json.loads(result.stdout)
    assert payload["data"]["applied"] is False
    assert payload["data"]["error"] == "model boom"
    assert payload["actions"], "apply failure must emit a recovery hint"
    assert any("moneybin transform plan" in a for a in payload["actions"])
    assert all("moneybin_discover" not in a for a in payload["actions"])


def test_refresh_quiet_failure_exits_nonzero(runner: CliRunner) -> None:
    """Quiet mode must still exit non-zero on apply failure."""
    fake_result = RefreshResult(applied=False, duration_seconds=0.5, error="boom")
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--quiet"])

    assert result.exit_code == 1


def test_refresh_text_failure_exits_nonzero(runner: CliRunner) -> None:
    """Text mode logs the error and exits non-zero on apply failure."""
    fake_result = RefreshResult(applied=False, duration_seconds=0.5, error="model boom")
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 1


def test_refresh_step_transform_only(runner: CliRunner) -> None:
    """``--step transform`` runs only the transform step."""
    fake_result = RefreshResult(applied=True, duration_seconds=0.5, error=None)
    with (
        patch(
            "moneybin.orchestration.refresh.refresh", return_value=fake_result
        ) as svc,
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--step", "transform"])

    assert result.exit_code == 0
    assert svc.call_args.kwargs == {"steps": ["transform"]}


def test_refresh_step_identity_only(runner: CliRunner) -> None:
    """Identity proposal backfill is a selectable CLI refresh stage."""
    fake_result = RefreshResult(applied=False, duration_seconds=None)
    with (
        patch(
            "moneybin.orchestration.refresh.refresh", return_value=fake_result
        ) as svc,
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--step", "identity"])

    assert result.exit_code == 0, result.output
    assert svc.call_args.kwargs == {"steps": ["identity"]}


def test_refresh_step_repeatable(runner: CliRunner) -> None:
    """``--step match --step categorize`` collects into a list."""
    fake_result = RefreshResult(applied=False, duration_seconds=None, error=None)
    with (
        patch(
            "moneybin.orchestration.refresh.refresh", return_value=fake_result
        ) as svc,
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app, ["refresh", "--step", "match", "--step", "categorize"]
        )

    # applied=False with error=None (transform deliberately skipped) → exit 0.
    # The user got what they asked for; only genuine errors fail the command.
    assert result.exit_code == 0
    assert svc.call_args.kwargs == {"steps": ["match", "categorize"]}


def test_refresh_step_json_partial_cascade(runner: CliRunner) -> None:
    """``--step transform --output json`` returns the same envelope MCP returns."""
    fake_result = RefreshResult(applied=True, duration_seconds=0.7, error=None)
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app, ["refresh", "--step", "transform", "--output", "json"]
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["applied"] is True
    assert payload["data"]["duration_seconds"] == 0.7
    # No follow-up hint because transform was requested without match
    # (the hint fires only on match-without-categorize).
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT not in payload["actions"]


def test_refresh_step_match_without_categorize_emits_followup_hint(
    runner: CliRunner,
) -> None:
    """``--step match --output json`` emits the categorize follow-up hint."""
    fake_result = RefreshResult(applied=False, duration_seconds=None, error=None)
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--step", "match", "--output", "json"])

    # match-only → no transform but no error → exit 0.
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT in payload["actions"]


def _match_crash(
    *,
    applied: bool = True,
    duration_seconds: float = 2.0,
    error: str | None = None,
) -> RefreshResult:
    """A refresh whose match step raised, in the shape ``refresh`` records it.

    ``ran=True``: the step executed and then crashed, which is a different
    state from one that reached its precondition and declined. ``error`` is
    the apply step's — the only step that can fail the command.
    """
    return RefreshResult(
        applied=applied,
        duration_seconds=duration_seconds,
        error=error,
        stages=(StageOutcome(step="match", ran=True, error="matcher boom"),),
    )


def test_refresh_matcher_crash_surfaced_in_json(runner: CliRunner) -> None:
    """A matcher crash (best-effort) surfaces in JSON without failing the command."""
    fake_result = _match_crash()
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--output", "json"])

    assert result.exit_code == 0  # best-effort crash doesn't fail the command
    payload = json.loads(
        result.stdout
    )  # stdout stays clean JSON (warning is on stderr)
    stages = {s["step"]: s for s in payload["data"]["stages"]}
    assert stages["match"]["error"] == "matcher boom"
    recovery = {ra["tool"]: ra["arguments"] for ra in payload["recovery_actions"]}
    assert "refresh_run" in recovery
    assert recovery["system_status"] == {
        "sections": ["doctor"],
        "detail": "full",
    }
    # The crash is also surfaced as a stderr warning, not only in the payload.
    assert "Matching step failed" in result.output


def _rates_result(
    *,
    failed: tuple[str, ...] = (),
    unsupported: tuple[str, ...] = (),
    discarded: tuple[str, ...] = (),
) -> RefreshResult:
    """An applied refresh whose only complaint is about exchange rates."""
    return RefreshResult(
        applied=True,
        duration_seconds=2.0,
        rate_backfill=RateBackfillResult(
            rates_written=0,
            pairs_failed=failed,
            pairs_unsupported=unsupported,
            pairs_discarded=discarded,
        ),
    )


def _invoke_refresh(runner: CliRunner, result: RefreshResult, *args: str) -> Result:
    """Run `moneybin refresh` against a canned service result."""
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        return runner.invoke(app, ["refresh", *args])


def test_refresh_warns_when_the_rates_step_itself_crashed(runner: CliRunner) -> None:
    """A crashed rates step is a ⚠️, not silence, and withholds the ✅.

    Distinct from every pair-level warning below: the step never got far enough
    to name a pair, so all three pair lists are empty and the run would
    otherwise print a clean success banner over a step that failed outright.
    """
    out = _invoke_refresh(
        runner,
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            rate_backfill=None,
            # ran=False with an error is what the step reports when it never
            # produced a backfill: it crashed before naming a single pair.
            stages=(
                StageOutcome(
                    step="rates",
                    ran=False,
                    error="Rate backfill failed — the cause is in the local log",
                ),
            ),
        ),
    )

    assert out.exit_code == 0, "the rates step is best-effort, like its siblings"
    assert "Exchange rate backfill failed" in out.output
    assert "✅ Refresh complete" not in out.output


def test_refresh_unfilled_rate_pair_warns_and_withholds_the_success_banner(
    runner: CliRunner,
) -> None:
    """The ✅ must not print directly beneath a ⚠️ that contradicts it.

    The rates step is best-effort like matching and categorization, so it
    follows their rule: a warning and a clean success banner in the same output
    tell the user two different things about the same run.
    """
    out = _invoke_refresh(runner, _rates_result(failed=("EUR/USD",)))

    assert out.exit_code == 0
    assert "Exchange rates unavailable for EUR/USD" in out.output
    assert "✅ Refresh complete" not in out.output


def test_refresh_names_an_unsupported_pair_and_its_manual_remedy(
    runner: CliRunner,
) -> None:
    """A pair the provider never publishes is not fixed by running again.

    Retrying is the remedy for a failed pair and is useless for this one, so
    the line carries the only thing that does fill it — and the generic
    "re-run the failed step" hint is deliberately withheld, because following
    it would change nothing.
    """
    out = _invoke_refresh(runner, _rates_result(unsupported=("JPY/USD",)))

    assert out.exit_code == 0
    assert "No exchange rate series is published for JPY/USD" in out.output
    assert "moneybin fx set" in out.output
    assert "Re-run the failed step" not in out.output
    assert "✅ Refresh complete" not in out.output


def test_refresh_offers_a_retry_when_a_rate_pair_merely_failed(
    runner: CliRunner,
) -> None:
    """The negative control on the test above: a failed pair does earn the hint."""
    out = _invoke_refresh(runner, _rates_result(failed=("EUR/USD",)))

    assert "Re-run the failed step" in out.output


def test_refresh_rate_warnings_survive_quiet(runner: CliRunner) -> None:
    """-q suppresses status and ✅, never a warning — same rule as the matcher."""
    out = _invoke_refresh(runner, _rates_result(unsupported=("JPY/USD",)), "--quiet")

    assert out.exit_code == 0
    assert "No exchange rate series is published for JPY/USD" in out.output


def test_refresh_reports_a_pair_whose_rates_were_partly_unusable(
    runner: CliRunner,
) -> None:
    """A discarded rate is neither an outage nor a missing series.

    The provider answered, so retrying re-sends a request that returns the same
    unusable value — which is why this earns no retry hint. It is also not a
    permanent absence: the pair may have stored most of its span, so the line
    says coverage *may* be short instead of naming a manual remedy for a gap
    that might not exist.
    """
    out = _invoke_refresh(runner, _rates_result(discarded=("GBP/USD",)))

    assert out.exit_code == 0
    assert "Exchange rate coverage is short for GBP/USD" in out.output
    assert "Re-run the failed step" not in out.output
    assert "✅ Refresh complete" not in out.output


def test_refresh_json_carries_every_rate_pair_list(runner: CliRunner) -> None:
    """An agent on the JSON surface gets the same split a human gets on stderr."""
    out = _invoke_refresh(
        runner,
        _rates_result(
            failed=("EUR/USD",),
            unsupported=("JPY/USD",),
            discarded=("GBP/USD",),
        ),
        "--output",
        "json",
    )

    payload = json.loads(out.stdout)["data"]
    assert payload["rate_pairs_failed"] == ["EUR/USD"]
    assert payload["rate_pairs_unsupported"] == ["JPY/USD"]
    assert payload["rate_pairs_discarded"] == ["GBP/USD"]


def test_refresh_clean_rates_still_prints_the_success_banner(
    runner: CliRunner,
) -> None:
    """The negative control on the banner suppression above.

    Without this, a change that suppressed ✅ unconditionally would leave every
    assertion in this section passing.
    """
    out = _invoke_refresh(runner, _rates_result())

    assert "✅ Refresh complete" in out.output


def test_refresh_matcher_crash_warns_in_text(runner: CliRunner) -> None:
    """A matcher crash emits a ⚠️ warning in human output, exit 0."""
    fake_result = _match_crash()
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 0
    assert "Matching step failed" in result.output


def test_refresh_matcher_crash_warns_even_in_quiet(runner: CliRunner) -> None:
    """--quiet suppresses ✅/status but NOT a best-effort step-crash warning."""
    fake_result = _match_crash()
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--quiet"])

    assert result.exit_code == 0  # best-effort crash doesn't fail the command
    assert "Matching step failed" in result.output  # warning still surfaced


def test_refresh_clean_success_keeps_check_banner(runner: CliRunner) -> None:
    """A clean run still prints the ✅ success banner (no contradictory output)."""
    fake_result = RefreshResult(applied=True, duration_seconds=2.0)
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 0
    assert "✅ Refresh complete" in result.output


def test_refresh_apply_failure_with_matcher_crash_suppresses_retry_hint(
    runner: CliRunner,
) -> None:
    """Apply failure + matcher crash: ⚠️ warning shows, 💡 step-retry hint suppressed."""
    fake_result = _match_crash(applied=False, duration_seconds=1.0, error="apply boom")
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 1
    assert "Matching step failed" in result.output  # crash still surfaced
    assert "Re-run the failed step" not in result.output  # retry hint suppressed
    assert "Refresh failed: apply boom" in result.output


def test_refresh_warns_when_the_match_step_retired_an_accepted_transfer(
    runner: CliRunner,
) -> None:
    """Reversing a decision the user made is never a silent ✅.

    ``accounts links set`` already warns on this; a plain ``moneybin refresh``
    reaches the same reconciliation through the match step and reported a clean
    success.
    """
    fake_result = RefreshResult(applied=True, duration_seconds=2.0, transfers_retired=2)
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 0
    assert "Retired 2 previously accepted transfer" in result.output


def test_refresh_clean_pass_does_not_warn_about_retired_transfers(
    runner: CliRunner,
) -> None:
    """Negative twin: nothing retired, nothing said.

    Without it the test above would pass on an implementation that prints the
    warning unconditionally, which would train the user to ignore it.
    """
    fake_result = RefreshResult(applied=True, duration_seconds=2.0)
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 0
    assert "previously accepted transfer" not in result.output


def test_refresh_json_discloses_the_match_counts(runner: CliRunner) -> None:
    """The JSON surface carries the same disclosure as the MCP envelope.

    ``transfers_retired`` stays top-level rather than joining the match stage's
    counts: ``AccountLinksService.set`` adds account-collapse retirements the
    matcher never saw, so it is an operation total, not a step count.
    """
    fake_result = RefreshResult(
        applied=True,
        duration_seconds=1.0,
        stages=(
            StageOutcome(
                step="match",
                ran=True,
                counts=MappingProxyType({
                    "auto_merged": 3,
                    "pending_review": 0,
                    "pending_transfers": 0,
                }),
            ),
        ),
        transfers_retired=1,
    )
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    stages = {s["step"]: s for s in payload["data"]["stages"]}
    assert stages["match"]["counts"]["auto_merged"] == 3
    assert payload["data"]["transfers_retired"] == 1


def test_refresh_unknown_step_rejected_at_parse_time(runner: CliRunner) -> None:
    """Unknown step name is rejected by Typer before the service runs (exit 2)."""
    result = runner.invoke(app, ["refresh", "--step", "bogus"])
    assert result.exit_code == 2, result.output
    assert "bogus" in result.output  # Typer prints the bad value


# --- Requirement 18: one note per pipeline stage ----------------------------


def _staged_result(*stages: StageOutcome) -> RefreshResult:
    return RefreshResult(
        applied=True, duration_seconds=1.5, error=None, stages=tuple(stages)
    )


def _run_text_refresh(runner: CliRunner, result: RefreshResult) -> Result:
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        return runner.invoke(app, ["refresh"])


def test_refresh_names_every_stage_that_ran(runner: CliRunner) -> None:
    """A run that changed things says what each stage did, on stderr.

    Requirement 18: the outcome per stage is the payload. Before this, a full
    refresh printed one duration line and the counts lived only in the log.
    """
    invocation = _run_text_refresh(
        runner,
        _staged_result(
            StageOutcome(
                step="categorize",
                ran=True,
                counts=MappingProxyType({
                    "total": 400,
                    "merchant": 250,
                    "rule": 120,
                    "plaid": 30,
                }),
            ),
            StageOutcome(
                step="identity",
                ran=True,
                counts=MappingProxyType({
                    "accounts_linked": 7,
                    "merchants_bound": 12,
                    "merchant_conflicts": 0,
                }),
            ),
        ),
    )

    assert invocation.exit_code == 0, invocation.output
    # The note names the stage in the user's vocabulary; the machine-readable
    # step name stays in the JSON branch, asserted separately below.
    assert "Categorization: 400 categorized" in invocation.output
    assert "Identity: 7 accounts linked, 12 merchants bound" in invocation.output


def test_refresh_distinguishes_a_zero_stage_from_a_skipped_one(
    runner: CliRunner,
) -> None:
    """Zero and skipped must not print the same sentence.

    A skipped match examined nothing, so reporting "no duplicates found" would
    invent a result the run never established.
    """
    invocation = _run_text_refresh(
        runner,
        _staged_result(
            StageOutcome(
                step="match",
                ran=False,
            ),
            StageOutcome(
                step="categorize",
                ran=True,
                counts=MappingProxyType({
                    "total": 0,
                    "merchant": 0,
                    "rule": 0,
                    "plaid": 0,
                }),
            ),
        ),
    )

    assert invocation.exit_code == 0, invocation.output
    lines = [ln for ln in invocation.output.splitlines() if ln.strip()]
    match_line = next(ln for ln in lines if "Matching" in ln)
    categorize_line = next(ln for ln in lines if "Categorization" in ln)
    assert "skipped" in match_line
    # A skipped step reports no counts at all — four zeros would be a claim
    # about rows it never examined.
    assert "0" not in match_line
    # The zero stage still reports its zero, and never says it was skipped.
    assert "0 categorized" in categorize_line
    assert "skipped" not in categorize_line


def test_refresh_stage_notes_are_silenced_by_quiet(runner: CliRunner) -> None:
    """``-q`` suppresses status lines, and a stage note is one (requirement 4)."""
    invocation = _run_text_refresh_quiet(
        runner,
        _staged_result(
            StageOutcome(
                step="categorize",
                ran=True,
                counts=MappingProxyType({
                    "total": 400,
                    "merchant": 0,
                    "rule": 0,
                    "plaid": 0,
                }),
            )
        ),
    )

    assert invocation.exit_code == 0, invocation.output
    assert "categorize" not in invocation.output


def _run_text_refresh_quiet(runner: CliRunner, result: RefreshResult) -> Result:
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        return runner.invoke(app, ["refresh", "-q"])


def test_refresh_json_carries_the_stages(runner: CliRunner) -> None:
    """The agent surface gets the same outcomes as structured data, not prose."""
    fake = _staged_result(
        StageOutcome(
            step="categorize",
            ran=True,
            counts=MappingProxyType({
                "total": 400,
                "merchant": 250,
                "rule": 120,
                "plaid": 30,
            }),
        )
    )
    with (
        patch("moneybin.orchestration.refresh.refresh", return_value=fake),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        invocation = runner.invoke(app, ["refresh", "--output", "json"])

    assert invocation.exit_code == 0, invocation.output
    payload = json.loads(invocation.stdout)
    stages = payload["data"]["stages"]
    assert stages == [
        {
            "step": "categorize",
            "ran": True,
            "counts": {"total": 400, "merchant": 250, "rule": 120, "plaid": 30},
            "error": None,
        }
    ]


@pytest.mark.unit
def test_a_failed_apply_is_warned_about_once_not_once_per_surface(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``warn_refresh_steps`` is deliberately silent about the transform stage.

    Every embedded caller — import, sync pull, inbox drain, sheet pull — reports
    the SQLMesh apply through its own ``transforms_error`` field and then calls
    this helper for the best-effort steps. Once the apply became a stage like
    any other, warning on it here would print the same failure twice on four
    surfaces. The neighbouring steps still warn, so this is silence about one
    stage rather than silence about a failed run.
    """
    from moneybin.cli.utils import warn_refresh_steps
    from moneybin.services.refresh_outcome import RefreshStepOutcome

    with caplog.at_level(logging.WARNING):
        warn_refresh_steps(
            RefreshStepOutcome(
                stages=(
                    StageOutcome(step="transform", ran=False, error="model boom"),
                    StageOutcome(step="categorize", ran=True, error="categorizer boom"),
                )
            )
        )

    assert "model boom" not in caplog.text
    assert "categorizer boom" in caplog.text
