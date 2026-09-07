"""Unit tests for the refresh service-layer cascade.

These tests mock the four dependent services (GSheetPullService,
MatchingService, TransformService, CategorizationService) and assert
that ``refresh()``:

- Runs the full cascade when ``steps=None`` (current default).
- Runs only the requested subset when ``steps`` is a list.
- Executes steps in canonical order (gsheet → match → transform → categorize)
  regardless of input-list order.
- Raises ``UserError(code="refresh_unknown_step")`` on unknown step names.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, datetime
from typing import Any
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import duckdb
import pytest
import time_machine

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.orchestration.refresh import (
    RefreshResult,
    # The step's own branches are the subject of the last section in this file.
    # Only two of them are swallows; a step that ran and crashed reports an
    # error `refresh()` carries out to the caller.
    _run_identity_step,  # pyright: ignore[reportPrivateUsage]
    _run_rates_step,  # pyright: ignore[reportPrivateUsage]
    refresh,
)
from moneybin.services import matching_service
from moneybin.services.merchant_resolver import HarvestResult
from moneybin.services.rate_backfill import (
    RateBackfillNotReadyError,
    RateBackfillResult,
)
from moneybin.services.refresh_outcome import StageOutcome
from moneybin.services.transform_service import ApplyResult


def _make_apply_result(applied: bool = True) -> ApplyResult:
    return ApplyResult(applied=applied, duration_seconds=1.0, error=None)


def _match_double(**overrides: Any) -> MagicMock:
    """A ``MatchResult`` stand-in whose counts are real ints.

    The counts are spelled out rather than left to attribute auto-creation:
    they land in ``StageOutcome.counts``, typed ``Mapping[str, int]``, so a
    bare ``MagicMock`` attribute puts a mock object where a count belongs and
    every assertion comparing one to a number passes on identity alone.
    """
    attrs: dict[str, Any] = {
        "has_matches": False,
        "has_pending": False,
        "auto_merged": 0,
        "pending_review": 0,
        "pending_transfers": 0,
        "transfers_retired": 0,
    }
    attrs.update(overrides)
    return MagicMock(**attrs)


@pytest.fixture
def patched_services() -> Iterator[dict[str, MagicMock]]:
    """Patch all refresh backends and yield handles for call inspection."""
    gsheet_pull = MagicMock(return_value=[])
    matcher_run = MagicMock(return_value=_match_double())
    transform_apply = MagicMock(return_value=_make_apply_result(applied=True))
    categorize_pending = MagicMock(
        return_value={"total": 0, "rule": 0, "merchant": 0, "plaid": 0}
    )
    auto_stats = MagicMock(return_value=MagicMock(pending_proposals=0))
    identity = MagicMock(
        return_value=(StageOutcome(step="identity", ran=True), ()),
    )

    # Patches target the consumer module (moneybin.orchestration.refresh) where
    # each name is bound — refresh.py imports TransformService at module level
    # and the other backends via deferred imports, so patching the source
    # modules wouldn't intercept the call paths used here.
    with (
        patch(
            "moneybin.orchestration.refresh._run_gsheet_step",
            gsheet_pull,
        ),
        patch(
            "moneybin.services.matching_service.MatchingService.run",
            matcher_run,
        ),
        patch(
            "moneybin.orchestration.refresh.TransformService",
            return_value=MagicMock(apply=transform_apply),
        ),
        patch(
            "moneybin.services.categorization.CategorizationService",
            return_value=MagicMock(categorize_pending=categorize_pending),
        ),
        patch(
            "moneybin.services.auto_rule_service.AutoRuleService",
            return_value=MagicMock(stats=auto_stats),
        ),
        patch(
            "moneybin.orchestration.refresh._run_identity_step",
            identity,
            create=True,
        ),
    ):
        yield {
            "gsheet_pull": gsheet_pull,
            "matcher_run": matcher_run,
            "transform_apply": transform_apply,
            "categorize_pending": categorize_pending,
            "auto_stats": auto_stats,
            "identity": identity,
        }


def patch_all_refresh_stages(monkeypatch: pytest.MonkeyPatch, calls: list[str]) -> None:
    """Patch each stage with a call marker so cascade order stays observable."""

    def _gsheet(_db: Database) -> list[Any]:
        calls.append("gsheet")
        return []

    def _match(
        _self: matching_service.MatchingService,
        *,
        auto_accept_transfers: bool = False,
        actor: str = "system",
    ) -> Any:
        # Mirrors `MatchingService.run`'s keyword-only signature rather than
        # absorbing `**kwargs`: a double that swallows arguments turns a caller
        # passing the wrong one into a swallowed TypeError on the catch-all
        # branch, which reads as a matcher crash rather than a broken call.
        calls.append("match")
        return _match_double()

    def _transform(_db: Database) -> Any:
        service = MagicMock()

        def _apply() -> ApplyResult:
            calls.append("transform")
            return _make_apply_result()

        service.apply.side_effect = _apply
        return service

    def _categorize(_db: Database) -> StageOutcome:
        calls.append("categorize")
        return StageOutcome(step="categorize", ran=True)

    def _identity(_db: Database) -> tuple[StageOutcome, tuple[str, ...]]:
        calls.append("identity")
        return StageOutcome(step="identity", ran=True), ()

    def _rates(_db: Database) -> tuple[RateBackfillResult | None, str | None]:
        # Spelled out rather than `-> Any`: this double stands in for the real
        # step in every cascade-ordering test, so a signature it is free to
        # drift from is one that stops catching a contract change.
        calls.append("rates")
        return None, None

    monkeypatch.setattr(
        "moneybin.orchestration.refresh._run_gsheet_step",
        _gsheet,
    )
    monkeypatch.setattr(
        matching_service.MatchingService,
        "run",
        _match,
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService",
        _transform,
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh._run_categorize_step",
        _categorize,
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh._run_identity_step",
        _identity,
        raising=False,
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh._run_rates_step",
        _rates,
        raising=False,
    )


@pytest.mark.unit
def test_refresh_result_has_error_surfacing_fields() -> None:
    """RefreshResult surfaces each step's own error, plus self-heal records.

    A result built with no stages reports no step failure anywhere, and one
    carrying a crashed match and a crashed categorize answers each on its own
    stage — the two remedies differ, so one flag for both would misroute.
    """
    from moneybin.orchestration.refresh import SelfHealRecord

    r = RefreshResult(applied=True, duration_seconds=1.0)
    assert r.stage("match") is None
    assert r.stage("categorize") is None
    assert r.identity_errors == ()
    assert r.self_heal_actions == ()

    rec = SelfHealRecord(
        recipe_id="orphan_categorizations_cleanup",
        rows_affected=3,
        operation_id="op_self_heal_orphan_categorizations_cleanup_abc",
        timestamp="2026-05-22T00:00:00Z",
    )
    r2 = RefreshResult(
        applied=True,
        duration_seconds=1.0,
        identity_errors=("accounts",),
        self_heal_actions=(rec,),
        stages=(
            StageOutcome(step="match", ran=True, error="boom"),
            StageOutcome(step="categorize", ran=True, error="bang"),
        ),
    )
    match_stage = r2.stage("match")
    assert match_stage is not None
    assert match_stage.error == "boom"
    categorize_stage = r2.stage("categorize")
    assert categorize_stage is not None
    assert categorize_stage.error == "bang"
    assert r2.identity_errors == ("accounts",)
    assert r2.self_heal_actions[0].recipe_id == "orphan_categorizations_cleanup"


@pytest.mark.unit
def test_refresh_reports_what_the_matcher_found(
    patched_services: dict[str, MagicMock],
) -> None:
    """The matcher's counts reach the caller instead of only the log.

    A match pass can auto-merge silently (``engine._classify_pair`` writes
    ``accepted`` for an agreeing pair over the confidence threshold), so a
    caller that triggers one — the post-merge re-match especially — has to be
    able to tell the user what it did.
    """
    patched_services["matcher_run"].return_value = _match_double(
        auto_merged=2, pending_review=5, pending_transfers=1
    )

    result = refresh(MagicMock())

    stage = result.stage("match")
    assert stage is not None
    assert stage.count("auto_merged") == 2
    assert stage.count("pending_review") == 5
    assert stage.count("pending_transfers") == 1


@pytest.mark.unit
def test_refresh_reports_no_match_stage_when_the_step_is_skipped(
    patched_services: dict[str, MagicMock],
) -> None:
    """A caller that excluded match gets no match stage, not a fabricated zero.

    The matcher never ran, so it examined nothing and produced no count. A
    zero-count stage would claim it looked and found no duplicates.
    """
    result = refresh(MagicMock(), steps=["transform"])

    assert result.stage("match") is None
    patched_services["matcher_run"].assert_not_called()


@pytest.mark.unit
def test_refresh_matcher_crash_populates_the_match_stage_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """A real matcher crash sets the match stage's error; transform still runs."""
    patched_services["matcher_run"].side_effect = RuntimeError("matcher boom")
    result = refresh(MagicMock())
    # Set but not quoting the exception: the field crosses the MCP boundary, so
    # the crash is reported, not repeated.
    stage = result.stage("match")
    assert stage is not None
    assert stage.error is not None
    assert "matcher boom" not in stage.error
    assert result.applied is True  # transform still ran despite the matcher crash


@pytest.mark.unit
def test_refresh_matcher_crash_preserved_when_apply_also_fails(
    patched_services: dict[str, MagicMock],
) -> None:
    """A matcher crash is preserved in the result even when SQLMesh apply fails."""
    patched_services["matcher_run"].side_effect = RuntimeError("matcher boom")
    patched_services["transform_apply"].return_value = ApplyResult(
        applied=False, duration_seconds=1.0, error="apply boom"
    )
    result = refresh(MagicMock())
    assert result.applied is False
    assert result.error == "apply boom"  # apply failure surfaced
    stage = result.stage("match")
    assert stage is not None
    assert stage.error is not None  # matcher crash still preserved
    assert "matcher boom" not in stage.error


@pytest.mark.unit
@pytest.mark.parametrize(
    "exc",
    [duckdb.CatalogException("no view"), duckdb.BinderException("no col")],
)
def test_refresh_matcher_missing_views_is_not_an_error(
    patched_services: dict[str, MagicMock], exc: Exception
) -> None:
    """Catalog/Binder exceptions (views not built on first load) are expected, not surfaced."""
    patched_services["matcher_run"].side_effect = exc
    result = refresh(MagicMock())
    stage = result.stage("match")
    assert stage is not None
    assert stage.error is None


@pytest.mark.unit
def test_refresh_categorizer_crash_populates_the_categorize_stage_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """A real categorizer crash sets the categorize stage's error; pipeline continues."""
    patched_services["categorize_pending"].side_effect = RuntimeError("cat boom")
    result = refresh(MagicMock())
    stage = result.stage("categorize")
    assert stage is not None
    assert stage.error is not None
    assert "cat boom" not in stage.error
    assert result.applied is True


@pytest.mark.unit
def test_refresh_auto_rule_stats_crash_is_not_a_categorize_stage_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """A crash in the post-step auto-rule stats read must leave the stage clean.

    categorize_pending() succeeded; the proposal-count read is informational.
    Conflating the two would falsely tell the agent to retry categorization.
    """
    patched_services["auto_stats"].side_effect = RuntimeError("stats boom")
    result = refresh(MagicMock())
    stage = result.stage("categorize")
    assert stage is not None
    assert stage.error is None
    assert result.applied is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "exc",
    [duckdb.CatalogException("nope"), duckdb.BinderException("no col")],
)
def test_refresh_categorizer_missing_tables_is_not_an_error(
    patched_services: dict[str, MagicMock], exc: Exception
) -> None:
    """Catalog/Binder exceptions (tables not built on first load) are expected, not surfaced."""
    patched_services["categorize_pending"].side_effect = exc
    result = refresh(MagicMock())
    stage = result.stage("categorize")
    assert stage is not None
    assert stage.error is None


@pytest.mark.unit
def test_refresh_steps_none_runs_full_cascade(
    patched_services: dict[str, MagicMock],
) -> None:
    """``steps=None`` (default) runs every canonical refresh stage."""
    result = refresh(MagicMock())
    assert isinstance(result, RefreshResult)
    assert result.applied is True
    assert patched_services["gsheet_pull"].call_count == 1
    assert patched_services["matcher_run"].call_count == 1
    assert patched_services["transform_apply"].call_count == 1
    assert patched_services["categorize_pending"].call_count == 1
    assert patched_services["identity"].call_count == 1


@pytest.mark.unit
def test_identity_runs_after_categorize(monkeypatch: pytest.MonkeyPatch) -> None:
    """Identity proposal generation is the final canonical refresh stage."""
    calls: list[str] = []
    patch_all_refresh_stages(monkeypatch, calls)

    refresh(MagicMock())

    assert calls == [
        "gsheet",
        "match",
        "transform",
        "categorize",
        "identity",
        "rates",
    ]


@pytest.mark.unit
def test_identity_can_run_surgically(monkeypatch: pytest.MonkeyPatch) -> None:
    """Identity can generate proposals without rebuilding derived tables."""
    calls: list[str] = []
    patch_all_refresh_stages(monkeypatch, calls)

    result = refresh(MagicMock(), steps=["identity"])

    assert calls == ["identity"]
    assert result.applied is False


@pytest.mark.unit
def test_rates_run_last_because_nothing_downstream_reads_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pairs and dates to fetch come from core.*, so transform must run first.

    Nothing in the cascade consumes the rates, so the step goes last: a provider
    outage then costs the run nothing that had already succeeded.
    """
    calls: list[str] = []
    patch_all_refresh_stages(monkeypatch, calls)

    refresh(MagicMock())

    assert calls.index("rates") > calls.index("transform")
    assert calls[-1] == "rates"


@pytest.mark.unit
def test_rates_can_run_surgically(monkeypatch: pytest.MonkeyPatch) -> None:
    """An operator can top up rate coverage without rebuilding anything."""
    calls: list[str] = []
    patch_all_refresh_stages(monkeypatch, calls)

    refresh(MagicMock(), steps=["rates"])

    assert calls == ["rates"]


@pytest.mark.unit
def test_rates_is_a_known_step() -> None:
    """A step the service rejects would be unreachable from either surface."""
    from moneybin.orchestration.refresh import CANONICAL_STEPS

    assert "rates" in CANONICAL_STEPS


@pytest.mark.unit
def test_identity_failure_does_not_prevent_other_domain(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Account failure is sanitized and does not block merchant backfill."""
    from moneybin.services import account_links_service, merchant_links_service

    calls: list[str] = []
    sensitive_error = "account number 123456789 merchant Secret Shop"

    def _accounts_run() -> None:
        calls.append("accounts")
        raise RuntimeError(sensitive_error)

    def _merchants_run() -> HarvestResult:
        calls.append("merchants")
        # Mirrors the real return type: refresh reads `.conflicts` to decide
        # whether to surface a review notice, so a bare None here would only
        # pass by accident.
        return HarvestResult(bound=0, conflicts=0)

    accounts_run = MagicMock(side_effect=_accounts_run)
    merchants_run = MagicMock(side_effect=_merchants_run)

    def _accounts_service(_db: Database) -> Any:
        return MagicMock(run=accounts_run)

    def _merchants_service(_db: Database) -> Any:
        return MagicMock(run=merchants_run)

    monkeypatch.setattr(
        account_links_service,
        "AccountLinksService",
        _accounts_service,
    )
    monkeypatch.setattr(
        merchant_links_service,
        "MerchantLinksService",
        _merchants_service,
    )

    caplog.set_level(logging.ERROR, logger="moneybin.orchestration.refresh")
    result = refresh(MagicMock(), steps=["identity"])

    accounts_run.assert_called_once()
    merchants_run.assert_called_once()
    assert calls == ["accounts", "merchants"]
    assert result.identity_errors == ("accounts",)
    refresh_records = [
        record
        for record in caplog.records
        if record.name == "moneybin.orchestration.refresh"
    ]
    assert len(refresh_records) == 1
    assert sensitive_error not in refresh_records[0].getMessage()
    assert (
        "accounts identity backfill failed: RuntimeError"
        in refresh_records[0].getMessage()
    )
    assert all(record.exc_info is None for record in refresh_records)


# --------------------------- the rates step's own branches ---------------------------
#
# Reached directly rather than through `refresh()`. `patch_all_refresh_stages`
# replaces `_run_rates_step` wholesale, so every cascade test above proves only
# that the step is *called* — the swallows below are exactly where a misread
# home currency or an unbuilt catalog turns into silence, which is the one place
# a cascade test cannot see.


def _patch_rates_step(
    monkeypatch: pytest.MonkeyPatch,
    *,
    home_currency: str | Exception | None,
    backfill: RateBackfillResult | Exception,
    reached: list[str],
    through_seen: list[date] | None = None,
) -> None:
    """Stand in for the two collaborators `_run_rates_step` composes."""
    from moneybin.connectors.rates import frankfurter
    from moneybin.repositories import profile_settings_repo
    from moneybin.services import rate_backfill

    def _get_home_currency(_self: Any) -> str | None:
        if isinstance(home_currency, Exception):
            raise home_currency
        return home_currency

    def _run(
        _db: Database, *, home_currency: str, through: date, **_kw: Any
    ) -> RateBackfillResult:
        reached.append(home_currency)
        if through_seen is not None:
            through_seen.append(through)
        if isinstance(backfill, Exception):
            raise backfill
        return backfill

    monkeypatch.setattr(
        profile_settings_repo.ProfileSettingsRepo,
        "get_home_currency",
        _get_home_currency,
    )
    monkeypatch.setattr(rate_backfill, "run_rate_backfill", _run)
    # Keeps the step hermetic: the real constructor opens an httpx client that
    # nothing here would close.
    monkeypatch.setattr(frankfurter, "FrankfurterRateAdapter", MagicMock())


@pytest.mark.unit
def test_rates_step_closes_its_window_on_the_provider_s_day_not_the_host_s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The window closes on the UTC day, the one Frankfurter keys its series by.

    Frozen at 00:30 in a UTC+2 zone, where the host has already turned over to
    the 11th while UTC is still on the 10th. A host-local ``through`` asks for a
    day the provider has not published yet, which is the failure
    ``PriceService.__init__`` documents for the same class of feed
    (``price_service.py``: "a host-local clock disagrees with the data whenever
    the host is not on UTC"). Harmless for one refresh — the pair simply comes
    back short and re-requests next run — but it is the same mistake in the same
    shape, and the codebase already resolved it once.
    """
    reached: list[str] = []
    through_seen: list[date] = []
    _patch_rates_step(
        monkeypatch,
        home_currency="USD",
        backfill=RateBackfillResult(rates_written=1, pairs_failed=()),
        reached=reached,
        through_seen=through_seen,
    )

    with time_machine.travel(
        datetime(2026, 3, 11, 0, 30, tzinfo=ZoneInfo("Europe/Kyiv")), tick=False
    ):
        _run_rates_step(MagicMock())

    assert through_seen == [date(2026, 3, 10)], (
        "the window must close on the UTC day, not the host's already-turned-over one"
    )


@pytest.mark.unit
def test_rates_step_returns_what_the_backfill_gathered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The success path hands the profile's home currency to the backfill."""
    gathered = RateBackfillResult(rates_written=7, pairs_failed=("EUR/USD",))
    reached: list[str] = []
    _patch_rates_step(
        monkeypatch, home_currency="USD", backfill=gathered, reached=reached
    )

    backfill, error = _run_rates_step(MagicMock())
    assert backfill is gathered
    assert error is None, "a step that ran cleanly reports no error"
    assert reached == ["USD"]


@pytest.mark.unit
def test_rates_step_without_a_home_currency_never_calls_the_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing is ever converted without a home currency, so nothing is implied."""
    reached: list[str] = []
    _patch_rates_step(
        monkeypatch,
        home_currency=None,
        backfill=RateBackfillResult(rates_written=1, pairs_failed=()),
        reached=reached,
    )

    assert _run_rates_step(MagicMock()) == (None, None), (
        "declining to run is not a failure — no error accompanies it"
    )
    assert reached == [], "an unset home currency must not reach the network"


@pytest.mark.unit
def test_rates_step_survives_an_unreadable_home_currency(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A settings read that raises costs the step, not the whole refresh."""
    reached: list[str] = []
    _patch_rates_step(
        monkeypatch,
        home_currency=RuntimeError("settings boom"),
        backfill=RateBackfillResult(rates_written=1, pairs_failed=()),
        reached=reached,
    )
    caplog.set_level(logging.ERROR, logger="moneybin.orchestration.refresh")

    backfill, error = _run_rates_step(MagicMock())
    assert backfill is None
    assert error is not None, "a settings read that raised is a failure, not a decline"
    assert "settings boom" not in error, "the raw message never reaches the caller"
    assert reached == [], "the backfill is unreachable once the read failed"
    assert "settings boom" not in caplog.text, "the raw message is never logged"


@pytest.mark.unit
def test_rates_step_missing_core_views_is_not_an_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """`core.*` not built yet is a first-load precondition, not a failure.

    Distinguished from the generic swallow below by the log level: this one is
    expected on a fresh profile and must not put an error in the operator's log
    every time refresh runs before the first transform.

    Raises the named precondition rather than the DuckDB types it wraps, because
    those types alone no longer mean this — see the test above.
    """
    reached: list[str] = []
    _patch_rates_step(
        monkeypatch,
        home_currency="USD",
        backfill=RateBackfillNotReadyError(),
        reached=reached,
    )
    caplog.set_level(logging.ERROR, logger="moneybin.orchestration.refresh")

    assert _run_rates_step(MagicMock()) == (None, None), (
        "a first-load precondition is not a failure the caller must report"
    )
    assert reached == ["USD"], "the backfill is what raised"
    assert caplog.records == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "exc",
    [duckdb.CatalogException("late"), duckdb.BinderException("late bind")],
)
def test_rates_step_reports_a_duckdb_failure_raised_after_planning(
    monkeypatch: pytest.MonkeyPatch, exc: Exception
) -> None:
    """Only planning may read a DuckDB error as "core.* is not built yet".

    The storage half of the step raises those same two types — a drifted rate
    cache, a bind failure on write. Reading one of those as the first-load
    precondition tells the user nothing happened on a profile whose core models
    were built long ago, and an explicitly requested `--step rates` run is
    exactly where that answer is wrong.
    """
    reached: list[str] = []
    _patch_rates_step(monkeypatch, home_currency="USD", backfill=exc, reached=reached)

    backfill, error = _run_rates_step(MagicMock())
    assert backfill is None
    assert error is not None, "a DuckDB failure after planning is a crash, not a skip"
    assert reached == ["USD"]


@pytest.mark.unit
def test_rates_step_reports_a_backfill_crash_without_naming_it(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A crash is reported to the caller, but never in the exception's own words.

    Two separate obligations. The caller must be able to tell a step that ran
    and failed from one that declined to run — the `None` backfill is the same
    on both paths, so the error is what carries the difference. And the text it
    carries goes through `_step_error` like every sibling step, because a rates
    crash can hold a provider URL with a currency pair in it and the field
    lands in CLI JSON and the MCP envelope.
    """
    reached: list[str] = []
    _patch_rates_step(
        monkeypatch,
        home_currency="USD",
        backfill=RuntimeError("rates boom"),
        reached=reached,
    )
    caplog.set_level(logging.ERROR, logger="moneybin.orchestration.refresh")

    backfill, error = _run_rates_step(MagicMock())
    assert backfill is None
    assert error is not None, "a step that ran and crashed must say so"
    assert reached == ["USD"]
    assert "rates boom" not in error, "the raw message never reaches the caller"
    assert "rates boom" not in caplog.text, "nor the log"


@pytest.mark.unit
@pytest.mark.parametrize("steps", [["transform", "rates"], ["rates"]])
@pytest.mark.usefixtures("patched_services")
def test_refresh_carries_a_crashed_rates_step_out_to_the_caller(
    steps: list[str],
) -> None:
    """Both return paths report the crash, not just the one that applied.

    `refresh()` has two exits that build a rates result — the normal one and
    the early return for a cascade omitting `transform`. A crash reported on
    only one of them would be invisible on exactly the partial cascades an
    embedded caller runs.
    """
    with patch(
        "moneybin.orchestration.refresh._run_rates_step",
        return_value=(None, "Rate backfill failed — the cause is in the local log"),
    ):
        result = refresh(MagicMock(), steps=steps)

    assert result.rate_backfill is None
    stage = result.stage("rates")
    assert stage is not None
    assert stage.ran is False
    assert stage.error == ("Rate backfill failed — the cause is in the local log"), (
        "a null backfill alone cannot distinguish a crash from a declined step"
    )


@pytest.mark.unit
@pytest.mark.usefixtures("patched_services")
def test_refresh_leaves_the_rates_error_unset_when_the_step_was_not_asked_for() -> None:
    """An unrequested step reports neither a result nor a failure.

    No stage at all, rather than a stage carrying no error: the step was never
    reached, so there is nothing for it to have declined or crashed on.
    """
    result = refresh(MagicMock(), steps=["transform"])
    assert result.rate_backfill is None
    assert result.stage("rates") is None


@pytest.mark.unit
def test_refresh_steps_transform_only(patched_services: dict[str, MagicMock]) -> None:
    """``steps=["transform"]`` skips gsheet, match, and categorize."""
    result = refresh(MagicMock(), steps=["transform"])
    assert result.applied is True
    assert patched_services["gsheet_pull"].call_count == 0
    assert patched_services["matcher_run"].call_count == 0
    assert patched_services["transform_apply"].call_count == 1
    assert patched_services["categorize_pending"].call_count == 0


@pytest.mark.unit
def test_refresh_steps_match_and_categorize_skips_transform(
    patched_services: dict[str, MagicMock],
) -> None:
    """``steps=["match","categorize"]`` runs match + categorize; no transform.

    No SQLMesh apply means ``applied=False`` and ``duration_seconds=None`` —
    the result fields describe the SQLMesh step specifically (per the
    RefreshResult docstring), so a skipped transform leaves them empty.
    """
    result = refresh(MagicMock(), steps=["match", "categorize"])
    assert result.applied is False
    assert result.duration_seconds is None
    assert result.error is None
    assert patched_services["matcher_run"].call_count == 1
    assert patched_services["transform_apply"].call_count == 0
    assert patched_services["categorize_pending"].call_count == 1


@pytest.mark.unit
def test_refresh_steps_canonical_order_enforced(
    patched_services: dict[str, MagicMock],
) -> None:
    """Input-list order is ignored; canonical order gsheet→match→transform→categorize wins."""
    call_log: list[str] = []

    def _gsheet_side(*a: Any, **kw: Any) -> list[Any]:
        call_log.append("gsheet")
        return []

    def _match_side(*a: Any, **kw: Any) -> MagicMock:
        call_log.append("match")
        return _match_double()

    def _transform_side(*a: Any, **kw: Any) -> ApplyResult:
        call_log.append("transform")
        return _make_apply_result(applied=True)

    def _categorize_side(*a: Any, **kw: Any) -> dict[str, int]:
        call_log.append("categorize")
        return {"total": 0, "rule": 0, "merchant": 0, "plaid": 0}

    patched_services["gsheet_pull"].side_effect = _gsheet_side
    patched_services["matcher_run"].side_effect = _match_side
    patched_services["transform_apply"].side_effect = _transform_side
    patched_services["categorize_pending"].side_effect = _categorize_side

    refresh(MagicMock(), steps=["categorize", "transform", "match", "gsheet"])
    assert call_log == ["gsheet", "match", "transform", "categorize"]


@pytest.mark.unit
def test_refresh_unknown_step_raises_user_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """Unknown step name raises UserError with hint enumerating valid steps."""
    with pytest.raises(UserError) as excinfo:
        refresh(MagicMock(), steps=["transform", "bogus"])
    assert excinfo.value.code == "refresh_unknown_step"
    assert "gsheet" in (excinfo.value.hint or "")
    assert "match" in (excinfo.value.hint or "")
    assert "transform" in (excinfo.value.hint or "")
    assert "categorize" in (excinfo.value.hint or "")
    assert "identity" in (excinfo.value.hint or "")
    # None of the step backends should run when validation fails.
    assert patched_services["gsheet_pull"].call_count == 0
    assert patched_services["matcher_run"].call_count == 0
    assert patched_services["transform_apply"].call_count == 0
    assert patched_services["categorize_pending"].call_count == 0


@pytest.mark.unit
def test_refresh_empty_steps_list_runs_nothing(
    patched_services: dict[str, MagicMock],
) -> None:
    """``steps=[]`` is valid: validates as empty subset, runs no step."""
    result = refresh(MagicMock(), steps=[])
    assert result.applied is False
    assert result.duration_seconds is None
    assert patched_services["gsheet_pull"].call_count == 0
    assert patched_services["matcher_run"].call_count == 0
    assert patched_services["transform_apply"].call_count == 0
    assert patched_services["categorize_pending"].call_count == 0


@pytest.mark.unit
def test_refresh_step_order_puts_gsheet_before_match(
    patched_services: dict[str, MagicMock],
) -> None:
    """Gsheet runs before match (pulled rows feed downstream matching)."""
    call_log: list[str] = []

    def _gsheet_side(*a: Any, **kw: Any) -> list[Any]:
        call_log.append("gsheet")
        return []

    def _match_side(*a: Any, **kw: Any) -> MagicMock:
        call_log.append("match")
        return _match_double()

    patched_services["gsheet_pull"].side_effect = _gsheet_side
    patched_services["matcher_run"].side_effect = _match_side

    refresh(MagicMock(), steps=["gsheet", "match"])
    assert call_log == ["gsheet", "match"]
    # Verify both ran
    assert patched_services["gsheet_pull"].call_count == 1
    assert patched_services["matcher_run"].call_count == 1


@pytest.mark.unit
def test_refresh_gsheet_step_skippable(
    patched_services: dict[str, MagicMock],
) -> None:
    """Gsheet step can be skipped via steps parameter."""
    result = refresh(MagicMock(), steps=["match", "transform", "categorize"])
    # Verify gsheet did not run
    assert patched_services["gsheet_pull"].call_count == 0
    # But others did
    assert patched_services["matcher_run"].call_count == 1
    assert patched_services["transform_apply"].call_count == 1
    assert patched_services["categorize_pending"].call_count == 1
    assert result.applied is True


@pytest.mark.unit
def test_refresh_reports_retirements_a_crashed_match_step_already_committed() -> None:
    """A crash after the reconciliation must not swallow what it reversed.

    ``retire_transfers_invalidated_by_dedup`` commits each reversal individually
    and runs before Tier 4, so a Tier 4 failure leaves accepted transfers
    genuinely reversed while ``run()`` never returns its ``MatchResult``.
    Reporting zero there describes a decision of the user's as untouched.
    """
    from moneybin.matching.engine import MatchResult, MatchRunError

    with patch.object(
        matching_service.MatchingService,
        "run",
        side_effect=MatchRunError(
            RuntimeError("tier 4 boom"), partial=MatchResult(transfers_retired=3)
        ),
    ):
        result = refresh(MagicMock(), steps=["match"])

    stage = result.stage("match")
    assert stage is not None
    assert stage.error is not None
    assert "tier 4 boom" not in stage.error
    assert result.transfers_retired == 3
    assert stage.ran is True


@pytest.mark.unit
def test_refresh_reports_decisions_a_crashed_match_step_already_committed() -> None:
    """The tiers commit too, and their counts die with the same exception.

    A dedup tier persists one decision per pair with no transaction around the
    loop, so a pair that raises leaves every earlier merge in the ledger — where
    it suppresses the duplicate side of a transaction. Reporting zero auto-merges
    there tells the caller the ledger is unchanged when it is not.
    """
    from moneybin.matching.engine import MatchResult, MatchRunError

    with patch.object(
        matching_service.MatchingService,
        "run",
        side_effect=MatchRunError(
            RuntimeError("tier 3 boom"),
            partial=MatchResult(auto_merged=4, pending_review=2),
        ),
    ):
        result = refresh(MagicMock(), steps=["match"])

    stage = result.stage("match")
    assert stage is not None
    assert stage.count("auto_merged") == 4
    assert stage.count("pending_review") == 2
    assert stage.error is not None
    assert "tier 3 boom" not in stage.error
    assert stage.ran is True


@pytest.mark.unit
def test_refresh_does_not_call_a_late_view_failure_a_skipped_match_step() -> None:
    """A catalog error *after* the tiers ran is a crash, not a missing view.

    A match stage with ``ran=False`` claims nothing was examined and suppresses
    the error entirely. Once the dedup tiers have written decisions and the
    reconciliation has reversed a transfer, that claim is false — and it is the
    one that hides the reversal. Only a failure that reaches ``run()`` unwrapped
    is the first-load precondition.
    """
    from moneybin.matching.engine import MatchResult, MatchRunError

    with patch.object(
        matching_service.MatchingService,
        "run",
        side_effect=MatchRunError(
            duckdb.CatalogException("no view"),
            partial=MatchResult(transfers_retired=1),
        ),
    ):
        result = refresh(MagicMock(), steps=["match"])

    stage = result.stage("match")
    assert stage is not None
    assert stage.ran is True
    assert stage.error is not None
    assert result.transfers_retired == 1


@pytest.mark.unit
def test_refresh_keeps_a_crashed_matchers_cause_out_of_its_error_field() -> None:
    """The returned error must not repeat what the exception said.

    ``MatchRunError.__init__`` passes ``str(cause)`` to ``Exception``, so the
    carrier's own message *is* the raw failure — DuckDB binder text, file paths,
    row values. The match stage's ``error`` is a ``DataClass.DESCRIPTION`` field
    that reaches ``refresh_run`` and CLI JSON, so returning it there puts the
    cause on the wrong side of the boundary the direct matcher surfaces already
    hold.
    """
    from moneybin.matching.engine import MatchResult, MatchRunError

    cause = RuntimeError("Binder Error: no column named acct_1098 in /Users/x/db")
    with patch.object(
        matching_service.MatchingService,
        "run",
        side_effect=MatchRunError(cause, partial=MatchResult(transfers_retired=3)),
    ):
        result = refresh(MagicMock(), steps=["match"])

    stage = result.stage("match")
    assert stage is not None
    assert stage.error is not None
    assert "acct_1098" not in stage.error
    assert "/Users/x/db" not in stage.error
    assert "partway through" in stage.error
    # The counts are the disclosable half and must survive the sanitizing.
    assert result.transfers_retired == 3


@pytest.mark.unit
def test_refresh_keeps_an_unclassified_crashs_cause_out_of_its_error_field() -> None:
    """The catch-all branch feeds the same field, so it needs the same boundary.

    Sanitizing only the ``MatchRunError`` branch would leave the match stage's
    ``error`` with two behaviours depending on which exception fired — and the
    catch-all is the branch that catches the types nobody anticipated.
    """
    with patch.object(
        matching_service.MatchingService,
        "run",
        side_effect=RuntimeError("row 4412 amount -2412.55 failed /Users/x/db"),
    ):
        result = refresh(MagicMock(), steps=["match"])

    stage = result.stage("match")
    assert stage is not None
    assert stage.error is not None
    assert "2412.55" not in stage.error
    assert "/Users/x/db" not in stage.error


@pytest.mark.unit
def test_refresh_keeps_a_crashed_categorizers_cause_out_of_its_error_field() -> None:
    """The categorize stage's error is the same declared class on the same payload.

    Holding the line for the matcher alone would leave the sibling stage on
    ``RefreshRunPayload`` free to say whatever its exception said.
    """
    from moneybin.services import categorization

    with patch.object(
        categorization.CategorizationService,
        "categorize_pending",
        side_effect=RuntimeError("row 4412 amount -2412.55 failed /Users/x/db"),
    ):
        result = refresh(MagicMock(), steps=["categorize"])

    stage = result.stage("categorize")
    assert stage is not None
    assert stage.error is not None
    assert "2412.55" not in stage.error
    assert "/Users/x/db" not in stage.error


@pytest.mark.unit
def test_refresh_threads_a_callers_actor_into_the_match_step() -> None:
    """A surface-triggered re-match owes its decisions the surface's name.

    ``app-integrity-invariant.md`` binds matcher-created decisions to the actor
    of the surface that caused them; the post-merge re-match is caused by a user
    accepting a link, so its decisions are not ``system``'s work.
    """
    with patch.object(matching_service.MatchingService, "run") as run:
        refresh(MagicMock(), steps=["match"], actor="mcp")

    assert run.call_args.kwargs["actor"] == "mcp"


@pytest.mark.unit
def test_refresh_attributes_an_unasked_for_actor_to_system() -> None:
    """The same spec keeps ordinary automated refreshes on ``system``.

    ``moneybin refresh`` and ``refresh_run`` are the automated callers the spec
    names, so threading the parameter must not silently re-attribute them.
    """
    with patch.object(matching_service.MatchingService, "run") as run:
        refresh(MagicMock(), steps=["match"])

    assert run.call_args.kwargs["actor"] == "system"


@pytest.mark.unit
def test_step_outcome_carries_every_best_effort_failure_channel() -> None:
    """Every channel a caller must report, flattened onto one carrier.

    The four best-effort steps fail independently and route to different
    remedies, so a carrier that folded them into one flag would send a caller
    to the wrong one. Flattened rather than nesting ``RateBackfillResult``
    because the Pydantic result carriers that embed this sit on the CLI's cold
    path, and that type pulls polars in behind it.
    """
    from moneybin.orchestration.refresh import RefreshResult, step_outcome
    from moneybin.services.rate_backfill import RateBackfillResult

    outcome = step_outcome(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            identity_errors=("merchants",),
            rate_backfill=RateBackfillResult(
                rates_written=3,
                pairs_failed=("EUR/USD",),
                pairs_unsupported=("EUR/XTS",),
                pairs_discarded=("GBP/USD",),
            ),
            stages=(
                StageOutcome(step="match", ran=True, error="matcher blew up"),
                StageOutcome(step="categorize", ran=True, error="categorizer blew up"),
                StageOutcome(step="rates", ran=True, counts={"rates_written": 3}),
            ),
        )
    )

    match_stage = outcome.stage("match")
    assert match_stage is not None
    assert match_stage.error == "matcher blew up"
    categorize_stage = outcome.stage("categorize")
    assert categorize_stage is not None
    assert categorize_stage.error == "categorizer blew up"
    assert outcome.identity_errors == ("merchants",)
    rates_stage = outcome.stage("rates")
    assert rates_stage is not None
    assert rates_stage.count("rates_written") == 3
    assert outcome.rate_pairs_failed == ("EUR/USD",)
    assert outcome.rate_pairs_unsupported == ("EUR/XTS",)
    assert outcome.rate_pairs_discarded == ("GBP/USD",)
    assert outcome.has_failure is True


@pytest.mark.unit
def test_step_outcome_keeps_a_rates_step_that_never_ran_distinct_from_zero() -> None:
    """A step that never ran must not read as a step that found nothing.

    Neither is a failure and both leave the pair lists empty, so the stage is
    the only thing that separates them: absent when the caller never asked for
    rates, and present with ``ran=True`` and a zero count when the step ran and
    honestly wrote nothing.
    """
    from moneybin.orchestration.refresh import RefreshResult, step_outcome

    never_ran = step_outcome(RefreshResult(applied=True, duration_seconds=1.0))
    found_nothing = step_outcome(
        RefreshResult(
            applied=True,
            duration_seconds=1.0,
            stages=(StageOutcome(step="rates", ran=True, counts={"rates_written": 0}),),
        )
    )

    assert never_ran.stage("rates") is None
    assert never_ran.has_failure is False
    stage = found_nothing.stage("rates")
    assert stage is not None
    assert stage.ran is True
    assert stage.count("rates_written") == 0
    assert found_nothing.has_failure is False


@pytest.mark.unit
def test_a_failed_apply_is_not_a_best_effort_failure() -> None:
    """The SQLMesh apply rides in ``stages`` but is not what ``has_failure`` asks.

    It is the one step that can hard-fail: every carrier reports it separately
    as ``transforms_error``, and it gates a non-zero exit the best-effort steps
    do not. A caller reading ``has_failure`` is asking "did something degrade
    that I can offer a retry for?" — counting the apply there answers yes for a
    blocker the user has to fix before a retry means anything.
    """
    from moneybin.services.refresh_outcome import RefreshStepOutcome

    apply_failed = RefreshStepOutcome(
        stages=(StageOutcome(step="transform", ran=False, error="model boom"),)
    )
    assert apply_failed.has_failure is False

    # The second half of the boundary: the exclusion is scoped to the apply, so
    # a best-effort step failing in the same run still answers True. Without
    # this, deleting the stage check outright would pass.
    also_matcher = RefreshStepOutcome(
        stages=(
            StageOutcome(step="transform", ran=False, error="model boom"),
            StageOutcome(step="match", ran=True, error="matcher boom"),
        )
    )
    assert also_matcher.has_failure is True


# --- Requirement 18: per-stage outcomes reach the caller ---------------------
#
# `refresh` runs six steps on the user's behalf and, at spec time, four of them
# computed a real outcome number and dropped it: gsheet and categorize logged
# theirs, identity discarded both service return values. A renderer cannot
# recover a count the service already threw away, and must not re-query for it,
# so the counts travel out on the result carrier.


def test_refresh_reports_what_the_categorizer_did(
    patched_services: dict[str, MagicMock],
) -> None:
    """A run that recategorized 400 rows differs from one that did nothing.

    The categorize step already computes this breakdown and logs it at DEBUG.
    Requirement 18 needs it on the result instead, because stderr at default
    verbosity is where the user reads the outcome.
    """
    patched_services["categorize_pending"].return_value = {
        "total": 400,
        "merchant": 250,
        "rule": 120,
        "plaid": 30,
    }

    result = refresh(db=MagicMock(spec=Database), steps=["transform", "categorize"])

    stage = result.stage("categorize")
    assert stage is not None
    assert stage.ran is True
    assert stage.counts == {
        "total": 400,
        "merchant": 250,
        "rule": 120,
        "plaid": 30,
    }


def test_refresh_records_a_categorize_that_ran_and_found_nothing(
    patched_services: dict[str, MagicMock],
) -> None:
    """A zero outcome is still an outcome and still reports.

    Requirement 18 names the zero case specifically: a stage that stays silent
    reads as a stage that was never reached, so ``ran`` is True and the counts
    are present and zero.
    """
    patched_services["categorize_pending"].return_value = {
        "total": 0,
        "merchant": 0,
        "rule": 0,
        "plaid": 0,
    }

    result = refresh(db=MagicMock(spec=Database), steps=["transform", "categorize"])

    stage = result.stage("categorize")
    assert stage is not None
    assert stage.ran is True
    assert stage.counts["total"] == 0


def test_refresh_omits_a_stage_the_caller_never_asked_for(
    patched_services: dict[str, MagicMock],
) -> None:
    """Narrowing ``steps`` narrows the stage list, so the renderer stays quiet.

    A note for every canonical step would tell a user who ran
    ``--step transform`` about five stages their command excluded.
    """
    result = refresh(db=MagicMock(spec=Database), steps=["transform"])

    assert result.stage("categorize") is None
    # Only the step that was asked for — the other five contribute nothing.
    assert [s.step for s in result.stages] == ["transform"]


def test_refresh_marks_a_requested_categorize_that_could_not_run(
    patched_services: dict[str, MagicMock],
) -> None:
    """Asked-for-but-declined is neither a zero outcome nor an absence.

    On a first load the categorizer's views postdate SQLMesh apply, so the step
    is reached and correctly declines. That is not a crash — no error — but the
    user must not read it as "categorized nothing", which is what a zero count
    would say.
    """
    patched_services["categorize_pending"].side_effect = duckdb.CatalogException(
        "Table with name stg_transactions does not exist!"
    )

    result = refresh(db=MagicMock(spec=Database), steps=["transform", "categorize"])

    stage = result.stage("categorize")
    assert stage is not None
    assert stage.ran is False
    assert stage.error is None
    assert stage.counts == {}


def test_identity_step_reports_what_both_domains_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both identity services return a count, and both were being thrown away.

    ``AccountLinksService.run`` returns an int and ``MerchantLinksService.run``
    a ``HarvestResult``; the step's loop called each for its side effect and
    kept only the labels of the ones that raised, so a clean pass had no
    observable outcome at all.
    """

    def _accounts(_db: Database) -> MagicMock:
        return MagicMock(run=MagicMock(return_value=7))

    def _merchants(_db: Database) -> MagicMock:
        return MagicMock(
            run=MagicMock(return_value=HarvestResult(bound=12, conflicts=3))
        )

    monkeypatch.setattr(
        "moneybin.services.account_links_service.AccountLinksService", _accounts
    )
    monkeypatch.setattr(
        "moneybin.services.merchant_links_service.MerchantLinksService", _merchants
    )

    stage, errors = _run_identity_step(MagicMock(spec=Database))

    assert errors == ()
    assert stage.ran is True
    assert stage.counts == {
        "accounts_linked": 7,
        "merchants_bound": 12,
        "merchant_conflicts": 3,
    }


def test_identity_step_keeps_the_healthy_domain_s_count_when_the_other_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One domain failing must not erase what the other actually did.

    The two domains route to different remedies, so the failed label and the
    surviving count both have to travel.
    """

    def _accounts(_db: Database) -> MagicMock:
        return MagicMock(run=MagicMock(side_effect=RuntimeError("accounts boom")))

    def _merchants(_db: Database) -> MagicMock:
        return MagicMock(
            run=MagicMock(return_value=HarvestResult(bound=12, conflicts=0))
        )

    monkeypatch.setattr(
        "moneybin.services.account_links_service.AccountLinksService", _accounts
    )
    monkeypatch.setattr(
        "moneybin.services.merchant_links_service.MerchantLinksService", _merchants
    )

    stage, errors = _run_identity_step(MagicMock(spec=Database))

    assert errors == ("accounts",)
    assert "accounts_linked" not in stage.counts
    assert stage.counts["merchants_bound"] == 12


def test_refresh_reports_what_the_gsheet_pull_fetched(
    patched_services: dict[str, MagicMock],
) -> None:
    """The gsheet step's two numbers were computed for a log line and dropped.

    ``refresh`` itself derives completed-connection and row counts to log them,
    so the renderer needs no new query — only the carrier.
    """
    patched_services["gsheet_pull"].return_value = [
        MagicMock(
            status="complete",
            load_result=MagicMock(rows_inserted=40, rows_upserted=2),
        ),
        MagicMock(
            status="complete",
            load_result=MagicMock(rows_inserted=8, rows_upserted=0),
        ),
        MagicMock(status="partial", load_result=None),
    ]

    result = refresh(db=MagicMock(spec=Database), steps=["gsheet", "transform"])

    stage = result.stage("gsheet")
    assert stage is not None
    assert stage.ran is True
    assert stage.counts == {"completed": 2, "rows": 50, "non_complete": 1}


def test_refresh_reports_what_the_matcher_examined_as_a_stage(
    patched_services: dict[str, MagicMock],
) -> None:
    """The match counts already existed on the result; they join the stage list.

    Leaving them as flat fields beside a structured stage list would be two
    ways to say the same thing, which is what the renderer would then have to
    reconcile. ``transfers_retired`` is the deliberate exception and stays on
    the result: ``AccountLinksService.set`` adds account-collapse retirements
    the matcher never saw, so a match stage reporting the sum would be claiming
    a number the matcher did not produce.
    """
    patched_services["matcher_run"].return_value = _match_double(
        has_matches=True,
        has_pending=True,
        auto_merged=4,
        pending_review=2,
        pending_transfers=1,
        summary=MagicMock(return_value="4 auto-merged, 2 pending"),
    )

    result = refresh(db=MagicMock(spec=Database), steps=["match", "transform"])

    stage = result.stage("match")
    assert stage is not None
    assert stage.ran is True
    assert stage.counts == {
        "auto_merged": 4,
        "pending_review": 2,
        "pending_transfers": 1,
    }
    assert result.transfers_retired == 0


def test_refresh_marks_a_match_step_whose_views_were_missing(
    patched_services: dict[str, MagicMock],
) -> None:
    """A skipped match must not report zero duplicates found.

    Nothing was examined, so a zero count would be an invented result. That is
    the distinction ``ran`` exists to draw: the step was requested, reached, and
    declined.
    """
    patched_services["matcher_run"].side_effect = duckdb.CatalogException(
        "Table with name fct_transactions does not exist!"
    )

    result = refresh(db=MagicMock(spec=Database), steps=["match", "transform"])

    stage = result.stage("match")
    assert stage is not None
    assert stage.ran is False
    assert stage.counts == {}


def test_refresh_reports_the_rates_step_as_a_stage(
    monkeypatch: pytest.MonkeyPatch, patched_services: dict[str, MagicMock]
) -> None:
    """The rates step's count moves into the stage list with the others."""

    def _rates(_db: Database) -> tuple[RateBackfillResult | None, str | None]:
        return (
            RateBackfillResult(
                rates_written=31,
                pairs_failed=(),
                pairs_unsupported=(),
                pairs_discarded=(),
            ),
            None,
        )

    monkeypatch.setattr("moneybin.orchestration.refresh._run_rates_step", _rates)

    result = refresh(db=MagicMock(spec=Database), steps=["transform", "rates"])

    stage = result.stage("rates")
    assert stage is not None
    assert stage.ran is True
    assert stage.counts == {"rates_written": 31}


def test_refresh_lists_stages_in_canonical_pipeline_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The renderer prints the list in order, so the order is the contract.

    A note sequence that does not match the order the steps actually ran in
    would misdescribe the pipeline it is reporting on.
    """
    calls: list[str] = []
    patch_all_refresh_stages(monkeypatch, calls)

    result = refresh(db=MagicMock(spec=Database), steps=None)

    assert [s.step for s in result.stages] == [
        "gsheet",
        "match",
        "transform",
        "categorize",
        "identity",
        "rates",
    ]
