"""Unit tests for the refresh service-layer cascade.

These tests mock the four dependent services (GSheetPullService,
MatchingService, TransformService, CategorizationService) and assert
that ``refresh()``:

- Runs the full cascade when ``steps=None`` (current default).
- Runs only the requested subset when ``steps`` is a list.
- Executes steps in canonical order (gsheet → match → transform → categorize)
  regardless of input-list order.
- Raises ``UserError(code="UNKNOWN_REFRESH_STEP")`` on unknown step names.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services import matching_service
from moneybin.services.refresh import RefreshResult, refresh
from moneybin.services.transform_service import ApplyResult


def _make_apply_result(applied: bool = True) -> ApplyResult:
    return ApplyResult(applied=applied, duration_seconds=1.0, error=None)


@pytest.fixture
def patched_services() -> Iterator[dict[str, MagicMock]]:
    """Patch all refresh backends and yield handles for call inspection."""
    gsheet_pull = MagicMock(return_value=[])
    matcher_run = MagicMock(
        return_value=MagicMock(has_matches=False, has_pending=False)
    )
    transform_apply = MagicMock(return_value=_make_apply_result(applied=True))
    categorize_pending = MagicMock(
        return_value={"total": 0, "rule": 0, "merchant": 0, "plaid": 0}
    )
    auto_stats = MagicMock(return_value=MagicMock(pending_proposals=0))
    identity = MagicMock(return_value=())

    # Patches target the consumer module (moneybin.services.refresh) where
    # each name is bound — refresh.py imports TransformService at module level
    # and the other backends via deferred imports, so patching the source
    # modules wouldn't intercept the call paths used here.
    with (
        patch(
            "moneybin.services.refresh._run_gsheet_step",
            gsheet_pull,
        ),
        patch(
            "moneybin.services.matching_service.MatchingService.run",
            matcher_run,
        ),
        patch(
            "moneybin.services.refresh.TransformService",
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
            "moneybin.services.refresh._run_identity_step",
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

    def _match(_self: matching_service.MatchingService) -> Any:
        calls.append("match")
        return MagicMock(has_matches=False, has_pending=False)

    def _transform(_db: Database) -> Any:
        service = MagicMock()

        def _apply() -> ApplyResult:
            calls.append("transform")
            return _make_apply_result()

        service.apply.side_effect = _apply
        return service

    def _categorize(_db: Database) -> str | None:
        calls.append("categorize")
        return None

    def _identity(_db: Database) -> tuple[str, ...]:
        calls.append("identity")
        return ()

    monkeypatch.setattr(
        "moneybin.services.refresh._run_gsheet_step",
        _gsheet,
    )
    monkeypatch.setattr(
        matching_service.MatchingService,
        "run",
        _match,
    )
    monkeypatch.setattr(
        "moneybin.services.refresh.TransformService",
        _transform,
    )
    monkeypatch.setattr(
        "moneybin.services.refresh._run_categorize_step",
        _categorize,
    )
    monkeypatch.setattr(
        "moneybin.services.refresh._run_identity_step",
        _identity,
        raising=False,
    )


@pytest.mark.unit
def test_refresh_result_has_error_surfacing_fields() -> None:
    """RefreshResult carries matcher/categorizer errors and self-heal records."""
    from moneybin.services.refresh import SelfHealRecord

    r = RefreshResult(applied=True, duration_seconds=1.0)
    assert r.matching_error is None
    assert r.categorization_error is None
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
        matching_error="boom",
        categorization_error="bang",
        identity_errors=("accounts",),
        self_heal_actions=(rec,),
    )
    assert r2.matching_error == "boom"
    assert r2.categorization_error == "bang"
    assert r2.identity_errors == ("accounts",)
    assert r2.self_heal_actions[0].recipe_id == "orphan_categorizations_cleanup"


@pytest.mark.unit
def test_refresh_matcher_crash_populates_matching_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """A real matcher crash sets matching_error; pipeline continues to transform."""
    patched_services["matcher_run"].side_effect = RuntimeError("matcher boom")
    result = refresh(MagicMock())
    assert result.matching_error == "matcher boom"
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
    assert result.matching_error == "matcher boom"  # matcher crash still preserved


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
    assert result.matching_error is None


@pytest.mark.unit
def test_refresh_categorizer_crash_populates_categorization_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """A real categorizer crash sets categorization_error; pipeline continues."""
    patched_services["categorize_pending"].side_effect = RuntimeError("cat boom")
    result = refresh(MagicMock())
    assert result.categorization_error == "cat boom"
    assert result.applied is True


@pytest.mark.unit
def test_refresh_auto_rule_stats_crash_is_not_a_categorization_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """A crash in the post-step auto-rule stats read must NOT set categorization_error.

    categorize_pending() succeeded; the proposal-count read is informational.
    Conflating the two would falsely tell the agent to retry categorization.
    """
    patched_services["auto_stats"].side_effect = RuntimeError("stats boom")
    result = refresh(MagicMock())
    assert result.categorization_error is None
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
    assert result.categorization_error is None


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

    assert calls == ["gsheet", "match", "transform", "categorize", "identity"]


@pytest.mark.unit
def test_identity_can_run_surgically(monkeypatch: pytest.MonkeyPatch) -> None:
    """Identity can generate proposals without rebuilding derived tables."""
    calls: list[str] = []
    patch_all_refresh_stages(monkeypatch, calls)

    result = refresh(MagicMock(), steps=["identity"])

    assert calls == ["identity"]
    assert result.applied is False


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

    def _merchants_run() -> None:
        calls.append("merchants")

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

    caplog.set_level(logging.ERROR, logger="moneybin.services.refresh")
    result = refresh(MagicMock(), steps=["identity"])

    accounts_run.assert_called_once()
    merchants_run.assert_called_once()
    assert calls == ["accounts", "merchants"]
    assert result.identity_errors == ("accounts",)
    refresh_records = [
        record
        for record in caplog.records
        if record.name == "moneybin.services.refresh"
    ]
    assert len(refresh_records) == 1
    assert sensitive_error not in refresh_records[0].getMessage()
    assert (
        "accounts identity backfill failed: RuntimeError"
        in refresh_records[0].getMessage()
    )
    assert all(record.exc_info is None for record in refresh_records)


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
        return MagicMock(has_matches=False, has_pending=False)

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
    assert excinfo.value.code == "UNKNOWN_REFRESH_STEP"
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
        return MagicMock(has_matches=False, has_pending=False)

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
