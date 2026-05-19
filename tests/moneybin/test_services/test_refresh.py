"""Unit tests for the refresh service-layer cascade.

These tests mock the three dependent services (MatchingService,
TransformService, CategorizationService) and assert that ``refresh()``:

- Runs the full cascade when ``steps=None`` (current default).
- Runs only the requested subset when ``steps`` is a list.
- Executes steps in canonical order (match → transform → categorize)
  regardless of input-list order.
- Raises ``UserError(code="UNKNOWN_REFRESH_STEP")`` on unknown step names.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from moneybin.errors import UserError
from moneybin.services.refresh import RefreshResult, refresh
from moneybin.services.transform_service import ApplyResult


def _make_apply_result(applied: bool = True) -> ApplyResult:
    return ApplyResult(applied=applied, duration_seconds=1.0, error=None)


@pytest.fixture
def patched_services() -> Iterator[dict[str, MagicMock]]:
    """Patch all three step backends and yield handles for call inspection."""
    matcher_run = MagicMock(
        return_value=MagicMock(has_matches=False, has_pending=False)
    )
    transform_apply = MagicMock(return_value=_make_apply_result(applied=True))
    categorize_pending = MagicMock(return_value={"total": 0, "rule": 0, "merchant": 0})
    auto_stats = MagicMock(return_value=MagicMock(pending_proposals=0))

    # Patches target the consumer module (moneybin.services.refresh) where
    # each name is bound — refresh.py imports TransformService at module level
    # and the other backends via deferred imports, so patching the source
    # modules wouldn't intercept the call paths used here.
    with (
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
    ):
        yield {
            "matcher_run": matcher_run,
            "transform_apply": transform_apply,
            "categorize_pending": categorize_pending,
        }


@pytest.mark.unit
def test_refresh_steps_none_runs_full_cascade(
    patched_services: dict[str, MagicMock],
) -> None:
    """``steps=None`` (default) preserves current behavior: all three steps run."""
    result = refresh(MagicMock())
    assert isinstance(result, RefreshResult)
    assert result.applied is True
    assert patched_services["matcher_run"].call_count == 1
    assert patched_services["transform_apply"].call_count == 1
    assert patched_services["categorize_pending"].call_count == 1


@pytest.mark.unit
def test_refresh_steps_transform_only(patched_services: dict[str, MagicMock]) -> None:
    """``steps=["transform"]`` skips match and categorize."""
    result = refresh(MagicMock(), steps=["transform"])
    assert result.applied is True
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
    """Input-list order is ignored; canonical order match→transform→categorize wins."""
    call_log: list[str] = []

    def _match_side(*a: Any, **kw: Any) -> MagicMock:
        call_log.append("match")
        return MagicMock(has_matches=False, has_pending=False)

    def _transform_side(*a: Any, **kw: Any) -> ApplyResult:
        call_log.append("transform")
        return _make_apply_result(applied=True)

    def _categorize_side(*a: Any, **kw: Any) -> dict[str, int]:
        call_log.append("categorize")
        return {"total": 0, "rule": 0, "merchant": 0}

    patched_services["matcher_run"].side_effect = _match_side
    patched_services["transform_apply"].side_effect = _transform_side
    patched_services["categorize_pending"].side_effect = _categorize_side

    refresh(MagicMock(), steps=["categorize", "transform", "match"])
    assert call_log == ["match", "transform", "categorize"]


@pytest.mark.unit
def test_refresh_unknown_step_raises_user_error(
    patched_services: dict[str, MagicMock],
) -> None:
    """Unknown step name raises UserError with hint enumerating valid steps."""
    with pytest.raises(UserError) as excinfo:
        refresh(MagicMock(), steps=["transform", "bogus"])
    assert excinfo.value.code == "UNKNOWN_REFRESH_STEP"
    assert "match" in (excinfo.value.hint or "")
    assert "transform" in (excinfo.value.hint or "")
    assert "categorize" in (excinfo.value.hint or "")
    # None of the step backends should run when validation fails.
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
    assert patched_services["matcher_run"].call_count == 0
    assert patched_services["transform_apply"].call_count == 0
    assert patched_services["categorize_pending"].call_count == 0
