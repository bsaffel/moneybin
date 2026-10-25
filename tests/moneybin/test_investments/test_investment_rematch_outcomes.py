"""Account merges retain planner failures after the durable identity decision."""

import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.adapters import rematch_report
from moneybin.cli.commands.accounts.links import app
from moneybin.mcp.tools import accounts as accounts_module
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services.refresh_outcome import StageOutcome
from tests.moneybin.test_mcp.test_accounts import (
    _stub_merge_prompt,  # pyright: ignore[reportPrivateUsage]  # existing tool-boundary fixture
)
from tests.moneybin.test_mcp.test_review import (
    _decide_with_rematch,  # pyright: ignore[reportPrivateUsage]  # existing tool-boundary fixture
)


def _blocked(*, deterministic: bool, cash_failed: bool = False) -> RefreshResult:
    return RefreshResult(
        applied=False,
        duration_seconds=None,
        requested_steps=("match", "investment_match", "transform"),
        stages=(
            StageOutcome(
                step="match",
                ran=True,
                counts={"auto_merged": 2},
                error="Cash matcher failed" if cash_failed else None,
            ),
            StageOutcome(
                step="investment_match",
                ran=True,
                error="Planner refused",
                error_code=(
                    error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE
                    if deterministic
                    else None
                ),
            ),
        ),
    )


@pytest.mark.parametrize("deterministic", [True, False])
def test_rematch_names_blocked_rebuild_and_uses_classified_recovery(
    deterministic: bool,
) -> None:
    result = _blocked(deterministic=deterministic, cash_failed=True)
    hints = " ".join(rematch_report.rematch_actions(result))
    assert "rebuild" in hints.lower()
    assert "not reflected" in hints.lower()
    recovery = rematch_report.rematch_recovery_actions(result)
    steps = [
        action.arguments["steps"] for action in recovery if action.tool == "refresh_run"
    ]
    assert ["match"] in steps
    if deterministic:
        assert ["transform"] not in steps
        assert "refresh_run(steps=['match','transform'])" not in hints
        assert "Retrying unchanged inputs cannot succeed" in hints
    else:
        assert ["transform"] in steps


@pytest.mark.parametrize("deterministic", [True, False])
def test_cli_merge_exits_nonzero_when_planner_prevents_rebuild(
    deterministic: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        patch("moneybin.cli.commands.accounts.links.get_database") as database,
        patch(
            "moneybin.services.account_links_service.AccountLinksService.set"
        ) as set_link,
        caplog.at_level(logging.INFO),
    ):
        database.return_value.__enter__.return_value = MagicMock()
        set_link.return_value = _blocked(deterministic=deterministic, cash_failed=True)
        result = CliRunner().invoke(
            app, ["set", "proposal", "--into", "candidate", "--yes"]
        )
    assert result.exit_code == 1
    assert set_link.call_count == 1
    assert "not reflected" in result.output
    assert "moneybin refresh --step match" in result.output
    if deterministic:
        assert "re-run 'moneybin refresh'" not in result.output
        assert "re-run 'moneybin refresh'" not in caplog.text
        assert "moneybin refresh --step transform" not in result.output
        assert "Retrying unchanged inputs cannot succeed" in caplog.text
    else:
        assert "moneybin refresh --step transform" in result.output


@pytest.mark.parametrize("surface", ["accounts", "identity_batch"])
@pytest.mark.parametrize("deterministic", [True, False])
@pytest.mark.asyncio
async def test_mcp_merge_surfaces_blocked_planner_with_classified_recovery(
    surface: str,
    deterministic: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rematch = _blocked(deterministic=deterministic)
    if surface == "identity_batch":
        response = await _decide_with_rematch(monkeypatch, rematch)
    else:

        def apply(*_args: Any, **_kwargs: Any) -> RefreshResult:
            return rematch

        async def granted(**_kwargs: Any) -> Any:
            def verify(_binding: Any) -> None:
                return None

            return SimpleNamespace(verify=verify)

        monkeypatch.setattr(accounts_module, "_apply_account_accept", apply)
        monkeypatch.setattr(accounts_module, "grant_confirmation_or_raise", granted)
        _stub_merge_prompt(monkeypatch, accounts_module)
        response = await accounts_module.accounts_links_set(
            decision_id="dec001", action="accept", target_account_id="CAND001"
        )
    assert response.error is None
    assert "not reflected" in " ".join(response.actions)
    steps = [
        action.arguments["steps"]
        for action in response.recovery_actions or []
        if action.tool == "refresh_run"
    ]
    if deterministic:
        assert ["transform"] not in steps
        assert "Retrying unchanged inputs cannot succeed" in " ".join(response.actions)
    else:
        assert ["transform"] in steps
