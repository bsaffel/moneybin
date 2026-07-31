"""Tests for `investments prices pull` and its refresh wiring.

`pull` writes `raw.security_prices`; every consumer reads
`core.fct_security_prices`. Nothing sits between them, so a pull whose rows
never reach a SQLMesh apply is invisible to `prices list`, to
`investments holdings`, and to every total that sums a market value. These
tests pin the two ways out: the command says so, and `--refresh` closes the
loop itself.

CLI tests mock the service layer and assert argument parsing, exit codes, and
output shape.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.investments.prices import app
from moneybin.services.price_service import PullResult
from moneybin.services.refresh import RefreshResult

runner = CliRunner()

_REFRESH_HINT = "moneybin refresh run"


def _pull_result(*, rows_written: int = 12, securities_priced: int = 4) -> PullResult:
    return PullResult(
        rows_written=rows_written,
        observations=rows_written,
        securities_priced=securities_priced,
        queued_for_review=0,
        unpriced=(),
    )


def _patched_pull(result: PullResult):
    """Patch the whole DB + service seam `prices pull` opens."""
    return patch(
        "moneybin.services.price_service.PriceService.pull", return_value=result
    )


class TestPricesPullRefresh:
    """`pull` must never leave its rows silently unreachable."""

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_pull(_pull_result())
    def test_pull_names_the_command_that_makes_its_rows_visible(
        self, _pull: MagicMock, _db: MagicMock
    ) -> None:
        """Without --refresh the rows sit in raw until a SQLMesh apply runs.

        `prices list` reads core.fct_security_prices, so a user who pulls and
        immediately lists sees the pre-pull series with nothing explaining why.
        """
        result = runner.invoke(app, ["pull"])

        assert result.exit_code == 0
        assert _REFRESH_HINT in result.output

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_pull(_pull_result(rows_written=0, securities_priced=0))
    def test_a_pull_that_wrote_nothing_suggests_nothing(
        self, _pull: MagicMock, _db: MagicMock
    ) -> None:
        """No new rows means no stale core — the hint would be noise."""
        result = runner.invoke(app, ["pull"])

        assert result.exit_code == 0
        assert _REFRESH_HINT not in result.output

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_pull(_pull_result())
    def test_quiet_suppresses_the_hint(self, _pull: MagicMock, _db: MagicMock) -> None:
        """-q drops status lines; the hint is one."""
        result = runner.invoke(app, ["pull", "--quiet"])

        assert result.exit_code == 0
        assert _REFRESH_HINT not in result.output

    @patch("moneybin.services.refresh.refresh")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_pull(_pull_result())
    def test_refresh_rebuilds_only_the_transform_step(
        self, _pull: MagicMock, _db: MagicMock, mock_refresh: MagicMock
    ) -> None:
        """Prices reach holdings through SQLMesh alone.

        Matching, categorization, and identity are transaction-side stages that
        no price row can affect, so a price refresh that ran them would charge
        the user for work with no output.
        """
        mock_refresh.return_value = RefreshResult(applied=True, duration_seconds=1.5)

        result = runner.invoke(app, ["pull", "--refresh"])

        assert result.exit_code == 0
        assert mock_refresh.call_count == 1
        assert mock_refresh.call_args.kwargs["steps"] == ["transform"]
        # Having done the work, it must not also tell the user to do it.
        assert _REFRESH_HINT not in result.output

    @patch("moneybin.services.refresh.refresh")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_pull(_pull_result())
    def test_a_failed_apply_exits_nonzero_and_says_the_rows_landed(
        self,
        _pull: MagicMock,
        _db: MagicMock,
        mock_refresh: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A soft-failing refresh leaves the exit code as the only stop signal.

        raw.security_prices is append-only and the pull already committed, so
        the retry is a bare `refresh run` — re-pulling would fetch the same
        closes again against a rate-limited provider for nothing.
        """
        mock_refresh.return_value = RefreshResult(
            applied=False, duration_seconds=None, error="model VTI not found"
        )

        with caplog.at_level(logging.WARNING):
            result = runner.invoke(app, ["pull", "--refresh"])

        assert result.exit_code == 1
        assert "model VTI not found" in caplog.text
        assert _REFRESH_HINT in caplog.text

    @patch("moneybin.services.refresh.refresh")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_pull(_pull_result())
    def test_json_carries_the_refresh_outcome(
        self, _pull: MagicMock, _db: MagicMock, mock_refresh: MagicMock
    ) -> None:
        """An agent gets the same signal the text hint gives a human."""
        mock_refresh.return_value = RefreshResult(applied=True, duration_seconds=1.5)

        result = runner.invoke(app, ["pull", "--refresh", "--output", "json"])

        assert result.exit_code == 0
        assert json.loads(result.output)["data"]["refreshed"] is True

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_pull(_pull_result())
    def test_json_reports_an_unrefreshed_pull_as_unrefreshed(
        self, _pull: MagicMock, _db: MagicMock
    ) -> None:
        """The default path must not read as though core were current."""
        result = runner.invoke(app, ["pull", "--output", "json"])

        assert result.exit_code == 0
        assert json.loads(result.output)["data"]["refreshed"] is False
