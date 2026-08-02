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

from moneybin import error_codes
from moneybin.cli.commands.investments.prices import PriceSourceChoice, app
from moneybin.errors import UserError
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


def _patched_resolve():
    return patch(
        "moneybin.services.price_service.PriceService.resolve_security",
        return_value="sec_privateco",
    )


class TestPriceMarkRefresh:
    """`set` and `delete` cross the same raw/core boundary `pull` does.

    A mark lands in `app.security_price_overrides`; `core.fct_security_prices` is
    a SQLMesh model built from it. So a corrective mark reports `✅ Marked` while
    `prices list` and every holdings valuation keep serving the price the user
    just overrode — indefinitely, until some unrelated refresh happens to run.
    That is the failure `pull` already has both remedies for, and these commands
    are the ones a user reaches for precisely *because* a price is wrong.
    """

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch("moneybin.services.price_service.PriceService.set_mark", return_value=None)
    def test_set_names_the_command_that_makes_the_mark_visible(
        self, _set: MagicMock, _resolve: MagicMock, _db: MagicMock
    ) -> None:
        result = runner.invoke(
            app, ["set", "PRIVCO", "2026-07-30", "42.50", "--currency", "USD"]
        )

        assert result.exit_code == 0
        assert _REFRESH_HINT in result.output

    @patch("moneybin.services.refresh.refresh")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch("moneybin.services.price_service.PriceService.set_mark", return_value=None)
    def test_set_refresh_rebuilds_only_the_transform_step(
        self,
        _set: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
        mock_refresh: MagicMock,
    ) -> None:
        """Same reasoning as `pull`: a mark cannot affect a transaction-side stage."""
        mock_refresh.return_value = RefreshResult(applied=True, duration_seconds=1.5)

        result = runner.invoke(
            app,
            ["set", "PRIVCO", "2026-07-30", "42.50", "--currency", "USD", "--refresh"],
        )

        assert result.exit_code == 0
        assert mock_refresh.call_count == 1
        assert mock_refresh.call_args.kwargs["steps"] == ["transform"]
        assert _REFRESH_HINT not in result.output

    @patch("moneybin.services.refresh.refresh")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch("moneybin.services.price_service.PriceService.set_mark", return_value=None)
    def test_a_failed_apply_after_set_exits_nonzero(
        self,
        _set: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
        mock_refresh: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The mark is committed; only the propagation failed, so retry the apply."""
        mock_refresh.return_value = RefreshResult(
            applied=False, duration_seconds=None, error="model VTI not found"
        )

        with caplog.at_level(logging.WARNING):
            result = runner.invoke(
                app,
                [
                    "set",
                    "PRIVCO",
                    "2026-07-30",
                    "42.50",
                    "--currency",
                    "USD",
                    "--refresh",
                ],
            )

        assert result.exit_code == 1
        assert "model VTI not found" in caplog.text
        assert _REFRESH_HINT in caplog.text

    @patch("moneybin.services.refresh.refresh")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch("moneybin.services.price_service.PriceService.set_mark", return_value=None)
    def test_set_json_carries_the_refresh_outcome(
        self,
        _set: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
        mock_refresh: MagicMock,
    ) -> None:
        """An agent gets the same signal the text hint gives a human."""
        mock_refresh.return_value = RefreshResult(applied=True, duration_seconds=1.5)

        result = runner.invoke(
            app,
            [
                "set",
                "PRIVCO",
                "2026-07-30",
                "42.50",
                "--currency",
                "USD",
                "--refresh",
                "--output",
                "json",
            ],
        )

        assert result.exit_code == 0
        assert json.loads(result.output)["data"]["refreshed"] is True

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch(
        "moneybin.services.price_service.PriceService.delete_mark", return_value=True
    )
    def test_delete_names_the_command_that_returns_the_date_to_provider_pricing(
        self, _delete: MagicMock, _resolve: MagicMock, _db: MagicMock
    ) -> None:
        """Removing a mark changes the resolved series exactly as writing one does."""
        result = runner.invoke(
            app, ["delete", "PRIVCO", "2026-07-30", "--currency", "USD"]
        )

        assert result.exit_code == 0
        assert _REFRESH_HINT in result.output

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch(
        "moneybin.services.price_service.PriceService.delete_mark", return_value=False
    )
    def test_a_delete_that_removed_nothing_suggests_nothing(
        self, _delete: MagicMock, _resolve: MagicMock, _db: MagicMock
    ) -> None:
        """No row removed means core is already current — the hint would be noise.

        Mirrors `test_a_pull_that_wrote_nothing_suggests_nothing`.
        """
        result = runner.invoke(
            app, ["delete", "PRIVCO", "2026-07-30", "--currency", "USD"]
        )

        assert result.exit_code == 0
        assert _REFRESH_HINT not in result.output

    @patch("moneybin.services.refresh.refresh")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch(
        "moneybin.services.price_service.PriceService.delete_mark", return_value=False
    )
    def test_a_delete_that_removed_nothing_does_not_pay_for_an_apply(
        self,
        _delete: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
        mock_refresh: MagicMock,
    ) -> None:
        """Nothing changed, so --refresh has nothing to propagate.

        Unlike `pull --refresh`, which stays unconditional because an earlier
        pull may have been left unapplied, a no-op delete cannot have stranded
        anything: the mark it would have removed was never there.
        """
        result = runner.invoke(
            app, ["delete", "PRIVCO", "2026-07-30", "--currency", "USD", "--refresh"]
        )

        assert result.exit_code == 0
        assert mock_refresh.call_count == 0


class TestMarkCurrencyDefault:
    """`set`/`delete` take the position's currency, or refuse — never a fixed USD.

    `core.dim_holdings` values a position only where the price's quote_currency
    equals the position's currency_code. A hardcoded USD default therefore wrote
    a mark, printed `✅ Marked`, and left every non-USD holding valued exactly as
    before — a wrong answer that announced itself as a right one.
    """

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch(
        "moneybin.services.price_service.PriceService.resolve_quote_currency",
        return_value="EUR",
    )
    @patch("moneybin.services.price_service.PriceService.set_mark", return_value=None)
    def test_set_marks_in_the_currency_the_position_is_held_in(
        self,
        set_mark: MagicMock,
        _currency: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
    ) -> None:
        result = runner.invoke(app, ["set", "PRIVCO", "2026-07-30", "42.50"])

        assert result.exit_code == 0
        assert set_mark.call_args.kwargs["quote_currency"] == "EUR"
        assert "EUR" in result.output, "the echo must name the currency actually used"

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch(
        "moneybin.services.price_service.PriceService.resolve_quote_currency",
        return_value="GBP",
    )
    @patch("moneybin.services.price_service.PriceService.set_mark", return_value=None)
    def test_an_explicit_currency_is_handed_to_the_resolver_verbatim(
        self,
        set_mark: MagicMock,
        derive: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
    ) -> None:
        """The CLI must not canonicalize on the way in.

        Whether an explicit code short-circuits the holdings lookup is the
        resolver's contract and is tested there. What this pins is that the CLI
        forwards the raw flag rather than upper-casing it first — a second
        spelling rule beside the resolver's own is how the two drift apart.
        """
        result = runner.invoke(
            app, ["set", "PRIVCO", "2026-07-30", "42.50", "--currency", "gbp"]
        )

        assert result.exit_code == 0
        assert derive.call_args.args[-1] == "gbp"
        assert set_mark.call_args.kwargs["quote_currency"] == "GBP"

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch(
        "moneybin.services.price_service.PriceService.resolve_quote_currency",
        return_value="EUR",
    )
    @patch(
        "moneybin.services.price_service.PriceService.delete_mark", return_value=True
    )
    def test_delete_targets_the_same_series_set_would_have_written(
        self,
        delete_mark: MagicMock,
        _currency: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
    ) -> None:
        """A delete that defaulted to USD could not reach a mark set in EUR.

        The two commands must derive identically or `delete` becomes a no-op that
        reports success on exactly the marks `set` was right to create.
        """
        result = runner.invoke(app, ["delete", "PRIVCO", "2026-07-30"])

        assert result.exit_code == 0
        assert delete_mark.call_args.kwargs["quote_currency"] == "EUR"

    @patch("moneybin.cli.commands.investments.prices.get_database")
    @_patched_resolve()
    @patch(
        "moneybin.services.price_service.PriceService.resolve_quote_currency",
        side_effect=UserError(
            "Cannot tell which currency to mark s1 in: it is held in AUD and GBP. "
            "Pass --currency to say which series this price belongs to.",
            code=error_codes.INVESTMENT_PRICE_MARK_CURRENCY_AMBIGUOUS,
        ),
    )
    @patch("moneybin.services.price_service.PriceService.set_mark", return_value=None)
    def test_set_refuses_rather_than_guessing_when_the_currency_is_ambiguous(
        self,
        set_mark: MagicMock,
        _currency: MagicMock,
        _resolve: MagicMock,
        _db: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The service's refusal has to survive the CLI, not be swallowed into a write.

        Which cases are ambiguous is the service's call and is tested there; this
        asserts the CLI's own half — a clean non-zero exit naming the flag, and
        critically nothing written on the way out. The message rides the error
        logger to stderr per cli.md, so it is read from caplog, not stdout.
        """
        result = runner.invoke(app, ["set", "PRIVCO", "2026-07-30", "42.50"])

        assert result.exit_code == 1
        assert not set_mark.called, "a refused mark must not write"
        assert "--currency" in caplog.text


class TestPriceArgumentParsing:
    """A malformed PRICE is a usage error, so it must exit 2 before any work.

    `Decimal` accepts `NaN` and `Infinity` as ordinary literals, so both survive
    parsing and fail much deeper — `NaN` on the `close <= 0` comparison, infinity
    inside DuckDB — surfacing as an internal error for what the user experiences
    as a typo.
    """

    # "-Infinity" is deliberately absent: Typer rejects it as an unknown option
    # before the parser sees it, so it would exit 2 with the finite check removed.
    @pytest.mark.parametrize("value", ["NaN", "Infinity", "inf", "nan"])
    @patch("moneybin.cli.commands.investments.prices.get_database")
    def test_set_refuses_a_non_finite_price(self, db: MagicMock, value: str) -> None:
        result = runner.invoke(app, ["set", "PRIVCO", "2026-07-30", value])

        assert result.exit_code == 2, f"{value!r} must be a usage error"
        assert not db.called, "a usage error must not open the database"

    @patch("moneybin.cli.commands.investments.prices.get_database")
    def test_set_still_refuses_ordinary_nonsense(self, db: MagicMock) -> None:
        """The finite check must not displace the pre-existing decimal check."""
        result = runner.invoke(app, ["set", "PRIVCO", "2026-07-30", "forty"])

        assert result.exit_code == 2
        assert not db.called


class TestPriceSourceFilter:
    """`--source` names a closed set, so an unknown value is a usage error.

    `list_prices` filters on equality. An unrecognized source therefore matches
    no row and returns an ordinary empty series at exit 0 — the same answer a
    real source with no stored closes gives. That is the one reply a typo must
    not be able to counterfeit, because it reads as evidence about the security
    rather than about the command.
    """

    def test_the_choices_are_the_sources_the_price_fact_can_produce(self) -> None:
        """Set equality, because both directions of drift are silent.

        A source that wins dates in `core.fct_security_prices` but is missing
        here is a series the user can no longer ask for; a spelling that drifts
        from the stored value filters to nothing while still exiting 0. The
        walk below derives its expectation from this enum, so it cannot see
        either — it proves the gate opens, not that it opens on the right five.
        """
        assert {choice.value for choice in PriceSourceChoice} == {
            "plaid",
            "tiingo",
            "coingecko",
            "override",
            "trade_implied",
        }

    @patch("moneybin.cli.commands.investments.prices.get_database")
    def test_list_refuses_a_source_outside_the_set(self, db: MagicMock) -> None:
        result = runner.invoke(app, ["list", "PRIVCO", "--source", "tiingoo"])

        assert result.exit_code == 2
        assert not db.called, "a usage error must not open the database"

    @patch("moneybin.services.price_service.build_price_service")
    @patch("moneybin.cli.commands.investments.prices.get_database")
    def test_list_accepts_every_source_it_documents(
        self, _db: MagicMock, build: MagicMock
    ) -> None:
        """A gate that refused all five would pass the typo test above.

        Walks the whole enum rather than sampling one member, so a source
        dropped from the choices — or misspelled in it — fails here instead of
        becoming a filter the user can no longer ask for.
        """
        service = build.return_value
        service.resolve_security.return_value = "s1"
        service.list_prices.return_value.rows = []

        for choice in PriceSourceChoice:
            result = runner.invoke(app, ["list", "PRIVCO", "--source", choice.value])

            assert result.exit_code == 0, choice.value
            assert service.list_prices.call_args.kwargs["source_type"] == choice.value
