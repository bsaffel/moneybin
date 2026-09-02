"""Tests for the `fx` command group: resolve a rate, correct one, remove it.

CLI tests mock the service layer and assert argument parsing, exit codes, and
output shape. `CurrencyService` itself is covered by
`tests/moneybin/test_services/test_currency_service.py`.

Two things here are contract rather than convenience. A conversion the user
cannot audit is worse than one that did not happen, so `rate` has to name the
day its number was published, not only the day asked about. And the two ways a
rate can be absent — a currency the provider prices on no date, and a date it
happens to lack — carry different remedies, so they must stay distinguishable
after passing through the CLI's error handler.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.commands.fx import app
from moneybin.services.currency_service import (
    RateUnavailableError,
    ResolvedRate,
)

runner = CliRunner()


def _resolved(
    *,
    requested: date = date(2026, 3, 13),
    published: date = date(2026, 3, 13),
    rate: Decimal = Decimal("0.87138000"),
    source: str = "frankfurter",
) -> ResolvedRate:
    return ResolvedRate(
        from_currency="USD",
        to_currency="EUR",
        requested_date=requested,
        rate_date=published,
        rate=rate,
        source=source,
    )


def _patched_resolve(result: ResolvedRate):
    return patch(
        "moneybin.services.currency_service.CurrencyService.resolve_rate",
        return_value=result,
    )


def _patched_resolve_raising(error: RateUnavailableError):
    return patch(
        "moneybin.services.currency_service.CurrencyService.resolve_rate",
        side_effect=error,
    )


class TestFxRate:
    """`fx rate` answers one pair and one date, with its provenance."""

    @patch("moneybin.cli.commands.fx.get_database")
    @_patched_resolve(_resolved())
    def test_rate_prints_the_rate_and_the_day_it_was_published(
        self, _resolve: MagicMock, _db: MagicMock
    ) -> None:
        """Requirement 10: the rate behind a figure, and the day it applied."""
        result = runner.invoke(app, ["rate", "USD", "EUR", "2026-03-13"])

        assert result.exit_code == 0
        assert "0.87138" in result.output
        assert "2026-03-13" in result.output
        assert "frankfurter" in result.output

    @patch("moneybin.cli.commands.fx.get_database")
    @_patched_resolve(
        _resolved(requested=date(2026, 3, 15), published=date(2026, 3, 13))
    )
    def test_a_weekend_says_which_day_actually_priced_it(
        self, _resolve: MagicMock, _db: MagicMock
    ) -> None:
        """A Sunday is priced with Friday's rate, and must say so.

        Printing only the number would report a rate for a day no provider
        published one — the same output a correct Sunday rate would produce.
        """
        result = runner.invoke(app, ["rate", "USD", "EUR", "2026-03-15"])

        assert result.exit_code == 0
        assert "2026-03-13" in result.output
        assert "2026-03-15" in result.output

    @patch("moneybin.cli.commands.fx.get_database")
    @_patched_resolve(_resolved())
    def test_rate_defaults_to_today_when_no_date_is_given(
        self, resolve: MagicMock, _db: MagicMock
    ) -> None:
        """The common question is "what is it worth now", not "on some date"."""
        result = runner.invoke(app, ["rate", "USD", "EUR"])

        assert result.exit_code == 0
        assert resolve.call_args.args[2] == date.today()  # noqa: DTZ011  # a calendar date, not an instant

    @patch("moneybin.cli.commands.fx.get_database")
    @_patched_resolve(
        _resolved(requested=date(2026, 3, 15), published=date(2026, 3, 13))
    )
    def test_rate_json_carries_both_dates_and_the_source(
        self, _resolve: MagicMock, _db: MagicMock
    ) -> None:
        """An agent gets the provenance the text line spells out."""
        result = runner.invoke(
            app, ["rate", "USD", "EUR", "2026-03-15", "--output", "json"]
        )

        assert result.exit_code == 0
        data = json.loads(result.output)["data"]
        assert data["requested_date"] == "2026-03-15"
        assert data["rate_date"] == "2026-03-13"
        assert data["source"] == "frankfurter"
        assert Decimal(str(data["rate"])) == Decimal("0.87138000")

    @patch("moneybin.cli.commands.fx.get_database")
    def test_a_malformed_date_exits_two_without_opening_the_database(
        self, db: MagicMock
    ) -> None:
        """A usage error is not a runtime failure, and costs no connection."""
        result = runner.invoke(app, ["rate", "USD", "EUR", "15-03-2026"])

        assert result.exit_code == 2
        assert db.call_count == 0

    @patch("moneybin.cli.commands.fx.get_database")
    @_patched_resolve_raising(
        RateUnavailableError(
            "The rate provider published no USD/EUR rate for the requested date.",
            code=error_codes.FX_RATE_UNAVAILABLE,
            hint="Nothing was published for 2026-03-13. Try a nearby date, or "
            "record the rate yourself with 'moneybin fx set'.",
        )
    )
    def test_an_unresolvable_rate_exits_one_and_says_why(
        self, _resolve: MagicMock, _db: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Nothing is substituted, so the exit code is the whole signal.

        The date assertion is the CLI half of the service's own guarantee: the
        message goes through ``logger.error`` and persists to the durable log,
        while the hint carrying the date goes to stderr and never does. Only this
        layer can prove the split, because the routing lives in
        ``handle_cli_errors`` rather than in the error.
        """
        with caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["rate", "USD", "EUR", "2026-03-13"])

        assert result.exit_code == 1
        assert "no USD/EUR rate" in caplog.text
        assert "2026-03-13" not in caplog.text
        assert "2026-03-13" in result.output, "the user is still told which day"

    @patch("moneybin.cli.commands.fx.get_database")
    @_patched_resolve_raising(
        RateUnavailableError(
            "The rate provider publishes no series for XPF.",
            code=error_codes.FX_CURRENCY_UNSUPPORTED,
            hint="Record the rate yourself with 'moneybin fx set' — this pair "
            "will not become available by retrying.",
        )
    )
    def test_a_permanently_unsupported_currency_keeps_its_own_code(
        self, _resolve: MagicMock, _db: MagicMock
    ) -> None:
        """Retrying fixes one absence and never fixes the other.

        Both reach the caller as exit 1, so the code is what an agent has to
        route on — collapsing them would send a user to retry a pair the
        provider will never publish.
        """
        result = runner.invoke(
            app, ["rate", "USD", "XPF", "2026-03-13", "--output", "json"]
        )

        assert result.exit_code == 1
        assert (
            json.loads(result.output)["error"]["code"]
            == error_codes.FX_CURRENCY_UNSUPPORTED
        )


class TestFxList:
    """`fx list` shows the resolved series already on disk."""

    @patch("moneybin.cli.commands.fx.get_database")
    @patch(
        "moneybin.services.currency_service.CurrencyService.list_rates",
        return_value=[
            _resolved(requested=date(2026, 3, 13), published=date(2026, 3, 13)),
            _resolved(
                requested=date(2026, 3, 12),
                published=date(2026, 3, 12),
                rate=Decimal("0.86000000"),
                source="override",
            ),
        ],
    )
    def test_list_renders_one_row_per_date_with_its_source(
        self, _rates: MagicMock, _db: MagicMock
    ) -> None:
        """The source column is why the series is readable at all.

        An override and a provider rate are both real answers for their date;
        without the source, a corrected day is indistinguishable from a
        provider that happened to publish that number.
        """
        result = runner.invoke(app, ["list", "USD", "EUR"])

        assert result.exit_code == 0
        assert "2026-03-13" in result.output
        assert "override" in result.output

    @patch("moneybin.cli.commands.fx.get_database")
    @patch(
        "moneybin.services.currency_service.CurrencyService.list_rates",
        return_value=[
            _resolved(requested=date(2026, 3, 13), published=date(2026, 3, 13))
        ],
    )
    def test_list_names_its_columns(self, _rates: MagicMock, _db: MagicMock) -> None:
        """Requirement 1: three padded values are a table, so render one.

        The series was three unlabelled columns, which left a reader to infer
        from the values which one was the rate and which the date.
        """
        result = runner.invoke(app, ["list", "USD", "EUR"])

        assert result.exit_code == 0
        assert "┃" in result.output
        for header in ("date", "rate", "source"):
            assert header in result.output

    @patch("moneybin.cli.commands.fx.get_database")
    @patch(
        "moneybin.services.currency_service.CurrencyService.list_rates",
        return_value=[],
    )
    def test_list_passes_since_through_as_a_date(
        self, rates: MagicMock, _db: MagicMock
    ) -> None:
        """--since is a bound on the query, not a filter applied afterwards."""
        result = runner.invoke(app, ["list", "USD", "EUR", "--since", "2026-03-01"])

        assert result.exit_code == 0
        assert rates.call_args.kwargs["since"] == date(2026, 3, 1)


class TestFxSet:
    """`fx set` records the user's own rate, outranking the provider."""

    @patch("moneybin.cli.commands.fx.get_database")
    @patch("moneybin.services.currency_service.CurrencyService.set_override")
    def test_set_passes_an_exact_decimal_and_a_parsed_date(
        self, override: MagicMock, _db: MagicMock
    ) -> None:
        """The rate must reach the service as Decimal, never through float.

        `raw.exchange_rates` and `app.exchange_rate_overrides` are both
        DECIMAL(18,8); a rate that arrives as a float has already lost the
        digits the column exists to keep.
        """
        result = runner.invoke(
            app, ["set", "usd", "eur", "2026-03-13", "0.87138000", "--note", "bank"]
        )

        assert result.exit_code == 0
        assert override.call_args.args[2] == date(2026, 3, 13)
        assert override.call_args.args[3] == Decimal("0.87138000")
        assert override.call_args.kwargs["note"] == "bank"

    @patch("moneybin.cli.commands.fx.get_database")
    def test_a_malformed_rate_exits_two_without_opening_the_database(
        self, db: MagicMock
    ) -> None:
        """A typo is a usage error, not a failed write."""
        result = runner.invoke(app, ["set", "USD", "EUR", "2026-03-13", "seven"])

        assert result.exit_code == 2
        assert db.call_count == 0

    @patch("moneybin.cli.commands.fx.get_database")
    def test_a_non_finite_rate_exits_two_without_opening_the_database(
        self, db: MagicMock
    ) -> None:
        """Decimal parses "NaN" as an ordinary literal; the column will not.

        Left alone it survives as a number and fails inside the service's
        positivity comparison, reporting an internal error for what is a typo.
        """
        result = runner.invoke(app, ["set", "USD", "EUR", "2026-03-13", "NaN"])

        assert result.exit_code == 2
        assert db.call_count == 0


class TestFxDelete:
    """`fx delete` returns a date to provider pricing."""

    @patch("moneybin.cli.commands.fx.get_database")
    @patch(
        "moneybin.services.currency_service.CurrencyService.delete_override",
        return_value=True,
    )
    def test_delete_confirms_a_removal(
        self, _delete: MagicMock, _db: MagicMock
    ) -> None:
        result = runner.invoke(app, ["delete", "USD", "EUR", "2026-03-13"])

        assert result.exit_code == 0
        assert "✅" in result.output

    @patch("moneybin.cli.commands.fx.get_database")
    @patch(
        "moneybin.services.currency_service.CurrencyService.delete_override",
        return_value=False,
    )
    def test_deleting_nothing_says_nothing_was_there(
        self, _delete: MagicMock, _db: MagicMock
    ) -> None:
        """Not an error — the end state the caller wanted already holds.

        Reporting a success would read as "your correction was removed", which
        is the one thing that did not happen.
        """
        result = runner.invoke(app, ["delete", "USD", "EUR", "2026-03-13"])

        assert result.exit_code == 0
        assert "No override" in result.output
        assert "✅" not in result.output

    @patch("moneybin.cli.commands.fx.get_database")
    @patch(
        "moneybin.services.currency_service.CurrencyService.delete_override",
        return_value=False,
    )
    def test_delete_json_reports_the_no_op_as_removed_false(
        self, _delete: MagicMock, _db: MagicMock
    ) -> None:
        """An agent needs the same distinction the text line makes."""
        result = runner.invoke(
            app, ["delete", "USD", "EUR", "2026-03-13", "--output", "json"]
        )

        assert result.exit_code == 0
        assert json.loads(result.output)["data"]["removed"] is False
