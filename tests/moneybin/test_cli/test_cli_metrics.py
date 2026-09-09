"""The three CLI counters cli-output-coherence.md's Observability section declares.

Each is labelled by command, and the label comes from `derive_cli_actor()` —
the same derivation the audit trail uses — so a counter and an audit row name
one command the same way.
"""

from __future__ import annotations

from collections.abc import Callable

import typer
from prometheus_client import REGISTRY
from typer.testing import CliRunner

from moneybin.cli.render import column_view, render_rows

runner = CliRunner()


def _count(name: str, command: str) -> float:
    return REGISTRY.get_sample_value(name, {"command": command}) or 0.0


# A probe app rather than a real command: these assertions are about the
# counter and its label, and a real command would drag its service and database
# in to prove a fact neither participates in.
probe_app = typer.Typer()
probe_group = typer.Typer()
probe_app.add_typer(probe_group, name="probe")

_Record = dict[str, str]
_COLUMNS: list[tuple[str, Callable[[_Record], object]]] = [
    ("first", lambda record: record["first"]),
    ("second", lambda record: record["second"]),
]
_NO_RECORDS: list[_Record] = []


@probe_group.command("widened")
def probe_widened() -> None:
    column_view(_COLUMNS, _NO_RECORDS, default=["first"], wide=True)


@probe_group.command("narrow")
def probe_narrow() -> None:
    column_view(_COLUMNS, _NO_RECORDS, default=["first"], wide=False)


@probe_group.command("omitting")
def probe_omitting() -> None:
    render_rows(["first"], [("a",)], total_columns=4)


@probe_group.command("whole")
def probe_whole() -> None:
    render_rows(["first"], [("a",)], total_columns=1)


def test_wide_is_counted_against_the_command_that_was_asked() -> None:
    """The `--wide` half of the wide-versus-omitted ratio."""
    before = _count("moneybin_cli_wide_requested_total", "probe_widened")

    result = runner.invoke(probe_app, ["probe", "widened"])

    assert result.exit_code == 0, result.output
    assert _count("moneybin_cli_wide_requested_total", "probe_widened") == before + 1


def test_a_default_render_counts_no_wide_request() -> None:
    """The other half of that boundary.

    Counting every render would make the ratio the spec reads as a signal come
    out at 1.0 for every command, which is the same as not collecting it.
    """
    before = _count("moneybin_cli_wide_requested_total", "probe_narrow")

    result = runner.invoke(probe_app, ["probe", "narrow"])

    assert result.exit_code == 0, result.output
    assert _count("moneybin_cli_wide_requested_total", "probe_narrow") == before


def test_a_narrowed_render_counts_the_omission() -> None:
    """The omission half — the same render that prints the disclosure line."""
    before = _count("moneybin_cli_columns_omitted_total", "probe_omitting")

    result = runner.invoke(probe_app, ["probe", "omitting"])

    assert result.exit_code == 0, result.output
    assert _count("moneybin_cli_columns_omitted_total", "probe_omitting") == before + 1


def test_a_render_of_the_whole_projection_counts_no_omission() -> None:
    """Nothing was withheld, so nothing is counted."""
    before = _count("moneybin_cli_columns_omitted_total", "probe_whole")

    result = runner.invoke(probe_app, ["probe", "whole"])

    assert result.exit_code == 0, result.output
    assert _count("moneybin_cli_columns_omitted_total", "probe_whole") == before


def test_a_library_caller_outside_a_command_counts_nothing() -> None:
    """`column_view` serves MCP and library callers too, which have no command.

    Counting them would attribute a caller that pressed no flag to whichever
    command label happened to be reachable — or force a fallback label that
    silently aggregates unrelated traffic.
    """

    def total() -> float:
        return sum(
            sample.value
            for metric in REGISTRY.collect()
            if metric.name == "moneybin_cli_wide_requested"
            for sample in metric.samples
            if sample.name.endswith("_total")
        )

    before = total()

    column_view(_COLUMNS, _NO_RECORDS, default=["first"], wide=True)

    assert total() == before


def test_invoking_a_stub_counts_it_against_that_command() -> None:
    """Requirement 31 hides stubs from `--help` but keeps them invocable.

    Which hidden stubs users still reach is the demand signal for building one,
    and it is unobservable without this.
    """
    from moneybin.cli.main import app

    before = _count("moneybin_cli_stub_invoked_total", "sync_schedule_show")

    result = runner.invoke(app, ["sync", "schedule", "show"])

    assert result.exit_code == 0, result.output
    assert _count("moneybin_cli_stub_invoked_total", "sync_schedule_show") == before + 1


def test_an_unfinished_mode_of_a_working_command_is_not_counted_as_a_stub() -> None:
    """`review --interactive` prints the stub message but is not stub demand.

    The counter ranks whole commands a user reached without `--help`
    advertising them. `review` is documented and runs; only its interactive
    mode is unfinished, so a `review` label here would report demand for
    something that already exists — and the deprecated `transactions review`
    alias would split that one mode across two labels on top of it.
    """
    from moneybin.cli.main import app

    def total() -> float:
        return sum(
            sample.value
            for metric in REGISTRY.collect()
            if metric.name == "moneybin_cli_stub_invoked"
            for sample in metric.samples
            if sample.name.endswith("_total")
        )

    before = total()

    result = runner.invoke(app, ["review", "--interactive"])

    assert result.exit_code == 0, result.output
    assert "not yet implemented" in result.output
    # Every label, not just `review`: the alias would land on a second one.
    assert total() == before
