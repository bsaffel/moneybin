"""CLI-layer tests for `moneybin demo` (arg parsing, exit codes, output).

Business logic (DemoService) is mocked; the real orchestration is tested in
test_demo_service.py and the e2e suite.
"""

import dataclasses
import json
from decimal import Decimal
from typing import Any

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.demo_service import DemoResult

runner = CliRunner()


def _fake_result(**overrides: Any) -> DemoResult:
    base = DemoResult(
        profile="demo",
        persona="basic",
        seed=42,
        account_count=2,
        transaction_count=900,
        categorized_count=850,
        doctor_failing=0,
        doctor_failing_names=[],
        net_worth=Decimal("12345.67"),
        total_assets=Decimal("20000.00"),
        total_liabilities=Decimal("7654.33"),
        previous_default="personal",
    )
    return dataclasses.replace(base, **overrides)


def _patch_service(mocker: Any, result: DemoResult) -> Any:
    svc = mocker.patch("moneybin.services.demo_service.DemoService").return_value
    svc.profile_has_data.return_value = False
    svc.run.return_value = result
    return svc


@pytest.mark.unit
def test_demo_runs_and_prints_networth(mocker: Any) -> None:
    svc = _patch_service(mocker, _fake_result())
    result = runner.invoke(app, ["demo", "--yes"])
    assert result.exit_code == 0, result.output
    assert "12345.67" in result.output
    svc.run.assert_called_once()


@pytest.mark.unit
def test_demo_json_uses_standard_envelope(mocker: Any) -> None:
    _patch_service(mocker, _fake_result())
    result = runner.invoke(app, ["demo", "--yes", "--output", "json"])
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    # Standard CLI/MCP envelope shape, not a hand-rolled dict.
    assert "data" in envelope
    assert "summary" in envelope
    assert envelope["data"]["profile"] == "demo"
    assert envelope["data"]["net_worth"] == "12345.67"
    assert envelope["data"]["transaction_count"] == 900


@pytest.mark.unit
def test_demo_dirty_doctor_exits_nonzero(mocker: Any) -> None:
    _patch_service(
        mocker,
        _fake_result(doctor_failing=2, doctor_failing_names=["dedup", "orphans"]),
    )
    result = runner.invoke(app, ["demo", "--yes"])
    assert result.exit_code == 1


@pytest.mark.unit
def test_demo_rejects_unknown_persona(mocker: Any) -> None:
    _patch_service(mocker, _fake_result())
    result = runner.invoke(app, ["demo", "--yes", "--persona", "tycoon"])
    assert result.exit_code == 2  # usage error


@pytest.mark.unit
def test_demo_has_no_profile_flag(mocker: Any) -> None:
    # demo always targets the dedicated `demo` profile — it must not be
    # pointable at an arbitrary (possibly real) profile.
    _patch_service(mocker, _fake_result())
    result = runner.invoke(app, ["demo", "--yes", "--profile", "my-real-money"])
    assert result.exit_code == 2  # unknown option


@pytest.mark.unit
def test_demo_declining_rebuild_aborts(mocker: Any) -> None:
    svc = _patch_service(mocker, _fake_result())
    svc.profile_has_data.return_value = True  # existing demo data
    result = runner.invoke(app, ["demo"], input="n\n")  # decline the prompt
    assert result.exit_code != 0
    svc.run.assert_not_called()


@pytest.mark.unit
def test_demo_first_run_needs_no_yes_and_no_prompt(mocker: Any) -> None:
    # The most common real path: a first `moneybin demo` with no --yes and nothing
    # to overwrite. There is nothing to lose, so no prompt fires and the run goes
    # straight through with reset_confirmed=False.
    svc = _patch_service(mocker, _fake_result())  # profile_has_data() -> False
    result = runner.invoke(app, ["demo"])
    assert result.exit_code == 0, result.output
    assert svc.run.call_args.kwargs["reset_confirmed"] is False


@pytest.mark.unit
def test_demo_forwards_non_default_args_to_the_service(mocker: Any) -> None:
    svc = _patch_service(mocker, _fake_result())
    result = runner.invoke(
        app,
        ["demo", "--yes", "--persona", "family", "--seed", "7", "--years", "2"],
    )
    assert result.exit_code == 0, result.output
    kwargs = svc.run.call_args.kwargs
    assert kwargs["persona"] == "family"
    assert kwargs["seed"] == 7
    assert kwargs["years"] == 2


@pytest.mark.unit
def test_demo_rejects_a_nonpositive_years(mocker: Any) -> None:
    # `--years -1` used to reach the generator, which computed start_year after
    # end_year and produced zero transactions while still reporting a ready profile.
    svc = _patch_service(mocker, _fake_result())
    result = runner.invoke(app, ["demo", "--yes", "--years", "-1"])
    assert result.exit_code == 2  # usage error
    svc.run.assert_not_called()


@pytest.mark.unit
def test_demo_quiet_suppresses_status_but_not_the_answer(mocker: Any) -> None:
    _patch_service(mocker, _fake_result())
    result = runner.invoke(app, ["demo", "--yes", "--quiet"])
    assert result.exit_code == 0, result.output
    # The answer is the data — never suppressed.
    assert "12345.67" in result.stdout
    # Status chatter is.
    assert "Demo profile" not in result.output
    assert "Try next" not in result.output


@pytest.mark.unit
def test_demo_announces_the_default_profile_switch(mocker: Any) -> None:
    # Demo repoints every later command at itself; that must not happen silently.
    _patch_service(mocker, _fake_result(previous_default="personal"))
    result = runner.invoke(app, ["demo", "--yes"])
    assert result.exit_code == 0, result.output
    assert "Default profile is now 'demo'" in result.output
    assert "moneybin profile switch personal" in result.output
