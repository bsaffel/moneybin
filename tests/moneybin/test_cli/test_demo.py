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
        doctor_failing=0,
        doctor_failing_names=[],
        net_worth=Decimal("12345.67"),
        total_assets=Decimal("20000.00"),
        total_liabilities=Decimal("7654.33"),
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
def test_demo_json_output(mocker: Any) -> None:
    _patch_service(mocker, _fake_result())
    result = runner.invoke(app, ["demo", "--yes", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["profile"] == "demo"
    assert payload["net_worth"] == "12345.67"
    assert payload["transaction_count"] == 900


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
def test_demo_declining_reset_aborts(mocker: Any) -> None:
    svc = _patch_service(mocker, _fake_result())
    svc.profile_has_data.return_value = True  # existing demo data
    result = runner.invoke(app, ["demo"], input="n\n")  # decline the prompt
    assert result.exit_code != 0
    svc.run.assert_not_called()
