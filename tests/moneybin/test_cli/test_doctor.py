"""Unit tests for the `doctor` CLI command."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.doctor_service import DoctorReport, InvariantResult

runner = CliRunner()

_PASSING_REPORT = DoctorReport(
    invariants=[
        InvariantResult("fct_transactions_fk_integrity", "pass", None, []),
        InvariantResult("fct_transactions_sign_convention", "pass", None, []),
        InvariantResult("bridge_transfers_balanced", "pass", None, []),
        InvariantResult("categorization_coverage", "pass", None, []),
        InvariantResult("staging_coverage", "skipped", "no is_primary column", []),
    ],
    transaction_count=100,
)

_FAILING_REPORT = DoctorReport(
    invariants=[
        InvariantResult("fct_transactions_fk_integrity", "pass", None, []),
        InvariantResult("fct_transactions_sign_convention", "fail", "1 violation(s)", []),
        InvariantResult("bridge_transfers_balanced", "pass", None, []),
        InvariantResult("categorization_coverage", "warn", "80% uncategorized", []),
        InvariantResult("staging_coverage", "skipped", "no is_primary column", []),
    ],
    transaction_count=50,
)


@pytest.mark.unit
def test_doctor_help_exits_cleanly() -> None:
    result = runner.invoke(app, ["doctor", "--help"])
    assert result.exit_code == 0
    assert "--verbose" in result.output
    assert "--output" in result.output


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.cli.commands.doctor.DoctorService")
def test_doctor_exits_0_when_all_pass(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _PASSING_REPORT
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    assert "✅" in result.output


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.cli.commands.doctor.DoctorService")
def test_doctor_exits_1_when_any_fail(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _FAILING_REPORT
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 1


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.cli.commands.doctor.DoctorService")
def test_doctor_json_output_shape(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _PASSING_REPORT
    result = runner.invoke(app, ["doctor", "--output", "json"])
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    assert "summary" in envelope
    assert "data" in envelope
    data = envelope["data"]
    assert "passing" in data
    assert "failing" in data
    assert "warning" in data
    assert "transaction_count" in data
    assert "invariants" in data
    assert len(data["invariants"]) == 5


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.cli.commands.doctor.DoctorService")
def test_doctor_verbose_passes_flag_to_service(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _PASSING_REPORT
    runner.invoke(app, ["doctor", "--verbose"])
    mock_svc_cls.return_value.run_all.assert_called_once_with(verbose=True)


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.cli.commands.doctor.DoctorService")
def test_doctor_warn_only_exits_0(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    """warn-only status → exit 0 (not a hard failure)."""
    warn_report = DoctorReport(
        invariants=[
            InvariantResult("categorization_coverage", "warn", "80% uncategorized", []),
        ],
        transaction_count=10,
    )
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = warn_report
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.cli.commands.doctor.DoctorService")
def test_doctor_json_verbose_includes_affected_ids(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    verbose_report = DoctorReport(
        invariants=[
            InvariantResult(
                "fct_transactions_fk_integrity", "fail",
                "1 violation(s)", ["abc123"]
            ),
        ],
        transaction_count=5,
    )
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = verbose_report
    result = runner.invoke(app, ["doctor", "--output", "json", "--verbose"])
    assert result.exit_code == 1
    envelope = json.loads(result.stdout)
    inv = envelope["data"]["invariants"][0]
    assert inv["affected_ids"] == ["abc123"]
