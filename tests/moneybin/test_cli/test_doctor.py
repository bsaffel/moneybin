"""Unit tests for the `doctor` CLI command."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.errors import RecoveryAction
from moneybin.services.doctor_service import DoctorReport, InvariantResult

runner = CliRunner()

_PASSING_REPORT = DoctorReport(
    invariants=[
        InvariantResult("fct_transactions_fk_integrity", "pass", None, []),
        InvariantResult("fct_transactions_sign_convention", "pass", None, []),
        InvariantResult("bridge_transfers_balanced", "pass", None, []),
        InvariantResult("categorization_coverage", "pass", None, []),
        InvariantResult("dedup_reconciliation", "pass", None, []),
    ],
    transaction_count=100,
)

_FAILING_REPORT = DoctorReport(
    invariants=[
        InvariantResult("fct_transactions_fk_integrity", "pass", None, []),
        InvariantResult(
            "fct_transactions_sign_convention", "fail", "1 violation(s)", []
        ),
        InvariantResult("bridge_transfers_balanced", "pass", None, []),
        InvariantResult("categorization_coverage", "warn", "80% uncategorized", []),
        InvariantResult("dedup_reconciliation", "pass", None, []),
    ],
    transaction_count=50,
)


@pytest.mark.unit
def test_doctor_help_exits_cleanly() -> None:
    result = runner.invoke(app, ["system", "doctor", "--help"])
    assert result.exit_code == 0
    assert "--verbose" in result.output
    assert "--output" in result.output


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_exits_0_when_all_pass(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _PASSING_REPORT
    result = runner.invoke(app, ["system", "doctor"])
    assert result.exit_code == 0
    assert "✅" in result.output


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_exits_1_when_any_fail(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _FAILING_REPORT
    result = runner.invoke(app, ["system", "doctor"])
    assert result.exit_code == 1


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_json_output_shape(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _PASSING_REPORT
    result = runner.invoke(app, ["system", "doctor", "--output", "json"])
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


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_verbose_passes_flag_to_service(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _PASSING_REPORT
    runner.invoke(app, ["system", "doctor", "--verbose"])
    mock_svc_cls.return_value.run_all.assert_called_once_with(verbose=True, full=False)


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_full_passes_flag_to_service(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _PASSING_REPORT
    runner.invoke(app, ["system", "doctor", "--full"])
    mock_svc_cls.return_value.run_all.assert_called_once_with(verbose=False, full=True)


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
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
    result = runner.invoke(app, ["system", "doctor"])
    assert result.exit_code == 0


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_renders_skipped_invariant(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    """A skipped invariant renders the skip icon and is counted in the summary."""
    skipped_report = DoctorReport(
        invariants=[
            InvariantResult("fct_transactions_fk_integrity", "pass", None, []),
            InvariantResult(
                "dedup_reconciliation",
                "skipped",
                "prep/core layer not available; run transform first",
                [],
            ),
        ],
        transaction_count=0,
    )
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = skipped_report
    result = runner.invoke(app, ["system", "doctor"])
    assert result.exit_code == 0  # skipped is not a failure
    assert "⏭️" in result.output
    assert "dedup_reconciliation" in result.output
    assert "skipped" in result.output


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_json_verbose_includes_affected_ids(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    verbose_report = DoctorReport(
        invariants=[
            InvariantResult(
                "fct_transactions_fk_integrity", "fail", "1 violation(s)", ["abc123"]
            ),
        ],
        transaction_count=5,
    )
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = verbose_report
    result = runner.invoke(app, ["system", "doctor", "--output", "json", "--verbose"])
    assert result.exit_code == 1
    envelope = json.loads(result.stdout)
    inv = envelope["data"]["invariants"][0]
    assert inv["affected_ids"] == ["abc123"]


# Fixture mirrors the real orphan_app_state recipe contract:
# notes_delete is "suggested" (non-idempotent across a batch — see recipe
# docstring), tags_set is "certain" (clear-to-empty is idempotent). Holding
# the fixture to the real recipe values makes this test catch future
# confidence-value regressions in the recipe itself, not just rendering.
_RECOVERY_REPORT = DoctorReport(
    invariants=[
        InvariantResult(
            "orphan_app_state",
            "fail",
            "2 orphan rows",
            ["note:n1", "tag:t2"],
            recovery_actions=[
                RecoveryAction(
                    tool="transactions_notes_delete",
                    arguments={"note_id": "n1"},
                    rationale="Delete orphan note n1.",
                    confidence="suggested",
                    idempotent=False,
                ),
                RecoveryAction(
                    tool="transactions_tags_set",
                    arguments={"transaction_id": "t2", "tags": []},
                    rationale="Clear orphan tags on t2.",
                    confidence="certain",
                    idempotent=True,
                ),
            ],
        ),
    ],
    transaction_count=10,
)


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_text_renders_recovery_action_hints(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    """Failing invariants with recipes render each action's tool + confidence."""
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _RECOVERY_REPORT
    result = runner.invoke(app, ["system", "doctor"])
    assert result.exit_code == 1
    # Tool names appear so an agent reading the text output sees the next steps.
    assert "transactions_notes_delete" in result.output
    assert "transactions_tags_set" in result.output
    # Confidence tag accompanies each action for fast scanning.
    assert "certain" in result.output


@patch("moneybin.cli.commands.system.doctor.get_database")
@patch("moneybin.cli.commands.system.doctor.DoctorService")
def test_doctor_json_includes_recovery_actions_per_invariant(
    mock_svc_cls: MagicMock, mock_get_db: MagicMock
) -> None:
    mock_get_db.return_value = MagicMock()
    mock_svc_cls.return_value.run_all.return_value = _RECOVERY_REPORT
    result = runner.invoke(app, ["system", "doctor", "--output", "json"])
    assert result.exit_code == 1
    envelope = json.loads(result.stdout)
    inv = envelope["data"]["invariants"][0]
    assert inv["name"] == "orphan_app_state"
    tools = sorted(a["tool"] for a in inv["recovery_actions"])
    assert tools == ["transactions_notes_delete", "transactions_tags_set"]
    # Each action ships its full executable shape — arguments are not stringified.
    notes_action = next(
        a for a in inv["recovery_actions"] if a["tool"] == "transactions_notes_delete"
    )
    assert notes_action["arguments"] == {"note_id": "n1"}
    assert notes_action["confidence"] == "suggested"
    assert notes_action["idempotent"] is False
