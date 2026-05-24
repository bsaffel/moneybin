"""Tests for the dynamic Typer CLI registrar."""

from __future__ import annotations

import inspect
import json
from unittest.mock import MagicMock, patch

import typer
from typer.testing import CliRunner

from moneybin.cli.output import OutputFormat
from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.cli_register import (
    build_cli_command,
    register_report_cli,
)
from moneybin.reports._framework.contract import ReportQuery
from moneybin.reports._framework.execute import ReportResult
from moneybin.reports._framework.introspect import build_spec
from moneybin.tables import TableRef

_VIEW = TableRef("reports", "test_summary")
_runner_cli = CliRunner()


def _runner(db: Database, *, top: int = 25) -> ReportQuery:
    """Per-account summary.

    Args:
        db: Open read-only database connection.
        top: Maximum rows to return.
    """
    return ReportQuery("SELECT 1", [])


def _spec():  # noqa: ANN202 — test helper
    return build_spec(_runner, name="balance_drift", view=_VIEW)


def _multi_command_app():  # noqa: ANN202 — test helper
    """A Typer app with the report command plus a sibling.

    Typer collapses a single-command app (the subcommand name becomes
    unnecessary); a second command keeps it in multi-command mode so the
    report is invoked as ``<app> balance-drift ...`` like in production.
    """
    app = typer.Typer()
    register_report_cli(_spec(), app)
    app.command("noop")(lambda: None)
    return app


def _result() -> ReportResult:
    return ReportResult(
        records=[{"account_id": "****2222", "txn_count": 2}],
        columns=["account_id", "txn_count"],
        output_classes={
            "account_id": DataClass.ACCOUNT_IDENTIFIER,
            "txn_count": DataClass.AGGREGATE,
        },
        tier=Tier.CRITICAL,
        total_count=1,
        truncated=False,
    )


def test_build_cli_command_signature_has_params_and_output() -> None:
    cmd = build_cli_command(_spec())
    sig = inspect.signature(cmd)
    assert "top" in sig.parameters
    assert "output" in sig.parameters
    assert sig.parameters["output"].annotation is OutputFormat


def test_register_report_cli_adds_named_command() -> None:
    app = typer.Typer()
    register_report_cli(_spec(), app)
    names = {c.name for c in app.registered_commands}
    assert "balance-drift" in names  # cli_name = name with hyphens


def test_cli_command_json_output_emits_envelope() -> None:
    app = _multi_command_app()
    with (
        patch("moneybin.reports._framework.cli_register.get_database", MagicMock()),
        patch(
            "moneybin.reports._framework.cli_register.run_report",
            return_value=_result(),
        ),
    ):
        result = _runner_cli.invoke(
            app, ["balance-drift", "--top", "5", "--output", "json"]
        )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["data"] == [{"account_id": "****2222", "txn_count": 2}]
    assert payload["summary"]["sensitivity"] == "critical"


def test_cli_command_surfaces_value_error_as_bad_parameter() -> None:
    app = _multi_command_app()
    with (
        patch("moneybin.reports._framework.cli_register.get_database", MagicMock()),
        patch(
            "moneybin.reports._framework.cli_register.run_report",
            side_effect=ValueError("Unknown status: bogus"),
        ),
    ):
        result = _runner_cli.invoke(app, ["balance-drift", "--output", "json"])
    assert result.exit_code != 0
    assert "Unknown status: bogus" in result.output
