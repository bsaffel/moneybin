"""Tests for the dynamic Typer CLI registrar."""

from __future__ import annotations

import inspect
import json
from unittest.mock import MagicMock, patch

import typer
from typer.testing import CliRunner

from moneybin import error_codes
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
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

_VIEW = TableRef("reports", "test_summary")
_CLASSES = {"account_id": DataClass.ACCOUNT_IDENTIFIER}
_runner_cli = CliRunner()


def _runner(db: Database, *, top: int = 25) -> ReportQuery:
    """Per-account summary.

    Args:
        db: Open read-only database connection.
        top: Maximum rows to return.
    """
    return ReportQuery("SELECT 1", [])


def _spec():  # noqa: ANN202 — test helper
    return build_spec(
        _runner,
        report_id="test:balance_drift",
        name="balance_drift",
        view=_VIEW,
        classes=_CLASSES,
        parameter_classes={"top": DataClass.AGGREGATE},
        columns=output_columns(_CLASSES),
        semantics=TEST_SEMANTICS,
    )


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


def _windowed_runner(
    db: Database, *, from_month: str | None = None, to_month: str | None = None
) -> ReportQuery:
    """Windowed summary.

    Args:
        db: Open read-only database connection.
        from_month: Inclusive start month (YYYY-MM).
        to_month: Inclusive end month (YYYY-MM).
    """
    return ReportQuery("SELECT 1", [])


def _windowed_app():  # noqa: ANN202 — test helper
    app = typer.Typer()
    spec = build_spec(
        _windowed_runner,
        report_id="test:windowed",
        name="windowed",
        view=_VIEW,
        classes=_CLASSES,
        parameter_classes={
            "from_month": DataClass.TXN_DATE,
            "to_month": DataClass.TXN_DATE,
        },
        columns=output_columns(_CLASSES),
        semantics=TEST_SEMANTICS,
    )
    register_report_cli(spec, app)
    app.command("noop")(lambda: None)
    return app


def test_cli_command_accepts_hyphenated_window_flags() -> None:
    # The underscore→hyphen flag derivation (from_month → --from-month) is the
    # most prominent breaking change in the report-framework migration. Assert
    # the derived flags parse and forward end-to-end through the injected
    # __signature__ — not just that the Python param name exists.
    app = _windowed_app()
    captured: dict[str, object] = {}

    def _fake_run_report(
        spec: object, db: object, *, max_rows: int, **kwargs: object
    ) -> ReportResult:
        captured.update(kwargs)
        return _result()

    with (
        patch("moneybin.reports._framework.cli_register.get_database", MagicMock()),
        patch(
            "moneybin.reports._framework.execute.run_report",
            side_effect=_fake_run_report,
        ),
    ):
        result = _runner_cli.invoke(
            app,
            [
                "windowed",
                "--from-month",
                "2024-01",
                "--to-month",
                "2024-06",
                "--output",
                "json",
            ],
        )
    assert result.exit_code == 0, result.output
    assert captured == {"from_month": "2024-01", "to_month": "2024-06"}


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
            "moneybin.reports._framework.execute.run_report",
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


def test_cli_command_passes_classes_returned_to_audit() -> None:
    # Bare-list payload + lineage-derived classes: classes_returned must reach
    # render_or_json so the privacy.log audit event records the real data
    # classes instead of an empty set (the `sql query` contract).
    app = _multi_command_app()
    with (
        patch("moneybin.reports._framework.cli_register.get_database", MagicMock()),
        patch(
            "moneybin.reports._framework.execute.run_report",
            return_value=_result(),
        ),
        patch("moneybin.reports._framework.cli_register.render_or_json") as mock_render,
    ):
        result = _runner_cli.invoke(
            app, ["balance-drift", "--top", "5", "--output", "json"]
        )
    assert result.exit_code == 0, result.output
    assert mock_render.call_args.kwargs["classes_returned"] == [
        "account_identifier",
        "aggregate",
    ]


def test_cli_command_value_error_emits_json_error_envelope() -> None:
    # A runner ValueError (bad enum value) under --output json must flow through
    # the shared classified-error path (handle_cli_errors → INFRA_INVALID_INPUT)
    # and emit a JSON error envelope — NOT a plain-text typer.BadParameter that
    # bypasses the envelope and exits 2, breaking the JSON contract for agents.
    app = _multi_command_app()
    with (
        patch("moneybin.reports._framework.cli_register.get_database", MagicMock()),
        patch(
            "moneybin.reports._framework.execute.run_report",
            side_effect=ValueError("Unknown status: bogus"),
        ),
    ):
        result = _runner_cli.invoke(app, ["balance-drift", "--output", "json"])
    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"]["code"] == error_codes.INFRA_INVALID_INPUT
    assert "Unknown status: bogus" in payload["error"]["message"]
