"""Regression coverage for the final CLI human-experience repair wave."""

from __future__ import annotations

import ast
import json
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.errors import UserError
from moneybin.services.doctor_service import DoctorReport, InvariantResult
from moneybin.services.transaction_service import (
    Transaction,
    TransactionGetResult,
    TransactionService,
)

runner = CliRunner()


@contextmanager
def _mock_database(*_args: object, **_kwargs: object):
    yield MagicMock()


def _ascii_policy() -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=False,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols(success="OK", attention="!", failure="X", action=">"),
        minus="-",
    )


@pytest.mark.unit
def test_root_profile_is_replayed_in_transaction_continuation() -> None:
    """A cursor action must stay in the profile that issued it."""
    transaction = Transaction(
        transaction_id="T1",
        account_id="A1",
        transaction_date="2026-04-10",
        amount=Decimal("-50.00"),
        description="Coffee Shop",
        memo=None,
        source_type="ofx",
        category="Food & Drink",
        subcategory=None,
        notes=None,
        tags=None,
        splits=None,
    )
    result_data = TransactionGetResult(
        transactions=[transaction], next_cursor="next-page", total_count=2
    )

    with patch("moneybin.database.get_database", _mock_database):
        with patch.object(TransactionService, "get", return_value=result_data):
            result = runner.invoke(
                app,
                [
                    "--profile",
                    "research",
                    "transactions",
                    "list",
                    "--limit",
                    "1",
                    "--output",
                    "json",
                ],
            )

    assert result.exit_code == 0, result.output
    continuation = next(
        action
        for action in json.loads(result.stdout)["actions"]
        if "--cursor" in action
    )
    assert "moneybin --profile research transactions list" in continuation


@pytest.mark.unit
def test_root_profile_is_replayed_in_fx_import_and_gsheet_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every reviewed generated-action seam retains the selected root profile."""
    from moneybin.cli import utils
    from moneybin.cli.commands.gsheet import (
        _connections_envelope,  # type: ignore[reportPrivateUsage]  # direct action seam regression
    )
    from moneybin.cli.commands.import_cmd import (
        _sign_recovery_commands,  # type: ignore[reportPrivateUsage]  # direct recovery seam regression
    )
    from moneybin.cli.output import applied_rates_note
    from moneybin.services.currency_service import ResolvedRate

    monkeypatch.setattr(utils._flags, "profile", "research")  # type: ignore[reportPrivateUsage]  # root callback state under test
    note = applied_rates_note(
        (
            ResolvedRate(
                "EUR",
                "USD",
                date(2026, 5, 2),
                date(2026, 5, 2),
                Decimal("1.13"),
                "ecb",
            ),
            ResolvedRate(
                "GBP",
                "USD",
                date(2026, 5, 1),
                date(2026, 5, 1),
                Decimal("1.31"),
                "ecb",
            ),
        ),
        "USD",
    )
    import_actions = _sign_recovery_commands("statement.pdf", channel="pdf")
    row = MagicMock(connection_id="conn_abc123", status="drift_detected")
    with patch("moneybin.cli.commands.gsheet.gsheet_connection_row", return_value=row):
        gsheet_actions = _connections_envelope([MagicMock()]).actions

    assert note is not None
    assert "moneybin --profile research fx rate EUR USD 2026-05-02" in note
    assert all(
        "moneybin --profile research import files statement.pdf" in action
        for action in import_actions
    )
    assert gsheet_actions == [
        "Run 'moneybin --profile research gsheet reconnect conn_abc123' to re-detect this sheet's structure"
    ]


@pytest.mark.unit
def test_pending_parse_error_is_audited_json_error() -> None:
    """A JSON caller receives the usual typed failure and audit record."""
    with patch("moneybin.privacy.log.write_privacy_event") as write_event:
        result = runner.invoke(
            app,
            [
                "transactions",
                "categorize",
                "pending",
                "--min-amount",
                "not-a-decimal",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 2, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "infra_invalid_input"
    assert result.stderr == ""
    assert write_event.call_args.args[0]["actor"] == "cli.categorize_pending"


@pytest.mark.unit
def test_pending_invalid_min_amount_preserves_text_guidance() -> None:
    """The text failure names the invalid flag and its supplied value."""
    result = runner.invoke(
        app,
        [
            "transactions",
            "categorize",
            "pending",
            "--min-amount",
            "nope",
        ],
    )

    assert result.exit_code == 2, result.output
    assert "Invalid --min-amount: nope" in result.output
    assert "ConversionSyntax" not in result.output


@pytest.mark.unit
def test_create_invalid_amount_preserves_text_guidance() -> None:
    """Manual creation keeps its decimal-specific text contract."""
    result = runner.invoke(
        app,
        [
            "transactions",
            "create",
            "--account",
            "A1",
            "--",
            "not-a-decimal",
            "Coffee",
        ],
    )

    assert result.exit_code == 2, result.output
    assert "Invalid --amount 'not-a-decimal': not a decimal" in result.output
    assert "ConversionSyntax" not in result.output


@pytest.mark.unit
def test_create_invalid_date_preserves_text_guidance() -> None:
    """Manual creation keeps the expected date form in its text failure."""
    result = runner.invoke(
        app,
        [
            "transactions",
            "create",
            "--account",
            "A1",
            "--date",
            "2026-13-40",
            "--",
            "-12.50",
            "Coffee",
        ],
    )

    assert result.exit_code == 2, result.output
    assert "Invalid --date '2026-13-40': expected YYYY-MM-DD" in result.output


@pytest.mark.unit
def test_split_invalid_amount_preserves_text_guidance() -> None:
    """Split creation keeps its concise amount-specific text failure."""
    result = runner.invoke(
        app,
        ["transactions", "splits", "add", "T1", "not-a-decimal"],
    )

    assert result.exit_code == 2, result.output
    assert "Invalid amount 'not-a-decimal'" in result.output
    assert "ConversionSyntax" not in result.output


@pytest.mark.unit
def test_logs_invalid_regex_preserves_text_guidance(
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """Log filtering keeps its command-specific regex failure prefix."""
    from moneybin.cli.commands.logs import logs_command_app

    with patch("moneybin.cli.commands.logs.get_settings") as get_settings:
        get_settings.return_value.logging.log_file_path = tmp_path / "moneybin.log"
        result = runner.invoke(logs_command_app, ["cli", "--grep", "[invalid"])

    assert result.exit_code == 2, result.output
    assert "Invalid regex pattern: " in caplog.text


@pytest.mark.unit
def test_logs_invalid_regex_is_audited_json_error(tmp_path: Path) -> None:
    """A known regex parse failure keeps the shared JSON/audit contract."""
    from moneybin.cli.commands.logs import logs_command_app

    with patch("moneybin.cli.commands.logs.get_settings") as get_settings:
        get_settings.return_value.logging.log_file_path = tmp_path / "moneybin.log"
        with patch("moneybin.privacy.log.write_privacy_event") as write_event:
            result = runner.invoke(
                logs_command_app,
                ["cli", "--grep", "[invalid", "--output", "json"],
            )

    assert result.exit_code == 2, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "infra_invalid_input"
    assert result.stderr == ""
    assert isinstance(result.exception, SystemExit)
    assert write_event.call_args.args[0]["actor"] == "cli.logs"


@pytest.mark.unit
def test_doctor_uses_ascii_state_markers() -> None:
    """ASCII output keeps success, failure, warning and skipped states distinct."""
    report = DoctorReport(
        invariants=[
            InvariantResult("healthy", "pass", None, []),
            InvariantResult("broken", "fail", "bad row", []),
            InvariantResult("watch", "warn", "needs review", []),
            InvariantResult("deferred", "skipped", "run transform", []),
        ],
        transaction_count=1,
    )
    with patch("moneybin.cli.commands.system.doctor.get_database", _mock_database):
        with patch(
            "moneybin.cli.commands.system.doctor.DoctorService.run_all",
            return_value=report,
        ):
            with patch(
                "moneybin.cli.commands.system.doctor.get_terminal_policy",
                return_value=_ascii_policy(),
            ):
                result = runner.invoke(app, ["system", "doctor", "--verbose"])

    assert result.exit_code == 1, result.output
    assert "OK healthy" in result.stdout
    assert "X broken" in result.stdout
    assert "! watch" in result.stdout
    assert "! deferred" in result.stdout
    assert not any(marker in result.output for marker in ("✅", "❌", "⚠", "⏭"))


@pytest.mark.unit
def test_user_error_hint_uses_ascii_action_marker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Service-owned recovery hints are adapted only when shown to a human."""
    from unittest.mock import patch

    from moneybin.cli import utils

    with patch.object(utils, "get_terminal_policy", return_value=_ascii_policy()):
        with pytest.raises(typer.Exit):
            with utils.handle_cli_errors():
                raise UserError(
                    "Unable to continue",
                    code="infra_invalid_input",
                    hint="💡 Run 'moneybin transactions list'",
                )

    captured = capsys.readouterr()
    assert "> Run 'moneybin transactions list'" in captured.err
    assert "💡" not in captured.err


@pytest.mark.unit
def test_hint_adapter_preserves_emoji_in_literal_user_data() -> None:
    """Only a fixed hint prefix is a state marker; labels and arguments are data."""
    from moneybin.cli.utils import format_cli_hint

    hint = "Review label '💡 quarterly' in '💡 statement.csv' before retrying."

    assert format_cli_hint(hint, policy=_ascii_policy()) == hint


@pytest.mark.unit
def test_profiled_generated_command_quotes_literal_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Selected-profile actions remain runnable when their path needs shell quoting."""
    from moneybin.cli import utils

    monkeypatch.setattr(utils._flags, "profile", "research")  # type: ignore[reportPrivateUsage]  # root callback state under test

    assert utils.generated_cli_command("import", "files", "Owner's statement.csv") == (
        "moneybin --profile research import files 'Owner'\"'\"'s statement.csv'"
    )


@pytest.mark.unit
def test_cli_authored_state_markers_use_terminal_policy() -> None:
    """Direct terminal output uses policy markers while data values stay untouched."""
    source_root = Path(__file__).parents[3] / "src" / "moneybin" / "cli"
    legacy_markers = ("✅", "❌", "⚠", "⏭", "👀", "❓", "💡", "⚙")
    violations: list[str] = []

    for source_path in source_root.rglob("*.py"):
        if source_path.name == "utils.py":
            continue
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(
                node.func, ast.Attribute
            ):
                continue
            receiver = node.func.value
            is_terminal_call = (
                isinstance(receiver, ast.Name)
                and receiver.id == "typer"
                and node.func.attr == "echo"
            ) or (isinstance(receiver, ast.Name) and receiver.id == "logger")
            if not is_terminal_call:
                continue
            for argument in ast.walk(node):
                if (
                    isinstance(argument, ast.Constant)
                    and isinstance(argument.value, str)
                    and any(marker in argument.value for marker in legacy_markers)
                ):
                    violations.append(
                        f"{source_path.relative_to(source_root)}:{argument.lineno}"
                    )

    assert not violations, (
        "Use terminal-policy helpers for authored state markers: "
        + ", ".join(violations)
    )


@pytest.mark.unit
def test_normal_cli_warning_logs_use_a_portable_attention_marker() -> None:
    """Service warnings keep logger ownership without pictographic state tokens."""
    source_root = Path(__file__).parents[3] / "src" / "moneybin"
    service_paths = (
        source_root / "services" / "account_links_service.py",
        source_root / "services" / "matching_service.py",
        source_root / "services" / "undo_service.py",
    )
    legacy_markers = ("✅", "❌", "⚠", "⏭", "👀", "❓", "💡", "⚙")
    violations: list[str] = []

    for source_path in service_paths:
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(
                node.func, ast.Attribute
            ):
                continue
            if not (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id == "logger"
                and node.func.attr == "warning"
            ):
                continue
            if any(
                isinstance(argument, ast.Constant)
                and isinstance(argument.value, str)
                and any(marker in argument.value for marker in legacy_markers)
                for argument in ast.walk(node)
            ):
                violations.append(
                    f"{source_path.relative_to(source_root)}:{node.lineno}"
                )

    assert not violations, (
        "Use portable attention markers in normal CLI warnings: "
        + ", ".join(violations)
    )
