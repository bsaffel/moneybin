"""Tests for the shared CLI error handler."""

import pytest
import typer
from _pytest.logging import LogCaptureFixture

from moneybin import error_codes
from moneybin.database import DatabaseKeyError


def test_handle_cli_errors_yields_none() -> None:
    """handle_cli_errors() is a pure error handler — it yields None, not a Database."""
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors() as value:
        assert value is None


def test_handle_cli_errors_translates_key_error_to_exit(
    caplog: LogCaptureFixture,
) -> None:
    """DatabaseKeyError raised inside the block is caught, logged, and converted to typer.Exit(1)."""
    from moneybin.cli.utils import handle_cli_errors

    with caplog.at_level("ERROR"), pytest.raises(typer.Exit) as exc_info:
        with handle_cli_errors():
            raise DatabaseKeyError("locked")
    assert exc_info.value.exit_code == 1
    assert "locked" in caplog.text


def test_handle_cli_errors_hint_reaches_console_not_log(
    capsys: pytest.CaptureFixture[str],
    caplog: LogCaptureFixture,
) -> None:
    """A UserError's hint is rendered to the user but never written to the log.

    `sql_query`'s hint can carry the head of a DuckDB binder/catalog message,
    which — unlike every other hint in the codebase — may include text the
    caller typed into the query (see
    tests/privacy/test_sql_query.py::test_binder_error_head_without_a_line_marker_can_carry_caller_text).
    The file log handler is unfiltered (`_ConsoleNoiseFilter` only guards the
    console handler, per its own docstring), so a hint routed through
    `logger.info` would persist to the durable `cli_YYYY-MM-DD.log`.

    `DatabaseLockError`'s hint is a static, safe string — deliberately chosen
    over `sql_query`'s dynamic one so this test isolates the DELIVERY
    MECHANISM (does a hint reach the console without touching the log?) from
    the content-masking question `test_sql_query.py` already covers.
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import DatabaseLockError

    hint_text = "Run 'moneybin db ps' for details or wait and retry"
    with caplog.at_level("INFO"), pytest.raises(typer.Exit):
        with handle_cli_errors():
            raise DatabaseLockError("busy")

    assert hint_text not in caplog.text
    assert hint_text in capsys.readouterr().err


def test_handle_cli_errors_translates_file_not_found_in_block(
    caplog: LogCaptureFixture,
) -> None:
    """FileNotFoundError raised inside the block is classified and exits 1."""
    from moneybin.cli.utils import handle_cli_errors

    with caplog.at_level("ERROR"), pytest.raises(typer.Exit) as exc_info:
        with handle_cli_errors():
            raise FileNotFoundError("missing.csv")
    assert exc_info.value.exit_code == 1
    assert "missing.csv" in caplog.text


def test_handle_cli_errors_lets_other_exceptions_propagate() -> None:
    """Non-classified exceptions raised inside the block pass through."""
    from moneybin.cli.utils import handle_cli_errors

    with pytest.raises(RuntimeError, match="boom"):
        with handle_cli_errors():
            raise RuntimeError("boom")


def test_handle_cli_errors_json_mode_emits_envelope_on_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With JSON output mode active, classified errors emit a JSON envelope to stdout."""
    import json

    from moneybin.cli.output import OutputFormat
    from moneybin.cli.utils import handle_cli_errors, set_output_flag

    set_output_flag(OutputFormat.JSON)
    with pytest.raises(typer.Exit) as exc_info:
        with handle_cli_errors():
            raise FileNotFoundError("missing.csv")

    assert exc_info.value.exit_code == 1
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "error"
    assert out["error"]["code"] == error_codes.INFRA_FILE_NOT_FOUND
    assert "missing.csv" in out["error"]["message"]


def test_handle_cli_errors_json_mode_no_log_output(
    capsys: pytest.CaptureFixture[str],
    caplog: LogCaptureFixture,
) -> None:
    """With JSON output mode active, error goes to stdout envelope — not to stderr log."""
    from moneybin.cli.output import OutputFormat
    from moneybin.cli.utils import handle_cli_errors, set_output_flag

    set_output_flag(OutputFormat.JSON)
    with caplog.at_level("ERROR"), pytest.raises(typer.Exit):
        with handle_cli_errors():
            raise FileNotFoundError("gone.csv")

    assert "gone.csv" not in caplog.text
    captured = capsys.readouterr()
    assert captured.out.strip()


def test_handle_cli_errors_text_mode_unchanged(
    caplog: LogCaptureFixture,
) -> None:
    """Text mode (default) still logs and does not emit JSON."""
    from moneybin.cli.output import OutputFormat
    from moneybin.cli.utils import handle_cli_errors, set_output_flag

    set_output_flag(OutputFormat.TEXT)
    with caplog.at_level("ERROR"), pytest.raises(typer.Exit):
        with handle_cli_errors():
            raise FileNotFoundError("also.csv")

    assert "also.csv" in caplog.text


def test_error_audit_classification_defaults_high_without_payload_type() -> None:
    """No payload type → conservative HIGH default (never under-report a failure)."""
    from moneybin.cli.utils import (
        _error_audit_classification,  # pyright: ignore[reportPrivateUsage]
    )

    sensitivity, classes = _error_audit_classification(None)
    assert sensitivity == "high"
    assert classes == []


def test_error_audit_classification_derives_critical_from_payload() -> None:
    """A CRITICAL payload type derives 'critical' + its data classes."""
    from moneybin.cli.utils import (
        _error_audit_classification,  # pyright: ignore[reportPrivateUsage]
    )
    from moneybin.privacy.payloads.accounts import AccountDetail

    sensitivity, classes = _error_audit_classification(AccountDetail)
    assert sensitivity == "critical"
    # AccountDetail is CRITICAL via routing_number (account_id is RECORD_ID per D6).
    assert "routing_number" in classes
