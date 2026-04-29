"""Tests for the shared CLI error handler."""

from unittest.mock import MagicMock, patch

import pytest
import typer
from _pytest.logging import LogCaptureFixture

from moneybin.database import DatabaseKeyError


def test_handle_cli_errors_yields_database() -> None:
    """When get_database succeeds, the context manager yields the Database."""
    from moneybin.cli.utils import handle_cli_errors

    fake_db = MagicMock()
    with patch("moneybin.cli.utils.get_database", return_value=fake_db):
        with handle_cli_errors() as db:
            assert db is fake_db


def test_handle_cli_errors_translates_key_error_to_exit(
    caplog: LogCaptureFixture,
) -> None:
    """DatabaseKeyError is caught, logged, and converted to typer.Exit(1)."""
    from moneybin.cli.utils import handle_cli_errors

    with patch(
        "moneybin.cli.utils.get_database",
        side_effect=DatabaseKeyError("locked"),
    ):
        with caplog.at_level("ERROR"), pytest.raises(typer.Exit) as exc_info:
            with handle_cli_errors():
                pass
    assert exc_info.value.exit_code == 1
    assert "locked" in caplog.text


def test_handle_cli_errors_translates_file_not_found_in_block(
    caplog: LogCaptureFixture,
) -> None:
    """FileNotFoundError raised inside the block is classified and exits 1."""
    from moneybin.cli.utils import handle_cli_errors

    fake_db = MagicMock()
    with patch("moneybin.cli.utils.get_database", return_value=fake_db):
        with caplog.at_level("ERROR"), pytest.raises(typer.Exit) as exc_info:
            with handle_cli_errors():
                raise FileNotFoundError("missing.csv")
    assert exc_info.value.exit_code == 1
    assert "missing.csv" in caplog.text


def test_handle_cli_errors_lets_other_exceptions_propagate() -> None:
    """Non-classified exceptions raised inside the block pass through."""
    from moneybin.cli.utils import handle_cli_errors

    fake_db = MagicMock()
    with patch("moneybin.cli.utils.get_database", return_value=fake_db):
        with pytest.raises(RuntimeError, match="boom"):
            with handle_cli_errors():
                raise RuntimeError("boom")
