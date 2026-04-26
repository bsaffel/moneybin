"""Tests for the shared CLI database-error handler."""

from unittest.mock import MagicMock, patch

import pytest
import typer

from moneybin.database import DatabaseKeyError


def test_handle_database_errors_yields_database() -> None:
    """When get_database succeeds, the context manager yields the Database."""
    from moneybin.cli.utils import handle_database_errors

    fake_db = MagicMock()
    with patch("moneybin.cli.utils.get_database", return_value=fake_db):
        with handle_database_errors() as db:
            assert db is fake_db


def test_handle_database_errors_translates_key_error_to_exit(caplog) -> None:
    """DatabaseKeyError is caught, logged, and converted to typer.Exit(1)."""
    from moneybin.cli.utils import handle_database_errors

    with patch(
        "moneybin.cli.utils.get_database",
        side_effect=DatabaseKeyError("locked"),
    ):
        with caplog.at_level("ERROR"), pytest.raises(typer.Exit) as exc_info:
            with handle_database_errors():
                pass
    assert exc_info.value.exit_code == 1
    assert "locked" in caplog.text


def test_handle_database_errors_lets_other_exceptions_propagate() -> None:
    """Non-DatabaseKeyError exceptions raised inside the block pass through."""
    from moneybin.cli.utils import handle_database_errors

    fake_db = MagicMock()
    with patch("moneybin.cli.utils.get_database", return_value=fake_db):
        with pytest.raises(RuntimeError, match="boom"):
            with handle_database_errors():
                raise RuntimeError("boom")
