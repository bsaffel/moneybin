"""Shared helpers for CLI commands."""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager

import typer

from moneybin.database import Database, get_database
from moneybin.errors import classify_user_error

logger = logging.getLogger(__name__)


@contextmanager
def handle_cli_errors() -> Generator[Database, None, None]:
    """Get the active database with cross-cutting CLI error handling.

    Yields a ``Database`` and catches user-facing exceptions raised either
    during database open or inside the ``with`` block (e.g.,
    ``DatabaseKeyError``, ``FileNotFoundError``). Classified errors are
    logged with the standard ``❌`` prefix plus any recovery hint, and the
    process exits with code 1. Unrecognized exceptions propagate unchanged.
    """
    try:
        db = get_database()
        yield db
    except Exception as e:
        user_error = classify_user_error(e)
        if user_error is None:
            raise
        logger.error(f"❌ {user_error.message}")
        if user_error.hint:
            logger.info(user_error.hint)
        raise typer.Exit(1) from e
