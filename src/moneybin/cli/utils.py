"""Shared helpers for CLI commands."""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager

import typer

from moneybin.database import (
    Database,
    DatabaseKeyError,
    database_key_error_hint,
    get_database,
)

logger = logging.getLogger(__name__)


@contextmanager
def handle_database_errors() -> Generator[Database, None, None]:
    """Get the active database with standard CLI error handling.

    Catches ``DatabaseKeyError`` (raised when the encryption key is not
    available — e.g. database is locked), logs a user-facing error and the
    standard ``moneybin db unlock`` hint, and exits with code 1. All other
    exceptions propagate unchanged so callers can handle them.
    """
    try:
        db = get_database()
    except DatabaseKeyError as e:
        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e

    yield db
