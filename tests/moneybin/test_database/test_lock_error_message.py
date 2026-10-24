"""Wording of the DatabaseLockError raised when an open times out."""

from __future__ import annotations

import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from moneybin.database import (
    _lock_error_message,  # type: ignore[reportPrivateUsage]  # test-only access to the message builder
)
from moneybin.db_lock import write_lock


def _no_blockers(_db_path: Path) -> list[dict[str, object]]:
    return []


@pytest.fixture
def no_lsof(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "moneybin.utils.db_processes.find_blocking_processes", _no_blockers
    )


pytestmark = pytest.mark.usefixtures("no_lsof")


@contextmanager
def _held(db_path: Path) -> Generator[None, None, None]:
    acquired = threading.Event()
    release = threading.Event()

    def holder() -> None:
        with write_lock(
            db_path,
            deadline=time.monotonic() + 5.0,
            operation_type="transform_apply",
        ):
            acquired.set()
            release.wait(timeout=5.0)

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert acquired.wait(timeout=2.0)
        yield
    finally:
        release.set()
        t.join(timeout=5.0)


def test_read_only_timeout_does_not_claim_a_write_lock(tmp_path: Path) -> None:
    message = _lock_error_message(tmp_path / "x.duckdb", 10.0, read_only=True)
    assert message.startswith("Could not open the database for reading after 10s")
    assert "write lock" not in message


def test_write_timeout_names_the_write_lock(tmp_path: Path) -> None:
    message = _lock_error_message(tmp_path / "x.duckdb", 10.0, read_only=False)
    assert message.startswith("Could not acquire write lock after 10s")


def test_message_names_the_holders_operation(tmp_path: Path) -> None:
    db_path = tmp_path / "x.duckdb"
    db_path.touch()
    with _held(db_path):
        message = _lock_error_message(db_path, 10.0, read_only=True)
    assert "held by another MoneyBin process running transform_apply" in message
