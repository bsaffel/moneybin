"""Tests for Database.checkpoint() helper."""

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.metrics.registry import DB_CHECKPOINT_TOTAL


@pytest.fixture
def db(tmp_path: Path) -> Generator[Database, None, None]:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "checkpoint-test-key"
    database = Database(
        tmp_path / "checkpoint.duckdb",
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def test_checkpoint_executes_sql_command(db: Database) -> None:
    db.execute("CREATE TABLE t (x INTEGER)")
    db.execute("INSERT INTO t VALUES (1)")
    db.checkpoint("post_migration")
    # If CHECKPOINT didn't run, we'd see the SQL error here. Pass = no error.


def test_checkpoint_increments_metric_counter_with_reason_label(
    db: Database,
) -> None:
    before = DB_CHECKPOINT_TOTAL.labels(reason="post_transform")._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    db.checkpoint("post_transform")
    after = DB_CHECKPOINT_TOTAL.labels(reason="post_transform")._value.get()  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
    assert after == before + 1


def test_checkpoint_logs_at_debug_level(
    db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="moneybin.database")
    db.checkpoint("pre_backup")
    assert any(
        record.levelno == logging.DEBUG
        and "checkpoint" in record.message.lower()
        and "pre_backup" in record.message
        for record in caplog.records
    )
