"""Tests for system_status's database_connections section."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from moneybin.db_lock.lock import (
    _LOCK_SUFFIX,  # type: ignore[reportPrivateUsage]  # test-only access to the canonical lock-file suffix
)
from moneybin.mcp.tools.system import (
    _database_connections_block,  # type: ignore[reportPrivateUsage]  # test-only access to the private helper
)


def test_block_reports_writer_from_lock_file(tmp_path: Path) -> None:
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    lock_path = tmp_path / ("status.duckdb" + _LOCK_SUFFIX)
    lock_path.write_text(
        json.dumps({
            "pid": 12345,
            "command": "moneybin transform apply",
            "started_at": "2026-06-04T15:22:14+00:00",
            "operation_type": "transform_apply",
        })
    )
    with patch(
        "moneybin.mcp.tools.system.find_blocking_processes",
        return_value=[],
    ):
        block = _database_connections_block(db_path)
    assert len(block["writers"]) == 1
    assert block["writers"][0]["pid"] == 12345
    assert block["writers"][0]["operation_type"] == "transform_apply"
    assert block["readers"] == []


def test_block_reports_readers_from_lsof(tmp_path: Path) -> None:
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    # No lock file -> no writers; lsof returns one reader.
    with patch(
        "moneybin.mcp.tools.system.find_blocking_processes",
        return_value=[
            {"pid": 9999, "command": "python", "cmdline": "moneybin reports spending"}
        ],
    ):
        block = _database_connections_block(db_path)
    assert block["writers"] == []
    assert len(block["readers"]) == 1
    assert block["readers"][0]["pid"] == 9999


def test_block_excludes_writer_pid_from_readers_to_avoid_double_listing(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    lock_path = tmp_path / ("status.duckdb" + _LOCK_SUFFIX)
    lock_path.write_text(
        json.dumps({
            "pid": 5555,
            "command": "moneybin sync pull",
            "started_at": "2026-06-04T15:22:14+00:00",
            "operation_type": "interactive",
        })
    )
    # lsof returns the writer pid AND a separate reader pid.
    with patch(
        "moneybin.mcp.tools.system.find_blocking_processes",
        return_value=[
            {"pid": 5555, "command": "moneybin", "cmdline": "moneybin sync pull"},
            {"pid": 6666, "command": "moneybin", "cmdline": "moneybin reports"},
        ],
    ):
        block = _database_connections_block(db_path)
    reader_pids = [r["pid"] for r in block["readers"]]
    assert 5555 not in reader_pids
    assert 6666 in reader_pids


def test_block_returns_empty_when_no_lock_file_and_no_lsof_output(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    with patch(
        "moneybin.mcp.tools.system.find_blocking_processes",
        return_value=[],
    ):
        block = _database_connections_block(db_path)
    assert block == {"writers": [], "readers": []}


def test_block_tolerates_corrupted_lock_file(tmp_path: Path) -> None:
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    lock_path = tmp_path / ("status.duckdb" + _LOCK_SUFFIX)
    lock_path.write_text("not-json{{")
    with patch(
        "moneybin.mcp.tools.system.find_blocking_processes",
        return_value=[],
    ):
        block = _database_connections_block(db_path)
    # Corrupted metadata is treated as "no writer info available."
    assert block["writers"] == []
