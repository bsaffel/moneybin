"""Tests for system_status's database_connections section."""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from moneybin.db_lock import write_lock
from moneybin.db_lock.lock import (
    _LOCK_SUFFIX,  # type: ignore[reportPrivateUsage]  # test-only access to the canonical lock-file suffix
)
from moneybin.mcp.tools.system import (
    _database_connections_block,  # type: ignore[reportPrivateUsage]  # test-only access to the private helper
)


@contextmanager
def _holding_write_lock(
    db_path: Path, *, operation_type: str = "interactive"
) -> Generator[None, None, None]:
    """Hold the real ``write_lock`` for ``db_path`` in a background thread.

    The block reports a writer only when a process actually holds the fcntl
    lock, so tests that expect a writer must hold a real lock rather than
    just dropping a metadata file. The holder runs in a thread; flock
    conflicts are per-open-file-description, so the main thread's
    ``_writer_is_live`` probe sees the held ``LOCK_EX`` even in-process.
    """
    acquired = threading.Event()
    release = threading.Event()

    def holder() -> None:
        deadline = time.monotonic() + 5.0
        with write_lock(db_path, deadline=deadline, operation_type=operation_type):  # type: ignore[arg-type]
            acquired.set()
            release.wait(timeout=5.0)

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert acquired.wait(timeout=2.0), "background holder never acquired the lock"
        yield
    finally:
        release.set()
        t.join(timeout=5.0)


def test_block_reports_writer_while_lock_is_held(tmp_path: Path) -> None:
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    with (
        _holding_write_lock(db_path, operation_type="transform_apply"),
        patch(
            "moneybin.mcp.tools.system.find_blocking_processes",
            return_value=[],
        ),
    ):
        block = _database_connections_block(db_path)
    assert len(block["writers"]) == 1
    assert block["writers"][0]["pid"] == os.getpid()
    assert block["writers"][0]["operation_type"] == "transform_apply"
    assert block["readers"] == []


def test_block_sanitizes_writer_command_to_friendly_name(tmp_path: Path) -> None:
    """The reported command is a path-free friendly name, not raw argv.

    Process command lines can carry local paths, usernames, and filename
    arguments. Routing them through describe_process keeps the LOW-sensitivity
    database_connections payload free of that PII while still identifying the
    holder.
    """
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    lock_path = tmp_path / ("status.duckdb" + _LOCK_SUFFIX)
    with (
        _holding_write_lock(db_path),
        patch(
            "moneybin.mcp.tools.system.find_blocking_processes",
            return_value=[],
        ),
    ):
        # Overwrite the held lock file's metadata (same inode, holder's fcntl
        # unaffected) with a command carrying absolute local paths.
        lock_path.write_text(
            json.dumps({
                "pid": os.getpid(),
                "command": (
                    "/Users/bob/.venv/bin/moneybin transform apply "
                    "/Users/bob/data/moneybin.duckdb"
                ),
                "started_at": "2026-06-04T15:22:14+00:00",
                "operation_type": "transform_apply",
            })
        )
        block = _database_connections_block(db_path)
    assert len(block["writers"]) == 1
    assert block["writers"][0]["command"] == "transform pipeline"
    # No absolute path / username leaked into the LOW payload.
    assert "/Users/bob" not in block["writers"][0]["command"]


def test_block_omits_stale_lock_file_when_no_writer_holds(tmp_path: Path) -> None:
    """A lock file with valid metadata but no live holder is NOT a writer.

    write_lock never unlinks the metadata file on release, so it persists with
    the last holder's pid. The block must probe the lock, not trust the file's
    existence — otherwise every post-write system_status reports a phantom
    writer for as long as that process stays alive.
    """
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    lock_path = tmp_path / ("status.duckdb" + _LOCK_SUFFIX)
    lock_path.write_text(
        json.dumps({
            "pid": os.getpid(),  # a live pid — but nobody holds the lock
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
    assert block["writers"] == []


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
    writer_pid = os.getpid()
    # lsof returns the (live) writer pid AND a separate reader pid.
    with (
        _holding_write_lock(db_path, operation_type="interactive"),
        patch(
            "moneybin.mcp.tools.system.find_blocking_processes",
            return_value=[
                {"pid": writer_pid, "command": "moneybin", "cmdline": "moneybin sync"},
                {"pid": 6666, "command": "moneybin", "cmdline": "moneybin reports"},
            ],
        ),
    ):
        block = _database_connections_block(db_path)
    reader_pids = [r["pid"] for r in block["readers"]]
    assert writer_pid not in reader_pids
    assert 6666 in reader_pids


def test_block_resolves_symlinked_db_path(tmp_path: Path) -> None:
    """The block resolves db_path so it finds the lock at the real path.

    write_lock keys the lock file on db_path.resolve(); the block must do the
    same or it looks for a nonexistent lock beside the symlink and misses a
    live writer.
    """
    real_path = tmp_path / "real.duckdb"
    real_path.touch()
    link_path = tmp_path / "link.duckdb"
    link_path.symlink_to(real_path)
    with (
        _holding_write_lock(link_path, operation_type="migration"),
        patch(
            "moneybin.mcp.tools.system.find_blocking_processes",
            return_value=[],
        ),
    ):
        block = _database_connections_block(link_path)
    assert len(block["writers"]) == 1
    assert block["writers"][0]["operation_type"] == "migration"


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


def test_block_tolerates_corrupted_lock_file_while_held(tmp_path: Path) -> None:
    """Corrupted JSON in a held lock file is treated as no-writer-info."""
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    lock_path = tmp_path / ("status.duckdb" + _LOCK_SUFFIX)
    with (
        _holding_write_lock(db_path),
        patch(
            "moneybin.mcp.tools.system.find_blocking_processes",
            return_value=[],
        ),
    ):
        # Overwrite the held lock file's contents in place (same inode, so the
        # holder's fcntl lock is unaffected) with invalid JSON.
        lock_path.write_text("not-json{{")
        block = _database_connections_block(db_path)
    assert block["writers"] == []


def test_block_tolerates_non_dict_json_while_held(tmp_path: Path) -> None:
    """Non-dict JSON (null/list/scalar) in a held lock file does not crash.

    json.loads succeeds but metadata["pid"] raises TypeError; the block must
    catch it and report no writer rather than propagating.
    """
    db_path = tmp_path / "status.duckdb"
    db_path.touch()
    lock_path = tmp_path / ("status.duckdb" + _LOCK_SUFFIX)
    with (
        _holding_write_lock(db_path),
        patch(
            "moneybin.mcp.tools.system.find_blocking_processes",
            return_value=[],
        ),
    ):
        lock_path.write_text("null")
        block = _database_connections_block(db_path)
    assert block["writers"] == []
