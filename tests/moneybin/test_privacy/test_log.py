"""JSONL privacy event log: append, rotate, fail-soft."""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from moneybin.privacy.log import (
    read_privacy_events,
    read_privacy_events_page,
    write_privacy_event,
)


@pytest.fixture
def profile_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Provide an isolated profile dir and patch the resolver to use it."""
    d = tmp_path / "profile"
    d.mkdir(mode=0o700)
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: d,
    )
    return d


def _sample_event() -> dict[str, object]:
    return {
        "ts": "2026-05-17T12:00:00Z",
        "actor": "mcp.transactions_search",
        "action": "tool_call",
        "sensitivity": "medium",
        "classes_returned": ["txn_amount", "txn_date"],
        "row_count": 42,
    }


def test_write_creates_jsonl_file_with_0600_perms(profile_dir: Path) -> None:
    write_privacy_event(_sample_event())
    log = profile_dir / "privacy.log.jsonl"
    assert log.exists()
    mode = stat.S_IMODE(log.stat().st_mode)
    assert mode == 0o600, f"expected 0o600, got {oct(mode)}"


def test_append_produces_parseable_lines(profile_dir: Path) -> None:
    write_privacy_event(_sample_event())
    write_privacy_event(_sample_event() | {"row_count": 100})
    log = profile_dir / "privacy.log.jsonl"
    lines = log.read_text().splitlines()
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["row_count"] == 42
    assert parsed[1]["row_count"] == 100


def test_daily_rotation_renames_previous_day(profile_dir: Path) -> None:
    # Write a line "yesterday" — manipulate mtime to simulate.
    log = profile_dir / "privacy.log.jsonl"
    write_privacy_event(_sample_event())
    yesterday_ts = datetime(2026, 5, 16, 0, 0, tzinfo=UTC).timestamp()
    os.utime(log, (yesterday_ts, yesterday_ts))

    with patch("moneybin.privacy.log._today_utc", return_value="2026-05-17"):
        write_privacy_event(_sample_event() | {"actor": "mcp.next_day"})

    rotated = profile_dir / "privacy.log.2026-05-16.jsonl"
    assert rotated.exists(), "previous-day file should have been renamed"
    new_log = profile_dir / "privacy.log.jsonl"
    assert new_log.exists()
    new_lines = new_log.read_text().splitlines()
    assert len(new_lines) == 1
    assert json.loads(new_lines[0])["actor"] == "mcp.next_day"


def test_append_failure_logs_warning_and_does_not_raise(
    profile_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Make the dir un-writable, then write — should warn but not raise.
    profile_dir.chmod(0o500)
    try:
        with caplog.at_level("WARNING", logger="moneybin.privacy.log"):
            write_privacy_event(_sample_event())  # must not raise
        assert any(
            "privacy log write failed" in r.message.lower() for r in caplog.records
        )
    finally:
        profile_dir.chmod(0o700)


def test_read_filters_by_actor(profile_dir: Path) -> None:
    write_privacy_event(_sample_event() | {"actor": "mcp.a"})
    write_privacy_event(_sample_event() | {"actor": "mcp.b"})
    write_privacy_event(_sample_event() | {"actor": "mcp.a"})
    rows = read_privacy_events({"actor": "mcp.a"}, max_rows=10)
    assert len(rows) == 2
    assert all(r["actor"] == "mcp.a" for r in rows)


def test_read_respects_max_rows(profile_dir: Path) -> None:
    for i in range(5):
        write_privacy_event(_sample_event() | {"row_count": i})
    rows = read_privacy_events({}, max_rows=3)
    assert len(rows) == 3


def test_read_page_returns_exact_total_beyond_legacy_cap(profile_dir: Path) -> None:
    for i in range(1002):
        write_privacy_event(_sample_event() | {"row_count": i})

    rows, total = read_privacy_events_page({}, limit=2, offset=1000)

    assert [row["row_count"] for row in rows] == [1, 0]
    assert total == 1002
