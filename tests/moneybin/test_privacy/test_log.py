"""JSONL privacy event log: append, rotate, fail-soft."""

from __future__ import annotations

import json
import multiprocessing
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


def _write_event_in_process(
    profile_dir: str,
    start: object,
    ready: object,
    row_count: int,
) -> None:
    """Append after a shared start signal from an isolated process."""
    import moneybin.privacy.log as privacy_log

    privacy_log._resolve_privacy_log_dir = (  # pyright: ignore[reportPrivateUsage]  # isolate child log
        lambda: Path(profile_dir)
    )
    privacy_log.time.time_ns = lambda: 1
    ready.put(row_count)  # type: ignore[attr-defined]  # multiprocessing proxy
    start.wait()  # type: ignore[attr-defined]  # multiprocessing proxy
    privacy_log.write_privacy_event(_sample_event() | {"row_count": row_count})


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
    assert len(parsed[0]["event_id"]) == 32
    assert parsed[0]["event_id"] != parsed[1]["event_id"]


def test_concurrent_processes_persist_monotonic_event_ids(profile_dir: Path) -> None:
    process_count = 4
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    ready = context.Queue()
    processes = [
        context.Process(
            target=_write_event_in_process,
            args=(str(profile_dir), start, ready, row_count),
        )
        for row_count in range(process_count)
    ]
    for process in processes:
        process.start()
    for _ in processes:
        ready.get(timeout=10)
    start.set()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    events = [
        json.loads(line)
        for line in (profile_dir / "privacy.log.jsonl").read_text().splitlines()
    ]
    event_ids = [event["event_id"] for event in events]
    assert len(event_ids) == process_count
    assert len(set(event_ids)) == process_count
    assert event_ids == sorted(event_ids)


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

    page = read_privacy_events_page({}, limit=2)

    assert [row["row_count"] for row in page.events] == [1001, 1000]
    assert page.total_count == 1002
    assert page.snapshot_event_id is not None
    assert page.has_more is True


def test_page_synthesizes_duplicate_legacy_ids_without_rewriting(
    profile_dir: Path,
) -> None:
    log = profile_dir / "privacy.log.jsonl"
    event = _sample_event()
    log.write_text(
        "\n".join(json.dumps(event, sort_keys=True) for _ in range(3)) + "\n"
    )
    log.chmod(0o600)
    original = log.read_bytes()
    original_inode = log.stat().st_ino

    first = read_privacy_events_page({}, limit=2)
    second = read_privacy_events_page(
        {},
        limit=2,
        snapshot_event_id=first.snapshot_event_id,
        after_event_id=first.events[-1]["event_id"],
        snapshot_total=first.total_count,
        legacy_digest=first.legacy_digest,
    )

    assert len({event["event_id"] for event in first.events + second.events}) == 3
    assert len(first.events) == 2
    assert len(second.events) == 1
    assert log.read_bytes() == original
    assert log.stat().st_ino == original_inode
    assert stat.S_IMODE(log.stat().st_mode) == 0o600


def test_late_legacy_append_invalidates_cursor_without_rewriting(
    profile_dir: Path,
) -> None:
    for row_count in range(3):
        write_privacy_event(_sample_event() | {"row_count": row_count})
    log = profile_dir / "privacy.log.jsonl"
    original = [json.loads(line) for line in log.read_text().splitlines()]
    first = read_privacy_events_page({}, limit=1)
    assert first.snapshot_event_id is not None

    with log.open("a", encoding="utf-8") as stream:
        stream.write(
            json.dumps(
                _sample_event() | {"row_count": 99, "ts": "2099-01-01T00:00:00+00:00"}
            )
            + "\n"
        )
    with pytest.raises(ValueError, match="legacy privacy log changed"):
        read_privacy_events_page(
            {},
            limit=2,
            snapshot_event_id=first.snapshot_event_id,
            after_event_id=first.events[-1]["event_id"],
            snapshot_total=first.total_count,
            legacy_digest=first.legacy_digest,
        )
    persisted = [json.loads(line) for line in log.read_text().splitlines()]
    fresh = read_privacy_events_page({}, limit=10)
    late = next(event for event in fresh.events if event["row_count"] == 99)

    assert [event["event_id"] for event in persisted[:3]] == [
        event["event_id"] for event in original
    ]
    assert "event_id" not in persisted[-1]
    assert late["event_id"] > first.snapshot_event_id


def test_duplicate_legacy_removal_invalidates_cursor(profile_dir: Path) -> None:
    log = profile_dir / "privacy.log.jsonl"
    line = json.dumps(_sample_event(), sort_keys=True)
    log.write_text(f"{line}\n{line}\n{line}\n")
    first = read_privacy_events_page({}, limit=1)
    assert first.snapshot_event_id is not None

    log.write_text(f"{line}\n{line}\n")

    with pytest.raises(ValueError, match="legacy privacy log changed"):
        read_privacy_events_page(
            {},
            limit=1,
            snapshot_event_id=first.snapshot_event_id,
            after_event_id=first.events[-1]["event_id"],
            snapshot_total=first.total_count,
            legacy_digest=first.legacy_digest,
        )
