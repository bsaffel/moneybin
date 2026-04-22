"""Log management commands for MoneyBin CLI.

Commands for viewing, cleaning, and tailing log files.
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Literal

import typer

from moneybin.config import get_settings
from moneybin.utils.parsing import parse_duration

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage log files",
    no_args_is_help=True,
)


@app.command("path")
def logs_path() -> None:
    """Print the log directory for the current profile."""
    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent
    typer.echo(str(log_dir))


@app.command("clean")
def logs_clean(
    older_than: str = typer.Option(
        ..., "--older-than", help="Delete logs older than this (e.g., 30d, 7d, 24h)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted without deleting"
    ),
) -> None:
    """Delete log files older than a specified duration."""
    try:
        delta = parse_duration(older_than)
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e

    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent
    cutoff = datetime.now() - delta

    if not log_dir.exists():
        logger.info(f"Log directory does not exist: {log_dir}")
        return

    deleted = 0
    freed_bytes = 0

    for log_file in log_dir.iterdir():
        if not log_file.is_file():
            continue
        mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
        if mtime < cutoff:
            size = log_file.stat().st_size
            if dry_run:
                logger.info(f"  Would delete: {log_file.name} ({size / 1024:.1f} KB)")
            else:
                log_file.unlink()
                logger.info(f"  Deleted: {log_file.name}")
            deleted += 1
            freed_bytes += size

    if deleted == 0:
        logger.info(f"No log files older than {older_than}")
    elif dry_run:
        logger.info(
            f"Would delete {deleted} file(s), freeing {freed_bytes / 1024:.1f} KB"
        )
    else:
        logger.info(f"✅ Deleted {deleted} file(s), freed {freed_bytes / 1024:.1f} KB")


# Matches HumanFormatter "full" variant: "2026-04-21 14:30:00,123 - name - LEVEL - msg"
_LOG_LINE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (.+?) - (\w+) - (.*)$"
)

_LEVEL_PRIORITY = {
    "DEBUG": 0,
    "INFO": 1,
    "WARNING": 2,
    "ERROR": 3,
    "CRITICAL": 4,
}


class _LogEntry:
    """A parsed log entry, possibly with continuation lines (tracebacks)."""

    __slots__ = ("timestamp_str", "logger_name", "level", "message", "extra_lines")

    def __init__(
        self,
        timestamp_str: str,
        logger_name: str,
        level: str,
        message: str,
    ) -> None:
        self.timestamp_str = timestamp_str
        self.logger_name = logger_name
        self.level = level
        self.message = message
        self.extra_lines: list[str] = []

    def timestamp(self) -> datetime:
        """Parse the timestamp string into a datetime."""
        return datetime.strptime(self.timestamp_str, "%Y-%m-%d %H:%M:%S,%f")

    def to_text(self) -> str:
        """Reconstruct the original log text."""
        header = (
            f"{self.timestamp_str} - {self.logger_name} - {self.level} - {self.message}"
        )
        if self.extra_lines:
            return header + "\n" + "\n".join(self.extra_lines)
        return header

    def to_dict(self) -> dict[str, str]:
        """Convert to a JSON-serializable dict."""
        d: dict[str, str] = {
            "timestamp": self.timestamp_str,
            "logger": self.logger_name,
            "level": self.level,
            "message": self.message,
        }
        if self.extra_lines:
            d["traceback"] = "\n".join(self.extra_lines)
        return d


def _parse_log_lines(lines: list[str]) -> list[_LogEntry]:
    """Parse raw log lines into structured entries.

    Lines that don't match the log format (e.g. traceback continuations)
    are attached to the preceding entry.

    Args:
        lines: Raw text lines from a log file.

    Returns:
        List of parsed log entries.
    """
    entries: list[_LogEntry] = []
    for line in lines:
        m = _LOG_LINE_RE.match(line)
        if m:
            entries.append(_LogEntry(m.group(1), m.group(2), m.group(3), m.group(4)))
        elif entries:
            entries[-1].extra_lines.append(line)
    return entries


def _filter_entries(
    entries: list[_LogEntry],
    *,
    level: str | None = None,
    since: datetime | None = None,
    pattern: re.Pattern[str] | None = None,
) -> list[_LogEntry]:
    """Filter parsed log entries by level, time, and/or pattern.

    Args:
        entries: Parsed log entries.
        level: Minimum log level (e.g. "ERROR" includes ERROR and CRITICAL).
        since: Only include entries at or after this time.
        pattern: Regex pattern to match against the message.

    Returns:
        Filtered list of entries.
    """
    min_priority = _LEVEL_PRIORITY.get(level.upper(), 0) if level else 0
    result: list[_LogEntry] = []
    for entry in entries:
        entry_priority = _LEVEL_PRIORITY.get(entry.level, 0)
        if entry_priority < min_priority:
            continue
        if since and entry.timestamp() < since:
            continue
        if pattern and not pattern.search(entry.message):
            # Also search extra lines (tracebacks)
            if not any(pattern.search(line) for line in entry.extra_lines):
                continue
        result.append(entry)
    return result


def _tail_file(path: Path, n: int, block_size: int = 8192) -> list[str]:
    """Read the last n lines of a file without loading it entirely into memory.

    Args:
        path: Path to the file.
        n: Number of lines to return.
        block_size: Bytes to read per backward seek step.

    Returns:
        The last n lines of the file.
    """
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        if size == 0:
            return []

        data = b""
        pos = size
        while pos > 0 and data.count(b"\n") <= n:
            step = min(block_size, pos)
            pos -= step
            f.seek(pos)
            data = f.read(step) + data

        lines = data.decode(errors="replace").splitlines()
        return lines[-n:]


_VALID_STREAMS = {"all", "cli", "mcp", "sqlmesh"}


def _find_log_files(log_dir: Path, stream: str) -> list[Path]:
    """Find log files for the given stream, sorted newest first.

    Args:
        log_dir: Directory containing log files.
        stream: Stream name ("cli", "mcp", "sqlmesh") or "all".

    Returns:
        Matching log files sorted by name descending (newest first).
    """
    if stream == "all":
        # Match any known stream prefix
        files: list[Path] = []
        for prefix in ("cli", "mcp", "sqlmesh"):
            files.extend(log_dir.glob(f"{prefix}_*.log"))
        return sorted(files, key=lambda p: p.name, reverse=True)
    return sorted(
        log_dir.glob(f"{stream}_*.log"),
        key=lambda p: p.name,
        reverse=True,
    )


@app.command("tail")
def logs_tail(
    stream: str | None = typer.Option(
        None,
        "--stream",
        help="Log stream to view: all (default), cli, mcp, sqlmesh",
    ),
    follow: bool = typer.Option(False, "-f", "--follow", help="Follow log output"),
    lines: int = typer.Option(20, "-n", "--lines", help="Number of lines to show"),
    level: str | None = typer.Option(
        None,
        "--level",
        help="Minimum log level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    ),
    since: str | None = typer.Option(
        None, "--since", help="Time window (e.g., 5m, 1h, 7d)"
    ),
    grep: str | None = typer.Option(
        None, "--grep", help="Regex pattern to filter log messages"
    ),
    output: Literal["text", "json"] = typer.Option(
        "text", "--output", help="Output format: text or json"
    ),
) -> None:
    """Show recent log entries, optionally following new output.

    By default, shows logs from all streams (cli, mcp, sqlmesh) merged
    by timestamp. Use --stream to view a single stream.

    Filters (--level, --since, --grep) parse structured log lines and
    return matching entries with their tracebacks. Use --output json for
    machine-readable output.

    Note: each invocation reads from the most recent log file per stream.
    Entries from older daily files are not included.
    """
    # Validate --level
    if level and level.upper() not in _LEVEL_PRIORITY:
        logger.error(
            f"❌ Unknown level '{level}'. Choose from: {', '.join(_LEVEL_PRIORITY)}"
        )
        raise typer.Exit(1)

    # Parse --since into a datetime cutoff
    since_dt: datetime | None = None
    if since:
        try:
            delta = parse_duration(since)
        except ValueError as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(1) from e
        since_dt = datetime.now() - delta

    # Compile --grep pattern
    grep_pattern: re.Pattern[str] | None = None
    if grep:
        try:
            grep_pattern = re.compile(grep)
        except re.error as e:
            logger.error(f"❌ Invalid regex pattern: {e}")
            raise typer.Exit(1) from e

    stream_name = (stream or "all").lower()
    if stream_name not in _VALID_STREAMS:
        logger.error(
            f"❌ Unknown stream '{stream_name}'. "
            f"Choose from: {', '.join(sorted(_VALID_STREAMS))}"
        )
        raise typer.Exit(1)

    # --follow requires a specific stream (can't tail multiple files)
    if follow and stream_name == "all":
        logger.error(
            "❌ --follow requires a specific stream. "
            "Use --stream cli, --stream mcp, or --stream sqlmesh."
        )
        raise typer.Exit(1)

    has_filters = level or since_dt or grep_pattern or output == "json"
    # "all" mode merges multiple files, so it always uses the parsed path
    needs_merge = stream_name == "all"

    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent

    if not log_dir.exists():
        logger.info(f"No log directory found: {log_dir}")
        return

    log_files = _find_log_files(log_dir, stream_name)

    if not log_files:
        logger.info(f"No log files found for stream '{stream_name}' in {log_dir}")
        return

    if has_filters or needs_merge:
        # Read more lines than requested to account for filtering
        read_lines = lines * 10 if (level or grep_pattern) else lines

        if needs_merge:
            # Read from the most recent file per stream prefix and merge
            seen_prefixes: set[str] = set()
            all_entries: list[_LogEntry] = []
            for log_file in log_files:
                prefix = log_file.name.split("_", 1)[0]
                if prefix in seen_prefixes:
                    continue
                seen_prefixes.add(prefix)
                raw_lines = _tail_file(log_file, read_lines)
                all_entries.extend(_parse_log_lines(raw_lines))
            # Sort merged entries by timestamp
            all_entries.sort(key=lambda e: e.timestamp_str)
            entries = all_entries
        else:
            raw_lines = _tail_file(log_files[0], read_lines)
            entries = _parse_log_lines(raw_lines)

        filtered = _filter_entries(
            entries, level=level, since=since_dt, pattern=grep_pattern
        )
        # Take only the last N entries after filtering
        filtered = filtered[-lines:]

        if output == "json":
            typer.echo(json.dumps([e.to_dict() for e in filtered], indent=2))
        else:
            for entry in filtered:
                typer.echo(entry.to_text())
    else:
        # No filters, single stream — fast path, raw tail
        tail_lines = _tail_file(log_files[0], lines)
        for raw_line in tail_lines:
            typer.echo(raw_line.rstrip())

    if follow:
        typer.echo("--- Following (Ctrl+C to stop) ---")
        try:
            with open(log_files[0], encoding="utf-8") as f:
                f.seek(0, 2)
                while True:
                    raw_line = f.readline()
                    if raw_line:
                        typer.echo(raw_line.rstrip())
                    else:
                        time.sleep(0.5)
        except KeyboardInterrupt:
            pass
