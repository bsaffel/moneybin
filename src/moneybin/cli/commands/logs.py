"""Log management command for MoneyBin CLI.

Single leaf command for viewing, pruning, and locating log files for the
active profile.
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.config import get_settings
from moneybin.utils.parsing import parse_duration

logger = logging.getLogger(__name__)


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

_VALID_STREAMS = ("cli", "mcp", "sqlmesh")


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
    until: datetime | None = None,
    pattern: re.Pattern[str] | None = None,
) -> list[_LogEntry]:
    """Filter parsed log entries by level, time bounds, and/or pattern."""
    min_priority = _LEVEL_PRIORITY.get(level.upper(), 0) if level else 0
    result: list[_LogEntry] = []
    for entry in entries:
        entry_priority = _LEVEL_PRIORITY.get(entry.level, 0)
        if entry_priority < min_priority:
            continue
        if since and entry.timestamp() < since:
            continue
        if until and entry.timestamp() > until:
            continue
        if pattern and not pattern.search(entry.message):
            if not any(pattern.search(line) for line in entry.extra_lines):
                continue
        result.append(entry)
    return result


def _tail_file(path: Path, n: int, block_size: int = 8192) -> list[str]:
    """Read the last n lines of a file without loading it entirely into memory."""
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


def _find_log_files(log_dir: Path, stream: str) -> list[Path]:
    """Find log files for the given stream, sorted newest first."""
    return sorted(
        log_dir.glob(f"{stream}_*.log"),
        key=lambda p: p.name,
        reverse=True,
    )


def _parse_time_bound(value: str) -> datetime:
    """Parse --since/--until: accepts a duration ('5m') or ISO-8601 timestamp."""
    try:
        delta = parse_duration(value)
        return datetime.now() - delta
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(value.rstrip("Z"))
    except ValueError as e:
        raise ValueError(
            f"--since/--until must be a duration (5m, 1h, 7d) "
            f"or ISO-8601 timestamp; got '{value}'"
        ) from e
    if parsed.tzinfo is not None:
        # Convert tz-aware input to naive local time so comparisons against
        # the naive datetimes parsed out of log lines (which are written in
        # local time by `SanitizedLogFormatter`) reference the same instant.
        # Naive stripping (`.replace(tzinfo=None)`) would silently drop the
        # offset and shift the cutoff by the offset's magnitude.
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def _do_prune(log_dir: Path, older_than: str, *, dry_run: bool, quiet: bool) -> None:
    try:
        delta = parse_duration(older_than)
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(2) from e

    cutoff = datetime.now() - delta
    if not log_dir.exists():
        if not quiet:
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
                if not quiet:
                    logger.info(
                        f"  Would delete: {log_file.name} ({size / 1024:.1f} KB)"
                    )
            else:
                log_file.unlink()
                if not quiet:
                    logger.info(f"  Deleted: {log_file.name}")
            deleted += 1
            freed_bytes += size

    if quiet:
        return
    if deleted == 0:
        logger.info(f"No log files older than {older_than}")
    elif dry_run:
        logger.info(
            f"Would delete {deleted} file(s), freeing {freed_bytes / 1024:.1f} KB"
        )
    else:
        logger.info(f"✅ Deleted {deleted} file(s), freed {freed_bytes / 1024:.1f} KB")


def _do_view(
    *,
    log_dir: Path,
    stream: str,
    follow: bool,
    lines: int,
    level: str | None,
    since: str | None,
    until: str | None,
    grep: str | None,
    output: OutputFormat,
    quiet: bool,
) -> None:
    if level and level.upper() not in _LEVEL_PRIORITY:
        logger.error(
            f"❌ Unknown level '{level}'. Choose from: {', '.join(_LEVEL_PRIORITY)}"
        )
        raise typer.Exit(2)

    since_dt: datetime | None = None
    if since:
        try:
            since_dt = _parse_time_bound(since)
        except ValueError as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(2) from e

    until_dt: datetime | None = None
    if until:
        try:
            until_dt = _parse_time_bound(until)
        except ValueError as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(2) from e

    grep_pattern: re.Pattern[str] | None = None
    if grep:
        try:
            grep_pattern = re.compile(grep)
        except re.error as e:
            logger.error(f"❌ Invalid regex pattern: {e}")
            raise typer.Exit(2) from e

    if not log_dir.exists():
        if not quiet:
            logger.info(f"No log directory found: {log_dir}")
        return

    log_files = _find_log_files(log_dir, stream)
    if not log_files:
        if not quiet:
            logger.info(f"No log files found for stream '{stream}' in {log_dir}")
        return

    has_filters = bool(
        level or since_dt or until_dt or grep_pattern or output == "json"
    )
    if has_filters:
        read_lines = lines * 10 if (level or grep_pattern) else lines
        raw_lines = _tail_file(log_files[0], read_lines)
        entries = _parse_log_lines(raw_lines)
        filtered = _filter_entries(
            entries,
            level=level,
            since=since_dt,
            until=until_dt,
            pattern=grep_pattern,
        )
        filtered = filtered[-lines:]
        if output == "json":
            typer.echo(json.dumps([e.to_dict() for e in filtered], indent=2))
        else:
            for entry in filtered:
                typer.echo(entry.to_text())
    else:
        for raw_line in _tail_file(log_files[0], lines):
            typer.echo(raw_line.rstrip())

    if follow:
        if not quiet:
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


def logs_command(
    stream: Annotated[
        str | None,
        typer.Argument(
            help="Log stream to view: cli, mcp, sqlmesh. Required unless "
            "--print-path or --prune is used.",
        ),
    ] = None,
    follow: Annotated[
        bool, typer.Option("-f", "--follow", help="Follow log output")
    ] = False,
    lines: Annotated[
        int, typer.Option("-n", "--lines", help="Number of lines to show")
    ] = 20,
    level: Annotated[
        str | None,
        typer.Option(
            "--level",
            help="Minimum log level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
        ),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help=(
                "Time window or absolute timestamp "
                "(e.g., 5m, 1h, 7d, 2026-04-01T00:00:00)"
            ),
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help="Upper time bound: duration ago or absolute timestamp",
        ),
    ] = None,
    grep: Annotated[
        str | None,
        typer.Option("--grep", help="Regex pattern to filter log messages"),
    ] = None,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    print_path: Annotated[
        bool,
        typer.Option(
            "--print-path",
            help="Print the log directory and exit (no stream required)",
        ),
    ] = False,
    prune: Annotated[
        bool,
        typer.Option(
            "--prune",
            help="Delete old log files instead of viewing (no stream required)",
        ),
    ] = False,
    older_than: Annotated[
        str | None,
        typer.Option(
            "--older-than",
            help="With --prune: delete logs older than this duration (e.g., 30d)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="With --prune: show what would be deleted"),
    ] = False,
) -> None:
    """View, prune, or locate MoneyBin log files for the active profile."""
    # Argument validation runs before any profile-dependent work so a bare
    # `moneybin logs` exits with a clean usage error (docker/kubectl style)
    # instead of triggering the first-run wizard or hitting profile-load
    # errors. Pairs with `_logs_bare_invocation` in cli/main.py, which keeps
    # the parent callback inert in this case.
    if stream is None and not print_path and not prune:
        typer.echo(
            "Error: Missing argument 'STREAM'. Pick one of: "
            f"{', '.join(_VALID_STREAMS)}",
            err=True,
        )
        raise typer.Exit(2)

    if prune and not older_than:
        typer.echo("Error: --prune requires --older-than DURATION", err=True)
        raise typer.Exit(2)

    if (
        not print_path
        and not prune
        and stream is not None
        and stream.lower() not in _VALID_STREAMS
    ):
        typer.echo(
            f"Error: Unknown stream '{stream}'. Choose from: "
            f"{', '.join(_VALID_STREAMS)}",
            err=True,
        )
        raise typer.Exit(2)

    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent

    if print_path:
        typer.echo(str(log_dir))
        return

    if prune:
        # older_than presence enforced by guard above; type narrows here.
        assert older_than is not None  # noqa: S101 — type-narrowing aid
        _do_prune(log_dir, older_than, dry_run=dry_run, quiet=quiet)
        return

    # stream presence and validity enforced by guards above; type narrows here.
    assert stream is not None  # noqa: S101 — type-narrowing aid

    _do_view(
        log_dir=log_dir,
        stream=stream.lower(),
        follow=follow,
        lines=lines,
        level=level,
        since=since,
        until=until,
        grep=grep,
        output=output,
        quiet=quiet,
    )


logs_command_app = typer.Typer(
    name="logs",
    help="View, prune, or locate MoneyBin log files for the active profile.",
    invoke_without_command=False,
    add_completion=False,
)
logs_command_app.command()(logs_command)
