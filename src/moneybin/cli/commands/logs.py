"""Log management commands for MoneyBin CLI.

Commands for viewing, cleaning, and tailing log files.
"""

import logging
import re
import time
from collections import deque
from datetime import datetime, timedelta

import typer

from moneybin.config import get_settings

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage log files",
    no_args_is_help=True,
)


def _parse_duration(duration: str) -> timedelta:
    """Parse a duration string like '30d', '7d', '24h' into a timedelta.

    Args:
        duration: Duration string (e.g., "30d", "7d", "24h", "60m").

    Returns:
        timedelta for the specified duration.

    Raises:
        ValueError: If format is invalid.
    """
    match = re.match(r"^(\d+)([dhm])$", duration.strip())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration}'. Use <number><unit> "
            "where unit is d (days), h (hours), or m (minutes)."
        )
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return timedelta(days=value)
    elif unit == "h":
        return timedelta(hours=value)
    else:
        return timedelta(minutes=value)


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
        delta = _parse_duration(older_than)
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


@app.command("tail")
def logs_tail(
    stream: str | None = typer.Option(
        None, "--stream", help="Stream to tail: cli (default), mcp, sqlmesh"
    ),
    follow: bool = typer.Option(False, "-f", "--follow", help="Follow log output"),
    lines: int = typer.Option(20, "-n", "--lines", help="Number of lines to show"),
) -> None:
    """Show recent log entries, optionally following new output."""
    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent

    if not log_dir.exists():
        logger.info(f"No log directory found: {log_dir}")
        return

    # Find the most recent log file for the requested stream
    stream_prefix = (stream or "cli").lower()
    log_files = sorted(
        log_dir.glob(f"{stream_prefix}_*.log"),
        key=lambda p: p.name,
        reverse=True,
    )

    if not log_files:
        logger.info(f"No log files found for stream '{stream_prefix}' in {log_dir}")
        return

    log_path = log_files[0]  # Most recent by name (date-sorted)

    tail_buf: deque[str] = deque(maxlen=lines)
    with open(log_path) as f:
        for line in f:
            tail_buf.append(line)

    for line in tail_buf:
        typer.echo(line.rstrip())

    if follow:
        typer.echo("--- Following (Ctrl+C to stop) ---")
        try:
            with open(log_path) as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        typer.echo(line.rstrip())
                    else:
                        time.sleep(0.5)
        except KeyboardInterrupt:
            pass
