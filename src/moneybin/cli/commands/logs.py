"""Log management commands for MoneyBin CLI.

Commands for viewing, cleaning, and tailing log files.
"""

import logging
import re
import time
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
        logger.info("Log directory does not exist: %s", log_dir)
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
                logger.info("  Would delete: %s (%.1f KB)", log_file.name, size / 1024)
            else:
                log_file.unlink()
                logger.info("  Deleted: %s", log_file.name)
            deleted += 1
            freed_bytes += size

    if deleted == 0:
        logger.info("No log files older than %s", older_than)
    elif dry_run:
        logger.info(
            "Would delete %d file(s), freeing %.1f KB", deleted, freed_bytes / 1024
        )
    else:
        logger.info("✅ Deleted %d file(s), freed %.1f KB", deleted, freed_bytes / 1024)


@app.command("tail")
def logs_tail(
    stream: str | None = typer.Option(
        None, "--stream", help="Filter by stream: mcp, sqlmesh"
    ),
    follow: bool = typer.Option(False, "-f", "--follow", help="Follow log output"),
    lines: int = typer.Option(20, "-n", "--lines", help="Number of lines to show"),
) -> None:
    """Show recent log entries, optionally following new output."""
    settings = get_settings()
    log_path = settings.logging.log_file_path

    if not log_path.exists():
        logger.info("No log file found: %s", log_path)
        return

    with open(log_path) as f:
        all_lines = f.readlines()

    if stream:
        all_lines = [line for line in all_lines if stream.lower() in line.lower()]

    for line in all_lines[-lines:]:
        typer.echo(line.rstrip())

    if follow:
        typer.echo("--- Following (Ctrl+C to stop) ---")
        try:
            with open(log_path) as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        if stream is None or stream.lower() in line.lower():
                            typer.echo(line.rstrip())
                    else:
                        time.sleep(0.5)
        except KeyboardInterrupt:
            pass
