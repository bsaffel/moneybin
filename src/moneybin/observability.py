"""Observability facade for MoneyBin.

This module is the single public entry point for all observability setup.
Consumers use:

    from moneybin.observability import setup_observability, tracked, track_duration

Standard Python logging remains unchanged:

    import logging
    logger = logging.getLogger(__name__)

Internal modules (``moneybin.logging``, ``moneybin.metrics``) should not
be imported directly by application code except for manual gauge/counter
access.
"""

import atexit
import logging
import threading
from typing import Literal

from moneybin.logging.config import setup_logging
from moneybin.metrics.instruments import track_duration, tracked

logger = logging.getLogger(__name__)

__all__ = ["setup_observability", "tracked", "track_duration"]

_initialized = False


def setup_observability(
    stream: Literal["cli", "mcp", "sqlmesh"] = "cli",
    verbose: bool = False,
    profile: str | None = None,
) -> None:
    """Initialize logging and metrics for the application.

    This should be called once at application startup:

        # CLI (main.py callback)
        setup_observability(stream="cli", verbose=verbose)

        # MCP server
        setup_observability(stream="mcp")

        # SQLMesh transforms
        setup_observability(stream="sqlmesh")

    What it does:
        1. Calls setup_logging() — handlers, formatters, sanitizer
        2. Registers atexit handler for metrics flush on shutdown
        3. For MCP stream: starts periodic flush timer (every 5 min)

    Args:
        stream: Log stream name ("cli", "mcp", "sqlmesh").
        verbose: Enable DEBUG level logging.
        profile: Profile name. When set, logging config is resolved from
            ``get_settings()``. When None (e.g. profile commands),
            console-only logging with defaults is used.
    """
    global _initialized

    # Step 1: Configure logging (always — allows reconfiguration)
    # Resolve log config from settings when a profile is available.
    # Without a profile (e.g. profile commands), use defaults (console only).
    if profile is not None:
        from moneybin.config import get_settings

        log_config = get_settings().logging
        setup_logging(
            stream=stream,
            verbose=verbose,
            level=log_config.level,
            log_format=log_config.format,
            log_to_file=log_config.log_to_file,
            log_file_path=log_config.log_file_path,
        )
    else:
        setup_logging(stream=stream, verbose=verbose)

    if not _initialized:
        # Step 2: Register atexit handler for metrics flush (once only)
        # TODO: Call load_from_duckdb() here to restore counter values from
        # the previous session. Deferred — requires deciding on startup cost
        # trade-offs (DB may not exist yet on first run). See persistence.py.
        atexit.register(_flush_metrics_on_exit)
        _initialized = True

    # Step 3: For MCP, start periodic flush (idempotent — checks _periodic_timer)
    if stream == "mcp" and _periodic_timer is None:
        from moneybin.config import get_settings

        interval = get_settings().metrics.flush_interval_seconds
        _start_periodic_flush(interval_seconds=interval)

    logger.debug(f"Observability initialized (stream={stream})")


def _flush_metrics_on_exit() -> None:
    """Flush all metrics to DuckDB on process exit.

    This is best-effort — if the database is unavailable, metrics are lost
    for this session (they'll be re-accumulated on next run).

    Only flushes if a database connection already exists — never creates one.
    Creating a connection on exit would recreate directories for a deleted
    profile and run migrations unexpectedly.
    """
    try:
        from moneybin.database import get_database_if_initialized
        from moneybin.metrics.persistence import flush_to_duckdb

        db = get_database_if_initialized()
        if db is None:
            return
        flush_to_duckdb(db)
    except Exception:  # noqa: BLE001  # best-effort shutdown flush; DB may be unavailable
        logger.debug("Metrics flush on exit failed", exc_info=True)


_periodic_timer: threading.Timer | None = None


def _start_periodic_flush(interval_seconds: int = 300) -> None:
    """Start a background timer that flushes metrics every interval.

    Args:
        interval_seconds: Seconds between flushes (default: 300 = 5 min).
    """
    global _periodic_timer

    def _flush_and_reschedule() -> None:
        global _periodic_timer
        _flush_metrics_on_exit()
        _periodic_timer = threading.Timer(interval_seconds, _flush_and_reschedule)
        _periodic_timer.daemon = True
        _periodic_timer.start()

    _periodic_timer = threading.Timer(interval_seconds, _flush_and_reschedule)
    _periodic_timer.daemon = True
    _periodic_timer.start()
    logger.debug(f"Periodic metrics flush started (every {interval_seconds}s)")
