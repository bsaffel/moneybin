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

import logging
from typing import Literal

from moneybin.logging.config import setup_logging
from moneybin.metrics.instruments import track_duration, tracked

logger = logging.getLogger(__name__)

__all__ = ["setup_observability", "tracked", "track_duration", "flush_metrics"]

_atexit_registered = False


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
        2. For ``stream="cli"`` only, registers an atexit hook that calls
           flush_metrics() at process shutdown. MCP sessions flush via
           ``close_db()`` in mcp/server.py instead.

    Args:
        stream: Log stream name ("cli", "mcp", "sqlmesh").
        verbose: Enable DEBUG level logging.
        profile: Profile name. When set, logging config is resolved from
            ``get_settings()``. When None (e.g. profile commands),
            console-only logging with defaults is used.
    """
    global _atexit_registered

    # Configure logging (always — allows reconfiguration)
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

    # CLI sessions register an atexit flush so counters reach app.metrics
    # at process exit. MCP sessions intentionally skip atexit — they call
    # flush_metrics() inside close_db() in the server lifecycle instead.
    # The gate is per-stream (not a one-shot init flag) so a process that
    # first booted as mcp/sqlmesh and later runs CLI work still wires the
    # hook up. Long-term: write-piggybacked persistence; see private/plans/.
    if stream == "cli" and not _atexit_registered:
        import atexit

        atexit.register(flush_metrics)
        _atexit_registered = True

    logger.debug(f"Observability initialized (stream={stream})")


def flush_metrics() -> None:
    """Flush all metrics to DuckDB.

    This is best-effort — if the database is unavailable, metrics are lost
    for this session (they'll be re-accumulated on next run).

    Only flushes if a write connection was opened this session — never flushes
    for read-only sessions. This avoids opening a write lock at exit purely to
    persist counters when no business data was written.

    Call paths:
    - CLI sessions: registered as an atexit hook in setup_observability().
    - MCP sessions: called inside mcp/server.py:close_db().
    """
    try:
        from moneybin.database import database_was_written, get_database
        from moneybin.metrics.persistence import flush_to_duckdb

        if not database_was_written():
            return
        with get_database(read_only=False, max_wait=2.0) as db:
            flush_to_duckdb(db)
    except Exception:  # noqa: BLE001  # best-effort shutdown flush; DB may be unavailable
        logger.debug("Metrics flush on exit failed", exc_info=True)
