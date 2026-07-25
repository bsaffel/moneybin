"""Logging configuration for MoneyBin.

This module provides ``setup_logging()`` which configures Python's logging
system. It is called internally by ``setup_observability()`` — application
code should not call it directly.

``setup_logging()`` does not call ``get_settings()``. All configuration
values are passed explicitly by the caller, decoupling logging setup from
profile resolution and settings loading.
"""

import logging
import stat as stat_mod
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

from moneybin.log_sanitizer import SanitizedLogFormatter
from moneybin.logging.formatters import HumanFormatter, JSONFormatter

logger = logging.getLogger(__name__)


class _SuppressFilter(logging.Filter):
    """Filter out noisy SQLMesh analytics shutdown messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "Shutting down the event dispatcher" not in record.getMessage()


# Logger-name prefixes whose INFO/DEBUG output is too noisy for the console but
# should still reach the log file. That second half is the point: `logger.debug`
# would drop these records everywhere, because the root logger sits at INFO and
# never emits them at all. Use this list when the detail belongs in the file;
# use `logger.debug` only when a surviving INFO line already carries it.
#
# - sqlmesh: model evaluation, plan creation, state sync, analytics. Has its own
#   file via _setup_sqlmesh_file_handler.
# - httpx/httpcore: one line per request, so a `sync pull` becomes a wall of
#   "HTTP/1.1 201 Created".
# - matching.engine: per-tier counts in the engine's own vocabulary. The file
#   keeps them because MatchResult.summary() reports only run-wide totals.
# - extractors.plaid: per-table row counts that name a Chase card a "Plaid
#   account". The file keeps them because the CLI's per-institution totals go
#   out through typer.echo, which never reaches the log file.
#
# This is a denylist: anything not named here reaches the console. That is the
# deliberate choice. An allowlist would be quiet by default — nicer as new
# dependencies arrive — but it inverts the default for every one of the ~168
# `logger.info` sites in the tree, and each one whose output a user actually
# needs becomes a silent regression. The cases worth hiding are enumerable; the
# cases that must stay visible are not.
_CONSOLE_SUPPRESSED_PREFIXES: tuple[str, ...] = (
    "sqlmesh",
    "httpx",
    "httpcore",
    "moneybin.matching.engine",
    "moneybin.extractors.plaid",
    # Which of --profile / MONEYBIN_PROFILE / config.yaml chose the profile.
    # A child logger so this one line can be suppressed without silencing
    # `moneybin.cli`, which is ordinary user-facing output.
    "moneybin.cli.utils.profile_source",
    # Per-engine categorization counts. `categorize_pending()` restates them
    # in its run summary, so they duplicate on the console — but the
    # per-method path calls the engines directly with no summary, so the file
    # needs them. A child logger keeps the parent's run summary visible.
    "moneybin.services.categorization.orchestrator.engine_counts",
)


def _matches(name: str, prefixes: tuple[str, ...]) -> bool:
    """True when ``name`` is one of ``prefixes`` or a descendant of one."""
    return any(name == p or name.startswith(f"{p}.") for p in prefixes)


class _ConsoleNoiseFilter(logging.Filter):
    """Suppress noisy INFO messages from the console only.

    Attached to the console handler so file handlers still see everything.
    WARNING and above always pass — quieting noise must never quiet a problem.
    """

    def __init__(self, *, file_sink: bool = False) -> None:
        super().__init__()
        # Suppression assumes the log file keeps a copy of what stderr drops.
        # With `log_to_file: false` there is no copy, and both
        # docs/guides/observability.md and threat-model.md promise stderr stays
        # "unaffected" so containers and journald can capture it — so filtering
        # there would destroy records, not relocate them. Stand down instead.
        self._file_sink = file_sink

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING or not self._file_sink:
            return True
        return not _matches(record.name, _CONSOLE_SUPPRESSED_PREFIXES)


def session_log_path(
    configured_path: Path,
    prefix: str = "cli",
    now: datetime | None = None,
) -> Path:
    """Derive a daily log path from the configured log file path.

    Transforms ``logs/{profile}/moneybin.log`` into
    ``logs/{profile}/{prefix}_YYYY-MM-DD.log`` so that logs are
    grouped by stream and day. Each stream appends to its daily file.

    Args:
        configured_path: The log_file_path from configuration (used to find
            the profile log directory).
        prefix: Stream name prefix (e.g. "cli", "mcp", "sqlmesh").
        now: Timestamp to use for the path; defaults to the current time.

    Returns:
        Path to the stream-specific daily log file.
    """
    if now is None:
        now = datetime.now()
    profile_log_dir = configured_path.parent
    return profile_log_dir / f"{prefix}_{now.strftime('%Y-%m-%d')}.log"


def _make_file_handler(
    log_file: Path, formatter: logging.Formatter
) -> logging.FileHandler:
    """Create a file handler with restrictive permissions.

    Args:
        log_file: Path to the log file.
        formatter: Formatter to apply (will be wrapped in SanitizedLogFormatter).

    Returns:
        Configured FileHandler.
    """
    handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    if sys.platform != "win32":
        try:
            log_file.chmod(stat_mod.S_IRUSR | stat_mod.S_IWUSR)  # 0600
        except OSError:
            pass
    handler.setFormatter(SanitizedLogFormatter(formatter))
    return handler


def _setup_sqlmesh_file_handler(
    log_file_path: Path, formatter: logging.Formatter
) -> None:
    """Route SQLMesh logger output to a dedicated sqlmesh log file.

    Per the observability spec, SQLMesh output goes to
    ``sqlmesh_YYYY-MM-DD.log`` (file only — suppressed from console).
    The handler is attached to the root ``sqlmesh`` logger so every
    ``sqlmesh.*`` descendant inherits it, and propagation is disabled so
    the same records don't also land in the CLI log file.

    Args:
        log_file_path: Base log file path (used to derive the sqlmesh log path).
        formatter: Formatter to apply to the file handler.
    """
    sqlmesh_log = session_log_path(log_file_path, prefix="sqlmesh")
    resolved_path = str(sqlmesh_log.resolve())

    sqlmesh_logger = logging.getLogger("sqlmesh")

    # Evict any stale FileHandlers pointing at a different path before
    # adding the new one. Long-lived processes (MCP server) crossing
    # midnight would otherwise accumulate one handler per day.
    for stale in [
        h
        for h in sqlmesh_logger.handlers
        if isinstance(h, logging.FileHandler)
        and getattr(h, "baseFilename", None) != resolved_path
    ]:
        sqlmesh_logger.removeHandler(stale)
        stale.close()

    if not any(
        isinstance(h, logging.FileHandler)
        and getattr(h, "baseFilename", None) == resolved_path
        for h in sqlmesh_logger.handlers
    ):
        sqlmesh_logger.addHandler(_make_file_handler(sqlmesh_log, formatter))

    # propagate=False stops INFO/DEBUG noise from reaching the CLI/MCP
    # log file, but it would also hide WARNING/ERROR from the root
    # console handler. Add a dedicated WARNING-level stderr handler on
    # the sqlmesh logger so important runtime issues are still visible.
    if not any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in sqlmesh_logger.handlers
    ):
        sqlmesh_console = logging.StreamHandler(sys.stderr)
        sqlmesh_console.setLevel(logging.WARNING)
        sqlmesh_console.setFormatter(SanitizedLogFormatter(formatter))
        sqlmesh_logger.addHandler(sqlmesh_console)
    sqlmesh_logger.propagate = False


def setup_logging(
    stream: Literal["cli", "mcp", "sqlmesh"] = "cli",
    verbose: bool = False,
    *,
    level: str = "INFO",
    log_format: Literal["human", "json"] = "human",
    log_to_file: bool = False,
    log_file_path: Path | None = None,
) -> None:
    """Set up centralized logging configuration.

    All parameters are passed explicitly — this function does not call
    ``get_settings()``. The caller (``setup_observability()``) resolves
    log settings from the profile config and passes them here.

    Args:
        stream: Log stream name — determines file prefix and console format.
            "cli" uses message-only console format; "mcp" and "sqlmesh" use
            full format with timestamps.
        verbose: If True, enable DEBUG level logging (overrides ``level``).
        level: Log level name (e.g. "INFO", "DEBUG"). Ignored when
            ``verbose`` is True.
        log_format: Log output format: "human" or "json".
        log_to_file: Whether to write logs to a file.
        log_file_path: Path used to derive the daily log file location.
            Required when ``log_to_file`` is True.
    """
    # Determine log level
    if verbose:
        resolved_level = logging.DEBUG
    else:
        resolved_level = getattr(logging, level)

    # Build formatter based on config
    if log_format == "json":
        inner_formatter: logging.Formatter = JSONFormatter()
    elif stream == "cli":
        inner_formatter = HumanFormatter(variant="cli")
    else:
        inner_formatter = HumanFormatter(variant="full")

    # Console always uses human-readable format (JSON is for file output only).
    # When log_format=="json", inner_formatter is JSONFormatter, so substitute
    # a HumanFormatter for the console so users don't see JSON on stderr.
    if log_format == "json":
        console_formatter: logging.Formatter = HumanFormatter(
            variant="cli" if stream == "cli" else "full"
        )
    else:
        console_formatter = inner_formatter

    # Prepare handlers
    handlers: list[logging.Handler] = []

    # Console handler (always present, writes to stderr). Its filter is
    # attached below, once we know whether a file handler actually landed.
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(SanitizedLogFormatter(console_formatter))
    handlers.append(console_handler)

    # File handler — only write to file if the log directory's parent exists.
    # Profile directories are created by ProfileService.create(), not here.
    # Using parents=True would silently recreate a deleted profile's tree.
    if log_to_file and log_file_path is not None:
        log_file: Path | None = session_log_path(log_file_path, prefix=stream)
        try:
            log_file.parent.mkdir(parents=False, exist_ok=True)
        except FileNotFoundError:
            logger.warning("Log directory not found; writing logs to console only")
            log_file = None

        if log_file is not None:
            file_handler = _make_file_handler(log_file, inner_formatter)
            handlers.append(file_handler)

            # Dedicated sqlmesh log file — SQLMesh output is noisy so it
            # gets its own stream file per the observability spec. The
            # console filter already suppresses these from stderr; this
            # ensures they still reach a log file for debugging.
            _setup_sqlmesh_file_handler(log_file_path, inner_formatter)

    # Attach the console filter now that the file-handler outcome is known —
    # including the mkdir failure above, which leaves stderr as the only sink
    # just as surely as `log_to_file: false` does.
    console_handler.addFilter(
        _ConsoleNoiseFilter(
            file_sink=any(isinstance(h, logging.FileHandler) for h in handlers)
        )
    )

    # Configure root logger
    logging.basicConfig(
        level=resolved_level,
        handlers=handlers,
        force=True,
    )

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("plaid").setLevel(logging.INFO)
    # Suppress SQLMesh analytics dispatcher via the same filter as other
    # noisy SQLMesh loggers — setLevel would hide file output too.
    # The _SuppressFilter below handles the specific shutdown message string.

    # Suppress SQLMesh analytics shutdown message (guard against duplicates)
    if not any(isinstance(f, _SuppressFilter) for f in logging.root.filters):
        logging.root.addFilter(_SuppressFilter())
