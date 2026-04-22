"""Instrumentation helpers for MoneyBin.

Provides ``@tracked`` (decorator) and ``track_duration`` (context manager)
for recording call counts, durations, and errors with minimal boilerplate.

Both emit a DEBUG-level log line on completion with operation name and duration.
"""

import functools
import logging
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import ParamSpec, TypeVar

from prometheus_client import Counter, Histogram

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

# Generic tracked metrics — used by @tracked and track_duration.
# Domain-specific metrics (IMPORT_RECORDS_TOTAL, etc.) are in registry.py
# and recorded manually at domain-specific call sites.
_TRACKED_CALLS = Counter(
    "moneybin_tracked_calls_total",
    "Total tracked operation calls",
    ["operation", "source_type"],
)

_TRACKED_DURATION = Histogram(
    "moneybin_tracked_duration_seconds",
    "Duration of tracked operations in seconds",
    ["operation", "source_type"],
)

_TRACKED_ERRORS = Counter(
    "moneybin_tracked_errors_total",
    "Total tracked operation errors",
    ["operation", "error_type", "source_type"],
)


def tracked(
    operation: str,
    labels: dict[str, str] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator that records call count, duration, and errors for a function.

    Args:
        operation: Name of the operation (e.g. "import", "dedup").
        labels: Optional static labels to attach to all metrics.

    Returns:
        Decorator that wraps the function with instrumentation.
    """
    extra_labels = labels or {}
    source_type = extra_labels.get("source_type", "")

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            call_labels = {"operation": operation, "source_type": source_type}
            _TRACKED_CALLS.labels(**call_labels).inc()
            start = time.monotonic()
            error: Exception | None = None
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 — re-raised immediately; caught only to record error metrics
                error = exc
                raise
            finally:
                duration = time.monotonic() - start
                _TRACKED_DURATION.labels(**call_labels).observe(duration)
                if error is not None:
                    _TRACKED_ERRORS.labels(
                        operation=operation,
                        error_type=type(error).__name__,
                        source_type=source_type,
                    ).inc()
                    logger.debug(
                        f"{operation} failed after {duration:.3f}s: "
                        f"{type(error).__name__}"
                    )
                else:
                    logger.debug(f"{operation} completed in {duration:.3f}s")

        return wrapper

    return decorator


@contextmanager
def track_duration(
    operation: str, labels: dict[str, str] | None = None
) -> Generator[None, None, None]:
    """Context manager that records the duration of a block.

    Args:
        operation: Name of the operation.
        labels: Optional static labels.

    Yields:
        None — the block executes normally.
    """
    extra_labels = labels or {}
    call_labels = {
        "operation": operation,
        "source_type": extra_labels.get("source_type", ""),
    }
    start = time.monotonic()
    error: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        error = exc
        raise
    finally:
        duration = time.monotonic() - start
        _TRACKED_DURATION.labels(**call_labels).observe(duration)
        if error is not None:
            logger.debug(
                f"{operation} failed after {duration:.3f}s: {type(error).__name__}"
            )
        else:
            logger.debug(f"{operation} completed in {duration:.3f}s")
