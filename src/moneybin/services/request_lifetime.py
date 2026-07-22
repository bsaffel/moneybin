"""Cross-thread cancellation and final-publication coordination."""

from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar


class PublicationCancelledError(RuntimeError):
    """Raised when a request ended before its final publication boundary."""


class RequestLifetime:
    """Coordinate cancellation with one or more atomic publication sections."""

    def __init__(self) -> None:
        """Create one live request with no active publication sections."""
        self._condition = threading.Condition()
        self._cancelled = False
        self._publishing = 0

    def raise_if_cancelled(self) -> None:
        """Stop surviving worker activity after its caller has gone away."""
        with self._condition:
            if self._cancelled:
                raise PublicationCancelledError(
                    "Request ended before publication completed."
                )

    def cancel_and_wait(self) -> None:
        """Cancel future publication and wait for an entered boundary to leave."""
        with self._condition:
            self._cancelled = True
            while self._publishing:
                self._condition.wait()

    @contextmanager
    def publication_barrier(self) -> Generator[None]:
        """Enter a final side-effect section only while the request is live."""
        with self._condition:
            if self._cancelled:
                raise PublicationCancelledError(
                    "Request ended before publication completed."
                )
            self._publishing += 1
        try:
            yield
        finally:
            with self._condition:
                self._publishing -= 1
                self._condition.notify_all()


_current_request_lifetime: ContextVar[RequestLifetime | None] = ContextVar(
    "current_request_lifetime",
    default=None,
)


@contextmanager
def request_lifetime_scope(lifetime: RequestLifetime) -> Generator[None]:
    """Bind a request lifetime so copied worker contexts share cancellation."""
    token = _current_request_lifetime.set(lifetime)
    try:
        yield
    finally:
        _current_request_lifetime.reset(token)


def current_request_lifetime() -> RequestLifetime | None:
    """Return the lifetime inherited by the current task or worker thread."""
    return _current_request_lifetime.get()


@contextmanager
def publication_barrier(
    lifetime: RequestLifetime | None,
) -> Generator[None]:
    """Use the lifetime's barrier, or a no-op outside managed requests."""
    if lifetime is None:
        yield
        return
    with lifetime.publication_barrier():
        yield
