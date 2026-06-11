"""Shared fixtures for the write-lock primitive tests."""

from __future__ import annotations

from collections.abc import Generator

import pytest


@pytest.fixture(autouse=True)
def _assert_no_leaked_lock_holders() -> (  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture, applied by name
    Generator[None, None, None]
):
    """Fail (and reset) if a test leaves an entry in the process-global _held_by.

    write_lock removes its ``(pid, thread_id)`` key on context exit, but a test
    that calls write_lock directly and aborts mid-scope (an assertion before the
    context exits) would leak the key. Because ``_held_by`` is process-global, a
    later test on the same xdist worker would then see a spurious reentrancy
    bypass. This guard surfaces such a leak at the offending test's boundary and
    clears the dict so the leak can't cascade into unrelated tests.
    """
    from moneybin.db_lock import lock as lock_module

    yield
    leaked = dict(lock_module._held_by)  # type: ignore[reportPrivateUsage]  # test-only leak guard
    lock_module._held_by.clear()  # type: ignore[reportPrivateUsage]  # test-only leak guard
    assert not leaked, f"test leaked write-lock holder(s) in _held_by: {leaked}"
