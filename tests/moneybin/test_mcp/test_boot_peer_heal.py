"""check_schema_at_boot when another server on the profile holds the lock."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import (
    DatabaseLockError,
    check_core_schema_drift,
    get_database,
)
from moneybin.db_lock import OperationType, write_lock
from moneybin.mcp import server
from moneybin.services.transform_service import ApplyResult, TransformService


@contextmanager
def _peer_holding_lock(
    db_path: Path,
    operation_type: OperationType,
    *,
    hold_seconds: float,
    before_release: Callable[[], None] | None = None,
) -> Generator[None, None, None]:
    """Hold the real write lock from another thread, like a peer server would.

    flock contends per open file description, so a lock held on another
    thread's fd is seen by this thread's ``live_writer`` probe.
    """
    acquired = threading.Event()

    def peer() -> None:
        with write_lock(
            db_path, deadline=time.monotonic() + 5.0, operation_type=operation_type
        ):
            acquired.set()
            time.sleep(hold_seconds)
            if before_release is not None:
                before_release()

    t = threading.Thread(target=peer)
    t.start()
    try:
        assert acquired.wait(timeout=2.0), "peer never acquired the lock"
        yield
    finally:
        t.join(timeout=10.0)


def _drop_display_name() -> None:
    with get_database(read_only=False) as db:
        db.execute("ALTER TABLE core.dim_accounts DROP COLUMN display_name")


def _restore_display_name() -> None:
    # Runs on the peer thread inside its held lock; write_lock is reentrant per
    # thread, so this open joins the peer's lock rather than contending with it.
    with get_database(read_only=False) as db:
        db.execute("ALTER TABLE core.dim_accounts ADD COLUMN display_name VARCHAR")


def _fail_apply(svc: TransformService) -> ApplyResult:
    raise AssertionError("boot ran its own heal after a peer's heal fixed drift")


@pytest.mark.unit
def test_boot_waits_for_peer_heal_and_skips_its_own(
    mcp_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A peer's in-progress heal is waited out, and its result is reused."""
    _drop_display_name()
    monkeypatch.setattr(TransformService, "apply", _fail_apply)

    with _peer_holding_lock(
        mcp_db,
        "transform_apply",
        hold_seconds=1.0,
        before_release=_restore_display_name,
    ):
        started = time.monotonic()
        server.check_schema_at_boot()
        waited = time.monotonic() - started

    assert waited >= 0.5  # blocked on the peer rather than racing it
    with get_database(read_only=True) as db:
        assert not check_core_schema_drift(db)


@pytest.mark.unit
def test_boot_gives_up_when_peer_heal_outlasts_the_wait(
    mcp_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server, "_PEER_HEAL_WAIT_SECONDS", 0.3)
    monkeypatch.setattr(server, "_PEER_HEAL_POLL_SECONDS", 0.05)

    with _peer_holding_lock(mcp_db, "transform_apply", hold_seconds=1.5):
        with pytest.raises(DatabaseLockError, match="still updating the database"):
            server.check_schema_at_boot()


@pytest.mark.unit
def test_boot_retries_when_a_peer_heal_blocked_its_open(
    mcp_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A peer heal that starts after the pre-open wait still gets waited out.

    Both servers can pass the pre-open wait before either starts healing; the
    loser's open then times out while the winner heals. That lock error must
    loop back to the wait, not fail boot.
    """
    check = MagicMock(side_effect=[DatabaseLockError("blocked"), None])
    monkeypatch.setattr(server, "_check_and_heal_schema", check)
    # Stub the pre-open wait so the first open runs while the peer still heals.
    wait = MagicMock(return_value=True)
    monkeypatch.setattr(server, "_wait_out_peer_heal", wait)

    with _peer_holding_lock(mcp_db, "transform_apply", hold_seconds=0.5):
        server.check_schema_at_boot()

    assert check.call_count == 2
    assert wait.call_count == 2


@pytest.mark.unit
def test_boot_fails_fast_when_holder_is_not_a_heal(
    mcp_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stuck non-heal writer surfaces the ordinary lock error, not a wait."""
    check = MagicMock(side_effect=DatabaseLockError("held by a write"))
    monkeypatch.setattr(server, "_check_and_heal_schema", check)

    with _peer_holding_lock(mcp_db, "interactive", hold_seconds=0.5):
        with pytest.raises(DatabaseLockError, match="held by a write"):
            server.check_schema_at_boot()

    assert check.call_count == 1
