"""Integration tests for ``write_lock`` + ``get_database`` composition.

Asserts the lifetime contract added in PR B: the per-profile file lock is
held for the entire lifetime of a write-mode ``Database`` (not merely
during ATTACH), so a second writer cannot slip in between
``get_database()`` returning and ``Database.close()`` running and produce
a raw ``duckdb.IOException`` at the ATTACH layer.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

import moneybin.database as db_module
from moneybin.database import Database, DatabaseLockError, get_database
from moneybin.db_lock.lock import (
    _LOCK_SUFFIX,  # type: ignore[reportPrivateUsage]  # test-only access to the canonical lock-file suffix
)


@pytest.fixture
def configured_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Path, None, None]:
    """Bootstrap an encrypted DB and configure ``get_database()`` to find it.

    Mirrors the ``mock_secret_store`` + settings-injection pattern used by
    other ``test_database/`` modules: a real Database is created once with
    ``no_auto_upgrade=True`` so that subsequent ``get_database()`` calls
    open against an initialized file without paying the migration cost.
    """
    db_path = tmp_path / "integration.duckdb"

    mock_store = MagicMock()
    mock_store.get_key.return_value = "integration-test-key-for-lock-integration"

    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.no_auto_upgrade = True

    monkeypatch.setattr(db_module, "get_settings", lambda: mock_settings)
    monkeypatch.setattr(db_module, "SecretStore", lambda: mock_store)
    # The autouse clean_profile_state fixture resets these between tests, but
    # set them explicitly so this fixture is self-contained.
    monkeypatch.setattr(db_module, "_cached_encryption_key", None)
    monkeypatch.setattr(db_module, "_migration_check_done", set[Path]())
    monkeypatch.setattr(db_module, "_active_write_conn", None)

    with Database(
        db_path,
        read_only=False,
        secret_store=mock_store,
        no_auto_upgrade=True,
    ) as db:
        db.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")

    yield db_path


def test_write_open_acquires_file_lock_then_releases_on_close(
    configured_db: Path,
) -> None:
    """Verify the file lock is held for the Database's lifetime, not just ATTACH.

    Inspects ``_held_by`` between ``get_database()`` and the context exit to
    catch regressions where the lock would release after ATTACH instead of
    after ``Database.close()``.
    """
    import moneybin.db_lock.lock as lock_module

    lock_path = configured_db.parent / (configured_db.name + _LOCK_SUFFIX)
    resolved_path = configured_db.resolve()

    with get_database(read_only=False) as db:
        # Lock still held — metadata present, holder registered.
        metadata = json.loads(lock_path.read_text())
        assert metadata["pid"] == os.getpid()
        assert metadata["operation_type"] == "interactive"
        assert resolved_path in lock_module._held_by  # pyright: ignore[reportPrivateUsage]
        db.execute("INSERT INTO t VALUES (1)")

    # After close, holder is cleared.
    assert resolved_path not in lock_module._held_by  # pyright: ignore[reportPrivateUsage]

    # A second write open must succeed immediately — the first release
    # actually freed the lock.
    with get_database(read_only=False) as db:
        db.execute("INSERT INTO t VALUES (2)")


def test_read_open_does_not_touch_file_lock(configured_db: Path) -> None:
    """Read-mode opens bypass ``write_lock`` entirely.

    No lock file is created and no holder is registered for read-only opens.
    """
    import moneybin.db_lock.lock as lock_module

    lock_path = configured_db.parent / (configured_db.name + _LOCK_SUFFIX)
    resolved_path = configured_db.resolve()
    assert not lock_path.exists()  # baseline

    with get_database(read_only=True) as db:
        assert not lock_path.exists()  # still untouched
        assert resolved_path not in lock_module._held_by  # pyright: ignore[reportPrivateUsage]
        db.execute("SELECT COUNT(*) FROM t").fetchone()

    assert not lock_path.exists()


def test_operation_type_kwarg_recorded_in_lock_metadata(
    configured_db: Path,
) -> None:
    """The ``operation_type`` kwarg is recorded in the lock-file metadata.

    Verifies the value flows from ``get_database()`` through ``write_lock``
    into the holder JSON payload.
    """
    lock_path = configured_db.parent / (configured_db.name + _LOCK_SUFFIX)
    with get_database(read_only=False, operation_type="migration") as db:
        metadata = json.loads(lock_path.read_text())
        assert metadata["operation_type"] == "migration"
        db.execute("INSERT INTO t VALUES (3)")


def test_shared_deadline_caps_total_writer_wait(configured_db: Path) -> None:
    """A single shared deadline drives both file-lock acquire and ATTACH retry.

    Happy-path open finishes well under ``max_wait``. Cross-process contention
    is exercised in Task 11's scenario coverage; this case guards against a
    regression that pushed the wait past the policy ceiling on the
    uncontested path.
    """
    start = time.monotonic()
    with get_database(read_only=False, max_wait=10.0) as db:
        elapsed = time.monotonic() - start
        assert elapsed < 10.0
        db.execute("INSERT INTO t VALUES (4)")


# ---------------------------------------------------------------------------
# Regression tests for code-review findings F4, F6, F10.
# ---------------------------------------------------------------------------


def test_f4_close_closes_conn_before_releasing_lock(configured_db: Path) -> None:
    """F4 regression: ``close()`` tears down the DuckDB conn before the lock.

    Our DuckDB connection holds DuckDB's own OS-level lock on the file until
    it closes. Releasing the process file lock first opens a window where a
    peer acquires the file lock and hits a raw IOException at ATTACH. This
    pins the order by observing that ``self._conn`` is already closed
    (``None``) at the moment the lock-release callable runs. Pre-fix, release
    ran first and would observe a still-open conn.
    """
    observed: dict[str, bool] = {}
    with get_database(read_only=False) as db:
        original_release = db._lock_release  # pyright: ignore[reportPrivateUsage]

        def recording_release() -> None:
            observed["conn_closed_at_release"] = db._conn is None  # pyright: ignore[reportPrivateUsage]
            if original_release is not None:
                original_release()

        db._lock_release = recording_release  # pyright: ignore[reportPrivateUsage]
        db.execute("INSERT INTO t VALUES (1)")

    assert observed["conn_closed_at_release"] is True


def test_f6_failure_after_bind_closes_db_not_just_lock(
    configured_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F6 regression: a post-bind ``get_database`` failure closes the conn too.

    If bookkeeping after the Database is bound raises (here: a simulated
    failure in ``_migration_check_done.add``), the ``except`` handler must
    close the bound Database — tearing down its DuckDB connection — not merely
    release the lock stack. Pre-fix it called ``stack.close()`` alone, leaking
    the open connection.
    """
    import moneybin.db_lock.lock as lock_module

    captured: list[Database] = []
    real_open = db_module._open_with_attach_retry  # pyright: ignore[reportPrivateUsage]

    def capturing_open(**kwargs: object) -> Database:
        db = real_open(**kwargs)  # type: ignore[arg-type]
        captured.append(db)
        return db

    class _RaisingSet:
        def __contains__(self, item: object) -> bool:
            return False

        def add(self, item: object) -> None:
            raise RuntimeError("boom during post-bind bookkeeping")

    monkeypatch.setattr(db_module, "_open_with_attach_retry", capturing_open)
    monkeypatch.setattr(db_module, "_migration_check_done", _RaisingSet())

    with pytest.raises(RuntimeError, match="boom during post-bind bookkeeping"):
        get_database(read_only=False)

    assert captured, "Database was never bound — test setup is wrong"
    bound = captured[0]
    # The bound Database was closed: conn torn down AND lock released.
    assert bound._conn is None  # pyright: ignore[reportPrivateUsage]
    assert bound._closed is True  # pyright: ignore[reportPrivateUsage]
    assert configured_db.resolve() not in lock_module._held_by  # pyright: ignore[reportPrivateUsage]


def test_f10_write_init_failure_rolls_back_conn(
    configured_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F10 regression: a write ``__init__`` failure after conn-open closes it.

    ``Database.__init__`` opens ``self._conn`` then runs attach, schema init,
    migrations, ``checkpoint``, and ``refresh_views`` — any can raise, and on
    failure no Database is returned for anyone to close. The ExitStack guard
    must close the orphaned conn. Triggered here via ``refresh_views`` (same
    guarded region as the ``checkpoint`` call the finding named).
    """
    captured: list[Database] = []

    def boom_refresh(db: Database) -> None:
        captured.append(db)
        raise RuntimeError("refresh boom")

    monkeypatch.setattr("moneybin.seeds.refresh_views", boom_refresh)

    with pytest.raises(RuntimeError, match="refresh boom"):
        get_database(read_only=False)

    assert captured, "refresh_views was never reached — test setup is wrong"
    conn = captured[0]._conn  # pyright: ignore[reportPrivateUsage]
    assert conn is not None
    with pytest.raises(duckdb.Error):
        conn.execute("SELECT 1")  # closed connection rejects use


def test_f10_read_init_failure_rolls_back_conn(
    configured_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F10 regression: a read ``__init__`` failure after conn-open closes it.

    Mirror of the write-path guard for the read-only branch: if
    ``_attach_encrypted`` raises after ``duckdb.connect()``, the ExitStack
    guard closes the orphaned conn.
    """
    captured: list[duckdb.DuckDBPyConnection] = []
    real_connect = duckdb.connect

    def capturing_connect(*args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        conn = real_connect(*args, **kwargs)  # type: ignore[arg-type]
        captured.append(conn)
        return conn

    def boom_attach(conn: object, sql: str) -> None:
        raise RuntimeError("attach boom")

    monkeypatch.setattr(db_module.duckdb, "connect", capturing_connect)
    monkeypatch.setattr(db_module, "_attach_encrypted", boom_attach)

    with pytest.raises(RuntimeError, match="attach boom"):
        get_database(read_only=True)

    assert captured, "duckdb.connect was never called — test setup is wrong"
    with pytest.raises(duckdb.Error):
        captured[0].execute("SELECT 1")  # closed connection rejects use


def test_write_open_creates_missing_profile_dir_before_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write open creates the leaf profile dir before write_lock runs.

    Regression: write_lock places its lock file inside the profile directory
    and runs before Database.__init__'s mkdir. On a fresh profile (e.g. the
    first write from `synthetic generate`), the directory did not exist yet
    and os.open(<db_path>.write.lock, O_CREAT) raised FileNotFoundError.
    get_database must create the leaf profile directory first, restoring the
    pre-PR-B behaviour where Database.__init__ was the first filesystem touch.
    """
    profiles_root = tmp_path / "profiles"
    profiles_root.mkdir()  # the profile *root* exists (per ProfileService)...
    profile_dir = profiles_root / "freshprofile"  # ...but this leaf does not
    db_path = profile_dir / "moneybin.duckdb"
    assert not profile_dir.exists()

    mock_store = MagicMock()
    mock_store.get_key.return_value = "fresh-profile-dir-regression-key"
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.no_auto_upgrade = True

    monkeypatch.setattr(db_module, "get_settings", lambda: mock_settings)
    monkeypatch.setattr(db_module, "SecretStore", lambda: mock_store)
    monkeypatch.setattr(db_module, "_cached_encryption_key", None)
    monkeypatch.setattr(db_module, "_migration_check_done", set[Path]())
    monkeypatch.setattr(db_module, "_active_write_conn", None)

    with get_database(read_only=False) as db:
        db.execute("CREATE TABLE t (x INTEGER)")
        db.execute("INSERT INTO t VALUES (1)")

    assert profile_dir.is_dir()
    assert db_path.exists()


@pytest.mark.integration
def test_concurrent_write_opens_serialize_across_threads(configured_db: Path) -> None:
    """Two in-process threads opening get_database(write) serialize at the lock.

    The primary in-process concurrency case (concurrent MCP write tools running
    on separate threads): while one thread holds the write lock, a second
    thread's write open must block at write_lock — under a short max_wait it
    times out rather than acquiring concurrently. write_lock contends per
    open-file-description, so different threads do not share a reentrancy entry;
    this exercises that end to end through get_database (not just the primitive,
    which tests/moneybin/test_db_lock/test_lock.py covers directly).
    """
    a_holding = threading.Event()
    a_release = threading.Event()
    b_outcome: dict[str, str] = {}

    def thread_a() -> None:
        with get_database(read_only=False) as db:
            db.execute("INSERT INTO t VALUES (1)")
            a_holding.set()
            a_release.wait(timeout=10.0)

    def thread_b() -> None:
        # main asserts a_holding before starting this thread, so A already holds.
        try:
            with get_database(read_only=False, max_wait=0.5):
                b_outcome["result"] = "acquired"
        except DatabaseLockError:
            b_outcome["result"] = "blocked"
        except Exception as exc:  # noqa: BLE001 — surface unexpected errors for diagnosis
            b_outcome["result"] = f"error:{type(exc).__name__}"

    ta = threading.Thread(target=thread_a)
    ta.start()
    try:
        assert a_holding.wait(timeout=5.0), "thread A never acquired the write lock"
        tb = threading.Thread(target=thread_b)
        tb.start()
        # B attempts while A still holds; with max_wait=0.5 s (well under A's
        # hold), B must block at write_lock and time out — proving it could not
        # acquire concurrently.
        tb.join(timeout=10.0)
        assert not tb.is_alive(), "thread B did not finish in time"
        assert b_outcome.get("result") == "blocked", (
            f"second writer was not serialized behind the first: {b_outcome}"
        )
    finally:
        a_release.set()
        ta.join(timeout=5.0)
