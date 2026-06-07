"""Multi-process scenarios for the write critical-section lock + ATTACH fix.

Each scenario spawns one or more subprocess workers (under
``tests/scenarios/_lock_workers/``) against a real encrypted DuckDB file. The
assertion contract: success paths return expected data; failure paths return a
MoneyBin-classified ``DatabaseLockError`` envelope. The trust-boundary
guarantee these scenarios lock in is that cross-process contention NEVER
leaks as a raw ``duckdb.IOException`` / ``Could not set lock on file`` string
into stderr — every contention path produces a classified envelope.

Workers are stand-alone scripts (no test-helper imports) so they accurately
model what an unrelated subprocess does. The test process's autouse fixtures
in ``conftest.py`` apply only to itself; the subprocess receives the
encryption key, fast-Argon2 params, and PYTHONPATH explicitly via ``env=...``.
"""

from __future__ import annotations

import json
import os
import subprocess  # noqa: S404  # explicit command list, never shell=True
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

import pytest

from tests.e2e.conftest import FAST_ARGON2_ENV

pytestmark = pytest.mark.scenarios


_WORKERS_DIR = Path(__file__).parent / "_lock_workers"
_BOOTSTRAP = _WORKERS_DIR / "bootstrap.py"
_READER = _WORKERS_DIR / "reader.py"
_HOLDER = _WORKERS_DIR / "holder.py"
_CONTENDER = _WORKERS_DIR / "contender.py"
_VERIFY = _WORKERS_DIR / "verify.py"
_WRITE_CHECKPOINT = _WORKERS_DIR / "write_then_checkpoint.py"

# Marker timeouts. These are wall-clock budgets for "sync marker should arrive";
# the actual workload sleeps are much shorter (1-3 s). Generous enough to
# tolerate slow CI without masking a hung subprocess.
_MARKER_TIMEOUT = 15.0
_FINAL_WAIT_TIMEOUT = 20.0


def _worker_env() -> dict[str, str]:
    """Subprocess env: real env + encryption key + fast-Argon2 + memory keyring."""
    return {
        **os.environ,
        **FAST_ARGON2_ENV,
        "MONEYBIN_DATABASE__ENCRYPTION_KEY": "scenario-ephemeral-key-tmpdir-only",
    }


def _spawn(worker: Path, *args: str) -> subprocess.Popen[str]:
    """Spawn a worker subprocess with stdout/stderr captured as text."""
    cmd = [sys.executable, str(worker), *args]
    return subprocess.Popen(  # noqa: S603  # controlled test script invocation
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_worker_env(),
    )


def _wait_for_marker(
    proc: subprocess.Popen[str], marker: str, timeout: float = _MARKER_TIMEOUT
) -> None:
    """Block until ``marker`` appears on stdout or the worker dies.

    Reads line-by-line via ``proc.stdout.readline()``. The plan caller is
    responsible for finishing the read with ``communicate()`` so all
    remaining output is drained.
    """
    import time

    assert proc.stdout is not None
    stdout: IO[str] = proc.stdout
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = stdout.readline()
        if not line:
            # readline() returns "" on EOF — worker exited before emitting marker.
            stderr_text = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(
                f"worker exited before emitting {marker!r}: stderr={stderr_text!r}"
            )
        if marker in line:
            return
        if proc.poll() is not None:
            stderr_text = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(
                f"worker died before emitting {marker!r}: stderr={stderr_text!r}"
            )
    raise RuntimeError(f"timeout waiting for {marker!r} after {timeout}s")


def _drain(
    proc: subprocess.Popen[str], timeout: float = _FINAL_WAIT_TIMEOUT
) -> tuple[str, str]:
    """Run ``communicate(timeout=...)`` and return (stdout, stderr)."""
    return proc.communicate(timeout=timeout)


@pytest.fixture
def bootstrapped_db(tmp_path: Path) -> Iterator[Path]:
    """Initialize an encrypted DB once per test with a tiny ``t (x INTEGER)`` table.

    Runs the bootstrap worker in a subprocess so the test process's DuckDB
    module state never touches the file — every scenario test then opens it
    cleanly via its own subprocess workers.
    """
    db_path = tmp_path / "scenario.duckdb"
    proc = _spawn(_BOOTSTRAP, str(db_path))
    out, err = _drain(proc, timeout=30.0)
    assert proc.returncode == 0, f"bootstrap failed: stdout={out!r} stderr={err!r}"
    yield db_path


# ---------------------------------------------------------------------------
# Scenario 1 — Two readers concurrent (no writer)
#
# Verifies the baseline DuckDB invariant from ADR-010: read-only attaches
# coexist across processes. If this fails, every other contention scenario
# is meaningless — the read-read floor is broken.
# ---------------------------------------------------------------------------


def test_scenario_1_two_readers_coexist(bootstrapped_db: Path) -> None:
    """Two read-only opens run concurrently against the same encrypted file."""
    # Both readers hold for 2 s. If DuckDB's read-read coexistence is intact,
    # both run to completion and each sees the single bootstrap row.
    r1 = _spawn(_READER, str(bootstrapped_db), "2.0")
    r2 = _spawn(_READER, str(bootstrapped_db), "2.0")
    try:
        out1, err1 = _drain(r1)
        out2, err2 = _drain(r2)
    finally:
        for p in (r1, r2):
            if p.poll() is None:
                p.kill()
                p.wait(timeout=5.0)

    assert r1.returncode == 0, f"reader1 failed: stdout={out1!r} stderr={err1!r}"
    assert r2.returncode == 0, f"reader2 failed: stdout={out2!r} stderr={err2!r}"
    assert "READER_CLOSED:1" in out1
    assert "READER_CLOSED:1" in out2
    # Trust-boundary assertion: no raw DuckDB IOException leaked.
    assert "Could not set lock" not in err1
    assert "Could not set lock" not in err2


# ---------------------------------------------------------------------------
# Scenario 2 — Reader holds; writer attempts mid-read; writer succeeds after
#              the reader closes.
#
# Verifies the Plan Task 1 bug fix: the writer's ATTACH retry now classifies
# DuckDB 1.5.3's "Could not set lock on file" string as DatabaseLockError and
# retries cleanly, rather than leaking it as a raw IOException.
# ---------------------------------------------------------------------------


def test_scenario_2_reader_blocks_writer_writer_succeeds_after_release(
    bootstrapped_db: Path,
) -> None:
    """Reader holds 1 s; writer attempts; writer succeeds after reader releases."""
    reader = _spawn(_READER, str(bootstrapped_db), "1.0")
    try:
        _wait_for_marker(reader, "READER_OPEN")
        # Launch the writer while the reader still holds. Writer's ATTACH must
        # fail-then-retry under the bug fix; without the fix the IOException
        # propagates as-is.
        writer = _spawn(_HOLDER, str(bootstrapped_db), "0.1")
        try:
            writer_out, writer_err = _drain(writer)
            reader_out, reader_err = _drain(reader)
        finally:
            if writer.poll() is None:
                writer.kill()
                writer.wait(timeout=5.0)
    finally:
        if reader.poll() is None:
            reader.kill()
            reader.wait(timeout=5.0)

    assert writer.returncode == 0, (
        f"writer failed: stdout={writer_out!r} stderr={writer_err!r}"
    )
    assert "HOLDER_CLOSED" in writer_out
    assert reader.returncode == 0, (
        f"reader failed: stdout={reader_out!r} stderr={reader_err!r}"
    )
    # Critical: writer's ATTACH retry must have caught and classified the
    # DuckDB lock error rather than letting it surface raw.
    assert "Could not set lock" not in writer_err
    assert "duckdb.IOException" not in writer_err


# ---------------------------------------------------------------------------
# Scenario 3 — Writer holds; reader attempts mid-write; reader succeeds after
#              the writer closes.
#
# Mirror of scenario 2 with roles reversed — verifies that read-mode opens
# also retry on DatabaseLockError (a read open can fail when another process
# holds the write attach).
# ---------------------------------------------------------------------------


def test_scenario_3_writer_blocks_reader_reader_succeeds_after_release(
    bootstrapped_db: Path,
) -> None:
    """Writer holds 1 s; reader attempts; reader succeeds after writer releases."""
    writer = _spawn(_HOLDER, str(bootstrapped_db), "1.0")
    try:
        _wait_for_marker(writer, "HOLDER_OPEN")
        # Launch the reader while the writer still holds. Reader must retry
        # at the ATTACH layer until the writer closes.
        reader = _spawn(_READER, str(bootstrapped_db), "0.0")
        try:
            reader_out, reader_err = _drain(reader)
            writer_out, writer_err = _drain(writer)
        finally:
            if reader.poll() is None:
                reader.kill()
                reader.wait(timeout=5.0)
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.wait(timeout=5.0)

    assert reader.returncode == 0, (
        f"reader failed: stdout={reader_out!r} stderr={reader_err!r}"
    )
    # The writer's INSERT bumps the row count from 1 to 2, so the reader
    # observes 2 once it acquires (after the writer commits + closes).
    assert "READER_CLOSED:2" in reader_out
    assert writer.returncode == 0, (
        f"writer failed: stdout={writer_out!r} stderr={writer_err!r}"
    )
    assert "Could not set lock" not in reader_err


# ---------------------------------------------------------------------------
# Scenario 4 — Two writers contend; the file lock serializes them; both
#              succeed.
#
# Without the MoneyBin-owned file lock, two concurrent writers would race at
# the DuckDB ATTACH layer and one would surface a raw IOException. With the
# write_lock primitive, the second writer waits at the file lock, observes
# the first's holder metadata, then proceeds once the first releases.
# ---------------------------------------------------------------------------


def test_scenario_4_two_writers_serialize_via_file_lock(
    bootstrapped_db: Path,
) -> None:
    """Two write-mode opens serialize through the per-profile file lock."""
    # Writer A holds for 1.5 s. Writer B is launched while A still holds —
    # B must wait at the file lock, NOT at the DuckDB ATTACH layer.
    a = _spawn(_HOLDER, str(bootstrapped_db), "1.5")
    try:
        _wait_for_marker(a, "HOLDER_OPEN")
        b = _spawn(_HOLDER, str(bootstrapped_db), "0.1")
        try:
            a_out, a_err = _drain(a)
            b_out, b_err = _drain(b)
        finally:
            if b.poll() is None:
                b.kill()
                b.wait(timeout=5.0)
    finally:
        if a.poll() is None:
            a.kill()
            a.wait(timeout=5.0)

    assert a.returncode == 0, f"writer A failed: stdout={a_out!r} stderr={a_err!r}"
    assert b.returncode == 0, f"writer B failed: stdout={b_out!r} stderr={b_err!r}"
    assert "HOLDER_CLOSED" in a_out
    assert "HOLDER_CLOSED" in b_out
    # File-lock serialization fired correctly — no DuckDB-layer contention
    # leaked because the file lock kept the writers from racing at ATTACH.
    assert "Could not set lock" not in a_err
    assert "Could not set lock" not in b_err


# ---------------------------------------------------------------------------
# Scenario 5 — Writer holds past the contender's max_wait; the contender
#              raises a classified DatabaseLockError envelope, never a raw
#              IOException.
#
# Verifies the 10-second policy ceiling fires cleanly: classify_user_error()
# routes the DatabaseLockError into a UserError carrying the system_status
# recovery action.
# ---------------------------------------------------------------------------


def test_scenario_5_writer_timeout_produces_envelope_not_raw_ioexception(
    bootstrapped_db: Path,
) -> None:
    """Writer holds past contender's max_wait → DatabaseLockError envelope."""
    # Holder sleeps 3 s; contender's max_wait is 1 s, so the file-lock wait
    # is guaranteed to time out before the holder releases.
    holder = _spawn(_HOLDER, str(bootstrapped_db), "3.0")
    try:
        _wait_for_marker(holder, "HOLDER_OPEN")
        contender = _spawn(_CONTENDER, str(bootstrapped_db), "1.0")
        try:
            c_out, c_err = _drain(contender)
            h_out, h_err = _drain(holder)
        finally:
            if contender.poll() is None:
                contender.kill()
                contender.wait(timeout=5.0)
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=5.0)

    assert contender.returncode == 0, (
        f"contender failed: stdout={c_out!r} stderr={c_err!r}"
    )
    assert "CONTENDER_TIMEOUT:" in c_out, (
        f"contender did not emit timeout envelope: stdout={c_out!r}"
    )
    # Trust-boundary assertion: the raw DuckDB error string must not appear
    # in stderr — the classifier should have caught it.
    assert "Could not set lock" not in c_err
    assert "duckdb.IOException" not in c_err

    # Parse the envelope and verify the recovery action contract.
    payload_start = c_out.find("CONTENDER_TIMEOUT:") + len("CONTENDER_TIMEOUT:")
    payload_line = c_out[payload_start:].splitlines()[0]
    payload: dict[str, Any] = json.loads(payload_line)
    assert "message" in payload
    actions: list[dict[str, Any]] = payload.get("recovery_actions") or []
    assert len(actions) >= 1, f"envelope missing recovery_actions: {payload!r}"
    first = actions[0]
    assert first["tool"] == "system_status"
    assert first["arguments"] == {}
    assert first["confidence"] == "suggested"
    assert first["idempotent"] is True

    # Holder eventually finishes its own write cycle.
    assert holder.returncode == 0, f"holder failed: stdout={h_out!r} stderr={h_err!r}"
    assert "HOLDER_CLOSED" in h_out


# ---------------------------------------------------------------------------
# Scenario 6 — Write + Database.checkpoint("post_migration") + close; reopen
#              read-only sees the row.
#
# Verifies the workstream #2 Database.checkpoint helper: the named durable
# boundary forces a flush, so a separate subprocess opening read-only after
# the writer exits observes the new row. If the checkpoint were silently
# skipped, an unclean reopen could miss the row.
# ---------------------------------------------------------------------------


def test_scenario_6_checkpoint_after_write_persists_across_reopen(
    bootstrapped_db: Path,
) -> None:
    """A write + checkpoint + close is observable by a fresh subprocess reopen."""
    # The write_then_checkpoint worker inserts x=6, then checkpoints with
    # reason="post_migration", then closes.
    write = _spawn(_WRITE_CHECKPOINT, str(bootstrapped_db), "post_migration")
    try:
        w_out, w_err = _drain(write)
    finally:
        if write.poll() is None:
            write.kill()
            write.wait(timeout=5.0)
    assert write.returncode == 0, f"write failed: stdout={w_out!r} stderr={w_err!r}"
    assert "WRITE_CHECKPOINT_DONE" in w_out

    # Reopen in a fresh subprocess. The reader sees both the bootstrap row
    # (x=1) and the post-checkpoint row (x=6) — the durable boundary forced
    # the new row to disk before close, so the reopen is guaranteed visible.
    verify = _spawn(_VERIFY, str(bootstrapped_db))
    try:
        v_out, v_err = _drain(verify)
    finally:
        if verify.poll() is None:
            verify.kill()
            verify.wait(timeout=5.0)
    assert verify.returncode == 0, f"verify failed: stdout={v_out!r} stderr={v_err!r}"
    assert "VERIFY:1,6" in v_out, f"verify output missing expected rows: {v_out!r}"
