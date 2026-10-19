"""Restore helpers wait for slow file operations but still detect lost processes."""

import hashlib
import textwrap
from pathlib import Path

import pytest

from moneybin.services import database_restore_lock as restore_lock


def _delayed_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, delayed_call: str
) -> None:
    original = str(Path(restore_lock.__file__).resolve())
    wrapper = tmp_path / "delayed_restore_helper.py"
    wrapper.write_text(
        textwrap.dedent(
            f"""
            import hashlib
            import os
            import runpy
            import time

            original = {delayed_call}
            def delayed(*args, **kwargs):
                time.sleep(0.15)
                return original(*args, **kwargs)

            {delayed_call} = delayed
            runpy.run_path({original!r}, run_name="__main__")
            """
        )
    )
    monkeypatch.setattr(restore_lock, "__file__", str(wrapper))


@pytest.mark.parametrize("operation", ["startup", "snapshot", "publish", "promote"])
def test_lease_waits_for_slow_file_operations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    path = tmp_path / "source.duckdb"
    original = b"synthetic original bytes"
    path.write_bytes(original)
    _delayed_helper(
        tmp_path,
        monkeypatch,
        "os.pread" if operation == "startup" else "os.fsync",
    )

    if operation == "startup":
        monkeypatch.setattr(restore_lock, "_LEASE_REPLY_SECONDS", 0.03, raising=False)
    context = (
        restore_lock._source_lease(path, shared=True)  # pyright: ignore[reportPrivateUsage]  # isolate the snapshot request from startup hashing
        if operation == "snapshot"
        else restore_lock.exclusive_source(path)
    )
    with context as lease:
        # Shorten the old reply deadline only after startup has acknowledged.
        monkeypatch.setattr(restore_lock, "_LEASE_REPLY_SECONDS", 0.03, raising=False)
        assert lease.fingerprint == hashlib.sha256(original).hexdigest()
        if operation == "snapshot":
            snapshot = tmp_path / "snapshot.duckdb"
            lease.snapshot(snapshot)
            assert snapshot.read_bytes() == original
            lease.check()
        elif operation == "publish":
            published = tmp_path / "published.duckdb"
            lease.publish(published)
            assert published.read_bytes() == original
            assert not path.exists()
        elif operation == "promote":
            candidate = tmp_path / "candidate.duckdb"
            replacement = b"synthetic replacement bytes"
            candidate.write_bytes(replacement)
            lease.promote(
                candidate, fingerprint=hashlib.sha256(replacement).hexdigest()
            )
            assert path.read_bytes() == replacement
            assert not candidate.exists()
        else:
            lease.check()


def test_fingerprint_waits_for_slow_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.duckdb"
    content = b"synthetic fingerprint bytes"
    path.write_bytes(content)
    _delayed_helper(tmp_path, monkeypatch, "hashlib.file_digest")
    monkeypatch.setattr(restore_lock, "_LEASE_REPLY_SECONDS", 0.03, raising=False)
    assert restore_lock.fingerprint_file(path) == hashlib.sha256(content).hexdigest()


def test_lease_detects_helper_eof(tmp_path: Path) -> None:
    path = tmp_path / "source.duckdb"
    path.write_bytes(b"synthetic source bytes")
    with restore_lock.exclusive_source(path) as lease:
        lease.process.kill()
        lease.process.wait(timeout=5)
        with pytest.raises(ValueError, match="lease_lost"):
            restore_lock._read_reply(lease.process)  # pyright: ignore[reportPrivateUsage]  # exercise EOF after initial acknowledgement


def test_lease_shutdown_kills_unresponsive_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wrapper = tmp_path / "unresponsive_restore_helper.py"
    wrapper.write_text(
        "import json, time\n"
        "print(json.dumps({'ok': True, 'identity': [1, 2], "
        "'fingerprint': 'synthetic'}), flush=True)\n"
        "time.sleep(60)\n"
    )
    monkeypatch.setattr(restore_lock, "__file__", str(wrapper))
    monkeypatch.setattr(restore_lock, "_HELPER_SHUTDOWN_SECONDS", 0.1, raising=False)
    with restore_lock.exclusive_source(tmp_path / "source.duckdb") as lease:
        monkeypatch.setattr(restore_lock, "_LEASE_REPLY_SECONDS", 0.1, raising=False)
        process = lease.process
    assert process.returncode is not None and process.returncode < 0
    assert process.stdout is not None and process.stdout.closed
