"""A separate process retains DuckDB-compatible POSIX locks during promotion.

DuckDB 1.5.5 uses process-owned F_SETLK locks. A close of any descriptor for
that inode releases a process's locks, so the copier cannot own this lease.
The helper receives paths and fingerprints only, never encryption keys.
"""

from __future__ import annotations

import hashlib
import json
import os
import select
import subprocess  # noqa: S404  # fixed local stdlib lock holder; no shell or credentials
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO, cast

_LEASE_REPLY_SECONDS = 10


@dataclass
class SourceLease:
    """An exclusive source lease whose helper also performs atomic promotion."""

    process: subprocess.Popen[str]
    fingerprint: str
    identity: tuple[int, int]

    def request(self, payload: dict[str, object]) -> dict[str, object]:
        """Fail closed if the holder exits or cannot acknowledge an operation."""
        return _request(self.process, payload)

    def check(self) -> None:
        """Confirm the source path still names the exclusively held inode."""
        self.request({"operation": "check"})

    def snapshot(self, destination: Path) -> None:
        """Copy encrypted bytes without opening the live inode in the parent."""
        reply = self.request({"operation": "snapshot", "destination": str(destination)})
        if reply.get("fingerprint") != self.fingerprint:
            raise ValueError("stale_preflight")

    def publish(self, destination: Path) -> None:
        """Atomically create an absent destination while retaining the inode lock."""
        self.request({"operation": "publish", "destination": str(destination)})

    def promote(self, candidate: Path, *, fingerprint: str) -> None:
        """Let the lock owner validate both inodes and atomically replace."""
        self.request({
            "operation": "promote",
            "candidate": str(candidate),
            "fingerprint": fingerprint,
            "source_fingerprint": self.fingerprint,
        })


def _read_reply(process: subprocess.Popen[str]) -> dict[str, object]:
    output = process.stdout
    if output is None or not select.select([output], [], [], _LEASE_REPLY_SECONDS)[0]:
        raise ValueError("lease_lost")
    line = output.readline()
    if not line:
        raise ValueError("lease_lost")
    reply: object = json.loads(line)
    if not isinstance(reply, dict):
        raise ValueError("lease_lost")
    reply = cast(dict[str, object], reply)
    if not reply.get("ok"):
        reason = reply.get("reason")
        if reason not in {"active_access", "checkpoint_required", "stale_preflight"}:
            reason = "lease_lost"
        raise ValueError(str(reason))
    return reply


def _request(
    process: subprocess.Popen[str], payload: dict[str, object]
) -> dict[str, object]:
    if process.poll() is not None or process.stdin is None:
        raise ValueError("lease_lost")
    try:
        process.stdin.write(json.dumps(payload) + "\n")
        process.stdin.flush()
    except (OSError, ValueError):
        raise ValueError("lease_lost") from None
    return _read_reply(process)


@contextmanager
def exclusive_source(path: Path) -> Generator[SourceLease, None, None]:
    """Exclude existing and future DuckDB readers/writers without source writes."""
    with _source_lease(path, shared=False) as lease:
        yield lease


@contextmanager
def encrypted_inspection_snapshot(
    path: Path,
) -> Generator[tuple[Path, SourceLease], None, None]:
    """Inspect encrypted scratch while a separate shared lease protects its source."""
    with tempfile.TemporaryDirectory(prefix="moneybin-restore-inspect-") as directory:
        snapshot = Path(directory) / "snapshot.duckdb"
        with _source_lease(path, shared=True) as lease:
            lease.snapshot(snapshot)
            yield snapshot, lease
            lease.check()


@contextmanager
def _source_lease(path: Path, *, shared: bool) -> Generator[SourceLease, None, None]:
    if sys.platform not in {"darwin", "linux"}:
        raise ValueError("unsupported_lock_platform")
    if Path(str(path) + ".wal").exists():
        raise ValueError("checkpoint_required")
    process = subprocess.Popen(  # noqa: S603  # fixed local stdlib helper; path is an argument, never shell text
        [
            sys.executable,
            "-I",
            str(Path(__file__).resolve()),
            str(path.resolve()),
            "inspect" if shared else "exclusive",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        reply = _read_reply(process)
        identity = reply["identity"]
        if not isinstance(identity, list):
            raise ValueError("lease_lost")
        identity = cast(list[object], identity)
        if len(identity) != 2 or not all(isinstance(part, int) for part in identity):
            raise ValueError("lease_lost")
        yield SourceLease(
            process,
            str(reply["fingerprint"]),
            (cast(int, identity[0]), cast(int, identity[1])),
        )
    finally:
        if process.stdin is not None:
            process.stdin.close()
        try:
            process.wait(timeout=_LEASE_REPLY_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()  # Only our own ephemeral lock helper.
            process.wait()
        if process.stdout is not None:
            process.stdout.close()


def fingerprint_file(path: Path) -> str:
    """Hash outside the parent process so closing cannot release its reader locks."""
    try:
        result = subprocess.run(  # noqa: S603  # fixed local stdlib helper; no shell, keys or user code
            [
                sys.executable,
                "-I",
                str(Path(__file__).resolve()),
                str(path.resolve()),
                "fingerprint",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
            timeout=_LEASE_REPLY_SECONDS,
        )
        fingerprint = result.stdout.strip()
        if (
            result.returncode
            or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
        ):
            raise ValueError("fingerprint_failed")
        return fingerprint
    except (OSError, subprocess.SubprocessError):
        raise ValueError("fingerprint_failed") from None


def _digest(fd: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while chunk := os.pread(fd, 1024 * 1024, offset):
        digest.update(chunk)
        offset += len(chunk)
    return digest.hexdigest()


def _held_identity(fd: int, path: Path) -> tuple[int, int]:
    held, current = os.fstat(fd), path.stat()
    if (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino):
        raise ValueError("stale_preflight")
    if Path(str(path) + ".wal").exists():
        raise ValueError("checkpoint_required")
    return held.st_dev, held.st_ino


def _reply(output: TextIO, **values: object) -> None:
    output.write(json.dumps(values) + "\n")
    output.flush()


def _hold(path: Path, *, shared: bool = False) -> None:
    import fcntl

    descriptors: list[int] = []
    try:
        fd = os.open(path, os.O_RDONLY if shared else os.O_RDWR)
        descriptors.append(fd)
        try:
            fcntl.lockf(
                fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB
            )
        except OSError:
            raise ValueError("active_access") from None
        identity = _held_identity(fd, path)
        fingerprint = _digest(fd)
        _reply(sys.stdout, ok=True, fingerprint=fingerprint, identity=identity)
        for line in sys.stdin:
            message = json.loads(line)
            _held_identity(fd, path)
            operation = message.get("operation")
            if operation == "check":
                _reply(sys.stdout, ok=True)
            elif operation == "snapshot" and shared:
                destination = Path(message["destination"])
                copied_digest = hashlib.sha256()
                offset = 0
                # The parent must never open/close the live inode: that would
                # release locks held by its other DuckDB connections.
                with os.fdopen(
                    os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                    "wb",
                ) as target:
                    while chunk := os.pread(fd, 1024 * 1024, offset):
                        target.write(chunk)
                        copied_digest.update(chunk)
                        offset += len(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                _held_identity(fd, path)
                if (
                    copied_digest.hexdigest() != fingerprint
                    or _digest(fd) != fingerprint
                ):
                    raise ValueError("stale_preflight")
                _reply(sys.stdout, ok=True, fingerprint=fingerprint)
            elif operation == "publish" and not shared:
                destination = Path(message["destination"])
                if destination.parent != path.parent or destination == path:
                    raise ValueError("stale_preflight")
                os.link(path, destination)
                os.unlink(path)
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                _reply(sys.stdout, ok=True)
                return
            elif operation == "promote" and not shared:
                candidate = Path(message["candidate"])
                if candidate.parent != path.parent or candidate == path:
                    raise ValueError("stale_preflight")
                new_fd = os.open(candidate, os.O_RDWR)
                descriptors.append(new_fd)
                fcntl.lockf(new_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                _held_identity(new_fd, candidate)
                if (
                    _digest(fd) != message["source_fingerprint"]
                    or _digest(new_fd) != message["fingerprint"]
                ):
                    raise ValueError("stale_preflight")
                os.replace(candidate, path)
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                _reply(sys.stdout, ok=True)
                return
            else:
                raise ValueError("lease_lost")
    except Exception as exc:  # IPC exposes fixed reasons only, never exception details.
        reason = str(exc) if isinstance(exc, ValueError) else "lease_lost"
        _reply(sys.stdout, ok=False, reason=reason)
    finally:
        for fd in descriptors:
            os.close(fd)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[2] == "fingerprint":
        with Path(sys.argv[1]).open("rb") as source_file:
            sys.stdout.write(
                hashlib.file_digest(source_file, "sha256").hexdigest() + "\n"
            )
    else:
        _hold(Path(sys.argv[1]), shared=len(sys.argv) == 3 and sys.argv[2] == "inspect")
