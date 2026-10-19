"""Profile-bound native scheduling and short, durable attempt claims."""

import fcntl
import hashlib
import logging
import os
import re
import stat
import subprocess  # noqa: S404  # fixed argv, no shell, native supervised child
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from moneybin.config import get_sync_schedule_dir
from moneybin.errors import UserError
from moneybin.services.sync_schedule_native import (
    NativeStatus,
    ScheduleDefinition,
    atomic_write,
    clean_environment,
    resolve_schedule_launcher,
    schedule_error,
)
from moneybin.utils.user_config import normalize_profile_name

logger = logging.getLogger(__name__)
_MAX_METADATA_BYTES = 16_384


class _Adapter(Protocol):
    @property
    def mechanism(self) -> Literal["launchd", "systemd"]: ...

    def validate_session(self) -> None: ...
    def inspect(self, job: ScheduleDefinition) -> NativeStatus: ...
    def install(self, job: ScheduleDefinition) -> None: ...
    def remove(self, job: ScheduleDefinition) -> None: ...


class ScheduledAttempt(BaseModel):
    """One bounded outcome, never a financial receipt or exception archive."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    state: Literal["running", "completed", "failed", "interrupted-or-unknown"]
    started_at: AwareDatetime
    ended_at: AwareDatetime | None = None
    exit_status: int | None = None
    recovery_code: Literal["sync_failed", "interrupted", "launch_failed"] | None = None


class _ScheduleState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    definition: ScheduleDefinition
    last_claimed_occurrence: date | None = None
    latest_attempt: ScheduledAttempt | None = None


@dataclass(frozen=True)
class ScheduleResult:
    """One metadata-only view of configured and live native state."""

    state: Literal[
        "absent", "installed", "inactive-session", "running", "stale", "partial"
    ]
    mechanism: str
    time: str | None
    timezone: str
    nominal_next_run: str | None
    diagnostic_location: str
    latest_attempt: ScheduledAttempt | None = None


def _local_now() -> datetime:
    # astimezone() without a zone returns today's fixed offset, losing DST rules.
    with Path("/etc/localtime").open("rb") as zone_file:
        zone = ZoneInfo.from_file(zone_file)
    return datetime.now(zone)


def _occurrence(day: date, daily_time: str, now: datetime) -> datetime | None:
    hour, minute = map(int, daily_time.split(":"))
    candidate = datetime.combine(day, datetime.min.time(), tzinfo=now.tzinfo).replace(
        hour=hour,
        minute=minute,
        fold=0,
    )
    # A load/login check must not invent a nonexistent local calendar occurrence.
    if datetime.fromtimestamp(candidate.timestamp(), now.tzinfo) != candidate:
        return None
    return candidate


def _latest_occurrence(now: datetime, daily_time: str) -> datetime:
    day = now.date()
    while True:
        candidate = _occurrence(day, daily_time, now)
        if candidate is not None and candidate <= now:
            return candidate
        day -= timedelta(days=1)


class SyncScheduleService:
    """Serialize lifecycle and claims, then let the existing CLI do all sync work."""

    def __init__(
        self,
        *,
        home: Path,
        profile: str,
        adapter: _Adapter,
        launcher: tuple[str, ...] | None = None,
        diagnostic_location: Path | None = None,
        now: Callable[[], datetime] = _local_now,
    ):
        """Bind a normalized profile without opening its database or credentials."""
        self.home = home.expanduser().resolve()
        self.profile = normalize_profile_name(profile)
        self.adapter = adapter
        self.launcher = launcher
        self.diagnostic_location = diagnostic_location
        self.now = now
        binding = f"{self.home}\x00{self.profile}".encode()
        self.job_id = "com.moneybin.sync." + hashlib.sha256(binding).hexdigest()[:24]
        root = get_sync_schedule_dir(self.home)
        self.state_path = root / f"{self.job_id}.json"
        self.lock_path = root / f"{self.job_id}.lock"

    def _identity(self) -> str | None:
        path = self.home / "profiles" / self.profile / "profile_id"
        if not path.is_file() or path.is_symlink() or path.stat().st_size > 64:
            return None
        value = path.read_text().strip()
        return value if re.fullmatch(r"[a-f0-9]{12}", value) else None

    @contextmanager
    def _locked(self) -> Generator[None, None, None]:
        root = self.state_path.parent
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = root.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
        ):
            raise schedule_error("Schedule metadata directory must be owner-only.")
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
            ):
                raise schedule_error("Schedule metadata lock must be owner-only.")
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _read(self) -> _ScheduleState | None:
        if not self.state_path.exists() and not self.state_path.is_symlink():
            return None
        try:
            info = self.state_path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
                or info.st_size > _MAX_METADATA_BYTES
            ):
                raise ValueError("unsafe metadata")
            state = _ScheduleState.model_validate_json(self.state_path.read_bytes())
        except (OSError, ValueError, ValidationError) as exc:
            raise schedule_error(
                "Schedule metadata is invalid or has an unknown format; use a "
                "compatible MoneyBin version to inspect or remove it."
            ) from exc
        job = state.definition
        if (
            job.home != str(self.home)
            or job.profile != self.profile
            or job.job_id != self.job_id
            or job.mechanism != self.adapter.mechanism
        ):
            raise schedule_error(
                "Schedule metadata binding does not match this target."
            )
        return state

    def _write(self, state: _ScheduleState) -> None:
        atomic_write(self.state_path, state.model_dump_json().encode())

    def _result(self, state: _ScheduleState | None) -> ScheduleResult:
        now = self.now()
        diagnostic = self.diagnostic_location or (
            self.home / "profiles" / self.profile / "logs"
        )
        if state is None:
            return ScheduleResult(
                "absent",
                self.adapter.mechanism,
                None,
                now.tzname() or "local",
                None,
                str(diagnostic),
            )
        job = state.definition
        status = self.adapter.inspect(job)
        attempt = state.latest_attempt
        if attempt is not None and attempt.state == "running" and not status.running:
            attempt = attempt.model_copy(
                update={
                    "state": "interrupted-or-unknown",
                    "recovery_code": "interrupted",
                }
            )
        if job.profile_id != self._identity():
            outcome = "stale"
        elif not status.owned or not status.present or status.partial:
            outcome = "partial"
        elif not status.active:
            outcome = "inactive-session"
        elif status.running:
            outcome = "running"
        else:
            outcome = "installed"
        day = now.date()
        candidate = _occurrence(day, job.time, now)
        while candidate is None or candidate <= now:
            day += timedelta(days=1)
            candidate = _occurrence(day, job.time, now)
        return ScheduleResult(
            outcome,
            job.mechanism,
            job.time,
            now.tzname() or "local",
            candidate.isoformat(),
            str(diagnostic),
            attempt,
        )

    def show(self) -> ScheduleResult:
        """Compare persisted ownership against live native state."""
        if not self.state_path.exists() and not self.state_path.is_symlink():
            result = self._result(None)
        else:
            with self._locked():
                result = self._result(self._read())
        self._operation("show", result.state)
        return result

    def set(self, time: str) -> ScheduleResult:
        """Install one validated daily job, preserving an identical interval."""
        if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", time):
            raise schedule_error("Time must be HH:MM in 24-hour local time.")
        identity = self._identity()
        if identity is None:
            raise schedule_error(
                "Scheduling requires an existing configured sync profile; "
                "complete 'moneybin sync login' for this profile first."
            )
        launcher = self.launcher or resolve_schedule_launcher()
        if (
            not launcher
            or not Path(launcher[0]).is_absolute()
            or not os.access(launcher[0], os.X_OK)
        ):
            raise schedule_error(
                "The scheduled executable is unavailable or unresolved."
            )
        self.adapter.validate_session()
        with self._locked():
            previous = self._read()
            if previous is not None:
                old = previous.definition
                if old.profile_id != identity:
                    raise schedule_error(
                        "The schedule is stale; remove it before setting "
                        "a schedule for the recreated profile."
                    )
                status = self.adapter.inspect(old)
                if not status.owned:
                    raise schedule_error("Refusing to change an externally edited job.")
                if (
                    old.time == time
                    and old.launcher == launcher
                    and status.active
                    and not status.partial
                ):
                    self._operation("set", "unchanged")
                    return self._result(previous)
                self.adapter.remove(old)
            definition = ScheduleDefinition(
                home=str(self.home),
                profile=self.profile,
                profile_id=identity,
                job_id=self.job_id,
                revision=uuid.uuid4().hex,
                time=time,
                mechanism=self.adapter.mechanism,
                installed_at=self.now(),
                launcher=launcher,
            )
            # Unknown jobs are never adopted, including collisions with no metadata.
            if not self.adapter.inspect(definition).owned:
                raise schedule_error("Refusing to replace a job that is not owned.")
            state = _ScheduleState(definition=definition)
            try:
                self._write(state)
                self.adapter.install(definition)
                status = self.adapter.inspect(definition)
                if not status.active or not status.owned or status.partial:
                    raise schedule_error("The native schedule could not be verified.")
            except (OSError, RuntimeError, UserError) as exc:
                # Retain metadata even when repair fails, so show/remove can explain
                # the actual partial state and never adopt the leftover definition.
                try:
                    self.adapter.remove(definition)
                    if previous is not None:
                        self._write(previous)
                        self.adapter.install(previous.definition)
                except (OSError, RuntimeError, UserError):
                    logger.warning(
                        "Schedule restoration incomplete; inspect schedule state"
                    )
                self._operation("set", "failed")
                raise schedule_error(
                    "Schedule activation failed; inspect the retained "
                    "metadata and native state before retrying."
                ) from exc
            self._operation("set", "installed")
            return self._result(state)

    def remove(self) -> ScheduleResult:
        """Stop and unregister only the owned definition before deleting state."""
        if not self.state_path.exists() and not self.state_path.is_symlink():
            self._operation("remove", "absent")
            return self._result(None)
        with self._locked():
            state = self._read()
            if state is not None:
                try:
                    self.adapter.remove(state.definition)
                except (OSError, RuntimeError, UserError) as exc:
                    self._operation("remove", "failed")
                    raise schedule_error(
                        "The owned job could not be stopped or removed; "
                        "metadata was retained for a retry."
                    ) from exc
                self.state_path.unlink()
            self._operation("remove", "absent")
            return self._result(None)

    def validate_pull_binding(self, *, profile_id: str, revision: str) -> None:
        """Fence a delayed child against removed schedules or replaced profiles."""
        if not self.state_path.exists():
            raise schedule_error("The scheduled job no longer exists.")
        with self._locked():
            state = self._read()
            if (
                state is None
                or state.definition.profile_id != profile_id
                or state.definition.revision != revision
                or self._identity() != profile_id
            ):
                raise schedule_error("The scheduled profile or revision is stale.")
            status = self.adapter.inspect(state.definition)
            if not status.owned or not status.active or status.partial:
                raise schedule_error("The scheduled job is not active and owned.")

    def run(self, *, revision: str | None = None) -> int:
        """Claim and spawn while locked; wait and record only the same revision."""
        if not self.state_path.exists():
            return 0
        try:
            with self._locked():
                state = self._read()
                if state is None or (
                    revision and revision != state.definition.revision
                ):
                    return 0
                job = state.definition
                if job.profile_id != self._identity():
                    raise schedule_error("The scheduled profile is stale.")
                status = self.adapter.inspect(job)
                if not status.owned or not status.active or status.partial:
                    raise schedule_error("The scheduled job is not active and owned.")
                now = self.now()
                occurrence = None
                if job.mechanism == "launchd":
                    due = _latest_occurrence(now, job.time)
                    if due <= job.installed_at or (
                        state.last_claimed_occurrence is not None
                        and due.date() <= state.last_claimed_occurrence
                    ):
                        return 0
                    occurrence = due.date()
                attempt = ScheduledAttempt(state="running", started_at=now)
                claimed = state.model_copy(
                    update={
                        "last_claimed_occurrence": occurrence,
                        "latest_attempt": attempt,
                    }
                )
                self._write(claimed)
                command = [
                    *job.launcher,
                    "--home",
                    job.home,
                    "--profile",
                    job.profile,
                    "sync",
                    "pull",
                    "--scheduled",
                    "--scheduled-profile-id",
                    job.profile_id,
                    "--scheduled-revision",
                    job.revision,
                    "--output",
                    "json",
                ]
                try:
                    child = subprocess.Popen(  # noqa: S603  # validated absolute launcher; no shell
                        command,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        env=clean_environment(),
                    )
                except OSError:
                    self._write(
                        claimed.model_copy(
                            update={
                                "latest_attempt": attempt.model_copy(
                                    update={
                                        "state": "failed",
                                        "ended_at": self.now(),
                                        "exit_status": 1,
                                        "recovery_code": "launch_failed",
                                    }
                                )
                            }
                        )
                    )
                    self._attempt("failed")
                    return 1
            try:
                status_code = child.wait()
            except KeyboardInterrupt:
                child.terminate()
                child.wait()
                status_code = 130
            outcome = (
                "completed"
                if status_code == 0
                else "interrupted-or-unknown"
                if status_code < 0 or status_code == 130
                else "failed"
            )
            exit_status = 128 - status_code if status_code < 0 else status_code
            receipt = attempt.model_copy(
                update={
                    "state": outcome,
                    "ended_at": self.now(),
                    "exit_status": exit_status,
                    "recovery_code": None
                    if outcome == "completed"
                    else "interrupted"
                    if outcome == "interrupted-or-unknown"
                    else "sync_failed",
                }
            )
            with self._locked():
                current = self._read()
                if current is not None and current.definition.revision == job.revision:
                    self._write(current.model_copy(update={"latest_attempt": receipt}))
            self._attempt(outcome)
            logger.info(f"Scheduled sync attempt {outcome}; exit status {exit_status}")
            return exit_status
        except (OSError, ValueError, UserError):
            self._attempt("refused")
            logger.warning(
                "Scheduled sync refused; inspect schedule metadata and native state"
            )
            return 1

    def _operation(self, operation: str, outcome: str) -> None:
        from moneybin.metrics.registry import SYNC_SCHEDULE_OPERATIONS_TOTAL

        SYNC_SCHEDULE_OPERATIONS_TOTAL.labels(
            operation=operation,
            mechanism=self.adapter.mechanism,
            outcome=outcome,
        ).inc()

    def _attempt(self, outcome: str) -> None:
        from moneybin.metrics.registry import SYNC_SCHEDULE_ATTEMPTS_TOTAL

        SYNC_SCHEDULE_ATTEMPTS_TOTAL.labels(
            mechanism=self.adapter.mechanism,
            outcome=outcome,
        ).inc()
