"""Owned scheduling state and durable, coalesced attempt claims."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from unittest.mock import Mock

import pytest

from moneybin.services.sync_schedule import ScheduledAttempt, SyncScheduleService
from moneybin.services.sync_schedule_native import NativeStatus, ScheduleDefinition

pytestmark = pytest.mark.unit


class MemoryAdapter:
    """Native lifecycle fake, storing the whole installed definition."""

    mechanism: Literal["launchd", "systemd"] = "launchd"

    def __init__(self) -> None:
        """Start with no installed job or running process."""
        self.job: ScheduleDefinition | None = None
        self.running = False
        self.active = True
        self.fail_install = False
        self.fail_remove = False
        self.partial = False

    def validate_session(self) -> None:
        if not self.active:
            raise RuntimeError("inactive session")

    def inspect(self, job: ScheduleDefinition) -> NativeStatus:
        from moneybin.services.sync_schedule_native import NativeStatus

        return NativeStatus(
            present=self.job is not None,
            owned=self.job is None
            or self.job.model_dump(mode="json") == job.model_dump(mode="json"),
            active=self.active and self.job is not None,
            running=self.running,
            partial=self.partial,
        )

    def install(self, job: ScheduleDefinition) -> None:
        if self.fail_install:
            raise RuntimeError("activation refused")
        self.job = job
        self.partial = False

    def remove(self, job: ScheduleDefinition) -> None:
        if self.fail_remove:
            raise RuntimeError("stop refused")
        self.job = None
        self.running = False


ScheduleFixture = tuple[SyncScheduleService, MemoryAdapter, Mock]


def _latest(service: SyncScheduleService) -> ScheduledAttempt:
    attempt = service.show().latest_attempt
    assert attempt is not None
    return attempt


@pytest.fixture
def schedule(tmp_path: Path) -> ScheduleFixture:
    from moneybin.services.sync_schedule import SyncScheduleService

    profile = tmp_path / "profiles" / "alice"
    profile.mkdir(parents=True)
    (profile / "profile_id").write_text("0123456789ab")
    clock = Mock(return_value=datetime(2026, 10, 18, 8, tzinfo=UTC))
    adapter = MemoryAdapter()
    service = SyncScheduleService(
        home=tmp_path,
        profile="Alice",
        adapter=adapter,
        launcher=("/usr/bin/true",),
        now=clock,
    )
    return service, adapter, clock


def test_missing_profile_is_not_created(tmp_path: Path) -> None:
    from moneybin.errors import UserError
    from moneybin.services.sync_schedule import SyncScheduleService

    service = SyncScheduleService(
        home=tmp_path,
        profile="absent",
        adapter=MemoryAdapter(),
        launcher=("/usr/bin/true",),
    )
    assert service.show().state == "absent"
    assert service.remove().state == "absent"
    assert service.run() == 0
    with pytest.raises(UserError, match="configured sync profile"):
        service.set("09:00")
    assert not (tmp_path / "profiles").exists()


def test_atomic_owner_only_state_and_stable_lock(schedule: ScheduleFixture) -> None:
    service, adapter, _ = schedule
    result = service.set("09:00")
    assert result.state == "installed"
    assert service.state_path.stat().st_mode & 0o777 == 0o600
    assert service.state_path.parent.stat().st_mode & 0o777 == 0o700
    inode = service.lock_path.stat().st_ino
    first = adapter.job
    assert first is not None
    assert service.set("09:00").state == "installed"
    assert adapter.job == first
    service.set("10:00")
    assert adapter.job is not None
    assert adapter.job.revision != first.revision
    assert service.lock_path.stat().st_ino == inode


def test_identical_set_repairs_owned_active_partial_registration(
    schedule: ScheduleFixture,
) -> None:
    service, adapter, _ = schedule
    service.set("09:00")
    old = adapter.job
    adapter.partial = True
    assert service.show().state == "partial"
    assert service.set("09:00").state == "installed"
    assert adapter.job is not None and old is not None
    assert adapter.job.revision != old.revision


@pytest.mark.parametrize("text", ["{", '{"version":999}', "{}"])
def test_malformed_or_unknown_metadata_refused(
    schedule: ScheduleFixture, text: str
) -> None:
    from moneybin.errors import UserError

    service, _, _ = schedule
    service.set("09:00")
    service.state_path.write_text(text)
    with pytest.raises(UserError, match="metadata"):
        service.set("10:00")
    assert service.run() != 0


def test_recreated_profile_cannot_inherit_authority(schedule: ScheduleFixture) -> None:
    from moneybin.errors import UserError

    service, _, clock = schedule
    service.set("09:00")
    (service.home / "profiles/alice/profile_id").write_text("abcdef012345")
    clock.return_value += timedelta(days=1)
    assert service.show().state == "stale"
    assert service.run() != 0
    with pytest.raises(UserError, match="stale"):
        service.set("09:00")
    assert service.remove().state == "absent"


def test_complete_binding_rejects_copied_metadata(
    schedule: ScheduleFixture, tmp_path: Path
) -> None:
    from moneybin.errors import UserError
    from moneybin.services.sync_schedule import SyncScheduleService

    first, _, _ = schedule
    first.set("09:00")
    other = SyncScheduleService(
        home=tmp_path / "other",
        profile="alice",
        adapter=MemoryAdapter(),
        launcher=("/usr/bin/true",),
    )
    assert first.job_id != other.job_id
    other.state_path.parent.mkdir(parents=True, mode=0o700)
    other.state_path.write_bytes(first.state_path.read_bytes())
    other.state_path.chmod(0o600)
    with pytest.raises(UserError, match="binding"):
        other.show()


def test_failed_claim_write_prevents_spawn(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, clock = schedule
    service.set("09:00")
    clock.return_value += timedelta(hours=2)
    spawn = Mock()
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    monkeypatch.setattr(
        "moneybin.services.sync_schedule.os.replace",
        Mock(side_effect=OSError("disk full")),
    )
    assert service.run() != 0
    spawn.assert_not_called()


def test_due_checks_coalesce_and_consume_failed_attempt(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, adapter, clock = schedule
    service.set("09:00")
    child = Mock(pid=123, wait=Mock(return_value=1))
    spawn = Mock(return_value=child)
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0  # login before the deadline
    clock.return_value += timedelta(days=4, hours=2)
    assert service.run() == 1
    assert service.run() == 0
    spawn.assert_called_once()
    assert _latest(service).state == "failed"
    assert _latest(service).exit_status == 1
    assert adapter.job is not None
    assert spawn.call_args.args[0][-9:] == [
        "sync",
        "pull",
        "--scheduled",
        "--scheduled-profile-id",
        "0123456789ab",
        "--scheduled-revision",
        adapter.job.revision,
        "--output",
        "json",
    ]
    assert spawn.call_args.kwargs["stdin"] == -3
    assert spawn.call_args.kwargs["stdout"] == -3
    assert spawn.call_args.kwargs["stderr"] == -3
    assert not any(k.startswith("MONEYBIN_") for k in spawn.call_args.kwargs["env"])
    adapter.running = False


def test_install_after_deadline_is_not_retroactive(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, clock = schedule
    clock.return_value += timedelta(hours=2)
    service.set("09:00")
    spawn = Mock()
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0
    clock.return_value -= timedelta(hours=1)
    assert service.run() == 0
    spawn.assert_not_called()


def test_unknown_running_receipt_never_becomes_success(
    schedule: ScheduleFixture,
) -> None:
    service, _, _ = schedule
    service.set("09:00")
    data = json.loads(service.state_path.read_text())
    data["latest_attempt"] = {
        "state": "running",
        "started_at": "2026-10-18T09:00:00+00:00",
        "ended_at": None,
        "exit_status": None,
        "recovery_code": None,
    }
    service.state_path.write_text(json.dumps(data))
    assert _latest(service).state == "interrupted-or-unknown"


def test_stop_failure_retains_authority_for_retry(schedule: ScheduleFixture) -> None:
    from moneybin.errors import UserError

    service, adapter, _ = schedule
    service.set("09:00")
    adapter.fail_remove = True
    with pytest.raises(UserError):
        service.remove()
    assert service.state_path.exists()
    assert adapter.job is not None


def test_activation_failure_restores_previous_definition(
    schedule: ScheduleFixture,
) -> None:
    from moneybin.errors import UserError

    service, adapter, _ = schedule
    service.set("09:00")
    previous = adapter.job
    assert previous is not None
    adapter.fail_install = True
    with pytest.raises(UserError):
        service.set("10:00")
    assert (
        json.loads(service.state_path.read_text())["definition"]["revision"]
        == previous.revision
    )


def test_noop_does_not_touch_database_secrets_or_client(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, _ = schedule
    service.set("09:00")

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("metadata path touched financial infrastructure")

    monkeypatch.setattr("moneybin.database.get_database", forbidden)
    monkeypatch.setattr(
        "moneybin.services.sync_server_address.build_sync_client", forbidden
    )
    monkeypatch.setattr(
        "moneybin.utils.user_config.get_or_create_profile_id", forbidden
    )
    monkeypatch.setattr("moneybin.secrets.SecretStore.for_sync", forbidden)
    assert service.run() == 0
    assert service.show().state == "installed"


def test_remove_cannot_finish_between_claim_and_spawn(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    service, adapter, clock = schedule
    service.set("09:00")
    clock.return_value += timedelta(hours=2)
    claimed = Event()
    spawn_allowed = Event()
    spawned = Event()
    removed = Event()
    child_done = Event()
    real_write = service._write  # pyright: ignore[reportPrivateUsage]  # barrier at durable claim boundary

    def write(state: Any) -> None:
        real_write(state)
        if state.latest_attempt is not None and state.latest_attempt.state == "running":
            claimed.set()
            assert spawn_allowed.wait(5)

    def spawn(*args: object, **kwargs: object) -> Mock:
        assert not removed.is_set()
        spawned.set()
        return Mock(wait=lambda: child_done.wait(5) and 0)

    def remove() -> None:
        assert claimed.wait(5)
        service.remove()
        removed.set()
        child_done.set()

    monkeypatch.setattr(service, "_write", write)
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    with ThreadPoolExecutor(max_workers=2) as pool:
        run_future = pool.submit(service.run)
        assert claimed.wait(5)
        remove_future = pool.submit(remove)
        assert not removed.wait(0.05)
        spawn_allowed.set()
        assert run_future.result(5) == 0
        remove_future.result(5)
    assert spawned.is_set() and removed.is_set()
    assert not service.state_path.exists()
    assert adapter.job is None


def test_simultaneous_triggers_claim_once(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    service, _, clock = schedule
    service.set("09:00")
    clock.return_value += timedelta(hours=2)
    spawn = Mock(return_value=Mock(wait=Mock(return_value=0)))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(service.run)
        second = pool.submit(service.run)
        assert [first.result(), second.result()] == [0, 0]
    spawn.assert_called_once()


def test_old_completion_cannot_overwrite_new_revision(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, clock = schedule
    service.set("09:00")
    clock.return_value += timedelta(hours=2)

    def finish() -> int:
        service.set("10:00")
        return 1

    monkeypatch.setattr(
        "moneybin.services.sync_schedule.subprocess.Popen",
        Mock(return_value=Mock(wait=finish)),
    )
    assert service.run() == 1
    assert service.show().time == "10:00"
    assert service.show().latest_attempt is None


def test_post_claim_crash_consumes_occurrence(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, clock = schedule
    service.set("09:00")
    clock.return_value += timedelta(hours=2)
    spawn = Mock(side_effect=SystemExit(99))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    with pytest.raises(SystemExit):
        service.run()
    assert service.run() == 0
    assert _latest(service).state == "interrupted-or-unknown"
    spawn.assert_called_once()


def test_dst_rollback_does_not_repeat_daily_occurrence(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zoneinfo import ZoneInfo

    service, _, clock = schedule
    zone = ZoneInfo("America/New_York")
    clock.return_value = datetime(2026, 10, 31, 8, tzinfo=zone)
    service.set("01:30")
    clock.return_value = datetime(2026, 11, 1, 1, 45, tzinfo=zone, fold=0)
    spawn = Mock(return_value=Mock(wait=Mock(return_value=0)))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0
    clock.return_value = datetime(2026, 11, 1, 1, 45, tzinfo=zone, fold=1)
    assert service.run() == 0
    spawn.assert_called_once()
    assert service.show().nominal_next_run == "2026-11-02T01:30:00-05:00"


def test_dst_rollback_login_catches_up_elapsed_first_fold(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zoneinfo import ZoneInfo

    service, _, clock = schedule
    zone = ZoneInfo("America/New_York")
    clock.return_value = datetime(2026, 10, 31, 8, tzinfo=zone)
    service.set("01:30")
    clock.return_value = datetime(2026, 11, 1, 1, 15, tzinfo=zone, fold=1)
    spawn = Mock(return_value=Mock(wait=Mock(return_value=0)))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0
    spawn.assert_called_once()
    assert json.loads(service.state_path.read_text())["last_claimed_occurrence"] == (
        "2026-11-01"
    )
    clock.return_value = datetime(2026, 11, 1, 1, 45, tzinfo=zone, fold=1)
    assert service.run() == 0
    spawn.assert_called_once()


def test_dst_rollback_nominal_next_run_is_a_future_instant(
    schedule: ScheduleFixture,
) -> None:
    from zoneinfo import ZoneInfo

    service, _, clock = schedule
    zone = ZoneInfo("America/New_York")
    clock.return_value = datetime(2026, 10, 31, 8, tzinfo=zone)
    service.set("01:30")
    clock.return_value = datetime(2026, 11, 1, 1, 15, tzinfo=zone, fold=1)
    assert service.show().nominal_next_run == "2026-11-02T01:30:00-05:00"


@pytest.mark.parametrize("fold, expected_runs", [(0, 1), (1, 0)])
def test_dst_rollback_install_boundary_uses_elapsed_instants(
    schedule: ScheduleFixture,
    monkeypatch: pytest.MonkeyPatch,
    fold: int,
    expected_runs: int,
) -> None:
    from zoneinfo import ZoneInfo

    service, _, clock = schedule
    zone = ZoneInfo("America/New_York")
    clock.return_value = datetime(2026, 11, 1, 1, 10, tzinfo=zone, fold=fold)
    service.set("01:30")
    clock.return_value = datetime(2026, 11, 1, 1, 15, tzinfo=zone, fold=1)
    spawn = Mock(return_value=Mock(wait=Mock(return_value=0)))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0
    assert spawn.call_count == expected_runs


def test_linux_native_timer_owns_catchup_not_local_due_date(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, adapter, _ = schedule
    adapter.mechanism = "systemd"
    service.set("09:00")
    spawn = Mock(return_value=Mock(wait=Mock(return_value=0)))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0  # only the native timer invokes this entry point
    spawn.assert_called_once()
    assert json.loads(service.state_path.read_text())["last_claimed_occurrence"] is None


def test_login_check_does_not_invent_nonexistent_dst_time(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zoneinfo import ZoneInfo

    service, _, clock = schedule
    zone = ZoneInfo("America/New_York")
    clock.return_value = datetime(2026, 3, 7, 8, tzinfo=zone)
    service.set("02:30")
    clock.return_value = datetime(2026, 3, 8, 3, 15, tzinfo=zone)
    spawn = Mock(return_value=Mock(wait=Mock(return_value=0)))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0
    spawn.assert_not_called()
    assert service.show().nominal_next_run == "2026-03-09T02:30:00-04:00"
    clock.return_value = datetime(2026, 3, 9, 3, 15, tzinfo=zone)
    assert service.run() == 0
    spawn.assert_called_once()


@pytest.mark.parametrize("change", ["identity", "missing", "revision"])
def test_delayed_pull_refuses_replaced_profile_or_revision(
    schedule: ScheduleFixture,
    change: str,
) -> None:
    from moneybin.errors import UserError

    service, adapter, _ = schedule
    service.set("09:00")
    assert adapter.job is not None
    revision = adapter.job.revision
    identity = service.home / "profiles/alice/profile_id"
    if change == "identity":
        identity.write_text("abcdef012345")
    elif change == "missing":
        identity.unlink()
    else:
        service.set("10:00")
    with pytest.raises(UserError):
        service.validate_pull_binding(profile_id="0123456789ab", revision=revision)
    assert identity.exists() == (change != "missing")


def test_idempotent_operations_count_without_profile_or_path_labels(
    schedule: ScheduleFixture,
) -> None:
    from moneybin.metrics.registry import SYNC_SCHEDULE_OPERATIONS_TOTAL

    def total(operation: str) -> float:
        return sum(
            sample.value
            for metric in SYNC_SCHEDULE_OPERATIONS_TOTAL.collect()
            for sample in metric.samples
            if sample.name == "moneybin_sync_schedule_operations_total"
            and sample.labels.get("operation") == operation
            and sample.labels.get("mechanism") == "launchd"
        )

    service, _, _ = schedule
    before_set, before_remove = total("set"), total("remove")
    service.set("09:00")
    service.set("09:00")
    service.remove()
    service.remove()
    assert total("set") == before_set + 2
    assert total("remove") == before_remove + 2
    for metric in SYNC_SCHEDULE_OPERATIONS_TOTAL.collect():
        for sample in metric.samples:
            assert set(sample.labels) == {"operation", "mechanism", "outcome"}
