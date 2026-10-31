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


def test_local_now_refuses_unreadable_os_time_zone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.errors import UserError
    from moneybin.services.sync_schedule import _local_now  # pyright: ignore[reportPrivateUsage]  # local-time failure boundary

    def unavailable(*args: object, **kwargs: object) -> None:
        raise OSError("synthetic unavailable localtime")

    monkeypatch.setattr(Path, "open", unavailable)

    with pytest.raises(UserError, match="OS local time zone") as raised:
        _local_now()

    assert raised.value.code == "sync_schedule_error"


def test_local_now_refuses_malformed_os_time_zone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from io import BytesIO

    from moneybin.errors import UserError
    from moneybin.services.sync_schedule import _local_now  # pyright: ignore[reportPrivateUsage]  # local-time failure boundary

    monkeypatch.setattr(Path, "open", Mock(return_value=BytesIO(b"not a TZif file")))

    with pytest.raises(UserError, match="OS local time zone") as raised:
        _local_now()

    assert raised.value.code == "sync_schedule_error"


def test_schedule_show_refuses_unreadable_os_time_zone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.errors import UserError

    def unavailable(*args: object, **kwargs: object) -> None:
        raise OSError("synthetic unavailable localtime")

    monkeypatch.setattr(Path, "open", unavailable)
    service = SyncScheduleService(
        home=tmp_path,
        profile="alice",
        adapter=MemoryAdapter(),
        launcher=("/usr/bin/true",),
    )

    with pytest.raises(UserError, match="OS local time zone") as raised:
        service.show()

    assert raised.value.code == "sync_schedule_error"


@pytest.fixture
def schedule(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ScheduleFixture:
    from moneybin.services.sync_schedule import SyncScheduleService

    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr(
        "keyring.util.platform_.config_root", lambda: tmp_path / "keyring"
    )
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


def test_schedule_binds_stock_secret_service_addressing_without_store_access(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    for name, value in {
        "SCHEME": "KeePassXC",
        "PREFERRED_COLLECTION": "/org/freedesktop/secrets/collection/synthetic",
        "APPID": "synthetic-application",
    }.items():
        monkeypatch.setenv(f"KEYRING_PROPERTY_{name}", value)
    forbidden = Mock(side_effect=AssertionError("metadata must not open a store"))
    monkeypatch.setattr("keyring.get_keyring", forbidden)
    monkeypatch.setattr("moneybin.database.get_database", forbidden)
    service.set("09:00")
    assert adapter.job is not None
    definition = json.loads(service.state_path.read_text())["definition"]
    assert definition["secret_addressing"] == {
        "backend": "secret-service",
        "configuration": {
            "path": str(service.home / "keyring/keyringrc.cfg"),
            "present": False,
            "source": "default",
            "selector": None,
        },
        "scheme": "KeePassXC",
        "preferred_collection": "/org/freedesktop/secrets/collection/synthetic",
        "appid": "synthetic-application",
    }
    rendered = SystemdScheduleAdapter(user_home=service.home).render(adapter.job)
    service_text = next(
        v.decode() for p, v in rendered.items() if p.suffix == ".service"
    )
    assert '"KEYRING_PROPERTY_SCHEME=KeePassXC"' in service_text
    assert '"KEYRING_PROPERTY_APPID=synthetic-application"' in service_text
    assert "KEYRING_PROPERTY_PREFERRED_COLLECTION=" in service_text
    forbidden.assert_not_called()


def test_runner_forwards_bound_addressing_after_ambient_properties_change(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    service, _, clock = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    config_home = service.home / "config space %literal"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("KEYRING_PROPERTY_SCHEME", "KeePassXC")
    monkeypatch.setenv(
        "KEYRING_PROPERTY_PREFERRED_COLLECTION",
        "/org/freedesktop/secrets/collection/synthetic",
    )
    monkeypatch.setenv("KEYRING_PROPERTY_APPID", "synthetic-application")
    service.set("09:00")
    monkeypatch.setenv("KEYRING_PROPERTY_SCHEME", "default")
    monkeypatch.setenv("KEYRING_PROPERTY_APPID", "other-application")
    monkeypatch.setenv("KEYRING_PROPERTY_GET_PASSWORD", "must-not-copy")
    monkeypatch.setenv("MONEYBIN_SYNTHETIC_SECRET", "must-not-copy")
    clock.return_value += timedelta(hours=2)
    spawn = Mock(return_value=Mock(wait=Mock(return_value=0)))
    monkeypatch.setattr("moneybin.services.sync_schedule.subprocess.Popen", spawn)
    assert service.run() == 0
    environment = spawn.call_args.kwargs["env"]
    assert environment["XDG_CONFIG_HOME"] == str(config_home)
    assert environment["KEYRING_PROPERTY_SCHEME"] == "KeePassXC"
    assert environment["KEYRING_PROPERTY_APPID"] == "synthetic-application"
    assert environment["KEYRING_PROPERTY_PREFERRED_COLLECTION"].endswith("/synthetic")
    assert "KEYRING_PROPERTY_GET_PASSWORD" not in environment
    assert "MONEYBIN_SYNTHETIC_SECRET" not in environment


def test_changed_store_addressing_starts_a_new_schedule_revision(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    service.set("09:00")
    previous = adapter.job
    monkeypatch.setenv("KEYRING_PROPERTY_SCHEME", "KeePassXC")
    service.set("09:00")
    assert adapter.job is not None and previous is not None
    assert adapter.job.revision != previous.revision


@pytest.mark.parametrize(
    "name,value",
    [
        ("GET_PASSWORD", "unsupported"),
        ("PRIORITY", "999"),
        ("SCHEME", "unknown"),
        ("PREFERRED_COLLECTION", "not/an/object/path"),
        ("PREFERRED_COLLECTION", "/collection/new\nline"),
        ("APPID", "new\nline"),
        ("APPID", "control\u0085value"),
        ("APPID", "x" * 257),
    ],
)
def test_invalid_store_addressing_refuses_before_schedule_mutation(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    import sys

    from moneybin.errors import UserError

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv(f"KEYRING_PROPERTY_{name}", value)
    with pytest.raises(UserError):
        service.set("09:00")
    assert adapter.job is None
    assert not service.state_path.exists()


def test_direct_scheduled_child_refuses_changed_addressing_before_secret_scope(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    from moneybin.cli.commands import sync
    from moneybin.errors import UserError

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("KEYRING_PROPERTY_SCHEME", "KeePassXC")
    service.set("09:00")
    assert adapter.job is not None
    revision = adapter.job.revision
    monkeypatch.setenv("KEYRING_PROPERTY_SCHEME", "default")
    monkeypatch.setattr(sync, "_build_schedule_service", lambda: service)
    monkeypatch.setattr("moneybin.observability.setup_observability", Mock())
    monkeypatch.setattr("moneybin.observability.finish_cli_metrics", Mock())
    forbidden = Mock(side_effect=AssertionError("credential policy entered too soon"))
    monkeypatch.setattr("moneybin.secrets.scheduled_secret_access", forbidden)
    with pytest.raises(UserError):
        with sync._scheduled_pull_access(True, "0123456789ab", revision):  # pyright: ignore[reportPrivateUsage]  # exercise the pre-credential startup boundary
            raise AssertionError("scheduled child must refuse before work")
    forbidden.assert_not_called()


def test_schedule_refuses_unsupported_explicit_backend_without_discovery(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    from moneybin.errors import UserError

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("PYTHON_KEYRING_BACKEND", "keyring.backends.fail.Keyring")
    forbidden = Mock(side_effect=AssertionError("backend discovery is forbidden"))
    monkeypatch.setattr("keyring.get_keyring", forbidden)
    with pytest.raises(UserError):
        service.set("09:00")
    assert adapter.job is None
    forbidden.assert_not_called()


def test_schedule_refuses_unbound_pre_release_metadata(
    schedule: ScheduleFixture,
) -> None:
    from moneybin.errors import UserError

    service, _, _ = schedule
    service.set("09:00")
    state = json.loads(service.state_path.read_text())
    state["version"] = 1
    state["definition"].pop("secret_addressing", None)
    service.state_path.write_text(json.dumps(state))
    with pytest.raises(UserError):
        service.show()


@pytest.mark.parametrize("drift", ["backend", "removed", "path", "unreadable"])
def test_bound_keyring_config_drift_refuses_before_scheduled_access(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch, drift: str
) -> None:
    import sys

    from moneybin.errors import UserError

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    config = service.home / "keyring/keyringrc.cfg"
    config.parent.mkdir()
    config.write_text(
        "[backend]\ndefault-keyring=keyring.backends.SecretService.Keyring\n"
    )
    service.set("09:00")
    assert adapter.job is not None
    if drift == "removed":
        config.unlink()
    elif drift == "backend":
        config.write_text("[backend]\ndefault-keyring=custom.Backend\n")
    elif drift == "path":
        config.write_text("[backend]\nkeyring-path=/synthetic/untrusted\n")
    else:
        monkeypatch.setattr(
            "moneybin.secrets.ConfigParser.read_file",
            Mock(side_effect=OSError("synthetic unreadable config")),
        )
    with pytest.raises(UserError):
        service.validate_pull_binding(
            profile_id="0123456789ab", revision=adapter.job.revision
        )


def test_custom_keyring_config_uses_bound_stock_selection_after_env_clear(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    from keyring.backends.SecretService import Keyring

    from moneybin.cli.commands import sync

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    config = service.home / "keyring/keyringrc.cfg"
    config.parent.mkdir()
    config.write_text(
        "[backend]\ndefault-keyring=keyring.backends.SecretService.Keyring\n"
    )
    service.set("09:00")
    assert adapter.job is not None
    original = adapter.job
    competing = service.home / "unrelated-keyring"
    competing.mkdir()
    (competing / "keyringrc.cfg").write_text(
        "[backend]\ndefault-keyring=custom.Backend\n"
    )
    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: competing)
    monkeypatch.setattr(sync, "_build_schedule_service", lambda: service)
    monkeypatch.setattr("moneybin.observability.setup_observability", Mock())
    monkeypatch.setattr("moneybin.observability.finish_cli_metrics", Mock())
    loader = Mock(return_value=Keyring())
    discovery = Mock(side_effect=AssertionError("unrelated discovery ran"))
    monkeypatch.setattr("keyring.core.load_keyring", loader)
    monkeypatch.setattr("keyring.get_keyring", discovery)
    monkeypatch.setattr("moneybin.secrets._LinuxSecrets", Mock())
    with sync._scheduled_pull_access(True, "0123456789ab", original.revision):  # pyright: ignore[reportPrivateUsage]  # exercise the bound startup policy
        pass
    loader.assert_called_once_with("keyring.backends.SecretService.Keyring")
    discovery.assert_not_called()


def test_custom_empty_keyring_config_refuses_competing_global_selection(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    from moneybin.errors import UserError

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    config = service.home / "keyring/keyringrc.cfg"
    config.parent.mkdir()
    config.write_text("[backend]\n")
    service.set("09:00")
    assert adapter.job is not None
    competing = service.home / "competing-keyring"
    competing.mkdir()
    (competing / "keyringrc.cfg").write_text(
        "[backend]\ndefault-keyring=keyring.backends.SecretService.Keyring\n"
    )
    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: competing)
    with pytest.raises(UserError):
        service.validate_pull_binding(
            profile_id="0123456789ab", revision=adapter.job.revision
        )


def test_custom_empty_keyring_config_retains_guarded_default_discovery(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys

    from keyring.backends.SecretService import Keyring

    from moneybin.secrets import scheduled_secret_access

    service, adapter, _ = schedule
    monkeypatch.setattr(sys, "platform", "linux")
    config = service.home / "keyring/keyringrc.cfg"
    config.parent.mkdir()
    config.write_text("[backend]\n")
    service.set("09:00")
    assert adapter.job is not None
    monkeypatch.setattr(
        "keyring.util.platform_.config_root", lambda: service.home / "default-keyring"
    )
    addressing = service.validate_pull_binding(
        profile_id="0123456789ab", revision=adapter.job.revision
    )
    discovery = Mock(return_value=Keyring())
    loader = Mock(
        side_effect=AssertionError("default discovery must not force a store")
    )
    monkeypatch.setattr("keyring.get_keyring", discovery)
    monkeypatch.setattr("keyring.core.load_keyring", loader)
    monkeypatch.setattr("moneybin.secrets._LinuxSecrets", Mock())
    with scheduled_secret_access(addressing):
        pass
    discovery.assert_called_once_with()
    loader.assert_not_called()


def test_keyring_config_location_is_part_of_native_binding(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.services.sync_schedule_native import LaunchdScheduleAdapter

    service, adapter, _ = schedule
    service.set("09:00")
    assert adapter.job is not None
    first = adapter.job
    monkeypatch.setattr(
        "keyring.util.platform_.config_root", lambda: service.home / "another-keyring"
    )
    service.set("09:00")
    assert adapter.job is not None
    assert adapter.job.revision != first.revision
    assert (
        adapter.job.secret_addressing.environment()
        != first.secret_addressing.environment()
    )
    native = LaunchdScheduleAdapter(user_home=service.home)
    assert list(native.render(first).values()) != list(
        native.render(adapter.job).values()
    )


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


def test_identical_set_refuses_live_cached_timer_after_both_files_are_deleted(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.errors import UserError
    from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

    service, memory, _ = schedule
    memory.mechanism = "systemd"
    service.set("09:00")
    assert memory.job is not None
    original = memory.job
    metadata = service.state_path.read_bytes()

    def native_query(args: list[str], **kwargs: Any) -> Mock:
        if args[2] == "is-enabled":
            return Mock(returncode=1, stdout="disabled\n", stderr="")
        assert args[2] == "is-active"
        return Mock(returncode=0, stdout="", stderr="")

    command = Mock(side_effect=native_query)
    adapter = SystemdScheduleAdapter(user_home=service.home, command=command)
    service.adapter = adapter
    monkeypatch.setattr(
        adapter,
        "_unit_states",
        Mock(
            return_value=[
                {
                    "LoadState": "loaded",
                    "ActiveState": "active",
                    "FragmentPath": str(adapter.timer_path(original)),
                    "DropInPaths": "",
                    "NeedDaemonReload": "no",
                },
                {
                    "LoadState": "loaded",
                    "ActiveState": "inactive",
                    "MainPID": "0",
                    "FragmentPath": str(adapter.service_path(original)),
                    "DropInPaths": "",
                    "NeedDaemonReload": "no",
                },
            ]
        ),
    )
    assert not any(path.exists() for path in adapter.render(original))
    with pytest.raises(UserError, match="retained metadata"):
        service.set("09:00")
    assert service.state_path.read_bytes() == metadata
    assert service.show().state == "partial"
    assert all(
        call.args[0][2] in {"is-active", "is-enabled"}
        for call in command.call_args_list
    )


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
    assert adapter.job is None
    assert service.show().state == "partial"


@pytest.mark.parametrize("failure", ["os", "runtime", "user"])
def test_replacement_inspection_failure_restores_previous_native_schedule(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from moneybin.errors import UserError

    service, adapter, _ = schedule
    service.set("09:00")
    previous = adapter.job
    assert previous is not None
    original_inspect = adapter.inspect
    failed = False

    def inspect(job: ScheduleDefinition) -> NativeStatus:
        nonlocal failed
        if job.revision != previous.revision and not failed:
            failed = True
            assert adapter.job is None
            raise {
                "os": OSError("synthetic inspection failure"),
                "runtime": RuntimeError("synthetic inspection failure"),
                "user": UserError(
                    "Synthetic inspection failure", code="sync_schedule_error"
                ),
            }[failure]
        return original_inspect(job)

    monkeypatch.setattr(adapter, "inspect", inspect)
    with pytest.raises((OSError, RuntimeError, UserError)) as caught:
        service.set("10:00")
    assert adapter.job == previous
    assert service.show().state == "installed"
    assert isinstance(caught.value, UserError)
    assert (
        json.loads(service.state_path.read_text())["definition"]["revision"]
        == previous.revision
    )


@pytest.mark.parametrize("remainder", ["absent", "partial"])
def test_replacement_partial_remove_failure_restores_using_old_binding(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch, remainder: str
) -> None:
    from moneybin.errors import UserError

    service, adapter, _ = schedule
    service.set("09:00")
    previous = adapter.job
    assert previous is not None
    original_remove = adapter.remove
    removals: list[ScheduleDefinition] = []

    def remove(job: ScheduleDefinition) -> None:
        removals.append(job)
        if len(removals) == 1:
            if remainder == "absent":
                adapter.job = None
            adapter.partial = True
            raise OSError("synthetic failure after native removal began")
        assert job.revision == previous.revision
        original_remove(job)

    monkeypatch.setattr(adapter, "remove", remove)
    with pytest.raises((OSError, UserError)):
        service.set("10:00")
    assert adapter.job == previous
    assert service.show().state == "installed"
    assert len(removals) == 2


def test_replacement_definition_failure_does_not_remove_previous_schedule(
    schedule: ScheduleFixture,
) -> None:
    service, adapter, clock = schedule
    service.set("09:00")
    previous = adapter.job
    clock.side_effect = RuntimeError("synthetic clock failure")
    with pytest.raises(RuntimeError):
        service.set("10:00")
    assert adapter.job == previous


def test_replacement_ownership_collision_preserves_external_definition(
    schedule: ScheduleFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.errors import UserError

    service, adapter, _ = schedule
    service.set("09:00")
    previous = adapter.job
    assert previous is not None
    external = previous.model_copy(update={"revision": "c" * 32})
    original_inspect, original_remove = adapter.inspect, adapter.remove

    def inspect(job: ScheduleDefinition) -> NativeStatus:
        if adapter.job is None and job.revision != previous.revision:
            adapter.job = external
        return original_inspect(job)

    def remove(job: ScheduleDefinition) -> None:
        if not original_inspect(job).owned:
            raise UserError("Synthetic ownership refusal", code="sync_schedule_error")
        original_remove(job)

    monkeypatch.setattr(adapter, "inspect", inspect)
    monkeypatch.setattr(adapter, "remove", remove)
    install = Mock(wraps=adapter.install)
    monkeypatch.setattr(adapter, "install", install)
    with pytest.raises(UserError):
        service.set("10:00")
    assert adapter.job == external
    install.assert_not_called()
    assert service.show().state == "partial"
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
