"""Native argument serialization, session lifetime, and ownership."""

import plistlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from moneybin.services.sync_schedule_native import ScheduleDefinition

pytestmark = pytest.mark.unit


class RemovalManager:
    """Model native missing-unit failures and cached definitions without jobs."""

    def __init__(self, tmp_path: Path) -> None:
        """Start with absent native units and a synthetic persistent timestamp."""
        from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

        self.adapter = SystemdScheduleAdapter(user_home=tmp_path, command=self.run)
        self.job = make_job(tmp_path).model_copy(update={"mechanism": "systemd"})
        self.files = self.adapter.render(self.job)
        self.loaded: set[str] = set()
        self.active = False
        self.external = False
        self.persistent = True
        self.fail_final_reload = False
        self.fail_clean = False
        self.omit_service_pid = False
        self.commands: list[list[str]] = []
        self.registration = "disabled"

    def run(self, args: list[str], **kwargs: Any) -> Mock:
        self.commands.append(args)
        operation = args[2] if args[0].endswith("systemctl") else "verify"
        suffix = "timer" if any(arg.endswith(".timer") for arg in args) else "service"
        path = (
            self.adapter.timer_path(self.job)
            if suffix == "timer"
            else self.adapter.service_path(self.job)
        )
        if operation == "show":
            loaded = suffix in self.loaded or path.exists()
            fragment = (
                str(path.parent / "external.service") if self.external else str(path)
            )
            pid = (
                f"MainPID={123 if self.active else 0}\n"
                if suffix == "service" and not self.omit_service_pid
                else ""
            )
            output = (
                f"LoadState={'loaded' if loaded else 'not-found'}\n"
                f"ActiveState={'active' if self.active else 'inactive'}\n"
                f"{pid}"
                f"FragmentPath={fragment if loaded else ''}\nDropInPaths=\n"
                f"NeedDaemonReload={'yes' if loaded and not path.exists() else 'no'}\n"
            )
            return Mock(returncode=0, stdout=output, stderr="")
        if operation == "is-enabled":
            link = path.parent / "graphical-session.target.wants" / path.name
            if self.registration == "other-target":
                link = path.parent / "default.target.wants" / path.name
            state = "enabled-runtime" if self.registration == "runtime" else "enabled"
            return Mock(
                returncode=0 if self.registration != "disabled" else 1,
                stdout=(
                    f"{state}\n  {link}\n"
                    if self.registration != "disabled"
                    else "disabled\n"
                ),
                stderr="",
            )
        if operation == "daemon-reload":
            if self.fail_final_reload and not any(path.exists() for path in self.files):
                self.fail_final_reload = False
                return Mock(returncode=1, stdout="", stderr="synthetic reload failure")
            self.loaded = {path.suffix[1:] for path in self.files if path.exists()}
        if operation in {"disable", "stop", "clean"}:
            if suffix not in self.loaded and not path.exists():
                return Mock(returncode=5, stdout="", stderr="Unit not loaded")
            if operation == "clean":
                if self.fail_clean:
                    return Mock(
                        returncode=1, stdout="", stderr="synthetic clean failure"
                    )
                assert all(
                    path.read_bytes() == content for path, content in self.files.items()
                )
                self.persistent = False
        return Mock(returncode=0, stdout="", stderr="")


@pytest.mark.parametrize(
    "registration",
    ["disabled", "missing-link", "wrong-target", "other-target", "runtime"],
)
def test_linux_active_timer_requires_owned_persistent_graphical_registration(
    tmp_path: Path, registration: str
) -> None:
    manager = RemovalManager(tmp_path)
    for path, content in manager.files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    manager.active = True
    timer = manager.adapter.timer_path(manager.job)
    link = timer.parent / "graphical-session.target.wants" / timer.name
    if registration != "disabled":
        manager.registration = (
            registration if registration in {"other-target", "runtime"} else "enabled"
        )
    if registration == "wrong-target":
        link.parent.mkdir()
        link.symlink_to(timer.parent / "unrelated.timer")
    status = manager.adapter.inspect(manager.job)
    assert status.partial
    assert status.owned is (registration == "disabled")
    if not status.owned:
        from moneybin.errors import UserError

        with pytest.raises(UserError):
            manager.adapter.remove(manager.job)
        assert all(path.exists() for path in manager.files)
        assert not any("disable" in args for args in manager.commands)


def test_linux_registered_timer_stays_owned_in_inactive_session(tmp_path: Path) -> None:
    manager = RemovalManager(tmp_path)
    for path, content in manager.files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    manager.registration = "enabled"
    timer = manager.adapter.timer_path(manager.job)
    link = timer.parent / "graphical-session.target.wants" / timer.name
    link.parent.mkdir()
    link.symlink_to(timer)

    def inactive(args: list[str], **kwargs: Any) -> Mock:
        if "is-active" in args:
            return Mock(returncode=3, stdout="inactive", stderr="")
        return manager.run(args, **kwargs)

    manager.adapter.command = inactive
    status = manager.adapter.inspect(manager.job)
    assert status.owned and not status.partial and not status.active


def test_linux_remove_recovers_install_failure_before_any_native_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = RemovalManager(tmp_path)
    with monkeypatch.context() as context:
        context.setattr(
            "moneybin.services.sync_schedule_native.atomic_write",
            Mock(side_effect=OSError("synthetic write failure")),
        )
        with pytest.raises(OSError):
            manager.adapter.install(manager.job)
    manager.adapter.remove(manager.job)
    assert not manager.persistent
    assert not any(path.exists() for path in manager.files)
    assert not any(
        "enable" in args or "start" in args or "restart" in args
        for args in manager.commands
    )


def test_linux_remove_retry_after_unlink_then_reload_failure(tmp_path: Path) -> None:
    from moneybin.errors import UserError

    manager = RemovalManager(tmp_path)
    for path, content in manager.files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    manager.loaded = {"timer", "service"}
    manager.fail_final_reload = True
    with pytest.raises(UserError):
        manager.adapter.remove(manager.job)
    assert not any(path.exists() for path in manager.files)
    manager.adapter.remove(manager.job)
    assert not manager.persistent
    assert not any(path.exists() for path in manager.files)


@pytest.mark.parametrize("unsafe", ["active", "external", "missing_service_pid"])
def test_linux_absent_file_recovery_refuses_unsafe_loaded_units(
    tmp_path: Path, unsafe: str
) -> None:
    from moneybin.errors import UserError

    manager = RemovalManager(tmp_path)
    manager.loaded = {"timer", "service"}
    manager.active = unsafe == "active"
    manager.external = unsafe == "external"
    manager.omit_service_pid = unsafe == "missing_service_pid"
    with pytest.raises(UserError):
        manager.adapter.remove(manager.job)
    assert manager.persistent
    assert not any(path.exists() for path in manager.files)
    assert not any(
        "disable" in args or "stop" in args or "clean" in args
        for args in manager.commands
    )


def test_linux_failed_recovery_cleanup_retains_exact_definitions(
    tmp_path: Path,
) -> None:
    from moneybin.errors import UserError

    manager = RemovalManager(tmp_path)
    manager.fail_clean = True
    with pytest.raises(UserError):
        manager.adapter.remove(manager.job)
    assert manager.persistent
    assert all(
        path.exists() and path.read_bytes() == content
        for path, content in manager.files.items()
    )


def make_job(tmp_path: Path) -> ScheduleDefinition:
    from moneybin.services.sync_schedule_native import ScheduleDefinition

    return ScheduleDefinition(
        home=str(tmp_path / 'space "quote" %percent\nline'),
        profile="alice",
        profile_id="0123456789ab",
        job_id="com.moneybin.sync." + "a" * 24,
        revision="b" * 32,
        time="09:05",
        mechanism="launchd",
        installed_at=datetime(2026, 10, 18, 8, tzinfo=UTC),
        launcher=("/opt/space path/uv", "run", "--directory", "/opt/repo"),
    )


def test_launchd_is_aqua_daily_and_load_due_check_with_null_streams(
    tmp_path: Path,
) -> None:
    from moneybin.services.sync_schedule_native import LaunchdScheduleAdapter

    adapter = LaunchdScheduleAdapter(user_home=tmp_path)
    job = make_job(tmp_path)
    contents = adapter.render(job)
    payload = plistlib.loads(next(iter(contents.values())))
    assert payload["Label"] == job.job_id
    assert payload["LimitLoadToSessionType"] == "Aqua"
    assert payload["StartCalendarInterval"] == {"Hour": 9, "Minute": 5}
    assert payload["RunAtLoad"] is True
    assert "KeepAlive" not in payload
    assert payload["AbandonProcessGroup"] is False
    assert payload["StandardOutPath"] == "/dev/null"
    assert payload["StandardErrorPath"] == "/dev/null"
    args = payload["ProgramArguments"]
    assert args[:2] == ["/usr/bin/env", "-i"]
    assert args[-9:] == [
        "--home",
        job.home,
        "--profile",
        "alice",
        "sync",
        "schedule",
        "_run",
        "--revision",
        job.revision,
    ]


def test_systemd_units_bind_session_without_starting_it_and_escape_args(
    tmp_path: Path,
) -> None:
    from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

    adapter = SystemdScheduleAdapter(user_home=tmp_path)
    job = make_job(tmp_path).model_copy(update={"mechanism": "systemd"})
    files = adapter.render(job)
    timer = files[adapter.timer_path(job)].decode()
    service = files[adapter.service_path(job)].decode()
    for text in (timer, service):
        assert "Requisite=graphical-session.target\n" in text
        assert "PartOf=graphical-session.target\n" in text
        assert "After=graphical-session.target\n" in text
        assert "Requires=" not in text and "Wants=" not in text
    assert "WantedBy=graphical-session.target\n" in timer
    assert "OnCalendar=*-*-* 09:05:00\n" in timer
    assert "Persistent=true\n" in timer
    assert "WakeSystem=false\n" in timer
    assert "KillMode=control-group\n" in service
    assert "Restart=no\n" in service
    assert "StandardOutput=null\nStandardError=null\n" in service
    assert "%%percent\\nline" in service
    assert '\\"quote\\"' in service
    assert 'ExecStart="/usr/bin/env" "-i"' in service


def test_linux_preserves_session_bus_without_copying_manager_environment(
    tmp_path: Path,
) -> None:
    from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

    adapter = SystemdScheduleAdapter(user_home=tmp_path)
    job = make_job(tmp_path).model_copy(update={"mechanism": "systemd"})
    service = adapter.render(job)[adapter.service_path(job)].decode()
    assert '"XDG_RUNTIME_DIR=%t"' in service
    assert '"DBUS_SESSION_BUS_ADDRESS=unix:path=%t/bus"' in service
    assert "MONEYBIN_" not in service


def test_linux_timer_does_not_cycle_through_basic_and_graphical_targets(
    tmp_path: Path,
) -> None:
    from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

    adapter = SystemdScheduleAdapter(user_home=tmp_path)
    job = make_job(tmp_path).model_copy(update={"mechanism": "systemd"})
    timer = adapter.render(job)[adapter.timer_path(job)].decode()
    # Default timer Before=timers.target -> basic.target -> graphical-session
    # conflicts with this timer's required After=graphical-session.target.
    assert "DefaultDependencies=no\n" in timer
    assert "Conflicts=shutdown.target\n" in timer
    assert "Before=shutdown.target\n" in timer


def test_linux_inactive_session_refused_without_mutation(tmp_path: Path) -> None:
    from moneybin.errors import UserError
    from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

    run = Mock(return_value=Mock(returncode=3, stdout="inactive", stderr=""))
    adapter = SystemdScheduleAdapter(user_home=tmp_path, command=run)
    with pytest.raises(UserError, match="graphical"):
        adapter.install(make_job(tmp_path))
    assert not (tmp_path / ".config/systemd/user").exists()


def test_unmanaged_definition_is_refused(tmp_path: Path) -> None:
    from moneybin.errors import UserError
    from moneybin.services.sync_schedule_native import LaunchdScheduleAdapter

    run = Mock(return_value=Mock(returncode=0, stdout="", stderr=""))
    adapter = LaunchdScheduleAdapter(user_home=tmp_path, command=run)
    job = make_job(tmp_path)
    path = next(iter(adapter.render(job)))
    path.parent.mkdir(parents=True)
    path.write_text("unmanaged")
    assert not adapter.inspect(job).owned
    with pytest.raises(UserError, match="owned"):
        adapter.remove(job)
    assert path.read_text() == "unmanaged"


@pytest.mark.parametrize(
    "drift",
    [
        "none",
        "profile",
        "origin",
        "unknown-format",
        "delimiter",
        "collapsed-list",
        "collapsed-print",
    ],
)
def test_launchd_loaded_binding_must_match_complete_argv_and_origin(
    tmp_path: Path, drift: str
) -> None:
    from moneybin.services.sync_schedule_native import LaunchdScheduleAdapter

    job = make_job(tmp_path)
    adapter = LaunchdScheduleAdapter(user_home=tmp_path)
    path, content = next(iter(adapter.render(job).items()))
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    payload = plistlib.loads(content)
    arguments: list[str] = payload["ProgramArguments"]
    if drift == "profile":
        arguments[arguments.index("--profile") + 1] = "other"
    if drift == "delimiter":
        arguments.insert(3, '\n\t);\n\t"ProgramArguments" = (\n')
    if drift == "collapsed-list":
        arguments = ['";\n\t\t"'.join(arguments)]
    if drift == "collapsed-print":
        arguments = ["\n\t\t".join(arguments)]
    # Observed legacy list shape: literals are quoted but not escaped, including
    # actual embedded quotes/newlines. It is deliberately not a property list.
    listing = (
        '{\n\t"Program" = "/usr/bin/env";\n'
        f'\t"Label" = "{job.job_id}";\n\t"ProgramArguments" = (\n'
        + "".join(f'\t\t"{arg}";\n' for arg in arguments)
        + "\t);\n};\n"
    )
    if drift == "unknown-format":
        listing = "unknown new launchctl output"
    origin = path if drift != "origin" else tmp_path / "external.plist"

    printed = (
        f"job = {{\n\tpath = {origin}\n\targuments = {{\n"
        + "".join(f"\t\t{arg}\n" for arg in arguments)
        + "\t}\n}\n"
    )

    def run(args: list[str], **kwargs: object) -> Mock:
        stdout = listing if "list" in args else printed
        return Mock(returncode=0, stdout=stdout, stderr="")

    adapter.command = run
    assert adapter.inspect(job).owned is (drift == "none")


def test_linux_remove_cleans_persistent_timestamp_before_unlink(tmp_path: Path) -> None:
    from moneybin.services.sync_schedule_native import SystemdScheduleAdapter

    commands: list[list[str]] = []

    def run(args: list[str], **kwargs: object) -> Mock:
        commands.append(args)
        if "is-enabled" in args:
            return Mock(returncode=1, stdout="disabled\n", stderr="")
        if "show" in args:
            return Mock(
                returncode=0, stdout="ActiveState=inactive\nMainPID=0\n", stderr=""
            )
        return Mock(returncode=0, stdout="", stderr="")

    adapter = SystemdScheduleAdapter(user_home=tmp_path, command=run)
    job = make_job(tmp_path).model_copy(update={"mechanism": "systemd"})
    for path, content in adapter.render(job).items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    adapter.remove(job)
    assert [
        "/usr/bin/systemctl",
        "--user",
        "clean",
        "--what=state",
        job.job_id + ".timer",
    ] in commands
    assert not adapter.timer_path(job).exists()
    assert not adapter.service_path(job).exists()
