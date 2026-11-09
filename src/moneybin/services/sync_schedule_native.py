"""Owned desktop-session jobs; no shell, scheduler daemon, or credential access."""

import os
import plistlib
import re
import shutil
import stat
import subprocess  # noqa: S404  # native tools with explicit argv and no shell
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from moneybin import error_codes
from moneybin.config import canonical_checkout_root, running_checkout_root
from moneybin.errors import UserError
from moneybin.secrets import ScheduledSecretAddressing

_SAFE_PATH = "/usr/bin:/bin:/usr/sbin:/sbin"
_Command = Callable[..., subprocess.CompletedProcess[str]]


def schedule_error(message: str) -> UserError:
    """Build a bounded scheduling error without persisting OS exception text."""
    return UserError(
        message,
        code=error_codes.SYNC_SCHEDULE_ERROR,
        hint="Inspect 'moneybin sync schedule show'; remove the owned schedule "
        "before repairing or recreating it.",
    )


def clean_environment() -> dict[str, str]:
    """Keep session connectivity, excluding configuration and environment secrets."""
    allowed = {
        "HOME",
        "USER",
        "LOGNAME",
        "LANG",
        "TZ",
        "XDG_RUNTIME_DIR",
        "DBUS_SESSION_BUS_ADDRESS",
    }
    env = {k: v for k, v in os.environ.items() if k in allowed or k.startswith("LC_")}
    config_home = os.environ.get("XDG_CONFIG_HOME", "")
    if sys.platform == "linux" and Path(config_home).is_absolute():
        env["XDG_CONFIG_HOME"] = config_home
    env["PATH"] = _SAFE_PATH
    return env


def resolve_schedule_launcher() -> tuple[str, ...]:
    """Bind the canonical checkout or the current persistent tool executable."""
    uv = shutil.which("uv")
    if uv is None:
        raise schedule_error("An absolute uv executable is required for scheduling.")
    executable = Path(uv).resolve()
    root = running_checkout_root()
    if root is not None:
        root = canonical_checkout_root(root).resolve()
        paths = (executable, root)
        args = (
            str(executable),
            "run",
            "--offline",
            "--no-sync",
            "--directory",
            str(root),
            "moneybin",
        )
    else:
        environment = clean_environment()
        for name in ("UV_TOOL_DIR", "XDG_DATA_HOME"):
            if name in os.environ:
                environment[name] = os.environ[name]
        try:
            result = subprocess.run(  # noqa: S603  # absolute uv, read-only directory query
                [str(executable), "tool", "dir"],
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                env=environment,
                timeout=30,
                check=False,
            )
            tools = Path(result.stdout.strip())
            prefix = Path(sys.prefix).absolute()
            console = prefix / "bin/moneybin"
            if (
                result.returncode
                or not tools.is_absolute()
                or prefix.resolve() != (tools / "moneybin").resolve()
                or not Path(__file__).resolve().is_relative_to(prefix.resolve())
                or not console.is_file()
                or not console.resolve().is_relative_to(prefix.resolve())
                or not os.access(console, os.X_OK)
            ):
                raise ValueError
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            raise schedule_error(
                "Scheduling requires the current persistent uv tool installation."
            ) from exc
        # Keep the tool entrypoint in its venv; resolving its Python symlink
        # instead would discard the installed package environment.
        paths = (executable, prefix, console)
        args = (str(console),)
    temporary_roots = {
        Path("/tmp").resolve(),  # noqa: S108  # rejection boundary, never a write target
        Path("/var/tmp").resolve(),  # noqa: S108  # rejection boundary, never a write target
        Path(tempfile.gettempdir()).resolve(),
    }
    if any(
        not p.exists() or any(p.is_relative_to(t) for t in temporary_roots)
        for p in paths
    ):
        raise schedule_error("Scheduling requires a durable installation or checkout.")
    return args


class ScheduleDefinition(BaseModel):
    """Complete binding of one native definition to a persisted revision."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    home: str
    profile: str
    profile_id: str = Field(pattern=r"^[a-f0-9]{12}$")
    job_id: str = Field(pattern=r"^com\.moneybin\.sync\.[a-f0-9]{24}$")
    revision: str = Field(pattern=r"^[a-f0-9]{32}$")
    time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    mechanism: Literal["launchd", "systemd"]
    installed_at: AwareDatetime
    launcher: tuple[str, ...] = Field(min_length=1, max_length=16)
    secret_addressing: ScheduledSecretAddressing

    @field_validator("home")
    @classmethod
    def absolute_home(cls, value: str) -> str:
        """Reject relative or unrepresentable homes in persisted definitions."""
        if not Path(value).is_absolute() or "\x00" in value:
            raise ValueError("home must be absolute")
        return value


@dataclass(frozen=True)
class NativeStatus:
    """OS registration, ownership, and process facts for one owned job."""

    present: bool
    owned: bool
    active: bool
    running: bool
    partial: bool = False


def atomic_write(path: Path, content: bytes) -> None:
    """Replace owner-only content and sync its directory before returning."""
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _matches(path: Path, contents: bytes) -> bool:
    if path.is_symlink():
        return False
    info = path.stat()
    return (
        stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and not info.st_mode & 0o022
        and path.read_bytes() == contents
    )


def _runner_args(
    job: ScheduleDefinition, user_home: Path, config_home: Path | None = None
) -> list[str]:
    # env -i strips the user manager's environment before uv or MoneyBin starts.
    return [
        "/usr/bin/env",
        "-i",
        f"HOME={user_home}",
        f"PATH={_SAFE_PATH}",
        *([f"XDG_CONFIG_HOME={config_home}"] if config_home is not None else []),
        *(
            f"{name}={value}"
            for name, value in job.secret_addressing.environment().items()
        ),
        *job.launcher,
        "--home",
        job.home,
        "--profile",
        job.profile,
        "sync",
        "schedule",
        "_run",
        "--revision",
        job.revision,
    ]


def _launchd_binding_matches(
    job: ScheduleDefinition,
    user_home: Path,
    path: Path,
    listing: str,
    printed: str,
) -> bool:
    """Fail closed when either native rendering cannot prove the complete argv."""
    arguments = _runner_args(job, user_home)
    listed_start, listed_end = '\t"ProgramArguments" = (\n', "\t);\n"
    printed_start, printed_end = "\targuments = {\n", "\t}\n"
    expected_list = (
        listed_start + "".join(f'\t\t"{arg}";\n' for arg in arguments) + listed_end
    )
    expected_print = (
        printed_start + "".join(f"\t\t{arg}\n" for arg in arguments) + printed_end
    )

    def complete_block(text: str, start: str, end: str) -> str | None:
        if text.count(start) != 1:
            return None
        offset = text.index(start)
        stop = text.find(end, offset + len(start))
        return None if stop < 0 else text[offset : stop + len(end)]

    # launchctl does not escape these strings. One rendering alone admits
    # delimiter-shaped argv spoofing; the two different separators must agree.
    # `print` is explicitly not a stable API: new formats are an ownership
    # refusal, never permission to modify an unverified live job.
    return (
        complete_block(listing, listed_start, listed_end) == expected_list
        and complete_block(printed, printed_start, printed_end) == expected_print
        and listing.count(f'\t"Label" = "{job.job_id}";\n') == 1
        and listing.count('\t"Program" = "/usr/bin/env";\n') == 1
        and printed.count("\tpath = ") == 1
        and f"\tpath = {path}\n" in printed
    )


class LaunchdScheduleAdapter:
    """Manage only a bound Aqua LaunchAgent in the current user's GUI domain."""

    mechanism: Literal["launchd"] = "launchd"

    def __init__(
        self, *, user_home: Path | None = None, command: _Command = subprocess.run
    ) -> None:
        """Target the current user GUI domain and per-user LaunchAgent folder."""
        self.user_home = user_home or Path.home()
        self.command = command
        self.domain = f"gui/{os.getuid()}"

    def _call(self, args: list[str], *, check: bool = True):
        try:
            result = self.command(
                args,
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                env=clean_environment(),
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise schedule_error(
                "The native scheduler could not be contacted."
            ) from exc
        if check and result.returncode:
            raise schedule_error(
                "launchd refused the schedule operation; metadata "
                "was retained for inspection and removal."
            )
        return result

    def validate_session(self) -> None:
        """Refuse installation outside a graphical login session."""
        if self._call(["/bin/launchctl", "print", self.domain], check=False).returncode:
            raise schedule_error(
                "Scheduling requires an active graphical login session."
            )

    def path(self, job: ScheduleDefinition) -> Path:
        """Return the single owned LaunchAgent path."""
        return self.user_home / "Library/LaunchAgents" / f"{job.job_id}.plist"

    def render(self, job: ScheduleDefinition) -> dict[Path, bytes]:
        """Serialize the literal runner argument array into a LaunchAgent plist."""
        hour, minute = map(int, job.time.split(":"))
        payload = {
            "Label": job.job_id,
            "ProgramArguments": _runner_args(job, self.user_home),
            "LimitLoadToSessionType": "Aqua",
            "RunAtLoad": True,
            "StartCalendarInterval": {"Hour": hour, "Minute": minute},
            "AbandonProcessGroup": False,
            "StandardInPath": "/dev/null",
            "StandardOutPath": "/dev/null",
            "StandardErrorPath": "/dev/null",
        }
        return {self.path(job): plistlib.dumps(payload)}

    def inspect(self, job: ScheduleDefinition) -> NativeStatus:
        """Check the complete file and registration without adopting another job."""
        path = self.path(job)
        exists = path.exists() or path.is_symlink()
        owned = not exists or _matches(path, self.render(job)[path])
        loaded = self._call(
            ["/bin/launchctl", "print", f"{self.domain}/{job.job_id}"], check=False
        )
        session = not self._call(
            ["/bin/launchctl", "print", self.domain], check=False
        ).returncode
        # The PID comes from legacy list; binding proof requires both complete
        # literal renderings, because their raw string formats are ambiguous alone.
        listing = self._call(["/bin/launchctl", "list", job.job_id], check=False)
        if loaded.returncode == 0:
            owned = (
                owned
                and listing.returncode == 0
                and _launchd_binding_matches(
                    job,
                    self.user_home,
                    path,
                    listing.stdout,
                    loaded.stdout,
                )
            )
        running = listing.returncode == 0 and bool(
            re.search(r'"PID"\s*=\s*[1-9]\d*;', listing.stdout)
        )
        return NativeStatus(
            present=exists or loaded.returncode == 0,
            owned=owned and not (not exists and loaded.returncode == 0),
            active=session and loaded.returncode == 0,
            running=running,
            partial=exists != (loaded.returncode == 0) and session,
        )

    def install(self, job: ScheduleDefinition) -> None:
        """Validate and register one owned definition in the GUI domain."""
        self.validate_session()
        status = self.inspect(job)
        if not status.owned:
            raise schedule_error("Refusing to replace a job that is not owned.")
        path, content = next(iter(self.render(job).items()))
        atomic_write(path, content)
        self._call(["/usr/bin/plutil", "-lint", str(path)])
        self._call(["/bin/launchctl", "enable", f"{self.domain}/{job.job_id}"])
        self._call(["/bin/launchctl", "bootstrap", self.domain, str(path)])
        if not self.inspect(job).active:
            raise schedule_error("The installed launchd job could not be verified.")

    def remove(self, job: ScheduleDefinition) -> None:
        """Disable, stop the owned process group, and remove its definition."""
        status = self.inspect(job)
        if not status.owned:
            raise schedule_error("Refusing to remove a job that is not owned.")
        if status.active:
            target = f"{self.domain}/{job.job_id}"
            self._call(["/bin/launchctl", "disable", target])
            self._call(["/bin/launchctl", "bootout", target])
            if (
                self._call(["/bin/launchctl", "print", target], check=False).returncode
                == 0
            ):
                raise schedule_error("The owned launchd process did not stop.")
        self.path(job).unlink(missing_ok=True)


def _unit_quote(argument: str) -> str:
    # systemd.syntax C escaping, plus ExecStart specifier/environment escaping.
    escaped = (
        argument
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("%", "%%")
        .replace("$", "$$")
    )
    return f'"{escaped}"'


class SystemdScheduleAdapter:
    """Manage one user timer and service tied to graphical-session.target."""

    mechanism: Literal["systemd"] = "systemd"

    def __init__(
        self, *, user_home: Path | None = None, command: _Command = subprocess.run
    ) -> None:
        """Target the current user's systemd manager and unit directory."""
        self.user_home = user_home or Path.home()
        configured = Path(os.environ.get("XDG_CONFIG_HOME", ""))
        self.config_home = (
            configured if configured.is_absolute() else self.user_home / ".config"
        )
        self.command = command

    def _environment(self) -> dict[str, str]:
        return clean_environment() | {
            "HOME": str(self.user_home),
            "XDG_CONFIG_HOME": str(self.config_home),
        }

    def _call(self, args: list[str], *, check: bool = True):
        try:
            result = self.command(
                ["/usr/bin/systemctl", "--user", *args],
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                env=self._environment(),
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise schedule_error("The systemd user manager is unavailable.") from exc
        if check and result.returncode:
            raise schedule_error(
                "systemd refused the schedule operation; metadata "
                "was retained for inspection and removal."
            )
        return result

    def validate_session(self) -> None:
        """Require a desktop that manages graphical-session.target."""
        if self._call(
            ["is-active", "--quiet", "graphical-session.target"], check=False
        ).returncode:
            raise schedule_error(
                "This desktop has no active systemd graphical-session "
                "target; scheduling is unsupported in this session."
            )

    def service_path(self, job: ScheduleDefinition) -> Path:
        """Return the single owned service definition path."""
        return self.config_home / "systemd/user" / f"{job.job_id}.service"

    def timer_path(self, job: ScheduleDefinition) -> Path:
        """Return the matching owned daily timer path."""
        return self.service_path(job).with_suffix(".timer")

    def render(self, job: ScheduleDefinition) -> dict[Path, bytes]:
        """Render session-bound units with native argument escaping."""
        unit = (
            "[Unit]\nDescription=MoneyBin scheduled sync\n"
            "Requisite=graphical-session.target\n"
            "After=graphical-session.target\nPartOf=graphical-session.target\n"
        )
        # The default env -i runner already derives .config from HOME; retain
        # its existing bytes so installed default jobs keep their ownership.
        config_home = (
            self.config_home if self.config_home != self.user_home / ".config" else None
        )
        arguments = list(
            map(_unit_quote, _runner_args(job, self.user_home, config_home))
        )
        # %t is the native user-runtime directory, resolved anew each session;
        # only these fixed specifiers expand, never percent signs from user paths.
        arguments[4:4] = [
            '"XDG_RUNTIME_DIR=%t"',
            '"DBUS_SESSION_BUS_ADDRESS=unix:path=%t/bus"',
        ]
        args = " ".join(arguments)
        service = (
            unit + "[Service]\nType=oneshot\nExecStart=" + args + "\n"
            "KillMode=control-group\nRestart=no\nTimeoutStartSec=infinity\n"
            "StandardInput=null\nStandardOutput=null\nStandardError=null\n"
        )
        # The default Before=timers.target cycles through basic.target back
        # to graphical-session.target. Keep explicit shutdown ordering instead.
        timer = (
            unit + "DefaultDependencies=no\nConflicts=shutdown.target\n"
            "Before=shutdown.target\n"
            f"[Timer]\nOnCalendar=*-*-* {job.time}:00\n"
            "Persistent=true\nWakeSystem=false\n"
            "[Install]\nWantedBy=graphical-session.target\n"
        )
        return {
            self.service_path(job): service.encode(),
            self.timer_path(job): timer.encode(),
        }

    def _unit_states(self, job: ScheduleDefinition) -> list[dict[str, str]]:
        states: list[dict[str, str]] = []
        for suffix in ("timer", "service"):
            result = self._call(
                [
                    "show",
                    "--all",
                    f"{job.job_id}.{suffix}",
                    "--property=LoadState,ActiveState,MainPID,FragmentPath,"
                    "DropInPaths,NeedDaemonReload",
                ],
                check=False,
            )
            states.append(
                dict(
                    line.split("=", 1)
                    for line in result.stdout.splitlines()
                    if "=" in line
                )
            )
        return states

    def _registration(self, job: ScheduleDefinition) -> tuple[bool, bool]:
        """Prove persistent graphical-session registration without adopting links."""
        timer = self.timer_path(job)
        link = timer.parent / "graphical-session.target.wants" / timer.name
        linked = link.exists() or link.is_symlink()
        if linked and (
            link.parent.is_symlink()
            or not link.is_symlink()
            or link.lstat().st_uid != os.getuid()
            or link.resolve() != timer.resolve()
        ):
            return False, False
        # Runtime activity survives disable. --full also exposes unrelated
        # registration that disable would otherwise remove along with ours.
        result = self._call(["is-enabled", "--full", timer.name], check=False)
        output = result.stdout.splitlines(keepends=True)
        if (
            result.returncode == 0
            and len(output) == 2
            and output[0] == "enabled\n"
            and output[1].startswith((" ", "\t"))
            and output[1].lstrip(" \t") == f"{link}\n"
        ):
            return linked, linked
        if (result.returncode, result.stdout) in {
            (1, "disabled\n"),
            (4, "not-found\n"),
        }:
            return True, False
        return False, False

    def inspect(self, job: ScheduleDefinition) -> NativeStatus:
        """Verify both files, loaded paths, drop-ins, and live session state."""
        files = self.render(job)
        present = [p.exists() or p.is_symlink() for p in files]
        owned = all(
            not exists or _matches(path, files[path])
            for path, exists in zip(files, present, strict=True)
        )
        states = self._unit_states(job)
        registration_owned, registered = self._registration(job)
        owned = owned and registration_owned
        for suffix, values in zip(("timer", "service"), states, strict=True):
            path = self.timer_path(job) if suffix == "timer" else self.service_path(job)
            if values.get("DropInPaths") or values.get("NeedDaemonReload") == "yes":
                owned = False
            if values.get("FragmentPath") and values["FragmentPath"] != str(path):
                owned = False
        session = not self._call(
            ["is-active", "--quiet", "graphical-session.target"], check=False
        ).returncode
        active = states[0].get("ActiveState") == "active"
        running = states[1].get("ActiveState") in {"active", "activating"}
        return NativeStatus(
            any(present) or active or running,
            owned,
            active and session,
            running,
            (any(present) or active or running)
            and (not all(present) or not registered or (session and not active)),
        )

    def install(self, job: ScheduleDefinition) -> None:
        """Validate both complete units and activate only the owned timer."""
        self.validate_session()
        if not self.inspect(job).owned:
            raise schedule_error("Refusing to replace a job that is not owned.")
        for path, content in self.render(job).items():
            atomic_write(path, content)
        try:
            result = self.command(
                [
                    "/usr/bin/systemd-analyze",
                    "--user",
                    "verify",
                    str(self.service_path(job)),
                    str(self.timer_path(job)),
                ],
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                env=self._environment(),
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise schedule_error(
                "Native systemd definition validation failed."
            ) from exc
        if result.returncode:
            raise schedule_error("Native systemd definition validation failed.")
        self._call(["daemon-reload"])
        self._call(["enable", "--now", f"{job.job_id}.timer"])
        status = self.inspect(job)
        if not status.owned or not status.active or status.partial:
            raise schedule_error("The installed systemd timer could not be verified.")

    def remove(self, job: ScheduleDefinition) -> None:
        """Disable the timer, stop its process tree, and clear persistent state."""
        files = self.render(job)
        if not self._registration(job)[0]:
            raise schedule_error("Refusing to remove a job that is not owned.")
        missing = {
            path for path in files if not path.exists() and not path.is_symlink()
        }
        if missing:
            self._restore_for_cleanup(job, files, missing)
        if not self.inspect(job).owned:
            raise schedule_error("Refusing to remove a job that is not owned.")
        self._call(["disable", "--now", f"{job.job_id}.timer"])
        self._call(["stop", f"{job.job_id}.service"])
        self._call(["clean", "--what=state", f"{job.job_id}.timer"])
        for path in self.render(job):
            path.unlink(missing_ok=True)
        self._call(["daemon-reload"])

    def _restore_for_cleanup(
        self,
        job: ScheduleDefinition,
        files: dict[Path, bytes],
        missing: set[Path],
    ) -> None:
        # clean needs a loaded timer; recreating its exact inactive definition
        # avoids guessing the native persistent timestamp's filesystem location.
        if any(
            path not in missing and not _matches(path, content)
            for path, content in files.items()
        ):
            raise schedule_error("Refusing recovery of edited native definitions.")
        paths = (self.timer_path(job), self.service_path(job))

        def validate(*, after_reload: bool = False) -> None:
            for path, state in zip(paths, self._unit_states(job), strict=True):
                load = state.get("LoadState")
                if (
                    load not in {"loaded", "not-found"}
                    or state.get("ActiveState") not in {"inactive", "failed"}
                    or (path.suffix == ".service" and state.get("MainPID") != "0")
                    or state.get("DropInPaths") != ""
                    or state.get("FragmentPath")
                    != (str(path) if load == "loaded" else "")
                    or (after_reload and path in missing and load != "not-found")
                ):
                    raise schedule_error(
                        "Cannot prove absent native definitions are inactive and owned."
                    )

        validate()
        # Evict stale inactive manager cache after an earlier unlink/reload failure.
        self._call(["daemon-reload"])
        validate(after_reload=True)
        for path in missing:
            atomic_write(path, files[path])
        self._call(["daemon-reload"])
        status = self.inspect(job)
        if not status.owned or status.active or status.running:
            raise schedule_error(
                "Recovered native cleanup definitions could not be verified."
            )


def native_adapter() -> LaunchdScheduleAdapter | SystemdScheduleAdapter:
    """Select the supported native desktop mechanism without fallback jobs."""
    if sys.platform == "darwin":
        return LaunchdScheduleAdapter()
    if sys.platform == "linux":
        return SystemdScheduleAdapter()
    raise schedule_error("Scheduled sync supports macOS and systemd Linux desktops.")
