"""Schedule CLI receipts and real pull completion boundary."""

import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_models import PullResult

pytestmark = pytest.mark.unit
runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_keyring_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "keyring.util.platform_.config_root", lambda: tmp_path / "keyring"
    )


def test_bound_scheduled_failure_reaches_real_configured_sanitized_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.cli.commands import sync
    from moneybin.config import clear_settings_cache, set_current_profile
    from moneybin.errors import UserError
    from moneybin.services.sync_schedule import SyncScheduleService
    from moneybin.services.sync_schedule_native import NativeStatus

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    profile = tmp_path / "profiles/alice"
    profile.mkdir(parents=True)
    (profile / "profile_id").write_text("0123456789ab")
    log_path = tmp_path / "custom-logs/moneybin.log"
    (profile / "config.yaml").write_text(
        f"logging:\n  log_to_file: true\n  log_file_path: {log_path}\n"
    )
    set_current_profile("alice")
    clear_settings_cache()
    logging_at_binding: list[bool] = []

    def inspect(job: object) -> NativeStatus:
        logging_at_binding.append(bool(list(log_path.parent.glob("cli_*.log"))))
        return NativeStatus(True, True, True, False)

    adapter = Mock(mechanism="launchd", inspect=Mock(side_effect=inspect))
    service = SyncScheduleService(
        home=tmp_path, profile="alice", adapter=adapter, launcher=("/usr/bin/true",)
    )
    service.set("09:00")
    logging_at_binding.clear()
    job = adapter.install.call_args.args[0]
    monkeypatch.setattr(
        "moneybin.services.sync_schedule_native.native_adapter", lambda: adapter
    )

    @contextmanager
    def guard(addressing: object = None) -> Generator[None, None, None]:
        yield

    monkeypatch.setattr("moneybin.secrets.scheduled_secret_access", guard)

    def client(*, profile_id: str) -> None:
        assert profile_id == "0123456789ab"
        logging.getLogger("moneybin.scheduled.regression").error(
            "scheduled diagnostic account 123456789"
        )
        raise UserError("Synthetic scheduled failure", code="sync_error")

    monkeypatch.setattr(sync, "_build_sync_client", client)
    db = Mock(side_effect=AssertionError("failure must not open DB"))
    monkeypatch.setattr("moneybin.database.get_database", db)
    monkeypatch.setattr("moneybin.database.database_was_written", lambda: False)
    root = logging.getLogger()
    old_handlers, old_level = list(root.handlers), root.level
    try:
        result = runner.invoke(
            app,
            [
                "--profile",
                "alice",
                "sync",
                "pull",
                "--scheduled",
                "--scheduled-profile-id",
                "0123456789ab",
                "--scheduled-revision",
                job.revision,
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 1, result.output
        assert logging_at_binding and logging_at_binding[0] is False
        logs = list(log_path.parent.glob("cli_*.log"))
        assert len(logs) == 1
        content = logs[0].read_text()
        assert "scheduled diagnostic account" in content
        assert "123456789" not in content
        assert "****...6789" in content
        db.assert_not_called()
    finally:
        for handler in root.handlers:
            if handler not in old_handlers:
                handler.close()
        root.handlers = old_handlers
        root.setLevel(old_level)


@pytest.mark.parametrize("failure", ["missing", "replacement", "logging"])
def test_scheduled_initialization_failure_discards_metrics_before_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    import moneybin.observability as observability
    from moneybin.config import clear_settings_cache, set_current_profile
    from moneybin.services.sync_schedule import SyncScheduleService
    from moneybin.services.sync_schedule_native import NativeStatus

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    profile = tmp_path / "profiles/alice"
    profile.mkdir(parents=True)
    identity = profile / "profile_id"
    identity.write_text("0123456789ab")
    set_current_profile("alice")
    clear_settings_cache()
    adapter = Mock(
        mechanism="launchd",
        inspect=Mock(return_value=NativeStatus(True, True, True, False)),
    )
    service = SyncScheduleService(
        home=tmp_path, profile="alice", adapter=adapter, launcher=("/usr/bin/true",)
    )
    service.set("09:00")
    job = adapter.install.call_args.args[0]
    if failure == "replacement":
        identity.write_text("abcdef012345")
    elif failure == "missing":
        identity.unlink()
        profile.rmdir()
    monkeypatch.setattr(
        "moneybin.services.sync_schedule_native.native_adapter", lambda: adapter
    )
    resolver = Mock(
        side_effect=AssertionError("invalid binding invoked profile resolver")
    )
    secret = Mock(
        side_effect=AssertionError("invalid binding entered credential scope")
    )
    monkeypatch.setattr("moneybin.cli.main.resolve_profile", resolver)
    monkeypatch.setattr("moneybin.secrets.scheduled_secret_access", secret)
    db = Mock(
        side_effect=AssertionError("initialization failure must not flush DB metrics")
    )
    monkeypatch.setattr("moneybin.database.get_database", db)
    monkeypatch.setattr("moneybin.database.database_was_written", lambda: True)
    if failure == "logging":
        monkeypatch.setattr(
            observability,
            "setup_observability",
            Mock(side_effect=OSError("synthetic logging setup failure")),
        )
    result = runner.invoke(
        app,
        [
            "--profile",
            "alice",
            "sync",
            "pull",
            "--scheduled",
            "--scheduled-profile-id",
            "0123456789ab",
            "--scheduled-revision",
            job.revision,
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 1, result.output
    resolver.assert_not_called()
    secret.assert_not_called()
    db.assert_not_called()
    assert not observability._atexit_registered  # pyright: ignore[reportPrivateUsage]  # prove real shutdown hook was discarded
    assert not (profile / "logs").exists()
    assert profile.exists() is (failure != "missing")


@pytest.mark.parametrize("command", ["set", "show", "remove"])
def test_schedule_commands_are_visible(command: str) -> None:
    result = runner.invoke(app, ["sync", "schedule", "--help"])
    assert result.exit_code == 0
    assert command in result.output
    result = runner.invoke(app, ["sync", "--help"])
    assert "schedule" in result.output


def test_schedule_cli_reports_and_repairs_active_partial_registration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.cli.commands import sync
    from moneybin.services.sync_schedule import SyncScheduleService
    from moneybin.services.sync_schedule_native import NativeStatus

    profile = tmp_path / "profiles/alice"
    profile.mkdir(parents=True)
    (profile / "profile_id").write_text("0123456789ab")
    adapter = Mock(mechanism="systemd")
    adapter.inspect.return_value = NativeStatus(True, True, True, False)
    service = SyncScheduleService(
        home=tmp_path, profile="alice", adapter=adapter, launcher=("/usr/bin/true",)
    )
    service.set("09:00")
    original = adapter.install.call_args.args[0]
    adapter.inspect.return_value = NativeStatus(True, True, True, False, partial=True)
    monkeypatch.setattr(sync, "_build_schedule_service", lambda: service)
    result = runner.invoke(app, ["sync", "schedule", "show", "--output", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["state"] == "partial"

    def install(job: object) -> None:
        adapter.inspect.return_value = NativeStatus(True, True, True, False)

    adapter.install.side_effect = install
    result = runner.invoke(
        app, ["sync", "schedule", "set", "--time", "09:00", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["state"] == "installed"
    assert adapter.install.call_args.args[0].revision != original.revision


def test_stale_profile_metadata_commands_need_no_first_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("first run is forbidden for metadata commands")

    monkeypatch.setattr("moneybin.cli.utils.resolve_profile", forbidden)
    for command in ("show", "remove"):
        result = runner.invoke(
            app, ["--profile", "gone", "sync", "schedule", command, "--output", "json"]
        )
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["data"]["state"] == "absent"
    assert not (tmp_path / "profiles").exists()


def test_time_rejected_before_any_profile_or_native_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    result = runner.invoke(
        app,
        [
            "--profile",
            "gone",
            "sync",
            "schedule",
            "set",
            "--time",
            "25:99",
            "--output",
            "json",
        ],
    )
    assert result.exit_code != 0
    assert "HH:MM" in result.output
    assert not (tmp_path / "profiles").exists()


def test_schedule_diagnostics_follow_persisted_logging_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    profile = tmp_path / "profiles/alice"
    profile.mkdir(parents=True)
    log_path = tmp_path / "custom-logs/cli.log"
    (profile / "config.yaml").write_text(f"logging:\n  log_file_path: {log_path}\n")
    result = runner.invoke(
        app, ["--profile", "alice", "sync", "schedule", "show", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["diagnostic_location"] == str(
        log_path.parent
    )
    assert not log_path.parent.exists()


def test_schedule_show_reports_unreadable_os_time_zone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unavailable(*args: object, **kwargs: object) -> None:
        raise OSError("synthetic unavailable localtime")

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    monkeypatch.setattr(Path, "open", unavailable)

    result = runner.invoke(
        app, ["--profile", "alice", "sync", "schedule", "show", "--output", "json"]
    )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "sync_schedule_error"
    assert "OS local time zone" in payload["error"]["message"]


def test_schedule_text_json_have_same_operational_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.services.sync_schedule import ScheduledAttempt, ScheduleResult

    result = ScheduleResult(
        state="installed",
        mechanism="launchd",
        time="09:00",
        timezone="EDT",
        nominal_next_run="2026-10-19T09:00:00-04:00",
        diagnostic_location=str(tmp_path / "logs"),
        latest_attempt=ScheduledAttempt(
            state="failed",
            started_at=datetime(2026, 10, 18, tzinfo=UTC),
            exit_status=1,
            recovery_code="sync_failed",
        ),
    )
    service = Mock(show=Mock(return_value=result))
    monkeypatch.setattr(
        "moneybin.cli.commands.sync._build_schedule_service", lambda: service
    )
    human = runner.invoke(app, ["sync", "schedule", "show"])
    structured = runner.invoke(app, ["sync", "schedule", "show", "--output", "json"])
    assert human.exit_code == structured.exit_code == 0
    payload = json.loads(structured.stdout)["data"]
    for value in ("installed", "launchd", "09:00", "EDT", "failed", "sync_failed"):
        assert value in human.stdout
    assert payload["state"] == "installed"
    assert payload["latest_attempt"]["exit_status"] == 1


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("success", 0),
        ("auth", 1),
        ("offline", 1),
        ("writer", 1),
        ("refresh", 1),
        ("cancelled", 130),
    ],
)
def test_existing_pull_completion_is_scheduled_receipt_boundary(
    mode: str, expected: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.connectors.sync_models import PullResult
    from moneybin.errors import UserError

    @contextmanager
    def service() -> Generator[Mock, None, None]:
        if mode in {"auth", "offline", "writer"}:
            raise UserError("Synthetic sync refusal", code="sync_error")
        if mode == "cancelled":
            raise KeyboardInterrupt
        yield Mock(
            pull=Mock(
                return_value=PullResult(
                    job_id="synthetic",
                    transactions_loaded=0,
                    accounts_loaded=0,
                    balances_loaded=0,
                    transactions_removed=0,
                    institutions=[],
                    transforms_error="Synthetic refresh failure"
                    if mode == "refresh"
                    else None,
                )
            )
        )

    monkeypatch.setattr("moneybin.cli.commands.sync._build_sync_service", service)
    result = runner.invoke(app, ["sync", "pull", "--output", "json"])
    assert result.exit_code == expected, result.output


@pytest.mark.parametrize("failure", ["none", "entry", "body"])
def test_scheduled_pull_guards_service_cleanup_and_metrics(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    events: list[str] = []

    @contextmanager
    def guard(addressing: object = None) -> Generator[None, None, None]:
        events.append("enter")
        if failure == "entry":
            raise RuntimeError("synthetic guard refusal")
        try:
            yield
        finally:
            events.append("exit")

    @contextmanager
    def service() -> Generator[Mock, None, None]:
        assert events == ["enter"]
        try:
            if failure == "body":
                raise RuntimeError("synthetic pull refusal")
            yield Mock(
                pull=Mock(
                    return_value=PullResult(
                        job_id="synthetic",
                        transactions_loaded=0,
                        accounts_loaded=0,
                        balances_loaded=0,
                        transactions_removed=0,
                        institutions=[],
                    )
                )
            )
        finally:
            events.append("close")

    def finish(*, persist: bool = True) -> None:
        events.append("flush" if persist else "discard")

    monkeypatch.setattr(
        "moneybin.secrets.scheduled_secret_access", guard, raising=False
    )
    monkeypatch.setattr(
        "moneybin.observability.finish_cli_metrics", finish, raising=False
    )
    monkeypatch.setattr("moneybin.cli.commands.sync._build_sync_service", service)
    result = runner.invoke(app, ["sync", "pull", "--scheduled", "--output", "json"])
    assert result.exit_code == (0 if failure == "none" else 1), result.output
    assert events == (
        ["enter", "discard"]
        if failure == "entry"
        else ["enter", "close", "flush", "exit"]
    )


def test_scheduled_database_refusal_does_not_recommend_env_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.database import DatabaseKeyError

    @contextmanager
    def guard(addressing: object = None) -> Generator[None, None, None]:
        yield

    monkeypatch.setattr("moneybin.secrets.scheduled_secret_access", guard)
    monkeypatch.setattr("moneybin.observability.finish_cli_metrics", Mock())
    monkeypatch.setattr(
        "moneybin.cli.commands.sync._build_sync_service",
        Mock(
            side_effect=DatabaseKeyError("keychain or env var MONEYBIN_ENCRYPTION_KEY")
        ),
    )
    result = runner.invoke(app, ["sync", "pull", "--scheduled", "--output", "json"])
    assert result.exit_code == 1, result.output
    assert "MONEYBIN_ENCRYPTION_KEY" not in result.output
    assert "env var" not in result.output
    assert "keychain" in result.output.lower()


def test_scheduled_store_refusal_names_only_supported_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.secrets import SecretStorageUnavailableError

    monkeypatch.setattr(
        "moneybin.secrets.scheduled_secret_access",
        Mock(side_effect=SecretStorageUnavailableError("synthetic unavailable store")),
    )
    monkeypatch.setattr("moneybin.observability.finish_cli_metrics", Mock())
    result = runner.invoke(app, ["sync", "pull", "--scheduled", "--output", "json"])
    assert result.exit_code == 1, result.output
    assert "KWallet" not in result.output
    assert "macOS Keychain or Linux Secret Service" in result.output


@pytest.mark.parametrize("replacement", ["before", "client", "database"])
def test_scheduled_construction_never_adopts_replacement_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    from moneybin.cli.commands import sync
    from moneybin.errors import UserError

    identity = tmp_path / "profile_id"
    identity.write_text("abcdef012345" if replacement == "before" else "0123456789ab")

    def validate(*, profile_id: str, revision: str) -> None:
        assert (profile_id, revision) == ("0123456789ab", "1" * 32)
        if identity.read_text() != profile_id:
            raise UserError("Stale scheduled profile", code="sync_error")

    fence = Mock(validate_pull_binding=validate)
    monkeypatch.setattr(sync, "_build_schedule_service", lambda: fence)

    def client(*, profile_id: str) -> Mock:
        assert profile_id == "0123456789ab"
        if replacement == "client":
            identity.write_text("abcdef012345")
        return Mock()

    build = Mock(side_effect=client)
    monkeypatch.setattr(sync, "_build_sync_client", build)

    @contextmanager
    def database(**kwargs: object) -> Generator[Mock, None, None]:
        if replacement == "database":
            identity.write_text("abcdef012345")
        yield Mock()

    db = Mock(side_effect=database)
    monkeypatch.setattr("moneybin.database.get_database", db)
    with (
        pytest.raises(UserError),
        sync._build_sync_service(  # pyright: ignore[reportPrivateUsage]  # exercise construction fence
            scheduled_profile_id="0123456789ab",
            scheduled_revision="1" * 32,
        ),
    ):
        pytest.fail("a replaced profile reached the sync service")
    assert build.call_count == int(replacement != "before")
    assert db.call_count == int(replacement == "database")


def test_existing_client_factory_uses_explicit_identity_without_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.services import sync_server_address as address

    create = Mock(
        side_effect=AssertionError("scheduled identity must never be created")
    )
    monkeypatch.setattr(address, "get_or_create_profile_id", create)
    constructor = Mock()
    monkeypatch.setattr(address, "SyncClient", constructor)
    address._new_client("https://synthetic.invalid", profile_id="0123456789ab")  # pyright: ignore[reportPrivateUsage]  # factory authority boundary
    constructor.assert_called_once_with(
        server_url="https://synthetic.invalid", profile_id="0123456789ab"
    )
    create.assert_not_called()


def test_scheduled_child_passes_binding_to_service_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def guard(addressing: object = None) -> Generator[None, None, None]:
        yield

    @contextmanager
    def service(**kwargs: object) -> Generator[Mock, None, None]:
        assert kwargs == {
            "scheduled_profile_id": "0123456789ab",
            "scheduled_revision": "1" * 32,
        }
        yield Mock(
            pull=Mock(
                return_value=PullResult(
                    job_id="synthetic",
                    transactions_loaded=0,
                    accounts_loaded=0,
                    balances_loaded=0,
                    transactions_removed=0,
                    institutions=[],
                )
            )
        )

    monkeypatch.setattr("moneybin.secrets.scheduled_secret_access", guard)
    monkeypatch.setattr("moneybin.observability.finish_cli_metrics", Mock())
    build = Mock(side_effect=service)
    monkeypatch.setattr("moneybin.cli.commands.sync._build_sync_service", build)
    monkeypatch.setattr(
        "moneybin.cli.commands.sync._build_schedule_service", lambda: Mock()
    )
    result = runner.invoke(
        app,
        [
            "sync",
            "pull",
            "--scheduled",
            "--scheduled-profile-id",
            "0123456789ab",
            "--scheduled-revision",
            "1" * 32,
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    build.assert_called_once()


def test_stale_binding_cannot_reopen_database_for_shutdown_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.errors import UserError

    @contextmanager
    def guard(addressing: object = None) -> Generator[None, None, None]:
        yield

    @contextmanager
    def service(**kwargs: object) -> Generator[Mock, None, None]:
        raise UserError("Stale scheduled profile", code="sync_schedule_error")
        yield Mock()  # pragma: no cover  # contextmanager declaration after refusal

    db = Mock(side_effect=AssertionError("stale metrics must not open a database"))

    def finish(*, persist: bool = True) -> None:
        if persist:
            db()

    stale = Mock(
        validate_pull_binding=Mock(
            side_effect=UserError("Stale scheduled profile", code="sync_schedule_error")
        )
    )
    monkeypatch.setattr("moneybin.secrets.scheduled_secret_access", guard)
    monkeypatch.setattr("moneybin.observability.finish_cli_metrics", finish)
    monkeypatch.setattr(
        "moneybin.cli.commands.sync._build_schedule_service", lambda: stale
    )
    monkeypatch.setattr("moneybin.cli.commands.sync._build_sync_service", service)
    result = runner.invoke(
        app,
        [
            "sync",
            "pull",
            "--scheduled",
            "--scheduled-profile-id",
            "0123456789ab",
            "--scheduled-revision",
            "1" * 32,
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 1, result.output
    db.assert_not_called()
