"""Tests for saving and resolving the per-profile sync server address."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest
import yaml

from moneybin import error_codes
from moneybin.config import clear_settings_cache, get_settings, set_current_profile
from moneybin.connectors.sync_errors import SyncAPIError
from moneybin.connectors.sync_models import DeviceAuthorizationChallenge
from moneybin.errors import UserError
from moneybin.services import sync_server_address as address
from moneybin.services.profile_service import ProfileService

_A = "https://a.example.test"
_B = "https://b.example.test"


class _FakeClient:
    def __init__(self, server_url: str, *, reachable: bool = True) -> None:
        self.server_url = server_url
        self._reachable = reachable

    def begin_login(self) -> DeviceAuthorizationChallenge:
        if not self._reachable:
            raise SyncAPIError(f"sync server unreachable at {self.server_url}")
        return DeviceAuthorizationChallenge.model_validate({
            "device_code": "synthetic-device-code",
            "user_code": "ABCD-EFGH",
            "verification_uri": f"{self.server_url}/device",
            "expires_in": 600,
        })


def _unreachable_client(server_url: str) -> _FakeClient:
    return _FakeClient(server_url, reachable=False)


def _skip_database_init(*_args: object, **_kwargs: object) -> None:
    return None


@pytest.fixture
def events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[list[str], None, None]:
    """An isolated `alice` profile; records the order of side effects."""
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    monkeypatch.delenv("MONEYBIN_SYNC__SERVER_URL", raising=False)
    # Profile creation would otherwise initialize a database through the keychain.
    monkeypatch.setattr(ProfileService, "_init_database", _skip_database_init)
    ProfileService().create("alice")
    clear_settings_cache()
    set_current_profile("alice")
    log: list[str] = []

    def new_client(server_url: str) -> _FakeClient:
        log.append(f"contact {server_url}")
        return _FakeClient(server_url)

    def clear_credentials(client: object) -> None:
        log.append("clear credentials")

    monkeypatch.setattr(address, "_new_client", new_client)
    monkeypatch.setattr(address, "_clear_credentials", clear_credentials)
    yield log
    clear_settings_cache()


def _saved(tmp_path: Path) -> object:
    data = yaml.safe_load((tmp_path / "profiles" / "alice" / "config.yaml").read_text())
    return data.get("sync", {}).get("server_url")


class TestNormalizeServerUrl:
    """Only a bare http(s) address is accepted."""

    def test_strips_a_trailing_slash(self) -> None:
        assert address.normalize_server_url(" https://a.example.test/ ") == _A

    def test_keeps_port_and_path(self) -> None:
        url = "http://localhost:8000/sync"
        assert address.normalize_server_url(url) == url

    @pytest.mark.parametrize(
        "raw",
        [
            "a.example.test",
            "ftp://a.example.test",
            "https://",
            "https://user:pw@a.example.test",
            "https://a.example.test/?next=x",
            "https://a.example.test/#frag",
        ],
    )
    def test_refuses_anything_but_a_plain_http_address(self, raw: str) -> None:
        with pytest.raises(UserError) as excinfo:
            address.normalize_server_url(raw)
        assert excinfo.value.code == error_codes.MUTATION_INVALID_INPUT


class TestIsUnencrypted:
    """Plain http is flagged unless it stays on this machine."""

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("https://a.example.test", False),
            ("http://localhost:8000", False),
            ("http://127.0.0.1:8000", False),
            ("http://[::1]:8000", False),
            ("http://192.168.1.5:8000", True),
            ("http://a.example.test", True),
        ],
    )
    def test_only_plain_http_off_loopback(self, url: str, expected: bool) -> None:
        assert address.is_unencrypted(url) is expected


class TestBuildSyncClient:
    """A client is built from the configured address or refused."""

    def test_unconfigured_names_the_login_command_not_an_env_var(
        self, events: list[str]
    ) -> None:
        with pytest.raises(UserError) as excinfo:
            address.build_sync_client()

        assert excinfo.value.code == error_codes.SYNC_SERVER_NOT_CONFIGURED
        assert "moneybin sync login --server" in (excinfo.value.hint or "")
        assert "MONEYBIN_" not in excinfo.value.message
        assert "MONEYBIN_" not in (excinfo.value.hint or "")

    def test_a_blank_override_counts_as_unconfigured(
        self, events: list[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`MONEYBIN_SYNC__SERVER_URL=` in a dotenv must not build a hostless client."""
        monkeypatch.setenv("MONEYBIN_SYNC__SERVER_URL", "")
        clear_settings_cache()
        set_current_profile("alice")

        with pytest.raises(UserError) as excinfo:
            address.build_sync_client()

        assert excinfo.value.code == error_codes.SYNC_SERVER_NOT_CONFIGURED

    def test_uses_the_saved_address(self, events: list[str]) -> None:
        ProfileService().set("alice", "sync.server_url", _A)
        clear_settings_cache()
        set_current_profile("alice")

        client = address.build_sync_client()

        assert client.server_url == _A  # type: ignore[attr-defined]  # fake client


class TestStartLogin:
    """The address is saved only once the server answers."""

    def test_first_address_is_saved_after_the_server_answers(
        self, events: list[str], tmp_path: Path
    ) -> None:
        plan = address.plan_server_address(_A)
        assert plan.change == "first"

        start = address.start_login(plan, surface="cli")

        assert start.outcome == "saved"
        assert start.challenge is not None
        assert _saved(tmp_path) == _A
        assert get_settings().sync.server_url == _A
        # Cleared even with nothing saved before: credentials can outlive an
        # address that came from the environment and has since been unset.
        assert events == [f"contact {_A}", "clear credentials"]

    def test_unreachable_server_is_not_saved(
        self, events: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(address, "_new_client", _unreachable_client)

        with pytest.raises(SyncAPIError):
            address.start_login(address.plan_server_address(_A), surface="cli")

        assert _saved(tmp_path) is None
        assert events == []

    def test_replacing_clears_credentials_before_the_new_address_is_saved(
        self, events: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        address.start_login(address.plan_server_address(_A), surface="cli")
        events.clear()
        saved_when_cleared: list[object] = []

        def record_saved(client: object) -> None:
            saved_when_cleared.append(_saved(tmp_path))

        monkeypatch.setattr(address, "_clear_credentials", record_saved)

        plan = address.plan_server_address(_B)
        assert plan.change == "replace"
        start = address.start_login(plan, surface="cli")

        assert start.outcome == "replaced"
        assert saved_when_cleared == [_A]
        assert _saved(tmp_path) == _B

    def test_replacing_with_an_unreachable_server_keeps_credentials(
        self, events: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        address.start_login(address.plan_server_address(_A), surface="cli")
        events.clear()
        monkeypatch.setattr(address, "_new_client", _unreachable_client)

        with pytest.raises(SyncAPIError):
            address.start_login(address.plan_server_address(_B), surface="cli")

        assert "clear credentials" not in events
        assert _saved(tmp_path) == _A

    def test_same_address_changes_nothing(
        self, events: list[str], tmp_path: Path
    ) -> None:
        address.start_login(address.plan_server_address(_A), surface="cli")
        events.clear()

        plan = address.plan_server_address(_A + "/")
        start = address.start_login(plan, surface="cli")

        assert plan.change == "unchanged"
        assert start.outcome == "unchanged"
        assert start.challenge is None
        assert "clear credentials" not in events


class TestOverrideConflict:
    """A save the environment would shadow is refused."""

    def test_a_different_address_is_refused_while_the_environment_overrides(
        self, events: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MONEYBIN_SYNC__SERVER_URL", _A)
        clear_settings_cache()
        set_current_profile("alice")

        with pytest.raises(UserError) as excinfo:
            address.plan_server_address(_B)

        assert excinfo.value.code == error_codes.SYNC_SERVER_OVERRIDE_CONFLICT
        assert "MONEYBIN_SYNC__SERVER_URL" in excinfo.value.message
        assert _saved(tmp_path) is None

    def test_the_overriding_address_itself_is_unchanged(
        self, events: list[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MONEYBIN_SYNC__SERVER_URL", _A)
        clear_settings_cache()
        set_current_profile("alice")

        assert address.plan_server_address(_A).change == "unchanged"
