"""Strict background access reuses supported stores without UI or fallback."""

import ctypes
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from keyring.backends.macOS import Keyring
from keyring.errors import KeyringLocked, PasswordDeleteError

from moneybin.secrets import (
    SecretNotFoundError,
    SecretStorageUnavailableError,
    SecretStore,
    SecretUnavailableError,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def stock_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Keyring, list[str]]:
    import moneybin.secrets as secrets

    backend = Keyring()
    events: list[str] = []

    @contextmanager
    def guard() -> Generator[None, None, None]:
        events.append("guarded")
        try:
            yield
        finally:
            events.append("restored")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: tmp_path)
    monkeypatch.setattr(secrets.keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(secrets, "_macos_interaction_guard", guard, raising=False)
    monkeypatch.setattr(secrets, "_macos_require_unlocked", lambda: None, raising=False)
    return backend, events


def test_strict_reads_and_new_thread_stores_are_guarded(
    stock_store: tuple[Keyring, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from moneybin.secrets import scheduled_secret_access

    backend, events = stock_store
    before = SecretStore(profile="alice")

    def read(service: str, username: str) -> str:
        assert events == ["guarded"]
        assert service == "moneybin-alice"
        return "synthetic-value"

    monkeypatch.setattr(backend, "get_password", read)
    with scheduled_secret_access():
        with scheduled_secret_access():
            assert before.get_key("test-key") == "synthetic-value"
            assert before.has_keychain_entry("test-key")
            with ThreadPoolExecutor(max_workers=1) as pool:
                assert (
                    pool.submit(SecretStore("alice").get_key, "test-key").result()
                    == "synthetic-value"
                )
    assert events == ["guarded", "restored"]


@pytest.mark.parametrize("failure", [None, KeyringLocked("locked")])
def test_preexisting_store_never_uses_env_in_strict_scope(
    stock_store: tuple[Keyring, list[str]],
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception | None,
) -> None:
    from moneybin.secrets import scheduled_secret_access

    backend, _ = stock_store
    store = SecretStore("alice")
    monkeypatch.setenv(store.env_var_name("test-key"), "forbidden-fallback")
    monkeypatch.setattr(
        backend, "get_password", Mock(return_value=None, side_effect=failure)
    )
    with scheduled_secret_access(), pytest.raises(SecretNotFoundError) as error:
        store.get_key("test-key")
    assert "env var" not in str(error.value)
    assert "forbidden-fallback" not in str(error.value)


def test_environment_only_secret_is_refused_in_strict_scope(
    stock_store: tuple[Keyring, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.secrets import scheduled_secret_access

    monkeypatch.setenv("MONEYBIN_SYNC__API_KEY", "forbidden-fallback")
    with scheduled_secret_access(), pytest.raises(SecretNotFoundError):
        SecretStore("alice").get_env("SYNC__API_KEY")


def test_write_delete_and_failure_verification_stay_guarded(
    stock_store: tuple[Keyring, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.secrets import scheduled_secret_access

    backend, events = stock_store
    store = SecretStore.for_sync("0123456789ab")

    def write(service: str, username: str, value: str) -> None:
        assert events == ["guarded"]
        assert (service, username) == ("moneybin-sync", "0123456789ab:access_token")

    def delete(service: str, username: str) -> None:
        assert events == ["guarded"]
        raise PasswordDeleteError("synthetic refused deletion")

    def read(service: str, username: str) -> str:
        assert events == ["guarded"]
        return "still-present"

    monkeypatch.setattr(backend, "set_password", write)
    monkeypatch.setattr(backend, "delete_password", delete)
    monkeypatch.setattr(backend, "get_password", read)
    with scheduled_secret_access():
        store.set_key("access_token", "synthetic")
        with pytest.raises(SecretStorageUnavailableError):
            store.delete_key("access_token")
    assert events[-1] == "restored"


def test_exception_teardown_clears_db_key_cache(
    stock_store: tuple[Keyring, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.secrets import scheduled_secret_access

    clear = Mock()
    monkeypatch.setattr("moneybin.database.invalidate_encryption_key_cache", clear)
    with pytest.raises(RuntimeError), scheduled_secret_access():
        assert clear.call_count == 1
        raise RuntimeError("synthetic failure")
    assert clear.call_count == 2
    assert stock_store[1] == ["guarded", "restored"]


@pytest.mark.parametrize(
    "selector", ["keyring.backends.kwallet.DBusKeyring", "custom.Backend"]
)
def test_unsupported_persisted_selector_is_refused_before_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selector: str
) -> None:
    from moneybin.secrets import scheduled_secret_access

    (tmp_path / "keyringrc.cfg").write_text(f"[backend]\ndefault-keyring={selector}\n")
    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: tmp_path)
    discovery = Mock(side_effect=AssertionError("unsupported discovery ran"))
    monkeypatch.setattr("moneybin.secrets.keyring.get_keyring", discovery)
    with pytest.raises(SecretStorageUnavailableError), scheduled_secret_access():
        pytest.fail("unsupported backend was accepted")
    discovery.assert_not_called()


def test_third_party_discovery_is_refused_before_priority_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.secrets import scheduled_secret_access

    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: tmp_path)
    monkeypatch.setattr(
        "moneybin.secrets.entry_points",
        Mock(return_value=[Mock(dist=Mock(name="other"))]),
    )
    discovery = Mock(side_effect=AssertionError("plugin priority ran"))
    monkeypatch.setattr("moneybin.secrets.keyring.get_keyring", discovery)
    with pytest.raises(SecretStorageUnavailableError), scheduled_secret_access():
        pytest.fail("third-party discovery was accepted")
    discovery.assert_not_called()


@pytest.mark.parametrize(
    "failure", [RuntimeError("unavailable"), ImportError("missing")]
)
def test_explicit_bound_loader_refusal_is_controlled_and_guarded(
    stock_store: tuple[Keyring, list[str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    from moneybin.secrets import (
        capture_scheduled_secret_addressing,
        scheduled_secret_access,
    )

    backend, events = stock_store
    (tmp_path / "keyringrc.cfg").write_text(
        "[backend]\ndefault-keyring=keyring.backends.macOS.Keyring\n"
    )
    addressing = capture_scheduled_secret_addressing()

    def load(selector: str) -> None:
        assert selector == "keyring.backends.macOS.Keyring"
        assert events == ["guarded"]
        raise failure

    monkeypatch.setattr("keyring.core.load_keyring", load)
    credentials = Mock(side_effect=AssertionError("credential method ran"))
    db = Mock(side_effect=AssertionError("database opened"))
    monkeypatch.setattr(backend, "get_password", credentials)
    monkeypatch.setattr("moneybin.database.get_database", db)
    with (
        pytest.raises(SecretStorageUnavailableError),
        scheduled_secret_access(addressing),
    ):
        pytest.fail("unavailable loader was accepted")
    assert events == ["guarded", "restored"]
    credentials.assert_not_called()
    db.assert_not_called()


def test_selected_chain_is_not_replaced_by_stock_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from keyring.backends.chainer import ChainerBackend

    from moneybin.secrets import scheduled_secret_access

    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: tmp_path)
    monkeypatch.setattr(
        "moneybin.secrets.keyring.get_keyring", lambda: ChainerBackend()
    )
    with pytest.raises(SecretStorageUnavailableError), scheduled_secret_access():
        pytest.fail("chain was accepted")


def test_backend_identity_cannot_be_spoofed_by_class_name(
    stock_store: tuple[Keyring, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.secrets import scheduled_secret_access

    impostor = type("Keyring", (), {"__module__": "keyring.backends.macOS"})()
    monkeypatch.setattr("moneybin.secrets.keyring.get_keyring", lambda: impostor)
    with pytest.raises(SecretStorageUnavailableError), scheduled_secret_access():
        pytest.fail("a backend with a spoofed class name was accepted")


def test_unreadable_persisted_backend_configuration_refuses_before_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.secrets import scheduled_secret_access

    (tmp_path / "keyringrc.cfg").write_text("[backend]\n")
    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: tmp_path)
    monkeypatch.setattr(
        "moneybin.secrets.ConfigParser.read_file",
        Mock(side_effect=OSError("unreadable")),
    )
    discovery = Mock()
    monkeypatch.setattr("moneybin.secrets.keyring.get_keyring", discovery)
    with pytest.raises(SecretStorageUnavailableError), scheduled_secret_access():
        pytest.fail("unreadable configuration was ignored")
    discovery.assert_not_called()


@pytest.mark.parametrize("initial", [0, 1])
def test_native_macos_guard_binds_status_and_restores_prior_state(
    monkeypatch: pytest.MonkeyPatch, initial: int
) -> None:
    from moneybin.secrets import _macos_interaction_guard  # pyright: ignore[reportPrivateUsage]  # native binding boundary

    native = Mock()

    def get(pointer: Any) -> int:
        ctypes.cast(pointer, ctypes.POINTER(ctypes.c_ubyte)).contents.value = initial
        return 0

    native.SecKeychainGetUserInteractionAllowed.side_effect = get
    native.SecKeychainSetUserInteractionAllowed.return_value = 0
    monkeypatch.setattr("moneybin.secrets.ctypes.CDLL", Mock(return_value=native))
    with _macos_interaction_guard():
        native.SecKeychainSetUserInteractionAllowed.assert_called_once_with(0)
    assert [
        call.args[0]
        for call in native.SecKeychainSetUserInteractionAllowed.call_args_list
    ] == [0, initial]
    assert native.SecKeychainGetUserInteractionAllowed.argtypes == [
        ctypes.POINTER(ctypes.c_ubyte)
    ]
    assert native.SecKeychainSetUserInteractionAllowed.restype is ctypes.c_int32


def test_native_guard_failure_prevents_backend_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.secrets import _macos_interaction_guard  # pyright: ignore[reportPrivateUsage]  # native binding boundary

    native = Mock()
    native.SecKeychainGetUserInteractionAllowed.return_value = 0
    native.SecKeychainSetUserInteractionAllowed.return_value = -1
    monkeypatch.setattr("moneybin.secrets.ctypes.CDLL", Mock(return_value=native))
    with pytest.raises(SecretStorageUnavailableError), _macos_interaction_guard():
        pytest.fail("failed native suppression was accepted")


def test_native_guard_restore_failure_retains_operation_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.secrets import _macos_interaction_guard  # pyright: ignore[reportPrivateUsage]  # native binding boundary

    native = Mock()
    native.SecKeychainGetUserInteractionAllowed.return_value = 0
    native.SecKeychainSetUserInteractionAllowed.side_effect = [0, -1]
    monkeypatch.setattr("moneybin.secrets.ctypes.CDLL", Mock(return_value=native))

    with pytest.raises(SecretStorageUnavailableError) as raised:
        with _macos_interaction_guard():
            raise KeyringLocked("locked")

    assert isinstance(raised.value.__context__, KeyringLocked)


@pytest.mark.parametrize("operation", ["read", "write", "delete"])
def test_observed_locked_macos_store_refuses_before_backend_operation(
    stock_store: tuple[Keyring, list[str]],
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    from moneybin.secrets import scheduled_secret_access

    backend, _ = stock_store
    check = Mock(side_effect=KeyringLocked("locked"))
    monkeypatch.setattr("moneybin.secrets._macos_require_unlocked", check)
    read, write, delete = Mock(), Mock(), Mock()
    monkeypatch.setattr(backend, "get_password", read)
    monkeypatch.setattr(backend, "set_password", write)
    monkeypatch.setattr(backend, "delete_password", delete)
    with (
        scheduled_secret_access(),
        pytest.raises((SecretUnavailableError, SecretStorageUnavailableError)),
    ):
        store = SecretStore("alice")
        if operation == "read":
            store.get_key("key")
        elif operation == "write":
            store.set_key("key", "synthetic")
        else:
            store.delete_key("key")
    read.assert_not_called()
    write.assert_not_called()
    delete.assert_not_called()


@pytest.mark.parametrize("locked", [101, 202])
def test_native_lock_check_covers_default_and_unrelated_search_list(
    monkeypatch: pytest.MonkeyPatch,
    locked: int,
) -> None:
    from moneybin.secrets import _macos_require_unlocked  # pyright: ignore[reportPrivateUsage]  # native boundary

    native, core = Mock(), Mock()

    def copy_default(pointer: Any) -> int:
        ctypes.cast(pointer, ctypes.POINTER(ctypes.c_void_p)).contents.value = 101
        return 0

    def copy_list(pointer: Any) -> int:
        ctypes.cast(pointer, ctypes.POINTER(ctypes.c_void_p)).contents.value = 999
        return 0

    def status(reference: int, pointer: Any) -> int:
        ctypes.cast(pointer, ctypes.POINTER(ctypes.c_uint32)).contents.value = int(
            reference != locked
        )
        return 0

    native.SecKeychainCopyDefault.side_effect = copy_default
    native.SecKeychainCopySearchList.side_effect = copy_list
    native.SecKeychainGetStatus.side_effect = status
    core.CFArrayGetCount.return_value = 1
    core.CFArrayGetValueAtIndex.return_value = 202

    def load_native(name: str) -> Any:
        return core if "CoreFoundation" in name else native

    monkeypatch.setattr("moneybin.secrets.ctypes.CDLL", load_native)
    with pytest.raises(KeyringLocked):
        _macos_require_unlocked()
    assert {call.args[0] for call in core.CFRelease.call_args_list} == {101, 999}


def test_native_unknown_lock_status_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.secrets import _macos_require_unlocked  # pyright: ignore[reportPrivateUsage]  # native boundary

    native = Mock()
    native.SecKeychainCopyDefault.return_value = -1
    monkeypatch.setattr("moneybin.secrets.ctypes.CDLL", Mock(return_value=native))
    with pytest.raises(KeyringLocked):
        _macos_require_unlocked()


@pytest.mark.parametrize("operation", ["write", "delete"])
def test_observed_lock_after_mutation_is_failure(
    stock_store: tuple[Keyring, list[str]],
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    from moneybin.secrets import scheduled_secret_access

    backend, _ = stock_store
    checks = Mock(side_effect=[None, KeyringLocked("racing lock")])
    monkeypatch.setattr("moneybin.secrets._macos_require_unlocked", checks)
    monkeypatch.setattr(backend, "get_password", Mock(return_value="synthetic"))
    mutation = Mock()
    monkeypatch.setattr(
        backend, "set_password" if operation == "write" else "delete_password", mutation
    )
    with scheduled_secret_access(), pytest.raises(SecretStorageUnavailableError):
        store = SecretStore("alice")
        if operation == "write":
            store.set_key("key", "synthetic")
        else:
            store.delete_key("key")
    mutation.assert_called_once()
