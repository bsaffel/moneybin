"""Real SecretStorage APIs over synthetic D-Bus; never connect to a real store."""

import sys
from collections import deque
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest
import respx
from keyring.backends.SecretService import Keyring

from moneybin.secrets import (
    SecretNotFoundError,
    SecretStorageUnavailableError,
    SecretStore,
)

secretstorage = pytest.importorskip("secretstorage", reason="Linux-marked dependency")
jeepney = pytest.importorskip("jeepney", reason="Linux-marked dependency")

pytestmark = pytest.mark.unit


class Vault:
    """Synthetic service state, shared by fresh operation-scoped connections."""

    def __init__(self) -> None:
        """Start unlocked with no stored credentials."""
        self.items: dict[str, tuple[dict[str, str], bytes]] = {}
        self.calls: list[Any] = []
        self.connections: list[Connection] = []
        self.collection_locked = False
        self.item_locked = False
        self.missing_collection = False
        self.prompt_create = False
        self.prompt_delete = False
        self.race_method: str | None = None
        self.prompt_username: str | None = None
        self.lock_after_failed_write = False

    def connect(self) -> "Connection":
        connection = Connection(self)
        self.connections.append(connection)
        return connection


class Connection:
    """Speak the actual Jeepney message API without a socket or secret daemon."""

    def __init__(self, vault: Vault) -> None:
        """Bind this operation's synthetic transport."""
        self.vault = vault
        self.closed = False

    def send_and_get_reply(self, message: Any, *, timeout: float | None = None) -> Any:
        self.vault.calls.append(message)
        fields = message.header.fields
        interface = fields[jeepney.HeaderFields.interface]
        member = fields[jeepney.HeaderFields.member]
        path = fields[jeepney.HeaderFields.path]

        def reply(signature: str, *body: Any) -> Any:
            return jeepney.new_method_return(message, signature, body)

        if interface == "org.freedesktop.DBus" and member == "NameHasOwner":
            return reply("b", True)
        if member == self.vault.race_method:
            return jeepney.new_error(
                message,
                "org.freedesktop.Secret.Error.IsLocked",
                "s",
                ("synthetic locked race",),
            )
        if interface == "org.freedesktop.DBus.Properties":
            prop = message.body[1]
            if (
                self.vault.missing_collection
                and "collection" in message.body[0].lower()
            ):
                return jeepney.new_error(
                    message,
                    "org.freedesktop.DBus.Error.UnknownObject",
                    "s",
                    ("synthetic missing collection",),
                )
            if prop == "Label":
                return reply("v", ("s", "Synthetic"))
            if prop == "Locked":
                locked = (
                    self.vault.item_locked
                    if "Item" in message.body[0]
                    else self.vault.collection_locked
                )
                return reply("v", ("b", locked))
        if member == "SearchItems":
            query = message.body[0]
            matches = [
                p
                for p, (attrs, _) in self.vault.items.items()
                if all(attrs.get(k) == v for k, v in query.items())
            ]
            return reply("ao", matches)
        if member == "OpenSession":
            if message.body[0] != "plain":
                return jeepney.new_error(
                    message,
                    "org.freedesktop.DBus.Error.NotSupported",
                    "s",
                    ("synthetic plaintext session",),
                )
            return reply("vo", ("s", ""), "/session/synthetic")
        if member == "GetSecret":
            return reply(
                "(oayays)",
                ("/session/synthetic", b"", self.vault.items[path][1], "text/plain"),
            )
        if member == "CreateItem":
            properties, secret, replace = message.body
            attrs = properties["org.freedesktop.Secret.Item.Attributes"][1]
            if self.vault.prompt_create or (
                self.vault.prompt_username is not None
                and attrs.get("username") == self.vault.prompt_username
            ):
                if self.vault.lock_after_failed_write:
                    self.vault.collection_locked = True
                return reply("oo", "/", "/prompt/synthetic")
            assert replace is True
            selected = next(
                (p for p, (a, _) in self.vault.items.items() if a == attrs),
                f"/item/{len(self.vault.items) + 1}",
            )
            self.vault.items[selected] = (attrs, bytes(secret[2]))
            return reply("oo", selected, "/")
        if member == "Delete":
            if self.vault.prompt_delete:
                return reply("o", "/prompt/synthetic")
            del self.vault.items[path]
            return reply("o", "/")
        raise AssertionError(f"unexpected synthetic method {interface}.{member}")

    def send(self, message: Any, serial: int | None = None) -> None:
        self.vault.calls.append(message)

    @contextmanager
    def filter(
        self, rule: Any, *, queue: Any = None, bufsize: int = 1
    ) -> Generator[deque[Any], None, None]:
        yield deque()

    def recv_until_filtered(self, queue: Any, *, timeout: float | None = None) -> Any:
        raise AssertionError("a prompt completion wait reached transport")

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Vault, Keyring]:
    import moneybin.secrets as secrets

    synthetic = Vault()
    backend = Keyring()
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr("keyring.util.platform_.config_root", lambda: tmp_path)
    monkeypatch.setattr(secrets.keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(secretstorage, "dbus_init", synthetic.connect)
    monkeypatch.setattr(
        backend,
        "get_preferred_collection",
        Mock(side_effect=AssertionError("interactive backend helper used")),
    )
    return synthetic, backend


def assert_no_ui(vault: Vault) -> None:
    members = {m.header.fields[jeepney.HeaderFields.member] for m in vault.calls}
    assert not members & {"Prompt", "Unlock", "CreateCollection"}
    assert all(c.closed for c in vault.connections)


def test_existing_credential_identity_and_absent_marker_round_trip(
    vault: tuple[Vault, Keyring],
) -> None:
    from moneybin.secrets import scheduled_secret_access

    synthetic, backend = vault
    backend.scheme = "KeePassXC"
    preferred = "/org/freedesktop/secrets/collection/custom"
    cast(Any, backend).preferred_collection = preferred
    store = SecretStore.for_sync("0123456789ab")
    with scheduled_secret_access():
        store.set_key("pending", "synthetic-marker")
        assert store.get_key("pending") == "synthetic-marker"
        assert store.has_keychain_entry("pending")
        store.delete_key("pending")
        assert not store.has_keychain_entry("pending")
    create = next(
        m
        for m in synthetic.calls
        if m.header.fields[jeepney.HeaderFields.member] == "CreateItem"
    )
    assert create.header.fields[jeepney.HeaderFields.path] == preferred
    assert create.body[0]["org.freedesktop.Secret.Item.Attributes"][1] == {
        "Title": "moneybin-sync",
        "UserName": "0123456789ab:pending",
        "application": backend.appid,
    }
    assert (
        create.body[0]["org.freedesktop.Secret.Item.Label"][1]
        == "Password for '0123456789ab:pending' on 'moneybin-sync'"
    )
    assert_no_ui(synthetic)


@pytest.mark.parametrize("explicit_selection", [False, True])
def test_bound_addressing_reconstructs_fresh_stock_backend_without_address_drift(
    vault: tuple[Vault, Keyring],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    explicit_selection: bool,
) -> None:
    import moneybin.secrets as secrets
    from moneybin.secrets import (
        ScheduledSecretAddressing,
        capture_scheduled_secret_addressing,
        scheduled_secret_access,
    )

    synthetic, _ = vault
    if explicit_selection:
        (tmp_path / "keyringrc.cfg").write_text(
            "[backend]\ndefault-keyring=keyring.backends.SecretService.Keyring\n"
        )
    addressing = ScheduledSecretAddressing(
        backend="secret-service",
        configuration=capture_scheduled_secret_addressing().configuration,
        scheme="KeePassXC",
        preferred_collection="/org/freedesktop/secrets/collection/synthetic",
        appid="synthetic-application",
    )
    restored = ScheduledSecretAddressing.model_validate_json(
        addressing.model_dump_json()
    )
    for name, value in restored.environment().items():
        monkeypatch.setenv(name, value)
    if explicit_selection:
        monkeypatch.setattr(
            "keyring.util.platform_.config_root", lambda: tmp_path / "unrelated-config"
        )
        monkeypatch.setattr(
            secrets.keyring,
            "get_keyring",
            Mock(side_effect=AssertionError("bound selection used global discovery")),
        )
    else:
        backend = Keyring()
        monkeypatch.setattr(secrets.keyring, "get_keyring", lambda: backend)
    store = SecretStore.for_sync("0123456789ab")
    with scheduled_secret_access(restored):
        store.set_key("pending", "synthetic-value")
        assert store.get_key("pending") == "synthetic-value"
        store.delete_key("pending")
    assert (
        any(
            m.header.fields[jeepney.HeaderFields.member] == "NameHasOwner"
            for m in synthetic.calls
        )
        is explicit_selection
    )
    created = next(
        m
        for m in synthetic.calls
        if m.header.fields[jeepney.HeaderFields.member] == "CreateItem"
    )
    assert (
        created.header.fields[jeepney.HeaderFields.path]
        == addressing.preferred_collection
    )
    assert created.body[0]["org.freedesktop.Secret.Item.Attributes"][1] == {
        "Title": "moneybin-sync",
        "UserName": "0123456789ab:pending",
        "application": addressing.appid,
    }
    assert_no_ui(synthetic)


@pytest.mark.parametrize(
    "failure",
    [
        "collection",
        "item",
        "missing",
        "missing_preferred",
        "GetSecret",
        "CreateItem",
        "Delete",
    ],
)
def test_lock_miss_and_mid_operation_races_fail_without_unlocking(
    vault: tuple[Vault, Keyring],
    failure: str,
) -> None:
    from moneybin.secrets import scheduled_secret_access

    synthetic, backend = vault
    synthetic.items["/item/1"] = (
        {"service": "moneybin-alice", "username": "key"},
        b"synthetic",
    )
    synthetic.collection_locked = failure == "collection"
    synthetic.item_locked = failure == "item"
    synthetic.missing_collection = failure in {"missing", "missing_preferred"}
    if failure == "missing_preferred":
        cast(
            Any, backend
        ).preferred_collection = "/org/freedesktop/secrets/collection/missing"
    synthetic.race_method = (
        failure if failure in {"GetSecret", "CreateItem", "Delete"} else None
    )
    store = SecretStore("alice")
    with (
        scheduled_secret_access(),
        pytest.raises((SecretNotFoundError, SecretStorageUnavailableError)),
    ):
        if failure == "CreateItem":
            store.set_key("key", "new-synthetic")
        elif failure == "Delete":
            store.delete_key("key")
        else:
            store.get_key("key")
    assert_no_ui(synthetic)


@pytest.mark.parametrize("operation", ["create", "delete"])
def test_returned_prompt_is_refused_before_transmission_and_wait(
    vault: tuple[Vault, Keyring],
    operation: str,
) -> None:
    from moneybin.secrets import scheduled_secret_access

    synthetic, _ = vault
    synthetic.items["/item/1"] = (
        {"service": "moneybin-alice", "username": "key"},
        b"synthetic",
    )
    synthetic.prompt_create = operation == "create"
    synthetic.prompt_delete = operation == "delete"
    with scheduled_secret_access(), pytest.raises(SecretStorageUnavailableError):
        if operation == "create":
            SecretStore("alice").set_key("key", "synthetic-new")
        else:
            SecretStore("alice").delete_key("key")
    assert_no_ui(synthetic)


@pytest.mark.parametrize(
    "member,interface",
    [
        ("Prompt", "org.freedesktop.Secret.Prompt"),
        ("Unlock", "org.freedesktop.Secret.Service"),
        ("CreateCollection", "org.freedesktop.Secret.Service"),
        ("UnknownMethod", "org.freedesktop.Secret.Item"),
    ],
)
@pytest.mark.parametrize("method", ["send", "send_message", "send_and_get_reply"])
def test_transport_guard_covers_every_outgoing_alias(
    member: str,
    interface: str,
    method: str,
) -> None:
    from moneybin.secrets import _NoPromptConnection  # pyright: ignore[reportPrivateUsage]  # public dependency transport boundary

    synthetic = Vault()
    connection = _NoPromptConnection(synthetic.connect())
    message = jeepney.new_method_call(
        jeepney.DBusAddress(
            "/synthetic", bus_name="org.freedesktop.secrets", interface=interface
        ),
        member,
    )
    with pytest.raises(Exception, match="background"):
        getattr(connection, method)(message)
    assert synthetic.calls == []


def test_transport_rejects_interactive_authorization_flag() -> None:
    from moneybin.secrets import _NoPromptConnection  # pyright: ignore[reportPrivateUsage]  # public dependency transport boundary

    synthetic = Vault()
    connection = _NoPromptConnection(synthetic.connect())
    message = jeepney.new_method_call(
        jeepney.DBusAddress(
            "/synthetic",
            bus_name="org.freedesktop.secrets",
            interface="org.freedesktop.Secret.Item",
        ),
        "GetSecret",
        "o",
        ("/session",),
    )
    message.header.flags |= 4
    with pytest.raises(Exception, match="background"):
        connection.send_and_get_reply(message)
    assert synthetic.calls == []


@pytest.mark.parametrize("failure", ["none", "write", "cleanup"])
@respx.mock
def test_real_client_refresh_publication_readback_and_cleanup(
    vault: tuple[Vault, Keyring],
    failure: str,
) -> None:
    from moneybin.connectors.sync_client import SyncClient
    from moneybin.secrets import scheduled_secret_access

    synthetic, _ = vault
    store = SecretStore.for_sync(None)
    with scheduled_secret_access():
        for name, value in {
            "jwt": "old-jwt",
            "refresh_token": "old-refresh",
            "server": "https://synthetic.invalid",
        }.items():
            store.set_key(name, value)
        if failure != "none":
            synthetic.prompt_username = "refresh_token"
            synthetic.lock_after_failed_write = failure == "cleanup"
        institutions = respx.get("https://synthetic.invalid/institutions").mock(
            side_effect=[
                httpx.Response(401),
                httpx.Response(200, json=[]),
            ]
        )
        respx.post("https://synthetic.invalid/auth/refresh").mock(
            return_value=httpx.Response(
                200,
                json={
                    "access_token": "new-jwt",
                    "refresh_token": "new-refresh",
                    "expires_in": 3600,
                },
            )
        )
        client = SyncClient("https://synthetic.invalid", secret_store=store)
        with closing(client._client):  # pyright: ignore[reportPrivateUsage]  # close synthetic HTTP transport
            if failure == "none":
                assert client.list_institutions() == []
                assert store.get_key("jwt") == "new-jwt"
                assert store.get_key("refresh_token") == "new-refresh"
                assert not store.has_keychain_entry("pending")
                assert institutions.call_count == 2
            else:
                with pytest.raises(SecretStorageUnavailableError):
                    client.list_institutions()
                assert institutions.call_count == 1
        synthetic.collection_locked = False
        if failure == "write":
            assert synthetic.items == {}
        if failure == "cleanup":
            assert store.has_keychain_entry("pending")
            fresh = SyncClient("https://synthetic.invalid", secret_store=store)
            with closing(fresh._client):  # pyright: ignore[reportPrivateUsage]  # close synthetic HTTP transport
                with pytest.raises(SecretStorageUnavailableError, match="interrupted"):
                    fresh.list_institutions()
    assert_no_ui(synthetic)
