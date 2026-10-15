"""Profile isolation and secure migration of synthetic sync credentials."""

# pyright: reportPrivateUsage=false
# Exercise internal storage transitions without network traffic.
# ruff: noqa: S106  # all token arguments in this file are synthetic fixtures

from pathlib import Path

import keyring
import keyring.errors
import pytest

from moneybin.connectors.sync_client import SyncClient
from moneybin.crypto_constants import KEY_NAME, SALT_NAME
from moneybin.secrets import (
    SecretNotFoundError,
    SecretStorageUnavailableError,
    SecretStore,
)

Vault = dict[tuple[str, str], str]


@pytest.fixture
def vault(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> dict[tuple[str, str], str]:
    """Replace the external credential backend, never the storage behavior."""
    values: dict[tuple[str, str], str] = {}
    monkeypatch.setenv("HOME", str(tmp_path))
    for name in ("ALL_PROXY", "all_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.delenv(name, raising=False)

    def get(service: str, name: str) -> str | None:
        return values.get((service, name))

    def put(service: str, name: str, value: str) -> None:
        values[(service, name)] = value

    monkeypatch.setattr(keyring, "get_password", get)
    monkeypatch.setattr(keyring, "set_password", put)

    def delete(service: str, name: str) -> None:
        if (service, name) not in values:
            raise keyring.errors.PasswordDeleteError()
        del values[service, name]

    monkeypatch.setattr(keyring, "delete_password", delete)
    return values


def legacy_file(
    tmp_path: Path, content: str = '{"jwt":"old-access","refresh_token":"old-refresh"}'
) -> Path:
    path = tmp_path / ".moneybin" / ".sync_token-aaaaaaaaaaaa"
    path.parent.mkdir(exist_ok=True)
    path.write_text(content)
    return path


def test_profile_never_uses_global_secret(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MONEYBIN_DATABASE__ENCRYPTION_KEY", "other-profile-key")
    with pytest.raises(SecretNotFoundError):
        SecretStore(profile="alice").get_key("DATABASE__ENCRYPTION_KEY")


def test_normalized_profile_uses_its_own_environment(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "MONEYBIN_PROFILE__ALICE_HOME__DATABASE__ENCRYPTION_KEY", "alice-key"
    )
    monkeypatch.setenv("MONEYBIN_DATABASE__ENCRYPTION_KEY", "wrong-key")
    assert (
        SecretStore(profile="Alice Home").get_key("DATABASE__ENCRYPTION_KEY")
        == "alice-key"
    )


def test_sync_missing_backend_never_creates_plaintext(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def unavailable(*args: object) -> None:
        raise keyring.errors.NoKeyringError("synthetic-sensitive-backend-detail")

    monkeypatch.setattr(keyring, "set_password", unavailable)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError) as error:
        client._store_tokens(access_token="access", refresh_token="refresh")
    assert "synthetic-sensitive" not in str(error.value)
    assert not (tmp_path / ".moneybin" / ".sync_token-aaaaaaaaaaaa").exists()


def test_exact_profile_file_is_imported_and_removed(
    vault: Vault, tmp_path: Path
) -> None:
    path = legacy_file(tmp_path)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    assert client._read_token() == "old-access"
    assert client._read_refresh_token() == "old-refresh"
    assert vault[("moneybin-sync", "aaaaaaaaaaaa:jwt")] == "old-access"
    assert vault[("moneybin-sync", "aaaaaaaaaaaa:refresh_token")] == "old-refresh"
    assert not path.exists()


@pytest.mark.parametrize(
    "content",
    [
        "{}",
        '{"jwt":"","refresh_token":"r"}',
        '{"jwt":"a","refresh_token":2}',
        "[]",
        "invalid",
    ],
)
def test_invalid_file_is_preserved_without_authentication(
    vault: Vault, tmp_path: Path, content: str
) -> None:
    path = legacy_file(tmp_path, content)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client._read_token()
    assert path.read_text() == content
    assert not vault


def test_partial_keychain_pair_is_not_combined_with_file(
    vault: Vault, tmp_path: Path
) -> None:
    path = legacy_file(tmp_path)
    vault[("moneybin-sync", "aaaaaaaaaaaa:jwt")] = "new-access"
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client._read_token()
    assert path.exists()
    assert ("moneybin-sync", "aaaaaaaaaaaa:refresh_token") not in vault


def test_failed_import_retains_file_and_rolls_back_partial_pair(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = legacy_file(tmp_path)

    def write(service: str, name: str, value: str) -> None:
        if name.endswith(":refresh_token"):
            raise keyring.errors.KeyringError("synthetic-sensitive-detail")
        vault[(service, name)] = value

    monkeypatch.setattr(keyring, "set_password", write)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client._read_token()
    assert path.exists()
    assert not vault


def test_import_verifies_storage_before_removing_file(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = legacy_file(tmp_path)

    def ignore_write(*args: object) -> None:
        pass

    monkeypatch.setattr(keyring, "set_password", ignore_write)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client._read_token()
    assert path.exists()


def test_keychain_pair_wins_over_stale_file(vault: Vault, tmp_path: Path) -> None:
    legacy_file(tmp_path)
    vault[("moneybin-sync", "aaaaaaaaaaaa:jwt")] = "new-access"
    vault[("moneybin-sync", "aaaaaaaaaaaa:refresh_token")] = "new-refresh"
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    assert client._read_token() == "new-access"
    assert client._read_refresh_token() == "new-refresh"


def test_denied_logout_does_not_report_success(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    def denied(*args: object) -> None:
        raise keyring.errors.KeyringLocked("synthetic-sensitive-detail")

    monkeypatch.setattr(keyring, "delete_password", denied)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client.logout()


def test_unscoped_file_is_never_adopted(vault: Vault, tmp_path: Path) -> None:
    path = tmp_path / ".moneybin" / ".sync_token"
    path.parent.mkdir()
    path.write_text('{"jwt":"other-access","refresh_token":"other-refresh"}')
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    assert client._read_token() is None
    assert not vault
    assert path.exists()


def test_failed_refresh_cannot_expose_mixed_pair_after_cleanup_denial(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    vault[("moneybin-sync", "aaaaaaaaaaaa:jwt")] = "old-access"
    vault[("moneybin-sync", "aaaaaaaaaaaa:refresh_token")] = "old-refresh"

    def write(service: str, name: str, value: str) -> None:
        if name.endswith(":refresh_token"):
            raise keyring.errors.KeyringError("write refused")
        vault[(service, name)] = value

    def denied(*args: object) -> None:
        raise keyring.errors.KeyringLocked("delete refused")

    monkeypatch.setattr(keyring, "set_password", write)
    monkeypatch.setattr(keyring, "delete_password", denied)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client._store_tokens(access_token="new-access", refresh_token="new-refresh")
    reopened = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        reopened._read_token()


def test_failed_pending_marker_writes_neither_token(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    def write(service: str, name: str, value: str) -> None:
        if name.endswith(":pending"):
            raise keyring.errors.KeyringError("write refused")
        vault[(service, name)] = value

    monkeypatch.setattr(keyring, "set_password", write)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client._store_tokens(access_token="new-access", refresh_token="new-refresh")
    assert not vault


def test_pending_marker_clear_failure_refuses_fresh_client(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    def denied(*args: object) -> None:
        raise keyring.errors.KeyringLocked("delete refused")

    monkeypatch.setattr(keyring, "delete_password", denied)
    client = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        client._store_tokens(access_token="new-access", refresh_token="new-refresh")
    reopened = SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")
    with pytest.raises(SecretStorageUnavailableError):
        reopened._read_token()


def test_denied_delete_is_not_caught_as_missing(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    def denied(*args: object) -> None:
        raise keyring.errors.KeyringLocked("synthetic backend detail")

    monkeypatch.setattr(keyring, "delete_password", denied)
    with pytest.raises(SecretStorageUnavailableError):
        try:
            SecretStore(profile="alice").delete_key("credential")
        except SecretNotFoundError:
            pytest.fail("Denied deletion was mistaken for a missing secret")


def test_profile_delete_keeps_identity_when_sync_cleanup_is_denied(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from moneybin.services.profile_service import ProfileService

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    monkeypatch.setattr(
        "moneybin.services.profile_service.get_default_profile", lambda: "default"
    )
    profile = tmp_path / "profiles" / "alice"
    profile.mkdir(parents=True)
    (profile / "profile_id").write_text("aaaaaaaaaaaa")

    def denied(service: str, name: str) -> None:
        if service == "moneybin-sync":
            raise keyring.errors.KeyringLocked("synthetic-sensitive-detail")
        raise keyring.errors.PasswordDeleteError()

    monkeypatch.setattr(keyring, "delete_password", denied)
    with pytest.raises(SecretStorageUnavailableError):
        ProfileService().delete("alice")
    assert (profile / "profile_id").read_text() == "aaaaaaaaaaaa"


@pytest.mark.parametrize("denied_name", [KEY_NAME, SALT_NAME])
def test_profile_delete_retries_database_secret_cleanup(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, denied_name: str
) -> None:
    from moneybin.services.profile_service import ProfileService

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    monkeypatch.setattr(
        "moneybin.services.profile_service.get_default_profile", lambda: "default"
    )
    profile = tmp_path / "profiles" / "alice"
    profile.mkdir(parents=True)
    (profile / "moneybin.duckdb").write_bytes(b"synthetic encrypted data")
    vault[("moneybin-alice", KEY_NAME)] = "synthetic-key"
    vault[("moneybin-alice", SALT_NAME)] = "synthetic-salt"
    vault[("moneybin-bob", KEY_NAME)] = "sibling-key"
    original_delete = keyring.delete_password

    def denied(service: str, name: str) -> None:
        if service == "moneybin-alice" and name == denied_name:
            raise keyring.errors.KeyringLocked("synthetic denial")
        original_delete(service, name)

    monkeypatch.setattr(keyring, "delete_password", denied)
    with pytest.raises(SecretStorageUnavailableError):
        ProfileService().delete("alice")
    assert profile.is_dir()
    assert not list(profile.iterdir())
    assert ("moneybin-alice", denied_name) in vault
    monkeypatch.setattr(keyring, "delete_password", original_delete)
    ProfileService().delete("alice")
    assert not profile.exists()
    assert vault == {("moneybin-bob", KEY_NAME): "sibling-key"}


def test_profile_delete_preserves_keys_when_content_removal_fails(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from moneybin.services.profile_service import ProfileService

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    monkeypatch.setattr(
        "moneybin.services.profile_service.get_default_profile", lambda: "default"
    )
    profile = tmp_path / "profiles" / "alice"
    backup = profile / "backups" / "saved.duckdb"
    backup.parent.mkdir(parents=True)
    backup.write_bytes(b"synthetic encrypted backup")
    vault[("moneybin-alice", KEY_NAME)] = "synthetic-key"
    vault[("moneybin-alice", SALT_NAME)] = "synthetic-salt"

    def denied(path: Path) -> None:
        raise PermissionError("synthetic file denial")

    monkeypatch.setattr("moneybin.services.profile_service.shutil.rmtree", denied)
    with pytest.raises(PermissionError):
        ProfileService().delete("alice")
    assert backup.read_bytes() == b"synthetic encrypted backup"
    assert vault[("moneybin-alice", KEY_NAME)] == "synthetic-key"
    assert vault[("moneybin-alice", SALT_NAME)] == "synthetic-salt"


@pytest.mark.parametrize("root_link", [True, False])
def test_profile_delete_never_follows_directory_symlinks(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, root_link: bool
) -> None:
    from moneybin.services.profile_service import ProfileService

    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    monkeypatch.setattr(
        "moneybin.services.profile_service.get_default_profile", lambda: "default"
    )
    target = tmp_path / "outside-profile"
    target.mkdir()
    sentinel = target / "retained.duckdb"
    sentinel.write_bytes(b"retain this file")
    profile = tmp_path / "profiles" / "alice"
    profile.parent.mkdir()
    vault[("moneybin-alice", KEY_NAME)] = "synthetic-key"
    if root_link:
        profile.symlink_to(target, target_is_directory=True)
        with pytest.raises((OSError, ValueError)):
            ProfileService().delete("alice")
        assert vault[("moneybin-alice", KEY_NAME)] == "synthetic-key"
    else:
        profile.mkdir()
        (profile / "linked-directory").symlink_to(target, target_is_directory=True)
        ProfileService().delete("alice")
        assert not profile.exists()
    assert sentinel.read_bytes() == b"retain this file"


def test_unpersisted_pending_marker_cannot_start_token_writes(
    vault: Vault, monkeypatch: pytest.MonkeyPatch
) -> None:
    writes: list[str] = []

    def write(service: str, name: str, value: str) -> None:
        if name.endswith(":pending"):
            return
        writes.append(name)
        vault[(service, name)] = value

    monkeypatch.setattr(keyring, "set_password", write)
    with pytest.raises(SecretStorageUnavailableError):
        SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")._store_tokens(
            access_token="new-access", refresh_token="new-refresh"
        )
    assert writes == []


def test_import_file_removal_failure_refuses_again_until_cleanup_succeeds(
    vault: Vault, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = legacy_file(tmp_path)
    original = Path.unlink

    def denied(self: Path, missing_ok: bool = False) -> None:
        if self == path:
            raise PermissionError("denied")
        original(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", denied)
    with pytest.raises(SecretStorageUnavailableError):
        SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")._read_token()
    assert path.exists()
    with pytest.raises(SecretStorageUnavailableError):
        SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")._read_token()
    monkeypatch.setattr(Path, "unlink", original)
    assert (
        SyncClient("https://test.api", profile_id="aaaaaaaaaaaa")._read_token()
        == "old-access"
    )
    assert not path.exists()
