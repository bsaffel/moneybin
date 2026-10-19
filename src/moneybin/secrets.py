"""Centralized secret management for MoneyBin.

SecretStore is the sole module that imports ``keyring``. All other modules
access secrets through this interface — the Database class for encryption
keys, CLI commands for key lifecycle, MoneyBinSettings for sensitive config.

SecretStore does NOT cache, derive, rotate, or orchestrate secret lifecycle.
Passphrase derivation (Argon2id) and rotation sequencing live in the CLI
commands that call set_key() / delete_key().
"""

import ctypes
import hashlib
import io
import logging
import os
import sys
import threading
from collections.abc import Generator
from configparser import ConfigParser
from configparser import Error as ConfigError
from contextlib import contextmanager, nullcontext
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Literal, Protocol, Self, cast

import keyring
from keyring.errors import (
    KeyringError,
    KeyringLocked,
    NoKeyringError,
    PasswordDeleteError,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator

logger = logging.getLogger(__name__)

_SERVICE_PREFIX = "moneybin"
_ENV_PREFIX = "MONEYBIN_"

# Google Sheets connector OAuth secret keys.
GSHEET_REFRESH_TOKEN_KEY = "gsheet:refresh_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_ACCESS_TOKEN_KEY = "gsheet:access_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_ACCESS_TOKEN_EXPIRES_KEY = "gsheet:access_token_expires_at"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_WRITE_REFRESH_TOKEN_KEY = "gsheet:write_refresh_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_WRITE_ACCESS_TOKEN_KEY = "gsheet:write_access_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY = "gsheet:write_access_token_expires_at"  # noqa: S105  # keyring lookup name, not a secret value

# Market-data price feeds. Only Tiingo needs a credential — CoinGecko's keyless
# tier serves the endpoint this codebase uses.
TIINGO_API_TOKEN_KEY = "tiingo:api_token"  # noqa: S105  # keyring lookup name, not a secret value


def _resolve_profile(profile: str | None) -> str | None:
    from moneybin.config import get_current_profile
    from moneybin.utils.user_config import normalize_profile_name

    if profile is None:
        try:
            profile = get_current_profile()
        except RuntimeError:
            return None
    return normalize_profile_name(profile)


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found in keychain or environment."""


class SecretUnavailableError(SecretNotFoundError):
    """Raised when the OS keychain reports a read as denied, not missing.

    Subclasses ``SecretNotFoundError`` so every existing ``except
    SecretNotFoundError`` call site keeps working unchanged — this only lets
    a caller that wants to react more precisely catch it first. Most
    platforms can't actually raise this: macOS reports a sandbox-denied read
    identically to a missing item (``errSecItemNotFound``), so this fires
    only where the keyring backend itself distinguishes the two (e.g. a
    locked Linux secret service, or an explicit macOS keychain ACL denial).
    """


class SecretStorageUnavailableError(Exception):
    """Raised when a keychain write or deletion cannot be completed safely.

    Distinct from ``SecretNotFoundError`` — read paths can fall back to env
    vars, but writes have nowhere to go and must surface a clear error
    rather than silently losing the value.
    """


_SCHEDULED_STORES = {
    "darwin": ("keychain", "keyring.backends.macOS.Keyring"),
    "linux": ("secret-service", "keyring.backends.SecretService.Keyring"),
}
_MAX_SCHEDULED_KEYRING_CONFIG_CHARS = 16_384


class ScheduledKeyringConfiguration(BaseModel):
    """Original config location and validated nonsecret backend selection."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    path: str = Field(min_length=1, max_length=4096)
    present: bool
    source: Literal["environment", "config", "default"]
    selector: (
        Literal[
            "keyring.backends.macOS.Keyring", "keyring.backends.SecretService.Keyring"
        ]
        | None
    )

    @model_validator(mode="after")
    def validate_selection(self) -> Self:
        """Keep the persisted source and selection internally consistent."""
        if not Path(self.path).is_absolute() or not self.path.isprintable():
            raise ValueError("Keyring config requires a bounded absolute path")
        if (self.source == "default") != (self.selector is None):
            raise ValueError("Keyring selection source is inconsistent")
        if self.source == "config" and not self.present:
            raise ValueError("A configured keyring selection requires its source file")
        return self


class ScheduledSecretAddressing(BaseModel):
    """Bound nonsecret stock-store selectors, never arbitrary backend properties."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    backend: Literal["keychain", "secret-service"]
    configuration: ScheduledKeyringConfiguration
    scheme: Literal["default", "KeePassXC"] = "default"
    preferred_collection: str | None = Field(
        default=None,
        max_length=1024,
        pattern=r"^/(?:[A-Za-z0-9_]+(?:/[A-Za-z0-9_]+)*)?$",
    )
    appid: str = Field(
        default="Python keyring library",
        min_length=1,
        max_length=256,
        pattern=r"^[^\x00-\x1f\x7f]+$",
    )

    @model_validator(mode="after")
    def validate_platform_fields(self) -> Self:
        """MacOS has no Secret Service collection or attribute scheme."""
        if not self.appid.isprintable():
            raise ValueError("Application attributes must contain printable text")
        allowed = _SCHEDULED_STORES[
            "darwin" if self.backend == "keychain" else "linux"
        ][1]
        if self.configuration.selector not in (None, allowed):
            raise ValueError("Keyring selection does not match its stock backend")
        if self.backend == "keychain" and (
            self.scheme != "default"
            or self.preferred_collection is not None
            or self.appid != "Python keyring library"
        ):
            raise ValueError("Secret Service selectors require its stock backend")
        return self

    def environment(self) -> dict[str, str]:
        """Render only the validated selectors used by stock Secret Service."""
        result = {
            # Native argv ownership must bind the original config location too.
            "_KEYRING_SCHEDULE_BINDING": hashlib.sha256(
                self.model_dump_json().encode()
            ).hexdigest(),
        }
        selector = self.configuration.selector
        if self.configuration.source == "environment" and selector is not None:
            result["PYTHON_KEYRING_BACKEND"] = selector
        if self.backend == "secret-service":
            result.update(
                KEYRING_PROPERTY_SCHEME=self.scheme,
                KEYRING_PROPERTY_APPID=self.appid,
            )
            if self.preferred_collection is not None:
                result["KEYRING_PROPERTY_PREFERRED_COLLECTION"] = (
                    self.preferred_collection
                )
        return result


def _scheduled_keyring_configuration(path: Path) -> ScheduledKeyringConfiguration:
    """Validate selectors before persisting them or loading any backend code."""
    config = ConfigParser()
    present = path.exists()
    if present:
        with path.open(encoding="utf-8") as source:
            contents = source.read(_MAX_SCHEDULED_KEYRING_CONFIG_CHARS + 1)
        if len(contents) > _MAX_SCHEDULED_KEYRING_CONFIG_CHARS:
            raise ValueError("Keyring config is too large")
        config.read_file(io.StringIO(contents))
    allowed = _SCHEDULED_STORES[sys.platform][1]
    configured = config.get("backend", "default-keyring", fallback=None)
    selected = os.environ.get("PYTHON_KEYRING_BACKEND")
    if config.has_option("backend", "keyring-path") or any(
        value and value != allowed for value in (configured, selected)
    ):
        raise ValueError("Unsupported keyring selection")
    return ScheduledKeyringConfiguration.model_validate({
        "path": str(path.absolute()),
        "present": present,
        "source": "environment" if selected else "config" if configured else "default",
        "selector": selected or configured or None,
    })


def capture_scheduled_secret_addressing(
    *, configuration_path: Path | None = None
) -> ScheduledSecretAddressing:
    """Read only known addressing configuration without initializing a store."""
    try:
        from keyring.util.platform_ import config_root

        backend = _SCHEDULED_STORES[sys.platform][0]
        configuration = _scheduled_keyring_configuration(
            configuration_path or config_root() / "keyringrc.cfg"
        )
        fields = {
            "KEYRING_PROPERTY_SCHEME": "scheme",
            "KEYRING_PROPERTY_PREFERRED_COLLECTION": "preferred_collection",
            "KEYRING_PROPERTY_APPID": "appid",
        }
        values: dict[str, Any] = {
            "backend": backend,
            "configuration": configuration,
        }
        for name in os.environ:
            # Upstream accepts this substring anywhere in an environment key.
            if "KEYRING_PROPERTY_" not in name:
                continue
            if name not in fields or backend != "secret-service":
                raise ValueError
            values[fields[name]] = os.environ[name]
        return ScheduledSecretAddressing.model_validate(values)
    except (ConfigError, OSError, UnicodeError, KeyError, ValueError):
        raise SecretStorageUnavailableError(
            "Scheduled sync requires supported, bounded stock-store addressing."
        ) from None


def validate_scheduled_secret_addressing(
    addressing: ScheduledSecretAddressing,
) -> ScheduledSecretAddressing:
    """Revalidate the bound source without changing global keyring discovery."""
    current = capture_scheduled_secret_addressing(
        configuration_path=Path(addressing.configuration.path)
    )
    if current != addressing:
        raise SecretStorageUnavailableError(
            "The scheduled store addressing has changed."
        )
    if addressing.configuration.source == "default":
        effective = capture_scheduled_secret_addressing()
        if effective.configuration.selector is not None:
            raise SecretStorageUnavailableError(
                "The effective keyring configuration conflicts with the scheduled store."
            )
    return current


class _ScheduledBackend(Protocol):
    def get_password(self, service: str, username: str) -> str | None: ...
    def set_password(self, service: str, username: str, password: str) -> None: ...
    def delete_password(self, service: str, username: str) -> None: ...


_scheduled_backend: _ScheduledBackend | None = None
_scheduled_scope_lock = threading.RLock()
_SECURITY_FRAMEWORK = "/System/Library/Frameworks/Security.framework/Security"
_CORE_FRAMEWORK = "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"


@contextmanager
def _macos_interaction_guard() -> Generator[None, None, None]:
    """Suppress stock keyring's file-keychain UI for this dedicated process."""
    try:
        native = ctypes.CDLL(_SECURITY_FRAMEWORK)
        getter = native.SecKeychainGetUserInteractionAllowed
        setter = native.SecKeychainSetUserInteractionAllowed
        getter.argtypes = [ctypes.POINTER(ctypes.c_ubyte)]
        setter.argtypes = [ctypes.c_ubyte]
        getter.restype = setter.restype = ctypes.c_int32
        previous = ctypes.c_ubyte()
        if getter(ctypes.byref(previous)) != 0 or setter(0) != 0:
            raise OSError
    except (OSError, AttributeError):
        raise SecretStorageUnavailableError(
            "Scheduled sync could not disable keychain interaction."
        ) from None
    try:
        yield
    finally:
        if setter(previous.value) != 0:
            raise SecretStorageUnavailableError(
                "Scheduled sync could not restore keychain interaction policy."
            )


def _macos_require_unlocked() -> None:
    """Conservatively require default and every searched file keychain unlocked."""
    default, search = ctypes.c_void_p(), ctypes.c_void_p()
    core = None
    try:
        native = ctypes.CDLL(_SECURITY_FRAMEWORK)
        core = ctypes.CDLL(_CORE_FRAMEWORK)
        native.SecKeychainCopyDefault.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        native.SecKeychainCopySearchList.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        native.SecKeychainGetStatus.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        native.SecKeychainCopyDefault.restype = ctypes.c_int32
        native.SecKeychainCopySearchList.restype = ctypes.c_int32
        native.SecKeychainGetStatus.restype = ctypes.c_int32
        core.CFArrayGetCount.argtypes = [ctypes.c_void_p]
        core.CFArrayGetCount.restype = ctypes.c_long
        core.CFArrayGetValueAtIndex.argtypes = [ctypes.c_void_p, ctypes.c_long]
        core.CFArrayGetValueAtIndex.restype = ctypes.c_void_p
        core.CFRelease.argtypes = [ctypes.c_void_p]
        core.CFRelease.restype = None
        if (
            native.SecKeychainCopyDefault(ctypes.byref(default)) != 0
            or not default.value
        ):
            raise OSError
        if (
            native.SecKeychainCopySearchList(ctypes.byref(search)) != 0
            or not search.value
        ):
            raise OSError
        references = [default.value]
        count = core.CFArrayGetCount(search.value)
        if count < 0:
            raise OSError
        references.extend(
            core.CFArrayGetValueAtIndex(search.value, i) for i in range(count)
        )
        for reference in references:
            status = ctypes.c_uint32()
            if (
                not reference
                or native.SecKeychainGetStatus(reference, ctypes.byref(status)) != 0
                or not status.value & 1
            ):
                raise OSError
    except (OSError, AttributeError):
        raise KeyringLocked(
            "Scheduled background access requires unlocked keychains."
        ) from None
    finally:
        if core is not None:
            for reference in (default.value, search.value):
                if reference:
                    core.CFRelease(reference)


class _MacOSSecrets:
    def __init__(self, backend: _ScheduledBackend) -> None:
        self.backend = backend

    def get_password(self, service: str, username: str) -> str | None:
        _macos_require_unlocked()
        return self.backend.get_password(service, username)

    def set_password(self, service: str, username: str, password: str) -> None:
        _macos_require_unlocked()
        self.backend.set_password(service, username, password)
        _macos_require_unlocked()

    def delete_password(self, service: str, username: str) -> None:
        _macos_require_unlocked()
        # A locked keychain can permit deletion without reading its secret.
        self.backend.get_password(service, username)
        self.backend.delete_password(service, username)
        _macos_require_unlocked()


class _NoPromptConnection:
    """Limit the public transport passed to SecretStorage's Collection/Item APIs."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def _check(self, message: Any) -> None:
        jeepney = import_module("jeepney")
        fields = message.header.fields
        headers = jeepney.HeaderFields
        allowed = {
            ("org.freedesktop.DBus.Properties", "Get"),
            ("org.freedesktop.Secret.Service", "OpenSession"),
            ("org.freedesktop.Secret.Collection", "SearchItems"),
            ("org.freedesktop.Secret.Collection", "CreateItem"),
            ("org.freedesktop.Secret.Item", "GetSecret"),
            ("org.freedesktop.Secret.Item", "Delete"),
        }
        if (
            fields.get(headers.destination) != "org.freedesktop.secrets"
            or message.header.message_type != jeepney.MessageType.method_call
            or int(message.header.flags) & 4
            or (fields.get(headers.interface), fields.get(headers.member))
            not in allowed
        ):
            raise KeyringError(
                "Interaction is refused during background credential access."
            )

    def send_and_get_reply(self, message: Any, *, timeout: float | None = None) -> Any:
        self._check(message)
        return self.connection.send_and_get_reply(message, timeout=timeout)

    def send(self, message: Any, serial: int | None = None) -> None:
        self._check(message)
        self.connection.send(message, serial=serial)

    send_message = send

    def filter(self, rule: Any, *, queue: Any = None, bufsize: int = 1) -> Any:
        return self.connection.filter(rule, queue=queue, bufsize=bufsize)

    def recv_until_filtered(self, queue: Any, *, timeout: float | None = None) -> Any:
        raise KeyringError(
            "Prompt waits are refused during background credential access."
        )

    def close(self) -> None:
        self.connection.close()


class _SecretServiceProperties(Protocol):
    schemes: dict[str, dict[str, str]]
    scheme: str
    appid: str


class _LinuxSecrets:
    """Use existing Secret Service identity without its backend's unlock helpers."""

    def __init__(self, backend: _ScheduledBackend) -> None:
        self.backend = cast(_SecretServiceProperties, backend)
        # Linux-marked dependencies must remain importable on their own platform.
        self.library = import_module("secretstorage")
        self.dbus_errors = import_module("jeepney.wrappers")

    def _query(self, service: str, username: str) -> dict[str, str]:
        scheme = self.backend.schemes[self.backend.scheme]
        return {scheme["service"]: service, scheme["username"]: username}

    @contextmanager
    def _collection(self) -> Generator[Any, None, None]:
        connection: _NoPromptConnection | None = None
        try:
            connection = _NoPromptConnection(self.library.dbus_init())
            preferred: object = getattr(self.backend, "preferred_collection", None)
            # Upstream annotates a concrete DBusConnection; its public APIs use
            # this small duck-typed transport (tested against the locked library).
            collection = (
                self.library.Collection(connection, preferred)
                if isinstance(preferred, str)
                else self.library.Collection(connection)
            )
            collection.ensure_not_locked()
            yield collection
        except PasswordDeleteError:
            raise
        except (
            self.library.exceptions.SecretStorageException,
            self.dbus_errors.DBusErrorResponse,
            OSError,
            UnicodeError,
            ValueError,
        ):
            raise KeyringError(
                "Secure storage is unavailable for background access."
            ) from None
        finally:
            if connection is not None:
                connection.close()

    def get_password(self, service: str, username: str) -> str | None:
        with self._collection() as collection:
            for item in collection.search_items(self._query(service, username)):
                return str(item.get_secret().decode("utf-8"))
        return None

    def set_password(self, service: str, username: str, password: str) -> None:
        with self._collection() as collection:
            attributes = self._query(service, username)
            attributes["application"] = self.backend.appid
            item = collection.create_item(
                f"Password for '{username}' on '{service}'",
                attributes,
                password.encode("utf-8"),
                replace=True,
            )
            item.ensure_not_locked()
            collection.ensure_not_locked()

    def delete_password(self, service: str, username: str) -> None:
        with self._collection() as collection:
            for item in collection.search_items(self._query(service, username)):
                item.ensure_not_locked()
                item.delete()
                collection.ensure_not_locked()
                return
        raise PasswordDeleteError("No such background credential.")


def _scheduled_backend_selection(
    addressing: ScheduledSecretAddressing | None = None,
) -> _ScheduledBackend:
    bound = addressing is not None
    addressing = (
        validate_scheduled_secret_addressing(addressing)
        if addressing is not None
        else capture_scheduled_secret_addressing()
    )
    allowed = _SCHEDULED_STORES[sys.platform][1]
    try:
        selector = addressing.configuration.selector
        # Discovery evaluates plugin priorities; refuse unknown code before it runs.
        if not selector and any(
            ep.dist is None or ep.dist.name != "keyring"
            for ep in entry_points(group="keyring.backends")
        ):
            raise ValueError
        if bound and selector:
            from keyring.core import load_keyring

            try:
                backend = load_keyring(selector)
            except (ImportError, RuntimeError):
                # The public loader refuses missing or nonviable stock backends.
                raise ValueError("The selected stock backend is unavailable") from None
        else:
            backend = keyring.get_keyring()
        module, name = allowed.rsplit(".", 1)
        if type(backend) is not getattr(import_module(module), name):
            raise ValueError
    except (ConfigError, OSError, ValueError, KeyringError):
        raise SecretStorageUnavailableError(
            "Scheduled sync requires stock macOS Keychain or Linux Secret Service; configure an unlocked supported store and retry manually."
        ) from None
    # keyring's setter decorator obscures its signature; exact class was checked.
    checked_backend = cast(_ScheduledBackend, backend)
    if sys.platform == "darwin":
        return _MacOSSecrets(checked_backend)
    return _LinuxSecrets(checked_backend)


@contextmanager
def scheduled_secret_access(
    addressing: ScheduledSecretAddressing | None = None,
) -> Generator[None, None, None]:
    """Enforce no-UI, no-fallback access throughout a dedicated scheduled pull."""
    global _scheduled_backend
    from moneybin.database import invalidate_encryption_key_cache

    with _scheduled_scope_lock:
        if _scheduled_backend is not None:
            yield
            return
        invalidate_encryption_key_cache()
        try:
            with (
                _macos_interaction_guard()
                if sys.platform == "darwin"
                else nullcontext()
            ):
                _scheduled_backend = _scheduled_backend_selection(addressing)
                try:
                    yield
                finally:
                    _scheduled_backend = None
        finally:
            invalidate_encryption_key_cache()


class SecretStore:
    """Keychain and environment variable interface for secrets.

    Three operations for keychain-backed secrets (encryption keys, E2E keys):
    - get_key(name): keychain → env var → SecretNotFoundError
    - set_key(name, value): write to keychain
    - delete_key(name): clear from keychain

    One operation for env-var-only secrets (API keys, server credentials):
    - get_env(name): env var → SecretNotFoundError

    Each ``SecretStore`` is scoped to a single profile — its keychain entries
    live under ``service="moneybin-<profile>"``. Passing ``profile=None``
    falls back to the current profile (set via ``set_current_profile``).
    """

    def __init__(self, profile: str | None = None) -> None:
        """Capture one normalized profile for keychain and environment access."""
        self._profile = _resolve_profile(profile)
        self._service = (
            f"{_SERVICE_PREFIX}-{self._profile}" if self._profile else _SERVICE_PREFIX
        )
        self._username_prefix = ""
        self._allow_env_fallback = True

    @property
    def profile(self) -> str | None:
        """The normalized profile captured when this store was constructed."""
        return self._profile

    @classmethod
    def for_sync(cls, profile_id: str | None) -> "SecretStore":
        """Preserve broker credential addresses without environment fallback."""
        if profile_id is not None and (
            not profile_id or not profile_id.isascii() or not profile_id.isalnum()
        ):
            raise ValueError("Invalid sync profile ID")
        # Broker identity does not depend on a resolved database profile.
        store = cls.__new__(cls)
        store._profile = profile_id
        store._service = "moneybin-sync"
        store._username_prefix = f"{profile_id}:" if profile_id else ""
        store._allow_env_fallback = False
        return store

    def env_var_name(self, name: str) -> str:
        """Name the environment fallback for this captured profile."""
        if self._profile:
            profile = self._profile.upper().replace("-", "_")
            return f"{_ENV_PREFIX}PROFILE__{profile}__{name}"
        return f"{_ENV_PREFIX}{name}"

    def get_key(self, name: str) -> str:
        """Retrieve from keychain, then this profile's environment fallback."""
        denied = False
        allow_env = self._allow_env_fallback and _scheduled_backend is None
        backend = _scheduled_backend or keyring
        try:
            value = backend.get_password(self._service, self._username_prefix + name)
        except NoKeyringError:
            if not allow_env:
                raise SecretStorageUnavailableError(
                    "Sync requires an available secure OS keychain."
                ) from None
            value = None
        except KeyringError:
            value = None
            denied = True
        if value is not None:
            return value

        env_var = self.env_var_name(name)
        if allow_env:
            value = os.environ.get(env_var)
            if value is not None:
                return value
        if denied:
            guidance = (
                f" Set env var {env_var} to bypass the keychain."
                if allow_env
                else " Unlock the OS keychain and retry."
            )
            raise SecretUnavailableError(
                f"Secret '{name}' could not be read — the OS keychain denied "
                f"access or is unavailable; existence could not be determined.{guidance}"
            ) from None
        guidance = (
            f" Set it via OS keychain (moneybin db init) or env var {env_var}."
            if allow_env
            else " Run `moneybin sync login`."
        )
        raise SecretNotFoundError(f"Secret '{name}' not found.{guidance}")

    def get_env(self, name: str) -> str:
        """Retrieve a configuration secret from its global environment name."""
        if _scheduled_backend is not None:
            raise SecretNotFoundError(
                "Scheduled sync requires OS keychain credentials."
            )
        env_var = f"{_ENV_PREFIX}{name}"
        value = os.environ.get(env_var)
        if value is not None:
            return value
        raise SecretNotFoundError(f"Secret '{name}' not found. Set env var {env_var}.")

    def has_keychain_entry(self, name: str) -> bool:
        """Check keychain presence without consulting environment fallback."""
        try:
            return (_scheduled_backend or keyring).get_password(
                self._service, self._username_prefix + name
            ) is not None
        except NoKeyringError:
            return False
        except KeyringError:
            raise SecretUnavailableError(
                f"Secret '{name}' could not be checked — the OS keychain denied "
                "access or is unavailable; existence could not be determined."
            ) from None

    def set_key(self, name: str, value: str) -> None:
        """Store a secret in the OS keychain, never a fallback file."""
        try:
            (_scheduled_backend or keyring).set_password(
                self._service, self._username_prefix + name, value
            )
        except KeyringError:
            guidance = (
                f" Or supply the value via env var {self.env_var_name(name)}."
                if self._allow_env_fallback and _scheduled_backend is None
                else " Sync login and refresh require writable secure storage."
            )
            raise SecretStorageUnavailableError(
                f"Unable to store secret '{name}' in the OS keychain. "
                f"Configure or unlock a secure OS keychain and retry.{guidance}"
            ) from None
        logger.debug(f"Stored secret '{name}' in OS keychain")

    def delete_key(self, name: str) -> None:
        """Clear a keychain entry; refusal is distinct from absence."""
        try:
            (_scheduled_backend or keyring).delete_password(
                self._service, self._username_prefix + name
            )
        except PasswordDeleteError:
            # Backends may use this error for deletion failures as well as misses.
            # Only report absence when a fresh read confirms it.
            try:
                present = self.has_keychain_entry(name)
            except SecretUnavailableError:
                raise SecretStorageUnavailableError(
                    f"Secret '{name}' removal could not be verified."
                ) from None
            if present:
                raise SecretStorageUnavailableError(
                    f"Secret '{name}' could not be removed from the OS keychain."
                ) from None
            raise SecretNotFoundError(
                f"Secret '{name}' not found in keychain."
            ) from None
        except NoKeyringError:
            if not self._allow_env_fallback or _scheduled_backend is not None:
                raise SecretStorageUnavailableError(
                    "Sync credentials could not be removed; configure the OS keychain."
                ) from None
            raise SecretNotFoundError(
                f"Secret '{name}' not found (no keyring backend available)."
            ) from None
        except KeyringError:
            raise SecretStorageUnavailableError(
                f"Secret '{name}' could not be removed — the OS keychain denied access."
            ) from None
        logger.debug(f"Removed secret '{name}' from OS keychain")
