"""The moneybin-sync server address a profile talks to.

The address is a trust anchor: the browser is sent there to authenticate and
bank linking runs through it. It is saved to the profile's ``config.yaml`` only
at login, only once the server answers, and changing it clears the credentials
the previous server issued. See ``sync-overview.md`` → Server address.
"""

from __future__ import annotations

import ipaddress
import logging
from dataclasses import dataclass
from typing import Literal, NoReturn, cast
from urllib.parse import urlsplit

import httpx

from moneybin import error_codes
from moneybin.config import get_settings, reload_settings, setting_override
from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError
from moneybin.connectors.sync_models import DeviceAuthorizationChallenge
from moneybin.errors import UserError
from moneybin.metrics.registry import SYNC_SERVER_ADDRESS_OUTCOMES
from moneybin.services.profile_service import ProfileService
from moneybin.utils.user_config import get_or_create_profile_id

logger = logging.getLogger(__name__)

_SETTING_KEY = "sync.server_url"
_ENV_VAR = "MONEYBIN_SYNC__SERVER_URL"
LOGIN_COMMAND = "moneybin sync login --server <url>"
# Where moneybin-sync's compose file publishes the server. Offered, never
# assumed: a person accepts it once and it is saved like any other address.
SUGGESTED_SERVER_URL = "http://127.0.0.1:8000"
_DEFAULT_PORTS = {"http": 80, "https": 443}

Surface = Literal["cli", "mcp"]
Change = Literal["unchanged", "first", "replace"]


@dataclass(frozen=True)
class AddressPlan:
    """A validated address and what saving it would do to the profile."""

    url: str
    change: Change
    unencrypted: bool


@dataclass(frozen=True)
class LoginStart:
    """A client bound to the address, plus the challenge that proved it answers."""

    client: SyncClient
    challenge: DeviceAuthorizationChallenge | None
    outcome: Literal["unchanged", "saved", "replaced"]


def normalize_server_url(raw: str) -> str:
    """Return ``raw`` as a bare http(s) address, or refuse it.

    Scheme and host are lowercased and a default port dropped, so one server
    has one spelling: a second spelling would read as a replacement and sign
    the profile out of the server it is already using.
    """
    url = raw.strip().rstrip("/")
    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = (
            parts.port
        )  # Parsed lazily; raises on a non-numeric or out-of-range port.
    except ValueError:
        host, parts, port = None, None, None
    scheme = parts.scheme.lower() if parts is not None else ""
    if (
        not url.isprintable()
        or parts is None
        or scheme not in _DEFAULT_PORTS
        or not host
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise UserError(
            "The sync server address must look like https://sync.example.com — "
            "http or https, a host, and no credentials, query, or fragment.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        netloc = f"{netloc}:{port}"
    return f"{scheme}://{netloc}{parts.path}"


def is_unencrypted(url: str) -> bool:
    """True for plain http to anything but this machine."""
    parts = urlsplit(url)
    if parts.scheme != "http":
        return False
    host = parts.hostname or ""
    if host == "localhost":
        return False
    try:
        return not ipaddress.ip_address(host).is_loopback
    except ValueError:
        return True


def _saved_address(profile: str) -> str | None:
    config = cast(dict[str, object], ProfileService().show(profile)["config"])
    section = config.get("sync")
    if not isinstance(section, dict):
        return None
    saved = cast(dict[str, object], section).get("server_url")
    return saved if isinstance(saved, str) else None


def _same_server(effective: str, url: str) -> bool:
    try:
        return normalize_server_url(effective) == url
    except UserError:
        return False


def _new_client(server_url: str) -> SyncClient:
    # Scope the broker identity to the active profile so each profile
    # authenticates as a distinct user.
    profile_id = get_or_create_profile_id(get_settings().profile_dir)
    return SyncClient(server_url=server_url, profile_id=profile_id)


def _clear_credentials(client: SyncClient) -> None:
    from moneybin.connectors.sync_auth import SyncAuthService

    SyncAuthService(client=client).logout()


def build_sync_client() -> SyncClient:
    """Construct a SyncClient for the configured address."""
    server_url = get_settings().sync.server_url
    # Falsy, not None: a blank override line must not build a hostless client.
    if not server_url:
        raise UserError(
            "No sync server address is configured for this profile.",
            code=error_codes.SYNC_SERVER_NOT_CONFIGURED,
            hint=(
                f"Run '{LOGIN_COMMAND}' with your moneybin-sync server's address; "
                "login saves it to this profile."
            ),
        )
    # Normalized so the issuer recorded beside a token pair matches however
    # the override happens to spell the same server.
    return _new_client(normalize_server_url(server_url))


def plan_server_address(raw: str, *, surface: Surface = "cli") -> AddressPlan:
    """Validate an address and classify what saving it would change."""
    url = normalize_server_url(raw)
    # Reloaded, not cached: a long-running MCP server must see an address the
    # CLI saved since it started, or "unchanged" would skip the prompt for a
    # server this profile has already left.
    settings = reload_settings()
    effective = settings.sync.server_url or None
    if effective is not None and _same_server(effective, url):
        change: Change = "unchanged"
    elif setting_override(settings.profile, "sync", "server_url") is not None:
        # The environment or a dotenv outranks config.yaml, so a save here
        # would be shadowed and the next command would use the other server.
        SYNC_SERVER_ADDRESS_OUTCOMES.labels(
            surface=surface, outcome="override_conflict"
        ).inc()
        raise UserError(
            f"{_ENV_VAR} is set, in the environment or a .env file, and "
            "overrides any saved sync server address.",
            code=error_codes.SYNC_SERVER_OVERRIDE_CONFLICT,
            hint=f"Unset {_ENV_VAR}, then run '{LOGIN_COMMAND}' again.",
        )
    else:
        saved = _saved_address(settings.profile)
        change = "first" if saved is None else "replace"
    return AddressPlan(url=url, change=change, unencrypted=is_unencrypted(url))


def _refuse_as_not_moneybin_sync(
    plan: AddressPlan, surface: Surface, cause: Exception
) -> NoReturn:
    # It answered, but not with a device code: most often a mistyped address
    # that reached some other web server.
    SYNC_SERVER_ADDRESS_OUTCOMES.labels(surface=surface, outcome="rejected").inc()
    raise UserError(
        # Static: the user-typed address must not reach the durable error log.
        "The server at that address answered, but not as a moneybin-sync server.",
        code=error_codes.MUTATION_INVALID_INPUT,
        hint="Check the address, then run login again.",
    ) from cause


def start_login(plan: AddressPlan, *, surface: Surface) -> LoginStart:
    """Bind a client to the planned address, saving it once the server answers."""
    client = _new_client(plan.url)
    try:
        challenge = client.begin_login()
    except SyncAPIError:
        SYNC_SERVER_ADDRESS_OUTCOMES.labels(
            surface=surface, outcome="unreachable"
        ).inc()
        raise
    except httpx.HTTPStatusError as e:
        if e.response.status_code < 500:
            _refuse_as_not_moneybin_sync(plan, surface, e)
        # A 5xx is the right server failing, not the wrong one: keep the
        # address, report it as unreachable, and let a retry succeed.
        SYNC_SERVER_ADDRESS_OUTCOMES.labels(
            surface=surface, outcome="unreachable"
        ).inc()
        raise SyncAPIError(
            f"sync server returned {e.response.status_code} at login"
        ) from e
    except ValueError as e:
        _refuse_as_not_moneybin_sync(plan, surface, e)
    if plan.change == "unchanged":
        # Already in force: the challenge is fresh, so nothing is saved or cleared.
        return LoginStart(client=client, challenge=challenge, outcome="unchanged")
    settings = get_settings()
    # A token or device code one server issued must never reach another. Not
    # gated on a previously *saved* address: credentials can outlive one that
    # came from the environment. SyncClient also binds each pair to its issuer,
    # which covers the address changes that never pass through here.
    _clear_credentials(client)
    ProfileService().set(settings.profile, _SETTING_KEY, plan.url)
    reload_settings()
    outcome: Literal["saved", "replaced"] = (
        "replaced" if plan.change == "replace" else "saved"
    )
    SYNC_SERVER_ADDRESS_OUTCOMES.labels(surface=surface, outcome=outcome).inc()
    logger.info(f"Sync server address {outcome}")
    return LoginStart(client=client, challenge=challenge, outcome=outcome)
