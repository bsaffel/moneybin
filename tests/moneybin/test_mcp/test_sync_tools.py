"""Tests for the consolidated sync MCP workflow."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import FastMCP
from fastmcp.server.elicitation import AcceptedElicitation

import moneybin.mcp.tools.sync as sync_module
from moneybin.connectors.sync_auth import SyncAuthResult, SyncLogoutResult
from moneybin.errors import UserError
from moneybin.mcp.tools.sync import register_sync_tools
from moneybin.services.sync_server_address import AddressPlan, LoginStart

_EXPECTED_TOOLS = {
    "sync_link",
    "sync_disconnect",
    "sync_pull",
    "sync_status",
}


@pytest.mark.unit
async def test_register_sync_tools_registers_expected_tools() -> None:
    """Expected sync tools register; schedule stubs/login/logout/key-rotate excluded."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # pyright: ignore[reportPrivateUsage]
    assert names == _EXPECTED_TOOLS
    assert "sync_login" not in names
    assert "sync_logout" not in names
    assert "sync_key_rotate" not in names
    assert "sync_rotate_key" not in names
    assert "sync_schedule_set" not in names
    assert "sync_schedule_show" not in names
    assert "sync_schedule_remove" not in names


@pytest.mark.unit
async def test_register_sync_workflow_tools_excludes_live_aliases() -> None:
    srv = FastMCP("test")
    sync_module.register_sync_workflow_tools(srv)

    names = {t.name for t in await srv._list_tools()}  # pyright: ignore[reportPrivateUsage]

    assert names == {"sync_link", "sync_status", "sync_pull", "sync_disconnect"}
    assert "sync_link_status" not in names
    assert "sync_connect" not in names
    assert "sync_connect_status" not in names


@pytest.mark.unit
async def test_sync_workflow_status_accepts_optional_session_id() -> None:
    srv = FastMCP("test")
    sync_module.register_sync_workflow_tools(srv)
    tool = next(t for t in await srv._list_tools() if t.name == "sync_status")  # pyright: ignore[reportPrivateUsage]

    assert tool.parameters["properties"]["session_id"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "default": None,
    }
    assert tool.output_schema is None


@pytest.mark.unit
async def test_sync_workflow_renders_explicit_auth_variants() -> None:
    """Existing tools carry strict login/status/logout variants without new names."""
    srv = FastMCP("test")
    sync_module.register_sync_workflow_tools(srv)
    tools = {tool.name: tool for tool in await srv._list_tools()}  # pyright: ignore[reportPrivateUsage]

    assert tools["sync_link"].parameters["properties"]["mode"] == {
        "default": "institution",
        "enum": ["institution", "login"],
        "type": "string",
    }
    assert tools["sync_status"].parameters["properties"]["auth_session_id"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "default": None,
    }
    assert tools["sync_disconnect"].parameters["properties"]["mode"] == {
        "default": "institution",
        "enum": ["institution", "logout"],
        "type": "string",
    }
    assert tools["sync_disconnect"].parameters["properties"]["confirmation_token"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "default": None,
    }
    assert tools["sync_disconnect"].parameters["properties"]["provider_item_id"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "default": None,
    }


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_auth_service")
async def test_sync_link_login_begins_nonblocking_safe_session(
    mock_build: MagicMock,
) -> None:
    mock_build.return_value.begin.return_value = SyncAuthResult(
        auth_session_id="syncauth_abc",
        status="pending",
        user_code="ABCD-EFGH",
        verification_url="https://auth.example/activate?code=ABCD-EFGH",
        expiration="2026-07-19T00:15:00+00:00",
    )

    envelope = await sync_module.sync_link_coarse(mode="login")

    assert envelope.data.kind == "auth"
    assert envelope.data.status == "pending"
    assert envelope.data.auth_session_id == "syncauth_abc"
    assert "device_code" not in envelope.to_dict()["data"]
    assert any("sync_status" in action for action in envelope.actions)


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_auth_service")
async def test_sync_status_completes_auth_session_idempotently(
    mock_build: MagicMock,
) -> None:
    mock_build.return_value.status.return_value = SyncAuthResult(
        auth_session_id="syncauth_abc",
        status="authenticated",
        user_code="ABCD-EFGH",
        verification_url="https://auth.example/activate?code=ABCD-EFGH",
        expiration="2026-07-19T00:15:00+00:00",
        replayed=True,
    )

    envelope = await sync_module.sync_status_coarse(auth_session_id="syncauth_abc")

    assert envelope.data.kind == "auth"
    assert envelope.data.status == "authenticated"
    assert envelope.data.replayed is True
    mock_build.return_value.status.assert_called_once_with("syncauth_abc")


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_auth_service")
async def test_sync_disconnect_logout_clears_scoped_auth(
    mock_build: MagicMock,
) -> None:
    mock_build.return_value.logout.return_value = SyncLogoutResult(
        status="logged_out",
        cleared_auth_sessions=2,
    )

    envelope = await sync_module.sync_disconnect(mode="logout")

    assert envelope.data.kind == "auth"
    assert envelope.data.status == "logged_out"
    assert envelope.data.cleared_auth_sessions == 2
    assert any("sync_link" in action for action in envelope.actions)


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_auth_service")
async def test_sync_disconnect_logout_rejects_confirmation_token(
    mock_build: MagicMock,
) -> None:
    envelope = await sync_module.sync_disconnect(
        mode="logout",
        confirmation_token="institution-only-token",  # noqa: S106  # opaque test token
    )

    assert envelope.error is not None
    assert envelope.error.code == "sync_confirmation_not_allowed"
    mock_build.return_value.logout.assert_not_called()


@pytest.mark.unit
async def test_sync_disconnect_logout_rejects_provider_item_id() -> None:
    envelope = await sync_module.sync_disconnect(
        mode="logout", provider_item_id="item_a"
    )
    assert envelope.error is not None
    assert envelope.error.code == "sync_disconnect_mode_conflict"


def test_sync_workflow_registrar_uses_public_privacy_actor_names() -> None:
    registered: list[tuple[str, str | None]] = []

    def capture(
        _mcp: object,
        _callback: object,
        name: str,
        _description: str,
        *,
        privacy_actor: str | None = None,
        **_kwargs: object,
    ) -> None:
        registered.append((name, privacy_actor))

    with patch.object(sync_module, "register", capture):
        sync_module.register_sync_workflow_tools(MagicMock())

    assert registered == [(name, name) for name, _ in registered]


# --- sync_link(mode="login", server_url=...) --------------------------------

_SERVER = "https://sync.example.test"


def _plan(change: str, *, unencrypted: bool = False) -> AddressPlan:
    return AddressPlan(url=_SERVER, change=change, unencrypted=unencrypted)  # type: ignore[arg-type]  # test literal


@pytest.fixture
def address() -> Generator[MagicMock, None, None]:
    """The address service, with a login that would succeed if reached."""
    with (
        patch.object(sync_module, "sync_server_address") as service,
        patch.object(sync_module, "SyncAuthService") as auth_service,
        patch.object(sync_module, "get_settings") as settings,
    ):
        settings.return_value.profile = "alice"
        service.LOGIN_COMMAND = "moneybin sync login --server <url>"
        service.start_login.return_value = LoginStart(
            client=MagicMock(), challenge=MagicMock(name="challenge"), outcome="saved"
        )
        auth_service.return_value.begin.return_value = SyncAuthResult(
            auth_session_id="syncauth_abc",
            status="pending",
            user_code="ABCD-EFGH",
            verification_url="https://auth.example/activate?code=ABCD-EFGH",
            expiration="2026-07-19T00:15:00+00:00",
        )
        service.auth_service = auth_service
        yield service


@contextmanager
def _prompt(answer: object) -> Generator[AsyncMock, None, None]:
    """A client that shows the confirmation prompt and answers it."""
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=answer)

    def _supports(_ctx: object) -> bool:
        return True

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("moneybin.mcp.confirmation._active_context", lambda: ctx)
        mp.setattr("moneybin.mcp.confirmation.supports_elicitation", _supports)
        yield ctx.elicit


@pytest.mark.unit
async def test_sync_link_server_url_is_login_only(address: MagicMock) -> None:
    envelope = await sync_module.sync_link_coarse(server_url=_SERVER)

    assert envelope.error is not None
    assert envelope.error.code == "sync_link_mode_conflict"
    address.plan_server_address.assert_not_called()


@pytest.mark.unit
async def test_sync_link_new_server_url_is_saved_after_a_person_confirms(
    address: MagicMock,
) -> None:
    address.plan_server_address.return_value = _plan("first")

    with _prompt(AcceptedElicitation(data=True)) as elicit:
        envelope = await sync_module.sync_link_coarse(mode="login", server_url=_SERVER)

    assert envelope.error is None, envelope.error
    assert _SERVER in elicit.call_args.args[0]
    address.start_login.assert_called_once_with(
        address.plan_server_address.return_value, surface="mcp"
    )
    # The device code that proved the server answers is the one login uses.
    address.auth_service.return_value.begin.assert_called_once_with(
        challenge=address.start_login.return_value.challenge
    )
    assert envelope.data.auth_session_id == "syncauth_abc"


@pytest.mark.unit
async def test_sync_link_prompt_says_when_it_replaces_or_is_unencrypted(
    address: MagicMock,
) -> None:
    address.plan_server_address.return_value = _plan("replace", unencrypted=True)

    with _prompt(AcceptedElicitation(data=True)) as elicit:
        await sync_module.sync_link_coarse(mode="login", server_url=_SERVER)

    message = elicit.call_args.args[0]
    assert "replace" in message.lower()
    assert "not encrypted" in message


@pytest.mark.unit
async def test_sync_link_declined_server_url_contacts_and_saves_nothing(
    address: MagicMock,
) -> None:
    address.plan_server_address.return_value = _plan("first")

    with _prompt(AcceptedElicitation(data=False)):
        envelope = await sync_module.sync_link_coarse(mode="login", server_url=_SERVER)

    assert envelope.error is not None
    address.start_login.assert_not_called()


@pytest.mark.unit
async def test_sync_link_server_url_is_refused_when_the_client_cannot_prompt(
    address: MagicMock,
) -> None:
    """No token is issued: the calling agent could redeem it for its own choice."""
    address.plan_server_address.return_value = _plan("first")

    envelope = await sync_module.sync_link_coarse(mode="login", server_url=_SERVER)

    assert envelope.error is not None
    assert envelope.error.code == "mutation_confirmation_required"
    assert "moneybin sync login --server" in (envelope.error.hint or "")
    assert "confirmation_token" not in str(envelope.to_dict())
    address.start_login.assert_not_called()


@pytest.mark.unit
async def test_sync_link_server_url_equal_to_the_current_one_needs_no_prompt(
    address: MagicMock,
) -> None:
    address.plan_server_address.return_value = _plan("unchanged")
    address.start_login.return_value = LoginStart(
        client=MagicMock(), challenge=None, outcome="unchanged"
    )

    envelope = await sync_module.sync_link_coarse(mode="login", server_url=_SERVER)

    assert envelope.error is None, envelope.error
    address.auth_service.return_value.begin.assert_called_once_with(challenge=None)


@pytest.mark.unit
async def test_sync_link_unconfigured_login_tells_the_agent_to_ask_the_user() -> None:
    not_configured = UserError(
        "No sync server address is configured for this profile.",
        code="sync_server_not_configured",
        hint="Run 'moneybin sync login --server <url>'.",
    )
    with patch.object(
        sync_module, "_build_sync_auth_service", side_effect=not_configured
    ):
        envelope = await sync_module.sync_link_coarse(mode="login")

    assert envelope.error is not None
    assert envelope.error.code == "sync_server_not_configured"
    assert "server_url" in (envelope.error.hint or "")
    assert "ask the user" in (envelope.error.hint or "").lower()


@pytest.mark.unit
async def test_unconfigured_sync_status_points_the_agent_at_sync_link() -> None:
    """An agent cannot act on a hint that names only a terminal command."""
    not_configured = UserError(
        "No sync server address is configured for this profile.",
        code="sync_server_not_configured",
        hint="Run 'moneybin sync login --server <url>'.",
    )
    with patch.object(
        sync_module.sync_server_address,
        "build_sync_client",
        side_effect=not_configured,
    ):
        envelope = await sync_module.sync_status_coarse()

    assert envelope.error is not None
    assert envelope.error.code == "sync_server_not_configured"
    assert "sync_link(mode='login', server_url=" in (envelope.error.hint or "")
