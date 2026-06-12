"""Unit tests for MCP first-run setup middleware."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp.server.elicitation import (
    AcceptedElicitation,
    DeclinedElicitation,
)
from fastmcp.tools import ToolResult

from moneybin import error_codes
from moneybin.mcp.first_run import FirstRunSetupMiddleware
from moneybin.services.profile_service import ProfileExistsError


def _fake_ctx(*, supports_elicit: bool, elicit_result: Any = None) -> MagicMock:
    """Build a fake fastmcp Context with session + elicit wired."""
    ctx = MagicMock()
    ctx.session.check_client_capability.return_value = supports_elicit
    ctx.elicit = AsyncMock(return_value=elicit_result)
    return ctx


def _fake_mw_context(ctx: MagicMock | None) -> MagicMock:
    """Build a fake MiddlewareContext carrying fastmcp_context."""
    mw_context = MagicMock()
    mw_context.fastmcp_context = ctx
    mw_context.message.name = "accounts_summary"
    return mw_context


@pytest.mark.asyncio
async def test_passthrough_when_already_configured() -> None:
    """Once configured, the middleware does not elicit; it calls through."""
    mw = FirstRunSetupMiddleware()
    mw._configured = True  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    call_next = AsyncMock(return_value="tool-result")
    ctx = _fake_ctx(supports_elicit=False)

    result = await mw.on_call_tool(_fake_mw_context(ctx), call_next)

    assert result == "tool-result"
    call_next.assert_awaited_once()
    ctx.elicit.assert_not_called()


@pytest.mark.asyncio
async def test_setup_envelope_when_elicitation_unsupported() -> None:
    """Tools-only client gets the setup_required envelope, no call_next."""
    mw = FirstRunSetupMiddleware()
    call_next = AsyncMock()
    ctx = _fake_ctx(supports_elicit=False)

    result = await mw.on_call_tool(_fake_mw_context(ctx), call_next)

    assert isinstance(result, ToolResult)
    assert result.structured_content is not None
    assert (
        result.structured_content["error"]["code"] == error_codes.INFRA_SETUP_REQUIRED
    )
    call_next.assert_not_called()


@pytest.mark.asyncio
async def test_bootstrap_and_proceed_on_accept() -> None:
    """Elicit-accept with a valid name creates the profile and proceeds."""
    mw = FirstRunSetupMiddleware()
    call_next = AsyncMock(return_value="tool-result")
    ctx = _fake_ctx(
        supports_elicit=True,
        elicit_result=AcceptedElicitation(data="Brandon"),
    )

    with patch("moneybin.mcp.first_run._bootstrap_profile") as boot:
        result = await mw.on_call_tool(_fake_mw_context(ctx), call_next)

    boot.assert_called_once_with("Brandon")
    call_next.assert_awaited_once()
    assert result == "tool-result"
    assert mw._configured is True  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_setup_envelope_on_decline() -> None:
    """Declining the elicitation returns the envelope, no bootstrap."""
    mw = FirstRunSetupMiddleware()
    call_next = AsyncMock()
    ctx = _fake_ctx(
        supports_elicit=True,
        elicit_result=DeclinedElicitation(),
    )

    with patch("moneybin.mcp.first_run._bootstrap_profile") as boot:
        result = await mw.on_call_tool(_fake_mw_context(ctx), call_next)

    boot.assert_not_called()
    call_next.assert_not_called()
    assert isinstance(result, ToolResult)
    assert result.structured_content is not None
    assert (
        result.structured_content["error"]["code"] == error_codes.INFRA_SETUP_REQUIRED
    )


@pytest.mark.asyncio
async def test_invalid_name_retries_once_then_succeeds() -> None:
    """A blank first answer re-elicits once; the valid second answer proceeds."""
    mw = FirstRunSetupMiddleware()
    call_next = AsyncMock(return_value="ok")
    ctx = MagicMock()
    ctx.session.check_client_capability.return_value = True
    ctx.elicit = AsyncMock(
        side_effect=[
            AcceptedElicitation(data="   "),  # invalid → ValueError
            AcceptedElicitation(data="Brandon"),  # valid
        ]
    )

    with patch("moneybin.mcp.first_run._bootstrap_profile") as boot:
        result = await mw.on_call_tool(_fake_mw_context(ctx), call_next)

    assert ctx.elicit.await_count == 2
    boot.assert_called_once_with("Brandon")
    assert result == "ok"


def test_bootstrap_adopts_existing_profile_on_collision() -> None:
    """A name that already exists is adopted, not errored."""
    from moneybin.mcp.first_run import (
        _bootstrap_profile,  # pyright: ignore[reportPrivateUsage]
    )

    with (
        patch("moneybin.mcp.first_run.ProfileService") as svc,
        patch("moneybin.mcp.first_run.set_default_profile") as set_default,
        patch("moneybin.mcp.first_run.set_current_profile") as set_current,
        patch("moneybin.mcp.first_run.setup_observability"),
    ):
        svc.return_value.create.side_effect = ProfileExistsError("brandon exists")
        _bootstrap_profile("Brandon")

    set_default.assert_called_once_with("brandon")
    set_current.assert_called_once_with("brandon")


def test_bootstrap_creates_and_activates_profile() -> None:
    """Happy path: create, write config, set in-process profile."""
    from moneybin.mcp.first_run import (
        _bootstrap_profile,  # pyright: ignore[reportPrivateUsage]
    )

    with (
        patch("moneybin.mcp.first_run.ProfileService") as svc,
        patch("moneybin.mcp.first_run.set_default_profile") as set_default,
        patch("moneybin.mcp.first_run.set_current_profile") as set_current,
        patch("moneybin.mcp.first_run.setup_observability") as obs,
    ):
        _bootstrap_profile("Brandon")

    svc.return_value.create.assert_called_once_with("brandon")
    set_default.assert_called_once_with("brandon")
    set_current.assert_called_once_with("brandon")
    obs.assert_called_once_with(stream="mcp", profile="brandon")
