"""MCP first-run setup: bootstrap a profile on the first tool call.

When `moneybin mcp serve` boots with no configured profile, the interactive
first-run wizard cannot run — its stdout prompts would corrupt the stdio
JSON-RPC stream (see docs/specs/mcp-first-run-setup.md). Instead, the server
boots unconfigured and this middleware drives setup on the first tool call:
an elicitation-capable client (Claude Desktop) is asked for a profile name;
clients without elicitation receive a single structured `setup_required`
envelope telling the user to run `moneybin profile create` and reconnect.

Only the profile name crosses the LLM context — the encryption key is
generated server-side by `ProfileService.create` and never leaves the host.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastmcp.server.elicitation import AcceptedElicitation
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools import ToolResult
from mcp.types import ClientCapabilities, ElicitationCapability

from moneybin import error_codes
from moneybin.config import set_current_profile
from moneybin.errors import UserError
from moneybin.observability import setup_observability
from moneybin.protocol.envelope import build_error_envelope
from moneybin.services.profile_service import ProfileExistsError, ProfileService
from moneybin.utils.user_config import normalize_profile_name, set_default_profile

if TYPE_CHECKING:
    import mcp.types as mt
    from fastmcp.server.context import Context

logger = logging.getLogger(__name__)

_ELICIT_PROMPT = (
    "MoneyBin isn't set up yet. What would you like to name your profile? "
    "(For example, your first name — it labels your local data store.)"
)
_SETUP_HINT = (
    "Run 'moneybin profile create <name>' in a terminal, then reconnect the MCP server."
)


class FirstRunSetupMiddleware(Middleware):
    """Drive first-run profile setup on the first tool call when unconfigured.

    Registered by `mcp serve` only on the no-profile boot path, so a
    configured server never pays for this. After a successful bootstrap the
    instance is marked configured and every later call passes straight
    through.
    """

    def __init__(self, *, verbose: bool = False) -> None:
        """Initialize the middleware with unconfigured state.

        ``verbose`` mirrors the serve command's --verbose flag so the
        observability re-init during bootstrap preserves the log level the
        operator launched with, rather than silently reverting to the
        profile's configured default.
        """
        super().__init__()
        self._configured = False
        self._verbose = verbose

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Bootstrap a profile on the first call; pass through once configured."""
        if self._configured:
            return await call_next(context)

        ctx = context.fastmcp_context
        if ctx is None or not _supports_elicitation(ctx):
            return _setup_required_result()

        name = await _elicit_profile_name(ctx)
        if name is None:
            return _setup_required_result()

        try:
            _bootstrap_profile(name, verbose=self._verbose)
        except Exception:  # noqa: BLE001 — middleware must not raise; bootstrap touches DB/keychain/FS
            logger.error(
                "First-run profile bootstrap failed; returning setup envelope",
                exc_info=True,
            )
            return _setup_required_result()
        self._configured = True
        logger.info("First-run setup complete; profile configured in-process")
        return await call_next(context)


def _supports_elicitation(ctx: Context) -> bool:
    """True when the connected client declared the elicitation capability."""
    return ctx.session.check_client_capability(
        ClientCapabilities(elicitation=ElicitationCapability())
    )


async def _elicit_profile_name(ctx: Context) -> str | None:
    """Elicit a valid profile name, retrying once on an invalid answer.

    Returns the raw accepted name (un-normalized) or None if the user
    declined/cancelled or gave two invalid answers.
    """
    for attempt in range(2):
        message = (
            _ELICIT_PROMPT
            if attempt == 0
            else (
                "That name has no usable letters or numbers. "
                "Please enter a name like 'brandon'."
            )
        )
        result = await ctx.elicit(message, response_type=str)
        if not isinstance(result, AcceptedElicitation):
            return None
        try:
            normalize_profile_name(result.data)
        except ValueError:
            continue
        return result.data
    return None


def _bootstrap_profile(name: str, *, verbose: bool = False) -> None:
    """Create-or-adopt the profile and activate it in-process and on disk."""
    normalized = normalize_profile_name(name)
    try:
        ProfileService().create(normalized)
    except ProfileExistsError:
        logger.info("First-run name matches an existing profile; adopting it")
    set_default_profile(normalized)
    set_current_profile(normalized)
    setup_observability(stream="mcp", verbose=verbose, profile=normalized)


def _setup_required_result() -> ToolResult:
    """Build the structured 'no profile yet' envelope for tools-only clients."""
    envelope = build_error_envelope(
        error=UserError(
            "MoneyBin has no profile configured yet.",
            code=error_codes.INFRA_SETUP_REQUIRED,
            hint=_SETUP_HINT,
        ),
        sensitivity="low",
    )
    return ToolResult(
        content=envelope.to_json(),
        structured_content=envelope.to_dict(),
    )
