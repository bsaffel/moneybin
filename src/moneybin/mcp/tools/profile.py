"""Profile namespace tools — profile metadata and managed settings.

Tools:
    - profile     — read the active profile's metadata and managed settings
    - profile_set — set a managed setting (currently the home currency)

The home currency lives in ``app.profile_settings`` rather than ``config.yaml``
because the no-blend guard and the report views that read it are SQLMesh models
(``docs/specs/multi-currency.md`` Requirement 4). Reports segment money per
currency until conversion ships (M1K.2), so an agent reading a segmented report
uses this tool to learn which segment is home.
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.config import get_current_profile
from moneybin.database import get_database
from moneybin.errors import RecoveryAction
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.profile import ProfilePayload, ProfileSetPayload
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.mutation_context import current_operation_id
from moneybin.services.profile_settings_service import ProfileSettingsService


@mcp_tool(domain="profile")
def profile() -> ResponseEnvelope[ProfilePayload]:
    """Read the active profile's name and its managed settings.

    ``home_currency`` is the currency this profile treats as home. It is null
    when the user has not chosen one — MoneyBin does not assume USD.
    ``display_currency_targets`` names additional currencies refresh prepares
    for report reads.
    """
    with get_database(read_only=True) as db:
        settings = ProfileSettingsService(db).get_settings()
    return build_envelope(
        data=ProfilePayload(
            name=get_current_profile(auto_resolve=False),
            home_currency=settings.home_currency,
            display_currency_targets=settings.display_currency_targets,
        ),
        actions=['Use profile_set(display_currency_targets=["EUR"]) to change it'],
    )


@mcp_tool(domain="profile", read_only=False, idempotent=True)
def profile_set(
    home_currency: str | None = None,
    display_currency_targets: list[str] | None = None,
) -> ResponseEnvelope[ProfileSetPayload]:
    """Set the profile's home currency or explicit display targets.

    Args:
        home_currency: ISO 4217 code, three uppercase letters (USD, EUR, GBP).
        display_currency_targets: ISO 4217 codes to prepare for report reads.

    This does not convert any stored amount — every transaction and balance
    keeps its original currency. It records which currency this profile
    considers home.

    Writes one row in ``app.profile_settings`` for the active profile.
    Reversible via system_audit_undo(operation_id). The agent-visible copy of
    that disclosure is in ``register_profile_tools`` below — this docstring is
    not served.
    """
    if (home_currency is None) == (display_currency_targets is None):
        raise ValueError(
            "Set exactly one of home_currency or display_currency_targets."
        )
    with get_database(read_only=False) as db:
        service = ProfileSettingsService(db)
        if home_currency is not None:
            service.set_setting("home_currency", home_currency, actor="mcp.profile_set")
        else:
            service.set_setting(
                "display_currency_targets",
                display_currency_targets or (),
                actor="mcp.profile_set",
            )
        settings = service.get_settings()
    operation_id = current_operation_id()
    return build_envelope(
        data=ProfileSetPayload(
            home_currency=settings.home_currency,
            display_currency_targets=settings.display_currency_targets,
            operation_id=operation_id,
        ),
        actions=["Use profile() to see the profile's current settings"],
        recovery_actions=[
            RecoveryAction(
                tool="system_audit_undo",
                arguments={"operation_id": operation_id},
                rationale="Restore the previous profile setting.",
                confidence="certain",
                idempotent=False,
            ),
        ],
    )


def register_profile_tools(mcp: FastMCP) -> None:
    """Register the profile metadata read and its managed-setting write."""
    register(
        mcp,
        profile,
        "profile",
        "Read the active profile's name and managed settings, including its home "
        "currency and declared display-currency targets. The home currency is null "
        "until the user chooses one; MoneyBin never assumes USD.",
    )
    register(
        mcp,
        profile_set,
        "profile_set",
        "Set the profile's home currency or report display-currency targets (ISO "
        "4217). Updating targets prepares provider-published rate pairs; it converts "
        "nothing and does not restate accounting. Writes app.profile_settings. Reverse with "
        "system_audit_undo(operation_id=...).",
    )
