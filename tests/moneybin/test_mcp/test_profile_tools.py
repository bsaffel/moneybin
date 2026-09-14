"""MCP tests for the profile metadata tools (multi-currency Requirement 4)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moneybin import error_codes
from moneybin.mcp.tools.profile import (
    profile,
    profile_set,
    register_profile_tools,
)
from tests.moneybin.test_mcp.schema_assertions import call_tool_raw, isolated_server

pytestmark = pytest.mark.usefixtures("mcp_db")


async def test_register_profile_tools_registers_expected() -> None:
    """The namespace is exactly one noun read plus one `_set` write."""
    from fastmcp import FastMCP

    srv = FastMCP("test")
    register_profile_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # pyright: ignore[reportPrivateUsage]
    assert names == {"profile", "profile_set"}


async def test_profile_reports_no_home_currency_before_one_is_chosen(
    mcp_db: object,
) -> None:
    """An agent is told the home currency is unknown, not told it is USD.

    Reporting a default here would let an agent label a EUR-only profile's
    money as dollars — the exact failure M1K.1 exists to prevent.
    """
    env = await profile()

    assert env.error is None
    assert env.data.home_currency is None


async def test_profile_answers_on_a_database_that_predates_the_settings_table(
    mcp_db: object,
) -> None:
    """The read tool degrades to "unset" on a pre-V044 database, not an error.

    `profile` opens read-only, and read-only opens skip `init_schemas` and the
    migration runner alike — so an agent's first call against an upgrading
    user's database finds no `app.profile_settings`. The CLI twin
    (`moneybin profile show`) is covered in test_cli_profile_settings.py; both
    inherit the guard from `ProfileSettingsRepo`, and this asserts the agent
    surface independently so the guard cannot regress to CLI-only.
    """
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute("DROP TABLE app.profile_settings")

    env = await profile()

    assert env.error is None
    assert env.data.home_currency is None


async def test_profile_set_then_profile_round_trips_the_home_currency(
    mcp_db: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The write tool's effect is visible to the read tool."""
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        MagicMock(),
    )
    write_env = await profile_set(home_currency="EUR")
    assert write_env.error is None
    assert write_env.data.home_currency == "EUR"

    assert (await profile()).data.home_currency == "EUR"


async def test_profile_set_round_trips_normalized_display_currency_targets(
    mcp_db: object,
) -> None:
    """The agent surface accepts a target list without changing home currency."""
    write_env = await profile_set(display_currency_targets=["eur", "GBP", "EUR"])

    assert write_env.error is None
    assert write_env.data.display_currency_targets == ("EUR", "GBP")
    assert write_env.actions == [
        'Run refresh_run(steps=["rates"]) to fetch rates for the display targets',
        "Use profile() to see the profile's current settings",
    ]
    assert (await profile()).data.display_currency_targets == ("EUR", "GBP")


async def test_profile_set_empty_display_targets_does_not_offer_rate_refresh(
    mcp_db: object,
) -> None:
    """Clearing targets restores the no-extra-rate-work default."""
    await profile_set(display_currency_targets=["EUR"])

    env = await profile_set(display_currency_targets=[])

    assert env.data.display_currency_targets == ()
    assert env.actions == ["Use profile() to see the profile's current settings"]


async def test_profile_set_home_currency_preserves_declared_display_targets(
    mcp_db: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The patch-shaped tool changes the requested setting and leaves targets intact."""
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        MagicMock(),
    )
    await profile_set(display_currency_targets=["EUR"])

    env = await profile_set(home_currency="USD")

    assert env.error is None
    assert env.data.home_currency == "USD"
    assert env.data.display_currency_targets == ("EUR",)
    assert env.actions == ["Use profile() to see the profile's current settings"]


async def test_profile_set_rejects_a_malformed_currency(mcp_db: object) -> None:
    """A bad code returns an error envelope and leaves the setting unchanged."""
    env = await profile_set(home_currency="dollars")

    assert env.error is not None
    assert (await profile()).data.home_currency is None


async def test_profile_set_rejects_a_bad_target_without_changing_the_collection(
    mcp_db: object,
) -> None:
    """An MCP validation error cannot clobber a target selected earlier."""
    await profile_set(display_currency_targets=["EUR"])

    env = await profile_set(display_currency_targets=["not-a-code"])

    assert env.error is not None
    assert (await profile()).data.display_currency_targets == ("EUR",)


async def test_profile_set_classifies_an_empty_target_as_invalid_input(
    mcp_db: object,
) -> None:
    """A malformed list must not surface as an unclassified server failure."""
    await profile_set(display_currency_targets=["EUR"])

    env = await profile_set(display_currency_targets=["EUR", "", "GBP"])

    assert env.error is not None
    assert env.error.code == "mutation_invalid_input"
    assert (await profile()).data.display_currency_targets == ("EUR",)


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"home_currency": "USD", "display_currency_targets": ["GBP"]},
    ],
)
async def test_registered_profile_set_requires_exactly_one_setting_without_mutation(
    mcp_db: object,
    arguments: dict[str, object],
) -> None:
    """The registered transport classifies either invalid shape as a mutation error."""
    await profile_set(display_currency_targets=["EUR"])

    response = await call_tool_raw(
        isolated_server(register_profile_tools), "profile_set", arguments
    )

    assert response.structuredContent is not None
    assert (
        response.structuredContent["error"]["code"]
        == error_codes.MUTATION_INVALID_INPUT
    )
    settings = (await profile()).data
    assert settings.home_currency is None
    assert settings.display_currency_targets == ("EUR",)


async def test_profile_actions_explain_how_to_set_home_and_display_currencies(
    mcp_db: object,
) -> None:
    """A first profile read leaves both the required and optional choices visible."""
    env = await profile()

    assert env.actions == [
        'Use profile_set(home_currency="USD") to set your home currency',
        (
            'Use profile_set(display_currency_targets=["EUR"]) to set or replace '
            "the full report target list"
        ),
    ]


async def test_profile_actions_do_not_imply_display_targets_are_appended(
    mcp_db: object,
) -> None:
    """The target write replaces its collection, so its hint must say that."""
    await profile_set(display_currency_targets=["GBP"])

    env = await profile()

    assert (
        'Use profile_set(display_currency_targets=["EUR"]) to set or replace '
        "the full report target list"
    ) in env.actions
    assert not any("add report targets" in action for action in env.actions)


def test_profile_read_is_annotated_read_only() -> None:
    """`profile` must not advertise itself as a mutation.

    Hosts gate on `readOnlyHint`; a read tagged destructive would make agents
    ask for confirmation on a harmless lookup.
    """
    assert getattr(profile, "_mcp_read_only", None) is True
    assert getattr(profile_set, "_mcp_read_only", None) is False
