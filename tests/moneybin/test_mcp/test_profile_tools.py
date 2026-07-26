"""MCP tests for the profile metadata tools (multi-currency Requirement 4)."""

from __future__ import annotations

import pytest

from moneybin.mcp.tools.profile import (
    profile,
    profile_set,
    register_profile_tools,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


async def test_register_profile_tools_registers_expected() -> None:
    """The namespace is exactly one noun read plus one `_set` write."""
    from fastmcp import FastMCP

    srv = FastMCP("test")
    register_profile_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
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


async def test_profile_set_then_profile_round_trips_the_home_currency(
    mcp_db: object,
) -> None:
    """The write tool's effect is visible to the read tool."""
    write_env = await profile_set(home_currency="EUR")
    assert write_env.error is None
    assert write_env.data.home_currency == "EUR"

    assert (await profile()).data.home_currency == "EUR"


async def test_profile_set_rejects_a_malformed_currency(mcp_db: object) -> None:
    """A bad code returns an error envelope and leaves the setting unchanged."""
    env = await profile_set(home_currency="dollars")

    assert env.error is not None
    assert (await profile()).data.home_currency is None


def test_profile_read_is_annotated_read_only() -> None:
    """`profile` must not advertise itself as a mutation.

    Hosts gate on `readOnlyHint`; a read tagged destructive would make agents
    ask for confirmation on a harmless lookup.
    """
    assert getattr(profile, "_mcp_read_only", None) is True
    assert getattr(profile_set, "_mcp_read_only", None) is False
