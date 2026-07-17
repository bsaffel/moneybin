"""The client-visible tool count is a public-surface fact with a hard client ceiling.

MoneyBin retired client-driven progressive disclosure (`mcp-architecture.md` §3):
every registered tool is visible at connect, so "registered" and "visible" are the
same number. That number is not free — Cascade (Windsurf), a client `mcp install`
supports, enforces a hard ceiling on how many tools it will hold at once.

These tests pin the count so crossing a client's limit is a conscious act recorded
in a diff, not something a user discovers when their tools silently stop working.
"""

import asyncio
import json
from pathlib import Path

import pytest
from mcp.types import Tool

from moneybin.mcp.surface import (
    VISIBLE_TOOL_COUNT,
    WINDSURF_ACTIVE_TOOL_CAP,
    assert_surface_contract,
    description_budget_violations,
)
from moneybin.mcp.surface_inventory import SurfaceInventory

BASELINE_PATH = (
    Path(__file__).parents[2] / "fixtures/mcp_surface/baseline-2026-07-17.json"
)

# The declared counts live in `moneybin.mcp.surface` because `mcp install` cites them
# in its Windsurf warning and cannot afford to boot the server to compute them. This
# module is what keeps that declaration honest against the live registry — bump it
# deliberately, not reflexively: read `docs/guides/mcp-clients.md` → Windsurf first,
# and if the change pushes us further past the cap, say so in the PR.


def _visible_tool_names() -> set[str]:
    """Tool names a connecting client actually receives (visibility filters applied)."""
    from moneybin.mcp.server import init_db, mcp

    init_db()
    return {tool.name for tool in asyncio.run(mcp.list_tools())}


def _load_inventory(path: Path) -> SurfaceInventory:
    payload = json.loads(path.read_text())
    tools = [Tool.model_validate(row["definition"]) for row in payload["tools"]]
    return SurfaceInventory.from_tools(tools)


def _inventory_server_sync() -> SurfaceInventory:
    from scripts.mcp_surface_snapshot import inventory_server

    return asyncio.run(inventory_server())


def _inventory(*tools: Tool) -> SurfaceInventory:
    return SurfaceInventory.from_tools(list(tools))


def _tool(name: str, description: str = "Describe a distinct operation.") -> Tool:
    return Tool(
        name=name,
        description=description,
        inputSchema={"type": "object"},
    )


def test_surface_contract_rejects_name_drift() -> None:
    inventory = _inventory(_tool("accounts"))

    with pytest.raises(AssertionError, match="Missing: transactions"):
        assert_surface_contract(
            inventory,
            expected_names=frozenset({"transactions"}),
            enforce_hard_limit=False,
            enforce_description_budget=False,
        )


def test_surface_contract_enforces_hard_limit_when_enabled() -> None:
    inventory = _inventory(*[_tool(f"tool_{index}") for index in range(51)])

    with pytest.raises(AssertionError, match="exceeds 50 tools"):
        assert_surface_contract(
            inventory,
            expected_names=frozenset(tool.name for tool in inventory.tools),
            enforce_hard_limit=True,
            enforce_description_budget=False,
        )


def test_description_budget_violations_measure_each_kind_of_debt() -> None:
    shared_opening = (
        "A distinct operation does useful work for this domain and its callers."
    )
    inventory = _inventory(
        _tool("duplicate_one", f"{shared_opening} First variation."),
        _tool("duplicate_two", f"{shared_opening} Second variation."),
        _tool("long_sentence", f"{'x' * 121}. Short tail."),
        _tool("long_description", f"Short. {'x' * 900}"),
    )

    violations = description_budget_violations(inventory)
    observed = {(violation.tool_name, violation.budget) for violation in violations}

    assert ("duplicate_one", "opening") in observed
    assert ("duplicate_two", "opening") in observed
    assert ("long_sentence", "first_sentence") in observed
    assert ("long_description", "description") in observed


def test_surface_contract_enforces_description_budget_when_enabled() -> None:
    inventory = _inventory(_tool("long_description", f"Short. {'x' * 900}"))

    with pytest.raises(AssertionError, match="description budget"):
        assert_surface_contract(
            inventory,
            expected_names=frozenset({"long_description"}),
            enforce_hard_limit=False,
            enforce_description_budget=True,
        )


@pytest.mark.integration
def test_live_surface_matches_frozen_registry() -> None:
    inventory = _inventory_server_sync()
    baseline = _load_inventory(BASELINE_PATH)
    expected_names = frozenset(tool.name for tool in baseline.tools)

    assert inventory.tool_count == VISIBLE_TOOL_COUNT
    assert_surface_contract(
        inventory,
        expected_names=expected_names,
        enforce_hard_limit=False,
        enforce_description_budget=False,
    )
    assert inventory.total_bytes > 0
    assert any(tool.output_schema_bytes > 0 for tool in inventory.tools)


@pytest.mark.integration
def test_legacy_description_debt_is_measured() -> None:
    baseline_names = frozenset(
        tool.name for tool in _load_inventory(BASELINE_PATH).tools
    )
    violations = description_budget_violations(_inventory_server_sync())

    assert violations
    assert {violation.tool_name for violation in violations} <= baseline_names


@pytest.mark.integration
def test_nothing_is_hidden_from_connecting_clients() -> None:
    """Guards the §3 claim itself: no tool is quietly withheld at connect.

    If this ever fails, MoneyBin has grown a hidden-tool tier — which would change
    the Windsurf math below and mean the docs (and the cap arithmetic) are stale.
    """
    from moneybin.mcp.server import init_db, mcp

    init_db()
    registered = {tool.name for tool in asyncio.run(mcp._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # public API filters by visibility; we want the raw registry
    assert registered == _visible_tool_names()


@pytest.mark.integration
def test_windsurf_overflow_is_documented() -> None:
    """We exceed Cascade's ceiling. That is a shipped limitation, so it must be written down.

    Not a hypothetical: `mcp install --client windsurf` writes a config for a client
    that cannot hold our surface, and Windsurf gives the user no signal about which
    tools got dropped. The guide has to tell them.
    """
    from pathlib import Path

    overflow = VISIBLE_TOOL_COUNT - WINDSURF_ACTIVE_TOOL_CAP
    assert overflow > 0, (
        "MoneyBin now fits inside Cascade's tool ceiling — delete this test and the "
        "Windsurf overflow warning in docs/guides/mcp-clients.md."
    )
    guide = Path(__file__).parents[3] / "docs" / "guides" / "mcp-clients.md"
    assert str(WINDSURF_ACTIVE_TOOL_CAP) in guide.read_text(), (
        "docs/guides/mcp-clients.md must state Cascade's 100-tool ceiling and that "
        "MoneyBin exceeds it."
    )
