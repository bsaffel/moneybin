"""Facts about the client-visible MCP tool surface.

Plain constants, no FastMCP import: `mcp install` needs to cite these and must not
pay the server's import cost to do it (`.claude/rules/cli.md`, Cold-Start Hygiene).
They are kept honest by `tests/moneybin/test_mcp/test_tool_surface_budget.py`, which
asserts them against the live registry — so they cannot quietly go stale.
"""

# Cascade (Windsurf): "Cascade has a limit of 100 total tools that it has access to
# at any given time." Verified 2026-07-11 against the Windsurf MCP docs. It is a
# ceiling on ACTIVE tools across ALL of the user's MCP servers, so MoneyBin's share
# of the budget is smaller still for anyone running a second server alongside us.
WINDSURF_ACTIVE_TOOL_CAP = 100

# Tools a client receives at connect. MoneyBin retired client-driven progressive
# disclosure (`mcp-architecture.md` §3), so every registered tool is visible and
# "registered" and "visible" are the same number.
VISIBLE_TOOL_COUNT = 105
