"""Integration entry point for executable CLI/MCP capability parity."""

from tests.moneybin.test_mcp.test_capability_parity import (
    UNIMPLEMENTED_CLI_PATHS,
    load_outcome_map,
    registered_cli_commands,
)


def test_live_surfaces_are_covered_by_the_outcome_map() -> None:
    """Name similarity is irrelevant; every executable route needs an outcome."""
    rows = load_outcome_map()
    mapped_cli = {path for row in rows for path in row.cli_commands}
    registered_cli = set(registered_cli_commands())
    assert registered_cli - UNIMPLEMENTED_CLI_PATHS == mapped_cli
