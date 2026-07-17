"""Tests for canonical MCP tool surface byte inventories."""

import json
from pathlib import Path

import pytest
from fastmcp import Client
from mcp.types import Tool, ToolAnnotations

from moneybin.mcp.surface_inventory import SurfaceInventory


def test_inventory_accounts_for_every_serialized_component() -> None:
    tool = Tool(
        name="example",
        description="Example.",
        inputSchema={"type": "object", "properties": {}},
        outputSchema={"type": "object", "properties": {}},
        annotations=ToolAnnotations(readOnlyHint=True),
    )
    inventory = SurfaceInventory.from_tools([tool])
    row = inventory.tools[0]
    assert inventory.tool_count == 1
    assert row.description_bytes == len(b"Example.")
    assert row.input_schema_bytes > 0
    assert row.output_schema_bytes > 0
    assert row.annotation_bytes > 0
    assert inventory.total_bytes > row.total_bytes
    assert len(inventory.sha256) == 64


def test_inventory_is_independent_of_tool_order() -> None:
    first = Tool(name="a", inputSchema={"type": "object"})
    second = Tool(name="b", inputSchema={"type": "object"})
    assert SurfaceInventory.from_tools([second, first]).to_dict() == (
        SurfaceInventory.from_tools([first, second]).to_dict()
    )


def test_inventory_omits_bytes_for_absent_components() -> None:
    inventory = SurfaceInventory.from_tools([
        Tool(name="example", inputSchema={"type": "object"})
    ])
    row = inventory.tools[0]
    assert row.description_bytes == 0
    assert row.output_schema_bytes == 0
    assert row.annotation_bytes == 0


@pytest.mark.integration
async def test_live_inventory_matches_committed_baseline() -> None:
    from moneybin.mcp.server import init_db, mcp

    init_db()
    async with Client(mcp) as client:
        actual = SurfaceInventory.from_tools(await client.list_tools()).to_dict()

    baseline = (
        Path(__file__).parents[2] / "fixtures/mcp_surface/baseline-2026-07-17.json"
    )
    assert actual == json.loads(baseline.read_text())
