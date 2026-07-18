"""Tests for canonical MCP tool surface byte inventories."""

import json
from pathlib import Path

from mcp.types import Tool, ToolAnnotations

from moneybin.mcp.surface_inventory import SurfaceInventory


def test_inventory_accounts_for_serialized_components() -> None:
    tool = Tool(
        name="example",
        description="Example.",
        inputSchema={"type": "object", "properties": {}},
        annotations=ToolAnnotations(readOnlyHint=True),
    )
    inventory = SurfaceInventory.from_tools([tool])
    row = inventory.tools[0]
    assert inventory.tool_count == 1
    assert row.description_bytes == len(b"Example.")
    assert row.input_schema_bytes > 0
    assert row.output_schema_bytes == 0
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


def test_inventory_accounts_for_advertised_output_schema() -> None:
    inventory = SurfaceInventory.from_tools([
        Tool(
            name="example",
            inputSchema={"type": "object"},
            outputSchema={"type": "object", "properties": {}},
        )
    ])
    row = inventory.tools[0]

    assert row.output_schema_bytes > 0
    assert row.total_bytes == (
        row.description_bytes
        + row.input_schema_bytes
        + row.output_schema_bytes
        + row.annotation_bytes
        + row.other_bytes
    )


def test_committed_baseline_is_self_consistent() -> None:
    baseline = (
        Path(__file__).parents[2] / "fixtures/mcp_surface/baseline-2026-07-17.json"
    )
    expected = json.loads(baseline.read_text())
    tools = [Tool.model_validate(row["definition"]) for row in expected["tools"]]

    assert SurfaceInventory.from_tools(tools).to_dict() == expected


async def test_live_inventory_snapshot_is_deterministic() -> None:
    from scripts.mcp_surface_snapshot import inventory_server

    first = (await inventory_server()).to_dict()
    second = (await inventory_server()).to_dict()

    assert first == second
    assert first["tool_count"] == 105
