"""Write or verify the canonical client-visible MCP tool inventory."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from fastmcp import Client

from moneybin.mcp.server import init_db, mcp
from moneybin.mcp.surface_inventory import SurfaceInventory


def parse_args() -> argparse.Namespace:
    """Parse snapshot output and check-mode options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


async def inventory_server() -> SurfaceInventory:
    """Inventory the public tool list exposed by the local FastMCP server."""
    init_db()
    async with Client(mcp) as client:
        return SurfaceInventory.from_tools(await client.list_tools())


def main() -> int:
    """Write the snapshot, or return non-zero when it does not match."""
    args = parse_args()
    actual = asyncio.run(inventory_server()).to_dict()
    if args.check:
        expected = json.loads(args.output.read_text())
        return 0 if actual == expected else 1
    args.output.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
