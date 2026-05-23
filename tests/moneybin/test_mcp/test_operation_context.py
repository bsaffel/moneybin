"""The MCP tool decorator establishes one operation per tool call (REC-PR1).

A sync tool body runs via ``asyncio.to_thread``; these tests also confirm the
operation_id contextvar set in the async wrapper propagates into that worker
thread, so a sync tool's downstream audit writes inherit the call's id.
"""

from __future__ import annotations

import re
from typing import Any

from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta
from moneybin.services.mutation_context import current_operation_id

_OP_ID = re.compile(r"^op_[0-9a-f]{32}$")


def _empty_envelope() -> ResponseEnvelope[Any]:
    return ResponseEnvelope(
        summary=SummaryMeta(total_count=0, returned_count=0),
        data=[],
    )


async def test_one_call_groups_reads_under_one_operation_id() -> None:
    seen: list[str] = []

    @mcp_tool(unclassified=True)
    def my_tool() -> ResponseEnvelope[Any]:
        # Two reads inside the sync body (run in a worker thread) must agree.
        seen.append(current_operation_id())
        seen.append(current_operation_id())
        return _empty_envelope()

    await my_tool()

    assert len(seen) == 2
    assert seen[0] == seen[1]
    assert _OP_ID.match(seen[0])


async def test_separate_calls_get_distinct_operation_ids() -> None:
    seen: list[str] = []

    @mcp_tool(unclassified=True)
    def my_tool() -> ResponseEnvelope[Any]:
        seen.append(current_operation_id())
        return _empty_envelope()

    await my_tool()
    await my_tool()

    assert seen[0] != seen[1]
