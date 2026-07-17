"""Canonical byte inventory for the client-visible MCP tool surface."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass

from mcp.types import Tool
from pydantic import JsonValue


@dataclass(frozen=True, slots=True)
class ToolInventory:
    """Serialized-byte accounting for one MCP tool definition."""

    name: str
    definition: Mapping[str, JsonValue]
    description_bytes: int
    input_schema_bytes: int
    output_schema_bytes: int
    annotation_bytes: int
    other_bytes: int
    total_bytes: int


@dataclass(frozen=True, slots=True)
class SurfaceInventory:
    """Canonical byte inventory for an ordered-independent tool registry."""

    tools: tuple[ToolInventory, ...]
    tool_count: int
    total_bytes: int
    sha256: str

    @classmethod
    def from_tools(cls, tools: Sequence[Tool]) -> SurfaceInventory:
        """Create a stable inventory from the protocol tool definitions."""
        payloads = sorted(
            (tool.model_dump(mode="json", exclude_none=True) for tool in tools),
            key=lambda item: str(item["name"]),
        )
        canonical = _canonical_json(payloads)
        rows = tuple(_inventory_row(payload) for payload in payloads)
        encoded = canonical.encode()
        return cls(
            tools=rows,
            tool_count=len(rows),
            total_bytes=len(encoded),
            sha256=hashlib.sha256(encoded).hexdigest(),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the JSON-ready inventory representation."""
        return {
            "tools": [asdict(tool) for tool in self.tools],
            "tool_count": self.tool_count,
            "total_bytes": self.total_bytes,
            "sha256": self.sha256,
        }


def _inventory_row(payload: dict[str, JsonValue]) -> ToolInventory:
    description = payload.get("description", "")
    other = {
        key: value
        for key, value in payload.items()
        if key not in {"description", "inputSchema", "outputSchema", "annotations"}
    }
    description_bytes = len(str(description).encode())
    input_schema_bytes = _field_bytes(payload, "inputSchema")
    output_schema_bytes = _field_bytes(payload, "outputSchema")
    annotation_bytes = _field_bytes(payload, "annotations")
    other_bytes = _serialized_bytes(other)
    return ToolInventory(
        name=str(payload["name"]),
        definition=payload,
        description_bytes=description_bytes,
        input_schema_bytes=input_schema_bytes,
        output_schema_bytes=output_schema_bytes,
        annotation_bytes=annotation_bytes,
        other_bytes=other_bytes,
        total_bytes=(
            description_bytes
            + input_schema_bytes
            + output_schema_bytes
            + annotation_bytes
            + other_bytes
        ),
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _serialized_bytes(value: object) -> int:
    return len(_canonical_json(value).encode())


def _field_bytes(payload: Mapping[str, JsonValue], field: str) -> int:
    return _serialized_bytes(payload[field]) if field in payload else 0
