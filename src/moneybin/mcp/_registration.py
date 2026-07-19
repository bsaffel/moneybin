"""Shared helpers for registering MCP tools with the FastMCP server.

The ``mcp_tool`` decorator (see ``decorator.py``) sets ``_mcp_*`` attributes
on each wrapped function. ``register`` translates those attributes into the
arguments expected by ``mcp.tool()`` — ``tags`` for progressive disclosure
and ``annotations`` for protocol-standard tool hints (readOnlyHint,
destructiveHint, idempotentHint, openWorldHint).
"""

from __future__ import annotations

import functools
import inspect
import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from fastmcp.tools import FunctionTool, ToolResult
from mcp.types import ToolAnnotations
from pydantic import JsonValue, TypeAdapter

from moneybin.mcp.decorator import privacy_actor_scope
from moneybin.protocol.envelope import ResponseEnvelope

if TYPE_CHECKING:
    from fastmcp import FastMCP


_JSON_OBJECT_ADAPTER = TypeAdapter(dict[str, JsonValue])


def _wire_result_adapter(
    fn: Callable[..., Any],
    *,
    privacy_actor: str | None = None,
) -> Callable[..., Any]:
    """Return a FastMCP adapter that emits the canonical envelope wire shape."""
    signature = inspect.signature(fn)

    @functools.wraps(fn)
    async def wire_result(*args: Any, **kwargs: Any) -> ToolResult:
        with privacy_actor_scope(privacy_actor):
            envelope = cast(ResponseEnvelope[Any], await fn(*args, **kwargs))
        content_json = envelope.to_json()
        return ToolResult(
            content=content_json,
            structured_content=json.loads(content_json),
        )

    wire_result.__annotations__ = {  # pyright: ignore[reportFunctionMemberAccess]
        **getattr(fn, "__annotations__", {}),
        "return": ToolResult,
    }
    wire_result.__signature__ = signature.replace(  # type: ignore[attr-defined]
        return_annotation=ToolResult
    )
    return wire_result


def _validate_input_schema_extra(
    base_schema: dict[str, Any],
    extra: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    """Validate an overlay that relates generated top-level parameters."""
    validated = _JSON_OBJECT_ADAPTER.validate_python(extra)
    collisions = set(base_schema).intersection(validated)
    if collisions:
        names = ", ".join(sorted(collisions))
        raise ValueError(f"input_schema_extra cannot replace generated keys: {names}")

    parameter_names = set(base_schema.get("properties", {}))

    def validate_parameter_references(value: JsonValue) -> None:
        if isinstance(value, dict):
            properties = value.get("properties")
            if isinstance(properties, dict):
                unknown = set(properties).difference(parameter_names)
                if unknown:
                    names = ", ".join(sorted(unknown))
                    raise ValueError(
                        f"input_schema_extra references unknown parameter: {names}"
                    )
            required = value.get("required")
            if isinstance(required, list):
                unknown = {
                    name
                    for name in required
                    if isinstance(name, str) and name not in parameter_names
                }
                if unknown:
                    names = ", ".join(sorted(unknown))
                    raise ValueError(
                        f"input_schema_extra references unknown parameter: {names}"
                    )
            for nested in value.values():
                validate_parameter_references(nested)
        elif isinstance(value, list):
            for nested in value:
                validate_parameter_references(nested)

    validate_parameter_references(validated)
    return validated


def register(
    mcp: FastMCP,
    fn: Callable[..., Any],
    name: str,
    description: str,
    *,
    privacy_actor: str | None = None,
    input_schema_extra: dict[str, JsonValue] | None = None,
) -> None:
    """Register an mcp_tool-decorated function with FastMCP.

    Reads ``_mcp_domain`` for tag-based visibility and the four annotation
    attrs (``_mcp_read_only``, ``_mcp_destructive``, ``_mcp_idempotent``,
    ``_mcp_open_world``) for the protocol-standard ``ToolAnnotations``.
    ``privacy_actor`` explicitly attributes a replacement callback to its
    public tool identity without changing existing registrations by default.
    """
    domain = getattr(fn, "_mcp_domain", None)
    tags = {domain} if domain else None
    annotations = ToolAnnotations(
        readOnlyHint=getattr(fn, "_mcp_read_only", True),
        destructiveHint=getattr(fn, "_mcp_destructive", False),
        idempotentHint=getattr(fn, "_mcp_idempotent", True),
        openWorldHint=getattr(fn, "_mcp_open_world", False),
    )
    adapter = _wire_result_adapter(fn, privacy_actor=privacy_actor)
    if input_schema_extra is None:
        mcp.tool(
            name=name,
            description=description,
            tags=tags,
            annotations=annotations,
        )(adapter)
        return

    tool = FunctionTool.from_function(
        adapter,
        name=name,
        description=description,
        tags=tags,
        annotations=annotations,
    )
    validated_extra = _validate_input_schema_extra(
        tool.parameters,
        input_schema_extra,
    )
    mcp.add_tool(
        tool.model_copy(update={"parameters": tool.parameters | validated_extra})
    )
