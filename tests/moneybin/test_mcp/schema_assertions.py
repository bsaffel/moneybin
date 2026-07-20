"""Reusable assertions for rendered MCP input schemas."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, cast

from fastmcp import Client, FastMCP
from fastmcp.tools import FunctionTool
from jsonschema import Draft202012Validator
from jsonschema import validate as validate_json_schema
from mcp.types import CallToolResult, Tool

from moneybin.errors import RecoveryAction
from moneybin.mcp.surface import STANDARD_TOOL_NAMES


def isolated_server(registrar: Callable[[FastMCP], None]) -> FastMCP:
    """Build a server containing only tools registered by ``registrar``."""
    mcp = FastMCP("schema-contract")
    registrar(mcp)
    return mcp


async def listed_tool(mcp: FastMCP, name: str) -> Tool:
    """Return one client-rendered tool definition by name."""
    async with Client(mcp) as client:
        tools = await client.list_tools()
    return next(tool for tool in tools if tool.name == name)


async def call_tool_raw(
    mcp: FastMCP,
    name: str,
    arguments: dict[str, Any],
) -> CallToolResult:
    """Call a tool through FastMCP without pre-validating its arguments."""
    async with Client(mcp) as client:
        return await client.call_tool_mcp(name, arguments)


async def assert_recovery_actions_executable(
    actions: Iterable[RecoveryAction],
) -> None:
    """Assert recovery actions name standard tools with schema-valid arguments."""
    from moneybin.mcp.server import init_db, mcp

    init_db()
    for action in actions:
        assert action.tool in STANDARD_TOOL_NAMES
        tool = await mcp.get_tool(action.tool)
        assert isinstance(tool, FunctionTool)
        Draft202012Validator.check_schema(tool.parameters)
        validate_json_schema(
            action.arguments,
            tool.parameters,
            cls=Draft202012Validator,
        )


def resolve_ref(schema: dict[str, Any], root: dict[str, Any]) -> dict[str, Any]:
    """Resolve a local JSON Pointer reference without changing the schema."""
    node = schema
    seen: set[str] = set()
    while (ref := node.get("$ref")) is not None:
        assert isinstance(ref, str), f"JSON Schema $ref must be a string: {ref!r}"
        assert ref not in seen, f"Cyclic JSON Schema $ref: {ref}"
        assert ref == "#" or ref.startswith("#/"), f"Non-local JSON Schema $ref: {ref}"
        seen.add(ref)
        value: Any = root
        if ref != "#":
            for part in ref.removeprefix("#/").split("/"):
                value = value[part.replace("~1", "/").replace("~0", "~")]
        assert isinstance(value, dict), (
            f"JSON Schema $ref does not point to an object: {ref}"
        )
        node = cast(dict[str, Any], value)
    return node


def _schema_at_path(schema: dict[str, Any], path: tuple[str, ...]) -> dict[str, Any]:
    node: Any = schema
    for part in path:
        node = resolve_ref(cast(dict[str, Any], node), schema)
        node = node[int(part)] if isinstance(node, list) else node[part]
    return resolve_ref(cast(dict[str, Any], node), schema)


def assert_literal_values(
    schema: dict[str, Any],
    path: tuple[str, ...],
    expected: set[str],
) -> None:
    """Assert a rendered literal enum at ``path`` contains exactly ``expected``."""
    node = _schema_at_path(schema, path)
    assert set(node["enum"]) == expected


def assert_discriminated_variants(
    schema: dict[str, Any],
    expected: Mapping[str, set[str]],
) -> None:
    """Assert nested discriminated variants have their expected required fields."""
    candidates = [
        _discriminated_variant_fields(union, schema)
        for union in _schema_nodes(schema)
        if "discriminator" in union and ("oneOf" in union or "anyOf" in union)
    ]
    matching = [
        candidate for candidate in candidates if candidate.keys() == expected.keys()
    ]
    assert matching, (
        f"Discriminated variants not found: {expected}; found: {candidates}"
    )
    assert expected in matching


def _schema_nodes(schema: dict[str, Any]) -> Iterator[dict[str, Any]]:
    seen: set[int] = set()

    def walk(node: Any) -> Iterator[dict[str, Any]]:
        if isinstance(node, list):
            for item in node:
                yield from walk(item)
            return
        if not isinstance(node, dict):
            return

        resolved = resolve_ref(cast(dict[str, Any], node), schema)
        if id(resolved) in seen:
            return
        seen.add(id(resolved))
        yield resolved
        for child in resolved.values():
            yield from walk(child)

    yield from walk(schema)


def _discriminated_variant_fields(
    union: dict[str, Any],
    root: dict[str, Any],
) -> dict[str, set[str]]:
    discriminator = cast(dict[str, Any], union["discriminator"])["propertyName"]
    variants = union.get("oneOf") or union.get("anyOf")
    assert isinstance(discriminator, str)
    assert isinstance(variants, list)

    observed: dict[str, set[str]] = {}
    for variant in variants:
        resolved = resolve_ref(cast(dict[str, Any], variant), root)
        property_schema = resolve_ref(
            cast(dict[str, Any], resolved["properties"][discriminator]),
            root,
        )
        value = property_schema.get("const")
        if value is None:
            values = property_schema["enum"]
            assert len(values) == 1
            value = values[0]
        assert isinstance(value, str)
        observed[value] = set(resolved.get("required", []))

    return observed
