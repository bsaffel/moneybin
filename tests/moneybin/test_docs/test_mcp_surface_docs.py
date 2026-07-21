"""Durable documentation checks for the bounded MCP registry."""

from __future__ import annotations

import ast
import json
import re
import warnings
from hashlib import sha256
from inspect import getdoc
from pathlib import Path
from typing import cast

import pytest
from fastmcp import Client
from fastmcp.tools import FunctionTool

from moneybin.mcp.surface import STANDARD_TOOL_NAMES
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.tables import INTERFACE_TABLES

ROOT = Path(__file__).parents[3]
SCALING_SPEC = ROOT / "docs/specs/mcp-tool-surface-scaling.md"
ARCHITECTURE_SPEC = ROOT / "docs/specs/mcp-architecture.md"
MCP_SPEC = ROOT / "docs/specs/moneybin-mcp.md"
CLI_SPEC = ROOT / "docs/specs/moneybin-cli.md"
CLIENT_COMPATIBILITY_SPEC = ROOT / "docs/specs/ai-client-compatibility.md"
ARCHIVED_MCP_SPEC = ROOT / "docs/specs/archived/moneybin-mcp-pre-cutover.md"
CAPABILITIES_SPEC = ROOT / "docs/specs/moneybin-capabilities.md"
EXTENSIONS_SPEC = ROOT / "docs/specs/extension-contracts.md"
INDEX = ROOT / "docs/specs/INDEX.md"
ADR = ROOT / "docs/decisions/016-bounded-mcp-tool-registry.md"
MCP_RULE = ROOT / ".claude/rules/mcp.md"
SURFACE_RULE = ROOT / ".claude/rules/surface-design.md"
RESOURCES = ROOT / "src/moneybin/mcp/resources.py"
PROMPTS = ROOT / "src/moneybin/mcp/prompts.py"
CHANGELOG = ROOT / "CHANGELOG.md"
CLIENT_GUIDE = ROOT / "docs/guides/mcp-clients.md"
MCP_SERVER_GUIDE = ROOT / "docs/guides/mcp-server.md"
FEATURES = ROOT / "docs/features.md"
CONTRIBUTING = ROOT / "CONTRIBUTING.md"
REPORT_RECIPE_SPEC = ROOT / "docs/specs/reports-recipe-library.md"
QUERYABLE_INTERNAL_SCHEMAS_SPEC = ROOT / "docs/specs/queryable-internal-schemas.md"
STANDARD_SNAPSHOT = ROOT / "tests/fixtures/mcp_surface/standard-45.json"
BASELINE_SNAPSHOT = ROOT / "tests/fixtures/mcp_surface/baseline-2026-07-17.json"
BASELINE_EVAL_CAPTURE = ROOT / "tests/fixtures/mcp_eval/captures/baseline-105.json"
HISTORICAL_TOOL_HEADINGS = (
    ROOT / "tests/fixtures/mcp_surface/historical-tool-headings.json"
)
OUTCOME_MAP = ROOT / "tests/fixtures/mcp_capabilities/outcome-map.json"
FROZEN_HISTORICAL_MCP_EVIDENCE = {
    BASELINE_SNAPSHOT: "89c641f7d39cad5026bb0f5d6a20254669b5f42d748b8ade8db6123d9085ae69",
    BASELINE_EVAL_CAPTURE: "39739e016c660b2461a7868795ab28028544b10782f07f902ed45a5a5c416294",
    HISTORICAL_TOOL_HEADINGS: "cd94d16725857c45ba32138fc15a000407f991f25835215f4332e03e31616498",
}
CURRENT_PUBLIC_ROOTS = tuple(
    sorted({
        *ROOT.glob("*.md"),
        *(ROOT / ".github").rglob("*.md"),
        *(ROOT / ".claude/rules").glob("*.md"),
        *(ROOT / "docs").rglob("*.md"),
    })
)
HISTORICAL_PUBLIC_PATHS = frozenset({CHANGELOG, ADR})
HISTORICAL_PUBLIC_PREFIXES = (ROOT / "docs/specs/archived",)
RETIRED_COUNT_PATTERNS = (
    re.compile(
        r"(?:~|\b(?:about|approximately|around)\s+)?"
        r"105(?:-|\s+(?:registered\s+)?)tools?\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bmore than 100 tools\b", re.IGNORECASE),
    re.compile(r"\bour 105\b", re.IGNORECASE),
    re.compile(r"\btotal_count:\s*105\b", re.IGNORECASE),
)
INLINE_CODE_SPAN_PATTERN = re.compile(r"(?<!`)`([^`\n]+)`(?!`)")
MCP_RESOURCE_URI_PATTERN = re.compile(
    r"\b[a-z][a-z0-9+.-]*://[A-Za-z0-9_./{}<>*?=&%#~-]+"
)
CALL_START_PATTERN = re.compile(r"\b([a-z][a-z0-9_]*)\(")
MCP_IDENTIFIER_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])([a-z][a-z0-9]*(?:[_.-](?:[a-z0-9]+|\*))+)(?![A-Za-z0-9_*])"
)
MCP_LABELLED_IDENTIFIER_PATTERN = re.compile(
    r"\bMCP[ \t]+(?:tool|call|operation)[ \t]+`([^`\n]+)`",
    re.IGNORECASE,
)
MCP_LABELLED_PROMPT_PATTERN = re.compile(
    r"\bMCP[ \t]+prompt[ \t]+`([^`\n]+)`",
    re.IGNORECASE,
)
MCP_FENCED_IDENTITY_PATTERN = re.compile(
    r"(?m)^```[ \t]*mcp[ \t]+(?P<kind>tools?|prompts?)[ \t]*\n"
    r"[ \t]*(?P<name>[a-z][a-z0-9]*)[ \t]*\n^```[ \t]*$"
)
FENCED_ONE_WORD_IDENTIFIER_PATTERN = re.compile(
    r"(?m)^(?!```[ \t]*mcp\b)```[^\n]*\n"
    r"[ \t]*(?P<name>[a-z][a-z0-9]*)[ \t]*\n^```[ \t]*$"
)
MCP_CONTRACT_TABLE_PATTERN = re.compile(
    r"(?m)^(?P<header>[ \t]*\|?[^\n|]+(?:\|[^\n|]+)+\|?[ \t]*)\n"
    r"^[ \t]*\|?[ \t]*:?-+:?[ \t]*(?:\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*\n"
    r"(?P<rows>(?:^[ \t]*\|?[^\n|]+(?:\|[^\n|]+)+\|?[ \t]*(?:\n|$))*)"
)
REMOVED_MCP_DECORATOR_ARGUMENT_PATTERN = re.compile(
    r"@mcp_tool\([^)]*\bsensitivity\s*=", re.DOTALL
)
PROSPECTIVE_MCP_NAMES = frozenset({
    "airtable_connect",
    "notion_connect",
    "smartsheet_connect",
    "sql_describe",
    "system_audit_undo_cascade",
    "system_audit_undo_range",
})
AMBIGUOUS_RETIRED_MCP_NAMES = frozenset({"categories", "merchants", "review"})
UNAMBIGUOUS_RETIRED_MCP_NAMES = frozenset({
    "moneybin_discover",
    "review_matches",
})


def _snapshot_tool_schemas() -> dict[str, dict[str, object]]:
    snapshot = json.loads(STANDARD_SNAPSHOT.read_text())
    return {
        str(tool["name"]): tool["definition"]["inputSchema"]
        for tool in snapshot["tools"]
    }


STANDARD_TOOL_SCHEMAS = _snapshot_tool_schemas()


def _schema_string_literals(value: object) -> set[str]:
    if isinstance(value, dict):
        literals: set[str] = set()
        for key, child in cast(dict[object, object], value).items():
            if key == "const" and isinstance(child, str):
                literals.add(child)
            elif key == "enum" and isinstance(child, list):
                literals.update(item for item in child if isinstance(item, str))
            else:
                literals.update(_schema_string_literals(child))
        return literals
    if isinstance(value, list):
        literals = set()
        for child in cast(list[object], value):
            literals.update(_schema_string_literals(child))
        return literals
    return set()


BASELINE_TOOL_NAMES = frozenset(
    str(tool["name"]) for tool in json.loads(BASELINE_SNAPSHOT.read_text())["tools"]
)
HISTORICAL_SURFACE = json.loads(HISTORICAL_TOOL_HEADINGS.read_text())
HISTORICAL_TOOL_NAMES = frozenset(
    str(name) for name in HISTORICAL_SURFACE["tool_names"]
)
HISTORICAL_PROMPT_NAMES = frozenset(
    str(name) for name in HISTORICAL_SURFACE["prompt_names"]
)
HISTORICAL_RESOURCE_URIS = frozenset(
    str(uri) for uri in HISTORICAL_SURFACE["resource_uris"]
)
REGISTERED_RESOURCE_URIS = frozenset(
    re.findall(r'@mcp\.resource\("([^"]+)"\)', RESOURCES.read_text())
)


def _current_public_mcp_docs() -> tuple[Path, ...]:
    return tuple(
        path
        for path in CURRENT_PUBLIC_ROOTS
        if path.is_file()
        and path not in HISTORICAL_PUBLIC_PATHS
        and not any(prefix in path.parents for prefix in HISTORICAL_PUBLIC_PREFIXES)
    )


def _line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _identifier_is_qualified_or_data_value(
    text: str,
    match: re.Match[str],
) -> bool:
    start, end = match.span(1) if match.lastindex else match.span()
    if start > 0 and text[start - 1] in ".:/@":
        return True
    if end < len(text) and text[end] in "./":
        return True
    if re.match(r"[ \t]*\(", text[end:]):
        return False

    prefix = text[max(0, start - 96) : start]
    return bool(
        re.search(
            r"(?:\b[a-z][a-z0-9_]*|['\"][^'\"]+['\"])[ \t]*"
            r"(?:=|:)[ \t]*['\"]?$",
            prefix,
        )
        or re.search(r"\bLiteral\[[^\]]*$", prefix)
    )


def _markdown_heading_breadcrumb(text: str, offset: int) -> tuple[str, ...]:
    headings: list[tuple[int, str]] = []
    for match in re.finditer(r"(?m)^(#{1,6})[ \t]+([^\n]+)$", text[:offset]):
        level = len(match.group(1))
        while headings and headings[-1][0] >= level:
            headings.pop()
        headings.append((level, match.group(2)))
    return tuple(title for _, title in headings)


def _markdown_table_column_header(text: str, offset: int) -> str | None:
    line_start = text.rfind("\n", 0, offset) + 1
    line_end = text.find("\n", offset)
    if line_end == -1:
        line_end = len(text)
    line = text[line_start:line_end]
    if "|" not in line:
        return None

    block_start = line_start
    while block_start > 0:
        previous_end = block_start - 1
        previous_start = text.rfind("\n", 0, previous_end) + 1
        previous = text[previous_start:previous_end]
        if "|" not in previous or not previous.strip():
            break
        block_start = previous_start

    header_end = text.find("\n", block_start)
    if header_end == -1 or header_end >= line_start:
        return None
    header = text[block_start:header_end]
    separator_end = text.find("\n", header_end + 1)
    if separator_end == -1:
        separator_end = len(text)
    separator = text[header_end + 1 : separator_end]
    if not re.fullmatch(r"[|:\- \t]+", separator):
        return None

    raw_index = line[: offset - line_start].count("|")
    header_cells = header.split("|")
    if raw_index >= len(header_cells):
        return None
    return header_cells[raw_index].strip() or None


def _mcp_contract_table_kind(header: str) -> str | None:
    normalized = re.sub(r"[`*_]", "", header).strip()
    if re.fullmatch(
        r"MCP|tools?|(?:MCP|standard|registered|public)[ \t]+"
        r"(?:tools?|operations?|calls?)",
        normalized,
        re.IGNORECASE,
    ):
        return "tool"
    if re.fullmatch(
        r"(?:(?:MCP|registered|public)[ \t]+)?prompts?",
        normalized,
        re.IGNORECASE,
    ):
        return "prompt"
    if re.fullmatch(
        r"(?:(?:MCP|registered|public)[ \t]+)?(?:resources?|URIs?)",
        normalized,
        re.IGNORECASE,
    ):
        return "resource"
    return None


def _identifier_has_non_mcp_context(text: str, match: re.Match[str]) -> bool:
    line_start = text.rfind("\n", 0, match.start()) + 1
    line_end = text.find("\n", match.end())
    if line_end == -1:
        line_end = len(text)
    line = text[line_start:line_end]
    relative_start = match.start() - line_start
    relative_end = match.end() - line_start
    context_line = f"{line[:relative_start]} {line[relative_end:]}"
    line_is_non_mcp = bool(
        re.search(
            r"\b(?:CLI|command|def|method|function|scenario|pipeline|"
            r"service|internal|audit actions?|operation_type|Literal|SQL|model|"
            r"schema|table|report ID)\b",
            context_line,
            re.IGNORECASE,
        )
    )
    if line_is_non_mcp:
        return True

    introduction = text[max(0, line_start - 320) : line_start]
    if re.search(
        r"\binternal[ \t]+(?:service|API|function|method)[^\n]{0,120}"
        r"\bcalls?[ \t]*:[ \t\n]*(?:```[a-z]*[ \t\n]*)?$",
        introduction,
        re.IGNORECASE,
    ):
        return True

    breadcrumb = " / ".join(_markdown_heading_breadcrumb(text, match.start()))
    if not re.search(r"\bMCP\b", breadcrumb, re.IGNORECASE) and re.search(
        r"\b(?:Pillars?|SyncClient|internal API|service methods?|pipeline hooks?)\b",
        breadcrumb,
        re.IGNORECASE,
    ):
        return True

    table_header = _markdown_table_column_header(text, match.start())
    if table_header is None:
        return False
    if re.search(
        r"\b(?:CLI|method|Python|function|service|hook|pillar|SQL|model|"
        r"schema|table|report ID)\b",
        table_header,
        re.IGNORECASE,
    ):
        return True
    return bool(
        not re.search(r"\bMCP\b", breadcrumb, re.IGNORECASE)
        and re.fullmatch(
            r"(?:tools?|operations?)",
            table_header.strip("`*_ "),
            re.IGNORECASE,
        )
    )


def _mcp_presentation_kind(
    text: str,
    match: re.Match[str],
) -> str | None:
    line_start = text.rfind("\n", 0, match.start()) + 1
    line_end = text.find("\n", match.end())
    if line_end == -1:
        line_end = len(text)
    direct = text[max(line_start, match.start() - 64) : min(line_end, match.end() + 64)]
    table_header = _markdown_table_column_header(text, match.start())

    if explicit := _explicit_mcp_presentation_kind(text, match):
        return explicit
    if re.search(r"\bMCP[ \t]+(?:resources?|URIs?)\b", direct, re.IGNORECASE):
        return "resource"
    if re.search(r"\bMCP[ \t]+prompts?\b", direct, re.IGNORECASE):
        return "prompt"
    if re.search(
        r"\bMCP[ \t]+(?:tools?|calls?|operations?|registry|surface)\b",
        direct,
        re.IGNORECASE,
    ):
        return "tool"
    if re.search(r"\bMCP\b", direct, re.IGNORECASE):
        if re.search(r"\b(?:resources?|URIs?)\b", direct, re.IGNORECASE):
            return "resource"
        if re.search(r"\bprompts?\b", direct, re.IGNORECASE):
            return "prompt"
        if re.search(
            r"\b(?:tools?|calls?|operations?|registry|surface|family)\b",
            direct,
            re.IGNORECASE,
        ):
            return "tool"
    if table_header and re.search(r"\bMCP\b", table_header, re.IGNORECASE):
        return "tool"
    if _identifier_has_non_mcp_context(text, match):
        return "non_mcp"

    breadcrumb = " / ".join(_markdown_heading_breadcrumb(text, match.start()))
    if not re.search(r"\bMCP\b", breadcrumb, re.IGNORECASE):
        return None
    if re.search(r"\b(?:resources?|URIs?)\b", breadcrumb, re.IGNORECASE):
        return "resource"
    if re.search(r"\bprompts?\b", breadcrumb, re.IGNORECASE):
        return "prompt"
    if re.search(
        r"\b(?:tools?|operations?|registry|surface|interface|contract matrix)\b",
        breadcrumb,
        re.IGNORECASE,
    ):
        return "tool"
    return None


def _explicit_mcp_presentation_kind(
    text: str,
    match: re.Match[str],
) -> str | None:
    line_start = text.rfind("\n", 0, match.start()) + 1
    line_end = text.find("\n", match.end())
    if line_end == -1:
        line_end = len(text)
    line = text[line_start:line_end]
    if re.search(r"\b(?:not\s+an?\s+MCP|non-MCP)\b", line, re.IGNORECASE):
        return "non_mcp"

    before = text[line_start : match.start()]
    after = text[match.end() : line_end]
    if re.search(
        r"\b(?:selected[ \t]+)?MCP[ \t]+prompt[ \t]+is[ \t]*`?$",
        before,
        re.IGNORECASE,
    ):
        return "prompt"
    if (
        re.search(
            r"\bMCP\b[^\n]{0,64}\b(?:exposes|supports|entry[ \t]+point[ \t]+is)"
            r"[ \t]*`?$",
            before,
            re.IGNORECASE,
        )
        or re.search(
            r"\bMCP[ \t]+(?:tool|operation|call)[ \t]*:[ \t]*`?$",
            before,
            re.IGNORECASE,
        )
        or (
            re.search(r"\b(?:call|use|invoke)[ \t]*`?$", before, re.IGNORECASE)
            and re.search(r"\b(?:through|via)[ \t]+MCP\b", after, re.IGNORECASE)
        )
    ):
        return "tool"
    return None


def _closed_world_presentation_kind(
    text: str,
    match: re.Match[str],
) -> str | None:
    table_header = _markdown_table_column_header(text, match.start())
    if table_header is not None:
        return _mcp_contract_table_kind(table_header)

    breadcrumb = _markdown_heading_breadcrumb(text, match.start())
    if breadcrumb:
        current = re.sub(r"[`*_]", "", breadcrumb[-1]).strip()
        ancestors = " / ".join(breadcrumb)
        has_mcp_ancestor = bool(re.search(r"\bMCP\b", ancestors, re.IGNORECASE))
        if has_mcp_ancestor and re.fullmatch(
            r"(?:(?:registered|implemented|MCP)[ \t]+)?tools?(?:[ \t]+\(\d+\))?"
            r"|standard registry|contract matrix",
            current,
            re.IGNORECASE,
        ):
            return "tool"
        if has_mcp_ancestor and re.fullmatch(
            r"(?:(?:registered|MCP)[ \t]+)?prompts?(?:[ \t]+\([^)]*\))?",
            current,
            re.IGNORECASE,
        ):
            return "prompt"
        if has_mcp_ancestor and re.fullmatch(
            r"(?:(?:registered|MCP)[ \t]+)?(?:resources?|URIs?)"
            r"(?:[ \t]+\([^)]*\))?",
            current,
            re.IGNORECASE,
        ):
            return "resource"

    return None


def _contextual_identifier_is_mcp(
    text: str,
    match: re.Match[str],
) -> bool:
    if _identifier_is_qualified_or_data_value(text, match):
        return False

    presentation_kind = _mcp_presentation_kind(text, match)
    if presentation_kind == "non_mcp":
        return False
    if presentation_kind == "tool":
        return True

    start, end = match.span()
    line_start = text.rfind("\n", 0, start) + 1
    line_end = text.find("\n", end)
    if line_end == -1:
        line_end = len(text)
    local = text[max(line_start, start - 96) : min(line_end, end + 96)]
    direct = text[max(line_start, start - 48) : min(line_end, end + 48)]
    if re.search(r"\bMCP\b", direct, re.IGNORECASE):
        return True
    if re.search(r"\bMCP\b", local, re.IGNORECASE):
        return True

    if re.match(r"[ \t]*\(", text[end:]):
        return True

    presentation = f"{' / '.join(_markdown_heading_breadcrumb(text, start))}\n{local}"
    return bool(
        re.search(
            r"\b(?:tools?|meta-tool|registry|surface|prototype|renamed|"
            r"calls?|use)\b",
            presentation,
            re.IGNORECASE,
        )
    )


def _balanced_call_arguments(text: str, open_paren: int) -> str | None:
    depth = 1
    quote: str | None = None
    escaped = False
    index = open_paren + 1
    while index < len(text):
        char = text[index]
        if quote is not None:
            if char == "\\" and index + 1 < len(text) and text[index + 1] == quote:
                after_quote = text[index + 2 :].lstrip()
                if not after_quote or after_quote[0] in ",)]}":
                    quote = None
                index += 1
            elif escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
        elif char == "\\" and index + 1 < len(text) and text[index + 1] in {"'", '"'}:
            quote = text[index + 1]
            index += 1
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[open_paren + 1 : index]
        index += 1
    return None


def _split_top_level_arguments(arguments: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depths = {"(": 0, "[": 0, "{": 0}
    closing = {")": "(", "]": "[", "}": "{"}
    quote: str | None = None
    escaped = False
    for index, char in enumerate(arguments):
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char in depths:
            depths[char] += 1
        elif char in closing:
            opener = closing[char]
            depths[opener] = max(0, depths[opener] - 1)
        elif char == "," and not any(depths.values()):
            parts.append(arguments[start:index].strip())
            start = index + 1
    tail = arguments[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _top_level_keyword(part: str) -> tuple[str, str] | None:
    quote: str | None = None
    escaped = False
    depths = {"(": 0, "[": 0, "{": 0}
    closing = {")": "(", "]": "[", "}": "{"}
    for index, char in enumerate(part):
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char in depths:
            depths[char] += 1
        elif char in closing:
            opener = closing[char]
            depths[opener] = max(0, depths[opener] - 1)
        elif char == "=" and not any(depths.values()):
            key = part[:index].strip()
            if re.fullmatch(r"[a-z][a-z0-9_]*", key):
                return key, part[index + 1 :].strip()
            return None
    return None


_MISSING_LITERAL = object()


def _literal_expression(expression: str) -> ast.expr | None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            return ast.parse(expression, mode="eval").body
    except SyntaxError:
        return None


def _literal_scalar(node: ast.expr) -> object:
    if isinstance(node, ast.Constant):
        return node.value
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError):
        return _MISSING_LITERAL


def _schema_branches(
    schema: dict[str, object],
    keyword: str,
) -> tuple[dict[str, object], ...]:
    value = schema.get(keyword)
    if not isinstance(value, list):
        return ()
    return tuple(
        cast(dict[str, object], item) for item in value if isinstance(item, dict)
    )


def _schema_properties(schema: dict[str, object]) -> dict[str, dict[str, object]]:
    value = schema.get("properties")
    if not isinstance(value, dict):
        return {}
    properties = cast(dict[object, object], value)
    return {
        str(key): cast(dict[str, object], child)
        for key, child in properties.items()
        if isinstance(child, dict)
    }


def _schema_required(schema: dict[str, object]) -> frozenset[str]:
    value = schema.get("required")
    if not isinstance(value, list):
        return frozenset()
    return frozenset(item for item in value if isinstance(item, str))


def _union_discriminator(
    branches: tuple[dict[str, object], ...],
) -> tuple[str, dict[object, dict[str, object]]] | None:
    candidates: set[str] | None = None
    for branch in branches:
        properties = _schema_properties(branch)
        keys = {
            key
            for key in _schema_required(branch)
            if key in properties and "const" in properties[key]
        }
        candidates = keys if candidates is None else candidates & keys

    for key in sorted(candidates or set()):
        mapping = {
            _schema_properties(branch)[key]["const"]: branch for branch in branches
        }
        if len(mapping) == len(branches):
            return key, mapping
    return None


def _dict_literal_items(node: ast.Dict) -> dict[str, ast.expr]:
    items: dict[str, ast.expr] = {}
    for key_node, value_node in zip(node.keys, node.values, strict=True):
        if key_node is None:
            continue
        key = _literal_scalar(key_node)
        if isinstance(key, str):
            items[key] = value_node
    return items


def _dict_literal_duplicate_keys(node: ast.Dict) -> frozenset[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for key_node in node.keys:
        if key_node is None:
            continue
        key = _literal_scalar(key_node)
        if not isinstance(key, str):
            continue
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    return frozenset(duplicates)


def _literal_json_type(node: ast.expr) -> str | None:
    if isinstance(node, ast.Dict):
        return "object"
    if isinstance(node, ast.List):
        return "array"
    if isinstance(node, ast.Tuple):
        return "tuple"
    if isinstance(node, ast.Set):
        return "set"
    value = _literal_scalar(node)
    if value is _MISSING_LITERAL or value is Ellipsis:
        return None
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    return None


def _schema_accepts_literal_type(schema: dict[str, object], node: ast.expr) -> bool:
    expected = schema.get("type")
    actual = _literal_json_type(node)
    if expected is None or actual is None:
        return True
    if isinstance(expected, list):
        return actual in expected or (actual == "integer" and "number" in expected)
    return actual == expected or (actual == "integer" and expected == "number")


def _literal_definitely_matches_schema(
    node: ast.expr,
    schema: dict[str, object],
) -> bool:
    any_of = _schema_branches(schema, "anyOf")
    if any_of:
        return any(
            _literal_definitely_matches_schema(node, branch) for branch in any_of
        )

    if not _schema_accepts_literal_type(schema, node):
        return False
    if isinstance(node, ast.Dict):
        items = _dict_literal_items(node)
        if not _schema_required(schema) <= items.keys():
            return False
        for key, child_schema in _schema_properties(schema).items():
            if key not in items:
                continue
            child = items[key]
            if "const" in child_schema:
                value = _literal_scalar(child)
                if value is _MISSING_LITERAL or value is Ellipsis:
                    return False
                if value != child_schema["const"]:
                    return False
    return True


def _schema_literal_violations(
    node: ast.expr,
    schema: dict[str, object],
    field_path: str,
    *,
    conditional: bool = False,
) -> list[str]:
    if isinstance(node, ast.Constant) and node.value is Ellipsis:
        return []

    all_of_violations: list[str] = []
    for branch in _schema_branches(schema, "allOf"):
        condition = branch.get("if")
        consequence = branch.get("then")
        if isinstance(condition, dict) and isinstance(consequence, dict):
            if _literal_definitely_matches_schema(
                node, cast(dict[str, object], condition)
            ):
                all_of_violations.extend(
                    _schema_literal_violations(
                        node,
                        cast(dict[str, object], consequence),
                        field_path,
                        conditional=True,
                    )
                )
        else:
            all_of_violations.extend(
                _schema_literal_violations(node, branch, field_path)
            )

    excluded = schema.get("not")
    if isinstance(excluded, dict) and _literal_definitely_matches_schema(
        node, cast(dict[str, object], excluded)
    ):
        return [*all_of_violations, f"{field_path} violates a conditional exclusion"]

    for keyword in ("oneOf", "anyOf"):
        branches = _schema_branches(schema, keyword)
        if not branches:
            continue
        discriminator = _union_discriminator(branches)
        if discriminator is not None and isinstance(node, ast.Dict):
            key, mapping = discriminator
            items = _dict_literal_items(node)
            if key not in items:
                return [f"{field_path} is missing required discriminator {key!r}"]
            value = _literal_scalar(items[key])
            if value is _MISSING_LITERAL or value is Ellipsis:
                return []
            branch = mapping.get(value)
            if branch is None:
                return [f"{field_path}.{key} has invalid const {value!r}"]
            return [
                *all_of_violations,
                *_schema_literal_violations(node, branch, field_path),
            ]

        compatible = tuple(
            branch for branch in branches if _schema_accepts_literal_type(branch, node)
        )
        if len(compatible) == 1:
            return [
                *all_of_violations,
                *_schema_literal_violations(node, compatible[0], field_path),
            ]
        actual = _literal_json_type(node)
        if not compatible and actual is not None:
            return [
                *all_of_violations,
                f"{field_path} has invalid literal type {actual!r}",
            ]
        return all_of_violations

    value = _literal_scalar(node)
    if value is not _MISSING_LITERAL and value is not Ellipsis:
        if isinstance(value, str) and re.fullmatch(r"<[^>]+>", value):
            if "|" in value:
                return [
                    *all_of_violations,
                    f"{field_path} uses a non-executable union selector",
                ]
            return all_of_violations
        if "const" in schema and value != schema["const"]:
            return [f"{field_path} has invalid const {value!r}"]
        enum = schema.get("enum")
        if isinstance(enum, list) and value not in enum:
            return [*all_of_violations, f"{field_path} has invalid enum {value!r}"]

    actual_type = _literal_json_type(node)
    if actual_type is not None and not _schema_accepts_literal_type(schema, node):
        return [
            *all_of_violations,
            f"{field_path} has invalid literal type {actual_type!r}",
        ]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        if isinstance(minimum, (int, float)) and value < minimum:
            return [
                *all_of_violations,
                f"{field_path} is below minimum {minimum}",
            ]

    if isinstance(node, ast.Dict):
        items = _dict_literal_items(node)
        properties = _schema_properties(schema)
        violations: list[str] = list(all_of_violations)
        for key in sorted(_dict_literal_duplicate_keys(node)):
            violations.append(f"{field_path} uses duplicate property {key!r}")
        if schema.get("additionalProperties") is False:
            for key in items.keys() - properties.keys():
                violations.append(f"{field_path} uses unknown property {key!r}")
        for key in sorted(_schema_required(schema) - items.keys()):
            child = properties.get(key)
            if conditional:
                label = "conditionally required property"
            elif child is not None and "const" in child:
                label = "required discriminator"
            else:
                label = "required property"
            violations.append(f"{field_path} is missing {label} {key!r}")
        for key, child_node in items.items():
            child_schema = properties.get(key)
            if child_schema is not None:
                violations.extend(
                    _schema_literal_violations(
                        child_node,
                        child_schema,
                        f"{field_path}.{key}",
                    )
                )
        return violations

    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        items_schema = schema.get("items")
        if not isinstance(items_schema, dict):
            return []
        violations = list(all_of_violations)
        for index, child_node in enumerate(node.elts):
            violations.extend(
                _schema_literal_violations(
                    child_node,
                    cast(dict[str, object], items_schema),
                    f"{field_path}[{index}]",
                )
            )
        return violations

    return all_of_violations


def _identifier_is_contract_subject(
    text: str,
    match: re.Match[str],
) -> bool:
    start, end = match.span(1) if match.lastindex else match.span()
    table_header = _markdown_table_column_header(text, start)
    if table_header and _mcp_contract_table_kind(table_header) is not None:
        line_start = text.rfind("\n", 0, start) + 1
        cell_start = text.rfind("|", line_start, start) + 1
        cell_prefix = text[cell_start:start]
        if cell_prefix.count("(") > cell_prefix.count(")"):
            return False
        subject_prefix = re.split(r",|/|\band\b", cell_prefix)[-1]
        return not subject_prefix.strip("` ")

    line_start = text.rfind("\n", 0, start) + 1
    line_end = text.find("\n", end)
    if line_end == -1:
        line_end = len(text)
    raw_prefix = text[line_start:start]
    prefix = raw_prefix.rstrip("` ")
    suffix = text[end:line_end].lstrip("` ")
    stripped_prefix = raw_prefix.strip()
    previous_line = ""
    if line_start > 0:
        previous_end = line_start - 1
        previous_start = text.rfind("\n", 0, previous_end) + 1
        previous_line = text[previous_start:previous_end].strip()
    is_wrapped_continuation = bool(
        stripped_prefix in {"", "`"}
        and previous_line
        and not re.match(
            r"(?:#{1,6}[ \t]|[-*][ \t]|\d+\.[ \t]|\||```)",
            previous_line,
        )
        and not previous_line.endswith((".", ":", ";", "?", "!", "`", "|"))
    )
    is_list_subject = bool(re.fullmatch(r"(?:\||[-*]|\d+\.)[ \t]*`?", stripped_prefix))
    is_code_line_subject = bool(
        (stripped_prefix == "`" and not is_wrapped_continuation)
        or (
            not stripped_prefix
            and not is_wrapped_continuation
            and (
                not suffix.strip(".;:—- ")
                or re.match(
                    r"(?:is|are|writes?|reads?|returns?|syncs?|runs?|—|-)\b",
                    suffix,
                    re.IGNORECASE,
                )
            )
        )
    )
    return bool(
        is_list_subject
        or is_code_line_subject
        or re.search(
            r"\b(?:use|call|invoke|named|include|includes|included|exposes|supports|"
            r"(?:selected[ \t]+)?MCP[ \t]+prompt is|"
            r"MCP[ \t]+(?:tool|operation|call)[ \t]*:|"
            r"(?:recommended[ \t]+)?entry point remains|"
            r"MCP[ \t]+entry point is|"
            r"(?:default[ \t]+)?template remains|"
            r"(?:available[ \t]+)?operations are)\s*$",
            prefix,
            re.IGNORECASE,
        )
    )


def _is_documentation_placeholder(expression: str) -> bool:
    return bool(
        re.fullmatch(
            r"(?:\.{3}|…|\{(?:\.{3}|…)\}|<[^>]+>|[a-z][a-z0-9_]*\??)",
            expression.strip(),
        )
    )


def _is_signature_argument(part: str) -> bool:
    return bool(
        re.fullmatch(
            r"[a-z][a-z0-9_]*\??[ \t]*:[ \t]*[^,=]+(?:=[^,]+)?",
            part.strip(),
        )
    )


def _mcp_contract_violations(
    text: str,
    path: Path,
    *,
    tool_schemas: dict[str, dict[str, object]] | None = None,
    resource_uris: frozenset[str] | None = None,
    prompt_names: frozenset[str] | None = None,
) -> list[str]:
    violations: set[str] = set()
    schemas = STANDARD_TOOL_SCHEMAS if tool_schemas is None else tool_schemas
    registered_resources = (
        REGISTERED_RESOURCE_URIS if resource_uris is None else resource_uris
    )
    registered_prompts = frozenset() if prompt_names is None else prompt_names
    schema_literals = frozenset(_schema_string_literals(schemas))
    retired_names = (BASELINE_TOOL_NAMES | HISTORICAL_TOOL_NAMES) - schemas.keys()

    for name in sorted(retired_names | PROSPECTIVE_MCP_NAMES):
        pattern = rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])"
        for match in re.finditer(pattern, text):
            requires_closed_world_context = (
                name in AMBIGUOUS_RETIRED_MCP_NAMES or name in schema_literals
            )
            is_unambiguous = (
                name in UNAMBIGUOUS_RETIRED_MCP_NAMES
                and not _identifier_is_qualified_or_data_value(text, match)
                and not _identifier_has_non_mcp_context(text, match)
            )
            is_mcp = (
                _closed_world_presentation_kind(text, match) == "tool"
                and _identifier_is_contract_subject(text, match)
                if requires_closed_world_context
                else _contextual_identifier_is_mcp(text, match)
            )
            if is_unambiguous or is_mcp:
                violations.add(
                    f"{path}:{_line_number(text, match.start())}: "
                    f"unregistered MCP identifier {name!r}"
                )

    for match in MCP_LABELLED_IDENTIFIER_PATTERN.finditer(text):
        token = match.group(1).strip()
        name = token.partition("(")[0].strip()
        if name not in schemas:
            violations.add(
                f"{path}:{_line_number(text, match.start(1))}: "
                f"unregistered MCP identifier {name!r}"
            )

    for match in MCP_LABELLED_PROMPT_PATTERN.finditer(text):
        name = match.group(1).strip()
        if name not in registered_prompts:
            violations.add(
                f"{path}:{_line_number(text, match.start(1))}: "
                f"unregistered MCP prompt {name!r}"
            )

    for name in sorted(HISTORICAL_PROMPT_NAMES):
        for match in re.finditer(rf"\b{re.escape(name)}\b", text):
            if _mcp_presentation_kind(text, match) == "prompt":
                violations.add(
                    f"{path}:{_line_number(text, match.start())}: "
                    f"unregistered MCP prompt {name!r}"
                )

    for match in INLINE_CODE_SPAN_PATTERN.finditer(text):
        token = match.group(1).strip()
        identifier = re.fullmatch(r"[a-z][a-z0-9_.-]*", token)
        if identifier is None:
            continue
        name = identifier.group()
        presentation_kind = _closed_world_presentation_kind(
            text, match
        ) or _explicit_mcp_presentation_kind(text, match)
        is_subject = _identifier_is_contract_subject(text, match)
        if (
            presentation_kind == "tool"
            and is_subject
            and not _identifier_has_non_mcp_context(text, match)
            and name not in schemas
        ):
            violations.add(
                f"{path}:{_line_number(text, match.start(1))}: "
                f"unregistered MCP identifier {name!r}"
            )
        elif (
            presentation_kind == "prompt"
            and is_subject
            and not _identifier_has_non_mcp_context(text, match)
            and name not in registered_prompts
            and name not in schemas
        ):
            violations.add(
                f"{path}:{_line_number(text, match.start(1))}: "
                f"unregistered MCP prompt {name!r}"
            )

    for match in MCP_FENCED_IDENTITY_PATTERN.finditer(text):
        name = match.group("name")
        kind = match.group("kind").rstrip("s")
        label = "identifier" if kind == "tool" else "prompt"
        registered = schemas if kind == "tool" else registered_prompts
        if name not in registered:
            violations.add(
                f"{path}:{_line_number(text, match.start('name'))}: "
                f"unregistered MCP {label} {name!r}"
            )

    for match in FENCED_ONE_WORD_IDENTIFIER_PATTERN.finditer(text):
        kind = _closed_world_presentation_kind(text, match)
        if kind not in {"tool", "prompt"}:
            continue
        name = match.group("name")
        label = "identifier" if kind == "tool" else "prompt"
        registered = schemas if kind == "tool" else registered_prompts
        if name not in registered:
            violations.add(
                f"{path}:{_line_number(text, match.start('name'))}: "
                f"unregistered MCP {label} {name!r}"
            )

    for table in MCP_CONTRACT_TABLE_PATTERN.finditer(text):
        header = table.group("header").strip().removeprefix("|").removesuffix("|")
        headers = header.split("|")
        table_kind = _closed_world_presentation_kind(text, table)
        kinds: list[str | None] = []
        for header in headers:
            kind = _mcp_contract_table_kind(header.strip())
            is_explicit_mcp = bool(re.search(r"\bMCP\b", header, re.IGNORECASE))
            kinds.append(kind if is_explicit_mcp or kind == table_kind else None)
        if not any(kind in {"tool", "prompt"} for kind in kinds):
            continue
        rows_start = table.start("rows")
        row_offset = 0
        for row in table.group("rows").splitlines(keepends=True):
            normalized = row.strip().removeprefix("|").removesuffix("|")
            cells = normalized.split("|")
            for kind, cell in zip(kinds, cells, strict=False):
                name = cell.strip()
                if kind not in {"tool", "prompt"} or not re.fullmatch(
                    r"[a-z][a-z0-9]*", name
                ):
                    continue
                label = "identifier" if kind == "tool" else "prompt"
                registered = schemas if kind == "tool" else registered_prompts
                if name not in registered:
                    violations.add(
                        f"{path}:{_line_number(text, rows_start + row_offset)}: "
                        f"unregistered MCP {label} {name!r}"
                    )
            row_offset += len(row)

    for match in MCP_IDENTIFIER_TOKEN_PATTERN.finditer(text):
        name = match.group(1)
        presentation_kind = _closed_world_presentation_kind(
            text, match
        ) or _explicit_mcp_presentation_kind(text, match)
        if (
            presentation_kind is None
            or not _identifier_is_contract_subject(text, match)
            or _identifier_has_non_mcp_context(text, match)
        ):
            continue
        if name.endswith((".md", ".py", ".json", ".sql")):
            continue
        if name in schemas or name in registered_prompts:
            continue
        if presentation_kind == "tool":
            if name.endswith("_*"):
                prefix = name[:-1]
                if any(current.startswith(prefix) for current in schemas):
                    continue
                label = "unregistered MCP identifier family"
            else:
                label = "unregistered MCP identifier"
            violations.add(
                f"{path}:{_line_number(text, match.start(1))}: {label} {name!r}"
            )
        elif presentation_kind == "prompt":
            violations.add(
                f"{path}:{_line_number(text, match.start(1))}: "
                f"unregistered MCP prompt {name!r}"
            )

    retired_name_set = set(retired_names)
    for span in _retired_mcp_code_spans(text, retired_name_set, frozenset(schemas)):
        if not span.endswith("_*"):
            continue
        for match in re.finditer(re.escape(f"`{span}`"), text):
            line_start = text.rfind("\n", 0, match.start()) + 1
            line_end = text.find("\n", match.end())
            if line_end == -1:
                line_end = len(text)
            line = text[line_start:line_end]
            explicit_family = bool(
                re.search(r"\bMCP\b", line, re.IGNORECASE)
                and re.search(r"\btools?\b|\bfamily\b", line, re.IGNORECASE)
            )
            if explicit_family or (
                _closed_world_presentation_kind(text, match) == "tool"
                and _identifier_is_contract_subject(text, match)
            ):
                violations.add(
                    f"{path}:{_line_number(text, match.start())}: "
                    f"unregistered MCP identifier family {span!r}"
                )

    for match in MCP_RESOURCE_URI_PATTERN.finditer(text):
        uri = match.group()
        if uri.endswith(".") and not uri.endswith("..."):
            uri = uri[:-1]
        scheme = uri.partition(":")[0]
        presentation_kind = _closed_world_presentation_kind(text, match)
        if scheme in {"http", "https"} and presentation_kind != "resource":
            continue
        is_mcp_resource = (
            uri in HISTORICAL_RESOURCE_URIS
            or scheme in {item.partition(":")[0] for item in registered_resources}
            or presentation_kind == "resource"
        )
        if is_mcp_resource and uri not in registered_resources:
            violations.add(
                f"{path}:{_line_number(text, match.start())}: "
                f"unregistered MCP resource {uri!r}"
            )

    for match in CALL_START_PATTERN.finditer(text):
        name = match.group(1)
        if _identifier_is_qualified_or_data_value(text, match):
            continue
        presentation_kind = _mcp_presentation_kind(text, match)
        if name not in schemas:
            if _closed_world_presentation_kind(text, match) == "tool":
                violations.add(
                    f"{path}:{_line_number(text, match.start())}: "
                    f"unregistered MCP identifier {name!r}"
                )
            continue
        if presentation_kind == "non_mcp":
            continue
        line = _line_number(text, match.start())
        arguments = _balanced_call_arguments(text, match.end() - 1)
        if arguments is None:
            violations.add(f"{path}:{line}: {name} has malformed arguments")
            continue
        schema = schemas[name]
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            continue
        seen_keywords: set[str] = set()
        has_wildcard_arguments = False
        for part in _split_top_level_arguments(arguments):
            keyword = _top_level_keyword(part)
            if keyword is None:
                if part.strip() in {"...", "…"} or _is_signature_argument(part):
                    has_wildcard_arguments = True
                else:
                    violations.add(
                        f"{path}:{line}: {name} uses positional argument {part!r}"
                    )
                continue
            key, expression = keyword
            if key in seen_keywords:
                violations.add(f"{path}:{line}: {name} uses duplicate keyword {key!r}")
                continue
            seen_keywords.add(key)
            if key not in properties:
                violations.add(f"{path}:{line}: {name} uses unknown property {key!r}")
                continue
            if re.search(r"(['\"]).*?\1\s*\|\s*(['\"]).*?\2", expression):
                violations.add(
                    f"{path}:{line}: {name}.{key} uses a non-executable union selector"
                )
                continue
            if _is_documentation_placeholder(expression):
                continue
            parse_expression = expression.replace('\\"', '"').replace("\\'", "'")
            literal = _literal_expression(parse_expression)
            property_schema = properties[key]
            if literal is None:
                if not _is_documentation_placeholder(expression):
                    violations.add(f"{path}:{line}: {name} has malformed arguments")
                continue
            if not isinstance(property_schema, dict):
                continue
            for message in _schema_literal_violations(
                literal,
                cast(dict[str, object], property_schema),
                key,
            ):
                violations.add(f"{path}:{line}: {name}.{message}")

        required = schema.get("required", [])
        if isinstance(required, list) and not has_wildcard_arguments:
            required_keys = {
                key for key in cast(list[object], required) if isinstance(key, str)
            }
            for key in sorted(required_keys - seen_keywords):
                violations.add(
                    f"{path}:{line}: {name} is missing required property {key!r}"
                )

    return sorted(violations)


def _retired_mcp_code_spans(
    text: str,
    retired_names: set[str],
    current_names: frozenset[str] = frozenset(),
) -> set[str]:
    retired_spans: set[str] = set()
    for span in INLINE_CODE_SPAN_PATTERN.findall(text):
        name, has_arguments, _ = span.partition("(")
        if name in retired_names and (not has_arguments or span.endswith(")")):
            retired_spans.add(span)
        elif (
            span.endswith("_*")
            and any(
                retired_name.startswith(span[:-1]) for retired_name in retired_names
            )
            and not any(
                current_name.startswith(span[:-1]) for current_name in current_names
            )
        ):
            retired_spans.add(span)
    return retired_spans


def test_documented_standard_names_match_runtime() -> None:
    text = SCALING_SPEC.read_text()
    registry = text.partition("## Standard registry")[2].partition(
        "### Review decision persistence"
    )[0]
    documented = frozenset(re.findall(r"`([a-z][a-z0-9_]+)`", registry))

    assert documented == STANDARD_TOOL_NAMES


def test_governing_spec_records_runtime_facts_without_promotion_claim() -> None:
    text = " ".join(SCALING_SPEC.read_text().split())
    snapshot = json.loads(STANDARD_SNAPSHOT.read_text())
    output_schema_count = sum(
        tool["definition"].get("outputSchema") is not None for tool in snapshot["tools"]
    )

    assert output_schema_count == 0

    for fact in (
        f"{snapshot['tool_count']} tools",
        f"{snapshot['total_bytes']:,} bytes",
        snapshot["sha256"],
        "zero advertised output schemas",
        "contract_passed: true",
        "promotion_ready: false",
        "context budget: not_observed",
        "host-native deferral: not_observed",
    ):
        assert fact in text
    assert "**Status:** in-progress" in text
    assert "**Status:** implemented" not in text
    assert "pre-cutover registry" in text
    assert "ADR-016" in text


def test_client_compatibility_records_current_windsurf_headroom() -> None:
    text = " ".join(CLIENT_COMPATIBILITY_SPEC.read_text().split())
    index_row = next(
        line
        for line in INDEX.read_text().splitlines()
        if "[AI Client Compatibility & Distribution]" in line
    )

    for current_fact in (
        "45 MoneyBin tools",
        "100-active-tool",
        "55 tool slots",
    ):
        assert current_fact in text
        assert current_fact in index_row
    assert "over the ceiling" not in index_row


def test_cli_mcp_examples_use_coarse_operations_with_selectors() -> None:
    text = CLI_SPEC.read_text()

    for mapping in (
        '`accounts get <id>` | `accounts(view="detail", reference=<id>)`',
        '`accounts balance history` | `accounts_balances(view="history", reference=...)`',
        '`reports networth` | `reports(report_id="core:networth")`',
        '`transactions matches pending` | `reviews(kind="matches", status="pending")`',
        '`transactions matches run` | `refresh_run(steps=["match"])`',
    ):
        assert mapping in text
    assert "`transactions matches undo <match_id>`" in text
    assert '`system_audit(view="history", ...)`' in text
    assert '`system_audit(view="events", ...)`' in text
    assert "`system_audit_undo(operation_id=<operation_id>)`" in text
    assert "The identifiers are not interchangeable" in text
    assert (
        "`transactions matches undo <id>` | `system_audit_undo(operation_id=<id>)`"
    ) not in text
    assert "MCP mirrors CLI exactly" not in text


def test_architecture_documents_validated_manual_transaction_creation() -> None:
    text = " ".join(ARCHITECTURE_SPEC.read_text().split())

    assert "`transactions_create` is the validated batch-creation surface" in text
    assert "No general-purpose transaction insertion surface" not in text


def test_features_maps_categorization_queue_to_the_reviews_capability() -> None:
    outcome_map = json.loads(OUTCOME_MAP.read_text())
    reviews_capability = next(
        item for item in outcome_map if item["capability_id"] == "reviews.read"
    )
    snapshot = json.loads(STANDARD_SNAPSHOT.read_text())
    reviews_tool = next(tool for tool in snapshot["tools"] if tool["name"] == "reviews")
    kinds = reviews_tool["definition"]["inputSchema"]["properties"]["kind"]["enum"]

    assert "transactions categorize pending" in reviews_capability["cli_commands"]
    assert reviews_capability["mcp_tools"] == ["reviews"]
    assert "categorization" in kinds

    queue_line = next(
        line
        for line in FEATURES.read_text().splitlines()
        if "Curator-impact queue" in line
    )
    assert '`reviews(kind="categorization", status="pending")`' in queue_line
    assert "transactions_categorize_assist" not in queue_line


def test_features_documents_the_executable_manual_batch_contract() -> None:
    from moneybin.mcp.tools.curation import transactions_create

    outcome_map = json.loads(OUTCOME_MAP.read_text())
    create_capability = next(
        item for item in outcome_map if item["capability_id"] == "transactions.create"
    )
    doc = getdoc(transactions_create)

    assert create_capability["mcp_tools"] == ["transactions_create"]
    assert create_capability["service_methods"] == [
        "moneybin.services.transaction_service.TransactionService.create_manual_batch"
    ]
    assert doc is not None
    batch_range = re.search(r"Create (\d+)\.\.(\d+) manual transactions", doc)
    assert batch_range is not None

    manual_line = next(
        line
        for line in FEATURES.read_text().splitlines()
        if "Manual transaction entry" in line
    )
    assert (
        f"validated batch of {batch_range.group(1)}–{batch_range.group(2)} transactions"
        in manual_line
    )
    assert "one at a time" not in manual_line
    assert "not yet wired" not in manual_line


def test_cli_spec_describes_outcome_parity_without_input_identity() -> None:
    text = CLI_SPEC.read_text()

    assert (
        "Equivalent requests reach the mapped services and preserve observable "
        "outcomes."
    ) in " ".join(text.split())
    assert "Equal inputs reach the same services" not in text


def test_future_mcp_capabilities_remain_unnamed_until_admission() -> None:
    for path in (ARCHITECTURE_SPEC, MCP_SPEC, SCALING_SPEC, MCP_RULE, SURFACE_RULE):
        text = " ".join(path.read_text().split())
        assert "Future MCP capabilities remain unnamed until admission" in text, path

    combined = "\n".join(
        path.read_text()
        for path in (ARCHITECTURE_SPEC, MCP_SPEC, SCALING_SPEC, MCP_RULE, SURFACE_RULE)
    )
    for speculative_name in (
        "investments.record_trade",
        "airtable_connect",
        "smartsheet_connect",
        "notion_connect",
    ):
        assert speculative_name not in combined


def test_governance_describes_one_current_registry_and_future_admission() -> None:
    for path in (
        ARCHITECTURE_SPEC,
        MCP_SPEC,
        CAPABILITIES_SPEC,
        EXTENSIONS_SPEC,
        INDEX,
        ADR,
        MCP_RULE,
        SURFACE_RULE,
        CLIENT_GUIDE,
        MCP_SERVER_GUIDE,
    ):
        text = " ".join(path.read_text().split())
        assert "45-tool standard registry" in text, path
        assert "same registry" in text, path

    adr = " ".join(ADR.read_text().split())
    rule = " ".join(MCP_RULE.read_text().split())
    extensions = " ".join(EXTENSIONS_SPEC.read_text().split())
    client_guide = " ".join(CLIENT_GUIDE.read_text().split())
    assert "**Status:** Proposed" in adr
    assert "promotion_ready: false" in adr
    assert "seven-question admission record" in rule
    assert "reports never consume tool slots" in extensions
    assert "without reconnect, packs, or profiles" in client_guide


def test_current_mcp_guidance_uses_only_standard_tool_names() -> None:
    prompt_text = PROMPTS.read_text()
    resource_text = RESOURCES.read_text()

    assert "accounts_balances" in prompt_text
    assert "accounts(view='balances')" not in prompt_text
    assert "sql_query" in resource_text
    assert "45-tool standard registry" in resource_text


def test_runtime_mcp_modules_do_not_point_to_removed_spec_sections() -> None:
    for path in (PROMPTS, RESOURCES):
        text = path.read_text()
        assert "moneybin-mcp.md`` section 14" not in text
        assert "moneybin-mcp.md`` section 15" not in text


def test_changelog_records_prelaunch_surface_cutover() -> None:
    assert CHANGELOG.exists()
    assert ADR.exists()
    assert ARCHIVED_MCP_SPEC.exists()
    assert BASELINE_SNAPSHOT.exists()
    assert STANDARD_SNAPSHOT.exists()

    text = CHANGELOG.read_text()

    assert "45-tool standard registry" in text
    assert "pre-launch" in text
    assert "reports" in text


def test_spec_index_describes_the_current_mcp_contract() -> None:
    row = next(
        line for line in INDEX.read_text().splitlines() if "[MoneyBin MCP]" in line
    )

    for current_fact in (
        "45-tool standard registry",
        "seven prompts",
        "single `reports` catalog",
        "outcome parity",
        "zero output schemas",
        "Promotion",
    ):
        assert current_fact in row
    assert "`reports_*`" not in row
    assert "sync + transform" not in row


def test_spec_index_keeps_deferred_loading_optional() -> None:
    row = next(
        line
        for line in INDEX.read_text().splitlines()
        if "[MCP Tool Surface Scaling]" in line
    )

    assert "deferred-loading hosts may use that same registry" in row
    assert "deferred-loading hosts use that same registry" not in row


def test_active_governance_does_not_teach_legacy_registry_names() -> None:
    active_paths = (
        ARCHITECTURE_SPEC,
        MCP_SPEC,
        EXTENSIONS_SPEC,
        MCP_RULE,
        SURFACE_RULE,
        CLIENT_GUIDE,
        MCP_SERVER_GUIDE,
    )
    stale_terms = (
        "reports_spending",
        "reports_cashflow",
        "reports_networth",
        "transactions_get",
        "accounts_get",
        "transactions_review",
        "privacy_status",
        "accounts_links_pending",
        "gsheet_reconnect",
        "105-tool",
        "approximately 45",
        "proposal does not change operating",
    )

    for path in active_paths:
        text = path.read_text()
        for term in stale_terms:
            assert term not in text, f"{path}: {term}"


def test_retired_mcp_code_spans_include_calls_and_wildcard_families() -> None:
    baseline = json.loads(BASELINE_SNAPSHOT.read_text())
    current = json.loads(STANDARD_SNAPSHOT.read_text())
    retired_names = {
        tool["name"] for tool in baseline["tools"] if "_" in tool["name"]
    } - {tool["name"] for tool in current["tools"]}

    text = (
        "`system_audit_history(...)` and `transactions_matches_*` are retired; "
        "`categories` remains a valid domain noun."
    )

    assert _retired_mcp_code_spans(text, retired_names) == {
        "system_audit_history(...)",
        "transactions_matches_*",
    }


def test_retired_mcp_code_spans_ignore_generic_inline_code() -> None:
    retired_names = {"system_audit_history", "transactions_matches_run"}

    assert _retired_mcp_code_spans("`categories` and `*`", retired_names) == set()


async def test_current_public_docs_use_the_live_mcp_contract() -> None:
    from moneybin.mcp import resources as resources_module
    from moneybin.mcp.prompts import PROMPT_FUNCTIONS
    from moneybin.mcp.server import init_db, mcp

    assert resources_module.resource_schema is not None
    init_db()
    async with Client(mcp) as client:
        tools = await client.list_tools()
        resources = await client.list_resources()
        prompts = await client.list_prompts()
    live_schemas = {tool.name: dict(tool.inputSchema) for tool in tools}
    live_resources = frozenset(str(resource.uri) for resource in resources)
    live_prompts = frozenset(prompt.name for prompt in prompts)
    expected_prompts = frozenset(prompt.__name__ for prompt in PROMPT_FUNCTIONS)
    assert set(live_schemas) == STANDARD_TOOL_NAMES
    assert live_resources == REGISTERED_RESOURCE_URIS
    assert live_prompts == expected_prompts
    violations: list[str] = []

    for path in _current_public_mcp_docs():
        text = path.read_text()
        relative = path.relative_to(ROOT)
        violations.extend(
            _mcp_contract_violations(
                text,
                relative,
                tool_schemas=live_schemas,
                resource_uris=live_resources,
                prompt_names=live_prompts,
            )
        )
        for pattern in RETIRED_COUNT_PATTERNS:
            if match := pattern.search(text):
                violations.append(f"{relative}: retired count {match.group()!r}")

    assert not violations, "\n".join(violations)


async def test_contract_scanner_uses_the_live_registry_and_resources() -> None:
    from moneybin.mcp import resources as resources_module
    from moneybin.mcp.prompts import PROMPT_FUNCTIONS
    from moneybin.mcp.server import init_db, mcp

    assert resources_module.resource_schema is not None
    init_db()
    async with Client(mcp) as client:
        tools = await client.list_tools()
        resources = await client.list_resources()
        prompts = await client.list_prompts()

    assert set(STANDARD_TOOL_SCHEMAS) == STANDARD_TOOL_NAMES
    assert {tool.name for tool in tools} == STANDARD_TOOL_NAMES
    assert {str(resource.uri) for resource in resources} == REGISTERED_RESOURCE_URIS
    assert REGISTERED_RESOURCE_URIS == {"moneybin://schema"}
    assert {prompt.name for prompt in prompts} == {
        prompt.__name__ for prompt in PROMPT_FUNCTIONS
    }


def test_mcp_spec_is_current_and_archives_the_pre_cutover_catalog() -> None:
    text = MCP_SPEC.read_text()
    registry = text.partition("## Standard registry")[2].partition(
        "## Contract matrix"
    )[0]
    documented = frozenset(re.findall(r"`([a-z][a-z0-9_]+)`", registry))

    assert documented == STANDARD_TOOL_NAMES
    assert ARCHIVED_MCP_SPEC.exists()
    assert "Archived pre-cutover catalog" in ARCHIVED_MCP_SPEC.read_text()


async def test_mcp_spec_enumerates_the_registered_prompts_and_resource() -> None:
    from moneybin.mcp.server import init_db, mcp

    init_db()
    text = MCP_SPEC.read_text()
    prompt_section = text.partition("### Registered prompts")[2].partition(
        "### Resources"
    )[0]
    documented_prompts = set(re.findall(r"`([a-z][a-z0-9_]+)`", prompt_section))
    registered_prompts = {
        prompt.name for prompt in await mcp.list_prompts(run_middleware=False)
    }

    assert documented_prompts == registered_prompts
    assert "`moneybin://schema`" in text
    architecture = ARCHITECTURE_SPEC.read_text()
    assert "Seven prompts" in architecture
    assert "`sync_review`" in architecture


def test_mcp_contract_matrix_uses_the_snapshot_input_property_names() -> None:
    matrix = (
        MCP_SPEC
        .read_text()
        .partition("## Contract matrix")[2]
        .partition("## Response contract")[0]
    )
    documented = dict(re.findall(r"^\| `([^`]+)` \| (.*?) \|", matrix, re.MULTILINE))
    snapshot = json.loads(STANDARD_SNAPSHOT.read_text())

    expected = {
        tool["name"]: ", ".join(
            f"`{property_name}`"
            for property_name in sorted(tool["definition"]["inputSchema"]["properties"])
        )
        for tool in snapshot["tools"]
    }

    assert documented == expected


async def test_mcp_contract_matrix_matches_live_sensitivity_metadata() -> None:
    from inspect import getclosurevars

    from moneybin.mcp.server import init_db, mcp

    init_db()
    matrix = (
        MCP_SPEC
        .read_text()
        .partition("## Contract matrix")[2]
        .partition("## Response contract")[0]
    )
    documented = dict(
        re.findall(r"^\| `([^`]+)` \| .*? \| .*? \| (.*?) \|$", matrix, re.MULTILINE)
    )

    assert set(documented) == STANDARD_TOOL_NAMES
    for name in STANDARD_TOOL_NAMES:
        tool = await mcp.get_tool(name)
        assert isinstance(tool, FunctionTool)
        callback = getclosurevars(tool.fn).nonlocals["fn"]
        maximum = callback._mcp_maximum_sensitivity  # type: ignore[attr-defined]
        assert maximum is not None, f"{name}: missing declared maximum sensitivity"
        sensitivity = maximum.value
        is_dynamic = callback._mcp_dynamic_classification  # type: ignore[attr-defined]
        safety = documented[name].lower()

        assert f"maximum {sensitivity}" in safety, f"{name}: {documented[name]}"
        if is_dynamic:
            assert "dynamic" in safety, f"{name}: {documented[name]}"

    assert (
        documented["reports"].lower()
        == "read / dynamic / maximum critical / report-derived"
    )


def test_mcp_rule_repeats_the_exact_seven_question_admission_record() -> None:
    text = MCP_RULE.read_text()
    admission = text.partition("**Admission sequence.**")[2].partition(
        "**Output-schema admission.**"
    )[0]

    for question in (
        "Which capability ID and user intent does it serve?",
        "What is the closest existing tool?",
        "Why can it not be an existing filter, projection, method, batch input,",
        "Which safety, authorization, sensitivity, confirmation, output, audit, or",
        "What serialized count and byte delta does it add?",
        "Which evaluation tasks prove the new surface is better?",
        "Does the resulting standard registry remain within budget and workflow",
    ):
        assert question in admission


def test_surface_rule_uses_current_registry_examples() -> None:
    text = SURFACE_RULE.read_text()

    for example in (
        "`accounts_set`",
        "`taxonomy_set`",
        "`privacy_consent_set`",
        "`transactions_create`",
        "`refresh_run`",
        "`system_status`, `import_status`, `sync_status`",
        "`_revert`, `_disconnect`, `_decide`,\n`_annotate`",
    ):
        assert example in text
    for stale_example in (
        "accounts_summary",
        "budget_set",
        "tags_set",
        "categories_create",
        "categories_delete",
        "`_status` | Not admitted",
    ):
        assert stale_example not in text


def test_active_consent_guidance_discloses_deferred_enforcement() -> None:
    for path in (MCP_SPEC, ARCHITECTURE_SPEC, MCP_RULE):
        text = path.read_text()
        assert "global consent enforcement is deferred" in text, path

    current_contract = MCP_SPEC.read_text()
    rule = MCP_RULE.read_text()
    assert "Sensitivity is middleware-enforced" not in current_contract
    assert (
        "Without consent, tools return useful degraded envelopes"
        not in current_contract
    )
    assert "The middleware enforces consent and redaction automatically" not in rule
    assert "Tools without consent return **degraded responses**" not in rule


def test_client_guide_keeps_where_data_goes_body_before_next_section() -> None:
    text = CLIENT_GUIDE.read_text()
    where_data_goes = text.partition("## Where data goes")[2].partition(
        "## Bounded tool surface"
    )[0]

    assert "The MCP transport is local-only" in where_data_goes
    assert "The MCP client" in where_data_goes
    assert "Sensitivity tiers" in where_data_goes
    assert "Other MoneyBin surfaces" in where_data_goes
    assert "Local-LLM clients" in where_data_goes


def test_current_public_scan_exempts_only_approved_historical_boundaries() -> None:
    current_paths = set(_current_public_mcp_docs())

    assert set(ROOT.glob("*.md")) - {CHANGELOG} <= current_paths
    assert ROOT / "CONTRIBUTING.md" in current_paths
    assert ROOT / "SECURITY.md" in current_paths
    assert ROOT / ".github/ai-review-protocol.md" in current_paths
    assert ROOT / "docs/decisions/000-duckdb-as-embedded-store.md" in current_paths
    assert ADR not in current_paths
    assert ARCHIVED_MCP_SPEC not in current_paths
    assert CHANGELOG not in current_paths


def test_current_docs_do_not_advertise_removed_mcp_decorator_arguments() -> None:
    stale: list[str] = []

    for path in _current_public_mcp_docs():
        if REMOVED_MCP_DECORATOR_ARGUMENT_PATTERN.search(path.read_text()):
            stale.append(str(path.relative_to(ROOT)))

    assert stale == []


def test_removed_mcp_decorator_argument_guard_spans_lines() -> None:
    stale = """@mcp_tool(
        sensitivity="medium",
        read_only=True,
    )"""

    assert REMOVED_MCP_DECORATOR_ARGUMENT_PATTERN.search(stale)


@pytest.mark.parametrize(
    ("text", "identifier"),
    [
        ("Call reports_spending for the total.", "reports_spending"),
        ("```text\ntransactions_get(account='checking')\n```", "transactions_get"),
        ("Use `spending_summary detail=summary` for context.", "spending_summary"),
        ("| `review_matches` | Review the queue |", "review_matches"),
        (
            "Future MCP tool `future_budget_sync` would write state.",
            "future_budget_sync",
        ),
    ],
)
def test_mcp_contract_scan_finds_retired_and_unregistered_identifiers(
    text: str,
    identifier: str,
) -> None:
    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any(identifier in violation for violation in violations)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "### MCP tools\n\nMCP tools include `future_budget_sync(account_id=...)`.",
            "unregistered MCP identifier 'future_budget_sync'",
        ),
        (
            "## MCP Interface\n\n### Tools\n\n| `future_budget_sync` | Sync budgets |",
            "unregistered MCP identifier 'future_budget_sync'",
        ),
        (
            "MCP tools include `transactions_matches_*`.",
            "unregistered MCP identifier family 'transactions_matches_*'",
        ),
        (
            "Use the MCP `reports_*` tool family.",
            "unregistered MCP identifier family 'reports_*'",
        ),
        (
            "Future MCP tool `future.budget_sync` would write state.",
            "unregistered MCP identifier 'future.budget_sync'",
        ),
        (
            "### Surface parity\n\n| CLI | MCP |\n|---|---|\n"
            "| `moneybin budgets sync` | `future_budget_sync` |",
            "unregistered MCP identifier 'future_budget_sync'",
        ),
        (
            "## MCP Interface\n\n### Tools\n\n"
            "The recommended entry point remains `future_budget_sync`.",
            "unregistered MCP identifier 'future_budget_sync'",
        ),
        (
            "## MCP Interface\n\n### Tools\n\n```text\nfuture_budget_sync\n```",
            "unregistered MCP identifier 'future_budget_sync'",
        ),
        (
            "## MCP Interface\n\n### Tools\n\n```text\nreports_*\n```",
            "unregistered MCP identifier family 'reports_*'",
        ),
        (
            "### MCP resources\n\nRead `future://context`.",
            "unregistered MCP resource 'future://context'",
        ),
        (
            "### MCP prompts\n\nUse `future_review`.",
            "unregistered MCP prompt 'future_review'",
        ),
        (
            "### MCP prompts\n\nThe default template remains `future_review`.",
            "unregistered MCP prompt 'future_review'",
        ),
        (
            "The selected MCP prompt is `future_review`.",
            "unregistered MCP prompt 'future_review'",
        ),
        (
            "MCP operation: `future_budget_sync`.",
            "unregistered MCP identifier 'future_budget_sync'",
        ),
    ],
)
def test_mcp_contract_scan_uses_section_context_for_unknown_contracts(
    text: str,
    expected: str,
) -> None:
    violations = _mcp_contract_violations(
        text,
        Path("docs/example.md"),
        prompt_names=frozenset({"monthly_review"}),
    )

    assert any(expected in violation for violation in violations)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "```mcp tool\nadvisor\n```",
            "unregistered MCP identifier 'advisor'",
        ),
        (
            "```mcp prompt\nbriefing\n```",
            "unregistered MCP prompt 'briefing'",
        ),
        (
            "## MCP Interface\n\n### Tools\n\n```text\nadvisor\n```",
            "unregistered MCP identifier 'advisor'",
        ),
        (
            "## MCP Interface\n\n### Prompts\n\n```text\nbriefing\n```",
            "unregistered MCP prompt 'briefing'",
        ),
        (
            "| MCP tool | Purpose |\n|---|---|\n| advisor | Give advice |",
            "unregistered MCP identifier 'advisor'",
        ),
        (
            "| MCP prompt | Purpose |\n|---|---|\n| briefing | Summarize finances |",
            "unregistered MCP prompt 'briefing'",
        ),
        (
            "## MCP Interface\n\n### Tools\n\n"
            "Tool | Purpose\n---|---\nadvisor | Give advice",
            "unregistered MCP identifier 'advisor'",
        ),
        (
            "## MCP Interface\n\n### Prompts\n\n"
            "Prompt | Purpose\n---|---\nbriefing | Summarize finances",
            "unregistered MCP prompt 'briefing'",
        ),
    ],
)
def test_mcp_contract_scan_rejects_one_word_names_in_explicit_identity_contexts(
    text: str,
    expected: str,
) -> None:
    violations = _mcp_contract_violations(
        text,
        Path("docs/example.md"),
        prompt_names=frozenset({"monthly_review"}),
    )

    assert any(expected in violation for violation in violations)


@pytest.mark.parametrize(
    "text",
    [
        "An advisor can explain a budget without becoming an MCP tool.",
        ("| MCP tool | Domain value |\n|---|---|\n| reports | advisor |"),
        ("| Tool | Purpose |\n|---|---|\n| advisor | Give advice |"),
        "## Developer tools\n\n```text\nadvisor\n```",
        (
            "# MCP Server\n\n## Developer tools\n\n"
            "| Tool | Purpose |\n|---|---|\n| advisor | Give advice |"
        ),
        (
            "# MCP Server\n\n## Developer tools\n\n"
            "Tool | Purpose\n---|---\nadvisor | Give advice"
        ),
    ],
)
def test_mcp_contract_scan_ignores_one_word_prose_and_domain_values(
    text: str,
) -> None:
    assert _mcp_contract_violations(text, Path("docs/example.md")) == []


@pytest.mark.parametrize("identifier", ["categories", "merchants", "review"])
def test_mcp_contract_scan_rejects_ambiguous_retired_names_in_tool_tables(
    identifier: str,
) -> None:
    text = f"## MCP Interface\n\n### Tools\n\n| `{identifier}` | Retired tool |"

    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any(identifier in violation for violation in violations)


@pytest.mark.parametrize(
    "body",
    ["- `categories` — retired", "`categories` is retired."],
)
def test_mcp_contract_scan_rejects_ambiguous_names_in_standard_registry(
    body: str,
) -> None:
    text = f"# MCP Server\n\n### Standard registry\n\n{body}"

    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any("categories" in violation for violation in violations)


@pytest.mark.parametrize("identifier", ["account_id", "confirmation_token", "pending"])
def test_mcp_contract_scan_does_not_treat_contract_subjects_as_schema_details(
    identifier: str,
) -> None:
    text = (
        "## MCP Interface\n\n### Tools\n\n"
        "| Tool | Purpose |\n|---|---|\n"
        f"| `{identifier}` | Not a registered tool |"
    )

    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any(identifier in violation for violation in violations)


@pytest.mark.parametrize(
    "text",
    [
        "Run the CLI command `moneybin transform validate`.",
        "The internal `ImportService.import_file` method owns ingestion.",
        "The SQL model `reports.spending_trend` is queryable.",
        "The report ID is `core:networth_history`.",
        "The request discriminator is `kind='match'`.",
        "The internal `LedgerService.transactions(date_from='2026-01-01')` method is not an MCP call.",
        'The internal function `reviews(kind="match")` returns rows.',
        (
            "### Review plumbing\n\nThe internal service calls:\n\n"
            '`reviews(kind="match")`'
        ),
        (
            "### Review plumbing\n\nThe internal service calls:\n\n"
            '```python\nreviews(kind="match")\n```'
        ),
        (
            "## Pillars\n\n"
            "| Pillar | Purpose |\n|---|---|\n"
            "| Auto-rule generation | Hook `categorize_transaction()` / "
            "`categorize_items()` to synthesize rules. |"
        ),
        (
            "### SyncClient\n\n"
            "| Method | API call | Purpose |\n|---|---|---|\n"
            "| `list_institutions()` | `GET /institutions` | Connected institutions |"
        ),
        (
            "## Connection overhead\n\n"
            "| Operation | Median | Notes |\n|---|---|---|\n"
            "| `refresh_views()` | 1.7 ms | Internal database initialization |"
        ),
        (
            "## Developer tools\n\n"
            "| Tool | Purpose |\n|---|---|\n"
            "| `future_budget_sync` | Internal test helper |"
        ),
        (
            "## MCP Interface\n\n### Tools\n\n"
            "**Why notes stay imperative inside one MCP umbrella:** "
            "add/edit/delete on\n`note_id` have distinct lifecycle semantics."
        ),
        "Categories, merchants, and reviews are ordinary domain nouns.",
    ],
)
def test_mcp_contract_scan_ignores_non_mcp_identifiers(text: str) -> None:
    assert _mcp_contract_violations(text, Path("docs/example.md")) == []


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "transactions_categorize_rules_set(targets=[...])",
            "unknown property 'targets'",
        ),
        ("reviews(kind='match')", "invalid enum 'match'"),
        ("refresh_run(steps=['matching'])", "invalid enum 'matching'"),
        (
            'reviews(kind="matches", status="pending" | "history")',
            "non-executable union selector",
        ),
    ],
)
def test_mcp_contract_scan_validates_presented_properties_and_enums(
    text: str,
    expected: str,
) -> None:
    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any(expected in violation for violation in violations)


def test_mcp_contract_scan_validates_assigned_calls() -> None:
    invalid_live = _mcp_contract_violations(
        'result = reviews(kind="match")', Path("docs/example.md")
    )
    invalid_unknown = _mcp_contract_violations(
        "### MCP tools\n\nresult = future_budget_sync(account_id=...)",
        Path("docs/example.md"),
    )

    assert any("invalid enum 'match'" in item for item in invalid_live)
    assert any("future_budget_sync" in item for item in invalid_unknown)


@pytest.mark.parametrize(
    "text",
    [
        "transactions_categorize_rules_set(rules=[...])",
        'reviews(kind="matches", status="pending")',
        'refresh_run(steps=["match", "transform"])',
        '{"actions":["Use reports(report_id=\\"core:spending\\")"]}',
    ],
)
def test_mcp_contract_scan_accepts_live_properties_and_enums(text: str) -> None:
    assert _mcp_contract_violations(text, Path("docs/example.md")) == []


def test_mcp_contract_scan_validates_resource_uris() -> None:
    valid = _mcp_contract_violations(
        "Read `moneybin://schema`.", Path("docs/example.md")
    )
    invalid = _mcp_contract_violations(
        "Read `moneybin://status` and `moneybin://schema/all`.",
        Path("docs/example.md"),
    )

    assert valid == []
    assert any("moneybin://status" in violation for violation in invalid)
    assert any("moneybin://schema/all" in violation for violation in invalid)


def test_mcp_contract_scan_ignores_web_urls_and_consumes_resource_queries() -> None:
    assert (
        _mcp_contract_violations(
            "See https://example.com/docs.", Path("docs/example.md")
        )
        == []
    )

    web_resource = _mcp_contract_violations(
        "## MCP Interface\n\n### Resources\n\nRead https://future.example/context.",
        Path("docs/example.md"),
    )
    assert any("https://future.example/context" in item for item in web_resource)

    invalid = _mcp_contract_violations(
        "Read `moneybin://schema?scope=all#details`.", Path("docs/example.md")
    )
    assert any(
        "moneybin://schema?scope=all#details" in violation for violation in invalid
    )

    assert (
        _mcp_contract_violations("Read moneybin://schema.", Path("docs/example.md"))
        == []
    )


@pytest.mark.parametrize("uri", ["future://...", "moneybin://..."])
def test_mcp_contract_scan_rejects_ellipsis_resource_families(uri: str) -> None:
    violations = _mcp_contract_violations(
        f"## MCP Interface\n\n### Resources\n\nRead `{uri}`.",
        Path("docs/example.md"),
    )

    assert any(uri in item for item in violations)


def test_mcp_contract_scan_ignores_non_mcp_ellipsis_uris() -> None:
    assert (
        _mcp_contract_violations(
            "## UI protocol\n\nUse `ui://...` for app resources.",
            Path("docs/example.md"),
        )
        == []
    )


@pytest.mark.parametrize(
    "text",
    [
        "The MCP server exposes `future_budget_sync`.",
        "Call `future_budget_sync` through MCP.",
        "The MCP entry point is `future_budget_sync`.",
        "MCP supports `future_budget_sync`.",
    ],
)
def test_mcp_contract_scan_rejects_unknown_tools_in_explicit_mcp_prose(
    text: str,
) -> None:
    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any("future_budget_sync" in item for item in violations)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "identity_links_decide(decisions=[{"
            '"kind": "merchant_links", "decision_id": "d1", '
            '"decision": "reject"}])',
            "decisions[0].kind has invalid const 'merchant_links'",
        ),
        (
            "identity_links_decide(decisions=[{"
            '"decision_id": "d1", "decision": "reject"}])',
            "decisions[0] is missing required discriminator 'kind'",
        ),
        (
            "identity_links_decide(decisions=[{"
            '"kind": "merchant_link", "decision_id": "d1", '
            '"decision": "reject", "status": "pending"}])',
            "decisions[0] uses unknown property 'status'",
        ),
        (
            "transactions_categorize_rules_set(rules=[{"
            '"kind": "rule", "state": "present", '
            '"matcher": {"type": "substring", "value": "MARKET"}, '
            '"category": "Groceries", "priority": 200}])',
            "rules[0].matcher.type has invalid enum 'substring'",
        ),
        (
            "reviews_decide(decisions=[{"
            '"kind": "match", "decision_id": "<id>", '
            '"decision": "<accept\\|reject>"}])',
            "decisions[0].decision uses a non-executable union selector",
        ),
        (
            'reviews_decide(decisions=[{"kind": "match"}])',
            "decisions[0] is missing required property 'decision_id'",
        ),
        (
            "identity_links_decide(decisions=[{"
            '"kind": "account_link", "decision_id": "d1", '
            '"decision": "accept"}])',
            "decisions[0] is missing conditionally required property 'target_id'",
        ),
        (
            "identity_links_decide(decisions=[{"
            '"kind": "account_link", "decision_id": "d1", '
            '"decision": "reject", "target_id": "a1"}])',
            "decisions[0] violates a conditional exclusion",
        ),
    ],
)
def test_mcp_contract_scan_validates_concrete_nested_request_shapes(
    text: str,
    expected: str,
) -> None:
    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any(expected in violation for violation in violations)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("import_preview()", "missing required property 'file_path'"),
        ('transactions(limit="ten")', "limit has invalid literal type"),
        ("accounts(limit=0)", "limit is below minimum 1"),
        ('reviews("matches")', "uses positional argument"),
        ("reviews(matches)", "uses positional argument"),
        (
            'reviews(kind="matches", kind="summary")',
            "uses duplicate keyword 'kind'",
        ),
        (
            '`reviews(kind="matches" status="pending")`',
            "has malformed arguments",
        ),
        (
            'refresh_run(steps={"match"})',
            "steps has invalid literal type 'set'",
        ),
        (
            "reviews_decide(decisions=[{"
            '"kind": "match", "kind": "auto_rule", '
            '"decision_id": "d1", "decision": "reject"}])',
            "decisions[0] uses duplicate property 'kind'",
        ),
        ("import_confirm(account_id=...)", "missing required property 'preview_id'"),
        (
            "identity_links_decide(confirmation_token=...)",
            "missing required property 'decisions'",
        ),
    ],
)
def test_mcp_contract_scan_rejects_non_executable_call_syntax(
    text: str,
    expected: str,
) -> None:
    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any(expected in violation for violation in violations)


@pytest.mark.parametrize(
    "text",
    [
        "identity_links_decide(decisions=[...])",
        (
            "identity_links_decide(decisions=[{"
            '"kind": "merchant_link", "decision_id": ..., '
            '"decision": "reject"}])'
        ),
        (
            "transactions_categorize_rules_set(rules=[{"
            '"kind": "rule", "state": "present", '
            '"matcher": {"type": "contains", "value": ...}, '
            '"category": ..., "priority": ...}])'
        ),
    ],
)
def test_mcp_contract_scan_accepts_nested_placeholders_and_live_literals(
    text: str,
) -> None:
    assert _mcp_contract_violations(text, Path("docs/example.md")) == []


@pytest.mark.parametrize(
    ("text", "identifier"),
    [
        ("Use `csv_preview_file(path=...)` to inspect it.", "csv_preview_file"),
        ("The `moneybin_discover` meta-tool enables a domain.", "moneybin_discover"),
        (
            "### Retrofit existing tools\n\n"
            "9f — Transform (`transform_apply`, `transform_validate`).",
            "transform_apply",
        ),
        ('Call `moneybin.discover(domain="reports")`.', "moneybin.discover"),
        ("The tool was renamed from singular `import_file`.", "import_file"),
    ],
)
def test_mcp_contract_scan_finds_pre_baseline_names_in_mcp_context(
    text: str,
    identifier: str,
) -> None:
    violations = _mcp_contract_violations(text, Path("docs/example.md"))

    assert any(identifier in violation for violation in violations)


@pytest.mark.parametrize(
    "text",
    [
        "operation_type: Literal['transform_apply']",
        "The internal `ImportService.import_file` method owns ingestion.",
        "The internal function `transactions_get` accepts an account filter.",
        "```python\ndef import_file(path: Path) -> ImportResult: ...\n```",
        "Scenario pipeline steps include `import_file`.",
        "The SQL model `reports.spending_summary` is queryable.",
        "The schema table `core.reports_spending_summary` is materialized.",
        "The report ID is `core:spending_summary`.",
    ],
)
def test_mcp_contract_scan_contextual_history_avoids_non_mcp_false_positives(
    text: str,
) -> None:
    assert _mcp_contract_violations(text, Path("docs/example.md")) == []


def test_mcp_contract_scan_validates_labelled_prompt_names() -> None:
    valid = _mcp_contract_violations(
        "Use MCP prompt `monthly_review`.",
        Path("docs/example.md"),
        prompt_names=frozenset({"monthly_review"}),
    )
    invalid = _mcp_contract_violations(
        "Use MCP prompt `future_review`.",
        Path("docs/example.md"),
        prompt_names=frozenset({"monthly_review"}),
    )

    assert valid == []
    assert any("unregistered MCP prompt 'future_review'" in item for item in invalid)

    malformed = _mcp_contract_violations(
        "Use MCP prompt `future-review`.",
        Path("docs/example.md"),
        prompt_names=frozenset({"monthly_review"}),
    )
    assert any("unregistered MCP prompt 'future-review'" in item for item in malformed)


def test_frozen_historical_mcp_evidence_has_exact_paths_and_hashes() -> None:
    expected = {
        ROOT / "tests/fixtures/mcp_surface/baseline-2026-07-17.json",
        ROOT / "tests/fixtures/mcp_eval/captures/baseline-105.json",
        ROOT / "tests/fixtures/mcp_surface/historical-tool-headings.json",
    }

    assert set(FROZEN_HISTORICAL_MCP_EVIDENCE) == expected
    for path, expected_sha256 in FROZEN_HISTORICAL_MCP_EVIDENCE.items():
        assert path.exists()
        assert _sha256(path) == expected_sha256


def test_historical_mcp_surface_corpus_is_complete_and_scannable() -> None:
    corpus = json.loads(HISTORICAL_TOOL_HEADINGS.read_text())
    assert corpus["tool_names"] == sorted(set(corpus["tool_names"]))
    assert corpus["prompt_names"] == sorted(set(corpus["prompt_names"]))
    assert corpus["resource_uris"] == sorted(set(corpus["resource_uris"]))
    tool_names = frozenset(str(name) for name in corpus["tool_names"])
    prompt_names = frozenset(str(name) for name in corpus["prompt_names"])
    resource_uris = frozenset(str(uri) for uri in corpus["resource_uris"])

    assert "full git history" in str(corpus["source"])
    assert (len(tool_names), len(prompt_names), len(resource_uris)) == (315, 10, 14)
    assert BASELINE_TOOL_NAMES - STANDARD_TOOL_NAMES <= tool_names
    archived_tool_sources = {
        ROOT / "docs/specs/archived/sync-client-integration.md": {
            "sync.trigger",
            "sync.status",
            "sync.connect",
        },
        ROOT / "docs/specs/archived/ofx-import.md": {
            "accounts.list",
            "accounts.balances",
            "transactions.search",
            "institutions.list",
            "import_file",
        },
        ROOT / "docs/specs/archived/w2-extraction.md": {"tax.w2_summary"},
    }
    for source, names in archived_tool_sources.items():
        source_text = source.read_text()
        assert names <= tool_names
        assert all(f"`{name}`" in source_text for name in names)
    assert {
        "accounts.details",
        "budget.summary",
        "categorize.auto_review",
        "overview.status",
        "privacy.status",
        "spending.compare",
        "transactions.matches.revoke",
        "sync.pull",
        "sync.disconnect",
        "sync.schedule",
    } <= tool_names
    assert {
        "analyze_spending",
        "find_anomalies",
        "tax_preparation",
        "account_overview",
        "transaction_search",
        "categorize_transactions",
        "auto_categorize_transactions",
    } <= prompt_names
    archived_resources = ROOT / "docs/specs/archived/mcp-read-tools.md"
    archived_resources_text = archived_resources.read_text()
    archived_resource_uris = {
        "moneybin://schema/tables",
        "moneybin://schema/{table_name}",
        "moneybin://accounts/summary",
        "moneybin://transactions/recent",
        "moneybin://w2/{tax_year}",
    }
    assert archived_resource_uris <= resource_uris
    assert all(f"`{uri}`" in archived_resources_text for uri in archived_resource_uris)
    assert {
        "accounts://summary",
        "net-worth://summary",
        "moneybin://accounts",
        "moneybin://investments/holdings",
        "moneybin://privacy",
        "moneybin://recent-curation",
        "moneybin://spending/categories",
        "moneybin://status",
        "moneybin://tools",
    } <= resource_uris

    retired_tools = tool_names - STANDARD_TOOL_NAMES
    for name in retired_tools:
        text = f"## MCP Interface\n\n### Tools\n\n| `{name}` | Historical tool |"
        violations = _mcp_contract_violations(text, Path("docs/example.md"))
        assert any(name in item for item in violations), name

    for name in prompt_names:
        text = f"## MCP Interface\n\n### Prompts\n\n| `{name}` | Historical prompt |"
        violations = _mcp_contract_violations(
            text,
            Path("docs/example.md"),
            prompt_names=frozenset({"monthly_review"}),
        )
        assert any(name in item for item in violations), name

    for uri in resource_uris:
        text = f"## MCP Interface\n\n### Resources\n\n| `{uri}` | Historical resource |"
        violations = _mcp_contract_violations(text, Path("docs/example.md"))
        assert any(uri in item for item in violations), uri


@pytest.mark.parametrize(
    ("relative", "stale"),
    [
        ("docs/specs/observability.md", "reports_spending"),
        ("docs/specs/observability.md", "transactions_get"),
        ("docs/specs/categorization-bulk.md", "transactions_categorize_bulk_apply"),
        ("docs/specs/smart-import-financial.md", "renamed from `import_file`"),
        ("docs/specs/2026-05-13-plaid-sync-design.md", "spending_summary"),
        ("docs/guides/connect-gsheet.md", "airtable_connect"),
        ("docs/guides/connect-gsheet.md", "smartsheet_connect"),
        ("docs/guides/connect-gsheet.md", "notion_connect"),
        ("docs/specs/data-recovery-contract.md", "transactions(text=...)"),
        ("docs/specs/data-recovery-contract.md", "system_audit_undo_range"),
        ("docs/specs/data-recovery-contract.md", "system_audit_undo_cascade"),
        ("docs/roadmap.md", "moneybin://context"),
        ("docs/roadmap.md", "approximately 45"),
        ("docs/specs/mcp-sql-discoverability.md", "moneybin://status"),
        ("docs/specs/mcp-sql-discoverability.md", "moneybin://accounts"),
        ("docs/specs/mcp-sql-discoverability.md", "moneybin://privacy"),
        ("docs/specs/mcp-sql-discoverability.md", "moneybin://schema/<table>"),
        ("docs/specs/mcp-sql-discoverability.md", "moneybin://schema/all"),
        ("docs/specs/mcp-sql-discoverability.md", "sql_describe"),
        ("docs/specs/matching-same-record-dedup.md", "review_matches"),
        ("docs/specs/matching-transfer-detection.md", "review_matches"),
        ("docs/specs/smart-import-confirmation.md", "Shape 5 (read-projection)"),
        ("docs/specs/smart-import-confirmation.md", "Read-only inspect"),
        ("docs/specs/smart-import-confirmation.md", "No new tables"),
        ("docs/specs/moneybin-mcp.md", "Inspect an import before mutation"),
        ("docs/specs/database-writer-coordination.md", "Every registered tool"),
        (".claude/rules/surface-design.md", "rules_set(targets="),
        ("docs/specs/merchant-entity-resolution.md", 'status="pending" | "history"'),
        (
            "docs/specs/reports-net-worth.md",
            '"core:networth" | "core:networth_history"',
        ),
        ("docs/specs/moneybin-cli.md", 'view="history"|"events"'),
        (
            "docs/reference/account-matching.md",
            "identity_links_decide(decisions=[...])",
        ),
        ("docs/specs/architecture-shared-primitives.md", "Same noun ordering"),
        ("docs/specs/architecture-shared-primitives.md", "optional `domain`"),
        ("docs/specs/account-identity-resolution.md", "(CLI-only, matching today's"),
        ("docs/specs/account-management.md", 'view="list" | "detail"'),
        (
            "docs/specs/account-management.md",
            "All write tools are dynamically classified",
        ),
        ("docs/specs/account-management.md", "### Resource"),
        ("docs/specs/connect-gsheet.md", "### `system_status` extension"),
        (
            "docs/specs/investments-data-model.md",
            "Market value/unrealized gain unavailable until price feeds ship",
        ),
        ("docs/specs/privacy-data-classification.md", "SpendingService.by_category()"),
        (
            "docs/specs/privacy-data-classification.md",
            "NetworthService.history()` | medium",
        ),
        ("docs/features.md", "Declarative reports (in flight)"),
        ("docs/guides/mcp-server.md", "Supported hosts may defer"),
        ("docs/specs/INDEX.md", "Eight `reports.*` SQLMesh views"),
        ("docs/specs/extension-contracts.md", "for every row above"),
        ("docs/guides/mcp-clients.md", "The initial registry"),
    ],
)
def test_final_review_stale_contract_corpus_is_absent(
    relative: str,
    stale: str,
) -> None:
    assert stale not in (ROOT / relative).read_text()


def test_import_preview_docs_match_the_live_write_annotation_and_retention() -> None:
    confirmation = " ".join(
        (ROOT / "docs/specs/smart-import-confirmation.md").read_text().split()
    )
    mcp_spec = " ".join(MCP_SPEC.read_text().split())
    coordination = " ".join(
        (ROOT / "docs/specs/database-writer-coordination.md").read_text().split()
    )

    for text in (confirmation, mcp_spec, coordination):
        assert "readOnlyHint=false" in text
        assert "app.import_previews" in text
        assert "raw.import_preview_snapshots" in text
        assert "audit" in text
        assert "expir" in text


def test_final_review_calls_are_separate_and_schema_complete() -> None:
    merchant = (ROOT / "docs/specs/merchant-entity-resolution.md").read_text()
    networth = (ROOT / "docs/specs/reports-net-worth.md").read_text()
    cli = (ROOT / "docs/specs/moneybin-cli.md").read_text()
    account_matching = (ROOT / "docs/reference/account-matching.md").read_text()
    sync = (ROOT / "docs/specs/sync-overview.md").read_text()

    assert 'reviews(kind="merchant_links", status="pending")' in merchant
    assert 'reviews(kind="merchant_links", status="history")' in merchant
    assert '"kind":"merchant_link"' in merchant
    assert 'reports(report_id="core:networth")' in networth
    assert 'reports(report_id="core:networth_history", parameters={' in networth
    assert '"from_date":' in networth and '"to_date":' in networth
    assert '`system_audit(view="history", ...)`' in cli
    assert '`system_audit(view="events", ...)`' in cli
    assert '"kind":"match"' in cli
    assert '"kind":"account_link"' in account_matching
    assert 'sync_disconnect(mode="institution", institution=<institution>)' in sync
    assert "confirmation_token=<token>" in sync
    assert 'sync_disconnect(mode="logout")' in sync


def test_final_review_architecture_and_current_prose_match_runtime() -> None:
    architecture = " ".join(
        (ROOT / "docs/specs/architecture-shared-primitives.md").read_text().split()
    )
    recovery = (ROOT / "docs/specs/data-recovery-contract.md").read_text()
    account_identity = (ROOT / "docs/specs/account-identity-resolution.md").read_text()
    account_management = (ROOT / "docs/specs/account-management.md").read_text()
    privacy = (ROOT / "docs/specs/privacy-data-classification.md").read_text()
    index = (ROOT / "docs/specs/INDEX.md").read_text()
    extensions = (ROOT / "docs/specs/extension-contracts.md").read_text()

    assert "observable outcomes" in architecture
    assert "45-tool standard registry" in architecture
    assert "domain metadata does not control disclosure" in architecture
    assert "refresh_run()" in recovery
    assert '`system_audit(view="history"' in account_identity
    assert "system_audit_undo(operation_id=...)" in account_identity
    assert "include_closed is a read filter" in account_management
    assert "data.warnings" in account_management
    assert 'reports(report_id="core:spending")' in privacy
    assert 'reports(report_id="core:networth_history"' in privacy
    assert privacy.count("| high |") >= 2
    assert "Eight registered report routes" in index
    assert "seven `reports.*` SQLMesh views" in index
    assert "Report rows use `reports(report_id=..., parameters=...)`" in extensions


def test_final_review_host_and_report_wording_is_current() -> None:
    features = (ROOT / "docs/features.md").read_text()
    server_guide = " ".join(MCP_SERVER_GUIDE.read_text().split())
    client_guide = CLIENT_GUIDE.read_text()
    roadmap = (ROOT / "docs/roadmap.md").read_text()

    assert "Declarative reports" in features
    assert "implemented" in features.partition("Declarative reports")[2].splitlines()[0]
    assert "A capable host may optionally defer" in server_guide
    assert "Observed host-native deferral evidence remains absent" in server_guide
    assert (
        "Promotion remains blocked until both observed context-budget evidence "
        "and observed host-native-deferral evidence exist."
    ) in server_guide
    assert "The current registry advertises zero output schemas" in client_guide
    assert "registry of 45 intent-shaped tools" in roadmap


def test_final_review_refresh_and_report_counts_match_runtime() -> None:
    recovery = (ROOT / "docs/specs/data-recovery-contract.md").read_text()
    features = FEATURES.read_text()
    roadmap = (ROOT / "docs/roadmap.md").read_text()
    reports = REPORT_RECIPE_SPEC.read_text()
    default_sequence = "gsheet → match → transform → categorize → identity"

    assert default_sequence in recovery
    assert default_sequence in features
    assert "8 registered report routes" in roadmap
    assert "seven `reports.*` SQLMesh views" in reports
    assert "six `@report` SQL runners" in reports
    assert "two service-backed net-worth routes" in reports
    assert "eight SQLMesh views" not in reports
    assert "all eight `reports.*` views" not in reports


def test_current_report_docs_match_live_catalog_and_interface_views() -> None:
    report_routes = {report.report_id for report in get_report_catalog().list()}
    report_views = {
        table.full_name for table in INTERFACE_TABLES if table.schema == "reports"
    }
    assert len(report_views) == 7
    assert len(report_routes) == 8

    current_surface_summary = (
        f"{('zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven')[len(report_views)]} "
        "SQLMesh report views back "
        f"{('zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight')[len(report_routes)]} "
        "report routes"
    )
    queryable_schemas = QUERYABLE_INTERNAL_SCHEMAS_SPEC.read_text()
    reports = REPORT_RECIPE_SPEC.read_text()
    normalized_reports = " ".join(reports.split())

    assert (
        current_surface_summary.lower() in " ".join(queryable_schemas.split()).lower()
    )
    assert current_surface_summary.lower() in normalized_reports.lower()
    assert "reports.uncategorized_queue" not in reports
    assert "core.uncategorized_queue" in reports
    assert "internal categorization-review queue" in reports
    assert "no registered report route" in normalized_reports


def test_final_review_mcp_decorator_and_writer_guidance_match_runtime() -> None:
    architecture = (ROOT / "docs/architecture.md").read_text()
    mcp_architecture = ARCHITECTURE_SPEC.read_text()
    smart_import = (ROOT / "docs/specs/smart-import-transform.md").read_text()
    adr_008 = (ROOT / "docs/decisions/008-fastmcp-3x-sdk.md").read_text()
    mcp_rule = MCP_RULE.read_text()

    for path, text in (
        (CONTRIBUTING, CONTRIBUTING.read_text()),
        (ROOT / "docs/architecture.md", architecture),
        (ARCHITECTURE_SPEC, mcp_architecture),
        (ROOT / "docs/specs/smart-import-transform.md", smart_import),
    ):
        assert "@mcp_tool(sensitivity=" not in text, path
    assert "ReportRegistry" not in mcp_architecture
    assert "superseded decorator example" in adr_008
    assert "All tools use `get_database()`" not in mcp_rule
    assert "default `read_only=False`" not in mcp_rule
    assert "get_database(read_only=False)" in mcp_rule


def test_final_review_classification_and_consent_wording_match_runtime() -> None:
    architecture = ARCHITECTURE_SPEC.read_text()
    privacy = (ROOT / "docs/specs/privacy-data-classification.md").read_text()
    trust = " ".join((ROOT / "docs/specs/privacy-and-ai-trust.md").read_text().split())
    observability = (ROOT / "docs/specs/observability.md").read_text()
    pdf = " ".join((ROOT / "docs/specs/smart-import-pdf.md").read_text().split())

    for tier in ("`low`", "`medium`", "`high`", "`critical`"):
        assert tier in architecture
    assert "`transactions` is statically high" in trust
    assert "dynamic; maximum critical" in privacy
    assert (
        "global consent enforcement and automatic degraded responses remain deferred"
        in trust
    )
    assert "Consent not granted, returning degraded response" not in observability
    assert "dynamic file-derived classification, maximum critical sensitivity" in pdf


def test_final_review_egress_timeout_and_connection_wording_match_runtime() -> None:
    egress_docs = (
        ROOT / "docs/guides/mcp-clients.md",
        ROOT / "docs/guides/setting-up-claude-desktop.md",
        ROOT / "docs/guides/threat-model.md",
        ROOT / "docs/reference/system-overview.md",
    )
    for path in egress_docs:
        text = path.read_text()
        assert "sync_*" in text, path
        assert "gsheet_*" in text, path

    threat = (ROOT / "docs/guides/threat-model.md").read_text()
    timeouts = (ROOT / "docs/specs/mcp-tool-timeouts.md").read_text()
    system = (ROOT / "docs/reference/system-overview.md").read_text()
    assert "No egress redaction" not in threat
    assert "No session-wide DuckDB connection is held" in system
    for tool in (
        "gsheet_connect",
        "gsheet_disconnect",
        "sync_disconnect",
        "identity_links_decide",
        "import_confirm",
        "import_revert",
    ):
        assert tool in timeouts
    assert "The cap is global" not in timeouts
    assert "No per-tool overrides" not in timeouts


def test_final_review_table_refs_and_account_contract_match_runtime() -> None:
    table_docs = (
        ROOT / "docs/architecture.md",
        ROOT / "docs/guides/data-pipeline.md",
        ROOT / "docs/specs/architecture-shared-primitives.md",
        ROOT / "docs/specs/reports-net-worth.md",
        REPORT_RECIPE_SPEC,
        ROOT / ".claude/rules/security.md",
    )
    for path in table_docs:
        assert re.search(r"\bTableRef\.[A-Z]", path.read_text()) is None, path

    account = (ROOT / "docs/specs/account-management.md").read_text()
    reports = REPORT_RECIPE_SPEC.read_text()
    assert 'accounts(view="resolve", query=...)' in account
    assert "include_closed is a read filter only" in account
    assert "AccountSettingsService" not in account
    assert "TableRef itself carries only schema, name, and audience" in reports.replace(
        "`", ""
    )


def test_final_review_refresh_surface_semantics_match_runtime() -> None:
    recovery = (ROOT / "docs/specs/data-recovery-contract.md").read_text()
    pipeline = " ".join((ROOT / "docs/guides/data-pipeline.md").read_text().split())
    demo = " ".join((ROOT / "docs/specs/demo-preset.md").read_text().split())
    gsheet = " ".join((ROOT / "docs/specs/connect-gsheet.md").read_text().split())

    matching_workflow = recovery.partition("6. The matches MCP workflow")[2].partition(
        "7. `RefreshResult`"
    )[0]
    pr6 = recovery.partition("### PR 6")[2].partition("### PR 7")[0]
    assert '`refresh_run(steps=["match"])`' in matching_workflow
    assert '`refresh_run(steps=["match"])`' in pr6
    assert "`dedup_reconciliation` emits `refresh_run()`" in recovery

    assert (
        "MCP default: `gsheet → match → transform → categorize → identity`" in pipeline
    )
    assert (
        "CLI selectable steps: `match → transform → categorize → identity`" in pipeline
    )
    for field in (
        "`data.error`",
        "`matching_error`",
        "`categorization_error`",
        "`identity_errors`",
        "`recovery_actions`",
    ):
        assert field in pipeline
    assert "top-level `error`" not in pipeline
    assert "log-only" not in pipeline

    assert 'refresh(db, steps=["transform"])' in demo
    assert 'refresh(db, steps=["match", "categorize"])' in demo
    assert "The `gsheet` and `identity` steps are intentionally omitted" in demo
    assert "`transform` → `categorize` → `identity`" in demo

    assert "CLI pull refreshes downstream by default" in gsheet
    assert "MCP `gsheet_pull` is pull-only" in gsheet
    assert "per-connection PullResults" not in gsheet
    assert 'query per-connection health with `gsheet(view="status")`' in gsheet
