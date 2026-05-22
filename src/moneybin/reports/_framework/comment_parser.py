"""Parser for @-block structured comments in report SQL files.

Grammar (informal):

  /*
  @name <snake_case_identifier>
  @description <free text until next @ or */>
  @param <name> <SQL_TYPE> [optional] [default=<literal>] "<doc string>"
  @example <free text until next @ or */>
  */

The parser only inspects the LEADING block-comment (the first /* */
group in the file). Comments after that — inline column comments, etc. —
are ignored. The parser is deliberately strict: missing @name raises;
malformed @param raises; the framework refuses to register a report
whose comments don't parse cleanly.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from moneybin.reports._framework.spec import ParamSpec, ReportSpec

_BLOCK_COMMENT_RE = re.compile(r"/\*(.*?)\*/", re.DOTALL)
_NAME_RE = re.compile(r"@name\s+([a-z_][a-z0-9_]*)\s*$", re.MULTILINE)
_DESCRIPTION_RE = re.compile(
    r"@description\s+(.+?)(?=^\s*@|\Z)", re.MULTILINE | re.DOTALL
)
_PARAM_RE = re.compile(
    r"""
    @param\s+
    (?P<name>[a-z_][a-z0-9_]*)\s+
    (?P<type>[A-Z]+(?:\[\])?)\s+
    (?P<optional>optional\s+)?
    (?:default\s*=\s*(?P<default>\S+)\s+)?
    "(?P<doc>[^"]*)"
    """,
    re.VERBOSE,
)
_EXAMPLE_RE = re.compile(r"@example\s+(.+?)(?=^\s*@|\Z)", re.MULTILINE | re.DOTALL)


def parse_report_sql(path: Path) -> ReportSpec:
    """Parse the leading /* */ block of a report SQL file.

    Raises:
        FileNotFoundError: if path does not exist
        ValueError: if @name is missing or any @param is malformed
    """
    text = path.read_text(encoding="utf-8")
    block_match = _BLOCK_COMMENT_RE.search(text)
    if block_match is None:
        raise ValueError(f"{path}: missing leading /* */ block comment")
    block = block_match.group(1)

    name_match = _NAME_RE.search(block)
    if name_match is None:
        raise ValueError(f"{path}: missing @name in block comment")
    name = name_match.group(1)

    description_match = _DESCRIPTION_RE.search(block)
    description = description_match.group(1).strip() if description_match else ""

    params: list[ParamSpec] = []
    for m in _PARAM_RE.finditer(block):
        default_raw = m.group("default")
        default_value: Any = _parse_default(default_raw) if default_raw else None
        params.append(
            ParamSpec(
                name=m.group("name"),
                type_hint=m.group("type"),
                optional=bool(m.group("optional")),
                default=default_value,
                doc=m.group("doc"),
            )
        )

    examples = [match.group(1).strip() for match in _EXAMPLE_RE.finditer(block)]

    return ReportSpec(
        name=name,
        description=description,
        params=params,
        examples=examples,
        source_path=path,
    )


def _parse_default(raw: str) -> Any:
    """Convert a default literal from @param text to a Python value.

    Accepts: null, true, false, integers, decimals, double-quoted strings,
    bracketed JSON-style lists (["a","b"]).
    """
    cleaned = raw.rstrip(",")
    if cleaned in ("null", "None"):
        return None
    if cleaned == "true":
        return True
    if cleaned == "false":
        return False
    try:
        if "." in cleaned:
            return float(cleaned)
        return int(cleaned)
    except ValueError:
        pass
    if cleaned.startswith("["):
        return json.loads(cleaned)
    if cleaned.startswith('"') and cleaned.endswith('"'):
        return cleaned[1:-1]
    return cleaned  # fallback: raw string token
