"""Turn a report runner into a :class:`ReportSpec` by introspection.

Reads the runner's signature (parameters, resolved types, defaults) and its
Google-style docstring (summary, ``Args:`` help, ``Examples:``). The first
parameter must be ``db``; the rest must be keyword-only — that shape is what
lets the framework map params 1:1 onto the MCP tool and CLI command.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Mapping
from typing import cast

from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportSemantics,
    ReportSpec,
    Runner,
    TableRef,
)

# A docstring section header like "Args:" / "Examples:" (no further text).
_HEADER = re.compile(r"^(\w+):$")
# Known Google-docstring section names. Header detection is gated on this set so
# a bare "<word>:" prose line that is NOT a real section (e.g. "Options:") is
# treated as prose, not a boundary — otherwise it would truncate the description
# and drop the prose (and potentially the Args block) after it.
_KNOWN_SECTIONS = frozenset({
    "args",
    "arguments",
    "examples",
    "returns",
    "raises",
    "yields",
    "notes",
    "attributes",
})
# An "name: help" or "name (type): help" Args entry.
_ARG_ENTRY = re.compile(r"^(\w+)\s*(?:\([^)]*\))?\s*:\s*(.*)$")

# Names the CLI registrar injects as shared options (see cli_register._cli_signature).
# A runner param colliding with one of these would raise a cryptic duplicate-
# parameter error deep in Signature construction, crashing the whole reports
# command group at build; reject it here with a clear message instead.
_RESERVED_CLI_PARAMS = frozenset({"output", "quiet"})


def _section_tag(stripped: str) -> str | None:
    """Lowercased section name if ``stripped`` is a known Google header, else None."""
    match = _HEADER.match(stripped)
    if match is None:
        return None
    tag = match.group(1).lower()
    return tag if tag in _KNOWN_SECTIONS else None


def build_spec(
    fn: Runner,
    *,
    report_id: str,
    name: str,
    view: TableRef,
    classes: Mapping[str, DataClass],
    columns: tuple[OutputColumn, ...],
    semantics: ReportSemantics,
    domain: str | None = None,
) -> ReportSpec:
    """Introspect ``fn`` into a :class:`ReportSpec`.

    Raises:
        ValueError: if the runner has no docstring, its first parameter is not
            ``db``, any non-``db`` parameter is not keyword-only or collides with
            a reserved CLI option (``output``/``quiet``), or ``classes`` is empty
            (every report must declare its column privacy contract).
    """
    if view.schema != "reports":
        raise ValueError(
            f"Report {name!r} view must be a reports.* table, got {view.full_name!r} "
            "(reports_* surfaces read from the reports schema)."
        )
    if not classes:
        raise ValueError(
            f"Report {name!r} must declare a non-empty `classes` map "
            "(the output-column privacy contract)."
        )

    doc = inspect.getdoc(fn)
    if not doc:
        raise ValueError(f"Report runner {fn.__name__!r} needs a docstring.")

    # eval_str resolves the string annotations produced by `from __future__
    # import annotations` back to real types against the runner's globals.
    sig = inspect.signature(fn, eval_str=True)
    params = list(sig.parameters.values())
    if not params or params[0].name != "db":
        raise ValueError(
            f"Report runner {fn.__name__!r} must take 'db' as its first parameter."
        )

    summary, arg_help, examples = _parse_docstring(doc)

    param_specs: list[ParamSpec] = []
    for p in params[1:]:
        if p.kind is not inspect.Parameter.KEYWORD_ONLY:
            raise ValueError(
                f"Report runner {fn.__name__!r} parameter {p.name!r} must be "
                "keyword-only (declare runner params after a bare '*')."
            )
        if p.name in _RESERVED_CLI_PARAMS:
            raise ValueError(
                f"Report runner {fn.__name__!r} parameter {p.name!r} collides "
                "with a shared CLI option; rename it (reserved: "
                f"{', '.join(sorted(_RESERVED_CLI_PARAMS))})."
            )
        required = p.default is inspect.Parameter.empty
        param_specs.append(
            ParamSpec(
                name=p.name,
                annotation=None
                if p.annotation is inspect.Parameter.empty
                else p.annotation,
                default=None if required else p.default,
                required=required,
                help=arg_help.get(p.name, ""),
            )
        )

    return ReportSpec(
        report_id=report_id,
        name=name,
        description=summary,
        view=view,
        runner=fn,
        classes=dict(classes),
        columns=columns,
        semantics=semantics,
        params=tuple(param_specs),
        examples=examples,
        domain=domain,
    )


def _parse_docstring(doc: str) -> tuple[str, dict[str, str], tuple[str, ...]]:
    """Split a Google-style docstring into (summary, arg_help, examples)."""
    lines = doc.splitlines()

    # Description = all prose before the first section header (Args:/Examples:).
    # Keeping the body paragraphs (sign convention, currency) is required for
    # amount-bearing reports per mcp.md — they become the agent-visible tool
    # description. The Args block is excluded so the non-passable `db` param
    # never reaches the agent. Wrapped lines collapse within a paragraph; blank
    # lines separate paragraphs.
    desc_lines: list[str] = []
    idx = 0
    while idx < len(lines) and _section_tag(lines[idx].strip()) is None:
        desc_lines.append(lines[idx])
        idx += 1
    paragraphs = [
        " ".join(line.strip() for line in para.splitlines() if line.strip())
        for para in "\n".join(desc_lines).split("\n\n")
    ]
    summary = "\n\n".join(p for p in paragraphs if p).strip()

    arg_help: dict[str, str] = {}
    examples: list[str] = []
    section: str | None = None
    current: str | None = None
    # Indent of the first Args entry. A line indented deeper than this is a
    # continuation of the current entry, even when it is shaped like "word:
    # text" (e.g. "default: today") — the indent, not the colon, marks an entry.
    arg_indent: int | None = None
    for line in lines[idx:]:
        stripped = line.strip()
        tag = _section_tag(stripped)
        if tag is not None:
            section = (
                "args"
                if tag in ("args", "arguments")
                else ("examples" if tag == "examples" else None)
            )
            current = None
            arg_indent = None  # each section's Args entries set their own baseline
            continue
        if not stripped:
            current = None
            continue
        if section == "args":
            indent = len(line) - len(line.lstrip())
            is_continuation = current is not None and (
                arg_indent is not None and indent > arg_indent
            )
            entry = None if is_continuation else _ARG_ENTRY.match(stripped)
            if entry:
                current = cast("str", entry.group(1))  # \w+ always yields a str
                arg_help[current] = entry.group(2).strip()
                if arg_indent is None:
                    arg_indent = indent
            elif current is not None:
                arg_help[current] = f"{arg_help[current]} {stripped}".strip()
        elif section == "examples":
            examples.append(stripped)

    return summary, arg_help, tuple(examples)
