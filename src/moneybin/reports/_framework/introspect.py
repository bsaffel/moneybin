"""Turn a report runner into a :class:`ReportSpec` by introspection.

Reads the runner's signature (parameters, resolved types, defaults) and its
Google-style docstring (summary, ``Args:`` help, ``Examples:``). The first
parameter must be ``db``; the rest must be keyword-only — that shape is what
lets the framework map params 1:1 onto the MCP tool and CLI command.
"""

from __future__ import annotations

import inspect
import re
from typing import cast

from moneybin.reports._framework.contract import (
    ParamSpec,
    ReportSpec,
    Runner,
    TableRef,
)

# A docstring section header like "Args:" / "Examples:" (no further text).
_HEADER = re.compile(r"^(\w+):$")
# An "name: help" or "name (type): help" Args entry.
_ARG_ENTRY = re.compile(r"^(\w+)\s*(?:\([^)]*\))?\s*:\s*(.*)$")


def build_spec(
    fn: Runner, *, name: str, view: TableRef, domain: str | None = None
) -> ReportSpec:
    """Introspect ``fn`` into a :class:`ReportSpec`.

    Raises:
        ValueError: if the runner has no docstring, its first parameter is not
            ``db``, or any non-``db`` parameter is not keyword-only.
    """
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
        name=name,
        description=summary,
        view=view,
        runner=fn,
        params=tuple(param_specs),
        examples=examples,
        domain=domain,
    )


def _parse_docstring(doc: str) -> tuple[str, dict[str, str], tuple[str, ...]]:
    """Split a Google-style docstring into (summary, arg_help, examples)."""
    lines = doc.splitlines()

    summary_parts: list[str] = []
    idx = 0
    while idx < len(lines) and lines[idx].strip():
        summary_parts.append(lines[idx].strip())
        idx += 1
    summary = " ".join(summary_parts).strip()

    arg_help: dict[str, str] = {}
    examples: list[str] = []
    section: str | None = None
    current: str | None = None
    for line in lines[idx:]:
        stripped = line.strip()
        header = _HEADER.match(stripped)
        if header:
            tag = header.group(1).lower()
            section = (
                "args"
                if tag in ("args", "arguments")
                else ("examples" if tag == "examples" else None)
            )
            current = None
            continue
        if not stripped:
            current = None
            continue
        if section == "args":
            entry = _ARG_ENTRY.match(stripped)
            if entry:
                current = cast("str", entry.group(1))  # \w+ always yields a str
                arg_help[current] = entry.group(2).strip()
            elif current is not None:
                arg_help[current] = f"{arg_help[current]} {stripped}".strip()
        elif section == "examples":
            examples.append(stripped)

    return summary, arg_help, tuple(examples)
