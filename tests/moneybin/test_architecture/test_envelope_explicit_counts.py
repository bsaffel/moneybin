"""Structural guardrail: `build_envelope` calls must state both counts together.

MB-175: `build_envelope`'s derived-count path used to "promote" a `0` returned
count up to `total_count` whenever a caller passed `total_count` but not
`returned_count`. That is exactly wrong for an empty page of a paginated read —
an empty page reported `returned_count == total_count` and `has_more=False`
while carrying zero rows. The fix was "both counts everywhere": every call site
passing a non-``None`` `total_count` must also pass an explicit
`returned_count`, so the promotion branch is unreachable and could be deleted
(see `src/moneybin/protocol/envelope.py::build_envelope`).

This guard keeps that true going forward. `returned_count` is a proper keyword
whose absence is meaningful, so an AST scan for the keyword's presence is the
correct check — not a `grep` for the substring, which cannot tell a keyword
argument from a comment or a docstring mentioning the same words.

Scope is `src/moneybin/` — every production call site. Tests may call
`build_envelope` with only `total_count` to exercise the (still-real, just no
longer promoted) implicit-count path deliberately; they are not held to this
convention.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.moneybin.test_architecture._import_graph import SRC

REPO_ROOT = SRC.parents[1]


def _violations_in_source(source: str, path_label: str) -> list[str]:
    """Return `"<path_label>:<line>"` for each offending `build_envelope` call.

    A call offends when it passes `total_count` as a keyword without also
    passing `returned_count` as a keyword. `build_envelope` is keyword-only
    after `data`, so both are always `ast.keyword` nodes, never positional —
    the scan does not need to reason about positional argument order.
    """
    tree = ast.parse(source, filename=path_label)
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else None
        if name is None and isinstance(func, ast.Attribute):
            name = func.attr
        if name != "build_envelope":
            continue
        kwarg_names = {kw.arg for kw in node.keywords if kw.arg is not None}
        if "total_count" in kwarg_names and "returned_count" not in kwarg_names:
            violations.append(f"{path_label}:{node.lineno}")
    return violations


def _scan_source_tree(root: Path) -> list[str]:
    """Walk every `.py` file under `root` and collect violation sites."""
    found: list[str] = []
    for path in sorted(root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        found.extend(_violations_in_source(source, str(path.relative_to(REPO_ROOT))))
    return found


def test_no_build_envelope_call_passes_total_count_without_returned_count() -> None:
    """Every `total_count=` call site in production code must also pass `returned_count=`.

    Passing only `total_count` reaches `build_envelope`'s implicit-count path,
    which infers `returned_count` from the payload shape. For a paginated read
    whose page comes back empty, that inference silently disagrees with the
    caller's own truncation state — the exact defect MB-175 fixed. Naming the
    real count explicitly at the call site is the only way to keep that
    disagreement structurally impossible.
    """
    violations = _scan_source_tree(SRC)
    assert not violations, (
        "These call sites pass total_count without returned_count. Pass the "
        "number of rows the payload actually carries as returned_count=... "
        "(see MB-175 / src/moneybin/protocol/envelope.py):\n  "
        + "\n  ".join(violations)
    )


def test_violations_in_source_flags_total_count_only_call(tmp_path: Path) -> None:
    """Mutation probe: a call missing `returned_count` must be caught and named.

    Adversarial fixture, not redundant coverage — proves the guard's detection
    logic actually fires (and names the exact line) rather than merely existing
    with no code path to reach it.
    """
    offending = tmp_path / "evasion.py"
    offending.write_text(
        "from moneybin.protocol.envelope import build_envelope\n"
        "\n"
        "def f():\n"
        "    return build_envelope(data=[], total_count=5)\n",
        encoding="utf-8",
    )

    violations = _violations_in_source(
        offending.read_text(encoding="utf-8"), "evasion.py"
    )

    assert violations == ["evasion.py:4"]


@pytest.mark.parametrize(
    "call",
    [
        "build_envelope(data=[], total_count=5, returned_count=5)",
        "build_envelope(data=[])",
        "build_envelope(data=[], returned_count=5)",
    ],
)
def test_violations_in_source_ignores_compliant_calls(call: str) -> None:
    """A call carrying both counts, or no total_count at all, is not a violation."""
    source = f"def f():\n    return {call}\n"
    violations = _violations_in_source(source, "sample.py")
    assert violations == []


def test_violations_in_source_flags_attribute_access_by_name_alone() -> None:
    """Scan-behavior note: the guard matches on bare/attribute name alone.

    `some_module.build_envelope(...)` is flagged even though it is not
    necessarily *the* `build_envelope` — the same name-matching approach used
    to derive MB-175's original site inventory. No second function named
    `build_envelope` exists in the tree today, so this is a documented
    scan-behavior tradeoff, not a false-positive risk.
    """
    call = "some_module.build_envelope(data=[], total_count=5)"
    source = f"def f():\n    return {call}\n"
    violations = _violations_in_source(source, "sample.py")
    assert violations == ["sample.py:2"]
