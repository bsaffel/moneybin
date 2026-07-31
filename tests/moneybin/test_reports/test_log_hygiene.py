"""No user-authored text reaches a log record on the reports surface.

A saved report's name, its output aliases, and its declared parameter names are
all text the user typed. ``amazon_spend`` is as plausible a merchant name as a
column one, and ``SanitizedLogFormatter`` recognizes neither — it masks SSNs,
account numbers, and dollar amounts by pattern, not arbitrary labels. So
``.claude/rules/security.md``'s "never log merchant names or descriptions" has to
be held at the call site, and the response is where the caller learns which
column was affected.

This guard exists because the same defect was found and fixed six times across
three review rounds, in six different functions — the collision warning, the
reclassify log, the two save notes, the undeclared-column warning, and the
parameter-reclassification warning. Fixing each as it was reported never
generalized, because nothing enumerated the shape. This does.

Paired with behavioural tests rather than standing alone (a source scan cannot
prove a record's *contents*): ``test_classify``, ``test_dynamic``,
``test_reports_user``, and ``test_user_reports`` each assert that a specific
user-authored value is absent from what the relevant path emits.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_SURFACE = (
    "src/moneybin/reports",
    "src/moneybin/cli/commands/reports",
    "src/moneybin/services/user_reports_service.py",
    "src/moneybin/repositories/user_reports_repo.py",
)

#: Identifier tokens that name user-authored text rather than a count or an id.
#: ``report_id`` and ``report_ids`` are deliberately absent — an id is minted.
_USER_AUTHORED = re.compile(
    r"\b(name|names|column|columns|col|cols|alias|aliases|reason|reasons"
    r"|description|descriptions|merchant|merchants|label|labels)\b"
)

#: Expression wrappers that reduce user-authored text to a safe scalar.
_SAFE_WRAPPERS = ("len(", "type(", "sql_digest(", "sorted(set(")

#: Interpolations that legitimately survive, as ``(module, expression)``. Checked
#: by set equality, so a stale entry fails as loudly as a new leak — an exemption
#: for a call that was since fixed would otherwise pre-authorize the next one.
_ALLOWED: frozenset[tuple[str, str]] = frozenset()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _modules() -> list[Path]:
    root = _repo_root()
    found: list[Path] = []
    for entry in _SURFACE:
        target = root / entry
        found.extend(sorted(target.rglob("*.py")) if target.is_dir() else [target])
    assert found, "the reports surface resolved to no modules; check _SURFACE"
    return found


def _is_logger_call(node: ast.Call) -> bool:
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id == "logger"
    )


def _interpolations(node: ast.Call, source: str) -> list[str]:
    """Source text of every ``{...}`` inside this call's f-string arguments."""
    found: list[str] = []
    for argument in node.args:
        for part in ast.walk(argument):
            if isinstance(part, ast.FormattedValue):
                found.append(ast.get_source_segment(source, part.value) or "")
    return found


def _leaks(path: Path, source: str) -> set[tuple[str, str]]:
    tree = ast.parse(source)
    found: set[tuple[str, str]] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _is_logger_call(node)):
            continue
        for expression in _interpolations(node, source):
            stripped = expression.strip()
            if stripped.startswith(_SAFE_WRAPPERS) or "__" in stripped:
                continue
            if _USER_AUTHORED.search(stripped):
                found.add((path.name, stripped))
    return found


def test_no_log_record_on_the_reports_surface_interpolates_user_authored_text() -> None:
    """Set equality: a new leak fails, and so does an exemption for a fixed one."""
    found: set[tuple[str, str]] = set()
    for path in _modules():
        found |= _leaks(path, path.read_text(encoding="utf-8"))

    assert found == _ALLOWED, (
        "a log record on the reports surface interpolates user-authored text; log "
        "the report id with a count or a digest instead, and echo the detail to the "
        "terminal:\n"
        + "\n".join(
            f"  {module}: {expression}"
            for module, expression in sorted(found ^ _ALLOWED)
        )
    )


def test_the_scan_reaches_the_modules_it_claims_to_cover() -> None:
    """The guard's own coverage, since an empty scan also reports zero leaks.

    A path typo in ``_SURFACE``, or an ``ast`` walk that stopped matching
    ``logger.<level>`` calls, would leave the guard above green forever. Assert it
    still finds the calls that are known to exist and are known to be safe.
    """
    modules = {path.name for path in _modules()}
    assert {"classify.py", "dynamic.py", "user_reports.py", "catalog.py"} <= modules

    root = _repo_root()
    classify = root / "src/moneybin/reports/_framework/classify.py"
    tree = ast.parse(classify.read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _is_logger_call(node)
    ]
    assert calls, "classify.py has a logger call; the matcher stopped finding it"
