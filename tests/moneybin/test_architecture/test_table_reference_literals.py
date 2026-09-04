r"""Structural guardrail: no hardcoded schema-qualified table literal reaches SQL.

AGENTS.md's Key Abstractions table requires every table reference to come from
``moneybin.tables`` (``from moneybin.tables import FCT_TRANSACTIONS``, etc.) —
never a hardcoded ``"core.fct_transactions"``-shaped string. #519 swept 95
pre-existing literals to ``TableRef`` constants; nothing stopped the next PR
from reintroducing one. This test closes that gap.

Why not a plain regex over the source text? A regex for
``\\b(schema)\\.[a-z][a-z0-9_]+`` matches ~1900 times across ``src/**/*.py`` —
almost all of it Python attribute access (``app.config``, ``core.foo``) and
prose, not SQL. That noise floor makes a bare regex either deafening (flag
everything) or vacuous (exempt everything). Two design choices bring the
signal back:

1. **AST first, not text first.** Only ``ast.Constant``/``ast.JoinedStr``
   string literals that *reach* a ``.execute()`` / ``.executemany()`` call are
   candidates — not every string in the tree. "Reaches" means: passed
   directly as an argument, OR assigned to a local name that is later passed
   as an argument (including transitively, through an f-string interpolation
   inside another candidate literal — see ``_NEWEST_HOLDINGS_SNAPSHOT_CTE`` in
   ``doctor_service.py`` for the shape this exists to catch: a module-level
   query fragment assigned once, then spliced into other queries via
   ``f"...{_CTE}..."``). This is a bounded, file-local heuristic, not a full
   dataflow analysis — deciding "does this string reach a DB call" is
   undecidable in general (a query built via ``.join()``, string
   concatenation across branches, or a helper function's return value can
   evade it). The tradeoff is deliberate: false negatives here are a coverage
   gap to close by hand if discovered; false positives are a source of guard
   fatigue that erodes trust in the whole check.

2. **A SQL-keyword gate on the schema.table match itself.** Even restricted to
   candidate literals, a bare schema-prefix regex still false-positives on a
   query that aliases a source table AS its own schema name — e.g.
   ``FROM raw.tabular_accounts AS raw`` makes every later ``raw.account_id``
   a column reference through the alias, not a table name (see
   ``account_resolver.py``), and a ``-- core.fct_transactions is expensive``
   SQL comment reads identically to a real reference. Requiring the match to
   be immediately preceded by a clause keyword that only precedes a genuine
   table/view target (``FROM``, ``JOIN``, ``INTO``, ``UPDATE``, ``TABLE``,
   ``VIEW``, ``EXISTS``, ``REFERENCES``, ``TRUNCATE``) eliminates both false-
   positive classes: an alias reference is preceded by an operator, a
   function call, or a comma, never by one of these keywords, and a
   sentence fragment inside a SQL comment essentially never is either.

Known residual false-positive shape (none currently in the tree, so no
allowlist entry exists for it): prose that literally reads "the table
core.foo" or "the view app.bar" inside a SQL comment. If that ever fires,
add an occurrence to ``TABLE_LITERAL_ALLOWLIST`` with a ``# why`` explaining
it is prose, not a reference.

Exemptions:

- ``src/moneybin/sql/migrations/`` is skipped wholesale (a directory-level
  structural rule, not a maintained list): each migration is frozen
  point-in-time DDL that must reflect the schema exactly as it was when it
  ran, so binding to ``moneybin.tables`` — a live, current-schema registry —
  would be wrong, not merely undesirable. ``src/moneybin/migrations.py`` (the
  runner) is a different file, lives one directory up, and is NOT exempt —
  ``test_migrations_runner_is_not_exempt`` pins that.
- Everything else is an individual ``TABLE_LITERAL_ALLOWLIST`` entry with a
  ``# why`` comment. Prose, docstrings, comments, and the
  ``schema_catalog.py`` ``EXAMPLES``/hint-text strings from the #519 sweep
  needed no entries here: none of them are arguments to an ``execute()``
  call, so the AST-first design excludes them by construction.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src" / "moneybin"
MIGRATIONS_DIR = SRC_ROOT / "sql" / "migrations"

EXECUTE_METHOD_NAMES = frozenset({"execute", "executemany"})

_SCHEMA_NAMES = (
    "raw",
    "prep",
    "core",
    "app",
    "reports",
    "meta",
    "seeds",
    "synthetic",
)
_TABLE_KEYWORDS = (
    "FROM",
    "JOIN",
    "INTO",
    "UPDATE",
    "TABLE",
    "VIEW",
    "EXISTS",
    "REFERENCES",
    "TRUNCATE",
)
# A schema-qualified name only counts as a table *reference* when it directly
# follows a clause keyword that introduces one — see module docstring point 2.
SCHEMA_TABLE_PATTERN = re.compile(
    r"\b(?:" + "|".join(_TABLE_KEYWORDS) + r")\s+"
    r"(" + "|".join(_SCHEMA_NAMES) + r")\.([a-z][a-z0-9_]+)\b",
    re.IGNORECASE,
)

# Allowlist entries are (file_relpath, "schema.table") pairs. `file_relpath`
# is relative to src/moneybin/ for stability across moves. Every entry
# carries a `# why` comment; entries are asserted by set equality against
# the live scan (`test_allowlist_has_no_dead_entries`), so a stale entry
# fails as loudly as a new violation.
TABLE_LITERAL_ALLOWLIST: frozenset[tuple[str, str]] = frozenset({
    # `refresh_views` drops two pre-migration legacy view names during its
    # backward-compat upgrade path. `app.categories` and `app.merchants`
    # predate today's `core.dim_categories` / `core.dim_merchants` (CATEGORIES
    # / MERCHANTS) and were retired by the V006/V032 migrations — they have
    # no current TableRef *by design*: tables.py is a registry of live,
    # current names, and the whole point of these two statements is to strip
    # away a name that no longer exists in that registry. Giving them a
    # TableRef would misrepresent them as live tables.
    ("seeds.py", "app.categories"),
    ("seeds.py", "app.merchants"),
})


def _literal_text(node: ast.expr) -> str | None:
    """Static string content of a Constant or JoinedStr (f-string) node.

    For a JoinedStr, only the literal (non-interpolated) segments are
    returned — an embedded ``{TABLE.full_name}`` expression contributes
    nothing, which is correct: a query built that way has no hardcoded
    schema.table text in its static shape.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value
            for v in node.values
            if isinstance(v, ast.Constant) and isinstance(v.value, str)
        )
    return None


def _embedded_names(node: ast.expr) -> set[str]:
    """Names referenced inside a JoinedStr's f-string interpolations.

    Lets the scan follow one query fragment spliced into another via
    ``f"...{_SOME_CTE}..."`` — see module docstring point 1.
    """
    if not isinstance(node, ast.JoinedStr):
        return set()
    return {
        n.id
        for v in node.values
        if isinstance(v, ast.FormattedValue)
        for n in ast.walk(v.value)
        if isinstance(n, ast.Name)
    }


def _scan_file(path: Path) -> list[tuple[int, str]]:
    """Return (lineno, "schema.table") violations for one file."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []

    var_literals: dict[str, list[tuple[int, str, set[str]]]] = {}
    seed_names: set[str] = set()
    scanned: list[tuple[int, str]] = []  # (lineno, text) reaching execute()

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            text = _literal_text(node.value)
            if text is not None:
                embedded = _embedded_names(node.value)
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        var_literals.setdefault(target.id, []).append((
                            node.lineno,
                            text,
                            embedded,
                        ))
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else func.id
                if isinstance(func, ast.Name)
                else None
            )
            if name not in EXECUTE_METHOD_NAMES:
                continue
            for arg in (*node.args, *(kw.value for kw in node.keywords)):
                if isinstance(arg, ast.Name):
                    seed_names.add(arg.id)
                    continue
                text = _literal_text(arg)
                if text is not None:
                    scanned.append((arg.lineno, text))
                    seed_names |= _embedded_names(arg)

    # Fixed-point worklist: a name "reaches" execute() directly, or
    # transitively through an f-string interpolation inside another
    # reaching literal (module docstring point 1).
    worklist = list(seed_names)
    visited: set[str] = set()
    while worklist:
        name = worklist.pop()
        if name in visited:
            continue
        visited.add(name)
        for lineno, text, embedded in var_literals.get(name, []):
            scanned.append((lineno, text))
            worklist.extend(embedded - visited)

    violations: list[tuple[int, str]] = []
    for lineno, text in scanned:
        for match in SCHEMA_TABLE_PATTERN.finditer(text):
            violations.append((lineno, f"{match.group(1)}.{match.group(2)}"))
    return violations


def _is_exempt_migration(path: Path) -> bool:
    """True for any file under the frozen migrations directory."""
    return path.is_relative_to(MIGRATIONS_DIR)


def _scan_source_tree() -> list[tuple[str, int, str]]:
    """Walk src/moneybin/**/*.py and collect every (relpath, lineno, table)."""
    found: list[tuple[str, int, str]] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if _is_exempt_migration(path):
            continue
        relpath = path.relative_to(SRC_ROOT).as_posix()
        for lineno, table in _scan_file(path):
            found.append((relpath, lineno, table))
    return found


def test_no_hardcoded_table_literals_reach_execute() -> None:
    """Every schema-qualified literal reaching execute() must be allowlisted.

    A failure here means a new call site hardcoded a schema.table string
    instead of importing the TableRef constant from moneybin.tables. Fix by
    routing through the constant; add an allowlist entry with a `# why`
    comment only for a genuine non-executed or historically-frozen exception.
    """
    found = _scan_source_tree()
    violations = [
        (relpath, lineno, table)
        for relpath, lineno, table in found
        if (relpath, table) not in TABLE_LITERAL_ALLOWLIST
    ]
    if violations:
        formatted = "\n".join(
            f"  - {relpath}:{lineno}: {table}" for relpath, lineno, table in violations
        )
        pytest.fail(
            "Hardcoded schema-qualified table literal(s) reach a SQL execute "
            "call. Import the TableRef constant from moneybin.tables instead, "
            'or add (file, "schema.table") to TABLE_LITERAL_ALLOWLIST with a '
            f"`# why` comment.\n\nViolations:\n{formatted}"
        )


def test_allowlist_has_no_dead_entries() -> None:
    """Every allowlist entry must match a real occurrence in the tree.

    A stale entry silently widens the exception surface — if the swept
    literal it names is removed or fixed, the entry should go too.
    """
    found = {(relpath, table) for relpath, _lineno, table in _scan_source_tree()}
    stale = TABLE_LITERAL_ALLOWLIST - found
    if stale:
        formatted = "\n".join(f"  - {relpath}: {table}" for relpath, table in stale)
        pytest.fail(
            "TABLE_LITERAL_ALLOWLIST contains entries with no matching "
            f"occurrence in the tree — remove them.\n\nStale entries:\n{formatted}"
        )


def test_migrations_runner_is_not_exempt() -> None:
    """The migration *runner* is a different file from the frozen scripts.

    ``migrations.py`` (the runner) vs. ``sql/migrations/*.py`` (the frozen
    scripts) must stay in scope. Guards against the exemption predicate
    accidentally widening to match it — e.g. a substring check on
    "migrations" instead of a directory membership check.
    """
    runner = SRC_ROOT / "migrations.py"
    assert runner.is_file(), "expected src/moneybin/migrations.py to exist"
    assert not _is_exempt_migration(runner)
