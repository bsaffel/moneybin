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
   directly as an argument, OR bound to a local name — via ``=``, an
   annotated ``x: str = ...``, an augmented ``x += ...``, or a walrus
   ``(x := ...)`` (``ast.Assign``/``ast.AnnAssign``/``ast.AugAssign``/
   ``ast.NamedExpr``) — that is later passed as an argument (including
   transitively, through an f-string interpolation inside another candidate
   literal — see ``_NEWEST_HOLDINGS_SNAPSHOT_CTE`` in ``doctor_service.py``
   for the shape this exists to catch: a module-level query fragment
   assigned once, then spliced into other queries via ``f"...{_CTE}..."``).
   The full enumeration of name-binding AST nodes considered, and why the
   other binding forms (loop targets, ``with``/``except`` targets, function
   parameters, tuple/list-destructured assignment) are excluded, lives as a
   comment above the binding-collection loop in ``_scan_file``. This is a
   bounded, file-local heuristic, not a full dataflow analysis — deciding
   "does this string reach a DB call" is undecidable in general (a query
   built via ``.join()``, string concatenation across branches, or a helper
   function's return value can evade it). The tradeoff is deliberate: false
   negatives here are a coverage gap to close by hand if discovered; false
   positives are a source of guard fatigue that erodes trust in the whole
   check.

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
# The keyword itself is captured (group 1) so the allowlist can key on it —
# see TABLE_LITERAL_ALLOWLIST below.
SCHEMA_TABLE_PATTERN = re.compile(
    r"\b(" + "|".join(_TABLE_KEYWORDS) + r")\s+"
    r"(" + "|".join(_SCHEMA_NAMES) + r")\.([a-z][a-z0-9_]+)\b",
    re.IGNORECASE,
)

# Allowlist entries are (file_relpath, clause_keyword, "schema.table",
# occurrence) 4-tuples. `file_relpath` is relative to src/moneybin/ for
# stability across moves. `clause_keyword` is the matched SQL clause word
# (FROM/JOIN/VIEW/EXISTS/...) — keying on it, not just the table name,
# separates a `DROP VIEW ... app.merchants` from an unrelated
# `FROM app.merchants` naming the same table for a different reason.
# `occurrence` is a 0-based count of prior matches of that same
# (clause_keyword, table) pair *within the file*, assigned in source order
# (`_scan_file` sorts by lineno before counting) — this, not a line number,
# disambiguates two genuinely distinct occurrences that share both the same
# clause keyword and the same table (e.g. two separate `FROM app.merchants`
# reads in the same file). It is deliberately NOT the line number: a line
# number shifts on any unrelated edit above it, turning every such edit into
# a guard failure. The occurrence count only changes when an occurrence of
# that exact (clause, table) pair is itself added, removed, or reordered
# relative to its siblings — reordering is safe too, since same-pair
# occurrences are interchangeable by construction (each gets a distinct
# index regardless of which physical occurrence holds it).
#
# Collision behavior: two occurrences collide (compute to the same key) only
# if they share file, clause keyword, table, AND relative order — which,
# given the sort-by-lineno step, requires two distinct occurrences to start
# on the exact same source line. No such case exists in the tree today; if
# one arises, this scheme cannot order them and the fix is to additionally
# key on the enclosing statement's text or function name.
#
# Every entry carries a `# why` comment; entries are asserted by set
# equality against the live scan (`test_allowlist_has_no_dead_entries`), so
# a stale entry fails as loudly as a new violation.
TABLE_LITERAL_ALLOWLIST: frozenset[tuple[str, str, str, int]] = frozenset({
    # `refresh_views` drops two pre-migration legacy view names during its
    # backward-compat upgrade path. `app.categories` and `app.merchants`
    # predate today's `core.dim_categories` / `core.dim_merchants` (CATEGORIES
    # / MERCHANTS) and were retired by the V006/V032 migrations — they have
    # no current TableRef *by design*: tables.py is a registry of live,
    # current names, and the whole point of these two statements is to strip
    # away a name that no longer exists in that registry. Giving them a
    # TableRef would misrepresent them as live tables.
    ("seeds.py", "EXISTS", "app.categories", 0),
    ("seeds.py", "EXISTS", "app.merchants", 0),
    # NOT a retired-view drop — `app.merchants` here is read live, inside the
    # pre-V006 backward-compat passthrough that wraps the legacy TABLE (still
    # a BASE TABLE, not yet migrated to `app.user_merchants`) so
    # categorization reads keep working before V006 runs. It has no
    # TableRef because tables.py registers only the *current* schema shape;
    # this statement exists specifically to read the pre-migration one.
    ("seeds.py", "FROM", "app.merchants", 0),
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


def _record_literal_binding(
    var_literals: dict[str, list[tuple[int, str, set[str]]]],
    name: str,
    value: ast.expr,
    lineno: int,
) -> None:
    """Record `name`'s literal text (if any) as a candidate reaching execute()."""
    text = _literal_text(value)
    if text is not None:
        var_literals.setdefault(name, []).append((lineno, text, _embedded_names(value)))


def _scan_file(path: Path) -> list[tuple[int, str, str]]:
    """Return (lineno, clause_keyword, "schema.table") violations for one file.

    Returned in source order (sorted by lineno) so a caller can assign
    stable per-(clause, table) occurrence indices — see
    TABLE_LITERAL_ALLOWLIST's key-shape comment.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []

    var_literals: dict[str, list[tuple[int, str, set[str]]]] = {}
    seed_names: set[str] = set()
    scanned: list[tuple[int, str]] = []  # (lineno, text) reaching execute()

    # Enumeration of AST nodes that bind a name to a value, and whether each
    # can carry a hardcoded string literal into an execute() call:
    #
    # HANDLED — the value is written inline at the binding site, so a
    # literal there is a real candidate:
    #   - ast.Assign        x = "..."
    #   - ast.AnnAssign     x: str = "..."   (skipped when .value is None,
    #                                          i.e. a bare annotation)
    #   - ast.AugAssign     x += "..."       (records the added text; does
    #                                          not track prior concatenation
    #                                          — same file-local-heuristic
    #                                          limit as module docstring
    #                                          point 1)
    #   - ast.NamedExpr     (x := "...")     (walrus; target is always a
    #                                          plain Name per grammar)
    #
    # CONSCIOUSLY EXCLUDED — no realistic or in-scope path to a hardcoded
    # literal:
    #   - ast.For / ast.AsyncFor / comprehension `for` clauses: the target
    #     is bound to elements of an *iterable*, not a literal written at
    #     the binding site. Extracting a string from a List/Tuple-of-
    #     Constants iterable is a materially different shape than "value is
    #     a Constant/JoinedStr" and falls under the same disclaimed gap as
    #     ".join()"/concatenation in module docstring point 1.
    #   - ast.With / ast.AsyncWith `as` targets: bound to a context
    #     manager's __enter__() return value — never a literal present in
    #     source at the binding site.
    #   - ast.ExceptHandler `as` targets: bound to a raised exception
    #     instance, never a literal.
    #   - Function/lambda parameters (incl. defaults): the runtime value
    #     comes from the call site, not the default text — tracking it
    #     requires interprocedural analysis, explicitly out of scope
    #     ("a helper function's return value can evade it", docstring
    #     point 1).
    #   - ast.Global / ast.Nonlocal: declarations with no value to record.
    #   - Tuple/list-destructured Assign (`a, b = "core.x", "core.y"`):
    #     still an ast.Assign node (no separate node type), but
    #     `_literal_text` only extracts Constant/JoinedStr — a destructured
    #     Tuple/List RHS is silently not unpacked. Noted as a residual gap
    #     on the existing Assign handler, not a new node type to enumerate.
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    _record_literal_binding(
                        var_literals, target.id, node.value, node.lineno
                    )
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.value is not None:
                _record_literal_binding(
                    var_literals, node.target.id, node.value, node.lineno
                )
        elif isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name):
                _record_literal_binding(
                    var_literals, node.target.id, node.value, node.lineno
                )
        elif isinstance(node, ast.NamedExpr):
            # node.target is always ast.Name per the walrus grammar — no
            # isinstance check needed (and pyright flags one as redundant).
            _record_literal_binding(
                var_literals, node.target.id, node.value, node.lineno
            )
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

    violations: list[tuple[int, str, str]] = []
    for lineno, text in scanned:
        for match in SCHEMA_TABLE_PATTERN.finditer(text):
            clause = match.group(1).upper()
            table = f"{match.group(2)}.{match.group(3)}"
            violations.append((lineno, clause, table))
    # Sort by lineno so occurrence indices (assigned by the caller) reflect
    # top-to-bottom source order rather than ast.walk()'s traversal order —
    # see TABLE_LITERAL_ALLOWLIST's key-shape comment. Stable sort preserves
    # finditer()'s left-to-right order for multiple matches sharing one
    # literal's (single) lineno.
    violations.sort(key=lambda item: item[0])
    return violations


def _is_exempt_migration(path: Path) -> bool:
    """True for any file under the frozen migrations directory."""
    return path.is_relative_to(MIGRATIONS_DIR)


def _scan_source_tree() -> list[tuple[str, int, str, str, int]]:
    """Walk src/moneybin/**/*.py, collecting every occurrence.

    Returns (relpath, lineno, clause_keyword, table, occurrence) 5-tuples.
    `occurrence` is a 0-based count of prior matches of the same
    (clause_keyword, table) pair within this file, assigned in the source
    order `_scan_file` returns (see TABLE_LITERAL_ALLOWLIST's key-shape
    comment for why this — not the line number — is the stable identity).
    """
    found: list[tuple[str, int, str, str, int]] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if _is_exempt_migration(path):
            continue
        relpath = path.relative_to(SRC_ROOT).as_posix()
        occurrence_counts: dict[tuple[str, str], int] = {}
        for lineno, clause, table in _scan_file(path):
            key = (clause, table)
            occurrence = occurrence_counts.get(key, 0)
            occurrence_counts[key] = occurrence + 1
            found.append((relpath, lineno, clause, table, occurrence))
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
        (relpath, lineno, clause, table, occurrence)
        for relpath, lineno, clause, table, occurrence in found
        if (relpath, clause, table, occurrence) not in TABLE_LITERAL_ALLOWLIST
    ]
    if violations:
        formatted = "\n".join(
            f"  - {relpath}:{lineno}: {clause} {table} (occurrence {occurrence})"
            for relpath, lineno, clause, table, occurrence in violations
        )
        pytest.fail(
            "Hardcoded schema-qualified table literal(s) reach a SQL execute "
            "call. Import the TableRef constant from moneybin.tables instead, "
            'or add (file, clause_keyword, "schema.table", occurrence) to '
            f"TABLE_LITERAL_ALLOWLIST with a `# why` comment.\n\n"
            f"Violations:\n{formatted}"
        )


def test_allowlist_has_no_dead_entries() -> None:
    """Every allowlist entry must match a real occurrence in the tree.

    A stale entry silently widens the exception surface — if the swept
    literal it names is removed or fixed, the entry should go too.
    """
    found = {
        (relpath, clause, table, occurrence)
        for relpath, _lineno, clause, table, occurrence in _scan_source_tree()
    }
    stale = TABLE_LITERAL_ALLOWLIST - found
    if stale:
        formatted = "\n".join(
            f"  - {relpath}: {clause} {table} (occurrence {occurrence})"
            for relpath, clause, table, occurrence in stale
        )
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
