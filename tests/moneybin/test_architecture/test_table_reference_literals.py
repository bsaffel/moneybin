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
   string literals that *reach* a ``.execute()`` / ``.executemany()`` /
   ``.sql()`` call are candidates — not every string in the tree. "Reaches" means: passed
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

3. **Function-scoped name binding, with module-level fallback.** A candidate
   name is resolved within the module or function/method body that binds it
   — NOT across the whole file. Two unrelated functions in the same module
   reusing a conventional local name (``query``, ``sql``) no longer
   contaminate each other: a literal assigned to ``query`` in function ``f``
   is invisible when a *different* ``query`` (e.g. a parameter) reaches
   ``execute()`` in function ``g``. A name with no local binding falls back
   to the module-level scope — this is what lets a module-level constant
   like ``_NEWEST_HOLDINGS_SNAPSHOT_CTE`` (point 1) still resolve from
   inside a method. This is Local + Global, not full Python LEGB: a name
   bound only in an *enclosing function* (a closure) is not resolved,
   consistent with this being a bounded, file-local heuristic. See the
   scoping comment above ``_scan_file``'s per-scope loop.

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
  needed no entries here: none of them are arguments to a recognized SQL
  sink call, so the AST-first design excludes them by construction.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src" / "moneybin"
MIGRATIONS_DIR = SRC_ROOT / "sql" / "migrations"

# Method names treated as SQL-execution sinks. `sql` covers
# `Database.sql()` (`database.py`) — a real, parameter-free execution path
# (e.g. `db.sql("SELECT version()")` in `cli/commands/db.py`) distinct from
# `.execute()`/`.executemany()` but reaching the same DuckDB connection.
EXECUTE_METHOD_NAMES = frozenset({"execute", "executemany", "sql"})

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


_ScopeLiterals = dict[str, list[tuple[int, str, set[str]]]]


def _direct_scope_nodes(root: ast.AST) -> list[ast.AST]:
    """All descendant nodes of `root`, not descending into a nested function.

    A nested ``ast.FunctionDef``/``ast.AsyncFunctionDef`` is itself included
    (so a decorator or default-argument literal is still visible to this
    scope) but its own body is NOT descended into here — that subtree is a
    separate scope, walked independently by ``_scan_file``. Everything else
    (``if``/``for``/``with``/``try``/class bodies, comprehensions) is not a
    distinct variable scope in this heuristic and is walked through, mirroring
    Python's actual scoping rule that only ``def``/``lambda`` introduce a new
    local namespace.
    """
    nodes: list[ast.AST] = []
    stack = list(ast.iter_child_nodes(root))
    while stack:
        node = stack.pop()
        nodes.append(node)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        stack.extend(ast.iter_child_nodes(node))
    return nodes


def _collect_scope(
    nodes: list[ast.AST],
) -> tuple[_ScopeLiterals, set[str], list[tuple[int, str]]]:
    """Collect literal bindings, execute()-seed names, and direct literal args.

    `nodes` is one lexical scope's node list from `_direct_scope_nodes` — the
    module body, or one function/method body. Enumeration of AST nodes that
    bind a name to a value, and whether each can carry a hardcoded string
    literal into an execute() call:

    HANDLED — the value is written inline at the binding site, so a
    literal there is a real candidate:
      - ast.Assign        x = "..."
      - ast.AnnAssign     x: str = "..."   (skipped when .value is None,
                                             i.e. a bare annotation)
      - ast.AugAssign     x += "..."       (records the added text; does
                                             not track prior concatenation
                                             — same file-local-heuristic
                                             limit as module docstring
                                             point 1)
      - ast.NamedExpr     (x := "...")     (walrus; target is always a
                                             plain Name per grammar)

    CONSCIOUSLY EXCLUDED — no realistic or in-scope path to a hardcoded
    literal:
      - ast.For / ast.AsyncFor / comprehension `for` clauses: the target
        is bound to elements of an *iterable*, not a literal written at
        the binding site. Extracting a string from a List/Tuple-of-
        Constants iterable is a materially different shape than "value is
        a Constant/JoinedStr" and falls under the same disclaimed gap as
        ".join()"/concatenation in module docstring point 1.
      - ast.With / ast.AsyncWith `as` targets: bound to a context
        manager's __enter__() return value — never a literal present in
        source at the binding site.
      - ast.ExceptHandler `as` targets: bound to a raised exception
        instance, never a literal.
      - Function/lambda parameters (incl. defaults): the runtime value
        comes from the call site, not the default text — tracking it
        requires interprocedural analysis, explicitly out of scope
        ("a helper function's return value can evade it", docstring
        point 1).
      - ast.Global / ast.Nonlocal: declarations with no value to record.
      - Tuple/list-destructured Assign (`a, b = "core.x", "core.y"`):
        still an ast.Assign node (no separate node type), but
        `_literal_text` only extracts Constant/JoinedStr — a destructured
        Tuple/List RHS is silently not unpacked. Noted as a residual gap
        on the existing Assign handler, not a new node type to enumerate.
    """
    var_literals: _ScopeLiterals = {}
    seed_names: set[str] = set()
    scanned: list[tuple[int, str]] = []  # (lineno, text) reaching execute()

    for node in nodes:
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

    return var_literals, seed_names, scanned


def _resolve_worklist(
    seed_names: set[str],
    local_literals: _ScopeLiterals,
    fallback_literals: _ScopeLiterals,
) -> list[tuple[int, str]]:
    """Fixed-point worklist: resolve seed names to their literal text.

    Checks `local_literals` (this scope) first; a name with no local binding
    falls back to `fallback_literals` (the module scope) — module docstring
    point 3. A name "reaches" execute() directly, or transitively through an
    f-string interpolation inside another reaching literal (docstring
    point 1).
    """
    worklist = list(seed_names)
    visited: set[str] = set()
    scanned: list[tuple[int, str]] = []
    while worklist:
        name = worklist.pop()
        if name in visited:
            continue
        visited.add(name)
        entries = local_literals.get(name) or fallback_literals.get(name, [])
        for lineno, text, embedded in entries:
            scanned.append((lineno, text))
            worklist.extend(embedded - visited)
    return scanned


def _scan_file(path: Path) -> list[tuple[int, str, str]]:
    """Return (lineno, clause_keyword, "schema.table") violations for one file.

    Scans the module scope and every function/method body as independent
    scopes (module docstring point 3), each falling back to the module scope
    for names it doesn't bind locally. Returned in source order (sorted by
    lineno) so a caller can assign stable per-(clause, table) occurrence
    indices — see TABLE_LITERAL_ALLOWLIST's key-shape comment.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []

    module_literals, module_seeds, all_scanned = _collect_scope(
        _direct_scope_nodes(tree)
    )
    all_scanned = list(all_scanned)
    all_scanned.extend(_resolve_worklist(module_seeds, module_literals, {}))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            local_literals, local_seeds, local_scanned = _collect_scope(
                _direct_scope_nodes(node)
            )
            all_scanned.extend(local_scanned)
            all_scanned.extend(
                _resolve_worklist(local_seeds, local_literals, module_literals)
            )

    violations: list[tuple[int, str, str]] = []
    for lineno, text in all_scanned:
        for match in SCHEMA_TABLE_PATTERN.finditer(text):
            clause = match.group(1).upper()
            table = f"{match.group(2)}.{match.group(3)}"
            violations.append((lineno, clause, table))
    # Sort by lineno so occurrence indices (assigned by the caller) reflect
    # top-to-bottom source order rather than scan-order — see
    # TABLE_LITERAL_ALLOWLIST's key-shape comment. Stable sort preserves
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


# --- Synthetic-fixture scanner unit tests -----------------------------------
#
# The tests above assert against whatever src/moneybin currently contains —
# real coverage of the scanner's own core logic (keyword gate, alias
# exclusion, CTE-splice tracing, function-scope boundary) is incidental to
# that, not guaranteed. These exercise `_scan_file` directly against small
# synthetic snippets written to `tmp_path`, so each mechanism is pinned
# independently of what the live tree happens to contain.


def _scan_source(tmp_path: Path, source: str) -> list[tuple[int, str, str]]:
    """Write `source` to a temp module and return `_scan_file`'s violations."""
    module = tmp_path / "_synthetic_module.py"
    module.write_text(source, encoding="utf-8")
    return _scan_file(module)


def test_keyword_gate_flags_a_genuine_table_reference(tmp_path: Path) -> None:
    """A schema.table immediately after FROM is a violation."""
    source = 'db.execute("SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_keyword_gate_excludes_column_reference_through_alias(tmp_path: Path) -> None:
    """A table aliased to its own schema name is not a false positive on its column refs.

    ``raw.account_id`` is a column reference through the ``AS raw`` alias —
    only the genuine ``FROM raw.tabular_accounts`` table target is flagged
    (module docstring point 2).
    """
    source = 'db.execute("SELECT raw.account_id FROM raw.tabular_accounts AS raw")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "raw.tabular_accounts")]


def test_keyword_gate_excludes_sql_comment_prose(tmp_path: Path) -> None:
    """A schema.table mentioned in a SQL comment, not after a clause keyword, is not flagged."""
    source = 'db.execute("-- core.fct_transactions is expensive\\nSELECT 1")\n'
    assert _scan_source(tmp_path, source) == []


def test_sql_method_is_a_recognized_sink(tmp_path: Path) -> None:
    """`.sql()` (Database.sql()) is scanned like `.execute()`/`.executemany()`."""
    source = 'db.sql("SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_cte_splice_transitively_reaches_execute(tmp_path: Path) -> None:
    """A module-level literal spliced via f-string into a method's query is traced.

    Mirrors ``_NEWEST_HOLDINGS_SNAPSHOT_CTE`` in ``doctor_service.py``
    (module docstring point 1): a module-level fragment assigned once, then
    interpolated into a query built inside a function.
    """
    source = (
        '_CTE = "SELECT * FROM core.fct_transactions"\n'
        "\n\n"
        "def f(db):\n"
        '    db.execute(f"WITH x AS ({_CTE}) SELECT 1")\n'
    )
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_local_binding_reaches_execute_within_its_own_function(tmp_path: Path) -> None:
    """A literal assigned and used within one function is caught (positive control)."""
    source = (
        "def f(db):\n"
        '    query = "SELECT * FROM core.fct_transactions"\n'
        "    db.execute(query)\n"
    )
    assert _scan_source(tmp_path, source) == [(2, "FROM", "core.fct_transactions")]


def test_function_scope_does_not_leak_a_same_named_local_across_functions(
    tmp_path: Path,
) -> None:
    """A local reused as a name in an unrelated function is not a false positive.

    ``f``'s ``query`` local is a real literal; ``g``'s ``query`` is a
    parameter with no local binding of its own. Before function-scoping
    (module docstring point 3), the file-global name lookup let ``g``'s
    ``execute(query)`` pick up ``f``'s unrelated literal.
    """
    source = (
        "def f():\n"
        '    query = "SELECT * FROM core.fct_transactions"\n'
        "    return query\n"
        "\n\n"
        "def g(db, query):\n"
        "    db.execute(query)\n"
    )
    assert _scan_source(tmp_path, source) == []
