"""Static guard: migrations must not write to SQLMesh-owned schemas.

Migrations own ``app.*`` and ``raw.*`` (plus the ``app.schema_migrations``
tracking table). They must never *write* a relation in a SQLMesh-owned schema
(``seeds`` / ``meta`` / ``core`` / ``prep`` / ``reports`` / ``analytics``),
because on any database whose SQLMesh virtual layer is materialized those
relations are **views** — and ``ALTER`` / ``DROP`` / ``TRUNCATE`` / ``INSERT`` /
``UPDATE`` / ``DELETE`` / ``MERGE`` / ``COPY … FROM`` / non-idempotent ``CREATE``
against a view raises at apply time.

This is the exact bug V032 shipped (PR #306): ``ALTER TABLE seeds.categories``
(plus an ``UPDATE seeds.categories``) passed every test — each ran against a
fresh DB where ``seeds.categories`` was still the migration-bootstrapped table —
then stuck-failed a real ``moneybin sync pull`` on a materialized database.

Rather than run every migration against a materialized DB (circular: SQLMesh can
only materialize once the schema the migration itself adds exists), this test
statically inspects each migration's SQL and fails if any writes a SQLMesh-owned
relation. ``CREATE TABLE ... IF NOT EXISTS`` is allowed — it is an idempotent
no-op on a view (V014's legitimate seed-table bootstrap). ``SELECT`` is a read.

Write-target resolution and its limit: the target relation is resolved when it
is an inline literal, a module constant (``conn.execute(_CREATE_SQL)``, e.g.
V034), a ``for <var> in (<string literals>)`` loop variable interpolated into an
f-string (V012's ``DROP TABLE IF EXISTS {table}`` over a literal tuple), or a
local literal assignment. Three binder gaps are known and NOT resolved (each
would blank the target); all are unexploited by the current ladder and tracked
as one follow-up: (1) ``for k, v in {dict}.items()`` (V003, keys all ``raw.*``);
(2) ``for a, b in <name>`` where the name is a built/mutated list (V014's
``list(_BACKFILLS)`` + conditional append, all ``app.*``); (3) a resolvable
binding applied through an *intermediate local* — ``sql = f"... {table}";
execute(sql)`` — where the f-string is flattened (interpolation blanked) at
assignment time, before the loop/name binding is applied (only a *direct*
``execute(f"... {table}")`` resolves today). A target computed at true *runtime*
— a function argument, a name read from ``duckdb_indexes()`` /
``duckdb_tables()`` — can't be resolved statically at all. The regex fallback
still covers the common static-target / dynamic-*value* shape. SQL is read only from ``.execute()``
arguments, so a docstring or comment mentioning ``ALTER TABLE seeds.x`` never
triggers a false positive.
"""

from __future__ import annotations

import ast
import itertools
import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

# sqlglot's base node type isn't in its public `__all__`; alias once with a
# single suppression rather than annotating every helper against it.
_Node = exp.Expression  # pyright: ignore[reportPrivateImportUsage]

_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[2] / "src" / "moneybin" / "sql" / "migrations"
)

# Schemas SQLMesh owns and exposes as views on a materialized database. app.* and
# raw.* are migration-owned. `analytics` ships starter user models and is
# explicitly "never touched by migrations" (analytics_schema.sql header,
# migrations/README.md). See AGENTS.md "Architecture: Data Layers".
_SQLMESH_OWNED_SCHEMAS = frozenset({
    "seeds",
    "meta",
    "core",
    "prep",
    "reports",
    "analytics",
})

# Top-level statement types that mutate their target relation. exp.Merge covers
# DuckDB's MERGE INTO upsert (an INSERT/UPDATE combined) — its target is the
# same `stmt.this` Table the other write types expose, so _owned_schema_of works.
_WRITE_TYPES = (exp.Alter, exp.Drop, exp.Insert, exp.Update, exp.Delete, exp.Merge)

# Fallback for SQL sqlglot can't parse (f-string fragments, dialect quirks): a
# write keyword immediately targeting an owned schema. CREATE TABLE IF NOT EXISTS
# is excluded (idempotent). Conservative — requires the keyword AND the schema.
# The owned-schema alternation is derived from the frozenset above so the two
# can't drift apart.
_OWNED_SCHEMA_ALT = "|".join(sorted(_SQLMESH_OWNED_SCHEMAS))
_FALLBACK_RE = re.compile(
    r"\b(ALTER\s+TABLE|UPDATE|INSERT\s+INTO|DELETE\s+FROM|MERGE\s+INTO|"
    r"TRUNCATE(?:\s+TABLE)?|DROP\s+(?:TABLE|VIEW|INDEX|SCHEMA)|CREATE\s+INDEX|"
    r"CREATE\s+OR\s+REPLACE\s+TABLE|CREATE\s+TABLE(?!\s+IF\s+NOT\s+EXISTS))\s+"
    r"(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"
    rf"[\"']?({_OWNED_SCHEMA_ALT})[\"']?\.",
    re.IGNORECASE,
)


def _owned_schema_of(stmt: _Node) -> str | None:
    """Return the target table's schema if it is SQLMesh-owned, else None."""
    if stmt.this is None:
        return None
    target = stmt.this.find(exp.Table)
    if target is None:
        return None
    db = (target.db or "").lower()
    return db if db in _SQLMESH_OWNED_SCHEMAS else None


def _describe(stmt: _Node, schema: str) -> str:
    target = stmt.this.find(exp.Table) if stmt.this is not None else None
    if target is not None and target.name:
        return f"{stmt.key.upper()} {schema}.{target.name}"
    # Schema-level op (e.g. DROP SCHEMA) — the target Table carries the schema in
    # `.db` with no `.name`, so name it by kind + schema (avoids "DROP seeds.").
    kind = str(stmt.args.get("kind") or "").upper()
    return " ".join(p for p in (stmt.key.upper(), kind, schema) if p)


def _fallback(sql: str) -> list[str]:
    # finditer (not search): a multi-statement file that falls back can hold more
    # than one owned-schema write; report every distinct one, not just the first.
    out: list[str] = []
    for match in _FALLBACK_RE.finditer(sql):
        desc = f"{match.group(1).upper().split()[0]} {match.group(2)}.* (dynamic SQL)"
        if desc not in out:
            out.append(desc)
    return out


def violations_in_sql(sql: str) -> list[str]:
    """Descriptions of writes to SQLMesh-owned relations in one SQL string."""
    try:
        statements = sqlglot.parse(sql, dialect="duckdb")
    except Exception:  # noqa: BLE001 — sqlglot raises varied parse/token errors on non-SQL
        return _fallback(sql)

    out: list[str] = []
    saw_command = False
    for stmt in statements:
        if stmt is None:
            continue
        if isinstance(stmt, exp.Command):
            # sqlglot couldn't model this statement (malformed / f-string-
            # flattened) but returned a catch-all Command instead of raising —
            # so the except-branch fallback never fired. Regex-scan it here too,
            # or a write it can't parse (e.g. a flattened ALTER) slips through.
            saw_command = True
            continue
        if isinstance(stmt, exp.Create):
            kind = (stmt.args.get("kind") or "").upper()
            exists = bool(stmt.args.get("exists"))
            replace = bool(stmt.args.get("replace"))
            if kind == "SCHEMA":
                continue  # CREATE SCHEMA — a namespace, never a relation write
            if kind == "TABLE" and exists:
                continue  # CREATE TABLE IF NOT EXISTS — idempotent no-op on a view
            if kind == "VIEW" and (replace or exists):
                continue  # CREATE OR REPLACE / IF NOT EXISTS VIEW — safe on a view
            schema = _owned_schema_of(stmt)
            if schema is not None:
                out.append(_describe(stmt, schema))
        elif isinstance(stmt, exp.Copy):
            # COPY <tbl> FROM <file> imports into the table (kind=True = a write);
            # COPY <tbl> TO <file> exports (kind=False = a read). Only FROM writes.
            # Direction follows the target, so the regex fallback can't tell them
            # apart — COPY is handled on the AST path only.
            if bool(stmt.args.get("kind")):
                schema = _owned_schema_of(stmt)
                if schema is not None:
                    out.append(_describe(stmt, schema))
        elif isinstance(stmt, exp.TruncateTable):
            # TRUNCATE empties its target (a write). sqlglot puts the target in
            # `.expressions`, not `.this`, and TRUNCATE has no read clause — so the
            # sole Table in the statement is the write target.
            target = stmt.find(exp.Table)
            db = (target.db or "").lower() if target is not None else ""
            if db in _SQLMESH_OWNED_SCHEMAS and target is not None:
                out.append(f"TRUNCATE {db}.{target.name}")
        elif isinstance(stmt, _WRITE_TYPES):
            schema = _owned_schema_of(stmt)
            if schema is not None:
                out.append(_describe(stmt, schema))
    if saw_command:
        out.extend(_fallback(sql))
    return out


def _flatten_joinedstr(node: ast.JoinedStr, subst: dict[str, str]) -> str:
    """Flatten an f-string to text, resolving interpolations against ``subst``.

    Literal parts stay verbatim; a ``{name}`` interpolation becomes ``subst[name]``
    when known, else a single space — so a resolved (or blanked) write *target*
    reaches the scanner while a dynamic *value* drops out.
    """
    parts: list[str] = []
    for v in node.values:
        if isinstance(v, ast.Constant) and isinstance(v.value, str):
            parts.append(v.value)
        elif (
            isinstance(v, ast.FormattedValue)
            and isinstance(v.value, ast.Name)
            and v.value.id in subst
        ):
            parts.append(subst[v.value.id])
        else:
            parts.append(" ")
    return "".join(parts)


def _static_str(node: ast.expr | None) -> str | None:
    """A string constant, or an f-string with every interpolation blanked, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return _flatten_joinedstr(node, {})
    return None


def _string_bindings(tree: ast.AST) -> dict[str, list[str]]:
    """Map each name to the static string value(s) it can hold in a migration.

    Three sources, walked across the whole module (so intra-function locals count,
    not just module globals): literal/f-string assignments (``sql = "..."``),
    annotated assignments, and ``for <name> in (<string literals>)`` loop targets
    (the V012 ``for table in ("app.x", "seeds.y", ...)`` shape). Values accumulate
    per name so a loop over a literal tuple contributes every element.
    """
    bindings: dict[str, list[str]] = {}

    def add(name: str, value: str) -> None:
        vals = bindings.setdefault(name, [])
        if value not in vals:
            vals.append(value)

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            text = _static_str(node.value)
            if text is not None:
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        add(target.id, text)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            text = _static_str(node.value)
            if text is not None:
                add(node.target.id, text)
        elif (
            isinstance(node, ast.For)
            and isinstance(node.target, ast.Name)
            and isinstance(node.iter, (ast.Tuple, ast.List, ast.Set))
        ):
            for elt in node.iter.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    add(node.target.id, elt.value)
    return bindings


def _expand_execute_arg(arg: ast.expr, bindings: dict[str, list[str]]) -> list[str]:
    """Concrete SQL string(s) an ``.execute()`` argument can take.

    Resolves names and f-string interpolations against ``bindings`` so a non-inline
    write *target* still reaches the scanner. An f-string interpolating a bound name
    expands to one string per value (cross-product across distinct bound names).
    """
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return [arg.value]
    if isinstance(arg, ast.Name):
        return list(bindings.get(arg.id, []))
    if isinstance(arg, ast.JoinedStr):
        names = sorted({
            v.value.id
            for v in arg.values
            if isinstance(v, ast.FormattedValue)
            and isinstance(v.value, ast.Name)
            and v.value.id in bindings
        })
        if not names:
            return [_flatten_joinedstr(arg, {})]
        return [
            _flatten_joinedstr(arg, dict(zip(names, combo, strict=True)))
            for combo in itertools.product(*(bindings[n] for n in names))
        ]
    return []


def _execute_sql_args(py_source: str) -> list[str]:
    """SQL strings passed to ``.execute()`` / ``.executemany()`` in a .py migration.

    Resolves ``conn.execute(_CONST_SQL)`` (module constant, e.g. V034) and
    loop-variable / local-literal write targets interpolated into an f-string
    (V012's ``DROP TABLE IF EXISTS {table}`` over a literal tuple) via
    ``_string_bindings``. A target computed at runtime stays unresolved — see the
    module docstring's limitation note.
    """
    tree = ast.parse(py_source)
    bindings = _string_bindings(tree)

    out: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("execute", "executemany")
            and node.args
        ):
            out.extend(_expand_execute_arg(node.args[0], bindings))
    return out


def _migration_paths() -> list[Path]:
    return sorted(
        p
        for p in _MIGRATIONS_DIR.iterdir()
        if p.is_file() and p.suffix in (".sql", ".py") and p.name.startswith("V")
    )


# --- classifier golden cases (hand-authored expected values) ------------------

_GOLDEN: list[tuple[str, bool]] = [
    ("ALTER TABLE seeds.categories ADD COLUMN class VARCHAR", True),
    ("ALTER TABLE app.user_categories ADD COLUMN class VARCHAR", False),
    ("UPDATE seeds.categories SET class = 'x'", True),
    ("UPDATE app.user_categories SET class = 'x'", False),
    ("DROP TABLE core.dim_x", True),
    ("DROP TABLE app.foo", False),
    ("DELETE FROM meta.model_freshness WHERE x = 1", True),
    ("INSERT INTO core.dim_x VALUES (1)", True),
    # MERGE INTO is a DuckDB upsert (INSERT+UPDATE) — a write to its target
    (
        "MERGE INTO core.dim_x t USING app.s s ON t.id=s.id "
        "WHEN MATCHED THEN UPDATE SET a=s.a",
        True,
    ),
    (
        "MERGE INTO app.dim_x t USING core.s s ON t.id=s.id "
        "WHEN MATCHED THEN UPDATE SET a=s.a",
        False,
    ),
    # COPY ... FROM imports (writes the target); COPY ... TO exports (reads it)
    ("COPY core.dim_x FROM 'f.csv'", True),
    ("COPY core.dim_x TO 'f.csv'", False),
    ("COPY app.x FROM 'f.csv'", False),
    # TRUNCATE empties the target (a write); target lives in .expressions
    ("TRUNCATE core.dim_x", True),
    ("TRUNCATE TABLE seeds.categories", True),
    ("TRUNCATE app.x", False),
    # DROP SCHEMA on an owned namespace is a destructive write; app.* is fine
    ("DROP SCHEMA seeds CASCADE", True),
    ("DROP SCHEMA IF EXISTS meta", True),
    ("DROP SCHEMA app", False),
    # reports is a SQLMesh view layer too (reports.net_worth, etc.)
    ("ALTER TABLE reports.net_worth ADD COLUMN x INT", True),
    ("SELECT amount FROM reports.net_worth", False),
    # idempotent create on a seed table is the allowed V014 bootstrap
    ("CREATE TABLE IF NOT EXISTS seeds.categories (id VARCHAR)", False),
    ("CREATE TABLE seeds.categories (id VARCHAR)", True),
    ("CREATE INDEX idx ON prep.foo (id)", True),
    ("CREATE TABLE app.category_source_map (id VARCHAR)", False),
    # namespace + idempotent view bootstraps are safe on a materialized DB
    ("CREATE SCHEMA IF NOT EXISTS seeds", False),
    ("CREATE OR REPLACE VIEW core.dim_categories AS SELECT 1 AS x", False),
    ("CREATE VIEW IF NOT EXISTS core.x AS SELECT 1 AS x", False),
    ("CREATE VIEW core.x AS SELECT 1 AS x", True),
    # CREATE OR REPLACE TABLE overwrites (non-idempotent) — flagged on owned, both
    # via the AST path and, for a flattened f-string target, the regex fallback
    ("CREATE OR REPLACE TABLE core.dim_x AS SELECT 1 AS a", True),
    ("CREATE OR REPLACE TABLE app.x AS SELECT 1 AS a", False),
    ("CREATE OR REPLACE TABLE seeds.  AS SELECT 1", True),
    # writes to app.* that merely READ a SQLMesh-owned relation are fine
    ("INSERT INTO app.x SELECT id FROM seeds.y", False),
    ("CREATE TABLE app.x AS SELECT id FROM core.y", False),
    ("SELECT plaid_detailed FROM seeds.categories", False),
    # f-string fragment (sqlglot can't parse) — regex fallback still catches it
    ("UPDATE seeds.categories SET class =  ", True),
    ("MERGE INTO seeds.categories t USING  ", True),
    # sqlglot returns an exp.Command (not a raise) for this flattened ALTER;
    # the fallback must still fire so the write isn't silently skipped
    ("ALTER TABLE seeds.categories ADD COLUMN   ", True),
    ("ALTER TABLE app.user_categories ADD COLUMN   ", False),
]


@pytest.mark.parametrize(("sql", "is_violation"), _GOLDEN)
def test_classifier_flags_writes_to_sqlmesh_schemas(
    sql: str, is_violation: bool
) -> None:
    assert bool(violations_in_sql(sql)) is is_violation, violations_in_sql(sql)


def test_no_migration_writes_sqlmesh_owned_schemas() -> None:
    """Every shipped migration must leave SQLMesh-owned relations unwritten."""
    paths = _migration_paths()
    assert paths, "no migrations discovered — path resolution is wrong"

    violations: list[str] = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        sqls = [text] if path.suffix == ".sql" else _execute_sql_args(text)
        for sql in sqls:
            violations.extend(f"{path.name}: {v}" for v in violations_in_sql(sql))

    assert not violations, (
        "Migrations must not write to SQLMesh-owned schemas "
        f"({'/'.join(sorted(_SQLMESH_OWNED_SCHEMAS))}) — those are views on a "
        "materialized database (see this module's docstring / PR #306):\n  • "
        + "\n  • ".join(violations)
    )


def test_execute_sql_args_resolves_module_constants() -> None:
    """SQL defined as a module constant and executed by name is scanned (V034 style)."""
    source = (
        '_BAD_SQL = "ALTER TABLE seeds.categories ADD COLUMN x VARCHAR"\n'
        "def migrate(conn):\n"
        "    conn.execute(_BAD_SQL)\n"
    )
    extracted = _execute_sql_args(source)
    assert any(violations_in_sql(sql) for sql in extracted), extracted


def test_execute_sql_args_resolves_loop_variable_targets() -> None:
    """A `for <var> in (<literals>)` write target in an f-string is scanned (V012 style).

    The write target is the loop variable, invisible to constant resolution — this
    is the exact shape that let V012's `DROP TABLE IF EXISTS seeds.merchants_*`
    slip past the guard until the loop-literal resolver was added.
    """
    source = (
        "def migrate(conn):\n"
        '    for table in ("app.merchant_overrides", "seeds.merchants_global"):\n'
        '        conn.execute(f"DROP TABLE IF EXISTS {table}")\n'
    )
    flagged = [sql for sql in _execute_sql_args(source) if violations_in_sql(sql)]
    # Exactly the seeds.* element is a violation; the app.* element is not.
    assert flagged == ["DROP TABLE IF EXISTS seeds.merchants_global"], flagged
