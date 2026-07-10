"""Static guard: migrations must not write to SQLMesh-owned schemas.

Migrations own ``app.*`` and ``raw.*`` (plus the ``app.schema_migrations``
tracking table). They must never *write* a relation in a SQLMesh-owned schema
(``seeds`` / ``meta`` / ``core`` / ``prep``), because on any database whose
SQLMesh virtual layer is materialized those relations are **views** — and
``ALTER`` / ``DROP`` / ``INSERT`` / ``UPDATE`` / ``DELETE`` / ``MERGE`` /
non-idempotent ``CREATE`` against a view raises at apply time.

This is the exact bug V032 shipped (PR #306): ``ALTER TABLE seeds.categories``
(plus an ``UPDATE seeds.categories``) passed every test — each ran against a
fresh DB where ``seeds.categories`` was still the migration-bootstrapped table —
then stuck-failed a real ``moneybin sync pull`` on a materialized database.

Rather than run every migration against a materialized DB (circular: SQLMesh can
only materialize once the schema the migration itself adds exists), this test
statically inspects each migration's SQL and fails if any writes a SQLMesh-owned
relation. ``CREATE TABLE ... IF NOT EXISTS`` is allowed — it is an idempotent
no-op on a view (V014's legitimate seed-table bootstrap). ``SELECT`` is a read.

Limitation: SQL assembled dynamically at runtime (f-strings interpolating a
*table name*, string concatenation) can't be fully resolved statically; the
common shape — a static write target with a dynamic *value* — is covered by the
regex fallback. SQL is read only from ``.execute()`` arguments, so a docstring
or comment mentioning ``ALTER TABLE seeds.x`` never triggers a false positive.
"""

from __future__ import annotations

import ast
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
# raw.* are migration-owned. See AGENTS.md "Architecture: Data Layers".
_SQLMESH_OWNED_SCHEMAS = frozenset({"seeds", "meta", "core", "prep", "reports"})

# Top-level statement types that mutate their target relation. exp.Merge covers
# DuckDB's MERGE INTO upsert (an INSERT/UPDATE combined) — its target is the
# same `stmt.this` Table the other write types expose, so _owned_schema_of works.
_WRITE_TYPES = (exp.Alter, exp.Drop, exp.Insert, exp.Update, exp.Delete, exp.Merge)

# Fallback for SQL sqlglot can't parse (f-string fragments, dialect quirks): a
# write keyword immediately targeting an owned schema. CREATE TABLE IF NOT EXISTS
# is excluded (idempotent). Conservative — requires the keyword AND the schema.
_FALLBACK_RE = re.compile(
    r"\b(ALTER\s+TABLE|UPDATE|INSERT\s+INTO|DELETE\s+FROM|MERGE\s+INTO|"
    r"DROP\s+(?:TABLE|VIEW|INDEX)|CREATE\s+INDEX|"
    r"CREATE\s+TABLE(?!\s+IF\s+NOT\s+EXISTS))\s+"
    r"(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"
    r"[\"']?(seeds|meta|core|prep|reports)[\"']?\.",
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
    name = f"{schema}.{target.name}" if target is not None else schema
    return f"{stmt.key.upper()} {name}"


def _fallback(sql: str) -> list[str]:
    match = _FALLBACK_RE.search(sql)
    if match is None:
        return []
    return [f"{match.group(1).upper().split()[0]} {match.group(2)}.* (dynamic SQL)"]


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
        elif isinstance(stmt, _WRITE_TYPES):
            schema = _owned_schema_of(stmt)
            if schema is not None:
                out.append(_describe(stmt, schema))
    if saw_command:
        out.extend(_fallback(sql))
    return out


def _static_str(node: ast.expr | None) -> str | None:
    """A string constant, or an f-string flattened to its literal parts, else None.

    f-string interpolations become a space so a static write *target* survives
    for the regex fallback while a dynamic *value* drops out.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else " "
            for v in node.values
        )
    return None


def _execute_sql_args(py_source: str) -> list[str]:
    """SQL strings passed to ``.execute()`` / ``.executemany()`` in a .py migration.

    Resolves the common ``conn.execute(_CONST_SQL)`` pattern (e.g. V034) by
    first mapping module-level string/f-string assignments, then substituting a
    bare-name argument for its assigned value.
    """
    tree = ast.parse(py_source)

    consts: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = [t for t in node.targets if isinstance(t, ast.Name)]
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target]
            value = node.value
        else:
            continue
        text = _static_str(value)
        if text is not None:
            for target in targets:
                consts[target.id] = text

    out: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("execute", "executemany")
            and node.args
        ):
            arg = node.args[0]
            text = _static_str(arg)
            if text is not None:
                out.append(text)
            elif isinstance(arg, ast.Name) and arg.id in consts:
                out.append(consts[arg.id])
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
        "(seeds/meta/core/prep/reports) — those are views on a materialized "
        "database (see this module's docstring / PR #306):\n  • "
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
