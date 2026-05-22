"""sqlglot-based SQL inspection helpers shared across validators.

Both the capability validator (writes) and the prefix validator (writes today;
reads when Plan 5 wires read-prefix validation) walk SQL ASTs to find CREATE
targets and table references. Centralize the parsing here so each validator
focuses on its semantic check.

Parsing uses the duckdb dialect — same setting used by src/moneybin/schema.py
for catalog comment extraction.

sqlglot falls back to a raw Command node rather than raising ParseError when it
encounters unsupported syntax. We treat any Command node in the result as a
parse failure and raise ValueError, since package SQL files must be fully
parseable to be validated.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path

import sqlglot
import sqlglot.expressions as exp
from sqlglot import ParseError


def _parse(sql_file: Path) -> Sequence[exp.Expr | None]:
    """Parse sql_file with the duckdb dialect; raise ValueError on failure.

    The returned sequence may contain None entries (empty statements / trailing
    semicolons). Callers must skip None entries before inspecting statement types.
    extract_create_targets() relies on isinstance(None, exp.Create) == False;
    iter_table_refs() has an explicit ``if statement is None: continue`` guard.
    """
    try:
        sql = sql_file.read_text(encoding="utf-8")
    except OSError as exc:
        # Wrap read failures (permissions, dangling symlink, race deletion) as
        # ValueError so every extract_*/find_* helper raises a single type the
        # validators already catch — keeps their "return violations, never
        # raise" contract intact even when a schema file is unreadable.
        raise ValueError(f"failed to read {sql_file}: {exc}") from exc
    try:
        statements = sqlglot.parse(sql, dialect="duckdb")
    except ParseError as exc:
        raise ValueError(f"failed to parse {sql_file}: {exc}") from exc

    for stmt in statements:
        if isinstance(stmt, exp.Command):
            raise ValueError(
                f"failed to parse {sql_file}: unsupported syntax "
                f"(sqlglot fell back to Command node)"
            )
    return statements


def _create_target(statement: exp.Create) -> exp.Table | None:
    """Resolve the Table a CREATE statement writes to.

    Derive from ``statement.this`` (the direct Schema/Table child) rather than
    the first Table in DFS order: a LIKE clause or FK constraint can place a
    *referenced* Table node earlier in the traversal, so ``statement.find(exp.Table)``
    would wrongly return that referenced table instead of the CREATE target —
    silently validating capability/prefix against the wrong name.

    exp.Create.this is a Schema (CREATE TABLE name (cols)) or a Table
    (CREATE VIEW name AS ...); resolve whichever down to the Table node.
    """
    this = statement.this
    return this.find(exp.Table) if this is not None else None


def extract_create_targets(sql_file: Path) -> list[tuple[str, str]]:
    """Return [(schema, name), ...] for every persistent CREATE in sql_file.

    Picks up both CREATE TABLE and CREATE VIEW. TEMP/TEMPORARY tables are
    skipped — they're ephemeral (dropped at session end) and never persist to a
    schema. An *unqualified* persistent CREATE (e.g. CREATE TABLE scratch)
    resolves to DuckDB's default 'main' schema and is returned as ('main', name)
    — NOT skipped — so the capability/prefix validators flag it: an unqualified
    write still escapes the package's declared globs.

    Raises:
        ValueError: if sqlglot cannot parse the file.
    """
    statements = _parse(sql_file)
    targets: list[tuple[str, str]] = []
    for statement in statements:
        if statement is None:
            continue
        if not isinstance(statement, exp.Create):
            continue
        if statement.kind not in ("TABLE", "VIEW"):
            continue
        if statement.find(exp.TemporaryProperty) is not None:
            continue
        table = _create_target(statement)
        if table is None:
            continue
        # Lowercase: DuckDB treats unquoted identifiers case-insensitively;
        # canonical form is lowercase so downstream glob/prefix matching is
        # predictable. Unqualified CREATE → DuckDB's default 'main' schema.
        db = table.args.get("db")
        schema = db.name.lower() if db else "main"
        name = table.name.lower()
        targets.append((schema, name))
    return targets


def find_quoted_create_identifiers(sql_file: Path) -> list[str]:
    """Return quoted schema/table identifiers used in CREATE targets.

    DuckDB treats quoted identifiers as case-SENSITIVE, but the capability,
    prefix, and layer validators canonicalize targets to lowercase (matching
    DuckDB's UNQUOTED case-insensitivity). A quoted, mixed-case target like
    ``CREATE TABLE "App".pkg_state`` would therefore pass an ``app.*`` check yet
    execute against a distinct ``App`` schema — bypassing the validate-then-
    execute guarantee. Package SQL must use unquoted lowercase identifiers; this
    surfaces any quoted schema/table name in a CREATE target so the validator
    can refuse it. (Unquoted mixed case is safe: DuckDB lowercases it at
    execution, matching the canonicalization.)

    Raises:
        ValueError: if sqlglot cannot parse the file.
    """
    statements = _parse(sql_file)
    offenders: list[str] = []
    for statement in statements:
        if statement is None or not isinstance(statement, exp.Create):
            continue
        if statement.kind not in ("TABLE", "VIEW"):
            continue
        table = _create_target(statement)
        if table is None:
            continue
        for ident in (table.args.get("db"), table.this):
            if isinstance(ident, exp.Identifier) and ident.quoted:
                offenders.append(ident.name)
    return offenders


def find_disallowed_statements(sql_file: Path) -> list[str]:
    """Return a descriptor for every statement that isn't CREATE TABLE/VIEW.

    A package's schema/ SQL may only declare its own *persistent* tables and
    views. Any other statement type — DML (INSERT/UPDATE/DELETE), destructive
    DDL (DROP/ALTER/TRUNCATE), or a non-table/view CREATE (INDEX/SCHEMA) — can
    read, mutate, or drop tables the capability/prefix validators never inspect
    (those look only at CREATE targets). A CREATE TEMPORARY TABLE/VIEW is also
    flagged: its kind is still "TABLE"/"VIEW", but extract_create_targets skips
    temp objects, so a temp CREATE would execute (Plan 4) without ever passing
    the capability/prefix checks. Flagging the statement type closes that bypass.

    Returns an empty list when every statement is an allowed persistent
    CREATE TABLE/VIEW.

    Raises:
        ValueError: if sqlglot cannot parse the file.
    """
    statements = _parse(sql_file)
    disallowed: list[str] = []
    for statement in statements:
        if statement is None:
            continue
        if isinstance(statement, exp.Create):
            if statement.find(exp.TemporaryProperty) is not None:
                # Mirrors extract_create_targets' temp skip: a temp CREATE never
                # reaches the capability/prefix validators, so flag it here.
                disallowed.append(f"CREATE TEMPORARY {statement.kind}")
                continue
            if statement.kind in ("TABLE", "VIEW"):
                continue
            disallowed.append(f"CREATE {statement.kind}")
        else:
            disallowed.append(statement.key.upper())
    return disallowed


def iter_table_refs(sql_file: Path) -> Iterator[tuple[str, str]]:
    """Yield (schema, name) for every schema-qualified table reference in sql_file.

    Foundation for Plan 5's read-prefix validation. Unqualified table references
    (FROM scratch) are skipped — they're either same-statement CTEs or temp
    objects, neither of which counts as a cross-schema dependency. The CREATE
    target of each statement is excluded so only read dependencies are yielded.
    """
    statements = _parse(sql_file)
    for statement in statements:
        if statement is None:
            continue
        # Exclude the CREATE target itself — it's a write, not a read dependency.
        create_target = (
            _create_target(statement) if isinstance(statement, exp.Create) else None
        )
        for table in statement.find_all(exp.Table):
            if table is create_target:
                continue
            if not table.args.get("db"):
                continue
            # Lowercase: same DuckDB case-insensitivity rationale as extract_create_targets.
            yield (table.args["db"].name.lower(), table.name.lower())
