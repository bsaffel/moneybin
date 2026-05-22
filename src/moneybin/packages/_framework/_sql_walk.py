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
    sql = sql_file.read_text()
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
    """Return [(schema, name), ...] for every schema-qualified CREATE in sql_file.

    Picks up both CREATE TABLE and CREATE VIEW. Unqualified CREATE statements
    (CREATE TEMP TABLE, CREATE TABLE scratch) are skipped — packages must
    write to schema-qualified tables; a bare CREATE is either an in-memory
    helper or a bug, neither of which counts toward capability validation.

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
        table = _create_target(statement)
        if table is None or not table.args.get("db"):
            continue
        # Lowercase: DuckDB treats unquoted identifiers case-insensitively;
        # canonical form is lowercase so downstream glob/prefix matching is predictable.
        schema = table.args["db"].name.lower()
        name = table.name.lower()
        targets.append((schema, name))
    return targets


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
