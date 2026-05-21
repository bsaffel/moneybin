"""sqlglot-based SQL inspection helpers shared across validators.

Both the capability validator (writes) and the prefix validator (writes +
reads) walk SQL ASTs to find CREATE targets and table references. Centralize
the parsing here so each validator focuses on its semantic check.

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

    sqlglot returns a Command node (instead of raising) for unsupported or
    malformed syntax. Command nodes are treated as parse failures here because
    package SQL files must be fully parseable for validation to be meaningful.
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
        if not isinstance(statement, exp.Create):
            continue
        if statement.kind not in ("TABLE", "VIEW"):
            continue
        table = statement.find(exp.Table)
        if table is None or not table.args.get("db"):
            continue
        schema = table.args["db"].name
        name = table.name
        targets.append((schema, name))
    return targets


def iter_table_refs(sql_file: Path) -> Iterator[tuple[str, str]]:
    """Yield (schema, name) for every schema-qualified table reference in sql_file.

    Used by the prefix validator to confirm a package reads only from declared
    sources. Unqualified table references (FROM scratch) are skipped — they're
    either same-statement CTEs or temp objects, neither of which counts as a
    cross-schema dependency.
    """
    statements = _parse(sql_file)
    for statement in statements:
        if statement is None:
            continue
        for table in statement.find_all(exp.Table):
            if not table.args.get("db"):
                continue
            yield (table.args["db"].name, table.name)
