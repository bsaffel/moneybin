"""sqlglot column-lineage resolution for the dynamic SQL surface.

``redact_typed`` masks typed payloads by walking ``Annotated[T, DataClass]``
metadata. ``sql_query`` has no static return type — its shape is the caller's
query. This module recovers the same per-column ``DataClass`` mapping by
parsing the SQL, expanding ``*`` against the live schema, and resolving each
output column to a class via the ``CLASSIFICATION`` registry. The result feeds
``redact_records`` (CRITICAL masking today; whatever ``_TRANSFORMS`` does
tomorrow) and sets the per-call envelope sensitivity.

Fail-closed: anything we cannot resolve is treated as the most sensitive
class the query could touch (max tier over all input columns), so we
over-redact rather than leak.

Scope: this module classifies columns in the ``core`` and ``app`` schemas
only — those are the schemas the ``CLASSIFICATION`` registry covers. A query
that references other schemas (``raw``/``prep``) yields no classifiable input
columns, so an unresolvable projection falls back to ``AGGREGATE`` (LOW)
rather than CRITICAL. Callers (the ``sql_query`` wiring) MUST restrict the
query to classified schemas before relying on this module's masking — see the
table-allowlist gate in the tool layer.

API note (sqlglot 30.8.0): after ``qualify()``, ``Column.table`` is the
alias that appeared in the SQL (e.g. ``"t"`` for ``t.amount``) or the
real table name when no alias was used. ``Column.db`` is NOT populated.
To resolve a column to (schema, table), we build a
``{alias: (schema, real_table)}`` map from the ``Table`` AST nodes, which
DO carry both ``name`` (real table) and ``db`` (schema) after qualify.
"""

from __future__ import annotations

import functools
import logging
import re
from dataclasses import dataclass
from typing import cast

import sqlglot
from sqlglot import MappingSchema, exp
from sqlglot.errors import OptimizeError, ParseError
from sqlglot.optimizer.qualify import qualify

from moneybin.database import Database
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass, Tier

logger = logging.getLogger(__name__)

_WHITESPACE = re.compile(r"\s+")

# Aggregate functions that destroy individual values → LOW tier. Every other
# aggregate (SUM/AVG/MIN/MAX/STDDEV/VARIANCE) preserves the source class, which
# the generic "max-tier of referenced columns" path in _classify_projection
# already produces — so only the counting set needs an explicit check.
_COUNTING_AGGS: tuple[type[exp.Expr], ...] = (exp.Count,)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class SqlParseError(Exception):
    """SQL did not parse under the DuckDB dialect."""


class SqlSchemaError(Exception):
    """Query references a table/column absent from the schema snapshot."""


# ---------------------------------------------------------------------------
# Parse cache
# ---------------------------------------------------------------------------


def _normalize(sql: str) -> str:
    """Collapse whitespace so parameterised re-runs share one cache entry."""
    return _WHITESPACE.sub(" ", sql.strip())


@functools.lru_cache(maxsize=256)
def _parse_normalized(normalized_sql: str) -> exp.Expr:
    try:
        tree = sqlglot.parse_one(normalized_sql, dialect="duckdb")
    except ParseError as e:
        raise SqlParseError(str(e)) from e
    return tree


def parse_cached(sql: str) -> exp.Expr:
    """Parse ``sql`` (DuckDB dialect) with an LRU cache keyed on normalized text.

    The returned expression is shared across callers via the cache — do NOT
    mutate it in place. ``expand_star`` / ``_qualified`` already ``.copy()``
    before transforming; any new caller must do the same.
    """
    return _parse_normalized(_normalize(sql))


# ---------------------------------------------------------------------------
# Schema snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SchemaSnapshot:
    """Catalog columns for core.*/app.* plus a sqlglot MappingSchema.

    ``columns`` is the (schema, table, column) set for membership checks and
    the conservative fallback. ``mapping`` drives sqlglot star expansion and
    column qualification. ``version`` is the migration version this snapshot
    was built at — the cache key.
    """

    version: int
    columns: frozenset[tuple[str, str, str]]
    mapping: MappingSchema


def _schema_version(db: Database) -> int:
    row = db.execute(
        "SELECT COALESCE(MAX(version), 0) FROM app.schema_migrations"
    ).fetchone()
    return int(row[0]) if row else 0


@functools.lru_cache(maxsize=4)
def _build_snapshot(
    version: int, columns: frozenset[tuple[str, str, str]]
) -> SchemaSnapshot:
    # MappingSchema wants {db: {table: {column: type}}}. Types are irrelevant to
    # name-based lineage; use a uniform placeholder.
    # Build as dict[str, dict[str, dict[str, str]]] then cast to the
    # MappingSchema constructor's dict[str, object] parameter to satisfy
    # pyright's invariant dict check.
    raw: dict[str, dict[str, dict[str, str]]] = {}
    for schema, table, column in columns:
        raw.setdefault(schema, {}).setdefault(table, {})[column] = "UNKNOWN"
    nested = cast("dict[str, object]", raw)
    return SchemaSnapshot(
        version=version,
        columns=columns,
        mapping=MappingSchema(nested, dialect="duckdb"),
    )


def get_current_schema_snapshot(db: Database) -> SchemaSnapshot:
    """Return a SchemaSnapshot whose expensive MappingSchema build is cached.

    Per call this issues two cheap catalog queries (the migration version and
    the core/app column list); both are sub-millisecond on a local DuckDB and
    dwarfed by the per-call connection open. The costly part — building the
    sqlglot ``MappingSchema`` — is memoised by ``_build_snapshot`` keyed on
    (version, columns), so it runs only when the schema actually changes.
    """
    version = _schema_version(db)
    rows = db.execute(
        """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema IN ('core', 'app')
        """
    ).fetchall()
    columns = frozenset((str(s), str(t), str(c)) for s, t, c in rows)
    return _build_snapshot(version, columns)


# ---------------------------------------------------------------------------
# Table-alias map: alias → (schema, real_table)
# ---------------------------------------------------------------------------


def _build_alias_map(tree: exp.Expr) -> dict[str, tuple[str, str]]:
    """Map every table alias (or name-as-alias) to (schema, real_table).

    After ``qualify()``, Table nodes carry the real ``name`` and ``db``
    (schema); the ``alias`` field is what the query used (e.g. ``"t"``).
    When no alias was written, sqlglot sets alias = name (the table name).
    """
    alias_map: dict[str, tuple[str, str]] = {}
    for tbl in tree.find_all(exp.Table):
        schema = tbl.db  # populated by qualify for schema-qualified tables
        real_name = tbl.name
        alias = tbl.alias or real_name
        if schema and real_name:
            alias_map[alias] = (schema, real_name)
            # Also register the real name directly (covers the no-alias case
            # where qualify sets alias = name, plus direct name references).
            alias_map[real_name] = (schema, real_name)
    return alias_map


# ---------------------------------------------------------------------------
# Star expansion + input-column collection
# ---------------------------------------------------------------------------


def _qualified(tree: exp.Expr, snapshot: SchemaSnapshot) -> exp.Expr:
    """Return a copy with stars expanded and columns qualified to real tables.

    ``qualify`` resolves table aliases, expands ``SELECT *`` against the
    schema, and stamps every Column with its source table alias. We do this
    once; both expand_star and resolution consume the qualified tree.
    """
    try:
        return qualify(
            tree.copy(),
            schema=snapshot.mapping,
            dialect="duckdb",
            validate_qualify_columns=False,  # don't raise on unresolved; we fall back
        )
    except OptimizeError as e:
        raise SqlSchemaError(str(e)) from e


def expand_star(tree: exp.Expr, snapshot: SchemaSnapshot) -> exp.Expr:
    """Expand ``*`` / ``t.*`` against the schema; returns a new qualified tree."""
    return _qualified(tree, snapshot)


def _column_key(
    col: exp.Column,
    alias_map: dict[str, tuple[str, str]],
    snapshot: SchemaSnapshot,
) -> tuple[str, str, str] | None:
    """Map a qualified Column to (schema, table, column), or None if unresolved.

    After qualify(), col.table is the alias or dealiased table name.
    col.db is NOT populated. We resolve via the alias_map.
    """
    name = col.name
    table_ref = col.table  # alias or real table name
    if not table_ref:
        return None

    # Resolve alias → (schema, real_table)
    if table_ref in alias_map:
        schema, real_table = alias_map[table_ref]
        key = (schema, real_table, name)
        return key if key in snapshot.columns else None

    # No alias hit — unqualified table reference. Search the catalog for a
    # (table, column) match; the schema from snapshot.columns is authoritative.
    for actual_schema, t, c in snapshot.columns:
        if t == table_ref and c == name:
            return (actual_schema, t, c)
    return None


def collect_input_columns(
    tree: exp.Expr, snapshot: SchemaSnapshot
) -> set[tuple[str, str, str]]:
    """All (schema, table, column) tuples referenced anywhere in the query."""
    alias_map = _build_alias_map(tree)
    found: set[tuple[str, str, str]] = set()
    for col in tree.find_all(exp.Column):
        key = _column_key(col, alias_map, snapshot)
        if key is not None:
            found.add(key)
    return found


# ---------------------------------------------------------------------------
# Output-class resolution + aggregation tier rules
# ---------------------------------------------------------------------------


def _class_of_key(key: tuple[str, str, str]) -> DataClass | None:
    schema, table, column = key
    return CLASSIFICATION.get((schema, table), {}).get(column)


def _fallback_class(
    tree: exp.Expr, snapshot: SchemaSnapshot, sql_for_log: str
) -> DataClass:
    """Max-tier class among all input columns; AGGREGATE if none resolvable.

    "None resolvable" → AGGREGATE (LOW) is correct only when the query is
    restricted to classified (core/app) schemas: an unclassified-schema query
    has no input columns here to raise the floor. The caller enforces that
    restriction (see module docstring); within core/app, any query touching a
    CRITICAL column raises the fallback floor to CRITICAL.
    """
    logger.warning(
        f"sql_lineage: unresolved projection; conservative fallback. sql={sql_for_log!r}"
    )
    best: DataClass = DataClass.AGGREGATE
    for key in collect_input_columns(tree, snapshot):
        dc = _class_of_key(key)
        if dc is not None and dc.tier > best.tier:
            best = dc
    return best


def _classify_projection(
    proj: exp.Expr,
    tree: exp.Expr,
    alias_map: dict[str, tuple[str, str]],
    snapshot: SchemaSnapshot,
    sql_for_log: str,
) -> DataClass:
    inner = proj.unalias() if isinstance(proj, exp.Alias) else proj

    # Any counting aggregate in the projection → LOW (destroys individual values).
    if any(isinstance(n, _COUNTING_AGGS) for n in inner.find_all(exp.AggFunc)):
        return DataClass.AGGREGATE

    cols = list(inner.find_all(exp.Column))
    if not cols:
        # Literal / constant expression with no column refs.
        return DataClass.AGGREGATE

    classes: list[DataClass] = []
    for col in cols:
        key = _column_key(col, alias_map, snapshot)
        dc = _class_of_key(key) if key is not None else None
        if dc is None:
            return _fallback_class(tree, snapshot, sql_for_log)
        classes.append(dc)

    # Value-preserving agg or plain expression: highest-tier referenced class.
    return max(classes, key=lambda c: c.tier)


def resolve_output_classes(
    tree: exp.Expr,
    snapshot: SchemaSnapshot,
    sql_for_log: str = "",
) -> dict[str, DataClass]:
    """Map each output column name (insertion-ordered) to its DataClass."""
    select = tree.find(exp.Select)
    if select is None:
        raise SqlSchemaError("Query has no SELECT projection")
    alias_map = _build_alias_map(tree)
    out: dict[str, DataClass] = {}
    for proj in select.selects:
        name = proj.alias_or_name or "?"
        out[name] = _classify_projection(proj, tree, alias_map, snapshot, sql_for_log)
    return out


def derive_query_tier(output_classes: dict[str, DataClass]) -> Tier:
    """Max tier across all output columns; LOW for an empty projection."""
    if not output_classes:
        return Tier.LOW
    return max(c.tier for c in output_classes.values())
