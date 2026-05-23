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
import hashlib
import logging
import re
from dataclasses import dataclass
from typing import cast

import duckdb
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
    try:
        row = db.execute(
            "SELECT COALESCE(MAX(version), 0) FROM app.schema_migrations"
        ).fetchone()
        return int(row[0]) if row else 0
    except duckdb.CatalogException:
        # app.schema_migrations absent (fresh/bootstrap/test DBs) → version 0.
        # Narrow to the catalog error so a genuine query failure still surfaces.
        return 0


@functools.lru_cache(maxsize=4)
def _build_snapshot(
    version: int, ordered_columns: tuple[tuple[str, str, str], ...]
) -> SchemaSnapshot:
    # ``ordered_columns`` preserves DuckDB's definition order (column_index) so
    # sqlglot's MappingSchema expands ``SELECT *`` in the SAME order DuckDB
    # returns columns at runtime. A frozenset here (hash-bucket order) would
    # desync the two, and the privacy classification would then be matched to
    # the wrong column — see ``redact`` in ``sql_query.py`` for why alignment
    # matters. MappingSchema wants {db: {table: {column: type}}}; types are
    # irrelevant to name-based lineage, so use a uniform placeholder.
    raw: dict[str, dict[str, dict[str, str]]] = {}
    for schema, table, column in ordered_columns:
        raw.setdefault(schema, {}).setdefault(table, {})[column] = "UNKNOWN"
    nested = cast("dict[str, object]", raw)
    return SchemaSnapshot(
        version=version,
        columns=frozenset(ordered_columns),
        mapping=MappingSchema(nested, dialect="duckdb"),
    )


def get_current_schema_snapshot(db: Database) -> SchemaSnapshot:
    """Return a SchemaSnapshot whose expensive MappingSchema build is cached.

    Per call this issues two cheap catalog queries (the migration version and
    the core/app column list); both are sub-millisecond on a local DuckDB and
    dwarfed by the per-call connection open. The costly part — building the
    sqlglot ``MappingSchema`` — is memoised by ``_build_snapshot`` keyed on
    (version, ordered columns), so it runs only when the schema actually changes.

    Columns are ordered by ``column_index`` (DuckDB's definition order) so star
    expansion matches the runtime column order — see ``_build_snapshot``.
    """
    version = _schema_version(db)
    rows = db.execute(
        """
        SELECT schema_name, table_name, column_name
        FROM duckdb_columns()
        WHERE schema_name IN ('core', 'app')
        ORDER BY schema_name, table_name, column_index
        """
    ).fetchall()
    ordered_columns = tuple((str(s), str(t), str(c)) for s, t, c in rows)
    return _build_snapshot(version, ordered_columns)


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
    """Expand ``*`` / ``t.*`` against the schema; returns a new qualified tree.

    Public alias for ``_qualified`` — gives callers a stable name for the
    star-expansion step (which sqlglot folds into ``qualify``) without exposing
    the internal helper.
    """
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
    scope: exp.Expr, snapshot: SchemaSnapshot, sql_for_log: str
) -> DataClass:
    """Max-tier class among ``scope``'s input columns; AGGREGATE if none resolve.

    ``scope`` is the SELECT the unresolved projection belongs to — a single
    UNION branch, not the whole tree. Scoping per branch keeps alias resolution
    correct: a UNION can reuse one alias for different tables across branches,
    so collecting input columns tree-wide would resolve aliases against the
    wrong branch's table and miss the CRITICAL column we're falling back to
    protect.

    "None resolvable" → AGGREGATE (LOW) is correct only when the query is
    restricted to classified (core/app) schemas: an unclassified-schema query
    has no input columns here to raise the floor. The caller enforces that
    restriction (see module docstring); within core/app, any query touching a
    CRITICAL column raises the fallback floor to CRITICAL.
    """
    # Never log the raw SQL — it can carry literal PII (e.g. a description or
    # account-number filter). A short hash gives forensic correlation without
    # leaking content (No PII in logs).
    sql_hash = (
        hashlib.sha256(sql_for_log.encode()).hexdigest()[:12] if sql_for_log else "n/a"
    )
    logger.warning(
        f"sql_lineage: unresolved projection; conservative fallback (sql sha256={sql_hash})"
    )
    best: DataClass = DataClass.AGGREGATE
    for key in collect_input_columns(scope, snapshot):
        dc = _class_of_key(key)
        if dc is not None and dc.tier > best.tier:
            best = dc
    return best


def _within_subquery(node: exp.Expr, stop: exp.Expr) -> bool:
    """True if ``node`` sits inside a scalar subquery nested within ``stop``."""
    parent = node.parent
    while parent is not None and parent is not stop:
        if isinstance(parent, exp.Subquery):
            return True
        parent = parent.parent
    return False


def _within_counting_agg(node: exp.Expr, stop: exp.Expr) -> bool:
    """True if ``node`` sits inside a counting aggregate at or below ``stop``.

    A column inside ``COUNT(...)`` / ``COUNT(DISTINCT ...)`` is collapsed to a
    count — its value never surfaces. Unlike ``_within_subquery`` this checks
    ``stop`` itself, so a projection that *is* the count (``COUNT(account_id)``)
    is handled correctly.
    """
    parent = node.parent
    while parent is not None:
        if isinstance(parent, _COUNTING_AGGS):
            return True
        if parent is stop:
            break
        parent = parent.parent
    return False


def _classify_projection(
    proj: exp.Expr,
    scope: exp.Expr,
    alias_map: dict[str, tuple[str, str]],
    snapshot: SchemaSnapshot,
    sql_for_log: str,
) -> DataClass:
    # ``scope`` is the projection's own SELECT (a single UNION branch), passed
    # through to _fallback_class so alias resolution stays branch-local.
    inner = proj.unalias() if isinstance(proj, exp.Alias) else proj

    # A counting aggregate at the projection's TOP level collapses values to a
    # count — but it only governs the projection when EVERY column reference is
    # itself inside a counting aggregate (e.g. COUNT(DISTINCT account_id) → LOW).
    # A column that surfaces ALONGSIDE the count (COUNT(*) + account_id) returns
    # its value directly, so the count must NOT suppress it; fall through to
    # classify by that column. A count inside a scalar subquery
    # (`(SELECT COUNT(*) FROM t) + amount`) never governs — its tier still comes
    # from the co-referenced columns (here, `amount`).
    if any(
        isinstance(n, _COUNTING_AGGS) and not _within_subquery(n, inner)
        for n in inner.find_all(exp.AggFunc)
    ) and not any(
        not _within_counting_agg(c, inner) for c in inner.find_all(exp.Column)
    ):
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
            return _fallback_class(scope, snapshot, sql_for_log)
        classes.append(dc)

    # Value-preserving agg or plain expression: highest-tier referenced class.
    return max(classes, key=lambda c: c.tier)


def _union_select_branches(node: exp.Expr) -> list[exp.Select]:
    """Top-level SELECT branches of a (possibly nested) set operation.

    A plain SELECT returns ``[self]``. A UNION/EXCEPT/INTERSECT (all subclass
    ``exp.Union``) returns every branch's SELECT. ``tree.find(exp.Select)``
    alone would see only the first branch and let a CRITICAL column in a later
    branch leak: the result takes the first branch's column NAMES but its VALUES
    come from every branch by position, so each position must be classified
    across all branches.
    """
    if isinstance(node, exp.Union):
        return _union_select_branches(node.left) + _union_select_branches(node.right)
    if isinstance(node, exp.Select):
        return [node]
    inner = node.find(exp.Select)
    return [inner] if inner is not None else []


def resolve_output_classes(
    tree: exp.Expr,
    snapshot: SchemaSnapshot,
    sql_for_log: str = "",
) -> dict[str, DataClass]:
    """Map each output column name (insertion-ordered) to its DataClass.

    Output names come from the first branch (SQL semantics). For set operations
    each output position is classified across ALL branches and combined by max
    tier, so a CRITICAL column in any branch masks that position.
    """
    branches = _union_select_branches(tree)
    if not branches:
        raise SqlSchemaError("Query has no SELECT projection")
    # Alias scope is per-branch: a UNION may reuse one alias for different
    # tables across branches (legal SQL), so a tree-wide map (last-write-wins)
    # would resolve a branch's column against the wrong table and under-redact.
    per_branch: list[list[DataClass]] = [
        [
            _classify_projection(
                proj, sel, _build_alias_map(sel), snapshot, sql_for_log
            )
            for proj in sel.selects
        ]
        for sel in branches
    ]
    out: dict[str, DataClass] = {}
    for i, proj in enumerate(branches[0].selects):
        # Unaliased expressions (e.g. MIN(account_id)) yield "" from
        # alias_or_name; a positional suffix keeps each one a distinct key so
        # two unnamed projections don't collide (the second overwriting the
        # first would drop a class and weaken sql_query's position-aligned
        # fallback). The suffix preserves the projection's positional order.
        name = proj.alias_or_name or f"?_{i}"
        candidates = [b[i] for b in per_branch if i < len(b)]
        out[name] = (
            max(candidates, key=lambda c: c.tier) if candidates else DataClass.AGGREGATE
        )
    return out


def is_data_query(tree: exp.Expr) -> bool:
    """True for row-returning queries (SELECT / set operations).

    False for DESCRIBE / SHOW / PRAGMA / EXPLAIN, whose output is schema or
    plan text, not classified row data — callers route those past the lineage
    gate and treat them as LOW.
    """
    return isinstance(tree, (exp.Select, exp.Union))


def tables_outside_schemas(
    tree: exp.Expr, snapshot: SchemaSnapshot, allowed: frozenset[str]
) -> list[str]:
    """Return table references that resolve to a schema outside ``allowed``.

    Schema-qualified tables are checked directly; unqualified tables are
    resolved by name against the snapshot. CTE references (names bound by a
    ``WITH`` clause) are not real tables and are skipped. Anything resolving to
    a disallowed schema — or to no allowed schema at all — is returned so the
    caller can refuse the query before any masking decision. This is what makes
    the masking guarantee sound: every queryable column lives in a classified
    schema.
    """
    known_by_name: dict[str, set[str]] = {}
    for schema, table, _col in snapshot.columns:
        known_by_name.setdefault(table, set()).add(schema)
    cte_names = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    bad: list[str] = []
    for tbl in tree.find_all(exp.Table):
        schema = tbl.db
        name = tbl.name
        if not schema and name in cte_names:
            continue
        if schema:
            if schema not in allowed:
                bad.append(f"{schema}.{name}")
        elif not (known_by_name.get(name, set()) & allowed):
            bad.append(name)
    return bad


def derive_query_tier(output_classes: dict[str, DataClass]) -> Tier:
    """Max tier across all output columns; LOW for an empty projection."""
    if not output_classes:
        return Tier.LOW
    return max(c.tier for c in output_classes.values())
