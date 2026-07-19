"""sqlglot column-lineage resolution for the dynamic SQL surface.

``redact_typed`` masks typed payloads by walking ``Annotated[T, DataClass]``
metadata. ``sql_query`` has no static return type — its shape is the caller's
query. This module recovers the same per-column ``DataClass`` mapping by
parsing the SQL, expanding ``*`` against the live schema, and resolving each
output column to a class via the ``CLASSIFICATION`` registry (and, for
``reports`` columns, each report's declared ``@report(classes=…)`` map). The
result feeds ``redact_records`` (CRITICAL masking today; whatever
``_TRANSFORMS`` does tomorrow) and sets the per-call envelope sensitivity.

Fail-closed: anything we cannot resolve is treated as the most sensitive
class the query could touch (max tier over all input columns), so we
over-redact rather than leak.

Scope: this module classifies columns in the ``core``, ``app``, and
``reports`` schemas. ``core``/``app`` resolve via the ``CLASSIFICATION``
registry; ``reports`` resolves via each report's declared
``@report(classes=…)`` map (ADR-013), because SQLMesh deploys report views
as ``SELECT *`` pointers lineage cannot classify. Both sources are
completeness-tested, so every deployed column in these schemas is declared
and resolves. A query that references any other schema (``raw``/``prep``/
``meta``) yields no classifiable input columns, so an unresolvable
projection falls back to ``AGGREGATE`` (LOW) rather than CRITICAL. Callers
(the ``sql_query`` wiring) MUST restrict the query to the allowlisted
schemas before relying on this module's masking — see the table-allowlist
gate in the tool layer.

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
from dataclasses import dataclass, replace
from typing import cast

import duckdb
import sqlglot
from sqlglot import MappingSchema, exp
from sqlglot.errors import OptimizeError, ParseError
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, build_scope

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


class UnresolvedProjectionError(Exception):
    """A projection needed the conservative fallback under ``strict=True``.

    Only the build-time report-class deriver sets ``strict``; the runtime
    ``sql_query`` path always prefers a conservative answer to a refusal.
    """


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
    """Catalog columns for core.*/app.*/reports.* plus a sqlglot MappingSchema.

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


# Sentinel version for snapshots not built from a live database (the build-time
# report-class deriver). Negative so it can never collide with a real
# migration version, which is always >= 0 (see _schema_version).
_SYNTHETIC_SNAPSHOT_VERSION = -1


def snapshot_from_columns(
    ordered_columns: tuple[tuple[str, str, str], ...],
) -> SchemaSnapshot:
    """Build a snapshot from an explicit column list, with no database.

    ``get_current_schema_snapshot`` is the live-catalog counterpart, used by
    the ``sql_query`` runtime path. This one backs the connectionless
    build-time report-class deriver (``report_class_derivation.py``), whose
    caller has no migration version to key the cache on — ``_build_snapshot``
    is keyed on ``(version, ordered_columns)``, so this passes the reserved
    negative sentinel instead.

    Column order drives star expansion (see ``_build_snapshot``), so callers
    that rely on ``SELECT *`` must preserve definition order; the build-time
    deriver sidesteps this entirely by rejecting ``SELECT *`` outright.
    """
    return _build_snapshot(_SYNTHETIC_SNAPSHOT_VERSION, ordered_columns)


def get_current_schema_snapshot(db: Database) -> SchemaSnapshot:
    """Return a SchemaSnapshot whose expensive MappingSchema build is cached.

    Per call this issues two cheap catalog queries (the migration version and
    the core/app/reports column list); both are sub-millisecond on a local
    DuckDB and dwarfed by the per-call connection open. The costly part —
    building the sqlglot ``MappingSchema`` — is memoised by ``_build_snapshot``
    keyed on (version, ordered columns), so it runs only when the schema
    actually changes.

    Columns are ordered by ``column_index`` (DuckDB's definition order) so star
    expansion matches the runtime column order — see ``_build_snapshot``.
    """
    version = _schema_version(db)
    rows = db.execute(
        """
        SELECT schema_name, table_name, column_name
        FROM duckdb_columns()
        WHERE schema_name IN ('core', 'app', 'reports')
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


def _scope_source_names(tree: exp.Expr) -> frozenset[str]:
    """Every name bound by a CTE or a derived-table alias anywhere in ``tree``.

    These names SHADOW a same-named base table — that is plain SQL semantics,
    not a security rule: given ``WITH dim_accounts AS (…) SELECT … FROM
    dim_accounts``, the reference is the CTE and the catalog table is invisible.
    ``_column_key`` uses this to refuse the catalog for such a reference.

    Deliberately whole-query rather than per-scope: a name bound anywhere is at
    worst declined here and answered by ``_conservative_floor``, so a too-wide
    set over-redacts, while a too-narrow one under-redacts.
    """
    names = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    names |= {sub.alias for sub in tree.find_all(exp.Subquery)}
    return frozenset(n for n in names if n)


def _column_key(
    col: exp.Column,
    alias_map: dict[str, tuple[str, str]],
    snapshot: SchemaSnapshot,
    shadowed: frozenset[str] = frozenset(),
) -> tuple[str, str, str] | None:
    """Map a qualified Column to (schema, table, column), or None if unresolved.

    After qualify(), col.table is the alias or dealiased table name.
    col.db is NOT populated. We resolve via the alias_map.

    ``shadowed`` names CTEs / derived tables (see ``_scope_source_names``); a
    column qualified by one of them NEVER resolves against the catalog. Both
    lookups below would otherwise produce a false-positive catalog hit for a
    CTE named after a real table: ``_build_alias_map`` walks ``Table`` nodes
    *inside* CTE bodies, so ``core.dim_accounts`` self-registers under the bare
    key ``dim_accounts``, and the bare-name scan matches any CTE named like a
    catalog table. Callers on the *resolution* path must pass it;
    ``collect_input_columns`` deliberately does not (see its docstring).
    """
    name = col.name
    table_ref = col.table  # alias or real table name
    if not table_ref:
        return None
    if table_ref in shadowed:
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
    """All (schema, table, column) tuples referenced anywhere in the query.

    Intentionally does NOT pass ``shadowed`` to ``_column_key``. Its only
    consumer is ``_scope_input_max``, which computes a conservative *floor* —
    there, resolving a CTE-qualified column to a same-named catalog table can
    only ADD a class to the max, never remove one, so the shadowing imprecision
    is safe and the extra reach is desirable. On the resolution path the same
    imprecision is a leak, which is why ``_resolve_projection`` does pass it.
    """
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
    dc = CLASSIFICATION.get((schema, table), {}).get(column)
    if dc is not None:
        return dc
    # reports.* columns are declared on the @report runner, not in
    # CLASSIFICATION — SQLMesh deploys the view as a `SELECT *` pointer that
    # lineage can't classify (ADR-013).
    if schema == "reports":
        return reports_class_map().get((schema, table), {}).get(column)
    return None


# THE fail-closed class for this module, and for sql_query's runtime
# name-mismatch fallback. One class, one meaning: "a value reached the user
# without lineage establishing what it holds." Two paths reach it — a deployed
# catalog column with no declaration (a registry gap, not a query problem), and
# a runtime column no projection resolved to — and both want the same outcome,
# so they share the constant rather than growing a second fail-closed pattern
# beside it. UNRESOLVED masks WHOLE; see its taxonomy comment for why the
# partial ACCOUNT_IDENTIFIER mask is the wrong tool here.
FAIL_CLOSED_CLASS = DataClass.UNRESOLVED


def _coverage_gap_class(key: tuple[str, str, str], sql_for_log: str) -> DataClass:
    schema, table, column = key
    sql_hash = (
        hashlib.sha256(sql_for_log.encode()).hexdigest()[:12] if sql_for_log else "n/a"
    )
    # schema/table/column are identifiers, not data — safe to log (No PII in logs).
    logger.warning(
        f"sql_lineage: undeclared deployed column {schema}.{table}.{column}; "
        f"failing closed (sql sha256={sql_hash})"
    )
    return FAIL_CLOSED_CLASS


def _combined_class(classes: list[DataClass]) -> DataClass:
    """The single class describing a value drawn from ALL of ``classes``.

    Max-by-tier, with one exception: **two DIFFERENT CRITICAL classes have no
    representative.** Their transforms are not interchangeable — ROUTING_NUMBER
    masks WHOLE while INSTITUTION_ACCOUNT_NUMBER masks PARTIALLY (``"****" +
    value[-4:]``) — so standing one in for the other publishes the last four
    characters of a value it does not describe. A bare ``max`` picks the first
    maximal element, making mask STRENGTH depend on source order: ``last_four ||
    routing_number`` returned ``****0021`` (the real routing number's last four)
    while ``routing_number || last_four`` correctly returned ``*****``. The same
    order-dependence appeared across a UNION, where one output position draws
    values from every branch. Disagreement at CRITICAL therefore collapses to
    the whole-masking ``FAIL_CLOSED_CLASS``.

    A UNANIMOUS CRITICAL class is kept: it genuinely describes every value in
    the position, so collapsing it would discard a correct, more specific answer
    (and turn ``SELECT last_four`` into a whole mask).

    Below CRITICAL every transform is passthrough, so the class is pure
    reporting and the identified max is the more informative answer. Collapsing
    there would also inflate a HIGH bound to CRITICAL — the over-classification
    this module must not introduce.

    ``classes`` must be non-empty; every caller already guards that.
    """
    best = max(classes, key=lambda c: c.tier)
    if best.tier is not Tier.CRITICAL:
        return best
    at_top = {c for c in classes if c.tier is Tier.CRITICAL}
    return best if len(at_top) == 1 else FAIL_CLOSED_CLASS


def _scope_input_max(
    select: exp.Expr, snapshot: SchemaSnapshot, sql_for_log: str
) -> DataClass:
    """Max-tier class among ``select``'s input columns; AGGREGATE if none resolve.

    Alias resolution is local to ``select``: ``collect_input_columns`` builds the
    alias map from this subtree alone. That locality is the point — a UNION can
    reuse one alias for different tables across branches, so a single tree-wide
    alias map (last-write-wins) would resolve a branch's column against the
    wrong branch's table and silently drop the CRITICAL column we are falling
    back to protect. ``_conservative_floor`` restores whole-query coverage by
    calling this once per scope instead of widening the map.

    An undeclared deployed column among the inputs raises the floor to CRITICAL
    (via ``_coverage_gap_class``) rather than being silently skipped: the
    registry is incomplete, so nothing in this scope can be trusted LOW.

    Widening note: ``collect_input_columns`` gathers every column referenced
    anywhere in ``select`` — including a JOIN condition or WHERE clause — not
    just the projection being classified, so a query joining an undeclared view
    raises this floor even when only a declared column is projected. This is
    intentional (coherent with the max-tier-over-inputs design), not a bug.
    """
    best: DataClass = DataClass.AGGREGATE
    for key in collect_input_columns(select, snapshot):
        dc = _class_of_key(key) or _coverage_gap_class(key, sql_for_log)
        if dc.tier > best.tier:
            best = dc
    return best


def _tables_in_scope(tree: exp.Expr, snapshot: SchemaSnapshot) -> set[tuple[str, str]]:
    """Every classified ``(schema, table)`` ``tree`` reads from, however it reads it.

    Resolution mirrors ``tables_outside_schemas``: schema-qualified tables are
    taken directly, bare names are resolved against the snapshot (and a name
    matching in two schemas contributes both — over-inclusion only raises a
    floor, never lowers one).

    CTE names are deliberately NOT filtered out here. A CTE named after a
    catalog table contributes that table's columns to the floor, which
    over-redacts a query that never touched the real table. That is the safe
    direction, and it is the same trade ``collect_input_columns`` already makes
    by not passing ``shadowed`` — see its docstring.
    """
    known_by_name: dict[str, set[str]] = {}
    for schema, table, _col in snapshot.columns:
        known_by_name.setdefault(table, set()).add(schema)
    found: set[tuple[str, str]] = set()
    for tbl in tree.find_all(exp.Table):
        if tbl.db:
            found.add((tbl.db, tbl.name))
        else:
            found |= {(s, tbl.name) for s in known_by_name.get(tbl.name, set())}
    return found


def _table_scope_max(
    tree: exp.Expr, snapshot: SchemaSnapshot, sql_for_log: str
) -> DataClass:
    """Max-tier class over EVERY column of every classified table ``tree`` reads.

    The column-reference floor (``_scope_input_max``) can only see columns the
    query NAMES. A projection that names none — ``SELECT dim_accounts FROM
    core.dim_accounts`` (DuckDB's whole-row pseudo-column), ``SELECT
    UNNEST(dim_accounts) …``, or a ``*`` over a ``SUMMARIZE`` — still returns
    every column of the table, so a floor built from named columns alone floors
    at AGGREGATE (LOW) and hands back ``routing_number`` in the clear.

    Reading a table is therefore treated as putting ALL of its columns in reach,
    which is exactly what those constructs do. This over-redacts an unresolvable
    projection over a table whose sensitive columns it never touched; that cost
    is accepted, because the alternative is trusting a decomposition we already
    know we could not perform.
    """
    columns_by_table: dict[tuple[str, str], set[str]] = {}
    for schema, table, col in snapshot.columns:
        columns_by_table.setdefault((schema, table), set()).add(col)
    best: DataClass = DataClass.AGGREGATE
    # Sorted, not set-ordered: a first-past-the-post max over a set reports a
    # different class run to run whenever a table carries two classes at its top
    # tier. `classes_returned` is user- and audit-visible and must be
    # reproducible.
    for schema, table in sorted(_tables_in_scope(tree, snapshot)):
        for col in sorted(columns_by_table.get((schema, table), set())):
            key = (schema, table, col)
            # One warning per undeclared table, not per undeclared column: the
            # gap is a property of the registry entry, and the first hit already
            # pins the floor at its ceiling.
            dc = _class_of_key(key)
            if dc is None:
                return _coverage_gap_class(key, sql_for_log)
            if dc.tier > best.tier:
                best = dc
    # A table-level bound names a TIER, never a column. At CRITICAL that
    # distinction is load-bearing, so the bound is reported as UNRESOLVED rather
    # than as whichever CRITICAL column sorted first:
    #
    #   * The CRITICAL transforms are not interchangeable. ACCOUNT_IDENTIFIER
    #     masks PARTIALLY (``"****" + value[-4:]``), which for a value we could
    #     not identify would publish its last four characters — and on the
    #     whole-row STRUCT that ``SELECT dim_accounts FROM core.dim_accounts``
    #     returns, it raises rather than masking at all.
    #   * Reporting `institution_account_number` for a whole-row struct claims a
    #     precision this path does not have.
    #
    # Below CRITICAL every transform is passthrough today, so the class is pure
    # reporting and the identified max is the more informative answer. Reporting
    # UNRESOLVED there would also inflate a HIGH bound to CRITICAL, which is the
    # over-classification this floor must not introduce.
    return FAIL_CLOSED_CLASS if best.tier is Tier.CRITICAL else best


def _conservative_floor(
    tree: exp.Expr, snapshot: SchemaSnapshot, sql_for_log: str
) -> DataClass:
    """The tier an unresolved projection takes: max over EVERY scope in ``tree``.

    **THE INVARIANT, stated once:** a projection is classified LOW only when we
    positively established what it is. Anything unresolved, opaque, or
    unexpanded takes a floor computed from the classified TABLES in scope — not
    from whichever columns happened to resolve, and never from a local scope
    that contains no catalog column at all.

    Two independent floors are combined, because each covers a blind spot of the
    other:

    * **Per-scope column max** (``_scope_input_max`` over every SELECT in the
      tree). Precise where the query names its columns. Walking every SELECT —
      each with its own local alias map, so per-branch alias correctness
      survives — is what keeps a CTE body (``SELECT v FROM c15``) or a
      CTE-only UNION branch from flooring at AGGREGATE and propagating that LOW
      outward as a real answer. That was the depth-exhaustion leak: a deep CTE
      chain over ``routing_number`` returned it unmasked.
    * **Table-scope max** (``_table_scope_max``). Covers the projections that
      name NO column at all — the whole-row pseudo-column and the
      ``PIVOT``/``UNPIVOT``/``SUMMARIZE``/``COLUMNS(…)`` family — where the
      column floor legitimately finds nothing and therefore says AGGREGATE.
      That was the second leak, through the same door.

    "None resolvable" → AGGREGATE (LOW) now means something much narrower than
    it used to: the query reads no classified table at all (e.g. a projection
    over a ``VALUES`` list, whose values are the caller's own literals rather
    than database data). It remains correct only because the caller restricts
    the query to the classified schemas (core/app via CLASSIFICATION, reports
    via declared @report class maps) — see the module docstring.
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
    for select in tree.find_all(exp.Select):
        dc = _scope_input_max(select, snapshot, sql_for_log)
        if dc.tier > best.tier:
            best = dc
    table_dc = _table_scope_max(tree, snapshot, sql_for_log)
    floor = table_dc if table_dc.tier > best.tier else best
    # A floor is a BOUND over what the query could touch — never a statement
    # about the projection, which is by definition unresolved on this path. At
    # CRITICAL that distinction leaks, so ANY critical floor collapses to the
    # whole-masking FAIL_CLOSED_CLASS (the same rule, for the same reason, that
    # `_table_scope_max` applies to its own bound):
    #
    #   * `best` is the max over every column the query NAMES ANYWHERE —
    #     `_scope_input_max` deliberately scans WHERE and JOIN predicates too —
    #     so it can name a class describing a completely different value than
    #     the one projected. Adding an unrelated `WHERE last_four IS NOT NULL`
    #     to a whole-row `(dim_accounts).routing_number` projection used to flip
    #     it from `*****` to `****0021`: an equal-CRITICAL tie returned
    #     INSTITUTION_ACCOUNT_NUMBER, whose PARTIAL mask published four digits
    #     of the real routing number.
    #   * Naming any specific CRITICAL class here claims a precision this path
    #     does not have, and CRITICAL transforms are not interchangeable.
    #
    # Below CRITICAL every transform is passthrough, so the class is pure
    # reporting: the more specific of the two floors is kept, and reporting
    # UNRESOLVED there would inflate a HIGH bound to CRITICAL.
    return FAIL_CLOSED_CLASS if floor.tier is Tier.CRITICAL else floor


def _within_subquery(node: exp.Expr, stop: exp.Expr) -> bool:
    """True if ``node`` sits inside a scalar subquery nested within ``stop``.

    ``node is stop`` is False by definition — nothing lies strictly between a
    node and itself. The guard is load-bearing, not defensive: the walk starts
    at ``node.parent``, so without it a projection that IS the aggregate
    (``COUNT(*)`` as the whole projection) walks straight PAST ``stop`` and
    keeps climbing. Inside a derived table it then reaches the enclosing
    ``exp.Subquery`` and answers True, suppressing the counting-aggregate rule
    for ``SELECT n FROM (SELECT COUNT(*) AS n FROM …) sub``. That misfire used
    to be invisible because the projection fell through to the permissive
    "no exp.Column → AGGREGATE" branch and landed on the right answer for the
    wrong reason; once that branch learned to decline on an unexpanded ``Star``
    (``COUNT(*)`` holds one), the misfire became a visible over-classification.
    """
    if node is stop:
        return False
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


# Depth bound for CTE / derived-table recursion. The deepest reports model
# (recurring_subscriptions) chains three levels; 16 leaves ample headroom while
# guaranteeing termination on a pathological nesting.
#
# Exhausting it makes the column UNRESOLVED, which is only safe because the
# unresolved answer is produced at the outermost projection by
# ``_conservative_floor`` — never by the local CTE body, which typically holds
# no catalog column and would floor at AGGREGATE (LOW). A previous revision of
# this comment claimed exhaustion "is not a leak" while the code floored
# locally; it was a leak (a 17-deep chain over ``routing_number`` returned it in
# the clear). Do not reintroduce a local floor for the nested path.
_MAX_SCOPE_DEPTH = 16


@dataclass(frozen=True)
class _ResolveCtx:
    """Per-call resolution state threaded through nested-scope recursion.

    ``seen`` holds ``id()`` of the scopes already entered on this path, which is
    what makes a self-referencing (``WITH RECURSIVE``) CTE terminate instead of
    looping. Every scope is kept alive by the root scope for the duration of the
    call, so the ids cannot be recycled underneath us.

    ``tree`` is the WHOLE query, kept so an unresolved projection can floor
    against every scope in it rather than against whatever local scope the
    recursion happened to bottom out in — see ``_conservative_floor``.
    ``strict`` turns that fallback into an error for the build-time deriver.

    ``shadowed`` is every CTE / derived-table name bound anywhere in ``tree``
    (see ``_scope_source_names``). Computed once per call and carried unchanged
    through the recursion, because a name bound by the outer query still shadows
    the catalog inside a nested scope.
    """

    snapshot: SchemaSnapshot
    sql_for_log: str
    depth: int
    seen: frozenset[int]
    tree: exp.Expr
    strict: bool
    shadowed: frozenset[str]


def _output_index(source: Scope, name: str) -> int | None:
    """Position of output column ``name`` within nested scope ``source``.

    Resolution is positional rather than by-name because a set operation takes
    its output NAMES from the first branch while drawing VALUES from every
    branch. Matching later branches by name would miss the branch that supplies
    a CRITICAL value under a different local alias, and under-redact.

    Unwraps ``exp.Union`` only — deliberately narrower than
    ``_union_select_branches``, which must handle ``exp.SetOperation`` because
    nothing above it can supply a fallback. Here an ``EXCEPT`` / ``INTERSECT``
    scope falls out of the loop as a non-Select and returns None, which the
    caller turns into the conservative floor. That is safe (a floor is never
    lower than the per-position answer), so widening this to ``SetOperation``
    would only trade over-redaction for precision — not a correctness fix, and
    it is not made here. Do NOT widen it without re-checking that
    ``_class_at_index`` still refuses to average away the right operand.
    """
    select = source.expression
    while isinstance(select, exp.Union):
        select = select.left
    if not isinstance(select, exp.Select):
        return None
    for i, proj in enumerate(select.selects):
        if proj.alias_or_name == name:
            return i
    return None


def _class_at_index(source: Scope, index: int, ctx: _ResolveCtx) -> DataClass | None:
    """Class of nested scope ``source``'s output column at ``index``.

    Returns None when the position cannot be resolved (depth exhausted, cycle,
    unexpanded star, or a shape we don't model) so the caller applies its own
    conservative fallback rather than inventing a permissive answer here.
    """
    if ctx.depth <= 0 or id(source) in ctx.seen:
        return None
    inner_ctx = replace(ctx, depth=ctx.depth - 1, seen=ctx.seen | {id(source)})
    if source.union_scopes:
        # ALL-OR-NOTHING. A set operation draws values from every branch, so a
        # branch we cannot resolve may be the one supplying the CRITICAL value.
        # Taking max() over only the branches that happened to resolve silently
        # drops it — a UNION of `category` (LOW) with an unresolvable deep chain
        # ending in `routing_number` returned CATEGORY/LOW, unmasked. Declining
        # outright hands the position to the caller's conservative floor, which
        # is the rule the top-level union path in resolve_output_classes already
        # follows.
        found: list[DataClass] = []
        for branch in source.union_scopes:
            dc = _class_at_index(branch, index, inner_ctx)
            if dc is None:
                return None
            found.append(dc)
        # `_combined_class`, not `max`: this position draws values from EVERY
        # branch, so two branches carrying different CRITICAL classes leave it
        # with no representative — see that helper.
        return _combined_class(found) if found else None
    select = source.expression
    if not isinstance(select, exp.Select) or index >= len(select.selects):
        return None
    return _resolve_projection(
        select.selects[index], source, inner_ctx, _build_alias_map(select)
    )


def _source_scope_of(col: exp.Column, scope: Scope | None) -> Scope | None:
    """The CTE / derived-table Scope ``col`` reads from, or None if it reads a table.

    Split out from the resolution step (``_class_in_source_scope``) to make one
    invariant expressible by the caller:

        **A column whose table reference names a CTE or derived table is NEVER
        resolved against the catalog. Its class comes from inside that scope,
        or from the conservative floor — never from a same-named catalog
        table.**

    This is ordinary SQL semantics before it is a security rule: a CTE shadows a
    same-named base table, so resolving ``dim_accounts.account_type`` against
    ``core.dim_accounts`` is simply the wrong answer. It is *also* a leak, and
    was the one this split closes: when resolution inside the scope declined
    (depth exhausted), control used to fall through to ``_column_key``, which
    resolved the CTE name to a catalog row and returned that permissive class as
    a confident answer — so the decline never reached ``_conservative_floor``.
    Same root cause as the depth-exhaustion leak, through a different door.
    Returning the Scope here lets the caller distinguish "not a scope source"
    (fall through to the catalog) from "scope source we could not resolve"
    (decline, and let the floor answer).

    A CTE reference parses with ``db=''``, so ``_build_alias_map`` never
    registers it; resolving to the matching projection INSIDE the source is what
    keeps each output column's class its own instead of collapsing to "max tier
    over every column in the query".
    """
    if scope is None:
        return None
    if col.table:
        source = scope.sources.get(col.table)
    else:
        # Unqualified column in a single-source SELECT: SQL semantics leave no
        # ambiguity about where it came from. Not a convenience — the build-time
        # deriver classifies model source WITHOUT running qualify(), so the
        # outer SELECT of a CTE-backed model (merchant_activity,
        # recurring_subscriptions) reaches here with col.table == "".
        #
        # ``selected_sources``, NOT ``sources``: the latter holds every CTE bound
        # by the WITH clause whether or not this SELECT reads it, so a
        # three-CTE model would look multi-source and never resolve. With two or
        # more genuinely selected sources the reference IS ambiguous, so we
        # decline and let the conservative fallback answer rather than guess.
        selected = scope.selected_sources
        if len(selected) != 1:
            return None
        source = next(iter(selected.values()))[1]
    # A real exp.Table (or nothing) — the alias-map path handles it.
    return source if isinstance(source, Scope) else None


def _class_in_source_scope(
    col: exp.Column, source: Scope, ctx: _ResolveCtx
) -> DataClass | None:
    """Class of ``col`` as produced by the CTE / derived-table scope it reads from.

    None means "this scope could not answer" — never "look somewhere else". See
    ``_source_scope_of`` for the invariant that makes the difference load-bearing.
    """
    index = _output_index(source, col.name)
    if index is None:
        return None
    return _class_at_index(source, index, ctx)


def _subquery_scopes_by_select(scope: Scope) -> dict[int, Scope]:
    """``{id(SELECT expression): Scope}`` for every subquery nested under ``scope``.

    Lets a column that physically sits inside a scalar / ``IN`` subquery resolve
    against the subquery's OWN scope. Resolving it against the enclosing scope
    instead is what made ``reports.large_transactions.is_top_100`` unresolvable:
    its ``t.transaction_id IN (SELECT transaction_id FROM top_n)`` names a column
    that only the inner scope can see, while the outer scope has three selected
    sources and so declines as ambiguous.
    """
    out: dict[int, Scope] = {}
    stack = list(scope.subquery_scopes)
    while stack:
        s = stack.pop()
        out[id(s.expression)] = s
        stack.extend(s.subquery_scopes)
        stack.extend(s.union_scopes)
    return out


def _scope_of_column(
    col: exp.Column,
    proj_root: exp.Expr,
    scope: Scope | None,
    subscopes: dict[int, Scope],
) -> Scope | None:
    """The scope ``col`` belongs to: its nearest enclosing SELECT within ``proj_root``.

    Falls back to the projection's own scope when the column is not nested in a
    subquery, or when that subquery has no scope we can name.
    """
    node = col.parent
    while node is not None and node is not proj_root:
        if isinstance(node, exp.Select):
            return subscopes.get(id(node), scope)
        node = node.parent
    return scope


# Node types that stand in for "some set of columns we cannot enumerate".
# Neither carries an ``exp.Column`` child, so both used to reach the
# literal-expression branch of ``_resolve_projection`` and be answered
# AGGREGATE (LOW) with full confidence:
#
#   * ``exp.Columns`` — DuckDB's ``COLUMNS('regex')`` / ``COLUMNS(c -> …)``.
#     Parses as an opaque call over a string literal or lambda; sqlglot models
#     the ARGUMENT, never the columns it selects. ``COLUMNS('.*') FROM
#     core.dim_accounts`` expands to all 19 columns at runtime.
#   * ``exp.Star`` — a ``*`` that survived ``qualify()``. ``qualify`` expands
#     ``*`` against the schema snapshot, but it cannot expand one over a
#     ``PIVOT`` / ``UNPIVOT`` / ``SUMMARIZE`` source, whose output columns are
#     computed by DuckDB at execution time and are absent from the catalog. A
#     surviving Star means expansion FAILED — the loudest possible signal that
#     we do not know what this projection returns.
#
# ``COUNT(*)`` also holds a Star but never reaches here: the counting-aggregate
# branch above returns AGGREGATE first, which is correct — a count destroys the
# values whatever the star covered.
_OPAQUE_PROJECTION_NODES: tuple[type[exp.Expr], ...] = (exp.Star, exp.Columns)


def _is_opaque(inner: exp.Expr) -> bool:
    """True if ``inner`` stands for columns we cannot enumerate."""
    return isinstance(inner, _OPAQUE_PROJECTION_NODES) or any(
        True for _ in inner.find_all(*_OPAQUE_PROJECTION_NODES)
    )


def _resolve_projection(
    proj: exp.Expr,
    scope: Scope | None,
    ctx: _ResolveCtx,
    alias_map: dict[str, tuple[str, str]],
) -> DataClass | None:
    """Class of ``proj``, or None when any part of it cannot be resolved.

    Declining (rather than answering with a local conservative floor) is what
    keeps the nested path safe: only the caller at the OUTERMOST projection —
    which owns the ``WITH`` subtree and therefore a scope containing catalog
    columns — is allowed to convert "unresolved" into a tier. See
    ``_conservative_floor``.

    ``scope`` is the projection's sqlglot Scope, which resolves CTE and
    derived-table sources; it is None only when scope analysis failed, in which
    case resolution degrades to the alias-map-only behaviour.
    """
    snapshot = ctx.snapshot
    sql_for_log = ctx.sql_for_log
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
        # THE INVARIANT: a projection is classified LOW only when we positively
        # established what it is. "No exp.Column node" is NOT that proof — it
        # conflates a genuine literal with an expression we could not
        # decompose, and the two must not share an answer. An opaque construct
        # declines here so the caller's conservative floor applies.
        if _is_opaque(inner):
            return None
        # Genuine literal / constant expression with no column refs.
        return DataClass.AGGREGATE

    # Built only when the projection actually nests a SELECT (a scalar or IN
    # subquery); the common case pays nothing.
    subscopes: dict[int, Scope] = (
        _subquery_scopes_by_select(scope)
        if scope is not None and inner.find(exp.Select) is not None
        else {}
    )

    classes: list[DataClass] = []
    for col in cols:
        col_scope = _scope_of_column(col, inner, scope, subscopes)
        source_scope = _source_scope_of(col, col_scope)
        if source_scope is not None:
            # INVARIANT: a CTE / derived-table column resolves INSIDE that
            # source or not at all — never against the catalog. Declining here
            # (rather than falling through to _column_key) is the whole point:
            # the catalog holds a same-named table whose class is unrelated, and
            # answering from it converts this decline into a confident, wrong,
            # usually more permissive class. Fall to the conservative floor.
            dc_scope = _class_in_source_scope(col, source_scope, ctx)
            if dc_scope is None:
                return None
            classes.append(dc_scope)
            continue
        key = _column_key(col, alias_map, snapshot, ctx.shadowed)
        if key is None:
            return None  # unresolvable — the outermost caller supplies the floor
        dc = _class_of_key(key)
        if dc is None:
            return _coverage_gap_class(key, sql_for_log)
        classes.append(dc)

    # Value-preserving agg or plain expression: highest-tier referenced class —
    # via `_combined_class`, because the projected value is derived from ALL of
    # these columns, and two different CRITICAL classes cannot stand in for each
    # other (`last_four || routing_number` must not take the partial mask).
    return _combined_class(classes)


def _classify_projection(
    proj: exp.Expr,
    scope: Scope | None,
    ctx: _ResolveCtx,
    alias_map: dict[str, tuple[str, str]],
) -> DataClass:
    """Outermost entry point: always answers, converting "unresolved" to a tier.

    This is the ONLY place allowed to make that conversion — ``proj`` belongs to
    a top-level branch, so ``ctx.tree`` spans every catalog column the query can
    touch.
    """
    dc = _resolve_projection(proj, scope, ctx, alias_map)
    if dc is not None:
        return dc
    if ctx.strict:
        # The alias is a SQL identifier, not row data — safe to surface.
        raise UnresolvedProjectionError(
            f"projection {proj.alias_or_name or '<unnamed>'!r} is not resolvable "
            "from source; strict mode forbids the conservative fallback"
        )
    return _conservative_floor(ctx.tree, ctx.snapshot, ctx.sql_for_log)


def _union_select_branches(node: exp.Expr) -> list[exp.Select]:
    """Top-level SELECT branches a set operation can draw output VALUES from.

    A plain SELECT returns ``[self]``. The set operations split by value
    provenance, NOT by a shared base class — on sqlglot 30.8.0 ``exp.Except``
    and ``exp.Intersect`` do NOT subclass ``exp.Union``; all three subclass
    ``exp.SetOperation``. (An earlier revision of this docstring asserted the
    common-``Union`` hierarchy; it is false, and dispatching on ``exp.Union``
    alone silently dropped branches — see below.)

    - ``UNION`` / ``UNION ALL`` draw values from BOTH branches, so both are
      returned and each output position is classified across all of them. The
      result takes the first branch's column NAMES but its VALUES come from
      every branch by position, so classifying only the first would let a
      CRITICAL column in a later branch leak.
    - ``EXCEPT`` / ``INTERSECT`` emit rows drawn from the LEFT branch only
      (the right operand filters, it does not contribute values), so only the
      left branch is returned.

    Recursing on ``node.left`` also matters for correctness, not just clarity:
    ``tree.find(exp.Select)`` walks breadth-first, so for
    ``(A UNION B) EXCEPT C`` it returns C — the wrong branch entirely, taking
    output names and classes from the operand that contributes no values while
    A and B go unclassified.
    """
    if isinstance(node, exp.Union):
        return _union_select_branches(node.left) + _union_select_branches(node.right)
    if isinstance(node, exp.SetOperation):  # EXCEPT / INTERSECT — left values only
        return _union_select_branches(node.left)
    if isinstance(node, exp.Select):
        return [node]
    if isinstance(node, exp.Subquery):  # parenthesised operand, e.g. (A UNION B)
        return _union_select_branches(node.this)
    inner = node.find(exp.Select)
    return [inner] if inner is not None else []


def _branch_scopes(tree: exp.Expr, branches: list[exp.Select]) -> list[Scope | None]:
    """Each branch SELECT's sqlglot Scope, positionally aligned with ``branches``.

    A None entry means scope analysis could not describe that branch; the
    classifier then degrades to alias-map-only resolution — the pre-existing,
    strictly more conservative behaviour — rather than failing the query.
    """
    try:
        root = build_scope(tree)
    except Exception as e:  # noqa: BLE001  # sqlglot raises untyped errors on exotic ASTs
        # Identifier-free: the exception text can carry SQL fragments (PII).
        logger.debug(f"sql_lineage: scope analysis unavailable ({type(e).__name__})")
        return [None] * len(branches)
    if root is None:
        return [None] * len(branches)
    by_expression = {id(scope.expression): scope for scope in root.traverse()}
    return [by_expression.get(id(sel)) for sel in branches]


def resolve_output_classes(
    tree: exp.Expr,
    snapshot: SchemaSnapshot,
    sql_for_log: str = "",
    strict: bool = False,
) -> dict[str, DataClass]:
    """Map each output column name (insertion-ordered) to its DataClass.

    Output names come from the first branch (SQL semantics). For set operations
    each output position is classified across ALL branches and combined by max
    tier, so a CRITICAL column in any branch masks that position.

    ``strict`` raises ``UnresolvedProjectionError`` instead of taking the
    conservative fallback. The runtime ``sql_query`` path leaves it False — a
    user query must never be refused over an expression lineage cannot model.
    The build-time report-class deriver sets it True, because a derived class
    map that quietly absorbed a fallback is not the verified artifact it claims
    to be.
    """
    branches = _union_select_branches(tree)
    if not branches:
        raise SqlSchemaError("Query has no SELECT projection")
    scopes = _branch_scopes(tree, branches)
    ctx = _ResolveCtx(
        snapshot=snapshot,
        sql_for_log=sql_for_log,
        depth=_MAX_SCOPE_DEPTH,
        seen=frozenset(),
        tree=tree,
        strict=strict,
        shadowed=_scope_source_names(tree),
    )
    # Alias scope is per-branch: a UNION may reuse one alias for different
    # tables across branches (legal SQL), so a tree-wide map (last-write-wins)
    # would resolve a branch's column against the wrong table and under-redact.
    per_branch: list[list[DataClass]] = [
        [
            _classify_projection(proj, scope, ctx, _build_alias_map(sel))
            for proj in sel.selects
        ]
        for sel, scope in zip(branches, scopes, strict=True)
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
        # `_combined_class`, not `max`: this output position receives rows from
        # every branch, so branches carrying different CRITICAL classes leave it
        # with no representative — see that helper.
        out[name] = _combined_class(candidates) if candidates else DataClass.AGGREGATE
    return out


def is_data_query(tree: exp.Expr) -> bool:
    """True for row-returning queries (SELECT / set operations).

    False for DESCRIBE / SHOW / PRAGMA / EXPLAIN, whose output is schema or
    plan text, not classified row data — callers route those past the lineage
    gate and treat them as LOW.

    Must test ``exp.SetOperation``, not ``exp.Union``: on sqlglot 30.8.0
    ``exp.Except`` / ``exp.Intersect`` are siblings of ``exp.Union`` under
    ``SetOperation``, so a bare ``exp.Union`` check answered False for a
    top-level ``EXCEPT`` / ``INTERSECT`` and routed a row-returning query down
    the metadata path — skipping BOTH the schema allowlist and CRITICAL
    masking. ``SELECT routing_number FROM core.dim_accounts EXCEPT SELECT …``
    returned the routing number in the clear at LOW.
    """
    return isinstance(tree, (exp.Select, exp.SetOperation))


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


# ---------------------------------------------------------------------------
# Reports declared-class lookup
# ---------------------------------------------------------------------------


@functools.cache
def reports_class_map() -> dict[tuple[str, str], dict[str, DataClass]]:
    """(schema, table) -> {column: DataClass}, merged from two sources.

    Reports declare their classes on @report(classes=...) (ADR-013); lineage
    can't derive them because SQLMesh deploys reports.* as `SELECT *` pointers.
    A deployed reports.* view without an @report runner yet (net_worth) is
    covered by the generated module in reports/definitions/_derived_classes.py
    instead — see that module's docstring and
    scripts/generate_derived_report_classes.py. Both sources are imported
    lazily to avoid a privacy<->reports import cycle and to keep the CLI
    cold-start path from eagerly loading report runners.

    The generator excludes every runner-backed view by construction (it reads
    ALL_REPORTS to build the exclusion set), so a key present in both sources
    is impossible rather than merely detected — unlike the hand-written bridge
    this module replaced, there is no duplicate-guard to maintain here.

    Coverage (package reports): this maps only the in-tree ``ALL_REPORTS``
    runners plus the generated module. The framework also ships
    ``discover_reports()`` (``reports/_framework/registry.py``) for
    package-contributed ``@report`` runners, but that scanner is NOT wired
    into the live server yet, so no ``reports.*`` view outside
    ``ALL_REPORTS``/the generated module can be deployed today. When
    package-report discovery IS wired in (M2M), it MUST feed this map too —
    otherwise a package report with an undeclared CRITICAL column resolves to
    the unmasked ``AGGREGATE`` fallback, reopening the masking hole this
    module closes.
    Backstop: ``test_reports_classification.py`` fails if any *deployed*
    ``reports.*`` view is uncovered here.
    """
    from moneybin.reports._framework.registry import spec_of  # noqa: PLC0415
    from moneybin.reports.definitions import ALL_REPORTS  # noqa: PLC0415
    from moneybin.reports.definitions._derived_classes import (  # noqa: PLC0415
        DERIVED_REPORT_CLASSES,
    )

    out: dict[tuple[str, str], dict[str, DataClass]] = {}
    for runner in ALL_REPORTS:
        spec = spec_of(runner)
        out[(spec.view.schema, spec.view.name)] = dict(spec.classes)
    for key, cols in DERIVED_REPORT_CLASSES.items():
        out[key] = dict(cols)
    return out
