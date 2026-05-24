"""Per-report column classification, derived from the view body (Option C).

A report runner queries a ``reports.*`` view, but the privacy lineage engine
classifies only ``core``/``app`` columns. We bridge that by deriving each
view's ``{column: DataClass}`` map **once** from the view's defining SQL — which
reads ``core``/``app`` — via :func:`resolve_output_classes`, then mapping a
report's actual result columns onto it by name. This keeps the hand-authored
``CLASSIFICATION`` registry untouched and reuses the one redaction path
(``redact_records``) the SQL surface already uses.
"""

from __future__ import annotations

import hashlib
import logging

from sqlglot import exp

from moneybin.database import Database
from moneybin.privacy.sql_lineage import (
    SqlParseError,
    SqlSchemaError,
    expand_star,
    get_current_schema_snapshot,
    parse_cached,
    resolve_output_classes,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import TableRef

logger = logging.getLogger(__name__)

# Keyed on (view, schema version, view-body hash). The body hash is load-bearing:
# `CREATE OR REPLACE VIEW` (a SQLMesh transform rebuild) changes a reports.* view
# in place without bumping the migration version, so a version-only key would
# serve stale column classifications until restart — a masking miss if the
# rebuilt view exposes newly sensitive columns. resolve_output_classes is
# otherwise pure per (view body, snapshot).
_CACHE: dict[tuple[str, int, str], dict[str, DataClass]] = {}

# Fail-closed fallback when lineage yields no classes at all (see
# classify_columns). DataClass has no CRITICAL member — CRITICAL is a Tier;
# ACCOUNT_IDENTIFIER is the Tier.CRITICAL class that redact_records actually
# masks, and is what the non-empty fallback resolves to in practice.
_FAIL_CLOSED = DataClass.ACCOUNT_IDENTIFIER


def _view_body(db: Database, view: TableRef) -> str:
    row = db.execute(
        "SELECT sql FROM duckdb_views() WHERE schema_name = ? AND view_name = ?",
        [view.schema, view.name],
    ).fetchone()
    if not row or not row[0]:
        raise SqlSchemaError(f"View {view.full_name} not found")
    return str(row[0])


def derive_view_classes(db: Database, view: TableRef) -> dict[str, DataClass]:
    """Map each output column of ``view`` to its DataClass via body lineage."""
    snapshot = get_current_schema_snapshot(db)
    body = _view_body(db, view)
    body_hash = hashlib.md5(body.encode(), usedforsecurity=False).hexdigest()
    key = (view.full_name, snapshot.version, body_hash)
    cached = _CACHE.get(key)
    if cached is not None:
        return cached

    try:
        tree = parse_cached(body)
        # duckdb_views().sql is the full ``CREATE VIEW ... AS <query>`` — unwrap it.
        if isinstance(tree, exp.Create):
            inner = tree.expression
            if inner is None:
                raise SqlSchemaError(f"View {view.full_name} has no query body")
            tree = inner
        qtree = expand_star(tree, snapshot)
        classes = resolve_output_classes(qtree, snapshot, view.full_name)
    except (SqlParseError, SqlSchemaError):
        # sqlglot can't parse/resolve a view DuckDB accepts: fail closed with an
        # empty map so classify_columns masks every column via _FAIL_CLOSED. The
        # report degrades to fully-masked rather than hard-failing. (A missing
        # view raises from _view_body above and is NOT caught — that's a real
        # config error, not a lineage gap.) Cached so we don't re-parse each call.
        logger.warning(
            f"Lineage failed for {view.full_name}; failing closed (all columns masked)"
        )
        classes = {}

    _CACHE[key] = classes
    return classes


def classify_columns(
    db: Database, view: TableRef, columns: list[str]
) -> dict[str, DataClass]:
    """Class for each result column by name; unknown columns fail closed.

    A column absent from the view's derived map (e.g. a runner-introduced
    expression) falls back to the highest tier present, mirroring
    ``execute_sql_query`` — over-redact rather than leak. When the map is empty
    (lineage parsed nothing), fall back to ``_FAIL_CLOSED`` so nothing leaks.
    """
    view_classes = derive_view_classes(db, view)
    # Highest tier present, with a deterministic tie-break by class value so two
    # classes at the same tier (e.g. ACCOUNT_IDENTIFIER vs ROUTING_NUMBER, both
    # CRITICAL but with different masks) don't resolve by dict insertion order.
    fallback = (
        max(view_classes.values(), key=lambda c: (c.tier, c.value))
        if view_classes
        else _FAIL_CLOSED
    )
    return {col: view_classes.get(col, fallback) for col in columns}
