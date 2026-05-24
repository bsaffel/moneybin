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

import threading

from sqlglot import exp

from moneybin.database import Database
from moneybin.privacy.sql_lineage import (
    SqlSchemaError,
    expand_star,
    get_current_schema_snapshot,
    parse_cached,
    resolve_output_classes,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.tables import TableRef

# Keyed on (view, schema version) so a migration that changes the view's shape
# invalidates the entry; resolve_output_classes is otherwise pure per view.
_CACHE: dict[tuple[str, int], dict[str, DataClass]] = {}
_LOCK = threading.Lock()


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
    key = (view.full_name, snapshot.version)
    cached = _CACHE.get(key)
    if cached is not None:
        return cached

    tree = parse_cached(_view_body(db, view))
    # duckdb_views().sql is the full ``CREATE VIEW ... AS <query>`` — unwrap it.
    if isinstance(tree, exp.Create):
        inner = tree.expression
        if inner is None:
            raise SqlSchemaError(f"View {view.full_name} has no query body")
        tree = inner
    qtree = expand_star(tree, snapshot)
    classes = resolve_output_classes(qtree, snapshot, view.full_name)

    with _LOCK:
        _CACHE[key] = classes
    return classes


def classify_columns(
    db: Database, view: TableRef, columns: list[str]
) -> dict[str, DataClass]:
    """Class for each result column by name; unknown columns fail closed.

    A column absent from the view's derived map (e.g. a runner-introduced
    expression) falls back to the highest tier present, mirroring
    ``execute_sql_query`` — over-redact rather than leak.
    """
    view_classes = derive_view_classes(db, view)
    fallback = (
        max(view_classes.values(), key=lambda c: c.tier)
        if view_classes
        else DataClass.AGGREGATE
    )
    return {col: view_classes.get(col, fallback) for col in columns}
