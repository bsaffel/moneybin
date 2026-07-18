"""Derive reports.* column classes from SQLMesh model source, with no database.

The declaration stays the runtime authority (ADR-013: SQLMesh deploys a
``kind VIEW`` model as a ``SELECT *`` pointer, so runtime introspection of the
deployed view sees the pointer, not the logic). This module makes that
declaration a *verified* artifact by deriving the same answer from the model
source, where lineage is complete.

It reuses ``resolve_output_classes`` — the classifier that masks user SQL at
runtime — rather than implementing a second classification path. That is why
``COUNT(DISTINCT account_id)`` comes back ``AGGREGATE`` here instead of
inheriting ``account_id``'s class as raw column lineage would.
"""

from __future__ import annotations

import logging
from pathlib import Path

from sqlglot import exp
from sqlmesh.core.dialect import parse as sqlmesh_parse
from sqlmesh.core.model import SqlModel, load_sql_based_model

from moneybin.privacy.sql_lineage import (
    SchemaSnapshot,
    resolve_output_classes,
    snapshot_from_columns,
)
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass

logger = logging.getLogger(__name__)

_REPORTS_SCHEMA = "reports"
_DIALECT = "duckdb"


class ReportDerivationError(Exception):
    """A reports.* model could not be derived. Never falls back silently."""


def _upstream_snapshot() -> SchemaSnapshot:
    """Snapshot of core.*/app.* built from CLASSIFICATION, not from a database.

    Sound because CLASSIFICATION completeness against the live catalog is
    already CI-enforced (tests/privacy/test_classification_completeness.py), so
    for core/app it *is* the catalog. Deliberately contains no reports.*
    entries: a report reading another report would make the derived map
    self-referential, and ``_assert_acyclic`` rejects that outright.
    """
    ordered = tuple(
        (schema, table, column)
        for (schema, table), columns in CLASSIFICATION.items()
        for column in columns
    )
    return snapshot_from_columns(ordered)


def _is_star_projection(proj: exp.Expr) -> bool:
    """True for a bare ``*`` or ``t.*`` top-level projection.

    Deliberately narrower than "contains a Star anywhere" — ``COUNT(*)`` also
    nests an ``exp.Star`` (as the aggregate's argument), but is a legitimate,
    fully-resolvable projection, not a wildcard column list. Only a star that
    IS the projection (unqualified ``*``, parsed as ``exp.Star``; or
    qualified ``t.*``, parsed as ``exp.Column(this=exp.Star())``) counts.
    """
    return isinstance(proj, exp.Star) or (
        isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star)
    )


def _assert_no_star(query: exp.Query, model_name: str) -> None:
    """Reject ``SELECT *`` (or ``t.*``) in ANY select — not just the final one.

    A star in a CTE body is just as disqualifying as one in the final
    projection: nothing expands it (the deriver runs without a live catalog for
    ``reports.*``), so ``_output_index`` cannot name-match through it and the
    column degrades to a fallback — silently, where this check is meant to be a
    hard error. Checking only ``query.selects`` left that gap.
    """
    for select in query.find_all(exp.Select):
        if any(_is_star_projection(p) for p in select.selects):
            raise ReportDerivationError(
                f"{model_name}: a projection uses SELECT *. Derivation needs an "
                "explicit column list; name the columns in the model."
            )


def _assert_acyclic(query: exp.Query, model_name: str) -> None:
    for table in query.find_all(exp.Table):
        if table.db == _REPORTS_SCHEMA:
            raise ReportDerivationError(
                f"{model_name}: reads {table.db}.{table.name}. A reports.* model "
                "must read only core.*/app.*, or the derived class map becomes "
                "self-referential."
            )


def derive_report_classes(
    models_root: Path | None = None,
) -> dict[tuple[str, str], dict[str, DataClass]]:
    """Map every reports.* model to its derived output-column classes.

    Raises ReportDerivationError if any model fails to derive. Never returns a
    partial map and never falls back to a permissive default — an unresolvable
    model is a CI failure, not a silent AGGREGATE.
    """
    from moneybin.database import SQLMESH_ROOT  # noqa: PLC0415  # avoid import cycle

    root = models_root or (SQLMESH_ROOT / "models" / _REPORTS_SCHEMA)
    snapshot = _upstream_snapshot()
    out: dict[tuple[str, str], dict[str, DataClass]] = {}

    for path in sorted(root.glob("*.sql")):
        # sqlmesh.core.dialect.parse (NOT sqlglot.parse) understands the
        # MODEL(...) DDL; load_sql_based_model then yields a Model with no
        # Context, no state connection, and no encrypted database — this is
        # what keeps derivation runnable at build time.
        try:
            model = load_sql_based_model(
                sqlmesh_parse(path.read_text(), default_dialect=_DIALECT),
                path=path,
                dialect=_DIALECT,
            )
        except Exception as e:
            raise ReportDerivationError(
                f"{path.name}: failed to parse model: {e}"
            ) from e
        if not isinstance(model, SqlModel):
            # None of today's reports.* models are SeedModel/PythonModel/
            # ExternalModel — a future one that was would have no `.query`
            # sqlglot expression for resolve_output_classes to walk.
            raise ReportDerivationError(
                f"{path.name}: {type(model).__name__} has no SQL query to "
                "derive; only SqlModel is supported."
            )
        query = model.query
        if not isinstance(query, exp.Query):
            # Jinja-templated / macro-returning queries have no sqlglot AST
            # until rendered with a live macro environment — out of scope for
            # a connectionless deriver. No reports.* model does this today.
            raise ReportDerivationError(
                f"{model.name}: query is {type(query).__name__}, not a "
                "resolvable SQL AST (Jinja/macro queries aren't supported)."
            )
        _assert_no_star(query, model.name)
        _assert_acyclic(query, model.name)
        try:
            # strict=True: the runtime classifier answers an unresolvable
            # projection with a conservative fallback, which is right for user
            # SQL and wrong here — a derived map that absorbed a fallback would
            # assert "verified" while carrying a guess. Surface it as a build
            # failure so the model gets fixed instead.
            classes = resolve_output_classes(query, snapshot, strict=True)
        except Exception as e:
            raise ReportDerivationError(f"{model.name}: {e}") from e
        schema, _, view = model.name.partition(".")
        out[(schema, view)] = classes

    if not out:
        raise ReportDerivationError(f"No reports models found under {root}")
    return out
