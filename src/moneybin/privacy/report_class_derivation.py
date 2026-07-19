"""Derive privacy classes for SQLMesh view models from source, with no database.

The declaration stays the runtime authority (ADR-013: SQLMesh deploys a
``kind VIEW`` model as a ``SELECT *`` pointer, so runtime introspection of the
deployed view sees the pointer, not the logic). This module makes that
declaration a *verified* artifact by deriving the same answer from the model
source, where lineage is complete.

It reuses ``resolve_output_classes`` — the classifier that masks user SQL at
runtime — rather than implementing a second classification path. That is why
``COUNT(DISTINCT account_id)`` comes back ``AGGREGATE`` here instead of
inheriting ``account_id``'s class as raw column lineage would.

Two callers share this one engine (``_derive_view_classes``), never a second
classification path:

- ``derive_report_classes()`` — every ``reports/*.sql`` model, unconditionally
  (today all of them are ``kind VIEW``; this matches its pre-existing
  behaviour exactly and must keep doing so — the generated-module freshness
  test and ``make generate-report-classes`` depend on it).
- ``derive_core_view_classes()`` — only the ``kind VIEW`` models under
  ``core/*.sql``. Most of ``core`` is materialized tables (``kind FULL`` SQL
  models, or Python models with no SQL text at all), which this
  connectionless, source-parsing deriver cannot classify. Those are never
  silently dropped from consideration: every non-view model is returned in
  the second element of ``derive_core_view_classes()``'s result, name mapped
  to the reason it was excluded. A ``core`` model that IS a view but fails to
  derive still raises ``ReportDerivationError`` — "is a view" is a
  precondition for attempting derivation, not an exemption from it.
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
_CORE_SCHEMA = "core"
_DIALECT = "duckdb"


class ReportDerivationError(Exception):
    """A view model could not be derived. Never falls back silently."""


def _upstream_snapshot() -> SchemaSnapshot:
    """Snapshot of core.*/app.* built from CLASSIFICATION, not from a database.

    Sound because CLASSIFICATION completeness against the live catalog is
    already CI-enforced (tests/privacy/test_classification_completeness.py), so
    for core/app it *is* the catalog. Deliberately contains no reports.*
    entries: reports.* classes come from derivation/declaration, not from
    CLASSIFICATION, so a model reading reports.* would make the derived map
    self-referential — ``_assert_acyclic`` rejects that outright.

    Shared unchanged by both ``derive_report_classes`` (reports.* models read
    core/app upstream) and ``derive_core_view_classes`` (core view models read
    core/app too — including OTHER core tables, whether or not those are
    themselves views subject to derivation, since CLASSIFICATION is the
    independently-authored ground truth for every core/app column regardless
    of a given table's kind). Neither caller is self-referential against this
    snapshot; only reading reports.* is.
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
    projection: nothing expands it (the deriver runs without a live catalog),
    so ``_output_index`` cannot name-match through it and the column degrades
    to a fallback — silently, where this check is meant to be a hard error.
    Checking only ``query.selects`` left that gap.
    """
    for select in query.find_all(exp.Select):
        if any(_is_star_projection(p) for p in select.selects):
            raise ReportDerivationError(
                f"{model_name}: a projection uses SELECT *. Derivation needs an "
                "explicit column list; name the columns in the model."
            )


def _assert_acyclic(query: exp.Query, model_name: str) -> None:
    """Reject any read of ``reports.*`` — the one schema with no ground truth.

    Applies identically whether the model under derivation is itself a
    reports.* model or a core.* view: core/app columns have an independently
    authored ground truth (CLASSIFICATION), so reading them is never circular
    regardless of who reads them. reports.* columns have no such ground
    truth — they ARE derivation's own output (or a hand-declared
    ``@report(classes=...)`` verified against it) — so a model of either kind
    reading reports.* would make the derived map self-referential.
    """
    for table in query.find_all(exp.Table):
        if table.db == _REPORTS_SCHEMA:
            raise ReportDerivationError(
                f"{model_name}: reads {table.db}.{table.name}. A model derived "
                "from source must read only core.*/app.*, or the derived class "
                "map becomes self-referential."
            )


def _load_model(path: Path) -> SqlModel:
    """Parse ``path`` into a SqlModel with no Context/database, or raise.

    ``sqlmesh.core.dialect.parse`` (NOT ``sqlglot.parse``) understands the
    ``MODEL(...)`` DDL; ``load_sql_based_model`` then yields a Model with no
    Context, no state connection, and no encrypted database — this is what
    keeps derivation runnable at build time.
    """
    try:
        model = load_sql_based_model(
            sqlmesh_parse(path.read_text(), default_dialect=_DIALECT),
            path=path,
            dialect=_DIALECT,
        )
    except Exception as e:
        raise ReportDerivationError(f"{path.name}: failed to parse model: {e}") from e
    if not isinstance(model, SqlModel):
        # A SeedModel/PythonModel/ExternalModel would have no `.query` sqlglot
        # expression for resolve_output_classes to walk.
        raise ReportDerivationError(
            f"{path.name}: {type(model).__name__} has no SQL query to "
            "derive; only SqlModel is supported."
        )
    return model


def _derive_model_classes(
    model: SqlModel, snapshot: SchemaSnapshot
) -> dict[str, DataClass]:
    """Derive one already-loaded model's output-column classes, or raise."""
    query = model.query
    if not isinstance(query, exp.Query):
        # Jinja-templated / macro-returning queries have no sqlglot AST until
        # rendered with a live macro environment — out of scope for a
        # connectionless deriver. No model does this today.
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
        return resolve_output_classes(query, snapshot, strict=True)
    except Exception as e:
        raise ReportDerivationError(f"{model.name}: {e}") from e


def _derive_view_classes(
    models_root: Path, *, exclude_non_derivable: bool
) -> tuple[dict[tuple[str, str], dict[str, DataClass]], dict[str, str]]:
    """Shared engine: derive every model under ``models_root``, or exclude it.

    ``exclude_non_derivable=False`` (``derive_report_classes``, reports.*):
    every ``*.sql`` model must derive, unconditionally — this is the
    pre-existing behaviour, unchanged. Kind is never inspected, and any
    failure (parse error, non-SqlModel, SELECT *, acyclicity, or an
    unresolvable projection) raises immediately. Reports authors are already
    bound by convention to a shape this deriver handles (see
    ``.claude/rules/reports.md``: read only core.*/app.*, no ``SELECT *``),
    so a failure here is always a genuine bug to fix, never an expected gap.

    ``exclude_non_derivable=True`` (``derive_core_view_classes``, core.*):
    core has no such convention — most of it is materialized tables, and the
    view models that remain are free to read prep.*/seeds.* (the normal
    medallion data flow) or use shapes (bare ``SELECT *``, unaliased
    single-table projections, ``UNNEST(...)`` struct access) this
    connectionless, no-``qualify()`` deriver was never built to resolve. The
    stated scoping rule has two parts, both surfaced in the second return
    value (name -> reason), never silently dropped from consideration:

    1. Only ``kind VIEW`` models are attempted at all. A non-view SQL model
       (``kind FULL``/incremental/etc.) is a materialized table by design —
       not a modeling error — and is excluded before derivation is attempted.
       Every ``*.py`` file in the directory (a SQLMesh Python model) is
       excluded the same way, by filename, without ever being loaded: it has
       no SQL text for this deriver to parse.
    2. A ``kind VIEW`` model that still fails to derive (reads prep.*/
       seeds.*, uses ``SELECT *``, or hits an unresolvable projection) is
       excluded with the derivation engine's own error message as the
       reason — accurate and specific, since it comes from the exact code
       path that would otherwise raise. This is NOT a license to wave away a
       genuine regression: ``tests/privacy/test_report_class_derivation.py``
       pins the exact excluded set, so any change (a model newly failing, or
       a model that starts deriving that used not to) must be a deliberate,
       reviewed edit to that test, not a silent pass-through.
    """
    snapshot = _upstream_snapshot()
    out: dict[tuple[str, str], dict[str, DataClass]] = {}
    excluded: dict[str, str] = {}

    if exclude_non_derivable:
        for py_path in sorted(models_root.glob("*.py")):
            if py_path.stem == "__init__":
                continue
            excluded[py_path.stem] = "python model — no derivable SQL source"

    for path in sorted(models_root.glob("*.sql")):
        model = _load_model(path)
        if exclude_non_derivable and not model.kind.is_view:
            # A materialized table is a legitimate design choice (dim_accounts
            # is FULL because it's rebuilt from a dedup window, not a thin
            # pointer) — not a modeling error, so this is a recorded skip, not
            # a ReportDerivationError.
            excluded[model.name] = (
                f"kind={model.kind.name} — materialized table, not a view; "
                "out of scope for connectionless source derivation"
            )
            continue
        if exclude_non_derivable:
            try:
                classes = _derive_model_classes(model, snapshot)
            except ReportDerivationError as e:
                excluded[model.name] = str(e)
                continue
        else:
            classes = _derive_model_classes(model, snapshot)
        schema, _, view = model.name.partition(".")
        out[(schema, view)] = classes

    if not out:
        raise ReportDerivationError(
            f"No derivable view models found under {models_root}"
        )
    return out, excluded


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
    out, _excluded = _derive_view_classes(root, exclude_non_derivable=False)
    return out


def derive_core_view_classes(
    models_root: Path | None = None,
) -> tuple[dict[tuple[str, str], dict[str, DataClass]], dict[str, str]]:
    """Map every derivable ``core.*`` view model to its derived classes.

    Returns ``(derived, excluded)``: ``derived`` is exactly like
    ``derive_report_classes``'s return value, scoped to whichever core view
    models this connectionless deriver can actually resolve today (see
    ``_derive_view_classes`` for the two-part scoping rule and why core, unlike
    reports, has a real excluded set rather than an empty one). ``excluded``
    maps every core model NOT included in ``derived`` to the reason, so the
    scope is a checked, visible fact — see
    ``tests/privacy/test_report_class_derivation.py`` for the pinned set —
    rather than a side effect of silently dropping what didn't fit.
    """
    from moneybin.database import SQLMESH_ROOT  # noqa: PLC0415  # avoid import cycle

    root = models_root or (SQLMESH_ROOT / "models" / _CORE_SCHEMA)
    return _derive_view_classes(root, exclude_non_derivable=True)
