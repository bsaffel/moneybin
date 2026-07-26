"""The two query-shape rules a report must satisfy to be materializable.

Both are pure sqlglot checks over a parsed query, and both have two callers
that must agree exactly:

- ``report_class_derivation`` enforces them on every ``reports.*`` model at
  build time — a model that breaks either one fails CI.
- the report-inspection surface reports them as graduation eligibility for a
  saved report (R6 of ``docs/specs/reports-dynamic.md``), because a saved report
  that breaks either one runs correctly today and can never be materialized.

They live here, apart from the deriver, so the second caller can reach them
without importing SQLMesh. That is not a cosmetic saving: importing SQLMesh
rewrites sqlglot's tokenizer so ``$name`` parses to ``Parameter(Var)`` instead of
``Placeholder`` for the rest of the process, and a lazy import inside an
inspection command would flip that shape partway through a run.
"""

from __future__ import annotations

from sqlglot import exp

REPORTS_SCHEMA = "reports"

# The only schemas a derivable model may read: CLASSIFICATION is an
# independently authored ground truth for both, and the snapshot is built from
# it. A read of any other schema (seeds/prep/raw/meta) has no ground truth to
# derive against, so it must fail loudly rather than resolve to a floor.
DERIVABLE_UPSTREAM_SCHEMAS = frozenset({"core", "app"})


class ReportDerivationError(Exception):
    """A view model could not be derived. Never falls back silently."""


def is_star_projection(proj: exp.Expr) -> bool:
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


def assert_no_star(query: exp.Query, model_name: str) -> None:
    """Reject ``SELECT *`` (or ``t.*``) in ANY select — not just the final one.

    A star in a CTE body is just as disqualifying as one in the final
    projection: nothing expands it (the deriver runs without a live catalog),
    so ``_output_index`` cannot name-match through it and the column degrades
    to a fallback — silently, where this check is meant to be a hard error.
    Checking only ``query.selects`` left that gap.
    """
    for select in query.find_all(exp.Select):
        if any(is_star_projection(p) for p in select.selects):
            raise ReportDerivationError(
                f"{model_name}: a projection uses SELECT *. Derivation needs an "
                "explicit column list; name the columns in the model."
            )


def assert_acyclic(query: exp.Query, model_name: str) -> None:
    """Reject any read of ``reports.*`` — the one schema with no ground truth.

    Applies identically whether the model under derivation is itself a
    reports.* model or a core.* view: core/app columns have an independently
    authored ground truth (CLASSIFICATION), so reading them is never circular
    regardless of who reads them. reports.* columns have no such ground
    truth — they ARE derivation's own output (or a hand-declared
    ``@report(classes=...)`` verified against it) — so a model of either kind
    reading reports.* would make the derived map self-referential.
    """
    # A CTE reference parses with an empty db — but so does an *unqualified* read
    # of a real table (`FROM large_transactions`), and skipping every bare name
    # made this check blind to the unqualified spelling of the very read it
    # exists to reject. Only names a CTE in this query actually defines are
    # skipped; any other bare name is an upstream read whose schema derivation
    # cannot know.
    cte_names = {
        cte.alias_or_name.lower() for cte in query.find_all(exp.CTE)
    } | _derived_table_names(query)
    for table in query.find_all(exp.Table):
        if not table.db:
            if table.name.lower() in cte_names:
                continue
            raise ReportDerivationError(
                f"{model_name}: reads {table.name} without a schema. Derivation "
                "resolves upstream columns by schema, so an unqualified read "
                "names no ground truth — qualify it as core.* or app.*."
            )
        if table.db == REPORTS_SCHEMA:
            raise ReportDerivationError(
                f"{model_name}: reads {table.db}.{table.name}. A model derived "
                "from source must read only core.*/app.*, or the derived class "
                "map becomes self-referential."
            )
        if table.db not in DERIVABLE_UPSTREAM_SCHEMAS:
            raise ReportDerivationError(
                f"{model_name}: reads {table.db}.{table.name}, which has no "
                "CLASSIFICATION ground truth. A model derived from source must "
                f"read only {'/'.join(sorted(DERIVABLE_UPSTREAM_SCHEMAS))}.* — "
                "columns from an unclassified schema cannot be derived, so the "
                "resulting map would silently under-describe them."
            )


def _derived_table_names(query: exp.Query) -> set[str]:
    """Aliases of inline derived tables — also bare names that read no upstream."""
    return {
        subquery.alias_or_name.lower()
        for subquery in query.find_all(exp.Subquery)
        if subquery.alias_or_name
    }


def materialization_blockers(query: exp.Query, model_name: str) -> tuple[str, ...]:
    """Every reason ``query`` could not become a ``reports.*`` SQLMesh model.

    Runs both checks rather than stopping at the first, because a report that
    needs two edits should learn both in one call. Empty means eligible — and
    it means so against the same functions the build-time deriver runs, not a
    restatement of their rules that could drift from them.
    """
    blockers: list[str] = []
    for check in (assert_no_star, assert_acyclic):
        try:
            check(query, model_name)
        except ReportDerivationError as e:
            blockers.append(str(e))
    return tuple(blockers)
