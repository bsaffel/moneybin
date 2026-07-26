"""R6: the verify surface — the same evidence for every report tier.

``docs/specs/reports-dynamic.md`` R6. This is the *verify* half of "create and
verify": for a report of any tier it returns the query in R9's two forms, the
resolved class map with per-column provenance, the upstream tables it reads,
freshness, and whether it could be materialized.

It reads a report's contract; it never runs one. The runner is invoked to
*build* its SELECT (which is the only way to obtain the query for a decorated
report), and the resulting string is rendered for display — never executed.

No MCP identity is assigned here. The handle resolves by the shared reference
contract's order — an exact ``report_id`` first, then an exact name — so a report
whose name is contested by a registry collision (R5) stays inspectable by its
stable ``report_id``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from pydantic import JsonValue
from sqlglot import exp

from moneybin.database import Database
from moneybin.privacy.report_materialization import materialization_blockers
from moneybin.privacy.sql_lineage import (
    FAIL_CLOSED_CLASS,
    ProjectionSource,
    SqlParseError,
    get_current_schema_snapshot,
    parse_cached,
    resolve_projection_sources,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import (
    RegisteredReport,
    ReportTier,
    ServiceReportSpec,
    get_report_catalog,
    report_tier,
    validate_report_parameters,
)
from moneybin.reports._framework.contract import ReportSpec
from moneybin.reports._framework.derive import type_sentinel
from moneybin.reports._framework.provenance import SqlForms, render_sql_forms

logger = logging.getLogger(__name__)

type ColumnOrigin = Literal["upstream", "computed", "unresolved", "undetermined"]
"""Where one output column's value comes from.

``upstream`` — a passthrough of one named column. ``computed`` — an expression,
so no single upstream column describes it. ``unresolved`` — nothing could
classify it, which is why it masks. ``undetermined`` — this surface has no query
text to read a provenance from (a service-backed report), or the projection's
name is one lineage never saw.
"""

type GraduationState = Literal["eligible", "blocked", "already_materialized"]


@dataclass(frozen=True, slots=True)
class ColumnProvenance:
    """One output column's class and where its value descends from."""

    column: str
    data_class: DataClass
    origin: ColumnOrigin
    upstream: str | None


@dataclass(frozen=True, slots=True)
class ReportExplanation:
    """Everything the verify surface returns about one report, any tier."""

    report_id: str
    name: str
    description: str
    tier: ReportTier
    sql: str | None
    sql_template: str | None
    sql_unavailable: str | None
    withheld_parameters: tuple[str, ...]
    sql_suppressed_by: tuple[str, ...]
    columns: tuple[ColumnProvenance, ...]
    lineage: tuple[str, ...]
    class_fingerprint: str | None
    drift_detected: bool
    drift_reason: str | None
    updated_at: str | None
    graduation: GraduationState
    graduation_blockers: tuple[str, ...]

    @property
    def sensitivity(self) -> Literal["low", "medium"]:
        """The tier this explanation's own text carries.

        A saved report's name, description, and SQL are user-authored free text —
        ``USER_NOTE``, MEDIUM. A built-in's are authored in the repo and
        reviewed. Same reasoning as ``catalog_sensitivity``, applied to the one
        surface that publishes a report's query verbatim.
        """
        return "medium" if self.tier == "user" else "low"


@dataclass(frozen=True, slots=True)
class _Freshness:
    """Drift state for a stored report; every field absent for a repo file."""

    class_fingerprint: str | None = None
    drift_detected: bool = False
    drift_reason: str | None = None
    updated_at: str | None = None


def explain_report(
    db: Database, *, handle: str, parameters: Mapping[str, JsonValue]
) -> ReportExplanation:
    """Explain the report ``handle`` names — a ``report_id`` or an exact name.

    Archived reports resolve like any other: archiving suppresses catalog noise
    rather than revoking access (R5), so a report a user hid must still be
    inspectable — otherwise archiving would quietly delete the evidence trail.
    """
    return explain_spec(
        db, get_report_catalog(db).resolve(handle), parameters=parameters
    )


def explain_spec(
    db: Database, report: RegisteredReport, *, parameters: Mapping[str, JsonValue]
) -> ReportExplanation:
    """Explain one already-resolved report.

    Split from :func:`explain_report` at the resolution seam so the evidence is
    assembled from a ``RegisteredReport`` alone — the same input every tier
    reduces to, which is what makes "the same evidence for every tier" a
    property of the code rather than a claim about it.
    """
    tier = report_tier(report)
    freshness = _freshness(db, report) if tier == "user" else _Freshness()
    forms, unavailable = _sql_forms(db, report, parameters=parameters, tier=tier)
    query_sql = None if forms is None else forms.sql_template
    graduation, blockers = _graduation(query_sql, report.report_id, tier=tier)

    return ReportExplanation(
        report_id=report.report_id,
        name=report.name,
        description=report.description,
        tier=tier,
        sql=None if forms is None else forms.sql,
        sql_template=query_sql,
        sql_unavailable=unavailable,
        withheld_parameters=() if forms is None else forms.withheld_parameters,
        sql_suppressed_by=() if forms is None else forms.suppressed_by,
        columns=_column_provenance(db, report, query_sql=query_sql),
        lineage=report.semantics.provenance,
        class_fingerprint=freshness.class_fingerprint,
        drift_detected=freshness.drift_detected,
        drift_reason=freshness.drift_reason,
        updated_at=freshness.updated_at,
        graduation=graduation,
        graduation_blockers=blockers,
    )


def _sql_forms(
    db: Database,
    report: RegisteredReport,
    *,
    parameters: Mapping[str, JsonValue],
    tier: ReportTier,
) -> tuple[SqlForms | None, str | None]:
    """Build the query's two forms, or say why no query exists.

    A ``ServiceReportSpec`` carries an ``executor`` returning a finished result,
    not a ``runner`` returning a ``ReportQuery``, so no SQL string exists
    anywhere in its path. Returning its declared provenance plus this reason
    tells the truth; fabricating a plausible ``SELECT`` to fill the slot does
    not, and the whole point of a provenance chip is that it can be checked.
    """
    if isinstance(report, ServiceReportSpec):
        return None, (
            f"service_backed: {report.report_id} is executed by a service, not a "
            "SELECT, so no query text exists in its path. Its lineage names the "
            "reports.* views the service reads."
        )

    unbound = _unbound(report, parameters) if tier == "user" else ()
    validated = validate_report_parameters(
        report, {**parameters, **{name: _sentinel(report, name) for name in unbound}}
    )
    return render_sql_forms(report.runner(db, **validated), unbound=unbound), None


def _unbound(
    report: ReportSpec, parameters: Mapping[str, JsonValue]
) -> tuple[str, ...]:
    """Required parameters the caller supplied no value for.

    Only the user tier reaches this. Its SQL is a stored template, so a missing
    value can render as its ``$name`` placeholder. The other tiers have no
    template — ``spec.runner(db, **params)`` raises on a missing keyword argument
    before a query exists, and a sentinel would fail the runner's own validation
    or ID resolution instead — so they require every ``required`` parameter and
    return a validation error naming the missing ones.
    """
    return tuple(
        parameter.name
        for parameter in report.params
        if parameter.required and parameter.name not in parameters
    )


def _sentinel(report: ReportSpec, name: str) -> JsonValue:
    """A type-valid stand-in so parameter validation still runs on the rest.

    The value is never rendered: :func:`render_sql_forms` withholds every
    ``unbound`` name and suppresses the executed form entirely, so this only
    has to satisfy the type check. A saved report's annotations always come from
    ``annotation_of``, so the sentinel table always covers them.
    """
    declared = next(parameter for parameter in report.params if parameter.name == name)
    return type_sentinel(declared.annotation)  # type: ignore[return-value]  # a JSON scalar by construction


def _freshness(db: Database, report: RegisteredReport) -> _Freshness:
    """Read the stored row's drift state — the R4 question, asked out loud.

    Re-deriving through ``spec_from_row`` rather than reading a flag: the
    catalog drops ``DynamicReport.degraded`` when it flattens to specs, and the
    fingerprint comparison is the only thing that actually knows. An inspection
    command can afford one re-derivation; publishing a stale "fresh" cannot.
    """
    from moneybin.reports._framework.dynamic import spec_from_row
    from moneybin.repositories.user_reports_repo import UserReportsRepo

    row = UserReportsRepo(db).get(report.report_id)
    if row is None:
        return _Freshness()
    dynamic = spec_from_row(db, row)
    updated_at = row.get("updated_at")
    return _Freshness(
        class_fingerprint=str(row["class_fingerprint"]) or None,
        drift_detected=dynamic.degraded,
        drift_reason=dynamic.degraded_reason,
        updated_at=None if updated_at is None else str(updated_at),
    )


def _column_provenance(
    db: Database, report: RegisteredReport, *, query_sql: str | None
) -> tuple[ColumnProvenance, ...]:
    """Join the report's class map to the projection each column came from.

    Keyed by the report's own ``classes`` map, so the classes shown are the ones
    redaction will actually apply — never a second derivation that could
    disagree with it. Provenance is layered on top and may be absent; the class
    never is.
    """
    sources = _projection_sources(db, query_sql)
    columns: list[ColumnProvenance] = []
    for name, data_class in report.classes.items():
        source = sources.get(name)
        if data_class is FAIL_CLOSED_CLASS:
            # The masked column self-explains (R3). Its provenance is exactly
            # the fact that nothing could be resolved for it, so reporting a
            # projection shape here would bury the answer the reader needs.
            origin: ColumnOrigin = "unresolved"
            upstream = None
        elif source is None:
            origin, upstream = "undetermined", None
        elif source.passthrough:
            origin, upstream = "upstream", source.upstream
        else:
            origin, upstream = "computed", None
        columns.append(
            ColumnProvenance(
                column=name, data_class=data_class, origin=origin, upstream=upstream
            )
        )
    return tuple(columns)


def _projection_sources(
    db: Database, query_sql: str | None
) -> dict[str, ProjectionSource]:
    """Resolve the query's projections, or nothing if it cannot be read."""
    if query_sql is None:
        return {}
    try:
        return resolve_projection_sources(
            parse_cached(query_sql), get_current_schema_snapshot(db)
        )
    except SqlParseError:
        logger.warning("report inspection: query does not parse; provenance omitted")
        return {}


def _graduation(
    query_sql: str | None, report_id: str, *, tier: ReportTier
) -> tuple[GraduationState, tuple[str, ...]]:
    """Whether this report could become a SQLMesh ``reports.*`` model.

    The save allowlist is deliberately wider than materialization's: composing
    on top of a built-in report is real value, and the graduation promise is
    explicitly conditional. The obligation is honesty, not restriction — so a
    report that runs correctly today and can never be materialized says so, with
    the specific reason.
    """
    if tier != "user" or query_sql is None:
        return "already_materialized", ()
    try:
        tree = parse_cached(query_sql)
    except SqlParseError:
        return "blocked", (f"{report_id}: the stored query no longer parses.",)
    if not isinstance(tree, exp.Query):
        return "blocked", (f"{report_id}: the stored query is not a SELECT.",)
    blockers = materialization_blockers(tree, report_id)
    return ("blocked", blockers) if blockers else ("eligible", ())
