"""Run a report: execute the runner's query, classify, redact, summarize.

The steps every tier's rows pass through — built-in, extension, and user-created
alike (R7 of ``docs/specs/reports-dynamic.md``). It mirrors ``execute_sql_query``
— same ``redact_records`` / ``derive_query_tier`` bottleneck — but the SQL comes
from a report runner and the per-column classes come from the report's declared
map (see ``classify``) rather than live lineage on a user query.

Production reaches these steps through ``ReportCatalog.execute``, which owns
reference resolution and parameter validation; the single ``reports`` MCP tool
and every CLI command go that way. ``run_report`` composes the same steps for a
caller that already holds a spec and wants neither — today only tests.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Protocol, cast

import duckdb
from pydantic import JsonValue

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import RecoveryAction, UserError
from moneybin.log_sanitizer import sql_digest
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.redaction import MaskStrength, mask_strength, redact_records
from moneybin.privacy.sql_lineage import derive_query_tier
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    resolve_display_currency,
)
from moneybin.reports._framework.classify import classify_columns
from moneybin.reports._framework.contract import (
    ORIGINAL_CURRENCY_COLUMN,
    ParamSpec,
    RecomputeDerived,
    ReportSemantics,
    ReportSpec,
    bound_value,
)
from moneybin.reports._framework.convert import convert_records, rates_pricing
from moneybin.services.currency_service import CurrencyService, ResolvedRate

logger = logging.getLogger(__name__)

type FrozenJsonValue = (
    None
    | bool
    | int
    | float
    | str
    | tuple["FrozenJsonValue", ...]
    | Mapping[str, "FrozenJsonValue"]
)


@dataclass(frozen=True)
class ReportResult:
    """Redacted rows plus the envelope-relevant metadata for one report call.

    Mirrors the envelope-facing fields of ``SqlQueryResult`` so the MCP and CLI
    registrars build identical envelopes to the SQL surface.
    """

    records: list[dict[str, Any]]
    columns: list[str]
    output_classes: dict[str, DataClass]
    tier: Tier
    total_count: int
    truncated: bool
    actions: list[str] = field(default_factory=list)
    period: str | None = None
    # Unknown, never a currency: a result built without one has not been told a
    # denomination, and a literal here would speak for every report that never
    # mentions currency at all (multi-currency.md Requirement 5).
    display_currency: str | None = None
    #: R4's drift state, set by ``ReportCatalog.execute`` for a saved report whose
    #: stored class map went stale. Never true for a packaged tier: a built-in is
    #: a file in the repo and has no stored map to go stale.
    degraded: bool = False
    degraded_reason: str | None = None
    #: The rates behind these rows' converted amounts, empty when none were
    #: converted. Rides the response only — never the durable log, which is why
    #: ``degraded_reason`` still names no date (see ``convert._missing_reason``).
    applied_rates: tuple[ResolvedRate, ...] = ()
    #: Structured next steps for an agent, beside the prose ``actions`` the CLI
    #: prints — a caveat's remedy an agent can call without parsing a hint.
    recovery_actions: tuple[RecoveryAction, ...] = ()

    @property
    def classes_returned(self) -> list[str]:
        """Sorted data-class values for the envelope/audit."""
        if not self.output_classes:
            return ["aggregate"]
        return sorted({c.value for c in self.output_classes.values()})

    def to_envelope(self) -> ResponseEnvelope[Any]:
        """Build the standard response envelope from this result.

        The ReportResult→envelope mapping is identical for both surfaces (only
        what each does with the envelope differs), so it lives here next to the
        fields it reads.
        """
        return build_envelope(
            data=self.records,
            sensitivity=tier_to_sensitivity(self.tier).value,
            total_count=self.total_count,
            classes_returned=self.classes_returned,
            actions=self.actions or None,
            period=self.period,
            display_currency=self.display_currency,
            degraded=self.degraded,
            degraded_reason=self.degraded_reason,
            recovery_actions=list(self.recovery_actions) or None,
            applied_rates=[rate.as_provenance() for rate in self.applied_rates] or None,
        )


@dataclass(frozen=True, kw_only=True)
class CatalogReportResult(ReportResult):
    """A report result tagged with its catalog identity and financial meaning."""

    report_id: str
    parameters: Mapping[str, FrozenJsonValue]
    semantics: ReportSemantics
    provenance: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class CatalogReportExecution:
    """One raw catalog-runner execution before terminal redaction."""

    report_id: str
    parameters: Mapping[str, JsonValue]
    sql: str | None
    records: list[dict[str, Any]]
    columns: list[str]
    column_types: list[str]
    output_classes: dict[str, DataClass]
    tier: Tier
    total_count: int
    truncated: bool
    actions: list[str]
    period: str | None
    semantics: ReportSemantics
    provenance: tuple[str, ...]
    # Same contract as ReportResult.display_currency — see the note there.
    display_currency: str | None = None
    #: Why a requested display currency was not applied, when one was requested
    #: and the rows fell back to per-currency segmentation (Requirement 15).
    degraded_reason: str | None = None
    #: Every distinct rate that priced these rows (Requirement 10). Empty until
    #: ``convert_execution`` runs, and on any result that stayed segmented.
    applied_rates: tuple[ResolvedRate, ...] = ()
    #: See ``ReportResult.recovery_actions``.
    recovery_actions: tuple[RecoveryAction, ...] = ()
    #: The row cap this execution still owes, or ``None`` once one has been
    #: applied. Set only on the converting path, where the cap has to wait for
    #: ``on_converted`` — see ``truncate_execution``.
    pending_limit: int | None = None
    on_converted: RecomputeDerived | None = None
    """The report's own repair for values *derived* from its money columns.

    Conversion multiplies each declared money column independently, which leaves
    anything computed from those columns describing the original currency: a
    ``balance_drift`` row keeps a status bucketed against the pre-conversion
    amount, and a row carrying both components and their total can round so the
    parts no longer sum to it.

    A callable rather than a declared relation because the two repairs have no
    shared shape — one re-buckets a label against thresholds, the other re-adds
    two columns — and inventing a vocabulary general enough for both would be a
    larger contract than the two functions it replaces.
    """


def _reordered(row: Mapping[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    """One converted row rebuilt in `columns` order.

    A key `columns` does not name keeps its relative position at the end rather
    than being dropped: this reorders a payload, and silently losing a field a
    caller reads would be a far worse trade than an out-of-order one.
    """
    ordered = {name: row[name] for name in columns if name in row}
    ordered.update({key: value for key, value in row.items() if key not in ordered})
    return ordered


def _original_currency_position(columns: Sequence[str], currency: str | None) -> int:
    """Where the runtime provenance column goes: beside the currency it records.

    Both columns are `DataClass.CURRENCY`, so `column-ordering.md` Rule B puts
    them in the same descriptive block. Appending instead would end a converted
    read on provenance rather than on the report's headline measure — the one
    place the declared projections are swept but the runtime one is not. The
    semantics name the column rather than this module assuming `currency_code`;
    a report naming none, or naming one this projection does not carry, keeps
    the old append.
    """
    if currency is None or currency not in columns:
        return len(columns)
    return columns.index(currency) + 1


def convert_execution(
    execution: CatalogReportExecution,
    *,
    to_currency: str,
    service: CurrencyService,
) -> CatalogReportExecution:
    """Price one execution's rows in ``to_currency``, or leave them segmented.

    Applied to the raw execution rather than the redacted result because masking
    replaces an amount with a string: a converted ``'*****'`` is not a number,
    and a partially masked one would be a different number than it claims.
    """
    outcome = convert_records(
        execution.records,
        classes=execution.output_classes,
        semantics=execution.semantics,
        to_currency=to_currency,
        service=service,
    )
    # Only a conversion that happened can leave a derived value stale, and only
    # then is there a target currency to recompute against. A segmented result
    # still holds its original amounts, so its derived values are already right.
    # `applied_rates` — not `display_currency` — is what says one happened: the
    # home currency is defaulted in, so the ordinary single-currency read asks
    # to be priced into the currency it is already in, resolves identity rates,
    # and comes back with a target set and nothing moved. Reusing the same
    # non-identity predicate the provenance already publishes keeps one
    # definition of "converted" rather than a second one beside it.
    removed = 0
    if (
        outcome.applied_rates
        and outcome.display_currency is not None
        and execution.on_converted is not None
    ):
        before = len(outcome.records)
        execution.on_converted(outcome.records, outcome.display_currency)
        removed = before - len(outcome.records)

    # `convert_records` attaches the original currency to each row under the
    # same "a rate was applied" condition. Declaring it here is what makes it
    # visible: the envelope's column list, the typed CLI table, and redaction
    # all read `columns`/`output_classes`, and an undeclared key would either
    # be dropped or reach a caller with no masking policy behind it. Declared
    # by the framework rather than by each report, because no report's SQL
    # projects it — it exists only on the converted read this module produces.
    columns = execution.columns
    column_types = execution.column_types
    output_classes = execution.output_classes
    records = outcome.records
    if outcome.applied_rates:
        at = _original_currency_position(columns, execution.semantics.currency)
        columns = [*columns[:at], ORIGINAL_CURRENCY_COLUMN, *columns[at:]]
        column_types = [*column_types[:at], "VARCHAR", *column_types[at:]]
        output_classes = {
            **output_classes,
            ORIGINAL_CURRENCY_COLUMN: DataClass.CURRENCY,
        }
        # `convert_records` put the same key on every row dict, and a row's own
        # insertion order is what `to_json()` emits — so repositioning the
        # column list alone would ship a payload whose keys contradict the
        # column list beside them. The rows carry the order too.
        records = [_reordered(row, columns) for row in records]

    return replace(
        execution,
        records=records,
        columns=columns,
        column_types=column_types,
        output_classes=output_classes,
        # `total_count` was fixed before conversion ran, so a callback that
        # collapses rows — `core:networth` merging its per-currency totals —
        # would leave the envelope deriving `has_more` from rows that no longer
        # exist, reporting an untruncated result as truncated. Floored at what
        # is actually being returned, since a total below that is never true.
        total_count=max(execution.total_count - removed, len(outcome.records)),
        applied_rates=outcome.applied_rates,
        # A fallback leaves the rows in their own currencies, so the currency
        # already derived from those rows is still the true answer. Overwriting
        # it with the failed conversion's ``None`` would report a mixed result
        # for rows that all agree — worse than the answer the caller had before
        # asking. Only a conversion that happened replaces it.
        display_currency=outcome.display_currency or execution.display_currency,
        degraded_reason=outcome.degraded_reason,
    )


def truncate_execution(execution: CatalogReportExecution) -> CatalogReportExecution:
    """Apply the row cap an execution deferred, once conversion has finished.

    The cap describes the answer, not conversion's inputs. ``core:networth``
    emits one totals row per currency held and merges them only after pricing
    has put them in one unit, so cutting first hands that merge a subset: a
    two-currency profile read at ``limit=1`` would publish one currency's
    subtotal as the whole position. Blend by omission is the same defect as
    blend by summation, and it is what the per-currency row split exists to
    prevent.

    A no-op for an execution that already applied its own cap, which is every
    execution that never converted.
    """
    max_rows = execution.pending_limit
    if max_rows is None:
        return execution
    truncated = len(execution.records) > max_rows
    records = execution.records[:max_rows]
    return replace(
        execution,
        records=records,
        truncated=truncated,
        # Provenance describes the rows in the response, so it is cut with them:
        # the sentinel row conversion priced is gone, and any rate resolved only
        # for it must go too. Left alone on the untruncated path, where every
        # rate still belongs to a row that is being returned.
        applied_rates=rates_pricing(
            records,
            execution.applied_rates,
            date_column=execution.semantics.fx_date,
        )
        if truncated
        else execution.applied_rates,
        # Same sentinel `build_catalog_execution` uses: one past the cap means
        # "at least this many", which is all a bounded fetch can honestly claim.
        total_count=max_rows + 1 if truncated else len(records),
        pending_limit=None,
    )


def _resolve_display_currency(records: list[dict[str, Any]]) -> str | None:
    """The one currency every report row is denominated in, else None.

    Thin projection over the shared envelope rule so reports and the balance
    reads answer ``display_currency`` identically.
    """
    return resolve_display_currency(record.get("currency_code") for record in records)


class _CatalogSpec(Protocol):
    """The result-building fields shared by SQL and service report specs."""

    @property
    def report_id(self) -> str: ...

    @property
    def name(self) -> str: ...

    @property
    def classes(self) -> Mapping[str, DataClass]: ...

    @property
    def semantics(self) -> ReportSemantics: ...

    @property
    def parameters(self) -> tuple[ParamSpec, ...]: ...

    @property
    def on_converted(self) -> RecomputeDerived | None: ...


def _redact_and_freeze_parameter(
    value: JsonValue,
    data_class: DataClass,
) -> FrozenJsonValue:
    """Redact parameter leaves and recursively freeze JSON containers."""
    if isinstance(value, dict):
        if data_class.tier >= Tier.MEDIUM:
            return MappingProxyType({
                "entry_count": len(value),
                "redacted": True,
            })
        return MappingProxyType({
            key: _redact_and_freeze_parameter(item, data_class)
            for key, item in value.items()
        })
    if isinstance(value, list):
        return tuple(_redact_and_freeze_parameter(item, data_class) for item in value)
    redacted = redact_records(
        [{"value": value}],
        {"value": data_class},
        consent=None,
    )[0]["value"]
    return cast(FrozenJsonValue, redacted)


def redact_report_parameters(
    spec: _CatalogSpec,
    parameters: Mapping[str, JsonValue],
) -> Mapping[str, FrozenJsonValue]:
    """Build immutable, redacted effective-parameter metadata."""
    classes = {parameter.name: parameter.data_class for parameter in spec.parameters}
    return MappingProxyType({
        name: _redact_and_freeze_parameter(value, classes[name])
        for name, value in parameters.items()
    })


def build_catalog_result(
    spec: _CatalogSpec,
    *,
    parameters: Mapping[str, JsonValue],
    records: list[dict[str, Any]],
    columns: list[str],
    max_rows: int,
    actions: list[str] | None = None,
    period: str | None = None,
) -> CatalogReportResult:
    """Redact and truncate tabular rows using the shared report rules."""
    execution = build_catalog_execution(
        spec,
        parameters=parameters,
        sql=None,
        records=records,
        columns=columns,
        column_types=[],
        max_rows=max_rows,
        actions=actions,
        period=period,
    )
    return redact_catalog_execution(spec, execution)


def build_catalog_execution(
    spec: _CatalogSpec,
    *,
    parameters: Mapping[str, JsonValue],
    sql: str | None,
    records: list[dict[str, Any]],
    columns: list[str],
    column_types: list[str],
    max_rows: int | None,
    actions: list[str] | None = None,
    period: str | None = None,
    defer_truncation: bool = False,
) -> CatalogReportExecution:
    """Build one raw, classified execution from already-fetched rows.

    ``defer_truncation`` keeps ``max_rows`` as the fetch bound while leaving the
    rows uncut, for a caller that must run ``on_converted`` over the whole
    result before the cap can mean anything — see ``truncate_execution``.
    """
    truncated = (
        not defer_truncation and max_rows is not None and len(records) > max_rows
    )
    limited = records if max_rows is None or defer_truncation else records[:max_rows]

    # ServiceReportSpec intentionally matches the classification-facing subset
    # of ReportSpec. The cast keeps classify_columns' existing public signature
    # stable while both kinds use its fail-closed undeclared-column behavior.
    col_classes = classify_columns(cast(ReportSpec, spec), columns)
    # A report whose rows carry currency_code is authoritative about its own
    # denomination, including when the answer is "no single one" — defaulting to
    # a currency there would relabel a segmented result as one currency. Reports
    # with no currency_code column say nothing either way and keep the unknown
    # default.
    # Resolved over every fetched row, not the truncated page: a mixed-currency
    # result whose first `max_rows` happen to share one currency would otherwise
    # advertise it confidently while the other currency's rows sit past the cap.
    #
    # The claim is scoped to the rows in THIS response, which is what the
    # envelope field means and what `has_more` already qualifies. `records` is
    # what the cursor fetched — max_rows + 1, one more than is returned — so
    # agreement across it is always true of the returned rows, never a guess
    # about rows past the fetch boundary. Withholding it whenever a result
    # truncates would cost every large single-currency report a correct label
    # and buy no safety, since the narrower claim was never wrong.
    declares_currency = "currency_code" in columns
    return CatalogReportExecution(
        on_converted=spec.on_converted,
        report_id=spec.report_id,
        parameters=MappingProxyType(dict(parameters)),
        sql=sql,
        records=limited,
        columns=columns,
        column_types=column_types,
        output_classes=col_classes,
        tier=derive_query_tier(col_classes),
        total_count=(max_rows + 1)
        if truncated and max_rows is not None
        else len(limited),
        truncated=truncated,
        actions=actions or [],
        period=period,
        semantics=spec.semantics,
        provenance=spec.semantics.provenance,
        # `None` is also the field default, so a report that declares no
        # currency column lands on the unknown default either way.
        display_currency=(
            _resolve_display_currency(records) if declares_currency else None
        ),
    )


def masked_columns(output_classes: Mapping[str, DataClass]) -> tuple[str, ...]:
    """The columns whose class masks their value, in declaration order.

    Measured from the class's own transform rather than from a tier comparison:
    all five CRITICAL classes share a tier but two of them mask only partially,
    and below CRITICAL every transform is passthrough except FLOORED. The
    transform is what the reader actually sees.
    """
    return tuple(
        name
        for name, data_class in output_classes.items()
        if mask_strength(data_class) > MaskStrength.PASSTHROUGH
    )


def inspection_hint(report_id: str, columns: tuple[str, ...]) -> str:
    """R3's masked-output hint — a ``'*****'`` with no explanation is a two-call fix.

    Names the CLI command, not an MCP tool: R3 requires the hint bind only to an
    admitted surface, the verify surface has no MCP identity, and pointing an
    agent at a tool that does not exist is worse than saying nothing.
    """
    return (
        f"Run `moneybin reports explain {report_id}` to see the derived class of "
        f"each column and why {', '.join(columns)} "
        f"{'is' if len(columns) == 1 else 'are'} masked"
    )


def redact_catalog_execution(
    spec: _CatalogSpec,
    execution: CatalogReportExecution,
) -> CatalogReportResult:
    """Apply the existing terminal report redaction to a raw execution."""
    redacted = redact_records(
        execution.records,
        execution.output_classes,
        consent=None,
    )

    # R3: masked output self-explains. Appended rather than inserted — a runner
    # that positions its own hints did so deliberately.
    masked = masked_columns(execution.output_classes)
    actions = list(execution.actions)
    if masked:
        actions.append(inspection_hint(execution.report_id, masked))

    return CatalogReportResult(
        report_id=execution.report_id,
        parameters=redact_report_parameters(spec, execution.parameters),
        semantics=execution.semantics,
        provenance=execution.provenance,
        records=redacted,
        columns=execution.columns,
        output_classes=execution.output_classes,
        tier=execution.tier,
        total_count=execution.total_count,
        truncated=execution.truncated,
        actions=actions,
        period=execution.period,
        display_currency=execution.display_currency,
        degraded=execution.degraded_reason is not None,
        degraded_reason=execution.degraded_reason,
        applied_rates=execution.applied_rates,
        recovery_actions=execution.recovery_actions,
    )


def execute_catalog_report(
    spec: ReportSpec,
    db: Database,
    *,
    max_rows: int | None,
    defer_truncation: bool = False,
    **params: Any,
) -> CatalogReportExecution:
    """Execute a catalog runner once; do not apply terminal redaction."""
    rq = spec.runner(db, **params)
    # `list()` on a Mapping yields its KEYS, which would bind parameter names as
    # values. A named-binding runner's params must reach DuckDB as the mapping.
    # Each entry is unwrapped from its `Binding`: the class travels with the value
    # for the provenance renderer, and DuckDB is handed the value alone.
    bindings = (
        {name: bound_value(item) for name, item in rq.params.items()}
        if isinstance(rq.params, Mapping)
        else [bound_value(item) for item in rq.params]
    )
    try:
        cursor = db.execute(rq.sql, bindings)
    except duckdb.Error as e:
        # A saved report's SQL is user-authored, and DuckDB's binder error echoes
        # the statement it failed to bind — inline literals included. The MCP
        # wrapper masks an unclassified failure, but `handle_cli_errors` re-raises
        # one, so an upstream rename would print a routing number in a CLI
        # traceback. Classify it here, where every tier's rows pass through, and
        # keep only the exception type and the digest in the log.
        logger.warning(
            f"report execution failed: {type(e).__name__} "
            f"(report_id={spec.report_id}, sql sha256={sql_digest(rq.sql)})"
        )
        raise UserError(
            "The report's query could not run against the current schema. An "
            "upstream table or column it reads may have been renamed or removed; "
            "`moneybin reports explain` shows what it reads.",
            code=error_codes.REPORT_QUERY_EXECUTION_FAILED,
        ) from e
    descriptions = cursor.description or []
    columns = [str(description[0]) for description in descriptions]
    column_types = [
        str(description[1]) if len(description) > 1 else "UNKNOWN"
        for description in descriptions
    ]
    rows = cursor.fetchall() if max_rows is None else cursor.fetchmany(max_rows + 1)
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return build_catalog_execution(
        spec,
        parameters=cast(Mapping[str, JsonValue], params),
        sql=rq.sql,
        records=records,
        columns=columns,
        column_types=column_types,
        actions=list(rq.actions),
        period=rq.period,
        max_rows=max_rows,
        defer_truncation=defer_truncation,
    )


def run_report(
    spec: ReportSpec, db: Database, *, max_rows: int, **params: Any
) -> CatalogReportResult:
    """Execute ``spec``'s runner with ``params`` and return redacted results.

    Fetches one extra row to detect truncation, classifies each output column
    via the report's view, and masks CRITICAL columns before returning.
    """
    execution = execute_catalog_report(spec, db, max_rows=max_rows, **params)
    return redact_catalog_execution(spec, execution)
