"""The second ``ReportSpec`` constructor: one stored row becomes one report.

``introspect.build_spec`` builds a spec from a decorated runner; this builds the
same frozen dataclass from an ``app.user_reports`` row. Everything downstream of
``ReportSpec`` — ``run_report``, ``classify_columns``, ``redact_records``, the
envelope, the catalog projections, exports — is shared by all three tiers and
stays unaware of which constructor built the spec. That is the whole claim of
``docs/specs/reports-dynamic.md``: a second constructor, not a second pattern.

R4's drift handling lives here because it decides which class map the spec
carries: a matching fingerprint serves the stored map, a mismatch re-resolves.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Final

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import USER_REPORT_DRIFT_DETECTED_TOTAL
from moneybin.privacy.redaction import is_safe_to_publish_verbatim
from moneybin.privacy.sql_lineage import (
    FAIL_CLOSED_CLASS,
    SchemaSnapshot,
    SqlParseError,
    get_current_schema_snapshot,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ParamSpec,
    ReportQuery,
    ReportSemantics,
    ReportSpec,
    Runner,
)
from moneybin.reports._framework.derive import (
    annotation_of,
    class_fingerprint,
    derive_classification,
    drifted_names,
    json_scalar,
    read_tables,
    token_of,
    typed_value,
    with_downgrades,
)

logger = logging.getLogger(__name__)

#: Leading token on ``degraded_reason`` for a stale class map. ``degraded`` also
#: carries the no-consent meaning, and two meanings on one flag with no way to
#: tell them apart is not acceptable — this is the discriminator R4 requires.
DEGRADED_STALE_CLASSIFICATION: Final = "stale_classification"

#: Leading token on ``degraded_reason`` when the stored SQL can no longer be
#: classified at all — an upstream table or column it reads is gone.
DEGRADED_UNRESOLVABLE_QUERY: Final = "unresolvable_query"

#: Leading token on ``degraded_reason`` when a stored token no longer decodes —
#: a renamed ``DataClass`` or a retired parameter type.
DEGRADED_UNREADABLE_ROW: Final = "unreadable_row"

#: Leading token on ``degraded_reason`` when undecided duplicate-match proposals
#: leave both rows of a pair in the ledger, so a total over them is provisional
#: (issue #409). Attached by the catalog to every report downstream of
#: ``core.fct_transactions``, packaged or saved.
DEGRADED_PENDING_DEDUP: Final = "pending_dedup_decisions"


@dataclass(frozen=True, slots=True)
class DynamicReport:
    """A spec built from a stored row, plus the row state the spec cannot carry.

    ``archived`` and the two ``degraded`` fields are facts about the *row*, not
    about the report's contract, which is why they live here and not on
    ``ReportSpec`` — a spec that could claim to be archived would be a spec whose
    provenance its own field contradicts.
    """

    spec: ReportSpec
    archived: bool = False
    degraded: bool = False
    degraded_reason: str | None = None
    #: The reason without the names — the leading token of ``degraded_reason``,
    #: carried as its own field rather than parsed back out of the sentence.
    #: A redacted export publishes this, since the sentence names the author's
    #: own columns and a stored exception message can quote their SQL back.
    degraded_code: str | None = None


def unknown_semantics(*, provenance: tuple[str, ...] = ()) -> ReportSemantics:
    """Financial semantics for a user query: explicitly unknown.

    MoneyBin cannot derive ``unit``, ``sign``, ``kind`` or the rest from an
    arbitrary ``SELECT``, and defaulting them would publish a claim about the
    user's query that nobody made — an agent reading ``sign: "natural"`` on a
    report whose author flipped the sign gets a confidently wrong answer, which
    is worse than getting none.

    ``provenance`` is the one field that *is* derivable, so it is passed in from
    the SQL rather than stored: it names the tables the query reads, and storing
    a copy would let it drift from the query it describes.
    """
    return ReportSemantics(
        unit=None,
        currency=None,
        sign=None,
        kind="unknown",
        valuation_basis=None,
        fx_basis=None,
        time_basis=None,
        denominator=None,
        comparison_window=None,
        exclusions=(),
        provenance=provenance,
    )


def stored_params(
    params: Sequence[ParamSpec], parameter_classes: Mapping[str, DataClass]
) -> list[dict[str, Any]]:
    """Render declared parameters as the ``params`` JSON column.

    An unresolved class is **omitted**, never written: ``taxonomy.py`` notes
    that declaring a column unresolved defeats the completeness tests that exist
    to find gaps, and the same holds for a parameter. Absent means unresolved.
    """
    entries: list[dict[str, Any]] = []
    for parameter in params:
        entry: dict[str, Any] = {
            "name": parameter.name,
            "annotation": token_of(parameter.annotation),
        }
        if not parameter.required:
            entry["default"] = json_scalar(parameter.default)
        if parameter.help:
            entry["help"] = parameter.help
        data_class = parameter_classes.get(parameter.name, FAIL_CLOSED_CLASS)
        if data_class is not FAIL_CLOSED_CLASS:
            entry["data_class"] = data_class.value
        entries.append(entry)
    return entries


def spec_from_row(
    db: Database, row: Mapping[str, Any], *, snapshot: SchemaSnapshot | None = None
) -> DynamicReport:
    """Build one report from a stored ``app.user_reports`` row.

    ``row``'s JSON columns arrive decoded, as ``UserReportsRepo`` returns them.
    ``snapshot`` lets a caller building many specs read the live schema once.

    Reads never persist a refreshed fingerprint: both adapters run reports inside
    ``get_database(read_only=True)``, and every ``app.user_reports`` mutation
    goes through the audited repo, so refreshing here would emit an audit row per
    read. A stale fingerprint therefore costs a re-resolution and nothing else.
    """
    query_sql = str(row["query_sql"])
    stored: Mapping[str, str] = row.get("classes") or {}
    try:
        declared = declared_params(row.get("params") or ())
        stored_classes = {name: DataClass(value) for name, value in stored.items()}
        stored_parameter_classes = {
            parameter.name: parameter.data_class for parameter in declared
        }
        downgrades: Mapping[str, Mapping[str, str]] = row.get("class_downgrades") or {}

        current = class_fingerprint(
            db,
            query_sql=query_sql,
            classes=stored_classes,
            parameter_classes=stored_parameter_classes,
            class_downgrades=downgrades,
            snapshot=snapshot,
        )
        provenance = read_tables(db, query_sql, snapshot=snapshot)
    except (UserError, ValueError, KeyError, SqlParseError) as e:
        # Every step that reads stored *text* — the class and annotation tokens,
        # and the stored SQL the fingerprint and provenance both parse. All are
        # written from allowlists by this repo, so this becomes reachable the
        # moment a release renames a `DataClass`, retires a parameter type, or
        # ships a parser that no longer accepts text an earlier one wrote — and
        # then it is every saved row at once. Letting it escape takes down the
        # whole catalog, built-ins included (one unparseable row would break
        # `reports list`, `reports run`, `export report` and the `reports` MCP
        # tool for every tier), which is worse than any answer this row can give.
        return _unreadable_row(row, e)
    unresolvable: UserError | None = None
    if current == str(row.get("class_fingerprint") or ""):
        classes, parameter_classes, changed = (
            stored_classes,
            stored_parameter_classes,
            (),
        )
    else:
        try:
            classes, parameter_classes, changed = _reresolved(
                db,
                query_sql=query_sql,
                declared=declared,
                stored_classes=stored_classes,
                stored_parameter_classes=stored_parameter_classes,
                downgrades=downgrades,
            )
        except UserError as e:
            # The report can no longer be classified — a table or column it
            # reads is gone. Dropping it from the catalog would hide the user's
            # work behind an upstream change they did not make, so it stays
            # listed and wholly masked. Running it fails on DuckDB's own error.
            unresolvable = e
            classes = dict.fromkeys(stored_classes, FAIL_CLOSED_CLASS)
            parameter_classes = {
                parameter.name: FAIL_CLOSED_CLASS for parameter in declared
            }
            changed = tuple(sorted(classes))

    # One list, bound twice: `spec.params` publishes each parameter's governing
    # class and `runner` binds it. Closing the runner over `declared` instead
    # would hand the provenance renderer the stale pre-drift class, so `explain`
    # would print a filter value as a literal inside the same response that calls
    # the parameter unresolved and masks every row.
    classed = _classed_params(declared, parameter_classes)
    spec = ReportSpec(
        report_id=str(row["report_id"]),
        name=str(row["name"]),
        description=str(row.get("description") or ""),
        # No `reports.*` view backs a query-time report, so it can never be
        # `kind FULL` or join scheduled refresh. Promotion is M2P.3.
        view=None,
        runner=_synthesized_runner(query_sql, classed),
        classes=classes,
        columns=tuple(
            # `__post_init__` compares columns against classes on name and class
            # only, so the description is free; the result column name is the
            # most honest thing to put there.
            OutputColumn(name=name, description=name, data_class=data_class)
            for name, data_class in classes.items()
        ),
        semantics=unknown_semantics(provenance=provenance),
        params=classed,
        class_downgrades={
            column: str(entry.get("reason", "")) for column, entry in downgrades.items()
        },
    )
    archived = not row.get("is_active", True)
    if unresolvable is not None:
        return DynamicReport(
            spec=spec,
            archived=archived,
            degraded=True,
            degraded_reason=(
                f"{DEGRADED_UNRESOLVABLE_QUERY}: {unresolvable}; every column is "
                "masked until the report's SQL is updated."
            ),
            degraded_code=DEGRADED_UNRESOLVABLE_QUERY,
        )
    if not changed:
        return DynamicReport(spec=spec, archived=archived)
    return DynamicReport(
        spec=spec,
        archived=archived,
        degraded=True,
        degraded_reason=(
            f"{DEGRADED_STALE_CLASSIFICATION}: upstream classification changed for "
            f"{', '.join(changed)}; serving them masked until the report is saved again."
        ),
        degraded_code=DEGRADED_STALE_CLASSIFICATION,
    )


def _unreadable_row(row: Mapping[str, Any], error: Exception) -> DynamicReport:
    """A row whose stored tokens no longer decode: listed, wholly masked, unrunnable.

    Same answer the unresolvable-query branch gives, for the same reason —
    dropping the row would hide the user's work behind a release they did not
    choose. Every column masks, and no parameter is declared, so a run fails on
    DuckDB's own unbound-parameter error rather than executing with values whose
    declared types this build cannot read.
    """
    classes: dict[str, DataClass] = dict.fromkeys(
        row.get("classes") or {}, FAIL_CLOSED_CLASS
    )
    return DynamicReport(
        spec=ReportSpec(
            report_id=str(row["report_id"]),
            name=str(row["name"]),
            description=str(row.get("description") or ""),
            view=None,
            runner=_synthesized_runner(str(row["query_sql"]), ()),
            classes=classes,
            columns=tuple(
                OutputColumn(name=name, description=name, data_class=data_class)
                for name, data_class in classes.items()
            ),
            # Provenance is derived from the SQL, but this row already proved it
            # cannot be read as this build understands it; claiming a read set
            # would be the same guess in a different field.
            semantics=unknown_semantics(),
            params=(),
        ),
        archived=not row.get("is_active", True),
        degraded=True,
        # The exception's *type*, never its message. A parser error quotes the
        # fragment it choked on, so interpolating it republished the stored SQL —
        # inline literals included — through the MCP envelope, the JSON output,
        # and the CLI, beside a result whose every row is masked. The type still
        # separates a parse failure from a decode one, and names no content.
        degraded_reason=(
            f"{DEGRADED_UNREADABLE_ROW}: {type(error).__name__}; the stored report "
            "cannot be read by this version of MoneyBin, so every column is masked. "
            "Save it again to rebuild its contract."
        ),
        degraded_code=DEGRADED_UNREADABLE_ROW,
    )


def user_report_specs(db: Database) -> tuple[DynamicReport, ...]:
    """Every saved report, archived included, in stable ``report_id`` order.

    Archived rows are *always* built (R5): archiving suppresses catalog noise
    rather than revoking access, so an archived report must stay resolvable and
    runnable by ``report_id``. Filtering here instead — which is what an
    ``include_archived`` flag on this function invited — meant every caller that
    forgot to pass it made archived reports unreachable, and three of four did.
    Visibility is the *listing's* decision; see ``ReportCatalog.list``.

    The live schema is read once here rather than once per row: the snapshot's
    expensive build is memoised, but its two catalog queries are not.
    """
    from moneybin.repositories.user_reports_repo import UserReportsRepo

    snapshot = get_current_schema_snapshot(db)
    rows = UserReportsRepo(db).list(include_archived=True)
    return tuple(spec_from_row(db, row, snapshot=snapshot) for row in rows)


def declared_params(entries: Sequence[Mapping[str, Any]]) -> tuple[ParamSpec, ...]:
    """Rebuild ``ParamSpec``s from the stored ``params`` JSON."""
    params: list[ParamSpec] = []
    for entry in entries:
        annotation = annotation_of(str(entry.get("annotation", "str")))
        params.append(
            ParamSpec(
                name=str(entry["name"]),
                annotation=annotation,
                default=typed_value(entry.get("default"), annotation),
                # `required` is true exactly when no default was declared, which
                # is also what distinguishes an omitted default from a stored null.
                required="default" not in entry,
                help=str(entry.get("help", "")),
                data_class=(
                    DataClass(entry["data_class"])
                    if entry.get("data_class")
                    else FAIL_CLOSED_CLASS
                ),
            )
        )
    return tuple(params)


def _classed_params(
    declared: Sequence[ParamSpec], parameter_classes: Mapping[str, DataClass]
) -> tuple[ParamSpec, ...]:
    """Attach the governing class, dropping a default that is no longer safe.

    The save path *refuses* an above-LOW default so one can never be stored.
    Reclassification can still raise a stored parameter's class after the fact,
    and the catalog publishes defaults unmasked — so the same invariant is held
    here by making the parameter required instead of by trusting the write gate
    that was correct at the time.
    """
    classed: list[ParamSpec] = []
    dropped: list[str] = []
    for parameter in declared:
        data_class = parameter_classes.get(parameter.name, FAIL_CLOSED_CLASS)
        if not parameter.required and not is_safe_to_publish_verbatim(data_class):
            dropped.append(data_class.value)
            classed.append(
                replace(parameter, data_class=data_class, default=None, required=True)
            )
        else:
            classed.append(replace(parameter, data_class=data_class))
    if dropped:
        # A declared parameter's name is user-authored, exactly like an output
        # alias — `--param amazon_spend:str=…` is a name a user picks. The count
        # and the new classes carry the operational fact; the spec the caller
        # receives names which parameter became required.
        logger.warning(
            f"{len(dropped)} user report parameter(s) reclassified to "
            f"{', '.join(sorted(set(dropped)))}; dropping their stored defaults."
        )
    return tuple(classed)


def _reresolved(
    db: Database,
    *,
    query_sql: str,
    declared: Sequence[ParamSpec],
    stored_classes: Mapping[str, DataClass],
    stored_parameter_classes: Mapping[str, DataClass],
    downgrades: Mapping[str, Mapping[str, str]],
) -> tuple[dict[str, DataClass], dict[str, DataClass], tuple[str, ...]]:
    """Re-derive on a fingerprint mismatch and decide what the run may serve.

    Approved downgrades are reapplied *before* the comparison, because a
    legitimately downgraded report differs from raw derivation by design — and
    since reads never refresh the fingerprint, comparing raw derivation would
    leave it degraded from the first unrelated classification change onward.

    A downgrade is reapplied **only where the derived class still equals the one
    it was approved against**. Reapplying by column name alone would let an
    approval collected against a weak class silently suppress a stronger one,
    which is the inverse of what the downgrade was reviewed for.

    ``drifted_names`` decides what moved, and its docstring carries the rule:
    any movement in either direction. The cost is real and worth naming: one
    upstream reclassification (``account_id``'s ``ACCOUNT_IDENTIFIER →
    RECORD_ID``, say) masks that column on every saved report reading the table
    until each is saved again, because reads never refresh the fingerprint.
    ``.claude/rules/reports.md`` describes this as failing closed on a column
    that moved "upward"; the code is deliberately stricter.
    """
    # Defaults are stripped for derivation: they cannot affect any class, and
    # leaving them on would let `_refuse_sensitive_defaults` raise on a *read*
    # for a report whose default was legal when it was stored. That is the
    # run-path face of the same rule, and `_classed_params` applies it.
    derived = derive_classification(
        db,
        query_sql=query_sql,
        params=tuple(
            replace(parameter, default=None, required=True) for parameter in declared
        ),
    )

    reapplied = with_downgrades(dict(derived.classes), downgrades)

    # Nothing can be masked for a column that no longer exists; saying the
    # contract moved is the whole of the answer here. `drifted_names` owns that
    # rule and the both-directions comparison, shared with the downgrade path.
    changed_columns = drifted_names(reapplied, stored_classes)
    # The same comparison, so a parameter cannot be held to a laxer rule than a
    # column: walking the derived map alone never visited a stored parameter the
    # derivation no longer produces.
    changed_parameters = drifted_names(
        derived.parameter_classes, stored_parameter_classes
    )
    if not changed_columns and not changed_parameters:
        USER_REPORT_DRIFT_DETECTED_TOTAL.labels(resolution="equal").inc()
        return reapplied, dict(derived.parameter_classes), ()

    USER_REPORT_DRIFT_DETECTED_TOTAL.labels(resolution="failed_closed").inc()
    logger.warning(
        "user report classification drifted; failing closed for "
        f"{len(changed_columns)} column(s) and {len(changed_parameters)} parameter(s)"
    )
    return (
        {
            name: FAIL_CLOSED_CLASS if name in changed_columns else data_class
            for name, data_class in reapplied.items()
        },
        {
            name: FAIL_CLOSED_CLASS if name in changed_parameters else data_class
            for name, data_class in derived.parameter_classes.items()
        },
        changed_columns + changed_parameters,
    )


def _synthesized_runner(query_sql: str, params: Sequence[ParamSpec]) -> Runner:
    """Close over the stored SQL to produce the callable a decorator would.

    ``run_report`` calls exactly ``spec.runner(db, **params)``, so this is what
    makes a stored row execute through the same path as a built-in. Binding is
    **by name** (R8): positional storage would need a name→position map beside
    the SQL, and editing that SQL to add a ``WHERE`` clause shifts every
    subsequent position — mis-binding arguments silently, producing wrong numbers
    rather than an error. Named binding cannot express that failure.
    """
    declared = {parameter.name: parameter for parameter in params}

    def run(_db: Database, **values: Any) -> ReportQuery:
        unknown = sorted(set(values) - set(declared))
        if unknown:
            raise UserError(
                "This report does not declare one of the supplied parameters.",
                code=error_codes.REPORT_PARAMETER_UNKNOWN,
                hint="Run `moneybin reports explain` to see what it declares.",
                details={"unknown": unknown, "declared": sorted(declared)},
            )
        # The class on each binding is the one derived from the column the
        # placeholder is compared against (R9) — never declared by the user, and
        # never recovered from the signature. The provenance renderer reads it
        # here to decide whether the value may be rendered as a literal.
        bindings: dict[str, Binding] = {}
        missing: list[str] = []
        for name, parameter in declared.items():
            if name in values:
                bindings[name] = Binding(values[name], parameter.data_class)
            elif parameter.required:
                missing.append(name)
            else:
                bindings[name] = Binding(parameter.default, parameter.data_class)
        if missing:
            raise UserError(
                "A required report parameter was not supplied.",
                code=error_codes.REPORT_PARAMETER_MISSING,
                details={"missing": sorted(missing)},
            )
        return ReportQuery(sql=query_sql, params=bindings)

    return run
