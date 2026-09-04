"""One internal catalog for SQL-backed and service-backed reports."""

from __future__ import annotations

import logging
import re
import types
import typing
from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Final, Literal, cast, get_args, get_origin

from pydantic import JsonValue, TypeAdapter

from moneybin import error_codes
from moneybin.database import (
    Database,
    DatabaseNotInitializedError,
    get_database,
)
from moneybin.errors import RecoveryAction, UserError
from moneybin.matching.persistence import count_pending_matches
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.metrics.registry import USER_REPORT_RUNS_TOTAL
from moneybin.privacy.payloads.reports import (
    ReportCatalogEntry,
    ReportCatalogPayload,
    ReportOutputColumn,
    ReportResultPayload,
    ReportSemanticsPayload,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    USER_NAMESPACE,
    DefaultColumns,
    OutputColumn,
    ParamSpec,
    RecomputeDerived,
    ReportSemantics,
    ReportSpec,
    validate_default_columns,
)
from moneybin.reports._framework.derive import json_scalar, typed_value
from moneybin.reports._framework.dynamic import DEGRADED_PENDING_DEDUP
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    CatalogReportResult,
    convert_execution,
    execute_catalog_report,
    redact_catalog_execution,
    truncate_execution,
)
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.services.currency_service import (
    build_cache_only_currency_service,
    require_currency,
)
from moneybin.sqlmesh_registry import relations_downstream_of
from moneybin.tables import FCT_TRANSACTIONS

logger = logging.getLogger(__name__)


def profile_home_currency(db: Database) -> str | None:
    """The currency to price a report in when the caller named none.

    Every report surface resolves the default through this one function, so a
    new surface inherits Requirement 9 by spelling it the same way. ``None`` is
    a real answer, not a missing one: a user who has chosen no home currency
    gets their amounts in the currencies they are actually in, never a guess.
    """
    return ProfileSettingsRepo(db).get_home_currency()


def _conversion_target(
    display_currency: str | None, home_currency: str | None
) -> str | None:
    """The currency to price into, refusing a request that names none.

    Resolved *before* the rows are read, because the row loop is not a
    validator: ``convert_records`` reaches ``resolve_rate`` only when there is a
    row to price, so a report returning nothing would otherwise label itself
    denominated in whatever string arrived.

    Only the caller's own request is refused. A malformed ``home_currency`` is a
    standing profile setting rather than this call's ask, and raising on one
    would fail every money-bearing report the profile owns until it is fixed —
    including the reads that would show what is wrong. It falls back silently,
    on the same rule every other defaulted-currency fallback follows.
    """
    if display_currency is not None:
        return require_currency(display_currency)
    if home_currency is None:
        return None
    try:
        return require_currency(home_currency)
    except UserError:
        return None


def merged_degraded_reason(*reasons: str | None) -> str | None:
    """Every stated reason a result is degraded, or ``None`` if it is not."""
    stated = [reason for reason in reasons if reason]
    return "; ".join(stated) if stated else None


#: The MCP half of the pending-duplicate next step: the queue an agent can read
#: and decide without parsing the CLI hint beside it.
_PENDING_DEDUP_RECOVERY: Final = RecoveryAction(
    tool="reviews",
    arguments={"kind": "matches", "status": "pending"},
    rationale=(
        "Undecided duplicate matches leave both rows of each pair in the ledger; "
        "decide them with reviews_decide and totals over them stop being "
        "provisional."
    ),
    confidence="suggested",
    idempotent=True,
)


def _reads_transactions(provenance: Iterable[str]) -> bool:
    """Whether any relation a report reads is fed by ``core.fct_transactions``."""
    downstream = relations_downstream_of(FCT_TRANSACTIONS.full_name)
    return any(relation.lower() in downstream for relation in provenance)


def pending_dedup_reason(db: Database) -> str | None:
    """The caveat a total over an undecided duplicate pair owes its reader (#409).

    Counted per profile, not per row: a report returns aggregates, so which
    transactions it summed is not recoverable from its result, and warning wider
    is the safe direction of that imprecision.
    """
    pending = count_pending_matches(db, match_type="dedup")
    if pending == 0:
        return None
    verb = "match leaves its" if pending == 1 else "matches leave their"
    return (
        f"{DEGRADED_PENDING_DEDUP}: {pending} undecided duplicate {verb} "
        "transactions unmerged, so totals over them are provisional"
    )


_REPORT_ID = re.compile(r"[a-z][a-z0-9_-]*:[a-z][a-z0-9_-]*")

type ReportTier = Literal["builtin", "extension", "user"]


@dataclass(frozen=True, slots=True)
class ServiceReportSpec:
    """Immutable catalog metadata and executor for one service-backed report."""

    report_id: str
    name: str
    description: str
    parameters: tuple[ParamSpec, ...]
    columns: tuple[OutputColumn, ...]
    semantics: ReportSemantics
    classes: Mapping[str, DataClass]
    examples: tuple[str, ...]
    executor: Callable[
        [Database, Mapping[str, JsonValue], int | None], CatalogReportExecution
    ]
    validator: Callable[[Mapping[str, JsonValue]], None] | None = None
    on_converted: RecomputeDerived | None = None
    """Same contract as ``ReportSpec.on_converted`` — see the note there."""
    default_columns: DefaultColumns | None = None
    """Same contract as ``ReportSpec.default_columns`` — see the note there."""

    def __post_init__(self) -> None:
        if _REPORT_ID.fullmatch(self.report_id) is None:
            raise ValueError("report_id must use namespace:name")
        declared = {column.name: column.data_class for column in self.columns}
        if len(declared) != len(self.columns) or declared != dict(self.classes):
            raise ValueError(
                "columns and classes must declare the same output fields "
                "with identical privacy classes"
            )
        validate_default_columns(self.default_columns, self.columns)
        object.__setattr__(self, "classes", MappingProxyType(dict(self.classes)))


type RegisteredReport = ReportSpec | ServiceReportSpec


@dataclass(frozen=True, slots=True)
class ReportStatus:
    """Stored-row state a ``ReportSpec`` cannot carry, keyed by ``report_id``.

    Only the user tier has any: a built-in is a file in the repo, so it can be
    neither archived nor stale. Keeping it beside the catalog rather than on the
    spec is what lets one catalog both *resolve* an archived report and *hide* it
    from a listing — and what carries R4's drift reason to the run envelope,
    which is the boundary a caller actually reads.
    """

    archived: bool = False
    degraded: bool = False
    degraded_reason: str | None = None
    #: ``degraded_reason`` without the user's own column names in it — see
    #: :class:`~moneybin.reports._framework.dynamic.DynamicReport`.
    degraded_code: str | None = None


_NO_STATUS: Final = ReportStatus()


class ReportCatalog:
    """Deterministic resolver and dispatcher for registered reports."""

    def __init__(
        self,
        reports: Iterable[RegisteredReport],
        *,
        status: Mapping[str, ReportStatus] | None = None,
    ) -> None:
        self._status: Mapping[str, ReportStatus] = MappingProxyType(dict(status or {}))
        ordered = tuple(sorted(reports, key=lambda report: report.report_id))
        duplicate_ids = sorted(
            report_id
            for report_id in {report.report_id for report in ordered}
            if sum(report.report_id == report_id for report in ordered) > 1
        )
        if duplicate_ids:
            raise ValueError(f"duplicate report_id: {', '.join(duplicate_ids)}")
        self._reports = ordered
        self._name_collisions = _name_collisions(ordered)
        for report_ids in self._name_collisions.values():
            # The name itself is withheld: a saved report is named by its user,
            # and `amazon-spend` is both a plausible name and a merchant name
            # `.claude/rules/security.md` forbids in a log file. The IDs are
            # identifiers, and they are what an operator acts on anyway.
            logger.warning(
                f"one report name is claimed by {len(report_ids)} reports "
                f"({', '.join(report_ids)}); each stays runnable by report_id."
            )

    def list(self, *, archived: bool | None = False) -> tuple[RegisteredReport, ...]:
        """Reports ordered by stable full ID, filtered by archived state.

        ``False`` (the default) is the active catalog, ``True`` the archived-only
        view, ``None`` everything. Visibility is decided *here* rather than when
        the catalog is built, because :meth:`resolve` must reach an archived
        report even when a listing must not show it — R5 makes archiving suppress
        catalog noise, not access.
        """
        if archived is None:
            return self._reports
        return tuple(
            report
            for report in self._reports
            if self.status(report.report_id).archived is archived
        )

    def status(self, report_id: str) -> ReportStatus:
        """Stored-row state for one report; all-default for the packaged tiers."""
        return self._status.get(report_id, _NO_STATUS)

    def name_collisions(self) -> Mapping[str, tuple[str, ...]]:
        """Names claimed by more than one report, mapped to the claiming IDs.

        R5's mutation-time checks cannot cover every collision: upgrading
        MoneyBin can add a built-in whose name a user already took, and neither
        path calls a lifecycle mutation. So the assembled registry is validated
        here rather than trusted, and a collision is surfaced instead of
        silently resolved — shadowing the user's report hides their work behind
        an upgrade they did not ask for, and shadowing the built-in makes a
        shipped report vanish with no visible cause. Both stay resolvable by
        ``report_id``; only the contested *name* stops resolving.
        """
        return self._name_collisions

    def resolve(self, report_id: str) -> RegisteredReport:
        """Resolve an exact full ID or an unambiguous short report name."""
        exact = [report for report in self._reports if report.report_id == report_id]
        if exact:
            return exact[0]

        short = [report for report in self._reports if report.name == report_id]
        if len(short) == 1:
            return short[0]
        if len(short) > 1:
            raise UserError(
                "Report ID is ambiguous.",
                code=error_codes.REPORT_ID_AMBIGUOUS,
                details={
                    "report_id": report_id,
                    "candidates": sorted(report.report_id for report in short),
                },
            )
        raise UserError(
            "Report not found.",
            code=error_codes.REPORT_ID_NOT_FOUND,
            details={"report_id": report_id},
        )

    def execute(
        self,
        db: Database,
        *,
        report_id: str,
        parameters: Mapping[str, JsonValue],
        limit: int,
        display_currency: str | None = None,
        home_currency: str | None = None,
    ) -> CatalogReportResult:
        """Validate parameters, then dispatch through the selected report kind.

        Both currencies price every money column in one currency (Requirement 9),
        and a pair that cannot be resolved from stored rates segments instead of
        failing — see ``convert_execution``. They differ in who asked, and so in
        what a fallback owes the caller: ``display_currency`` is the caller's own
        request and its fallback is explained, while ``home_currency`` is the
        profile's standing default and its fallback is silent. A default that
        announced itself would put a warning on every read of every report whose
        rates are not yet cached, and on every user-created report — which
        deliberately declares neither a currency nor a date column, so it can
        never convert. Callers resolve ``home_currency`` themselves (see
        ``profile_home_currency``) rather than having it read here, so a
        policy default stays the caller's to choose.
        """
        target = _conversion_target(display_currency, home_currency)
        spec, execution = self.execute_raw(
            db,
            report_id=report_id,
            parameters=parameters,
            limit=limit,
            # A conversion can change the row count — `core:networth` merges its
            # per-currency totals once they share a unit — so the cap has to
            # describe what that repair produced, not what fed it.
            defer_truncation=target is not None,
        )
        disclosed = execution.degraded_reason
        if target is not None:
            # Between execution and redaction, the only window where the rows
            # are both final and still numeric.
            execution = convert_execution(
                execution,
                to_currency=target,
                service=build_cache_only_currency_service(db),
            )
            # A requested currency's fallback is explained and a default's is
            # silent (see above); either way the caveat `execute_raw` attached
            # outlives the conversion, which only knows its own reason.
            execution = replace(
                execution,
                degraded_reason=merged_degraded_reason(
                    disclosed,
                    execution.degraded_reason if display_currency is not None else None,
                ),
            )
        execution = truncate_execution(execution)
        result = redact_catalog_execution(spec, execution)
        status = self.status(spec.report_id)
        if not status.degraded:
            return result
        # R4's drift reason belongs on the response, not only on the intermediate
        # object the catalog built the spec from: masked rows with no stated cause
        # are the failure the whole degraded flag exists to prevent. A conversion
        # that also degraded keeps its reason beside it rather than losing to it:
        # they are independent, and a reader told only about drift would think
        # the currency label was trustworthy.
        return replace(
            result,
            degraded=True,
            degraded_reason=merged_degraded_reason(
                status.degraded_reason, result.degraded_reason
            ),
        )

    def execute_raw(
        self,
        db: Database,
        *,
        report_id: str,
        parameters: Mapping[str, JsonValue],
        limit: int | None,
        defer_truncation: bool = False,
    ) -> tuple[RegisteredReport, CatalogReportExecution]:
        """Validate and execute one report without terminal redaction.

        ``defer_truncation`` returns the rows uncut and records the cap on the
        execution for ``truncate_execution`` to apply later. Callers that do not
        ask for it get an execution already capped, exactly as before.
        """
        spec, validated = self.resolve_request(
            report_id=report_id,
            parameters=parameters,
            limit=limit,
        )
        tier = report_tier(spec)
        try:
            if isinstance(spec, ReportSpec):
                execution = execute_catalog_report(
                    spec,
                    db,
                    max_rows=limit,
                    defer_truncation=defer_truncation,
                    **validated,
                )
            else:
                # A service executor builds every row it has before handing them
                # over, so withholding the cap costs it nothing and spares the
                # protocol a flag only one of its two implementations reads.
                execution = spec.executor(
                    db, validated, None if defer_truncation else limit
                )
        except Exception:
            USER_REPORT_RUNS_TOTAL.labels(tier=tier, outcome="error").inc()
            raise
        USER_REPORT_RUNS_TOTAL.labels(tier=tier, outcome="ok").inc()
        if _reads_transactions(execution.provenance):
            # Attached beneath every reading surface, so an export inherits the
            # caveat without asking for it (#409).
            reason = pending_dedup_reason(db)
            if reason is not None:
                # The CLI half is `PENDING_MATCHES_HINT` verbatim, so it cannot
                # drift from the command a test executes; imported here because
                # the service module drags the matching engine into every catalog
                # import.
                from moneybin.services.matching_service import (  # noqa: PLC0415
                    PENDING_MATCHES_HINT,
                )

                execution = replace(
                    execution,
                    degraded_reason=merged_degraded_reason(
                        execution.degraded_reason, reason
                    ),
                    actions=[*execution.actions, PENDING_MATCHES_HINT],
                    recovery_actions=(
                        *execution.recovery_actions,
                        _PENDING_DEDUP_RECOVERY,
                    ),
                )
        if defer_truncation:
            execution = replace(execution, pending_limit=limit)
        return spec, execution

    def resolve_request(
        self,
        *,
        report_id: str,
        parameters: Mapping[str, JsonValue],
        limit: int | None,
    ) -> tuple[RegisteredReport, dict[str, JsonValue]]:
        """Resolve and validate one request without executing its report."""
        if limit is not None and limit < 0:
            raise UserError(
                "Report limit must be non-negative.",
                code=error_codes.REPORT_LIMIT_INVALID,
                details={"minimum": 0},
            )
        spec = self.resolve(report_id)
        validated = validate_report_parameters(spec, parameters)
        if isinstance(spec, ServiceReportSpec) and spec.validator is not None:
            spec.validator(validated)
        return spec, validated


def _name_collisions(
    reports: Sequence[RegisteredReport],
) -> Mapping[str, tuple[str, ...]]:
    """Group report IDs by any name more than one of them claims."""
    by_name: dict[str, list[str]] = defaultdict(list)
    for report in reports:
        by_name[report.name].append(report.report_id)
    return MappingProxyType({
        name: tuple(report_ids)
        for name, report_ids in sorted(by_name.items())
        if len(report_ids) > 1
    })


def report_tier(report: RegisteredReport) -> ReportTier:
    """Which of R5's three tiers ``report`` belongs to.

    Keyed on the ``report_id`` namespace for the user tier and on the extension
    registry for the rest, because a spec carries no tier field — and adding one
    would let a spec claim a tier its provenance contradicts.
    """
    from moneybin.reports._framework.registry import extension_report_specs

    if report.report_id.startswith(f"{USER_NAMESPACE}:"):
        return "user"
    if any(
        extension.report_id == report.report_id
        for extension in extension_report_specs()
    ):
        return "extension"
    return "builtin"


def _parameter_specs(spec: RegisteredReport) -> tuple[ParamSpec, ...]:
    if isinstance(spec, ReportSpec):
        return spec.params
    return spec.parameters


def validate_report_parameters(
    spec: RegisteredReport,
    supplied: Mapping[str, JsonValue],
) -> dict[str, JsonValue]:
    """Reject unknown/missing/mistyped parameters and fill declared defaults.

    Public because the verify surface validates the same way before asking a
    runner to build its SELECT — one validator, so an explanation can never
    accept a request an execution would refuse.
    """
    declared = _parameter_specs(spec)
    declared_by_name = {parameter.name: parameter for parameter in declared}

    unknown = sorted(set(supplied) - set(declared_by_name))
    if unknown:
        raise UserError(
            "Unknown report parameter.",
            code=error_codes.REPORT_PARAMETER_UNKNOWN,
            details={"report_id": spec.report_id, "parameters": unknown},
        )

    missing = sorted(
        parameter.name
        for parameter in declared
        if parameter.required and parameter.name not in supplied
    )
    if missing:
        raise UserError(
            "Required report parameter is missing.",
            code=error_codes.REPORT_PARAMETER_MISSING,
            details={"report_id": spec.report_id, "parameters": missing},
        )

    validated: dict[str, JsonValue] = {}
    for parameter in declared:
        raw = (
            supplied[parameter.name]
            if parameter.name in supplied
            else parameter.default
        )
        # `date` and `decimal` are declarable but have no JSON form, so a JSON
        # caller can only send text where the report declared an object. Coerce
        # before the type check or those two types stay CLI-only — the CLI's own
        # binder builds real objects from `--param` strings.
        value = cast(JsonValue, typed_value(raw, parameter.annotation))
        if not _matches_annotation(value, parameter.annotation):
            raise UserError(
                "Report parameter has an invalid type.",
                code=error_codes.REPORT_PARAMETER_INVALID_TYPE,
                details={
                    "report_id": spec.report_id,
                    "parameter": parameter.name,
                    "expected": _annotation_name(parameter.annotation),
                },
            )
        validated[parameter.name] = value
    return validated


def _matches_annotation(value: object, annotation: object) -> bool:
    """Strictly match JSON-native values against one introspected annotation."""
    if annotation is None or annotation is Any:
        return True
    if annotation is type(None):
        return value is None

    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (types.UnionType, typing.Union):
        return any(_matches_annotation(value, arg) for arg in args)
    if origin is Literal:
        return value in args and any(type(value) is type(arg) for arg in args)
    if origin is list:
        item_type = args[0] if args else Any
        return isinstance(value, list) and all(
            _matches_annotation(item, item_type) for item in cast(list[object], value)
        )
    if origin is dict:
        key_type, value_type = args if args else (Any, Any)
        return isinstance(value, dict) and all(
            _matches_annotation(key, key_type) and _matches_annotation(item, value_type)
            for key, item in cast(dict[object, object], value).items()
        )
    if annotation is bool:
        return type(value) is bool
    if annotation is int:
        return type(value) is int
    if annotation is float:
        return type(value) in (int, float)
    if annotation is str:
        return isinstance(value, str)
    if isinstance(annotation, type):
        return isinstance(value, annotation)
    return False


def _annotation_name(annotation: object) -> str:
    if annotation is None or annotation is Any:
        return "any"
    if annotation is type(None):
        return "None"
    if isinstance(annotation, type):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def get_report_catalog(db: Database | None = None) -> ReportCatalog:
    """Build the union of every registered report across R5's three tiers.

    ``db`` adds the user tier — one ``ReportSpec`` per ``app.user_reports`` row,
    **archived rows included**. Pass it on any path that resolves a
    **caller-supplied** report reference, so a saved report is reachable by the
    same call a built-in is. Omitting it yields the packaged tiers alone, which is
    what extension registration needs (it runs before any database is open) and
    what a built-in's own generated CLI command needs (it resolves one fixed ID it
    already holds).

    Archived visibility is not a parameter here on purpose: an
    ``include_archived`` flag on the *builder* meant every caller had to remember
    to pass it to keep an archived report runnable, and three of four did not.
    ``ReportCatalog.list`` owns visibility instead.
    """
    from moneybin.reports._framework.dynamic import user_report_specs
    from moneybin.reports._framework.registry import (
        extension_report_specs,
        spec_of,
    )
    from moneybin.reports.definitions import ALL_REPORTS
    from moneybin.reports.service_reports import SERVICE_REPORTS

    core = (spec_of(runner) for runner in ALL_REPORTS)
    user = user_report_specs(db) if db is not None else ()
    return ReportCatalog(
        (
            *core,
            *SERVICE_REPORTS,
            *extension_report_specs(),
            *(report.spec for report in user),
        ),
        status={
            report.spec.report_id: ReportStatus(
                archived=report.archived,
                degraded=report.degraded,
                degraded_reason=report.degraded_reason,
                degraded_code=report.degraded_code,
            )
            for report in user
        },
    )


@contextmanager
def open_report_catalog() -> Generator[tuple[ReportCatalog, Database | None]]:
    """The full catalog over an open connection, or the packaged tiers alone.

    Browsing the catalog and resolving a report id are repo-metadata questions
    for two of the three tiers, and a profile with no database file has no saved
    reports to add. Requiring one turned "here are the eight built-in reports"
    into a database-not-initialized error for an agent orienting itself, and
    turned a mistyped ``export report`` id into advice to run ``db init``.

    ``DatabaseNotInitializedError`` is the only degradation admitted. A locked or
    wrong-key database is a real failure the caller must see, not a catalog with
    a silently missing tier.
    """
    # The open is guarded, the caller's body is not: wrapping the whole `with` in
    # the `except` would swallow the same error raised inside the body and then
    # yield a second time.
    try:
        opened = get_database(read_only=True)
    except DatabaseNotInitializedError:
        logger.info("no database yet; serving the packaged report tiers only")
        yield get_report_catalog(), None
        return
    with opened as db:
        yield get_report_catalog(db), db


def catalog_to_payload(
    catalog: ReportCatalog, *, include_archived: bool = False
) -> ReportCatalogPayload:
    """Expose the catalog's static metadata for one listing view.

    ``include_archived`` widens the listing rather than replacing it, matching
    ``accounts list --include-archived``: one answer to "show me the hidden ones"
    across the CLI, not one per command group. Each entry carries its own
    ``archived`` state, which is what makes widening safe — a caller reading a
    combined listing can still tell the two apart.
    """
    return ReportCatalogPayload(
        reports=[
            _catalog_entry_to_payload(
                report, archived=catalog.status(report.report_id).archived
            )
            for report in catalog.list(archived=_listing_view(include_archived))
        ]
    )


def _listing_view(include_archived: bool) -> bool | None:
    """The three-state selector arm one listing flag selects.

    ``None`` is every row, ``False`` the active ones. No listing surface asks for
    the archived-only arm, so the flag maps onto two of the three.
    """
    return None if include_archived else False


def catalog_sensitivity(
    entries: Sequence[ReportCatalogEntry],
) -> Literal["low", "medium"]:
    """The envelope sensitivity a listing of ``entries`` actually carries.

    A built-in's name and description are authored in the repo and reviewed, so
    the entry fields are annotated ``AGGREGATE``. A **user** report's name and
    description are user-authored free text — ``USER_NOTE``, MEDIUM, the same
    class the stored columns carry — so a listing that includes one is not a LOW
    response. The annotations stay AGGREGATE deliberately (masking a user's own
    report name would make the catalog unusable); what has to be honest is the
    tier the envelope reports.

    Takes the rows being returned, not the catalog they came from. Reading the
    catalog left every caller-side narrowing to disagree with the envelope:
    ``reports list --tier builtin`` reported MEDIUM and ``user_note`` over rows
    that held neither, because the filter runs after the listing. Passing the
    response's own rows makes the pair structurally impossible to desync — the
    ``include_archived`` view no longer has to be repeated here either.
    """
    if any(entry.tier == "user" for entry in entries):
        return "medium"
    return "low"


def catalog_classes_returned(sensitivity: Literal["low", "medium"]) -> list[str]:
    """The data classes a catalog listing's own fields carry, for the audit event.

    Derived from the sensitivity rather than restated per surface, so the CLI and
    MCP listings cannot disagree about what they returned.
    """
    return (
        [DataClass.AGGREGATE.value]
        if sensitivity == "low"
        else [DataClass.AGGREGATE.value, DataClass.USER_NOTE.value]
    )


def result_to_payload(result: CatalogReportResult) -> ReportResultPayload:
    """Expose an already-redacted catalog result without touching executor inputs."""
    return ReportResultPayload(
        report_id=result.report_id,
        parameters={
            name: _thaw_parameter_metadata(value)
            for name, value in result.parameters.items()
        },
        columns=[
            ReportOutputColumn(
                name=name,
                data_class=result.output_classes[name].value,
            )
            for name in result.columns
        ],
        rows=result.records,
        semantics=_semantics_to_payload(result.semantics),
        period=result.period,
        sensitivity=tier_to_sensitivity(result.tier).value,
        count=result.total_count,
        truncated=result.truncated,
    )


def _catalog_entry_to_payload(
    report: RegisteredReport, *, archived: bool = False
) -> ReportCatalogEntry:
    return ReportCatalogEntry(
        report_id=report.report_id,
        name=report.name,
        tier=report_tier(report),
        description=report.description,
        archived=archived,
        parameter_schema=_parameter_schema(report),
        parameter_classes={
            parameter.name: parameter.data_class.value
            for parameter in _parameter_specs(report)
        },
        examples=list(report.examples),
        columns=[
            ReportOutputColumn(
                name=column.name,
                description=column.description,
                data_class=column.data_class.value,
            )
            for column in report.columns
        ],
        output_classes={
            name: data_class.value for name, data_class in report.classes.items()
        },
        semantics=_semantics_to_payload(report.semantics),
    )


def _parameter_schema(report: RegisteredReport) -> dict[str, JsonValue]:
    """Build the strict object schema published for one report's parameters."""
    properties: dict[str, JsonValue] = {}
    required: list[JsonValue] = []
    for parameter in _parameter_specs(report):
        annotation = Any if parameter.annotation is None else parameter.annotation
        property_schema = TypeAdapter(annotation).json_schema()
        property_schema["description"] = parameter.help
        if parameter.required:
            required.append(parameter.name)
        else:
            # The schema is published as JSON, and two declarable types have no
            # JSON form — a `date` default here would break the whole listing.
            property_schema["default"] = json_scalar(parameter.default)
        properties[parameter.name] = property_schema

    schema: dict[str, JsonValue] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    return schema


def _semantics_to_payload(semantics: ReportSemantics) -> ReportSemanticsPayload:
    return ReportSemanticsPayload(
        unit=semantics.unit,
        currency=semantics.currency,
        sign=semantics.sign,
        kind=semantics.kind,
        valuation_basis=semantics.valuation_basis,
        fx_basis=semantics.fx_basis,
        fx_date=semantics.fx_date,
        time_basis=semantics.time_basis,
        denominator=semantics.denominator,
        comparison_window=semantics.comparison_window,
        exclusions=semantics.exclusions,
        provenance=semantics.provenance,
    )


def _thaw_parameter_metadata(value: object) -> JsonValue:
    """Convert only frozen JSON containers from safe result metadata to JSON shapes."""
    if isinstance(value, Mapping):
        return {
            name: _thaw_parameter_metadata(item)
            for name, item in cast(Mapping[str, object], value).items()
        }
    if isinstance(value, tuple):
        return [
            _thaw_parameter_metadata(item) for item in cast(tuple[object, ...], value)
        ]
    # A `date`/`decimal` parameter reaches the runner as a real Python object, so
    # the effective-parameter echo has to render it the way storage does.
    return cast(JsonValue, json_scalar(value))
