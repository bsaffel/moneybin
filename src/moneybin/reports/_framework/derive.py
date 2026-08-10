"""Derive a user-created report's privacy contract from its SQL.

R2 of ``docs/specs/reports-dynamic.md``: classification is never something the
user does and never something that blocks a save. Saving requires a name and a
row-returning read-only SELECT over fully-classified schemas; the class map is
derived here and stored.

The pipeline reuses the classifiers the ad-hoc ``sql_query`` surface already
runs — ``validate_read_only_query``, ``resolve_output_classes``,
``classes_by_result_column`` — rather than growing a second classification path
beside them. R4's ``class_fingerprint`` is the drift key over everything those
classifiers read.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Final

import duckdb
from sqlglot import exp

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.log_sanitizer import sql_digest
from moneybin.privacy.redaction import is_safe_to_publish_verbatim, mask_strength
from moneybin.privacy.sql_lineage import (
    FAIL_CLOSED_CLASS,
    SchemaSnapshot,
    SqlParseError,
    SqlSchemaError,
    expand_star,
    get_current_schema_snapshot,
    is_data_query,
    parse_cached,
    read_column_classes,
    resolve_output_classes,
    resolve_placeholder_classes,
    tables_outside_schemas,
)
from moneybin.privacy.sql_query import (
    classes_by_result_column,
    validate_read_only_query,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ParamSpec

logger = logging.getLogger(__name__)

#: Bumped whenever ANY function the persisted class map depends on changes how
#: it classifies — ``resolve_output_classes`` and the ``classes_by_result_column``
#: bridging step alike. The fingerprint's other terms describe derivation's
#: *inputs*, so a change to the algorithm moves no tuple: without this term a
#: fix that raises a computed column from LOW to HIGH would leave every saved
#: report on the match branch, serving the old class indefinitely.
DERIVATION_VERSION: Final = 1

#: Report creation tracks the ``sql_query`` gate exactly. M2O.2 answered the
#: question this constant used to defer: a durable report MAY read raw/prep.
#: Redaction runs per value at execution, so a stored FLOORED is a standing
#: instruction to re-scan live values, not a cached verdict — and graduation to
#: a materialized ``reports.*`` view stays blocked by
#: ``report_materialization.DERIVABLE_UPSTREAM_SCHEMAS``, surfaced through
#: ``reports explain``.
SAVE_SCHEMAS: Final = frozenset({"core", "app", "reports", "raw", "prep"})


@dataclass(frozen=True, slots=True)
class _ParamType:
    """One declarable parameter type and the value the DESCRIBE probe binds."""

    python: type
    sentinel: object


# The declarable parameter types. An allowlist, not a free-text type name: the
# token reaches a DuckDB binding, and the sentinel below is what resolves an
# overloaded builtin's candidate set.
_PARAM_TYPES: Final[dict[str, _ParamType]] = {
    "str": _ParamType(str, ""),
    "int": _ParamType(int, 0),
    "float": _ParamType(float, 0.0),
    "bool": _ParamType(bool, False),
    "date": _ParamType(date, date(1970, 1, 1)),
    "decimal": _ParamType(Decimal, Decimal(0)),
}

# Exact-type reverse map. Keyed on type identity because ``bool`` subclasses
# ``int`` — an isinstance walk would render a boolean parameter as "int".
_PARAM_TOKENS: Final[dict[type, str]] = {
    spec.python: token for token, spec in _PARAM_TYPES.items()
}


@dataclass(frozen=True, slots=True)
class DerivedClassification:
    """One run of the save pipeline over a report's SQL.

    ``classes`` is keyed by **DuckDB result column name**, which is what
    ``classify_columns`` looks up at run time. ``unresolved_columns`` feeds R3's
    non-blocking save note — an unresolvable projection never fails the save.
    """

    classes: Mapping[str, DataClass]
    parameter_classes: Mapping[str, DataClass]
    fingerprint: str
    unresolved_columns: tuple[str, ...]


def annotation_of(token: str) -> type:
    """Resolve a stored ``annotation`` token to its Python type."""
    declared = _PARAM_TYPES.get(token)
    if declared is None:
        raise UserError(
            f"Unsupported parameter type {token!r}.",
            code=error_codes.REPORT_PARAMETER_INVALID_TYPE,
            hint=f"Declare one of: {', '.join(sorted(_PARAM_TYPES))}.",
        )
    return declared.python


def type_sentinel(annotation: Any) -> object:
    """A typed stand-in value of ``annotation``'s type, carrying no meaning.

    The same table the ``DESCRIBE`` probe binds, reused by the verify surface so
    parameter *validation* can still run over a report whose required values the
    caller did not supply. It is never rendered into SQL and never executed.
    """
    return _PARAM_TYPES[token_of(annotation)].sentinel


def json_scalar(value: Any) -> Any:
    """Render a declared default as a JSON scalar the ``params`` column can hold.

    Two of the six declarable types coerce to Python objects ``json.dumps``
    refuses outright, and a ``TypeError`` there reaches the user as a bare
    traceback — ``classify_user_error`` does not classify it. Storing ISO text
    keeps the column JSON while :func:`typed_value` keeps the declared type
    authoritative on the way back.
    """
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def typed_value(value: Any, annotation: Any) -> Any:
    """Read a JSON scalar back as a declared type JSON cannot represent.

    ``date`` and ``decimal`` are declarable but have no JSON form, so both the
    stored ``params`` column and an MCP ``parameters`` object carry text where the
    report declared an object. Without this, those two types are CLI-only: the
    CLI's own binder builds real objects from ``--param`` strings, while every
    JSON caller is refused by the shared type check.

    A value that is already the declared type, or cannot be read as one, is
    returned untouched — so the caller's own type error is what the user sees
    rather than one raised from here.
    """
    try:
        if annotation is date and isinstance(value, str):
            return date.fromisoformat(value)
        if annotation is Decimal:
            # `bool` subclasses `int`, and `Decimal(True)` is 1 — a boolean here
            # is a type mistake, not a decimal, so leave it for the type check.
            if isinstance(value, str) or type(value) is int:
                return Decimal(value)
            if type(value) is float:
                # Through `str` so a JSON `0.1` becomes Decimal("0.1") rather
                # than the binary expansion `Decimal(0.1)` would produce.
                return Decimal(str(value))
    except (ValueError, ArithmeticError):
        return value
    return value


def token_of(annotation: Any) -> str:
    """Render a parameter's Python type as its stored ``annotation`` token."""
    token = _PARAM_TOKENS.get(annotation)
    if token is None:
        raise UserError(
            f"Unsupported parameter type {getattr(annotation, '__name__', annotation)!r}.",
            code=error_codes.REPORT_PARAMETER_INVALID_TYPE,
            hint=f"Declare one of: {', '.join(sorted(_PARAM_TYPES))}.",
        )
    return token


def derive_classification(
    db: Database, *, query_sql: str, params: Sequence[ParamSpec] = ()
) -> DerivedClassification:
    """Run the save pipeline over ``query_sql`` and return its privacy contract.

    ``params`` supplies each parameter's declared name, type, and default.
    Its ``data_class`` field is **ignored** — a class is derived (step 5), never
    declared, so a user cannot widen their own masking floor by asserting one.

    Raises:
        UserError: if the query is not a row-returning read-only SELECT over
            ``SAVE_SCHEMAS``, produces duplicate result column names, cannot be
            described, declares one parameter name twice, or declares a default
            on an above-LOW parameter.
    """
    _refuse_duplicate_parameters(params)
    tree, snapshot = _parsed(db, query_sql)
    qualified = _qualified_or_refuse(tree, snapshot, query_sql)

    resolved = resolve_placeholder_classes(qualified, snapshot)
    _refuse_unused_parameters(params, resolved)
    # Keyed by the *declared* parameters, not by what the SQL contains: an
    # undeclared placeholder cannot reach a run at all — DESCRIBE below refuses
    # it as unbound.
    parameter_classes = {
        parameter.name: resolved.get(parameter.name, FAIL_CLOSED_CLASS)
        for parameter in params
    }
    _refuse_sensitive_defaults(params, parameter_classes)
    # Resolved first because the output classifier needs them: a projected
    # parameter returns whatever is bound to it, so its class belongs to the
    # projection that returns it.
    output_classes = resolve_output_classes(
        qualified,
        snapshot,
        query_sql,
        strict=False,
        placeholder_classes=parameter_classes,
    )

    columns = _describe_result_columns(db, query_sql, params)
    classes = classes_by_result_column(columns, output_classes, query_sql)

    return DerivedClassification(
        classes=MappingProxyType(classes),
        parameter_classes=MappingProxyType(parameter_classes),
        fingerprint=class_fingerprint(
            db,
            query_sql=query_sql,
            classes=classes,
            parameter_classes=parameter_classes,
            class_downgrades={},
        ),
        unresolved_columns=tuple(
            name
            for name, data_class in classes.items()
            if data_class is FAIL_CLOSED_CLASS
        ),
    )


def downgrade_pair(entry: Mapping[str, str]) -> tuple[DataClass, DataClass]:
    """The two classes one stored downgrade entry approves — both required.

    Validated together and unconditionally. :func:`class_fingerprint` used to
    read each side only ``if entry.get(side)``, so a half-written entry keyed a
    fingerprint without complaint and then reached ``DataClass("")`` inside
    :func:`with_downgrades`. On the read path that ``ValueError`` lands outside
    every handler, so one corrupt row failed the whole catalog — built-ins
    included — instead of degrading itself.

    Neither side names a column, and the message names neither: a saved report's
    column is a user-authored alias, and this text reaches a log.
    """
    try:
        return DataClass(entry["from"]), DataClass(entry["to"])
    except (KeyError, ValueError) as e:
        raise UserError(
            "A stored classification downgrade does not name two valid classes.",
            code=error_codes.REPORT_DOWNGRADE_UNREADABLE,
            hint="Save the report again to rebuild its classification contract.",
        ) from e


def is_weaker_class(from_class: DataClass, to_class: DataClass) -> bool:
    """Whether ``to_class`` is a legitimate downgrade of ``from_class``.

    The tier must **strictly fall**, and masking may not strengthen. Requiring
    the tier to fall is what rejects an equal-tier weakening, which is the
    dangerous case a "neither component rises and at least one falls" rule
    admits: ``ROUTING_NUMBER → ACCOUNT_IDENTIFIER`` holds CRITICAL and drops
    masking from whole to partial, so every future run would render the real
    last four digits where every row previously showed ``'*****'``.

    The downgrade mechanism exists because derivation over-classifies *computed*
    columns — an author asserting "this z-score reveals no amount" makes a claim
    about information content. That argument is unavailable when both classes
    agree on the tier and differ only in transform, so no reason can waive it.
    Same rule ``.claude/rules/reports.md`` already applies to materialized
    reports at CI time; the runtime path gets the same guard, not a weaker one.

    Lives here rather than beside the service that gates a *new* approval,
    because :func:`with_downgrades` has to ask the same question of an *old*
    one on every re-derivation, and a second copy of this rule is how the two
    answers would drift.
    """
    return to_class.tier < from_class.tier and mask_strength(to_class) <= mask_strength(
        from_class
    )


def with_downgrades(
    classes: dict[str, DataClass], downgrades: Mapping[str, Mapping[str, str]]
) -> dict[str, DataClass]:
    """Apply approved downgrades over a freshly derived class map.

    Applied only where the derived class still equals the one the downgrade was
    approved against. Reapplying by column name alone would let an approval
    collected against a weak class silently suppress a stronger one.

    And only while the pair is *still* one R5 would approve. An approval is an
    assertion about the tier and transform its two classes carried when it was
    granted, which is why :func:`class_fingerprint` hashes those triples — but
    moving the fingerprint only buys a re-derivation, and re-derivation that
    reapplies on class identity alone spends that for nothing. A release that
    lifts the ``to`` class to the ``from`` class's tier turns a legal downgrade
    into the equal-tier weakening ``is_weaker_class`` refuses outright, and
    because the stored map already holds ``to``, nothing downstream would have
    noticed: the reapplied map would match it exactly and the report would serve
    normally. Declining to reapply is what makes it visible — the caller's own
    comparison against the stored map then differs, so the column fails closed
    and the report degrades, which is R4's answer to every other drift.

    One implementation for all three callers — the save path, the edit path, and
    the run path's re-resolution. They held two copies of this loop, and the
    validation gap above lived in both.
    """
    for column, entry in downgrades.items():
        approved_from, approved_to = downgrade_pair(entry)
        if classes.get(column) is approved_from and is_weaker_class(
            approved_from, approved_to
        ):
            classes[column] = approved_to
    return classes


def drifted_names(
    reapplied: Mapping[str, DataClass], stored: Mapping[str, DataClass]
) -> tuple[str, ...]:
    """Every name whose class differs between a re-derivation and the stored map.

    Asked of a report's output columns and of its parameters alike: both are
    name-to-class maps derived from the same SQL and persisted beside each other,
    and a parameter that moved is as unreviewed as a column that moved.

    ``reapplied`` must already carry the approved downgrades (:func:`with_downgrades`),
    because a legitimately downgraded report differs from raw derivation by design.
    Parameters carry no downgrades, so callers pass the derived map as it stands.

    **Any** movement counts, in either direction — not only a rise. A derived class
    that moved *down* is a weakening no human reviewed, and only
    ``reports reclassify`` may lower a floor.

    Compared in **both** directions. Walking the derived map alone never visits a
    name the stored map has and this one does not, so a saved ``SELECT *`` whose
    upstream column was retired compared equal — the read path served it as
    healthy with a narrower contract than the one it stored.

    One implementation for the callers that ask this question of the same two
    maps: the run path, which fails the moved names closed and degrades, and the
    downgrade path, which refuses to write an approval on top of movement nobody
    reviewed. They are the same comparison, so the both-directions rule above
    cannot hold in one and lapse in the other.
    """
    return tuple(
        sorted(
            {
                name
                for name, data_class in reapplied.items()
                if stored.get(name) is not data_class
            }
            | (set(stored) - set(reapplied))
        )
    )


def class_fingerprint(
    db: Database,
    *,
    query_sql: str,
    classes: Mapping[str, DataClass],
    parameter_classes: Mapping[str, DataClass],
    class_downgrades: Mapping[str, Mapping[str, str]],
    snapshot: SchemaSnapshot | None = None,
) -> str:
    """The drift key over everything the persisted class map depends on.

    ``SchemaSnapshot.version`` reads ``MAX(version) FROM app.schema_migrations``
    and is blind to every input here: ``CLASSIFICATION`` is a Python dict,
    ``reports_class_map()`` is built in-process, and ``core``/``reports`` are
    SQLMesh-built, so none of them bumps a migration. The key covers three
    things instead — the classes of every column the query reads, the
    ``(class, tier, mask strength)`` policy triple for every class in play (the
    read set as well as the stored map), and ``DERIVATION_VERSION``.

    The policy triples are what a downgrade actually turns on. An approval is
    not an assertion about a ``DataClass`` *name*; it is an assertion about the
    tier and transform that name carried when the approval was granted. If
    ``TXN_AMOUNT`` began masking under an unchanged classification, every stored
    map would keep its names and every column tuple would hold — so without them
    a downgrade approved against the old, weaker policy would keep applying with
    no revalidation.

    Same function on both paths, called with derived values at save and stored
    values at run: one implementation means a stale key can only cost a
    re-resolution, never a false match.

    ``snapshot`` lets a caller keying many reports at once read the live schema
    once instead of per report — see :func:`user_report_specs`.
    """
    tree, snapshot = _tree_and_snapshot(db, query_sql, snapshot)
    read_columns = read_column_classes(tree, snapshot)

    involved = set(classes.values()) | set(parameter_classes.values())
    for entry in class_downgrades.values():
        involved.update(downgrade_pair(entry))
    # The read set too, not only what the row stores. A derived column takes the
    # strongest class among its inputs, so an input that lost that contest still
    # decides the answer the moment its own tier or transform is raised — and
    # that move leaves both the stored names and `read_columns`' class names
    # untouched, which is the same blind spot the triples exist to close.
    involved.update(DataClass(class_name) for *_, class_name in read_columns)
    policy = sorted(
        (data_class.value, int(data_class.tier), int(mask_strength(data_class)))
        for data_class in involved
    )

    payload = json.dumps(
        {
            "version": DERIVATION_VERSION,
            # The query text itself, not only its read set: rewriting
            # `SELECT account_type AS v` to `SELECT routing_number AS v` over the
            # same table moves neither `read_columns` nor `policy`, so without
            # this term the key still matches and `spec_from_row` serves the
            # stale LOW map for a CRITICAL value. `UserReportsService.update`
            # re-derives on any SQL change, but the repo's own `set` does not.
            "query": hashlib.sha256(query_sql.encode()).hexdigest(),
            "read_columns": read_columns,
            "policy": policy,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def read_tables(
    db: Database, query_sql: str, *, snapshot: SchemaSnapshot | None = None
) -> tuple[str, ...]:
    """Sorted ``schema.table`` names ``query_sql`` reads.

    Feeds a dynamic report's ``semantics.provenance``, which is what the export
    receipt's ``lineage`` carries — the only place a query-time report's read set
    appears, since its manifest ``source`` is ``null``.
    """
    tree, snapshot = _tree_and_snapshot(db, query_sql, snapshot)
    return tuple(
        sorted({
            f"{schema}.{table}"
            for schema, table, _column, _class in read_column_classes(tree, snapshot)
        })
    )


def _tree_and_snapshot(
    db: Database, query_sql: str, snapshot: SchemaSnapshot | None = None
) -> tuple[exp.Expr, SchemaSnapshot]:
    """The **unqualified** tree plus the live snapshot, for every read-set term.

    Both the fingerprint and the provenance list are built from this pair, and
    both must use the *same* tree on the save and run paths: ``qualify`` can
    rewrite table references, so a qualified tree here would key one query two
    ways and leave every run on the mismatch branch.

    A caller may supply the snapshot it already read. ``get_current_schema_snapshot``
    memoises the costly ``MappingSchema`` build but still issues two catalog
    queries per call, which is per *report* on a catalog build.
    """
    return (
        parse_cached(query_sql),
        get_current_schema_snapshot(db) if snapshot is None else snapshot,
    )


def _parsed(db: Database, query_sql: str) -> tuple[exp.Expr, SchemaSnapshot]:
    """Gate, parse, and snapshot — steps 1 through 3."""
    error = validate_read_only_query(query_sql)
    if error:
        raise UserError(error, code=error_codes.REPORT_QUERY_INVALID)

    try:
        tree = parse_cached(query_sql)
    except SqlParseError as e:
        raise UserError(
            "Could not parse SQL.",
            code=error_codes.REPORT_QUERY_INVALID,
            details={"detail": str(e)},
        ) from e

    # `validate_read_only_query` also admits DESCRIBE/SHOW/PRAGMA/EXPLAIN. Those
    # would fail midway through this pipeline with "Query has no SELECT
    # projection", so refuse them here: a report is a durable classified
    # artifact, not a metadata read the SQL surface can skip classification for.
    if not is_data_query(tree):
        raise UserError(
            "A saved report must be a row-returning SELECT.",
            code=error_codes.REPORT_QUERY_INVALID,
            hint="DESCRIBE, SHOW, PRAGMA, and EXPLAIN return no classifiable rows.",
        )
    return tree, get_current_schema_snapshot(db)


def _qualified_or_refuse(
    tree: exp.Expr, snapshot: SchemaSnapshot, query_sql: str
) -> exp.Expr:
    """Expand stars, then refuse any table outside the classified schemas.

    ``query_sql`` is carried for the log digest alone — the *original* text, not
    ``tree.sql()``, so every record about one save names the same statement.
    """
    try:
        qualified = expand_star(tree, snapshot)
    except SqlSchemaError as e:
        # Type and digest only: the statement being saved is user-authored text
        # that may hold an inline merchant name, and sqlglot quotes what it
        # failed on. `sql_digest` carries the full reasoning.
        logger.warning(
            f"user report save: unknown table/column: {type(e).__name__} "
            f"(sql sha256={sql_digest(query_sql)})"
        )
        raise UserError(
            "Unknown table or column.", code=error_codes.SQL_UNKNOWN_TABLE
        ) from e

    disallowed = tables_outside_schemas(qualified, snapshot, SAVE_SCHEMAS)
    if disallowed:
        raise UserError(
            "Saved reports read from these schemas only: "
            f"{', '.join(sorted(SAVE_SCHEMAS))}.",
            code=error_codes.REPORT_QUERY_SCHEMA_NOT_ALLOWED,
            details={"disallowed": sorted(set(disallowed))},
        )
    return qualified


def _refuse_sensitive_defaults(
    params: Sequence[ParamSpec], parameter_classes: Mapping[str, DataClass]
) -> None:
    """Refuse a stored default the catalog could not publish safely.

    ``_parameter_schema`` copies a non-required parameter's default verbatim
    into the published parameter schema, and the catalog entry classes that
    whole schema ``AGGREGATE`` — LOW, unmasked. So a routing number pasted as a
    filter's default would be returned in the clear by a bare catalog listing,
    no execution required. A default masked to ``'*****'`` is not a useful
    default anyway, so the parameter becomes required instead.

    The test is :func:`is_safe_to_publish_verbatim`, not ``tier > Tier.LOW``.
    A FLOORED parameter — an undeclared ``raw``/``prep`` column — is ``Tier.LOW``
    yet masks per value at execution, and this path never executes anything, so
    a tier-only test published it in the clear.

    This lives in the same function that derives the class so no path can reach
    a stored default without passing it. The run path re-derives with defaults
    stripped, which is why re-resolution can never trip this on a read.
    """
    for parameter in params:
        if parameter.required:
            continue
        data_class = parameter_classes.get(parameter.name, FAIL_CLOSED_CLASS)
        if not is_safe_to_publish_verbatim(data_class):
            # The name is withheld from the message and carried in `details`
            # instead. A parameter name is the author's own text — `amazon_spend`
            # is as plausible a merchant name as a filter one — and in text mode
            # `handle_cli_errors` writes `message` and `hint` through
            # `logger.error`, which persists them where `SanitizedLogFormatter`
            # cannot recognize either. `details` reaches the JSON envelope and the
            # caller, neither of which is a durable log. Same rule as the
            # reclassify log, which withholds the column for the same reason.
            raise UserError(
                f"A parameter classifying as {data_class.value} "
                f"({data_class.tier.name} tier) cannot carry a default.",
                code=error_codes.REPORT_PARAMETER_DEFAULT_NOT_ALLOWED,
                hint="Declare it required — the report catalog publishes defaults unmasked.",
                details={"parameter": parameter.name},
            )


def _refuse_duplicate_parameters(params: Sequence[ParamSpec]) -> None:
    """Refuse one parameter name declared twice — step 0.

    Every name-keyed map collapses the pair, so the *derived* half of the
    contract looks consistent. ``params`` does not: it stores both entries, and
    ``_parameter_schema`` walks that list — appending the name to ``required``
    from a required entry and then overwriting ``properties[name]`` from a later
    defaulted one. The catalog publishes a parameter that is required and also
    carries a default, under ``additionalProperties: false``, so a caller
    trusting the published default is rejected for omitting it. Two declarations
    cannot both be honoured; neither is guessed at.

    Ahead of :func:`_parsed` because a declaration list contradicting itself is
    answerable without the query, the schema snapshot, or a DuckDB round trip.
    """
    names = [parameter.name for parameter in params]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        # Names withheld from the message and carried in `details`, the same split
        # `_refuse_sensitive_defaults` makes below and for the same reason: a
        # declaration name is the author's own text, `handle_cli_errors` writes
        # `message` and `hint` through `logger.error`, and `SanitizedLogFormatter`
        # cannot recognize `amazon_spend` as anything worth masking. `details`
        # reaches the caller and the JSON envelope, neither a durable log.
        raise UserError(
            "A parameter name is declared more than once.",
            code=error_codes.REPORT_PARAMETER_DUPLICATE,
            hint="Declare each parameter once; one name carries one type and default.",
            details={"parameters": repeated},
        )


def _refuse_unused_parameters(
    params: Sequence[ParamSpec], resolved: Mapping[str, DataClass]
) -> None:
    """Refuse a declared parameter the SQL never references — step 5b.

    DuckDB rejects an excess named binding outright (``Parameter argument/count
    mismatch, identifiers of the excess parameters: …``), so a declared-but-unused
    parameter cannot be described *or* run. Named here rather than left to
    :func:`_describe_result_columns`'s ``duckdb.Error`` handler, whose message —
    "every column must exist, and each parameter's declared type must fit every
    position it is used in" — is false for this case and sends the author
    hunting a column and a type that are both fine.
    """
    unused = [parameter.name for parameter in params if parameter.name not in resolved]
    if unused:
        raise UserError(
            "The query never references a declared parameter.",
            code=error_codes.REPORT_QUERY_INVALID,
            hint="Reference each declared parameter as $name, or drop the declaration.",
            details={"parameters": unused},
        )


def _describe_result_columns(
    db: Database, query_sql: str, params: Sequence[ParamSpec]
) -> list[str]:
    """DuckDB's real result column names for ``query_sql`` — step 6.

    Load-bearing, not an optimization. ``resolve_output_classes`` returns names
    from sqlglot projections while ``classify_columns`` looks them up by DuckDB
    result name, so persisting the unbridged map would mask ``COUNT(*)`` —
    sqlglot ``*``, DuckDB ``count_star()`` — on every run of every report
    containing one.

    Every placeholder is bound to a **typed sentinel value** of its declared
    annotation, and the query text inside the ``DESCRIBE`` is left character-for-
    character identical to what will execute — nothing rewrites the projection
    that names are read from.
    Two mechanisms were rejected against DuckDB 1.5.4. A *bare* NULL binding
    raises ``BinderException`` on the ``date_part``/``date_trunc``/``extract``
    family, so ``WHERE date_part('year', txn_date) = $year`` — an ordinary
    filter — would hard-crash rather than take one of R2's soft-fail paths.
    Substituting ``CAST(NULL AS <t>)`` for the placeholder fixes that but
    renames any unaliased projection holding one (``$x`` becomes
    ``CAST(NULL AS BIGINT)``), reopening a narrower version of the very
    name-divergence this step exists to close. A typed sentinel resolves the
    overload *and* returns the same names a value-bound run does, because column
    names derive from projection structure and never from parameter values.

    DESCRIBE returns one row per output column and evaluates no user rows. Its
    **type** column is not read: nothing here needs it, and reading it would
    make the map depend on a value binding.
    """
    bindings = {
        parameter.name: _PARAM_TYPES[token_of(parameter.annotation)].sentinel
        for parameter in params
    }
    try:
        # Security: `query_sql` passed `validate_read_only_query` in step 1 and
        # is intentionally user SQL that cannot be parameterized. DESCRIBE reads
        # the projection's schema and returns no rows from it.
        cursor = db.execute(f"DESCRIBE {query_sql}", bindings)  # noqa: S608
        rows = cursor.fetchall()
    except duckdb.Error as e:
        # DuckDB's binder error echoes the statement it bound, so only its type
        # and the digest may be logged — see `sql_digest`.
        logger.warning(
            f"user report save: could not describe result columns: "
            f"{type(e).__name__} (sql sha256={sql_digest(query_sql)})"
        )
        # The declarations move to `details` for the same reason the warning above
        # logs only a digest: this message reaches `logger.error` through
        # `handle_cli_errors`, and a parameter name is author-chosen text the
        # sanitizer cannot recognize. The type is usually the culprit here, so it
        # travels with the name rather than being dropped.
        raise UserError(
            "Could not resolve the report's result columns. Every column must "
            "exist, and each parameter's declared type must fit every position "
            "it is used in.",
            code=error_codes.REPORT_QUERY_UNRESOLVABLE,
            details={
                "parameters": [parameter.name for parameter in params],
                "declared": ", ".join(
                    f"${parameter.name}: {token_of(parameter.annotation)}"
                    for parameter in params
                ),
            },
        ) from e

    columns = [str(row[0]) for row in rows]
    duplicates = sorted({name for name in columns if columns.count(name) > 1})
    if duplicates:
        # DuckDB permits `SELECT 0 AS x, routing_number AS x`, but `classes` is
        # keyed by name and `redact_records` masks by that same key — one entry
        # survives holding whichever class resolved last, and it governs
        # whichever value survives. The mask stops corresponding to the value it
        # covers, so a duplicate name is refused rather than named as a risk.
        raise UserError(
            "Result column names must be unique; at least one repeats.",
            code=error_codes.REPORT_QUERY_COLUMN_DUPLICATE,
            hint="Alias each projection distinctly.",
            details={"columns": duplicates},
        )
    return columns
