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
from moneybin.privacy.redaction import mask_strength
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
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.contract import ParamSpec

logger = logging.getLogger(__name__)

#: Bumped whenever ANY function the persisted class map depends on changes how
#: it classifies — ``resolve_output_classes`` and the ``classes_by_result_column``
#: bridging step alike. The fingerprint's other terms describe derivation's
#: *inputs*, so a change to the algorithm moves no tuple: without this term a
#: fix that raises a computed column from LOW to HIGH would leave every saved
#: report on the match branch, serving the old class indefinitely.
DERIVATION_VERSION: Final = 1

#: Report creation is restricted to fully-classified schemas. ``raw``/``prep``
#: are not reachable through ``sql_query`` either; when M2O.2 opens them behind
#: a content-net floor, whether a *durable* artifact may be built over floored
#: columns is decided there.
SAVE_SCHEMAS: Final = frozenset({"core", "app", "reports"})


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
            described, or declares a default on an above-LOW parameter.
    """
    tree, snapshot = _parsed(db, query_sql)
    qualified = _qualified_or_refuse(tree, snapshot)

    output_classes = resolve_output_classes(
        qualified, snapshot, query_sql, strict=False
    )
    # Keyed by the *declared* parameters, not by what the SQL contains: a
    # declared parameter the SQL never mentions still needs a class for the
    # provenance renderer to withhold, and an undeclared placeholder cannot
    # reach a run at all — DESCRIBE below refuses it as unbound.
    resolved = resolve_placeholder_classes(qualified, snapshot)
    parameter_classes = {
        parameter.name: resolved.get(parameter.name, FAIL_CLOSED_CLASS)
        for parameter in params
    }
    _refuse_sensitive_defaults(params, parameter_classes)

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


def class_fingerprint(
    db: Database,
    *,
    query_sql: str,
    classes: Mapping[str, DataClass],
    parameter_classes: Mapping[str, DataClass],
    class_downgrades: Mapping[str, Mapping[str, str]],
) -> str:
    """The drift key over everything the persisted class map depends on.

    ``SchemaSnapshot.version`` reads ``MAX(version) FROM app.schema_migrations``
    and is blind to every input here: ``CLASSIFICATION`` is a Python dict,
    ``reports_class_map()`` is built in-process, and ``core``/``reports`` are
    SQLMesh-built, so none of them bumps a migration. The key covers three
    things instead — the classes of every column the query reads, the
    ``(class, tier, mask strength)`` policy triple for every class in play, and
    ``DERIVATION_VERSION``.

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
    """
    tree, snapshot = _tree_and_snapshot(db, query_sql)

    involved = set(classes.values()) | set(parameter_classes.values())
    for entry in class_downgrades.values():
        involved.update(
            DataClass(entry[side]) for side in ("from", "to") if entry.get(side)
        )
    policy = sorted(
        (data_class.value, int(data_class.tier), int(mask_strength(data_class)))
        for data_class in involved
    )

    payload = json.dumps(
        {
            "version": DERIVATION_VERSION,
            "read_columns": read_column_classes(tree, snapshot),
            "policy": policy,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def read_tables(db: Database, query_sql: str) -> tuple[str, ...]:
    """Sorted ``schema.table`` names ``query_sql`` reads.

    Feeds a dynamic report's ``semantics.provenance``, which is what the export
    receipt's ``lineage`` carries — the only place a query-time report's read set
    appears, since its manifest ``source`` is ``null``.
    """
    tree, snapshot = _tree_and_snapshot(db, query_sql)
    return tuple(
        sorted({
            f"{schema}.{table}"
            for schema, table, _column, _class in read_column_classes(tree, snapshot)
        })
    )


def _tree_and_snapshot(db: Database, query_sql: str) -> tuple[exp.Expr, SchemaSnapshot]:
    """The **unqualified** tree plus the live snapshot, for every read-set term.

    Both the fingerprint and the provenance list are built from this pair, and
    both must use the *same* tree on the save and run paths: ``qualify`` can
    rewrite table references, so a qualified tree here would key one query two
    ways and leave every run on the mismatch branch.
    """
    return parse_cached(query_sql), get_current_schema_snapshot(db)


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


def _qualified_or_refuse(tree: exp.Expr, snapshot: SchemaSnapshot) -> exp.Expr:
    """Expand stars, then refuse any table outside the classified schemas."""
    try:
        qualified = expand_star(tree, snapshot)
    except SqlSchemaError as e:
        logger.warning(f"user report save: unknown table/column: {e}")
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
    """Refuse a stored default on a parameter classed above LOW tier.

    ``_parameter_schema`` copies a non-required parameter's default verbatim
    into the published parameter schema, and the catalog entry classes that
    whole schema ``AGGREGATE`` — LOW, unmasked. So a routing number pasted as a
    filter's default would be returned in the clear by a bare catalog listing,
    no execution required. A default masked to ``'*****'`` is not a useful
    default anyway, so the parameter becomes required instead.

    This lives in the same function that derives the class so no path can reach
    a stored default without passing it. The run path re-derives with defaults
    stripped, which is why re-resolution can never trip this on a read.
    """
    for parameter in params:
        if parameter.required:
            continue
        data_class = parameter_classes.get(parameter.name, FAIL_CLOSED_CLASS)
        if data_class.tier > Tier.LOW:
            raise UserError(
                f"Parameter ${parameter.name} classifies as {data_class.value} "
                f"({data_class.tier.name} tier), so it cannot carry a default.",
                code=error_codes.REPORT_PARAMETER_DEFAULT_NOT_ALLOWED,
                hint="Declare it required — the report catalog publishes defaults unmasked.",
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
        logger.warning(f"user report save: could not describe result columns: {e}")
        declared = ", ".join(
            f"${parameter.name}: {token_of(parameter.annotation)}"
            for parameter in params
        )
        raise UserError(
            "Could not resolve the report's result columns. Every column must "
            "exist, and each parameter's declared type must fit every position "
            f"it is used in{f' (declared {declared})' if declared else ''}.",
            code=error_codes.REPORT_QUERY_UNRESOLVABLE,
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
            "Result column names must be unique; these repeat: "
            f"{', '.join(repr(name) for name in duplicates)}.",
            code=error_codes.REPORT_QUERY_COLUMN_DUPLICATE,
            hint="Alias each projection distinctly.",
        )
    return columns
