# src/moneybin/privacy/sql_query.py
"""Privacy-enforcing read-only SQL execution shared by the MCP and CLI surfaces.

This is the shared primitive behind the ``sql_query`` MCP tool and the
``moneybin sql query`` CLI command. Both surfaces call
:func:`execute_sql_query`, so privacy enforcement is
structural rather than per-surface: the read-only gate, the queryable-schema
allowlist, sqlglot column lineage, and CRITICAL masking all run here, below
the adapters. Neither surface can return rows that skipped redaction, and a
future third surface inherits the same guarantees by calling this primitive.

The read-only validation (``validate_read_only_query`` and its safety regexes)
lives here too — it is a SQL-safety primitive alongside ``sql_lineage`` and
``redaction``, not an MCP concern.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import duckdb
from sqlglot import exp

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.log_sanitizer import mask_pii_shaped, sql_digest
from moneybin.privacy.redaction import redact_records
from moneybin.privacy.sql_lineage import (
    FAIL_CLOSED_CLASS,
    SchemaSnapshot,
    SqlParseError,
    SqlSchemaError,
    derive_query_tier,
    expand_star,
    get_current_schema_snapshot,
    is_data_query,
    is_metadata_query,
    is_multi_statement,
    parse_cached,
    resolve_output_classes,
    tables_outside_schemas,
)
from moneybin.privacy.taxonomy import DataClass, Tier

logger = logging.getLogger(__name__)

# Data queries may reference these schemas: core/app (CLASSIFICATION registry),
# reports (declared @report classes, ADR-013), and raw/prep (a short CRITICAL
# declaration in INTERNAL_CRITICAL, everything else on the FLOORED content
# net). meta/seeds stay internal — no consumer need has surfaced.
#
# Admitting a schema here is only half the gate: `get_current_schema_snapshot`
# must ALSO cover it, or lineage resolves none of its columns, the conservative
# floor finds no classified table in scope, and every value comes back at
# AGGREGATE (LOW, passthrough) — including the declared CRITICAL ones. The two
# lists move together.
ALLOWED_QUERY_SCHEMAS = frozenset({"core", "app", "reports", "raw", "prep"})

# --- Read-only / file-access safety gate -----------------------------------
# DuckDB table-valued functions that read local files or make network requests.
# These pass the read-only prefix check (SELECT/WITH) but can exfiltrate data.
# Includes scan_* and legacy parquet_scan aliases (resolve identically to read_*).
# glob() is matched as a function call only — \bglob\b would false-positive on
# DuckDB's GLOB infix comparison operator (e.g. WHERE desc GLOB '*AMAZON*').
_FILE_ACCESS_FUNCTIONS = re.compile(
    r"\b(read_csv|read_csv_auto|read_parquet|read_json|read_json_auto|"
    r"read_ndjson|read_text|read_blob|read_delta|read_iceberg|"
    r"scan_parquet|scan_csv|scan_csv_auto|scan_json|scan_ndjson|parquet_scan|"
    r"glob)\s*\(",
    re.IGNORECASE,
)

# URL scheme literals used as path arguments to DuckDB table scans when httpfs
# is loaded. These bypass function-name matching because DuckDB accepts
# `SELECT * FROM 'https://evil.com/data.parquet'` with no function keyword.
#
# This list mirrors the remote filesystems the connection seal disables
# (`_DISABLED_FILESYSTEMS` in database.py): http/https + the S3-served schemes
# (s3/gcs/gs/r2) + hf (HuggingFace) + az (azure, blocked at extension-load).
# This validator and the connection seal are deliberate defense-in-depth for
# each other: the seal is the hard boundary (DuckDB refuses the scheme even if
# this regex misses it), and this layer rejects the query earlier with a clear
# message. Keep the two lists in sync when either changes.
_URL_SCHEME_PATTERNS = re.compile(
    r"(https?://|s3://|az://|gcs://|gs://|r2://|hf://)",
    re.IGNORECASE,
)

# DuckDB replacement scans can read files with `FROM 'path/to/file.csv'`
# without using read_csv/read_parquet. A single-quoted table source is not a
# normal catalog table reference, so reject it before execution.
_QUOTED_TABLE_SCAN = re.compile(
    r"(?<!')\b(FROM|JOIN)\s*'[^']+'",
    re.IGNORECASE,
)

# Patterns that indicate read-only SQL statements.
#
# The rule behind this list is one line: a statement is executable only if the
# schema gate can resolve every table it names. SELECT/WITH and DESCRIBE expose
# real `exp.Table` nodes and qualify; SHOW names no table at all, so there is
# nothing to resolve.
#
# PRAGMA and EXPLAIN are deliberately absent because they fail that test in the
# most dangerous way — they reference tables while hiding them from the gate. A
# PRAGMA's target is a string literal inside an `exp.Anonymous` call; an
# EXPLAIN's entire payload stays unparsed inside an `exp.Command` (sqlglot has
# no DuckDB EXPLAIN node). `find_all(exp.Table)` returns nothing for either, so
# both sail through a check that never examined anything.
#
# Both were live: `PRAGMA storage_info('<table>')` reports per-segment `stats`
# that are a cleartext PREFIX of the stored values, returning a CRITICAL routing
# number's first eight digits unmasked at LOW; and `EXPLAIN ANALYZE` EXECUTES
# its inner query, reaching `raw`/`prep` and reporting their column names and
# row counts from a path meant to return schema text.
_READ_ONLY_PREFIXES = re.compile(
    r"^\s*(SELECT|WITH|DESCRIBE|SHOW)\b",
    re.IGNORECASE,
)

# Concrete sqlglot expression types for every write operation this gate
# refuses (even nested inside a CTE). Structural, not textual: a query is
# checked by what it PARSES to, so an ordinary word that merely LOOKS like a
# keyword — inside a string literal (`'export'`, `e'export'`, `$$export$$`),
# a quoted identifier (`"update"`), or a `-- comment` — can never trip this,
# because none of those produce one of these node types. `find_all` walks the
# whole tree, so a write buried in a CTE or a semicolon-separated second
# statement is still caught (`SELECT 1; DROP TABLE x` verified).
#
# Only 8 of these are reachable nested inside a CTE under sqlglot's own
# grammar (verified): Insert, Update, Delete, Create, Drop, Attach, Detach,
# Merge. TruncateTable, Alter, and Copy are NOT — `WITH x AS (TRUNCATE TABLE
# y) SELECT 1 FROM x` (and the ALTER/COPY equivalents) is a sqlglot
# `ParseError`, so those three can only ever appear as a bare top-level
# statement, which `_READ_ONLY_PREFIXES` above already refuses before this
# check runs. They stay in the tuple as defense-in-depth against that prefix
# check ever loosening, not because this check currently reaches them.
#
# `EXPORT DATABASE`, `IMPORT DATABASE`, and bare `REPLACE INTO` have no
# dedicated sqlglot node for the duckdb dialect at all (`REPLACE INTO` isn't
# even valid DuckDB syntax — only `INSERT OR REPLACE INTO`, which parses as
# `exp.Insert`) and are not in this tuple. That's safe for the same structural
# reason: each is a top-level-only statement DuckDB's own grammar refuses
# anywhere but the start of a query (verified: `WITH x AS (SELECT 1) EXPORT
# DATABASE 'dir'` is a DuckDB parser error, not a valid nested form) — a bare
# occurrence is refused above by `_READ_ONLY_PREFIXES`, and a WITH-prefixed
# attempt fails to parse at all, which this function defers to the caller
# (see the `except SqlParseError` below) and `execute_sql_query`'s own parse
# then reports as `sql_invalid_query`. The same holds for DDL sqlglot falls
# back to a generic `exp.Command` for (e.g. `ALTER SEQUENCE ... RESTART`,
# `DROP TYPE`, `CREATE TYPE ... AS ENUM`) — verified each is likewise rejected
# by DuckDB when following `WITH x AS (SELECT 1)`.
_WRITE_EXPRESSION_TYPES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Merge,
    exp.Copy,
    exp.Attach,
    exp.Detach,
)


def _contains_write_operation(tree: exp.Expr) -> bool:
    return any(tree.find_all(*_WRITE_EXPRESSION_TYPES))


def validate_read_only_query(sql: str) -> str | None:
    """Validate that a SQL query is read-only.

    Args:
        sql: The SQL query string to validate.

    Returns:
        None if the query is valid, or an error message string if rejected.
    """
    stripped = sql.strip()

    if not stripped:
        return "Empty query is not allowed."

    if not _READ_ONLY_PREFIXES.match(stripped):
        return (
            "Only read-only queries are allowed. "
            "Queries must start with SELECT, WITH, DESCRIBE, or SHOW. "
            "PRAGMA and EXPLAIN are not supported; use DESCRIBE <table> or "
            "SHOW ALL TABLES to inspect schema."
        )

    if _FILE_ACCESS_FUNCTIONS.search(stripped):
        return (
            "File-access functions (read_csv, read_parquet, read_json, glob, etc.) "
            "are not allowed."
        )

    if _URL_SCHEME_PATTERNS.search(stripped):
        return (
            "URL literals (https://, s3://, etc.) are not allowed. "
            "Queries must read from database tables only."
        )

    if _QUOTED_TABLE_SCAN.search(stripped):
        return (
            "Quoted file/table path scans are not allowed. "
            "Queries must read from database tables only."
        )

    # A parse error here is not this gate's to report: return None and let the
    # caller's own parse surface it with its own error code. Both remaining
    # checks need the parsed tree, so parse once here and reuse it for both —
    # `execute_sql_query` parses again on its own (unstripped) copy of the
    # query text, a separate `parse_cached` cache entry whenever the query
    # carries leading/trailing whitespace; this only dedupes the two checks
    # in this function.
    try:
        tree = parse_cached(stripped)
    except SqlParseError:
        return None

    if _contains_write_operation(tree):
        return (
            "Write operations (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.) "
            "are not allowed."
        )

    # Every statement in `SELECT 1; SELECT routing_number FROM ...` is
    # individually a legal read, so none of the checks above fire — but DuckDB
    # returns the LAST statement's rows while classification reads the first.
    if is_multi_statement(tree):
        return "Queries must be one statement; remove the extra ';'-separated SQL."

    return None


@dataclass(frozen=True)
class SqlQueryResult:
    """Outcome of a privacy-enforced SQL query, ready for either surface.

    ``records`` are already redacted (CRITICAL columns masked); both adapters
    consume them as-is. ``output_classes`` maps each result column to its
    resolved data class — empty for metadata (DESCRIBE/SHOW) queries, which
    carry no row-data classification.
    """

    records: list[dict[str, Any]]
    columns: list[str]
    output_classes: dict[str, DataClass]
    tier: Tier
    total_count: int
    truncated: bool
    is_metadata: bool = field(default=False)

    @property
    def classes_returned(self) -> list[str]:
        """Sorted data-class values for the envelope/audit.

        ``["aggregate"]`` when no row-data classes apply (metadata or
        pure-aggregate queries).
        """
        if not self.output_classes:
            return ["aggregate"]
        return sorted({c.value for c in self.output_classes.values()})


def _fetch(
    db: Database, query: str, max_rows: int
) -> tuple[list[str], list[Any], bool]:
    """Execute ``query`` read-only and fetch up to ``max_rows`` (+1 to detect more)."""
    # Security: the caller validated the query is read-only and free of
    # file-access vectors; the entire string is intentionally user SQL and
    # cannot be parameterized.
    result = db.execute(query)  # noqa: S608 — read-only, validated by caller
    columns = [desc[0] for desc in result.description]
    rows = result.fetchmany(max_rows + 1)
    truncated = len(rows) > max_rows
    return columns, rows[:max_rows], truncated


def classes_by_result_column(
    columns: list[str],
    output_classes: dict[str, DataClass],
    query: str,
) -> dict[str, DataClass]:
    """Map every DuckDB RESULT column to a DataClass, failing closed on a miss.

    Matching is BY NAME, which is robust to any divergence between sqlglot's
    projection order and DuckDB's runtime column order (the ``SELECT *`` case) —
    a positional join is not. Named projections and expanded ``*`` columns match
    directly.

    A miss means one of two very different things, and they must not be
    conflated:

    1. **Naming-only divergence.** sqlglot keyed an unaliased projection
       differently than DuckDB named it (``COUNT(*)`` → DuckDB
       ``count_star()`` vs lineage ``*``; ``MIN(account_id)`` → DuckDB
       ``min(account_id)`` vs lineage ``?_0``). Lineage RESOLVED this column;
       only the label differs.
    2. **Missing lineage** — the dangerous case: the query produced runtime
       columns lineage never saw at all. ``COLUMNS('.*')``, ``PIVOT``,
       ``UNPIVOT``, ``SUMMARIZE`` and ``UNNEST`` over a row struct each emit
       12–19 such columns from a single projection.

    Cardinality separates them. Case 1 preserves the projection count, so when
    it matches, position reconciles the two namings exactly (``output_classes``
    is insertion-ordered by projection). Case 2 is precisely the case where one
    projection fans out into many runtime columns, so the counts disagree and
    every unmatched column fails closed to ``FAIL_CLOSED_CLASS``.

    Failing closed on case 1 is not "merely conservative" — it masks
    ``SELECT COUNT(*)``, returning ``'*****'`` and a CRITICAL tier for the most
    common analytical query there is.

    A miss must NOT fall back to the max class present in ``output_classes``.
    That is what this code used to do, under a comment asserting "an unmasked
    CRITICAL value can therefore never slip through" — and the assertion was
    false in the only case that mattered. When lineage classified the single
    opaque projection AGGREGATE, "the most sensitive class present" WAS
    AGGREGATE, so all 19 columns of ``core.dim_accounts`` fell back to LOW and
    ``routing_number`` was returned in the clear. A fallback computed from the
    classes that happened to resolve cannot bound the classes that did not.
    """
    # A name shared by two result columns identifies neither. Lineage would
    # hand both the one class it resolved — and since the caller builds records
    # with dict(zip(columns, row)), the LAST value wins, so a safe literal's
    # class can front for a sensitive column that overwrites it
    # (SELECT 0 AS routing_number, COLUMNS('routing_number')). Nothing
    # downstream can recover the association, so the whole row fails closed.
    if len(set(columns)) != len(columns):
        return {col: _fail_closed(col, query) for col in columns}

    if all(col in output_classes for col in columns):
        return {col: output_classes[col] for col in columns}

    if len(columns) == len(output_classes):
        positional = dict(zip(columns, output_classes.values(), strict=True))
        # Position is only trustworthy if the two orderings actually agree, and
        # every column whose name DID match is a free check on that: if any of
        # them lands on a different class positionally, the orders are skewed
        # and every positional answer here is suspect — including the ones for
        # columns with no name to check against. Fall through to fail closed
        # rather than shift a LOW class onto a CRITICAL column.
        if all(
            positional[col] is output_classes[col]
            for col in columns
            if col in output_classes
        ):
            return positional

    return {
        col: (
            output_classes[col] if col in output_classes else _fail_closed(col, query)
        )
        for col in columns
    }


def _fail_closed(column: str, query: str) -> DataClass:
    sql_hash = sql_digest(query)
    column_hash = hashlib.sha256(column.encode()).hexdigest()[:12]
    # For an ordinary named/expanded projection the column NAME is an
    # identifier DuckDB derives from the query text. But the opaque-projection
    # family this fail-closed path exists to catch — PIVOT, UNPIVOT,
    # COLUMNS(lambda) — is exactly the case where DuckDB derives the column
    # NAME from ROW DATA (e.g. one output column per distinct merchant name),
    # so this path cannot assume the name is safe to log. Only its hash is
    # logged, same treatment as the query text (No PII in logs).
    logger.warning(
        f"sql_query: result column (sha256={column_hash}) absent from lineage "
        f"output; failing closed (sql sha256={sql_hash})"
    )
    return FAIL_CLOSED_CLASS


def _shown_schema(tree: exp.Expr) -> str | None:
    """The schema a ``SHOW ... FROM <schema>`` is scoped to, if it names one.

    sqlglot parses that identifier into a ``Table`` node's NAME slot with no
    ``db``, so the generic table walk reads it as a *table* called ``core`` —
    finds no such table, and refuses ``SHOW TABLES FROM core`` for naming an
    unknown one, with a message that lists ``core`` among the allowed schemas.
    It is a schema; check it as one. (``SHOW TABLES FROM raw`` was refused
    before this, but by that same accident rather than by the schema rule.)

    A catalog-qualified form (``SHOW TABLES FROM cat.sch``) fills ``db`` and
    returns None here, falling through to the generic walk, which refuses it —
    the conservative answer for a shape this gate cannot resolve.
    """
    if not isinstance(tree, exp.Show):
        return None
    source = tree.args.get("from_")
    if isinstance(source, exp.Table) and not source.db:
        return source.name.lower()
    return None


def _refuse_disallowed_schemas(tree: exp.Expr, snapshot: SchemaSnapshot) -> None:
    """Raise unless every schema ``tree`` reaches is an allowed one.

    Shared by the data and metadata paths so both refuse the same targets with
    the same code and message — a caller must not be able to learn which
    spelling the gate forgot about.
    """
    shown = _shown_schema(tree)
    if shown is not None:
        disallowed = [] if shown in ALLOWED_QUERY_SCHEMAS else [shown]
    else:
        disallowed = tables_outside_schemas(tree, snapshot, ALLOWED_QUERY_SCHEMAS)
    if disallowed:
        raise UserError(
            "Queries are limited to these schemas: "
            f"{', '.join(sorted(ALLOWED_QUERY_SCHEMAS))}.",
            code=error_codes.SQL_SCHEMA_NOT_ALLOWED,
            # State the RULE, not a sample of the complement. An earlier hint
            # enumerated refused schemas — a few of roughly ten — which reads as
            # exhaustive and leaves an agent no reason to expect `sqlmesh__core.…`
            # or `main` to be refused. Physical table names are discoverable via
            # SHOW ALL TABLES, so that gap was reachable in practice. The rule
            # now covers raw/prep too: they carry no per-column registry, but
            # every value they return passes the FLOORED content net, which is
            # what makes them safe to admit — and is why naming the complement
            # would have had to be rewritten again here.
            #
            # Name only SQL both surfaces can run. This primitive backs the MCP
            # tool and `moneybin sql query` alike, and there is no CLI
            # counterpart to `sql_schema` — pointing at it would hand a CLI
            # caller a tool it cannot invoke.
            hint=(
                "Those carry per-column privacy classifications or a content-net "
                "floor, which is what makes masking sound; every other schema is "
                "internal and has neither. SHOW ALL TABLES lists what exists."
            ),
            details={"disallowed": sorted(set(disallowed))},
        )


# The DuckDB binder/catalog message has up to three parts: an identifier-
# bearing first line ('Referenced column "x" not found', 'Could not find key
# "y" in struct'), an optional candidate-enumeration clause ('Candidate
# bindings: "a", "b"', 'Candidate Entries: "c"', 'Candidate functions: ...'),
# and a `LINE n:` tail that echoes the query VERBATIM, literal values
# included. Dropping the tail removes the query-echo leak the blanket
# suppression was protecting against.
#
# The first line is not risk-free on its own: DuckDB echoes whatever
# identifier the CALLER wrote, verbatim, including a quoted string naming no
# real column — `SELECT "4111 1111 1111 1111" FROM core.fct_transactions`
# puts that exact text in the head, not just names `sql_schema` already
# publishes. This is a round-trip of caller-authored text, not a new
# disclosure, but it is not nothing either, so `mask_pii_shaped` runs over the
# head as a backstop. That backstop is narrow: it catches exactly two shapes,
# an SSN (`NNN-NN-NNNN`) and 8+ consecutive digits (see log_sanitizer.py) —
# not general PII. A quoted name, address, or other free text the caller
# wrote passes through unmasked. A CatalogException's first line can also
# carry a `Did you mean "x"?` suggestion DuckDB derived from its own catalog
# (a table or function name) — not caller-authored, but not row data either,
# and no more than `SHOW ALL TABLES` already discloses (see
# `test_show_all_tables_exposes_internal_shape_but_no_values`).
#
# The candidate-enumeration clause is where a STORED ROW VALUE can leak.
# DuckDB derives PIVOT/UNPIVOT output column names, and SUMMARIZE/struct-key
# names, from actual row data — a routing number's digit prefix becomes a
# PIVOT column name, a merchant name becomes a struct key — and when a query
# references a name that's a near-miss for one of those, DuckDB's "did you
# mean" suggester lists the real ones it has, verbatim, in this clause. This
# is unlike the first line: it is DuckDB's own enumeration, not an echo of
# what the caller typed, and it can name text no CALLER ever wrote. There is
# no reusable syntactic detector for "this query used a construct that derives
# names from row data" — PIVOT/UNPIVOT/SUMMARIZE/struct access are unrelated
# grammar productions, and a new one could ship in any DuckDB release — so
# rather than enumerate constructs (which rots the moment DuckDB adds one),
# this drops the clause structurally: whatever marker introduces it. A survey
# across unknown column/table/function/schema, near-misses against PIVOT and
# struct-key output, ambiguous references, and malformed GROUP BY/LIMIT/regex
# found the row-derived text always behind this marker and never on the first
# line — see `test_binder_error_head_without_a_line_marker_can_carry_caller_text`
# and the PIVOT/struct-key tests beside it for the pinned cases.
_LINE_ECHO = re.compile(r"^LINE \d+:", re.MULTILINE)
_CANDIDATE_ENUMERATION = re.compile(r"Candidate \w+:")


def _identifier_detail(exc: Exception) -> str | None:
    """The identifier-bearing head of a DuckDB catalog/binder message.

    Truncates at whichever of the `LINE n:` query echo or the
    candidate-enumeration clause (`Candidate bindings:`, `Candidate Entries:`,
    `Candidate functions:`) comes first — both can follow the first line, in
    either order depending on the error shape, and either can carry text this
    primitive must not thread through (see the module comment above).
    """
    text = str(exc)
    cutoffs = [
        m.start()
        for m in (_LINE_ECHO.search(text), _CANDIDATE_ENUMERATION.search(text))
        if m is not None
    ]
    if cutoffs:
        text = text[: min(cutoffs)]
    masked, _ = mask_pii_shaped(text.strip())
    return masked or None


def execute_sql_query(db: Database, query: str, *, max_rows: int) -> SqlQueryResult:
    """Run a read-only SQL query with full privacy enforcement.

    Pipeline: read-only gate → parse → metadata-or-data routing → allowlisted
    schema gate (BOTH branches) → (data only: sqlglot lineage → execute →
    CRITICAL masking). Returns redacted rows plus the resolved tier and
    per-column classes.

    Args:
        db: An open (read-only) database connection.
        query: The SQL query to execute.
        max_rows: Row cap; one extra row is fetched to detect truncation.

    Raises:
        UserError: On a rejected, unparseable, out-of-scope, unknown-table, or
            failed query. The ``code`` is one of the ``error_codes.SQL_*``
            values so both surfaces classify failures identically.
    """
    error = validate_read_only_query(query)
    if error:
        raise UserError(error, code=error_codes.SQL_INVALID_QUERY)

    try:
        tree = parse_cached(query)
    except SqlParseError as e:
        raise UserError(
            "Could not parse SQL.",
            code=error_codes.SQL_INVALID_QUERY,
            details={"detail": str(e)},
        ) from e

    # Everything below stays inside this handler, including the snapshot fetch
    # and the whole metadata branch. A raw duckdb.Error escaping here would
    # reach the CLI as an unhandled traceback (handle_cli_errors passes types it
    # doesn't recognize straight through) carrying DuckDB's message, which can
    # quote the query verbatim — the exact leak the `str(e)` note below guards.
    try:
        snapshot = get_current_schema_snapshot(db)

        # DESCRIBE/SHOW return schema text, not row data — run them directly at
        # LOW; the lineage gate applies only to data queries.
        # Route on the positive metadata test, never on `not is_data_query`:
        # this branch executes its string unclassified, so anything neither
        # recognizably data nor recognizably metadata must be refused, not run.
        if not is_data_query(tree):
            if not is_metadata_query(tree):
                raise UserError(
                    "Only SELECT queries and DESCRIBE/SHOW are supported.",
                    code=error_codes.SQL_INVALID_QUERY,
                )
            # The SCHEMA gate binds this path too. Lineage does not — metadata
            # rows carry no classified column — but "which schemas may I be
            # asked about" is a question about the TARGET, not about the
            # answer's shape, and DESCRIBE names its target as an ordinary
            # table node. Skipping the gate here let `DESCRIBE raw.x` describe
            # a table `SELECT ... FROM raw.x` refuses, so the same secret had a
            # gated spelling and an ungated one.
            _refuse_disallowed_schemas(tree, snapshot)
            columns, rows, truncated = _fetch_metadata(db, query, max_rows)
            records = [dict(zip(columns, row, strict=False)) for row in rows]
            return SqlQueryResult(
                records=records,
                columns=columns,
                output_classes={},
                tier=Tier.LOW,
                total_count=max_rows + 1 if truncated else len(records),
                truncated=truncated,
                is_metadata=True,
            )

        qtree = expand_star(tree, snapshot)
        _refuse_disallowed_schemas(qtree, snapshot)
        output_classes = resolve_output_classes(qtree, snapshot, query)
        columns, rows, truncated = _fetch(db, query, max_rows)
    # SqlSchemaError comes from the lineage qualify step; CatalogException from
    # DuckDB at execute time for an unknown TABLE; BinderException from DuckDB
    # at execute time for an unknown COLUMN (CatalogException is tables only —
    # a column DuckDB cannot bind previously fell through to the generic
    # handler below and returned "Query execution failed." with no name). All
    # three mean "table/column doesn't exist". (Parsing happens above at
    # parse_cached, outside this block, so SqlParseError can't surface here.)
    except (SqlSchemaError, duckdb.CatalogException, duckdb.BinderException) as e:
        # `hint` below is where the head described above (module comment,
        # `_LINE_ECHO`, `_CANDIDATE_ENUMERATION`) reaches the caller — not
        # just identifiers, caller-authored STRING LITERALS too. It reaches
        # the MCP error envelope and the CLI console (stderr) — never the log
        # file, which `handle_cli_errors` (cli/utils.py) echoes deliberately
        # to keep it out of. `str(e)` and the log line right below never
        # carry it either; that line names only the exception TYPE.
        #
        # A binder or catalog error is raised BEFORE execution, so it is NOT
        # true that its message can only quote text the CALLER typed: DuckDB
        # derives PIVOT/UNPIVOT/SUMMARIZE output column names and struct keys
        # from STORED ROW VALUES, and a near-miss reference to one makes
        # DuckDB's own suggester name the real ones in a candidate-enumeration
        # clause — that clause is what `_CANDIDATE_ENUMERATION` strips. What
        # survives after both truncations is the first line, which in every
        # shape probed quotes only the caller's own reference (plus, for a
        # CatalogException, a same-catalog `Did you mean` name — see the
        # module comment). `duckdb.ConversionException` is the case that
        # still can't be threaded at all: it fires evaluating a row at fetch
        # time, so its FIRST LINE quotes the offending stored value directly,
        # with no marker to truncate at — why that family stays in the
        # silent, no-hint bucket below instead of being threaded here.
        logger.warning(
            f"sql_query unknown table/column: {type(e).__name__} "
            f"(sql sha256={sql_digest(query)})"
        )
        raise UserError(
            "Query could not be bound to the schema.",
            code=error_codes.SQL_UNKNOWN_TABLE,
            # This code now means "binder or catalog rejection" — broader
            # than its name: negative LIMIT/OFFSET, an out-of-range GROUP BY
            # term, a malformed regex, and more, not only a missing table or
            # column. A rename/split is a public-contract change under
            # separate review; the code stays as-is for now.
            #
            # SqlSchemaError gets no hint. Its messages are safe today only as
            # a side effect of `_qualified` in sql_lineage.py calling
            # `qualify(..., validate_qualify_columns=False)` — a flag chosen
            # for fallback behavior ("don't raise on unresolved"), not for
            # message safety. Flip it and sqlglot's OptimizeError starts
            # embedding the raw query with no `LINE n:` marker for
            # `_identifier_detail` to split on, silently piping the query into
            # this hint. DuckDB's own CatalogException/BinderException don't
            # depend on that flag, so only they get the identifier thread.
            hint=_identifier_detail(e) if isinstance(e, duckdb.Error) else None,
        ) from e
    except duckdb.Error as e:
        # No detail: ConversionException and friends quote the offending VALUE
        # in the head itself, so there is no safe substring to thread.
        logger.warning(
            f"sql_query execution error: {type(e).__name__} "
            f"(sql sha256={sql_digest(query)})"
        )
        raise UserError(
            "Query execution failed.",
            code=error_codes.SQL_QUERY_ERROR,
        ) from e

    records = [dict(zip(columns, row, strict=False)) for row in rows]
    col_classes = classes_by_result_column(columns, output_classes, query)
    redacted = redact_records(records, col_classes, consent=None)

    return SqlQueryResult(
        records=redacted,
        columns=columns,
        # The per-RESULT-column map, not lineage's raw output. Lineage keys its
        # answer by sqlglot projection name, which for the opaque constructs
        # below is not a result column name at all; reporting that map would let
        # `tier` and `classes_returned` advertise LOW for a column this function
        # just masked.
        output_classes=col_classes,
        tier=derive_query_tier(col_classes),
        # total_count > returned makes has_more true downstream. We don't pay
        # for an exact COUNT(*); +1 signals "at least one more row".
        total_count=max_rows + 1 if truncated else len(records),
        truncated=truncated,
    )


def _fetch_metadata(
    db: Database, query: str, max_rows: int
) -> tuple[list[str], list[Any], bool]:
    """Execute a metadata statement (DESCRIBE/SHOW) at LOW.

    Wraps DuckDB errors in a UserError so the metadata path classifies
    failures the same way the data path does.
    """
    try:
        return _fetch(db, query, max_rows)
    # See execute_sql_query: DuckDB raises BinderException for an unknown
    # COLUMN and CatalogException for an unknown TABLE; both get the same
    # named-identifier treatment here as the data path.
    except (duckdb.CatalogException, duckdb.BinderException) as e:
        logger.warning(
            f"sql_query metadata unknown table/column: {type(e).__name__} "
            f"(sql sha256={sql_digest(query)})"
        )
        raise UserError(
            "Query could not be bound to the schema.",
            # See execute_sql_query: this code now means "binder or catalog
            # rejection", broader than its name; a rename is under separate
            # review.
            code=error_codes.SQL_UNKNOWN_TABLE,
            hint=_identifier_detail(e),
        ) from e
    except duckdb.Error as e:
        # See execute_sql_query: keep str(e) out of both the envelope and the log.
        logger.warning(
            f"sql_query metadata error: {type(e).__name__} "
            f"(sql sha256={sql_digest(query)})"
        )
        raise UserError(
            "Query execution failed.",
            code=error_codes.SQL_QUERY_ERROR,
        ) from e
