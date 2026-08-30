"""Tests for the shared privacy-enforcing SQL execution primitive.

``execute_sql_query`` is the single primitive behind both the ``sql_query``
MCP tool and the ``moneybin sql query`` CLI command. These tests pin the
enforcement contract at the primitive level — redaction, schema gating,
aggregation tiers, truncation, and error classification — so both surfaces
inherit identical behavior structurally.
"""

from __future__ import annotations

import logging

import duckdb
import pytest
from pytest_mock import MockerFixture

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.sql_query import (
    ALLOWED_QUERY_SCHEMAS,
    execute_sql_query,
    validate_read_only_query,
)
from moneybin.privacy.taxonomy import DataClass, Tier

# Every remote URL scheme the read-only validator must reject. Kept in lockstep
# with the filesystems the connection seal disables (`_DISABLED_FILESYSTEMS` in
# database.py) — this validator is the earlier, clearer-message layer of that
# defense-in-depth pair. gs/r2/hf were added alongside the seal's HuggingFace +
# S3-served schemes; https/s3/az/gcs predate it.
_REMOTE_URL_SCHEMES = [
    "https://evil.example/x.parquet",
    "http://evil.example/x.parquet",
    "s3://bucket/x.parquet",
    "az://container/x.parquet",
    "gcs://bucket/x.parquet",
    "gs://bucket/x.parquet",
    "r2://bucket/x.parquet",
    "hf://datasets/user/repo/x.csv",
]


@pytest.mark.parametrize("url", _REMOTE_URL_SCHEMES)
def test_url_scheme_literal_is_rejected(url: str) -> None:
    """A remote URL literal anywhere in the query is refused before execution.

    Guards `_URL_SCHEME_PATTERNS`, including the gs/r2/hf schemes added with the
    extension seal. The query is otherwise a valid read-only SELECT, so the URL
    scheme — not the prefix, a file-access function, or a quoted-path scan — is
    what trips the gate.
    """
    error = validate_read_only_query(
        f"SELECT account_id FROM core.dim_accounts WHERE note = '{url}'"  # noqa: S608  # parametrized test URLs, not user input; asserting the validator rejects them
    )
    assert error is not None
    assert "URL literals" in error


def test_url_scheme_rejection_surfaces_as_user_error(populated_db: Database) -> None:
    """End-to-end: the primitive raises UserError(sql_invalid_query) on a remote scheme."""
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            "SELECT account_id FROM core.dim_accounts WHERE note = 'hf://a/b/c.csv'",
            max_rows=10,
        )
    assert ei.value.code == error_codes.SQL_INVALID_QUERY


def _seed_account(db: Database, last_four: str | None = None) -> None:
    """Insert one account row so masking tests have a CRITICAL value to mask.

    ``last_four`` defaults to NULL so existing callers are unaffected. The
    CRITICAL-transform-substitution tests pass it, because they need a SECOND,
    differently-transformed CRITICAL column (INSTITUTION_ACCOUNT_NUMBER, which
    masks partially) alongside ``routing_number`` (which masks whole) — and they
    assert on returned VALUES, so the row must survive an ``IS NOT NULL`` filter.
    """
    db.execute(
        "INSERT INTO core.dim_accounts "
        "(account_id, routing_number, last_four, account_type) "
        "VALUES ('ACC000123456789', '021000021', ?, 'checking')",
        [last_four],
    )


def _seed_txn(db: Database) -> None:
    """Insert one transaction row so amount/aggregate tests have data."""
    db.execute("""
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount,
             amount_absolute, transaction_direction, description,
             category, source_type, loaded_at, updated_at)
        VALUES (
            'TXN001', 'ACC000123456789', '2025-06-15', -42.50,
            42.50, 'expense', 'Test coffee shop',
            'Food', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)


def test_account_id_passes_through_unmasked(populated_db: Database) -> None:
    """account_id is RECORD_ID (opaque minted surrogate, spec D6) — LOW, unmasked."""
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT account_id FROM core.dim_accounts", max_rows=100
    )
    assert result.tier is Tier.LOW
    assert result.records[0]["account_id"] == "ACC000123456789"


def test_routing_number_masked(populated_db: Database) -> None:
    """CRITICAL routing_number is masked to the fixed placeholder."""
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT routing_number FROM core.dim_accounts", max_rows=100
    )
    assert result.tier is Tier.CRITICAL
    assert result.records[0]["routing_number"] == "*****"


def test_high_amount_passes_through(populated_db: Database) -> None:
    """HIGH-tier amount is returned in the clear — parity with the typed tools."""
    _seed_txn(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT amount FROM core.fct_transactions", max_rows=100
    )
    assert result.tier is Tier.HIGH
    assert result.records[0]["amount"] is not None
    assert not str(result.records[0]["amount"]).startswith("****")


def test_aggregate_is_low(populated_db: Database) -> None:
    """COUNT(*) yields a LOW tier and the aggregate class."""
    _seed_txn(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT category, COUNT(*) AS n FROM core.fct_transactions GROUP BY 1",
        max_rows=100,
    )
    assert result.tier is Tier.LOW
    assert "aggregate" in result.classes_returned


def test_bare_count_star_returns_the_count(populated_db: Database) -> None:
    """An UNALIASED ``COUNT(*)`` must return the number, not a masked value.

    DuckDB names the result column ``count_star()`` while lineage keys it
    ``*`` — a naming-only divergence, not missing lineage. Failing closed on it
    masks the single most common analytical query in existence: the user asks
    how many transactions they have and gets ``'*****'`` back, labelled
    CRITICAL. ``test_aggregate_is_low`` does not cover this because it aliases
    (``COUNT(*) AS n``), which makes the names agree.
    """
    _seed_txn(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT COUNT(*) FROM core.fct_transactions", max_rows=100
    )
    (value,) = result.records[0].values()
    assert value == 1
    assert result.tier is Tier.LOW


def test_unaliased_mixed_projection_keeps_each_column_own_class(
    populated_db: Database,
) -> None:
    """Reconciling names positionally must not hand a class to the wrong column.

    ``routing_number`` matches by name; ``COUNT(*)`` does not. Both must land on
    their own class — the CRITICAL one masked, the aggregate returned.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT routing_number, COUNT(*) FROM core.dim_accounts GROUP BY 1",
        max_rows=100,
    )
    routing, count = result.records[0].values()
    assert str(routing).startswith("*")
    assert count == 1


def test_duplicate_result_column_names_fail_closed(populated_db: Database) -> None:
    """Two result columns sharing a name destroy per-column identity.

    ``SELECT 0 AS routing_number, COLUMNS('routing_number')`` yields two columns
    both named ``routing_number``. Lineage sees only the literal (AGGREGATE), a
    name lookup hands that safe class to BOTH, and ``dict(zip(...))`` keeps the
    LAST value — so the real routing number was returned in the clear under a
    LOW tier. A name that does not identify exactly one column cannot key the
    class map.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT 0 AS routing_number, COLUMNS('routing_number') FROM core.dim_accounts",
        max_rows=100,
    )
    assert "021000021" not in str(result.records)
    assert result.tier is Tier.CRITICAL


def test_metadata_query_not_classified(populated_db: Database) -> None:
    """DESCRIBE is metadata: LOW, no row data classes, returns schema rows."""
    result = execute_sql_query(
        populated_db, "DESCRIBE core.fct_transactions", max_rows=100
    )
    assert result.is_metadata is True
    assert result.tier is Tier.LOW
    assert result.output_classes == {}
    assert len(result.records) > 0
    assert result.classes_returned == ["aggregate"]


# Statements the metadata path used to run unclassified. Each is a spelling an
# agent can reach today; none may return a CRITICAL value or reach a schema the
# data path refuses.
_METADATA_SPELLINGS = [
    "PRAGMA storage_info('core.dim_accounts')",
    "PRAGMA table_info('core.dim_accounts')",
    "PRAGMA metadata_info",
    "DESCRIBE core.dim_accounts",
    "SHOW ALL TABLES",
]


def _payload(db: Database, sql: str) -> str:
    """Everything ``sql`` hands back to the caller, or '' when it is refused.

    A refusal and a masked answer are both acceptable outcomes; returning the
    value is not. Collapsing them here lets a test assert the invariant that
    actually matters — the secret never reaches the caller — instead of pinning
    one particular refusal, which a future change could satisfy while a
    different statement went on leaking.
    """
    try:
        return repr(execute_sql_query(db, sql, max_rows=200).records)
    except UserError:
        return ""


@pytest.mark.parametrize("sql", _METADATA_SPELLINGS)
def test_metadata_path_never_returns_a_critical_value(
    populated_db: Database, sql: str
) -> None:
    """No metadata spelling returns CRITICAL row data, in whole or in part.

    ``PRAGMA storage_info`` reports per-segment min/max statistics, which for a
    VARCHAR column are a CLEARTEXT PREFIX of the stored value:
    ``[Min: 02100002, Max: 02100002, ...]`` for a ``routing_number`` of
    ``021000021``. Eight of nine digits is the whole secret — an ABA routing
    number's ninth digit is a check digit determined by the first eight
    (3(d1+d4+d7) + 7(d2+d5+d8) + (d3+d6+d9) ≡ 0 mod 10), so the leaked prefix
    reconstructs the full number arithmetically.

    Asserting on the PREFIX, not just the full value, is the point. The full
    string never appears in ``stats`` at all, so a test that looked only for
    ``021000021`` would have passed against the live leak.
    """
    _seed_account(populated_db, last_four="4321")
    populated_db.execute("CHECKPOINT")  # stats are computed when segments flush

    payload = _payload(populated_db, sql)

    assert "021000021" not in payload
    assert "02100002" not in payload


def test_metadata_path_cannot_reach_a_schema_the_data_path_refuses(
    populated_db: Database,
) -> None:
    """The schema allowlist binds the metadata path too, not just SELECT.

    Without this, the two paths disagree about the same table: ``SELECT ssn
    FROM meta.leaky`` is refused by the allowlist while ``DESCRIBE meta.leaky``
    describes it — and ``PRAGMA storage_info('meta.leaky')`` returned that
    column's min/max outright. The refusal must not depend on which spelling
    the caller reaches for.

    Re-aimed from ``raw`` to ``meta`` when M2O.2 admitted ``raw``/``prep``. The
    property under test is path parity over a *refused* schema, so the fixture
    has to sit in the complement; ``meta`` is a real MoneyBin schema that stays
    internal. Nothing else about the shape changes.
    """
    populated_db.execute("CREATE SCHEMA IF NOT EXISTS meta")
    populated_db.execute("CREATE TABLE meta.leaky (ssn VARCHAR)")
    populated_db.execute("INSERT INTO meta.leaky VALUES ('123456789')")
    populated_db.execute("CHECKPOINT")

    with pytest.raises(UserError) as ei:
        execute_sql_query(populated_db, "SELECT ssn FROM meta.leaky", max_rows=100)
    assert ei.value.code == error_codes.SQL_SCHEMA_NOT_ALLOWED

    # Every spelling that names the table, not just the bare-table one. A
    # DESCRIBE can wrap a whole SELECT, and SHOW takes a FROM-schema form; both
    # reach the same table by a route the bare-table case would not have
    # exercised.
    for sql in (
        "DESCRIBE meta.leaky",
        "describe META.leaky",
        "DESCRIBE SELECT * FROM meta.leaky",
        "SHOW TABLES FROM meta",
        "SHOW TABLES FROM META",
    ):
        with pytest.raises(UserError) as metadata_error:
            execute_sql_query(populated_db, sql, max_rows=100)
        assert metadata_error.value.code == error_codes.SQL_SCHEMA_NOT_ALLOWED, sql

    for sql in ("PRAGMA storage_info('meta.leaky')", "PRAGMA table_info('meta.leaky')"):
        assert "12345678" not in _payload(populated_db, sql)


_UNGATEABLE_STATEMENTS = [
    "PRAGMA show_tables",
    "PRAGMA storage_info('core.dim_accounts')",
    "EXPLAIN SELECT 1",
    # Named tables the gate WOULD admit. The refusal has to come from the
    # statement kind alone, so a fixture on a fenced schema would prove nothing:
    # it would be refused either way and could not tell the two guards apart.
    "EXPLAIN SELECT routing_number FROM raw.ofx_accounts",
    "EXPLAIN ANALYZE SELECT count(*) FROM raw.ofx_accounts",
]


@pytest.mark.parametrize("sql", _UNGATEABLE_STATEMENTS)
def test_ungateable_statements_are_refused(populated_db: Database, sql: str) -> None:
    """PRAGMA and EXPLAIN are refused: the schema gate cannot see their targets.

    Both hide their target from ``tables_outside_schemas``, for different
    reasons — a PRAGMA's is a string literal inside ``exp.Anonymous``, and an
    EXPLAIN's whole payload stays unparsed inside ``exp.Command`` (sqlglot has
    no DuckDB EXPLAIN node). Either way ``find_all(exp.Table)`` returns nothing,
    so every one of them reads as table-free and passes a gate that never
    examined anything. Admitting the kind is therefore admitting it ungated.

    That is not theoretical for either: ``PRAGMA storage_info`` returned a
    CRITICAL routing number's cleartext prefix, and ``EXPLAIN ANALYZE``
    *executes* its inner query, returning row counts at LOW from a path meant to
    run schema text — over any table, including the declared-CRITICAL columns
    the two fixtures below name.

    The surviving rule is one line: a statement is executable only if the gate
    can resolve every table it names. SELECT/WITH and DESCRIBE expose real
    ``exp.Table`` nodes and qualify; SHOW names no table and has nothing to
    resolve; these two claim to name tables while hiding them, and do not.
    """
    assert validate_read_only_query(sql) is not None

    with pytest.raises(UserError) as ei:
        execute_sql_query(populated_db, sql, max_rows=100)
    assert ei.value.code == error_codes.SQL_INVALID_QUERY


def test_show_all_tables_exposes_internal_shape_but_no_values(
    populated_db: Database,
) -> None:
    """Pins the one hole the schema gate structurally cannot close.

    ``tables_outside_schemas`` works by resolving table REFERENCES, and ``SHOW
    ALL TABLES`` contains none — it is a catalog listing, so there is nothing
    for the gate to check. DuckDB's listing happens to carry a
    ``column_names``/``column_types`` array per table, so the SHAPE of every
    schema in the database stays reachable, including the ones the gate fences.

    The fixture sits in ``meta`` rather than ``raw``: since M2O.2 admitted
    ``raw``/``prep``, ``DESCRIBE raw.x`` succeeds, so a ``raw`` fixture would
    show shape through the ordinary metadata path and prove nothing about this
    listing. ``meta`` is refused by every gated spelling, so what leaks here
    leaks through ``SHOW ALL TABLES`` alone.

    That asymmetry is deliberate and documented (``docs/guides/sql-access.md``)
    rather than accidental, so it is pinned here: the line is structure vs.
    values. If a later change closes it, this test should fail and be updated
    deliberately — and the row-value assertion below must survive that change
    either way.
    """
    populated_db.execute("CREATE SCHEMA IF NOT EXISTS meta")
    populated_db.execute("CREATE TABLE meta.internal_only (ssn VARCHAR)")
    populated_db.execute("INSERT INTO meta.internal_only VALUES ('123456789')")
    populated_db.execute("CHECKPOINT")

    payload = _payload(populated_db, "SHOW ALL TABLES")

    # Current boundary: internal shape is visible.
    assert "internal_only" in payload
    assert "ssn" in payload
    # The line that must never move: no row values, whole or partial.
    assert "123456789" not in payload
    assert "12345678" not in payload


@pytest.mark.parametrize(
    "sql",
    [
        "DESCRIBE CORE.dim_accounts",
        "DESCRIBE Core.Dim_Accounts",
        'DESCRIBE "CORE".dim_accounts',
        "SHOW TABLES FROM CORE",
    ],
)
def test_allowed_schema_is_matched_case_insensitively(
    populated_db: Database, sql: str
) -> None:
    """An allowed schema stays allowed however the caller cases it.

    DuckDB identifiers are case-insensitive (case-preserving, but matched
    without regard to case, quoted or not), so ``CORE.dim_accounts`` and
    ``core.dim_accounts`` are the same table and DuckDB runs both.

    The data path never had to think about this: it gates the tree returned by
    ``expand_star``, which qualifies identifiers and normalizes their case on
    the way. The metadata path gates the raw parsed tree, where the caller's
    casing survives — so comparing it against a lowercase allowlist refused
    ``DESCRIBE CORE.dim_accounts`` while ``SELECT ... FROM CORE.dim_accounts``
    succeeded. That is the same one-spelling-gated/one-not asymmetry this gate
    exists to remove, pointed the other way: a false refusal rather than a
    false admission.

    Note ``raw``-cased fixtures cannot catch this — ``RAW`` is refused whatever
    its case — so only an ALLOWED schema in non-canonical case isolates it.
    """
    result = execute_sql_query(populated_db, sql, max_rows=100)

    assert result.is_metadata is True
    assert len(result.records) > 0


def test_snapshot_failure_is_classified_not_raised_raw(
    populated_db: Database, mocker: MockerFixture
) -> None:
    """A DuckDB failure while building the schema snapshot stays classified.

    The snapshot is fetched before the metadata/data fork so both paths can be
    schema-gated from it, which puts it ahead of the handler that converts
    DuckDB errors into ``UserError``. It has to stay inside that handler: a raw
    ``duckdb.Error`` is not one of the types ``handle_cli_errors`` recognizes,
    so it would reach the CLI as an unhandled traceback — and DuckDB error text
    can quote the query verbatim, including its literal values, which is the
    whole reason this module never echoes ``str(e)`` to the caller.
    """
    mocker.patch(
        "moneybin.privacy.sql_query.get_current_schema_snapshot",
        side_effect=duckdb.IOException("disk fell over reading 021000021"),
    )

    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db, "SELECT account_id FROM core.dim_accounts", max_rows=10
        )

    assert ei.value.code == error_codes.SQL_QUERY_ERROR
    assert "021000021" not in str(ei.value)


def test_metadata_path_still_answers_schema_questions(populated_db: Database) -> None:
    """The benign case keeps working — the gate must not fail closed on everything.

    A privacy fix that refused all metadata would pass every leak test above
    while destroying the surface, and no test here would notice. This is that
    test: DESCRIBE on an allowed schema, and the catalog listing, still return
    rows.
    """
    described = execute_sql_query(
        populated_db, "DESCRIBE core.dim_accounts", max_rows=100
    )
    assert described.is_metadata is True
    assert [r["column_name"] for r in described.records].count("routing_number") == 1

    listed = execute_sql_query(populated_db, "SHOW ALL TABLES", max_rows=200)
    assert len(listed.records) > 0


def test_disallowed_schema_raises(populated_db: Database) -> None:
    """Querying outside the allowlist raises UserError with the schema-gate code.

    The gate fires on schema name before execution, so the table need not exist.
    ``meta`` replaced ``raw.ofx_transactions`` when M2O.2 admitted ``raw``/``prep``;
    the assertion is about the complement, so the fixture must live in it.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db, "SELECT account_id FROM meta.internal_only", max_rows=100
        )
    assert ei.value.code == error_codes.SQL_SCHEMA_NOT_ALLOWED


def test_invalid_sql_raises(populated_db: Database) -> None:
    """Syntactically invalid SQL raises UserError(sql_invalid_query)."""
    with pytest.raises(UserError) as ei:
        execute_sql_query(populated_db, "SELECT FROM WHERE )(", max_rows=100)
    assert ei.value.code == error_codes.SQL_INVALID_QUERY


def test_write_query_raises(populated_db: Database) -> None:
    """Write SQL is rejected by the read-only gate before parsing."""
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db, "INSERT INTO core.fct_transactions VALUES (1)", max_rows=100
        )
    assert ei.value.code == error_codes.SQL_INVALID_QUERY


# Every keyword the old text-scanning guard blocked, lowercased the way the
# issue's repro used them — write detection is case-insensitive either way.
_WRITE_KEYWORDS = [
    "insert",
    "update",
    "delete",
    "drop",
    "create",
    "alter",
    "truncate",
    "replace",
    "merge",
    "copy",
    "attach",
    "detach",
    "export",
    "import",
]


@pytest.mark.parametrize("keyword", _WRITE_KEYWORDS)
def test_write_keyword_inside_string_literal_is_not_rejected(keyword: str) -> None:
    """A write keyword occurring only inside a quoted literal is not a write.

    Regression test for #447: the write-operation guard used to scan raw
    query text, so ``SELECT 'export' AS probe`` was refused as though it
    contained a real ``EXPORT`` statement — one character away,
    ``SELECT 'expor' AS control`` was always accepted. The bug was scanning
    quoted content at all, not anything specific to the word "export". The
    guard now checks the parsed AST's expression types instead of matching
    text, so a word appearing in a position sqlglot never resolves to a write
    node (a literal, an identifier, a comment) cannot trip it.
    """
    assert validate_read_only_query(f"SELECT '{keyword}' AS probe") is None


def test_write_keyword_inside_string_literal_with_escaped_quote() -> None:
    """An escaped quote (``''``) inside the literal is still just data.

    Pins that the shared parser (also used for schema/lineage resolution)
    tokenizes ``it''s`` as one literal rather than ending it early at the
    first apostrophe, which would otherwise un-mask ``update`` between them.
    """
    assert validate_read_only_query("SELECT 'it''s time to update' AS note") is None


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT e'export' AS probe", id="escape-string"),
        pytest.param("SELECT $$export$$ AS probe", id="dollar-quoted-string"),
        pytest.param('SELECT "export" FROM t', id="double-quoted-identifier"),
        pytest.param("SELECT 1 -- export this later", id="trailing-comment"),
    ],
)
def test_write_keyword_outside_a_string_literal_is_not_rejected(sql: str) -> None:
    """A write keyword in a non-literal quoted/commented position is not a write.

    DuckDB has more ways to carry the text "export" without writing anything:
    an escape string (``e'...'``), a dollar-quoted string (``$$...$$``), a
    double-quoted identifier (a column literally named ``export``), and a
    ``--`` comment. None of these produce a write-type AST node, so the
    AST-based guard passes all four (verified against the real ``duckdb``
    engine that each is valid, keyword-free SQL).
    """
    assert validate_read_only_query(sql) is None


def test_real_write_keyword_still_rejected_alongside_string_literal() -> None:
    """A quoted literal elsewhere must not blind the guard to a REAL write.

    The query is a legal read-only prefix (``WITH ...``) carrying a write verb
    in the CTE body, plus an unrelated string literal that also happens to
    contain a write keyword — proving the AST check only reacts to an actual
    write-type node, not to the word appearing anywhere in the text.
    """
    error = validate_read_only_query(
        "WITH x AS (UPDATE core.fct_transactions SET amount = 1) "
        "SELECT 'export' AS probe FROM x"
    )
    assert error is not None
    assert "Write operations" in error


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(
            "WITH x AS (INSERT INTO y VALUES (1)) SELECT 1 FROM x", id="insert"
        ),
        pytest.param("WITH x AS (DELETE FROM y) SELECT 1 FROM x", id="delete"),
        pytest.param("WITH x AS (CREATE TABLE y (a INT)) SELECT 1 FROM x", id="create"),
        pytest.param("WITH x AS (DROP TABLE y) SELECT 1 FROM x", id="drop"),
        pytest.param("WITH x AS (ATTACH 'y.db' AS y) SELECT 1 FROM x", id="attach"),
        pytest.param("WITH x AS (DETACH y) SELECT 1 FROM x", id="detach"),
        pytest.param(
            "WITH x AS (MERGE INTO t USING s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET x = s.x) SELECT 1 FROM x",
            id="merge",
        ),
    ],
)
def test_write_type_nested_in_cte_is_independently_isolated(sql: str) -> None:
    """Each AST write-node type the guard checks is exercised on its own.

    A bare top-level write statement (e.g. ``DROP TABLE t``) is already
    refused by the read-only-prefix check before the AST write check ever
    runs, so a fixture using only bare statements would still pass even with
    the corresponding entry deleted from ``_WRITE_EXPRESSION_TYPES`` — the
    "fixture trips two guards, isolates neither" trap this repo's testing.md
    documents. Nesting each write inside a CTE forces the prefix check to
    pass, so only the structural write check can be what refuses it.

    UPDATE already has its own CTE-nested test above
    (``test_real_write_keyword_still_rejected_alongside_string_literal``).
    TruncateTable, Alter, and Copy are absent here because DuckDB's own
    grammar (via sqlglot) refuses to parse any of them as a CTE body at all —
    verified, each raises a ``ParseError`` rather than reaching this check in
    either a bare or nested position, so no isolating fixture is
    constructible for them; they stay in the checked tuple purely as
    defense-in-depth (see the module comment on `_WRITE_EXPRESSION_TYPES`).
    """
    error = validate_read_only_query(sql)
    assert error is not None
    assert "Write operations" in error


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("EXPORT DATABASE 'dir'", id="export-bare"),
        pytest.param("IMPORT DATABASE 'dir'", id="import-bare"),
        pytest.param(
            "REPLACE INTO core.fct_transactions VALUES (1)", id="replace-bare"
        ),
        pytest.param(
            "WITH x AS (SELECT 1) EXPORT DATABASE 'dir'", id="export-with-prefixed"
        ),
        pytest.param(
            "WITH x AS (SELECT 1) IMPORT DATABASE 'dir'", id="import-with-prefixed"
        ),
        pytest.param(
            "WITH x AS (SELECT 1) REPLACE INTO core.fct_transactions VALUES (1)",
            id="replace-with-prefixed",
        ),
    ],
)
def test_keywords_with_no_dedicated_ast_node_are_still_refused(
    sql: str, populated_db: Database
) -> None:
    """EXPORT/IMPORT DATABASE and bare REPLACE INTO are refused end to end.

    These three have no dedicated sqlglot node for the duckdb dialect, so
    `_WRITE_EXPRESSION_TYPES` deliberately doesn't name them (see the module
    comment). Pins that the rest of the pipeline still refuses every shape
    regardless: a bare statement fails the read-only-prefix check, and a
    WITH-prefixed attempt is a genuine DuckDB syntax error (a CTE body must be
    a SELECT) that fails to parse — `validate_read_only_query` defers a parse
    failure to the caller, and `execute_sql_query`'s own parse then raises
    `sql_invalid_query`.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(populated_db, sql, max_rows=10)
    assert ei.value.code == error_codes.SQL_INVALID_QUERY


def test_audit_log_export_action_query_executes(populated_db: Database) -> None:
    """``... WHERE action LIKE 'export%'`` against ``app.audit_log`` returns rows.

    The motivating case from #447: this exact shape was refused before the
    fix because ``'export%'`` contains the blocked ``EXPORT`` keyword inside a
    string literal, masking the real binder/execution behavior entirely.
    """
    populated_db.execute(
        "INSERT INTO app.audit_log (audit_id, actor, action, operation_id) "
        "VALUES (?, ?, ?, ?)",
        ["audit_test_1", "system", "export.run", "op_test_1"],
    )
    result = execute_sql_query(
        populated_db,
        "SELECT action FROM app.audit_log WHERE action LIKE 'export%'",
        max_rows=10,
    )
    assert result.records == [{"action": "export.run"}]


def test_multi_statement_query_is_rejected() -> None:
    """Two statements in one string are refused before any classification.

    Guards the trailing-statement bypass: the read-only prefix check, the
    file/URL scans, and the write-pattern scan all pass on
    ``SELECT 1; SELECT <critical> FROM ...`` because every statement is
    individually a legal read. Only a statement-count check catches it.
    """
    error = validate_read_only_query(
        "SELECT 1 AS a; SELECT routing_number AS a FROM core.dim_accounts"
    )
    assert error is not None
    assert "one statement" in error


def test_trailing_comment_or_semicolon_is_still_one_statement() -> None:
    """``SELECT 1; -- note`` is one statement and must be accepted.

    sqlglot puts the tail of ``SELECT 1; -- note`` in an ``exp.Block`` beside
    the SELECT — as an ``exp.Semicolon`` carrying the comment, or ``None`` for
    a bare extra ``;``. Treating any ``Block`` as multi-statement therefore
    refuses two ordinary ways to end a hand-written query. Only a Block
    holding more than one real statement is the smuggling shape.
    """
    assert validate_read_only_query("SELECT 1 AS a; -- how many rows") is None
    assert validate_read_only_query("SELECT 1 AS a;;") is None


def test_trailing_comment_query_still_executes(populated_db: Database) -> None:
    """A query ending ``; -- note`` runs and classifies like the bare statement.

    Accepting it at the validator is not enough: it reaches the router still
    wrapped in an ``exp.Block``, which is neither data nor metadata, so the
    fail-closed route would refuse it one layer later. The single real
    statement has to be unwrapped before any of that.
    """
    result = execute_sql_query(
        populated_db,
        "SELECT COUNT(*) AS n FROM core.dim_accounts; -- how many",
        max_rows=100,
    )
    assert result.records[0]["n"] >= 0
    assert result.is_metadata is False


def test_trailing_statement_cannot_smuggle_critical_columns(
    populated_db: Database,
) -> None:
    """A second statement cannot return CRITICAL data unclassified.

    DuckDB executes a multi-statement string and returns the LAST statement's
    rows, while the classifier reads the first. Before the statement-count
    gate, ``is_data_query`` saw the two-statement ``Block`` as non-data and
    routed the whole string to the metadata path — executing it and returning
    routing numbers at ``Tier.LOW`` with ``output_classes == {}``, bypassing
    redaction entirely.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            "SELECT 1 AS a; SELECT routing_number AS a FROM core.dim_accounts",
            max_rows=100,
        )
    assert ei.value.code == error_codes.SQL_INVALID_QUERY


def test_multi_line_query_with_comments_still_executes(
    populated_db: Database,
) -> None:
    """Ordinary formatted SQL — newlines, indentation, inline comments — runs.

    The statement gate now reads the raw text, so newlines carry meaning they
    did not before. Nothing about a query being *formatted* may make it look
    like smuggling: this is the benign twin of the smuggling test below, and
    the case a fail-closed fix would silently break without ever failing a
    privacy assertion.
    """
    result = execute_sql_query(
        populated_db,
        "SELECT\n"
        "    COUNT(*) AS n  -- how many accounts\n"
        "FROM core.dim_accounts\n"
        "WHERE account_id IS NOT NULL\n",
        max_rows=100,
    )
    assert result.records[0]["n"] >= 0
    assert result.is_metadata is False


def test_comment_smuggled_statement_cannot_bypass_the_gate(
    populated_db: Database,
) -> None:
    """A `--` comment must not hide a second statement from the classifier.

    The gate read whitespace-collapsed text while DuckDB read the original. A
    `--` comment ends at a newline, so collapsing it swallowed the statement
    that followed: the classifier saw one benign ``SELECT 1 AS a`` and DuckDB
    returned the smuggled statement's rows. Aliasing both to ``a`` matched the
    classified column name, so the fail-closed name check never fired and
    routing numbers returned at ``Tier.LOW`` (#346).
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            "SELECT 1 AS a; -- note\nSELECT routing_number AS a FROM core.dim_accounts",
            max_rows=100,
        )
    assert ei.value.code == error_codes.SQL_INVALID_QUERY


def test_unknown_table_raises(populated_db: Database) -> None:
    """A nonexistent table raises UserError(sql_unknown_table)."""
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db, "SELECT * FROM core.does_not_exist", max_rows=100
        )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE


def test_select_star_masks_every_critical_column(populated_db: Database) -> None:
    """SELECT * masks all CRITICAL columns regardless of column order.

    Redaction maps DuckDB result columns to classes BY NAME, so it cannot be
    fooled by any divergence between sqlglot's `*` expansion order and DuckDB's
    runtime column order (the round-5 SELECT * bypass). account_id is RECORD_ID
    (spec D6) — it passes through; the CRITICAL routing_number is masked.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT * FROM core.dim_accounts", max_rows=100
    )
    row = result.records[0]
    assert row["account_id"] == "ACC000123456789"  # RECORD_ID — not masked
    assert row["routing_number"] == "*****"  # CRITICAL — masked
    assert result.tier is Tier.CRITICAL


def test_union_reused_alias_masks_critical(populated_db: Database) -> None:
    """A UNION reusing one alias for two tables still masks the CRITICAL column.

    Both branches bind alias ``a`` to a different table; branch 0 projects the
    CRITICAL ``routing_number``. Per-branch alias scoping classifies the output
    position CRITICAL, so every value in that column is masked — the routing
    number is never returned in the clear (the round-6 UNION alias-collision
    leak).
    """
    _seed_account(populated_db)
    _seed_txn(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT a.routing_number FROM core.dim_accounts a "
        "UNION ALL "
        "SELECT a.description FROM core.fct_transactions a",
        max_rows=100,
    )
    assert result.tier is Tier.CRITICAL
    values = [str(r["routing_number"]) for r in result.records]
    assert "021000021" not in values
    assert all(v == "*****" for v in values)


def test_unaliased_aggregate_over_critical_column_is_masked(
    populated_db: Database,
) -> None:
    """An unaliased expression DuckDB names differently is still masked.

    ``MIN(routing_number)`` → DuckDB column ``min(routing_number)`` vs lineage
    ``?_0``. The projection count is preserved, so this reconciles positionally
    onto lineage's own answer (ROUTING_NUMBER) rather than failing closed —
    a different mechanism reaching the same required outcome. What this test
    pins is the outcome: the aggregate of a CRITICAL column is never returned
    in the clear, whichever branch of ``classes_by_result_column`` claims it.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT MIN(routing_number) FROM core.dim_accounts", max_rows=100
    )
    (value,) = result.records[0].values()
    assert str(value).startswith("****")
    assert result.tier is Tier.CRITICAL


def test_unknown_table_error_omits_raw_detail(populated_db: Database) -> None:
    """The unknown-table error must not echo the raw query/DuckDB message.

    str(e) from DuckDB/lineage can quote the query verbatim (literal values
    included); it stays in the server log, never the client-facing envelope.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            "SELECT x FROM core.does_not_exist WHERE note = 'acct 4111111111111111'",
            max_rows=10,
        )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    assert ei.value.details is None


def test_unknown_column_returns_unknown_table_code_and_names_the_column(
    populated_db: Database,
) -> None:
    """An unknown COLUMN classifies the same as an unknown table and is named.

    DuckDB raises BinderException here, not CatalogException.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            "SELECT nonexistent_col FROM core.fct_transactions",
            max_rows=10,
        )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    assert "nonexistent_col" in (ei.value.hint or "")


def test_unknown_column_hint_does_not_echo_query_literals(
    populated_db: Database,
) -> None:
    """The hint carries the identifier head only, never the ``LINE n:`` tail.

    That tail echoes the submitted query verbatim, literals included. The
    literal here (``'Dr Smith Cardiology'``) is deliberately a shape
    ``mask_pii_shaped`` does not catch — no SSN pattern, no 8+ consecutive
    digits — so a pass here can only be explained by the ``LINE`` split, not
    by the PII-mask backstop also stripping it.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            "SELECT nosuchcol FROM core.fct_transactions WHERE description = "
            "'Dr Smith Cardiology'",
            max_rows=10,
        )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    assert "Dr Smith Cardiology" not in (ei.value.hint or "")
    assert "LINE" not in (ei.value.hint or "")


def test_binder_error_head_without_a_line_marker_can_carry_caller_text(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """Some BinderExceptions carry no ``LINE n:`` tail at all — pin that shape.

    ``SELECT ({'a':1})['<literal>']`` is DuckDB's struct key-lookup error: it
    carries no ``LINE`` marker at all, so the ``LINE`` split has nothing to cut.
    (It does emit a ``Candidate Entries: "a"`` clause, which the candidate-
    enumeration split drops — but that clause lists only the literal's own key,
    so dropping it removes nothing the caller did not write.) The literal the
    caller wrote reaches
    ``hint`` unmodified — this module documents that as an accepted contract,
    not a gap (see the comment above the ``except`` clause in
    ``execute_sql_query``): the first line quotes only what the caller typed.
    (A struct key DuckDB derives from row data, not a literal, is the
    genuinely dangerous shape — see
    ``test_pivot_near_miss_hint_omits_row_derived_candidate_prefixes`` and
    ``test_struct_key_near_miss_hint_omits_row_derived_candidate_keys``
    below, where the candidate-enumeration clause carries exactly that and is
    dropped.) Making the ``LINE`` split fail-closed to plug this was tried
    and rejected — see the module comment above ``_LINE_ECHO`` for the
    genuinely useful no-marker messages (e.g. a GROUP BY error) that would
    break instead.

    The literal is deliberately lowercase: DuckDB downcases struct keys in
    this error, so an exact-match assertion needs a literal that survives
    that unchanged.
    """
    literal = "acme plumbing llc"
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        with pytest.raises(UserError) as ei:
            execute_sql_query(
                populated_db, f"SELECT ({{'a':1}})['{literal}']", max_rows=10
            )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    assert "LINE" not in (ei.value.hint or "")
    assert literal in (ei.value.hint or "")
    assert literal not in ei.value.message
    assert literal not in caplog.text


def test_pivot_near_miss_hint_omits_row_derived_candidate_prefixes(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """A near-miss PIVOT reference must not leak candidate routing-number prefixes.

    DuckDB names PIVOT output columns from the pivoted expression's runtime
    VALUES — here, ``substr(routing_number, 1, 4)`` over three distinct
    accounts. Referencing a near-miss column (``"0211"``, one digit off every
    real prefix) makes DuckDB's binder list the real prefixes it has in a
    ``Candidate bindings:`` clause, verbatim:

        Binder Error: Referenced column "0211" not found in FROM clause!
        Candidate bindings: "0210", "0260", "1210"

    Those prefixes are read out of STORED rows, not typed by the caller, and
    a 4-digit run passes `mask_pii_shaped`'s 8+-consecutive-digit backstop
    clean. Only truncating at the ``Candidate `` marker keeps them out of the
    hint. The literal the caller DID type (``"0211"``) is expected to survive
    in the first line — it identifies which reference failed without
    revealing any account's real prefix.
    """
    database = populated_db
    database.execute(
        "INSERT INTO core.dim_accounts "
        "(account_id, routing_number, last_four, account_type) VALUES "
        "('ACC1', '021000021', '1111', 'checking'),"
        "('ACC2', '121000248', '2222', 'checking'),"
        "('ACC3', '026009593', '3333', 'checking')"
    )
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        with pytest.raises(UserError) as ei:
            execute_sql_query(
                database,
                'SELECT "0211" FROM '
                "(PIVOT core.dim_accounts ON substr(routing_number,1,4) "
                "USING count(*))",
                max_rows=10,
            )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    hint = ei.value.hint or ""
    assert "0211" in hint
    for real_prefix in ("0210", "0260", "1210"):
        assert real_prefix not in hint
    assert "Candidate" not in hint
    assert "LINE" not in hint
    for real_prefix in ("0210", "0260", "1210"):
        assert real_prefix not in ei.value.message
        assert real_prefix not in caplog.text


def test_struct_key_near_miss_hint_omits_row_derived_candidate_keys(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """A near-miss struct-key reference must not leak the real keys from a row.

    Struct keys DuckDB derives from a row's own data — here, merchant names
    aggregated into a MAP — surface the same way: an unknown key lookup lists
    the real keys it has under ``Candidate Entries:``. The literal the caller
    typed (an intentional misspelling) is expected to survive in the first
    line; the real merchant names must not.
    """
    database = populated_db
    # A curly-brace literal builds a STRUCT, whose field names are part of the
    # static TYPE — unlike MAP, where a missing-key lookup returns NULL at
    # runtime instead of raising. That static typing is what makes DuckDB's
    # binder able to name the real keys before the query ever executes.
    database.execute("""
        CREATE TABLE core.fct_transactions_by_merchant AS
        SELECT {
            'acme plumbing llc': 1,
            'bobs burgers': 2,
            'carla consulting': 3
        } AS merchant_counts
    """)
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        with pytest.raises(UserError) as ei:
            execute_sql_query(
                database,
                "SELECT merchant_counts['acme plumbing llx'] "
                "FROM core.fct_transactions_by_merchant",
                max_rows=10,
            )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    hint = ei.value.hint or ""
    assert "acme plumbing llx" in hint
    for real_key in ("acme plumbing llc", "bobs burgers", "carla consulting"):
        assert real_key not in hint
    assert "Candidate" not in hint
    for real_key in ("acme plumbing llc", "bobs burgers", "carla consulting"):
        assert real_key not in ei.value.message
        assert real_key not in caplog.text


def test_conversion_error_still_says_nothing(populated_db: Database) -> None:
    """ConversionException stays in the generic no-detail bucket.

    It quotes the offending VALUE in its head, not just the ``LINE`` tail, so
    there is no safe substring of it to thread through.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            "SELECT CAST('ACCT-12345678' AS INTEGER) AS n",
            max_rows=10,
        )
    assert ei.value.code == error_codes.SQL_QUERY_ERROR
    assert ei.value.hint is None
    assert "ACCT-12345678" not in ei.value.message


def test_truncation_sets_total_count(populated_db: Database) -> None:
    """When rows exceed max_rows, records are capped and total_count signals more."""
    _seed_txn(populated_db)
    populated_db.execute("""
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount,
             amount_absolute, transaction_direction, description,
             category, source_type, loaded_at, updated_at)
        VALUES (
            'TXN002', 'ACC000123456789', '2025-06-16', -10.00,
            10.00, 'expense', 'Second row', 'Food', 'ofx',
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)
    result = execute_sql_query(
        populated_db, "SELECT amount FROM core.fct_transactions", max_rows=1
    )
    assert len(result.records) == 1
    assert result.truncated is True
    assert result.total_count > len(result.records)


# ---------------------------------------------------------------------------
# End-to-end masking for the shadowing + set-operation under-classification
# leaks. These assert on the RETURNED VALUE, not just the tier: the tier is
# what the classifier says, the value is what the user actually receives.
# ---------------------------------------------------------------------------

# 5 resolves inside the CTE scope; 16/30/60 exhaust _MAX_SCOPE_DEPTH and must
# reach the conservative floor rather than the same-named catalog table.
_SHADOW_DEPTHS = [5, 16, 30, 60]

_ROUTING_NUMBER = "021000021"


def _shadowing_query(depth: int, *, alias_form: bool) -> str:
    """A ``routing_number`` chain hidden behind a CTE named like a catalog table."""
    ctes = ["c0 AS (SELECT routing_number AS account_type FROM core.dim_accounts)"]
    ctes += [
        f"c{i} AS (SELECT account_type FROM c{i - 1})"  # noqa: S608  # test input string, not executing SQL
        for i in range(1, depth + 1)
    ]
    if alias_form:
        tail = f"SELECT dim_accounts.account_type FROM c{depth} AS dim_accounts"  # noqa: S608  # test input string, not executing SQL
    else:
        ctes.append(f"dim_accounts AS (SELECT account_type FROM c{depth})")  # noqa: S608  # test input string, not executing SQL
        tail = "SELECT dim_accounts.account_type FROM dim_accounts"
    return "WITH " + ", ".join(ctes) + " " + tail


@pytest.mark.parametrize("alias_form", [False, True], ids=["with-name", "from-alias"])
@pytest.mark.parametrize("depth", _SHADOW_DEPTHS)
def test_shadowing_cte_does_not_return_routing_number_in_the_clear(
    depth: int, alias_form: bool, populated_db: Database
) -> None:
    """The end-to-end proof that the shadowing leak is closed.

    A CTE named ``dim_accounts`` (or a FROM-alias of that name) used to resolve
    against ``core.dim_accounts`` once the chain exhausted ``_MAX_SCOPE_DEPTH``,
    yielding TXN_TYPE/LOW and returning the real routing number unmasked. The
    lineage-level regressions live in ``test_sql_lineage.py``; this pins the
    user-visible outcome, which is the thing that actually leaked.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, _shadowing_query(depth, alias_form=alias_form), max_rows=100
    )
    assert result.tier is Tier.CRITICAL
    assert result.records[0]["account_type"] == "*****"
    assert _ROUTING_NUMBER not in str(result.records)


def test_except_query_is_classified_and_masked(populated_db: Database) -> None:
    """``EXCEPT`` returns rows, so it must be masked like any other data query.

    ``is_data_query`` tested ``exp.Union``, which ``exp.Except`` does not
    subclass, so a top-level EXCEPT was routed down the DESCRIBE/SHOW path:
    ``is_metadata=True``, tier LOW, no masking. The routing number came back in
    the clear.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT routing_number FROM core.dim_accounts "
        "EXCEPT SELECT account_type FROM core.dim_accounts",
        max_rows=100,
    )
    assert result.is_metadata is False
    assert result.tier is Tier.CRITICAL
    assert result.records[0]["routing_number"] == "*****"
    assert _ROUTING_NUMBER not in str(result.records)


@pytest.mark.parametrize("op", ["EXCEPT", "INTERSECT"])
def test_set_operation_cannot_bypass_the_schema_allowlist(
    op: str, populated_db: Database
) -> None:
    """The metadata path skipped the schema gate too — fenced schemas became readable.

    The masking bypass was only half the damage: metadata queries never reach
    ``tables_outside_schemas``, so ``SELECT ... FROM meta.x EXCEPT ...`` returned
    unclassified rows from a schema a plain SELECT refuses outright.

    Re-aimed from ``raw`` to ``meta`` when M2O.2 admitted ``raw``/``prep``: what
    this pins is that a set operation cannot route *around* the allowlist, which
    only means something for a schema the allowlist actually refuses.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            f"SELECT account_id FROM meta.internal_only {op} SELECT 'x'",  # noqa: S608  # test input string, not executing SQL
            max_rows=100,
        )
    assert ei.value.code == error_codes.SQL_SCHEMA_NOT_ALLOWED


def test_unaliased_aggregate_critical_masked(populated_db: Database) -> None:
    """Unaliased MIN(routing_number) is masked despite the sqlglot/DuckDB name split.

    sqlglot names the projection ``''`` while DuckDB calls the column
    ``min(routing_number)``; position-aligned redaction masks it by the real name.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT MIN(routing_number) FROM core.dim_accounts", max_rows=100
    )
    assert result.tier is Tier.CRITICAL
    (value,) = result.records[0].values()
    assert str(value).startswith("****")


def test_classes_returned_includes_routing_number(populated_db: Database) -> None:
    """classes_returned surfaces the resolved data classes for observability."""
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT routing_number FROM core.dim_accounts", max_rows=100
    )
    assert "routing_number" in result.classes_returned


def test_gate_admits_internal_schemas_and_still_fences_meta_and_seeds() -> None:
    """Set equality, never a subset: the complement is the security property.

    A ``<=`` assertion passes just as happily when a later edit adds ``meta`` or
    ``sqlmesh__core``, and what this constant governs is precisely which schemas
    a caller may reach. The two named negatives are redundant against the
    equality and kept anyway — they name the schemas whose exclusion is a
    decision rather than an accident.
    """
    assert ALLOWED_QUERY_SCHEMAS == {"core", "app", "reports", "raw", "prep"}
    assert "meta" not in ALLOWED_QUERY_SCHEMAS
    assert "seeds" not in ALLOWED_QUERY_SCHEMAS


# The digit run inside the seeded `description` below: the shape
# `_mask_floored`'s content net exists to catch, in the kind of column no
# declaration can enumerate. Invented digits, interpolated into the view body so
# the constant and the seeded value cannot drift apart — spelling them out twice
# made every `_MEMO_DIGIT_RUN not in ...` assertion vacuous on a one-sided edit.
_MEMO_DIGIT_RUN = "5555000011112222"


def _seed_internal_schemas(db: Database) -> None:
    """Seed the real ``raw.ofx_accounts`` and add a prep view over it.

    ``raw.ofx_accounts`` is created by ``init_schemas`` from the OFX extractor's
    own DDL, so this seeds the real table rather than redefining it: the table
    and column names are what make the ``INTERNAL_CRITICAL`` declarations apply,
    and a stand-in name would leave every column undeclared and quietly turn the
    CRITICAL assertion below into a FLOORED one. Every VALUE is invented.

    ``prep`` models are SQLMesh-built, so nothing creates one in a unit-test
    database; this one is a VIEW because that is how SQLMesh deploys them, and
    the snapshot, the gate, and lineage must treat it exactly as a table.
    """
    db.execute(
        "INSERT INTO raw.ofx_accounts "
        "(account_id, routing_number, source_file, extracted_at) "
        "VALUES (?, ?, ?, TIMESTAMP '2026-01-01 00:00:00')",
        ["55550000111", _ROUTING_NUMBER, "statement.ofx"],
    )
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE VIEW prep.int_transactions__merged AS SELECT "  # noqa: S608  # test fixture DDL over an invented literal, not user input
        "'txn_0001' AS transaction_id, "
        f"'MEMO {_MEMO_DIGIT_RUN}' AS description, "
        "account_id FROM raw.ofx_accounts"
    )


def test_prep_select_returns_readable_rows(populated_db: Database) -> None:
    """A prep model is readable — the whole point of opening the schema.

    ``SELECT *`` needs BOTH halves of the widening, which is the useful thing it
    pins. With the gate open and the snapshot narrow, ``qualify`` cannot expand
    the star against a view it has never seen, so one projection faces three
    runtime columns, ``classes_by_result_column`` fails the cardinality check,
    and every column comes back ``'*****'``. Opening a schema without teaching
    the snapshot about it yields a surface that is either whole-masked or
    unclassified, never useful.
    """
    _seed_internal_schemas(populated_db)

    result = execute_sql_query(
        populated_db, "SELECT * FROM prep.int_transactions__merged LIMIT 5", max_rows=5
    )

    assert result.records
    assert result.records[0]["transaction_id"] == "txn_0001"


def test_raw_declared_critical_column_masks_through_the_widened_snapshot(
    populated_db: Database,
) -> None:
    """``INTERNAL_CRITICAL`` only reaches a value the SNAPSHOT can resolve.

    The dangerous half of this task, and the reason the snapshot query is widened
    in the same commit as the gate. With the gate open and the snapshot still
    scoped to core/app/reports, ``_column_key`` returns None for every raw
    column, the projection declines, ``_table_scope_max`` finds no classified
    table in scope, and ``_conservative_floor`` answers AGGREGATE — LOW,
    passthrough. The declared ``ROUTING_NUMBER`` never runs and the value is
    returned verbatim.
    """
    _seed_internal_schemas(populated_db)

    result = execute_sql_query(
        populated_db, "SELECT routing_number FROM raw.ofx_accounts", max_rows=5
    )

    assert result.records[0]["routing_number"] == "*****"
    assert result.tier is Tier.CRITICAL
    assert _ROUTING_NUMBER not in str(result.records)


def test_undeclared_prep_column_rides_the_content_net(
    populated_db: Database,
) -> None:
    """An undeclared raw/prep column resolves FLOORED, not AGGREGATE.

    The other half of the snapshot's load: FLOORED is what a column with no
    declaration gets, and it is reachable only once ``_class_of_key`` is handed a
    key the snapshot resolved. Without the widened snapshot this column answers
    AGGREGATE and the digit run is published.
    """
    _seed_internal_schemas(populated_db)

    result = execute_sql_query(
        populated_db,
        "SELECT description FROM prep.int_transactions__merged",
        max_rows=5,
    )

    assert result.output_classes["description"] is DataClass.FLOORED
    assert _MEMO_DIGIT_RUN not in str(result.records)


# Four SQL shapes that put one output position on BOTH a floored prep column and
# a declared `core` column. Each is a distinct route into `_combined_class` — a
# set operation merges positionally across branches, a COALESCE/CASE merges the
# classes of one expression's arms — so each is its own parametrized case rather
# than a loop, and each fails on its own.
_MIXED_SCHEMA_PROJECTIONS = [
    (
        "union-all",
        "SELECT description FROM prep.int_transactions__merged "
        "UNION ALL SELECT description FROM core.fct_transactions",
    ),
    (
        "union-distinct",
        "SELECT description FROM prep.int_transactions__merged "
        "UNION SELECT description FROM core.fct_transactions",
    ),
    (
        "coalesce",
        "SELECT COALESCE(p.description, t.description) AS description "
        "FROM prep.int_transactions__merged p "
        "LEFT JOIN core.fct_transactions t ON p.transaction_id = t.transaction_id",
    ),
    (
        "case",
        "SELECT CASE WHEN t.transaction_id IS NULL THEN p.description "
        "ELSE t.description END AS description "
        "FROM prep.int_transactions__merged p "
        "LEFT JOIN core.fct_transactions t ON p.transaction_id = t.transaction_id",
    ),
]


@pytest.mark.parametrize(
    "sql",
    [sql for _id, sql in _MIXED_SCHEMA_PROJECTIONS],
    ids=[case_id for case_id, _sql in _MIXED_SCHEMA_PROJECTIONS],
)
def test_a_mixed_schema_position_keeps_the_content_net(
    populated_db: Database, sql: str
) -> None:
    """Mixing a floored column with a `core` one must not retire the net.

    ``core.fct_transactions.description`` is ``DESCRIPTION`` — MEDIUM tier, and
    ``_passthrough`` until PR 3 wires its transform. Merging by tier alone
    therefore answered ``DESCRIPTION`` for a position that also draws from
    ``prep``, and the prep row's digit run came back verbatim under a class that
    masks nothing. Seven of the classes above ``Tier.LOW`` pass through today,
    so this was reachable from any of them.

    The class assertion is the load-bearing one; the digit-run assertion is the
    disclosure it caused, kept because a class name is not evidence a value was
    masked.
    """
    _seed_internal_schemas(populated_db)

    result = execute_sql_query(populated_db, sql, max_rows=5)

    assert result.output_classes["description"] is DataClass.FLOORED
    assert _MEMO_DIGIT_RUN not in str(result.records)


def test_reports_net_worth_balance_columns_classify_high(
    populated_db: Database,
) -> None:
    """reports.net_worth's BALANCE columns classify HIGH end-to-end (#330).

    Retargeted #330 regression test — this used to be
    `test_reports_uncategorized_queue_masks_account_id`, which asserted that
    `uncategorized_queue.account_id` masks. That assertion was wrong:
    `account_id` is a deliberately opaque minted surrogate classified
    `RECORD_ID` (LOW), same as every other `account_id` column in
    `CLASSIFICATION` (spec D6, commit c465f181) — see
    `test_account_id_passes_through_unmasked`. Masking it was an artifact of
    the now-deleted hand-written bridge's mistaken premise, not a real
    privacy requirement.

    What #330 actually broke was never caught at this (fast, unit-level)
    layer: the retired `test_reports_class_map_bridges_uncategorized_queue_and_net_worth`
    only asserted `("reports", "net_worth") in reports_class_map()` —
    membership, not the TIER of its declared columns — the identical
    "coverage guard checks presence, not depth" shape that let
    `uncategorized_queue.account_id` slip through unmasked in the first
    place. This test closes that gap for `net_worth`'s BALANCE columns (now
    declared via the generated `_derived_classes.py` module, Task 4's
    replacement for the bridge) at unit speed, rather than relying solely on
    `test_declared_classes_match_derivation`
    (`tests/privacy/test_report_class_derivation.py`).
    """
    populated_db.execute("""
        CREATE OR REPLACE VIEW reports.net_worth AS
        SELECT
            DATE '2026-06-15' AS balance_date,
            CAST(125000.00 AS DECIMAL(18,2)) AS net_worth,
            2 AS account_count,
            CAST(150000.00 AS DECIMAL(18,2)) AS total_assets,
            CAST(-25000.00 AS DECIMAL(18,2)) AS total_liabilities
    """)

    result = execute_sql_query(
        populated_db,
        "SELECT net_worth, total_assets, total_liabilities FROM reports.net_worth",
        max_rows=5,
    )

    assert result.tier is Tier.HIGH
    assert result.output_classes["net_worth"] is DataClass.BALANCE
    assert result.output_classes["total_assets"] is DataClass.BALANCE
    assert result.output_classes["total_liabilities"] is DataClass.BALANCE
    # HIGH-tier BALANCE passes through unmasked here (redaction.py:
    # _passthrough) — HIGH gates on MCP consent, not value redaction in this
    # primitive. Confirms the columns aren't ALSO wrongly over-masked.
    assert result.records[0]["net_worth"] == 125000.00


def test_generated_classes_are_current() -> None:
    """The checked-in generated module matches what derivation produces now.

    Regenerate with: make generate-report-classes
    """
    from moneybin.privacy.report_class_derivation import derive_report_classes
    from moneybin.reports._framework.registry import spec_of
    from moneybin.reports.definitions import ALL_REPORTS
    from moneybin.reports.definitions._derived_classes import (
        DERIVED_REPORT_CLASSES,
    )

    derived = derive_report_classes()
    runner_views = [spec_of(r).view for r in ALL_REPORTS]
    runner_keys = {
        (view.schema, view.name) for view in runner_views if view is not None
    }
    expected = {key: cols for key, cols in derived.items() if key not in runner_keys}
    assert DERIVED_REPORT_CLASSES == expected, (
        "Regenerate with: make generate-report-classes"
    )


def test_undeclared_deployed_column_fails_closed(populated_db: Database) -> None:
    """A deployed column with no declaration masks (coverage gap, not a query bug).

    ``undeclared_view`` is a real deployed ``reports.*`` view — its columns are
    in the schema snapshot — but it has no ``@report(classes=...)`` declaration
    and no bridge entry, so it is a genuine coverage gap. This is the shape of
    #330: the `reports` schema was widened to be queryable, but an undeclared
    column fell through to the permissive AGGREGATE fallback and returned
    unmasked. It must now fail closed instead.
    """
    _seed_account(populated_db)
    populated_db.execute(
        "CREATE OR REPLACE VIEW reports.undeclared_view AS "
        "SELECT account_id, 1 AS n FROM core.dim_accounts"
    )
    result = execute_sql_query(
        populated_db,
        "SELECT account_id FROM reports.undeclared_view",
        max_rows=100,
    )
    assert result.output_classes["account_id"].tier is Tier.CRITICAL
    assert str(result.records[0]["account_id"]).startswith("****")


def test_fail_closed_warning_fires_only_for_genuine_misses(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The fail-closed WARNING logs once per genuine lineage miss, never more.

    ``classes_by_result_column`` used to build its map with
    ``output_classes.get(col, _fail_closed(col, query))`` — Python evaluates a
    call's arguments before the call, so ``_fail_closed`` ran on every column
    of every query regardless of whether ``col`` was actually missing from
    ``output_classes``. That defeated the log's purpose (distinguishing a
    genuine fail-closed event from noise) without changing which class a
    column resolved to, since ``.get`` still returned the correct value either
    way. A normal, fully-resolved query must emit zero warnings; a query with
    one genuinely-unresolvable projection must emit exactly one.
    """
    _seed_txn(populated_db)
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        result = execute_sql_query(
            populated_db,
            "SELECT amount FROM core.fct_transactions",
            max_rows=100,
        )
    assert result.output_classes["amount"] is DataClass.TXN_AMOUNT
    assert caplog.text.count("failing closed") == 0

    caplog.clear()
    _seed_account(populated_db)
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        # A genuine miss is MISSING LINEAGE, not a naming divergence: one
        # COLUMNS() projection fans out into two runtime columns lineage never
        # saw, so the counts disagree and both fail closed. An unaliased
        # MIN(routing_number) is NOT an instance of this — lineage resolved it
        # and only the label differs, so it reconciles positionally and warns
        # zero times (see classes_by_result_column).
        execute_sql_query(
            populated_db,
            "SELECT COLUMNS('routing_number|last_four') FROM core.dim_accounts",
            max_rows=100,
        )
    assert caplog.text.count("failing closed") == 2


@pytest.mark.parametrize("depth", [17, 30, 60])
def test_deep_cte_chain_masks_routing_number(
    depth: int, populated_db: Database
) -> None:
    """The depth-exhaustion leak, asserted where it actually mattered: the value.

    A ~17-line generated query hid ``routing_number`` behind enough CTE
    aliases to exhaust ``_MAX_SCOPE_DEPTH``; the column then floored against the
    innermost CTE body (no catalog columns → AGGREGATE/LOW) and
    ``execute_sql_query`` returned the real routing number in the clear. The
    classification-level regression lives in ``test_sql_lineage.py``; this one
    pins the end-to-end consequence, so a future refactor that keeps the tier
    right but breaks position-aligned redaction cannot pass silently.
    """
    _seed_account(populated_db)
    ctes = ["c0 AS (SELECT routing_number AS v FROM core.dim_accounts)"]
    ctes += [f"c{i} AS (SELECT v FROM c{i - 1})" for i in range(1, depth + 1)]  # noqa: S608  # test input string, not executing SQL
    sql = "WITH " + ", ".join(ctes) + f" SELECT c{depth}.v FROM c{depth}"  # noqa: S608  # test input string, not executing SQL

    result = execute_sql_query(populated_db, sql, max_rows=100)

    assert result.tier is Tier.CRITICAL
    assert result.records[0]["v"] != "021000021"
    assert str(result.records[0]["v"]).startswith("*")


# ---------------------------------------------------------------------------
# Masking-bypass leaks (round 7). Two families, one shape: lineage produced a
# CONFIDENT LOW answer for a projection it had not actually decomposed, and the
# name-mismatch fallback in ``sql_query`` then spread that LOW over runtime
# columns lineage never saw.
#
#   * ``COLUMNS(...)`` / ``PIVOT`` / ``UNPIVOT`` / ``SUMMARIZE`` — the projection
#     is an opaque ``exp.Columns`` node or a ``Star`` ``qualify()`` cannot expand,
#     so ``_resolve_projection`` saw "no exp.Column" and returned AGGREGATE.
#   * The row-struct pseudo-column (``SELECT dim_accounts FROM core.dim_accounts``)
#     — lineage declined correctly, but ``_conservative_floor`` only looked at
#     resolvable input COLUMNS, found none, and floored at AGGREGATE.
#
# Every one of these returned the real routing number in the clear at Tier.LOW.
# Asserted end-to-end on the RETURNED VALUE, because that is what leaked.
# ---------------------------------------------------------------------------

_MASKING_BYPASS_QUERIES = {
    "columns-regex": "SELECT COLUMNS('routing.*') FROM core.dim_accounts",
    "columns-all": "SELECT COLUMNS('.*') FROM core.dim_accounts",
    "columns-lambda": "SELECT COLUMNS(c -> c LIKE 'routing%') FROM core.dim_accounts",
    "columns-in-cte": (
        "WITH w AS (SELECT COLUMNS('.*') FROM core.dim_accounts) SELECT * FROM w"
    ),
    "columns-co-projected-with-low": (
        "SELECT account_type, COLUMNS('routing.*') FROM core.dim_accounts"
    ),
    "unpivot-star": (
        "SELECT * FROM "
        "(UNPIVOT core.dim_accounts ON routing_number INTO NAME k VALUE v)"
    ),
    "unpivot-named": (
        "SELECT v FROM "
        "(UNPIVOT core.dim_accounts ON routing_number INTO NAME k VALUE v)"
    ),
    "pivot-star": (
        "SELECT * FROM "
        "(PIVOT core.dim_accounts ON account_type USING MAX(routing_number))"
    ),
    "pivot-named": (
        "SELECT checking FROM "
        "(PIVOT core.dim_accounts ON account_type USING MAX(routing_number))"
    ),
    "summarize": "SELECT * FROM (SUMMARIZE core.dim_accounts)",
    "row-struct": "SELECT dim_accounts FROM core.dim_accounts",
    "row-struct-via-alias": "SELECT a FROM core.dim_accounts a",
    "row-struct-field": "SELECT (dim_accounts).routing_number FROM core.dim_accounts",
    "row-struct-in-subquery": (
        "SELECT * FROM (SELECT dim_accounts FROM core.dim_accounts) z"
    ),
    "unnest-row-struct": "SELECT UNNEST(dim_accounts) FROM core.dim_accounts",
    "unnest-row-struct-via-alias": "SELECT UNNEST(a) FROM core.dim_accounts a",
}


@pytest.mark.parametrize(
    "sql",
    list(_MASKING_BYPASS_QUERIES.values()),
    ids=list(_MASKING_BYPASS_QUERIES),
)
def test_masking_bypass_never_returns_routing_number_in_the_clear(
    sql: str, populated_db: Database
) -> None:
    """No DuckDB projection form returns a CRITICAL value unmasked.

    The assertion is deliberately on the returned records rather than on
    ``output_classes``: several of these shapes emit runtime column names
    lineage never produced, so a class-level assertion would pass while the
    user still received ``021000021``.
    """
    _seed_account(populated_db)
    result = execute_sql_query(populated_db, sql, max_rows=100)
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert _ROUTING_NUMBER not in str(result.records)
    assert result.tier is Tier.CRITICAL


# ---------------------------------------------------------------------------
# The counting-aggregate collapse bypassed the opaque-node protection
#
# Same family as the block above, reached by a different door. The collapse's
# guard ("every exp.Column is inside a count") passes VACUOUSLY on an opaque
# projection — one carries no exp.Column child at all — so an opaque node
# combined with a counting aggregate escaped to AGGREGATE (LOW) while the SAME
# opaque node alone correctly declined to UNRESOLVED. Each query below returned
# the real routing number in the clear, several of them concatenated to a count
# (``'1021000021'``), which is why the assertion is on the returned VALUE.
#
# The classification-level cases, including the shapes DuckDB's binder rejects
# before execution, live in
# ``test_sql_lineage.py::test_counting_aggregate_never_collapses_an_opaque_projection``.
# ---------------------------------------------------------------------------

_COUNTING_AGG_BYPASS_QUERIES = {
    "count-concat-columns": (
        "SELECT COUNT(*) || first(COLUMNS('routing.*')) AS x FROM core.dim_accounts"
    ),
    "columns-concat-count": (
        "SELECT MIN(COLUMNS('routing.*')) || COUNT(*) AS x FROM core.dim_accounts"
    ),
    "count-concat-columns-grouped": (
        "SELECT COUNT(*) || COLUMNS('routing.*') AS x FROM core.dim_accounts "
        "GROUP BY routing_number"
    ),
    "count-concat-columns-in-cte": (
        "WITH w AS (SELECT COUNT(*) || first(COLUMNS('routing.*')) AS x "
        "FROM core.dim_accounts) SELECT x FROM w"
    ),
    "count-concat-columns-scalar-subquery": (
        "SELECT COUNT(*) || "
        "(SELECT first(COLUMNS('routing.*')) FROM core.dim_accounts) AS x "
        "FROM core.dim_accounts"
    ),
}


@pytest.mark.parametrize(
    "sql",
    list(_COUNTING_AGG_BYPASS_QUERIES.values()),
    ids=list(_COUNTING_AGG_BYPASS_QUERIES),
)
def test_counting_aggregate_does_not_unmask_an_opaque_projection(
    sql: str, populated_db: Database
) -> None:
    """A count beside an opaque node must not publish the value the node covers.

    The alias (``AS x``) matters: without it DuckDB names the output column
    after the expanded source column, the name-mismatch fallback in
    ``sql_query`` notices lineage never produced that name, and it fails closed
    anyway. Aliasing makes the names line up, so nothing downstream catches a
    wrong LOW — the projection's own class is the only thing standing between
    the user and ``021000021``.
    """
    _seed_account(populated_db)
    result = execute_sql_query(populated_db, sql, max_rows=100)
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert _ROUTING_NUMBER not in str(result.records)
    assert result.tier is Tier.CRITICAL


def test_count_of_opaque_projection_is_not_a_confident_aggregate(
    populated_db: Database,
) -> None:
    """``COUNT(COLUMNS(...))`` returns a count, but must not be certified LOW.

    No value leaks in this instance — the sibling projections DuckDB expands
    this into happen to all be counts. It is still not something lineage may
    answer AGGREGATE with confidence: ``COLUMNS(...)`` distributes into N output
    columns whose names lineage never produced, so a confident LOW here is a
    class asserted over columns we never saw. Pinned separately from the
    value-leak cases so a future narrowing of the veto to "only when a value
    provably escapes" fails here rather than passing quietly.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT COUNT(COLUMNS('routing.*')) AS x FROM core.dim_accounts",
        max_rows=100,
    )
    assert result.output_classes["x"] is not DataClass.AGGREGATE
    assert result.tier is not Tier.LOW


def test_count_star_over_unexpandable_source_stays_aggregate(
    populated_db: Database,
) -> None:
    """``COUNT(*)`` over a ``SUMMARIZE`` source is still LOW — the veto's boundary.

    ``COUNT(*)``'s Star is not a failed ``qualify()`` expansion; it names no
    columns and the count genuinely bounds it. If the opaque veto widens to
    "any Star anywhere", this ordinary row count fails closed and the
    ``net_worth.account_count`` derivation goes with it.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT COUNT(*) AS n FROM (SUMMARIZE core.dim_accounts)",
        max_rows=100,
    )
    assert result.output_classes["n"] is DataClass.AGGREGATE
    assert result.tier is Tier.LOW


def test_wrapped_scalar_count_stays_aggregate(populated_db: Database) -> None:
    """A scalar ``COUNT(*)`` subquery inside a larger expression is still LOW.

    ``(SELECT COUNT(*) FROM t) + 1`` reaches neither collapse: the
    counting-aggregate branch declines because the count sits in a subquery and
    so does not govern the projection, leaving a projection with no
    ``exp.Column`` at all. The count's Star is nonetheless genuinely bounded —
    identical to the bare ``COUNT(*)`` this suite already pins — so the
    arithmetic over it is an aggregate, not a CRITICAL unknown. Declining here
    returned ``'*****'`` for a plain number, because the conservative floor
    then saw ``dim_accounts``' CRITICAL columns.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT (SELECT COUNT(*) FROM core.dim_accounts) + 1 AS n",
        max_rows=100,
    )
    assert result.records[0]["n"] == 2
    assert result.tier is Tier.LOW


def test_unresolvable_expression_does_not_over_mask(populated_db: Database) -> None:
    """``COUNT(*)`` still classifies as AGGREGATE (LOW), not CRITICAL.

    Not a guard against blanket fail-closed: ``COUNT(*)`` has no column
    reference at all, so ``_resolve_projection``'s counting-aggregate branch
    returns AGGREGATE before ``_column_key``, ``_class_of_key``,
    ``_conservative_floor``, or ``_coverage_gap_class`` are ever reached — a
    maximal fail-closed patch there would leave this test passing unchanged.
    See ``test_unresolvable_column_reference_classifies_by_scope_inputs``
    below for the test that actually exercises (and discriminates) that path.
    """
    _seed_txn(populated_db)
    result = execute_sql_query(
        populated_db,
        "SELECT COUNT(*) AS n FROM core.fct_transactions",
        max_rows=100,
    )
    assert result.output_classes["n"] is DataClass.AGGREGATE
    assert result.tier is Tier.LOW


# --------------------------------------------------------------------------
# CRITICAL transforms are not interchangeable
#
# The CRITICAL classes do NOT share a mask: ROUTING_NUMBER masks WHOLE
# (``*****``) while INSTITUTION_ACCOUNT_NUMBER masks PARTIALLY (``"****" +
# value[-4:]``). So wherever lineage merges several classes into ONE answer, a
# plain ``max``-by-tier picks an arbitrary winner among equal-CRITICAL classes
# — and if the winner is the partial-masking one, four characters of a
# whole-mask value reach the user in the clear.
#
# Four merge points had this shape, all confirmed leaking the real routing
# number's last four digits (``****0021``) before the fix:
#   1. ``_conservative_floor``'s column-floor / table-floor tie-break
#   2. ``_resolve_projection``'s max over a projection's referenced columns
#   3. ``_class_at_index``'s merge across a nested set operation's branches
#   4. ``resolve_output_classes``'s merge across top-level UNION branches
#
# The tests below pin each. They assert on the returned VALUE, not just the
# class, because the value is what leaks.
# --------------------------------------------------------------------------


def test_co_referenced_critical_column_does_not_weaken_unresolved_mask(
    populated_db: Database,
) -> None:
    """An unrelated WHERE reference must not downgrade a whole mask to a partial one.

    THE REGRESSION: ``_conservative_floor`` combined a column floor
    (``_scope_input_max``, which scans the WHOLE tree including WHERE/JOIN
    predicates) with a table floor, and on an equal-CRITICAL tie returned the
    column floor. The whole-row projection here is unresolvable, so the table
    floor correctly collapsed to UNRESOLVED — but ``last_four``, named only in
    a WHERE clause and describing a completely different value, tied at CRITICAL
    and won, applying ITS partial mask to the routing number: ``****0021``.
    """
    _seed_account(populated_db, last_four="6789")
    result = execute_sql_query(
        populated_db,
        "SELECT (dim_accounts).routing_number AS x FROM core.dim_accounts "
        "WHERE dim_accounts.last_four IS NOT NULL",
        max_rows=100,
    )
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert result.output_classes["x"] is DataClass.UNRESOLVED
    assert result.tier is Tier.CRITICAL
    assert result.records[0]["x"] == "*****"
    assert _ROUTING_NUMBER[-4:] not in str(result.records)


def test_unresolved_mask_is_whole_without_a_co_referenced_critical_column(
    populated_db: Database,
) -> None:
    """The control for the test above: same projection, no co-referenced column.

    Pins that the WHERE clause is the only difference between the two, so the
    regression test above isolates the tie-break rather than the whole-row
    projection handling it shares with the masking-bypass suite.
    """
    _seed_account(populated_db, last_four="6789")
    result = execute_sql_query(
        populated_db,
        "SELECT (dim_accounts).routing_number AS x FROM core.dim_accounts",
        max_rows=100,
    )
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert result.output_classes["x"] is DataClass.UNRESOLVED
    assert result.tier is Tier.CRITICAL
    assert result.records[0]["x"] == "*****"


def test_critical_column_in_join_predicate_does_not_weaken_unresolved_mask(
    populated_db: Database,
) -> None:
    """Same substitution via a JOIN predicate rather than a WHERE clause.

    ``_scope_input_max`` collects columns from JOIN conditions too, so the
    tie-break was reachable through this door as well. Separate test because a
    fix scoped to ``exp.Where`` would close the WHERE case only.
    """
    _seed_account(populated_db, last_four="6789")
    result = execute_sql_query(
        populated_db,
        "SELECT (a).routing_number AS x FROM core.dim_accounts a "
        "JOIN core.dim_accounts b ON a.last_four = b.last_four",
        max_rows=100,
    )
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert result.output_classes["x"] is DataClass.UNRESOLVED
    assert result.tier is Tier.CRITICAL
    assert result.records[0]["x"] == "*****"
    assert _ROUTING_NUMBER[-4:] not in str(result.records)


@pytest.mark.parametrize(
    ("case", "sql"),
    [
        # Both orders: pre-fix, `max` returned the FIRST maximal element, so the
        # mask strength depended on which column was written first. Only the
        # `last_four`-first form leaked — which is exactly why both are pinned.
        (
            "concat-partial-class-first",
            "SELECT last_four || routing_number AS x FROM core.dim_accounts",
        ),
        (
            "concat-whole-class-first",
            "SELECT routing_number || last_four AS x FROM core.dim_accounts",
        ),
        (
            "coalesce",
            "SELECT COALESCE(last_four, routing_number) AS x FROM core.dim_accounts",
        ),
        (
            "top-level-union",
            "SELECT last_four AS x FROM core.dim_accounts "
            "UNION ALL SELECT routing_number AS x FROM core.dim_accounts",
        ),
        (
            "nested-union-in-derived-table",
            "SELECT x FROM ("
            "SELECT last_four AS x FROM core.dim_accounts "
            "UNION ALL SELECT routing_number AS x FROM core.dim_accounts) z",
        ),
    ],
)
def test_disagreeing_critical_classes_collapse_to_a_whole_mask(
    case: str, sql: str, populated_db: Database
) -> None:
    """A value fed by two DIFFERENT CRITICAL classes takes neither one's transform.

    Each case merges INSTITUTION_ACCOUNT_NUMBER (partial mask) with
    ROUTING_NUMBER (whole mask) into a single output position. No single class
    describes the result, so it must collapse to UNRESOLVED and mask whole.
    Pre-fix, the concat/coalesce/UNION forms that happened to list the
    partial-masking class first returned ``****0021``.
    """
    _seed_account(populated_db, last_four="6789")
    result = execute_sql_query(populated_db, sql, max_rows=100)
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert result.output_classes["x"] is DataClass.UNRESOLVED
    assert result.tier is Tier.CRITICAL
    assert all(r["x"] == "*****" for r in result.records)
    assert _ROUTING_NUMBER[-4:] not in str(result.records)


@pytest.mark.parametrize(
    ("case", "sql", "expected"),
    [
        # A SINGLE CRITICAL class still describes its value exactly, so it keeps
        # its own transform. Without these, collapsing "any CRITICAL" would pass
        # the tests above while silently whole-masking every last_four in the
        # product — the over-classification this module must not introduce.
        (
            "partial-masking-class-alone",
            "SELECT last_four AS x FROM core.dim_accounts",
            "****6789",
        ),
        (
            "whole-masking-class-alone",
            "SELECT routing_number AS x FROM core.dim_accounts",
            "*****",
        ),
        (
            "unanimous-critical-across-union",
            "SELECT last_four AS x FROM core.dim_accounts "
            "UNION ALL SELECT last_four AS x FROM core.dim_accounts",
            "****6789",
        ),
    ],
)
def test_unanimous_critical_class_keeps_its_own_transform(
    case: str, sql: str, expected: str, populated_db: Database
) -> None:
    """Agreement at CRITICAL is preserved — only DISAGREEMENT collapses."""
    _seed_account(populated_db, last_four="6789")
    result = execute_sql_query(populated_db, sql, max_rows=100)
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert result.tier is Tier.CRITICAL
    assert all(r["x"] == expected for r in result.records)


@pytest.mark.parametrize(
    ("case", "sql", "expected_class", "expected_tier", "expected_value"),
    [
        # Below CRITICAL every transform is passthrough except FLOORED, which
        # none of these ties involve, so the merge still reports the plain max
        # — unchanged by this fix. A collapse that reached below CRITICAL would
        # both mask these values and inflate their tier.
        (
            "low-low-tie-in-one-projection",
            "SELECT institution_name || account_type AS x FROM core.dim_accounts",
            DataClass.INSTITUTION,
            Tier.LOW,
            "Chasechecking",
        ),
        (
            "low-low-tie-across-union",
            "SELECT institution_name AS x FROM core.dim_accounts "
            "UNION ALL SELECT institution_name AS x FROM core.dim_accounts",
            DataClass.INSTITUTION,
            Tier.LOW,
            "Chase",
        ),
        (
            "medium-over-low-is-still-max",
            "SELECT display_name || institution_name AS x FROM core.dim_accounts",
            DataClass.USER_NOTE,
            Tier.MEDIUM,
            "My CheckingChase",
        ),
    ],
)
def test_below_critical_merge_behaviour_is_unchanged(
    case: str,
    sql: str,
    expected_class: DataClass,
    expected_tier: Tier,
    expected_value: str,
    populated_db: Database,
) -> None:
    """Ties below CRITICAL that skip FLOORED keep the max-by-tier answer, unmasked."""
    populated_db.execute(
        "INSERT INTO core.dim_accounts (account_id, routing_number, last_four, "
        "account_type, institution_name, display_name) "
        "VALUES ('ACC000123456789', '021000021', '6789', 'checking', "
        "'Chase', 'My Checking')"
    )
    result = execute_sql_query(populated_db, sql, max_rows=100)
    assert result.records, "query returned no rows — the assertion would be vacuous"
    assert result.output_classes["x"] is expected_class
    assert result.tier is expected_tier
    assert all(r["x"] == expected_value for r in result.records)


def test_unresolvable_column_reference_classifies_by_scope_inputs(
    populated_db: Database,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A LATERAL-derived column (`key is None`) classifies by scope, not blanket CRITICAL.

    ``x`` in the outer SELECT refers to the LATERAL derived table's alias
    ``l``, not a real catalog table or a source ``_class_via_source_scope``
    resolves — CTE and plain derived-table aliases resolve through
    ``scope.sources`` (see that function's docstring), but a LATERAL source
    does not, so ``_column_key`` returns ``None`` for ``l.x``,
    ``_resolve_projection`` declines (the ``key is None`` branch), and
    ``_classify_projection`` answers with ``_conservative_floor`` — unlike
    ``COUNT(*)`` above, which never gets there. The only real input column is
    ``t.amount``
    (TXN_AMOUNT, HIGH), so the query must classify HIGH and pass the value
    through unmasked — a blanket fail-closed (mask on any unresolved key)
    would flip this to CRITICAL and mask it, which is exactly the
    over-masking the coverage-gap fix must not introduce.
    """
    _seed_txn(populated_db)
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_lineage"):
        result = execute_sql_query(
            populated_db,
            "SELECT l.x FROM core.fct_transactions t, "
            "LATERAL (SELECT t.amount AS x) AS l",
            max_rows=100,
        )
    assert result.output_classes["x"] is DataClass.TXN_AMOUNT
    assert result.tier is Tier.HIGH
    assert not str(result.records[0]["x"]).startswith("*")
    # Pins that the `key is None` branch was actually taken. Without this, a
    # future sqlglot that resolves the LATERAL alias through scope.sources
    # would classify `x` directly, and the test would keep passing while
    # silently no longer exercising the branch it exists to guard.
    assert "unresolved projection; conservative fallback" in caplog.text


# One free-text merchant name, chosen because no `SanitizedLogFormatter` pattern
# can recognise it: the formatter masks SSNs, runs of 8+ digits, and dollar
# amounts. A leak of this string reaches the log file intact.
_MERCHANT = "ACME PLUMBING"


def _assert_log_names_the_failure_without_quoting_it(
    caplog: pytest.LogCaptureFixture, cause: BaseException, prefix: str
) -> None:
    """The log named which failure happened, by type and query digest only.

    Asserts all three properties one of these records must hold: the site fired
    (its own prefix), it says what went wrong (the exception's type name) and
    which statement (a digest), and it quotes neither the exception's message
    nor the literal that message carries.
    """
    assert prefix in caplog.text
    assert type(cause).__name__ in caplog.text
    assert "sql sha256=" in caplog.text
    assert str(cause) not in caplog.text
    assert _MERCHANT not in caplog.text


def test_unknown_table_log_names_the_error_type_not_the_query(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The unknown-table log must not carry the lineage message verbatim.

    The client envelope already withholds it (see
    ``test_unknown_table_error_omits_raw_detail``), but the log record is the
    other boundary, and it is the durable one: a ``LINE 1: SELECT ...`` echo of
    the failing statement writes any inline literal to a file
    ``.claude/rules/security.md`` forbids it in.

    An unknown *table* exercises CatalogException specifically. The unknown-
    *column* case (BinderException, same handler, same log line) has its own
    fixture in ``test_unknown_column_hint_does_not_echo_query_literals``,
    which also pins the identifier-detail masking this handler performs.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        with pytest.raises(UserError) as ei:
            execute_sql_query(
                populated_db,
                f"SELECT x FROM core.no_such_table WHERE note = '{_MERCHANT}'",  # noqa: S608  # `_MERCHANT` is a test constant; the query is meant to fail
                max_rows=10,
            )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    cause = ei.value.__cause__
    assert cause is not None
    _assert_log_names_the_failure_without_quoting_it(
        caplog, cause, "sql_query unknown table/column"
    )


def test_execution_error_log_names_the_error_type_not_the_query(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """A DuckDB failure at fetch time quotes the value it could not convert.

    Distinct site from the unknown-column one above, and reached by a distinct
    fixture: this statement resolves against the schema and fails only when
    DuckDB evaluates the cast, so the lineage handler cannot claim it.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        with pytest.raises(UserError) as ei:
            execute_sql_query(
                populated_db,
                f"SELECT CAST('{_MERCHANT}' AS INTEGER) AS n",
                max_rows=10,
            )
    assert ei.value.code == error_codes.SQL_QUERY_ERROR
    cause = ei.value.__cause__
    assert cause is not None
    _assert_log_names_the_failure_without_quoting_it(
        caplog, cause, "sql_query execution error"
    )


def test_metadata_unknown_column_log_names_the_error_type_not_the_query(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The DESCRIBE/SHOW branch has its own handler, so it needs its own guard.

    DESCRIBE only binds the inner query, never executes it, so an unknown
    column there raises DuckDB's BinderException — the same exception the data
    path routes to SQL_UNKNOWN_TABLE. This fixture used to land in the
    metadata path's generic bucket (SQL_QUERY_ERROR, no detail); it now gets
    the same named-identifier treatment as the data path.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        with pytest.raises(UserError) as ei:
            execute_sql_query(
                populated_db,
                "DESCRIBE SELECT nope FROM core.dim_accounts "  # noqa: S608  # `_MERCHANT` is a test constant; the query is meant to fail
                f"WHERE display_name = '{_MERCHANT}'",
                max_rows=10,
            )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    # The hint is the point of the clause, not a side effect: without these two
    # a regression passing hint=None from _fetch_metadata still passes.
    assert "nope" in (ei.value.hint or "")
    # _MERCHANT is neither an SSN nor an 8+ digit run, so mask_pii_shaped can't
    # catch it — only the LINE split keeps it out, which is what this pins.
    assert _MERCHANT not in (ei.value.hint or "")
    cause = ei.value.__cause__
    assert cause is not None
    _assert_log_names_the_failure_without_quoting_it(
        caplog, cause, "sql_query metadata unknown table/column"
    )


def test_metadata_unknown_table_log_names_the_error_type_not_the_query(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The metadata path's ``except`` clause also covers CatalogException.

    ``test_metadata_unknown_column_log_names_the_error_type_not_the_query``
    above exercises the BinderException half of
    ``except (duckdb.CatalogException, duckdb.BinderException)`` in
    ``_fetch_metadata`` — an unknown COLUMN inside a wrapped ``DESCRIBE
    SELECT``. This is the other half: ``DESCRIBE`` on a TABLE that doesn't
    exist raises CatalogException directly (no wrapped SELECT needed), and
    must classify, log, and mask identically.
    """
    with caplog.at_level(logging.WARNING, logger="moneybin.privacy.sql_query"):
        with pytest.raises(UserError) as ei:
            execute_sql_query(
                populated_db,
                "DESCRIBE core.nonexistent_table",
                max_rows=10,
            )
    assert ei.value.code == error_codes.SQL_UNKNOWN_TABLE
    assert "nonexistent_table" in (ei.value.hint or "")
    cause = ei.value.__cause__
    assert cause is not None
    assert isinstance(cause, duckdb.CatalogException)
    _assert_log_names_the_failure_without_quoting_it(
        caplog, cause, "sql_query metadata unknown table/column"
    )
