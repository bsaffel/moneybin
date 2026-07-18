"""Tests for the shared privacy-enforcing SQL execution primitive.

``execute_sql_query`` is the single primitive behind both the ``sql_query``
MCP tool and the ``moneybin sql query`` CLI command. These tests pin the
enforcement contract at the primitive level — redaction, schema gating,
aggregation tiers, truncation, and error classification — so both surfaces
inherit identical behavior structurally.
"""

from __future__ import annotations

import logging

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.sql_lineage import reports_class_map
from moneybin.privacy.sql_query import (
    _ALLOWED_QUERY_SCHEMAS,  # pyright: ignore[reportPrivateUsage]
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


def _seed_account(db: Database) -> None:
    """Insert one account row so masking tests have a CRITICAL value to mask."""
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, routing_number, account_type) "
        "VALUES ('ACC000123456789', '021000021', 'checking')"
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


def test_disallowed_schema_raises(populated_db: Database) -> None:
    """Querying outside core/app raises UserError with the schema-gate code.

    The gate fires on schema name before execution, so the table need not exist.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db, "SELECT account_id FROM raw.ofx_transactions", max_rows=100
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


def test_unaliased_aggregate_fails_closed_to_max_tier(populated_db: Database) -> None:
    """An unaliased expression DuckDB names differently than sqlglot fails closed.

    `MIN(routing_number)` → DuckDB column 'min(routing_number)' vs sqlglot ''. The
    name miss fails closed to the query's max tier (CRITICAL), so the value is
    masked — never returned in the clear.
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


def test_reports_schema_is_queryable() -> None:
    assert "reports" in _ALLOWED_QUERY_SCHEMAS
    # core/app still allowed; internal schemas still fenced in Phase 1.
    assert {"core", "app"} <= _ALLOWED_QUERY_SCHEMAS
    assert "meta" not in _ALLOWED_QUERY_SCHEMAS


def test_reports_class_map_bridges_uncategorized_queue_and_net_worth() -> None:
    """No-DB sanity: the transitional bridge covers both runner-less views.

    `reports_class_map()` merges `@report(classes=...)` declarations with
    `BRIDGED_REPORT_CLASSES` (`reports/definitions/_bridged_classes.py`) for
    views deployed without a runner yet. Pins that `uncategorized_queue`'s
    `account_id` resolves CRITICAL (`ACCOUNT_IDENTIFIER`) and that
    `net_worth` is registered, so a bridge-entry regression fails here
    before it fails as a masking hole in `execute_sql_query`.
    """
    mapping = reports_class_map()
    assert (
        mapping[("reports", "uncategorized_queue")]["account_id"]
        is DataClass.ACCOUNT_IDENTIFIER
    )
    assert ("reports", "net_worth") in mapping


def test_reports_uncategorized_queue_masks_account_id(populated_db: Database) -> None:
    """A real `reports.*` bridged view masks its CRITICAL column end-to-end.

    Mirrors `sqlmesh/models/reports/uncategorized_queue.sql` over the
    fixture's `core.*` tables (no scenario/SQLMesh build) so this runs at
    unit speed. Proves two things together: the `reports` schema is
    queryable through `execute_sql_query`, and `account_id` — classified
    CRITICAL by the transitional bridge, unlike `dim_accounts.account_id`
    which is LOW (see `test_account_id_passes_through_unmasked`) — is
    actually masked when read through the bridged view, not just declared
    masked in the class map.
    """
    _seed_account(populated_db)
    populated_db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, "
        " amount_absolute, transaction_direction, description, "
        " category, is_transfer, source_type, loaded_at, updated_at) "
        "VALUES ('TXN_UNCAT', 'ACC000123456789', '2025-06-15', -42.50, "
        " 42.50, 'expense', 'Uncategorized coffee', "
        " NULL, FALSE, 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    )
    # Local to this (function-scoped) populated_db instance — does not touch
    # the shared conftest.py fixture other tests depend on.
    populated_db.execute("""
        CREATE OR REPLACE VIEW reports.uncategorized_queue AS
        SELECT
            t.transaction_id,
            t.account_id,
            a.display_name AS account_name,
            t.transaction_date AS txn_date,
            t.amount,
            t.description,
            t.merchant_id,
            t.merchant_name AS merchant_normalized,
            CAST(CURRENT_DATE - t.transaction_date AS INT) AS age_days,
            ABS(t.amount) * CAST(CURRENT_DATE - t.transaction_date AS INT)
                AS priority_score,
            t.source_type,
            NULL::TEXT AS source_id
        FROM core.fct_transactions AS t
        INNER JOIN core.dim_accounts AS a ON t.account_id = a.account_id
        WHERE t.category IS NULL AND NOT t.is_transfer AND NOT a.archived
    """)

    result = execute_sql_query(
        populated_db,
        "SELECT account_id FROM reports.uncategorized_queue LIMIT 5",
        max_rows=5,
    )

    assert result.tier is Tier.CRITICAL
    assert len(result.records) == 1
    assert str(result.records[0]["account_id"]).startswith("****")


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


def test_unresolvable_expression_does_not_over_mask(populated_db: Database) -> None:
    """``COUNT(*)`` still classifies as AGGREGATE (LOW), not CRITICAL.

    Not a guard against blanket fail-closed: ``COUNT(*)`` has no column
    reference at all, so ``_classify_projection``'s counting-aggregate branch
    returns AGGREGATE before ``_column_key``, ``_class_of_key``,
    ``_fallback_class``, or ``_coverage_gap_class`` are ever reached — a
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


def test_unresolvable_column_reference_classifies_by_scope_inputs(
    populated_db: Database,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A LATERAL-derived column (`key is None`) classifies by scope, not blanket CRITICAL.

    ``x`` in the outer SELECT refers to the LATERAL derived table's alias
    ``l``, not a real catalog table or a source ``_class_via_source_scope``
    resolves — CTE and plain derived-table aliases resolve through
    ``scope.sources`` (see that function's docstring), but a LATERAL source
    does not, so ``_column_key`` returns ``None`` for ``l.x`` and
    classification falls through to ``_fallback_class`` (the ``key is None``
    branch in ``_classify_projection``), unlike ``COUNT(*)`` above which
    never gets there. The scope's only real input column is ``t.amount``
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
