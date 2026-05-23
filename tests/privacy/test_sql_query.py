"""Tests for the shared privacy-enforcing SQL execution primitive.

``execute_sql_query`` is the single primitive behind both the ``sql_query``
MCP tool and the ``moneybin sql query`` CLI command. These tests pin the
enforcement contract at the primitive level — redaction, schema gating,
aggregation tiers, truncation, and error classification — so both surfaces
inherit identical behavior structurally.
"""

from __future__ import annotations

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.sql_query import execute_sql_query
from moneybin.privacy.taxonomy import Tier


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


def test_critical_account_id_masked(populated_db: Database) -> None:
    """CRITICAL account_id is masked (****<last4>) and the tier is CRITICAL."""
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT account_id FROM core.dim_accounts", max_rows=100
    )
    assert result.tier is Tier.CRITICAL
    assert str(result.records[0]["account_id"]).startswith("****")


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
    runtime column order (the round-5 SELECT * bypass).
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT * FROM core.dim_accounts", max_rows=100
    )
    row = result.records[0]
    assert str(row["account_id"]).startswith("****")
    assert row["routing_number"] == "*****"
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

    `MIN(account_id)` → DuckDB column 'min(account_id)' vs sqlglot ''. The name
    miss fails closed to the query's max tier (CRITICAL), so the value is masked
    — never returned in the clear.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT MIN(account_id) FROM core.dim_accounts", max_rows=100
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
    """Unaliased MIN(account_id) is masked despite the sqlglot/DuckDB name split.

    sqlglot names the projection ``''`` while DuckDB calls the column
    ``min(account_id)``; position-aligned redaction masks it by the real name.
    """
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT MIN(account_id) FROM core.dim_accounts", max_rows=100
    )
    assert result.tier is Tier.CRITICAL
    (value,) = result.records[0].values()
    assert str(value).startswith("****")


def test_classes_returned_includes_account_identifier(populated_db: Database) -> None:
    """classes_returned surfaces the resolved data classes for observability."""
    _seed_account(populated_db)
    result = execute_sql_query(
        populated_db, "SELECT account_id FROM core.dim_accounts", max_rows=100
    )
    assert "account_identifier" in result.classes_returned
