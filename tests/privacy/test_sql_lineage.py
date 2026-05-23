"""Tests for the sqlglot-based SQL column lineage resolver.

TDD order: Task 1 (parse cache), Task 2 (schema snapshot),
Task 3 (star expansion + input columns), Task 4 (output-class resolution).
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.sql_lineage import (
    SqlParseError,
    expand_star,
    get_current_schema_snapshot,
    parse_cached,
    resolve_output_classes,
)
from moneybin.privacy.taxonomy import DataClass, Tier


def test_parse_cached_returns_expression_and_caches() -> None:
    sql = "SELECT amount FROM core.fct_transactions"
    first = parse_cached(sql)
    # Whitespace-normalized variant must hit the same cached object.
    second = parse_cached("  SELECT   amount   FROM core.fct_transactions  ")
    assert first is second


def test_parse_cached_raises_on_invalid_sql() -> None:
    with pytest.raises(SqlParseError):
        parse_cached("SELECT FROM WHERE )(")


# ---------------------------------------------------------------------------
# Task 2: Schema snapshot
# ---------------------------------------------------------------------------


def test_schema_snapshot_includes_core_columns(populated_db: Database) -> None:
    snap = get_current_schema_snapshot(populated_db)
    assert ("core", "fct_transactions", "amount") in snap.columns
    assert ("core", "dim_accounts", "account_id") in snap.columns


def test_schema_snapshot_cached_until_version_changes(populated_db: Database) -> None:
    a = get_current_schema_snapshot(populated_db)
    b = get_current_schema_snapshot(populated_db)
    assert a is b  # same migration version → cached identity


# ---------------------------------------------------------------------------
# Task 3: Star expansion + input-column collection
# ---------------------------------------------------------------------------


from sqlglot import exp  # noqa: E402 — imported after stdlib block

from moneybin.privacy.sql_lineage import collect_input_columns  # noqa: E402


def test_expand_star_lists_every_column(populated_db: Database) -> None:
    snap = get_current_schema_snapshot(populated_db)
    tree = expand_star(parse_cached("SELECT * FROM core.dim_accounts"), snap)
    # No bare Star node remains.
    assert not list(tree.find_all(exp.Star))
    # account_id is now an explicit projection.
    select = tree.find(exp.Select)
    assert select is not None
    names = {s.alias_or_name for s in select.selects}
    assert "account_id" in names


def test_collect_input_columns_finds_where_and_join_cols(
    populated_db: Database,
) -> None:
    snap = get_current_schema_snapshot(populated_db)
    sql = (
        "SELECT t.amount FROM core.fct_transactions t "
        "JOIN core.dim_accounts a ON t.account_id = a.account_id "
        "WHERE a.account_type = 'checking'"
    )
    cols = collect_input_columns(expand_star(parse_cached(sql), snap), snap)
    assert ("core", "fct_transactions", "amount") in cols
    assert ("core", "dim_accounts", "account_id") in cols
    assert ("core", "dim_accounts", "account_type") in cols


# ---------------------------------------------------------------------------
# Task 4: Output-class resolution + aggregation tier rules
# ---------------------------------------------------------------------------


def _classes(sql: str, db: Database) -> dict[str, DataClass]:
    snap = get_current_schema_snapshot(db)
    return resolve_output_classes(expand_star(parse_cached(sql), snap), snap)


def test_direct_column(populated_db: Database) -> None:
    assert _classes("SELECT amount FROM core.fct_transactions", populated_db) == {
        "amount": DataClass.TXN_AMOUNT
    }


def test_count_star_is_aggregate(populated_db: Database) -> None:
    assert _classes(
        "SELECT COUNT(*) AS n FROM core.fct_transactions", populated_db
    ) == {"n": DataClass.AGGREGATE}


def test_sum_preserves_source_class(populated_db: Database) -> None:
    assert _classes(
        "SELECT SUM(amount) AS spend FROM core.fct_transactions", populated_db
    ) == {"spend": DataClass.TXN_AMOUNT}


def test_count_distinct_account_id_is_aggregate(populated_db: Database) -> None:
    out = _classes(
        "SELECT COUNT(DISTINCT account_id) AS n FROM core.dim_accounts", populated_db
    )
    assert out == {"n": DataClass.AGGREGATE}


def test_min_account_id_stays_critical(populated_db: Database) -> None:
    out = _classes("SELECT MIN(account_id) AS m FROM core.dim_accounts", populated_db)
    assert out == {"m": DataClass.ACCOUNT_IDENTIFIER}


def test_multi_column_expression_takes_max_tier(populated_db: Database) -> None:
    out = _classes(
        "SELECT CONCAT(merchant_name, ' - ', description) AS d FROM core.fct_transactions",
        populated_db,
    )
    # Both merchant_name and description are MEDIUM; either may win the tie.
    assert out == {"d": DataClass.MERCHANT_NAME} or out == {"d": DataClass.DESCRIPTION}
    assert next(iter(out.values())).tier is Tier.MEDIUM


def test_derive_query_tier_takes_max(populated_db: Database) -> None:
    from moneybin.privacy.sql_lineage import derive_query_tier

    out = _classes(
        "SELECT account_id, account_type FROM core.dim_accounts", populated_db
    )
    assert derive_query_tier(out) is Tier.CRITICAL
