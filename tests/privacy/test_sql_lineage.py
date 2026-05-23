"""Tests for the sqlglot-based SQL column lineage resolver.

TDD order: Task 1 (parse cache), Task 2 (schema snapshot),
Task 3 (star expansion + input columns), Task 4 (output-class resolution),
Task 5 (corpus + parametrized), Task 6 (conservative fallback).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from moneybin.database import Database
from moneybin.privacy.sql_lineage import (
    SqlParseError,
    derive_query_tier,
    expand_star,
    get_current_schema_snapshot,
    is_data_query,
    parse_cached,
    resolve_output_classes,
    tables_outside_schemas,
)
from moneybin.privacy.taxonomy import DataClass, Tier

_CORPUS = yaml.safe_load(
    (Path(__file__).parent / "fixtures" / "sql_lineage_corpus.yaml").read_text()
)


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
    out = _classes(
        "SELECT account_id, account_type FROM core.dim_accounts", populated_db
    )
    assert derive_query_tier(out) is Tier.CRITICAL


# ---------------------------------------------------------------------------
# Task 5: Parametrized corpus (≥50 entries)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", _CORPUS, ids=[c["description"] for c in _CORPUS])
def test_corpus_resolves_expected_classes(
    case: dict[str, object], populated_db: Database
) -> None:
    sql = str(case["sql"])
    snap = get_current_schema_snapshot(populated_db)
    tree = expand_star(parse_cached(sql), snap)
    got = {k: v.value for k, v in resolve_output_classes(tree, snap, sql).items()}
    assert got == case["expected_output_classes"]
    tier = derive_query_tier(resolve_output_classes(tree, snap, sql))
    assert tier.name.lower() == case["expected_query_tier"]


# ---------------------------------------------------------------------------
# Task 6: Conservative fallback verification
# ---------------------------------------------------------------------------


def test_unresolvable_projection_falls_back_to_max_input_tier(
    populated_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Monkeypatch _column_key to fail for 'amount'; assert fallback = CRITICAL.

    The query touches account_id (CRITICAL) in its input columns, so when
    the 'amount' projection cannot be resolved, the conservative fallback
    raises the floor to the max input tier: CRITICAL. This ensures we
    over-redact rather than under-redact.
    """
    import moneybin.privacy.sql_lineage as lin

    sql = "SELECT account_id, amount FROM core.fct_transactions"
    snap = get_current_schema_snapshot(populated_db)
    tree = lin.expand_star(lin.parse_cached(sql), snap)

    real = lin._column_key  # pyright: ignore[reportPrivateUsage]

    def flaky(
        col: object,
        alias_map: object,
        snapshot: object,
    ) -> object:
        # _column_key(col, alias_map, snapshot) — fail only for 'amount'
        from sqlglot import exp as _exp

        if isinstance(col, _exp.Column) and col.name == "amount":
            return None
        return real(col, alias_map, snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(lin, "_column_key", flaky)  # pyright: ignore[reportPrivateUsage]

    out = lin.resolve_output_classes(tree, snap, sql)
    # account_id still resolves to CRITICAL; amount falls back to max input tier
    # (CRITICAL, because account_id is among the input columns).
    assert out["amount"].tier is Tier.CRITICAL


def test_cte_outer_column_falls_back_to_max_inner_tier(populated_db: Database) -> None:
    """CTE outer SELECT cannot resolve column to schema directly; falls back to max input tier.

    The CTE inner query references account_id (CRITICAL) and amount (HIGH).
    The outer SELECT's account_id projection hits the fallback (CTE column is
    not in the schema snapshot), and the max input tier from the whole tree is
    CRITICAL (account_id is present). The fallback is conservative — CRITICAL.
    """
    out = _classes(
        "WITH spend AS (SELECT account_id, amount FROM core.fct_transactions) "
        "SELECT account_id FROM spend",
        populated_db,
    )
    assert next(iter(out.values())).tier is Tier.CRITICAL


def test_union_classifies_every_branch_by_position(populated_db: Database) -> None:
    """A CRITICAL column in a later UNION branch masks the output position.

    Output names come from the first branch (``description``, MEDIUM), but the
    second branch supplies ``account_id`` (CRITICAL) by position. Classifying
    only the first branch would leak account numbers in the ``description``
    column; the per-position max-tier rule must yield CRITICAL.
    """
    out = _classes(
        "SELECT description FROM core.fct_transactions "
        "UNION ALL "
        "SELECT account_id FROM core.dim_accounts",
        populated_db,
    )
    assert list(out.keys()) == ["description"]
    assert out["description"] is DataClass.ACCOUNT_IDENTIFIER
    assert derive_query_tier(out) is Tier.CRITICAL


def test_tables_outside_schemas_flags_raw_and_reports(populated_db: Database) -> None:
    """raw.*/reports.* are flagged; core/app and CTE names are not."""
    snap = get_current_schema_snapshot(populated_db)

    def bad(sql: str) -> list[str]:
        return tables_outside_schemas(
            expand_star(parse_cached(sql), snap), snap, frozenset({"core", "app"})
        )

    assert bad("SELECT account_id FROM raw.ofx_transactions") == [
        "raw.ofx_transactions"
    ]
    assert bad("SELECT x FROM reports.spending") == ["reports.spending"]
    assert bad("SELECT amount FROM core.fct_transactions") == []
    # Unqualified core table resolves via the snapshot — not flagged.
    assert bad("SELECT amount FROM fct_transactions") == []
    # CTE name is not a real table — not flagged.
    assert (
        bad("WITH s AS (SELECT amount FROM core.fct_transactions) SELECT * FROM s")
        == []
    )


def test_is_data_query_separates_data_from_metadata() -> None:
    """SELECT/UNION are data queries; DESCRIBE/SHOW/PRAGMA/EXPLAIN are not."""
    assert is_data_query(parse_cached("SELECT 1"))
    assert is_data_query(parse_cached("SELECT a FROM t UNION ALL SELECT b FROM u"))
    assert not is_data_query(parse_cached("DESCRIBE core.fct_transactions"))
    assert not is_data_query(parse_cached("SHOW TABLES"))
    assert not is_data_query(parse_cached("PRAGMA database_list"))
    assert not is_data_query(parse_cached("EXPLAIN SELECT 1"))


def test_fallback_log_omits_raw_sql(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The conservative-fallback WARNING logs a hash, never the raw SQL (no PII)."""
    snap = get_current_schema_snapshot(populated_db)
    pii_literal = "Chase acct 123456789"
    # Literal embedded directly (no f-string) so this stays a static test string.
    sql = (
        "WITH s AS (SELECT account_id FROM core.fct_transactions "
        "WHERE description = 'Chase acct 123456789') SELECT account_id FROM s"
    )
    with caplog.at_level("WARNING"):
        resolve_output_classes(expand_star(parse_cached(sql), snap), snap, sql)
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert pii_literal not in logged
    assert "sha256=" in logged


def test_scalar_subquery_count_does_not_downgrade(populated_db: Database) -> None:
    """A COUNT inside a scalar subquery must not downgrade a co-referenced column.

    `(SELECT COUNT(*) ...) + amount` references `amount` (HIGH) at the top
    level; the nested COUNT must not collapse the projection to LOW aggregate.
    """
    out = _classes(
        "SELECT (SELECT COUNT(*) FROM core.fct_transactions) + amount AS total "
        "FROM core.fct_transactions",
        populated_db,
    )
    assert out == {"total": DataClass.TXN_AMOUNT}
