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
    _class_of_key,  # pyright: ignore[reportPrivateUsage]
    derive_query_tier,
    expand_star,
    get_current_schema_snapshot,
    is_data_query,
    parse_cached,
    reports_class_map,
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


def test_min_routing_number_stays_critical(populated_db: Database) -> None:
    out = _classes(
        "SELECT MIN(routing_number) AS m FROM core.dim_accounts", populated_db
    )
    assert out == {"m": DataClass.ROUTING_NUMBER}


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
        "SELECT routing_number, account_type FROM core.dim_accounts", populated_db
    )
    assert derive_query_tier(out) is Tier.CRITICAL


def test_union_reused_alias_does_not_leak_critical(populated_db: Database) -> None:
    """A UNION reusing one alias for two tables must not under-redact.

    Both branches bind alias ``a`` to a different table. A single tree-wide
    alias map (last-write-wins) resolves ``a`` to the *last* branch's table, so
    branch 0's ``a.routing_number`` misses, falls back to that branch's tier,
    and the CRITICAL routing number classifies as DESCRIPTION (MEDIUM) —
    unmasked. Per-branch alias scoping keeps the output position CRITICAL.
    """
    out = _classes(
        "SELECT a.routing_number FROM core.dim_accounts a "
        "UNION ALL "
        "SELECT a.description FROM core.fct_transactions a",
        populated_db,
    )
    assert out == {"routing_number": DataClass.ROUTING_NUMBER}
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
    """Monkeypatch _column_key to fail for 'credit_limit'; assert fallback = CRITICAL.

    The query touches routing_number (CRITICAL) in its input columns, so when
    the 'credit_limit' projection cannot be resolved, the conservative fallback
    raises the floor to the max input tier: CRITICAL. This ensures we
    over-redact rather than under-redact.
    """
    import moneybin.privacy.sql_lineage as lin

    sql = "SELECT routing_number, credit_limit FROM core.dim_accounts"
    snap = get_current_schema_snapshot(populated_db)
    tree = lin.expand_star(lin.parse_cached(sql), snap)

    real = lin._column_key  # pyright: ignore[reportPrivateUsage]

    def flaky(
        col: object,
        alias_map: object,
        snapshot: object,
    ) -> object:
        # _column_key(col, alias_map, snapshot) — fail only for 'credit_limit'
        from sqlglot import exp as _exp

        if isinstance(col, _exp.Column) and col.name == "credit_limit":
            return None
        return real(col, alias_map, snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(lin, "_column_key", flaky)  # pyright: ignore[reportPrivateUsage]

    out = lin.resolve_output_classes(tree, snap, sql)
    # routing_number still resolves to CRITICAL; credit_limit falls back to max
    # input tier (CRITICAL, because routing_number is among the input columns).
    assert out["credit_limit"].tier is Tier.CRITICAL


def test_cte_outer_column_falls_back_to_max_inner_tier(populated_db: Database) -> None:
    """CTE outer SELECT cannot resolve column to schema directly; falls back to max input tier.

    The CTE inner query references routing_number (CRITICAL) and credit_limit
    (HIGH). The outer SELECT's routing_number projection hits the fallback (CTE
    column is not in the schema snapshot), and the max input tier from the whole
    tree is CRITICAL (routing_number is present). The fallback is conservative —
    CRITICAL.
    """
    out = _classes(
        "WITH acct AS (SELECT routing_number, credit_limit FROM core.dim_accounts) "
        "SELECT routing_number FROM acct",
        populated_db,
    )
    assert next(iter(out.values())).tier is Tier.CRITICAL


def test_union_classifies_every_branch_by_position(populated_db: Database) -> None:
    """A CRITICAL column in a later UNION branch masks the output position.

    Output names come from the first branch (``description``, MEDIUM), but the
    second branch supplies ``routing_number`` (CRITICAL) by position. Classifying
    only the first branch would leak routing numbers in the ``description``
    column; the per-position max-tier rule must yield CRITICAL.
    """
    out = _classes(
        "SELECT description FROM core.fct_transactions "
        "UNION ALL "
        "SELECT routing_number FROM core.dim_accounts",
        populated_db,
    )
    assert list(out.keys()) == ["description"]
    assert out["description"] is DataClass.ROUTING_NUMBER
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


def test_count_plus_critical_column_not_downgraded(populated_db: Database) -> None:
    """A top-level COUNT alongside a surfaced CRITICAL column must not collapse to LOW.

    `COUNT(*) + routing_number GROUP BY routing_number` surfaces routing_number's
    value directly; the count does not suppress it. Classifying the projection as
    AGGREGATE would leak routing numbers unmasked at sensitivity=low.
    """
    out = _classes(
        "SELECT COUNT(*) + routing_number AS x FROM core.dim_accounts "
        "GROUP BY routing_number",
        populated_db,
    )
    assert out == {"x": DataClass.ROUTING_NUMBER}
    assert derive_query_tier(out) is Tier.CRITICAL


def test_count_of_critical_column_stays_aggregate(populated_db: Database) -> None:
    """COUNT(account_id) — value confined inside the count — stays AGGREGATE.

    Guards the boundary of the COUNT+sibling fix: a column whose only
    appearance is inside a counting aggregate is collapsed (the fix must not
    over-redact it to CRITICAL).
    """
    out = _classes("SELECT COUNT(account_id) AS n FROM core.dim_accounts", populated_db)
    assert out == {"n": DataClass.AGGREGATE}


def test_two_unaliased_projections_get_distinct_keys(populated_db: Database) -> None:
    """Two unnamed projections must not collide on one output key.

    `MIN(last_four)` and `MAX(routing_number)` both yield `""` from
    alias_or_name; a positional suffix keeps each a distinct key so neither
    class is dropped (a dropped class weakens sql_query's position-aligned
    fallback).
    """
    out = _classes(
        "SELECT MIN(last_four), MAX(routing_number) FROM core.dim_accounts",
        populated_db,
    )
    assert len(out) == 2
    assert all(c.tier is Tier.CRITICAL for c in out.values())


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


# ---------------------------------------------------------------------------
# Task 1: Reports declared-class lookup
# ---------------------------------------------------------------------------


def test_reports_class_map_is_keyed_by_reports_schema() -> None:
    m = reports_class_map()
    assert m, "expected at least one @report in ALL_REPORTS"
    assert all(schema == "reports" for (schema, _table) in m)


def test_reports_class_map_account_id_is_critical() -> None:
    # Every report that exposes account_id must declare it CRITICAL (ADR-013).
    m = reports_class_map()
    for cols in m.values():
        if "account_id" in cols:
            assert cols["account_id"] is DataClass.ACCOUNT_IDENTIFIER


# ---------------------------------------------------------------------------
# Task 2: Resolve reports.* columns in _class_of_key
# ---------------------------------------------------------------------------


def test_class_of_key_resolves_reports_via_declared_map() -> None:
    # Pick a real declared (schema, table, column) and assert it resolves.
    (schema, table), cols = next(iter(reports_class_map().items()))
    col, expected = next(iter(cols.items()))
    assert _class_of_key((schema, table, col)) is expected


def test_class_of_key_unknown_reports_column_is_none() -> None:
    assert _class_of_key(("reports", "net_worth", "no_such_column")) is None
