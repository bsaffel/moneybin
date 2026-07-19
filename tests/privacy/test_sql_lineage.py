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
    _MAX_SCOPE_DEPTH,  # pyright: ignore[reportPrivateUsage]
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


def _routing_chain_ctes(depth: int) -> list[str]:
    """CTE bodies passing ``routing_number`` through ``depth`` levels of aliasing.

    Each level adds one nested scope the classifier must walk, so a large
    ``depth`` is how a test drives the resolver past ``_MAX_SCOPE_DEPTH``.
    """
    ctes = ["c0 AS (SELECT routing_number AS v FROM core.dim_accounts)"]
    ctes += [f"c{i} AS (SELECT v FROM c{i - 1})" for i in range(1, depth + 1)]  # noqa: S608  # test input string, not executing SQL
    return ctes


def _with_query(ctes: list[str], final_select: str) -> str:
    return "WITH " + ", ".join(ctes) + " " + final_select


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
        shadowed: object = frozenset(),
    ) -> object:
        # _column_key(col, alias_map, snapshot, shadowed) — fail only for
        # 'credit_limit'. ``shadowed`` defaults so the call sites that omit it
        # (collect_input_columns) keep working through the patch.
        from sqlglot import exp as _exp

        if isinstance(col, _exp.Column) and col.name == "credit_limit":
            return None
        return real(col, alias_map, snapshot, shadowed)  # type: ignore[arg-type]

    monkeypatch.setattr(lin, "_column_key", flaky)  # pyright: ignore[reportPrivateUsage]

    out = lin.resolve_output_classes(tree, snap, sql)
    # routing_number still resolves to CRITICAL; credit_limit falls back to max
    # input tier (CRITICAL, because routing_number is among the input columns).
    assert out["credit_limit"].tier is Tier.CRITICAL


def test_unqualified_cte_column_resolves_to_base_table_class(
    populated_db: Database,
) -> None:
    """An UNQUALIFIED CTE column resolves precisely, and keeps CRITICAL.

    Renamed from ``test_cte_outer_column_falls_back_to_max_inner_tier``: the
    CTE-aware classifier resolves this shape rather than falling back, so the
    old name described a path this query no longer takes. It still earns its
    place — the outer ``routing_number`` carries no table prefix, so it exercises
    the single-``selected_sources`` branch of ``_class_via_source_scope`` rather
    than the aliased branch ``test_cte_column_resolves_to_base_table_class``
    covers. Asserting the CLASS, not just the tier, is what makes the
    distinction visible: the old tier-only assertion passed identically whether
    the answer came from precise resolution or from a CRITICAL fallback.
    """
    out = _classes(
        "WITH acct AS (SELECT routing_number, credit_limit FROM core.dim_accounts) "
        "SELECT routing_number FROM acct",
        populated_db,
    )
    assert out == {"routing_number": DataClass.ROUTING_NUMBER}


@pytest.mark.parametrize("depth", [17, 30, 60, 200])
def test_deep_cte_chain_beyond_depth_limit_stays_critical(
    depth: int, populated_db: Database
) -> None:
    """A CTE chain deeper than ``_MAX_SCOPE_DEPTH`` must not under-classify to LOW.

    Regression for the depth-exhaustion leak: once recursion ran out of depth,
    the column became unresolvable and the floor was computed over the LOCAL CTE
    body (``SELECT v FROM c15``) — which references no catalog column, so the
    floor was AGGREGATE (LOW). That LOW propagated outward as a real answer and
    ``routing_number`` came back in the clear from a ~17-line generated query.
    The floor must instead be computed over a scope that actually contains
    catalog columns, which for this query means CRITICAL.

    ``depth=200`` additionally pins that user-supplied SQL cannot exhaust the
    Python stack: this runs on untrusted input, so a RecursionError is a DoS,
    not a test failure.
    """
    ctes = _routing_chain_ctes(depth)
    sql = _with_query(ctes, f"SELECT c{depth}.v FROM c{depth}")  # noqa: S608  # test input string, not executing SQL

    out = _classes(sql, populated_db)

    assert out["v"].tier is Tier.CRITICAL
    assert derive_query_tier(out) is Tier.CRITICAL


def test_union_in_cte_with_one_unresolvable_branch_is_not_low(
    populated_db: Database,
) -> None:
    """A set operation must decline entirely when ANY branch fails to resolve.

    Regression for the partial-union leak: ``_class_at_index`` built its answer
    from the branches that happened to resolve and dropped the ``None`` ones, so
    a CTE unioning ``category`` (LOW) with a depth-exhausted chain ending in
    ``routing_number`` returned CATEGORY — LOW and unmasked. The unresolved
    branch is precisely the one that might be carrying the CRITICAL value, so
    the whole position must fall to the conservative floor.
    """
    ctes = _routing_chain_ctes(30)
    ctes.append(
        "u AS (SELECT category AS v FROM core.fct_transactions "
        "UNION ALL SELECT c30.v FROM c30)"
    )
    sql = _with_query(ctes, "SELECT u.v FROM u")

    out = _classes(sql, populated_db)

    assert out["v"].tier is not Tier.LOW
    assert out["v"].tier is Tier.CRITICAL


def test_column_in_scalar_subquery_resolves_in_its_own_scope(
    populated_db: Database,
) -> None:
    """A column inside an IN-subquery resolves against the subquery's scope.

    ``reports.large_transactions.is_top_100`` has this shape. Resolving the
    inner column against the OUTER scope (three selected sources → ambiguous)
    made the whole projection unresolvable and pushed it to a fallback; the
    inner scope names exactly one source and resolves it exactly.
    """
    out = _classes(
        "WITH base AS (SELECT transaction_id, amount FROM core.fct_transactions), "
        "top_n AS (SELECT transaction_id FROM base ORDER BY amount DESC LIMIT 10) "
        "SELECT b.transaction_id IN (SELECT transaction_id FROM top_n) AS flag "
        "FROM base b",
        populated_db,
    )
    assert out == {"flag": DataClass.RECORD_ID}


def test_cte_column_resolves_to_base_table_class(populated_db: Database) -> None:
    """A CTE-alias column classifies as its underlying base column, not by scope max."""
    out = _classes(
        "WITH c AS (SELECT account_id, amount FROM core.fct_transactions) "
        "SELECT c.account_id FROM c",
        populated_db,
    )
    # RECORD_ID (LOW), NOT TXN_AMOUNT (HIGH) inherited from `amount` in the CTE.
    assert out == {"account_id": DataClass.RECORD_ID}


def test_cte_does_not_leak_tier_across_unrelated_projections(
    populated_db: Database,
) -> None:
    """A projection must not inherit tier from a CTE column it does not depend on."""
    out = _classes(
        "WITH c AS (SELECT account_id, amount FROM core.fct_transactions) "
        "SELECT COUNT(*) AS n FROM c",
        populated_db,
    )
    assert out == {"n": DataClass.AGGREGATE}


def test_nested_cte_chain_resolves(populated_db: Database) -> None:
    """Three CTE levels (the recurring_subscriptions shape) still resolve."""
    out = _classes(
        "WITH a AS (SELECT account_id, amount FROM core.fct_transactions), "
        "b AS (SELECT account_id, amount FROM a), "
        "c AS (SELECT account_id FROM b) "
        "SELECT c.account_id FROM c",
        populated_db,
    )
    assert out == {"account_id": DataClass.RECORD_ID}


def test_cte_preserves_critical_class_through_alias_rename(
    populated_db: Database,
) -> None:
    """Precision must not under-redact: a renamed CRITICAL column stays CRITICAL.

    The CTE aliases ``routing_number`` to ``r``; resolving the outer ``r`` to
    the CTE's projection must carry ROUTING_NUMBER (CRITICAL) through, not the
    ``account_type`` (LOW) sitting beside it.
    """
    out = _classes(
        "WITH c AS (SELECT routing_number AS r, account_type FROM core.dim_accounts) "
        "SELECT c.r, c.account_type FROM c",
        populated_db,
    )
    assert out["r"] is DataClass.ROUTING_NUMBER
    assert out["account_type"].tier is Tier.LOW
    assert derive_query_tier(out) is Tier.CRITICAL


def test_cte_over_union_takes_max_tier_across_branches(populated_db: Database) -> None:
    """A CTE whose body is a UNION classifies the position across ALL branches."""
    out = _classes(
        "WITH c AS ("
        "SELECT description AS v FROM core.fct_transactions "
        "UNION ALL "
        "SELECT routing_number AS v FROM core.dim_accounts"
        ") SELECT c.v FROM c",
        populated_db,
    )
    assert out["v"] is DataClass.ROUTING_NUMBER


def test_recursive_cte_is_cycle_safe(populated_db: Database) -> None:
    """A self-referencing CTE terminates, and still masks the CRITICAL position.

    The seen-scope guard is what stops the self-reference from recursing
    forever. Both projections are asserted because termination alone is not the
    property that matters: position 1 must still resolve CRITICAL, proving the
    cycle guard bails conservatively instead of losing the routing number.
    """
    sql = (
        "WITH RECURSIVE r AS ("
        "SELECT account_id, routing_number FROM core.dim_accounts "
        "UNION ALL "
        "SELECT account_id, routing_number FROM r"
        ") SELECT r.account_id, r.routing_number FROM r"
    )
    out = _classes(sql, populated_db)
    # Position 0 is account_id in every branch — RECORD_ID is exact, not a leak.
    assert out["account_id"] is DataClass.RECORD_ID
    assert out["routing_number"] is DataClass.ROUTING_NUMBER
    assert derive_query_tier(out) is Tier.CRITICAL


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


@pytest.mark.parametrize("op", ["EXCEPT", "INTERSECT"])
def test_set_operations_are_data_queries(op: str) -> None:
    """EXCEPT / INTERSECT return rows, so they must NOT route as metadata.

    Regression for a schema-gate + masking bypass. On sqlglot 30.8.0
    ``exp.Except`` and ``exp.Intersect`` are siblings of ``exp.Union`` under
    ``exp.SetOperation`` — they do NOT subclass ``exp.Union``. The old
    ``isinstance(tree, (exp.Select, exp.Union))`` therefore answered False for a
    top-level ``EXCEPT``, and ``execute_sql_query`` sent it down the
    DESCRIBE/SHOW branch: no table allowlist, no lineage, no masking, tier LOW.
    """
    sql = f"SELECT a FROM t {op} SELECT b FROM u"  # noqa: S608  # test input string, not executing SQL
    assert is_data_query(parse_cached(sql))


def test_nested_set_operation_classifies_the_value_bearing_branches(
    populated_db: Database,
) -> None:
    """``A UNION B EXCEPT C`` takes its classes from A and B, not from C.

    ``_union_select_branches`` fell through to ``tree.find(exp.Select)`` for a
    set operation that wasn't an ``exp.Union``. ``find`` walks breadth-first, so
    for ``Except(left=Union(A, B), right=C)`` it returned **C** — the operand
    that contributes no output values — and A and B were never classified. Here
    C is the LOW branch, so misreading it as the source returned TXN_TYPE/LOW
    for a column carrying routing numbers.
    """
    out = _classes(
        "SELECT routing_number AS v FROM core.dim_accounts "
        "UNION SELECT routing_number AS v FROM core.dim_accounts "
        "EXCEPT SELECT account_type AS v FROM core.dim_accounts",
        populated_db,
    )
    assert out == {"v": DataClass.ROUTING_NUMBER}
    assert derive_query_tier(out) is Tier.CRITICAL


def test_except_takes_classes_from_the_left_branch_only(
    populated_db: Database,
) -> None:
    """``EXCEPT``/``INTERSECT`` emit LEFT-branch values; the right only filters.

    The counterpart to ``test_union_classifies_every_branch_by_position``: a
    UNION must take the max across branches because both supply values, but
    widening that rule to EXCEPT would over-redact every difference query. Pins
    that the asymmetry is deliberate, so a future "just treat all SetOperations
    like UNION" simplification has to argue with a test.
    """
    out = _classes(
        "SELECT account_type AS v FROM core.dim_accounts "
        "EXCEPT SELECT routing_number AS v FROM core.dim_accounts",
        populated_db,
    )
    assert out == {"v": DataClass.TXN_TYPE}
    assert derive_query_tier(out) is Tier.LOW


# ---------------------------------------------------------------------------
# A CTE / derived table named after a catalog table must never resolve to it
# ---------------------------------------------------------------------------


def _shadow_chain(depth: int) -> list[str]:
    """A ``routing_number`` chain ``depth`` levels deep, aliased to ``account_type``.

    The alias matters: ``account_type`` is a real ``core.dim_accounts`` column
    classified TXN_TYPE (LOW), so a classifier that resolves the shadowing name
    against the catalog produces a plausible LOW answer instead of declining.
    """
    ctes = ["c0 AS (SELECT routing_number AS account_type FROM core.dim_accounts)"]
    ctes += [
        f"c{i} AS (SELECT account_type FROM c{i - 1})"  # noqa: S608  # test input string, not executing SQL
        for i in range(1, depth + 1)
    ]
    return ctes


# 5 resolves inside the scope; 16/30/60 exhaust _MAX_SCOPE_DEPTH and must reach
# the conservative floor instead of the catalog.
_SHADOW_DEPTHS = [5, 16, 30, 60]


@pytest.mark.parametrize("depth", _SHADOW_DEPTHS)
def test_cte_named_after_catalog_table_never_resolves_to_it(
    depth: int, populated_db: Database
) -> None:
    """A CTE named ``dim_accounts`` must not borrow ``core.dim_accounts``'s classes.

    Regression for a depth-independent under-classification leak. Once the chain
    exhausted ``_MAX_SCOPE_DEPTH``, ``_class_via_source_scope`` correctly
    DECLINED — but control fell through to ``_column_key``, which resolved the
    CTE name ``dim_accounts`` to the catalog table by two independent paths
    (``_build_alias_map`` walks Table nodes inside CTE bodies, so
    ``core.dim_accounts`` self-registers under the bare key; and the bare-name
    catalog scan matches any CTE named like a real table). The decline became a
    confident TXN_TYPE/LOW and the routing number came back in the clear.

    The expected class is UNRESOLVED, not ROUTING_NUMBER: the projection is
    answered by ``_conservative_floor``, which reports a BOUND and so never
    names a specific CRITICAL class (see that function). This assertion read
    ROUTING_NUMBER until the equal-CRITICAL tie-break was fixed — the floor
    returned its column-max, which happened to be the right class here but was
    an unrelated one (and a WEAKER, partial mask) whenever the query merely
    co-referenced a different CRITICAL column. UNRESOLVED discriminates the
    guarded leak exactly as well: TXN_TYPE/LOW remains the failure mode, and
    the value is still masked whole end-to-end.
    """
    ctes = _shadow_chain(depth)
    ctes.append(f"dim_accounts AS (SELECT account_type FROM c{depth})")  # noqa: S608  # test input string, not executing SQL
    sql = _with_query(ctes, "SELECT dim_accounts.account_type FROM dim_accounts")

    out = _classes(sql, populated_db)

    assert out["account_type"] is DataClass.UNRESOLVED
    assert derive_query_tier(out) is Tier.CRITICAL


@pytest.mark.parametrize("depth", _SHADOW_DEPTHS)
def test_cte_aliased_to_a_catalog_table_name_never_resolves_to_it(
    depth: int, populated_db: Database
) -> None:
    """``FROM c{n} AS dim_accounts`` shadows just as a ``WITH`` name does.

    The same leak without the WITH-naming trick: the shadowing name arrives as a
    FROM-clause alias instead. Pins that the fix keys on "this reference names a
    Scope source", not on the syntax that bound the name.

    The expected class differs by depth, and that split is the point: a chain
    shallower than ``_MAX_SCOPE_DEPTH`` genuinely RESOLVES through the CTEs to
    the true source class, while a deeper one exhausts the depth guard and is
    answered by ``_conservative_floor`` — which reports a BOUND and so never
    names a specific CRITICAL class. Asserting one class for both depths (as
    this test did before the equal-CRITICAL tie-break fix) blurred exactly the
    distinction ``_SHADOW_DEPTHS`` exists to draw. Either way the tier is
    CRITICAL and the value masks whole; the guarded leak is TXN_TYPE/LOW.
    """
    sql = _with_query(
        _shadow_chain(depth),
        f"SELECT dim_accounts.account_type FROM c{depth} AS dim_accounts",  # noqa: S608  # test input string, not executing SQL
    )

    out = _classes(sql, populated_db)

    expected = (
        DataClass.ROUTING_NUMBER if depth < _MAX_SCOPE_DEPTH else DataClass.UNRESOLVED
    )
    assert out["account_type"] is expected
    assert derive_query_tier(out) is Tier.CRITICAL


def test_shadowing_cte_resolves_by_semantics_not_by_the_depth_guard(
    populated_db: Database,
) -> None:
    """A shadowing CTE with NO depth exhaustion still refuses the catalog.

    CORRECTED EXPECTATION AND DOCSTRING. This test previously asserted
    ROUTING_NUMBER and claimed that "the floor is not what saves it — the column
    must resolve THROUGH the CTE". That claim was false: this query has ALWAYS
    been answered by ``_conservative_floor`` (verified by the fallback WARNING
    it emits), and it passed only because the floor's buggy equal-CRITICAL
    tie-break returned the column-max, which here coincided with the right
    answer. Fixing the tie-break — which stopped an unrelated co-referenced
    CRITICAL column from substituting its WEAKER partial mask — surfaced the
    discrepancy.

    So this does NOT separate resolution from the floor, and naming it as if it
    did was worse than not having it. What it genuinely pins is unchanged and
    still worth pinning: a name shadowing a catalog table never borrows that
    table's classes, at one level of nesting, with no depth exhaustion involved.
    TXN_TYPE (LOW) remains the failure mode it guards against.
    """
    out = _classes(
        "WITH dim_accounts AS "
        "(SELECT routing_number AS account_type FROM core.dim_accounts) "
        "SELECT account_type FROM dim_accounts",
        populated_db,
    )
    assert out == {"account_type": DataClass.UNRESOLVED}
    assert derive_query_tier(out) is Tier.CRITICAL


def test_derived_table_named_after_catalog_table_never_resolves_to_it(
    populated_db: Database,
) -> None:
    """The non-CTE shadowing form: a derived table aliased to a catalog name.

    Answered by ``_conservative_floor`` (UNRESOLVED, a bound) rather than by
    resolution — same correction as the test above; see its docstring.
    """
    out = _classes(
        "SELECT dim_accounts.account_type FROM "
        "(SELECT routing_number AS account_type FROM core.dim_accounts) AS dim_accounts",
        populated_db,
    )
    assert out == {"account_type": DataClass.UNRESOLVED}
    assert derive_query_tier(out) is Tier.CRITICAL


def test_shadowing_does_not_over_redact_the_unshadowed_table(
    populated_db: Database,
) -> None:
    """The fix must not blanket-raise every query touching a shadowed name.

    Guards the other direction: ``core.dim_accounts.account_type`` read directly,
    with no CTE in sight, still classifies TXN_TYPE (LOW). A fix that declined on
    the bare NAME rather than on "names a Scope source in this query" would push
    this to CRITICAL and quietly mask ordinary queries.
    """
    out = _classes("SELECT account_type FROM core.dim_accounts", populated_db)
    assert out == {"account_type": DataClass.TXN_TYPE}


def test_fallback_log_omits_raw_sql(
    populated_db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The conservative-fallback WARNING logs a hash, never the raw SQL (no PII).

    ``column0`` in a ``VALUES`` row source is not a catalog column at all — no
    ``core``/``app`` table backs it, so ``_column_key`` returns ``None`` and
    ``_class_via_source_scope`` doesn't resolve it either (a VALUES row source
    isn't a nested SELECT scope), so classification reaches
    ``_conservative_floor``. The PII literal lives in the VALUES row itself; the
    log assertions confirm it never reaches the log line, only its hash.
    """
    snap = get_current_schema_snapshot(populated_db)
    pii_literal = "Chase acct 123456789"
    # Literal embedded directly (no f-string) so this stays a static test string.
    sql = "SELECT column0 FROM (VALUES ('Chase acct 123456789')) AS v"
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
# The counting aggregate must not outrank the opaque-node veto
#
# The counting-aggregate collapse governs a projection only when EVERY column
# reference sits inside a count. Its guard tests exactly that —
# ``not any(not _within_counting_agg(c, inner) for c in inner.find_all(Column))``
# — and an opaque node (``COLUMNS(...)``, an unexpanded ``Star``) carries NO
# ``exp.Column`` child at all. ``find_all`` yields nothing, ``any`` over nothing
# is False, ``not False`` is True: the guard passes VACUOUSLY and the projection
# collapsed to AGGREGATE (LOW) — the same "absence of evidence read as proof"
# shape as the other leaks on this branch.
#
# The fix orders the opaque veto FIRST. The one node it still lets through is a
# ``Star`` inside the count — the ``COUNT(*)`` / ``COUNT(t.*)`` idiom, which
# names no columns and is genuinely bounded by the count. That exception is
# load-bearing (``net_worth.account_count``), so the preservation tests below
# are as much a part of this guard as the leak tests.
# ---------------------------------------------------------------------------

_OPAQUE_COUNTING_AGG_QUERIES = {
    # COLUMNS() as the count's own argument. Unlike `*`, COLUMNS DISTRIBUTES:
    # COUNT(COLUMNS('a|b')) becomes the sibling projections COUNT(a), COUNT(b),
    # so one projection yields N runtime columns lineage never named.
    "count-of-columns": "SELECT COUNT(COLUMNS('routing.*')) AS x FROM core.dim_accounts",
    "count-of-columns-all": "SELECT COUNT(COLUMNS('.*')) AS x FROM core.dim_accounts",
    # A counting aggregate co-projected with COLUMNS() in ONE projection. The
    # count does not bound the COLUMNS half, which surfaces its value verbatim.
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
    # A Star that survived expansion, OUTSIDE the count — i.e. a `qualify()`
    # failure, not the COUNT(*) idiom. The count bounds only its own argument.
    "count-concat-unexpanded-star": (
        "SELECT COUNT(*) || * AS x FROM core.dim_accounts"
    ),
    # A resolvable column alongside the opaque node: answering from `account_id`
    # (RECORD_ID, LOW) would publish a confident class over the columns
    # COLUMNS(...) expands to.
    "count-of-column-concat-columns": (
        "SELECT COUNT(account_id) || first(COLUMNS('routing.*')) AS x "
        "FROM core.dim_accounts"
    ),
}


@pytest.mark.parametrize(
    "sql",
    list(_OPAQUE_COUNTING_AGG_QUERIES.values()),
    ids=list(_OPAQUE_COUNTING_AGG_QUERIES),
)
def test_counting_aggregate_never_collapses_an_opaque_projection(
    sql: str, populated_db: Database
) -> None:
    """No projection holding an unbounded opaque node classifies AGGREGATE.

    Asserted as "not AGGREGATE / not LOW" rather than as one exact class: the
    point is that lineage declines to certify a projection it could not
    decompose, and the conservative floor — not this rule — chooses what the
    decline becomes.
    """
    out = _classes(sql, populated_db)
    assert out, "expected at least one output class"
    assert DataClass.AGGREGATE not in out.values()
    assert derive_query_tier(out) is not Tier.LOW


_PRESERVED_COUNTING_AGG_QUERIES = {
    # The load-bearing case: COUNT(*)'s Star is not a failed expansion, and the
    # count really does destroy whatever it covered. net_worth.account_count
    # derives AGGREGATE through this path instead of inheriting account_id.
    "count-star": "SELECT COUNT(*) AS n FROM core.dim_accounts",
    "count-distinct-critical": (
        "SELECT COUNT(DISTINCT routing_number) AS n FROM core.dim_accounts"
    ),
    "count-of-column": "SELECT COUNT(account_id) AS n FROM core.dim_accounts",
    # COUNT(*) over a source whose star qualify() cannot expand. The star here is
    # still the count's own argument, so it stays bounded — the veto must not
    # widen to "any Star anywhere".
    "count-star-over-unexpandable-source": (
        "SELECT COUNT(*) AS n FROM (SUMMARIZE core.dim_accounts)"
    ),
    "count-star-table-qualified": "SELECT COUNT(a.*) AS n FROM core.dim_accounts a",
}


@pytest.mark.parametrize(
    "sql",
    list(_PRESERVED_COUNTING_AGG_QUERIES.values()),
    ids=list(_PRESERVED_COUNTING_AGG_QUERIES),
)
def test_ordinary_counting_aggregate_still_collapses_to_aggregate(
    sql: str, populated_db: Database
) -> None:
    """A genuine counting aggregate keeps returning AGGREGATE (LOW).

    The opaque veto is a narrowing of the collapse rule, not a repeal of it. If
    a fix to the vacuous-guard leak makes any of these fail closed, it has
    over-corrected.
    """
    out = _classes(sql, populated_db)
    assert set(out.values()) == {DataClass.AGGREGATE}
    assert derive_query_tier(out) is Tier.LOW


# ---------------------------------------------------------------------------
# Task 1: Reports declared-class lookup
# ---------------------------------------------------------------------------


def test_reports_class_map_is_keyed_by_reports_schema() -> None:
    m = reports_class_map()
    assert m, "expected at least one @report in ALL_REPORTS"
    assert all(schema == "reports" for (schema, _table) in m)


# A prior version of this module asserted every report's account_id column
# must declare ACCOUNT_IDENTIFIER (CRITICAL). That premise is wrong:
# account_id is a deliberately opaque minted surrogate classified RECORD_ID
# (LOW) everywhere in CLASSIFICATION (spec D6, commit c465f181) — see
# test_account_id_passes_through_unmasked in test_sql_query.py. Some runners
# (cash_flow, balance_drift, large_transactions) over-declare it
# ACCOUNT_IDENTIFIER anyway. That is safe here because it over-declares ACROSS
# tiers (RECORD_ID is LOW, ACCOUNT_IDENTIFIER CRITICAL) — NOT because
# over-declaring is safe in general: at equal CRITICAL tier a partial-masking
# class standing in for a whole-masking one leaks (see _declaration_is_safe in
# test_report_class_derivation.py). It is not required either, so no universal
# per-class assertion belongs here.
# Equivalent regression coverage now lives in
# test_account_id_derives_from_classification_not_the_gap_fallback
# (test_report_class_derivation.py) and test_generated_classes_are_current
# (test_sql_query.py).


# ---------------------------------------------------------------------------
# Task 2: Resolve reports.* columns in _class_of_key
# ---------------------------------------------------------------------------


def test_class_of_key_resolves_reports_via_declared_map() -> None:
    # Pick a real declared (schema, table, column) and assert it resolves.
    (schema, table), cols = next(iter(reports_class_map().items()))
    col, expected = next(iter(cols.items()))
    assert _class_of_key((schema, table, col)) is expected


def test_class_of_key_unknown_reports_column_is_none() -> None:
    # Real declared report table, but a column it does not declare -> None.
    # (Completeness guarantees real columns ARE declared; this probes the
    # known-table / unknown-column path specifically.)
    (schema, table), _cols = next(iter(reports_class_map().items()))
    assert _class_of_key((schema, table, "no_such_column_xyz")) is None
