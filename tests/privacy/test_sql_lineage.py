"""Tests for the sqlglot-based SQL column lineage resolver.

TDD order: Task 1 (parse cache), Task 2 (schema snapshot),
Task 3 (star expansion + input columns), Task 4 (output-class resolution),
Task 5 (corpus + parametrized), Task 6 (conservative fallback).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from sqlglot import exp

from moneybin.database import Database
from moneybin.privacy.redaction import MaskStrength, mask_strength
from moneybin.privacy.sql_lineage import (
    _FLOORED_SCHEMAS,  # pyright: ignore[reportPrivateUsage]
    _MAX_SCOPE_DEPTH,  # pyright: ignore[reportPrivateUsage]
    FAIL_CLOSED_CLASS,
    ProjectionSource,
    SqlParseError,
    _class_of_key,  # pyright: ignore[reportPrivateUsage]
    _combined_class,  # pyright: ignore[reportPrivateUsage]
    _conservative_floor,  # pyright: ignore[reportPrivateUsage]
    _scope_input_max,  # pyright: ignore[reportPrivateUsage]
    _table_scope_max,  # pyright: ignore[reportPrivateUsage]
    derive_query_tier,
    expand_star,
    get_current_schema_snapshot,
    is_data_query,
    is_metadata_query,
    is_multi_statement,
    parse_cached,
    read_column_classes,
    reports_class_map,
    resolve_output_classes,
    resolve_placeholder_classes,
    resolve_projection_sources,
    tables_outside_schemas,
)
from moneybin.privacy.sql_query import (
    _ALLOWED_QUERY_SCHEMAS,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.privacy.taxonomy import CLASSIFICATION, INTERNAL_CRITICAL, DataClass, Tier

_CORPUS = yaml.safe_load(
    (Path(__file__).parent / "fixtures" / "sql_lineage_corpus.yaml").read_text()
)


def test_parse_cached_returns_expression_and_caches() -> None:
    sql = "SELECT amount FROM core.fct_transactions"
    first = parse_cached(sql)
    second = parse_cached("SELECT amount FROM core.fct_transactions")
    assert first is second


def test_parse_cached_parses_the_text_the_database_executes() -> None:
    """The cache must not rewrite the query on its way to the parser.

    Callers classify ``parse_cached(sql)`` but execute ``sql`` itself, so any
    normalization that changes how the text parses lets the two disagree.
    Collapsing whitespace did exactly that in three ways, all of which let
    unclassified SQL reach the caller (#346).
    """
    # A `--` comment ends at a newline. Collapse it and the smuggled second
    # statement reads as comment text to the parser while DuckDB still runs it.
    smuggled = "SELECT 1 AS a; -- note\nSELECT routing_number AS a FROM t"
    assert is_multi_statement(parse_cached(smuggled))

    # Whitespace inside a quoted identifier or a string literal is data, not
    # formatting: rewriting it resolves a different column than DuckDB reads.
    assert "routing  number" in parse_cached('SELECT "routing  number" FROM t').sql(
        dialect="duckdb"
    )
    assert "a  b" in parse_cached("SELECT 'a  b' AS x").sql(dialect="duckdb")


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


# Schemas whose exclusion from the gate is a decision rather than an accident.
# The complement is unbounded, so it cannot be derived — these two are named for
# the same reason `test_gate_admits_internal_schemas_and_still_fences_meta_and_seeds`
# names them, and each gets a real table so the negative half tests something.
_FENCED_SCHEMAS = ("meta", "seeds")


def test_the_snapshot_covers_exactly_the_schemas_the_gate_admits(
    populated_db: Database,
) -> None:
    """Set equality against ``_ALLOWED_QUERY_SCHEMAS``, derived — never a literal.

    ``get_current_schema_snapshot``'s ``schema_name IN (…)`` list and
    ``sql_query._ALLOWED_QUERY_SCHEMAS`` are two hand-maintained copies of one
    list, and nothing but this test couples them. A schema admitted by the gate
    but missing from the snapshot does NOT fail closed: ``_column_key`` resolves
    against ``snapshot.columns``, so an absent column returns None, the
    projection declines, ``_table_scope_max`` finds no classified table in
    scope, and ``_conservative_floor`` answers AGGREGATE — LOW and passthrough,
    for a column that may be declared CRITICAL. This branch's own red step
    returned a cleartext routing number that way.

    Deriving the expectation from the constant is what makes the guard survive
    the *next* widening. Spelling the schemas out here would pass on the day
    someone adds a sixth to the gate alone, which is exactly the leak.

    A schema with no tables is absent from ``duckdb_columns()`` regardless of
    what this query selects, so every schema on both sides of the equality gets
    a probe table — otherwise the positive half would fail for the wrong reason
    and the negative half would hold vacuously.
    """
    for schema in sorted(_ALLOWED_QUERY_SCHEMAS) + list(_FENCED_SCHEMAS):
        populated_db.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        populated_db.execute(
            f'CREATE TABLE IF NOT EXISTS "{schema}".snapshot_probe (marker VARCHAR)'  # noqa: S608  # schema names come from a frozenset constant, not user input
        )
    # A prep model is a VIEW, not a table — `duckdb_columns()` covers both, and
    # nothing downstream of the snapshot distinguishes them.
    populated_db.execute(
        "CREATE VIEW prep.stg_ofx__accounts AS "
        "SELECT routing_number FROM raw.ofx_accounts"
    )

    snap = get_current_schema_snapshot(populated_db)

    assert {schema for schema, _table, _col in snap.columns} == set(
        _ALLOWED_QUERY_SCHEMAS
    )
    assert ("prep", "stg_ofx__accounts", "routing_number") in snap.columns


def test_every_admitted_schema_has_a_declaration_source() -> None:
    """The gate's schema list partitions into the three ways a class is resolved.

    ``_class_of_key`` answers from exactly three sources — ``CLASSIFICATION``
    (core/app), each report's declared ``@report`` map (reports), and
    ``INTERNAL_CRITICAL`` over the ``_FLOORED_SCHEMAS`` content net (raw/prep).
    ``_FLOORED_SCHEMAS`` is a fourth hand-maintained copy of part of the same
    list, and this is what couples it.

    Omitting a newly-admitted schema from all three is fail-closed, not a leak:
    ``_class_of_key`` returns None, ``_coverage_gap_class`` answers
    ``FAIL_CLOSED_CLASS``, and every value whole-masks. The failure it prevents
    is the usability one — a schema opened at the gate that returns nothing but
    ``*****`` — plus the reverse, a ``_FLOORED_SCHEMAS`` entry that outlives its
    place in the gate and silently floors a schema nobody can reach.
    """
    declared = {schema for schema, _table in CLASSIFICATION}
    declared |= {schema for schema, _table in reports_class_map()}

    assert declared | _FLOORED_SCHEMAS == set(_ALLOWED_QUERY_SCHEMAS)
    assert {schema for schema, _table in INTERNAL_CRITICAL} == _FLOORED_SCHEMAS


# ---------------------------------------------------------------------------
# Task 3: Star expansion + input-column collection
# ---------------------------------------------------------------------------


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


def _classes_bound(
    sql: str, db: Database, placeholder_classes: dict[str, DataClass]
) -> dict[str, DataClass]:
    """``_classes`` for a query with parameters, as the report deriver calls it."""
    snap = get_current_schema_snapshot(db)
    return resolve_output_classes(
        expand_star(parse_cached(sql), snap),
        snap,
        sql,
        placeholder_classes=placeholder_classes,
    )


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
    """SELECT/UNION are data queries; DESCRIBE/SHOW/PRAGMA/EXPLAIN are not.

    Answering False here is not the same as being supported: DESCRIBE and SHOW
    go on to the metadata path, while PRAGMA and EXPLAIN answer False to
    ``is_metadata_query`` too and are refused outright.
    """
    assert is_data_query(parse_cached("SELECT 1"))
    assert is_data_query(parse_cached("SELECT a FROM t UNION ALL SELECT b FROM u"))
    assert not is_data_query(parse_cached("DESCRIBE core.fct_transactions"))
    assert not is_data_query(parse_cached("SHOW TABLES"))
    assert not is_data_query(parse_cached("PRAGMA database_list"))
    assert not is_data_query(parse_cached("EXPLAIN SELECT 1"))


def test_is_metadata_query_is_an_allowlist_not_a_fallback() -> None:
    """Only DESCRIBE and SHOW are metadata — nothing else.

    ``not is_data_query(...)`` is not a safe stand-in for "this is metadata":
    the metadata path executes its string unclassified at LOW, so every
    expression kind sqlglot invents that isn't a SELECT would land there and
    return unredacted rows. That default-open reading is what let a top-level
    ``EXCEPT`` (see ``is_data_query``) and a ``;``-separated ``Block`` through.
    A tree that is neither data nor one of these two must answer False so
    callers fail closed.
    """
    assert is_metadata_query(parse_cached("DESCRIBE core.fct_transactions"))
    assert is_metadata_query(parse_cached("SHOW TABLES"))
    assert not is_metadata_query(parse_cached("SELECT 1"))
    assert not is_metadata_query(parse_cached("SELECT 1; SELECT 2"))


@pytest.mark.parametrize(
    "sql",
    [
        "PRAGMA database_list",
        "PRAGMA storage_info('core.dim_accounts')",
        "PRAGMA table_info('core.dim_accounts')",
        "EXPLAIN SELECT 1",
        "EXPLAIN SELECT a FROM raw.t",
        "EXPLAIN ANALYZE SELECT count(*) FROM raw.t",
    ],
)
def test_ungateable_kinds_are_neither_data_nor_metadata(sql: str) -> None:
    """PRAGMA and EXPLAIN answer False to BOTH tests — that is what fails closed.

    ``execute_sql_query`` refuses anything that is neither, so both answers have
    to stay False for the refusal to hold. Asserting only
    ``not is_metadata_query`` would keep passing if one were later misclassified
    as DATA — where it would reach the lineage path and be classified against a
    projection it does not have.

    Both kinds hide their target from the schema gate: a pragma's is a string
    literal inside ``exp.Anonymous``, and an EXPLAIN's payload stays unparsed
    inside ``exp.Command``. Neither exposes an ``exp.Table``, so neither can be
    gated by name, and no allowlist of verbs would change that.
    """
    tree = parse_cached(sql)
    assert not is_data_query(tree)
    assert not is_metadata_query(tree)


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
    """``EXCEPT`` emits LEFT-branch values; the right operand only filters.

    The counterpart to ``test_union_classifies_every_branch_by_position``: a
    UNION must take the max across branches because both supply values, but
    widening that rule to EXCEPT would over-redact every difference query. Pins
    that the asymmetry is deliberate, so a future "just treat all SetOperations
    like UNION" simplification has to argue with a test.

    ``INTERSECT`` is **not** in this bucket — see
    ``test_intersect_classifies_both_branches``.
    """
    out = _classes(
        "SELECT account_type AS v FROM core.dim_accounts "
        "EXCEPT SELECT routing_number AS v FROM core.dim_accounts",
        populated_db,
    )
    assert out == {"v": DataClass.TXN_TYPE}
    assert derive_query_tier(out) is Tier.LOW


def test_intersect_classifies_both_branches(populated_db: Database) -> None:
    """``INTERSECT`` draws values from BOTH operands, unlike ``EXCEPT``.

    A row survives an INTERSECT only when the value is present on both sides, so
    the value it returns *is* the right operand's value. Classifying from the
    left branch alone made the query an oracle: the LOW/MEDIUM left column named
    the class while every returned row was a real ``routing_number``.
    """
    out = _classes(
        "SELECT account_type AS v FROM core.dim_accounts "
        "INTERSECT SELECT routing_number AS v FROM core.dim_accounts",
        populated_db,
    )
    assert out == {"v": DataClass.ROUTING_NUMBER}
    assert derive_query_tier(out) is Tier.CRITICAL


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


def test_count_beside_a_projected_parameter_is_not_collapsed(
    populated_db: Database,
) -> None:
    """The counting-aggregate collapse passes VACUOUSLY over a placeholder.

    Its guard asks whether every ``exp.Column`` sits inside a count; a projection
    holding only ``COUNT(*)`` and a placeholder has no column at all, so ``any``
    over nothing is False and the projection collapsed to ``AGGREGATE`` —
    publishing the bound value beside a count of rows. The same shape of vacuous
    pass that the opaque-node veto above this rule exists to stop.
    """
    out = _classes_bound(
        "SELECT COUNT(*) || $acct AS x FROM core.dim_accounts",
        populated_db,
        {"acct": DataClass.ROUTING_NUMBER},
    )
    assert out == {"x": DataClass.ROUTING_NUMBER}


def test_a_projection_combining_a_column_and_a_parameter_takes_the_stronger(
    populated_db: Database,
) -> None:
    """A projection can return a column's value AND a bound one at once.

    ``account_id || $acct`` derives from both, so the class describing it is the
    combination — the same rule two co-referenced columns already follow. Reading
    only the columns publishes the binding beside them.
    """
    out = _classes_bound(
        "SELECT account_id || $acct AS x FROM core.dim_accounts",
        populated_db,
        {"acct": DataClass.ROUTING_NUMBER},
    )
    assert out == {"x": DataClass.ROUTING_NUMBER}


def test_a_parameter_confined_inside_a_count_stays_aggregate(
    populated_db: Database,
) -> None:
    """The benign twin of the vacuous-collapse guard.

    A placeholder whose only appearance is inside a counting aggregate has its
    value collapsed to a count exactly as a column does, so the guard must not
    treat every placeholder as surfacing. Without this, ``COUNT($x)`` would mask a
    row count.
    """
    out = _classes_bound(
        "SELECT COUNT($acct) AS n FROM core.dim_accounts",
        populated_db,
        {"acct": DataClass.ROUTING_NUMBER},
    )
    assert out == {"n": DataClass.AGGREGATE}


def test_an_unnamed_positional_placeholder_fails_closed(
    populated_db: Database,
) -> None:
    """A bare ``?`` names no parameter, so no class can be looked up for it.

    Nothing may bind a value this classifier cannot name. The fallback is the
    whole-masking class rather than the caller's declared map, which a positional
    placeholder is absent from by construction.
    """
    out = _classes_bound(
        "SELECT ? AS x FROM core.dim_accounts",
        populated_db,
        {"acct": DataClass.RECORD_ID},
    )
    assert out == {"x": FAIL_CLOSED_CLASS}


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


def test_reports_class_map_skips_a_spec_with_no_graph_backed_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dynamic report has no ``reports.*`` view, so it has nothing to key on.

    The map keys on ``(spec.view.schema, spec.view.name)``; without the skip a
    synthesized spec raises ``AttributeError`` on ``None.schema`` and takes down
    classification for every report, not just the dynamic one.
    """
    from moneybin.reports._framework.contract import (
        OutputColumn,
        ReportQuery,
        ReportSemantics,
        ReportSpec,
    )

    def _runner(db: object) -> ReportQuery:
        return ReportQuery(sql="SELECT 1 AS n")

    _runner._report_spec = ReportSpec(  # type: ignore[attr-defined]
        report_id="user:r0123456789ab",
        name="saved_report",
        description="A saved report",
        view=None,
        runner=_runner,
        classes={"n": DataClass.AGGREGATE},
        columns=(
            OutputColumn(name="n", description="n", data_class=DataClass.AGGREGATE),
        ),
        semantics=ReportSemantics(
            unit=None,
            currency=None,
            sign=None,
            kind="unknown",
            valuation_basis=None,
            fx_basis=None,
            time_basis=None,
            denominator=None,
            comparison_window=None,
            exclusions=(),
            provenance=(),
        ),
    )
    from moneybin.reports import definitions

    monkeypatch.setattr(definitions, "ALL_REPORTS", (*definitions.ALL_REPORTS, _runner))

    m = reports_class_map()

    assert all(schema == "reports" for (schema, _table) in m)
    assert (None, "saved_report") not in m


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


# ---------------------------------------------------------------------------
# Task 5: raw/prep — a short CRITICAL declaration, everything else FLOORED
# ---------------------------------------------------------------------------


def test_declared_raw_critical_column_resolves_critical() -> None:
    assert _class_of_key(("raw", "ofx_accounts", "routing_number")) is (
        DataClass.ROUTING_NUMBER
    )


def test_undeclared_raw_column_floors() -> None:
    assert _class_of_key(("raw", "ofx_transactions", "memo")) is DataClass.FLOORED


def test_undeclared_core_column_does_not_floor() -> None:
    """core/app keep their fail-closed default — the floor is raw/prep only."""
    assert _class_of_key(("core", "fct_transactions", "no_such_column")) is None


def test_account_id_is_classified_per_source_not_uniformly() -> None:
    """``raw.*.account_id`` is a different value per source; one class is wrong.

    OFX's is the institution's account number (the ``<ACCTID>`` element), so it
    is CRITICAL. Plaid's is the provider's own surrogate — the account-number
    material rides the separate ``mask`` column — and manual entry stores the
    canonical minted ``dim_accounts.account_id``, so both pass through the
    content net. Collapsing these to one declaration either masks two readable
    debugging keys or publishes an account number.
    """
    assert _class_of_key(("raw", "ofx_accounts", "account_id")) is (
        DataClass.INSTITUTION_ACCOUNT_NUMBER
    )
    assert _class_of_key(("raw", "plaid_accounts", "account_id")) is DataClass.FLOORED
    assert _class_of_key(("raw", "plaid_accounts", "mask")) is (
        DataClass.INSTITUTION_ACCOUNT_NUMBER
    )
    assert _class_of_key(("raw", "manual_transactions", "account_id")) is (
        DataClass.FLOORED
    )


def test_staging_source_account_key_carries_the_native_account_number() -> None:
    """``source_account_key`` is ``AS``-aliased from the source's ``account_id``.

    Every ``stg_*`` model re-projects the source-native key under this second
    name, so an enumeration that greps for account-shaped column NAMES misses
    it entirely while it holds exactly the same value.
    """
    assert _class_of_key(("prep", "stg_ofx__accounts", "source_account_key")) is (
        DataClass.INSTITUTION_ACCOUNT_NUMBER
    )


def test_staged_source_bytes_are_declared_not_floored() -> None:
    """The content net does not reach ``bytes`` — see ``_mask_floored``.

    ``raw.import_preview_snapshots.source_bytes`` holds a bank file verbatim
    (OFX carries ``<ACCTID>`` and ``<BANKID>`` in the clear), and FLOORED would
    return every byte of it untouched.
    """
    assert _class_of_key(("raw", "import_preview_snapshots", "source_bytes")) is (
        FAIL_CLOSED_CLASS
    )


def test_tabular_account_name_is_declared_like_its_slug() -> None:
    """``account_id`` is ``slugify()`` of ``account_name`` — one value, one class.

    A multi-account file's account column becomes ``raw_names``, and the import
    keeps BOTH: the slug lands in ``account_id`` and the unslugified original in
    ``account_name``. Declaring only the slug masks the derivative and publishes
    the source. The content net does not save it — a 7-digit number is under the
    8-digit run the net looks for, and a separator-formatted one contains no run
    at all, so either leaks whole.
    """
    for schema, table in (
        ("raw", "tabular_accounts"),
        ("prep", "stg_tabular__accounts"),
    ):
        assert _class_of_key((schema, table, "account_name")) is (
            DataClass.ACCOUNT_IDENTIFIER
        ), f"{schema}.{table}.account_name must match its account_id twin"


def test_import_log_account_names_masks_whole_not_partial() -> None:
    """The OFX importer writes ``<ACCTID>`` values into this column verbatim.

    Whole, not partial: a DuckDB ``JSON`` column arrives as ``str``, so
    ACCOUNT_IDENTIFIER's ``"****" + value[-4:]`` would publish the TAIL of the
    serialized array — which for a one-element array of a bare number is the
    tail of an account number.
    """
    assert _class_of_key(("raw", "import_log", "account_names")) is FAIL_CLOSED_CLASS
    assert mask_strength(FAIL_CLOSED_CLASS) is MaskStrength.WHOLE


# ---------------------------------------------------------------------------
# Parameter classing — both `$name` parse shapes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "shape",
    ["placeholder", "parameter"],
    ids=["bare-sqlglot", "sqlmesh-imported"],
)
def test_parameter_class_resolves_under_both_dollar_name_parse_shapes(
    populated_db: Database, shape: str
) -> None:
    """``$name`` parses to two different node types, and both must resolve.

    Bare sqlglot yields ``Placeholder(this="acct")``. Importing SQLMesh rewrites
    the tokenizer process-wide so the identical text yields
    ``Parameter(this=Var(this="acct"))`` — its macro-parameter syntax. MoneyBin
    imports SQLMesh on several paths, so which shape a process sees depends on
    import order.

    Matching one shape only is not merely a missed class: an unmatched
    placeholder resolves ``UNRESOLVED``, which changes the stored parameter
    classes, which moves the dynamic-report drift fingerprint — so match and
    mismatch would flip on import order alone. Both shapes are constructed here
    rather than inferred from whatever this process happens to have imported.
    """
    sql = "SELECT account_id FROM core.dim_accounts WHERE routing_number = $acct"
    snapshot = get_current_schema_snapshot(populated_db)
    tree = expand_star(parse_cached(sql), snapshot)

    target = next(
        node
        for node in tree.walk()
        if isinstance(node, (exp.Placeholder, exp.Parameter))
    )
    target.replace(
        exp.Placeholder(this="acct")
        if shape == "placeholder"
        else exp.Parameter(this=exp.Var(this="acct"))
    )

    assert resolve_placeholder_classes(tree, snapshot) == {
        "acct": DataClass.ROUTING_NUMBER
    }


# ---------------------------------------------------------------------------
# Case-insensitive table scoping, row-count placeholders, UNION provenance
# ---------------------------------------------------------------------------


def test_read_column_classes_resolves_a_table_named_in_any_case(
    populated_db: Database,
) -> None:
    """DuckDB matches identifiers without regard to case; the read set must too.

    ``tables_outside_schemas`` already case-folds, so ``FROM CORE.DIM_ACCOUNTS``
    passes every gate. If the read set does not fold too, the query resolves to
    no columns at all — which silently empties the dynamic-report drift
    fingerprint and its provenance list rather than raising anything.
    """
    snapshot = get_current_schema_snapshot(populated_db)
    lower = read_column_classes(
        parse_cached("SELECT account_id FROM core.dim_accounts"), snapshot
    )
    upper = read_column_classes(
        parse_cached("SELECT account_id FROM CORE.DIM_ACCOUNTS"), snapshot
    )

    assert lower, "expected the lowercase spelling to resolve columns"
    assert upper == lower


def test_a_row_count_placeholder_classes_as_an_aggregate(
    populated_db: Database,
) -> None:
    """``LIMIT $top`` binds a row count, which can echo no column's data.

    Every built-in declares its own ``LIMIT ?`` parameter ``AGGREGATE``. Leaving
    the saved-report path at ``UNRESOLVED`` would mean one report kind may carry
    a page-size default and the other may not, for the same value.
    """
    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached("SELECT account_id FROM core.dim_accounts LIMIT $top")

    assert resolve_placeholder_classes(tree, snapshot) == {"top": DataClass.AGGREGATE}


def test_a_filter_placeholder_still_takes_its_column_class(
    populated_db: Database,
) -> None:
    """The benign-input twin of the row-count case: a filter must not lower.

    A rule stated as "a placeholder outside a comparison is safe" would also
    admit ``WHERE routing_number = $acct``'s operand once someone widened it.
    """
    snapshot = get_current_schema_snapshot(populated_db)
    tree = expand_star(
        parse_cached(
            "SELECT account_id FROM core.dim_accounts "
            "WHERE routing_number = $acct LIMIT $top"
        ),
        snapshot,
    )

    assert resolve_placeholder_classes(tree, snapshot) == {
        "acct": DataClass.ROUTING_NUMBER,
        "top": DataClass.AGGREGATE,
    }


def test_a_placeholder_fails_closed_when_two_tables_bind_its_alias(
    populated_db: Database,
) -> None:
    """A reused alias must not resolve against whichever table sqlglot saw last.

    ``resolve_placeholder_classes`` needs one map for the whole tree — a
    placeholder can sit in any scope — and ``_build_alias_map`` is
    last-write-wins. Here both branches legally bind ``a``, to tables whose
    ``labels`` column carries different classes: the tree-wide map answered
    ``app.metrics.labels`` (AGGREGATE, LOW) for *both*, so the shared
    "used with two different classes" guard never fired and ``$tag`` came back
    LOW — which is the tier at which ``render_sql_forms`` splices a bound value
    into the published SQL as a literal. The real left-branch class is
    ``USER_NOTE``.
    """
    snapshot = get_current_schema_snapshot(populated_db)
    tree = expand_star(
        parse_cached(
            "SELECT a.labels AS v FROM app.imports a WHERE a.labels = $tag "
            "UNION ALL "
            "SELECT a.labels AS v FROM app.metrics a WHERE a.labels = $tag"
        ),
        snapshot,
    )

    assert resolve_placeholder_classes(tree, snapshot) == {"tag": FAIL_CLOSED_CLASS}


def test_projection_sources_withhold_an_upstream_two_tables_bind(
    populated_db: Database,
) -> None:
    """The provenance half of the same gap: no upstream beats a wrong one.

    ``resolve_output_classes`` gets this right by building its alias map per
    branch; ``resolve_projection_sources`` merges positionally and so needs one
    tree-wide map. It stays a passthrough — that is a fact about the projection
    text — but names no upstream, rather than pointing ``reports explain`` at a
    table the value never came from.
    """
    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached(
        "SELECT a.labels AS v FROM app.imports a "
        "UNION ALL SELECT a.labels AS v FROM app.metrics a"
    )

    assert resolve_projection_sources(tree, snapshot) == {
        "v": ProjectionSource(passthrough=True, upstream=None)
    }


def test_union_projection_sources_merge_by_position_not_by_alias(
    populated_db: Database,
) -> None:
    """One output position fed by two branches has one entry, keyed like classes.

    ``resolve_output_classes`` takes output names from the first branch and
    combines each *position* across branches. Merging sources by alias instead
    invents an output column per branch alias and reports the first branch's
    table as the sole upstream of a column every branch feeds.
    """
    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached(
        "SELECT account_id AS a FROM core.dim_accounts "
        "UNION ALL SELECT display_name AS b FROM core.dim_accounts"
    )

    sources = resolve_projection_sources(tree, snapshot)

    assert set(sources) == set(resolve_output_classes(tree, snapshot))
    assert sources["a"].passthrough is True
    assert sources["a"].upstream is None


def test_union_projection_sources_keep_an_agreed_upstream(
    populated_db: Database,
) -> None:
    """The benign twin: branches that agree still name their shared upstream.

    A positional merge that collapsed every set operation to ``upstream=None``
    would pass the test above while telling the user nothing.
    """
    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached(
        "SELECT account_id AS a FROM core.dim_accounts "
        "UNION ALL SELECT account_id AS b FROM core.dim_accounts"
    )

    sources = resolve_projection_sources(tree, snapshot)

    assert sources["a"].upstream == "core.dim_accounts.account_id"


@pytest.mark.parametrize(
    "classes",
    [
        [DataClass.FLOORED, DataClass.CATEGORY],
        [DataClass.CATEGORY, DataClass.FLOORED],
    ],
)
def test_floored_is_sticky_regardless_of_source_order(
    classes: list[DataClass],
) -> None:
    assert _combined_class(classes) is DataClass.FLOORED


@pytest.mark.parametrize(
    ("classes", "expected"),
    [
        ([DataClass.FLOORED, DataClass.DESCRIPTION], DataClass.FLOORED),
        ([DataClass.DESCRIPTION, DataClass.FLOORED], DataClass.FLOORED),
        ([DataClass.FLOORED, DataClass.TXN_AMOUNT], DataClass.FLOORED),
        (
            [DataClass.FLOORED, DataClass.ACCOUNT_IDENTIFIER],
            DataClass.ACCOUNT_IDENTIFIER,
        ),
        ([DataClass.FLOORED, DataClass.ROUTING_NUMBER], DataClass.ROUTING_NUMBER),
        (
            [DataClass.FLOORED, DataClass.ROUTING_NUMBER, DataClass.ROUTING_NUMBER],
            DataClass.ROUTING_NUMBER,
        ),
        (
            [
                DataClass.FLOORED,
                DataClass.ROUTING_NUMBER,
                DataClass.ACCOUNT_IDENTIFIER,
            ],
            FAIL_CLOSED_CLASS,
        ),
    ],
    ids=[
        "medium-passthrough",
        "medium-passthrough-reversed",
        "high-passthrough",
        "critical-partial-ties-on-strength",
        "critical-whole-outranks",
        "critical-unanimous",
        "critical-split",
    ],
)
def test_a_merged_position_keeps_the_strongest_mask(
    classes: list[DataClass], expected: DataClass
) -> None:
    """Strength decides first; tier only breaks a strength tie.

    The four ``medium``/``high`` cases used to expect the higher-tier class and
    were the defect written down: seven classes above ``Tier.LOW`` are
    ``_passthrough`` today, so a merge that read tier as a proxy for masking
    strength dropped ``FLOORED``'s content net whenever a position also drew
    from ``core``. ``SELECT description FROM prep.x UNION ALL SELECT description
    FROM core.fct_transactions`` returned the prep row's digit run verbatim
    under a ``DESCRIPTION`` label.

    The two CRITICAL cases keep the collapse unchanged. ``ACCOUNT_IDENTIFIER``
    ties ``FLOORED`` on strength (both PARTIAL) and wins on tier;
    ``ROUTING_NUMBER`` wins outright on strength (WHOLE).
    """
    assert _combined_class(classes) is expected


@pytest.mark.parametrize("other", list(DataClass), ids=lambda dc: dc.name.lower())
def test_a_merge_never_masks_more_weakly_than_its_inputs(other: DataClass) -> None:
    """The rule, derived over the registry — not a list of known counterexamples.

    Enumerating ``DataClass`` rather than naming the seven above-LOW passthrough
    classes is the point: the defect was a *category* error (tier read as a
    claim about masking), so the guard has to answer for a class nobody has
    written yet. A new above-LOW passthrough class, or a future transform that
    weakens an existing one, fails here rather than shipping another leak.

    Both orders, because a bare ``max`` returns the FIRST maximal element and
    every leak in this function's history was order-dependent.
    """
    floor = mask_strength(DataClass.FLOORED)
    for classes in ([DataClass.FLOORED, other], [other, DataClass.FLOORED]):
        merged = _combined_class(classes)
        assert mask_strength(merged) >= max(floor, mask_strength(other)), classes


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (a, b)
        for a in DataClass
        for b in DataClass
        if a.tier is Tier.CRITICAL and b.tier is Tier.CRITICAL and a is not b
    ],
    ids=lambda dc: dc.name.lower(),
)
def test_two_disagreeing_critical_classes_still_collapse(
    first: DataClass, second: DataClass
) -> None:
    """The pre-existing fail-closed collapse, pinned against the new merge key.

    Ranking by strength before tier means ``best`` is no longer guaranteed to be
    the highest-tier input: a future non-CRITICAL class that masks WHOLE would
    outrank a PARTIAL CRITICAL one and route past the ``best.tier is
    Tier.CRITICAL`` branch, silently retiring this collapse. Deriving the pairs
    from the registry makes that change red instead of quiet.
    """
    assert _combined_class([first, second]) is FAIL_CLOSED_CLASS


# ---------------------------------------------------------------------------
# Fix round 1: FLOORED must also survive the conservative-fallback
# accumulators (_scope_input_max, _table_scope_max, _conservative_floor).
# Each seeds `best = DataClass.AGGREGATE` (LOW) and previously advanced only
# on strict `dc.tier > best.tier` — since FLOORED is also LOW, that strict
# comparison can never select it, deterministically, regardless of source
# order. This is a from-scratch bug (not an ordering tie), distinct from
# `_combined_class`'s own stickiness fix above.
# ---------------------------------------------------------------------------


def test_scope_input_max_carries_floored(
    populated_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FLOORED must survive _scope_input_max's own LOW-tier accumulation."""
    import moneybin.privacy.sql_lineage as lin

    snapshot = get_current_schema_snapshot(populated_db)
    select = parse_cached("SELECT dim_categories.class FROM core.dim_categories").find(
        exp.Select
    )
    assert select is not None

    def always_floored(key: tuple[str, str, str]) -> DataClass:  # noqa: ARG001
        return DataClass.FLOORED

    monkeypatch.setattr(lin, "_class_of_key", always_floored)

    assert _scope_input_max(select, snapshot, "") is DataClass.FLOORED


def test_table_scope_max_carries_floored(
    populated_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FLOORED must survive _table_scope_max's own LOW-tier accumulation."""
    import moneybin.privacy.sql_lineage as lin

    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached("SELECT class FROM core.dim_categories")

    def always_floored(key: tuple[str, str, str]) -> DataClass:  # noqa: ARG001
        return DataClass.FLOORED

    monkeypatch.setattr(lin, "_class_of_key", always_floored)

    assert _table_scope_max(tree, snapshot, "") is DataClass.FLOORED


def test_conservative_floor_merge_carries_floored(
    populated_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_conservative_floor's own per-select loop must not drop a FLOORED scope max.

    Mocks its two callees directly (rather than ``_class_of_key``) so this
    isolates ``_conservative_floor``'s own accumulation from whether
    ``_scope_input_max`` / ``_table_scope_max`` are themselves correct — a
    ``_class_of_key`` patch would let ``_table_scope_max``'s own (separately
    tested) fix silently rescue a still-broken loop here, since it walks
    every column of the same table and would also see FLOORED.

    This pins the per-select loop (:658) specifically: ``_scope_input_max``
    returns FLOORED here, so ``best`` is already FLOORED by the time the
    ``table_dc`` merge (:660) runs, and that merge cannot discriminate a
    fixed implementation from a strict-`>` one on this fixture — either way
    it returns the ``best`` it was handed. See
    ``test_conservative_floor_table_dc_merge_carries_floored`` for the
    sibling fixture that isolates :660 instead.
    """
    import moneybin.privacy.sql_lineage as lin

    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached("SELECT class FROM core.dim_categories")

    def fake_scope_input_max(
        select: object, snapshot: object, sql_for_log: object
    ) -> DataClass:
        return DataClass.FLOORED

    def fake_table_scope_max(
        tree: object, snapshot: object, sql_for_log: object
    ) -> DataClass:
        return DataClass.AGGREGATE

    monkeypatch.setattr(lin, "_scope_input_max", fake_scope_input_max)
    monkeypatch.setattr(lin, "_table_scope_max", fake_table_scope_max)

    assert _conservative_floor(tree, snapshot, "") is DataClass.FLOORED


def test_table_scope_max_high_control_is_unaffected(populated_db: Database) -> None:
    """Above-LOW behaviour must stay identical: a lone HIGH class still wins."""
    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached("SELECT account_id FROM core.fct_balances")

    assert _table_scope_max(tree, snapshot, "") is DataClass.BALANCE


def test_table_scope_max_critical_control_is_unaffected(populated_db: Database) -> None:
    """Above-LOW behaviour must stay identical: disagreeing CRITICAL still fails closed."""
    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached("SELECT account_id FROM core.dim_accounts")

    assert _table_scope_max(tree, snapshot, "") is FAIL_CLOSED_CLASS


def test_conservative_floor_table_dc_merge_carries_floored(
    populated_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_conservative_floor's table_dc merge (:660) must not drop a FLOORED table max.

    Mirror of ``test_conservative_floor_merge_carries_floored`` with the two
    callees' roles swapped: ``_scope_input_max`` returns AGGREGATE, so the
    per-select loop's ``best`` going into the merge is AGGREGATE, not
    FLOORED. Only the ``floor = _combined_class([best, table_dc])`` merge
    itself can then carry FLOORED into the result — the sibling test above
    can't pin that merge, because there ``best`` is already FLOORED before
    the merge runs (a strict-`>` pre-fix merge returns `best` unchanged on a
    LOW-tier tie, so it stays green either way). This fixture fails on a
    strict-`>` merge and only passes when the merge itself is FLOORED-sticky.
    """
    import moneybin.privacy.sql_lineage as lin

    snapshot = get_current_schema_snapshot(populated_db)
    tree = parse_cached("SELECT class FROM core.dim_categories")

    def fake_scope_input_max(
        select: object, snapshot: object, sql_for_log: object
    ) -> DataClass:
        return DataClass.AGGREGATE

    def fake_table_scope_max(
        tree: object, snapshot: object, sql_for_log: object
    ) -> DataClass:
        return DataClass.FLOORED

    monkeypatch.setattr(lin, "_scope_input_max", fake_scope_input_max)
    monkeypatch.setattr(lin, "_table_scope_max", fake_table_scope_max)

    assert _conservative_floor(tree, snapshot, "") is DataClass.FLOORED
