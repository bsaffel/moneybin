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
    """The metadata path skipped the schema gate too — raw.* became readable.

    The masking bypass was only half the damage: metadata queries never reach
    ``tables_outside_schemas``, so ``SELECT ... FROM raw.x EXCEPT ...`` returned
    unclassified raw-schema rows that a plain SELECT refuses outright.
    """
    with pytest.raises(UserError) as ei:
        execute_sql_query(
            populated_db,
            f"SELECT account_id FROM raw.ofx_transactions {op} SELECT 'x'",  # noqa: S608  # test input string, not executing SQL
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


def test_reports_schema_is_queryable() -> None:
    assert "reports" in _ALLOWED_QUERY_SCHEMAS
    # core/app still allowed; internal schemas still fenced in Phase 1.
    assert {"core", "app"} <= _ALLOWED_QUERY_SCHEMAS
    assert "meta" not in _ALLOWED_QUERY_SCHEMAS


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
    the scenario-level `test_declared_classes_match_derivation`.
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

    Regenerate with: uv run python scripts/generate_derived_report_classes.py
    """
    from moneybin.privacy.report_class_derivation import derive_report_classes
    from moneybin.reports._framework.registry import spec_of
    from moneybin.reports.definitions import ALL_REPORTS
    from moneybin.reports.definitions._derived_classes import (
        DERIVED_REPORT_CLASSES,
    )

    derived = derive_report_classes()
    runner_keys = {(spec_of(r).view.schema, spec_of(r).view.name) for r in ALL_REPORTS}
    expected = {key: cols for key, cols in derived.items() if key not in runner_keys}
    assert DERIVED_REPORT_CLASSES == expected, (
        "Regenerate with: uv run python scripts/generate_derived_report_classes.py"
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
