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
    in the clear, whichever branch of ``_classes_by_result_column`` claims it.
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
    runner_keys = {(spec_of(r).view.schema, spec_of(r).view.name) for r in ALL_REPORTS}
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

    ``_classes_by_result_column`` used to build its map with
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
        # zero times (see _classes_by_result_column).
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
        # Below CRITICAL every transform is passthrough, so the class is pure
        # reporting and the merge must still report the max — unchanged by this
        # fix. A collapse that reached below CRITICAL would both mask these
        # values and inflate their tier.
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
    """Ties below CRITICAL keep the existing max-by-tier answer, unmasked."""
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
