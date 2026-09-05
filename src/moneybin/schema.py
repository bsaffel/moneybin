"""Database schema initialization.

Creates all schemas and tables required by MoneyBin. Every DDL statement
uses ``CREATE … IF NOT EXISTS`` so the function is idempotent and safe to
call on every startup.

Table and column comments are written as inline SQL comments in each schema
file and applied to DuckDB's catalog after each file executes. sqlglot parses
the SQL and extracts comments from the AST — the same mechanism SQLMesh uses
internally via ``register_comments`` for its own model files.

Table comments
--------------
A ``/* description */`` block comment on the line immediately before
``CREATE TABLE`` is attached by sqlglot to the ``Create`` expression and
applied as ``COMMENT ON TABLE``.

Column comments
---------------
A trailing ``-- text`` on a column definition line is attached by sqlglot to
the ``ColumnDef`` expression and applied as ``COMMENT ON COLUMN``.
"""

import logging
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

import duckdb
import sqlglot
import sqlglot.expressions as exp

from moneybin.database import escape_sql_literal
from moneybin.privacy.comment_sync import sync_classification_comments
from moneybin.privacy.taxonomy import strip_sigil

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).resolve().parent / "sql" / "schema"

# Provider-bundled DDL directories. Listed here as plain paths rather than
# imported via ``Provider.schema_files()`` to avoid triggering each
# provider package's lazy-import machinery — the extractor classes pull in
# polars and other heavy deps, and ``init_schemas`` only needs to discover
# `.sql` files. The provider package owns the contents of its schema
# directory; this list owns only the location.
_EXTRACTORS_DIR = Path(__file__).resolve().parent / "extractors"
_PROVIDER_SCHEMA_DIRS: list[Path] = [
    _EXTRACTORS_DIR / "ofx" / "schema",
    _EXTRACTORS_DIR / "plaid" / "schema",
    _EXTRACTORS_DIR / "tabular" / "schema",
]


@dataclass(frozen=True)
class _CommentPlan:
    """The catalog comments derived from one schema DDL file."""

    schema_name: str
    table_name: str
    table_sql: str
    table_comment: str | None
    column_comments: tuple[tuple[str, str], ...]


# Schema DDL is static for a given installed build, while init_schemas runs on
# each write open. Keep its parse result until the source file's mtime changes.
_COMMENT_PLAN_CACHE: dict[tuple[Path, int, str], tuple[_CommentPlan, ...]] = {}


# Cross-cutting (non-provider-owned) DDL files resolved against ``_SQL_DIR``.
# Order matters where dependencies exist: schema-create statements
# (``raw_schema.sql``, etc.) must run before any table DDL inside that
# schema. Tables within a schema have no ordering dependency on each other.
_NON_PROVIDER_SCHEMA_FILES: list[str] = [
    "raw_schema.sql",
    "core_schema.sql",
    "app_schema.sql",
    "analytics_schema.sql",
    "meta_schema.sql",
    "reports_schema.sql",
    "raw_import_log.sql",
    "raw_manual_transactions.sql",
    "app_categories.sql",
    "app_category_source_map.sql",
    "app_user_merchants.sql",
    "app_categorization_rules.sql",
    "app_transaction_categories.sql",
    "app_categorization_decisions.sql",
    "app_budgets.sql",
    "app_transaction_notes.sql",
    "app_metrics.sql",
    "app_schema_migrations.sql",
    "app_versions.sql",
    "app_tabular_formats.sql",
    "app_match_decisions.sql",
    "app_account_links.sql",
    "app_account_link_decisions.sql",
    "app_merchant_links.sql",
    "app_merchant_link_decisions.sql",
    "app_transaction_id_aliases.sql",
    "app_seed_source_priority.sql",
    "app_proposed_rules.sql",
    "app_rule_conflicts.sql",
    "app_account_settings.sql",
    "app_profile_settings.sql",
    "app_balance_assertions.sql",
    "app_audit_log.sql",
    "app_transaction_tags.sql",
    "app_transaction_splits.sql",
    "app_imports.sql",
    "app_import_previews.sql",
    "raw_import_preview_snapshots.sql",
    "app_gsheet_connections.sql",
    "app_export_destinations.sql",
    "app_user_reports.sql",
    "app_ai_consent_grants.sql",
    "raw_gsheet_seeds.sql",
    "raw_pdf_seeds.sql",
    "app_pdf_formats.sql",
    "app_securities.sql",
    "raw_manual_investment_transactions.sql",
    "app_lot_selections.sql",
    "app_security_links.sql",
    "app_security_link_decisions.sql",
    "raw_security_prices.sql",
    "app_security_price_overrides.sql",
    "raw_exchange_rates.sql",
    "app_exchange_rate_overrides.sql",
]


def _all_schema_files() -> list[Path]:
    """Enumerate every DDL file: cross-cutting plus provider-bundled.

    Schema-creation statements (cross-cutting) run first so provider
    tables can reference ``raw.*`` / ``app.*`` / etc. Each provider
    directory contributes every ``raw_*.sql`` it owns, discovered by
    glob. The glob is intentionally permissive within a provider's
    directory because the directory itself is the ownership boundary —
    each provider owns the entire contents of its ``schema/`` dir.
    By convention, provider files follow ``raw_<provider>_<entity>.sql``
    naming (matching the per-provider ``schema_files()`` contract); the
    permissive glob avoids re-declaring the prefix in two places.
    """
    files: list[Path] = [_SQL_DIR / name for name in _NON_PROVIDER_SCHEMA_FILES]
    for schema_dir in _PROVIDER_SCHEMA_DIRS:
        files.extend(sorted(schema_dir.glob("raw_*.sql")))
    return files


def _derive_comment_plan(sql: str) -> tuple[_CommentPlan, ...]:
    """Parse schema DDL into the comments to apply to each catalog."""
    plans: list[_CommentPlan] = []
    for statement in sqlglot.parse(sql, dialect="duckdb"):
        if not isinstance(statement, exp.Create) or statement.kind != "TABLE":
            continue

        table = statement.find(exp.Table)
        if table is None:
            continue
        schema_name = table.args["db"].name if table.args.get("db") else None
        if schema_name is None:
            continue

        table_comment = statement.comments[-1].strip() if statement.comments else None
        column_comments = tuple(
            (col_def.name, col_def.comments[-1].strip())
            for col_def in statement.find_all(exp.ColumnDef)
            if col_def.comments and col_def.comments[-1].strip()
        )
        plans.append(
            _CommentPlan(
                schema_name=schema_name,
                table_name=table.name,
                table_sql=table.sql(dialect="duckdb"),
                table_comment=table_comment,
                column_comments=column_comments,
            )
        )
    return tuple(plans)


def _comment_plan(sql_path: Path, sql: str | None = None) -> tuple[_CommentPlan, ...]:
    """Return a parsed comment plan, refreshing it when the DDL changes."""
    sql = sql if sql is not None else sql_path.read_text()
    cache_key = (
        sql_path,
        sql_path.stat().st_mtime_ns,
        sha256(sql.encode()).hexdigest(),
    )
    cached = _COMMENT_PLAN_CACHE.get(cache_key)
    if cached is not None:
        return cached

    plan = _derive_comment_plan(sql)
    _COMMENT_PLAN_CACHE[cache_key] = plan
    return plan


def _apply_comments(
    conn: duckdb.DuckDBPyConnection,
    comment_plan: tuple[_CommentPlan, ...],
    table_snapshot: dict[tuple[str, str], str | None],
    column_snapshot: dict[tuple[str, str, str], str | None],
) -> None:
    """Apply a pre-derived table and column comment plan to DuckDB's catalog.

    The plan preserves comments sqlglot attached to adjacent AST nodes:

    - A ``/* description */`` block comment immediately before ``CREATE TABLE``
      is attached to the ``Create`` expression and applied as
      ``COMMENT ON TABLE``.
    - A trailing ``-- text`` on a column definition line is attached to the
      ``ColumnDef`` expression and applied as ``COMMENT ON COLUMN``.

    This is the same mechanism SQLMesh uses internally for its own models.
    Tables that do not exist yet (e.g. core tables before SQLMesh has run) are
    silently skipped.

    The snapshots are pre-loop catalog reads (``duckdb_tables()`` /
    ``duckdb_columns()``) so the comparison is a dict lookup, not a
    per-column ``SELECT``. A row whose human-prefix already matches the
    DDL comment is skipped so the privacy sigil suffix written by
    ``sync_classification_comments`` survives across startups.
    """
    for table_plan in comment_plan:
        # Table-level comment: /* description */ on the line before CREATE TABLE.
        if table_plan.table_comment:
            description = table_plan.table_comment
            existing = table_snapshot.get((
                table_plan.schema_name,
                table_plan.table_name,
            ))
            if description and strip_sigil(existing or "") != description:
                try:
                    safe_desc = escape_sql_literal(description)
                    conn.execute(
                        f"COMMENT ON TABLE {table_plan.table_sql} IS '{safe_desc}'"
                    )
                    logger.debug(f"Applied table comment to {table_plan.table_sql}")
                except duckdb.CatalogException:
                    logger.debug(
                        f"Skipping table comment for {table_plan.table_sql} — table does not exist yet"
                    )

        # Column-level comments: trailing -- text on each column definition
        for column_name, comment in table_plan.column_comments:
            existing = column_snapshot.get((
                table_plan.schema_name,
                table_plan.table_name,
                column_name,
            ))
            if strip_sigil(existing or "") == comment:
                continue
            try:
                safe_comment = escape_sql_literal(comment)
                conn.execute(
                    f"COMMENT ON COLUMN {table_plan.table_sql}.{column_name} IS "
                    f"'{safe_comment}'"
                )
                logger.debug(
                    f"Applied column comment to {table_plan.table_sql}.{column_name}"
                )
            except (duckdb.CatalogException, duckdb.BinderException):
                # Column may not exist yet — either the table is created later
                # (e.g. SQLMesh-managed core tables) or a pending migration will
                # add the column. Comments will reapply on the next startup.
                logger.debug(
                    f"Skipping column comment for {table_plan.table_sql}.{column_name}"
                    " — column or table does not exist yet"
                )


def _snapshot_catalog_comments(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[
    dict[tuple[str, str], str | None],
    dict[tuple[str, str, str], str | None],
]:
    """Read every table and column comment in one pair of queries."""
    table_rows = conn.execute(
        "SELECT schema_name, table_name, comment FROM duckdb_tables()"
    ).fetchall()
    column_rows = conn.execute(
        "SELECT schema_name, table_name, column_name, comment FROM duckdb_columns()"
    ).fetchall()
    table_snapshot: dict[tuple[str, str], str | None] = {
        (s, t): c for s, t, c in table_rows
    }
    column_snapshot: dict[tuple[str, str, str], str | None] = {
        (s, t, col): c for s, t, col, c in column_rows
    }
    return table_snapshot, column_snapshot


def init_schemas(
    conn: duckdb.DuckDBPyConnection,
    additional_files: list[Path] | None = None,
    package_root: Path | None = None,
) -> None:
    """Create all database schemas and tables, then apply inline comments.

    Args:
        conn: An active read-write DuckDB connection.
        additional_files: Optional extra SQL DDL paths (e.g. from registered
            analysis packages). Executed AFTER the core schema files so
            package tables can reference core/app primitives.
        package_root: when provided, every additional_files path must resolve
            inside this directory or a ValueError is raised. The Plan 4 wiring
            that passes package SQL supplies the owning package's root so a
            manifest cannot point init_schemas at out-of-tree SQL
            (boundary path-traversal guard per .claude/rules/security.md).

    Raises:
        ValueError: an additional_files path escapes package_root (when given).
    """
    extras = additional_files or []
    if extras and package_root is None:
        # Refuse rather than silently skip the path-traversal guard. The only
        # intended caller (Plan 4 package-schema wiring) always has info.root,
        # so a missing package_root signals a wiring bug, not a valid call.
        raise ValueError(
            "init_schemas: additional_files requires package_root so each path "
            "can be confined to the package directory (pass package_root=info.root)"
        )
    if package_root is not None:
        root = package_root.resolve()
        for sql_path in extras:
            if not sql_path.resolve().is_relative_to(root):
                raise ValueError(
                    f"additional_files path {sql_path} is outside package root {root}"
                )
    table_snapshot, column_snapshot = _snapshot_catalog_comments(conn)
    schema_files = _all_schema_files()
    executed = 0
    for sql_path in [*schema_files, *extras]:
        if not sql_path.exists():
            logger.warning(f"Schema file not found, skipping: {sql_path.name}")
            continue
        sql = sql_path.read_text()
        conn.execute(sql)
        executed += 1
        _apply_comments(
            conn, _comment_plan(sql_path, sql), table_snapshot, column_snapshot
        )
        logger.debug(f"Executed {sql_path.name}")

    logger.debug(f"Executed {executed} schema files")

    # Mirror the DataClass registry into the catalog (suffix comments
    # with `[class: ...]`).
    try:
        sync_classification_comments(conn)
    except duckdb.CatalogException:
        # Core tables managed by SQLMesh may not exist yet on a fresh
        # DB — they appear after the first `sqlmesh run`. The sync
        # runs again from sqlmesh_context() once those tables land.
        logger.debug("Skipping classification sync — core tables not yet present")
