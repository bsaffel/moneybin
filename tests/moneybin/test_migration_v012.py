"""Tests for V012: drop app.merchant_overrides and rewrite legacy 'seed' rows.

V012 retires the seed merchant catalog. It drops the migration-owned
`app.merchant_overrides` table and rewrites historical
`categorized_by='seed'` rows to `'rule'`. It intentionally does NOT drop the
`seeds.merchants_global/us/ca` tables: those were SQLMesh SEED models, so on a
materialized database they are *views*, and `DROP TABLE IF EXISTS` on a view
raises `CatalogException` (the V032 / PR #306 bug class). SQLMesh owns their
teardown. `test_v012_tolerates_seeds_merchants_as_views` is the regression
guard for that crash.

See `docs/specs/categorization-cold-start.md` amendment 2026-05-15.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V012__drop_merchant_overrides import migrate

pytestmark = pytest.mark.fresh_db


def _table_exists(db: Database, schema: str, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _create_merchant_overrides(db: Database) -> None:
    """Recreate the retired app.merchant_overrides in its pre-retirement shape.

    On a fresh DB `init_schemas` no longer creates it, so to exercise the drop we
    put it back (the same DDL the deleted schema file produced).
    """
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS app.merchant_overrides (
            merchant_id VARCHAR PRIMARY KEY,
            is_active BOOLEAN NOT NULL,
            category VARCHAR,
            subcategory VARCHAR,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _create_seed_merchants_view(db: Database, table: str) -> None:
    """Create seeds.<table> as a VIEW — the shape it has on a materialized DB.

    SQLMesh exposes SEED models as views over a physical snapshot. This
    reproduces that so the test proves V012 no longer trips over it (a plain
    `DROP TABLE IF EXISTS` on a view raises CatalogException).
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
    db.execute(f"CREATE TABLE IF NOT EXISTS seeds.{table}__phys (merchant_id VARCHAR)")  # noqa: S608  # allowlisted literal
    db.execute(f"CREATE VIEW seeds.{table} AS SELECT * FROM seeds.{table}__phys")  # noqa: S608  # allowlisted literal


class TestV012Migration:
    """V012 migration: drop app.merchant_overrides, leave seeds.* to SQLMesh."""

    def test_v012_drops_merchant_overrides_when_present(self, db: Database) -> None:
        """app.merchant_overrides must be removed after migration."""
        _create_merchant_overrides(db)
        assert _table_exists(db, "app", "merchant_overrides")

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        assert not _table_exists(db, "app", "merchant_overrides")

    def test_v012_tolerates_seeds_merchants_as_views(self, db: Database) -> None:
        """When seeds.merchants_* are views (materialized DB), V012 must not crash.

        Regression for the V032-class bug: the shipped V012 ran
        `DROP TABLE IF EXISTS seeds.merchants_global` over what is a *view* on a
        materialized database, which raises CatalogException. V012 now leaves the
        seed relations to SQLMesh, so the views survive untouched and only
        app.merchant_overrides is dropped.
        """
        _create_merchant_overrides(db)
        for table in ("merchants_global", "merchants_us", "merchants_ca"):
            _create_seed_merchants_view(db, table)

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]  # must not raise

        assert not _table_exists(db, "app", "merchant_overrides")
        for table in ("merchants_global", "merchants_us", "merchants_ca"):
            assert _table_exists(db, "seeds", table), (
                f"V012 must leave the SQLMesh-owned seeds.{table} view untouched"
            )

    def test_v012_idempotent_on_fresh_install(self, db: Database) -> None:
        """On a fresh DB where the retired table never existed, migrate() is a no-op."""
        assert not _table_exists(db, "app", "merchant_overrides")

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        assert not _table_exists(db, "app", "merchant_overrides")

    def test_v012_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate() leaves the same end-state — app.merchant_overrides gone."""
        _create_merchant_overrides(db)

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        assert not _table_exists(db, "app", "merchant_overrides")

    def test_v012_rewrites_legacy_seed_categorizations(self, db: Database) -> None:
        """Historical `categorized_by='seed'` rows must be rewritten to 'rule'.

        Without this, the post-migration `_SOURCE_PRIORITY` CASE returns NULL
        for these rows and every subsequent precedence check (including user
        writes) silently fails because `priority <= NULL` is NULL.

        Populates >=3 'seed' rows alongside non-seed rows to verify selectivity:
        only 'seed' is rewritten, other categorized_by values survive.
        """
        # Bypass the service-layer Literal check; this is the pre-migration shape.
        # No FK enforcement against core.fct_transactions (which is a view), so a
        # bare insert into app.transaction_categories is sufficient.
        seed_rows = [
            ("t_legacy_seed_1", "Food & Dining", "Coffee Shops", "seed"),
            ("t_legacy_seed_2", "Transport", "Rideshare", "seed"),
            ("t_legacy_seed_3", "Shopping", "General Merchandise", "seed"),
        ]
        non_seed_rows = [
            ("t_user_1", "Food & Dining", "Restaurants", "user"),
            ("t_rule_1", "Bills & Utilities", "Internet", "rule"),
        ]
        for transaction_id, category, subcategory, categorized_by in (
            seed_rows + non_seed_rows
        ):
            db.execute(
                "INSERT INTO app.transaction_categories "
                "(transaction_id, category, subcategory, categorized_at, categorized_by) "
                "VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)",
                [transaction_id, category, subcategory, categorized_by],
            )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        for transaction_id, _category, _sub, _by in seed_rows:
            row = db.execute(
                "SELECT categorized_by FROM app.transaction_categories "
                "WHERE transaction_id = ?",
                [transaction_id],
            ).fetchone()
            assert row is not None
            assert row[0] == "rule"

        for transaction_id, _category, _sub, original_by in non_seed_rows:
            row = db.execute(
                "SELECT categorized_by FROM app.transaction_categories "
                "WHERE transaction_id = ?",
                [transaction_id],
            ).fetchone()
            assert row is not None
            assert row[0] == original_by

    def test_v012_seed_rewrite_is_noop_when_absent(self, db: Database) -> None:
        """No legacy 'seed' rows → UPDATE is a clean no-op; migration completes."""
        # transaction_categories starts empty; no rewrite needed.
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        count = db.execute("SELECT COUNT(*) FROM app.transaction_categories").fetchone()
        assert count is not None
        assert count[0] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
