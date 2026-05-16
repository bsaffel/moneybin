"""Tests for V012: drop app.merchant_overrides and seeds.merchants_*.

V012 retires the seed merchant catalog by dropping `app.merchant_overrides`
and any leftover `seeds.merchants_global/us/ca` tables. Fresh installs never
see these tables because `schema.py` and `sqlmesh/models/seeds/` no longer
declare them; existing installs get the drops on the next migration run.

See `docs/specs/categorization-cold-start.md` amendment 2026-05-15.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V012__drop_merchant_overrides import migrate


def _table_exists(db: Database, schema: str, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _recreate_retired_tables(db: Database) -> None:
    """Reverse the V012 end-state — recreate the dropped tables.

    On a fresh DB `init_schemas` no longer creates these, so to exercise the
    migration we put them back in their pre-retirement shape (the same DDL
    the deleted schema files and SQLMesh seed models produced).
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
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
    merchant_seed_ddl = """(
            merchant_id VARCHAR PRIMARY KEY,
            raw_pattern VARCHAR,
            match_type VARCHAR,
            canonical_name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            country VARCHAR
        )"""
    for table in ("merchants_global", "merchants_us", "merchants_ca"):
        db.execute(
            f"CREATE TABLE IF NOT EXISTS seeds.{table} {merchant_seed_ddl}"  # noqa: S608  # allowlisted literals, not user input
        )


class TestV012Migration:
    """V012 migration: drop retired seed-merchant tables. Idempotent."""

    def test_v012_drops_merchant_overrides_when_present(self, db: Database) -> None:
        """app.merchant_overrides must be removed after migration."""
        _recreate_retired_tables(db)
        assert _table_exists(db, "app", "merchant_overrides")

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        assert not _table_exists(db, "app", "merchant_overrides")

    def test_v012_drops_seed_merchant_tables_when_present(self, db: Database) -> None:
        """All three seeds.merchants_* tables must be removed after migration."""
        _recreate_retired_tables(db)
        for table in ("merchants_global", "merchants_us", "merchants_ca"):
            assert _table_exists(db, "seeds", table), f"setup failed for seeds.{table}"

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        for table in ("merchants_global", "merchants_us", "merchants_ca"):
            assert not _table_exists(db, "seeds", table)

    def test_v012_idempotent_on_fresh_install(self, db: Database) -> None:
        """On a fresh DB where the retired tables never existed, migrate() is a no-op."""
        for table in (
            "merchant_overrides",
            # The fresh `db` fixture has no `seeds.merchants_*` either.
        ):
            assert not _table_exists(db, "app", table)

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        assert not _table_exists(db, "app", "merchant_overrides")

    def test_v012_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate() leaves the same end-state — all retired tables gone."""
        _recreate_retired_tables(db)

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        assert not _table_exists(db, "app", "merchant_overrides")
        for table in ("merchants_global", "merchants_us", "merchants_ca"):
            assert not _table_exists(db, "seeds", table)

    def test_v012_rewrites_legacy_seed_categorizations(self, db: Database) -> None:
        """Historical `categorized_by='seed'` rows must be rewritten to 'rule'.

        Without this, the post-migration `_SOURCE_PRIORITY` CASE returns NULL
        for these rows and every subsequent precedence check (including user
        writes) silently fails because `priority <= NULL` is NULL.
        """
        # Bypass the service-layer Literal check; this is the pre-migration shape.
        # No FK enforcement against core.fct_transactions (which is a view), so a
        # bare insert into app.transaction_categories is sufficient.
        db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, subcategory, categorized_at, categorized_by) "
            "VALUES ('t_legacy_seed', 'Food & Dining', 'Coffee Shops', "
            "CURRENT_TIMESTAMP, 'seed')"
        )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        row = db.execute(
            "SELECT categorized_by FROM app.transaction_categories "
            "WHERE transaction_id = 't_legacy_seed'"
        ).fetchone()
        assert row is not None
        assert row[0] == "rule"

    def test_v012_seed_rewrite_is_noop_when_absent(self, db: Database) -> None:
        """No legacy 'seed' rows → UPDATE is a clean no-op; migration completes."""
        # transaction_categories starts empty; no rewrite needed.
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        count = db.execute("SELECT COUNT(*) FROM app.transaction_categories").fetchone()
        assert count is not None
        assert count[0] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
