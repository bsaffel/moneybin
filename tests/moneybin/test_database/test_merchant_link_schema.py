"""Schema-initialization tests for the M1T merchant-entity-id tables.

Mirrors test_account_identity_schema.py: read-only tests against the
initialized schema use the module-scoped ``module_db`` fixture. Covers both
new tables (merchant_links, merchant_link_decisions), their columns, and the
TableRef constants. The migration class uses the function-scoped ``db``
fixture with ``fresh_db`` so it can mutate without contaminating the
read-only module.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.tables import MERCHANT_LINK_DECISIONS, MERCHANT_LINKS


class TestMerchantLinkTableRefs:
    """TableRef constants for the two merchant-link tables."""

    def test_table_ref_full_names(self) -> None:
        assert MERCHANT_LINKS.full_name == "app.merchant_links"
        assert MERCHANT_LINK_DECISIONS.full_name == "app.merchant_link_decisions"


class TestMerchantLinkSchema:
    """app.merchant_links and app.merchant_link_decisions — column coverage."""

    def test_merchant_link_tables_exist_on_fresh_db(self, module_db: Database) -> None:
        """init_schemas creates both merchant-link tables with the expected columns."""
        links_cols = {
            r[0]
            for r in module_db.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE schema_name='app' AND table_name='merchant_links'"
            ).fetchall()
        }
        dec_cols = {
            r[0]
            for r in module_db.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE schema_name='app' AND table_name='merchant_link_decisions'"
            ).fetchall()
        }
        assert {
            "link_id",
            "merchant_id",
            "ref_kind",
            "ref_value",
            "source_type",
            "status",
            "decided_by",
            "decided_at",
            "reversed_at",
            "reversed_by",
        } <= links_cols
        assert {
            "decision_id",
            "ref_kind",
            "ref_value",
            "source_type",
            "provider_merchant_name",
            "candidate_merchant_id",
            "confidence_score",
            "match_signals",
            "status",
            "decided_by",
            "match_reason",
            "decided_at",
            "reversed_at",
            "reversed_by",
        } <= dec_cols


@pytest.mark.fresh_db
class TestV031MerchantLinkMigration:
    """V031 catch-up migration creates merchant link tables on pre-existing DBs."""

    def test_migration_creates_merchant_link_tables(self, db: Database) -> None:
        """V031 creates both tables on a DB that pre-dates the migration."""
        from moneybin.sql.migrations.V031__create_merchant_link_tables import migrate
        from tests.moneybin.migration_helpers import column_exists, run_migration

        # Simulate pre-migration state: drop both tables (init_schemas already ran).
        db.execute("DROP TABLE IF EXISTS app.merchant_links")
        db.execute("DROP TABLE IF EXISTS app.merchant_link_decisions")

        assert not column_exists(db, "app", "merchant_links", "link_id")
        assert not column_exists(db, "app", "merchant_link_decisions", "decision_id")

        run_migration(db, migrate)

        assert column_exists(db, "app", "merchant_links", "link_id")
        assert column_exists(db, "app", "merchant_links", "merchant_id")
        assert column_exists(db, "app", "merchant_link_decisions", "decision_id")
        assert column_exists(
            db, "app", "merchant_link_decisions", "candidate_merchant_id"
        )
