"""Schema-initialization tests for the M1J.1 investment tables.

Covers the three new tables (app.securities,
raw.manual_investment_transactions, app.lot_selections) and the
app.account_settings cost-basis-method column. Uses the function-scoped
``db`` fixture (not ``module_db``) because the CHECK-constraint probes
mutate — module_db is reserved for exclusively read-only modules.
"""

import pytest

from moneybin.database import Database

pytestmark = pytest.mark.unit


def _columns(db: Database, schema: str, table: str) -> set[str]:
    rows = db.execute(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE schema_name = ? AND table_name = ?",
        [schema, table],
    ).fetchall()
    return {r[0] for r in rows}


class TestInvestmentTables:
    """Column shapes and CHECK constraints for the M1J.1 investment tables."""

    def test_app_securities_columns(self, db: Database) -> None:
        cols = _columns(db, "app", "securities")
        assert {
            "security_id",
            "name",
            "security_type",
            "ticker",
            "exchange",
            "cusip",
            "isin",
            "figi",
            "coingecko_id",
            "is_cash_equivalent",
            "cost_basis_method",
            "currency_code",
            "created_at",
            "updated_at",
        } <= cols

    def test_raw_manual_investment_transactions_columns(self, db: Database) -> None:
        cols = _columns(db, "raw", "manual_investment_transactions")
        assert {
            "source_transaction_id",
            "source_type",
            "source_origin",
            "import_id",
            "account_id",
            "security_id",
            "security_ref",
            "type",
            "subtype",
            "event_group_id",
            "trade_date",
            "settlement_date",
            "original_acquisition_date",
            "quantity",
            "price",
            "amount",
            "fees",
            "currency_code",
            "description",
            "created_at",
            "created_by",
            "investment_transaction_id",
        } <= cols

    def test_app_lot_selections_columns(self, db: Database) -> None:
        cols = _columns(db, "app", "lot_selections")
        assert {
            "investment_transaction_id",
            "lot_id",
            "quantity",
            "created_at",
        } <= cols

    def test_account_settings_has_cost_basis_default(self, db: Database) -> None:
        cols = _columns(db, "app", "account_settings")
        assert "default_cost_basis_method" in cols

    def test_security_type_check_allows_cash(self, db: Database) -> None:
        """'cash' is a valid security_type (money-market/sweep positions)."""
        db.execute(
            "INSERT INTO app.securities (security_id, name, security_type) "
            "VALUES ('t_cash_ok000', 'Sweep Fund', 'cash')"
        )
        db.execute("DELETE FROM app.securities WHERE security_id = 't_cash_ok000'")

    def test_cost_basis_method_check_rejects_lifo(self, db: Database) -> None:
        """LIFO is deliberately outside the closed v1 method set."""
        with pytest.raises(Exception, match="(?i)constraint"):
            db.execute(
                "INSERT INTO app.securities "
                "(security_id, name, security_type, cost_basis_method) "
                "VALUES ('t_lifo_no000', 'Bad Method', 'equity', 'lifo')"
            )

    def test_cost_basis_method_check_allows_hifo(self, db: Database) -> None:
        db.execute(
            "INSERT INTO app.securities "
            "(security_id, name, security_type, cost_basis_method) "
            "VALUES ('t_hifo_ok000', 'HIFO Sec', 'crypto', 'hifo')"
        )
        db.execute("DELETE FROM app.securities WHERE security_id = 't_hifo_ok000'")
