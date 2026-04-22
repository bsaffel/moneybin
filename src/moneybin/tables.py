"""Table registry — single source of truth for schema-qualified table names.

All consumers (MCP server, CLI, services) import table constants from here.
"""

from typing import NamedTuple


class TableRef(NamedTuple):
    """Reference to a database table with schema and name."""

    schema: str
    name: str

    @property
    def full_name(self) -> str:
        """Schema-qualified table name for use in SQL queries."""
        return f"{self.schema}.{self.name}"


# -- Core layer (canonical tables built by SQLMesh transforms) --
DIM_ACCOUNTS = TableRef("core", "dim_accounts")
FCT_TRANSACTIONS = TableRef("core", "fct_transactions")

# -- Raw tables (used until core models are built for these entities) --
OFX_BALANCES = TableRef("raw", "ofx_balances")
OFX_INSTITUTIONS = TableRef("raw", "ofx_institutions")
W2_FORMS = TableRef("raw", "w2_forms")

# -- Raw tabular tables (replaces csv_* tables) --
TABULAR_TRANSACTIONS = TableRef("raw", "tabular_transactions")
TABULAR_ACCOUNTS = TableRef("raw", "tabular_accounts")
IMPORT_LOG = TableRef("raw", "import_log")

# -- App tables (application-managed data) --
TRANSACTION_CATEGORIES = TableRef("app", "transaction_categories")
BUDGETS = TableRef("app", "budgets")
TRANSACTION_NOTES = TableRef("app", "transaction_notes")
CATEGORIES = TableRef("app", "categories")
MERCHANTS = TableRef("app", "merchants")
CATEGORIZATION_RULES = TableRef("app", "categorization_rules")
SCHEMA_MIGRATIONS = TableRef("app", "schema_migrations")
VERSIONS = TableRef("app", "versions")

# -- App tabular tables --
TABULAR_FORMATS = TableRef("app", "tabular_formats")

# -- Seed tables (materialized by SQLMesh from CSV) --
SEED_CATEGORIES = TableRef("seeds", "categories")
