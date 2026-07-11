"""Table registry — single source of truth for schema-qualified table names.

All consumers (MCP server, CLI, services) import table constants from here.
"""

from __future__ import annotations

import sys
from typing import Literal, NamedTuple

# When adding a new TableRef constant: if it should be visible to MCP
# clients via the curated `moneybin://schema` resource, pass
# audience="interface". Otherwise it stays internal (default).


class TableRef(NamedTuple):
    """Reference to a database table with schema and name."""

    schema: str
    name: str
    audience: Literal["interface", "internal"] = "internal"

    @property
    def full_name(self) -> str:
        """Schema-qualified table name for use in SQL queries."""
        return f"{self.schema}.{self.name}"


# -- Core layer (canonical tables built by SQLMesh transforms) --
DIM_ACCOUNTS = TableRef("core", "dim_accounts", audience="interface")
FCT_TRANSACTIONS = TableRef("core", "fct_transactions", audience="interface")
FCT_TRANSACTION_LINES = TableRef("core", "fct_transaction_lines", audience="interface")
BRIDGE_TRANSFERS = TableRef("core", "bridge_transfers", audience="interface")
FCT_BALANCES = TableRef("core", "fct_balances", audience="interface")
FCT_BALANCES_DAILY = TableRef("core", "fct_balances_daily", audience="interface")

# -- Raw tables (used until core models are built for these entities) --
OFX_ACCOUNTS = TableRef("raw", "ofx_accounts")
OFX_TRANSACTIONS = TableRef("raw", "ofx_transactions")
OFX_BALANCES = TableRef("raw", "ofx_balances")
OFX_INSTITUTIONS = TableRef("raw", "ofx_institutions")
PLAID_ACCOUNTS = TableRef("raw", "plaid_accounts")
PLAID_TRANSACTIONS = TableRef("raw", "plaid_transactions")
PLAID_BALANCES = TableRef("raw", "plaid_balances")

# -- Raw tabular tables (replaces csv_* tables) --
TABULAR_TRANSACTIONS = TableRef("raw", "tabular_transactions")
TABULAR_ACCOUNTS = TableRef("raw", "tabular_accounts")
IMPORT_LOG = TableRef("raw", "import_log")
MANUAL_TRANSACTIONS = TableRef("raw", "manual_transactions")
GSHEET_SEEDS = TableRef("raw", "gsheet_seeds")
PDF_SEEDS = TableRef("raw", "pdf_seeds")

# -- App tables (application-managed data) --
PDF_FORMATS = TableRef("app", "pdf_formats")
ACCOUNT_SETTINGS = TableRef("app", "account_settings", audience="interface")
BALANCE_ASSERTIONS = TableRef("app", "balance_assertions", audience="interface")
TRANSACTION_CATEGORIES = TableRef("app", "transaction_categories", audience="interface")
BUDGETS = TableRef("app", "budgets", audience="interface")
TRANSACTION_NOTES = TableRef("app", "transaction_notes", audience="interface")
TRANSACTION_TAGS = TableRef("app", "transaction_tags", audience="interface")
TRANSACTION_SPLITS = TableRef("app", "transaction_splits", audience="interface")
IMPORTS = TableRef("app", "imports", audience="interface")
AUDIT_LOG = TableRef("app", "audit_log", audience="interface")
CATEGORIES = TableRef("core", "dim_categories", audience="interface")
USER_CATEGORIES = TableRef("app", "user_categories")
CATEGORY_OVERRIDES = TableRef("app", "category_overrides")
CATEGORY_SOURCE_MAP = TableRef("app", "category_source_map")
BRIDGE_CATEGORY_SOURCE_MAP = TableRef(
    "core", "bridge_category_source_map", audience="interface"
)
MERCHANTS = TableRef("core", "dim_merchants", audience="interface")
USER_MERCHANTS = TableRef("app", "user_merchants")
CATEGORIZATION_RULES = TableRef("app", "categorization_rules", audience="interface")
PROPOSED_RULES = TableRef("app", "proposed_rules")
SCHEMA_MIGRATIONS = TableRef("app", "schema_migrations")
VERSIONS = TableRef("app", "versions")
GSHEET_CONNECTIONS = TableRef("app", "gsheet_connections")
AI_CONSENT_GRANTS = TableRef("app", "ai_consent_grants")

# -- App tabular tables --
TABULAR_FORMATS = TableRef("app", "tabular_formats")

# -- App matching tables --
MATCH_DECISIONS = TableRef("app", "match_decisions")
SEED_SOURCE_PRIORITY = TableRef("app", "seed_source_priority")

# -- App account-identity tables (M1S) --
ACCOUNT_LINKS = TableRef("app", "account_links")
ACCOUNT_LINK_DECISIONS = TableRef("app", "account_link_decisions")
TRANSACTION_ID_ALIASES = TableRef("app", "transaction_id_aliases")

# -- App merchant-identity tables (M1T) --
MERCHANT_LINKS = TableRef("app", "merchant_links")
MERCHANT_LINK_DECISIONS = TableRef("app", "merchant_link_decisions")

# -- Investments tables (M1J) --
# The five core models are audience="interface": their SQLMesh models +
# schema-catalog examples have landed (INTERFACE_TABLES is a live contract:
# the table must exist in the catalog DB and carry query examples).
SECURITIES = TableRef("app", "securities")
LOT_SELECTIONS = TableRef("app", "lot_selections")
MANUAL_INVESTMENT_TRANSACTIONS = TableRef("raw", "manual_investment_transactions")
PLAID_SECURITIES = TableRef("raw", "plaid_securities")
PLAID_INVESTMENT_TRANSACTIONS = TableRef("raw", "plaid_investment_transactions")
PLAID_INVESTMENT_HOLDINGS = TableRef("raw", "plaid_investment_holdings")
PLAID_INVESTMENT_HOLDING_LOTS = TableRef("raw", "plaid_investment_holding_lots")
SECURITY_LINKS = TableRef("app", "security_links")
SECURITY_LINK_DECISIONS = TableRef("app", "security_link_decisions")
DIM_SECURITIES = TableRef("core", "dim_securities", audience="interface")
FCT_INVESTMENT_TRANSACTIONS = TableRef(
    "core", "fct_investment_transactions", audience="interface"
)
FCT_INVESTMENT_LOTS = TableRef("core", "fct_investment_lots", audience="interface")
FCT_REALIZED_GAINS = TableRef("core", "fct_realized_gains", audience="interface")
DIM_HOLDINGS = TableRef("core", "dim_holdings", audience="interface")

# -- Seed tables (materialized by SQLMesh from CSV) --
SEED_CATEGORIES = TableRef("seeds", "categories")
SEED_CATEGORY_SOURCE_MAP = TableRef("seeds", "category_source_map")
SEED_EXCHANGE_MIC_MAP = TableRef("seeds", "exchange_mic_map")

# -- Prep / staging views (built by SQLMesh transforms) --
INT_TRANSACTIONS_UNIONED = TableRef("prep", "int_transactions__unioned")
INT_TRANSACTIONS_MATCHED = TableRef("prep", "int_transactions__matched")
INT_TRANSACTIONS_MERGED = TableRef("prep", "int_transactions__merged")
STG_PLAID_TRANSACTIONS = TableRef("prep", "stg_plaid__transactions")

# -- Meta schema (cross-source provenance + lineage) --
FCT_TRANSACTION_PROVENANCE = TableRef("meta", "fct_transaction_provenance")

# -- Synthetic tables (created on demand by the generator) --
GROUND_TRUTH = TableRef("synthetic", "ground_truth")

# -- Reports presentation views (SQLMesh-managed, read-only) --
# One model per CLI/MCP `reports *` surface. Consumers read; never write.
REPORTS_NET_WORTH = TableRef("reports", "net_worth", audience="interface")
REPORTS_CASH_FLOW = TableRef("reports", "cash_flow", audience="interface")
REPORTS_SPENDING_TREND = TableRef("reports", "spending_trend", audience="interface")
REPORTS_RECURRING_SUBSCRIPTIONS = TableRef(
    "reports", "recurring_subscriptions", audience="interface"
)
REPORTS_UNCATEGORIZED_QUEUE = TableRef(
    "reports", "uncategorized_queue", audience="interface"
)
REPORTS_MERCHANT_ACTIVITY = TableRef(
    "reports", "merchant_activity", audience="interface"
)
REPORTS_LARGE_TRANSACTIONS = TableRef(
    "reports", "large_transactions", audience="interface"
)
REPORTS_BALANCE_DRIFT = TableRef("reports", "balance_drift", audience="interface")


def _all_table_refs() -> tuple[TableRef, ...]:
    """Collect every TableRef constant defined at module scope.

    Walks this module's globals so the interface set is derived from
    the constant declarations rather than maintained as a parallel list.
    """
    module = sys.modules[__name__]
    return tuple(
        value for value in vars(module).values() if isinstance(value, TableRef)
    )


INTERFACE_TABLES: tuple[TableRef, ...] = tuple(
    t for t in _all_table_refs() if t.audience == "interface"
)
