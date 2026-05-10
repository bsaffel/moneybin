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
AGG_NET_WORTH = TableRef("core", "agg_net_worth", audience="interface")

# -- Raw tables (used until core models are built for these entities) --
OFX_ACCOUNTS = TableRef("raw", "ofx_accounts")
OFX_TRANSACTIONS = TableRef("raw", "ofx_transactions")
OFX_BALANCES = TableRef("raw", "ofx_balances")
OFX_INSTITUTIONS = TableRef("raw", "ofx_institutions")
W2_FORMS = TableRef("raw", "w2_forms")

# -- Raw tabular tables (replaces csv_* tables) --
TABULAR_TRANSACTIONS = TableRef("raw", "tabular_transactions")
TABULAR_ACCOUNTS = TableRef("raw", "tabular_accounts")
IMPORT_LOG = TableRef("raw", "import_log")
MANUAL_TRANSACTIONS = TableRef("raw", "manual_transactions")

# -- App tables (application-managed data) --
ACCOUNT_SETTINGS = TableRef("app", "account_settings", audience="interface")
BALANCE_ASSERTIONS = TableRef("app", "balance_assertions", audience="interface")
TRANSACTION_CATEGORIES = TableRef("app", "transaction_categories", audience="interface")
BUDGETS = TableRef("app", "budgets", audience="interface")
TRANSACTION_NOTES = TableRef("app", "transaction_notes", audience="interface")
TRANSACTION_TAGS = TableRef("app", "transaction_tags", audience="interface")
TRANSACTION_SPLITS = TableRef("app", "transaction_splits", audience="interface")
IMPORTS = TableRef("app", "imports", audience="interface")
AUDIT_LOG = TableRef("app", "audit_log", audience="interface")
# view: seeds.categories ∪ app.user_categories, with overrides applied
# (migrating to core.dim_categories — see reports-recipe-library.md)
CATEGORIES = TableRef("app", "categories", audience="interface")
USER_CATEGORIES = TableRef("app", "user_categories")
CATEGORY_OVERRIDES = TableRef("app", "category_overrides")
# view: app.user_merchants ∪ seeds.merchants_*, with overrides applied
# (migrating to core.dim_merchants — see reports-recipe-library.md)
MERCHANTS = TableRef("app", "merchants", audience="interface")
USER_MERCHANTS = TableRef("app", "user_merchants")
MERCHANT_OVERRIDES = TableRef("app", "merchant_overrides")
SEED_MERCHANTS_GLOBAL = TableRef("seeds", "merchants_global")
SEED_MERCHANTS_US = TableRef("seeds", "merchants_us")
SEED_MERCHANTS_CA = TableRef("seeds", "merchants_ca")
CATEGORIZATION_RULES = TableRef("app", "categorization_rules", audience="interface")
PROPOSED_RULES = TableRef("app", "proposed_rules")
RULE_DEACTIVATIONS = TableRef("app", "rule_deactivations")
SCHEMA_MIGRATIONS = TableRef("app", "schema_migrations")
VERSIONS = TableRef("app", "versions")

# -- App tabular tables --
TABULAR_FORMATS = TableRef("app", "tabular_formats")

# -- App matching tables --
MATCH_DECISIONS = TableRef("app", "match_decisions")
SEED_SOURCE_PRIORITY = TableRef("app", "seed_source_priority")

# -- Seed tables (materialized by SQLMesh from CSV) --
SEED_CATEGORIES = TableRef("seeds", "categories")

# -- Prep / staging views (built by SQLMesh transforms) --
INT_TRANSACTIONS_MATCHED = TableRef("prep", "int_transactions__matched")

# -- Meta schema (cross-source provenance + lineage) --
FCT_TRANSACTION_PROVENANCE = TableRef("meta", "fct_transaction_provenance")

# -- Synthetic tables (created on demand by the generator) --
GROUND_TRUTH = TableRef("synthetic", "ground_truth")

# -- Reports presentation views (SQLMesh-managed, read-only) --
# One model per CLI/MCP `reports *` surface (per cli-restructure.md v2 +
# reports-recipe-library.md). Consumers read these views; never written to.
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
