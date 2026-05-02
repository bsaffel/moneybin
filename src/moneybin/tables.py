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
BRIDGE_TRANSFERS = TableRef("core", "bridge_transfers", audience="interface")

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

# -- App tables (application-managed data) --
TRANSACTION_CATEGORIES = TableRef("app", "transaction_categories", audience="interface")
BUDGETS = TableRef("app", "budgets", audience="interface")
TRANSACTION_NOTES = TableRef("app", "transaction_notes", audience="interface")
# view: seeds.categories ∪ app.user_categories, with overrides applied
CATEGORIES = TableRef("app", "categories", audience="interface")
USER_CATEGORIES = TableRef("app", "user_categories")
CATEGORY_OVERRIDES = TableRef("app", "category_overrides")
MERCHANTS = TableRef("app", "merchants", audience="interface")
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
