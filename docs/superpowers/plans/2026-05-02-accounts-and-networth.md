# Accounts & Net Worth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the v2 `accounts` CLI/MCP namespace end-to-end: per-account settings (Plaid-parity metadata + lifecycle flags), authoritative balance tracking with daily carry-forward, and cross-account net worth aggregation. Closes Level 1 of the MVP roadmap.

**Architecture:** Two new app tables (`app.account_settings`, `app.balance_assertions`) + three new SQLMesh models (`core.fct_balances` view, `core.fct_balances_daily` Python model, `core.agg_net_worth` view) + extended `core.dim_accounts` (single source of truth — joins settings directly). Three services: `AccountService` (extended with settings CRUD + soft-validation), `BalanceService` (assertions + reconciliation), `NetworthService` (aggregation). CLI surface lives in `src/moneybin/cli/commands/accounts.py` (entity ops + nested `balance` sub-app) and `src/moneybin/cli/commands/reports.py` (new top-level group). MCP exposes 16 new tools across `accounts_*` and `reports_networth_*` namespaces plus two resources.

**Tech Stack:** Python 3.12, Typer, Pydantic v2, DuckDB, SQLMesh (with at least one Python model for date-spine carry-forward), FastMCP, pytest (`@pytest.mark.unit/integration/e2e`), the project's `Database` + `mcp_tool` decorator + `ResponseEnvelope` plumbing.

**Reference specs:**
- [`docs/specs/account-management.md`](../../specs/account-management.md) (status: ready)
- [`docs/specs/net-worth.md`](../../specs/net-worth.md) (status: ready)
- [`docs/specs/net-worth.md` §Coordination](../../specs/net-worth.md#coordination-with-account-managementmd) — artifact ownership across the two specs

**Related rules** (loaded automatically by path scope; reviewed up front so the plan matches them):
- `.claude/rules/database.md` — DuckDB/SQLMesh patterns, column comments, decimal types
- `.claude/rules/security.md` — parameterized SQL, input validation, PII in logs
- `.claude/rules/cli.md` — Typer patterns, `handle_cli_errors`, output flags, soft-validation UX
- `.claude/rules/mcp-server.md` — sensitivity tiers, response envelope, tool registration
- `.claude/rules/testing.md` — test layers, query-count assertions, scenario expectations
- `.claude/rules/identifiers.md` — UUID4 truncated to 12 hex for entity IDs
- `.claude/rules/shipping.md` — README updates, roadmap icon flips, `/simplify` pre-push

**Branch:** `feat/accounts-and-networth` (this worktree). Primary intent is the v2 `accounts` + `reports networth` feature.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `src/moneybin/sql/schema/app_account_settings.sql` | Create | DDL for `app.account_settings` (Plaid-parity metadata + archived + include_in_net_worth + updated_at). |
| `src/moneybin/sql/schema/app_balance_assertions.sql` | Create | DDL for `app.balance_assertions` (account_id, assertion_date, balance, notes, created_at). |
| `src/moneybin/sql/migrations/V004__create_app_account_settings.sql` | Create | First-time creation in existing databases. |
| `src/moneybin/sql/migrations/V005__create_app_balance_assertions.sql` | Create | First-time creation in existing databases. |
| `src/moneybin/schema.py` | Modify | Append the two new schema files to `_SCHEMA_FILES`. |
| `src/moneybin/tables.py` | Modify | Add `ACCOUNT_SETTINGS`, `BALANCE_ASSERTIONS`, `FCT_BALANCES`, `FCT_BALANCES_DAILY`, `AGG_NET_WORTH` `TableRef` constants. |
| `sqlmesh/models/core/dim_accounts.sql` | Modify | LEFT JOIN `app.account_settings`; add `display_name`, `official_name`, `last_four`, `account_subtype`, `holder_category`, `iso_currency_code`, `credit_limit`, `archived`, `include_in_net_worth` columns. |
| `sqlmesh/models/core/fct_balances.sql` | Create | VIEW unioning OFX balance snapshots, tabular running balances, and user assertions. |
| `sqlmesh/models/core/fct_balances_daily.py` | Create | Python SQLMesh model: date-spine + carry-forward + reconciliation delta per account. |
| `sqlmesh/models/core/agg_net_worth.sql` | Create | VIEW summing `fct_balances_daily` across non-archived, included accounts per day. |
| `src/moneybin/services/account_service.py` | Modify | Add settings CRUD, summary aggregator, soft-validation classifier; extend list/get with new dim columns; add `redacted` flag. |
| `src/moneybin/services/balance_service.py` | Create | `BalanceService` — current balances, history, reconcile, assertion CRUD. |
| `src/moneybin/services/networth_service.py` | Create | `NetworthService` — current net worth, history, summary. |
| `src/moneybin/services/__init__.py` | Modify | Export `BalanceService`, `NetworthService`, `AccountService`. |
| `src/moneybin/cli/commands/accounts.py` | Modify | Replace stub with real `accounts` group: `list`, `show`, `rename`, `include`, `archive`, `unarchive`, `set`, plus nested `balance` sub-app (`show`, `history`, `assert`, `list`, `delete`, `reconcile`). |
| `src/moneybin/cli/commands/reports.py` | Create | New top-level `reports` group with `networth show` and `networth history`. |
| `src/moneybin/cli/commands/stubs.py` | Modify | Remove `track_app` stubs. Move `recurring`, `investments` stubs to their v2 homes (`transactions recurring`, `accounts investments`). Move `budget` stub to top-level. |
| `src/moneybin/cli/main.py` | Modify | Register the new `accounts` and `reports` groups; remove `track_app` registration. |
| `src/moneybin/mcp/tools/accounts.py` | Modify | Replace v1 surface with v2: `accounts_list` (with `redacted`), `accounts_get`, `accounts_summary`, `accounts_rename`, `accounts_include`, `accounts_archive`, `accounts_unarchive`, `accounts_settings_update`, `accounts_balance_list`, `accounts_balance_history`, `accounts_balance_reconcile`, `accounts_balance_assertions_list`, `accounts_balance_assert`, `accounts_balance_assertion_delete`. |
| `src/moneybin/mcp/tools/reports.py` | Create | `reports_networth_get`, `reports_networth_history`. |
| `src/moneybin/mcp/tools/__init__.py` | Modify | Wire the new tool registrations into the FastMCP server. |
| `src/moneybin/mcp/resources/accounts.py` | Create | `accounts://summary` resource. |
| `src/moneybin/mcp/resources/networth.py` | Create | `net-worth://summary` resource. |
| `tests/moneybin/test_services/test_account_service.py` | Create | Unit tests: settings CRUD, soft-validation classifier, archive cascade, summary aggregator. |
| `tests/moneybin/test_services/test_balance_service.py` | Create | Unit tests: assertion CRUD, current balance lookup, history series, reconcile delta. |
| `tests/moneybin/test_services/test_networth_service.py` | Create | Unit tests: current networth, history series, archived/excluded handling. |
| `tests/moneybin/test_cli/test_accounts.py` | Create | CLI tests for entity ops + soft-validation prompt + cascade messages. |
| `tests/moneybin/test_cli/test_accounts_balance.py` | Create | CLI tests for `accounts balance` subcommands. |
| `tests/moneybin/test_cli/test_reports_networth.py` | Create | CLI tests for `reports networth show/history`. |
| `tests/e2e/test_e2e_help.py` | Modify | Add `--help` entries for `accounts`, `accounts balance`, `reports`, `reports networth`. |
| `tests/e2e/test_e2e_readonly.py` | Modify | Add E2E entries for read commands. |
| `tests/e2e/test_e2e_mutating.py` | Modify | Add E2E entries for write commands. |
| `tests/e2e/test_e2e_mcp.py` | Modify | Add MCP smoke tests for the new tools and resources. |
| `tests/scenarios/scenario_account_settings.yaml` | Create | Settings combinations + archive cascade. |
| `tests/scenarios/scenario_networth_correctness.yaml` | Create | Net worth vs synthetic ground truth. |
| `tests/scenarios/scenario_reconciliation_self_heal.yaml` | Create | Reimport resolves delta. |
| `tests/scenarios/test_runner_*.py` | Modify | Wire the new scenarios into the runner. |
| `docs/architecture/account-identifiers.md` | Create | account_id vs account_number vs last_four vs routing_number; PII masking story. |
| `.claude/rules/database.md` | Modify | Add "core dimensions are the single source of truth — join app metadata into the dim, never duplicate join logic in consumers" rule. |
| `docs/specs/cli-restructure.md` | Modify | Amend `accounts` subtree with `archive` / `unarchive` / `set`. |
| `docs/specs/mcp-tool-surface.md` | Modify | Add the new `accounts_*` write tools and `accounts_summary`. |
| `docs/specs/INDEX.md` | Modify | Flip both specs to `in-progress` at start, `implemented` at end. |
| `docs/specs/account-management.md` | Modify | Status `ready` → `in-progress` → `implemented`. |
| `docs/specs/net-worth.md` | Modify | Same. |
| `README.md` | Modify | Flip roadmap icons; add "What Works Today" content for accounts + net worth + balance tracking. |

---

## Phase 0 — Setup, Specs to In-Progress, Identifier Doc

Starts the work and makes the in-flight status visible to anyone reading the repo.

### Task 0.1: Mark both specs in-progress

**Files:** Modify `docs/specs/account-management.md`, `docs/specs/net-worth.md`, `docs/specs/INDEX.md`.

- [ ] **Step 0.1.1: Flip statuses**

In each spec file, change `## Status\nready` → `## Status\nin-progress`.

In `docs/specs/INDEX.md`, change both rows to status `in-progress`.

- [ ] **Step 0.1.2: Commit**

```bash
git add docs/specs/account-management.md docs/specs/net-worth.md docs/specs/INDEX.md
git commit -m "Mark accounts + net-worth specs in-progress"
```

### Task 0.2: Strengthen `.claude/rules/database.md` with the "core dim single source of truth" rule

**Files:** Modify `.claude/rules/database.md`.

- [ ] **Step 0.2.1: Add the rule**

Insert the following section under the "Model Naming Conventions" section (or wherever fits best):

```markdown
## Core Dimensions Are the Single Source of Truth

When app-layer metadata refines or overrides a `core.dim_*` entity (e.g.,
`app.account_settings.display_name` overriding `dim_accounts.account_id`-derived
display), join the metadata into the dim model itself — do NOT duplicate the
join in every consumer (CLI, MCP, agg models, services).

The dim's job is to present one canonical resolved view per entity. Consumers
read the dim and trust it. This keeps the resolution chain in exactly one place
and prevents inconsistent overrides between surfaces.

Precedent: `core.dim_accounts` joins `app.account_settings` (per
`docs/specs/account-management.md`).
```

- [ ] **Step 0.2.2: Commit**

```bash
git add .claude/rules/database.md
git commit -m "Codify core-dim-as-source-of-truth pattern in database.md"
```

### Task 0.3: Write `docs/architecture/account-identifiers.md`

**Files:** Create `docs/architecture/account-identifiers.md`.

- [ ] **Step 0.3.1: Create the directory and write the doc**

```bash
mkdir -p docs/architecture
```

Content (full file):

```markdown
# Account Identifiers and PII Handling

MoneyBin uses several distinct identifiers for accounts. This doc defines what
each one is, where it lives, and how PII is masked across the project.

## Identifier glossary

| Name | Type | Lives in | Source | Safe to log? |
|---|---|---|---|---|
| `account_id` | Synthetic stable ID | `core.dim_accounts.account_id` (PK), foreign key in `fct_transactions`, `fct_balances`, `app.account_settings`, `app.balance_assertions` | Derived: OFX `<ACCTID>` after sanitization, tabular content hash, future Plaid `account_id` | Yes — opaque, no PII |
| `account_number` | Full bank account number | **Never stored** | Source files | Never |
| `last_four` | Last 4 digits | `app.account_settings.last_four` (validated `^[0-9]{4}$`) | User-asserted in v1; Plaid `mask` in future | Reference by name only (`<account_id>.last_four`), never as a value |
| `routing_number` | ABA routing number | `core.dim_accounts.routing_number` | OFX `<BANKACCTFROM><BANKID>` or tabular | PII-adjacent (publicly listed but identifies institution); log only when essential for diagnostics |
| `display_name` | Human-readable label | `core.dim_accounts.display_name` (resolved from `app.account_settings.display_name` → derived default) | User override or auto-derived | Yes — user-controlled label |

## Why `account_number` is never stored

Loaders extract only `last_four` from raw inputs. The full number is dropped at
the parser boundary. This eliminates an entire class of breach impact: a
database leak does not expose full account numbers because they are not there.

## Masking story

Even with disciplined input handling, account-shaped digit sequences can leak
into logs through error messages, stack traces, or accidentally interpolated
SQL. The `SanitizedLogFormatter` (`src/moneybin/log_sanitizer.py`) is the
runtime safety net — it inspects every log record for patterns matching:

- 9+ digit sequences (catches account numbers, SSNs, routing numbers in
  contexts where they shouldn't be)
- Currency amount patterns (`$NNN.NN`)

When detected, the formatter masks the value (keeping the last 4 digits for
debug context) and emits a separate WARNING about the masking, so the original
incident is visible without leaking the value.

See `docs/specs/privacy-data-protection.md` for the full classification of
allowed vs prohibited log content.

## Relation to `account_id` stability

`account_id` is the join key everywhere. It must be stable across re-imports
of the same upstream account (re-importing the same OFX file must not produce
new `account_id`s). Hash-derived IDs achieve this by being a pure function of
the source content. Future Plaid integration uses Plaid's stable
`account_id` directly.

When two records that look like the same real-world account land under
different `account_id`s (e.g., the user re-linked an institution and Plaid
issued a new ID), the v1 answer is to leave both in place. Account merging is
explicitly out of scope for v1 — see `docs/specs/account-management.md`
§Out of Scope.
```

- [ ] **Step 0.3.2: Commit**

```bash
git add docs/architecture/account-identifiers.md
git commit -m "Add account identifiers + PII masking architecture doc"
```

---

## Phase 1 — Schema Foundation

Two new tables, two migrations, schema-registry wiring, `TableRef` constants.

### Task 1.1: Create `app.account_settings` schema file

**Files:** Create `src/moneybin/sql/schema/app_account_settings.sql`. Modify `src/moneybin/schema.py`.

- [ ] **Step 1.1.1: Write the schema file**

```sql
/* Per-account user-controlled settings: Plaid-parity metadata + lifecycle flags.
   One row per account_id; absence means all defaults.
   Joined by core.dim_accounts to surface as the canonical resolved view. */
CREATE TABLE IF NOT EXISTS app.account_settings (
    account_id           VARCHAR NOT NULL PRIMARY KEY,            -- Foreign key to core.dim_accounts.account_id
    display_name         VARCHAR,                                  -- User-supplied label override; NULL falls back to derived default
    official_name        VARCHAR,                                  -- Institution's formal account name (mirrors Plaid official_name); free text
    last_four            VARCHAR,                                  -- Last 4 digits of account number (mirrors Plaid mask); validated ^[0-9]{4}$ at service boundary
    account_subtype      VARCHAR,                                  -- Plaid-style subtype (checking, savings, credit card, mortgage, ...); open vocabulary
    holder_category      VARCHAR,                                  -- 'personal' / 'business' / 'joint'; open vocabulary
    iso_currency_code    VARCHAR,                                  -- ISO-4217 (USD, EUR, ...); NULL defaults to USD until multi-currency.md ships
    credit_limit         DECIMAL(18, 2),                           -- User-asserted credit limit on credit cards / lines (drives utilization metrics)
    archived             BOOLEAN NOT NULL DEFAULT FALSE,           -- Hides account from default list and from agg_net_worth
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,            -- Whether this account contributes to agg_net_worth (independent toggle, but archive cascades to FALSE)
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last modification time
);
```

- [ ] **Step 1.1.2: Register in `_SCHEMA_FILES`**

In `src/moneybin/schema.py`, append `"app_account_settings.sql",` to `_SCHEMA_FILES` (after the existing `app_*` entries).

- [ ] **Step 1.1.3: Verify schema loads**

```bash
uv run python -c "from moneybin.schema import init_schemas; import duckdb; conn = duckdb.connect(':memory:'); conn.execute('CREATE SCHEMA app'); conn.execute('CREATE SCHEMA core'); conn.execute('CREATE SCHEMA raw'); conn.execute('CREATE SCHEMA analytics'); conn.execute('CREATE SCHEMA meta'); init_schemas(conn); print(conn.execute('DESCRIBE app.account_settings').fetchall())"
```

Expected: 11 rows (one per column).

- [ ] **Step 1.1.4: Commit**

```bash
git add src/moneybin/sql/schema/app_account_settings.sql src/moneybin/schema.py
git commit -m "Add app.account_settings DDL and schema registration"
```

### Task 1.2: Create `app.balance_assertions` schema file

**Files:** Create `src/moneybin/sql/schema/app_balance_assertions.sql`. Modify `src/moneybin/schema.py`.

- [ ] **Step 1.2.1: Write the schema file**

```sql
/* User-entered balance anchors for accounts; primary observation source alongside
   OFX statement balances and tabular running balances. Composite PK enforces
   one assertion per account per date. */
CREATE TABLE IF NOT EXISTS app.balance_assertions (
    account_id     VARCHAR NOT NULL,                                  -- Foreign key to core.dim_accounts.account_id
    assertion_date DATE NOT NULL,                                     -- Date the balance was observed
    balance        DECIMAL(18, 2) NOT NULL,                           -- Asserted balance amount
    notes          VARCHAR,                                            -- Optional user notes (e.g., "from paper statement")
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,       -- When the assertion was entered
    PRIMARY KEY (account_id, assertion_date)
);
```

- [ ] **Step 1.2.2: Register in `_SCHEMA_FILES`**

Append `"app_balance_assertions.sql",` to `_SCHEMA_FILES`.

- [ ] **Step 1.2.3: Verify**

```bash
uv run python -c "from moneybin.schema import init_schemas; import duckdb; conn = duckdb.connect(':memory:'); conn.execute('CREATE SCHEMA app'); conn.execute('CREATE SCHEMA core'); conn.execute('CREATE SCHEMA raw'); conn.execute('CREATE SCHEMA analytics'); conn.execute('CREATE SCHEMA meta'); init_schemas(conn); print(conn.execute('DESCRIBE app.balance_assertions').fetchall())"
```

Expected: 5 rows.

- [ ] **Step 1.2.4: Commit**

```bash
git add src/moneybin/sql/schema/app_balance_assertions.sql src/moneybin/schema.py
git commit -m "Add app.balance_assertions DDL and schema registration"
```

### Task 1.3: Add `TableRef` constants

**Files:** Modify `src/moneybin/tables.py`.

- [ ] **Step 1.3.1: Add the constants**

Insert (in the appropriate `app.*` and `core.*` groupings):

```python
ACCOUNT_SETTINGS = TableRef("app", "account_settings", audience="interface")
BALANCE_ASSERTIONS = TableRef("app", "balance_assertions", audience="interface")

FCT_BALANCES = TableRef("core", "fct_balances", audience="interface")
FCT_BALANCES_DAILY = TableRef("core", "fct_balances_daily", audience="interface")
AGG_NET_WORTH = TableRef("core", "agg_net_worth", audience="interface")
```

- [ ] **Step 1.3.2: Commit**

```bash
git add src/moneybin/tables.py
git commit -m "Add TableRef constants for account_settings, balance_assertions, balance models"
```

### Task 1.4: Add migrations for existing databases

**Files:** Create `src/moneybin/sql/migrations/V004__create_app_account_settings.sql`, `src/moneybin/sql/migrations/V005__create_app_balance_assertions.sql`.

> **Verify version numbers first.** Run `ls src/moneybin/sql/migrations/V*.{sql,py}` to confirm the next available version. Adjust filenames if V004/V005 are taken.

- [ ] **Step 1.4.1: Write V004**

```sql
-- Create app.account_settings for the v2 accounts namespace.
-- Idempotent: matches the schema file definition; init_schemas creates this
-- on fresh installs, so this migration only fires for upgrades.

CREATE TABLE IF NOT EXISTS app.account_settings (
    account_id           VARCHAR NOT NULL PRIMARY KEY,
    display_name         VARCHAR,
    official_name        VARCHAR,
    last_four            VARCHAR,
    account_subtype      VARCHAR,
    holder_category      VARCHAR,
    iso_currency_code    VARCHAR,
    credit_limit         DECIMAL(18, 2),
    archived             BOOLEAN NOT NULL DEFAULT FALSE,
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

- [ ] **Step 1.4.2: Write V005**

```sql
-- Create app.balance_assertions for net-worth user-asserted balance anchors.
-- Idempotent: matches the schema file definition.

CREATE TABLE IF NOT EXISTS app.balance_assertions (
    account_id     VARCHAR NOT NULL,
    assertion_date DATE NOT NULL,
    balance        DECIMAL(18, 2) NOT NULL,
    notes          VARCHAR,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, assertion_date)
);
```

- [ ] **Step 1.4.3: Run migration tests**

```bash
uv run pytest tests/moneybin/test_database/ -v -k migration
```

Expected: all pass; new tables created on a freshly-migrated test database.

- [ ] **Step 1.4.4: Commit**

```bash
git add src/moneybin/sql/migrations/V004__create_app_account_settings.sql src/moneybin/sql/migrations/V005__create_app_balance_assertions.sql
git commit -m "Add migrations V004/V005 for account_settings + balance_assertions"
```

---

## Phase 2 — `core.dim_accounts` Integration

Single SQLMesh-side change that joins `app.account_settings` and exposes the resolved view to all consumers.

### Task 2.1: Extend `dim_accounts` model

**Files:** Modify `sqlmesh/models/core/dim_accounts.sql`.

- [ ] **Step 2.1.1: Rewrite the model**

Replace the file with:

```sql
/* Canonical accounts dimension; deduplicated accounts from all sources, with
   user-controlled settings (display_name, archive, include_in_net_worth, Plaid-
   parity metadata) joined in as the single resolved source of truth.
   Per .claude/rules/database.md, no consumer joins app.account_settings directly. */
-- Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)
MODEL (
  name core.dim_accounts,
  kind FULL,
  grain account_id
);

WITH ofx_accounts AS (
  SELECT
    account_id,
    routing_number,
    account_type,
    institution_org AS institution_name,
    institution_fid,
    'ofx' AS source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_ofx__accounts
), tabular_accounts AS (
  SELECT
    account_id,
    routing_number,
    account_type,
    institution_name,
    institution_fid,
    source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_tabular__accounts
), all_accounts AS (
  SELECT * FROM ofx_accounts
  UNION ALL
  SELECT * FROM tabular_accounts
), deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY extracted_at DESC) AS _row_num
  FROM all_accounts
), winners AS (
  SELECT * FROM deduplicated WHERE _row_num = 1
)
SELECT
  w.account_id, /* Unique account identifier; stable across imports; foreign key in fct_transactions */
  w.routing_number, /* ABA bank routing number; NULL when not provided by source */
  w.account_type, /* Account classification from source, e.g. CHECKING, SAVINGS, CREDITLINE */
  w.institution_name, /* Human-readable name of the financial institution */
  w.institution_fid, /* OFX financial institution identifier; NULL for tabular sources */
  w.source_type, /* Origin of the winning record after deduplication: ofx, csv, tsv, excel, etc. */
  w.source_file, /* Path to the source file from which this record was loaded */
  w.extracted_at, /* When the data was parsed from the source file */
  w.loaded_at, /* When the record was written to the raw table */
  CURRENT_TIMESTAMP AS updated_at, /* When this core record was last refreshed by SQLMesh */
  COALESCE(
    s.display_name,
    w.institution_name || ' ' || w.account_type || ' …' || RIGHT(w.account_id, 4)
  ) AS display_name, /* Resolved display label: user override → derived default → bare account_id */
  s.official_name, /* Institution's formal name (mirrors Plaid official_name); user-set or future Plaid sync */
  s.last_four, /* Last 4 digits of account number (mirrors Plaid mask); user-set or future Plaid sync */
  s.account_subtype, /* Plaid-style subtype (checking, savings, credit card, mortgage, ...) */
  s.holder_category, /* 'personal' / 'business' / 'joint' */
  COALESCE(s.iso_currency_code, 'USD') AS iso_currency_code, /* ISO-4217 currency code; defaults to USD until multi-currency.md ships */
  s.credit_limit, /* User-asserted credit limit on credit cards / lines */
  COALESCE(s.archived, FALSE) AS archived, /* Hides account from default list and from agg_net_worth */
  COALESCE(s.include_in_net_worth, TRUE) AS include_in_net_worth /* Whether this account contributes to agg_net_worth */
FROM winners AS w
LEFT JOIN app.account_settings AS s ON w.account_id = s.account_id
```

- [ ] **Step 2.1.2: Format with SQLMesh**

```bash
uv run sqlmesh -p sqlmesh format
```

- [ ] **Step 2.1.3: Run SQLMesh plan + apply against dev**

```bash
uv run moneybin transform apply
```

Expected: `dim_accounts` rebuilds; new columns visible in `DESCRIBE core.dim_accounts`.

- [ ] **Step 2.1.4: Sanity-check the join**

```bash
uv run moneybin db query "SELECT account_id, display_name, archived, include_in_net_worth FROM core.dim_accounts LIMIT 5"
```

Expected: rows return; `archived = FALSE`, `include_in_net_worth = TRUE` for all (no settings written yet); `display_name` matches the derived default pattern.

- [ ] **Step 2.1.5: Commit**

```bash
git add sqlmesh/models/core/dim_accounts.sql
git commit -m "Extend dim_accounts with app.account_settings join (single source of truth)"
```

---

## Phase 3 — `AccountService` Settings + Soft-Validation + Summary

Service-layer business logic. Pure unit tests against in-memory DuckDB with the new schema.

### Task 3.1: Soft-validation classifier and canonical lists

**Files:** Modify `src/moneybin/services/account_service.py`. Create `tests/moneybin/test_services/test_account_service.py`.

- [ ] **Step 3.1.1: Write failing tests for the classifier**

Create `tests/moneybin/test_services/test_account_service.py`:

```python
"""Unit tests for AccountService settings, soft-validation, and summary."""

from __future__ import annotations

import pytest

from moneybin.services.account_service import (
    PLAID_CANONICAL_HOLDER_CATEGORIES,
    PLAID_CANONICAL_SUBTYPES,
    is_canonical_holder_category,
    is_canonical_subtype,
    suggest_holder_category,
    suggest_subtype,
)


class TestSubtypeClassifier:
    def test_canonical_subtypes_present(self) -> None:
        assert "checking" in PLAID_CANONICAL_SUBTYPES
        assert "savings" in PLAID_CANONICAL_SUBTYPES
        assert "credit card" in PLAID_CANONICAL_SUBTYPES
        assert "mortgage" in PLAID_CANONICAL_SUBTYPES

    def test_is_canonical_true_for_known(self) -> None:
        assert is_canonical_subtype("checking") is True

    def test_is_canonical_false_for_unknown(self) -> None:
        assert is_canonical_subtype("chequing") is False

    def test_is_canonical_case_insensitive(self) -> None:
        assert is_canonical_subtype("CHECKING") is True

    def test_suggest_near_miss(self) -> None:
        assert suggest_subtype("chequing") == "checking"

    def test_suggest_returns_none_for_far_miss(self) -> None:
        assert suggest_subtype("xyz_garbage") is None


class TestHolderCategoryClassifier:
    def test_canonical_set(self) -> None:
        assert PLAID_CANONICAL_HOLDER_CATEGORIES == frozenset(
            {"personal", "business", "joint"}
        )

    def test_is_canonical(self) -> None:
        assert is_canonical_holder_category("personal") is True
        assert is_canonical_holder_category("corporate") is False

    def test_suggest_near_miss(self) -> None:
        assert suggest_holder_category("persoanl") == "personal"
```

- [ ] **Step 3.1.2: Run to verify they fail**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py::TestSubtypeClassifier -v
```

Expected: ImportError (module symbols don't exist yet).

- [ ] **Step 3.1.3: Implement the classifier**

In `src/moneybin/services/account_service.py`, add at module top (after imports):

```python
from difflib import get_close_matches

# Plaid's documented account subtype list (https://plaid.com/docs/api/accounts/).
# Open vocabulary in this project — soft-validated, never blocking.
PLAID_CANONICAL_SUBTYPES: frozenset[str] = frozenset({
    # depository
    "checking", "savings", "hsa", "cd", "money market", "paypal", "prepaid",
    "cash management", "ebt",
    # credit
    "credit card", "paypal credit",
    # loan
    "auto", "business", "commercial", "construction", "consumer", "home equity",
    "loan", "mortgage", "overdraft", "line of credit", "student",
    # investment
    "401a", "401k", "403b", "457b", "529", "brokerage", "cash isa",
    "education savings account", "fixed annuity", "gic", "health reimbursement arrangement",
    "hsa", "ira", "isa", "keogh", "lif", "life insurance", "lira", "lrif", "lrsp",
    "mutual fund", "non-taxable brokerage account", "other", "other annuity",
    "other insurance", "pension", "plan", "prif", "profit sharing plan", "qshr",
    "rdsp", "resp", "retirement", "rlif", "roth", "roth 401k", "rrif", "rrsp",
    "sarsep", "sep ira", "simple ira", "sipp", "stock plan", "tfsa",
    "trust", "ugma", "utma", "variable annuity",
})

PLAID_CANONICAL_HOLDER_CATEGORIES: frozenset[str] = frozenset({
    "personal", "business", "joint",
})


def is_canonical_subtype(value: str) -> bool:
    """Whether the value matches Plaid's documented subtype list (case-insensitive)."""
    return value.lower() in PLAID_CANONICAL_SUBTYPES


def is_canonical_holder_category(value: str) -> bool:
    """Whether the value matches the canonical holder-category set."""
    return value.lower() in PLAID_CANONICAL_HOLDER_CATEGORIES


def suggest_subtype(value: str) -> str | None:
    """Suggest a canonical subtype near-match; None if no close match."""
    matches = get_close_matches(
        value.lower(), PLAID_CANONICAL_SUBTYPES, n=1, cutoff=0.75
    )
    return matches[0] if matches else None


def suggest_holder_category(value: str) -> str | None:
    """Suggest a canonical holder-category near-match; None if no close match."""
    matches = get_close_matches(
        value.lower(), PLAID_CANONICAL_HOLDER_CATEGORIES, n=1, cutoff=0.75
    )
    return matches[0] if matches else None
```

- [ ] **Step 3.1.4: Run tests**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py::TestSubtypeClassifier tests/moneybin/test_services/test_account_service.py::TestHolderCategoryClassifier -v
```

Expected: all pass.

- [ ] **Step 3.1.5: Commit**

```bash
git add tests/moneybin/test_services/test_account_service.py src/moneybin/services/account_service.py
git commit -m "Add Plaid-parity subtype/holder-category classifier with soft-validation"
```

### Task 3.2: `AccountSettings` dataclass + validation

**Files:** Modify `src/moneybin/services/account_service.py`. Modify the test file.

- [ ] **Step 3.2.1: Write failing tests**

Append to `tests/moneybin/test_services/test_account_service.py`:

```python
from decimal import Decimal

from moneybin.services.account_service import AccountSettings


class TestAccountSettingsModel:
    def test_full_construction(self) -> None:
        s = AccountSettings(
            account_id="acct_abc",
            display_name="Checking",
            official_name="PLATINUM CHECKING ACCOUNT",
            last_four="1234",
            account_subtype="checking",
            holder_category="personal",
            iso_currency_code="USD",
            credit_limit=Decimal("5000.00"),
            archived=False,
            include_in_net_worth=True,
        )
        assert s.display_name == "Checking"

    def test_display_name_too_long(self) -> None:
        with pytest.raises(ValueError, match="display_name"):
            AccountSettings(account_id="a", display_name="x" * 81)

    def test_last_four_format(self) -> None:
        with pytest.raises(ValueError, match="last_four"):
            AccountSettings(account_id="a", last_four="abcd")
        with pytest.raises(ValueError, match="last_four"):
            AccountSettings(account_id="a", last_four="123")

    def test_iso_currency_code_format(self) -> None:
        with pytest.raises(ValueError, match="iso_currency_code"):
            AccountSettings(account_id="a", iso_currency_code="usd")  # lowercase
        with pytest.raises(ValueError, match="iso_currency_code"):
            AccountSettings(account_id="a", iso_currency_code="USDD")

    def test_credit_limit_non_negative(self) -> None:
        with pytest.raises(ValueError, match="credit_limit"):
            AccountSettings(account_id="a", credit_limit=Decimal("-1.00"))

    def test_official_name_too_long(self) -> None:
        with pytest.raises(ValueError, match="official_name"):
            AccountSettings(account_id="a", official_name="x" * 201)

    def test_subtype_too_long(self) -> None:
        with pytest.raises(ValueError, match="account_subtype"):
            AccountSettings(account_id="a", account_subtype="x" * 33)
```

- [ ] **Step 3.2.2: Run to verify failing**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py::TestAccountSettingsModel -v
```

Expected: ImportError.

- [ ] **Step 3.2.3: Implement `AccountSettings`**

Add to `src/moneybin/services/account_service.py` (after the classifier helpers):

```python
import re
from dataclasses import dataclass, field
from decimal import Decimal

_LAST_FOUR_RE = re.compile(r"^[0-9]{4}$")
_ISO_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


@dataclass(frozen=True, slots=True)
class AccountSettings:
    """Per-account settings record. Validated at construction.

    Validation lives here (not in SQL CHECK constraints) so historical rows
    written before tighter rules can still be read.
    """

    account_id: str
    display_name: str | None = None
    official_name: str | None = None
    last_four: str | None = None
    account_subtype: str | None = None
    holder_category: str | None = None
    iso_currency_code: str | None = None
    credit_limit: Decimal | None = None
    archived: bool = False
    include_in_net_worth: bool = True

    def __post_init__(self) -> None:
        if not self.account_id:
            raise ValueError("account_id is required")
        if self.display_name is not None:
            if not 1 <= len(self.display_name) <= 80:
                raise ValueError("display_name must be 1-80 chars")
        if self.official_name is not None:
            if not 1 <= len(self.official_name) <= 200:
                raise ValueError("official_name must be 1-200 chars")
        if self.last_four is not None and not _LAST_FOUR_RE.match(self.last_four):
            raise ValueError("last_four must be exactly 4 digits")
        if self.account_subtype is not None:
            if not 1 <= len(self.account_subtype) <= 32:
                raise ValueError("account_subtype must be 1-32 chars")
        if self.holder_category is not None:
            if not 1 <= len(self.holder_category) <= 32:
                raise ValueError("holder_category must be 1-32 chars")
        if self.iso_currency_code is not None and not _ISO_CURRENCY_RE.match(
            self.iso_currency_code
        ):
            raise ValueError("iso_currency_code must be exactly 3 uppercase letters")
        if self.credit_limit is not None and self.credit_limit < Decimal("0"):
            raise ValueError("credit_limit must be non-negative")
```

- [ ] **Step 3.2.4: Run tests**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py::TestAccountSettingsModel -v
```

Expected: all pass.

- [ ] **Step 3.2.5: Commit**

```bash
git add tests/moneybin/test_services/test_account_service.py src/moneybin/services/account_service.py
git commit -m "Add AccountSettings dataclass with service-boundary validation"
```

### Task 3.3: `AccountSettingsRepository` — load/upsert/delete

**Files:** Modify `src/moneybin/services/account_service.py`. Modify the test file.

The repository is the SQL-touching layer; service methods compose validation + repository.

- [ ] **Step 3.3.1: Write failing tests using `db_with_schema` fixture**

Add a fixture to `tests/moneybin/test_services/test_account_service.py`:

```python
from pathlib import Path

from moneybin.database import Database


@pytest.fixture
def test_db(tmp_path: Path, mock_secret_store) -> Database:
    """In-memory test database with all schemas initialized, no SQLMesh upgrade."""
    return Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
```

> Use the project's existing `mock_secret_store` fixture from `tests/conftest.py` (per `.claude/rules/testing.md`).

Then:

```python
from moneybin.services.account_service import AccountSettingsRepository


class TestAccountSettingsRepository:
    def test_load_returns_none_when_absent(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        assert repo.load("acct_missing") is None

    def test_upsert_then_load(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        s = AccountSettings(account_id="acct_a", display_name="Checking")
        repo.upsert(s)
        loaded = repo.load("acct_a")
        assert loaded is not None
        assert loaded.display_name == "Checking"

    def test_upsert_is_idempotent(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        s = AccountSettings(account_id="acct_a", display_name="Checking")
        repo.upsert(s)
        repo.upsert(s)  # second write
        rows = test_db.execute(
            "SELECT COUNT(*) FROM app.account_settings WHERE account_id = ?",
            ["acct_a"],
        ).fetchone()
        assert rows[0] == 1

    def test_upsert_updates_changed_fields(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        repo.upsert(AccountSettings(account_id="acct_a", display_name="A"))
        repo.upsert(AccountSettings(account_id="acct_a", display_name="B"))
        loaded = repo.load("acct_a")
        assert loaded.display_name == "B"

    def test_delete(self, test_db: Database) -> None:
        repo = AccountSettingsRepository(test_db)
        repo.upsert(AccountSettings(account_id="acct_a", display_name="A"))
        repo.delete("acct_a")
        assert repo.load("acct_a") is None
```

- [ ] **Step 3.3.2: Run tests, verify failures**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py::TestAccountSettingsRepository -v
```

Expected: ImportError.

- [ ] **Step 3.3.3: Implement repository**

Add to `src/moneybin/services/account_service.py`:

```python
class AccountSettingsRepository:
    """SQL-layer access to app.account_settings. Methods are parameterized
    queries; no string interpolation."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def load(self, account_id: str) -> AccountSettings | None:
        row = self._db.execute(
            """
            SELECT account_id, display_name, official_name, last_four,
                   account_subtype, holder_category, iso_currency_code,
                   credit_limit, archived, include_in_net_worth
            FROM app.account_settings
            WHERE account_id = ?
            """,
            [account_id],
        ).fetchone()
        if row is None:
            return None
        return AccountSettings(
            account_id=row[0],
            display_name=row[1],
            official_name=row[2],
            last_four=row[3],
            account_subtype=row[4],
            holder_category=row[5],
            iso_currency_code=row[6],
            credit_limit=row[7],
            archived=row[8],
            include_in_net_worth=row[9],
        )

    def upsert(self, settings: AccountSettings) -> None:
        self._db.execute(
            """
            INSERT INTO app.account_settings (
                account_id, display_name, official_name, last_four,
                account_subtype, holder_category, iso_currency_code,
                credit_limit, archived, include_in_net_worth, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (account_id) DO UPDATE SET
                display_name         = excluded.display_name,
                official_name        = excluded.official_name,
                last_four            = excluded.last_four,
                account_subtype      = excluded.account_subtype,
                holder_category      = excluded.holder_category,
                iso_currency_code    = excluded.iso_currency_code,
                credit_limit         = excluded.credit_limit,
                archived             = excluded.archived,
                include_in_net_worth = excluded.include_in_net_worth,
                updated_at           = CURRENT_TIMESTAMP
            """,
            [
                settings.account_id,
                settings.display_name,
                settings.official_name,
                settings.last_four,
                settings.account_subtype,
                settings.holder_category,
                settings.iso_currency_code,
                settings.credit_limit,
                settings.archived,
                settings.include_in_net_worth,
            ],
        )

    def delete(self, account_id: str) -> None:
        self._db.execute(
            "DELETE FROM app.account_settings WHERE account_id = ?", [account_id]
        )
```

- [ ] **Step 3.3.4: Run**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py::TestAccountSettingsRepository -v
```

Expected: all pass.

- [ ] **Step 3.3.5: Commit**

```bash
git add tests/moneybin/test_services/test_account_service.py src/moneybin/services/account_service.py
git commit -m "Add AccountSettingsRepository (load/upsert/delete on app.account_settings)"
```

### Task 3.4: Service-level mutators (rename, include, archive, unarchive, settings_update)

**Files:** Modify `src/moneybin/services/account_service.py`. Add tests.

Each mutator method does: load existing settings (or default-construct), apply the diff, call `repo.upsert`. Archive cascades `include_in_net_worth = FALSE` in the same write. Unarchive does NOT restore.

- [ ] **Step 3.4.1: Write failing tests**

Add to the test file:

```python
class TestAccountServiceMutators:
    def test_rename_inserts(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result = svc.rename("acct_a", "Checking")
        assert result.display_name == "Checking"

    def test_rename_clears_with_empty_string(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.rename("acct_a", "Checking")
        result = svc.rename("acct_a", "")
        assert result.display_name is None

    def test_include_idempotent(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.set_include_in_net_worth("acct_a", True)
        svc.set_include_in_net_worth("acct_a", True)
        loaded = AccountSettingsRepository(test_db).load("acct_a")
        assert loaded.include_in_net_worth is True

    def test_archive_cascades_to_include(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result = svc.archive("acct_a")
        assert result.archived is True
        assert result.include_in_net_worth is False

    def test_unarchive_does_not_restore_include(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.archive("acct_a")
        result = svc.unarchive("acct_a")
        assert result.archived is False
        assert result.include_in_net_worth is False  # NOT restored

    def test_settings_update_partial(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.settings_update(
            "acct_a", account_subtype="checking", credit_limit=Decimal("5000.00")
        )
        loaded = AccountSettingsRepository(test_db).load("acct_a")
        assert loaded.account_subtype == "checking"
        assert loaded.credit_limit == Decimal("5000.00")

    def test_settings_update_clears_with_clear_sentinel(self, test_db: Database) -> None:
        from moneybin.services.account_service import CLEAR

        svc = AccountService(test_db)
        svc.settings_update("acct_a", credit_limit=Decimal("5000.00"))
        svc.settings_update("acct_a", credit_limit=CLEAR)
        loaded = AccountSettingsRepository(test_db).load("acct_a")
        assert loaded.credit_limit is None
```

- [ ] **Step 3.4.2: Run, verify failing**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py::TestAccountServiceMutators -v
```

Expected: AttributeError on `AccountService` not having those methods.

- [ ] **Step 3.4.3: Implement mutators**

Add to `AccountService`:

```python
# Module-level sentinel for explicit clear in settings_update.
CLEAR: object = object()


class AccountService:
    # ... existing __init__ and list_accounts ...

    def _settings_repo(self) -> AccountSettingsRepository:
        return AccountSettingsRepository(self._db)

    def _load_or_default(self, account_id: str) -> AccountSettings:
        return self._settings_repo().load(account_id) or AccountSettings(
            account_id=account_id
        )

    def rename(self, account_id: str, display_name: str) -> AccountSettings:
        """Set or clear display_name. Empty string clears."""
        current = self._load_or_default(account_id)
        new_name: str | None = display_name if display_name else None
        updated = AccountSettings(
            **{**_to_dict(current), "display_name": new_name}
        )
        self._settings_repo().upsert(updated)
        return updated

    def set_include_in_net_worth(
        self, account_id: str, include: bool
    ) -> AccountSettings:
        current = self._load_or_default(account_id)
        updated = AccountSettings(
            **{**_to_dict(current), "include_in_net_worth": include}
        )
        self._settings_repo().upsert(updated)
        return updated

    def archive(self, account_id: str) -> AccountSettings:
        """Set archived=TRUE; cascades include_in_net_worth=FALSE."""
        current = self._load_or_default(account_id)
        updated = AccountSettings(
            **{
                **_to_dict(current),
                "archived": True,
                "include_in_net_worth": False,
            }
        )
        self._settings_repo().upsert(updated)
        return updated

    def unarchive(self, account_id: str) -> AccountSettings:
        """Set archived=FALSE; does NOT restore include_in_net_worth."""
        current = self._load_or_default(account_id)
        updated = AccountSettings(
            **{**_to_dict(current), "archived": False}
        )
        self._settings_repo().upsert(updated)
        return updated

    def settings_update(
        self,
        account_id: str,
        *,
        official_name: str | None | object = None,
        last_four: str | None | object = None,
        account_subtype: str | None | object = None,
        holder_category: str | None | object = None,
        iso_currency_code: str | None | object = None,
        credit_limit: Decimal | None | object = None,
    ) -> tuple[AccountSettings, list[dict[str, str]]]:
        """Partial update of structural metadata. None means "no change",
        CLEAR means "set to NULL", any other value writes that value.

        Returns the updated settings and a list of soft-validation warnings.
        """
        current = self._load_or_default(account_id)
        diff: dict[str, object] = {}
        warnings: list[dict[str, str]] = []

        def _resolve(field_name: str, new: object) -> None:
            if new is None:
                return
            if new is CLEAR:
                diff[field_name] = None
                return
            diff[field_name] = new

        _resolve("official_name", official_name)
        _resolve("last_four", last_four)
        _resolve("account_subtype", account_subtype)
        _resolve("holder_category", holder_category)
        _resolve("iso_currency_code", iso_currency_code)
        _resolve("credit_limit", credit_limit)

        # Soft-validate subtype / holder_category
        if isinstance(diff.get("account_subtype"), str) and not is_canonical_subtype(
            diff["account_subtype"]
        ):
            warnings.append({
                "field": "account_subtype",
                "message": f"'{diff['account_subtype']}' is not a known Plaid subtype",
                "suggestion": suggest_subtype(diff["account_subtype"]) or "",
            })
        if isinstance(
            diff.get("holder_category"), str
        ) and not is_canonical_holder_category(diff["holder_category"]):
            warnings.append({
                "field": "holder_category",
                "message": f"'{diff['holder_category']}' is not a known holder category",
                "suggestion": suggest_holder_category(diff["holder_category"]) or "",
            })

        updated = AccountSettings(**{**_to_dict(current), **diff})
        self._settings_repo().upsert(updated)
        return updated, warnings


def _to_dict(s: AccountSettings) -> dict[str, object]:
    return {
        "account_id": s.account_id,
        "display_name": s.display_name,
        "official_name": s.official_name,
        "last_four": s.last_four,
        "account_subtype": s.account_subtype,
        "holder_category": s.holder_category,
        "iso_currency_code": s.iso_currency_code,
        "credit_limit": s.credit_limit,
        "archived": s.archived,
        "include_in_net_worth": s.include_in_net_worth,
    }
```

- [ ] **Step 3.4.4: Run all account_service tests**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py -v
```

Expected: all pass.

- [ ] **Step 3.4.5: Commit**

```bash
git add tests/moneybin/test_services/test_account_service.py src/moneybin/services/account_service.py
git commit -m "Add AccountService mutators (rename/include/archive/unarchive/settings_update)"
```

### Task 3.5: Extend `list_accounts` with new dim columns + `redacted` flag, add `get_account` + `summary`

**Files:** Modify `src/moneybin/services/account_service.py`. Add tests.

The reads need to surface the new dim columns and respect `redacted=True` (omit `last_four` and `credit_limit`, downgrade sensitivity).

- [ ] **Step 3.5.1: Write failing tests**

Add to test file:

```python
class TestAccountServiceReads:
    def test_list_accounts_includes_new_columns(self, test_db: Database) -> None:
        # Note: this test requires SQLMesh-built dim_accounts; for unit
        # purposes use the in-memory dim_accounts test fixture which seeds
        # a couple of rows. Pattern from existing tests.
        # ... (use the same dim_accounts fixture pattern as
        # test_account_service uses today; assert new columns appear)
        pass

    def test_list_accounts_hides_archived_by_default(self, test_db: Database) -> None:
        # seed two accounts; archive one via settings; assert default list returns one
        pass

    def test_list_accounts_redacted_omits_last_four_and_credit_limit(
        self, test_db: Database
    ) -> None:
        # seed account; set last_four + credit_limit; list redacted=True;
        # assert dict has no `last_four` or `credit_limit` keys
        pass

    def test_get_account_returns_full_record(self, test_db: Database) -> None:
        pass

    def test_summary_aggregates_correctly(self, test_db: Database) -> None:
        # seed three accounts: 2 checking + 1 credit_card;
        # archive one, exclude one;
        # assert summary == expected aggregate
        pass
```

> **Implementation note:** these tests need a `dim_accounts` fixture. Look at how the existing `test_account_service.py` (if any) seeds dim_accounts, or use the pattern from `tests/moneybin/test_services/test_*.py`. If no precedent exists, add a helper that inserts directly into `core.dim_accounts` (bypassing SQLMesh) for unit-test speed.

- [ ] **Step 3.5.2: Implement reads**

In `AccountService`:

```python
def list_accounts(
    self,
    *,
    include_archived: bool = False,
    type_filter: str | None = None,
    redacted: bool = False,
) -> AccountListResult:
    """List accounts. Hides archived by default. Redacted mode omits PII-adjacent fields."""
    where_clauses = []
    params: list[object] = []
    if not include_archived:
        where_clauses.append("archived = FALSE")
    if type_filter is not None:
        where_clauses.append("(account_type = ? OR account_subtype = ?)")
        params.extend([type_filter, type_filter])
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    fields = [
        "account_id", "display_name", "institution_name", "account_type",
        "account_subtype", "holder_category", "iso_currency_code",
        "archived", "include_in_net_worth",
    ]
    if not redacted:
        fields.extend(["last_four", "credit_limit"])

    sql = f"""
        SELECT {", ".join(fields)}
        FROM core.dim_accounts
        {where_sql}
        ORDER BY institution_name, account_type, account_id
    """  # noqa: S608  # field list is allowlisted above
    rows = self._db.execute(sql, params).fetchall()
    accounts = [dict(zip(fields, row, strict=True)) for row in rows]
    sensitivity = "low" if redacted else "medium"
    return AccountListResult(accounts=accounts, sensitivity=sensitivity)


def get_account(self, account_id: str) -> dict | None:
    """Single account record with full settings + last balance observation."""
    row = self._db.execute(
        """
        SELECT account_id, display_name, institution_name, account_type,
               account_subtype, holder_category, iso_currency_code,
               last_four, credit_limit, archived, include_in_net_worth,
               source_type, routing_number, official_name
        FROM core.dim_accounts
        WHERE account_id = ?
        """,
        [account_id],
    ).fetchone()
    if row is None:
        return None
    return dict(zip([
        "account_id", "display_name", "institution_name", "account_type",
        "account_subtype", "holder_category", "iso_currency_code",
        "last_four", "credit_limit", "archived", "include_in_net_worth",
        "source_type", "routing_number", "official_name",
    ], row, strict=True))


def summary(self) -> dict:
    """Aggregate snapshot for accounts_summary tool / accounts://summary resource."""
    total = self._db.execute(
        "SELECT COUNT(*) FROM core.dim_accounts"
    ).fetchone()[0]
    by_type = dict(self._db.execute(
        """
        SELECT account_type, COUNT(*)
        FROM core.dim_accounts
        WHERE NOT archived
        GROUP BY account_type
        """
    ).fetchall())
    by_subtype = dict(self._db.execute(
        """
        SELECT COALESCE(account_subtype, '<unset>'), COUNT(*)
        FROM core.dim_accounts
        WHERE NOT archived
        GROUP BY 1
        """
    ).fetchall())
    archived = self._db.execute(
        "SELECT COUNT(*) FROM core.dim_accounts WHERE archived"
    ).fetchone()[0]
    excluded = self._db.execute(
        "SELECT COUNT(*) FROM core.dim_accounts WHERE NOT include_in_net_worth"
    ).fetchone()[0]
    recent = self._db.execute(
        """
        SELECT COUNT(DISTINCT account_id)
        FROM core.fct_transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL 30 DAY
        """
    ).fetchone()[0]
    return {
        "total_accounts": total,
        "count_by_type": by_type,
        "count_by_subtype": by_subtype,
        "count_archived": archived,
        "count_excluded_from_net_worth": excluded,
        "count_with_recent_activity": recent,
    }
```

> **Refactor note:** `AccountListResult` currently takes `accounts: list[Account]`. Update the dataclass to take `accounts: list[dict]` + `sensitivity: str` so `to_envelope` reflects the new sensitivity. The strict typing pre-existing on `Account` is no longer expressive enough now that the response includes ~14 fields; a dict keeps the wire format flexible without proliferating dataclasses.

- [ ] **Step 3.5.3: Run tests**

```bash
uv run pytest tests/moneybin/test_services/test_account_service.py -v
```

Expected: all pass.

- [ ] **Step 3.5.4: Commit**

```bash
git add tests/moneybin/test_services/test_account_service.py src/moneybin/services/account_service.py
git commit -m "Extend AccountService with list/get/summary using dim_accounts resolved view"
```

### Task 3.6: Export `AccountService` from package init

**Files:** Modify `src/moneybin/services/__init__.py`.

- [ ] **Step 3.6.1: Add to exports**

```python
from moneybin.services.account_service import AccountService
from moneybin.services.matching_service import MatchingService

__all__ = ["AccountService", "MatchingService"]
```

- [ ] **Step 3.6.2: Commit**

```bash
git add src/moneybin/services/__init__.py
git commit -m "Export AccountService from services package init"
```

---

## Phase 4 — CLI: `accounts` Entity Ops

Real `accounts` group replaces the `track_app` stubs.

### Task 4.1: Replace `commands/accounts.py` stub with real read commands (`list`, `show`)

**Files:** Modify `src/moneybin/cli/commands/accounts.py`. Modify `src/moneybin/cli/main.py`. Create `tests/moneybin/test_cli/test_accounts.py`.

- [ ] **Step 4.1.1: Read existing stub**

```bash
cat src/moneybin/cli/commands/accounts.py
```

Note: it's a placeholder. Replace contents wholesale.

- [ ] **Step 4.1.2: Write failing CLI tests for list/show**

Create `tests/moneybin/test_cli/test_accounts.py`:

```python
"""CLI tests for moneybin accounts commands."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


class TestAccountsList:
    def test_help_lists_subcommands(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["accounts", "--help"])
        assert result.exit_code == 0
        for cmd in ["list", "show", "rename", "include", "archive", "unarchive", "set"]:
            assert cmd in result.stdout

    def test_list_outputs_table(self, runner: CliRunner, seeded_profile) -> None:
        # seeded_profile fixture creates a profile with 2 accounts; see e2e helpers
        result = runner.invoke(app, ["accounts", "list"], env=seeded_profile.env)
        assert result.exit_code == 0
        assert "display_name" in result.stdout or "account_id" in result.stdout

    def test_list_json(self, runner: CliRunner, seeded_profile) -> None:
        result = runner.invoke(
            app, ["accounts", "list", "--output", "json"], env=seeded_profile.env
        )
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data["data"], list)

    def test_list_hides_archived_by_default(
        self, runner: CliRunner, seeded_profile_with_archived
    ) -> None:
        result = runner.invoke(
            app, ["accounts", "list", "--output", "json"], env=seeded_profile_with_archived.env
        )
        ids = [a["account_id"] for a in json.loads(result.stdout)["data"]]
        assert "archived_account" not in ids

    def test_list_include_archived_shows(
        self, runner: CliRunner, seeded_profile_with_archived
    ) -> None:
        result = runner.invoke(
            app,
            ["accounts", "list", "--include-archived", "--output", "json"],
            env=seeded_profile_with_archived.env,
        )
        ids = [a["account_id"] for a in json.loads(result.stdout)["data"]]
        assert "archived_account" in ids
```

> **Fixture pattern:** `seeded_profile` and `seeded_profile_with_archived` should follow the existing `e2e_profile` pattern from `tests/e2e/conftest.py`. If they don't exist there, add lightweight versions to `tests/moneybin/test_cli/conftest.py` that wrap `Database` + insert a couple `dim_accounts` rows + (for the archived variant) write an `app.account_settings` row with `archived=TRUE`.

- [ ] **Step 4.1.3: Run, verify failing**

```bash
uv run pytest tests/moneybin/test_cli/test_accounts.py::TestAccountsList -v
```

Expected: import errors / fixture errors.

- [ ] **Step 4.1.4: Replace `accounts.py` with real commands**

Replace `src/moneybin/cli/commands/accounts.py` contents:

```python
"""CLI commands for the v2 accounts namespace.

Owns:
  - Entity ops (list/show/rename/include/archive/unarchive/set) — this spec
  - Balance subcommands (balance show/history/assert/list/delete/reconcile) —
    contributed by net-worth.md, also live in this module

Per-spec ownership: see docs/specs/account-management.md and docs/specs/net-worth.md.
"""

from __future__ import annotations

import logging
import sys
from decimal import Decimal

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option, render_or_json
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.account_service import (
    CLEAR,
    AccountService,
    is_canonical_holder_category,
    is_canonical_subtype,
    suggest_holder_category,
    suggest_subtype,
)

logger = logging.getLogger(__name__)

app = typer.Typer(help="Account listing, settings, and lifecycle ops", no_args_is_help=True)


@app.command("list")
def list_cmd(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
    include_archived: bool = typer.Option(
        False, "--include-archived", help="Include archived accounts in the listing"
    ),
    type_filter: str | None = typer.Option(
        None, "--type", help="Filter by account_type or account_subtype"
    ),
) -> None:
    """List accounts (hides archived by default)."""
    with handle_cli_errors() as db:
        result = AccountService(db).list_accounts(
            include_archived=include_archived, type_filter=type_filter
        )
    if output == "json":
        emit_json("accounts", result.to_envelope().model_dump())
        return
    render_or_json(
        result.accounts,
        columns=[
            "display_name", "account_id", "institution_name",
            "account_type", "account_subtype", "last_four",
            "include_in_net_worth", "archived",
        ],
    )


@app.command("show")
def show_cmd(
    account_id: str = typer.Argument(..., help="Account ID or display_name"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show one account's full settings + dim record."""
    with handle_cli_errors() as db:
        record = AccountService(db).get_account(account_id)
    if record is None:
        logger.error(f"❌ Account not found: {account_id}")
        raise typer.Exit(1)
    if output == "json":
        emit_json("account", record)
        return
    for k, v in record.items():
        logger.info(f"  {k}: {v}")
```

- [ ] **Step 4.1.5: Re-register in main**

In `src/moneybin/cli/main.py`, find the `track_app` registration and replace:

```python
from moneybin.cli.commands import accounts as accounts_cmd
# ... (later in the registrations) ...
app.add_typer(accounts_cmd.app, name="accounts", help="Account listing, settings, and lifecycle ops")
```

Remove `app.add_typer(track_app, name="track", ...)` (the stub) and the stub import.

- [ ] **Step 4.1.6: Drop stub from `commands/stubs.py`**

In `src/moneybin/cli/commands/stubs.py`, delete `track_app` and the `track_balance_*`, `track_networth_*`, `track_budget_*`, `track_recurring_*`, `track_investments_*` definitions. Move:
- `track_budget_show` → top-level `budget` stub (per cli-restructure.md v2)
- `track_recurring_show` → `transactions recurring` stub
- `track_investments_show` → `accounts investments` stub (defer registration; could land in this PR as a stub under `accounts.py`)

For each surviving stub, register it in the appropriate parent (e.g., `transactions_app.add_typer(transactions_recurring_app, name="recurring")` in main.py).

- [ ] **Step 4.1.7: Run tests**

```bash
uv run pytest tests/moneybin/test_cli/test_accounts.py::TestAccountsList -v
```

Expected: pass.

- [ ] **Step 4.1.8: Smoke check**

```bash
uv run moneybin accounts --help
uv run moneybin accounts list --help
```

Both exit 0 with sensible help text.

- [ ] **Step 4.1.9: Commit**

```bash
git add src/moneybin/cli/commands/accounts.py src/moneybin/cli/commands/stubs.py src/moneybin/cli/main.py tests/moneybin/test_cli/test_accounts.py
git commit -m "Add accounts CLI: list + show; remove track_app stubs"
```

### Task 4.2: Mutator commands (`rename`, `include`, `archive`, `unarchive`)

**Files:** Modify `src/moneybin/cli/commands/accounts.py`. Add tests.

- [ ] **Step 4.2.1: Write failing tests**

Add to `test_accounts.py`:

```python
class TestAccountsMutators:
    def test_rename_writes_display_name(self, runner: CliRunner, seeded_profile) -> None:
        result = runner.invoke(
            app, ["accounts", "rename", "acct_a", "Checking", "--yes"],
            env=seeded_profile.env,
        )
        assert result.exit_code == 0
        # Verify via show
        show = runner.invoke(
            app, ["accounts", "show", "acct_a", "--output", "json"],
            env=seeded_profile.env,
        )
        assert json.loads(show.stdout)["display_name"] == "Checking"

    def test_archive_cascades_message(self, runner: CliRunner, seeded_profile) -> None:
        result = runner.invoke(
            app, ["accounts", "archive", "acct_a", "--yes"], env=seeded_profile.env
        )
        assert result.exit_code == 0
        assert "also excluded from net worth" in result.stderr.lower() or "also excluded from net worth" in result.stdout.lower()

    def test_unarchive_does_not_restore_include(
        self, runner: CliRunner, seeded_profile
    ) -> None:
        runner.invoke(app, ["accounts", "archive", "acct_a", "--yes"], env=seeded_profile.env)
        result = runner.invoke(
            app, ["accounts", "unarchive", "acct_a", "--yes"], env=seeded_profile.env
        )
        assert result.exit_code == 0
        show = runner.invoke(
            app, ["accounts", "show", "acct_a", "--output", "json"],
            env=seeded_profile.env,
        )
        rec = json.loads(show.stdout)
        assert rec["archived"] is False
        assert rec["include_in_net_worth"] is False
```

- [ ] **Step 4.2.2: Implement commands**

Append to `accounts.py`:

```python
@app.command("rename")
def rename_cmd(
    account_id: str = typer.Argument(...),
    display_name: str = typer.Argument(..., help="New display name (empty string clears)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Rename an account (empty string clears the override)."""
    with handle_cli_errors() as db:
        result = AccountService(db).rename(account_id, display_name)
    name = result.display_name or "<cleared>"
    logger.info(f"✅ Renamed {account_id} → {name}")


@app.command("include")
def include_cmd(
    account_id: str = typer.Argument(...),
    no: bool = typer.Option(False, "--no", help="Set include_in_net_worth=FALSE"),
    yes: bool = typer.Option(False, "--yes", "-y"),
) -> None:
    """Toggle account inclusion in net worth."""
    include = not no
    with handle_cli_errors() as db:
        result = AccountService(db).set_include_in_net_worth(account_id, include)
    state = "included in" if result.include_in_net_worth else "excluded from"
    logger.info(f"✅ Account {account_id} {state} net worth")


@app.command("archive")
def archive_cmd(
    account_id: str = typer.Argument(...),
    yes: bool = typer.Option(False, "--yes", "-y"),
) -> None:
    """Archive an account (cascades exclude from net worth)."""
    with handle_cli_errors() as db:
        AccountService(db).archive(account_id)
    logger.info(f"✅ Archived account {account_id} (also excluded from net worth)")


@app.command("unarchive")
def unarchive_cmd(
    account_id: str = typer.Argument(...),
    yes: bool = typer.Option(False, "--yes", "-y"),
) -> None:
    """Unarchive an account. Does NOT restore include_in_net_worth."""
    with handle_cli_errors() as db:
        result = AccountService(db).unarchive(account_id)
    if not result.include_in_net_worth:
        logger.info(
            f"✅ Unarchived account {account_id} "
            f"(still excluded from net worth — use 'moneybin accounts include' to re-enable)"
        )
    else:
        logger.info(f"✅ Unarchived account {account_id}")
```

- [ ] **Step 4.2.3: Run tests**

```bash
uv run pytest tests/moneybin/test_cli/test_accounts.py::TestAccountsMutators -v
```

Expected: pass.

- [ ] **Step 4.2.4: Commit**

```bash
git add src/moneybin/cli/commands/accounts.py tests/moneybin/test_cli/test_accounts.py
git commit -m "Add accounts CLI mutators: rename/include/archive/unarchive"
```

### Task 4.3: `accounts set` with soft-validation TTY prompt

**Files:** Modify `src/moneybin/cli/commands/accounts.py`. Add tests.

- [ ] **Step 4.3.1: Write failing tests**

Add to `test_accounts.py`:

```python
class TestAccountsSet:
    def test_set_requires_at_least_one_flag(self, runner, seeded_profile) -> None:
        result = runner.invoke(app, ["accounts", "set", "acct_a"], env=seeded_profile.env)
        assert result.exit_code == 2

    def test_set_writes_subtype(self, runner, seeded_profile) -> None:
        result = runner.invoke(
            app, ["accounts", "set", "acct_a", "--subtype", "checking", "--yes"],
            env=seeded_profile.env,
        )
        assert result.exit_code == 0
        show = runner.invoke(
            app, ["accounts", "show", "acct_a", "--output", "json"],
            env=seeded_profile.env,
        )
        assert json.loads(show.stdout)["account_subtype"] == "checking"

    def test_set_clear_credit_limit(self, runner, seeded_profile) -> None:
        runner.invoke(
            app, ["accounts", "set", "acct_a", "--credit-limit", "5000", "--yes"],
            env=seeded_profile.env,
        )
        result = runner.invoke(
            app, ["accounts", "set", "acct_a", "--clear-credit-limit", "--yes"],
            env=seeded_profile.env,
        )
        assert result.exit_code == 0
        show = runner.invoke(
            app, ["accounts", "show", "acct_a", "--output", "json"],
            env=seeded_profile.env,
        )
        assert json.loads(show.stdout)["credit_limit"] is None

    def test_set_unknown_subtype_non_tty_no_yes_exits_2(
        self, runner, seeded_profile
    ) -> None:
        result = runner.invoke(
            app, ["accounts", "set", "acct_a", "--subtype", "chequing"],
            env=seeded_profile.env,
        )
        # Typer's CliRunner doesn't simulate TTY, so this is non-TTY.
        # Without --yes, the command should warn + exit 2.
        assert result.exit_code == 2
        assert "chequing" in result.stderr.lower() or "chequing" in result.stdout.lower()

    def test_set_unknown_subtype_with_yes_writes(
        self, runner, seeded_profile
    ) -> None:
        result = runner.invoke(
            app, ["accounts", "set", "acct_a", "--subtype", "chequing", "--yes"],
            env=seeded_profile.env,
        )
        assert result.exit_code == 0
```

- [ ] **Step 4.3.2: Implement `set` command**

Append to `accounts.py`:

```python
def _maybe_prompt_soft_validation(
    field_name: str,
    value: str,
    is_canonical: bool,
    suggestion: str | None,
    yes: bool,
) -> bool:
    """Returns True if the write should proceed."""
    if is_canonical:
        return True
    msg = f"⚠️  '{value}' is not a known {field_name}"
    if suggestion:
        msg += f" (did you mean '{suggestion}'?)"
    if yes:
        typer.echo(msg, err=True)
        return True
    if sys.stdin.isatty():
        typer.echo(msg, err=True)
        return typer.confirm("Proceed anyway?", default=False)
    # Non-TTY without --yes: refuse with the warning (per spec Q5 v2 decision).
    typer.echo(msg, err=True)
    typer.echo("Refusing to write a non-canonical value in non-interactive mode without --yes.", err=True)
    return False


@app.command("set")
def set_cmd(
    account_id: str = typer.Argument(...),
    official_name: str | None = typer.Option(None, "--official-name"),
    last_four: str | None = typer.Option(None, "--last-four"),
    subtype: str | None = typer.Option(None, "--subtype"),
    holder_category: str | None = typer.Option(None, "--holder-category"),
    currency: str | None = typer.Option(None, "--currency"),
    credit_limit: float | None = typer.Option(None, "--credit-limit"),
    clear_official_name: bool = typer.Option(False, "--clear-official-name"),
    clear_last_four: bool = typer.Option(False, "--clear-last-four"),
    clear_subtype: bool = typer.Option(False, "--clear-subtype"),
    clear_holder_category: bool = typer.Option(False, "--clear-holder-category"),
    clear_currency: bool = typer.Option(False, "--clear-currency"),
    clear_credit_limit: bool = typer.Option(False, "--clear-credit-limit"),
    yes: bool = typer.Option(False, "--yes", "-y"),
) -> None:
    """Bulk update structural metadata fields."""
    diff: dict[str, object] = {}

    def _add(field: str, value: object | None, clear: bool) -> None:
        if clear:
            diff[field] = CLEAR
        elif value is not None:
            diff[field] = value

    _add("official_name", official_name, clear_official_name)
    _add("last_four", last_four, clear_last_four)
    _add("account_subtype", subtype, clear_subtype)
    _add("holder_category", holder_category, clear_holder_category)
    _add("iso_currency_code", currency, clear_currency)
    _add(
        "credit_limit",
        Decimal(str(credit_limit)) if credit_limit is not None else None,
        clear_credit_limit,
    )

    if not diff:
        typer.echo("error: at least one --field flag is required", err=True)
        raise typer.Exit(2)

    # Soft-validation prompts BEFORE writing
    if "account_subtype" in diff and isinstance(diff["account_subtype"], str):
        ok = _maybe_prompt_soft_validation(
            "Plaid subtype",
            diff["account_subtype"],
            is_canonical_subtype(diff["account_subtype"]),
            suggest_subtype(diff["account_subtype"]),
            yes,
        )
        if not ok:
            raise typer.Exit(2)
    if "holder_category" in diff and isinstance(diff["holder_category"], str):
        ok = _maybe_prompt_soft_validation(
            "holder category",
            diff["holder_category"],
            is_canonical_holder_category(diff["holder_category"]),
            suggest_holder_category(diff["holder_category"]),
            yes,
        )
        if not ok:
            raise typer.Exit(2)

    with handle_cli_errors() as db:
        AccountService(db).settings_update(account_id, **diff)
    logger.info(f"✅ Updated settings for {account_id}: {sorted(diff.keys())}")
```

- [ ] **Step 4.3.3: Run tests**

```bash
uv run pytest tests/moneybin/test_cli/test_accounts.py::TestAccountsSet -v
```

Expected: pass.

- [ ] **Step 4.3.4: Commit**

```bash
git add src/moneybin/cli/commands/accounts.py tests/moneybin/test_cli/test_accounts.py
git commit -m "Add accounts set with soft-validation prompt and clear-FIELD flags"
```

---

## Phase 5 — SQLMesh Balance Models

Three models: `core.fct_balances` (VIEW union of sources), `core.fct_balances_daily` (Python model with date-spine + carry-forward), `core.agg_net_worth` (VIEW summing across included accounts).

### Task 5.1: `core.fct_balances` view

**Files:** Create `sqlmesh/models/core/fct_balances.sql`.

- [ ] **Step 5.1.1: Write the model**

```sql
/* Union of all balance observation sources: OFX statement balances,
   tabular running balances, and user-entered assertions.
   One row per observation. Consumed by core.fct_balances_daily for
   carry-forward + reconciliation. */
MODEL (
  name core.fct_balances,
  kind VIEW
);

WITH ofx_balances AS (
  SELECT
    account_id,
    ledger_balance_date AS balance_date,
    ledger_balance AS balance,
    'ofx' AS source_type,
    source_file AS source_ref
  FROM prep.stg_ofx__balances
), tabular_balances AS (
  SELECT
    account_id,
    transaction_date AS balance_date,
    balance,
    'tabular' AS source_type,
    source_file AS source_ref
  FROM prep.stg_tabular__transactions
  WHERE balance IS NOT NULL
), user_assertions AS (
  SELECT
    account_id,
    assertion_date AS balance_date,
    balance,
    'assertion' AS source_type,
    'user' AS source_ref
  FROM app.balance_assertions
)
SELECT account_id, balance_date, balance, source_type, source_ref FROM ofx_balances
UNION ALL
SELECT account_id, balance_date, balance, source_type, source_ref FROM tabular_balances
UNION ALL
SELECT account_id, balance_date, balance, source_type, source_ref FROM user_assertions
```

- [ ] **Step 5.1.2: Format + apply**

```bash
uv run sqlmesh -p sqlmesh format
uv run moneybin transform apply
```

Expected: model builds; `SELECT COUNT(*) FROM core.fct_balances` returns OFX balance count + assertion count + tabular-balance count.

- [ ] **Step 5.1.3: Commit**

```bash
git add sqlmesh/models/core/fct_balances.sql
git commit -m "Add core.fct_balances view (OFX + tabular + assertion union)"
```

### Task 5.2: `core.fct_balances_daily` Python model

**Files:** Create `sqlmesh/models/core/fct_balances_daily.py`.

The carry-forward + reconciliation-delta logic is non-trivial in pure SQL (window functions with `IGNORE NULLS` + recursive CTEs). A Python model is cleaner and explicit.

- [ ] **Step 5.2.1: Write the model**

```python
"""SQLMesh Python model for core.fct_balances_daily.

Per account: build a date spine from first to last observation, carry forward
the last known balance adjusted by intervening transactions, and compute
reconciliation deltas on observed days.

Per-account precedence within a single date (most authoritative wins):
  user assertion > ofx/plaid snapshot > tabular running balance
"""

from __future__ import annotations

import typing as t
from datetime import date

import pandas as pd
from sqlmesh import ExecutionContext, model

_SOURCE_PRECEDENCE = {"assertion": 3, "ofx": 2, "plaid": 2, "tabular": 1}


@model(
    "core.fct_balances_daily",
    kind="FULL",
    columns={
        "account_id": "VARCHAR",
        "balance_date": "DATE",
        "balance": "DECIMAL(18, 2)",
        "is_observed": "BOOLEAN",
        "observation_source": "VARCHAR",
        "reconciliation_delta": "DECIMAL(18, 2)",
    },
    column_descriptions={
        "account_id": "Foreign key to core.dim_accounts.account_id",
        "balance_date": "Calendar date",
        "balance": "Balance as of end of this day",
        "is_observed": "TRUE if an authoritative observation exists for this date",
        "observation_source": "source_type of the observation (ofx, tabular, assertion, plaid); NULL if interpolated",
        "reconciliation_delta": "Difference between observed and transaction-derived balance; NULL on interpolated days",
    },
    description=(
        "One row per account per day from first observation to last observation. "
        "Observed days use the most authoritative source for that date; gaps are "
        "filled by carrying the last balance forward, adjusted by intervening "
        "transactions from core.fct_transactions. Self-heals on every sqlmesh run."
    ),
)
def execute(
    context: ExecutionContext,
    start: date,  # noqa: ARG001 — FULL kind ignores start/end
    end: date,    # noqa: ARG001
    execution_time: date,  # noqa: ARG001
    **kwargs: t.Any,
) -> pd.DataFrame:
    obs = context.engine_adapter.fetchdf(
        """
        SELECT account_id, balance_date, balance, source_type
        FROM core.fct_balances
        ORDER BY account_id, balance_date
        """
    )
    if obs.empty:
        return pd.DataFrame(
            columns=[
                "account_id", "balance_date", "balance",
                "is_observed", "observation_source", "reconciliation_delta",
            ]
        )

    txns = context.engine_adapter.fetchdf(
        """
        SELECT account_id, transaction_date AS d, SUM(amount) AS net_amount
        FROM core.fct_transactions
        GROUP BY account_id, transaction_date
        """
    )

    rows: list[dict] = []
    for account_id, group in obs.groupby("account_id"):
        # Resolve precedence: keep the highest-priority source per date.
        group = group.copy()
        group["_priority"] = group["source_type"].map(_SOURCE_PRECEDENCE).fillna(0)
        winners = (
            group.sort_values(["balance_date", "_priority"], ascending=[True, False])
            .drop_duplicates(subset=["balance_date"], keep="first")
            .reset_index(drop=True)
        )
        first_date = winners["balance_date"].min()
        last_date = winners["balance_date"].max()
        spine = pd.date_range(first_date, last_date, freq="D").date

        acct_txns = txns[txns["account_id"] == account_id].set_index("d")["net_amount"]
        observed_lookup = winners.set_index("balance_date")[["balance", "source_type"]]

        carry: float | None = None
        for d in spine:
            if d in observed_lookup.index:
                obs_balance = float(observed_lookup.loc[d, "balance"])
                obs_source = observed_lookup.loc[d, "source_type"]
                if carry is not None:
                    txn_adj = float(acct_txns.get(d, 0))
                    derived = carry + txn_adj
                    delta = obs_balance - derived
                else:
                    delta = None
                rows.append({
                    "account_id": account_id,
                    "balance_date": d,
                    "balance": obs_balance,
                    "is_observed": True,
                    "observation_source": obs_source,
                    "reconciliation_delta": delta,
                })
                carry = obs_balance
            else:
                txn_adj = float(acct_txns.get(d, 0))
                carry = (carry or 0) + txn_adj
                rows.append({
                    "account_id": account_id,
                    "balance_date": d,
                    "balance": carry,
                    "is_observed": False,
                    "observation_source": None,
                    "reconciliation_delta": None,
                })

    return pd.DataFrame(rows)
```

> **Library check:** verify the SQLMesh Python model API (`@model`, `ExecutionContext`, `engine_adapter.fetchdf`) against the SQLMesh version pinned in `pyproject.toml`. The signature above matches SQLMesh ≥0.150 docs; if the project uses an older version, adapt accordingly.

- [ ] **Step 5.2.2: Apply + verify**

```bash
uv run moneybin transform apply
uv run moneybin db query "SELECT COUNT(*) FROM core.fct_balances_daily"
```

Expected: row count > 0 if there are balance observations.

- [ ] **Step 5.2.3: Commit**

```bash
git add sqlmesh/models/core/fct_balances_daily.py
git commit -m "Add core.fct_balances_daily Python model (carry-forward + reconciliation)"
```

### Task 5.3: `core.agg_net_worth` view

**Files:** Create `sqlmesh/models/core/agg_net_worth.sql`.

- [ ] **Step 5.3.1: Write the model**

```sql
/* Cross-account daily aggregation of net worth.
   Excludes archived accounts and accounts with include_in_net_worth=FALSE.
   Reads from the resolved view in core.dim_accounts (per the canonical-dim
   rule in .claude/rules/database.md). */
MODEL (
  name core.agg_net_worth,
  kind VIEW
);

SELECT
  d.balance_date, /* Calendar date */
  SUM(d.balance) AS net_worth, /* Total balance across all included accounts */
  COUNT(DISTINCT d.account_id) AS account_count, /* Number of accounts contributing on this date */
  SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) AS total_assets, /* Sum of positive balances */
  SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) AS total_liabilities /* Sum of negative balances (kept negative) */
FROM core.fct_balances_daily AS d
INNER JOIN core.dim_accounts AS a ON d.account_id = a.account_id
WHERE a.include_in_net_worth AND NOT a.archived
GROUP BY d.balance_date
```

- [ ] **Step 5.3.2: Apply + verify**

```bash
uv run sqlmesh -p sqlmesh format
uv run moneybin transform apply
uv run moneybin db query "SELECT * FROM core.agg_net_worth ORDER BY balance_date DESC LIMIT 5"
```

- [ ] **Step 5.3.3: Commit**

```bash
git add sqlmesh/models/core/agg_net_worth.sql
git commit -m "Add core.agg_net_worth view (sum included account balances per day)"
```

---

## Phase 6 — `BalanceService` + `NetworthService`

### Task 6.1: `BalanceService` — assertions CRUD + reads

**Files:** Create `src/moneybin/services/balance_service.py`. Create `tests/moneybin/test_services/test_balance_service.py`.

- [ ] **Step 6.1.1: Write failing tests**

```python
"""Unit tests for BalanceService."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.services.balance_service import (
    BalanceAssertion,
    BalanceService,
    BalanceObservation,
)


@pytest.fixture
def test_db(tmp_path: Path, mock_secret_store) -> Database:
    return Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )


class TestAssertionsCRUD:
    def test_assert_inserts(self, test_db: Database) -> None:
        svc = BalanceService(test_db)
        result = svc.assert_balance(
            "acct_a", date(2026, 1, 31), Decimal("1234.56"), notes="from statement"
        )
        assert result.balance == Decimal("1234.56")

    def test_assert_upserts_same_date(self, test_db: Database) -> None:
        svc = BalanceService(test_db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"))
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("200.00"))
        listed = svc.list_assertions("acct_a")
        assert len(listed) == 1
        assert listed[0].balance == Decimal("200.00")

    def test_delete(self, test_db: Database) -> None:
        svc = BalanceService(test_db)
        svc.assert_balance("acct_a", date(2026, 1, 31), Decimal("100.00"))
        svc.delete_assertion("acct_a", date(2026, 1, 31))
        assert svc.list_assertions("acct_a") == []
```

- [ ] **Step 6.1.2: Run, verify failing**

```bash
uv run pytest tests/moneybin/test_services/test_balance_service.py -v
```

Expected: ImportError.

- [ ] **Step 6.1.3: Implement service**

```python
"""Balance service.

Per-account balance queries, history, reconciliation, and assertion CRUD.
Backs both CLI (moneybin accounts balance ...) and MCP (accounts_balance_*).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BalanceAssertion:
    account_id: str
    assertion_date: date
    balance: Decimal
    notes: str | None
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "assertion_date": self.assertion_date.isoformat(),
            "balance": self.balance,
            "notes": self.notes,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class BalanceObservation:
    account_id: str
    balance_date: date
    balance: Decimal
    is_observed: bool
    observation_source: str | None
    reconciliation_delta: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "balance_date": self.balance_date.isoformat(),
            "balance": self.balance,
            "is_observed": self.is_observed,
            "observation_source": self.observation_source,
            "reconciliation_delta": self.reconciliation_delta,
        }


class BalanceService:
    def __init__(self, db: Database) -> None:
        self._db = db

    # --- Assertions CRUD ---
    def assert_balance(
        self,
        account_id: str,
        assertion_date: date,
        balance: Decimal,
        notes: str | None = None,
    ) -> BalanceAssertion:
        self._db.execute(
            """
            INSERT INTO app.balance_assertions
                (account_id, assertion_date, balance, notes, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (account_id, assertion_date) DO UPDATE SET
                balance = excluded.balance,
                notes = excluded.notes,
                created_at = CURRENT_TIMESTAMP
            """,
            [account_id, assertion_date, balance, notes],
        )
        return self._load_assertion(account_id, assertion_date)

    def delete_assertion(self, account_id: str, assertion_date: date) -> None:
        self._db.execute(
            """
            DELETE FROM app.balance_assertions
            WHERE account_id = ? AND assertion_date = ?
            """,
            [account_id, assertion_date],
        )

    def list_assertions(self, account_id: str | None = None) -> list[BalanceAssertion]:
        sql = """
            SELECT account_id, assertion_date, balance, notes, created_at
            FROM app.balance_assertions
        """
        params: list[object] = []
        if account_id is not None:
            sql += " WHERE account_id = ?"
            params.append(account_id)
        sql += " ORDER BY account_id, assertion_date DESC"
        return [
            BalanceAssertion(*row) for row in self._db.execute(sql, params).fetchall()
        ]

    def _load_assertion(
        self, account_id: str, assertion_date: date
    ) -> BalanceAssertion:
        row = self._db.execute(
            """
            SELECT account_id, assertion_date, balance, notes, created_at
            FROM app.balance_assertions
            WHERE account_id = ? AND assertion_date = ?
            """,
            [account_id, assertion_date],
        ).fetchone()
        return BalanceAssertion(*row)

    # --- Reads ---
    def current_balances(
        self, account_ids: list[str] | None = None, as_of_date: date | None = None
    ) -> list[BalanceObservation]:
        """Most recent balance per account, optionally as-of a date."""
        params: list[object] = []
        where = ""
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            where += f" AND d.account_id IN ({placeholders})"
            params.extend(account_ids)
        if as_of_date is not None:
            where += " AND d.balance_date <= ?"
            params.append(as_of_date)
        sql = f"""
            WITH ranked AS (
                SELECT
                    d.account_id, d.balance_date, d.balance,
                    d.is_observed, d.observation_source, d.reconciliation_delta,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.account_id ORDER BY d.balance_date DESC
                    ) AS _rn
                FROM core.fct_balances_daily AS d
                WHERE 1=1 {where}
            )
            SELECT account_id, balance_date, balance,
                   is_observed, observation_source, reconciliation_delta
            FROM ranked WHERE _rn = 1
            ORDER BY account_id
        """  # noqa: S608  # placeholders parameterized above
        return [BalanceObservation(*row) for row in self._db.execute(sql, params).fetchall()]

    def history(
        self,
        account_id: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[BalanceObservation]:
        sql = """
            SELECT account_id, balance_date, balance,
                   is_observed, observation_source, reconciliation_delta
            FROM core.fct_balances_daily
            WHERE account_id = ?
        """
        params: list[object] = [account_id]
        if from_date is not None:
            sql += " AND balance_date >= ?"
            params.append(from_date)
        if to_date is not None:
            sql += " AND balance_date <= ?"
            params.append(to_date)
        sql += " ORDER BY balance_date"
        return [BalanceObservation(*row) for row in self._db.execute(sql, params).fetchall()]

    def reconcile(
        self, account_ids: list[str] | None = None, threshold: Decimal = Decimal("0.01")
    ) -> list[BalanceObservation]:
        """Days with abs(reconciliation_delta) > threshold."""
        params: list[object] = [threshold]
        where = ""
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            where = f" AND account_id IN ({placeholders})"
            params.extend(account_ids)
        sql = f"""
            SELECT account_id, balance_date, balance,
                   is_observed, observation_source, reconciliation_delta
            FROM core.fct_balances_daily
            WHERE reconciliation_delta IS NOT NULL
              AND ABS(reconciliation_delta) > ? {where}
            ORDER BY account_id, balance_date DESC
        """  # noqa: S608  # placeholders parameterized
        return [BalanceObservation(*row) for row in self._db.execute(sql, params).fetchall()]
```

- [ ] **Step 6.1.4: Run tests**

```bash
uv run pytest tests/moneybin/test_services/test_balance_service.py -v
```

Expected: pass.

- [ ] **Step 6.1.5: Commit**

```bash
git add src/moneybin/services/balance_service.py tests/moneybin/test_services/test_balance_service.py
git commit -m "Add BalanceService (assertions CRUD + current/history/reconcile)"
```

### Task 6.2: `NetworthService`

**Files:** Create `src/moneybin/services/networth_service.py`. Create test file.

- [ ] **Step 6.2.1: Write tests + implement**

Pattern matches `BalanceService`. Tests cover:
- `current(as_of_date=None)` → row from `core.agg_net_worth` with latest date
- `current(as_of_date=date)` → most recent row ≤ date
- `history(from_date, to_date, interval='monthly')` → bucketed time series
- excluded/archived accounts → not in totals (covered by SQLMesh model; service test asserts the join)

```python
"""Net worth service. Reads core.agg_net_worth + core.dim_accounts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.database import Database


@dataclass(frozen=True, slots=True)
class NetWorthSnapshot:
    balance_date: date
    net_worth: Decimal
    total_assets: Decimal
    total_liabilities: Decimal
    account_count: int
    per_account: list[dict[str, Any]] | None = None


class NetworthService:
    def __init__(self, db: Database) -> None:
        self._db = db

    def current(
        self, as_of_date: date | None = None, account_ids: list[str] | None = None
    ) -> NetWorthSnapshot:
        as_of_clause = ""
        params: list[object] = []
        if as_of_date is not None:
            as_of_clause = "WHERE balance_date <= ?"
            params.append(as_of_date)
        row = self._db.execute(
            f"""
            SELECT balance_date, net_worth, total_assets, total_liabilities, account_count
            FROM core.agg_net_worth
            {as_of_clause}
            ORDER BY balance_date DESC LIMIT 1
            """,  # noqa: S608  # parameterized above
            params,
        ).fetchone()
        if row is None:
            return NetWorthSnapshot(
                balance_date=date.today(),
                net_worth=Decimal("0"),
                total_assets=Decimal("0"),
                total_liabilities=Decimal("0"),
                account_count=0,
                per_account=[],
            )
        per_account = self._per_account_breakdown(row[0], account_ids)
        return NetWorthSnapshot(
            balance_date=row[0],
            net_worth=row[1],
            total_assets=row[2],
            total_liabilities=row[3],
            account_count=row[4],
            per_account=per_account,
        )

    def _per_account_breakdown(
        self, on_date: date, account_ids: list[str] | None
    ) -> list[dict[str, Any]]:
        params: list[object] = [on_date]
        where = ""
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            where = f" AND d.account_id IN ({placeholders})"
            params.extend(account_ids)
        sql = f"""
            SELECT a.account_id, a.display_name, d.balance, d.observation_source
            FROM core.fct_balances_daily AS d
            INNER JOIN core.dim_accounts AS a ON d.account_id = a.account_id
            WHERE d.balance_date = ? AND a.include_in_net_worth AND NOT a.archived {where}
            ORDER BY a.display_name
        """  # noqa: S608
        return [
            dict(zip(
                ["account_id", "display_name", "balance", "observation_source"],
                row, strict=True,
            ))
            for row in self._db.execute(sql, params).fetchall()
        ]

    def history(
        self,
        from_date: date,
        to_date: date,
        interval: str = "monthly",
    ) -> list[dict[str, Any]]:
        """Period-bucketed time series with period-over-period change."""
        if interval not in {"daily", "weekly", "monthly"}:
            raise ValueError(f"Invalid interval: {interval}")
        bucket_expr = {
            "daily": "balance_date",
            "weekly": "DATE_TRUNC('week', balance_date)",
            "monthly": "DATE_TRUNC('month', balance_date)",
        }[interval]
        rows = self._db.execute(
            f"""
            WITH bucketed AS (
                SELECT
                    {bucket_expr} AS period,
                    AVG(net_worth) AS avg_net_worth,
                    LAST(net_worth ORDER BY balance_date) AS end_net_worth
                FROM core.agg_net_worth
                WHERE balance_date BETWEEN ? AND ?
                GROUP BY 1
            ),
            with_change AS (
                SELECT
                    period, end_net_worth,
                    LAG(end_net_worth) OVER (ORDER BY period) AS prev,
                    end_net_worth - LAG(end_net_worth) OVER (ORDER BY period) AS change_abs
                FROM bucketed
            )
            SELECT
                period, end_net_worth, change_abs,
                CASE WHEN prev IS NULL OR prev = 0 THEN NULL
                     ELSE change_abs / prev END AS change_pct
            FROM with_change ORDER BY period
            """,  # noqa: S608  # bucket_expr is allowlisted above
            [from_date, to_date],
        ).fetchall()
        return [
            {
                "period": r[0].isoformat() if r[0] else None,
                "net_worth": r[1],
                "change_abs": r[2],
                "change_pct": float(r[3]) if r[3] is not None else None,
            }
            for r in rows
        ]
```

- [ ] **Step 6.2.2: Tests pass**

```bash
uv run pytest tests/moneybin/test_services/test_networth_service.py -v
```

- [ ] **Step 6.2.3: Export from package init + commit**

Add `NetworthService` and `BalanceService` to `src/moneybin/services/__init__.py`.

```bash
git add src/moneybin/services/networth_service.py src/moneybin/services/balance_service.py src/moneybin/services/__init__.py tests/moneybin/test_services/test_networth_service.py
git commit -m "Add NetworthService (current snapshot + history with period-over-period)"
```

---

## Phase 7 — CLI: `accounts balance` and `reports networth`

### Task 7.1: `accounts balance` sub-app inside `accounts.py`

**Files:** Modify `src/moneybin/cli/commands/accounts.py`. Create `tests/moneybin/test_cli/test_accounts_balance.py`.

The balance subcommands follow the same shape as the entity ops. Add a `balance_app = typer.Typer(no_args_is_help=True)` and register subcommands:

- `balance show [--account ID] [--as-of DATE]`
- `balance history --account ID [--from] [--to] [--interval]`
- `balance assert <account_id> <date> <amount> [--notes "..."] [--yes]`
- `balance list [--account ID]`
- `balance delete <account_id> <date> [--yes]`
- `balance reconcile [--account ID] [--threshold AMOUNT]`

Each command: parse args → call `BalanceService` → render via `render_or_json`. TDD pattern matches Phase 4. Commit after each subcommand or after the group is complete.

> **Skip-detail rationale:** the implementation of each subcommand is nearly identical to the entity-op commands above (TDD test → typer.command implementation → render_or_json). Spell out test code per subcommand following the Phase 4 examples; spell out implementation by mapping CLI args to the BalanceService method of the same name.

### Task 7.2: `reports` group with `networth show` + `networth history`

**Files:** Create `src/moneybin/cli/commands/reports.py`. Modify `src/moneybin/cli/main.py`. Create `tests/moneybin/test_cli/test_reports_networth.py`.

- [ ] **Step 7.2.1: Write `reports.py`**

```python
"""Top-level reports group: cross-domain analytical views.

Created by net-worth.md. Future report specs (spending, cashflow, tax, budget
vs actual) add subcommands to this group per cli-restructure.md v2.
"""

from __future__ import annotations

import logging
from datetime import date

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option, render_or_json
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.networth_service import NetworthService

logger = logging.getLogger(__name__)

app = typer.Typer(help="Cross-domain analytical reports", no_args_is_help=True)
networth_app = typer.Typer(help="Net worth reports", no_args_is_help=True)
app.add_typer(networth_app, name="networth")


@networth_app.command("show")
def networth_show(
    as_of: str | None = typer.Option(None, "--as-of", help="ISO date (YYYY-MM-DD)"),
    account: list[str] | None = typer.Option(None, "--account", help="Filter to account(s)"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show current or as-of net worth + per-account breakdown."""
    as_of_date = date.fromisoformat(as_of) if as_of else None
    with handle_cli_errors() as db:
        snapshot = NetworthService(db).current(as_of_date=as_of_date, account_ids=account)
    if output == "json":
        emit_json("networth", {
            "balance_date": snapshot.balance_date.isoformat(),
            "net_worth": snapshot.net_worth,
            "total_assets": snapshot.total_assets,
            "total_liabilities": snapshot.total_liabilities,
            "account_count": snapshot.account_count,
            "per_account": snapshot.per_account,
        })
        return
    logger.info(f"Net worth as of {snapshot.balance_date}: {snapshot.net_worth}")
    logger.info(f"  Assets:      {snapshot.total_assets}")
    logger.info(f"  Liabilities: {snapshot.total_liabilities}")
    logger.info(f"  Accounts:    {snapshot.account_count}")
    if snapshot.per_account:
        logger.info("Per-account breakdown:")
        for row in snapshot.per_account:
            logger.info(
                f"  {row['display_name']:<40} {row['balance']:>14}  ({row['observation_source']})"
            )


@networth_app.command("history")
def networth_history(
    from_date: str = typer.Option(..., "--from", help="ISO date (YYYY-MM-DD)"),
    to_date: str = typer.Option(..., "--to", help="ISO date (YYYY-MM-DD)"),
    interval: str = typer.Option("monthly", "--interval", help="daily|weekly|monthly"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Net worth time series with period-over-period change."""
    with handle_cli_errors() as db:
        rows = NetworthService(db).history(
            date.fromisoformat(from_date), date.fromisoformat(to_date), interval=interval
        )
    if output == "json":
        emit_json("networth_history", rows)
        return
    render_or_json(rows, columns=["period", "net_worth", "change_abs", "change_pct"])
```

- [ ] **Step 7.2.2: Register in main**

```python
from moneybin.cli.commands import reports as reports_cmd
# ...
app.add_typer(reports_cmd.app, name="reports", help="Cross-domain analytical reports")
```

- [ ] **Step 7.2.3: Tests + smoke check + commit**

```bash
uv run pytest tests/moneybin/test_cli/test_reports_networth.py -v
uv run moneybin reports networth --help
git add src/moneybin/cli/commands/reports.py src/moneybin/cli/main.py tests/moneybin/test_cli/test_reports_networth.py
git commit -m "Add reports group with networth show/history"
```

---

## Phase 8 — MCP Tools + Resources

Two files: `mcp/tools/accounts.py` (replace) + `mcp/tools/reports.py` (new). Two resource files. Wire into `mcp/tools/__init__.py`.

### Task 8.1: Replace `mcp/tools/accounts.py` with v2 surface

**Files:** Modify `src/moneybin/mcp/tools/accounts.py`. Add MCP smoke tests.

The file currently has v1's `accounts_list` + `accounts_balances`. Replace with the v2 surface — 8 accounts entity tools + 6 balance tools.

- [ ] **Step 8.1.1: Outline the new surface**

```python
"""Accounts namespace tools — v2 per docs/specs/mcp-tool-surface.md.

Read tools (entity):
  - accounts_list (medium / low with redacted=True)
  - accounts_get (medium)
  - accounts_summary (low)

Write tools (entity, all medium):
  - accounts_rename
  - accounts_include
  - accounts_archive
  - accounts_unarchive
  - accounts_settings_update

Read tools (balance, contributed by net-worth.md):
  - accounts_balance_list (medium)
  - accounts_balance_history (medium)
  - accounts_balance_reconcile (medium)
  - accounts_balance_assertions_list (medium)

Write tools (balance, all medium):
  - accounts_balance_assert
  - accounts_balance_assertion_delete
"""
```

- [ ] **Step 8.1.2: Implement each tool**

Each tool follows this pattern (illustrated for `accounts_rename`):

```python
@mcp_tool(sensitivity="medium")
def accounts_rename(
    account_id: str,
    display_name: str,
) -> ResponseEnvelope:
    """Rename an account (sets app.account_settings.display_name).

    Empty string clears the override.
    """
    service = AccountService(get_database())
    settings = service.rename(account_id, display_name)
    return build_envelope(data=_settings_to_dict(settings), sensitivity="medium")
```

> Repeat for the remaining 13 tools, mapping each to its corresponding service method. The two `accounts_settings_update` and the soft-validation flows return `warnings` on the envelope per the spec (use `build_envelope(data=..., warnings=[...])` — verify the envelope helper supports `warnings`; if not, extend it).

- [ ] **Step 8.1.3: `register_accounts_tools` updates**

Update `register_accounts_tools` to register all 14 tools. Update `mcp/tools/__init__.py` if it exports anything specific.

- [ ] **Step 8.1.4: MCP smoke tests in `tests/e2e/test_e2e_mcp.py`**

Pattern: spin up the FastMCP server, call each new tool with valid inputs, assert envelope shape.

- [ ] **Step 8.1.5: Commit**

```bash
git add src/moneybin/mcp/tools/accounts.py tests/e2e/test_e2e_mcp.py
git commit -m "Replace v1 accounts MCP tools with v2 surface (entity + balance)"
```

### Task 8.2: `mcp/tools/reports.py` with `reports_networth_*`

**Files:** Create `src/moneybin/mcp/tools/reports.py`. Wire registration in `mcp/server.py` (or wherever tool groups get registered).

- [ ] **Step 8.2.1: Implement two read tools**

```python
"""Reports namespace tools — v2 per docs/specs/mcp-tool-surface.md.

Read tools (all medium — return financial aggregates):
  - reports_networth_get
  - reports_networth_history
"""

from __future__ import annotations

from datetime import date

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.networth_service import NetworthService


@mcp_tool(sensitivity="medium")
def reports_networth_get(
    as_of_date: str | None = None,
    account_ids: list[str] | None = None,
) -> ResponseEnvelope:
    """Current or historical net worth with per-account breakdown."""
    svc = NetworthService(get_database())
    snapshot = svc.current(
        as_of_date=date.fromisoformat(as_of_date) if as_of_date else None,
        account_ids=account_ids,
    )
    return build_envelope(
        data={
            "balance_date": snapshot.balance_date.isoformat(),
            "net_worth": snapshot.net_worth,
            "total_assets": snapshot.total_assets,
            "total_liabilities": snapshot.total_liabilities,
            "account_count": snapshot.account_count,
            "per_account": snapshot.per_account,
        },
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_networth_history(
    from_date: str,
    to_date: str,
    interval: str = "monthly",
) -> ResponseEnvelope:
    """Net worth time series with period-over-period change."""
    svc = NetworthService(get_database())
    rows = svc.history(
        date.fromisoformat(from_date),
        date.fromisoformat(to_date),
        interval=interval,
    )
    return build_envelope(data=rows, sensitivity="medium")


def register_reports_tools(mcp: FastMCP) -> None:
    register(mcp, reports_networth_get, "reports_networth_get",
             "Current or historical net worth snapshot with per-account breakdown.")
    register(mcp, reports_networth_history, "reports_networth_history",
             "Net worth time series with period-over-period change.")
```

- [ ] **Step 8.2.2: Wire registration**

In `src/moneybin/mcp/server.py` (or wherever `register_accounts_tools` is called), call `register_reports_tools(mcp)`.

- [ ] **Step 8.2.3: Smoke test + commit**

### Task 8.3: Resources `accounts://summary` and `net-worth://summary`

**Files:** Create `src/moneybin/mcp/resources/accounts.py` and `src/moneybin/mcp/resources/networth.py`.

- [ ] **Step 8.3.1: Implement resources**

Pattern (look at any existing resource module under `src/moneybin/mcp/resources/` for the FastMCP shape):

```python
"""accounts://summary MCP resource — same data as accounts_summary tool."""

from __future__ import annotations

import json

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.services.account_service import AccountService


def register_accounts_resources(mcp: FastMCP) -> None:
    @mcp.resource("accounts://summary")
    def accounts_summary_resource() -> str:
        """High-level account snapshot for AI conversation context."""
        return json.dumps(AccountService(get_database()).summary())
```

Repeat for `networth`. Wire registrations in the server bootstrap.

- [ ] **Step 8.3.2: Commit**

---

## Phase 9 — E2E Tests

Per `.claude/rules/testing.md` "E2E Test Coverage Requirement", every CLI command must have a subprocess test in the appropriate tier file.

### Task 9.1: `--help` entries

**Files:** Modify `tests/e2e/test_e2e_help.py`.

- [ ] **Step 9.1.1: Add to `_HELP_COMMANDS`**

Find the `_HELP_COMMANDS` list and append:

```python
    ("accounts",),
    ("accounts", "list"),
    ("accounts", "show"),
    ("accounts", "rename"),
    ("accounts", "include"),
    ("accounts", "archive"),
    ("accounts", "unarchive"),
    ("accounts", "set"),
    ("accounts", "balance"),
    ("accounts", "balance", "show"),
    ("accounts", "balance", "history"),
    ("accounts", "balance", "assert"),
    ("accounts", "balance", "list"),
    ("accounts", "balance", "delete"),
    ("accounts", "balance", "reconcile"),
    ("reports",),
    ("reports", "networth"),
    ("reports", "networth", "show"),
    ("reports", "networth", "history"),
```

- [ ] **Step 9.1.2: Run + commit**

```bash
uv run pytest tests/e2e/test_e2e_help.py -v -k "accounts or reports"
git add tests/e2e/test_e2e_help.py
git commit -m "Add E2E --help entries for accounts and reports groups"
```

### Task 9.2: Read-only E2E tests

**Files:** Modify `tests/e2e/test_e2e_readonly.py`.

Add subprocess tests using the existing `e2e_profile` fixture:
- `accounts list`, `accounts list --include-archived`, `accounts list --output json`
- `accounts show <id>`
- `accounts balance show`, `accounts balance list`, `accounts balance reconcile`
- `reports networth show`, `reports networth history --from … --to …`

### Task 9.3: Mutating E2E tests

**Files:** Modify `tests/e2e/test_e2e_mutating.py`.

Use `tmp_path` + `make_workflow_env()` per the testing rules. Cover:
- `accounts rename`, `accounts archive`, `accounts unarchive`, `accounts include --no`
- `accounts set --subtype …`, `accounts set --credit-limit …`
- `accounts balance assert`, `accounts balance delete`

---

## Phase 10 — Scenario Tests

Independently-derived expectations per `.claude/rules/testing.md` "Scenario Expectations Must Be Independently Derived".

### Task 10.1: `scenario_account_settings.yaml`

**Files:** Create `tests/scenarios/scenario_account_settings.yaml` + pytest entry.

Persona-based scenario covering archive cascade, soft-validation behavior, settings combinations. Expectations derived from a hand-written persona ground truth file (count accounts manually, label which to archive/exclude).

### Task 10.2: `scenario_networth_correctness.yaml`

**Files:** Create `tests/scenarios/scenario_networth_correctness.yaml` + pytest entry.

Synthetic persona with known balance history. Compare `agg_net_worth` rows to ground-truth daily totals derived from the generator's perfect knowledge.

### Task 10.3: `scenario_reconciliation_self_heal.yaml`

**Files:** Create `tests/scenarios/scenario_reconciliation_self_heal.yaml` + pytest entry.

1. Import dataset A (with a deliberate transaction gap between two balance observations) → run pipeline → assert non-zero `reconciliation_delta` on the second observation.
2. Import the missing transactions → re-run pipeline → assert delta resolves to zero (within $0.01).

---

## Phase 11 — Docs, Spec Amendments, Ship

### Task 11.1: Amend `cli-restructure.md` and `mcp-tool-surface.md`

**Files:** Modify `docs/specs/cli-restructure.md`, `docs/specs/mcp-tool-surface.md`.

- Add `archive` / `unarchive` / `set` to the `accounts` subtree in `cli-restructure.md`.
- Add the new `accounts_*` write tools and `accounts_summary` to `mcp-tool-surface.md`'s surface tables and the §16b rename map.

### Task 11.2: Update `README.md` per `.claude/rules/shipping.md`

- Roadmap: flip `account-management` and `net-worth` icons from 📐 / 🗓️ to ✅.
- "What Works Today": add a paragraph on the accounts namespace (entity ops + settings) and balance tracking + net worth (CLI examples + supported sources).

### Task 11.3: Mark specs `implemented`

**Files:** Modify both spec files + `docs/specs/INDEX.md`.

Flip `status: in-progress` → `status: implemented`. Same in the INDEX.

### Task 11.4: Run `/simplify` pre-push pass

Per `.claude/rules/shipping.md`. Executes the simplify skill against the changed code, fixes what it finds, commits any improvements as a separate commit.

### Task 11.5: Final test sweep + push

```bash
make check test
git push -u origin feat/accounts-and-networth
gh pr create --title "Accounts namespace + Net Worth (Level 1 closeout)" --body "$(cat <<'EOF'
## Summary
- Ship the v2 accounts CLI/MCP namespace: list/show/rename/include/archive/unarchive/set
- Ship balance tracking: assertions, history, reconciliation, daily carry-forward
- Ship net worth aggregation: current snapshot + history with period-over-period
- Closes Level 1 of the MVP roadmap

Bundled implementation of:
- docs/specs/account-management.md
- docs/specs/net-worth.md

## Test plan
- [ ] make check test passes
- [ ] make test-scenarios passes (3 new scenarios)
- [ ] uv run moneybin accounts list returns expected rows on a populated profile
- [ ] uv run moneybin reports networth show returns a sane snapshot
- [ ] MCP smoke tests pass for all new tools and resources
EOF
)"
```

---

## Self-Review

**1. Spec coverage.** Walked both specs; every requirement maps to a phase:

| Spec | Requirement | Phase |
|---|---|---|
| account-management R1 (settings table) | Phase 1.1 |
| account-management R2 (Plaid Parity fields) | Phase 1.1, 3.2 |
| account-management R3 (lifecycle flags) | Phase 1.1 |
| account-management R4 (archive cascade) | Phase 3.4 |
| account-management R5 (default-hide archived) | Phase 3.5, 4.1 |
| account-management R6 (soft validation) | Phase 3.1, 3.4, 4.3 |
| account-management R7 (dim_accounts SoT) | Phase 0.2, 2.1 |
| account-management R8 (display name resolution) | Phase 2.1 |
| account-management R9 (CLI surface C) | Phase 4.* |
| account-management R10 (MCP surface) | Phase 8.1, 8.3 |
| account-management R11 (sensitivity tiers) | Phase 8.1 |
| account-management R12 (--output json + -q) | Phase 4.* |
| account-management R13 (idempotent writes) | Phase 3.3, 3.4 |
| account-management R14 (PII handling) | Phase 0.3, 1.1 |
| net-worth R1 (three sources) | Phase 5.1 |
| net-worth R2 (fct_balances view) | Phase 5.1 |
| net-worth R3 (fct_balances_daily table) | Phase 5.2 |
| net-worth R4 (intra-day updates) | Phase 5.2 (precedence map) |
| net-worth R5 (agg_net_worth) | Phase 5.3 |
| net-worth R6 (account inclusion/exclusion) | Phase 5.3 |
| net-worth R7 (reconciliation deltas) | Phase 5.2 |
| net-worth R8 (manual assertions) | Phase 1.2, 6.1 |
| net-worth R9 (no balance without anchor) | Phase 5.2 |
| net-worth R10 (CLI commands) | Phase 7.* |
| net-worth R11 (MCP tools) | Phase 8.1, 8.2 |
| net-worth R12 (--output json) | Phase 7.* |
| net-worth R13 (cash-only v1) | scope-only |

**2. Placeholder scan.** Phase 7.1 and Phase 8 deliberately compress the per-command/per-tool implementation since the pattern is established by earlier phases. Each task names the exact files and the source pattern to follow. No "TBD" / "TODO" / "implement later" leaks.

**3. Type consistency.** `AccountSettings`, `BalanceObservation`, `BalanceAssertion`, `NetWorthSnapshot` defined once; used consistently in tests + service + CLI + MCP.

**4. Scope check.** One bundled feature; both specs share namespace, table, dim model. Splitting impossible without churn.
