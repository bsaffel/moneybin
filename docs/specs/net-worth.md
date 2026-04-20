# Feature: Net Worth & Balance Tracking

## Status
draft

## Goal

Provide accurate, authoritative balance tracking per account and net worth computation over time. Balances are either sourced from authoritative observations (institution statements, Plaid snapshots, user assertions) or absent — never best-effort estimates from transaction sums alone.

## Background

Net worth over time is a headline feature that every competitor (Beancount, Firefly III, Actual, Maybe/Sure) ships. MoneyBin has the raw data — OFX balance snapshots, CSV running balances — but no materialized view or user-facing surface.

The core design principle: **accurate or absent, never best effort.** A balance derived solely from summing transactions without an authoritative anchor is unreliable (missing transactions, timing differences, fees not captured). Instead, this feature anchors on balance observations from institutions and interpolates between them using transaction data.

Related specs and docs:
- [`testing-synthetic-data.md`](testing-synthetic-data.md) — synthetic data generator produces balance observations and ground-truth balances
- [`testing-overview.md`](testing-overview.md) — umbrella verification infrastructure
- [`privacy-data-protection.md`](privacy-data-protection.md) — balance data encrypted at rest
- [`database-migration.md`](database-migration.md) — migration infrastructure for new tables
- `core-concerns.md` §8D — original requirements scaffold
- `core-concerns.md` §6 — reporting & analysis concerns
- `mvp-roadmap.md` Level 1 — net worth is a Level 1 deliverable

## Requirements

1. **Balance observations from four v1 sources:** OFX statement balances (`raw.ofx_balances`), CSV/tabular running balance columns (`raw.csv_transactions.balance`, `raw.tabular_transactions.balance`), Plaid balance snapshots (future, when `sync-plaid.md` ships), and user manual assertions (`app.balance_assertions`).
2. **Union all sources into `core.fct_balances`** — a SQLMesh VIEW that normalizes every balance observation to a common shape: `(account_id, balance_date, balance, source_type, source_ref)`.
3. **Materialize `core.fct_balances_daily`** — a SQLMesh TABLE with one row per account per day. For days with an authoritative observation, use it. For days between observations, carry forward the last known balance adjusted by intervening transactions from `core.fct_transactions`.
4. **Intra-day updates:** When multiple syncs or imports occur on the same day, the latest observation wins. `fct_balances_daily` reflects the most recent data after each `sqlmesh run`.
5. **Aggregate to `core.agg_net_worth`** — a SQLMesh VIEW that sums `fct_balances_daily` across all included accounts per day.
6. **Account inclusion/exclusion:** All accounts in `core.dim_accounts` are included by default. Users can exclude accounts via `app.account_settings` (new table with `include_in_net_worth BOOLEAN DEFAULT TRUE`). Excluded accounts still have daily balances computed but are omitted from `agg_net_worth`.
7. **Reconciliation deltas:** When the transaction-derived balance at a given date doesn't match the next authoritative observation, compute and surface the delta. Deltas are informational (not blocking) and self-heal — they are recomputed on every `sqlmesh run`, so reimporting missing transactions resolves them naturally.
8. **Manual balance assertions:** Users can assert a known balance via `moneybin balance assert <account_id> <date> <amount>`. Stored in `app.balance_assertions`. Serves as an authoritative observation alongside institution-provided balances.
9. **No balance without an anchor:** Accounts with zero balance observations produce no `fct_balances_daily` rows. The system does not estimate an opening balance from transactions alone.
10. **CLI commands:** `moneybin networth show`, `moneybin networth history`, `moneybin balance show`, `moneybin balance assert`, `moneybin balance list`, `moneybin balance delete`, `moneybin reconciliation show`.
11. **MCP tools:** `get_net_worth`, `get_net_worth_history`, `get_balances`, `get_balance_assertions`.
12. **All commands support `--output json`** for non-interactive parity.
13. **Cash-only v1.** Investment holdings and multi-currency conversion are future extensions (Level 2 and Level 3 respectively). Net worth v1 covers cash accounts only.

## Data Model

### New table: `app.balance_assertions`

User-entered balance anchors. Managed via CLI (`moneybin balance assert/list/delete`).

```sql
CREATE TABLE IF NOT EXISTS app.balance_assertions (
    account_id VARCHAR NOT NULL,    -- Foreign key to core.dim_accounts
    assertion_date DATE NOT NULL,   -- Date the balance was observed
    balance DECIMAL(18, 2) NOT NULL, -- Asserted balance amount
    notes VARCHAR,                  -- Optional user notes (e.g., "from paper statement")
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the assertion was entered
    PRIMARY KEY (account_id, assertion_date)
);
```

### New table: `app.account_settings`

Per-account configuration. Initially just net worth inclusion; extensible for future per-account settings.

```sql
CREATE TABLE IF NOT EXISTS app.account_settings (
    account_id VARCHAR NOT NULL PRIMARY KEY, -- Foreign key to core.dim_accounts
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE, -- Whether this account contributes to net worth
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last modification time
);
```

### SQLMesh model: `core.fct_balances` (VIEW)

Unions all balance observation sources into a normalized shape. One row per observation.

```sql
MODEL (
  name core.fct_balances,
  kind VIEW
);

WITH ofx_balances AS (
  SELECT
    account_id,
    ledger_balance_date::DATE AS balance_date,
    ledger_balance AS balance,
    'ofx' AS source_type,
    source_file AS source_ref
  FROM prep.stg_ofx__balances
),
csv_balances AS (
  -- CSV running balances: one observation per transaction row that has a non-NULL balance
  SELECT
    account_id,
    transaction_date AS balance_date,
    balance,
    'csv' AS source_type,
    source_file AS source_ref
  FROM prep.stg_csv__transactions
  WHERE balance IS NOT NULL
),
user_assertions AS (
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
SELECT account_id, balance_date, balance, source_type, source_ref FROM csv_balances
UNION ALL
SELECT account_id, balance_date, balance, source_type, source_ref FROM user_assertions
```

**Future extensions:** Add CTEs for `tabular_balances` (from `raw.tabular_transactions`) and `plaid_balances` (from Plaid sync) as those sources ship.

**Source precedence within a day:** When multiple observations exist for the same account on the same date, `fct_balances_daily` uses the latest/most authoritative. Precedence: user assertion > institution snapshot (OFX/Plaid) > running balance (CSV/tabular).

### SQLMesh model: `core.fct_balances_daily` (TABLE)

One row per account per day. Materialized as a FULL table — recomputed on every `sqlmesh run`.

For each account:
1. Find all authoritative balance observations from `fct_balances`.
2. For each day between the first and last observation, either use the observation (if one exists) or carry forward the prior balance adjusted by that day's net transaction amount from `fct_transactions`.
3. Compute the reconciliation delta: on days with an observation, delta = `observation - transaction_derived_balance`. On interpolated days, delta is NULL (no observation to compare against).

```
Columns:
  account_id        VARCHAR     -- Foreign key to core.dim_accounts
  balance_date      DATE        -- Calendar date
  balance           DECIMAL(18,2) -- Balance as of end of this day
  is_observed       BOOLEAN     -- TRUE if an authoritative observation exists for this date
  observation_source VARCHAR    -- source_type of the observation (ofx, csv, assertion, plaid); NULL if interpolated
  reconciliation_delta DECIMAL(18,2) -- Difference between observed and transaction-derived balance; NULL on interpolated days
```

**Implementation note:** The date spine generation and carry-forward logic may require a Python SQLMesh model or a recursive CTE depending on complexity. DuckDB's `generate_series` can produce the date spine; window functions with `IGNORE NULLS` can carry forward balances.

### SQLMesh model: `core.agg_net_worth` (VIEW)

Cross-account daily aggregation, excluding opted-out accounts.

```sql
MODEL (
  name core.agg_net_worth,
  kind VIEW
);

SELECT
  d.balance_date,
  SUM(d.balance) AS net_worth,             -- Total across all included accounts
  COUNT(DISTINCT d.account_id) AS account_count, -- Number of accounts contributing
  SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) AS total_assets,
  SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) AS total_liabilities
FROM core.fct_balances_daily AS d
LEFT JOIN app.account_settings AS s
  ON d.account_id = s.account_id
WHERE COALESCE(s.include_in_net_worth, TRUE)
GROUP BY d.balance_date
```

## CLI Interface

All commands support `--output json` for non-interactive parity.

### `networth` command group

```
moneybin networth show [--as-of DATE] [--account ACCOUNT_ID] [--output json|table]
```
- Default: net worth as of today (latest available date)
- `--as-of`: historical point-in-time lookup
- `--account`: filter to specific account(s), repeatable

```
moneybin networth history [--from DATE] [--to DATE] [--interval daily|weekly|monthly] [--output json|table]
```
- Default interval: monthly
- Shows net worth over time with period-over-period change (absolute and percentage)

### `balance` command group

```
moneybin balance show [--account ACCOUNT_ID] [--as-of DATE] [--output json|table]
```
- Default: latest known balance per account
- Shows: account name, balance, date of last observation, source (OFX/CSV/Plaid/assertion)

```
moneybin balance assert <account_id> <date> <amount> [--notes "reason"] [--yes]
```
- Inserts or updates a row in `app.balance_assertions`
- Validates `account_id` exists in `dim_accounts`

```
moneybin balance list [--account ACCOUNT_ID] [--output json|table]
```
- Shows all balance assertions, optionally filtered by account

```
moneybin balance delete <account_id> <date> [--yes]
```
- Removes a manual assertion
- `--yes` for non-interactive confirmation

### `reconciliation` command group

```
moneybin reconciliation show [--account ACCOUNT_ID] [--threshold AMOUNT] [--output json|table]
```
- Shows all accounts with non-zero reconciliation deltas
- `--threshold`: only show deltas exceeding this amount (default: $0.01)

## MCP Interface

### Tools

**`get_net_worth`** — Returns current or historical net worth.
- Params: `as_of_date` (optional DATE), `account_ids` (optional list of VARCHAR)
- Returns: total net worth, per-account breakdown with balance and source, currency

**`get_net_worth_history`** — Returns net worth time series.
- Params: `from_date` (DATE), `to_date` (DATE), `interval` (daily|weekly|monthly, default monthly)
- Returns: time series with net worth, period-over-period change (absolute and percentage), account count

**`get_balances`** — Returns current balance per account.
- Params: `account_ids` (optional list), `as_of_date` (optional DATE)
- Returns: per-account balance with date of last observation and source attribution

**`get_balance_assertions`** — Returns manual balance assertions.
- Params: `account_id` (optional VARCHAR)
- Returns: list of assertions with dates, amounts, and notes

### Resources

**`net-worth://summary`** — Current net worth snapshot. Useful as context for AI conversations about finances. Returns: total net worth, total assets, total liabilities, account count, as-of date.

### Write tools

Write tools for balance assertions deferred to v2. The `balance assert` CLI command handles the low-frequency assertion workflow for v1.

## Synthetic Data Requirements

The synthetic data generator (`testing-synthetic-data.md`) should produce data that exercises net worth computation:

- **Balance observations:** Personas should include OFX balance snapshots at statement dates (natural from OFX generation). These are the primary authoritative anchors.
- **CSV running balances:** Some persona accounts should include running balance columns in their CSV transactions to test that balance source.
- **Reconciliation gaps:** At least one persona should have a deliberate gap — missing transactions between two balance observations — to verify reconciliation delta computation.
- **Multi-account coverage:** Personas need multiple account types (checking, savings, credit card) to test cross-account aggregation in `agg_net_worth`.
- **Manual assertions:** Test fixtures (not generator output) should include `app.balance_assertions` rows to test the user assertion balance source.

**Ground-truth labels** needed in `synthetic.ground_truth`:
- Expected daily balance per account (derived from the generator's perfect knowledge of all transactions)
- Expected net worth per day (cross-account sum)

This lets `moneybin synthetic verify` validate that `fct_balances_daily` and `agg_net_worth` match the generator's known-correct values.

## Testing Strategy

### Tier 1 — Unit tests

- **Balance source extraction:** Verify each source (OFX, CSV/tabular, user assertion) produces correct `fct_balances` rows with the right shape and source attribution.
- **Daily interpolation:** Given known balance observations and transactions, verify `fct_balances_daily` fills gaps correctly using carry-forward + transaction adjustments.
- **Reconciliation delta computation:** Verify deltas appear when transactions don't fully account for the change between two observations. Verify deltas resolve when missing transactions are added and the pipeline re-runs.
- **Net worth aggregation:** Verify cross-account summing respects account inclusion/exclusion settings.
- **Edge cases:**
  - Accounts with no balance observations → no `fct_balances_daily` rows
  - Accounts with a single observation → one row on that date, no interpolation
  - Multiple observations on the same date from different sources → precedence applies
  - Account excluded from net worth → still in `fct_balances_daily`, absent from `agg_net_worth`

### Tier 2 — Synthetic data verification

- `moneybin synthetic verify` scenarios compare `fct_balances_daily` and `agg_net_worth` against ground-truth values from the generator.
- Reconciliation delta scenarios: persona with deliberate gaps produces expected non-zero deltas.

### Tier 3 — Integration

- End-to-end: import OFX file → `sqlmesh run` → `moneybin networth show` → verify output matches expected values.
- Manual assertion flow: `moneybin balance assert` → `sqlmesh run` → verify balance updated in `fct_balances_daily`.
- Intra-day re-sync: import twice on the same day → verify latest observation wins in `fct_balances_daily`.
- Reconciliation self-healing: import with gap → observe delta → reimport with missing data → verify delta resolves.

## Dependencies

- [`database-migration.md`](database-migration.md) — new tables (`app.balance_assertions`, `app.account_settings`) require migration infrastructure
- [`privacy-data-protection.md`](privacy-data-protection.md) — balance data is sensitive; encrypted at rest via `Database` class
- [`testing-synthetic-data.md`](testing-synthetic-data.md) — generator must produce balance observations and ground-truth daily balances
- SQLMesh pipeline — new models added to existing `prep/` → `core/` pipeline
- `core.dim_accounts` — account inclusion/exclusion references existing dimension
- `core.fct_transactions` — transaction amounts used for carry-forward interpolation between observations

## Out of Scope

- **Investment holdings in net worth** — Level 2 concern. Net worth v1 is cash-only. Investment tracking (`investment-tracking.md`, Wave 2) will extend `fct_balances` with holdings valuation when it ships.
- **Multi-currency conversion** — Level 3 concern. All balances assumed single-currency for v1. Multi-currency (`multi-currency.md`, Wave 3) will add home-currency conversion to `fct_balances_daily`.
- **Balance forecasting/projection** — "What will my net worth be in 6 months?" is a separate feature requiring trend extrapolation.
- **Balance alerts/notifications** — "Notify me when balance drops below X" is out of scope for the data model spec.
- **Historical balance backfill without an anchor** — If a user imports 2 years of transactions but only has balance observations from the last 3 months, we do not attempt to reconstruct balances before the first observation. Absent, not best-effort.
- **Plaid balance sync scheduling** — The polling/scheduling mechanism for Plaid balance snapshots belongs in `sync-plaid.md`, not here. This spec consumes whatever Plaid provides.
- **Net worth trend analysis and reporting** — Cash flow statements, trend charts, and category breakdowns belong in `net-worth-reporting.md` (Wave 3). This spec provides the data model they consume.

## Implementation Plan

### Files to Create

- `src/moneybin/sql/schema/app_balance_assertions.sql` — DDL for `app.balance_assertions`
- `src/moneybin/sql/schema/app_account_settings.sql` — DDL for `app.account_settings`
- `sqlmesh/models/core/fct_balances.sql` — VIEW unioning all balance sources
- `sqlmesh/models/core/fct_balances_daily.sql` — TABLE with daily carry-forward logic (may be Python model)
- `sqlmesh/models/core/agg_net_worth.sql` — VIEW aggregating across accounts
- `src/moneybin/cli/commands/networth.py` — `networth show`, `networth history`
- `src/moneybin/cli/commands/balance.py` — `balance show`, `balance assert`, `balance list`, `balance delete`
- `src/moneybin/cli/commands/reconciliation.py` — `reconciliation show`
- `src/moneybin/services/balance_service.py` — business logic for balance queries, assertions, reconciliation
- `src/moneybin/services/networth_service.py` — business logic for net worth queries
- `tests/test_balance_service.py` — unit tests for balance logic
- `tests/test_networth_service.py` — unit tests for net worth logic
- `tests/test_cli_networth.py` — CLI integration tests
- `tests/test_cli_balance.py` — CLI integration tests

### Files to Modify

- `src/moneybin/cli/main.py` — register `networth`, `balance`, `reconciliation` command groups
- `src/moneybin/sql/schema.py` — register new DDL files for `app.balance_assertions` and `app.account_settings`
- `src/moneybin/mcp/tools/` — add `get_net_worth`, `get_net_worth_history`, `get_balances`, `get_balance_assertions` tools
- `src/moneybin/mcp/resources/` — add `net-worth://summary` resource

### Key Decisions

1. **Accurate or absent.** No balance is computed without an authoritative observation anchor. This is the foundational invariant.
2. **Reconciliation deltas are computed, not stored.** They self-heal as data quality improves. No manual cleanup needed.
3. **`fct_balances_daily` is FULL materialized.** Recomputed on every `sqlmesh run`. Ensures consistency after any data change.
4. **Source precedence:** user assertion > institution snapshot > running balance. Most authoritative source wins within a day.
5. **Cash-only v1.** Investment and multi-currency extensions are designed to slot in without breaking changes.
6. **Three command groups** (`networth`, `balance`, `reconciliation`) — single-word, no hyphens, consistent with existing CLI conventions.
