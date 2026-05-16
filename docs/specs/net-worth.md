# Feature: Net Worth & Balance Tracking

## Status
implemented

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
- `mvp-roadmap.md` M1 — net worth is an M1 deliverable

## Requirements

1. **Balance observations from three v1 sources:** OFX statement balances (`raw.ofx_balances` → `prep.stg_ofx__balances`), tabular running balances (`raw.tabular_transactions.balance` → `prep.stg_tabular__transactions.balance`; CSV/TSV/Excel/Parquet all flow through the unified tabular pipeline), and user manual assertions (`app.balance_assertions`). Plaid balance snapshots are a future extension (gated on `sync-plaid.md`).
2. **Union all sources into `core.fct_balances`** — a SQLMesh VIEW that normalizes every balance observation to a common shape: `(account_id, balance_date, balance, source_type, source_ref)`.
3. **Materialize `core.fct_balances_daily`** — a SQLMesh TABLE with one row per account per day. For days with an authoritative observation, use it. For days between observations, carry forward the last known balance adjusted by intervening transactions from `core.fct_transactions`.
4. **Intra-day updates:** When multiple syncs or imports occur on the same day, the latest observation wins. `fct_balances_daily` reflects the most recent data after each `sqlmesh run`.
5. **Aggregate to `core.agg_net_worth`** — a SQLMesh VIEW that sums `fct_balances_daily` across all included accounts per day.
6. **Account inclusion/exclusion:** All accounts in `core.dim_accounts` are included by default. Users can exclude accounts via `app.account_settings` (new table with `include_in_net_worth BOOLEAN DEFAULT TRUE`). Excluded accounts still have daily balances computed but are omitted from `agg_net_worth`.
7. **Reconciliation deltas:** When the transaction-derived balance at a given date doesn't match the next authoritative observation, compute and surface the delta. Deltas are informational (not blocking) and self-heal — they are recomputed on every `sqlmesh run`, so reimporting missing transactions resolves them naturally.
8. **Manual balance assertions:** Users can assert a known balance via `moneybin accounts balance assert <account_id> <date> <amount>`. Stored in `app.balance_assertions`. Serves as an authoritative observation alongside institution-provided balances.
9. **No balance without an anchor:** Accounts with zero balance observations produce no `fct_balances_daily` rows. The system does not estimate an opening balance from transactions alone.
10. **CLI commands:** `moneybin reports networth show`, `moneybin reports networth history`, `moneybin accounts balance show`, `moneybin accounts balance history`, `moneybin accounts balance assert`, `moneybin accounts balance list`, `moneybin accounts balance delete`, `moneybin accounts balance reconcile`. The `accounts` parent group is registered by [`account-management.md`](account-management.md); this spec contributes the `balance` sub-group.
11. **MCP tools** (per [`moneybin-mcp.md`](moneybin-mcp.md) v2): `reports_networth_get`, `reports_networth_history`, `accounts_balance_list`, `accounts_balance_history`, `accounts_balance_reconcile`, `accounts_balance_assertions_list`, `accounts_balance_assert` (write), `accounts_balance_assertion_delete` (write).
12. **All commands support `--output json`** for non-interactive parity.
13. **Cash-only v1.** Investment holdings and multi-currency conversion are future extensions (M3B and M3C respectively). Net worth v1 covers cash accounts only.

## Data Model

### New table: `app.balance_assertions`

User-entered balance anchors. Managed via CLI (`moneybin accounts balance assert/list/delete`).

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
tabular_balances AS (
  -- Running balances on tabular (CSV/TSV/Excel/Parquet) transaction rows that
  -- carry a non-NULL balance column. One observation per such row.
  SELECT
    account_id,
    transaction_date AS balance_date,
    balance,
    'tabular' AS source_type,
    source_file AS source_ref
  FROM prep.stg_tabular__transactions
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
SELECT account_id, balance_date, balance, source_type, source_ref FROM tabular_balances
UNION ALL
SELECT account_id, balance_date, balance, source_type, source_ref FROM user_assertions
```

**Future extensions:** Add a `plaid_balances` CTE when [`sync-plaid.md`](sync-plaid.md) ships balance snapshots.

**Source precedence within a day:** When multiple observations exist for the same account on the same date, `fct_balances_daily` uses the most authoritative source. Precedence (highest first): user assertion > institution snapshot (OFX/Plaid) > tabular running balance.

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
  observation_source VARCHAR    -- source_type of the observation (ofx, tabular, assertion, plaid); NULL if interpolated
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

### `accounts networth` / `reports networth` commands

```
moneybin reports networth show [--as-of DATE] [--account ACCOUNT_ID] [--output json|table]
```
- Default: net worth as of today (latest available date)
- `--as-of`: historical point-in-time lookup
- `--account`: filter to specific account(s), repeatable

```
moneybin reports networth history [--from DATE] [--to DATE] [--interval daily|weekly|monthly] [--output json|table]
```
- Default interval: monthly
- Shows net worth over time with period-over-period change (absolute and percentage)

### `accounts balance` commands

```
moneybin accounts balance show [--account ACCOUNT_ID] [--as-of DATE] [--output json|table]
```
- Default: latest known balance per account
- Shows: account name, balance, date of last observation, source (OFX/tabular/Plaid/assertion)

```
moneybin accounts balance history --account ACCOUNT_ID [--from DATE] [--to DATE] [--interval daily|weekly|monthly] [--output json|table]
```
- Per-account balance time series with source attribution per observed day
- Default interval: daily; default range: full available history for the account

```
moneybin accounts balance assert <account_id> <date> <amount> [--notes "reason"] [--yes]
```
- Inserts or updates a row in `app.balance_assertions`
- Validates `account_id` exists in `dim_accounts`

```
moneybin accounts balance list [--account ACCOUNT_ID] [--output json|table]
```
- Shows all balance assertions, optionally filtered by account

```
moneybin accounts balance delete <account_id> <date> [--yes]
```
- Removes a manual assertion
- `--yes` for non-interactive confirmation

### `accounts balance reconcile` command

```
moneybin accounts balance reconcile [--account ACCOUNT_ID] [--threshold AMOUNT] [--output json|table]
```
- Shows all accounts with non-zero reconciliation deltas
- `--threshold`: only show deltas exceeding this amount (default: $0.01)

## MCP Interface

Tool naming follows [`moneybin-mcp.md`](moneybin-mcp.md) v2 (path-prefix-verb-suffix). Cross-domain rollups live under `reports_*`; per-account workflows live under `accounts_balance_*`.

### Read tools

**`reports_networth_get`** — Current or historical net worth.
- Params: `as_of_date` (optional DATE), `account_ids` (optional list of VARCHAR)
- Returns: total net worth, total assets, total liabilities, per-account breakdown with balance and source, as-of date

**`reports_networth_history`** — Net worth time series.
- Params: `from_date` (DATE), `to_date` (DATE), `interval` (daily|weekly|monthly, default monthly)
- Returns: time series with net worth, period-over-period change (absolute and percentage), account count

**`accounts_balance_list`** — Current balance per account.
- Params: `account_ids` (optional list), `as_of_date` (optional DATE)
- Returns: per-account balance with date of last observation and source attribution

**`accounts_balance_history`** — Per-account balance time series.
- Params: `account_id` (VARCHAR, required), `from_date` (optional DATE), `to_date` (optional DATE), `interval` (daily|weekly|monthly, default daily)
- Returns: time series of `{date, balance, is_observed, observation_source, reconciliation_delta}`

**`accounts_balance_reconcile`** — Accounts with non-zero reconciliation deltas.
- Params: `account_ids` (optional list), `threshold` (optional DECIMAL, default 0.01)
- Returns: list of `{account_id, balance_date, observed_balance, transaction_derived_balance, delta, source_type}` for days where the delta exceeds the threshold

**`accounts_balance_assertions_list`** — Manual balance assertions.
- Params: `account_id` (optional VARCHAR)
- Returns: list of assertions with dates, amounts, and notes

### Write tools

**`accounts_balance_assert`** — Insert or update a manual balance assertion.
- Params: `account_id` (VARCHAR, required), `assertion_date` (DATE, required), `balance` (DECIMAL, required), `notes` (optional VARCHAR)
- Returns: the upserted assertion row
- Sensitivity: `medium` (writes financial data); requires confirmation per MCP write-tool conventions

**`accounts_balance_assertion_delete`** — Remove a manual balance assertion.
- Params: `account_id` (VARCHAR, required), `assertion_date` (DATE, required)
- Returns: status + the deleted row's previous values
- Sensitivity: `medium`; requires confirmation

### Resources

**`net-worth://summary`** — Current net worth snapshot. Useful as context for AI conversations about finances. Returns: total net worth, total assets, total liabilities, account count, as-of date.

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

This lets the scenario suite (`make test-scenarios`) validate that `fct_balances_daily` and `agg_net_worth` match the generator's known-correct values.

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

- Scenario tests under `tests/scenarios/` (run via `make test-scenarios`) compare `fct_balances_daily` and `agg_net_worth` against ground-truth values from the generator.
- Reconciliation delta scenarios: persona with deliberate gaps produces expected non-zero deltas.

### Tier 3 — Integration

- End-to-end: import OFX file → `sqlmesh run` → `moneybin reports networth show` → verify output matches expected values.
- Manual assertion flow: `moneybin accounts balance assert` → `sqlmesh run` → verify balance updated in `fct_balances_daily`.
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

- **Investment holdings in net worth** — M3B concern. Net worth v1 is cash-only. Investment tracking (`investment-tracking.md`, M3B) will extend `fct_balances` with holdings valuation when it ships.
- **Multi-currency conversion** — M3C concern. All balances assumed single-currency for v1. Multi-currency (`multi-currency.md`, M3C) will add home-currency conversion to `fct_balances_daily`.
- **Balance forecasting/projection** — "What will my net worth be in 6 months?" is a separate feature requiring trend extrapolation.
- **Balance alerts/notifications** — "Notify me when balance drops below X" is out of scope for the data model spec.
- **Historical balance backfill without an anchor** — If a user imports 2 years of transactions but only has balance observations from the last 3 months, we do not attempt to reconstruct balances before the first observation. Absent, not best-effort.
- **Plaid balance sync scheduling** — The polling/scheduling mechanism for Plaid balance snapshots belongs in `sync-plaid.md`, not here. This spec consumes whatever Plaid provides.
- **Net worth trend analysis and reporting** — Cash flow statements, trend charts, and category breakdowns belong in `net-worth-reporting.md` (M3). This spec provides the data model they consume.
- **Transaction-level reconciliation** — Per-transaction cleared/reconciled markers (the legacy "verified" concept) are not yet specced. When designed, that future spec should subsume what [`transaction-curation.md`](transaction-curation.md) §Out of Scope deferred as the "verified" curator flag — one transaction-grain reconciliation surface, not two parallel markers. Cross-link to [`transaction-curation.md`](transaction-curation.md) §Out of Scope.

## Coordination with `account-management.md`

This spec ships bundled with [`account-management.md`](account-management.md) — they share the `accounts` CLI/MCP namespace and one of them must own each shared artifact. Boundaries:

| Artifact | Owner |
|---|---|
| `app.balance_assertions` table + migration | **net-worth.md** |
| `app.account_settings` table + migration | **account-management.md** (consumed here for `include_in_net_worth`) |
| `accounts` CLI parent group registration (`src/moneybin/cli/commands/accounts.py`) | **account-management.md** (this spec contributes the `balance` sub-group inside it) |
| `reports` CLI parent group registration (`src/moneybin/cli/commands/reports.py`) | **net-worth.md** (this spec creates it; future report specs add subcommands) |
| `AccountService` extensions (entity ops: list/show/rename/archive/include) | **account-management.md** |
| `BalanceService` (balance queries, assertions, reconciliation) | **net-worth.md** |
| `NetworthService` (cross-account aggregation) | **net-worth.md** |
| Account merge → balance assertion fan-in invariants | **account-management.md** (must reassign assertion rows on merge; this spec verifies the post-merge invariant in scenarios) |

When implementing: land both specs' schema migrations in the same migration window so `agg_net_worth`'s `LEFT JOIN app.account_settings` is never against a missing table.

## Implementation Plan

### Files to Create

Schema + migrations:
- `src/moneybin/sql/schema/app_balance_assertions.sql` — DDL for `app.balance_assertions` (idempotent re-init)
- `src/moneybin/sql/migrations/V00N__create_app_balance_assertions.sql` — first-time creation in existing databases (next available version)

SQLMesh models (under existing `sqlmesh/models/core/` — no new subdirs):
- `sqlmesh/models/core/fct_balances.sql` — VIEW unioning all balance sources
- `sqlmesh/models/core/fct_balances_daily.sql` — TABLE (FULL kind) with daily carry-forward logic (may be a Python model — see Implementation note in §`fct_balances_daily`)
- `sqlmesh/models/core/agg_net_worth.sql` — VIEW aggregating across accounts

Services (flat module layout — matches existing `src/moneybin/services/`):
- `src/moneybin/services/balance_service.py` — `BalanceService`: balance queries, assertion CRUD, reconciliation
- `src/moneybin/services/networth_service.py` — `NetworthService`: cross-account aggregation, history series

CLI commands (flat module layout — matches existing `src/moneybin/cli/commands/`):
- `src/moneybin/cli/commands/reports.py` — top-level `reports` group (created by this spec) with `networth show / history`
- Balance subcommands live inside `src/moneybin/cli/commands/accounts.py` (created by `account-management.md`); this spec adds the `balance` `Typer` sub-app and its commands to that module

Tests:
- `tests/moneybin/test_services/test_balance_service.py` — unit tests for balance logic
- `tests/moneybin/test_services/test_networth_service.py` — unit tests for net worth logic
- `tests/moneybin/test_cli/test_reports_networth.py` — CLI tests for `reports networth`
- `tests/moneybin/test_cli/test_accounts_balance.py` — CLI tests for `accounts balance` subcommands
- `tests/e2e/test_e2e_readonly.py` / `test_e2e_mutating.py` — E2E entries for every new command (per `.claude/rules/testing.md` "E2E Test Coverage Requirement")
- `tests/e2e/test_e2e_help.py` — `--help` entries for `reports`, `reports networth`, `accounts balance`
- `tests/scenarios/scenario_networth_correctness.yaml` (+ pytest entry) — synthetic ground-truth comparison
- `tests/scenarios/scenario_reconciliation_self_heal.yaml` — reimport resolves delta

### Files to Modify

- `src/moneybin/sql/schema.py` — register `app_balance_assertions.sql` in the schema init list (account-management.md registers `app_account_settings.sql`)
- `src/moneybin/cli/main.py` — register the new top-level `reports` group; remove the `track networth` / `track balance` stubs from `commands/stubs.py` (they migrate to their v2 homes)
- `src/moneybin/mcp/tools/__init__.py` (and the per-tool registry) — add tools listed in §MCP Interface
- `src/moneybin/mcp/resources/` — add `net-worth://summary` resource
- `src/moneybin/protocol/sensitivity.py` (or equivalent) — register sensitivity tiers per `moneybin-mcp.md`
- `docs/specs/INDEX.md` — flip status to `in-progress` on entry; flip to `implemented` when shipped

### Key Decisions

1. **Accurate or absent.** No balance is computed without an authoritative observation anchor. This is the foundational invariant.
2. **Reconciliation deltas are computed, not stored.** They self-heal as data quality improves. No manual cleanup needed.
3. **`fct_balances_daily` is FULL materialized.** Recomputed on every `sqlmesh run`. Ensures consistency after any data change.
4. **Source precedence:** user assertion > institution snapshot > tabular running balance. Most authoritative source wins within a day.
5. **Cash-only v1.** Investment and multi-currency extensions are designed to slot in without breaking changes.
6. **CLI taxonomy follows `moneybin-cli.md` v2.** Per-account workflows live under `accounts balance *`; cross-domain aggregation lives under `reports networth *`. The `track` group is dissolved by the v2 restructure — net-worth ships against the v2 surface, not v1.
7. **Bundled landing with `account-management.md`.** Shared `accounts` namespace and `app.account_settings` cross-reference make a single PR cycle the only sane shape. See §Coordination.
