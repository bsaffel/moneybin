# Feature: Account Management

## Status
ready

## Goal

Own the `accounts` entity namespace end-to-end: list/show, user-controlled display preferences (rename, sort), lifecycle ops (archive/unarchive, include/exclude from net worth), and account merging with a reversible undo story. Provide the `app.account_settings` table that other specs (notably [`net-worth.md`](net-worth.md)) reference for per-account configuration.

## Background

`core.dim_accounts` is built from upstream sources (OFX statements, tabular imports, Plaid in the future). The dimension is correct for *what the institution says*, but it lacks anything the *user* needs to decide about an account: a friendly name, whether to show it in the default account list, whether to count it in net worth, or whether two records that look like one account should actually be merged into one.

The v1 surface has no answer for any of this. Users see raw account IDs, every account counts toward net worth, and a duplicate caused by a re-import under a different OFX `<ACCTID>` requires a manual SQL fix. The [v2 CLI restructure](cli-restructure.md) introduces the `accounts` top-level group as the home for these workflows; this spec defines what lives there.

Net worth ([`net-worth.md`](net-worth.md)) ships in the same release because the two specs share the `accounts` namespace and the `app.account_settings` table. Splitting them would mean two churning passes over the same files and a half-formed `accounts` group landing first. See [`net-worth.md` §Coordination](net-worth.md#coordination-with-account-managementmd) for the artifact ownership table.

Related specs and docs:
- [`net-worth.md`](net-worth.md) — consumes `app.account_settings.include_in_net_worth` and `archived` for `agg_net_worth`; ships bundled with this spec
- [`cli-restructure.md`](cli-restructure.md) v2 — defines the `accounts` top-level group; this spec extends the v2 tree with `archive` / `unarchive` / `merge` / `unmerge` (see §CLI Interface)
- [`mcp-tool-surface.md`](mcp-tool-surface.md) v2 — `accounts_list` / `accounts_get` already enumerated; this spec adds the entity-mutation tools
- [`matching-overview.md`](matching-overview.md) — reversibility pattern (status enum + `reversed_at` / `reversed_by`) reused here for account merges
- [`privacy-data-protection.md`](privacy-data-protection.md) — settings + merge tables encrypted at rest
- [`database-migration.md`](database-migration.md) — migration infrastructure for new tables

## Requirements

1. **Per-account settings table `app.account_settings`.** One row per `account_id` (foreign key to `core.dim_accounts`). Columns: `display_name` (override), `archived` (bool), `include_in_net_worth` (bool, default TRUE), `sort_order` (int, optional), `color` (varchar, optional), `notes` (varchar, optional), `updated_at` (timestamp). Upsert semantics — no row means "all defaults."
2. **Account merges table `app.account_merges`.** One row per merge decision. Columns: `merge_id` (UUID), `source_account_id`, `target_account_id`, `merged_at`, `merged_by`, `merge_status` (`active` / `reversed`), `reversed_at`, `reversed_by`, `reason` (optional). Mirrors the matching engine reversibility shape (`app.match_decisions`).
3. **Display name resolution order.** `app.account_settings.display_name` → derived default (`institution_name + account_type + last4(account_id)`) → bare `account_id`. The first non-empty wins. Resolution lives in `core.dim_accounts` itself via a `LEFT JOIN app.account_settings`.
4. **Archive semantics.** `archived = TRUE` hides the account from `accounts list` (default), excludes it from `agg_net_worth`, and labels it in `accounts show`. **Transactions remain queryable** — archive is not a soft-delete of data, only a UI/aggregation hint. No hard-delete CLI command in v1.
5. **Net worth inclusion toggle is independent of archive.** A non-archived account can still be excluded from net worth (e.g., a custodial account the user wants to track but not count). The two flags are orthogonal.
6. **Account merging.** Merging account A into B records a row in `app.account_merges` and rewrites all downstream references. Implemented via a `core.bridge_account_merges` SQLMesh model that resolves merged IDs at query time — no in-place rewrite of `raw.*` or `dim_accounts`. Transactions, balance assertions, and balance observations originally tagged with A surface as belonging to B in `core.fct_transactions` and `core.fct_balances`.
7. **Merge reversibility.** `accounts unmerge <merge_id>` flips `merge_status` to `reversed`. The next `sqlmesh run` restores the pre-merge view. No data is lost on either side. This matches the matching engine's reversibility contract.
8. **Merge invariants.** Cannot merge an account into itself. Cannot merge an account that is already a target of an active merge into a third account (avoids transitive merge confusion in v1 — explicit re-merge required after unmerging). Cannot merge an account whose `account_id` does not exist in `dim_accounts`. Validation runs in the service layer before insert.
9. **Source account_id remains queryable.** After merging A → B, `core.fct_transactions` shows transactions under B, but the original `account_id` is preserved in a new `pre_merge_account_id` column for audit. `accounts show <A>` still resolves and reports "merged into B on YYYY-MM-DD".
10. **CLI commands** under `accounts` (see CLI Interface section).
11. **MCP tools:** `accounts_list`, `accounts_get`, `accounts_rename`, `accounts_include`, `accounts_archive`, `accounts_unarchive`, `accounts_merge`, `accounts_unmerge`, `accounts_settings_update` (catch-all for sort_order/color/notes). Reads are sensitivity `low`; writes are sensitivity `medium` per [`mcp-tool-surface.md`](mcp-tool-surface.md).
12. **All commands support `--output json`** and the standard read-only flags (`-o`, `-q`) per `.claude/rules/cli.md`.
13. **Idempotent settings writes.** `accounts rename`, `accounts include`, etc. always upsert into `app.account_settings`. Setting the same value twice is a no-op (no error).

## Data Model

### New table: `app.account_settings`

User-controlled per-account configuration. One row per account; absence means defaults.

```sql
CREATE TABLE IF NOT EXISTS app.account_settings (
    account_id VARCHAR NOT NULL PRIMARY KEY,            -- Foreign key to core.dim_accounts
    display_name VARCHAR,                                -- User-supplied label override; NULL falls back to derived default
    archived BOOLEAN NOT NULL DEFAULT FALSE,             -- Hides account from default list and from agg_net_worth
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,  -- Whether this account contributes to net worth (independent of archived)
    sort_order INTEGER,                                  -- Optional manual ordering hint for accounts list; NULL sorts naturally
    color VARCHAR,                                       -- Optional UI hint (hex code or named color); reserved for future web/desktop UI
    notes VARCHAR,                                       -- Optional free-text notes the user attaches to the account
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last modification time
);
```

**Length constraints** (enforced at the service boundary):
- `display_name`: 1–80 chars
- `color`: 1–32 chars
- `notes`: 0–512 chars

### New table: `app.account_merges`

Append-only log of merge decisions. The `bridge_account_merges` SQLMesh model resolves the active-merge graph from this table on every run.

```sql
CREATE TABLE IF NOT EXISTS app.account_merges (
    merge_id VARCHAR NOT NULL PRIMARY KEY,           -- UUID4 truncated to 12 hex chars
    source_account_id VARCHAR NOT NULL,              -- The account being absorbed
    target_account_id VARCHAR NOT NULL,              -- The account that "wins" — receives all source's transactions
    merged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the merge was recorded
    merged_by VARCHAR NOT NULL DEFAULT 'user',       -- 'user' (CLI/MCP) or 'system' (future auto-merge)
    merge_status VARCHAR NOT NULL DEFAULT 'active' CHECK (merge_status IN ('active', 'reversed')), -- Active merges flow through bridge; reversed merges are ignored
    reversed_at TIMESTAMP,                           -- When the merge was undone; NULL if active
    reversed_by VARCHAR,                             -- 'user' or 'system'; NULL if active
    reason VARCHAR,                                  -- Optional free-text explanation
    CHECK (source_account_id != target_account_id)   -- Cannot merge an account into itself
);
```

### SQLMesh model: `core.bridge_account_merges` (VIEW)

Resolves active merges into a `(original_account_id → effective_account_id)` mapping. Identity rows for accounts with no merges. Used by `dim_accounts`, `fct_transactions`, and `fct_balances` to surface merged accounts under the target ID.

```sql
MODEL (
  name core.bridge_account_merges,
  kind VIEW
);

WITH active_merges AS (
  SELECT source_account_id, target_account_id
  FROM app.account_merges
  WHERE merge_status = 'active'
)
SELECT
  a.account_id AS original_account_id,
  COALESCE(m.target_account_id, a.account_id) AS effective_account_id
FROM core.dim_accounts AS a
LEFT JOIN active_merges AS m
  ON a.account_id = m.source_account_id
```

**v1 limitation:** Single-hop only. If A is merged into B, then B is later merged into C, queries through the bridge resolve A → B (not A → C). Per Requirement 8, the merge service refuses to merge a target that's already the target of an active merge — so this transitive case requires the user to first unmerge A → B, then merge A → C. Multi-hop chasing is a v2 enhancement if needed.

### Modified SQLMesh model: `core.dim_accounts`

Adds two computed columns derived from the bridge and settings tables. Existing columns unchanged.

New columns:
- `display_name` — `COALESCE(s.display_name, institution_name || ' ' || account_type || ' …' || RIGHT(account_id, 4))`
- `effective_account_id` — from `bridge_account_merges`
- `archived` — from `app.account_settings`, default FALSE
- `include_in_net_worth` — from `app.account_settings`, default TRUE
- `sort_order`, `color`, `notes` — passed through from settings (NULL when no row)

The model joins `LEFT JOIN app.account_settings s ON a.account_id = s.account_id LEFT JOIN core.bridge_account_merges b ON a.account_id = b.original_account_id`.

### Modified SQLMesh model: `core.fct_transactions`

Adds `effective_account_id` (resolved through the bridge) alongside the existing `account_id`. Consumers querying "all activity for account X" use `effective_account_id`; consumers needing the original tagging (e.g., audit, debugging) use `account_id`. The original column is renamed to `pre_merge_account_id` only in the SELECT, not in raw — keeping raw unchanged is required by the data layer contract.

> **Open question (resolve during implementation):** whether to rename the existing `account_id` column to `pre_merge_account_id` and surface `effective_account_id` as the new `account_id`, or keep both columns side-by-side. Renaming preserves a single canonical `account_id` for downstream consumers but is a wider-blast-radius change. Side-by-side columns are safer but require every consumer to opt into `effective_account_id`. **Default plan: side-by-side**, since the only v1 consumer that needs the resolved value is `agg_net_worth` and `accounts balance show`.

### Modified SQLMesh model: `core.fct_balances`

Resolves `account_id` through the bridge inside each source CTE so balance observations on a merged source account surface under the target. Logic mirrors `fct_transactions`.

## CLI Interface

The `accounts` top-level group is created by this spec. All commands support `--output json|table` (default `table`) and `-q` per `.claude/rules/cli.md`. Balance subcommands (`accounts balance …`) live inside the same `accounts.py` module but are owned by [`net-worth.md`](net-worth.md) — see that spec for their contract.

### Read commands

```
moneybin accounts list [--include-archived] [--include-merged] [--output json|table]
```
- Default: shows non-archived accounts; merged source accounts are hidden (their target is shown)
- Columns: `display_name`, `account_id`, `institution`, `type`, `archived`, `include_in_net_worth`, `last_activity`
- `--include-archived` adds archived accounts; `--include-merged` adds merged source accounts with a "→ target" annotation

```
moneybin accounts show <account_id> [--output json|table]
```
- Single-account detail: settings, merge status (if any), last balance observation, transaction count, date range
- Resolves both original IDs and target IDs (for an original ID, also reports "merged into X on YYYY-MM-DD")

### Mutation commands

```
moneybin accounts rename <account_id> <display_name> [--yes]
```
- Upserts `display_name` in `app.account_settings`. Empty string clears the override.

```
moneybin accounts include <account_id> [--no] [--yes]
```
- Toggles `include_in_net_worth`. `--no` sets FALSE, default sets TRUE.
- Idempotent — no error if the value is already as requested.

```
moneybin accounts archive <account_id> [--yes]
moneybin accounts unarchive <account_id> [--yes]
```
- Sets / clears `archived`. Archive does NOT exclude from net worth automatically (use `accounts include --no` for that).

```
moneybin accounts settings <account_id> [--sort N] [--color C] [--notes "..."] [--clear-notes] [--yes]
```
- Bulk updater for the catch-all settings (`sort_order`, `color`, `notes`). At least one flag required.

### Merge commands

```
moneybin accounts merge <source_account_id> <target_account_id> [--reason "..."] [--yes]
```
- Confirmation prompt by default ("Merge 247 transactions from A into B? This is reversible via `accounts unmerge`."); `--yes` skips
- Validates per Requirement 8; errors with exit code 1 on invariant violation
- Triggers a `sqlmesh run` after recording the merge so `dim_accounts` / `fct_transactions` reflect the change immediately

```
moneybin accounts unmerge <merge_id> [--yes]
```
- Flips `merge_status` to `reversed`; runs `sqlmesh run`
- Exit code 1 if `merge_id` is unknown or already reversed

```
moneybin accounts merge log [--account ACCOUNT_ID] [--include-reversed] [--output json|table]
```
- Lists merges. Default shows active merges only; `--include-reversed` shows all
- `--account` filters to merges where the account appears as source or target

## MCP Interface

Adds eight tools to the `accounts_*` namespace beyond the read-only `accounts_list` / `accounts_get` already enumerated in [`mcp-tool-surface.md`](mcp-tool-surface.md) v2 §16b. Naming follows the v2 convention (path-prefix-verb-suffix).

### Read tools

**`accounts_list`** — see `mcp-tool-surface.md` §11. This spec extends the response with `display_name`, `archived`, `include_in_net_worth`, `effective_account_id`, and `sort_order`. Adds optional params `include_archived` (bool, default FALSE) and `include_merged` (bool, default FALSE).

**`accounts_get`** — see `mcp-tool-surface.md` §11. This spec extends the response with the full settings row and any active merge involving this account.

### Write tools (sensitivity `medium`; require confirmation)

**`accounts_rename`** — Upsert `display_name`.
- Params: `account_id` (VARCHAR), `display_name` (VARCHAR, 1–80 chars; empty string clears)
- Returns: the updated settings row

**`accounts_include`** — Set `include_in_net_worth`.
- Params: `account_id` (VARCHAR), `include` (BOOLEAN, default TRUE)
- Returns: the updated settings row

**`accounts_archive`** / **`accounts_unarchive`** — Set / clear `archived`.
- Params: `account_id` (VARCHAR)
- Returns: the updated settings row

**`accounts_settings_update`** — Catch-all for `sort_order`, `color`, `notes`.
- Params: `account_id` (VARCHAR); any of `sort_order` (INTEGER), `color` (VARCHAR ≤32), `notes` (VARCHAR ≤512); explicit `null` clears
- Returns: the updated settings row

**`accounts_merge`** — Record a merge.
- Params: `source_account_id` (VARCHAR), `target_account_id` (VARCHAR), `reason` (optional VARCHAR ≤256)
- Returns: `{merge_id, source_account_id, target_account_id, merged_at, transaction_count}`
- Triggers `sqlmesh run` synchronously before returning so the response reflects post-merge state

**`accounts_unmerge`** — Reverse a merge.
- Params: `merge_id` (VARCHAR)
- Returns: `{merge_id, reversed_at, transaction_count_restored}`
- Triggers `sqlmesh run` synchronously

### Resources

**`accounts://summary`** — High-level account snapshot for AI conversation context: total account count, count by type, count archived, count excluded-from-net-worth, count of active merges. No per-account data, no balances.

## Testing Strategy

### Tier 1 — Unit tests

- **Settings upsert idempotence:** Repeated `rename(A, "Foo")` produces one row with the latest `updated_at`.
- **Display name resolution:** Verify the resolution chain (settings → derived default → bare ID) for accounts with and without settings rows.
- **Archive flag effects:** `accounts list` default vs. `--include-archived`; `agg_net_worth` excludes archived accounts.
- **Inclusion / archive orthogonality:** Setting one does not change the other.
- **Merge invariants:** Self-merge rejected; merging into an existing target rejected; merging an unknown account rejected.
- **Merge / unmerge round-trip:** Merge A → B; verify all transactions surface under B. Unmerge; verify pre-merge view restored exactly.
- **Bridge resolution:** `core.bridge_account_merges` produces identity rows for unmerged accounts, redirect rows for merged sources, and ignores reversed merges.

### Tier 2 — Synthetic data verification

- Persona with two duplicate accounts (different OFX `<ACCTID>` for the same real account): merge them, verify `agg_net_worth` does not double-count.
- Persona that exercises all settings combinations (renamed, archived, excluded) — golden CLI / MCP output snapshots for `accounts list` and `accounts_list`.

### Tier 3 — Integration / scenario

- End-to-end merge invariant scenario per [`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md):
  - Import two account fixtures (A, B) with disjoint transaction sets and known counts
  - Pre-merge: assert `count(A.transactions) + count(B.transactions) == known total`, balances per account
  - Merge A → B: assert `count(B.transactions) == known total`, A no longer in default list, balance assertions on A surface under B
  - Unmerge: assert original counts and per-account balances restored exactly
  - **Negative invariant:** assert no transaction is counted under both A and B at any point in the cycle
- Net worth × merge interaction: an account's `app.balance_assertions` must surface under the target after merge. Verified in net-worth's scenario suite.

### Tier 4 — E2E

Per `.claude/rules/testing.md`: every CLI command gets a subprocess test in the appropriate tier (`test_e2e_help.py`, `test_e2e_readonly.py`, `test_e2e_mutating.py`). Help entries for the new `accounts` group and every subcommand. Mutating tests use isolated `tmp_path` databases.

## Dependencies

- [`net-worth.md`](net-worth.md) — bundled co-release; consumes `app.account_settings`. Owns `accounts balance *` subcommands within `accounts.py`.
- [`database-migration.md`](database-migration.md) — new tables (`app.account_settings`, `app.account_merges`) require migration entries
- [`privacy-data-protection.md`](privacy-data-protection.md) — settings + merge tables encrypted at rest via `Database`
- [`matching-overview.md`](matching-overview.md) — reversibility pattern (`status` enum + `reversed_at` / `reversed_by`) reused
- [`mcp-tool-surface.md`](mcp-tool-surface.md) — registration of new write tools and sensitivity tiers
- [`cli-restructure.md`](cli-restructure.md) v2 — defines `accounts` parent group; this spec extends with `archive` / `unarchive` / `merge` / `unmerge` (cli-restructure.md amendment landed alongside)
- `core.dim_accounts` — extended with `display_name`, `effective_account_id`, settings columns

## Out of Scope

- **Hard delete of accounts.** Archive is the only v1 lifecycle terminator. Hard delete is dangerous (orphans transactions, breaks audit trails) and not requested.
- **Transitive / chained merges.** v1 is single-hop only — see Requirement 8 and the `bridge_account_merges` v1 limitation. Multi-hop resolution is a v2 enhancement if real usage demands it.
- **Auto-detected account merges.** The matching engine does not currently propose account-level merges. Future enhancement (`account-dedup.md`) could feed proposals into `app.account_merges` with `merged_by = 'system'` and a pending review queue, but v1 is user-initiated only.
- **Per-account currency.** Multi-currency lives in `multi-currency.md` (Wave 3). v1 assumes a single currency across all accounts.
- **Account groups / hierarchies.** Folders, tags, or parent-child account relationships are out of scope. `sort_order` provides minimal grouping affordance.
- **External institution mapping.** "All my Chase accounts" via institution metadata grouping is implicit in `institution_name` / `institution_fid` on `dim_accounts` — no new table needed.
- **Color theming UI.** `app.account_settings.color` is reserved for future web/desktop UI; v1 CLI does not render it.

## Implementation Plan

### Files to Create

Schema + migrations:
- `src/moneybin/sql/schema/app_account_settings.sql` — DDL for `app.account_settings` (idempotent re-init)
- `src/moneybin/sql/schema/app_account_merges.sql` — DDL for `app.account_merges`
- `src/moneybin/sql/migrations/V00N__create_app_account_settings.sql` — first-time creation in existing databases
- `src/moneybin/sql/migrations/V00N__create_app_account_merges.sql` — first-time creation

SQLMesh models (under existing `sqlmesh/models/core/` — no new subdirs):
- `sqlmesh/models/core/bridge_account_merges.sql` — VIEW resolving active merges to `(original_account_id → effective_account_id)`

CLI commands:
- `src/moneybin/cli/commands/accounts.py` — top-level `accounts` group with `list`, `show`, `rename`, `include`, `archive`, `unarchive`, `settings`, `merge`, `unmerge`, `merge log` subcommands. Net-worth ships the `balance` sub-app inside this same module.

Tests:
- `tests/moneybin/test_services/test_account_service.py` — extended to cover settings + merge logic
- `tests/moneybin/test_cli/test_accounts.py` — CLI tests for entity ops
- `tests/e2e/test_e2e_help.py` — `--help` entries for `accounts` and every subcommand
- `tests/e2e/test_e2e_readonly.py` / `test_e2e_mutating.py` — E2E entries per `.claude/rules/testing.md`
- `tests/scenarios/scenario_account_merge_invariants.yaml` (+ pytest entry) — full merge round-trip
- `tests/scenarios/scenario_account_settings.yaml` (+ pytest entry) — settings combinations

### Files to Modify

- `src/moneybin/sql/schema.py` — register the two new DDL files
- `src/moneybin/services/account_service.py` — add settings CRUD and merge / unmerge methods; extend list / show with new fields
- `src/moneybin/cli/main.py` — register the new top-level `accounts` group; remove the legacy `track` registration if [`net-worth.md`](net-worth.md) hasn't already (the two specs split the cleanup)
- `src/moneybin/cli/commands/stubs.py` — drop `track_app` and its sub-stubs (replaced by real `accounts` and `reports` groups; `recurring`, `investments`, `budget` stubs move to their v2 homes per `cli-restructure.md` v2)
- `sqlmesh/models/core/dim_accounts.sql` — add `LEFT JOIN app.account_settings` and `LEFT JOIN core.bridge_account_merges`; add `display_name`, `effective_account_id`, `archived`, `include_in_net_worth`, `sort_order`, `color`, `notes` columns
- `sqlmesh/models/core/fct_transactions.sql` — add `effective_account_id` column (side-by-side with existing `account_id`)
- `sqlmesh/models/core/fct_balances.sql` (created by net-worth.md) — resolve `account_id` through `bridge_account_merges` in each source CTE
- `src/moneybin/mcp/tools/__init__.py` (and per-tool registry) — register `accounts_rename`, `accounts_include`, `accounts_archive`, `accounts_unarchive`, `accounts_settings_update`, `accounts_merge`, `accounts_unmerge`
- `src/moneybin/mcp/resources/` — add `accounts://summary` resource
- `src/moneybin/protocol/sensitivity.py` (or equivalent) — register sensitivity tiers
- `docs/specs/cli-restructure.md` — amend the `accounts` subtree to include `archive` / `unarchive` / `merge` / `unmerge`
- `docs/specs/mcp-tool-surface.md` — add the new `accounts_*` write tools to the surface tables
- `docs/specs/INDEX.md` — flip status to `in-progress` on entry; flip to `implemented` when shipped

### Key Decisions

1. **Settings are upsert-only, never required.** Absence of a row means defaults; this keeps the table tiny and avoids backfilling on every account import.
2. **Archive ≠ exclude from net worth.** Two orthogonal flags. Documented and tested explicitly.
3. **Merge via bridge model, not data rewrite.** Reversibility comes free, audit trail is preserved, and re-running `sqlmesh run` is the only "apply" step.
4. **Single-hop merge in v1.** Transitive merges are blocked by Requirement 8; the bridge model is intentionally non-recursive. Re-merge is explicit.
5. **`pre_merge_account_id` preserved.** Audit and debugging require the original tagging to be queryable forever.
6. **Reversibility shape mirrors matching engine.** Same `status` enum and `reversed_at` / `reversed_by` columns, so users learn the pattern once.
7. **Bundled landing with `net-worth.md`.** Shared `accounts` namespace and `app.account_settings` cross-reference. See [`net-worth.md` §Coordination](net-worth.md#coordination-with-account-managementmd).
