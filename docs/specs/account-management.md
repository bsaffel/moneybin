# Feature: Account Management

## Status
implemented

## Goal

Own the `accounts` entity namespace end-to-end for v1: list/show,
user-controlled display preferences (rename), lifecycle ops (archive/unarchive,
include/exclude from net worth), and the per-account metadata layer. Its fields
align with source data that providers expose through account APIs. Provide the
`app.account_settings` table that other specs (notably
[`reports-net-worth.md`](reports-net-worth.md)) reference for per-account
configuration. Account merging is explicitly deferred — see [Out of Scope](#out-of-scope).

## Background

`core.dim_accounts` is built from upstream sources (OFX statements, tabular imports, Plaid in the future). The dimension is correct for *what the institution says*, but it lacks anything the *user* needs to decide about an account: a friendly name, whether to show it in the default account list, whether to count it in net worth, and the structural metadata (subtype, holder category, currency, credit limit) that Plaid surfaces but OFX/tabular sources don't.

The v1 surface has no answer for any of this. Users see raw account IDs, every account counts toward net worth, and there is no place to record metadata that isn't carried by the import. The [v2 CLI restructure](moneybin-cli.md) introduces the `accounts` top-level group as the home for these workflows; this spec defines what lives there.

Net worth ([`reports-net-worth.md`](reports-net-worth.md)) ships in the same release because the two specs share the `accounts` namespace and the `app.account_settings` table. Splitting them would mean two churning passes over the same files and a half-formed `accounts` group landing first. See [`reports-net-worth.md` §Coordination](reports-net-worth.md#coordination-with-account-managementmd) for the artifact ownership table.

The metadata schema mirrors **Plaid's account model** (Plaid Parity), so when [`sync-plaid.md`](sync-plaid.md) ships it can populate the same fields automatically with no migration.

Related specs and docs:
- [`reports-net-worth.md`](reports-net-worth.md) — consumes `app.account_settings.include_in_net_worth` and `archived` for `agg_net_worth`; ships bundled with this spec
- [`moneybin-cli.md`](moneybin-cli.md) v2 — defines the `accounts` top-level group; this spec extends with the unified `accounts set` (moneybin-cli.md amendment landed alongside)
- [`moneybin-mcp.md`](moneybin-mcp.md) — `accounts` owns the `detail` and `summary` read projections; this spec adds the entity-mutation contract in `accounts_set`
- [`privacy-data-protection.md`](privacy-data-protection.md) — settings table encrypted at rest; `last_four` and `credit_limit` are PII-adjacent and require sensitivity-tier handling
- [`database-migration.md`](database-migration.md) — migration infrastructure for new tables

## Requirements

1. **Per-account settings table `app.account_settings`.** One row per `account_id` (foreign key to `core.dim_accounts`). Plaid-parity metadata fields plus two lifecycle flags. Absence of a row means "all defaults." See [Data Model](#data-model).
2. **Plaid Parity metadata fields:** `display_name`, `official_name`, `last_four`, `account_subtype`, `holder_category`, `currency_code`, `credit_limit`. These mirror Plaid's account object structure so future Plaid sync (`sync-plaid.md`) can populate them automatically.
3. **Lifecycle flags:** `archived` (BOOLEAN, default FALSE) and `include_in_net_worth` (BOOLEAN, default TRUE).
4. **Archive cascades, unarchive does not restore.** Setting `archived = TRUE` automatically sets `include_in_net_worth = FALSE` in the same write. Setting `archived = FALSE` does NOT touch `include_in_net_worth` — the user must re-include explicitly. Rationale: archive expresses "this account is retired," and forcing the user to also flip the net worth flag is a footgun; restoring it on unarchive would silently re-include accounts the user might have intentionally excluded for unrelated reasons.
5. **Default reads hide archived accounts.** `accounts list` and `accounts(view="list")` omit accounts with `archived = TRUE`; detail lookup does the same by default. `--include-archived` (CLI) and `include_closed=true` (MCP) reveal them for list or detail reads. The contract is explicit: include_closed is a read filter only; archive changes flow through `accounts_set(is_archived=...)`.
6. **Open vocabulary for `account_subtype` and `holder_category` with soft validation.** Any string accepted at the SQL layer. The service maintains a canonical Plaid list and warns on non-canonical values:
   - **CLI:** interactive `[y/N]` prompt in TTY mode; `--yes` skips. Suggestions for near-misses ("did you mean 'checking'?").
   - **MCP:** write always succeeds; the response payload carries a `list[str]` at `data.warnings`. The agent decides whether to retry.
7. **`core.dim_accounts` is the single source of truth.** The dim model joins `app.account_settings` directly so `display_name`, `archived`, `include_in_net_worth`, and the metadata fields are always available to consumers without per-consumer join logic. This pattern is codified into [`.claude/rules/database.md`](#) by this spec — see [Files to Modify](#files-to-modify).
8. **Display name resolution chain:** `app.account_settings.display_name` → derived default (`institution_name + account_subtype + …last_four`) → `institution_name + …last_four` when the account has no type → bare `account_id`. First non-empty wins. Materialized inside `core.dim_accounts.display_name`. The subtype is preferred over the canonical `account_type` because "checking" reads to a human where "depository" does not; a user override of `account_subtype` flows through to the rendered name.
9. **CLI surface:** a single `accounts set` command is the partial-update entry point for every settings field. Structural metadata (`--official-name`, `--last-four`, `--subtype`, `--holder-category`, `--currency`, `--credit-limit`, `--default-cost-basis-method`, plus `--clear-FIELD` for each) sits alongside behavioral flags (`--display-name`, `--include/--exclude`, `--archive/--unarchive`). Archiving cascades `--exclude` atomically; unarchiving does NOT auto-restore include. See [CLI Interface](#cli-interface). The formerly-separate `accounts rename`, `accounts include`, `accounts archive`, `accounts unarchive` commands are folded into `accounts set` flags. (`--default-cost-basis-method` added by [`investments-data-model.md`](investments-data-model.md).)
10. **MCP surface:** mirrors CLI — one write tool (`accounts_set`) and one typed read tool. Use `accounts(view="list")`, `accounts(view="detail", reference=...)`, `accounts(view="summary")`, or `accounts(view="resolve", query=...)`. Detail requires `reference`; resolve requires `query`; summary is aggregate-only. `include_closed` applies to list and detail reads, never mutates state, and response data emits `archived`.
11. **Sensitivity tiers:** `accounts(view="summary")` is aggregate-only. `accounts` dynamically classifies its selected projection. `accounts_set` is statically classified with maximum sensitivity `critical`; its mutations remain audited.
12. **All commands support `--output json`** and the standard read-only flags (`-o`, `-q`) per `.claude/rules/cli.md`.
13. **Idempotent settings writes.** `accounts set` always upserts into `app.account_settings`. Setting the same value twice is a no-op (no error).
14. **PII handling for `last_four` and `credit_limit`.** `last_four` is a 4-digit string (validated `^[0-9]{4}$`). `credit_limit` is `DECIMAL(18,2)`. Neither flows through logger output (logger only records the `account_id` and the affected fields by name). The full account number never enters the system.

## Data Model

### New table: `app.account_settings`

User-controlled per-account configuration. One row per account; absence means defaults.

```sql
CREATE TABLE IF NOT EXISTS app.account_settings (
    account_id           VARCHAR NOT NULL PRIMARY KEY,            -- Foreign key to core.dim_accounts.account_id
    display_name         VARCHAR,                                  -- User-supplied label override; NULL falls back to derived default
    official_name        VARCHAR,                                  -- Institution's formal account name (mirrors Plaid official_name); free text
    last_four            VARCHAR,                                  -- Last 4 digits of account number (mirrors Plaid mask); validated ^[0-9]{4}$
    account_subtype      VARCHAR,                                  -- Plaid-style subtype (checking, savings, credit card, mortgage, ...); open vocabulary, soft-validated against canonical Plaid list
    holder_category      VARCHAR,                                  -- 'personal' / 'business' / 'joint'; open vocabulary, soft-validated
    currency_code        VARCHAR,                                  -- ISO-4217 (USD, EUR, ...); NULL inherits core.dim_accounts.currency_code's own fallback (see below)
    credit_limit         DECIMAL(18, 2),                           -- User-asserted credit limit on credit cards / lines (drives utilization metrics)
    archived             BOOLEAN NOT NULL DEFAULT FALSE,           -- Hides account from default list and from agg_net_worth
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,            -- Whether this account contributes to agg_net_worth (independent toggle, but archive cascades to FALSE)
    default_cost_basis_method VARCHAR CHECK (default_cost_basis_method IN ('fifo', 'hifo', 'specific', 'average')), -- Per-account cost-basis default (added by investments-data-model.md); NULL falls back to global FIFO
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last modification time
);
```

**Length and format constraints (enforced in `AccountService` at the service boundary, not via SQL CHECK constraints):**

| Field | Constraint |
|---|---|
| `display_name` | 1–80 chars |
| `official_name` | 1–200 chars |
| `last_four` | exactly 4 chars, `^[0-9]{4}$` |
| `account_subtype` | 1–32 chars |
| `holder_category` | 1–32 chars |
| `currency_code` | exactly 3 uppercase letters, `^[A-Z]{3}$` |
| `credit_limit` | non-negative `DECIMAL(18,2)` |

Service-layer validation rather than SQL CHECK keeps the table forgiving of historical rows if Plaid sync ever back-fills with looser data.

**`default_cost_basis_method`** is the one exception — added by
[`investments-data-model.md`](investments-data-model.md), it is a closed
four-value vocabulary (`fifo`, `hifo`, `specific`, `average`) enforced with a
SQL `CHECK` constraint rather than service-layer validation, since an
elected-but-unimplemented method would silently miscompute cost basis (see
that spec's Cost-Basis Engine section). `NULL` falls back to the global FIFO
default.

**Open question — Plaid precedence (deferred to `sync-plaid.md`):** When Plaid sync ships and starts populating `official_name` / `last_four` / `account_subtype` / `holder_category` / `currency_code` automatically, what happens if the user has already written a value? Options: (i) Plaid wins on resync, (ii) user wins, (iii) per-field "user_modified" tracking. For v1 (no Plaid yet) this doesn't matter. The table shape is forward-compatible with all three; pick one when `sync-plaid.md` is designed.

### Modified SQLMesh model: `core.dim_accounts`

`dim_accounts` is extended to be **the canonical resolved view of accounts**. New columns are derived from a `LEFT JOIN app.account_settings`. Existing source-derived columns are unchanged.

New columns (added to the final SELECT):

| Column | Source | Notes |
|---|---|---|
| `display_name` | `COALESCE(s.display_name, institution_name \|\| ' ' \|\| account_subtype \|\| ' …' \|\| last_four, institution_name \|\| ' …' \|\| last_four, …)` | Materialized resolution chain |
| `official_name` | `s.official_name` | Pass-through |
| `last_four` | `s.last_four` | Pass-through |
| `account_subtype` | `s.account_subtype` | Pass-through |
| `holder_category` | `s.holder_category` | Pass-through |
| `currency_code` | `COALESCE(s.currency_code, 'USD')` | USD is the last-resort fallback when no account-level currency was ever set; `multi-currency.md` M1K.1 Part A (2026-07-17) added the no-guess capture/inheritance layers upstream of this default — the true no-silent-blend guard that removes this fallback is Part B |
| `credit_limit` | `s.credit_limit` | Pass-through |
| `archived` | `COALESCE(s.archived, FALSE)` | Default FALSE for accounts with no settings row |
| `include_in_net_worth` | `COALESCE(s.include_in_net_worth, TRUE)` | Default TRUE |

Consumers (CLI, MCP, `agg_net_worth`) read from `core.dim_accounts` and get the resolved view automatically. **No consumer should join `app.account_settings` directly.** This is codified in `.claude/rules/database.md`.

## CLI Interface

The `accounts` top-level group is created by this spec. All commands support `--output json|table` (default `table`) and `-q` per `.claude/rules/cli.md`. Balance subcommands (`accounts balance …`) live inside the same `accounts.py` module but are owned by [`reports-net-worth.md`](reports-net-worth.md).

### Read commands

```
moneybin accounts list [--include-archived] [--type TYPE] [--output json|table] [-q]
```
- Default: hides `archived = TRUE`. Adds `--type` filter matching either the canonical `account_type` (`depository`, `credit`, `loan`, `investment`, `other`) or `account_subtype`.
- Columns: `display_name`, `account_id`, `institution`, `type/subtype`, `last_four`, `included` (✓/✗), `last_activity`.
- `--include-archived` adds archived rows with an `[archived]` annotation in the `included` column.

```
moneybin accounts get <account_id_or_display_name> [--output json|table]
```
- Resolves either an `account_id` or a unique `display_name`; ambiguous match prints disambiguation and exits non-zero.
- Reports the full settings row, source-derived fields from `dim_accounts`, last balance observation (from `core.fct_balances_daily`, when present), transaction count, date range.

### Mutation command — `set`

```
moneybin accounts set <account_id>
    [--display-name "..."]            # behavioral
    [--include | --exclude]           # behavioral (Optional[bool], Typer flag pair)
    [--archive | --unarchive]         # behavioral (Optional[bool], Typer flag pair)
    [--official-name "..."]           # structural
    [--last-four NNNN]
    [--subtype X]
    [--holder-category personal|business|joint]
    [--currency USD]
    [--credit-limit AMOUNT]
    [--default-cost-basis-method fifo|hifo|specific|average]
    [--clear-display-name]
    [--clear-official-name]
    [--clear-last-four]
    [--clear-subtype]
    [--clear-holder-category]
    [--clear-currency]
    [--clear-credit-limit]
    [--clear-default-cost-basis-method]
    [--yes]
```
- At least one field flag required (else exit `2` with usage error).
- `--clear-FIELD` writes NULL for nullable text fields. Booleans (`include_in_net_worth`, `archived`) are not clearable — pass the explicit value.
- Per-field service-boundary validation (see Data Model constraints). `display_name` is length-validated against the 1–80 constraint.
- **Archive cascade.** `--archive` flips `archived = TRUE` AND `include_in_net_worth = FALSE` in the same write. The CLI confirmation appends `(also excluded from net worth)` when `--archive` is passed. `--unarchive` flips `archived = FALSE` but does NOT restore `include_in_net_worth` — pass `--include` in the same or a subsequent invocation to re-include.
- **Soft validation on `--subtype` / `--holder-category`:** if value is not in the canonical Plaid list and stdin is a TTY, prompt:
  ```
  ⚠️  'chequing' is not a known Plaid subtype (did you mean 'checking'?)
  Proceed anyway? [y/N]:
  ```
  `--yes` skips the prompt and writes. Non-TTY contexts (scripts, CI) without `--yes` exit `2` with the warning text — forces explicit intent.
- **`--default-cost-basis-method`** (added by [`investments-data-model.md`](investments-data-model.md)): per-account default cost-basis method (`fifo`, `hifo`, `specific`, `average`); an invalid value is rejected before any write (SQL `CHECK`, not soft-validated). `NULL` (the default, or after `--clear-default-cost-basis-method`) falls back to global FIFO. A per-security override on the security catalog (`moneybin investments securities set --method`) takes precedence over this account default; see that spec's Cost-Basis Engine section.

## MCP Interface

The 47-tool registry uses coarse domain operations with typed projections and
view selectors. Account reads therefore share `accounts(view=...)`; the separate
`accounts_set` operation retains the account target-state mutation boundary.

### Read tools

| Tool | Sensitivity | Notes |
|---|---|---|
| `accounts(view="list")` | dynamic | Per-account rows. Optional inputs: `include_closed`, `limit`, and `cursor`. |
| `accounts(view="detail", reference=...)` | dynamic | Single-account detail. Returns full settings row + dim fields + last balance observation. |
| `accounts(view="summary")` | dynamic | Aggregate rollup, no per-account rows. |

### Write tool (static sensitivity; maximum `critical`)

| Tool | Params | Runtime payload |
|---|---|---|
| `accounts_set` | `account_id`; behavioral: `display_name`, `include_in_net_worth` (bool), `is_archived` (bool); structural: `official_name`, `last_four`, `account_subtype`, `holder_category`, `currency_code`, `credit_limit`, `default_cost_basis_method` (added by [`investments-data-model.md`](investments-data-model.md); `fifo`/`hifo`/`specific`/`average`). Pass `None` to leave unchanged; include the field name in `clear_fields` to clear (text fields only — booleans are not clearable). `is_archived=True` cascades `include_in_net_worth=False` atomically; unarchive does NOT auto-restore include. | Updated settings in `data`, including `data.warnings` as `list[str]`; `cascaded_include_in_net_worth: false` appears when `is_archived=True` caused the cascade. Data emits `archived`, not `is_archived`. The current registry advertises no output schema. |

### Soft-validation in MCP

No TTY, no prompt. Write always succeeds. Human-readable warning messages are
embedded in `data.warnings` as `list[str]`; they are not a top-level envelope field.
The agent decides whether to retry, prompt the user, or proceed. Forcing the write
to fail would block legitimate non-canonical values (HSA, FSA, custom
institution-specific subtypes).

### Aggregate projection

- `accounts(view="summary")` — the aggregate account projection, backed by `AccountService.summary()`.

### Aggregate shape (used by `accounts(view="summary")`)

```json
{
  "total_accounts": 8,
  "count_by_type": { "depository": 3, "credit": 3, "loan": 2 },
  "count_by_subtype": { "checking": 2, "money market": 1, "credit card": 3, "auto": 1, "mortgage": 1 },
  "count_archived": 1,
  "count_excluded_from_net_worth": 2,
  "count_with_recent_activity": 6
}
```

## Identifier and PII Documentation

This spec ships a new doc explaining the project-wide identifier conventions, since `account_id` vs `account_number` vs `last_four` vs `routing_number` is now spread across `dim_accounts`, `app.account_settings`, and the loaders:

**`docs/architecture/account-identifiers.md`** (new):
- `account_id` — synthetic stable identifier, primary key on `dim_accounts`. Source-derived (OFX `<ACCTID>` hash, tabular content hash, future Plaid `account_id`). Safe to log.
- `account_number` — full bank account number. **Never stored in MoneyBin.** Loaders extract only `last_four` from raw inputs; the full number is dropped at the parser boundary.
- `last_four` — last 4 digits, validated `^[0-9]{4}$`. Stored in `app.account_settings.last_four`. Logged only as `<account_id>.last_four` reference, never as a value.
- `routing_number` — ABA routing number on `dim_accounts`. PII-adjacent but not secret (publicly listed). Logged only when essential for diagnostics.
- Masking story: `SanitizedLogFormatter` (`src/moneybin/log_sanitizer.py`) detects and masks 9+ digit sequences and patterns matching account/SSN shapes as a runtime safety net. See [`privacy-data-protection.md`](../specs/privacy-data-protection.md).

## Testing Strategy

### Tier 1 — Unit (`tests/moneybin/test_services/test_account_service.py`)

- Settings upsert idempotence: `settings_update(A, display_name="Foo")` twice produces one row with the latest `updated_at`.
- Display name resolution chain: settings override → derived default → bare `account_id`. One test per branch.
- Archive cascade: `settings_update(A, archived=True)` flips `include_in_net_worth` to FALSE in the same write; `settings_update(A, archived=False)` does NOT restore it.
- Inclusion / archive orthogonality: `settings_update(A, include_in_net_worth=False)` does not touch `archived`; `settings_update(A, archived=False)` does not touch `include_in_net_worth`.
- Soft-validation classifier: `is_canonical_subtype("checking")` TRUE, `is_canonical_subtype("chequing")` FALSE, `suggest_subtype("chequing") == "checking"`.
- Field length / format constraints: writes exceeding bounds raise typed errors at the service boundary (each constraint gets a test).
- Backward-compat delegates: `rename` / `set_include_in_net_worth` / `archive` / `unarchive` still exist as thin one-line delegates to `settings_update` for internal callers; tests of the delegates assert the cascade is reached through the unified path.

### Tier 2 — CLI (`tests/moneybin/test_cli/test_accounts.py`)

- `accounts list` default hides archived; `--include-archived` shows them with the annotation.
- `accounts set --archive` cascade prints `(also excluded from net worth)`.
- `accounts set --unarchive` does not restore `include`; the cascade note is absent.
- `accounts set --subtype chequing` (TTY mock) triggers the prompt; `--yes` skips it; `--subtype checking` does not prompt.
- `accounts set --subtype chequing` (non-TTY, no `--yes`) exits `2` with the warning text.
- `accounts set --clear-display-name` clears the override.
- `--output json` matches the text branch's data exactly.

### Tier 3 — E2E (`tests/e2e/`)

- `test_e2e_help.py` — `--help` for `accounts` and every subcommand.
- `test_e2e_readonly.py` — `accounts list`, `accounts get`, `accounts list --include-archived`.
- `test_e2e_mutating.py` — full lifecycle: import → `accounts set --subtype …` → `accounts set --display-name "…"` → `accounts set --exclude` → `accounts set --archive` → assert `dim_accounts.archived = TRUE` and `include_in_net_worth = FALSE` → `accounts set --unarchive` → assert `archived` flipped, `include` did NOT.

### Tier 4 — Scenario (`tests/scenarios/scenario_account_settings.yaml`)

Synthetic persona with multiple account types. Hand-derived expectations:
- `accounts list` row count == `persona.account_count`.
- After archiving 2 accounts: `accounts list` count == `count - 2`; `agg_net_worth` excludes those 2 (both via the `include_in_net_worth` cascade and the `archived` filter).
- After `accounts set --credit-limit` on a credit card, `accounts get` returns the asserted limit.
- Soft-validation: `accounts set --subtype xyz --yes` writes the value; subsequent `accounts get` returns it; `accounts_set` MCP call returns the warning.
- **Negative invariants:**
  - Archiving an account does NOT mutate `core.fct_transactions` for it (transactions remain queryable, just account is hidden in default UI).
  - Unarchiving does NOT cause `agg_net_worth` to include the account if `include_in_net_worth` is still FALSE.

### Tier 5 — MCP integration (`tests/e2e/test_e2e_mcp.py`)

- `accounts` returns the resolved view including `display_name`.
- `accounts(view="list")` accepts only the documented list inputs: `include_closed`, `limit`, and `cursor`.
- `accounts_set` with non-canonical `account_subtype` returns `warnings` field; write succeeds.
- `accounts(view="summary")` returns the aggregate shape; no per-account leakage.

## Dependencies

- [`reports-net-worth.md`](reports-net-worth.md) — bundled co-release; consumes `app.account_settings.include_in_net_worth` and `archived`. Owns `accounts balance *` subcommands within `accounts.py`.
- [`database-migration.md`](database-migration.md) — new table requires migration entry
- [`privacy-data-protection.md`](privacy-data-protection.md) — `last_four` and `credit_limit` are PII-adjacent; sensitivity-tier handling enforced
- [`moneybin-mcp.md`](moneybin-mcp.md) — registration of new write tools and sensitivity tiers
- [`moneybin-cli.md`](moneybin-cli.md) v2 — defines `accounts` parent group; this spec extends with `archive` / `unarchive` / `set`
- `core.dim_accounts` — extended to be the single resolved source of truth (per Requirement 7)

## Out of Scope

- **Account merge / unmerge.** Deferred to a future spec. The data warehouse model means merge would require recomputing every consumer's view of `account_id`, which is non-trivial and out of v1 scope. Users with duplicate accounts must live with both until merge ships.
- **Hard delete of accounts.** Archive is the only v1 lifecycle terminator. Hard delete is dangerous (orphans transactions and balance observations, breaks audit trails) and rare. The data warehouse principle: data goes in, data does not get destructively removed.
- **Vanity / cosmetic fields** — `sort_order`, `color`, `notes`. Not part of v1's "structural metadata" framing. Easy to add later via migration if a real consumer surfaces.
- **Account groups, tags, or hierarchies.** `holder_category` (personal / business / joint) provides the only grouping affordance v1 needs.
- **Multi-currency arithmetic.** `currency_code` is recorded per account but no conversion happens in v1. `multi-currency.md` (M1K) handles home-currency conversion.
- **Plaid sync precedence rules.** When Plaid sync lands and back-fills metadata fields, conflict resolution between user-set and Plaid-set values is decided in [`sync-plaid.md`](sync-plaid.md), not here.
- **Closed enum for `account_subtype` / `holder_category`.** Open vocabulary with soft validation; closed enums age badly.
- **`accounts list` pagination.** v1 assumes account counts in the dozens, not thousands. Pagination if/when a real user crosses 100 accounts.

## Implementation Plan

### Files to Create

Schema + migrations:
- `src/moneybin/sql/schema/app_account_settings.sql` — DDL for `app.account_settings` (idempotent re-init)
- `src/moneybin/sql/migrations/V00N__create_app_account_settings.sql` — first-time creation in existing databases (next available version)

CLI commands:
- `src/moneybin/cli/commands/accounts/` — top-level `accounts` group with `list`, `get`, `set`, `resolve`. The `set` command is the partial-update entry point covering display_name, include/exclude, archive/unarchive, plus structural metadata. Net-worth ships the `balance` sub-app inside this same package.

Documentation:
- `docs/architecture/account-identifiers.md` — `account_id` vs `account_number` vs `last_four` vs `routing_number`, masking story (per Requirement 14 and §Identifier and PII Documentation)

Tests:
- `tests/moneybin/test_services/test_account_service.py` — extended for settings + soft-validation + cascade logic
- `tests/moneybin/test_cli/test_accounts.py` — CLI tests for the new surface
- `tests/e2e/test_e2e_help.py` — `--help` entries for `accounts` and every subcommand
- `tests/e2e/test_e2e_readonly.py` / `test_e2e_mutating.py` — E2E entries per `.claude/rules/testing.md`
- `tests/scenarios/scenario_account_settings.yaml` (+ pytest entry) — settings combinations, archive cascade, soft-validation

### Files to Modify

- `src/moneybin/sql/schema.py` — register `app_account_settings.sql`
- `src/moneybin/services/account_service.py` — add settings CRUD, soft-validation classifier, summary aggregator; extend list / show / get with new fields
- `src/moneybin/cli/main.py` — register the new top-level `accounts` group; remove the legacy `track` registration if [`reports-net-worth.md`](reports-net-worth.md) hasn't already (the two specs split the cleanup)
- `src/moneybin/cli/commands/stubs.py` — drop `track_app` and its sub-stubs (replaced by real `accounts` and `reports` groups; `recurring`, `investments`, `budget` stubs move to their v2 homes per `moneybin-cli.md` v2)
- `src/moneybin/sqlmesh/models/core/dim_accounts.sql` — add `LEFT JOIN app.account_settings`; add the new columns per [Modified SQLMesh model](#modified-sqlmesh-model-coredim_accounts)
- `src/moneybin/mcp/tools/__init__.py` (and per-tool registry) — register `accounts_set` (single write tool covering structural + behavioral fields after the Group 13 collapse); extend `accounts` with its typed read projections and revised sensitivity
- `src/moneybin/protocol/sensitivity.py` (or equivalent) — register sensitivity tiers
- `docs/specs/moneybin-cli.md` — amend the `accounts` subtree to describe the unified `accounts set` (folds in display_name, include/exclude, archive/unarchive)
- `docs/specs/moneybin-mcp.md` — add `accounts_set` and the `accounts(view="summary")` projection to the surface tables (the Group 13 collapse leaves a single write tool)
- `docs/specs/INDEX.md` — flip status to `in-progress` on entry; flip to `implemented` when shipped
- `.claude/rules/database.md` — strengthen with a new rule: "core dimensions are the single source of truth for entity attributes — when app-layer metadata refines or overrides a dim, join it into the core dim model itself, never duplicate join logic in consumers." Cite this spec as the precedent.

### Key Decisions

1. **Plaid Parity for metadata fields.** The `app.account_settings` schema mirrors Plaid's account model so future Plaid sync can populate fields automatically with no migration.
2. **Settings are upsert-only, never required.** Absence of a row means defaults; this keeps the table tiny and avoids backfilling on every account import.
3. **Archive cascades to net worth; unarchive does not restore.** Two-step ergonomics, one-step intent for the common case (retire an account); explicit re-include for the uncommon case (un-retire and re-count).
4. **`core.dim_accounts` is the single source of truth.** No consumer joins `app.account_settings` directly. Codified in `.claude/rules/database.md` by this spec.
5. **Open vocabulary for `account_subtype` / `holder_category` with soft validation.** Closed enums age badly; soft validation surfaces typos without blocking legitimate non-canonical values.
6. **Soft validation differs by surface.** CLI: TTY prompt, `--yes` skips, non-TTY without `--yes` exits 2. MCP: writes succeed, warnings on the envelope.
7. **`accounts(view="summary")` is universally accessible.** It is the aggregate projection of the account read contract.
8. **`accounts` dynamically classifies the selected projection.** The detail projection can carry PII-adjacent account fields.
9. **Account merge deferred.** v1 ships archive as the only lifecycle terminator. Merge requires recomputing consumer views of `account_id` and is not in scope.
10. **Bundled landing with `reports-net-worth.md`.** Shared `accounts` namespace and `app.account_settings` cross-reference. See [`reports-net-worth.md` §Coordination](reports-net-worth.md#coordination-with-account-managementmd).
