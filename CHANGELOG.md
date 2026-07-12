<!-- Last reviewed: 2026-07-09 -->

# Changelog

All notable changes to MoneyBin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). MoneyBin is pre-1.0 and pre-launch; entries are grouped by **milestone** rather than semantic releases until 1.0 ships. See [`docs/roadmap.md`](docs/roadmap.md) for the current milestone scheme.

> **Milestone taxonomy revised 2026-05-30.** The scheme is now four phase-aligned milestones — **M0 Foundation · M1 Ingestion Core · M2 Analysis & Reports · M3 Productization & Distribution** — with lettered increments (e.g. `M1J`) beneath. Entries written before this date (the `[Unreleased]` narrative and the dated sections below) reference the **pre-revision** grid (old M0/M1/M2A–C/M3A–F) and are preserved as historical record. See [`docs/roadmap.md`](docs/roadmap.md) for the old→new mapping.

## [Unreleased]

### Fixed
- **Credit-card PDF statements now import with correct signs.** A statement that
  names itself a credit card (via its required disclosures — "minimum payment",
  "credit limit", and the like) derives the inverted convention
  (`negative_is_income`) behind an explicit confirmation: charges record as
  expenses, payments as credits. Previously every card statement was refused,
  because the sign convention could not be expressed and guessing it would have
  silently inverted the ledger. The confirmation is once per statement format —
  confirm it is a card (`moneybin import files <path> --confirm`), or overrule a
  false detection (`--sign negative_is_expense`), and that override survives every
  future replay of the format. Confirming a card also types its account as
  `credit`, so it is counted as a liability in net worth. (MCP surfaces the same
  confirmation and, for now, routes it to the CLI to resolve; in-place
  confirmation is planned.)

M2 closing out and M3 underway. M2A curator state shipped (transaction notes, tags, splits, manual entry, audit log). M2B architecture reference shipped (`architecture-shared-primitives.md`; writer-coordination contract via short-lived per-call connections). M2C brand surface advancing: `moneybin system doctor` integrity command, `reports.*` recipe library (eight curated views), and the `transform_*` MCP toolset closing the agent ingest loop. M3A Plaid Transactions sync shipped (Phase 1). Doc surface tightened for the personas reachable today; MCP surface hardened with protocol-standard annotations, `accounts_resolve`, list-parameter cap, structured error envelopes, and shell completion. Categorization correctness pass: memo-aware matcher, exemplar accumulation, source-precedence enforcement, auto-fan-out after apply; seed merchant catalogs retired in favor of user-driven and LLM-assist-driven merchant creation.

### Added
- **`moneybin demo` evaluator preset (M3A).** One command sets up an isolated
  `demo` profile, generates synthetic data (`--persona
  basic`/`family`/`freelancer`), runs the full pipeline — match, and categorization
  by the real engine against the merchants the generator invented — to a clean
  `system doctor`, activates the profile, and prints net worth plus next steps: a
  from-install path to a working product with no real financial data. It always
  targets the dedicated `demo` profile (there is no `--profile` target, so it can
  never be pointed at a real one), and re-running rebuilds that profile's database
  from scratch and regenerates (deterministic by default); `--yes` for
  non-interactive use. (#310)
- **Investment data model & cost-basis engine (M1J.1).** A manually-maintained
  securities catalog (`investments securities add/set/list`) and an
  investment-transaction ledger (`investments add` — buy, sell, reinvest,
  dividend, interest, capital-gain distribution, transfer in/out, deposit,
  withdrawal, split, fee, return of capital) derive tax lots, realized
  gain/loss (short- and long-term, 1099-B-reconciliation-ready), and current
  holdings (`investments holdings` — cost basis only; market value awaits a
  future price-feed pillar). Four cost-basis methods — FIFO, HIFO, specific
  identification, and average cost — apply per-security
  (`investments securities set --method`) or per-account
  (`accounts set --default-cost-basis-method`), falling back to global FIFO;
  `investments lots select` overrides which lots a sale draws from. New
  `investments` / `investments_holdings` / `investments_lots` /
  `investments_gains` / `investments_securities` read and
  `investments_record` / `investments_securities_set` /
  `investments_lots_select` write MCP tools, plus the top-level `investments`
  CLI group (replacing the earlier `accounts investments` placeholder). (#300)
- **Plaid balance snapshots flow into net worth and balance drift.**
  Plaid sync balances now reach `core.fct_balances` → `core.fct_balances_daily`,
  so `reports networth` / `networth-history` and balance-drift detection include
  Plaid-connected accounts (previously only OFX statement balances, tabular
  running balances, and manual assertions contributed). Credit/loan balances are
  recorded as liabilities (negative), and `core.dim_accounts` now sources Plaid
  `official_name`/`account_subtype` under any user override. (#299)
- **Category taxonomy audit — 112-category curated set (M1W).**
  Audited all 108 seed categories against four principles (earn-the-split
  granularity, class-by-accounting-nature, no redundant/orphan categories,
  provider-neutral): retired 5 duplicate/orphan categories (resolving the
  two-mortgage-category ambiguity in favour of `LNP-MTG`) and added 9 — 6 finer
  categories from the 29 unmapped Plaid detailed codes, plus a 3-category
  **Family & Kids** group (`FAM`/`FAM-ACT`/`FAM-SUP`) folded in after a
  cross-aggregator comprehensiveness crosswalk against MX, Mint, Monarch, and
  Maybe validated coverage; `class` reconciled end-to-end (no reclasses needed).
  Net 108 − 5 + 9 → 112 categories. Seed validation now
  enforces a valid-class invariant, an enumerated coverage report, and an orphan
  allowlist. Purely additive on the M1V bridge — no consumer query changes. (#298)
- **`transactions categorize improve-ai` — upgrade AI-guessed categories to confident Plaid categories (M1U follow-up).**
  New CLI command and matching MCP tool (`transactions_categorize_improve_ai`)
  reverse-look-up every transaction currently `categorized_by='ai'` against the
  `core.bridge_category_source_map` bridge and upgrade it to `provider_native`
  when the match is at MEDIUM confidence or higher. Never touches user, rule,
  or merchant categorizations. (#294)
- **Automatic Plaid category assignment from Personal Finance Category (M1U).**
  Transactions synced from Plaid are now auto-categorized from Plaid's PFC codes
  via the `core.bridge_category_source_map` bridge (source `provider_native`,
  two-tier detailed→primary reverse lookup, confidence-gated at ≥MEDIUM), running
  last after rules and merchants in `categorize_pending` so it clears the long tail
  before the LLM. A rule or merchant you author after the import overrides the Plaid
  category on the next categorize run — the source-precedence ladder holds across
  runs, not just within one write. `transactions categorize stats` gains a
  `plaid_unmapped` count (Plaid transactions whose PFC code has no bridge mapping
  yet). (#292)
- **`core.bridge_category_source_map` — provider-code → canonical-category bridge (M1V).**
  A durable, aggregator-agnostic view resolving any provider's transaction-category
  code to exactly one canonical MoneyBin category, keyed `(source_type,
  source_category_code)`. Two-tier lookup (`code_level`: `detailed` preferred,
  `primary` fallback) so an unmapped detailed code still lands in the right
  top-level category. Backed by `seeds.category_source_map` (91 rows re-derived
  against Plaid's verified Personal-Finance-Category taxonomy) unioned with
  `app.category_source_map` (user overrides always win). Prerequisite for the
  parked Plaid Tier-2b categorizer.
- **Resolve transaction merchants by Plaid `merchant_entity_id` before name matching (M1T).** Two new `app.*` tables (`merchant_links` binding + `merchant_link_decisions` review queue) back an adopt-or-mint ladder that fires at categorization time; a backfill `harvest()` records existing assignments with zero review (conflicts-only). New `merchants links pending / set / history / run` CLI subgroup and `merchants_links_pending / _set / _history / _run` MCP tools surface fuzzy-match proposals; the top-level `review` tool gains a merchant-links queue.
- **Plaid max-data capture.** Plaid sync now captures the institution's original
  (raw) description as a new `original_description` column on
  `core.fct_transactions`, distinct from Plaid's cleaned `description`. The sync
  path also populates currency, authorized date, pending-transaction link, payment
  channel, check number, and merchant location on `core.fct_transactions`
  (previously NULL for Plaid). Merchant entity id and Plaid's detailed
  personal-finance category are captured into `raw.plaid_transactions` for later
  merchant-resolution / categorization work. Run `moneybin sync pull --force` to
  backfill existing transactions. (#283)
- **Import-time account-binding confirmation (M1S.4).** Tabular `import_confirm`
  now surfaces the account resolver's verdict at import time. When an
  interactive human imports a file whose source account resolves to weak merge
  candidate(s) (`institution+last4` / name), the import returns
  `confirmation_required` with `confirmation_payload.{reason="account_confirmation",
  account_proposals[]}` instead of silently minting — the column layout is
  settled, only the account identity needs ratifying. The caller binds each
  proposed account via `account_bindings` (MCP) / `--account-binding
  source_key=ACCOUNT_ID|new` (CLI): adopt an existing account, or `new` to mint
  a distinct one. A `"new"` account can capture `display_name` / `account_subtype`
  / `last_four` / `iso_currency_code` at mint via `account_metadata` (MCP) /
  `--account-meta source_key:field=value` (CLI). Agent / non-interactive imports
  never gate here — they load and leave the proposal in the account-link review
  queue (`accounts_links_pending`). The `moneybin_account_link_review_pending`
  gauge and `moneybin_account_link_confidence` histogram now emit.
- **Account-link review queue (M1S.5).** New `accounts_links_pending` /
  `accounts_links_set` / `accounts_links_history` / `accounts_links_run` MCP
  tools and the `moneybin accounts links` CLI subgroup surface the cross-source
  account-merge proposals the resolver raises (`institution+last4` / name) so a
  weak account match is reviewed, never silently merged. Accepting a proposal
  re-points the provisional account's native references onto the chosen
  canonical account (auto-rejecting siblings); `--standalone` keeps it separate.
  `accounts links run` backfills proposals over existing accounts. Account
  numbers are never surfaced (proposals carry opaque ids + labels only).
- **Smart-import-pdf Phase 2a — deterministic PDF routing to `raw.tabular_transactions`.**
  PDFs that auto-derive (or replay a saved) high-confidence recipe land
  rows in `raw.tabular_transactions` (`source_type='pdf'`) instead of the
  Phase 1 catch-all seed table; everything else (no transaction-shaped
  table, reconciliation failure, missing balance metadata) still falls
  back to `raw.pdf_seeds`. Auto-derived recipes persist to
  `app.pdf_formats` on first contact (keyed by layout fingerprint =
  issuer + sorted dedup headers + page bucket) so a second statement
  with the same layout replays the saved recipe instead of re-deriving.
  Reconciliation gate enforces pre-sign-normalization sum identity with
  the statement's reported balance delta within 1¢. See
  [`docs/specs/smart-import-pdf.md`](docs/specs/smart-import-pdf.md).
- **Smart-import-pdf Phase 2b — bridge round-trip to the driving agent.**
  A native-text PDF the deterministic rung can't crack (low confidence,
  failed reconciliation, missing balances) now hands the document to the AI
  agent already driving MoneyBin instead of silently seeding:
  `import_files`/`import_preview` return a `confirmation_required` envelope
  carrying the document text, a table preview, the layout fingerprint, and a
  plain transparency notice (proceeding surfaces the document to the agent),
  and `import_confirm(bridge_response={recipe, rows})` ratifies. MoneyBin
  re-runs the agent's recipe and reconciles the re-executed rows against the
  statement balances — the authority — before any transactions load, verifies
  the agent's returned rows against the re-execution, and reports any
  row-count divergence. Every hand-off writes a `smart_import_parse` privacy
  audit row and bumps `moneybin_pdf_bridge_egress_total{outcome}`. MCP-only
  for now (gated on `actor_kind="agent"`); a bare CLI keeps the seed fallback.
- **Smart-import-pdf Phase 2b complete — recipe auto-recovery + scanned-PDF
  degradation.** A saved PDF recipe that stops serving its layout (fails
  validation on replay, or stops reconciling) is now re-derived and installed
  as a new audited, undo-reversible version on the next import, instead of
  stranding the broken recipe so every future statement re-escalates. A
  scanned / image-only PDF with no selectable text layer now returns an
  explicit unsupported outcome (a clear "needs a vision-capable backend"
  message, error code `import_pdf_no_text_layer`) rather than a generic
  "no tables extracted" failure. The bridge parser also rejects an agent
  recipe whose amount fields don't match its declared sign convention. See
  [`docs/specs/smart-import-pdf.md`](docs/specs/smart-import-pdf.md).
- **`moneybin import formats list --type {tabular,pdf,all}`** (default
  `all`) filters by format kind and renders tabular + PDF sections in
  text; JSON output is a uniform list with a `type` discriminator per
  row. **`moneybin import formats show <name>`** resolves across both
  namespaces.
- **`import_formats` MCP tool now returns `pdf_formats: list[…]` alongside
  the existing `formats: list[…]`** so agents have parity with the CLI.
  Each PDF row carries `{name, institution_name, document_kind, routing,
  front_end, version, times_used, last_used_at}`.
- **Three new Prometheus metrics under `moneybin_pdf_*`:**
  `extraction_confidence` (Histogram, 0–1), `recipe_hit_total{outcome}`
  (Counter, outcomes: `replay_success`/`replay_failed`), and
  `replay_guard_failure_total` (Counter, no labels — separate raw signal
  for alerting on recipe drift).
- **`import_confirm` MCP tool + `moneybin import confirm` CLI subcommand.**
  Terminal `_confirm` step of the propose→review→confirm flow for smart tabular
  imports. First-encounter imports surface a `confirmation_required` envelope;
  the caller accepts (`accept=True` / `--accept`) or applies a partial-merge
  column-mapping override (`mapping={...}` / `--mapping field=col`). `save_format`
  (default `True`) pins the merged mapping to `app.tabular_formats` for silent reuse.
  Revertible via `import_revert` (data rows) + `system_audit_undo` (format save).
  See [`docs/specs/smart-import-confirmation.md`](docs/specs/smart-import-confirmation.md).
- **Cross-channel confidence contract.** Tabular and gsheet channels share a
  normalized `score` plus derived `tier` (`high`/`medium`/`low`) with configurable
  bands. Defaults: `T_high=0.90`, `T_med=0.70`. Env vars:
  `MONEYBIN_IMPORT___CONFIDENCE__T_HIGH` / `MONEYBIN_IMPORT___CONFIDENCE__T_MED`
  (three underscores between `IMPORT` and `CONFIDENCE` due to Pydantic nested-settings alias).
- **Tiered agent autonomy gate.** `MONEYBIN_IMPORT___SELF_ACCEPT_HIGH` (default
  `False`). When enabled after calibration earns the precision bar, MCP agents may
  self-accept `high`-tier first encounters. The CLI human path always prompts regardless.
- **New `--confirm`/`--mapping` flags on `moneybin import files`.** `--confirm` /
  `--no-confirm` accepts or declines a `confirmation_required` proposal inline;
  `--mapping field=column` (repeatable) is a partial-merge alias of `--override`.
  Non-TTY / `--output json` returns the `confirmation_required` envelope and exits 0.
- **`import_files` MCP envelope now returns `confirmation_required` state** on
  first-encounter unknown layouts, including `proposed_mapping`, `samples`, `flagged`,
  `missing_required`, `unmapped_columns`, and `actions[]` recovery hints pointing at
  `import_confirm`.
- **Six new Prometheus metrics under `moneybin_import_*`:**
  `confirmations_total{channel,tier,outcome}` (outcomes: `accepted|overridden|declined`),
  `detection_score` histogram, `self_accept_total{channel}`, `override_total{channel}`,
  `known_format_reuse_total{channel}`, `revalidation_failure_total{channel}`.
- **`DatabaseLockError` is now emitted consistently on cross-process database
  contention.** A new MoneyBin-owned write critical-section lock coordinates
  before DuckDB's own ATTACH layer, identifying the holder and timing out at
  10 seconds with a `system_status` recovery action. Fixes a regression where
  DuckDB 1.5.3's unified lock-error string (`"Could not set lock on file"`)
  was no longer matched by the classifier, causing raw `duckdb.IOException`
  to leak to MCP, CLI, and Web UI callers. See
  [`docs/specs/database-writer-coordination.md`](docs/specs/database-writer-coordination.md)
  § "PR B hardening pass" and [ADR-010](docs/decisions/010-writer-coordination.md).
- **`Database.checkpoint(reason)` helper** at durable boundaries — wired now
  at post-migration and post-transform-apply; pre-backup / post-compact /
  post-large-import sites land when those features ship. Emits
  `moneybin_db_checkpoint_total{reason=...}`.
- **`system_status` `database_connections` section** identifies the active
  writer (via the lock file) and concurrent readers (via `lsof`). Powers the
  `DatabaseLockError` recovery action.
- **`review` MCP tool and `moneybin review` CLI command** (M1S.5c) — domain-neutral
  orientation sweep that aggregates all three review queues in one call:
  `matches_pending`, `categorize_pending`, and `account_links_pending` (new).
  One "what needs my attention?" call now covers transaction matches, uncategorized
  transactions, and account-link decisions without a separate sweep per domain.

### Deprecated
- **`transactions_review` MCP tool** — use `review` instead. Registered as a
  deprecated alias with description starting with "DEPRECATED: use `review`";
  removed after one minor release.
- **`moneybin transactions review`** — use `moneybin review` instead. Emits a
  deprecation warning to stderr and delegates to the same implementation;
  removed after one minor release.

### Changed
- **`core.dim_categories` gains an accounting `class` (M1V).** Every category
  now carries `class` (`income` | `expense` | `transfer` | `debt`), assigned
  at curation time for seed categories and defaulting to `expense` for user
  categories. Unlocks income-statement separation and transfer-exclusion from
  spend reporting.
- **Inbox-sync pending entries now carry their account proposals in the response
  envelope.** Each `account_confirmation` entry returned by `import_inbox_sync` /
  `moneybin import inbox` now includes `account_proposals[]` (source key,
  proposed account, and the candidate pick-list) directly in the response, not
  only in the on-disk `.pending.yml` sidecar. A REST/MCP/CLI-JSON caller can now
  render the pick-list and bind an account without reading the sidecar off disk;
  the CLI human-readable output lists the candidate accounts inline instead of
  pointing at the sidecar.
- **`Database.__init__()` and `get_database()` now require `read_only` as a
  keyword-only argument.** The prior `read_only: bool = False` default is
  removed; every call site declares intent explicitly. This is the physical
  enforcement that complements the SQL allowlists at MCP/CLI boundaries —
  read surfaces open with `ATTACH ... READ_ONLY`, not just by convention.
  Internal API change only; no external callers. See
  [`docs/specs/database-writer-coordination.md`](docs/specs/database-writer-coordination.md)
  and [ADR-010](docs/decisions/010-writer-coordination.md).
- **GSheet alias limit tightened from 63 to 56 chars** (#228) so the
  generated `gsheet_<alias>` view name fits DuckDB's 63-char identifier
  limit. A pre-existing gsheet connection with a 57–63 char alias will
  now raise a clear error on the next `gsheet pull` telling the user to
  reconnect with a shorter alias. Connections with aliases ≤56 chars are
  unaffected.
- **`raw.gsheet_*` and `raw.pdf_*` views: lifecycle columns now `_`-prefixed** (#228).
  System carry columns surface as `_loaded_at`, `_row_number`,
  `_deleted_from_source_at`, and `_page` (instead of the bare names) so
  they can never collide with normalized user headers from the source
  data (e.g. a PDF "Page" column or a Google Sheet "row_number"
  column). Existing `raw.gsheet_<alias>` views regenerate on next
  `gsheet pull`; queries referencing the old names need updating to the
  underscored form. Pre-launch — no migration path.
- **`medium`-confidence tabular imports now gate on confirmation** instead of waving
  through with a sign-convention log warning. Callers receive a `confirmation_required`
  envelope (MCP / `--output json`) or an interactive prompt (TTY CLI). Closes the
  spec-vs-code drift `smart-import-tabular.md` already promised.
- **`gsheet connect --column-mapping` is now partial-merge.** Only the destination
  fields you name are overridden; unspecified fields fall back to the detected mapping.
  Previously the flag replaced the entire mapping — a behavior change to a shipped
  surface. Confidence bands are aligned to `ImportSettings.confidence`.
- **`moneybin import files <single-file>` exits 1 on per-file failure** when no
  per-file knobs are passed. Previously the single-file path used the batch
  soft-fail behavior and exited 0 even when the lone file failed; it now mirrors
  the fail-loud single-file contract so scripts and agents see the failure.
  Pre-launch behavior change — no users affected.
- **Report CLI flags auto-derive from parameter names.** With reports now
  generated from runner signatures, multi-word flags follow the parameter name:
  `moneybin reports cashflow`/`spending` use `--from-month` / `--to-month`
  (replacing the bespoke `--from` / `--to`). Tool/command names are unchanged.
  The `data` payload for the six view-backed reports is now a bare array of
  result rows (the standard envelope shape) instead of the previous typed
  `{rows: [...]}` wrapper — a pre-launch normalization; no other tool exposed
  report rows.
- **Pending-match output now groups copies of the same transaction by component.**
  `transactions_matches_pending` (MCP) and `moneybin transactions matches pending` (CLI)
  enrich each pending dedup row with a `component_key` — the lexicographic MIN packed
  member key of its connected component across all active+pending dedup edges. Edges
  belonging to the same N-way cluster share one `component_key`; the CLI groups them
  into one display block per cluster. Transfer rows are ungrouped (`component_key =
  match_id`). The `actions[]` summary hint reports the edge-to-group ratio.
- **The lock-error string classifier in `_attach_encrypted`** now matches DuckDB
  1.5.3's `"Could not set lock on file"` in addition to the legacy 1.5.2
  `"Conflicting lock"` and `"different configuration"` strings.
- **The default `max_wait` on `get_database()` is now `10.0` seconds** (was 5.0)
  to match the policy ceiling documented in `database-writer-coordination.md`.

### Removed
- **`core.dim_categories.plaid_detailed` (M1V).** The single-aggregator
  category tag is replaced by `core.bridge_category_source_map`, which
  supports multiple providers and guarantees a deterministic one-row-per-code
  reverse lookup.
- **`reports_budget` MCP tool and `reports budget` CLI command.** They
  synthesized from `BudgetService` rather than reading a `reports.*` view,
  violating the `reports_*` = reads-a-view convention; they return through the
  report framework once a `reports.budget` view ships (M3C). `BudgetService`
  and the `budget_*` mutation tools are unaffected.
- **`reports health` CLI stub** — an unimplemented placeholder with no backing
  spec.
- **`sync.enabled` config field.** It was seeded into every profile's
  `config.yaml` and shown by `moneybin profile show` but never read — sync
  gating is server-side. Existing `config.yaml` files keep working (the stale
  key is ignored).

### Fixed
- **PDF statements with no ruled table no longer import zero transactions.**
  Recipe derivation picked its transaction table from `pdfplumber`'s table
  detection, which only fires on *drawn ruling lines* — while the recipe
  executor reads the document's text lines. Real bank statements are typeset
  with whitespace-aligned columns and no rules, so derivation went blind on
  exactly the input the executor consumes: a real Chase statement with a clean
  `ACCOUNT ACTIVITY` section extracted **0 transactions** — its rows either
  landed in an opaque seed table or, for a statement with no ruled content
  anywhere, failed outright with "No tables extracted from PDF". Derivation now
  falls back to reconstructing the table from text lines using the same column
  splitter the recipe executes with. Statements already imported as seeds will
  import correctly on re-import. (#313)
- **Credit-card statements no longer import their charges as income.** The PDF
  importer assumes "negative = expense" for every single-amount-column layout —
  the deposit-account convention — and its only safeguard was "does this
  statement contain a negative amount?" A card statement carries the opposite
  convention (charges positive, payments negative), and almost always has a
  payment or refund row, so it sailed through that check and every charge was
  booked as **income**. Reconciliation could not catch it: it sums the raw signed
  amounts, which tie out to the balance change with the signs exactly backwards.
  The importer now reads the statement's own disclosures (minimum payment, credit
  limit, APR) instead of guessing at its arithmetic, and hands a card statement to
  the AI agent rather than importing it under the wrong convention. Signs cannot
  be inferred from the amounts alone — a checking statement and a card statement
  have identical sign distributions. This also closes the same hole on the
  saved-format replay path, which ran before derivation and skipped the guard
  entirely. (#313)
- **CSV/Excel imports no longer silently drop legitimately identical rows.**
  Transaction ids for sources without a native id are content hashes, so two
  genuinely distinct same-day purchases with the same amount and description
  (two $5.00 coffees at one shop) hashed identically and the staging dedup
  dropped one — real transactions, gone, with no error. The second and later
  rows of identical content now carry an occurrence suffix, matching the scheme
  PDF transaction ids already used. Ids of rows that were never colliding are
  unchanged, so **re-importing an affected file recovers the dropped rows** and
  leaves everything else alone. (#313)
- **PDF statements sharing a filename no longer eat each other's rows.** Seed
  rows were keyed on `(alias, page, row index, content)`, and the alias is just
  the filename stem — so `2024-01/chase.pdf` and `2024-02/chase.pdf` collided,
  and a recurring charge landing at the same row index in both months (an
  identical subscription line) was silently discarded from the second statement.
  The row key now includes the document's content identity. This re-keys existing
  `raw.pdf_seeds` rows: revert an affected PDF import (`moneybin import revert
  <id>`) before re-importing it, or the statement is seeded twice. (#313)
- **A PDF the importer can't parse now reaches the AI agent instead of being
  buried.** Every recipe-derivation failure reported the same reason
  (`no_transaction_table`), which is excluded from agent escalation on the
  grounds that the document isn't a statement at all. So a document that *was* a
  statement and merely defeated the parser was silently filed away as
  unparseable rather than handed to the AI agent that could read it — including
  the single most common bank layout (separate "Withdrawals" and "Deposits"
  columns), which the deterministic parser defers by design. Those now escalate.
  Genuinely non-transactional PDFs (a brokerage positions statement) are routed
  to the seed store as before, and so are statements in a number locale the
  importer cannot replay — escalating those would send your statement to an AI
  provider for a result it provably cannot use. (#313)
- **Net worth no longer drops accounts with older statements.**
  `core.fct_balances_daily` built each account's date spine only as far as *that
  account's* last balance observation, so on any later date the account simply
  vanished — and `reports.net_worth` sums the accounts present on a date. An account
  whose statement landed a week before another's therefore contributed nothing to
  the current net worth: a checking account with one January statement was absent
  from a December total. Every account is now carried forward to the newest known
  date, so net worth reflects each account's last known balance. Accounts that are
  genuinely gone are excluded by archiving them (`include_in_net_worth` / `archived`,
  already honored), not by silently ageing out. (#310)
- **First-run guidance points an unset-up profile at `profile create`.** When the
  active profile has never been set up at all, the "Database not found" message
  now recommends `moneybin profile create <name> --init-inbox` (which scaffolds
  config, database, and inbox) instead of `db init`, which would leave the profile
  unregistered — absent from `moneybin profile list`, with no inbox. If the
  profile *directory* already exists, the message still points at `db init`:
  `profile create` refuses on the directory alone, so recommending it there would
  dead-end the user. (#310)
- **OFX imports no longer silently drop transactions that share a duplicate
  FITID.** Some institutions (observed: Chase) reuse one OFX `FITID` for two
  distinct same-day transactions — a foreign purchase and its
  foreign-transaction fee. Because the raw primary key
  (`(source_transaction_id, account_id, source_file)`) and the OFX dedup window
  (keyed on `(source_transaction_id, account_id)`) both collapse the two rows —
  they always share `source_file` within one import — one of the two was silently
  dropped from the ledger. The extractor now disambiguates colliding FITIDs by
  content so both survive. New imports are correct going forward; to recover data
  **already** affected, revert the affected import (`moneybin import revert <id>`)
  and re-import the file — a plain re-import is not sufficient, because the
  forced-reimport write path upserts by primary key and leaves the stale pre-fix
  row in place. (#304)
- **`moneybin sync pull` no longer stuck-fails on a fully-materialized
  database.** Migration V032 issued `ALTER TABLE seeds.categories`, but on a
  database whose SQLMesh virtual layer is materialized that relation is a view —
  DuckDB rejects the ALTER, leaving the migration stuck and blocking every DB
  open. V032 now only rebuilds `app.user_categories`; the seed table's `class`
  column is owned by SQLMesh and derived by `refresh_views()`, so an upgraded
  database recovers automatically on the next run. (#306)
- **A second migration (V012) no longer stuck-fails on a fully-materialized
  database.** V012 ran `DROP TABLE IF EXISTS` over `seeds.merchants_global/us/ca` —
  former SQLMesh seed models that are views on a materialized database, where
  `DROP TABLE` on a view raises `CatalogException` (the same class as the V032 fix
  above). V012 now drops only the migration-owned `app.merchant_overrides` and
  leaves the seed relations to SQLMesh. A static test (`test_migration_schema_ownership`)
  now scans every migration and fails CI on any migration that writes a
  SQLMesh-owned schema. (#309)
- **`import_preview` surfaces header detection and row-count reconciliation.**
  Silent header-eating (a real data row mistaken for a header) was invisible in
  the preview envelope. The envelope now carries `has_header`, `skip_rows`, and
  `rows_in_file` (the reader's reconciled row accounting: `skip_rows + header +
  rows_read + rows_skipped_trailing`), plus `header_row_looks_like_data` — a
  flag when the row consumed as the header also parses as a transaction (raised
  for an explicit `skip_rows` that eats a data row, and for a headerless Excel
  sheet whose first row is a real transaction). When a red flag is present on an
  auto-detected (unknown) layout, detection `confidence` drops to `low`
  (previously a structurally-suspicious layout could still self-accept at
  `medium`), routing it to the propose→confirm gate instead of an agent
  auto-accepting a wrong mapping.
- **`moneybin system doctor` / `system_doctor` no longer hangs on a populated
  database.** Two integrity checks (the `transaction_categories` foreign-key
  check and the orphan `app.*`-state check) re-ran a correlated subquery
  against `core.fct_transactions` — an expensive merge/dedup/categorization
  view — once per row instead of once overall. Once `app.transaction_categories`
  held enough rows, a full doctor run could take over a minute (past the MCP
  30-second call cap) instead of the roughly 2 seconds it takes now; both
  checks are rewritten as a single anti-join. (#301)
- **Fixed stale command references in CLI hints and docstrings.** `make
  claude-mcp`'s remediation hints pointed at the pre-rename `mcp config
  generate --install` instead of `mcp install`; a synthetic-data reset hint
  pointed at a `moneybin db destroy` command that never existed instead of
  `moneybin profile delete`; and `DoctorSettings` docstrings referenced
  `moneybin doctor` instead of `moneybin system doctor`. (#291)
- **Sync credentials no longer collide across profiles.** Every profile now
  gets its own opaque profile id, and Plaid-broker keychain/token storage is
  scoped to it — previously every profile shared one token slot, so
  authenticating in one profile could affect another. Profiles created before
  this change get an id automatically on their next sync. (#279)
- **`moneybin sync pull` now advances the broker's sync cursor after every
  successful load.** The sync client never acknowledged a completed pull, so
  the broker's per-institution cursor never advanced and every `sync pull`
  re-fetched the same window from Plaid instead of only what's new —
  client-side dedup masked this as wasted work rather than duplicate data. The
  client now acks the broker once the pulled data is durable; a failed ack is
  best-effort and doesn't fail the pull. (#262)
- **A timed-out MCP write call could reset a different, healthy write's
  database connection.** When `tool_timeout_seconds` was configured below the
  database write-lock wait, a call that timed out before acquiring the lock
  could trigger a global connection reset that interrupted an unrelated,
  still-running write instead of only its own. The reset now targets only the
  timed-out call's own connection, and `MCPConfig` rejects a
  `tool_timeout_seconds` below the write-lock wait outright so the unsafe
  configuration can no longer be set. (#244)
- **SQLMesh state migrations now survive a dependency version bump.** After a
  SQLMesh upgrade, the in-process state migration wrote its bookkeeping to a
  throwaway in-memory catalog that vanished at process exit, so every subsequent
  `refresh`/`transform` failed with an opaque "local version ahead of remote"
  error with no CLI way to recover. The migration now targets the persistent
  database and verifies the state actually advanced before recording success;
  `moneybin db migrate status` reports SQLMesh state-vs-package drift, and
  `moneybin db migrate apply` repairs it. (#289)
- **The Plaid `sync link` flow no longer times out mid-approval.** The browser
  link-completion poll now allows 5 minutes (its own `_LINK_POLL_DEADLINE`,
  decoupled from the 120s `/sync/trigger` timeout), so completing a real bank's
  OAuth + MFA no longer aborts the link. (#282)
- **Bare single-account imports now elicit account confirmation instead of
  erroring (M1S.4 extension).** A single-account tabular file (CSV/TSV/Excel)
  imported with no account identifier — no `--account-name`/`--account-id`, no
  `account_bindings`, and no account-name column — previously failed with a
  `ValueError` (inbox: `failed/` with `needs_account_name`). It now returns the
  M1S.4 `confirmation_required` envelope (`reason="account_confirmation"`)
  carrying an account proposal, answered through the existing `import_confirm`
  account-binding channel (`account_bindings={source_key: account_id|"new"}` /
  `--account-binding`) or `--account-name`/`--account-id`. Inbox sync routes the
  file to `pending/` (recoverable) with an account-binding sidecar; the
  `needs_account_name` error code is retired.
- **Bare-import account gate now offers a pick-list instead of a dead end.** When
  a bare single-account file (no account number, no institution match) gates for
  `account_confirmation`, the proposal previously carried `candidates: []` — the
  confirmer was told to pick an account with nothing to pick from. The resolver
  now supplies a **fallback** candidate list (the institution-scoped existing
  accounts when the source's institution is known, otherwise all accounts, capped)
  so the human or agent can adopt an existing account directly. These fallback
  candidates are decision support only — they are never eligible for silent
  auto-adopt, and confirming "new" still mints a standalone account.
- **Confirmed pending files are now archived out of `pending/`.** A successful
  `import confirm` (`import_confirm` / `moneybin import confirm`) that ratifies a
  file sitting in `pending/` now moves the file to `processed/YYYY-MM/` and
  removes its `.pending.yml` sidecar, matching inbox drain semantics. Previously
  a confirmed file lingered in `pending/`, where a later sync could re-surface it.
- **Cross-source account linking now actually fires (M1S.7).** `core.dim_accounts.last_four`
  is now derived from each source's native field (OFX `<ACCTID>` digits, Plaid
  `mask`, tabular account number/label) instead of being NULL for every
  file-imported account. The account matcher's `institution + last4` bridge can
  therefore propose linking a CSV account to its OFX/Plaid twin — previously it
  only worked when forced with an explicit `account_bindings`. Weak matches stay
  review-only: an interactive import surfaces a confirmation, an agent import
  leaves a `pending` proposal in the account-link queue, and two accounts sharing
  a last 4 both surface for review rather than auto-merging. (#257)
- **Account display names now include the last 4 again (M1S.7).** File-imported
  accounts rendered as `Institution Type` with the last-4 fragment dropped
  because `last_four` was NULL; `core.dim_accounts.display_name` now shows the
  derived last 4 (`Institution Type …NNNN`). (#257)
- **Multi-account (Tiller-style) imports record each account's own institution
  (M1S.9).** For a multi-account exporter format with a per-row Institution
  column, every account now gets its own institution (which the cross-source
  bridge can use) instead of a single shared exporter/tool name stamped on all of
  them. (#258)
- **Saved tabular formats no longer store an account label as their institution
  (M1S.8).** An auto-saved format records its resolved (filename/format)
  institution or `unknown`, never the per-account `--account-name` — a format
  describes a column layout, not an account. (#258)
- **`refresh` now rebuilds materialized models after a data-only load.** A
  second import or sync pull (new `raw.*` rows, unchanged model SQL) left
  `core.dim_accounts` — the only `FULL` model — stale and `transforms_pending`
  stuck true, because the refresh drove SQLMesh with `plan` alone (which acts on
  model-definition changes, not data). `refresh`/transform `apply` now also runs
  SQLMesh data processing (`run`) and restates `FULL` models, so newly-pulled
  accounts appear and the pending flag clears.
- **Quieter refresh/import output.** The per-connection `Synced N privacy
  classification comment(s)` line dropped from INFO to DEBUG, and sqlglot's
  `REGEXP_REPLACE with non-literal position` transpile warnings (emitted several
  times per transform) are now suppressed within the SQLMesh boundary — neither
  is actionable signal for users or agents driving the CLI/MCP.

### Security
- **The unauthenticated HTTP MCP transport is now gated behind `--insecure`.**
  `moneybin mcp serve` refuses to start any non-stdio transport (`sse`,
  `streamable-http`) unless `--insecure` is passed, exiting with a usage error
  that names the risk plainly. MoneyBin has no HTTP authentication yet, so a
  network transport would expose all financial data to anyone who can reach the
  port. With `--insecure` the server starts but prints a loud startup warning;
  stdio — the supported install path — is unaffected. Install docs and CLI help
  no longer present the unauthenticated HTTP path as a normal setup route.
  (#287)
- **CVE fixes via dependency bumps.** `cryptography`, `pydantic-settings`, and
  `python-multipart` bumped to clear 5 CVEs; `joserfc` pinned for a transitive
  `authlib`/`fastmcp` CVE. Four starlette CVEs remain suppressed in
  `pip-audit` — the fix requires starlette 1.x, unreachable while
  `sqlmesh[lsp]` pins `fastapi==0.120.1` — and aren't exposed on MoneyBin's
  stdio-only MCP transport. (#280)

### Added
- **PDF import (seed path).** Native-text PDFs import via `moneybin import <file.pdf>` and the inbox; their tables land as a queryable JSON seed (`raw.pdf_seeds`) with an auto-generated typed view (`raw.pdf_<alias>`), reversible like any import. Mapping PDFs to transactions/core is a later phase.
- **Report auto-generation framework — one runner generates every surface.**
  A report is now a single decorated runner (`@report`) that returns a
  parameterized query against its `reports.*` view; the framework introspects
  its signature and docstring to generate the MCP tool, CLI command, and
  `TableRef` wiring, and at call time executes → classifies each output column
  via the report's declared `classes` map (ADR-013) → masks CRITICAL columns →
  builds the envelope. The six view-backed reports (cashflow, spending,
  recurring, merchants, large-transactions, balance-drift) now run through it;
  their query logic and results are unchanged (the `data` envelope shape is
  normalized — see Changed). Packages contribute reports the same way.
- **Audit-log undo consumer.** `system_audit_undo`, `system_audit_history`, and
  `system_audit_get` MCP tools (plus `moneybin system audit undo|history|get`
  CLI parity) make any audited `app.*` mutation reversible as a unit keyed on
  `operation_id`. Each row's inverse is synthesized from its full audit
  before/after image and routed back through the `*Repo` layer; the undo is
  itself audited (`is_undo`/`undoes_operation_id`) and undoable. Block-don't-
  cascade: when a later operation modified the same rows, undo refuses with
  `undo_cascade_blocked` and returns the blocker operations to walk explicitly,
  rather than silently reversing unrelated later work. Notes, tags, and splits
  mutations are now routed through dedicated repos so every annotation is
  undoable. See
  [`docs/specs/data-recovery-contract.md`](docs/specs/data-recovery-contract.md).
- **`sql_query` MCP tool resolves each output column's data class via SQL lineage.**
  sqlglot parses the query, expands `*` against a migration-version-keyed schema
  snapshot, and maps every output column to the `DataClass` it derives from in
  `core.*` / `app.*`. Aggregations follow settled tier rules: `COUNT(*)` /
  `COUNT(DISTINCT col)` → LOW aggregate; `SUM`/`AVG` preserve the source class;
  `MIN`/`MAX` preserve the source class; multi-column expressions take the
  max-tier class; unresolvable projections fall back conservatively to the
  max-tier input class. Data queries are limited to the `core`/`app` schemas
  (use the `reports_*` tools for curated views); `DESCRIBE`/`SHOW`/`PRAGMA`/
  `EXPLAIN` run as low-sensitivity metadata.
- **`moneybin sql query` CLI command — the privacy-safe ad-hoc SQL path.** Full
  CLI↔MCP parity with `sql_query`: both surfaces route through one shared
  `execute_sql_query` primitive (read-only gate, core/app schema restriction,
  sqlglot lineage, CRITICAL masking), so the CLI masks account/routing numbers
  identically and raw SQL is not a privacy bypass on either surface. `--output
  text|json` returns the same envelope shape as MCP. `moneybin db query`/`db
  shell`/`db ui` remain raw, unmasked operator access and point here via their
  banner.
- **N-way dedup collapse.** Three or more copies of the same transaction now
  collapse to a single record even when the duplicates span sources *and*
  overlapping within-source files (e.g. two CSV exports plus one OFX download
  of the same statement). A union-find spanning forest groups every transitively
  linked duplicate into one connected component, so chained matches (A=B, B=C)
  resolve to one gold record instead of leaving a stray copy behind.
- **Agent/CLI-callable `transactions matches pending`.** Lists pending matches
  grouped by component (copies of the same transaction cluster together),
  mirroring the `transactions_matches_pending` MCP tool. Closes the CLI gap where
  `transactions review --type matches --status` only reported counts, never rows.
- **Agent-callable transaction match accept/reject.** `transactions_matches_set` and
  `transactions_matches_pending` MCP tools (plus `transactions_matches_run` /
  `transactions_matches_history`), `moneybin transactions matches set`, and
  non-interactive `transactions review --type matches --confirm/--reject/--confirm-all`.
  Agents and scripts can now accept or reject pending dedup/transfer proposals without
  the interactive review queue; only `pending` decisions are settable, and rejecting an
  already-accepted match surfaces a recovery action pointing at `moneybin transactions
  matches undo`.
- AI consent ledger: `moneybin privacy grant/revoke/revoke-all/status/log` CLI
  commands and `privacy_consent_grant`, `privacy_consent_revoke`,
  `privacy_status`, `privacy_log` MCP tools, backed by the new
  `app.ai_consent_grants` table. Records which AI feature categories you've
  authorized for which backend, with paired audit-log entries. (#210)
- **`moneybin system doctor` app-state integrity checks.** Doctor verifies that every recent mutation of a protected `app.*` table has a paired `app.audit_log` row, plus per-table foreign-key and uniqueness checks; a `--full` flag scans every row instead of the default sampled, recent-only window (`doctor.audit_coverage_lookback_days` / `doctor.audit_coverage_sample_cap` settings). The app-state audit-routing layer routes every protected `app.*` write through a `*Repo` so it pairs with an audit-log row in the same transaction, rolled out per table: category taxonomy and per-transaction categories, merchant mappings, categorization and proposed rules, account settings, balance assertions, and budgets (`accounts set` / `accounts balance assert` / `budget_set` previously bypassed audit), and the "edge" writers outside the service layer — saved tabular-format profiles (`app.tabular_formats`), match decisions (`app.match_decisions`), and import labels (`app.imports`). FK checks resolve `proposed_rules → categorization_rules`, `transaction_categories → core.fct_transactions`, `account_settings`/`balance_assertions` → `core.dim_accounts`, `budgets` → `core.dim_categories`, and `match_decisions` → `core.dim_accounts`. Formally Invariant 10; see [`docs/specs/app-integrity-invariant.md`](docs/specs/app-integrity-invariant.md).
- **Google Sheets as a live tabular source (M3F).** New `moneybin gsheet` CLI subgroup and `gsheet_*` MCP tools support connecting a Google Sheet via direct OAuth (Google "Desktop app" PKCE flow — no shared client secret). Two adapters at connect time: `transactions` (Tiller-style ledger → matching, categorization, and reports pipeline) and `seed` (catch-all for any sheet → JSON storage in `raw.gsheet_seeds` plus an auto-generated typed view queryable via `sql_query` and `moneybin://schema`). Every `refresh_run` re-pulls connected sheets; live mirror with `deleted_from_source_at` soft-delete preserves audit history; per-connection drift detection refuses pulls on structural change until `gsheet reconnect`. New `app.gsheet_connections` + `raw.gsheet_seeds` tables; `deleted_from_source_at` column added to `raw.tabular_transactions` (V019). See [`docs/specs/connect-gsheet.md`](docs/specs/connect-gsheet.md) and the [Google Sheets guide](docs/guides/connect-gsheet.md).
- **`transactions_categorize_run` MCP tool + `moneybin transactions categorize run` CLI command.** Run the categorization engine cascade (rules + merchants) over uncategorized transactions. Fills the gap where adding a merchant mapping previously had no agent-callable path to re-sweep — the only re-trigger path was `transactions_categorize_rules_create(reapply=True)`, which only fires during rule creation. Methods cascade in order; a rule write blocks a merchant write at the same priority. The `"ml"` literal value will be added when ML categorization implementation lands.
- **`moneybin transactions categorize assist` CLI command.** Produces the same redacted records for LLM categorization that the MCP tool returns. Service-layer enforces the redaction contract, so the CLI inherits it — both surfaces are first-class agent paths.
- **`categories_delete` MCP tool + `moneybin categories delete` CLI command.** Hard-delete a user-created category. Refuses by default if the category is referenced by transactions or budgets; `--force` / `force=True` cascades by deleting referencing rows (affected transactions return to uncategorized). Default (seeded) categories cannot be hard-deleted — disable them via `categories_set` instead. Errors map to `CATEGORY_NOT_FOUND`, `CATEGORY_IS_DEFAULT`, and `CATEGORY_HAS_REFERENCES`.
- **`refresh` umbrella across MCP and CLI** — `refresh_run` MCP tool and `moneybin refresh` CLI command are the always-visible entry points for the refresh domain (matching → SQLMesh apply → categorization). Thin wrappers over `RefreshService.refresh()` (introduced in PR #151); both return the same response envelope. `actions[]` hints in `system_status`, `import_*`, and curation tools now point at `refresh_run` instead of the operator-territory `transform_apply`.
- **`moneybin transactions categorize rules create` and `... rules delete` CLI commands.** Closes the CLI-side parity gap for rule lifecycle — MCP counterparts `transactions_categorize_rules_create` and `transactions_categorize_rules_delete` already existed. `create` supports both single-rule (`NAME --pattern X --category Y`) and batch (`--from-file rules.json`) modes; both `create` and `delete` accept `--reapply` to re-evaluate previously-categorized rows. `--output json` returns the same envelope shape as the MCP tools.
- **Agent-experience fixes across the MCP surface.** A new `ValidationErrorMiddleware` converts raw `pydantic_core.ValidationError` on bad kwargs into a standard response envelope with `error.code="invalid_arguments"` and a hint listing accepted parameter names. `reports_networth`, `reports_networth_history`, `reports_spending`, and `reports_cashflow` now populate `actions[]` with concrete next-step suggestions. New `.claude/rules/agent-experience.md` requires an agent-experience report whenever a session touches the MCP server. (The companion `moneybin_discover` no-args enhancement from this batch was superseded by the disclosure-retirement entry below in the same Unreleased cycle.)
- MCP transform tools — `transform_status`, `transform_plan`, `transform_validate`, `transform_audit` — wrap a new `TransformService` and replace the previous CLI-only surface. (`transform_apply` initially shipped here too but has since been folded into `refresh_run(steps=["transform"])` — see Removed.) See [smart-import-transform.md](docs/specs/smart-import-transform.md).
- `system_status` envelope `data.transforms` block (`pending`, `last_apply_at`) plus a `refresh_run` action hint when derived tables are stale.
- Boot-time schema-drift check: when `core.dim_accounts` or `core.fct_balances_daily` is missing expected columns, the MCP server now runs one synchronous `transform apply` self-heal attempt before raising. Closes the chicken-and-egg where the recovery tool lived inside a server that wouldn't start. `system_status` envelope surfaces a `data.schema_drift` block when drift is observed at query time. (PR #146)
- `IMPORT_BATCH_SIZE` Prometheus histogram.
- `--output json` on `moneybin transform {plan,apply,status,validate,audit}` returning the MCP envelope shape.
- **Plaid sync (M3A Phase 1):** new `moneybin sync` CLI subgroup and corresponding MCP tools (`sync_pull`, `sync_status`, `sync_link`, `sync_link_status`, `sync_disconnect`, `sync_review` prompt). Pulls accounts, transactions, and balances from Plaid-connected banks via moneybin-sync, loads into `raw.plaid_*` tables, and flows through SQLMesh staging (with sign-convention flip) into `core.fct_transactions` and `core.dim_accounts`. See [`docs/specs/sync-plaid.md`](docs/specs/sync-plaid.md).
- `ResponseEnvelope`-based responses (all MCP tools and CLI `--output json` commands) now include a top-level `status` field (`"ok"` or `"error"`), giving agents a consistent signal without testing for presence of the `error` key. **Breaking change:** all `--output json` success responses now use `{"status":"ok","data":...}` instead of per-command `{"key":...}` shapes. (PR #128)
- `--json-fields` field-projection added to `moneybin transactions list` as the reference implementation (shared `json_fields_option` + `render_or_json` infrastructure; other read-only commands will adopt progressively). Comma-separated projection: `moneybin transactions list --output json --json-fields transaction_id,date,amount`.
- Shell completion enabled: `moneybin --install-completion` and `moneybin --show-completion` now work.
- Structured JSON error envelopes: when `--output json` is active, runtime errors (DB locked, file not found, etc.) emit a machine-readable error envelope to stdout instead of plain stderr text.
- `moneybin doctor` command — read-only pipeline integrity check that runs SQLMesh named audits (FK integrity, sign convention, transfer balance), dedup reconciliation (verifies raw→core row collapse is fully accounted for by recorded dedup decisions), and categorization coverage. Exits 0 on pass/warn, 1 on fail. Supports `--verbose` for affected IDs and `--output json` for agent consumption. Registered as `system_doctor` MCP tool.
- `transactions_get` MCP tool: primary transaction read with account/date/category/amount/description filters, curation fields (notes, tags, splits), and opaque cursor pagination.
- `moneybin transactions list` CLI command with the same filter surface as `transactions_get`; supports `--output text|json`.
- MCP tool decorator now emits protocol-standard `ToolAnnotations` (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`). Clients can render confirmation UI for destructive operations.
- Decorator-level cap on list-typed tool parameters via `MCPConfig.max_items` (default 500). Exceeding the cap returns `ResponseEnvelope.error` with `code="too_many_items"`.
- `accounts_resolve` MCP tool and `moneybin accounts resolve "<query>"` CLI command — fuzzy-matches free-text references to an `account_id`.
- **`reports.*` SQLMesh views.** Eight curated presentation models — `net_worth`, `cash_flow`, `spending_trend`, `recurring_subscriptions`, `uncategorized_queue`, `merchant_activity`, `large_transactions`, `balance_drift` — back the `moneybin reports *` CLI surface and `reports_*_get` MCP tools. Inaugurates the read-only `reports.*` schema per `architecture-shared-primitives.md`.
- **`moneybin reports recurring`, `merchants`, `uncategorized`, `large-transactions`, `balance-drift`.** New CLI subcommands powered by the recipe library; pair with `--output json` for AI consumers.
- **Transaction curation surface (M2A).** Multi-note threads (`transactions_notes_add/edit/delete/list` MCP tools and `moneybin transactions notes` CLI commands), free-form tags with rename/global rename, split-transaction support (one transaction → many `core.fct_transaction_lines`), manual-entry transactions (`raw.manual_transactions` flowing through staging into `core.fct_transactions`), and a unified `app.audit_log` capturing every curation mutation with row-level + audit-row transactional atomicity. V007 schema migration. (PR #120)
- **LLM-assist categorization workflow.** `transactions_categorize_assist` MCP tool produces a redacted view of uncategorized rows (description normalized, amounts/dates/accounts excluded) for an LLM to propose `(category, subcategory, canonical_merchant_name)`; the LLM persists results via the commit tool. Service-layer enforces the redaction contract so any future surface inherits it. (PR #116)
- **Privacy DataClass registry surfaced in DuckDB column comments.** Every `core.*` and `app.*` column is classified (e.g. `IDENTIFIER`, `AMOUNT`, `DESCRIPTION`, `MERCHANT`), and the classifications sync into DuckDB `COMMENT ON COLUMN` annotations on schema init so SQL clients and MCP `sql_schema` see the classification inline. (PR #169)
- `CHANGELOG.md` (Keep-A-Changelog format) with M0/M1 history backfilled from PR titles.
- `docs/guides/threat-model.md` — one-page user-facing distillation of `privacy-data-protection.md`. What encryption protects against; what it doesn't (forgotten passphrase, malware, AI vendor data flow).
- `docs/architecture.md` (placeholder pointing forward to `architecture-shared-primitives.md` at M2B).
- `docs/audience.md` — who MoneyBin is for, today and at launch.
- `docs/roadmap.md` — milestone status (M0 through M3E + post-launch). Replaces the in-README roadmap matrix.
- `docs/features.md` — capability snapshot with per-feature guide links. Replaces the in-README "What Works Today" table.
- `docs/comparison.md` — wider 8-way competitor comparison and tier framing.
- `docs/licensing.md` — why AGPL, what it does and doesn't mean.
- `pyproject.toml` PyPI-publish-ready metadata (description, classifiers, URLs, keywords). Bumped setuptools floor to ≥77.0 for PEP 639 license metadata.

### Changed
- **`sql_query` now reports per-query sensitivity instead of a fixed tier.**
  `summary.sensitivity` reflects the highest-tier data class present in the
  actual output columns (e.g. `"low"` for a pure `COUNT(*)` aggregate,
  `"critical"` when an account-identifier column is projected). Previously the
  tool always reported a static `"high"` tier via `unclassified=True`. An agent
  branching on the `sql_schema` unknown-table error code must update: it is now
  `sql_unknown_table` (was the bare `unknown_table`).
- **Refresh now surfaces matcher/categorizer crashes (M2D PR 6).** `refresh_run` and `moneybin refresh` previously swallowed best-effort matching/categorization failures at DEBUG, so a partial pipeline (cross-source dupes accumulating, rows left uncategorized) looked healthy. `RefreshResult` gains `matching_error`, `categorization_error`, and a `self_heal_actions` list; the response envelope now carries structured `recovery_actions` (targeted `refresh_run(steps=[…])` retry plus a `system_doctor` diagnostic) when a step crashes. Real crashes log at ERROR; a first-load missing-view precondition stays a quiet DEBUG so a fresh database's first refresh doesn't report a false failure. Best-effort crashes still don't abort the pipeline or fail the command.
- **Renamed CLI `sync connect` → `sync link` and MCP `sync_connect` → `sync_link`** (with `sync_connect_status` → `sync_link_status`). Establishes the verb-split formalized in `connect-gsheet.md`: `_link` for mediated providers (Plaid-style, server holds tokens), `_connect` for user-controlled storage (direct OAuth). The Plaid sync surface keeps Plaid's "Link" mental model users already recognize. Old names retained as deprecated aliases that warn and forward; will be removed in the next minor release.
- **Error code taxonomy renamed under prefix-grouped namespaces** (M2D PR 2 — data-recovery-contract foundation). Bare-string codes emitted by `classify_user_error` and the `@mcp_tool` decorator now use prefixed forms via the new `moneybin.error_codes` module. Renames an agent might be branching on: `database_not_initialized` → `infra_database_not_initialized`, `database_locked` → `infra_database_locked`, `wrong_key` → `infra_wrong_key`, `schema_drift` → `infra_schema_drift`, `file_not_found` → `infra_file_not_found`, `io_error` → `infra_io_error`, `invalid_input` → `infra_invalid_input` (read-path default; write callers should `raise UserError(code=MUTATION_INVALID_INPUT)` directly per the in-tree migration in PRs 9a–N), `not_found` → `infra_not_found` (read-path; same write-site override applies for `MUTATION_NOT_FOUND`), `too_many_items` → `infra_too_many_items`, `timed_out` → `infra_timed_out`, `sync_error` → `sync_error` (already prefixed). Agents matching code literals against the old strings must update to the new constants. The six recovery-contract prefixes (`import_*`, `mutation_*`, `audit_*`, `refresh_*`, `undo_*`, `recovery_*`) plus `infra_*` and `sync_*` for absorbed legacy codes are documented in `src/moneybin/error_codes.py` and `docs/specs/data-recovery-contract.md` Req 3.
- **AI Code Review now emits tiered findings.** Every inline comment and summary bullet starts with 🔴 **MUST FIX** (correctness / security / breaking / missing tests, gates merge), 🟡 **CONSIDER** (substantive quality: design, refactoring, potential bugs), or 🔵 **NIT** (small consistency issues: docstring formatting, naming drift). Contributors get a scannable severity signal; agent consumers (the `fix-review` skill) can dispatch by tier — fixing all tiers on early review iterations, deferring 🟡/🔵 to `private/followups.md` on later iterations to avoid endless docstring-rewording cycles. See `CONTRIBUTING.md` § "Reading the AI review".
- **Metrics persistence: 5-minute background flush timer removed.** MCP sessions flush inside `close_db()`; CLI sessions continue to flush via `atexit` (registered conditionally on `stream="cli"` in `setup_observability`). The in-process Prometheus registry and `moneybin stats` CLI are unchanged. Future PRs will wire persistence into write transactions instead of polling.
- **Tabular CSV import: `--format chase_credit`, `--format citi_credit`, and `--format maybe` are no longer accepted** — those built-in format YAMLs were retired in favor of auto-detection, which handles the same shapes. Omit `--format` to let the detector run. As a consequence, `source_origin` for Chase/Citi/"Maybe" imports is now derived from `slugify(account_name)` instead of the format name; to preserve a stable origin across re-imports, pass `--account-name` explicitly (flows that rely only on `--account-id` will record `source_origin="unknown"`). Existing imports keep their historical `source_origin` values. (#181)
- **`transactions_categorize_stats` gains `include_auto: bool = False`.** Pass `include_auto=True` to get auto-rule health metrics (`active_auto_rules`, `pending_proposals`, `transactions_categorized`) alongside the base coverage stats in a single call. The standalone `transactions_categorize_auto_stats` MCP tool is retired; `moneybin transactions categorize auto stats` CLI remains.
- **`transactions_categorize_pending` absorbs `reports_uncategorized`.** New parameters: `sort: Literal["date","impact"] = "date"` (sorts by `ABS(amount) × age_days` when `"impact"`), `min_amount: Decimal = Decimal("0")`, `account: str | None = None` (accepts account ID or display name). Response is now richer — includes `age_days`, `priority_score`, `merchant_id`, `merchant_normalized`, `account_name`, `source_type`, `source_id` from `reports.uncategorized_queue`.
- **`reports_balance_drift` description** now leads with the question it answers: categorical drift-status view, one row per assertion. `accounts_balance_reconcile` description leads with threshold-filtered mismatch-by-day. Mutual disambiguation prose removed.
- **Reports surface: `merchant_id` propagated through `core.fct_transactions` and four `reports.*` views** (`merchant_activity`, `recurring_subscriptions`, `large_transactions`, `uncategorized_queue`). Views project `merchant_id` alongside `merchant_normalized`; aggregations GROUP/PARTITION on the FK. Transactions without a canonical merchant collapse into a single `(uncategorized)` bucket — same shape as the prior `(unknown)` text bucket, but FK-keyed. Closes the identifier-hygiene gap where a merchant rename in `app.user_merchants.canonical_name` silently re-bucketed historical aggregations.
- **`reports_uncategorized` and `reports_balance_drift` accept `display_name` or `account_id` for the `account` filter.** Resolution happens at the service boundary via the new `AccountService.resolve_strict`; ambiguous display-name matches raise `AmbiguousAccountError` (new `account_ambiguous` error code) and unknown references raise `AccountNotFoundError` (new `account_not_found` error code) instead of silently returning doubled or empty results. CLI `--account` help and MCP tool descriptions updated.
- **`app.proposed_rules.rule_id` now links proposal→active-rule** (V016 migration with one-time backfill from `app.categorization_rules` via `merchant_pattern` for approved 1:1 active-rule matches; inactive duplicates from prior override cycles are skipped so the active replacement wins, and genuinely ambiguous matches remain NULL). `approve()` writes the minted rule_id back to its source proposal; `check_overrides()` supersedes via `WHERE rule_id = ?` instead of `LOWER(merchant_pattern)`. Closes a latent bug where two approved proposals sharing a merchant_pattern would both be marked superseded.
- **Renamed MCP tool `transactions_categorize_apply` → `transactions_categorize_commit`** (and matching CLI subcommand `apply` → `commit`, `apply-from-file` → `commit-from-file`). The verb now matches the propose→review→commit workflow vocabulary documented in `transactions_categorize_assist` — the LLM proposes via `_assist`, the user reviews, and the LLM persists via `_commit`. `_apply` was historically overloaded with refresh-domain "apply transforms" (since retired in favor of `refresh_run`); the rename closes that ambiguity. Pre-launch posture: clean rename, no deprecation alias. Prometheus metric names retain the historical `apply` prefix (renaming would break downstream dashboards).
- **MCP read tools dropped the `_list` suffix** to match the noun-only convention for collection / summary / aggregate / time-series reads (shape 5 of `.claude/rules/surface-design.md`). Renames: `categories_list` → `categories`, `merchants_list` → `merchants`, `import_formats_list` → `import_formats`, `import_inbox_list` → `import_inbox_pending` (disambiguated from the CLI bare-callable `moneybin import inbox` drain), `system_audit_list` → `system_audit`, `accounts_list` → `accounts`, `accounts_balance_list` → `accounts_balances` (plural), `accounts_balance_assertions_list` → `accounts_balance_assertions`, `transactions_categorize_rules_list` → `transactions_categorize_rules`, `transactions_categorize_pending_list` → `transactions_categorize_pending`. Hard cut, no deprecation aliases (pre-launch posture per `design-principles.md`). CLI subcommands (`moneybin <group> list`) are unchanged — surface-idiom divergence is intentional. MCP clients with cached tool lists must call the new names.
- **`category_id` FK introduced across seven `app.*` tables** (`transaction_categories`, `budgets`, `user_merchants`, `transaction_splits`, `categorization_rules`, `proposed_rules`, `rule_deactivations`) referencing `core.dim_categories.category_id`. Writers dual-write FK + text; readers (`core.fct_transactions`, `core.fct_transaction_lines`, `core.dim_merchants`) prefer the FK-resolved name and fall back to the text snapshot for orphans. `categories_delete` now cascades across all six writer tables via FK; audit-trail rows in `rule_deactivations` retain unresolvable FKs intentionally. Migrations V014 (backfill all seven tables) and V015 (drop `UNIQUE (category, subcategory)` on `user_categories`). The text-column drop is tracked as Phase 2 follow-up work.
- **Accounts CRUD-to-set collapse.** `accounts_set` (MCP) and `moneybin accounts set` (CLI) now cover every settings field on an account. Three behavioral fields fold in: `display_name` (replaces `accounts_rename`), `include_in_net_worth` (replaces `accounts_include` / `accounts set --include/--exclude`), and `is_archived` (replaces `accounts_archive` and `accounts_unarchive` / `accounts set --archive/--unarchive`). Archiving still cascades `include_in_net_worth=False` atomically; unarchiving does NOT auto-restore include. Service-layer `archive`/`unarchive`/`rename`/`set_include_in_net_worth` survive as thin deprecation delegates for internal callers. Hard cut on the public surfaces — no deprecation aliases (pre-launch posture per `design-principles.md`).
- **MCP tool renamed:** `categories_toggle` → `categories_set`. Matches the `_set` verb established by `budget_set` and `accounts_set` for shape-1b partial-update tools. Same behavior (flip `is_active`); only the verb changes. CLI command renamed in lockstep: `moneybin categories toggle` → `moneybin categories set`. Pre-launch, no deprecation alias.
- **Tool descriptions updated** to document defended exceptions inline: `accounts_balance_assert` (shape-1b upsert despite verb-shaped name), `transactions_tags_rename` (multi-row global mutation despite singular-shaped signature), `transactions_notes_*` (lifecycle-with-id triad), `accounts_balance_reconcile` vs `reports_balance_drift` (per-day numeric threshold filter vs per-assertion-date categorical drift series).
- **MCP money amounts are now JSON numbers, not quoted strings.** `Decimal` fields in the response envelope serialize as JSON numbers (`219584.05`) instead of strings (`"219584.05"`). Internal `Decimal` precision is preserved; the wire format matches what agents and JSON tooling expect by default. `DECIMAL(18,2)` (amounts) and `DECIMAL(18,8)` (prices/quantities/FX) both fit inside float64.
- **`reports.spending_trend.year_month` and `reports.cash_flow.year_month` are now `'YYYY-MM'` strings**, not DATE truncated-to-first-of-month. The output column matches the input parameter format (`from_month`/`to_month`). Existing callers that pass `'YYYY-MM-DD'` still work — the service strips the day before comparison.
- **`reports_spending` and `reports_cashflow` default to the last 12 months** when both `from_month` and `to_month` are omitted, instead of returning every historical month. `actions[]` includes a hint for widening or shifting the window. Agents that need the full history pass an explicit `from_month`.
- **`sql_schema` defaults to a compact catalog** (table names + purposes + column counts) instead of dumping the full ~50KB schema doc. Pass `table='<schema.name>'` for one table's columns and example queries, or `table='*'` for the full document.
- **OFX descriptions are now HTML-entity-decoded at import.** `_decode_text_field` repeatedly applies `html.unescape` to `payee` and `memo` until stable, fixing double-escaped bank output (e.g. Wells Fargo's `AT&amp;amp;T` → `AT&T`). Existing already-imported rows stay as-is until re-import.
- **Refresh is now a top-level domain concept.** Introduced `moneybin.services.refresh.refresh(db) -> RefreshResult` — the post-load pipeline that runs cross-source matching, SQLMesh apply, and deterministic categorization on the current database state. `ImportService.apply_post_import_hooks()`, `_apply_post_import_hooks()`, and the `PostImportHookResult` dataclass are removed; callers (`ImportService.import_files`, `InboxService.sync`, `SyncService.pull`) now invoke `refresh()` directly. Matching and categorization were always source-agnostic; "refresh" names what they do without implying file-import provenance.
- **`moneybin sync pull` auto-runs refresh by default.** After a successful Plaid sync that changes raw state (loads new rows or processes removals), `SyncService.pull()` runs the refresh pipeline once before returning, so `core.dim_accounts` and other derived models reflect the new data immediately. Pass `--no-refresh` (CLI) or `refresh=False` (MCP `sync_pull`) to defer. SQLMesh failures surface as `transforms_applied=false` + `transforms_error` in the result envelope (raw rows stay durable, CLI exits non-zero so agents detect the stale state); matching and categorization are best-effort and log-only on failure. High-frequency callers should defer refresh and schedule it separately — SQLMesh apply dominates pull latency (typically 5–30s).
- **Renamed: `apply_transforms` → `refresh` everywhere.** CLI flags `--apply-transforms/--no-apply-transforms` are now `--refresh/--no-refresh` on `moneybin sync pull` and `moneybin import files`. MCP parameters `apply_transforms` on `sync_pull`, `import_files`, and `import_inbox_sync` are now `refresh`. Service kwargs on `SyncService.pull`, `ImportService.import_file`, `ImportService.import_files`, `InboxService.sync` follow the same rename. Result-envelope fields (`transforms_applied`, `transforms_duration_seconds`, `transforms_error`) keep their names — they describe the SQLMesh-step outcome specifically, which is the only step that surfaces a structured error.
- **Breaking:** MCP `import_file` renamed to `import_files`; accepts `paths: list[str]` and applies transforms once at end of batch. Per-file overrides (`account_name`, `institution`, `format_name`) are no longer exposed on the MCP surface — use the CLI for those.
- **Breaking:** CLI `moneybin import file PATH` renamed to `moneybin import files PATHS...`; the `--skip-transform` flag is replaced by `--apply-transforms / --no-apply-transforms` (default on).
- `moneybin import inbox` and the `import_inbox_sync` MCP tool route through the batch import path; transforms now run once per inbox drain instead of once per file.
- Replace long-lived database singleton with short-lived per-call connections (`get_database(read_only=True/False)`). Write connections retry on lock contention with exponential backoff; read-only connections coexist across processes. New exceptions: `DatabaseLockError`, `DatabaseNotInitializedError`. (#131)
- Renamed `moneybin mcp config generate --install` to `moneybin mcp install`. Default behavior writes the client config; `--print` opts out. Hard cut, no alias. `mcp config path` (lookup-only) is unchanged.
- Tool description audit: every existing `@mcp_tool` description was reviewed against the sign-convention, currency, and mutation-surface invariant rules. Missing invariants were appended; descriptions otherwise unchanged.
- `transactions_categorize_rules_create` (and `CategorizationService.create_rules`) is now idempotent. Each input is deduped against active rules by the matcher+output tuple `(merchant_pattern, match_type, min/max_amount, account_id, category, subcategory)`; `name` and `priority` are metadata and excluded from the key. A retry of the same payload returns the existing `rule_id`s and creates no new rows. The result envelope gains an `existing` counter alongside `created`/`skipped`. Same matcher with a *different* category output still creates a new row — rule-conflict detection is a deferred follow-up.
- Internal rename: `BulkCategorizationResult` → `CategorizationResult`, `bulk_categorize` → `categorize_items`, `validate_bulk_items` → `validate_items`. The "bulk" qualifier is dropped from MoneyBin's surface — list inputs are the default, not the exceptional case.
- Prometheus metric names renamed: `moneybin_categorize_bulk_items_total` → `moneybin_categorize_items_total`, `moneybin_categorize_bulk_duration_seconds` → `moneybin_categorize_duration_seconds`, `moneybin_categorize_bulk_errors_total` → `moneybin_categorize_errors_total`. External dashboards/alerts referencing the old names need updating.
- **Categorization matcher input extended** to memo and structural fields. The deterministic matcher and the LLM-assist redacted view now both consume `match_text = description + memo` plus `transaction_type`, `check_number`, `is_transfer`, `transfer_pair_id`, `payment_channel`, and `amount_sign`. Aggregator transactions (PayPal, Venmo, Zelle, generic ACH) match on the wrapped merchant identity in memo instead of failing on the truncated description. Pattern matching is per-field so user-authored `exact` and anchored-`regex` patterns continue to hit the original field when memo is present. (PR #122)
- **`categorize assist` / `categorize commit` JSON envelope** (then named `categorize apply`; see Changed) carries `transaction_id` as the per-row key (no separate opaque identifier). Export files produced by `categorize assist` flow back into the commit tool unchanged. (PR #122)
- **LLM-assist redaction contract expanded.** The redactor now runs over `memo` in addition to `description`, and structural fields (`transaction_type`, `check_number`, `is_transfer`, `transfer_pair_id`, `payment_channel`, `amount_sign`) are exposed to the LLM as signals. The no-amount / no-date / no-account guarantee is preserved. (PR #122)
- **`transactions_categorize_commit` triggers auto-fan-out** (then named `transactions_categorize_apply`; see Changed). After the batch commits, `categorize_pending()` runs once to apply newly-created merchants and exemplars to remaining uncategorized rows in the same dataset. The "snowball" the cold-start spec promised now works — by the third or fourth import, the LLM is meaningfully less involved. (PR #122)
- **Auto-created merchants accumulate exemplars instead of inventing patterns.** When LLM-assist categorizes a row and proposes a `canonical_merchant_name`, the system appends the exact normalized `match_text` to a `oneOf` exemplar set on the merchant — it no longer creates a `contains` pattern from the full normalized description. Aggregator strings like `PAYPAL INST XFER` no longer over-match across unrelated transactions. (PR #122)
- **Source-precedence enforcement on write.** All categorization writes route through a single guarded path that compares the incoming source's priority against the existing row's. A user manual edit (`'user'`) can never be overwritten by any subsequent rule, merchant, or LLM-assist run. The `categorized_by` column is the lock; no separate lock table. (PR #122)
- **`core.agg_net_worth` retired.** Net worth aggregation now lives at `reports.net_worth` (same SELECT body, new schema) per the `reports.*` convention introduced in `architecture-shared-primitives.md`. Existing `moneybin reports networth` commands and `reports_networth_*` MCP tools transparently repointed.
- **Per-row `updated_at` on `core.*` models.** `updated_at` is now the `MAX` of contributing per-row input timestamps (NULL where all inputs are model-level seeds), instead of `CURRENT_TIMESTAMP` set at SQLMesh refresh time — so `core.dim_accounts.updated_at` / `core.fct_transactions.updated_at` reflect actual row changes instead of looking new after every transform. Model-level freshness is exposed separately via `meta.model_freshness`, which wraps SQLMesh's `_snapshots`. Adds `updated_at` to `app.user_categories`, `app.user_merchants`, and `app.category_overrides`. See [`core-updated-at-convention.md`](docs/specs/core-updated-at-convention.md). (PR #141)
- **`app.categories` and `app.merchants` views retired.** The resolved-dimension views (seeds + user state + overrides) now live as SQLMesh-managed `core.dim_categories` and `core.dim_merchants`. Consumer code already routed through the `TableRef` constants; no API change.
- **Milestone taxonomy re-unified into phase-aligned milestones (2026-05-30).** Replaced the flat M0–M3F grid — where the numbers had stopped tracking the build sequence — with four phase milestones: **M0 Foundation, M1 Ingestion Core, M2 Analysis & Reports, M3 Productization & Distribution**, each with lettered increments (`M1J`) and `.N` work items, and each closed by a test-functionality gate. The phase *is* the gate, so testing batches at four milestones rather than per-increment. `docs/roadmap.md` carries the new scheme and the old→new mapping; dated CHANGELOG history keeps its original labels.
- **Milestone terminology unified.** Retired "Level 0/1" + "Wave 2A/2B/2C/Wave 3" dual systems for one consistent **milestone** convention: M0, M1, M2A, M2B, M2C, M3A, M3B, M3C, M3D, M3E, Post-launch. M3 decomposes into sub-milestones because it has parallel domain (Plaid/investments/multi-currency) and surface (Web UI/hosted) tracks. M3E closing = launch.
- **README significantly tightened** — from ~196 lines to ~115 lines. Storefront pattern: tagline preserved, status callout + Why-bullets + How-It-Works diagram + Quick Start + 5×5 ✓/✗ comparison + Documentation/Community/Contributing/License pointers. In-README roadmap matrix removed (lives in `docs/roadmap.md`); detailed feature inventory removed (lives in `docs/features.md`); 8-column comparison table replaced with tight 5×5 (full version in `docs/comparison.md`); License essay condensed (full rationale in `docs/licensing.md`). Modeled on Bitwarden, Plausible, DuckDB, SQLMesh peer-set conventions.
- `.claude/rules/shipping.md` extended with the post-implementation checklist for `CHANGELOG.md`, `docs/roadmap.md`, `docs/features.md`. Documents what does and doesn't earn a CHANGELOG entry.
- `CONTRIBUTING.md` "Where the strategy lives" expanded to include the new docs and a one-line CHANGELOG rule.
- **Spec rename for surface symmetry.** `docs/specs/mcp-tool-surface.md` → `docs/specs/moneybin-mcp.md`; `docs/specs/cli-restructure.md` → `docs/specs/moneybin-cli.md`. Establishes the `moneybin-<surface>.md` naming pattern (extends to a future `moneybin-rest-api.md`). New cross-surface spec [`docs/specs/moneybin-capabilities.md`](docs/specs/moneybin-capabilities.md) maps user-facing capabilities to per-surface registered names; the `.claude/rules/mcp-server.md` "Surface change discipline" rule now requires every tool/command PR to update both the surface-specific spec AND the capabilities map. `git log --follow` works across the rename for history; bookmarks to the old paths should be updated.
- **Breaking — CLI/MCP naming pass (noun-only for query/read surfaces).** Applies the `mcp-server.md` "Tool Taxonomy" convention to ~14 tool/command name pairs that diverged between MCP and CLI. **Reports family (10 names):** MCP `reports_{networth,networth_history,spending,cashflow,recurring,merchants,uncategorized,large_transactions,balance_drift}_get` drop the `_get` suffix; MCP `reports_budget_status` → `reports_budget`. CLI counterparts: `reports networth show` → `reports networth`; `reports networth history` → `reports networth-history`; `reports {cashflow,spending,recurring,merchants,uncategorized,large-transactions,balance-drift} show` → leaf-only equivalents (each sub-app collapses). **Accounts:** CLI `accounts show` → `accounts get` (matches existing MCP `accounts_get`); MCP `accounts_settings_update` → `accounts_set` (matches existing CLI `accounts set`); CLI `accounts balance delete` → `accounts balance assertion-delete` (matches MCP `accounts_balance_assertion_delete`; clarifies scope — deletes the assertion row, not the balance). **Transactions:** MCP `transactions_review_status` → `transactions_review`; MCP `transactions_categorize_rule_delete` → `transactions_categorize_rules_delete` (plural matches sibling `_rules_create`). **Import:** MCP `import_list_formats` → `import_formats_list` (matches existing CLI `import formats list`). **System:** CLI `moneybin doctor` → `moneybin system doctor` (top-level leaf moves under the `system` group, matching MCP `system_doctor`). Shrinks the `tests/integration/test_surface_parity.py` name-drift backlog from 30 MCP-only + 57 CLI-only to 14 + 41 (32 fewer entries). Hard cut, no deprecation aliases (pre-launch posture per `design-principles.md`).
- **`refresh_run` MCP tool gains `steps` parameter; `moneybin refresh` CLI gains `--step` flag.** Optional `list[Literal["match", "transform", "categorize"]]` (MCP) / repeatable `--step` (CLI) scopes which sub-operations execute. Defaults preserved — `refresh_run()` and `moneybin refresh` still run the full cascade. Steps always execute in canonical order (match → transform → categorize) regardless of input order. Symmetric with `transactions_categorize_run(methods=...)`. Unknown step names raise `UserError(code="UNKNOWN_REFRESH_STEP")`.
- **`schema_drift.remediation` and `categories_list` action hints now point at `moneybin refresh`** rather than the operator-territory CLI form `moneybin transform apply`. Agents that hit schema drift or seeded-category gaps get pointed at the umbrella surface that's symmetric with `refresh_run`.
- **Tabular import no longer silently negates inverted-sign amounts.** When the running-balance check detects that amounts appear to be sign-inverted, amounts are imported as-is and a `⚠ Sign convention may be inverted` warning is emitted to stderr. Previously, MoneyBin auto-flipped the signs without notification. Re-run with `--sign` to override explicitly.

### Removed
- **MCP tool `transactions_categorize_auto_stats`** — folded into `transactions_categorize_stats(include_auto=True)`. CLI `moneybin transactions categorize auto stats` is unaffected.
- **MCP tool `reports_uncategorized` and CLI `moneybin reports uncategorized`** — folded into `transactions_categorize_pending` with `sort`, `min_amount`, and `account` parameters. `ReportsService.uncategorized_queue` removed; `CategorizationService.list_uncategorized_transactions` is the canonical path. **Migration note:** the previous tool always sorted by impact (`priority_score DESC`); the replacement defaults to `sort="date"` — pass `sort="impact"` to preserve the prior impact-ranked order.
- **MCP tools `accounts_rename`, `accounts_include`, `accounts_archive`, `accounts_unarchive`** — folded into `accounts_set`.
- **CLI commands `moneybin accounts rename`, `accounts include`, `accounts archive`, `accounts unarchive`** — folded into `moneybin accounts set` with new flags (`--display-name`, `--include/--exclude`, `--archive/--unarchive`, `--clear-display-name`).
- **Client-driven progressive disclosure retired.** Removed the `moneybin_discover` MCP meta-tool, the `MoneyBinSettings.mcp.progressive_disclosure` setting, and the `Visibility(False, tags=...)` server transform. The full registered tool surface is now visible at connect, with orientation delivered through the FastMCP `instructions` field and prefix-grouped tool names. Rationale: `tools/list_changed` client support is too uneven (Claude Desktop unreliable, most generic clients ignore) to design a portable disclosure mechanism around. The `@mcp_tool(domain=...)` decorator argument is preserved as dormant metadata. `moneybin://tools` resource shape simplified from `{core, extended, discover_tool}` to a flat `{namespaces}` list. Server `instructions` text trimmed from ~750 to ~180 tokens by dropping per-tool subsections already covered by tool descriptions. See `docs/specs/mcp-architecture.md` §3 "Tool disclosure: full surface, taxonomy-led".
- **MCP tools `budget_set`, `tax_w2`, `tax_deductions` and the `tax_prep` prompt de-registered** under the new stub-gating rule in `.claude/rules/mcp-server.md`. `budget-tracking.md` is `draft` (today's `budget_set` is a partial slice of the planned set/status/delete + rollovers feature); there is no backing tax spec at all. Tool implementations remain in `src/moneybin/mcp/tools/budget.py` and `tools/tax.py` as dormant building blocks — only the `register_*_tools(mcp)` call is gated. **CLI counterparts (`moneybin budget set`, `moneybin tax w2`, `moneybin tax deductions`) are unaffected** and still work. Re-register when each backing spec reaches `in-progress` or `implemented`. Tracked in `moneybin-mcp.md` §17 "Dependency tracker".
- **W-2 PDF extraction removed entirely.** The `moneybin tax w2` CLI command, `tax_w2` MCP tool, W-2 extractor and loader, `raw.w2_forms` schema table, and `TaxService` are deleted. PDF parsing dependencies (`pdfplumber`, `pytesseract`, `pdf2image`, `pillow`) dropped from the package. The IRS form layout changes annually and LLM-mediated PDF parsing is likely a better primitive than pdfplumber/tesseract for tax data; architecture will be revisited in a future brainstorm. The `docs/specs/archived/w2-extraction.md` spec documents the removed design.
- **MCP tool `transactions_recurring_list`** — duplicate of `reports_recurring` which is strictly richer (confidence scores, cadence, status filter, annualized cost). Consumers using `transactions_recurring_list` should call `reports_recurring` instead. Removed as a duplicate surface.
- `transactions_search` MCP tool (superseded by `transactions_get`, which covers all its filters plus multi-account, multi-category, curation fields, and cursor-based pagination).
- **Seed merchant catalogs retired.** The `seeds.merchants_global/us/ca` SQLMesh seeds, paired `app.merchant_overrides` table, and `'seed'` value in the `categorized_by` precedence enum are removed. `core.dim_merchants` is now a thin view over `app.user_merchants`; all merchants are user-created or system-created on the user's behalf (LLM-assist, auto-rule, Plaid, migration). The original cold-start design layered a curated catalog as priority 7; it shipped as plumbing but the catalog was never populated. Cold-start now relies on Plaid pass-through (when synced) + migration imports + LLM-assist + the auto-rule snowball. V012 migration drops `app.merchant_overrides` on existing databases. Spec amendments in `docs/specs/categorization-cold-start.md` and `categorization-matching-mechanics.md`.
- **`transform_apply` MCP tool.** Folded into `refresh_run(steps=["transform"])`. The granular CLI command `moneybin transform apply` remains as the operator path; only the MCP surface was retired. Pre-launch posture — no deprecation alias. Clients with cached tool lists that call `transform_apply` will receive a tool-not-found error; replace with `refresh_run(steps=["transform"])`.
- **MCP tools `sync_schedule_set`, `sync_schedule_show`, `sync_schedule_remove` removed.** These were stubs returning `not_implemented` — no backing spec and no implementation. The schedule use case is tracked but unbuilt; these tools were surface noise. On `refresh_run` apply failure, the hint now points at `moneybin transform plan` (CLI) rather than the removed MCP tool.
- **MCP tools `transform_status`, `transform_plan`, `transform_validate`, `transform_audit` de-registered from MCP.** These SQLMesh introspection tools are operator territory (category 2, mcp-server.md "When CLI-only is justified") — hands-on developer tooling with no meaningful agent use case absent a code change. CLI commands `moneybin transform status|plan|validate|audit` are unchanged. Tool implementation files remain in place; only the MCP registration is removed.
- **MCP resources `moneybin://status`, `moneybin://accounts`, `moneybin://privacy`, `moneybin://tools`, `accounts://summary`, `moneybin://recent-curation`, `net-worth://summary` removed.** These seven resources duplicated data already reachable via tools and added context-window overhead on every connect. `moneybin://schema` is retained — it has unique composition value for SQL generation that no single tool replicates.

### Security
- **Account/routing-number columns in raw `sql_query` results are now masked,**
  closing the raw-SQL masking bypass. CRITICAL-tier columns
  (`ACCOUNT_IDENTIFIER`, `INSTITUTION_ACCOUNT_NUMBER`, `ROUTING_NUMBER`) are
  masked with the same transforms the typed tools apply (`****<last4>` for
  account numbers, `*****` for routing numbers) — `sql_query` is no longer a
  privileged escape hatch around the privacy middleware.
- **Privacy middleware shipped.** Account numbers, routing numbers, and other CRITICAL-tier fields are now masked by default in every MCP tool response and CLI `--output json` output. Masking is type-driven: tools declare `-> ResponseEnvelope[PayloadType]` whose fields carry `Annotated[..., DataClass.X]` registry markers; the runtime walks the type, derives sensitivity as the max tier across all annotated fields, applies per-class transforms (e.g. account number → `****<last4>`), and writes a structured event to `<profile>/privacy.log.jsonl`. `@mcp_tool` no longer accepts a `sensitivity=` kwarg — sensitivity is derived at registration time and tool registration fails at import if the return type lacks classification. `ResponseEnvelope` is now generic over the payload type. CLI `--output json` runs through the same redactor + log writer; text output bypasses (caller's renderer owns formatting). The `unclassified=True` opt-out on `@mcp_tool` is the documented escape hatch for `sql_query` / `sql_schema`, whose payload shape is decided by the caller's input (PR 4 replaces with sqlglot lineage). See [`docs/specs/privacy-data-classification.md`](docs/specs/privacy-data-classification.md) §"Implemented middleware". (PR #192)
- Profile directories now created with `0o700` permissions (previously `0o755`), matching the `0o600` mode of the privacy event log and the privacy-sensitive nature of per-profile state (encrypted DB, secrets, daily events). (PR #192)

### Fixed
- **Cross-format duplicates no longer double-count.** The same transaction imported from two formats of one account (e.g. Wells Fargo `.qfx` and `.csv`) now collapses into one `core.fct_transactions` row with `source_count=2` instead of two rows. Previously, OFX truncating descriptions differently from CSV pushed cross-format similarity below the auto-merge threshold, so exact duplicates (same account + exact amount + same day) never merged — importing 5 WF `.csv` twins of 5 already-loaded `.qfx` produced 558 rows instead of 279. Exact-key cross-source pairs now auto-merge regardless of description similarity, with a source-cardinality guard that keeps N genuinely-distinct same-key transactions paired 1:1 rather than over-collapsing. See [`docs/specs/matching-exact-key-dedup.md`](docs/specs/matching-exact-key-dedup.md).
- `moneybin mcp serve` no longer corrupts the MCP JSON-RPC stream when no profile is configured. Previously the first-run wizard wrote a welcome banner to stdout, producing a cascade of "is not valid JSON" parse errors in the host (e.g. Claude Desktop). The server now boots regardless and, on the first tool call, guides setup: elicitation-capable clients are asked for a profile name and the profile is created in place (no restart); tools-only clients receive a single `infra_setup_required` message pointing to `moneybin profile create`. See [`docs/specs/mcp-first-run-setup.md`](docs/specs/mcp-first-run-setup.md).
- Every CLI and MCP entry point crashed at startup on databases created before PR #178 with `BinderException: Table "proposed_rules" does not have a column named "rule_id"`. The schema DDL (which runs before migrations) declared a `CREATE INDEX` on the V016-added `rule_id` column, binding against the pre-V016 table shape before V016 could add the column. The index now lives only in V016, where it belongs; V016 also commits the backfill before creating the index so DuckDB's "Cannot create index with outstanding updates" no longer blocks the upgrade path (same class as V010/V011, see PR #148).
- Migration runner self-heals stuck failure rows when the migration body has changed. Previously, a `success=false` row in `app.schema_migrations` from a prior failure required manual deletion before the next attempt would run. The runner now hashes every migration body, and if a previously-failed migration's body has changed since the failure, the stale row is auto-cleared and the migration retries once. Push the fix, tell users to re-run — no manual cleanup. (PR #156)
- V010 and V011 migrations crashed on existing populated databases with "Cannot create index with outstanding updates" because `ADD COLUMN ... DEFAULT` plus `SET NOT NULL` ran inside the same transaction. The two statements are now split across `COMMIT` / `BEGIN TRANSACTION` so the backfill writes flush before the NOT NULL constraint index builds. Recoverable from a crash via the existing idempotent re-run branch. (PR #148)
- Non-CLI SQLMesh entry points — the SQLMesh VSCode extension, direct `sqlmesh` shell invocations, and the language server — now honor `MONEYBIN_PROFILE`. Previously they loaded `sqlmesh/config.py` without running the MoneyBin CLI callback that registers the profile resolver, raising on `get_settings()`. (PR #160)
- Five categorization correctness bugs surfaced by live OFX checking-account testing: `memo` was dropped from the matcher and LLM input; `_match_description` only operated on `description`; system-generated merchants used over-generalizing `contains` patterns; `categorize_pending` was never called after the categorize-commit tool (then `transactions_categorize_apply`) so the snowball couldn't roll; OFX `<NAME>` truncation hid merchant identity in `<MEMO>` that the matcher never saw. See [`docs/specs/categorization-matching-mechanics.md`](docs/specs/categorization-matching-mechanics.md) for the full diagnosis. (PR #122)

### Security
- CVE fixes via dependency bumps: `urllib3` 2.6.3 → 2.7.0 (PR #127); `pip` and `python-multipart` advisories addressed (PR #124).

---

## [M1] — 2026-05-04 (Data Integrity)

Five M1 deliverables shipped plus companion work. `fct_transactions` is now trustworthy: dedup eliminates double-counting, transfer detection prevents transfer-as-spend distortion, auto-rules categorize new imports, net-worth tracks balances with self-healing reconciliation deltas.

### Added
- **Smart tabular importer** for CSV / TSV / Excel / Parquet / Feather with heuristic column detection, multi-account support, and migration profiles for Tiller, Mint, YNAB, and Maybe. Five-stage pipeline (Format Detection → Reader → Column Mapping → Transform & Validate → Load), three-tier confidence model, `TabularProfile` system with auto-save, `Database.ingest_dataframe()` primitive (#38).
- **OFX/QFX/QBO import parity** through the same `import_log` infrastructure as tabular: re-import detection, `--force` override, institution name auto-resolution from `<FI><ORG>` / FID lookup / filename heuristics, batch revert via `moneybin import revert <id>` (#82, #90).
- **Watched-folder inbox UX** at `~/Documents/MoneyBin/<profile>/inbox/`. `moneybin import inbox` drains successes to `processed/YYYY-MM/` and failures to `failed/YYYY-MM/` with YAML error sidecars. Per-profile lockfile + crash-recovery via staging-rename (#84).
- **Cross-source dedup** with SHA-256 content hashes and golden-record merge. `prep.seed_source_priority` config-driven seed table, `int_transactions__matched` view, `meta.fct_transaction_provenance` (#43, follow-ups #46).
- **Transfer detection** across accounts: shared matching engine Tier 4, `core.bridge_transfers`, always-review v1, four-signal scoring (date distance, keyword, roundness, pair frequency). `is_transfer` and `transfer_pair_id` on `fct_transactions` (#47).
- **Auto-rule learning** from user edits: merchant-first pattern extraction, `app.proposed_rules` review queue with four-state lifecycle, promotion to `app.categorization_rules` at priority 200, correction-handling threshold (#58, follow-ups #60).
- **`moneybin categorize bulk`** CLI with parity for the `categorize_bulk` MCP tool; `BulkRecordingContext` drops per-item DB lookups (#69).
- **Account management namespace.** `accounts list/show/rename/include/archive/unarchive/set` with Plaid-parity metadata (subtype, holder category, currency, credit limit, last four). Reversible account merging via bridge model. `app.account_settings` for display preferences and net-worth inclusion (#107).
- **Net-worth & balance tracking.** `accounts balance show/history/assert/list/delete/reconcile` per-account workflow; `reports networth show/history` cross-account rollup with period-over-period change. Three-model SQLMesh pipeline: `core.fct_balances` (VIEW) → `core.fct_balances_daily` (TABLE, daily carry-forward interpolation) → `core.agg_net_worth` (VIEW). Reconciliation deltas computed and self-healing on reimport (#107).
- **10-scenario test suite** with five-tier assertion taxonomy: structural invariants, semantic correctness (categorization P/R, transfer F1+P+R, negative expectations), pipeline behavior (idempotency, empty/malformed input handling), quality (date continuity, ground-truth coverage), operational. Bug-report recipe documented (#70, PRs #70–#83).
- **Whole-pipeline scenario runner.** Empty encrypted DB → `generate → transform → match → categorize` → assertions/expectations/evaluations against synthetic ground truth and hand-labeled fixtures. `make test-scenarios`. Validation primitives at `src/moneybin/validation/` reusable for live-data `data verify` (#59, #80).
- **Curated `moneybin://schema` MCP resource** + `sql_schema` tool mirror exposing core and select app interface tables with column comments and example queries — eliminates per-session schema reconnaissance (#87, #91).
- **MCP tool wall-clock timeouts** (configurable 30s default) with DuckDB `interrupt()` + connection close on timeout, so a hung tool can't wedge the server's write lock (#97).
- **MCP client install** across nine clients: claude-desktop, claude-code, cursor, windsurf, vscode, gemini-cli, codex (CLI / Desktop / IDE), chatgpt-desktop. Concurrency guide for the single-writer DuckDB lock (#94).
- **v2 MCP/CLI taxonomy.** Path-prefix-verb-suffix naming, entity groups (`accounts`, `transactions`), reference-data groups (`categories`, `merchants`), `reports` for cross-domain rollups, `system` for orientation, `tax` separated, `assets` reserved. ~50-tool rename map applied as a hard cut (#95, #96).
- **YAML golden cases** for `normalize_description()`; parametrized exact-equality tests; contributor-facing surface for adding real-world transaction descriptions (#66).

### Changed
- FastMCP 3.x adoption with per-session visibility (#71, #72).
- `CategorizationService` thin-wrapper consolidation across MCP, CLI, and service callers (#108).
- Simplify passes across `src/moneybin/` subsystems: matching, services, MCP tools, validation (#75, #76, #77, #79, #110).
- pytest-asyncio auto-mode; dropped `asyncio.run` boilerplate (#109).
- Tests run in parallel via pytest-xdist (#67).

### Fixed
- MCP tool names regex compliance for Anthropic/OpenAI clients (#89).
- Schema-mismatch crash on existing DB with stale schema; auto-reopen with migration (#88).
- App-table purpose strings overwritten by stale comments (#92).
- Migration auto-apply gate + inbox error surfacing (#93).
- SQLMesh fork-pool orphan processes causing MCP timeouts (#105).
- CLI `main` shadowing rename (#104).
- MCP schema drift coverage extended to `app.*` interface tables (#106).
- Account matching wired into the tabular import pipeline; `Decimal` end-to-end for monetary values; N+1 merchant batch fix; `ResolvedMapping` refactor (#51–#56).
- N+1 `COUNT(*)` queries in `db info` collapsed into one UNION ALL (#81).

---

## [M0] — 2026-04-30 (Infrastructure)

Foundational systems shipped: encryption-at-rest, schema migrations, observability, profiles, CLI/MCP scaffolding, and the synthetic data generator. Every M1+ feature builds on these.

### Added
- **AES-256-GCM database encryption at rest** via DuckDB's encryption extension. Argon2id KDF for passphrase mode; OS keychain integration for auto-key mode. `Database` connection factory (singleton `get_database()`), `SecretStore` for unified keyring + env-var secret retrieval, `SanitizedLogFormatter` PII safety net on all log handlers. Encryption CLI: `db init/lock/unlock/rotate-key/backup/restore/key show` (#29).
- **Profile system** with `~/.moneybin/profiles/{name}/` isolation. `moneybin profile create/list/switch/delete/show/set` (#30).
- **CLI restructure v1.** Domain command groups, `get_base_dir()` rewrite (defaults to `~/.moneybin/`), `transform` and `categorize` as top-level groups, `db ps`/`db kill`, `mcp list-tools/list-prompts/config generate --install`, `transform status/validate/audit/restate` (thin SQLMesh wrappers), `logs clean/path/tail`. Stubs for future command groups (#30).
- **Dual-path schema migration system.** SQL + Python migrations, auto-upgrade on first invocation, `app.versions` tracking, rebaseline command, SQLMesh version detection. Encrypted-database aware (#31).
- **Observability stack.** Single canonical `LoggingConfig`, `SanitizedLogFormatter` on all handlers, MCP server logging strategy (stderr for hosted, file for local), `prometheus_client` metrics with DuckDB persistence (flush on shutdown + periodic), `@tracked` decorator and `track_duration()` context manager. CLI: `logs clean/path/tail`, `stats` (#32).
- **Persona-based synthetic data generator.** Declarative YAML architecture, three v1 personas (`basic`/alice, `family`/bob, `freelancer`/charlie), ~200 real merchants, deterministic seeding, ground-truth labels in `synthetic.ground_truth` schema. CLI: `moneybin synthetic generate/reset/verify`. Level 2 realism (#37).
- **E2E test infrastructure.** Subprocess-based smoke tests (help, no-DB, DB commands), golden-path workflow tests (synthetic, CSV, OFX, lock/unlock, categorization) (#48).
- **MCP v1 scaffolding.** Response envelope, `@mcp_tool(sensitivity=...)` decorator, namespace registry, privacy middleware stub, prompts/resources (#42).

---

## [Pre-M0] — Pre-April 2026

Initial pipeline implementation that preceded the M0 design overhaul. Specs from this era live in [`docs/specs/archived/`](docs/specs/archived/): OFX import, CSV import (institution profiles), W-2 PDF extraction, rule-based transaction categorization, MCP read tools, MCP write tools.

These features survived the M0/M1 redesign — they're still shipped today, but reimplemented under the new abstractions (`Database` factory, service layer, encrypted-by-default storage, smart tabular importer that supersedes the profile-based CSV system).
