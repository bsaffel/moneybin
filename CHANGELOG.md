# Changelog

All notable changes to MoneyBin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). MoneyBin is pre-1.0 and pre-launch; entries are grouped by **milestone** (M0 → M1 → M2A/B/C → M3A–E → 1.0 launch) rather than semantic releases until 1.0 ships. See [`docs/roadmap.md`](docs/roadmap.md) for what each milestone covers.

## [Unreleased]

M2 work in flight; M2A `transaction-curation.md` spec published (PR #115). Doc surface tightened for the personas reachable today. MCP surface hardened: protocol-standard annotations, `accounts_resolve` for fuzzy account lookup, list-parameter cap, de-bulking renames.

### Added
- CLI `--output json` responses and MCP tool responses now include a top-level `status` field (`"ok"` or `"error"`), giving agents a consistent signal without testing for presence of the `error` key.
- `--json-fields` option added to all read-only CLI commands (infrastructure in place; per-command field lists added as commands are extended). Comma-separated projection: `moneybin <cmd> --output json --json-fields id,date,amount`.
- Shell completion enabled: `moneybin --install-completion` and `moneybin --show-completion` now work.
- Structured JSON error envelopes: when `--output json` is active, runtime errors (DB locked, file not found, etc.) emit a machine-readable error envelope to stdout instead of plain stderr text.
- MCP tool decorator now emits protocol-standard `ToolAnnotations` (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`). Clients can render confirmation UI for destructive operations.
- Decorator-level cap on list-typed tool parameters via `MCPConfig.max_items` (default 500). Exceeding the cap returns `ResponseEnvelope.error` with `code="too_many_items"`.
- `accounts_resolve` MCP tool and `moneybin accounts resolve "<query>"` CLI command — fuzzy-matches free-text references to an `account_id`.
- **`reports.*` SQLMesh views.** Eight curated presentation models — `net_worth`, `cash_flow`, `spending_trend`, `recurring_subscriptions`, `uncategorized_queue`, `merchant_activity`, `large_transactions`, `balance_drift` — back the `moneybin reports *` CLI surface and `reports_*_get` MCP tools. Inaugurates the read-only `reports.*` schema per `architecture-shared-primitives.md`.
- **`moneybin reports recurring`, `merchants`, `uncategorized`, `large-transactions`, `balance-drift`.** New CLI subcommands powered by the recipe library; pair with `--output json` for AI consumers.
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
- Renamed `moneybin mcp config generate --install` to `moneybin mcp install`. Default behavior writes the client config; `--print` opts out. Hard cut, no alias. `mcp config path` (lookup-only) is unchanged.
- Tool description audit: every existing `@mcp_tool` description was reviewed against the sign-convention, currency, and mutation-surface invariant rules. Missing invariants were appended; descriptions otherwise unchanged.
- `transactions_categorize_rules_create` (and `CategorizationService.create_rules`) is now idempotent. Each input is deduped against active rules by the matcher+output tuple `(merchant_pattern, match_type, min/max_amount, account_id, category, subcategory)`; `name` and `priority` are metadata and excluded from the key. A retry of the same payload returns the existing `rule_id`s and creates no new rows. The result envelope gains an `existing` counter alongside `created`/`skipped`. Same matcher with a *different* category output still creates a new row — rule-conflict detection is a deferred follow-up.
- Internal rename: `BulkCategorizationResult` → `CategorizationResult`, `bulk_categorize` → `categorize_items`, `validate_bulk_items` → `validate_items`. The "bulk" qualifier is dropped from MoneyBin's surface — list inputs are the default, not the exceptional case.
- Prometheus metric names renamed: `moneybin_categorize_bulk_items_total` → `moneybin_categorize_items_total`, `moneybin_categorize_bulk_duration_seconds` → `moneybin_categorize_duration_seconds`, `moneybin_categorize_bulk_errors_total` → `moneybin_categorize_errors_total`. External dashboards/alerts referencing the old names need updating.
- **Categorization matcher input extended** to memo and structural fields. The deterministic matcher and the LLM-assist redacted view now both consume `match_text = description + memo` plus `transaction_type`, `check_number`, `is_transfer`, `transfer_pair_id`, `payment_channel`, and `amount_sign`. Aggregator transactions (PayPal, Venmo, Zelle, generic ACH) match on the wrapped merchant identity in memo instead of failing on the truncated description. Pattern matching is per-field so user-authored `exact` and anchored-`regex` patterns continue to hit the original field when memo is present. (PR #122)
- **`categorize assist` / `categorize apply` JSON envelope** carries `transaction_id` as the per-row key (no separate opaque identifier). Export files produced by `categorize assist` flow back into `categorize apply` unchanged. (PR #122)
- **LLM-assist redaction contract expanded.** The redactor now runs over `memo` in addition to `description`, and structural fields (`transaction_type`, `check_number`, `is_transfer`, `transfer_pair_id`, `payment_channel`, `amount_sign`) are exposed to the LLM as signals. The no-amount / no-date / no-account guarantee is preserved. (PR #122)
- **`transactions_categorize_apply` triggers auto-fan-out.** After the batch commits, `categorize_pending()` runs once to apply newly-created merchants and exemplars to remaining uncategorized rows in the same dataset. The "snowball" the cold-start spec promised now works — by the third or fourth import, the LLM is meaningfully less involved. (PR #122)
- **Auto-created merchants accumulate exemplars instead of inventing patterns.** When LLM-assist categorizes a row and proposes a `canonical_merchant_name`, the system appends the exact normalized `match_text` to a `oneOf` exemplar set on the merchant — it no longer creates a `contains` pattern from the full normalized description. Aggregator strings like `PAYPAL INST XFER` no longer over-match across unrelated transactions. (PR #122)
- **Source-precedence enforcement on write.** All categorization writes route through a single guarded path that compares the incoming source's priority against the existing row's. A user manual edit (`'user'`) can never be overwritten by any subsequent rule, merchant, or LLM-assist run. The `categorized_by` column is the lock; no separate lock table. (PR #122)
- **`core.agg_net_worth` retired.** Net worth aggregation now lives at `reports.net_worth` (same SELECT body, new schema) per the `reports.*` convention introduced in `architecture-shared-primitives.md`. Existing `moneybin reports networth` commands and `reports_networth_*` MCP tools transparently repointed.
- **`app.categories` and `app.merchants` views retired.** The resolved-dimension views (seeds + user state + overrides) now live as SQLMesh-managed `core.dim_categories` and `core.dim_merchants`. Consumer code already routed through the `TableRef` constants; no API change.
- **Milestone terminology unified.** Retired "Level 0/1" + "Wave 2A/2B/2C/Wave 3" dual systems for one consistent **milestone** convention: M0, M1, M2A, M2B, M2C, M3A, M3B, M3C, M3D, M3E, Post-launch. M3 decomposes into sub-milestones because it has parallel domain (Plaid/investments/multi-currency) and surface (Web UI/hosted) tracks. M3E closing = launch.
- **README significantly tightened** — from ~196 lines to ~115 lines. Storefront pattern: tagline preserved, status callout + Why-bullets + How-It-Works diagram + Quick Start + 5×5 ✓/✗ comparison + Documentation/Community/Contributing/License pointers. In-README roadmap matrix removed (lives in `docs/roadmap.md`); detailed feature inventory removed (lives in `docs/features.md`); 8-column comparison table replaced with tight 5×5 (full version in `docs/comparison.md`); License essay condensed (full rationale in `docs/licensing.md`). Modeled on Bitwarden, Plausible, DuckDB, SQLMesh peer-set conventions.
- `.claude/rules/shipping.md` extended with the post-implementation checklist for `CHANGELOG.md`, `docs/roadmap.md`, `docs/features.md`. Documents what does and doesn't earn a CHANGELOG entry.
- `CONTRIBUTING.md` "Where the strategy lives" expanded to include the new docs and a one-line CHANGELOG rule.

### Fixed
- Five categorization correctness bugs surfaced by live OFX checking-account testing: `memo` was dropped from the matcher and LLM input; `_match_description` only operated on `description`; system-generated merchants used over-generalizing `contains` patterns; `categorize_pending` was never called after `transactions_categorize_apply` so the snowball couldn't roll; OFX `<NAME>` truncation hid merchant identity in `<MEMO>` that the matcher never saw. See [`docs/specs/categorization-matching-mechanics.md`](docs/specs/categorization-matching-mechanics.md) for the full diagnosis. (PR #122)

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
