# Changelog

All notable changes to MoneyBin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). MoneyBin is pre-1.0 and pre-launch; entries are grouped by **milestone** (Level 0 → Level 1 → Wave 2 → Wave 3 → 1.0 launch) rather than semantic releases until 1.0 ships. See the [README roadmap](README.md#roadmap) for what each milestone covers.

## [Unreleased]

Wave 2 work in flight. Nothing shipped to users since the Level 1 closeout.

### Added (planned, not yet shipped)
- Wave 2A: manual transaction entry, transaction notes, multi-tag table, "verified" curator flag, edit-history audit log, import-batch labels, split-via-annotation. New `app.*` user-state schema layer.
- Wave 2B: `architecture-shared-primitives.md` reference doc, `app.*` and `reports.*` schema conventions, local/hosted split contract.
- Wave 2C: `brew install moneybin` distribution, PyPI publish workflow, first-run wizard, `moneybin doctor` health command, `reports.*` recipe library, demo profile preset, static landing page.

### Changed
- README rewritten for the post-strategic-review framing: tagline preserved, sub-line refresh ("financial data platform you actually own"), Why-bullets reordered with lineage first, Wave-aligned roadmap, expanded comparison table (Era/BankSync, Lunch Money, Wealthfolio added), License section with substance. New `docs/guides/threat-model.md` user-facing one-pager.

---

## [Level 1] — 2026-05-04 (Data Integrity)

Five Level 1 deliverables shipped plus companion work. `fct_transactions` is now trustworthy: dedup eliminates double-counting, transfer detection prevents transfer-as-spend distortion, auto-rules categorize new imports, net-worth tracks balances with self-healing reconciliation deltas.

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

## [Level 0] — 2026-04-30 (Infrastructure Multipliers)

Foundational systems shipped: encryption-at-rest, schema migrations, observability, profiles, CLI/MCP scaffolding, and the synthetic data generator. Every Level 1+ feature builds on these.

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

## [Pre-Level-0] — Pre-April 2026

Initial pipeline implementation that preceded the Level 0 design overhaul. Specs from this era live in [`docs/specs/archived/`](docs/specs/archived/): OFX import, CSV import (institution profiles), W-2 PDF extraction, rule-based transaction categorization, MCP read tools, MCP write tools.

These features survived the Level 0/1 redesign — they're still shipped today, but reimplemented under the new abstractions (`Database` factory, service layer, encrypted-by-default storage, smart tabular importer that supersedes the profile-based CSV system).
