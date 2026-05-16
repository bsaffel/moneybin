# What Works Today

Snapshot of MoneyBin's shipped capabilities, with links to the per-feature guide for each.

> Pre-launch state. M0 (Infrastructure) and M1 (Data Integrity) are shipped. M2 (Curator State, Architecture Reference, Brand Surface) is in flight. See [Roadmap](roadmap.md) for what's coming.

## Ingestion

- **Smart tabular import** (CSV / TSV / Excel / Parquet / Feather) with heuristic column detection, multi-account support, and migration profiles for Tiller, Mint, YNAB, and Maybe. Five-stage pipeline (Format Detection → Reader → Column Mapping → Transform & Validate → Load). Three-tier confidence model. → [Data Import guide](guides/data-import.md)
- **OFX / QFX / QBO import** through the same `import_log` infrastructure as tabular: re-import detection, `--force` override, institution name auto-resolution from `<FI><ORG>` / FID lookup / filename heuristics, batch revert via `moneybin import revert <id>`. → [Data Import guide](guides/data-import.md)
- **W-2 PDF extraction.**
- **Batch-shaped imports.** `moneybin import files PATHS...` (or `import_files(paths)` via MCP) imports one or more files in a single call; per-file failures don't abort the batch, and the post-load refresh pipeline runs once at end of batch (toggle with `--no-refresh`).
- **Watched-folder inbox UX.** Drop files in `~/Documents/MoneyBin/<profile>/inbox/`; `moneybin import inbox` drains successes to `processed/YYYY-MM/` and failures to `failed/YYYY-MM/` with YAML error sidecars. Per-profile lockfile + crash-recovery via staging-rename. Routes through the batch import path so transforms run once per drain. → [Smart Import Inbox spec](specs/smart-import-inbox.md)

## Sync

- **Plaid bank sync (M3A Phase 1):** connect checking, savings, and credit card accounts via Plaid Hosted Link; pull transactions, balances, and account metadata through moneybin-server; loads into `raw.plaid_*` and flows through SQLMesh staging into `core.fct_transactions` / `core.dim_accounts` alongside OFX and CSV data. Incremental by default (cursor-based); `--force` for full re-fetch. CLI: `moneybin sync pull / connect / connect-status / disconnect / status / login / logout`. MCP tools: `sync_pull`, `sync_status`, `sync_connect`, `sync_connect_status`, `sync_disconnect`. MCP prompt: `sync_review`. → [Plaid Sync spec](specs/sync-plaid.md)

## Pipeline

- **Three-layer SQLMesh:** raw → prep → core. Plus emerging `app.*` (user-state) and time-series `agg_*` conventions. → [Data Pipeline guide](guides/data-pipeline.md)
- **Cross-source dedup** with SHA-256 content hashes and golden-record merge. `prep.seed_source_priority` config-driven seed table. → [matching specs](specs/matching-overview.md)
- **Transfer detection** across accounts: shared matching engine Tier 4, `core.bridge_transfers`, always-review v1, four-signal scoring. → [matching specs](specs/matching-overview.md)
- **Reconciliation deltas** computed and self-healing on reimport.
- **Transform handoff to the agent.** `transform_status`, `transform_plan`, `transform_validate`, `transform_audit`, `transform_apply` MCP tools (previously CLI-only). `system_status` reports whether derived tables are stale.

## Categorization

- **Rule-based** (exact / contains / regex / `oneOf` exemplars) with priority hierarchy (user > rule > auto_rule > migration > ml > plaid > ai). Source precedence is enforced on write — user manual edits are immune to subsequent rule, merchant, or LLM-assist runs.
- **Merchant normalization** with `match_text = description + memo` so aggregator transactions (PayPal, Venmo, Zelle, generic ACH) match on the wrapped merchant identity in memo. Structural fields (`transaction_type`, `check_number`, `is_transfer`, `payment_channel`) are matcher signals and LLM-assist signals.
- **Exemplar accumulation.** System-generated merchants (from LLM-assist) store the exact normalized `match_text` of each categorized row as a `oneOf` exemplar — they never invent a generalized `contains` pattern from one row's description, so aggregator strings don't over-match.
- **Snowball auto-apply.** Every `transactions_categorize_apply` commit fires `categorize_pending()` to fan newly-created merchants and exemplars out to remaining uncategorized rows in the same dataset. The cold-start vision ("by the third or fourth import, the LLM is barely involved") works because of this.
- **Auto-rule learning** from user edits (`app.proposed_rules` review queue with four-state lifecycle, promotion to `app.categorization_rules` at priority 200, correction-handling threshold).
- **Bulk operations** (CLI + MCP + service parity).

→ [Categorization guide](guides/categorization.md)

## Accounts and balances

- **Account management:** `accounts list / show / rename / include / archive / unarchive / set` with Plaid-parity metadata (subtype, holder category, currency, credit limit, last four). Reversible account merging via bridge model. → [Account Management spec](specs/account-management.md)
- **Net-worth and balance tracking:** `accounts balance show / history / assert / list / delete / reconcile` per-account workflow; `reports networth show / history` cross-account rollup with period-over-period change. Three-model SQLMesh pipeline: `core.fct_balances` (VIEW) → `core.fct_balances_daily` (TABLE, daily carry-forward) → `core.agg_net_worth` (VIEW). Authoritative observations from OFX, tabular running balances, and user assertions. → [Net Worth spec](specs/net-worth.md)

## Surfaces

- **CLI** (Typer) with v2 path-prefix taxonomy. → [CLI Reference](guides/cli-reference.md)
- **MCP server** (~33 tools across `accounts`, `transactions`, `reports`, `categories`, `merchants`, `system`, `budget`, `tax`, `sync`, `transform`, `import`). FastMCP 3.x with per-session visibility. `--output json` parity between CLI and MCP. → [MCP Server guide](guides/mcp-server.md)
- **MCP install** across nine clients (Claude Desktop, Claude Code, Cursor, Windsurf, VS Code, Gemini CLI, Codex CLI/Desktop/IDE, ChatGPT Desktop). → [Configuring MCP Clients](guides/mcp-clients.md)
- **MCP tool timeouts:** configurable wall-clock cap (default 30 s) with DuckDB lock release on timeout. → [Spec](specs/mcp-tool-timeouts.md)
- **Curated `moneybin://schema` MCP resource** + `sql_schema` tool mirror exposing core and select app interface tables with column comments and example queries.
- **Direct SQL** access via `moneybin db shell`, DuckDB UI, or any DuckDB client. → [SQL Access guide](guides/sql-access.md)

## Infrastructure

- **AES-256-GCM encryption at rest** by default. Argon2id KDF for passphrase mode. OS keychain for auto-key mode. → [Database & Security guide](guides/database-security.md) · [Threat Model](guides/threat-model.md)
- **Schema migrations:** dual-path (SQL + Python), auto-upgrade on first invocation, `app.versions` tracking, rebaseline command, SQLMesh version detection.
- **Multi-profile isolation:** per-profile DB, config, logs. → [Profiles guide](guides/profiles.md)
- **Observability:** structured logs with `SanitizedLogFormatter` PII safety net + Prometheus-style metrics with DuckDB persistence. `@tracked` decorator and `track_duration()` context manager. CLI: `logs clean / path / tail`, `stats`. → [Observability guide](guides/observability.md)

## Testing

- **Synthetic data generator:** persona-based (`basic`, `family`, `freelancer`), ~200 real merchants, deterministic seeding, ground-truth labels in `synthetic.ground_truth`. CLI: `moneybin synthetic generate / reset / verify`. → [Synthetic Data guide](guides/synthetic-data.md)
- **10-scenario test suite** with five-tier assertion taxonomy: structural invariants, semantic correctness, pipeline behavior, quality, operational. Bug-report recipe documented. → [Scenario Authoring guide](guides/scenario-authoring.md)
- **Whole-pipeline scenario runner:** empty encrypted DB → `generate → transform → match → categorize` → assertions / expectations / evaluations against synthetic ground truth and hand-labeled fixtures.
- **YAML golden cases** for `normalize_description()`; parametrized exact-equality tests.

## What's coming next

M3A (Plaid Transactions sync) shipped. Remaining M3 work: investments (M3B), multi-currency + budgets (M3C), Web UI hardening + Streamable HTTP MCP (M3D), and hosted launch (M3E). See [Roadmap](roadmap.md) for the full picture.
