<!-- Last reviewed: 2026-05-17 -->

# Changelog

All notable changes to MoneyBin are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). MoneyBin is pre-1.0 and pre-launch; entries are grouped by **milestone** (M0 → M1 → M2A/B/C → M3A–E → 1.0 launch) rather than semantic releases until 1.0 ships. See [`docs/roadmap.md`](docs/roadmap.md) for what each milestone covers.

## [Unreleased]

M2 closing out and M3 underway. M2A curator state shipped (transaction notes, tags, splits, manual entry, audit log). M2B architecture reference shipped (`architecture-shared-primitives.md`; writer-coordination contract via short-lived per-call connections). M2C brand surface advancing: `moneybin system doctor` integrity command, `reports.*` recipe library (eight curated views), and the `transform_*` MCP toolset closing the agent ingest loop. M3A Plaid Transactions sync shipped (Phase 1). Doc surface tightened for the personas reachable today; MCP surface hardened with protocol-standard annotations, `accounts_resolve`, list-parameter cap, structured error envelopes, and shell completion. Categorization correctness pass: memo-aware matcher, exemplar accumulation, source-precedence enforcement, auto-fan-out after apply; seed merchant catalogs retired in favor of user-driven and LLM-assist-driven merchant creation.

### Added
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

### Changed
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

### Removed
- **`reports_budget` MCP tool and `reports budget` CLI command.** They
  synthesized from `BudgetService` rather than reading a `reports.*` view,
  violating the `reports_*` = reads-a-view convention; they return through the
  report framework once a `reports.budget` view ships (M3C). `BudgetService`
  and the `budget_*` mutation tools are unaffected.
- **`reports health` CLI stub** — an unimplemented placeholder with no backing
  spec.

### Added
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
- **Plaid sync (M3A Phase 1):** new `moneybin sync` CLI subgroup and corresponding MCP tools (`sync_pull`, `sync_status`, `sync_link`, `sync_link_status`, `sync_disconnect`, `sync_review` prompt). Pulls accounts, transactions, and balances from Plaid-connected banks via moneybin-server, loads into `raw.plaid_*` tables, and flows through SQLMesh staging (with sign-convention flip) into `core.fct_transactions` and `core.dim_accounts`. See [`docs/specs/sync-plaid.md`](docs/specs/sync-plaid.md).
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
