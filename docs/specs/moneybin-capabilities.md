# MoneyBin Capabilities — Cross-Surface Coverage Map

> **Status:** draft (bootstrap rows only). The bootstrap rows below
> demonstrate every coverage pattern the catalog will track. Full
> population (~30–60 rows) lands incrementally as follow-up work
> closes the parity backlog.

This spec is the cross-surface outcome map: one row per user-facing
capability, one column per active surface. It is the canonical answer to
"what user outcome reaches which surface, and under what registered
name." Per-surface implementation detail (parameter schemas, sensitivity
tiers, response envelope shapes, CLI flag conventions) lives in the
surface-specific specs.

## What is a capability?

A **capability** is a user-language verb-object pair describing
something a person can accomplish with MoneyBin. Capabilities are stated
in domain terms, not implementation terms:

| Capability (good)                                  | Implementation (not a capability)                        |
|----------------------------------------------------|----------------------------------------------------------|
| "List transactions filtered by date and account"   | Call `transactions_get(date=..., account=...)`           |
| "Set the complete tag set on a transaction"        | Invoke `transactions_tags_set`                           |
| "Rotate the database encryption key"               | Run `db_key_rotate` and update keychain                  |

Granularity: roughly one capability per primary user task. The full
catalog is expected to land at ~30–60 capabilities once populated.

## How to read the map

- Each row is one capability.
- Each surface column shows the **registered name** reaching this
  capability on that surface, OR an **exemption citation** linking back
  to `.claude/rules/mcp.md` "When CLI-only is justified."
- The **Status** column indicates:
  - `live` — reachable today on every non-exempt surface.
  - `pending-build (<surface>)` — capability is real but the named
    surface has not yet built its name.
  - `future` — planned; no surface implemented yet.

A cell containing `—` means "no name on this surface today." Pair it
with the Status column to disambiguate exempt-by-policy from
not-yet-built.

## Active surfaces

| Surface  | Active            | Spec                                   | Live registry              |
|----------|-------------------|----------------------------------------|----------------------------|
| MCP      | yes               | [`moneybin-mcp.md`](moneybin-mcp.md)   | `moneybin.mcp.server`      |
| CLI      | yes               | [`moneybin-cli.md`](moneybin-cli.md)   | `moneybin.cli.main`        |
| REST API | no (planned M3D)  | `moneybin-rest-api.md` (future)        | —                          |

## Capability map

> Bootstrap rows only — chosen to demonstrate each pattern. Names
> verified against the live MCP registry (`mcp._list_tools()`) and the
> live Typer command tree as of 2026-05-16.

| # | Capability                                                       | MCP                          | CLI                                                | REST (M3D) | Status                |
|---|------------------------------------------------------------------|------------------------------|----------------------------------------------------|------------|-----------------------|
| 1 | List recent transactions filtered by date/account/category       | — *(pending-build)*          | `transactions_list`                                | —          | pending-build (MCP)   |
| 2 | Fetch a single transaction by ID                                 | `transactions_get`           | — *(pending-build)*                                | —          | pending-build (CLI)   |
| 3 | List all accounts                                                | `accounts`                   | `accounts_list`                                    | —          | live                  |
| 4 | Update an account's settings (display name, include/archive)     | `accounts_set`               | `accounts_set` *(`--display-name`, `--include/--exclude`, `--archive/--unarchive`)* | —          | live                  |
| 5 | Set the complete tag set on a transaction                        | `transactions_tags_set`      | `transactions_tags_{add,remove,list}` *(cat 3)*    | —          | live                  |
| 6 | Sync the import inbox                                            | `import_inbox_sync`          | `import_inbox` (bare group call)                   | —          | live                  |
| 7 | Summarize an account's activity                                  | `accounts_summary`           | — *(pending-build)*                                | —          | pending-build (CLI)   |
| 8 | Refresh derived tables after raw data changes (full or per-step) | `refresh_run` *(`steps=`)*   | `refresh` *(`--step transform|match|categorize`)*  | —          | live                  |
| 9 | Rotate the database encryption key                               | — *(cat 1 — secret material)*| `db_key_rotate`                                    | —          | live                  |
| 10| Run the MCP server                                               | — *(cat 2 — operator)*       | `mcp_serve`                                        | —          | live                  |
| 11| Hard-delete a user-created category                              | `categories_delete`          | `categories delete`                                | —          | live                  |
| 12| Create one or more categorization rules (single + batch)         | `transactions_categorize_rules_create` | `transactions categorize rules create` | —          | live                  |
| 13| Soft-delete a categorization rule by ID                          | `transactions_categorize_rules_delete` | `transactions categorize rules delete` | —          | live                  |
| 14| Commit externally-decided categorizations (LLM workflow's terminal step) | `transactions_categorize_commit`        | `transactions categorize commit`        | —          | live                  |
| 15| Run the categorization engine cascade (rules + merchants)        | `transactions_categorize_run`            | `transactions categorize run`            | —          | live                  |
| 16| Get redacted batch for LLM categorization                        | `transactions_categorize_assist`         | `transactions categorize assist`         | —          | live                  |
| 17| Categorization coverage statistics (with optional auto-rule health) | `transactions_categorize_stats` *(`include_auto=True` for auto metrics)* | `transactions categorize stats` | — | live |
| 18| Fetch uncategorized transactions queue (sortable by date or impact) | `transactions_categorize_pending` *(`sort`, `min_amount`, `account`)* | `transactions categorize pending` | — | live |
| 19| Check auto-rule health metrics in isolation                      | absorbed into row 17 (`include_auto=True`) | `transactions categorize auto stats` *(CLI-only after MCP tool retired)* | — | live |
| 20| Balance assertion drift by status category                       | `reports_balance_drift`                  | `reports balance-drift`                  | —          | live                  |
| 21| Threshold-filtered balance mismatch by day                       | `accounts_balance_reconcile`             | `accounts balance reconcile`             | —          | live                  |

| 22| Inspect SQLMesh model state (status/plan/validate/audit)        | — *(cat 2 — operator)*       | `transform status|plan|validate|audit`             | —          | live (CLI-only)       |

| 23| Authenticate with Google Sheets (OAuth installed-app + PKCE)     | `gsheet_auth` *(`force_reauth=True` to override short-circuit)* | `gsheet auth` *(`--force`)*                       | —          | live                  |
| 24| Bind a Google Sheet for live sync                                | `gsheet_connect` *(`url`, `adapter`, `alias`, `account_name`, `account_id`, `column_mapping`, `yes`, `accept_seed_fallback`, `no_initial_pull`)* | `gsheet connect <url>` *(same options)*           | —          | live                  |
| 25| Pull latest content from connected sheets                        | `gsheet_pull` *(`connection_id`)* | `gsheet pull` *(`--connection-id`, `--refresh/--no-refresh`)*                       | —          | live                  |
| 26| List Google Sheets connections                                   | `gsheet`                     | `gsheet list`                                      | —          | live                  |
| 27| Get status for one or all Google Sheets connections              | `gsheet_status` *(`connection_id`)* | `gsheet status` *(`--connection-id`)*       | —          | live                  |
| 28| Re-detect column mapping after sheet drift                       | `gsheet_reconnect` *(`yes` for medium-confidence remaps)* | `gsheet reconnect` *(`--yes`)*           | —          | live                  |
| 29| Disconnect a Google Sheet (soft or purge)                        | `gsheet_disconnect` *(`purge=True` permanent)* | `gsheet disconnect` *(`--purge`, `--yes`)* | —          | live                  |
| 30| Link a bank via mediated provider (Plaid)                        | `sync_link` *(`institution` for re-auth)* | `sync link` *(formerly `sync connect`)*  | —          | live                  |
| 31| Poll an in-flight bank-link session                              | `sync_link_status` *(`session_id`)* | `sync link-status` *(formerly `sync connect-status`)* | —          | live                  |
| 32| Grant consent for an AI feature category                         | `privacy_consent_grant` *(`category`, `backend?`, `mode`)* | `privacy grant` *(`--backend`, `--mode`, `--yes`)* | —     | live                  |
| 33| Revoke a previously granted consent                              | `privacy_consent_revoke` *(`category`, `backend?`)* | `privacy revoke` *(`--backend`, `--yes`)* | —          | live                  |
| 34| Revoke all active consent grants                                 | — *(bulk revoke; use `privacy_consent_revoke` per category)* | `privacy revoke-all` *(`--yes`)* | —             | live (CLI-only)       |
| 35| View current consent state and configured backend                | `privacy_status`                    | `privacy status` *(`--output json`)*               | —          | live                  |
| 36| Query recent privacy-log events (consent + tool calls)           | `privacy_log` *(`last?`, `actor?`)* | `privacy log` *(`--last`, `--actor`, `--output json`)* | —       | live                  |
| 37| List pending transaction match proposals awaiting a decision     | `transactions_matches_pending` *(`match_type?`, `limit?`)* — each row includes `component_key` for N-way cluster grouping | `transactions matches pending` *(grouped by component_key)* / `transactions review --type matches --status` *(counts)* / `transactions review --type matches` *(interactive queue)* | — | live |
| 38| Accept or reject one pending match proposal                      | `transactions_matches_set` *(`match_id`, `status: accepted\|rejected`)* | `transactions matches set <match_id> --status accepted\|rejected` | — | live |
| 39| Run the matching engine and propose new pending decisions         | `transactions_matches_run` *(operator alternative to `refresh_run(steps=["match"])`)* | `transactions matches run` | — | live |
| 40| View recent match decisions (accepted and rejected)              | `transactions_matches_history` *(`limit?`, `match_type?`)* | `transactions matches history` *(`--type`, `--limit`)* | — | live |
| 41| Execute a read-only SQL query over core/app with CRITICAL columns masked via lineage | `sql_query` *(`query`)* | `sql query <sql>` *(`--output text\|json`)* | — | live |
| 42| Reverse one audited operation as a unit (undo)                   | `system_audit_undo` *(`operation_id`)* | `system audit undo <operation_id>`                 | —          | live                  |
| 43| List recent audited operations with undoability                  | `system_audit_history` *(`domain?`, `since?`, `actor?`, `limit?`, `include_undone?`)* | `system audit history` *(`--domain`, `--since`, `--actor`, `--limit`, `--include-undone`)* | — | live |
| 44| Inspect full before/after for one operation before undoing        | `system_audit_get` *(`operation_id`)* | `system audit get <operation_id>`                  | —          | live                  |
| 45| Import a file and handle unknown layout via confirmation flow      | `import_files` *(returns `confirmation_required` envelope on first-encounter unknown layouts; `actions[]` contains `import_confirm` hint)* | `import files PATHS... [--confirm/--no-confirm] [--mapping field=col]` *(TTY: interactive prompt; non-TTY/`--output json`: envelope + exit 0)* | —          | live                  |
| 46| Confirm a proposed import column mapping                          | `import_confirm` *(`file_path`, `accept=True`, `mapping={...}`)* | `import confirm <file> --accept` / `--mapping field=column` | —          | live                  |
| 47| List available import formats (tabular + PDF) for selection / introspection | `import_formats` *(returns `formats` + `pdf_formats` arrays)* | `import formats list [--type tabular\|pdf\|all]` *(text or JSON; agent can also `import formats show <name>` for either kind)* | —          | live                  |
| 48| Import a native-text PDF an agent must help extract (bridge round-trip) | `import_preview`/`import_files` *(return `confirmation_required` with a `bridge_payload` when the deterministic rung can't crack the layout)* → `import_confirm` *(`bridge_response={recipe, rows}`; re-runs the recipe, reconciles, persists, loads)* | *deferred — the CLI keeps the seed fallback until the agent-aware CLI escalation signal lands* | —          | live (MCP)            |
| 49| List pending account-link decisions grouped by provisional account | `accounts_links_pending` | `accounts links pending` | — | live |
| 50| Accept (merge) or standalone-reject one pending account-link decision | `accounts_links_set` *(`decision_id`, `target_account_id: str\|null` — no default; null = standalone-reject)* | `accounts links set <decision_id> --into <account_id>` (merge) / `--standalone` (reject) | — | live |
| 51| Show recent account-link decisions (all statuses) | `accounts_links_history` *(`limit=50`)* | `accounts links history` *(`--limit`, `--output json`)* | — | live |
| 52| Backfill pending account-link proposals for existing accounts (cross-source twin discovery) | `accounts_links_run` *(returns `data.new_proposals`)* | `accounts links run` *(`--output json`)* | — | live |
| 53| "What needs my attention?" — pending counts across all four review queues in one sweep | `review` *(returns `{matches_pending, categorize_pending, account_links_pending, merchant_links_pending, total}`)* | `moneybin review --status` *(`--type`, `--output json`)* | — | live |
| 54| Confirm account identity at import time (which account is this file?) | `import_confirm` *(`account_bindings={source_key: account_id\|"new"}` ratifies an `account_confirmation`; `account_metadata` captures display_name/subtype/last_four/currency for `"new"` accounts; interactive-human imports gate on weak candidates, agents load + queue; a single-account file with no account identity also returns `account_confirmation` — a 1-entry no-candidate proposal — for both human and agent callers)* | `import confirm <file> --account-binding source_key=ACCOUNT_ID\|new [--account-meta source_key:field=value]` | — | live |
| 55| List pending merchant-link decisions grouped by provider entity id | `merchants_links_pending` | `merchants links pending` *(`--output json`)* | — | live |
| 56| Accept (bind) or reject one pending merchant-link decision | `merchants_links_set` *(`decision_id`, `target_merchant_id: str\|null` — no default; null = reject)* | `merchants links set <decision_id> --into <merchant_id>` (bind) / `--new` (reject; mints new merchant on next categorization pass) | — | live |
| 57| Show recent merchant-link decisions (all statuses) | `merchants_links_history` *(`limit=50`)* | `merchants links history` *(`--limit`, `--output json`)* | — | live |
| 58| Harvest pending merchant-link proposals from existing categorization facts | `merchants_links_run` *(returns `data.bound` + `data.conflicts`)* | `merchants links run` *(`--output json`; returns `data.bound` + `data.conflicts`)* | — | live |
| 59| Record one or more investment ledger events (buy/sell/dividend/transfer/split/...) | `investments_record` *(`events=[{account, type, date, security?, quantity?, price?, amount?, fees?, subtype?, acquired?, basis?, event_group_id?, currency?, description?}, ...]`; a `reinvest` event writes an acquisition + income row pair sharing one `event_group_id`)* | `investments add` *(`--account`, `--type`, `--date`, `--security`, `--quantity`, `--price`, `--amount`, `--fees`, `--subtype`, `--acquired`, `--basis`, `--event-group`, `--currency`)* | — | live |
| 60| List investment ledger events filtered by account/security/type/date | `investments` *(`account?`, `security?`, `type_filter?`, `from_date?`, `to_date?`)* | `investments list` *(`--account`, `--security`, `--type`, `--from`, `--to`)* | — | live |
| 61| View current investment positions (quantity, cost basis, average cost) | `investments_holdings` *(`account?`)* | `investments holdings` *(`--account`)* | — | live |
| 62| View tax lots with remaining quantity and basis | `investments_lots` *(`account?`, `security?`, `open_only=true`)* | `investments lots list` *(`--account`, `--security`, `--open/--all`)* | — | live |
| 63| Override which lots a disposal draws from (specific identification) | `investments_lots_select` *(`disposal_txn_id`, `selections=[{lot_id, quantity}, ...]`; empty list reverts to FIFO)* | `investments lots select <disposal_txn_id> --lot ID:QTY [--lot ...]` / `--clear` | — | live |
| 64| View realized gain/loss (the 1099-B surface) | `investments_gains` *(`account?`, `security?`, `from_date?`, `to_date?`, `term?`)* | `investments gains` *(`--account`, `--security`, `--from`, `--to`, `--term`)* | — | live |
| 65| List or create/update entries in the manually-maintained securities catalog | `investments_securities` *(read)* / `investments_securities_set` *(`security_id=None` creates; existing id partially updates)* | `investments securities list` / `investments securities add` / `investments securities set <id>` | — | live |

*(Bootstrap rows only; full table populates incrementally as
follow-up work closes the parity backlog. A prior row covering
"Discover currently-hidden MCP tools" was removed 2026-05-17
when client-driven progressive disclosure was retired (see
[`mcp-architecture.md`](mcp-architecture.md) §3); the current
rows 12–13 are unrelated and were added 2026-05-17 with the
rules-CLI parity work. Row 17 added 2026-05-19: transform_* de-registered
from MCP (PR #185) — operator territory per mcp.md category 2.
`sync_schedule_set/show/remove` stubs removed from MCP (PR #185) — were
not-implemented placeholders with no backing spec. Rows 23–29 added
2026-05-21 with the connect-gsheet PR; rows 30–31 capture the
`sync_connect` → `sync_link` rename co-shipped in the same PR.
Rows 32–36 added 2026-05-22 with the consent ledger PR; row 34 is
CLI-only because `revoke-all` is a bulk convenience with no MCP
equivalent — use `privacy_consent_revoke` per category from MCP.
Rows 37–40 added 2026-05-22 with the matches accept/reject PR: four
`transactions_matches_*` MCP tools registered; `transactions matches set`
CLI command and non-interactive `transactions review --type matches
--confirm/--reject/--confirm-all` flags wired.
Row 41 added 2026-05-23 with the SQL lineage PR: `sql_query` (MCP) and
`moneybin sql query` (CLI) both mask CRITICAL columns via sqlglot lineage
through the shared `execute_sql_query` primitive — full MCP↔CLI parity.
`moneybin db query`/`db shell`/`db ui` remain raw operator access (cat 2 —
no privacy middleware) and emit a banner pointing at `moneybin sql query`.
Rows 45–46 added 2026-05-29 with the smart-import-confirmation PR: `import_files`
gains a `confirmation_required` envelope state for first-encounter unknown layouts;
`import_confirm` (MCP) and `moneybin import confirm` (CLI) are the terminal `_confirm`
step for ratifying proposed column mappings.
Row 47 added 2026-05-31 with the smart-import-pdf Phase 2a PR: `import_formats`
gains a `pdf_formats` array surfacing auto-derived PDF recipes (layout fingerprint,
routing, replay statistics); CLI adds `--type {tabular,pdf,all}` filter and PDF
namespace fallthrough on `import formats show`.
Row 48 added 2026-06-07 with the smart-import-pdf Phase 2b bridge round-trip:
a native-text PDF the deterministic rung can't crack escalates to the driving
agent (`import_preview`/`import_files` return a `bridge_payload`), and
`import_confirm(bridge_response=...)` re-runs the agent's recipe, reconciles the
re-executed rows against the statement balances, persists the recipe, and loads
the transactions. MCP-only for now — escalation is gated on the agent caller
(`actor_kind="agent"`); the CLI keeps the Phase 2a seed fallback until its
agent-aware escalation signal lands as follow-up work.
Rows 49–51 added 2026-06-15 with the account-binding review-surface PR (M1S.5a):
`accounts_links_pending`, `accounts_links_set`, and `accounts_links_history` MCP tools
registered; `accounts links {pending,set,history}` CLI commands wired. Sensitivity `low`
throughout — opaque IDs + display names + signal/confidence only; `ref_value` never
surfaced.
Row 52 added 2026-06-16 with the accounts-links-run backfill PR (M1S.5b):
`accounts_links_run` (MCP) and `accounts links run` (CLI) registered. Backfills pending
proposals for cross-source twins already in `core.dim_accounts`; skips pairs already
proposed or decided in either direction. Undo deliberately deferred to M1L.
Row 53 added 2026-06-16 with the review-promotion PR (M1S.5c):
`review` (MCP) and `moneybin review` (CLI) replace `transactions_review` /
`moneybin transactions review` as the domain-neutral orientation sweep. Payload gains
`account_links_pending` so one call covers all three queues. Old names kept as
deprecated aliases for one minor release; descriptions start with "DEPRECATED:".
Rows 59–65 added 2026-07-04 with the investments-data-model PR (M1J.1 foundation
child, Pillars A+B): five `investments_*` read tools and three write tools
(`investments_record`, `investments_securities_set`, `investments_lots_select`)
register alongside the top-level `investments` CLI group. Sensitivity is derived
per tool from payload field classification rather than declared statically —
`high` for the ledger/holdings/lots/gains tools (cost basis and proceeds are
`BALANCE`-classified), `low` for the securities catalog (reference data only).)*

## Exemption categories

Defined in [`.claude/rules/mcp.md`](../../.claude/rules/mcp.md)
"When CLI-only is justified":

| # | Category                  | Short description                                                           | Status            |
|---|---------------------------|-----------------------------------------------------------------------------|-------------------|
| 1 | Secret material           | Passphrases/keys flowing through the LLM context window                     | formalized        |
| 2 | Operator territory        | Bootstrapping, recovery, dev tooling that needs physical operator presence  | formalized        |
| 3 | Surface-shape asymmetry   | Batch-set on MCP vs verb-list on CLI (e.g., `*_set` vs `*_{add,remove,list}`) | *pending follow-up* |
| 4 | File-based agent bridge   | CLI is the data plane for file-shaped ops                                   | *pending follow-up* |
| 5 | Privacy-design            | Tools that introspect/bypass redaction                                      | *pending follow-up* |

A capability marked exempt on a surface cites the category by number.
Categories 3–5 are documented here for forward-compatibility; the
formal definitions land with the follow-up allowlist refactor.

**MCP-only exemptions.** Reserved for tools that implement MCP-protocol-specific
mechanisms with no CLI semantic. Empty today — the prior entry
(`moneybin_discover` for session-scoped visibility re-enable) was
retired 2026-05-17. If the inventory grows again, exemptions will be
cited inline rather than via a numbered category, because the
CLI-only-justification list does not cover the reverse case.

## Contributor recipe

When a PR adds, renames, or removes a tool or command:

1. **Update the surface-specific spec** ([`moneybin-mcp.md`](moneybin-mcp.md)
   for MCP, [`moneybin-cli.md`](moneybin-cli.md) for CLI). Per-surface
   detail (parameter schemas, sensitivity tiers, envelope shape, flag
   conventions) lives there.
2. **Update this map** in the same PR. Add a new row (new capability)
   or update the existing row's cell (rename, removed, exempt change).
3. **Verify the user-language description** matches what the surface
   actually does — reviewer responsibility, not author judgment.
4. **If exempting a surface,** cite the category by number and ensure
   the citation is consistent with
   [`.claude/rules/mcp.md`](../../.claude/rules/mcp.md).

PR review enforces 1 and 2; the surface-change-discipline rule in
`.claude/rules/mcp.md` cites this contract.

## What this spec is NOT

- **Not an implementation spec.** Per-tool parameter schemas, response
  envelope shapes, sensitivity tiers, CLI flag conventions — those live
  in the surface-specific specs.
- **Not a test fixture.** The name-drift test
  (`tests/integration/test_surface_parity.py`, landing per PR #152)
  will derive its allowlist frozen sets from this spec once a
  follow-up wires the derivation. The spec itself is for humans first.
- **Not a marketing page.** That is [`docs/features.md`](../features.md).

## Related

- [`moneybin-mcp.md`](moneybin-mcp.md) — MCP-specific tool surface.
- [`moneybin-cli.md`](moneybin-cli.md) — CLI command taxonomy and conventions.
- [`mcp-architecture.md`](mcp-architecture.md) — Design-level MCP architecture (not surface-level).
- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — Cross-protocol symmetry contract.
- [`.claude/rules/mcp.md`](../../.claude/rules/mcp.md) — Surface change discipline and CLI-only justifications.
