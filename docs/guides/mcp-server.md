<!-- Last reviewed: 2026-05-17 -->
# MCP Server

MoneyBin's MCP server is what lets you ask an AI assistant questions like *"Where did I overspend last month?"* and have it run the multi-step pipeline that answers them — import the latest file, refresh derived tables, fetch the right report, then summarize. The agent picks the tools, chains them via action hints embedded in every response, and discovers parameter schemas at runtime. The same surface is reachable from the CLI for parity; this guide is about the chat-driven path.

The server is built on [FastMCP](https://github.com/jlowin/fastmcp) and registers around 30+ tools across nine domains. Names are stable today but pre-1.0 — every rename lands in `CHANGELOG.md`.

## Install

`moneybin mcp install --client <name>` writes the snippet your client expects (`--print` to inspect first). Supported clients today: Claude Desktop, Claude Code, Cursor, Windsurf, VS Code Copilot, Gemini CLI, Codex (CLI / Desktop / IDE), ChatGPT Desktop. See [`mcp-clients.md`](mcp-clients.md) for paths and per-client caveats.

There is no `mcp uninstall` command today. To turn the integration off, remove the `moneybin` entry from your MCP client's config file (Claude Desktop's `claude_desktop_config.json`, Cursor's `mcp.json`, etc.) and restart the client.

## Privacy: where does my data go?

The most common question from people coming from a CLI-only workflow. Plain answer:

- **The MCP server itself runs locally.** When the client launches `moneybin mcp serve`, it spawns a subprocess on your machine that reads/writes your local DuckDB file. The server makes no outbound network calls of its own.
- **Your MCP client does talk to a hosted LLM.** Claude Desktop sends your prompts to Anthropic. Cursor, Codex, Gemini CLI, and the rest each send prompts to their respective providers per their own privacy policies. MoneyBin does not change that.
- **Tool results travel the same path as your prompt.** When the agent calls a MoneyBin tool, the result envelope (which may contain transactions, balances, merchant names, categorizations) is sent back to the hosted LLM so it can continue the conversation. Anything the agent sees, the provider sees.
- **Sensitivity tiers are logged but not yet enforced as a consent gate.** Every tool declares `low`, `medium`, or `high`, and every invocation is stamped into the audit log. The planned consent prompt that gates `high`-tier calls is not in place yet. Until it ships, treat anything you ask the agent as if you sent it to your model provider, because you effectively did.
- **For a fully local path**, pair MoneyBin with an MCP client that runs against a local LLM. Some clients have experimental support; there is no first-class local-LLM option in our supported-client list today.

This is the honest current state, not the long-term target. The privacy framework that adds consent gates is tracked in the specs.

## Quick orientation

Once connected, the cheapest first call is the low-sensitivity `system_status` tool:

> *Agent prompt:* "Run `system_status` and tell me what data is loaded."

The response includes account/transaction inventory, freshness timestamps, pending review queue counts, and a `transforms.pending` flag (see "Glossary" below) that tells the agent whether derived tables need a `refresh_run` before analytics.

## A worked agent loop

This is what a daily-driver session actually looks like — chat prompts on the left, tool calls in fenced JSON on the right, abbreviated envelopes for the interesting steps. Imagine you dropped a Chase OFX file in your inbox and want last month's spending summarized.

**You:** *"Pull whatever's in my import inbox, categorize last month's transactions, and tell me where I overspent vs the trailing 12-month average."*

**Agent's plan** (it derives this from tool descriptions and action hints):

1. `import_inbox_sync` — drain new files
2. `refresh_run` — rebuild `core.*` and run categorization
3. `transactions_categorize_pending` — see what still needs review
4. `transactions_categorize_assist` → `transactions_categorize_commit` — propose + accept categorizations
5. `reports_spending` — summarize against the trailing window

Abbreviated tool calls:

```json
// Step 1 — drain the inbox
{"tool": "import_inbox_sync", "args": {}}
// → {"status": "ok", "data": {"imported_count": 87, "files": [...]},
//    "actions": ["Run refresh_run when ready to refresh derived tables"]}
```

```json
// Step 2 — follow the action hint
{"tool": "refresh_run", "args": {}}
// → {"status": "ok", "data": {"steps_ran": ["match", "transform", "categorize"], ...},
//    "actions": [{"type": "invoke", "tool": "transactions_categorize_pending", "args": {}}]}
```

```json
// Step 3 — see what's uncategorized
{"tool": "transactions_categorize_pending", "args": {"limit": 50}}
// → {"status": "ok", "summary": {"total_count": 12, "returned_count": 12, "sensitivity": "medium"},
//    "data": [{"transaction_id": "csv_abc...", "description": "SQ *BLUE BOTTLE", "amount": -6.50}, ...]}
```

```json
// Step 4a — ask for assisted categorization
{"tool": "transactions_categorize_assist",
 "args": {"transaction_ids": ["csv_abc...", "csv_def..."]}}
// → proposals returned with an action hint pointing at the commit tool
```

```json
// Step 4b — commit what the user accepts (the AI usually confirms with you first)
{"tool": "transactions_categorize_commit",
 "args": {"assignments": [{"transaction_id": "csv_abc...", "category_id": "cat_food"}, ...]}}
```

```json
// Step 5 — the answer
{"tool": "reports_spending", "args": {"period": "last_month", "compare": "trailing_12mo"}}
// → {"status": "ok", "summary": {"period": "2026-04-01 to 2026-04-30", "display_currency": "USD"},
//    "data": [{"category": "Food", "amount": -812.40, "vs_avg": "+38%"}, ...]}
```

**Agent (back to you):** *"April food spending was $812, +38% vs your trailing 12-month average — the biggest driver was a one-off $240 dinner. Everything else was within 10% of normal."*

Two things to notice: the agent never had to ask you "should I refresh first?" because `import_inbox_sync`'s action hint pointed at `refresh_run`. And `transactions_categorize_assist` returned proposals with a hint pointing at the commit tool — the curation loop is encoded in the surface, not in your prompt.

## Action hints

Every successful response carries an `actions[]` array. Each entry is either a string (human-readable suggestion) or a structured invoke object:

```json
{
  "actions": [
    {"type": "invoke", "tool": "refresh_run", "args": {"steps": ["transform", "categorize"]}},
    "Use transactions_categorize_pending to see what still needs review"
  ]
}
```

Hints are advisory — the agent can follow them or ignore them. Multiple hints may appear; the agent picks. When a hint includes structured `args`, they are fully-formed and ready to pass through; the agent is not expected to fill in blanks.

## `refresh_run`

`refresh_run` is the single umbrella that rebuilds derived state after any data change. It runs three steps in sequence:

1. **`match`** — cross-source dedup and merchant matching against `raw.*`
2. **`transform`** — SQLMesh model rebuild for `core.*`
3. **`categorize`** — apply rules and auto-accept high-confidence categorizations

Call it bare (`refresh_run()`) after imports and Plaid pulls. For targeted reruns, pass `steps=[...]`:

```json
{"tool": "refresh_run", "args": {"steps": ["transform"]}}
{"tool": "refresh_run", "args": {"steps": ["categorize"]}}
```

The standalone `transform_apply` MCP tool was removed in favor of `refresh_run(steps=["transform"])`. The four `transform_*` tools that remain (`transform_status`, `transform_plan`, `transform_validate`, `transform_audit`) are read-only.

## Tool naming

Two shapes coexist, and the difference matters:

- **Collection reads use the bare noun.** `accounts`, `merchants`, `categories` return the full list — no suffix.
- **Filtered reads, single-record reads, and actions use `domain_verb`** with the verb at the end. `accounts_get`, `transactions_categorize_commit`, `accounts_balance_assert`.

The `_list` suffix was dropped during taxonomy alignment, so there is no `accounts_list` or `transactions_list`. If you guessed `transactions_list`, the live name is `transactions_get` (with optional filters).

To enumerate the live surface, run `moneybin mcp list-tools` — every registered tool, its description, and its parameter schema.

## Tool catalog by domain

Per-tool input schemas are not in this guide on purpose; the `--help`-equivalent surface is `moneybin mcp list-tools` and the per-tool description string each agent sees at selection time.

| Domain | What it does | Representative tools |
|---|---|---|
| `system.*` / orientation | First call; freshness + pending queues + drift signals | `system_status`, `system_doctor`, `system_audit` |
| `accounts.*` | Inspect, resolve, and manage accounts and balances | `accounts`, `accounts_get`, `accounts_summary`, `accounts_set`, `accounts_resolve`, `accounts_balances`, `accounts_balance_history`, `accounts_balance_reconcile`, `accounts_balance_assertions`, `accounts_balance_assert`, `accounts_balance_assertion_delete` |
| `transactions.*` (read + curate) | Search transactions; manage notes, tags, splits, manual entry | `transactions_get`, `transactions_review`, `transactions_create`, `transactions_notes_add`, `transactions_notes_edit`, `transactions_notes_delete`, `transactions_tags_set`, `transactions_tags_rename`, `transactions_splits_set` |
| `transactions.categorize.*` | Categorization engine: rules, merchants, LLM-assist | `transactions_categorize_pending`, `transactions_categorize_assist`, `transactions_categorize_commit`, `transactions_categorize_run`, `transactions_categorize_rules`, `transactions_categorize_rules_create`, `transactions_categorize_rules_delete`, `transactions_categorize_stats`, `transactions_categorize_auto_review`, `transactions_categorize_auto_accept`, `transactions_categorize_auto_stats` |
| `reports.*` | Curated analytical views (one per report) | `reports_networth`, `reports_networth_history`, `reports_spending`, `reports_cashflow`, `reports_recurring`, `reports_merchants`, `reports_uncategorized`, `reports_large_transactions`, `reports_balance_drift`, `reports_budget` |
| `categories.*` | Category taxonomy management | `categories`, `categories_create`, `categories_set`, `categories_delete` |
| `merchants.*` | Merchant name mappings | `merchants`, `merchants_create` |
| `import.*` | File import + inbox drain + revert | `import_files`, `import_preview`, `import_status`, `import_revert`, `import_formats`, `import_inbox_sync`, `import_inbox_pending`, `import_labels_set` |
| `sync.*` | Plaid bank connections and pulls | `sync_connect`, `sync_connect_status`, `sync_disconnect`, `sync_pull`, `sync_status`, `sync_schedule_set`, `sync_schedule_show`, `sync_schedule_remove` |
| `refresh` | Single umbrella that rebuilds derived tables | `refresh_run` |
| `transform.*` | SQLMesh-step granularity (read tools) | `transform_status`, `transform_plan`, `transform_validate`, `transform_audit` |
| `sql.*` | Read-only SQL escape hatch + schema introspection | `sql_query`, `sql_schema` |

Two failure modes hide in this table — they look identical from the outside but mean different things:

- **`budget_*` and `tax_*` are unregistered.** Their service implementations exist (`budget_set`, `tax_w2`) but the `register(...)` call is not invoked until the backing specs reach `in-progress`. Calling these tool names returns an MCP protocol-level "unknown tool" error — they are 404s, not envelope errors. The CLI counterparts (`moneybin budget set`, `moneybin tax w2`) work today.
- **`sync_schedule_*` tools register but stub.** They are visible in `list-tools` and return a normal `status: "error"` envelope with `error.code = "not_implemented"` when called. The scheduler design is pending.

No invented namespaces. If a tool name does not appear in the table above, it is not registered.

## Response envelope

Every MCP tool returns the same shape — the same shape the CLI emits when called with `--output json`:

```json
{
  "status": "ok",
  "summary": {
    "total_count": 42,
    "returned_count": 42,
    "has_more": false,
    "sensitivity": "low",
    "display_currency": "USD"
  },
  "data": [ /* command-specific records */ ],
  "actions": [
    {"type": "invoke", "tool": "refresh_run", "args": {}}
  ],
  "next_cursor": null
}
```

Field gloss:

- **`status`** — `"ok"` on success, `"error"` on classified failure. Agents branch on this single field; never test for the presence of `error`.
- **`summary.total_count`** — total matching records (may exceed `returned_count` when paginated).
- **`summary.returned_count`** — records in `data` on this call.
- **`summary.has_more`** — `true` when more pages exist. Forced `true` when `next_cursor` is set.
- **`summary.sensitivity`** — `"low"`, `"medium"`, or `"high"`. See below.
- **`summary.display_currency`** — three-letter code (`"USD"` today). All amounts in `data` are in this currency unless a per-row `currency` field overrides.
- **`summary.period`** *(optional)* — human-readable window such as `"2026-01 to 2026-04"` for reports that operate over a date range.
- **`summary.degraded`** *(optional)* — `true` when the response is degraded (planned consent path; not yet exercised in production).
- **`data`** — payload. Either a list of records or a single result dict for write tools and snapshot reports.
- **`actions`** — contextual next-step hints (see "Action hints" above).
- **`next_cursor`** *(optional)* — opaque pagination token. Pass it back as the `cursor` argument on the next call to fetch the next page.
- **`error`** *(present only when `status: "error"`)* — `{code, message, hint?, details?}`. `data` is `[]` in this case.

**Pinned:** `audit_id` is **not** a top-level envelope field. It appears on individual audit records returned by `system_audit`.

## Sensitivity tiers

Every tool declares one of three tiers via the `@mcp_tool(sensitivity=...)` decorator argument.

| Tier | Data | What it controls today |
|---|---|---|
| `low` | Aggregates, counts, taxonomy reference data | None — always callable |
| `medium` | Row-level fields: descriptions, amounts, dates | Logging + audit metadata |
| `high` | Critical PII (account numbers, raw provider blobs) | Logging + audit metadata; planned consent gate |

What `sensitivity` does today: it tags the structured log line for every invocation (`tool`, `sensitivity`, `duration_ms`, `status`) and stamps the audit log row visible through `system_audit`. The logs live wherever your MoneyBin install writes them (default: stderr of the `mcp serve` subprocess; configurable via the standard logging settings). Retention is whatever you configure on the log destination — MoneyBin does not prune.

What `sensitivity` does **not** do today: there is no consent-prompt gate that requires explicit user approval before a `high`-tier call. The privacy framework that introduces the gate is planned; `summary.degraded` is wired through the envelope but not yet exercised. When the gate lands, calls without consent will return aggregate-only `data` with `summary.degraded: true` — they will never fail outright.

Tier names will not change. The enforcement layer above them may.

## Can the AI break things?

A fair question. The bounded-harm picture:

- **No writes to `core.*` or DDL of any kind.** SQLMesh owns those tables; agents cannot reach them directly. `sql_query` rejects writes and DDL at the middleware.
- **App-state mutations *do* go through MCP tools.** Categorizations, notes, tags, splits, manual transactions, category renames, merchant mappings, and assertion deletions are all reachable. The audit log captures every mutation (use `system_audit` to read it back).
- **`destructiveHint` flags the irreversible ones.** `categories_delete`, `accounts_balance_assertion_delete`, `import_revert`. Claude Desktop renders a confirmation modal for these. Cursor surfaces the annotation but does not gate the call. Codex CLI does not prompt at all — driving Codex means trusting the agent's planning, since there is no client-side gate.
- **`import_revert` is the escape hatch.** Any unwanted batch can be undone by passing its `import_id` (visible in `import_status`) to `import_revert`.
- **Until the consent gate ships, the strongest practical safeguard is `moneybin db lock`** when you're not actively using the agent. A locked database prevents the MCP server from starting at all — the trust boundary becomes the OS user, exactly as the transport section describes.

## Tool annotations

Every tool emits the four protocol-standard hints on its descriptor:

| Annotation | Meaning |
|---|---|
| `readOnlyHint` | Tool does not write app/raw/core state. Defaults to `true`. |
| `destructiveHint` | Tool performs an irreversible state change (e.g., `categories_delete`, `accounts_balance_assertion_delete`, `import_revert`). |
| `idempotentHint` | Safe to retry — same inputs produce the same result. |
| `openWorldHint` | Tool reaches an external system (the `sync_*` family). |

Client rendering varies; see the previous section for specifics.

## Failure modes

Every classified error returns `status: "error"` with an `error.code` agents can branch on. The codes that matter at the protocol layer:

| Code | Origin | Meaning |
|---|---|---|
| `invalid_arguments` | `ValidationErrorMiddleware` | Misnamed or missing parameter. The hint enumerates every accepted parameter — agents can recover on the next call without a separate schema lookup. |
| `too_many_items` | `@mcp_tool` decorator | A list-typed parameter exceeded `MCPConfig.max_items` (default 500). Split the batch and retry. |
| `timed_out` | `@mcp_tool` decorator | The tool body exceeded `MCPConfig.tool_timeout_seconds`. The DuckDB write connection is interrupted and reset so the next call gets a fresh lock. |
| `not_implemented` | `build_unimplemented_envelope` | The tool registered but its backing service isn't wired up yet (e.g., `sync_schedule_*`). |
| `not_found` | Service layer | The referenced entity (account, category, import) doesn't exist. |

Tool-specific codes (`unknown_table` from `sql_query`, `invalid_file_path` from `import_files`, `RULE_NOT_FOUND` from `transactions_categorize_rules_delete`, etc.) flow through the same envelope shape — the `error.hint` and `error.details` fields carry the recovery information.

Example error envelope:

```json
{
  "status": "error",
  "summary": {"total_count": 0, "returned_count": 0, "has_more": false, "sensitivity": "low", "display_currency": "USD"},
  "data": [],
  "actions": [],
  "error": {
    "code": "invalid_arguments",
    "message": "Invalid arguments for transactions_get",
    "hint": "Accepted parameters: accounts, category, cursor, description, from_date, limit, to_date; Remove unrecognized: 'date_from'",
    "details": {
      "tool": "transactions_get",
      "unexpected": ["date_from"],
      "accepted": ["accounts", "category", "cursor", "description", "from_date", "limit", "to_date"]
    }
  }
}
```

## Long-running tools

A few tools — `sync_pull`, `refresh_run`, large `import_files` calls, and `transactions_categorize_run` over big windows — can take minutes on real data. Honest current state:

- **No incremental progress stream over stdio.** Each tool returns a single final envelope when done. Clients see the call as "in flight" until then.
- **Wall-clock cap is enforced.** A tool that exceeds `MCPConfig.tool_timeout_seconds` returns `error.code = "timed_out"` and the DuckDB write connection is interrupted + reset. The next call gets a fresh lock.
- **Cancellation is not supported.** The MCP cancellation notification cancels the awaited task in the server, but the OS thread running the synchronous tool body keeps executing until it completes or hits the timeout. Treat in-flight long-running tools as committed.
- **Single-writer rule still applies.** The MCP write connection is shared across calls. A `refresh_run` in flight blocks other write tools from starting; read tools (`read_only=True`) can proceed because they attach in shared-read mode.

There are no measured rate limits or per-call cost numbers to publish — agents and clients vary too much. If `categorize_assist` is invoking an LLM, the cost is on the configured assist provider, billed where you configured the credentials.

## Pagination

Cursor-based, opaque. A response with more pages sets `summary.has_more: true` and `next_cursor` to a token. Pass the token back as the `cursor` argument on the same tool to fetch the next page. Cursors are not portable across tools or sessions and may expire; agents must not store them.

```text
transactions_get(limit=50) → {next_cursor: "abc..."}
transactions_get(limit=50, cursor: "abc...") → {next_cursor: "def..."}
transactions_get(limit=50, cursor: "def...") → {next_cursor: null, has_more: false}
```

List-typed parameters (e.g., `paths` on `import_files`, `transactions` on `transactions_categorize_commit`) are capped per-call via `MCPConfig.max_items` — default 500. Exceeding the cap returns `error.code = "too_many_items"`. Split large batches into multiple calls.

## `sql_query` and the curated schema

`sql_query` accepts a read-only SQL statement (`SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `PRAGMA`, `EXPLAIN`) against the DuckDB database. Writes to `core.*`, DDL, and writes outside the app-tier allowlist are rejected. Use `sql_query` as an escape hatch when no purpose-built tool covers the question.

Before composing non-trivial SQL, read the curated schema. Two routes:

- **MCP resource** — `moneybin://schema`. Returns the curated `core.*` and select `app.*` tables with column comments and example queries. Use this on clients that surface resources.
- **`sql_schema` tool** — the resource as a tool, for clients that don't expose resources (VS Code Copilot, others). Default call returns a compact catalog; pass `table='<schema.name>'` for one table, or `table='*'` for the full ~50KB document.

Other registered resources: `moneybin://status`, `moneybin://accounts`, `moneybin://privacy`, `moneybin://tools`, `moneybin://recent-curation`. Resources are an enhancement, not a requirement — every critical capability is reachable via a tool. Prefer tools when in doubt; use resources only on clients that promote them (resources are not universally rendered).

CLI footnote: `moneybin db query` (the CLI raw-SQL command) wraps `sql_query` and can emit raw row JSON via `--output json`. On the MCP surface, `sql_query` always returns the standard envelope.

## What MCP tools cannot do

- **No writes to `core.*`.** Canonical models are rebuilt by SQLMesh; agents trigger that via `refresh_run`, never by direct write.
- **No DDL.** `CREATE`, `ALTER`, `DROP` are rejected by `sql_query`.
- **No app-state mutations through `sql_query`.** Writes to `app.*` flow through dedicated tools (`categories_set`, `merchants_create`, `transactions_notes_add`, …) so the audit log captures intent.
- **No secret material.** Tools that touch passphrases, encryption keys, or sync credentials are CLI-only by design. See `.claude/rules/mcp-server.md` "When CLI-only is justified" for the policy.
- **No filesystem escape.** `import_files` validates paths before handing them to DuckDB readers.

## Transport

Stdio today. The client launches `moneybin mcp serve` as a subprocess and talks over stdin/stdout — there is no listening port, no network surface, no auth handshake. The OS user owning the client process is the trust boundary.

A Streamable HTTP transport is planned for the hosted tier but is not implemented yet. When it lands, the same tool surface will be reachable over HTTP with an explicit auth path.

## Testing without a real dataset

Generate a synthetic profile so you can drive the surface end-to-end against fake-but-plausible data: `moneybin synthetic generate --persona <name>`. To wipe it, `moneybin synthetic reset --persona <name>`. The MCP server reads the same database, so an agent connected after generation sees the synthetic data exactly as it would see real imports.

## Stability promise

Pre-1.0. Tool names, parameter shapes, and envelope fields may change before launch. The contract today:

- Every rename lands in `CHANGELOG.md`.
- Removed tools stay as deprecation-aliased shims for one minor release where practical; the pre-launch posture is a hard cut otherwise. Recent hard cuts are documented inline in `CHANGELOG.md` (`Changed` and `Removed` sections).
- The envelope shape (`status`, `summary`, `data`, `actions`, optional `error`, optional `next_cursor`) is locked. Adding new optional fields is non-breaking; existing field semantics will not change before 1.0 without a CHANGELOG `Changed` entry.
- The sensitivity tier names (`low` / `medium` / `high`) and the annotation hints (`readOnlyHint` / `destructiveHint` / `idempotentHint` / `openWorldHint`) will not change.

At 1.0 the surface locks: schema changes go through versioned migration paths, renames require a one-minor-release deprecation cycle, and the envelope shape requires an ADR to evolve.

## Glossary

- **drift signals** — fields on `system_status` that indicate state divergence: a balance assertion that no longer matches the computed balance, a category whose rule set changed since last apply, a Plaid connection whose last pull failed. Drift signals are how the agent learns it needs to act before reporting.
- **`transforms.pending`** — a counter on `system_status.data` showing how many SQLMesh models are out of date with respect to `raw.*`. Non-zero means `refresh_run` should run before analytics, and `system_status` will include an action hint pointing at it.

## Extending the server

Adding a new tool is a thin wrapper around the service layer plus a `register(...)` line; see CONTRIBUTING.md's "Adding a new MCP tool" recipe for the full checklist (decorator, description requirements, spec + capability-map updates).
