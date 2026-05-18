<!-- Last reviewed: 2026-05-17 -->
# MCP Server

MoneyBin's MCP server exposes the same functional surface as the CLI — agents in Claude Code, Cursor, Codex CLI, and Gemini CLI drive MoneyBin as a peer to the local terminal. This guide covers what the server is, how to install it, how to call tools, and the contract every response follows.

The server is built on [FastMCP](https://github.com/jlowin/fastmcp) and registers around 30+ tools across nine domains. Names are stable today but pre-1.0 — every rename lands in `CHANGELOG.md`. Read [`mcp-clients.md`](mcp-clients.md) for per-client install paths; this page assumes the client is already wired up.

## Install and first call

Install MoneyBin into your client of choice:

```bash
# Print the snippet your client expects (no write)
moneybin mcp install --client claude-desktop --print

# Or write the snippet into the client's config file
moneybin mcp install --client claude-desktop
```

Supported clients today: Claude Desktop, Claude Code, Cursor, Windsurf, VS Code Copilot, Gemini CLI, Codex (CLI / Desktop / IDE), ChatGPT Desktop. See [`mcp-clients.md`](mcp-clients.md) for paths and concurrency caveats.

Once connected, orient with the low-sensitivity `system_status` tool:

> *Agent prompt:* "Run `system_status` and tell me what data is loaded."

The response includes account/transaction inventory, freshness timestamps, pending review queue counts, and a `transforms.pending` flag that tells the agent whether derived tables need a `refresh_run` before analytics.

## Transport

Stdio today. The client launches `moneybin mcp serve` as a subprocess and talks over stdin/stdout — there is no listening port, no network surface, no auth handshake. The OS user owning the client process is the trust boundary.

A Streamable HTTP transport is planned for the hosted tier but is not implemented yet. When it lands, the same tool surface will be reachable over HTTP with an explicit auth path.

## Tool catalog by domain

Tool names follow `domain_<sub>_verb` with the verb at the end (`transactions_categorize_commit`, `accounts_balance_assert`). Read tools that return collections use noun-only names (`accounts`, `merchants`, `categories`) per the surface-design taxonomy — the `_list` suffix was dropped to match shape conventions, so there is no `accounts_list`.

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

Notes on the catalog:

- **`refresh_run` is the umbrella.** It runs match → SQLMesh apply → categorization. The standalone `transform_apply` MCP tool was removed; the granular form is `refresh_run(steps=["transform"])`. The four `transform_*` tools that remain are read-only (`status`, `plan`, `validate`, `audit`).
- **`transactions_get` is the primary list/search read.** There is no `transactions_list` MCP tool — the read verb is `_get` with optional filters. The CLI counterpart is `moneybin transactions list`.
- **`system_audit` is the audit-log read.** Each row in the response carries its own `audit_id`. `audit_id` is *not* a top-level envelope field; do not expect it on every response.
- **`budget_*` and `tax_*` are gated.** Their implementations exist (`budget_set`, `tax_w2`) but are not registered until the backing specs reach `in-progress`. The CLI counterparts (`moneybin budget set`, `moneybin tax w2`) work today.
- **Sync schedule tools (`sync_schedule_*`) are taxonomy stubs.** They register but return `not_implemented` envelopes pending the scheduler design.
- **No invented namespaces.** If a tool name does not appear in the table above, it is not registered.

To enumerate the live surface, run `moneybin mcp list-tools` — every registered tool, its description, and its parameter schema.

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
    { "type": "invoke", "tool": "refresh_run", "args": {} }
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
- **`actions`** — contextual next-step hints (see "Action hints" below).
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

What `sensitivity` does today: it tags the log line for every invocation and stamps the audit record so an operator can answer "which medium- and high-tier tools did this agent call this session?".

What `sensitivity` does **not** do today: there is no consent-prompt gate that requires explicit user approval before a `high`-tier call. The privacy framework that introduces the gate is planned; `summary.degraded` is wired through the envelope but not yet exercised. When the gate lands, calls without consent will return aggregate-only `data` with `summary.degraded: true` — they will never fail outright.

Tier names will not change. The enforcement layer above them may.

## Tool annotations

Every tool emits the four protocol-standard hints on its descriptor:

| Annotation | Meaning |
|---|---|
| `readOnlyHint` | Tool does not write app/raw/core state. Defaults to `true`. |
| `destructiveHint` | Tool performs an irreversible state change (e.g., `categories_delete`, `accounts_balance_assertion_delete`, `import_revert`). |
| `idempotentHint` | Safe to retry — same inputs produce the same result. |
| `openWorldHint` | Tool reaches an external system (the `sync_*` family). |

Client rendering varies. Claude Desktop renders a "destructive operation" confirmation modal for `destructiveHint: true`. Cursor surfaces the annotation in tool-listing UI but does not gate the call. Codex CLI does not render a prompt today — agents driving Codex must respect the hint at planning time, not at call time.

## Action hints

Every successful response carries an `actions[]` array of contextual next-step suggestions. Examples in practice:

- `import_files` returns an action pointing at `refresh_run` so the agent can rebuild derived tables without prompt-side instructions.
- `transactions_categorize_assist` returns an action pointing at `transactions_categorize_commit` with the proposed categorizations.
- `reports_spending` and `reports_cashflow` return actions for widening or shifting the date window when the caller defaulted to the trailing 12 months.
- `system_status` returns an action pointing at `refresh_run` when `data.transforms.pending` is non-zero.

Actions are advisory — they encode the workflow the surface designer expects, so prompt authors don't have to. Agents can follow them or ignore them.

## Validation errors

When the client calls a tool with a misnamed or missing argument, `ValidationErrorMiddleware` converts the raw `pydantic_core.ValidationError` into the standard envelope before the tool body runs:

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

The hint enumerates every accepted parameter — agents can recover on the next call without a separate schema lookup. Agents that received `"Unexpected keyword argument..."` strings from older releases should drop their guess-and-retry fallback paths.

## Pagination

Cursor-based, opaque. A response with more pages sets `summary.has_more: true` and `next_cursor` to a token. Pass the token back as the `cursor` argument on the same tool to fetch the next page. Cursors are not portable across tools or sessions and may expire; agents must not store them.

```text
transactions_get(limit=50) → {next_cursor: "abc..."}
transactions_get(limit=50, cursor: "abc...") → {next_cursor: "def..."}
transactions_get(limit=50, cursor: "def...") → {next_cursor: null, has_more: false}
```

List-typed parameters (e.g., `paths` on `import_files`, `transactions` on `transactions_categorize_commit`) are capped per-call via `MCPConfig.max_items` — default 500. Exceeding the cap returns `error.code = "too_many_items"`. Split large batches into multiple calls.

## Tool timeouts

Every tool body runs under a wall-clock cap (default configurable via `MCPConfig.tool_timeout_seconds`). A tool that exceeds the cap returns `error.code = "timed_out"` with elapsed and cap fields in `details`. The DuckDB write connection is interrupted and reset so the next call gets a fresh lock. See `mcp-tool-timeouts.md` for the full contract — including the documented limitation that a sync tool body may complete in the background after the client has received the timeout envelope.

## `sql_query` and the curated schema

`sql_query` accepts a read-only SQL statement (`SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `PRAGMA`, `EXPLAIN`) against the DuckDB database. Writes to `core.*`, DDL, and writes outside the app-tier allowlist are rejected. Use `sql_query` as an escape hatch when no purpose-built tool covers the question.

Before composing non-trivial SQL, read the curated schema. Two routes:

- **MCP resource** — `moneybin://schema`. Returns the curated `core.*` and select `app.*` tables with column comments and example queries. Use this on clients that surface resources.
- **`sql_schema` tool** — the resource as a tool, for clients that don't expose resources (VS Code Copilot, others). Default call returns a compact catalog; pass `table='<schema.name>'` for one table, or `table='*'` for the full ~50KB document.

Other registered resources: `moneybin://status`, `moneybin://accounts`, `moneybin://privacy`, `moneybin://tools`, `moneybin://recent-curation`. Resources are an enhancement, not a requirement — every critical capability is reachable via a tool.

CLI footnote: `moneybin db query` (the CLI raw-SQL command) wraps `sql_query` and can emit raw row JSON via `--output json`. On the MCP surface, `sql_query` always returns the standard envelope; the rows are nested in `data`.

## What MCP tools cannot do

- **No writes to `core.*`.** Canonical models are rebuilt by SQLMesh; agents trigger that via `refresh_run`, never by direct write.
- **No DDL.** `CREATE`, `ALTER`, `DROP` are rejected by `sql_query`.
- **No app-state mutations through `sql_query`.** Writes to `app.*` flow through dedicated tools (`categories_set`, `merchants_create`, `transactions_notes_add`, …) so the audit log captures intent.
- **No secret material.** Tools that touch passphrases, encryption keys, or sync credentials are CLI-only by design. See `.claude/rules/mcp-server.md` "When CLI-only is justified" for the policy.
- **No filesystem escape.** `import_files` validates paths before handing them to DuckDB readers.

## Stability promise

Pre-1.0. Tool names, parameter shapes, and envelope fields may change before launch. The contract today:

- Every rename lands in `CHANGELOG.md`.
- Removed tools stay as deprecation-aliased shims for one minor release where practical; the pre-launch posture is a hard cut otherwise. Recent hard cuts are documented inline in `CHANGELOG.md` (`Changed` and `Removed` sections).
- The envelope shape (`status`, `summary`, `data`, `actions`, optional `error`, optional `next_cursor`) is locked. Adding new optional fields is non-breaking; existing field semantics will not change before 1.0 without a CHANGELOG `Changed` entry.
- The sensitivity tier names (`low` / `medium` / `high`) and the annotation hints (`readOnlyHint` / `destructiveHint` / `idempotentHint` / `openWorldHint`) will not change.

At 1.0 the surface locks: schema changes go through versioned migration paths, renames require a one-minor-release deprecation cycle, and the envelope shape requires an ADR to evolve.

## Extending the server

New MCP tools are thin wrappers around the service layer. See CONTRIBUTING.md's "Adding a new MCP tool" recipe — the protocol is one `@mcp_tool` decoration, one `register(...)` line in the namespace's `register_*_tools` function, one description string covering sign/currency/mutation invariants, and matching surface-spec + capability-map updates per the surface-change discipline rule.

The CLI counterpart goes in alongside it. Parity is functional — same outcomes reachable on both surfaces — not 1:1 name matching.
