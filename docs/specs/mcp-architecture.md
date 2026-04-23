# MCP Architecture & Design

> Last updated: 2026-04-17
> Status: Ready
> Companions: [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) (AI data flow tiers, consent model), [`mcp-tool-surface.md`](mcp-tool-surface.md) (concrete tool/prompt/resource definitions), [ADR-003: MCP Primary Interface](../decisions/003-mcp-primary-interface.md)
> Supersedes: [`mcp-tier1-tools.md`](mcp-tier1-tools.md) (prototype-era tool list), [`archived/mcp-read-tools.md`](archived/mcp-read-tools.md), [`archived/mcp-write-tools.md`](archived/mcp-write-tools.md)

## Purpose

This spec defines the architecture, design philosophy, and conventions for MoneyBin's MCP server and its symmetric CLI surface. It is the "how we think about MCP tools" document. The companion spec [`mcp-tool-surface.md`](mcp-tool-surface.md) defines every concrete tool, prompt, and resource — the "what we're building" document.

Together they replace the prototype-era MCP specs (read tools, write tools, tier-1 tools) with a production-grade design built for modern AI desktop applications.

## Status

ready

## Mission

**MoneyBin's MCP server is the primary programmatic interface to the user's financial data.** It serves two classes of consumer — AI desktop apps (Claude, ChatGPT, Cursor) and AI-augmented CLIs (Claude Code, Codex) — with equal priority. The CLI is functionally equivalent and human-optimized.

---

## 1. Design Philosophy

### Core tenets

1. **Import-first, not ledger-first.** Transactions enter MoneyBin through source files (OFX, CSV, PDF) and connectors (Plaid). There is no general-purpose `add_transaction` tool. Corrections and annotations are metadata on source-imported records, not counter-entries. This is a data warehouse, not an accounting ledger.

2. **Privacy by architecture.** Every tool declares a data sensitivity tier. The MCP server enforces consent gates and response filtering automatically — individual tools don't implement their own privacy logic. The redaction engine, consent system, and audit log are shared infrastructure consumed via a middleware layer.

3. **Batch-first, composable tools.** Each tool is designed to be called once per turn and return a complete, actionable result. Tools that operate on collections (categorization, rule creation, corrections) accept lists, not single items. Complex workflows compose multiple tools across turns, not multiple calls within a turn.

4. **AI-ergonomic by default.** Tool names, descriptions, and parameter schemas are designed for LLM tool selection. Response shapes give the model enough context to reason (structured data with summary metadata) without overwhelming the context window (pagination, configurable detail levels).

5. **CLI symmetry.** Every MCP tool has a CLI equivalent. Same capabilities, same data, different ergonomics. The MCP server and CLI are thin surfaces over a shared service layer — neither implements business logic directly.

### Import-first rationale

Traditional personal finance tools (Beancount, GnuCash, YNAB) treat manual transaction entry as a primary workflow. This made sense when bank data was hard to get. In 2026, with OFX exports, CSV downloads, and Plaid API access, manual entry is a legacy interaction model.

MoneyBin's position:

- **Transactions come from sources** — files and connectors. Every transaction has provenance.
- **Corrections are metadata** — when a source record is wrong, the fix is an override in the prep layer ("for this transaction_id, the canonical amount is $42.50"), not a counter-entry in a ledger. The correction travels with the transaction, is auditable, and doesn't create phantom records.
- **Annotations enrich, they don't create** — tags, notes, and cash breakdowns are metadata on existing transactions. A $200 ATM withdrawal can be annotated with how the cash was spent, but the annotation doesn't create new transactions that double-count the withdrawal.
- **No general-purpose `add_transaction`** — investment trades, manual adjustments, and other sourceless records wait for their domain-specific source (broker CSV import, Plaid Investments). Domain-specific recording tools may exist within their namespace (e.g., future `investments.record_trade`), but there is no generic transaction creation tool.

This philosophy is a deliberate product decision. MoneyBin is a data platform that imports and analyzes financial data, not a bookkeeping tool that records it manually.

---

## 2. Architectural Layers

### The service layer contract

MCP tools and CLI commands are both thin wrappers around the same service layer. Neither contains business logic — they translate between their interface idiom (MCP tool calls / Typer commands) and service method calls.

```
+-----------------+  +-----------------+
|    MCP Tools    |  |       CLI       |
|    (FastMCP)    |  |     (Typer)     |
+--------+--------+  +--------+--------+
         |                     |
         v                     v
+------------------------------------------+
|          Privacy Middleware               |
|  (sensitivity gates, consent checks,     |
|   audit logging, response filtering)     |
+---------+--------------------------------+
          |
          v
+------------------------------------------+
|            Service Layer                 |
|  (business logic, queries,               |
|   data operations)                       |
+---------+--------------------------------+
          |
          v
+------------------------------------------+
|              DuckDB                      |
|     (raw / prep / core / app)            |
+------------------------------------------+
```

### Key boundaries

- **MCP/CLI layer** — Parameter validation, input/output formatting, help text. No SQL, no business logic. Maps 1:1 between the two surfaces.
- **Privacy middleware** — Intercepts every tool call. Checks sensitivity tier, verifies consent status, applies redaction for the active consent level, logs to audit table. Tools are unaware of their own privacy enforcement — they return full data and the middleware filters it.
- **Service layer** — Business logic, query construction, data operations. Parameterized SQL only. Returns typed Python objects (dataclasses or Pydantic models), never raw query results.
- **DuckDB** — Read-only connection for queries, short-lived write connection for mutations (existing pattern from the prototype).

### Privacy middleware behavior

The middleware sits between the tool/CLI layer and the service layer. On every call:

1. **Resolve sensitivity tier** — look up the tool's declared sensitivity.
2. **Check consent** — for tier-2+ tools, verify active consent grant for the relevant feature category (`mcp-data-sharing`).
3. **If consented (or tier 0/1)** — pass through to service layer, log to audit table, return full result.
4. **If not consented** — call the service layer's *degraded* variant (aggregates instead of row-level data), append consent instructions to the response, log the degraded call to audit table.
5. **Always** — redact critical-tier fields (account numbers, SSNs) regardless of consent, unless verified-local mode with `LOCAL_UNMASK_CRITICAL` enabled.

This means a tool author writes one service method that returns full data. The middleware handles the rest.

---

## 3. Tool Taxonomy & Namespace Design

### Namespace structure

Tools use a hybrid namespace that reflects the most natural way an AI or user would think about the action. The first segment is the **domain**, the second is the **action or view**.

| Namespace | Domain | Purpose |
|---|---|---|
| `spending.*` | Expense analysis | Summaries, trends, category breakdowns, merchant analysis |
| `cashflow.*` | Money movement | Income vs outflows, net cash position, income source analysis |
| `accounts.*` | Account management | Account listing, balance history, net worth |
| `transactions.*` | Transaction-level operations | Search, corrections, annotations, recurring detection |
| `import.*` | Data ingestion | File import, status, source management |
| `categorize.*` | Categorization pipeline | Rules, merchant mappings, bulk categorization, auto-rule review |
| `budget.*` | Budget tracking | Targets, status, rollovers |
| `tax.*` | Tax information | W-2 data, future: capital gains, deductions |
| `privacy.*` | Privacy & consent | Consent status, grants, revocations, audit log |
| `overview.*` | Cross-domain summaries | Data status, financial health snapshot, system info |

### Namespace principles

1. **One namespace per concern, not per entity.** `import` handles all file types — there's no `import_ofx`, `import_csv`, `import_pdf`. The tool figures out the file type or accepts a hint parameter.
2. **Read and write in the same namespace.** `categorize.bulk` (write) lives alongside `categorize.rules` (read). The verb in the tool name distinguishes intent.
3. **No CRUD naming.** Tools are named for what the user wants to accomplish, not the database operation. `categorize.bulk` not `create_transaction_categories`. `transactions.correct` not `update_transaction`.
4. **New domains added per quarter.** Q2 adds `investments.*`. Q4's multi-currency is a crosscutting concern handled at the service layer (amounts, conversions, rate lookups), not a separate tool namespace.

### Naming conventions

- **Noun = query.** `spending.summary`, `accounts.balances`, `categorize.rules` — returns data.
- **Verb = action.** `categorize.bulk`, `transactions.correct`, `import.file` — mutates state.
- **Dot separator** per MCP SEP-986. Tool names are lowercase, no underscores within segments.

### Progressive disclosure via namespace registration

The full tool surface (46+ tools across 10+ namespaces) exceeds the practical limit for AI tool selection accuracy (~20-30 tools). LLM performance degrades when tool descriptions overlap or when the schema payload consumes too much context. Rather than consolidating tools (which trades tool count for schema complexity and loses the "one tool does one thing" clarity), the server uses **namespace-based progressive disclosure**.

**Core namespaces** are registered at connection time. A namespace is core if the AI would need it in **nearly every session** — orientation, common queries, primary data entry. Extended namespaces serve **specific workflows** the user enters intentionally (categorization triage, tax prep, match review).

The default core set keeps the initial tool count under ~20:

| Namespace | Tools | Why core |
|---|---|---|
| `overview.*` | 2 | Orientation — the AI's first call |
| `spending.*` | 4 | Most common user intent |
| `cashflow.*` | 2 | Complements spending |
| `accounts.*` | 4 | Foundational context |
| `transactions.*` | 4 | Universal query + corrections (excludes `matches` sub-domain) |
| `import.*` | 2–4 | Primary data entry (core subset: `file`, `status`) |
| `sql.*` | 1 | Power-user escape hatch |

**Extended namespaces** are loaded on demand via the `moneybin.discover` meta-tool:

| Namespace | Tools | When needed |
|---|---|---|
| `categorize.*` | 6–15 | Categorization workflow |
| `budget.*` | 4 | Budget tracking |
| `tax.*` | 2 | Tax prep |
| `privacy.*` | 4 | Privacy management |
| `transactions.matches.*` | 6 | Match review workflow |
| `import.*` (extended) | 3 | AI parsing, folder import |

**The core set is configurable.** Users who primarily budget can add `budget` to core; users doing tax prep can add `tax`. Configuration lives in the profile's `config.yaml` under `mcp.core_namespaces`. Setting the list to `["*"]` disables progressive disclosure and loads all tools at connection (equivalent to `MONEYBIN_MCP_LOAD_ALL=true`).

**How it works:**

1. **At connection time:** The server registers core namespace tools only. The `moneybin://tools` resource lists all available namespaces with one-line descriptions, so the AI knows what's available without seeing every schema.
2. **On `moneybin.discover(namespace="categorize")`:** The server registers the `categorize.*` tools dynamically and sends a `tools/list_changed` notification. The response includes tool names and descriptions so the AI can immediately use them.
3. **Once loaded, tools stay loaded** for the session. No unloading — the AI might reference a loaded tool later.
4. **The `actions` array** in response envelopes serves as lightweight progressive disclosure within a session — when `spending.summary` suggests "Use spending.by_category for breakdown", the AI already has that tool (core namespace). When a tool suggests a tool from an extended namespace, the AI calls `discover` first.

**Design constraints:**

- **`moneybin.discover` is always registered.** It's the only tool outside of core namespaces that's available at connection time.
- **No tool consolidation.** Each tool does one thing with a clean schema. The progressive disclosure pattern handles scale; individual tools stay simple.
- **Graceful fallback.** If a client doesn't support `tools/list_changed`, the server can be configured to register all tools at startup (`MONEYBIN_MCP_LOAD_ALL=true`). The progressive disclosure pattern is an optimization, not a requirement.
- **Prompts reference tools by name.** When a prompt references a tool from an extended namespace, the prompt template includes a discover step.

### Multi-currency as a crosscutting concern

Multi-currency is not a tool domain. It surfaces as:

- A **parameter** on existing tools (e.g., `detail` level that includes native currency alongside home-currency amounts).
- **Response metadata** (`display_currency`, `native_currencies` — see section 4).
- A **service-layer concern** (conversion at query time using cached exchange rates).
- **Rate overrides** handled via `transactions.correct` as a correction on a specific transaction.

---

## 4. Tool Design Patterns

### Batch semantics

Tools that operate on collections accept and return lists in a single call. The pattern:

1. **Read tool** returns candidates with enough context for the AI to reason about all of them.
2. **AI reasons** across the full set in one turn.
3. **Write tool** accepts the full list of decisions in one call.

```
Turn 1: categorize.uncategorized(limit=50)
        -> returns 50 transactions with descriptions, amounts, dates, suggested categories

Turn 2: categorize.bulk([{id: "tx_1", category: "groceries"}, {id: "tx_2", category: "dining"}, ...])
        -> applies all 50 categorizations, returns summary: {applied: 48, skipped: 2, errors: [...]}
```

No tool accepts a single item when a list is the natural unit of work. Tools that *could* operate on one item still accept a list — a list of one is fine.

### Response envelope

Every tool returns a consistent envelope:

```json
{
  "summary": {
    "total_count": 247,
    "returned_count": 50,
    "has_more": true,
    "period": "2026-01 to 2026-04",
    "sensitivity": "medium",
    "display_currency": "USD"
  },
  "data": [ ... ],
  "actions": [
    "Use spending.by_category for category breakdown",
    "Use transactions.search with narrower date range for full results"
  ]
}
```

Three sections:

- **`summary`** — Metadata the AI needs to frame its response: counts, whether results are truncated, the time period covered, the sensitivity tier of the data returned, the currency amounts are denominated in. Always present, even on empty results.
- **`data`** — The payload. Structured objects, never pre-formatted strings. Shape is tool-specific but consistent within a namespace (all `spending.*` tools return amounts in the same format with the same field names).
- **`actions`** — Contextual next steps. Not prescriptive — the AI decides whether to surface them. Helps the AI discover composable follow-up tools without scanning the full tool catalog. Empty list when no follow-ups are relevant.

### Currency in responses

Currency information lives in response metadata, not per-row:

- **`summary.display_currency`** — the currency all amounts are denominated in (home currency after conversion). One field, top of response.
- **`summary.native_currencies`** — present only when the result spans multiple source currencies, so the AI knows conversions were applied.
- **Per-row `currency` field** — only when a tool returns rows in their native (unconverted) currencies. This is the exception, not the default.

Single-currency users see zero currency fields on individual rows.

### Degraded responses

When a tier-2 tool is called without consent, the middleware replaces the response with a degraded variant. The degraded response uses the **same envelope** — the AI doesn't need special handling:

```json
{
  "summary": {
    "total_count": 247,
    "returned_count": 5,
    "has_more": false,
    "period": "2026-01 to 2026-04",
    "sensitivity": "low",
    "degraded": true,
    "degraded_reason": "Transaction-level data requires data-sharing consent"
  },
  "data": [
    {"category": "Groceries", "total": 1245.67, "transaction_count": 42},
    {"category": "Dining", "total": 387.20, "transaction_count": 18}
  ],
  "actions": [
    "Run `moneybin privacy grant mcp-data-sharing` to enable full transaction details"
  ]
}
```

Key properties:

- **Never fail.** The tool always returns *something* useful within the current consent level.
- **Same envelope.** `summary.degraded: true` signals the AI that the response is limited. The AI can mention this to the user or silently work with what's available.
- **Aggregate, don't truncate.** A degraded response returns category totals, not the first 5 transactions. The user still gets value.
- **One consent action.** The `actions` array tells the AI exactly how to unlock full data. One command, not a privacy policy essay.

### Pagination

Tools return a configurable number of results (default varies by tool, respects `MAX_ROWS`). For large result sets:

- **`limit`** and **`offset`** parameters on read tools that can return unbounded results.
- **`summary.has_more: true`** signals more data is available.
- **Prefer filtering over paging.** Tools expose rich filter parameters (date ranges, amount thresholds, categories, accounts) so the AI narrows the query rather than paging through everything. A well-filtered query should rarely need page 2.

### Parameter conventions

Consistent across all tools:

| Parameter | Convention | Example |
|---|---|---|
| Date ranges | `start_date` / `end_date` as ISO 8601 strings, optional | `2026-01-01` |
| Lookback | `months` integer as alternative to explicit dates | `months=3` |
| Account filter | `account_id`, optional, accepts list | `["acct_1", "acct_2"]` |
| Pagination | `limit` (default per tool), `offset` (default 0) | `limit=50, offset=100` |
| Output detail | `detail` enum: `summary`, `standard`, `full` | `detail="full"` |

The `detail` parameter controls response verbosity — `summary` returns aggregates only (always tier-1 safe), `standard` is the default, `full` includes every available field. This gives the AI a way to request minimal data when it only needs a quick answer.

---

## 5. Sensitivity Tiers & Privacy Integration

### Tool sensitivity declarations

Every tool declares its **maximum data sensitivity** — the highest sensitivity tier that could appear in its full (non-degraded) response. This is a static property of the tool, not a runtime calculation.

| Sensitivity | Data characteristics | Consent required | Example tools |
|---|---|---|---|
| `low` | Aggregates, counts, category labels, structural metadata | None | `spending.summary`, `overview.status`, `accounts.list` |
| `medium` | Row-level data: descriptions, amounts, dates, merchant names | `mcp-data-sharing` (tier-2, persistent) | `transactions.search`, `spending.merchants`, `categorize.uncategorized` |
| `high` | Responses that include critical-tier fields (account numbers, routing numbers) — masked for cloud backends, unmaskable only in verified-local mode | `mcp-data-sharing` (tier-2) + masking invariant | `accounts.details` |

### Sensitivity behavior by tier

| | `low` tool | `medium` tool (consented) | `medium` tool (not consented) | `high` tool (consented) | `high` tool (not consented) |
|---|---|---|---|---|---|
| Response | Full data | Row-level data, critical fields masked | Degraded to aggregates | Full data, critical fields masked unless verified-local + `LOCAL_UNMASK_CRITICAL` | Degraded to aggregates |
| Audit logged | Yes | Yes | Yes (degraded) | Yes | Yes (degraded) |

### How sensitivity flows through the middleware

```
Tool declares: sensitivity = "medium"
                    |
                    v
        +-- Consent granted? ------- Yes --> Full response
        |                                    (critical fields still masked)
        |
        No
        |
        v
  Degraded response
  (aggregates only, sensitivity = "low")
```

The tool author doesn't write conditional logic. They write one service method that returns full data. The middleware handles:

- Checking consent status for the tool's declared sensitivity.
- Swapping in the degraded service method when consent is absent.
- Masking critical-tier fields in all responses regardless of consent (unless verified-local override).
- Logging the call to the audit table.

### Sensitivity and the `detail` parameter

The `detail` parameter interacts with sensitivity:

| `detail` value | `low` tool | `medium` tool (consented) | `medium` tool (not consented) |
|---|---|---|---|
| `summary` | Aggregates | Aggregates (no row-level data sent even with consent) | Aggregates |
| `standard` | Default view | Row-level with standard fields | Degraded aggregates |
| `full` | Expanded view | Row-level with all fields | Degraded aggregates |

`detail=summary` is a natural way for an AI to get a quick tier-1-safe answer from a medium-sensitivity tool without triggering consent gates — the AI can self-select the detail level based on what the user actually asked. The middleware only checks consent when `detail` is `standard` or `full`; a `summary` request always returns aggregates and never triggers a consent prompt.

### Verified-local bypass

When the configured AI backend is verified-local (Ollama on localhost):

- Tier-2 consent gates are skipped — data never leaves the machine.
- `summary.sensitivity` still reflects the true tier for transparency.
- Audit log still records the call with `backend_local=true`.
- Critical field masking remains on by default (user can override with `LOCAL_UNMASK_CRITICAL`).

### Dependencies consumed (not defined here)

| Subsystem | What this spec needs from it |
|---|---|
| Redaction engine | Field-level masking rules per sensitivity tier, deterministic redaction |
| Consent management | `app.ai_consent_grants` schema, grant/revoke lifecycle, consent check API |
| Audit log | `app.ai_audit_log` schema, logging contract, query API |
| Provider profiles | `AIBackend` interface, provider metadata, verified-local detection |

Each gets its own spec. This spec defines how the MCP layer *consumes* them.

---

## 6. Prompts & Resources

### Prompts

MCP prompts are guided workflows — structured templates that help the AI walk the user through a multi-step task. They're not tools (they don't execute actions), they're scripts that orchestrate tool calls.

**Design principles:**

1. **Workflow-oriented, not tool-oriented.** A prompt represents a user goal ("review my monthly finances"), not a tool wrapper ("call spending.summary with these params"). The prompt orchestrates multiple tools.
2. **Opinionated sequence.** Each prompt defines the order of operations, what data to gather, what to present, and when to ask the user for input. The AI follows the script rather than improvising.
3. **Composable with tools.** Prompts reference tools by name. When tools evolve (new parameters, richer responses), prompts automatically benefit.
4. **Few, high-value.** Prompts exist for workflows that are common enough to standardize and complex enough that an AI might not discover the right tool composition on its own.

**Prompt categories:**

| Category | Purpose | Examples |
|---|---|---|
| **Review** | Periodic financial review workflows | Monthly review, annual review, tax prep |
| **Triage** | Investigate and resolve pending items | Categorization triage, match review, anomaly investigation |
| **Setup** | First-run and configuration workflows | Onboarding, privacy setup, import wizard |

**Prompt-tool contract:** Prompts specify which tools to call and in what order, but the tools do the work. A prompt never bypasses the privacy middleware — if a prompt calls a medium-sensitivity tool and the user hasn't granted consent, the prompt receives the degraded response and handles it gracefully (acknowledges the limitation, suggests granting consent, continues with what's available).

### Resources

MCP resources are read-only data endpoints that give the AI ambient context without requiring a tool call. They're loaded into context when the AI connects, not on demand.

**Design principles:**

1. **Ambient, not interactive.** Resources provide background context the AI needs to be helpful — schema information, configuration state, available accounts. They don't accept parameters or perform actions.
2. **Small and stable.** Resources should be compact enough to load into context without waste. They change infrequently (account list, schema shape, privacy status).
3. **Bootstrap the AI.** The right set of resources means the AI's first tool call is the right one. Without resources, the AI has to call `overview.status` before it can do anything useful.

**Resources:**

| Resource | Content | Why ambient |
|---|---|---|
| `moneybin://status` | Data freshness, row counts, date ranges per source, last import timestamp | Lets the AI know what data exists without a tool call |
| `moneybin://accounts` | Account list with types, institutions, currencies | Lets the AI reference accounts by name, filter by type |
| `moneybin://privacy` | Active consent grants, configured backend, consent mode | Lets the AI know what it can and can't do before hitting a consent wall |
| `moneybin://schema` | Core table schemas with column descriptions | Lets the AI write accurate SQL in `sql.query` without calling `describe_table` first |

**What's NOT a resource:** Anything that changes frequently (transaction lists, balance snapshots, budget status) or requires parameters (filtered queries). Those are tools.

---

## 7. CLI Symmetry

### Shared service layer

The MCP server and CLI are co-equal consumers of the same service layer. The symmetry is structural, not aspirational — both import the same service modules, call the same methods, and get the same results.

```python
# Service layer (shared)
class SpendingService:
    def summary(self, months: int, account_id: str | None) -> SpendingSummary: ...


# MCP tool (thin wrapper)
@mcp_tool(sensitivity="low")
def spending_summary(months: int = 3, account_id: str | None = None) -> dict:
    service = SpendingService(get_db())
    return service.summary(months, account_id).to_response()


# CLI command (thin wrapper)
@spending_app.command("summary")
def spending_summary_cmd(
    months: int = typer.Option(3), account_id: str | None = None
) -> None:
    service = SpendingService(get_db())
    result = service.summary(months, account_id)
    render_table(result)
```

### What symmetry means in practice

| Concern | MCP | CLI |
|---|---|---|
| **Capabilities** | Identical | Identical |
| **Parameters** | JSON schema with descriptions | Typer options with help text |
| **Output format** | Structured JSON (response envelope) | Human-readable tables, status lines, icons per `cli.md` |
| **Privacy enforcement** | Middleware intercepts tool calls | Same middleware wraps service calls |
| **Error handling** | Structured error in response envelope | `logger.error` + `typer.Exit(1)` |
| **Discoverability** | Tool descriptions + `actions` array | `--help` + command group structure |

### What symmetry does NOT mean

- **Not identical UX.** The CLI uses tables, progress bars, and icons. MCP returns structured data. Same data, different presentation.
- **Not identical invocation.** `moneybin spending summary --months 3` vs `spending.summary(months=3)`. The CLI uses Typer's conventions; MCP uses tool-call conventions.
- **Not a generated surface.** The CLI is hand-crafted for human ergonomics. It's not auto-generated from MCP tool schemas. Both surfaces are independently authored but share the service layer.

### CLI command structure

The CLI mirrors the MCP namespace as command groups:

```
moneybin
+-- spending
|   +-- summary
|   +-- by-category
|   +-- merchants
|   +-- compare
+-- cashflow
|   +-- summary
|   +-- income
+-- accounts
|   +-- list
|   +-- balances
|   +-- details
|   +-- net-worth
+-- transactions
|   +-- search
|   +-- correct
|   +-- annotate
|   +-- recurring
+-- import
|   +-- file
|   +-- status
+-- categorize
|   +-- bulk
|   +-- uncategorized
|   +-- rules
|   +-- auto-review
+-- budget
|   +-- set
|   +-- status
|   +-- summary
+-- tax
|   +-- w2
+-- privacy
|   +-- status
|   +-- grant
|   +-- revoke
|   +-- audit-log
+-- overview
|   +-- status
|   +-- health
+-- data
    +-- transform apply  (existing)
```

### Metadata for AI consumers

Both surfaces carry enough metadata for AI tools (Claude Code, Codex) to use the CLI effectively:

- **CLI help text** mirrors MCP tool descriptions — same language, same parameter documentation.
- **Structured output flag** — `--output json` on any CLI command returns the same response envelope as the MCP tool, enabling AI tools that prefer CLI to parse results programmatically.
- **Exit codes** are consistent and documented: 0 = success, 1 = error, 2 = consent required.

`--output json` means an AI using Claude Code can call `moneybin spending summary --months 3 --output json` and get the exact same response envelope as the MCP tool. No parsing heuristics needed.

---

## 8. MCP Apps Readiness

This spec does not define MCP Apps — that is the immediate follow-on spec. But every tool designed here must be consumable by an MCP App without needing tool changes later.

### Design-for-Apps principles

1. **Structured data, never pre-formatted.** Tools return typed fields (`amount: 1245.67`, `date: "2026-04-15"`) not display strings (`"$1,245.67"`, `"April 15, 2026"`). Formatting is the consumer's job — whether that consumer is an AI composing a text response or an App rendering a chart.

2. **Aggregation-ready responses.** Tools that return time-series data (`spending.summary`, `cashflow.summary`, `accounts.net-worth`) include data in a shape that maps directly to chart axes — arrays of `{period, value}` objects, not prose summaries. An App can render a chart from the response without post-processing.

3. **Currency in metadata, not per-row.** `summary.display_currency` at the top of the response, not `currency: "USD"` on every row. Per-row currency fields only when the response contains mixed unconverted currencies (see section 4).

4. **Field names chosen deliberately.** Field names in response schemas will become the API contract at 1.0. Before 1.0, breaking changes are acceptable and handled by the migration playbook. No versioning overhead prematurely — but names are chosen with care because they'll stick.

5. **Summary metadata enables adaptive rendering.** The `summary` section (`total_count`, `has_more`, `period`, `degraded`) gives an App enough information to decide how to render: show a "load more" control, display a "limited data" banner, or switch from detail to aggregate view.

### What the MCP Apps spec will own

The follow-on MCP Apps spec will define:

- Which tools get App companions (spending dashboard, import wizard, portfolio when investments exist)
- HTML/JS rendering approach (inline vs. resource-served, chart library choice)
- App manifest format and client compatibility testing
- Interaction model (read-only charts vs. write-back actions)
- Fallback behavior when the MCP client doesn't support Apps

This spec's job is to ensure that when the Apps spec arrives, it finds a tool surface that's ready to consume. The Apps spec is planned as the immediate follow-on to ship a proof-of-concept App MVP as soon as the tool surface is implemented.

---

## 9. Dependencies & Foundational Work

### Specs this depends on (consumed, not defined)

| Dependency | What this spec needs from it | Status |
|---|---|---|
| **Redaction Engine** (new spec) | Field-level masking rules per sensitivity tier, deterministic redaction, reverse-lookup mapping | Not yet written |
| **Consent Management** (new spec) | `app.ai_consent_grants` schema, grant/revoke lifecycle, consent check API | Not yet written |
| **Audit Log** (new spec) | `app.ai_audit_log` schema, logging contract, query API | Not yet written |
| **Provider Profiles** (new spec) | `AIBackend` interface, provider metadata, verified-local detection | Not yet written |
| **AIConfig** (new spec) | `MoneyBinSettings.ai` configuration block, backend selection | Not yet written |
| **Privacy & AI Trust** | Framework spec — the design authority for all of the above | Draft |
| **Transaction Matching** | Provenance schema, `source_type` taxonomy, match review UX | Draft |

### Q1 12-month plan interactions

| Item | Relationship |
|---|---|
| **Schema migration playbook** | New tables (`ai_consent_grants`, `ai_audit_log`, corrections, annotations) need migrations on existing DBs |
| **`get_base_dir` fix** | MCP server uses this to locate the database; must be resolved before MCP v1 ships |
| **Synthetic data** | Testing the new tool surface requires realistic multi-account, multi-source data |
| **Transfer detection** | Affects spending/cashflow tool accuracy — tools should work correctly with and without transfer detection in place |

### Items removed from the 12-month plan

Based on the import-first design philosophy:

| Removed item | Original schedule | Rationale |
|---|---|---|
| **Manual transaction entry** | Q1 Month 2 | Import-first philosophy. Transactions come from sources, not manual data entry. Corrections and annotations are metadata on source-imported records, not counter-entries or phantom records. Cash spending is annotated on the ATM withdrawal, not double-counted as new transactions. |
| **Split transactions** | Q1 Month 2 | Deferred past MVP. Legacy accounting pattern for allocating one transaction across multiple categories. Not needed for MoneyBin's target segments with traditional budgeting. Revisit only if envelope budgeting is added. |

These decisions and their rationale should be documented in the 12-month plan.

### New concepts this spec introduces

| Concept | Description |
|---|---|
| **Transaction corrections** | First-class override mechanism in the prep layer — amount, date, description corrections as metadata on source-imported records, not counter-entries |
| **Transaction annotations** | Tags, notes, cash breakdowns as metadata on existing transactions |
| **Privacy middleware** | Shared infrastructure between MCP and CLI for sensitivity enforcement, consent checking, audit logging, and response filtering |
| **Service layer formalization** | Explicit shared services consumed by both MCP and CLI, returning typed Python objects |
| **Response envelope** | Consistent `{summary, data, actions}` shape across all tools |
| **Sensitivity declarations** | Static per-tool sensitivity tier driving automatic privacy enforcement |
| **Progressive disclosure** | Namespace-based tool registration with `moneybin.discover` meta-tool; core namespaces at connection, extended on demand via `tools/list_changed` |

---

## 10. Relationship to Existing Specs

### Superseded specs

| Spec | Disposition |
|---|---|
| [`mcp-tier1-tools.md`](mcp-tier1-tools.md) | Superseded. Prototype-era tool list designed before the privacy framework. Tool concepts that survive are redesigned in `mcp-tool-surface.md`. |
| [`archived/mcp-read-tools.md`](archived/mcp-read-tools.md) | Historical record of the prototype. Implementation will be replaced. |
| [`archived/mcp-write-tools.md`](archived/mcp-write-tools.md) | Historical record of the prototype. Implementation will be replaced. |

### Companion specs (planned)

| Spec | Relationship |
|---|---|
| **`mcp-tool-surface.md`** | Concrete tool, prompt, and resource definitions. Consumes this architecture spec. |
| **MCP Apps spec** (name TBD) | First MCP App MVP. Consumes the tool surface. Immediate follow-on to `mcp-tool-surface.md`. |

### Specs that reference or depend on this one

| Spec | How it relates |
|---|---|
| [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) | Defines the privacy framework this spec consumes. MCP field minimization section references tool sensitivity declarations. |
| [`smart-import-overview.md`](smart-import-overview.md) | Pillar F (AI-assisted parsing) uses the same consent/audit infrastructure. Import tools in this spec's surface replace the prototype `import_file` tool. |
| [`matching-overview.md`](matching-overview.md) | Match review tools (`transactions.review-matches`, etc.) will be defined in `mcp-tool-surface.md`. Audit log is shared infrastructure. |

## Resolved Decisions

- **`sql.query` tool.** Kept as a power-user escape hatch with guardrails: read-only validation, file-access function blocking, `MAX_ROWS` cap. Defined in [`mcp-tool-surface.md`](mcp-tool-surface.md) §13.
- **Prompt count.** Four prompts: `monthly-review`, `categorization-organize`, `onboarding`, `tax-prep`. Defined in [`mcp-tool-surface.md`](mcp-tool-surface.md) §14.
- **Service layer packaging.** All services live in `src/moneybin/services/` (flat directory, one file per service class). This directory already exists with `categorization_service.py` and `import_service.py`. New services (`spending_service.py`, `transaction_service.py`, etc.) follow the same pattern. Revisit if adding major new domains makes the flat structure unwieldy.
- **Privacy middleware implementation.** Decorator-based (`@mcp_tool(sensitivity="medium")`) that delegates to a middleware class. Decorator for ergonomics at the tool definition site; class for testability of the consent/audit/redaction logic.
- **Tool count strategy.** Progressive disclosure via namespace-based registration, not tool consolidation. The full surface (46+ tools) exceeds practical limits for AI tool selection (~20-30 tools). Consolidation (merging CRUD operations into action-parameter tools) was rejected because it trades tool count for schema complexity without reducing cognitive load on the model. Instead: core namespaces (~19 tools) registered at connection time, extended namespaces loaded on demand via `moneybin.discover` meta-tool and `tools/list_changed` notification. Each tool stays clean and single-purpose. See §3 "Progressive disclosure via namespace registration" and [`mcp-tool-surface.md`](mcp-tool-surface.md) §15b.
