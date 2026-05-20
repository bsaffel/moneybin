# MCP Tool Surface

> Last updated: 2026-05-17
> Status: in-progress
> Companion to: [`mcp-architecture.md`](mcp-architecture.md) (design philosophy, conventions, patterns)
> Supersedes: [`archived/mcp-read-tools.md`](archived/mcp-read-tools.md), [`archived/mcp-write-tools.md`](archived/mcp-write-tools.md)

## Purpose

This spec defines every concrete tool, prompt, and resource in MoneyBin's MCP surface, along with the service layer methods and CLI commands that back them. It is the "what we're building" companion to `mcp-architecture.md` (the "how we think about it" document).

## How to read this spec

- **Section 1 (Conventions)** summarizes cross-cutting patterns defined in `mcp-architecture.md`. Read that spec first for full context; this section is a quick reference, not a restatement.
- **Section 2 (Exemplars)** shows three tools in full detail — service method, MCP tool, CLI command, response shape, degraded response — to prove the patterns work end-to-end.
- **Sections 3-13 (Namespaces)** define every tool with signature and behavior. Patterns shown in the exemplars are not repeated.
- **Section 14 (Prompts)** defines goal-oriented workflow templates.
- **Section 15 (Resources)** defines ambient context endpoints.
- **Sections 16-17 (`sync_*`, `transform_*`)** document the provider-sync and pipeline namespaces.
- **Section 17c (Dependency tracker)** tracks which tools are blocked by unbuilt subsystems.

## Status

in-progress

> **Naming convention:** path-prefix-verb-suffix (`accounts_balance_assert`). Cross-domain analytical views live under `reports_*` (§3, §4, §12); sync and transform are MCP-exposed (§16, §17). The unified taxonomy is shared with [`moneybin-cli.md`](moneybin-cli.md). See `.claude/rules/surface-design.md` for the operation-shape taxonomy and verb vocabulary the surface is built from.

---

## 1. Conventions (quick reference)

Full definitions in [`mcp-architecture.md`](mcp-architecture.md). This section is a lookup aid.

### Response envelope

Every tool returns:

```json
{
  "status": "ok",
  "summary": {
    "total_count": 247,
    "returned_count": 50,
    "has_more": true,
    "period": "2026-01 to 2026-04",
    "sensitivity": "medium",
    "display_currency": "USD"
  },
  "data": [ ... ],
  "actions": ["Use reports_spending(category=\"<name>\") to drill into one category"]
}
```

`status` is always present: `"ok"` when the tool succeeded, `"error"` when it failed. On failure, an `error` field is also present:

```json
{
  "status": "error",
  "summary": {"total_count": 0, "returned_count": 0, "has_more": false, "sensitivity": "low", "display_currency": "USD"},
  "data": [],
  "actions": [],
  "error": {"message": "Database is locked", "code": "database_locked", "hint": "Run `moneybin db unlock`"}
}
```

This contract applies to all MCP tool responses and to CLI `--output json` responses on commands that use `render_or_json`; remaining CLI commands adopt the envelope shape incrementally.

Write tools return a result object in `data` (not an array). Currency lives in `summary.display_currency`, not per-row.

### Shared parameters

These apply to all tools that accept them and are not repeated per tool:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_date` / `end_date` | `str?` | — | ISO 8601 date range |
| `months` | `int?` | — | Recent months lookback (overridden by explicit dates) |
| `account_id` | `list[str]?` | — | Filter to specific accounts |
| `limit` / `offset` | `int` | varies / `0` | Pagination |
| `detail` | `str` | `"standard"` | `summary` (aggregates), `standard` (default), `full` (all fields) |

### Sensitivity tiers

| Tier | Data | Consent |
|---|---|---|
| `low` | Aggregates, counts, metadata | None |
| `medium` | Row-level: descriptions, amounts, dates | `mcp-data-sharing` |
| `high` | Critical PII (account numbers, SSN-adjacent) | `mcp-data-sharing` + always masked for cloud |

`detail=summary` on a `medium` tool returns aggregates without triggering consent. Degraded responses use the same envelope with `summary.degraded: true`.

### Namespace conventions

Path-prefix-verb-suffix. Tool names mirror the CLI hierarchy with underscores instead of spaces, ending in an explicit verb.

- **Pattern:** `<entity_or_domain>[_<sub_resource>]_<verb>`
- **Examples:** `accounts`, `accounts_balances`, `accounts_balance_assert`, `reports_networth`, `transactions_matches_confirm`, `reports_spending`
- **Verbs:** noun-only for collection / summary / aggregate / time-series reads (shape 5 of `.claude/rules/surface-design.md`); `_get` is reserved for single-entity-by-id reads (`accounts_get(account_id)`). `transactions_get` is a defended exception: filtered/paginated collection query rather than single-entity — the name was kept for "fetch the transactions I care about" intent over strict shape conformance; documented inline in its description. Also: `_assert`, `_confirm`, `_reject`, `_delete`, `_create`, plus domain-natural verbs (`_reconcile`, `_run`, `_train`). `_set` for idempotent state assertions (shape 1a/1b). `_list` is forbidden on read tools.
- **Pluralization:** singular for single-entity reads (`accounts_get`); noun-only for collection reads, pluralized to match the noun (`accounts`, `accounts_balances`, `merchants`, `system_audit`); singular for sub-resources inside compound names (`balance`, `networth`, `category`); plural for relationship collections (`matches`).
- **Encoding constraint:** lowercase ASCII with underscores, ≤64 chars (`^[a-zA-Z0-9_-]{1,64}$` per Anthropic and OpenAI MCP client regex).

This mirrors the CLI taxonomy in `moneybin-cli.md` v2. A user who knows `accounts balance list` already knows `accounts_balances` and `GET /accounts/balances`.

### Service layer convention

Each namespace maps to a service class. Tools and CLI commands are thin wrappers — parameter validation and output formatting only, no business logic or SQL. Service methods return typed Python objects (dataclasses or Pydantic models).

### CLI convention

CLI mirrors MCP namespaces as command groups. `--output json` on any command returns the same response envelope as the MCP tool. Default output is human-readable (tables, summary lines, icons per `cli.md` rules).

### When CLI-only is justified

Default: every operation is MCP-exposed. CLI-only status requires a justified exception. Acceptable exceptions are narrow:

1. **Secret material through the LLM context window.** Tools that accept passphrases or encryption keys (`db_unlock`, `db_rotate_key`, `sync_rotate_key`, `db_init`) — passing those through an LLM-mediated channel is a security model violation, not a capability gap.
2. **Hands-on operator territory.** Bootstrapping, troubleshooting, and developer-tooling operations that require physical operator presence (`db_init`, `db_lock`, `db_ps`, `db_kill`, `db_migrate apply/status`, `db_shell`, `db_ui`, `mcp_serve`, `mcp_config_*`, `profile_*`, `transform_restate`). MCP can't even start if the database is locked, so exposing recovery/lifecycle tools to MCP would be meaningless.

What is NOT a valid CLI-only justification:
- "Long-running" — MCP supports progress notifications.
- "Needs OAuth / browser" — tools can return redirect URLs; clients open them.
- "Destructive" — use a `confirm` parameter or elicitation; the AI must obtain explicit user agreement.
- "Interactive" — split into a list-tool (read) and act-tools (write); the AI orchestrates the loop.
- "Writes to scheduler / filesystem" — server has filesystem access; routine.

Apply this filter when adding any new tool. The default answer is "expose to MCP."

### Server `instructions` field

The MCP `initialize` response includes an `instructions` string that compatible clients inject into the LLM's system prompt at session start. The canonical text lives in `src/moneybin/mcp/server.py` (the `FastMCP(instructions=...)` argument) — this spec describes what it should *cover*, not the literal text.

Required content:
- One-line product description (local-first, on-device DuckDB)
- Top-level group enumeration with brief domain hint (entity groups, reference data, reports, system, pipeline, privacy)
- Naming convention reminder (path-prefix-verb-suffix, with 2–3 examples)
- Orientation pointers — which tool to call to "get oriented" (`system_status` for data status; `transactions_review` for review queues; `reports_networth` + `reports_spending` for a quick financial pulse)
- Response envelope shape (`{summary, data, actions}`) and pagination convention
- Batch-tool preference
- Sensitivity tiers and degraded-response behavior

Length budget: ~150–300 tokens. The text is loaded once per session, so the cost is amortized — but it competes with conversation and tool descriptions for working memory.

Keep the text in sync with the spec. Renames and new top-level groups must update both.

### Protocol-standard capability coverage matrix

MoneyBin layers its own primitives (sensitivity tiers, `ResponseEnvelope`, exposure principle) on top of the standard MCP capabilities. Both layers are first-class — clients consume the protocol-standard fields directly for confirmation UI, capability negotiation, and context injection, while MoneyBin's layer governs data exposure. **Every PR that adds tools, prompts, or resources is reviewed against this matrix; reject changes that ship a new surface without explicitly accounting for each capability.**

The matrix is intentionally exhaustive — including capabilities MoneyBin defers — so deferrals are conscious, not silent.

| Capability | MCP spec status | MoneyBin status | Notes / next action |
|---|---|---|---|
| **Tools** | core | ✅ shipped | ~19 core + extended namespaces; v2 surface in this spec |
| **Tool `annotations`** (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) | core (added 2025) | ✅ shipped | Emitted by `@mcp_tool` via `mcp.types.ToolAnnotations` in `src/moneybin/mcp/_registration.py`. Clients use these for confirmation UI; MoneyBin's sensitivity tiers complement them, not substitute. |
| **Prompts** | core | ✅ shipped | Registered via `@mcp.prompt()` in `src/moneybin/mcp/prompts.py`; surfaced via FastMCP. Add new prompts when a workflow is repeatable and benefits from a templated agent path. |
| **Resources** | core | ✅ shipped | Registered via `@mcp.resource(...)` in `src/moneybin/mcp/resources.py`, including the curated `moneybin://schema` resource. Pattern is established; extend for any read-only context that benefits from URI addressing (docs, schema docs, error-code catalog, BQL-style references if added). |
| **Resource templates** | core | ⏳ deliberate defer | Use direct URIs only today. Revisit if/when a parameterized resource (e.g. `moneybin://account/{id}/summary`) is genuinely cheaper than an equivalent tool. |
| **`tools/list_changed` notifications** | core | ⏳ retired strategy | Originally wired for client-driven progressive disclosure; that approach was retired 2026-05-17 because client support is too uneven (Claude Desktop unreliable, most generic clients ignore). Notification capability stays available on the FastMCP server for any future dynamic-surface needs. See `mcp-architecture.md` §3 "Tool disclosure: full surface, taxonomy-led". |
| **`resources/list_changed` notifications** | core | ⏳ not used | Resource set is static today. If `moneybin://schema` ever becomes dynamic per-session, wire this. |
| **`prompts/list_changed` notifications** | core | ⏳ not used | Prompt set is static today. |
| **Progress notifications** | core | ⏳ not used | Required for the job-handle pattern; `sync-plaid.md` rewrite is the first surface that needs it. |
| **Sampling** | optional | ⏳ deliberate defer | Server requesting LLM completions back from the client. No current use case; consider only if MoneyBin needs to delegate categorization to the host LLM (today this is an explicit non-goal — categorization runs locally). |
| **Roots** | optional | ⏳ deliberate defer | Filesystem scope advertisement from the client. Inbox-watching uses MoneyBin-side configuration today. Revisit if a future tool needs to ask the agent for the user's working directory. |
| **Elicitation** | optional | ⏳ deliberate defer | Server requesting structured user input mid-tool-call. Spec mentions it as an alternative to `confirm` parameters for destructive ops; we use the parameter pattern for now. Revisit when destructive write-tool count > 5. |
| **Logging level negotiation** | optional | ⏳ not used | Server-side log level honors `MoneyBinSettings.logging.level`; not negotiated per session. |
| **Pagination cursors** | core | ⏳ partial | `summary.has_more` + `summary.total_count` flag truncation; `offset` parameter handles paging. No opaque cursor pattern; revisit if any tool needs server-side iteration state. |
| **Server `instructions`** | core | ✅ shipped | `FastMCP(instructions=...)` in `src/moneybin/mcp/server.py`. See above subsection. |
| **MCP capability negotiation** | core | ✅ shipped via FastMCP | FastMCP handles `initialize` capabilities; MoneyBin doesn't negotiate per-capability flags today. |

**Discipline:** when a competitor review or MCP spec evolution surfaces a capability not in this table, add it before deciding what to do — the table itself is the audit trail. PRs that add a row marked `⏳ deliberate defer` must include the rationale; PRs that flip a row from `⏳` to `📐` or `✅` must update the relevant subsection of this spec.

#### Decorator: protocol annotations

The `@mcp_tool` decorator in `src/moneybin/mcp/decorator.py` accepts MoneyBin's `sensitivity` and `domain` keyword arguments today, plus MCP-standard `annotations` kwargs as of this PR.

**Implemented signature**:

```python
@mcp_tool(
    sensitivity="medium",
    read_only=False,        # MCP readOnlyHint — default True for sensitivity=low queries
    destructive=True,       # MCP destructiveHint — irreversible state change
    idempotent=False,       # MCP idempotentHint — safe to retry without side effects
    open_world=False,       # MCP openWorldHint — defaults to False (closed-world)
    # max_items=N           # Per-tool override of MCPSettings.max_items (see §Collection size cap)
)
def transactions_correct(...) -> ResponseEnvelope: ...
```

The decorator emits these as MCP `tool.annotations` so compatible clients can render confirmation UI (Claude Desktop's "are you sure" dialog, IDE-style permission prompts) without server-side dance. Defaults assume read-only retrieval (`read_only=True`, `destructive=False`, `idempotent=True`, `open_world=False`); writes opt out explicitly.

**Persistence — both wrapper attrs and FastMCP `ToolAnnotations`:**

- The decorator stores each flag as a wrapper attribute (`_mcp_read_only`, `_mcp_destructive`, `_mcp_idempotent`, `_mcp_open_world`) parallel to the existing `_mcp_sensitivity` and `_mcp_domain`. This keeps the metadata reachable from Python introspection (tests, audit tooling, future structural checks) without coupling to FastMCP's internal registry.
- `register()` in `src/moneybin/mcp/_registration.py` reads those attrs, builds a `fastmcp.tools.tool.ToolAnnotations` (or equivalent dict), and passes it via `mcp.tool(annotations=...)`. This is what reaches the wire — clients consume it from `tools/list` for confirmation UI.

Both surfaces are required: wrapper attrs alone never reach clients; FastMCP `annotations` alone are unreachable from Python tests except via private FastMCP APIs.

This is *complementary to* sensitivity tiers, not redundant — sensitivity describes data exposure (what the tool returns and to whom); annotations describe action class (what the tool does to state). Both are required; clients consume each layer differently.

#### Tool descriptions: invariants must be in the description, not just the rule files

The MCP description string is the only schema-attached prose the agent sees. Invariants the agent needs to apply correctly — accounting sign convention, decimal precision, ID-composite requirements on destructive ops, source-system value sets — must be stated in the tool description, not only in `.claude/rules/database.md` or `AGENTS.md`. Those files are for human contributors; the agent never sees them.

Required content in tool descriptions for any tool that:
- accepts an `amount` field — state the sign convention ("negative = expense, positive = income; transfers exempt"), the decimal precision (`DECIMAL(18,2)` for money), and the date format (`DATE`)
- mutates state — state the exact reversibility story ("this update writes to `app.*` and is recorded in `app.audit_log`; revert via …") and any required ID composites
- returns currency-bearing data — state that `amount` is in the currency named by the paired currency column, never inferred from context (per `architecture-shared-primitives.md` Invariant 7)

Audit pass required against the current tool surface; no new tool ships without this content. See [§Tool description audit](#tool-description-audit) work item.

#### Collection size cap

Tools that accept list-typed parameters obey a server-enforced upper bound on each list's length. The analog for reads is `MoneyBinSettings.mcp.max_rows` (default 1000); list inputs get a parallel cap that prevents unbounded write batches and oversized query inputs alike.

- **Setting:** `MoneyBinSettings.mcp.max_items` (default 500). Read at call time so test monkeypatching works.
- **Mechanism:** at decoration time, `@mcp_tool` walks `inspect.signature(fn).parameters` and records every parameter annotated as `list[X]` / `Sequence[X]` / `tuple[X, ...]`. At call time, each such parameter's length is checked against the cap before the tool body runs. `Collection`/`dict`/`set` are deliberately excluded — `len()` on a dict returns key-count, not item-count, so cap-checking them would surface confusing `too_many_items` errors.
- **Per-tool override:** `@mcp_tool(..., max_items=N)` sets a tool-specific cap. `max_items=None` disables the cap entirely (must be justified in the docstring). Default is to inherit from `MCPSettings.max_items`.
- **Error path:** exceeding the cap on any list parameter returns `ResponseEnvelope.error` with `code="too_many_items"` and `details={"limit": <cap>, "received": <N>, "parameter": <name>}`; never partial-success. The agent's retry logic uses `details.parameter` to know which list to chunk.
- **Empty lists** are not a cap violation. Tools that need to reject empty input handle that themselves (an empty list is sometimes a meaningful query, e.g., "no filters").
- **No opt-in flag.** Every tool with a list-typed parameter is capped implicitly; the cost of the per-tool override is the right place to disambiguate when (rarely) the default is wrong. Eliminating the explicit flag keeps the call-site API minimal and makes "did the author remember to enable it?" a non-question.

Reference: `lunchmoney-mcp` ships 1–500 caps on its bulk writes (`update_transactions_bulk`, `delete_transactions_bulk`); the cap convention here generalizes that to any list-typed parameter and removes the awkward "bulk vs single" distinction. MoneyBin's tool-name surface drops the "bulk" qualifier — see the §`transactions_categorize_commit` exemplar for the post-rename shape. Internal helpers (e.g., `BulkCategorizationResult` → `CategorizationResult`) carry the same rename; the sweep is part of the same PR that introduces this cap convention.

---

## 2. Exemplars

These three tools demonstrate every pattern in full detail. Subsequent namespace sections use a compact format and reference these for shared patterns.

### 2.1 `reports_spending` — low sensitivity, monthly trend with deltas

**Service layer**

```python
class ReportsService:
    def spending_trend(
        self,
        *,
        from_month: str | None,
        to_month: str | None,
        category: str | None,
        compare: str,
    ) -> tuple[list[str], list[tuple]]: ...
```

Reads `reports.spending_trend` (SQLMesh view). Returns `(columns, rows)` over a window of months with MoM, YoY, and 3-month-trailing deltas.

**MCP tool**

- **Name:** `reports_spending`
- **Description:** "Monthly spending trend with MoM, YoY, and 3-month-trailing deltas. Defaults to the last 12 calendar months."
- **Sensitivity:** `low` — aggregates only.
- **Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `from_month` | `str?` | — | Lower bound (inclusive) as `YYYY-MM` |
| `to_month` | `str?` | — | Upper bound (inclusive) as `YYYY-MM` |
| `category` | `str?` | — | Filter to a specific category text |
| `compare` | `str` | `"yoy"` | Caller-side intent only — the view returns all three comparison columns regardless |

- **Response `data` shape:** rows from `reports.spending_trend` — `{year_month, category, total_spend, txn_count, prev_month_spend, mom_delta, mom_pct, prev_year_spend, yoy_delta, yoy_pct, trailing_3mo_avg}`.
- **Degraded response:** N/A — `low` sensitivity, already aggregates.
- **Actions:** `["Use reports_spending(category=\"<name>\") to drill into one category", "Use reports_cashflow for inflow/outflow/net", "Use reports_recurring to find subscription-like patterns"]`

**CLI command**

```
moneybin reports spending [--from YYYY-MM] [--to YYYY-MM] [--category SLUG] [--compare yoy|mom|trailing] [-o text|json] [-q]
```

Default output is a table with the comparison columns; `--output json` returns the response envelope.

---

### 2.2 `transactions_get` — medium sensitivity, cursor pagination, curation fields

**Service layer**

```python
class TransactionService:
    def get(
        self,
        *,
        accounts: list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        categories: list[str] | None = None,
        amount_min: Decimal | None = None,
        amount_max: Decimal | None = None,
        description: str | None = None,
        uncategorized_only: bool = False,
        limit: int = 50,
        cursor: str | None = None,
    ) -> TransactionGetResult: ...
```

`TransactionGetResult` contains a list of `Transaction` records and an optional `next_cursor` string. `Transaction` includes curation fields (`notes`, `tags`, `splits`) joined from the app schema in `core.fct_transactions`. `accounts` entries are resolved: exact `account_id` matches are used directly; anything else is fuzzy-matched by display name via `AccountService`. Unresolvable entries are silently skipped.

**MCP tool**

- **Name:** `transactions_get`
- **Description:** "Fetch transactions with optional filters and cursor pagination. Returns row-level data including curation fields (notes, tags, splits). Amounts use the accounting convention: negative = expense, positive = income. Amount filter parameters accept decimal strings (e.g., `\"-50.00\"`) to preserve precision."
- **Sensitivity:** `medium` — row-level data (descriptions, amounts, dates).
- **Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `accounts` | `list[str]?` | — | Account IDs or display names (fuzzy-matched) |
| `date_from` | `str?` | — | ISO 8601 start date, inclusive |
| `date_to` | `str?` | — | ISO 8601 end date, inclusive |
| `categories` | `list[str]?` | — | Filter to specific category names |
| `amount_min` | `str?` | — | Minimum amount as decimal string (e.g., `"-50.00"`) |
| `amount_max` | `str?` | — | Maximum amount as decimal string |
| `description` | `str?` | — | ILIKE pattern matched against description and memo |
| `uncategorized_only` | `bool` | `false` | Only rows with no user/AI/rule categorization (`categorized_by IS NULL`) |
| `limit` | `int` | `50` | Max results per page |
| `cursor` | `str?` | — | Opaque pagination token from `next_cursor` in a prior response |

- **Response `data` shape:**

```json
[
  {
    "transaction_id": "abc123def456",
    "account_id": "chase-checking-1234",
    "transaction_date": "2026-04-15",
    "amount": -42.50,
    "description": "WHOLEFDS MKT #10234",
    "source_type": "ofx",
    "category": "Food & Drink",
    "subcategory": "Groceries",
    "tags": ["grocery", "weekly"],
    "notes": null,
    "splits": null
  }
]
```

- **Pagination:** `next_cursor` appears at the top level of the envelope when more pages exist. Pass it back as `cursor` to fetch the next page. Absent when all results fit in one page.
- **Actions:** `["Use transactions_get with the next_cursor value to fetch the next page", "Use reports_spending for category breakdowns", "Use transactions_categorize_commit to categorize uncategorized transactions"]`

**CLI command**

```
moneybin transactions list [--account ID_OR_NAME] [--from DATE] [--to DATE]
                           [--category NAME] [--amount-min N] [--amount-max N]
                           [--description PATTERN] [--uncategorized]
                           [--limit 50] [--cursor TOKEN] [--output text|json]
```

Default output is a table with date, description, amount, category, and account columns. `--output json` returns the response envelope. `--account` is repeatable and resolves by account ID or fuzzy display-name match.

---

### 2.3 `transactions_categorize_commit` — write tool, batch semantics, paired read tool

**Service layer**

```python
class CategorizationService:
    def categorize_items(
        self,
        items: list[CategorizationItem],
    ) -> CategorizationResult: ...
```

`CategorizationItem` is a Pydantic model (`src/moneybin/services/categorization/_shared.py`): `transaction_id`, `category`, `subcategory?`, `canonical_merchant_name?`. `CategorizationResult` contains applied/skipped/error counts and a list of error details.

The matcher auto-creates exemplar-only merchant mappings from each row's normalized `match_text` so future rows with the same `match_text` categorize automatically via the merchant matcher. When `canonical_merchant_name` is provided, multiple rows with different `match_text` values merge under one merchant identity by appending exemplars rather than spawning per-row merchants.

**MCP tool**

- **Name:** `transactions_categorize_commit`
- **Description:** "Commit externally-decided categorizations for a batch of transactions. Typical caller: an LLM that received redacted rows from `transactions_categorize_assist`, proposed categorizations, the user reviewed, and the LLM now persists the accepted decisions."
- **Sensitivity:** `medium` — reads transaction descriptions and persists category writes.
- **Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `items` | `Sequence[Mapping[str, str \| None]]` | (required) | List of dicts with `transaction_id`, `category`, optional `subcategory`, optional `canonical_merchant_name`. Validated through `CategorizationItem` at the boundary. |

- **Response `data` shape:**

```json
{
  "applied": 48,
  "skipped": 0,
  "errors": 2,
  "error_details": [
    {"transaction_id": "tx_xyz", "reason": "Transaction not found"},
    {"transaction_id": "tx_abc", "reason": "Category 'Foo' does not exist"}
  ],
  "merchants_created": 12
}
```

Note: for write tools, `data` is a result object, not an array. The envelope still wraps it — `summary.total_count` reflects the input list size.

- **Degraded response:** Write tools require consent unconditionally. If consent is not granted, the tool returns an error-style envelope with `summary.degraded: true` and an action pointing to the consent grant command. No partial execution.
- **Actions:** `["Use transactions_categorize_rules to review auto-created rules", "Use transactions_categorize_pending to fetch the next batch"]`

**CLI command**

```
moneybin transactions categorize commit [--input PATH | -] [--output json]
```

The CLI accepts a JSON file via `--input PATH` or stdin (`-`); batch data doesn't fit as flags. Default output is a summary line: "Applied 48, skipped 0, errors 2, merchants created 12."

---

## 3. `reports_spending` — Monthly trend with deltas

*Exemplar — see §2.1 for full pattern. Catalog entry:*

**Service class:** `ReportsService`

- **Name:** `reports_spending`
- **Sensitivity:** `low`
- **Parameters:** `from_month`, `to_month`, `category`, `compare` (see §2.1).
- **Behavior:** Reads `reports.spending_trend` over the window; returns the MoM/YoY/trailing-3mo delta columns.
- **CLI:** `moneybin reports spending` (flags per §2.1).

> Category breakdown is reached via `reports_spending(category="<name>")`. Cross-period comparison is reached by widening the window — every row carries MoM and YoY deltas by default. The earlier `_by_category` / `_compare` / `_merchants` sub-tools were folded into this single tool + `reports_merchants` (see §12).

---

## 4. `reports_cashflow` — Monthly inflow / outflow / net

**Service class:** `ReportsService`

- **Name:** `reports_cashflow`
- **Sensitivity:** `low`
- **Parameters:** `from_month`, `to_month`, optional account / category filters per the shipped signature.
- **Behavior:** Reads `reports.cash_flow` (SQLMesh view). Returns `{year_month, account_id, account_name, category, inflow, outflow, net, txn_count}` rows. Transfers are excluded (intra-portfolio movement, not cash flow); archived accounts are excluded.
- **CLI:** `moneybin reports cashflow [--from YYYY-MM] [--to YYYY-MM] [--by account|category|account-and-category]`.

> Income breakdown is reached by filtering `inflow > 0` and grouping by `category` or by `description` via `reports_merchants` (§12).

---

## 5. `accounts.*` — Account management and per-account workflows

**Service class:** `AccountService`

### `accounts`

List all known accounts with type and institution.

- **Sensitivity:** `low` — account metadata only, no balances or numbers.
- **Unique parameters:** None.
- **Behavior:** Returns array of `{account_id, account_type, institution_name, source_type, currency}`. No pagination — account count is always small.
- **Service:** `AccountService.list() -> list[Account]`
- **CLI:** `moneybin accounts list`

### `accounts_balances`

Most recent balance for each account.

- **Sensitivity:** `medium` — balance amounts are financial data.
- **Unique parameters:** None beyond `account_id` filter.
- **Behavior:** Returns array of `{account_id, institution_name, account_type, ledger_balance, available_balance, as_of_date}`. Degraded response returns total across all accounts without per-account breakdown.
- **Service:** `AccountService.balances() -> list[AccountBalance]`
- **CLI:** `moneybin accounts balance show`

### `accounts_get`

Full account details including routing/account numbers.

- **Sensitivity:** `high` — contains critical PII fields.
- **Unique parameters:** `account_id: str` (required — single account, not a list; this is an exception to the batch-first principle because requesting full PII details for multiple accounts in one call is not a natural workflow and would complicate audit logging).
- **Behavior:** Returns single account object with all fields including masked `routing_number` and `account_number` (e.g., `...1234`). Unmasked only in verified-local mode with `LOCAL_UNMASK_CRITICAL`. Degraded response returns the `accounts` view for that account (metadata only).
- **Service:** `AccountService.details() -> AccountDetail`
- **CLI:** `moneybin accounts get <account_id>`

### `accounts_resolve`

Free-text → account-ID resolution for natural-language references.

- **Sensitivity:** `low` — returns account metadata only (display name, type, masked institution); no balances, no PII.
- **Annotations:** `read_only=True`, `idempotent=True`, `destructive=False`, `open_world=False` — all decorator defaults; no override needed.
- **Unique parameters:** `query: str` (the free-text fragment, e.g. "my Chase account", "checking", "schwab brokerage"); `limit: int = 5` (max alternatives returned).
- **Behavior:** Returns the best-match account plus alternatives, with confidence scores. Implemented via fuzzy match (`difflib.SequenceMatcher` already used for tabular account matching) over `display_name`, `account_subtype`, and `institution_name` from `core.dim_accounts`. Empty result returns `data=[]` with a `low_confidence` action hint; top match below a low-confidence threshold gets an action hint to verify with the user.
- **Why this exists:** without it, every conversation that starts with a natural account reference takes three turns — `accounts` → agent scans the result → agent picks an `account_id`. Single-tool resolution collapses that to one call. Agent ergonomic primitive, not a new capability.
- **Service:** `AccountService.resolve(query: str, limit: int = 5) -> list[AccountResolution]` in `src/moneybin/services/account_service.py`. The `AccountResolution` frozen dataclass (fields: `account_id`, `display_name`, `account_subtype`, `institution_name`, `confidence: float`, plus `to_dict()`) lives in the same module — convention parallel to other service-typed return objects. The MCP tool wraps results via `build_envelope(data=[r.to_dict() for r in resolutions], sensitivity="low", actions=[...])`.
- **CLI:** `moneybin accounts resolve "<query>"` (`--output json` returns the same envelope)
- **Reference:** `agigante80/actual-mcp-server` ships an analogous `actual_get_id_by_name` for the same agent-ergonomic reason.

### `reports_networth`

Net worth across all accounts over time.

- **Sensitivity:** `medium` — aggregate but reveals total financial position.
- **Unique parameters:** None beyond shared date/months conventions.
- **Behavior:** Returns time-series `{period, total_assets, total_liabilities, net_worth}` based on balance history. Requires balance data from OFX or Plaid. Degraded response returns latest snapshot only, no history.
- **Service:** `AccountService.net_worth() -> NetWorthSeries`
- **CLI:** `moneybin accounts net-worth`

### `accounts_set`

Partial-update entry point for an account's settings.

- **Sensitivity:** `medium` — settings changes are non-financial but reveal account metadata.
- **Unique parameters:** `account_id: str` (required), plus optional structural fields (`account_subtype`, `holder_category`, `currency`, `credit_limit`, `last_four`, `official_name`) and behavioral fields (`display_name`, `include_in_net_worth`, `is_archived`).
- **Behavior:** Shape-1b partial update — only supplied fields change. Archiving an account atomically cascades `include_in_net_worth=False`. Single entry point for every structural and behavioral field on an account.
- **Mutation surface:** writes `app.account_settings`. Revert via a follow-up `accounts_set` with the prior values (audit trail in `app.audit_log`).
- **Service:** `AccountService.settings_update() -> AccountSettings`
- **CLI:** `moneybin accounts set <account_id> [--display-name TEXT] [--include/--exclude] [--archive/--unarchive] [--subtype TYPE] [--holder-category CAT] [--currency CODE] [--credit-limit N] [--last-four DIGITS] [--official-name TEXT] [--clear-display-name] [--clear-currency] [--yes]`

---

## 6. `transactions.*` — Transaction-level operations (matches and categorize workflows nested)

**Service class:** `TransactionService` (search, correct, annotate), `MatchService` (matches sub-domain)

### `transactions_get`

Primary transaction read tool. Returns full transaction records with curation metadata.

- **Sensitivity:** `medium`
- **Parameters:** `accounts: list[str]?`, `date_from: str?`, `date_to: str?`, `categories: list[str]?`, `amount_min: str?` (decimal string e.g. `"-50.00"`), `amount_max: str?` (decimal string), `description: str?`, `uncategorized_only: bool = false`, `limit: int = 50`, `cursor: str?`
- **Behavior:** Reads from `core.fct_transactions`. `accounts` accepts exact account IDs or display names (resolved internally). `cursor` is an opaque pagination token from `next_cursor` in a previous response. Returns `list[Transaction]` with optional `notes`, `tags`, `splits` fields.
- **Sign convention:** negative = expense, positive = income.
- **Service:** `TransactionService.get() -> TransactionGetResult`
- **CLI:** `moneybin transactions list`
- **read_only:** true

### `transactions_review`

Orientation tool: pending counts across the review queues.

- **Sensitivity:** `low` — counts only.
- **Unique parameters:** None.
- **Behavior:** Returns `{matches_pending: int, categorize_pending: int, total: int}` so the agent can answer "anything to review?" in one call. The agent drills into `transactions_categorize_pending` for categorization items. Match-item review currently routes to CLI (`moneybin transactions review --type matches`) — the `transactions_matches_*` MCP namespace is blocked on `matching-overview.md` registration; the response envelope's `actions[]` surfaces that CLI hint until those tools register.
- **Service:** `TransactionService.review_counts() -> ReviewCounts`
- **CLI:** `moneybin transactions review`

### Curation tools (`transactions_create`, notes / tags / splits, `import_labels_set`, `system_audit`)

The v1 `transactions_correct` / `transactions_annotate` design was replaced by the curation taxonomy. Source records remain immutable; curation adds parallel mutable metadata. See [`transaction-curation.md`](transaction-curation.md) for the umbrella spec.

#### `transactions_create`

Create 1..100 manual transactions atomically under one `import_id`.

- **Sensitivity:** `medium`
- **Unique parameters:** `transactions: list[object]` (required) — each `{account_id, amount, transaction_date, description, merchant_name?, memo?, payment_channel?, transaction_type?, check_number?, currency_code?, category?, subcategory?}`. Amounts use the accounting convention: negative = expense, positive = income; transfers exempt.
- **Behavior:** Whole batch shares one `import_id` (returned as `batch_id`). Validation runs over the full batch before any insert — a single bad row aborts the whole batch. Rows are exempt from the matcher.
- **Mutation surface:** writes to `raw.manual_transactions` with a generated `import_id`. Revert via `import_revert(import_id=batch_id)`.
- **Service:** `TransactionService.create_manual_batch()`
- **CLI:** Per `.claude/rules/surface-design.md` "parity is functional, not nominal" — MCP exposes the batch shape `transactions_create(transactions=[...])`. CLI ships single-row imperative: `moneybin transactions create AMOUNT DESCRIPTION --account ID [--date YYYY-MM-DD] [--merchant NAME] [--memo TEXT] [--category SLUG] [--subcategory SLUG]`. Agents that need batch entry call the MCP tool; humans add one row at a time at the shell. A future `--file BATCH.json` flag could mirror the MCP shape — tracked as a follow-up.

#### `transactions_notes_add` / `transactions_notes_edit` / `transactions_notes_delete`

Lifecycle on per-transaction notes. Each note has its own `note_id`.

- **Sensitivity:** `medium` (add/edit), `medium` w/ `destructive=True` (delete).
- **Parameters:** `_add(transaction_id, text)`, `_edit(note_id, text)`, `_delete(note_id)`.
- **Mutation surface:** `app.transaction_notes`. Hard delete — no revert; audit trail in `app.audit_log`.
- **Service:** `TransactionService.add_note() / edit_note() / delete_note()`
- **CLI:** `moneybin transactions notes add|edit|delete ...`

#### `transactions_tags_set`

Shape-1a declarative target-state for one transaction's tags.

- **Sensitivity:** `medium`
- **Unique parameters:** `transaction_id: str`, `tags: list[str]`.
- **Behavior:** Service diffs supplied list against current state and emits one `tag.add` / `tag.remove` per change in a single DuckDB transaction. No paired `_delete` — omit the tag to remove it.
- **Mutation surface:** `app.transaction_tags`. Revert via another `_set` with the prior list (audit trail in `app.audit_log`).
- **Service:** `TransactionService.set_tags()`
- **CLI:** Per "parity is functional, not nominal" — MCP exposes shape-1a `transactions_tags_set(transaction_id, tags=[...])` (collection state-set, omission = remove). CLI ships as shape-2 lifecycle ops: `moneybin transactions tags add TRANSACTION_ID TAG`, `transactions tags remove TRANSACTION_ID TAG`, `transactions tags list TRANSACTION_ID`, `transactions tags rename OLD NEW`. Same user outcomes; same underlying `TransactionService.set_tags()` primitive.

#### `transactions_tags_rename`

Global tag rename. Shape-3 discrete-verb (mutates N rows under one parent audit event).

- **Sensitivity:** `medium`
- **Unique parameters:** `old_tag: str`, `new_tag: str`.
- **Behavior:** Emits one parent + N child audit events. Returns `{row_count, parent_audit_id}`.
- **Mutation surface:** `app.transaction_tags` across all rows. Revert via a reverse `_rename`.
- **Service:** `TransactionService.rename_tag()`
- **CLI:** `moneybin transactions tags rename OLD NEW`

#### `transactions_splits_set`

Shape-1a declarative replace of a transaction's splits.

- **Sensitivity:** `medium`
- **Unique parameters:** `transaction_id: str`, `splits: list[{amount, category?, subcategory?, note?}]`. Amounts use the accounting convention.
- **Behavior:** Clears existing splits then adds the new sequence; order preserved.
- **Mutation surface:** `app.transaction_splits`. Revert via another `_set` with prior list.
- **Service:** `TransactionService.set_splits()`
- **CLI:** Per "parity is functional, not nominal" — MCP exposes shape-1a `transactions_splits_set(transaction_id, splits=[...])` (collection state-set). CLI ships as shape-2 lifecycle ops: `moneybin transactions splits add TRANSACTION_ID AMOUNT [--category SLUG]`, `transactions splits list TRANSACTION_ID`, `transactions splits remove SPLIT_ID`, `transactions splits clear TRANSACTION_ID`. Same user outcomes; same underlying primitive.

#### `import_labels_set`

Shape-1a declarative target-state for an import's labels.

- **Sensitivity:** `medium`
- **Unique parameters:** `import_id: str`, `labels: list[str]`.
- **Behavior:** Computes diff against prior labels; emits one `import_label.add` / `import_label.remove` per change.
- **Mutation surface:** `app.import_labels`. Revert via another `_set` with prior list.
- **Service:** `ImportService.set_labels()`
- **CLI:** Per `.claude/rules/surface-design.md` "parity is functional, not nominal" — MCP exposes shape 1a `import_labels_set(import_id, labels=[...])` (collection state-set, omission = delete). CLI ships as shape 2 lifecycle ops: `moneybin import labels add IMPORT_ID LABEL`, `moneybin import labels remove IMPORT_ID LABEL`, `moneybin import labels list IMPORT_ID`. Same user outcomes reachable on both surfaces; the underlying `ImportService.set_labels()` is the shared primitive.

#### `system_audit`

List audit events with filters.

- **Sensitivity:** `medium` — actor/action/target metadata.
- **Unique parameters:** `filters: {actor?, action_pattern?, target_table?, target_id?, from?, to?}`, `limit: int = 100`.
- **Behavior:** Returns audit events newest-first. `action_pattern` accepts SQL `LIKE` patterns (e.g. `tag.%`). For the full chain of one event, use CLI `moneybin system audit show <audit_id>`.
- **Service:** `AuditService.list_events()`
- **CLI:** `moneybin system audit list [...]`

### `transactions_matches.*` — Transaction matching sub-domain

**Status:** blocked on [`matching-overview.md`](matching-overview.md) registration — all six `transactions_matches_*` tools below are documented for design alignment but NOT registered on the MCP surface today. See §17c dependency tracker.

Match review is a distinct workflow within the transactions domain. These tools operate on match proposals — pairs of transactions that the matching engine believes represent the same real-world event (dedup) or two sides of a transfer.

**Service class:** `MatchService`

**Dependency:** All `transactions_matches.*` tools depend on the transaction matching spec (Pillars A+C for dedup, Pillar B for transfers).

#### `transactions_matches_pending`

List match proposals awaiting review.

- **Sensitivity:** `medium` — shows transaction descriptions and amounts from both sides of a proposed match.
- **Unique parameters:** `match_type: str?` (`dedup` or `transfer`), `min_confidence: float?`.
- **Behavior:** Returns array of `{match_id, match_type, confidence, reason, transaction_a: {id, date, amount, description, source}, transaction_b: {id, date, amount, description, source}}`. Degraded response returns count of pending matches by type without transaction details.
- **Service:** `MatchService.pending() -> list[PendingMatch]`
- **CLI:** `moneybin transactions matches pending [--type dedup|transfer]`

#### `transactions_matches_confirm`

Accept one or more match proposals.

- **Sensitivity:** `medium`
- **Unique parameters:** `match_ids: list[str]` (required).
- **Behavior:** Confirms matches, triggers gold-record merge (dedup) or transfer link (transfer). Returns `{confirmed, skipped, errors, error_details}`. Confirmed matches take effect on next `sqlmesh run`.
- **Service:** `MatchService.confirm() -> ActionResult`
- **CLI:** `moneybin transactions matches confirm --match-ids ID [ID ...]`

#### `transactions_matches_reject`

Reject one or more match proposals.

- **Sensitivity:** `medium`
- **Unique parameters:** `match_ids: list[str]` (required), `permanent: bool = false` — if true, the matcher won't re-propose this pair.
- **Behavior:** Rejects proposals, removes from review queue. Returns `{rejected, errors}`.
- **Service:** `MatchService.reject() -> ActionResult`
- **CLI:** `moneybin transactions matches reject --match-ids ID [ID ...] [--permanent]`

#### `transactions_matches_undo`

Un-merge a previously confirmed match.

- **Sensitivity:** `medium`
- **Unique parameters:** `match_ids: list[str]` (required).
- **Behavior:** Restores previously separate gold rows. Re-running the matcher will re-propose (not re-apply) the same match. Returns `{revoked, errors}`.
- **Service:** `MatchService.revoke() -> ActionResult`
- **CLI:** `moneybin transactions matches revoke --match-ids ID [ID ...]`

#### `transactions_matches_log`

Query match decision history.

- **Sensitivity:** `low` — decision metadata only, not financial data.
- **Unique parameters:** `match_type: str?`, `decided_by: str?` (`auto`, `user`, `system`).
- **Behavior:** Returns array of `{match_id, match_type, decided_by, decided_at, match_reason, confidence, reversed_at}`.
- **Service:** `MatchService.log() -> list[MatchDecision]`
- **CLI:** `moneybin transactions matches log [--type dedup|transfer] [--decided-by auto|user]`

#### `transactions_matches_run`

Trigger the matching engine on-demand.

- **Sensitivity:** `low` — triggers a process, doesn't return financial data.
- **Unique parameters:** `scope: str?` (`all`, `recent` — default `recent` scans transactions since last run).
- **Behavior:** Runs the matcher synchronously. Returns `{auto_merged, pending_review, no_match, duration_seconds}`.
- **Service:** `MatchService.run() -> MatchRunResult`
- **CLI:** `moneybin transactions matches run [--scope all|recent]`

---

## 7. `import.*` — Data ingestion

**Service class:** `ImportService`

### `import_files`

Import one or more financial data files into MoneyBin. Format detected automatically per file from extension (OFX/QFX/QBO, CSV/TSV/Excel/Parquet/Feather). Per-file failures do not abort the batch; transforms run once at end of batch by default.

- **Sensitivity:** `low` — return envelope reports per-file counts and status, not transaction content.
- **Unique parameters:** `paths: list[str]` (required, each path must be within the user's home directory), `refresh: bool = True`, `force: bool = False`.
- **Behavior:** Validates each path, delegates to `ImportService.import_files()`. Returns envelope with `data.{imported_count, failed_count, total_count, transforms_applied, transforms_duration_seconds, files: list[{path, status, source_type, rows_loaded, import_id, error?}]}`. Amounts use accounting convention: negative=expense, positive=income; transfers exempt.
- **Service:** `ImportService.import_files() -> BatchImportResult`
- **CLI:** `moneybin import files PATHS... [--no-refresh] [--output json]`

Per-file overrides (`account_name`, `institution`, `format_name`) are not exposed on the batch MCP surface — use the CLI for those.

### `import_status`

Show import history and data freshness per source.

- **Sensitivity:** `low` — metadata only (dates, counts, source types).
- **Unique parameters:** None.
- **Behavior:** Returns array of `{source_type, source_file, imported_at, record_count, date_range_start, date_range_end}` sorted by most recent.
- **Service:** `ImportService.status() -> list[ImportRecord]`
- **CLI:** `moneybin import status`

### `import_folder`

**Status:** blocked on Smart Import Pillar A — NOT registered.

Batch import a directory of mixed file types. `import_files` now subsumes the variadic-paths case; `import_folder` remains the directory-walker variant.

- **Sensitivity:** `medium`
- **Unique parameters:** `folder_path: str` (required), `account_id: str?` (applied to CSV files), `recursive: bool = false`.
- **Behavior:** Scans directory for supported file types, imports each. Returns per-file results: `{file, status, records, error?}`. Files that fail don't block others.
- **Service:** `ImportService.import_folder() -> list[ImportResult]`
- **CLI:** `moneybin import folder PATH [--account-id ID] [--recursive]`
- **Dependency:** Smart Import Pillar A.

### `import_revert`

Revert a previous import batch by `import_id`. Removes the imported rows from `raw.*` and triggers downstream re-materialization on the next refresh.

- **Sensitivity:** `low` — counts only in the response.
- **Annotations:** `read_only=False`, `destructive=True`, `idempotent=False`.
- **Unique parameters:** `import_id: str` (required) — the batch identifier returned by `import_files` or `transactions_create`.
- **Mutation surface:** deletes rows from the relevant `raw.*` table tagged with `import_id`. No revert — re-run the original import to restore.
- **Service:** `ImportService.revert()`
- **CLI:** `moneybin import revert IMPORT_ID`

### `import_inbox_sync`

Sweep the inbox directory: import any new files, archive them on success, surface failures.

- **Sensitivity:** `low` — return envelope reports per-file counts and status, not content.
- **Unique parameters:** `refresh: bool = True` — run the refresh pipeline once at end-of-sweep when at least one file imported.
- **Behavior:** Returns `dataclasses.asdict(InboxSyncResult)`: `{processed: [...], failed: [...], skipped: [...], ignored: [...], transforms_applied, transforms_duration_seconds, transforms_error}` — per-file lists bucketed by disposition, plus end-of-batch refresh hook state. Idempotent: rerunning over the same inbox is a no-op once files are archived.
- **Service:** `InboxService.sync()`
- **CLI:** `moneybin import inbox` (sync is the default callback; no `sync` subcommand and no `--no-refresh` flag — `InboxService.sync()` accepts `refresh: bool=True` internally but the CLI calls it with the default; agents that need to skip refresh use the MCP tool with `refresh=False`).

### `import_inbox_pending`

Show inbox status: files awaiting sync, archive contents, last-sync timestamp.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns `dataclasses.asdict(InboxListResult)`: `{would_process: [...], ignored: [...]}` — dry-run preview of what a sync would touch.
- **Service:** `InboxService.enumerate()`
- **CLI:** `moneybin import inbox list` (bare `moneybin import inbox` runs sync; `list` is the dry-run preview).

### `import_preview`

Preview a tabular file's headers and sample rows before importing. Format-agnostic (CSV, Excel, etc.).

- **Sensitivity:** `low` — structural metadata only.
- **Unique parameters:** `file_path: str` (required).
- **Behavior:** Returns `{file_name, headers, column_count, sample_rows}` with 3 sample rows as dicts keyed by header. Does not import or modify anything.
- **Service:** `ImportService.file_preview() -> FilePreview`
- **CLI:** `moneybin import file-preview PATH`

### `import_formats`

List available tabular import formats.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns array of `{name, institution_name, file_type, sign_convention, date_format, times_used, last_used_at, source}` for all built-in and user-saved formats.
- **Service:** `ImportService.list_formats() -> list[FormatSummary]`
- **CLI:** `moneybin import formats list`

### `import_ai_preview`

**Status:** blocked on Smart Import Pillar F + Privacy framework — NOT registered.

Preview what data would be sent to AI for parsing, with redaction applied.

- **Sensitivity:** `low` — shows redacted preview, not raw financial data.
- **Unique parameters:** `file_path: str` (required).
- **Behavior:** Returns `{file_name, backend, redacted_preview, fields_to_extract}`. Shows exactly what leaves the machine if the user confirms. Does not send anything.
- **Service:** `ImportService.ai_preview() -> AIParsePreview`
- **CLI:** `moneybin import ai-preview PATH`
- **Dependency:** Smart Import Pillar F + Privacy framework.

### `import_ai_parse`

**Status:** blocked on Smart Import Pillar F + Privacy framework — NOT registered.

Confirm and execute AI-assisted parsing for a file.

- **Sensitivity:** `medium` — sends redacted file content to configured AI backend.
- **Unique parameters:** `file_path: str` (required), `backend: str?` (override configured backend).
- **Behavior:** Requires explicit consent (per-file, not persistent). Sends redacted content to AI backend, receives column mapping or extracted data, imports the result. Returns standard `ImportResult` plus `{backend_used, fields_extracted}`.
- **Service:** `ImportService.ai_parse() -> AIParseResult`
- **CLI:** `moneybin import ai-parse PATH [--backend NAME]`
- **Dependency:** Smart Import Pillar F + Privacy framework.

---

## 8. `transactions_categorize_*`, `categories_*`, `merchants_*` — Categorization pipeline + reference data

**Service class:** `CategorizationService`

### `transactions_categorize_pending`

Fetch transactions that haven't been categorized yet. Absorbs the former `reports_uncategorized` tool.

- **Sensitivity:** `medium` — returns transaction descriptions and amounts.
- **Unique parameters:** `limit: int = 50`, `sort: Literal["date", "impact"] = "date"` (`impact` sorts by `ABS(amount) × age_days` descending), `min_amount: Decimal = Decimal("0")` (filter to `ABS(amount) >= min_amount`), `account: str | None = None` (filter by account ID or display name; ambiguous display names raise `account_ambiguous`).
- **Behavior:** Returns array of `{transaction_id, account_id, account_name, txn_date, amount, description, merchant_id, merchant_normalized, age_days, priority_score, source_type, source_id}` from `reports.uncategorized_queue`. Amounts use the accounting convention: negative = expense, positive = income. Degraded response returns uncategorized count by account and time period.
- **Service:** `CategorizationService.list_uncategorized_transactions(limit, sort, min_amount, account_id)`
- **CLI:** `moneybin transactions categorize pending [--limit N] [--sort date|impact] [--min-amount N] [--account NAME]`

### `transactions_categorize_commit`

*Exemplar — see section 2.3.*

### `transactions_categorize_run`

Run the categorization engine cascade over uncategorized transactions.

- **Sensitivity:** `medium` — writes categorizations to `app.transaction_categories`.
- **Unique parameters:** `methods: list[Literal["rules", "merchants"]] | None` (optional, default `["rules", "merchants"]`) — engines to run in order. A rule write blocks a merchant write at the same priority.
- **Behavior:** Returns `{applied_by_method: {rules: int, merchants: int}, total_applied: int}`. The canonical order `["rules", "merchants"]` takes an optimized shared-scan path; other orders run engines individually in the requested order. The `"ml"` literal will be added when ML categorization implementation lands.
- **Service:** `CategorizationService.categorize_run(methods=...) -> dict`
- **CLI:** `moneybin transactions categorize run [--methods rules,merchants] [--output json]`

### `transactions_categorize_rules`

List active categorization rules.

- **Sensitivity:** `low` — rule patterns are structural, not financial data.
- **Unique parameters:** None.
- **Behavior:** Returns array of `{rule_id, name, merchant_pattern, match_type, category, subcategory, min_amount, max_amount, account_id, priority, created_by}` sorted by priority.
- **Service:** `CategorizationService.rules() -> list[CategorizationRule]`
- **CLI:** `moneybin transactions categorize rules list`

### `transactions_categorize_rules_create`

Create one or more categorization rules.

- **Sensitivity:** `low`
- **Unique parameters:** `rules: list[object]` (required) — list of `{name, merchant_pattern, category, subcategory?, match_type?, min_amount?, max_amount?, account_id?, priority?}`.
- **Behavior:** Idempotent. Each item is deduped against active rules by the matcher+output tuple — `(merchant_pattern, match_type, min_amount, max_amount, account_id, category, subcategory)`. `name` and `priority` are metadata and excluded from the dedup key, so renaming a rule or shuffling priorities does not create a new row. If an active rule with the same key exists, the existing `rule_id` is returned. Returns `{created, existing, skipped, errors, error_details, rule_ids}`.
- **Service:** `CategorizationService.create_rules() -> RuleCreationResult`
- **CLI:** `moneybin transactions categorize rules create --file rules.json`

#### Rule-conflict detection (follow-up)

Same matcher with a *divergent* category output — e.g. one active rule `AMZN → Shopping` and a new request `AMZN → Business` — is currently treated as a brand-new rule, not a conflict. Both rules coexist; whichever has lower `priority` fires first. A future iteration should detect this case at write time and let the caller pick: keep both (current behavior), supersede the older rule, or refuse the write. Tracked as a deferred follow-up because the right resolution UX is unclear and depends on the categorization workflow's overall ergonomics.

### `transactions_categorize_rules_delete`

Delete a categorization rule.

- **Sensitivity:** `low`
- **Unique parameters:** `rule_id: str` (required).
- **Behavior:** Deletes the rule. Returns confirmation with the deleted rule's name.
- **Service:** `CategorizationService.delete_rule() -> DeleteResult`
- **CLI:** `moneybin transactions categorize rules delete --rule-id ID`

### `merchants`

List merchant name mappings.

- **Sensitivity:** `low` — mapping patterns, not financial data.
- **Unique parameters:** None.
- **Behavior:** Returns array of `{merchant_id, raw_pattern, match_type, canonical_name, category, subcategory, created_by}`.
- **Service:** `CategorizationService.merchants() -> list[MerchantMapping]`
- **CLI:** `moneybin merchants list`

### `merchants_create`

Create one or more merchant name mappings.

- **Sensitivity:** `low`
- **Unique parameters:** `merchants: list[object]` (required) — list of `{raw_pattern, canonical_name, match_type?, category?, subcategory?}`. Note: `category_id` resolution is not yet wired — pass `category`/`subcategory` text and let the existing categorization path resolve to `core.dim_categories`.
- **Behavior:** Returns `{created, skipped, errors, error_details}`.
- **Service:** `CategorizationService.create_merchants() -> CreateResult`
- **CLI:** `moneybin merchants create --file mappings.json`

### `categories`

List the category taxonomy.

- **Sensitivity:** `low`
- **Unique parameters:** `include_inactive: bool = false`.
- **Behavior:** Returns array of `{category_id, category, subcategory, description, is_default, is_active}`.
- **Service:** `CategorizationService.categories() -> list[Category]`
- **CLI:** `moneybin categories list [--include-inactive]`

### `categories_create`

Create a custom category.

- **Sensitivity:** `low`
- **Unique parameters:** `category: str` (required), `subcategory: str?`, `description: str?`.
- **Behavior:** Generates a category ID, returns the created category.
- **Service:** `CategorizationService.create_category() -> Category`
- **CLI:** `moneybin categories create --category NAME [--subcategory NAME]`

### `categories_set`

Update a category's settings (currently only `is_active`).

- **Sensitivity:** `low`
- **Unique parameters:** `category_id: str` (required), `is_active: bool` (required).
- **Behavior:** Idempotent partial update. Existing categorizations are preserved when a category is disabled. Shape-1b — matches `accounts_set` / `budget_set`.
- **Service:** `CategorizationService.toggle_category() -> ToggleResult`
- **CLI:** `moneybin categories set CATEGORY_ID --active/--inactive`

### `categories_delete`

Hard-delete a user-created category.

- **Sensitivity:** `low`
- **Unique parameters:** `category_id: str` (required), `force: bool = false`.
- **Behavior:** Refuses by default if the category is referenced by any rows in `app.transaction_categories`, `app.budgets`, `app.user_merchants`, `app.transaction_splits`, `app.categorization_rules`, or `app.proposed_rules` (matched by `category_id` FK). With `force=true`, deletes referencing rows across all six tables before removing the `app.user_categories` row; affected transactions return to the uncategorized state. Default (seeded) categories cannot be hard-deleted — use `categories_set` with `is_active=false` to preserve the canonical taxonomy row. `app.rule_deactivations` rows are NOT cascade-cleared — they are audit-trail history and intentionally retain their now-unresolvable `new_category_id` FK.
- **Mutation surface:** deletes the `app.user_categories` row. With `force=true`, cascade-deletes rows in the six writer tables listed above. No revert path — recreate with `categories_create`. The `CATEGORY_TEXT_COLLISION` error code from the PR #166 text-FK guard is retired (FK matching makes it unreachable).
- **Errors:** `CATEGORY_NOT_FOUND` (no such ID), `CATEGORY_IS_DEFAULT` (seeded default cannot be hard-deleted), `CATEGORY_HAS_REFERENCES` (`force=false` and references exist).
- **Service:** `CategorizationService.delete_category() -> None`
- **CLI:** `moneybin categories delete CATEGORY_ID [--force]`

### `transactions_categorize_stats`

Categorization coverage statistics; optionally includes auto-rule health metrics.

- **Sensitivity:** `low` — counts and percentages only.
- **Unique parameters:** `include_auto: bool = False` — when true, appends auto-rule health to the response.
- **Behavior:** Base response: `{total_transactions, categorized, uncategorized, percent_categorized, by_source}` where `by_source` breaks down by categorization source (user, rule, ai, plaid). With `include_auto=True`, returns `{overall: <base>, auto: {active_auto_rules, pending_proposals, transactions_categorized}}`. The `auto` block absorbs what was previously the standalone `transactions_categorize_auto_stats` tool.
- **Service:** `CategorizationService.stats() -> CategorizationStats`; with `include_auto=True` also calls `AutoRuleService.stats() -> AutoStatsResult`.
- **CLI:** `moneybin transactions categorize stats`

### `transactions_categorize_rules_apply`

**Retired.** The standalone rule-engine apply tool is folded into the polymorphic engine cascade. Use `transactions_categorize_run(methods=["rules"])` (MCP) or `moneybin transactions categorize run --methods rules` (CLI) instead. See `transactions_categorize_run` above.

### `transactions_categorize_auto_review`

List auto-generated rules pending user approval.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns array of `{proposed_rule_id, merchant_pattern, category, subcategory, source, trigger_count, sample_transactions}` where `source` indicates how the rule was generated (ml, pattern_detection).
- **Service:** `AutoRuleService.review() -> AutoReviewResult`
- **CLI:** `moneybin transactions categorize auto review`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar E: auto-rule generation), [Auto-rule generation](categorization-auto-rules.md).

### `transactions_categorize_auto_accept`

Accept or reject proposed auto-generated rules. Takes two parallel ID lists.

- **Sensitivity:** `low`
- **Unique parameters:** `accept: list[str]` (proposed_rule_ids to promote), `reject: list[str]` (proposed_rule_ids to refuse).
- **Behavior:** Accepted rules are promoted to active categorization rules in `app.categorization_rules` with `created_by='auto_rule'` and immediately evaluated against uncategorized transactions. Rejected rules are not re-proposed for the same pattern. Returns `{accepted, rejected, errors}`.
- **Mutation surface:** writes to `app.categorization_rules` (accept path) and `app.proposed_rules` (status transitions). Revert via `transactions_categorize_rules_delete` for promoted rules.
- **Service:** `AutoRuleService.accept(accept=..., reject=...) -> ActionResult`
- **CLI:** `moneybin transactions categorize auto accept --accept <id> [<id>...] --reject <id> [<id>...]`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar E: auto-rule generation), [Auto-rule generation](categorization-auto-rules.md).

### `transactions_categorize_auto_stats`

**Retired (MCP only).** The auto-rule health metrics are now available via `transactions_categorize_stats(include_auto=True)`. The CLI command `moneybin transactions categorize auto stats` remains — it calls `AutoRuleService.stats()` directly and is unaffected.

> ML-based categorization is deferred; see `categorization-ml.md` (planned) when work resumes.

---

## 9. `budget_*` (mutation) and `reports_budget_*` (vs-actual reads) — Budget tracking

**Service class:** `BudgetService`

### `budget_set`

Create or update a monthly budget for a category.

- **Sensitivity:** `low` — budget targets are user-authored metadata, not financial data.
- **Unique parameters:** `category: str` (required), `monthly_amount: float` (required), `start_month: str?` (YYYY-MM, defaults to current month), `end_month: str?` (open-ended if omitted).
- **Behavior:** Upserts — if an active budget exists for the category, updates the amount. Returns the created/updated budget with its ID.
- **Service:** `BudgetService.set() -> Budget`
- **CLI:** `moneybin budget set --category NAME --amount N [--start-month YYYY-MM]`

### `reports_budget`

Budget vs actual spending comparison for a month.

- **Sensitivity:** `low` — returns aggregates (budget target, total spent, remaining).
- **Unique parameters:** `month: str?` (YYYY-MM, defaults to current month).
- **Behavior:** Returns array of `{category, budget, spent, remaining, percent_used, status}` where status is `OK`, `WARNING` (>90%), or `OVER`. Includes only categories with active budgets. At `detail=full`, includes per-week spending pace within the month.
- **Service:** `BudgetService.status() -> list[BudgetStatus]`
- **CLI:** `moneybin reports budget status [--month YYYY-MM]`

### `reports_budget_summary`

Budget performance over multiple months — trend view.

- **Sensitivity:** `low`
- **Unique parameters:** Shared `months`/date conventions.
- **Behavior:** Returns array of `{month, total_budget, total_spent, total_remaining, categories_over, categories_on_track}`. Chart-ready time-series for budget adherence over time.
- **Service:** `BudgetService.summary() -> list[BudgetMonthlySummary]`
- **CLI:** `moneybin reports budget summary [--months 6]`

### `budget_delete`

Remove a budget for a category.

- **Sensitivity:** `low`
- **Unique parameters:** `category: str` (required).
- **Behavior:** Sets `end_month` to current month rather than hard-deleting, preserving history. Returns confirmation.
- **Service:** `BudgetService.delete() -> DeleteResult`
- **CLI:** `moneybin budget delete --category NAME`

---

## 10. `tax.*` — Tax information

**Status:** removed. `tax_w2` and the W-2 PDF extraction pipeline were cut entirely
(W-2 extractor, loader, `raw.w2_forms` schema, `TaxService`, CLI `tax` command group
all deleted). The `docs/specs/archived/w2-extraction.md` spec documents the removed
design. Tax data ingestion will be re-designed from scratch in a future brainstorm.

---

## 11. `privacy.*` — Privacy & consent

**Service class:** `PrivacyService`

**Status:** all four `privacy_*` tools below are blocked on the consent management spec — NONE are registered today. The catalog entries describe the target design; per the surface-discipline rule in `.claude/rules/mcp-server.md`, individual `privacy.*` tools surface only when their backing spec reaches `in-progress` or `implemented` in `docs/specs/INDEX.md`. See §17c dependency tracker.

**Dependency:** All `privacy.*` tools depend on the consent management spec, audit log spec, and provider profiles spec.

### `privacy_status`

Current consent state, configured AI backend, and privacy mode.

- **Sensitivity:** `low` — metadata about privacy configuration, not financial data.
- **Unique parameters:** None.
- **Behavior:** Returns `{consent_grants: [{feature, granted_at, backend}], configured_backend: {name, type, is_local}, consent_mode, unmask_critical}`. This is the tool version of the `moneybin://privacy` resource — useful when the AI needs to check consent before attempting a sensitive operation.
- **Service:** `PrivacyService.status() -> PrivacyStatus`
- **CLI:** `moneybin privacy status`

### `privacy_grant`

Grant consent for a privacy feature category.

- **Sensitivity:** `low` — modifying consent state, not accessing financial data.
- **Unique parameters:** `feature: str` (required — e.g., `mcp-data-sharing`), `backend: str?` (override configured backend for this grant).
- **Behavior:** Creates a persistent consent grant. Returns the grant record with timestamp. Idempotent — re-granting an active grant is a no-op that returns the existing grant.
- **Service:** `PrivacyService.grant() -> ConsentGrant`
- **CLI:** `moneybin privacy grant FEATURE`

### `privacy_revoke`

Revoke a previously granted consent.

- **Sensitivity:** `low`
- **Unique parameters:** `feature: str` (required).
- **Behavior:** Revokes the active grant. Future tool calls at the relevant sensitivity tier will return degraded responses. Returns confirmation with revocation timestamp.
- **Service:** `PrivacyService.revoke() -> RevokeResult`
- **CLI:** `moneybin privacy revoke FEATURE`

### `privacy_audit`

Query the AI audit log.

- **Sensitivity:** `low` — the audit log is metadata (which tools were called, when, at what sensitivity), not financial data.
- **Unique parameters:** `tool_name: str?` (filter to a specific tool).
- **Behavior:** Returns array of `{timestamp, tool_name, sensitivity, consented, degraded, backend, backend_local}`.
- **Service:** `PrivacyService.audit() -> list[AuditEntry]`
- **CLI:** `moneybin privacy audit [--start-date DATE] [--tool-name NAME]`

---

## 12. `system_*`, `reports_*`, `refresh_run` — Data status, reports projections, and the refresh umbrella

**Service class:** `OverviewService`

### `system_status`

Data status dashboard — what data exists, how fresh it is, what's pending action.

- **Sensitivity:** `low` — counts and dates only.
- **Unique parameters:** None.
- **Behavior:** Returns `{accounts, transactions, categorization, imports, matching, budgets}` where each section has relevant counts and dates. E.g., `transactions: {total: 4230, date_range: "2024-01 to 2026-04", last_import: "2026-04-15"}`, `categorization: {categorized: 3890, uncategorized: 340, percent: 92}`, `matching: {pending_review: 5}`. This is the tool version of the `moneybin://status` resource with richer detail.
- **Service:** `OverviewService.status() -> SystemStatus`
- **CLI:** `moneybin system status`

### `system_doctor`

Pipeline integrity check — confirms the data pipeline is self-consistent before analysis.

- **Sensitivity:** `low` — counts and status labels only.
- **Unique parameters:** None.
- **Behavior:** Runs all SQLMesh named audits (FK integrity, sign convention, transfer balance) plus two hardcoded checks (staging coverage, categorization coverage). Returns pass/fail/warn per invariant and total transaction count. Always runs with `verbose=False` — agents can query `core.fct_transactions` or `core.bridge_transfers` directly for drill-down. Exit is informational only (no exception on fail).
- **Service:** `DoctorService.run_all(verbose=False) -> DoctorReport`
- **CLI:** `moneybin system doctor [--verbose] [--output json]`

> Note: a unified `reports_health` snapshot tool is **planned, not registered**. Today the agent composes the same view from `reports_networth` + `reports_spending` + `reports_cashflow` + `reports_budget`. Re-evaluate as a dedicated tool when agent-experience reports show the composition friction outweighs the surface cost.

### `reports_recurring`

Recurring transaction detection — surfaces likely subscriptions, autopay, and other repeating charges.

- **Sensitivity:** `low` — aggregates and merchant labels.
- **Unique parameters:** `min_confidence: float = 0.5`, `status: str = "active"` (`active` | `inactive` | `all`), `cadence: str | None = None` (`weekly` | `biweekly` | `monthly` | `quarterly` | `yearly` | `irregular`).
- **Behavior:** Returns `reports.recurring_subscriptions` rows filtered by the above (merchant, cadence, avg_amount, confidence, etc.).
- **CLI:** `moneybin reports recurring [--min-confidence N] [--status STATUS] [--cadence CADENCE]`

### `reports_merchants`

Top merchants by lifetime activity.

- **Sensitivity:** `low` — aggregates only.
- **Unique parameters:** `top: int = 25`, `sort: str = "spend"` (`spend` | `count` | `recent`).
- **Behavior:** Returns `reports.merchant_activity` rows sorted per `sort`, limited to `top`.
- **CLI:** `moneybin reports merchants [--top N] [--sort spend|count|recent]`

### `reports_uncategorized`

**Retired.** Folded into `transactions_categorize_pending` with the addition of `sort` (`"date"` or `"impact"`) and `account` filter parameters. The CLI command `moneybin reports uncategorized` has been removed; use `moneybin transactions categorize pending [--sort impact] [--min-amount N] [--account NAME]` instead.

### `reports_large_transactions`

Anomaly-flavored transaction lens — top-N by absolute amount, with optional per-account or per-category z-score filtering.

- **Sensitivity:** `medium` — row-level transactions.
- **Unique parameters:** `top: int = 25`, `anomaly: str = "none"` (`account` | `category` | `none`; non-`none` filters to z > 2.5 in the named scope).
- **Behavior:** Returns top-N rows from `reports.large_transactions`, optionally filtered to anomalies.
- **CLI:** `moneybin reports large-transactions [--top N] [--anomaly account|category|none]`

### `reports_balance_drift`

Categorical view of balance assertions by drift status.

- **Sensitivity:** `medium` — balance amounts.
- **Unique parameters:** `account: str | None = None` (filter by account name), `status: str = "all"` (`drift` | `warning` | `clean` | `no-data` | `all`), `since: str | None = None` (ISO date; only assertions on or after).
- **Behavior:** Returns one row per assertion with computed drift from `reports.balance_drift`. Use when you want a status breakdown across all assertions. Amounts use the accounting convention: negative = expense, positive = income. Drift in `summary.display_currency`.
- **CLI:** `moneybin reports balance-drift [--account NAME] [--status STATUS] [--since YYYY-MM-DD]`

### `refresh_run`

Run the post-load refresh pipeline: cross-source matching, SQLMesh apply, deterministic categorization.

- **Sensitivity:** `low` — counts and durations only.
- **Unique parameters:**
  - `steps: list[Literal["match", "transform", "categorize"]] | None = None` — subset of canonical steps to run; defaults to None (full cascade). Steps execute in canonical order (match → transform → categorize) regardless of input order; dependencies enforce it (categorize reads SQLMesh-built views). Pass `steps=["transform"]` to run SQLMesh apply alone.
- **Mutation surface:** rebuilds `core.*` and `reports.*` via SQLMesh; writes `app.transaction_categories` for newly-matched rules. No revert path — re-run after fixing inputs.
- **Behavior:** Single user-facing entry point for the refresh domain. Idempotent; safe to retry after a failure. Matching and categorization steps are best-effort and log-only on failure — only SQLMesh apply errors surface in the response envelope. Returns `{applied, duration_seconds, error?}`. On apply failure, `actions[]` hints at `moneybin transform plan` (CLI operator tool) to inspect, or `refresh_run` to retry. When `steps` includes `match` but excludes `categorize`, `actions[]` includes a follow-up hint pointing at `refresh_run(steps=["categorize"])`. Unknown step names raise `UserError(code="UNKNOWN_REFRESH_STEP")`. Symmetric with `transactions_categorize_run(methods=...)`.
- **Service:** `moneybin.services.refresh.refresh(db, *, steps=None) -> RefreshResult`
- **CLI:** `moneybin refresh [--step STEP]... [--output json] [-q]`

---

## 13. `sql.*` — Direct SQL access

`sql_query` uses `get_db()` directly with query validation from the privacy module. No dedicated service class — this is a power-user escape hatch, not a structured service.

### `sql_query`

Execute an arbitrary read-only SQL query against DuckDB.

- **Sensitivity:** `medium` — can return any row-level data from core tables.
- **Unique parameters:** `sql: str` (required).
- **Behavior:** Validates query is read-only (SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN). Blocks file-access functions (`read_csv`, `read_parquet`, etc.) and URL literals. Results capped at `MAX_ROWS` and `MAX_CHARS`. Returns results in the standard response envelope with column names as field keys. Degraded response rejects the query with a consent instruction — arbitrary SQL can't be meaningfully degraded to aggregates.
- **CLI:** `moneybin db query "SELECT ..." [-o text|json|csv|markdown|box]`

### `sql_schema`

Return the curated database schema for ad-hoc SQL composition. Equivalent to reading the `moneybin://schema` MCP resource — provided as a tool for hosts that don't surface MCP resources to the model (e.g. Claude.ai chat).

- **Sensitivity:** `low` — schema metadata, no row data.
- **Unique parameters:** `table: str | None`. `None` (default) returns the compact catalog (table names + purposes + column counts). A full name like `'core.fct_transactions'` returns columns, comments, and example queries for that one table. `'*'` returns the full ~50KB schema document.
- **Behavior:** Unknown `table` returns a `UserError(code='unknown_table')` with the available-tables list as a hint.
- **CLI:** No direct CLI parity — the MCP tool wraps the same data the `moneybin://schema` resource exposes. CLI users get the schema via `moneybin db query "SELECT ..."` (or `\d`-style introspection via DuckDB's `information_schema`). A future `moneybin db schema` subcommand could mirror the MCP shape; tracked as a follow-up.

---

## 14. Prompts

Four goal-oriented workflow templates. Each defines the goal, relevant tools, guardrails, and decision points. The AI determines the exact tool sequence based on what data exists. Prompts are not step-by-step scripts — they describe what to accomplish, not how to accomplish it.

### Prompt categories

| Category | Purpose |
|---|---|
| **Review** | Periodic financial analysis workflows |
| **Organize** | Work through pending items to get data into shape |
| **Setup** | First-run and configuration workflows |

### `monthly-review` (Review)

**Goal:** Help the user understand their financial position for a given month — what they earned, what they spent, where, whether they're on budget, and what's unusual.

**Parameters:** `month: str?` (YYYY-MM, defaults to current month).

**Relevant tools:** `reports_spending` (with `category` filter for drill-down), `reports_cashflow`, `reports_budget`, `reports_recurring`, `reports_merchants`, `reports_networth`

**Guardrails:**

- Start with the big picture (income vs expenses, net) before drilling into categories
- Compare to the prior month to surface trends, not just absolutes
- If budgets are configured, include compliance; if not, skip — don't prompt budget setup mid-review
- Flag anomalies: categories with large month-over-month changes, new recurring charges, unusually large single transactions
- End with a concise summary and 2-3 actionable observations, not a data dump
- If data-sharing consent is not granted, work with degraded responses and note the limitation once

**Decision points:** None — read-only analysis. The AI presents findings and the user decides what to act on.

### `categorization-organize` (Organize)

**Goal:** Work through uncategorized transactions in batches, applying categories, creating merchant mappings, and building rules so future imports require less manual work.

**Relevant tools:** `transactions_categorize_stats`, `categories`, `transactions_categorize_pending`, `transactions_categorize_commit`, `transactions_categorize_rules_create`, `merchants_create`, `categories_create`

**Guardrails:**

- Defaults are seeded automatically by `db init`; no MCP-side seed step
- Fetch uncategorized transactions in manageable batches (50)
- Always use batch tools, never single-item equivalents
- Present proposed categorizations to the user for confirmation before applying
- After applying, propose merchant mappings and rules for patterns that appeared multiple times
- Track progress: "X of Y categorized, Z remaining"
- ML-assisted suggestions (`suggest=true`) are deferred pending ML categorization implementation
- Stop when the user says stop, not when the queue is empty

**Decision points:** User confirms each batch of categorizations before `transactions_categorize_commit` is called. User confirms proposed rules before `transactions_categorize_rules_create` is called.

### `onboarding` (Setup)

**Goal:** Guide a first-time user from empty database to imported, transformed, and categorized data.

**Relevant tools:** `system_status`, `import_files`, `import_preview`, `import_formats`, `transactions_categorize_stats`

**Guardrails:**

- Start by checking `system_status` — if data already exists, acknowledge and ask what the user wants to do next rather than re-running onboarding
- Ask the user for file paths — don't assume locations
- For tabular files, guide through the format creation flow if auto-detection fails
- After import, explain what happened (records loaded, accounts discovered) and what's available next
- Seed categories and mention categorization as a natural next step, but don't force it
- Keep the tone welcoming, not overwhelming — this is a first impression

**Decision points:** User provides file paths. User confirms column mappings. User decides whether to proceed to categorization.

### `tax-prep` (Removed)

**Status:** removed alongside the W-2 extraction pipeline. Tax data ingestion will be
re-designed from scratch; this prompt will be revisited when a new tax spec lands.

---

## 15. Resources

One ambient context endpoint: `moneybin://schema`. The seven resources removed in PR #185 (`moneybin://status`, `moneybin://accounts`, `moneybin://privacy`, `moneybin://tools`, `accounts://summary`, `moneybin://recent-curation`, `net-worth://summary`) were duplicates of tool responses and added context-window overhead without information gain. Their data remains available via the corresponding tools.

### `moneybin://schema`

Core and app table schemas with column names, types, and descriptions. Lets the AI write accurate SQL for `sql_query` without calling a discovery tool first. This resource has unique composition value: it provides a curated schema snapshot that is more useful for SQL generation than any single tool response.

---

## 16. `sync_*` — Provider sync (Plaid)

Per the MCP exposure principle, sync is fully MCP-exposed except for credential-handling commands. OAuth flows return redirect URLs; the client opens them.

| Tool | Sensitivity | Behavior |
|---|---|---|
| `sync_connect [institution]` | medium | Initiates Plaid Hosted Link flow. Returns `{session_id, link_url, expiration}`. `link_url` is a one-time bearer credential — treat as medium sensitivity. Pass `institution` to re-authenticate (Plaid update mode). |
| `sync_connect_status <session_id>` | low | Single-shot check of a connect session. Returns `{session_id, status, provider_item_id, institution_name, error, expiration}`. Does NOT poll internally — the agent invokes this when the user signals completion. |
| `sync_disconnect <institution>` | medium | Removes institution by name. No revert path. |
| `sync_pull [institution] [force] [refresh=true]` | medium | Triggers sync for one or all institutions; loads `raw.plaid_*` and propagates through SQLMesh. Amounts follow MoneyBin convention (negative = expense). When `refresh` (default true) and the sync changes raw state, the post-load refresh pipeline (matching + SQLMesh apply + categorization) runs once at end-of-pull so `core.dim_accounts` reflects new data before returning. Result envelope adds `transforms_applied`, `transforms_duration_seconds`, `transforms_error` (SQLMesh-step outcome — matching and categorization are log-only on failure). |
| `sync_status` | low | Read-only: connected institutions, last-sync times, guidance for error states. |

**CLI-only (security-justified):** `sync_login`, `sync_logout` (browser interaction + credential handling routed through LLM context is a security-model violation); `sync_rotate_key` — passphrase material through LLM context is a security-model violation.

**Prompts:**

| Prompt | Behavior |
|---|---|
| `sync_review` | Agent-driven health check. Walks the agent through `sync_status` + a quick `reports_spending` pulse to flag errored institutions, stale connections (last_sync > 7 days), and volume anomalies. Output is constrained to counts / dates / status codes / institution names — no PII. |

---

## 17. `transform_*` — SQLMesh pipeline operations

**CLI-only (operator territory, category 2).** The `transform_*` functions are not registered on the MCP surface as of PR #185. They remain accessible via `moneybin transform <subcommand>` for operators performing hands-on SQLMesh introspection. The mutating refresh path is `refresh_run` (§12); on apply failure, `refresh_run` emits a hint pointing at `moneybin transform plan`.

`transform_apply` was retired as a standalone MCP tool in favor of `refresh_run(steps=["transform"])`. See [`smart-import-transform.md`](smart-import-transform.md).

| CLI command | Behavior |
|---|---|
| `moneybin transform status` | Current model state, environment |
| `moneybin transform plan` | Preview pending SQLMesh changes (read-only) |
| `moneybin transform validate` | Check model SQL parses and resolves |
| `moneybin transform audit` | Run data quality assertions |

**CLI-only (operator-territory):** `transform_restate` — destructive force-recompute for a date range, used for bug fixes / late-data backfill / schema reinterpretation. Power-user / data-engineering territory; preceded by code changes the AI doesn't drive.

---

## 17b. Forward namespace: `assets_*`

Reserved for [`asset-tracking.md`](asset-tracking.md). Workflows (registration, valuation, liability linking, staleness) are defined there. Per the surface-discipline rule in `.claude/rules/mcp-server.md`, no `assets_*` tools register until the backing spec reaches `in-progress`; this entry exists so reviewers can confirm the namespace is reserved.

---

## 17c. Dependency tracker

Tools that depend on unbuilt subsystems are documented in the catalog with dependency markers but **are not registered on the MCP surface until their backing spec reaches `in-progress` or `implemented`** in `docs/specs/INDEX.md`. The "Blocked tools" column below names the future tool set; today only entries whose dependency is `in-progress`/`implemented` are live. See `.claude/rules/mcp-server.md` "Surface change discipline" for the rule.

| Dependency | Status | Blocked tools |
|---|---|---|
| **Consent management spec** | Not written | `privacy_grant`, `privacy_revoke`, `privacy_status`; all degraded response behavior across the surface |
| **Audit log spec** | Not written | `privacy_audit`; audit logging in middleware |
| **Redaction engine spec** | Not written | `accounts_get` field masking; `high` sensitivity behavior |
| **Provider profiles spec** | Not written | Verified-local bypass; `privacy_status` backend info |
| **Transaction matching (Pillars A+C)** | Draft (umbrella) | All `transactions_matches.*` tools |
| **Transaction matching (Pillar B)** | Draft (umbrella) | `transactions_matches.*` transfer-type filtering |
| **[Categorization overview](categorization-overview.md)** | Draft | ML tools deferred — see `categorization-ml.md` (planned) |
| **Smart Import (Pillar A)** | Not written | `import_folder` |
| **Smart Import (Pillar F) + Privacy** | Not written | `import_ai_preview`, `import_ai_parse` |
| **Corrections table schema** | Not written | `transactions_correct` |
| **Annotations table schema** | Not written | `transactions_annotate` |
| **Budget tracking spec** | Draft | `reports_budget_summary` rollover behavior; `budget_set` (de-registered 2026-05-17 — re-register when spec reaches `in-progress`) |
| **Tax spec (none yet)** | Not written | `tax_w2`, `tax_deductions`, `tax_prep` prompt — **removed entirely** 2026-05-19; W-2 extraction pipeline cut; tax data ingestion to be re-designed from scratch when a new tax spec is written |

### Tools shippable without dependencies

> **Surface status (2026-05-19):** All entries in §16 not marked "NOT registered" or de-registered are live and visible at connect. See the dependency tracker above for tools that remain blocked. `budget.*` and `transform_*` tool modules remain implemented but are **de-registered** in `src/moneybin/mcp/server.py:register_core_tools()`. `budget.*` re-registers when its backing spec reaches `in-progress`/`implemented`; `transform_*` are operator territory (category 2) and remain CLI-only (see §17). `tax.*` tools were **removed entirely** — the W-2 PDF extraction pipeline was cut; tax data ingestion will be re-designed from scratch when a new tax spec is written. A working implementation alone does not justify exposing a tool on the public surface.

---

## 18. Standing audits and review discipline

These are recurring review responsibilities, not one-shot work items. Each is owned by the spec and enforced via a documented PR-review checklist or — where the rule is mechanical — a structural test.

### Tool description audit (checklist)

Every tool whose description omits a relevant invariant fails review. Required content per tool class:

- Tools that accept an `amount` field — sign convention, decimal precision, date format (per [§Tool descriptions: invariants must be in the description, not just the rule files](#tool-descriptions-invariants-must-be-in-the-description-not-just-the-rule-files)).
- Tools that mutate state — reversibility statement, ID-composite requirements, `app.audit_log` reference.
- Tools that return currency-bearing data — currency-pairing invariant per `architecture-shared-primitives.md` Invariant 7.

Enforcement: a checklist applied during PR review on every change to an `@mcp_tool`-decorated function. Codified in [`.claude/rules/mcp-server.md`](../../.claude/rules/mcp-server.md) under "Description requirements" so future tool authors apply it at write-time. **No structural pytest** — description text is prose, and a regex-based check produces noise (false positives on intentional convention overrides, false negatives on synonyms). Graduate to a structural test only when ≥3 invariants become statable as `(signature predicate, literal-string requirement)` pairs.

Discharge: a one-time audit pass on the existing surface ran as part of the 2026-05 MCP gap-closure PR; subsequent enforcement is per-PR reviewer responsibility.

### Tool catalog discipline (PR-review rule)

The tool list documented in this spec must match the runtime-registered set. Without discipline, the documentation drifts (the competitor `copilot-money-mcp` README claims 17 tools but its server registers 33 — exactly the failure mode this rule prevents).

- **Rule:** any PR that adds, renames, or removes an `@mcp_tool`-decorated function MUST update this spec in the same change. Codified in [`.claude/rules/mcp-server.md`](../../.claude/rules/mcp-server.md) under "Surface change discipline."
- **Enforcement:** PR review. Reviewers grep for `@mcp_tool` diffs and verify each touches a corresponding spec section.
- **Why no automated test:** a fixture-based drift test would detect code-vs-fixture drift but not code-vs-spec drift (the original problem). A spec-parsing test would detect code-vs-spec drift but is fragile to spec restructuring. The PR-review rule directly addresses the documentation-discipline problem without introducing a third synced artifact. Revisit if review attention proves insufficient.
- **One-time audit:** the 2026-05 MCP gap-closure PR ran the first runtime-vs-spec diff; findings (orphaned spec entries, undocumented tools, visibility-tagging discrepancies) were resolved or logged as deferred follow-ups in that PR's CHANGELOG.

### Protocol-coverage matrix freshness

The matrix in [§Protocol-standard capability coverage matrix](#protocol-standard-capability-coverage-matrix) must be re-reviewed:

1. **Whenever the MCP spec adds or changes a capability.** Track the MCP spec changelog (https://modelcontextprotocol.io/) on a quarterly cadence; add new rows for any new capability before deciding adopt vs. defer.
2. **Whenever a competitor review surfaces a capability we've missed.** The 2026-05-08 MCP-tool-surface review identified `annotations` as such a gap; future reviews should produce similar diffs.
3. **Whenever a row flips status** (`⏳ deliberate defer` → `📐 designed` → `✅ shipped`). The matrix is the audit trail for these transitions.

Owner: whoever maintains this spec. Cadence: quarterly review minimum, plus ad-hoc on the triggers above.
