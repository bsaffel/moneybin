# MCP Tool Surface

> Last updated: 2026-04-17
> Status: Ready
> Companion to: [`mcp-architecture.md`](mcp-architecture.md) (design philosophy, conventions, patterns)
> Supersedes: [`archived/mcp-read-tools.md`](archived/mcp-read-tools.md), [`archived/mcp-write-tools.md`](archived/mcp-write-tools.md)

## Purpose

This spec defines every concrete tool, prompt, and resource in MoneyBin's MCP v1 surface, along with the service layer methods and CLI commands that back them. It is the "what we're building" companion to `mcp-architecture.md` (the "how we think about it" document).

Together they replace the prototype-era MCP specs (read tools, write tools) with a production-grade design built for modern AI desktop applications.

## How to read this spec

- **Section 1 (Conventions)** summarizes cross-cutting patterns defined in `mcp-architecture.md`. Read that spec first for full context; this section is a quick reference, not a restatement.
- **Section 2 (Exemplars)** shows three tools in full detail — service method, MCP tool, CLI command, response shape, degraded response — to prove the patterns work end-to-end.
- **Sections 3-13 (Namespaces)** define every tool with signature and behavior. Patterns shown in the exemplars are not repeated.
- **Section 14 (Prompts)** defines goal-oriented workflow templates.
- **Section 15 (Resources)** defines ambient context endpoints.
- **Section 16 (Migration)** maps prototype tool names to v1 names.
- **Section 17 (Dependencies)** tracks which tools are blocked by unbuilt subsystems.

## Status

in-progress

> **v2 revision (2026-05-02, in-progress):** Aligns MCP tool naming with the unified taxonomy in [`cli-restructure.md`](cli-restructure.md) v2. The convention is path-prefix-verb-suffix (`accounts_balance_list`), with cross-domain reports moving under a new `reports_*` namespace. v1 tool names existed before the cross-interface rule was settled and used noun-collection-as-list (`accounts_balances`) and standalone analytical domains (`spending_*`, `cashflow_*`, `tax_*`). v2 makes the verb explicit and groups analytical lenses under `reports`. Sync (9 tools) and transform (5 tools) gain MCP exposure under the v2 exposure principle. See [§16b. Rename Map (v1 → v2)](#16b-rename-map-v1--v2). Hard cut: no aliases. Implementation pass moves status `ready` → `in-progress`.

---

## 1. Conventions (quick reference)

Full definitions in [`mcp-architecture.md`](mcp-architecture.md). This section is a lookup aid.

### Response envelope

Every tool returns:

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
  "actions": ["Use reports_spending_by_category for category breakdown"]
}
```

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

### Namespace conventions (v2)

Path-prefix-verb-suffix. Tool names mirror the CLI hierarchy with underscores instead of spaces, ending in an explicit verb.

- **Pattern:** `<entity_or_domain>[_<sub_resource>]_<verb>`
- **Examples:** `accounts_list`, `accounts_balance_list`, `accounts_balance_assert`, `reports_networth_get`, `transactions_matches_confirm`, `reports_spending_summary`
- **Verbs:** `_list` (collection get), `_get` (single instance), `_assert`, `_confirm`, `_reject`, `_apply`, `_delete`, `_create`, `_update`, plus domain-natural verbs (`_reconcile`, `_run`, `_train`).
- **Pluralization:** singular for resource types (`balance`, `networth`, `category`); plural for relationship collections (`matches`); verb-suffix carries the list/single distinction so the noun stays consistent.
- **Encoding constraint:** lowercase ASCII with underscores, ≤64 chars (`^[a-zA-Z0-9_-]{1,64}$` per Anthropic and OpenAI MCP client regex).

This mirrors the CLI taxonomy in `cli-restructure.md` v2. A user who knows `accounts balance list` already knows `accounts_balance_list` and `GET /accounts/balances`.

### Service layer convention

Each namespace maps to a service class. Tools and CLI commands are thin wrappers — parameter validation and output formatting only, no business logic or SQL. Service methods return typed Python objects (dataclasses or Pydantic models).

### CLI convention

CLI mirrors MCP namespaces as command groups. `--output json` on any command returns the same response envelope as the MCP tool. Default output is human-readable (tables, summary lines, icons per `cli.md` rules).

### When CLI-only is justified (v2 MCP exposure principle)

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
- Orientation pointers — which tool to call to "get oriented" for both new (`system_status`) and returning (`reports_health`) sessions
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
| **`tools/list_changed` notifications** | core | ✅ shipped (off by default) | Wired for progressive disclosure but `MoneyBinSettings.mcp.progressive_disclosure` defaults `False` because client support is uneven (Claude Code honors it; Claude Desktop is unreliable). In default deployment the full tool surface is visible at connect; design new tools accordingly. See `mcp-architecture.md` §3 "Progressive disclosure" current-state callout. |
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

Reference: `lunchmoney-mcp` ships 1–500 caps on its bulk writes (`update_transactions_bulk`, `delete_transactions_bulk`); the cap convention here generalizes that to any list-typed parameter and removes the awkward "bulk vs single" distinction. MoneyBin's tool-name surface drops the "bulk" qualifier — see the §`transactions_categorize_apply` exemplar for the post-rename shape. Internal helpers (e.g., `BulkCategorizationResult` → `CategorizationResult`) carry the same rename; the sweep is part of the same PR that introduces this cap convention.

---

## 2. Exemplars

These three tools demonstrate every pattern in full detail. Subsequent namespace sections use a compact format and reference these for shared patterns.

### 2.1 `reports_spending_summary` — low sensitivity, time-series, no degradation

**Service layer**

```python
class SpendingService:
    def summary(
        self,
        months: int = 3,
        start_date: str | None = None,
        end_date: str | None = None,
        account_id: list[str] | None = None,
        detail: str = "standard",
    ) -> SpendingSummary: ...
```

`SpendingSummary` is a dataclass containing a list of `MonthlySpending` records (`period`, `income`, `expenses`, `net`, `transaction_count`) and summary metadata (total income, total expenses, date range).

**MCP tool**

- **Name:** `reports_spending_summary`
- **Description:** "Get income vs expense totals by month. Returns time-series data suitable for charting. Use `months` for recent history or `start_date`/`end_date` for a specific range."
- **Sensitivity:** `low` — returns aggregates only, no row-level data at any detail level.
- **Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `months` | `int` | `3` | Number of recent months to include |
| `start_date` | `str?` | — | ISO 8601 start date (overrides `months`) |
| `end_date` | `str?` | — | ISO 8601 end date |
| `account_id` | `list[str]?` | — | Filter to specific accounts |
| `detail` | `str` | `"standard"` | `summary` (totals only), `standard` (monthly breakdown), `full` (adds per-account splits) |

- **Response `data` shape:**

```json
[
  {"period": "2026-04", "income": 5200.00, "expenses": 3847.32, "net": 1352.68, "transaction_count": 87},
  {"period": "2026-03", "income": 5200.00, "expenses": 4102.15, "net": 1097.85, "transaction_count": 94},
  {"period": "2026-02", "income": 5200.00, "expenses": 3654.90, "net": 1545.10, "transaction_count": 78}
]
```

- **Degraded response:** N/A — this tool is `low` sensitivity and already returns aggregates. The response is identical regardless of consent status.
- **Actions:** `["Use reports_spending_by_category for category breakdown", "Use reports_spending_compare to compare periods"]`

**CLI command**

```
moneybin reports spending summary [--months 3] [--start-date DATE] [--end-date DATE]
                          [--account-id ID ...] [--detail standard] [--output json]
```

Default output is a table with period, income, expenses, net, and count columns. `--output json` returns the response envelope.

**Example response (full envelope):**

```json
{
  "summary": {
    "total_count": 3,
    "returned_count": 3,
    "has_more": false,
    "period": "2026-02 to 2026-04",
    "sensitivity": "low",
    "display_currency": "USD"
  },
  "data": [
    {"period": "2026-04", "income": 5200.00, "expenses": 3847.32, "net": 1352.68, "transaction_count": 87},
    {"period": "2026-03", "income": 5200.00, "expenses": 4102.15, "net": 1097.85, "transaction_count": 94},
    {"period": "2026-02", "income": 5200.00, "expenses": 3654.90, "net": 1545.10, "transaction_count": 78}
  ],
  "actions": [
    "Use reports_spending_by_category for category breakdown",
    "Use reports_spending_compare to compare periods"
  ]
}
```

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
- **Actions:** `["Use transactions_get with the next_cursor value to fetch the next page", "Use reports_spending_get for category breakdowns", "Use transactions_categorize_apply to categorize uncategorized transactions"]`

**CLI command**

```
moneybin transactions list [--account ID_OR_NAME] [--from DATE] [--to DATE]
                           [--category NAME] [--amount-min N] [--amount-max N]
                           [--description PATTERN] [--uncategorized]
                           [--limit 50] [--cursor TOKEN] [--output text|json]
```

Default output is a table with date, description, amount, category, and account columns. `--output json` returns the response envelope. `--account` is repeatable and resolves by account ID or fuzzy display-name match.

---

### 2.3 `transactions_categorize_apply` — write tool, batch semantics, paired read tool

**Service layer**

```python
class CategorizationService:
    def categorize_items(
        self,
        categorizations: list[Categorization],
        create_merchant_mappings: bool = True,
    ) -> CategorizationResult: ...
```

`Categorization` is a dataclass: `transaction_id`, `category`, `subcategory?`, `merchant_name?`. `CategorizationResult` contains applied/skipped/error counts and a list of error details.

When `create_merchant_mappings` is true, the service normalizes each transaction's description and creates a merchant mapping if one doesn't already exist. This is a side-effect of categorization, not a separate tool call — it's how the system learns.

**MCP tool**

- **Name:** `transactions_categorize_apply`
- **Description:** "Apply categories to multiple transactions at once. Pair with `transactions_categorize_pending_list` to fetch candidates first. Optionally auto-creates merchant mappings so future imports are categorized automatically."
- **Sensitivity:** `medium` — reads transaction descriptions to create merchant mappings.
- **Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `categorizations` | `list[object]` | (required) | List of `{transaction_id, category, subcategory?, merchant_name?}` |
| `create_merchant_mappings` | `bool` | `true` | Auto-create merchant mappings from descriptions |

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
- **Actions:** `["Use transactions_categorize_rules_list to review auto-created rules", "Use transactions_categorize_pending_list to fetch the next batch"]`

**CLI command**

```
moneybin transactions categorize apply --file categorizations.json [--no-merchant-mappings] [--output json]
```

The CLI accepts a JSON file (or stdin) since batch data doesn't work as flags. `--no-merchant-mappings` disables the auto-create side-effect. Default output is a summary line: "Applied 48, skipped 0, errors 2, merchants created 12."

---

## 3. `reports_spending_*` — Expense analysis (v2: moved into reports namespace)

**Service class:** `SpendingService`

### `reports_spending_summary`

*Exemplar — see section 2.1.*

### `reports_spending_by_category`

Income vs expense totals broken down by category for a period. Requires transactions to be categorized.

- **Sensitivity:** `low`
- **Unique parameters:** `top_n: int = 10` — limit to top N categories by total. `include_uncategorized: bool = true` — whether to include an "Uncategorized" rollup row.
- **Behavior:** Returns array of `{category, subcategory, total, transaction_count, percent_of_total}` sorted by total descending. At `detail=full`, includes per-month breakdown within each category.
- **Service:** `SpendingService.by_category() -> CategoryBreakdown`
- **CLI:** `moneybin reports spending by-category [--top-n 10] [--include-uncategorized]`

### `reports_spending_merchants`

Top merchants by spending for a period.

- **Sensitivity:** `medium` — merchant names are row-level data derived from transaction descriptions.
- **Unique parameters:** `top_n: int = 20`
- **Behavior:** Returns array of `{merchant_name, total, transaction_count, category, last_seen}`. Merchants without mappings appear by raw description. Degraded response returns category totals instead.
- **Service:** `SpendingService.merchants() -> MerchantBreakdown`
- **CLI:** `moneybin reports spending merchants [--top-n 20]`

### `reports_spending_compare`

Compare spending between two periods (month-over-month, year-over-year).

- **Sensitivity:** `low`
- **Unique parameters:** `period_a: str` (required, YYYY-MM), `period_b: str` (required, YYYY-MM).
- **Behavior:** Returns array of `{category, period_a_total, period_b_total, change_amount, change_percent}`. No `months`/`start_date`/`end_date` — this tool uses explicit period comparison. At `detail=summary`, returns only the overall totals for each period.
- **Service:** `SpendingService.compare() -> PeriodComparison`
- **CLI:** `moneybin reports spending compare --period-a 2026-03 --period-b 2026-04`

---

## 4. `reports_cashflow_*` — Money movement (v2: moved into reports namespace)

**Service class:** `CashflowService`

### `reports_cashflow_summary`

Net cash flow by period — income, outflows, and net position.

- **Sensitivity:** `low`
- **Unique parameters:** None beyond shared conventions.
- **Behavior:** Similar shape to `reports_spending_summary` but focused on the cash flow framing: `{period, inflows, outflows, net, running_balance}`. `running_balance` is cumulative net across the returned periods. Chart-ready time-series.
- **Service:** `CashflowService.summary() -> CashflowSummary`
- **CLI:** `moneybin reports cashflow summary`

### `reports_cashflow_income`

Income sources breakdown for a period.

- **Sensitivity:** `medium` — income source descriptions are row-level data.
- **Unique parameters:** `top_n: int = 10`
- **Behavior:** Returns array of `{source, total, transaction_count, frequency, last_seen}`. Groups by normalized description. Degraded response returns a single total income figure.
- **Service:** `CashflowService.income() -> IncomeBreakdown`
- **CLI:** `moneybin reports cashflow income [--top-n 10]`

---

## 5. `accounts.*` — Account management and per-account workflows

**Service class:** `AccountService`

### `accounts_list`

List all known accounts with type and institution.

- **Sensitivity:** `low` — account metadata only, no balances or numbers.
- **Unique parameters:** None.
- **Behavior:** Returns array of `{account_id, account_type, institution_name, source_type, currency}`. No pagination — account count is always small.
- **Service:** `AccountService.list() -> list[Account]`
- **CLI:** `moneybin accounts list`

### `accounts_balance_list`

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
- **Behavior:** Returns single account object with all fields including masked `routing_number` and `account_number` (e.g., `...1234`). Unmasked only in verified-local mode with `LOCAL_UNMASK_CRITICAL`. Degraded response returns the `accounts_list` view for that account (metadata only).
- **Service:** `AccountService.details() -> AccountDetail`
- **CLI:** `moneybin accounts show --account-id ID`

### `accounts_resolve`

Free-text → account-ID resolution for natural-language references.

- **Sensitivity:** `low` — returns account metadata only (display name, type, masked institution); no balances, no PII.
- **Annotations:** `read_only=True`, `idempotent=True`, `destructive=False`, `open_world=False` — all decorator defaults; no override needed.
- **Unique parameters:** `query: str` (the free-text fragment, e.g. "my Chase account", "checking", "schwab brokerage"); `limit: int = 5` (max alternatives returned).
- **Behavior:** Returns the best-match account plus alternatives, with confidence scores. Implemented via fuzzy match (`difflib.SequenceMatcher` already used for tabular account matching) over `display_name`, `account_subtype`, and `institution_name` from `core.dim_accounts`. Empty result returns `data=[]` with a `low_confidence` action hint; top match below a low-confidence threshold gets an action hint to verify with the user.
- **Why this exists:** without it, every conversation that starts with a natural account reference takes three turns — `accounts_list` → agent scans the result → agent picks an `account_id`. Single-tool resolution collapses that to one call. Agent ergonomic primitive, not a new capability.
- **Service:** `AccountService.resolve(query: str, limit: int = 5) -> list[AccountResolution]` in `src/moneybin/services/account_service.py`. The `AccountResolution` frozen dataclass (fields: `account_id`, `display_name`, `account_subtype`, `institution_name`, `confidence: float`, plus `to_dict()`) lives in the same module — convention parallel to other service-typed return objects. The MCP tool wraps results via `build_envelope(data=[r.to_dict() for r in resolutions], sensitivity="low", actions=[...])`.
- **CLI:** `moneybin accounts resolve "<query>"` (`--output json` returns the same envelope)
- **Reference:** `agigante80/actual-mcp-server` ships an analogous `actual_get_id_by_name` for the same agent-ergonomic reason.

### `reports_networth_get`

Net worth across all accounts over time.

- **Sensitivity:** `medium` — aggregate but reveals total financial position.
- **Unique parameters:** None beyond shared date/months conventions.
- **Behavior:** Returns time-series `{period, total_assets, total_liabilities, net_worth}` based on balance history. Requires balance data from OFX or Plaid. Degraded response returns latest snapshot only, no history.
- **Service:** `AccountService.net_worth() -> NetWorthSeries`
- **CLI:** `moneybin accounts net-worth`

---

## 6. `transactions.*` — Transaction-level operations (matches and categorize workflows nested)

**Service class:** `TransactionService` (search, correct, annotate, recurring), `MatchService` (matches sub-domain)

### `transactions_get`

Primary transaction read tool. Returns full transaction records with curation metadata.

- **Sensitivity:** `medium`
- **Parameters:** `accounts: list[str]?`, `date_from: str?`, `date_to: str?`, `categories: list[str]?`, `amount_min: str?` (decimal string e.g. `"-50.00"`), `amount_max: str?` (decimal string), `description: str?`, `uncategorized_only: bool = false`, `limit: int = 50`, `cursor: str?`
- **Behavior:** Reads from `core.fct_transactions`. `accounts` accepts exact account IDs or display names (resolved internally). `cursor` is an opaque pagination token from `next_cursor` in a previous response. Returns `list[Transaction]` with optional `notes`, `tags`, `splits` fields.
- **Sign convention:** negative = expense, positive = income.
- **Service:** `TransactionService.get() -> TransactionGetResult`
- **CLI:** `moneybin transactions list`
- **read_only:** true

### `transactions_correct`

Apply corrections to one or more transactions. Corrections are metadata overrides in the prep layer — the source record is preserved unchanged.

- **Sensitivity:** `medium`
- **Unique parameters:** `corrections: list[object]` (required) — list of `{transaction_id, field, original_value, corrected_value, reason?}`. `field` must be one of: `amount`, `date`, `description`.
- **Behavior:** Each correction creates a record in the corrections table. On next `sqlmesh run`, the prep layer applies corrections and core tables reflect the updated values. Returns `{applied, skipped, errors, error_details}`. Validates that `transaction_id` exists and `field` is in the allowlist.
- **Service:** `TransactionService.correct() -> CorrectionResult`
- **CLI:** `moneybin transactions correct --file corrections.json`
- **Dependency:** Corrections table schema (new).

### `transactions_annotate`

Add tags, notes, or cash breakdowns to transactions. Annotations are metadata — they don't create new transactions or modify amounts.

- **Sensitivity:** `medium`
- **Unique parameters:** `annotations: list[object]` (required) — list of `{transaction_id, tags?, note?, cash_breakdown?}`. `cash_breakdown` is a list of `{description, amount, category?}` that must sum to the transaction's amount.
- **Behavior:** Tags are additive (new tags merge with existing). Notes replace previous notes. Cash breakdowns replace previous breakdowns. Returns `{applied, skipped, errors, error_details}`.
- **Service:** `TransactionService.annotate() -> AnnotationResult`
- **CLI:** `moneybin transactions annotate --file annotations.json`
- **Dependency:** Annotations table schema (new).

### `transactions_recurring_list`

Detect recurring transactions (subscriptions, regular charges).

- **Sensitivity:** `medium` — returns merchant names and amounts.
- **Unique parameters:** `min_occurrences: int = 3` — minimum times a pattern must appear. `active_only: bool = true` — only show patterns with activity in the last 60 days.
- **Behavior:** Groups by normalized description and rounded amount. Returns array of `{description, merchant_name, avg_amount, frequency_days, occurrence_count, first_seen, last_seen, is_active}`. Degraded response returns count of recurring patterns and total monthly recurring spend without itemization.
- **Service:** `TransactionService.recurring() -> list[RecurringPattern]`
- **CLI:** `moneybin transactions recurring list [--min-occurrences 3] [--all]`

### `transactions_matches.*` — Transaction matching sub-domain

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

### `import_file`

Import a financial data file. Format detected automatically from extension (OFX/QFX, CSV, PDF/W-2, XLSX/XLS). CSV files require `account_id` and an existing format (see `import_list_formats`).

- **Sensitivity:** `medium` — import results reference transaction descriptions and amounts.
- **Unique parameters:** `file_path: str` (required), `account_id: str?` (required for CSV), `institution: str?` (override auto-detection).
- **Behavior:** Delegates to the appropriate extractor, loads to raw schema, triggers `sqlmesh run` to rebuild prep/core. Returns `{file_type, records_extracted, records_loaded, records_skipped_duplicate, accounts_affected, date_range}`. Validates path is within home directory.
- **Service:** `ImportService.import_file() -> ImportResult`
- **CLI:** `moneybin import file PATH [--account-id ID] [--institution NAME]`

### `import_status`

Show import history and data freshness per source.

- **Sensitivity:** `low` — metadata only (dates, counts, source types).
- **Unique parameters:** None.
- **Behavior:** Returns array of `{source_type, source_file, imported_at, record_count, date_range_start, date_range_end}` sorted by most recent.
- **Service:** `ImportService.status() -> list[ImportRecord]`
- **CLI:** `moneybin import status`

### `import_folder`

Batch import a directory of mixed file types.

- **Sensitivity:** `medium`
- **Unique parameters:** `folder_path: str` (required), `account_id: str?` (applied to CSV files), `recursive: bool = false`.
- **Behavior:** Scans directory for supported file types, imports each. Returns per-file results: `{file, status, records, error?}`. Files that fail don't block others.
- **Service:** `ImportService.import_folder() -> list[ImportResult]`
- **CLI:** `moneybin import folder PATH [--account-id ID] [--recursive]`
- **Dependency:** Smart Import Pillar A.

### `import_file_preview`

Preview a tabular file's headers and sample rows before importing. Format-agnostic (CSV, Excel, etc.).

- **Sensitivity:** `low` — structural metadata only.
- **Unique parameters:** `file_path: str` (required).
- **Behavior:** Returns `{file_name, headers, column_count, sample_rows}` with 3 sample rows as dicts keyed by header. Does not import or modify anything.
- **Service:** `ImportService.file_preview() -> FilePreview`
- **CLI:** `moneybin import file-preview PATH`

### `import_list_formats`

List available tabular import formats.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns array of `{name, institution_name, file_type, sign_convention, date_format, times_used, last_used_at, source}` for all built-in and user-saved formats.
- **Service:** `ImportService.list_formats() -> list[FormatSummary]`
- **CLI:** `moneybin import formats list`

### `import_ai_preview`

Preview what data would be sent to AI for parsing, with redaction applied.

- **Sensitivity:** `low` — shows redacted preview, not raw financial data.
- **Unique parameters:** `file_path: str` (required).
- **Behavior:** Returns `{file_name, backend, redacted_preview, fields_to_extract}`. Shows exactly what leaves the machine if the user confirms. Does not send anything.
- **Service:** `ImportService.ai_preview() -> AIParsePreview`
- **CLI:** `moneybin import ai-preview PATH`
- **Dependency:** Smart Import Pillar F + Privacy framework.

### `import_ai_parse`

Confirm and execute AI-assisted parsing for a file.

- **Sensitivity:** `medium` — sends redacted file content to configured AI backend.
- **Unique parameters:** `file_path: str` (required), `backend: str?` (override configured backend).
- **Behavior:** Requires explicit consent (per-file, not persistent). Sends redacted content to AI backend, receives column mapping or extracted data, imports the result. Returns standard `ImportResult` plus `{backend_used, fields_extracted}`.
- **Service:** `ImportService.ai_parse() -> AIParseResult`
- **CLI:** `moneybin import ai-parse PATH [--backend NAME]`
- **Dependency:** Smart Import Pillar F + Privacy framework.

---

## 8. `transactions_categorize_*`, `categories_*`, `merchants_*` — Categorization pipeline + reference data (v2: split into workflow + taxonomy + merchant mapping namespaces)

**Service class:** `CategorizationService`

### `transactions_categorize_pending_list`

Fetch transactions that haven't been categorized yet. The read side of the categorize-then-apply workflow.

- **Sensitivity:** `medium` — returns transaction descriptions and amounts.
- **Unique parameters:** `suggest: bool = false` — when true, include AI-suggested categories based on merchant mappings and existing rules (does not apply them).
- **Behavior:** Returns array of `{transaction_id, date, amount, description, account_id, suggested_category?, suggested_subcategory?, suggestion_source?}`. Degraded response returns uncategorized count by account and time period.
- **Service:** `CategorizationService.uncategorized() -> TransactionSearchResult`
- **CLI:** `moneybin transactions categorize pending [--suggest] [--limit 50]`

### `transactions_categorize_apply`

*Exemplar — see section 2.3.*

### `transactions_categorize_rules_list`

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

### `transactions_categorize_rule_delete`

Delete a categorization rule.

- **Sensitivity:** `low`
- **Unique parameters:** `rule_id: str` (required).
- **Behavior:** Deletes the rule. Returns confirmation with the deleted rule's name.
- **Service:** `CategorizationService.delete_rule() -> DeleteResult`
- **CLI:** `moneybin transactions categorize rules delete --rule-id ID`

### `merchants_list`

List merchant name mappings.

- **Sensitivity:** `low` — mapping patterns, not financial data.
- **Unique parameters:** None.
- **Behavior:** Returns array of `{merchant_id, raw_pattern, match_type, canonical_name, category, subcategory, created_by}`.
- **Service:** `CategorizationService.merchants() -> list[MerchantMapping]`
- **CLI:** `moneybin merchants list`

### `merchants_create`

Create one or more merchant name mappings.

- **Sensitivity:** `low`
- **Unique parameters:** `mappings: list[object]` (required) — list of `{raw_pattern, canonical_name, match_type?, category?, subcategory?}`.
- **Behavior:** Returns `{created, skipped, errors, error_details}`.
- **Service:** `CategorizationService.create_merchants() -> CreateResult`
- **CLI:** `moneybin merchants create --file mappings.json`

### `categories_list`

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

### `categories_toggle`

Enable or disable a category.

- **Sensitivity:** `low`
- **Unique parameters:** `category_id: str` (required), `is_active: bool` (required).
- **Behavior:** Toggles the active flag. Existing categorizations are preserved.
- **Service:** `CategorizationService.toggle_category() -> ToggleResult`
- **CLI:** `moneybin categories toggle --category-id ID --active/--inactive`

### `transactions_categorize_stats`

Categorization coverage statistics.

- **Sensitivity:** `low` — counts and percentages only.
- **Unique parameters:** None.
- **Behavior:** Returns `{total_transactions, categorized, uncategorized, percent_categorized, by_source}` where `by_source` breaks down by categorization source (user, rule, ai, plaid).
- **Service:** `CategorizationService.stats() -> CategorizationStats`
- **CLI:** `moneybin transactions categorize stats`

### `transactions_categorize_rules_apply`

Run the rule engine against uncategorized transactions.

- **Sensitivity:** `low`
- **Unique parameters:** `dry_run: bool = false` — preview what would be categorized without applying.
- **Behavior:** Applies active rules in priority order to uncategorized transactions. Returns `{applied, skipped, already_categorized}`. With `dry_run`, returns the proposed categorizations without applying them.
- **Service:** `CategorizationService.apply_rules() -> RuleApplicationResult`
- **CLI:** `moneybin transactions categorize rules apply [--dry-run]`
- **Dependency:** Categorization umbrella spec.

### `transactions_categorize_auto_review`

List auto-generated rules pending user approval.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns array of `{proposed_rule_id, merchant_pattern, category, subcategory, source, trigger_count, sample_transactions}` where `source` indicates how the rule was generated (ml, pattern_detection).
- **Service:** `CategorizationService.auto_review() -> list[ProposedRule]`
- **CLI:** `moneybin transactions categorize auto review`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar E: auto-rule generation), [Auto-rule generation](categorization-auto-rules.md).

### `transactions_categorize_auto_confirm`

Approve or reject proposed auto-generated rules.

- **Sensitivity:** `low`
- **Unique parameters:** `approvals: list[object]` (required) — list of `{proposed_rule_id, action}` where action is `approve` or `reject`.
- **Behavior:** Approved rules are promoted to active categorization rules in `app.categorization_rules` with `created_by='auto_rule'` and immediately evaluated against uncategorized transactions. Rejected rules are not re-proposed for the same pattern. Returns `{approved, rejected, errors}`.
- **Service:** `CategorizationService.auto_confirm() -> ActionResult`
- **CLI:** `moneybin transactions categorize auto confirm --approve <id> [<id>...] --reject <id> [<id>...]`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar E: auto-rule generation), [Auto-rule generation](categorization-auto-rules.md).

### `transactions_categorize_auto_stats`

Auto-rule health metrics.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns `{active_rules, pending_proposals, rejected_proposals, override_rate, top_rules}` where `top_rules` is an array of the most-matched auto-rules with match counts. `override_rate` is the percentage of auto-rule categorizations that were later overridden by the user.
- **Service:** `CategorizationService.auto_stats() -> AutoRuleStats`
- **CLI:** `moneybin transactions categorize auto stats`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar E: auto-rule generation), [Auto-rule generation](categorization-auto-rules.md).

### `transactions_categorize_ml_status`

ML model status and accuracy metrics.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns `{status, last_trained, training_samples, accuracy, confidence_distribution}` where `status` is `untrained`, `training`, or `ready`. `confidence_distribution` shows how many transactions fall in each confidence tier (high/moderate/low). Confidence scores are Platt-calibrated probabilities; user-facing output uses qualitative tiers (see [categorization overview](categorization-overview.md) Progressive Confidence Disclosure).
- **Service:** `CategorizationService.ml_status() -> MLModelStatus`
- **CLI:** `moneybin transactions categorize ml status`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar D: ML categorization).

### `transactions_categorize_ml_train`

Trigger model training or retraining on current categorization history.

- **Sensitivity:** `low` — training data stays local.
- **Unique parameters:** None.
- **Behavior:** Trains the model synchronously. Returns `{status, training_samples, accuracy, duration_seconds}`. Requires minimum number of categorized transactions to produce a useful model. Note: the pipeline auto-retrains when statistically significant new categorizations exist (see [categorization overview](categorization-overview.md)); this tool is a manual escape hatch.
- **Service:** `CategorizationService.ml_train() -> MLTrainResult`
- **CLI:** `moneybin transactions categorize ml train`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar D: ML categorization).

### `transactions_categorize_ml_apply`

Run ML categorization at a given confidence threshold.

- **Sensitivity:** `low`
- **Unique parameters:** `min_confidence: float = 0.9` — only apply predictions above this threshold. `dry_run: bool = false`.
- **Behavior:** Applies ML predictions to uncategorized transactions above the confidence threshold. Returns `{applied, below_threshold, already_categorized}`. With `dry_run`, returns predictions without applying.
- **Service:** `CategorizationService.ml_apply() -> MLApplyResult`
- **CLI:** `moneybin transactions categorize ml apply [--min-confidence 0.9] [--dry-run]`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar D: ML categorization).

---

## 9. `budget_*` (mutation) and `reports_budget_*` (vs-actual reads) — Budget tracking (v2: split)

**Service class:** `BudgetService`

### `budget_set`

Create or update a monthly budget for a category.

- **Sensitivity:** `low` — budget targets are user-authored metadata, not financial data.
- **Unique parameters:** `category: str` (required), `monthly_amount: float` (required), `start_month: str?` (YYYY-MM, defaults to current month), `end_month: str?` (open-ended if omitted).
- **Behavior:** Upserts — if an active budget exists for the category, updates the amount. Returns the created/updated budget with its ID.
- **Service:** `BudgetService.set() -> Budget`
- **CLI:** `moneybin budget set --category NAME --amount N [--start-month YYYY-MM]`

### `reports_budget_status`

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

**Service class:** `TaxService`

### `tax_w2`

W-2 tax form data for one or all years.

- **Sensitivity:** `high` — contains SSN-adjacent data (EIN, wages, withholdings).
- **Unique parameters:** `tax_year: int?` (omit for all years).
- **Behavior:** Returns array of `{tax_year, employer_name, employer_ein, wages, federal_income_tax, social_security_wages, social_security_tax, medicare_wages, medicare_tax, state_local_info}`. EIN is masked for cloud backends. Degraded response returns year and employer name only with total wages as an aggregate.
- **Service:** `TaxService.w2() -> list[W2Summary]`
- **CLI:** `moneybin tax w2 [--year 2025]`

### `tax_deductions`

Search transactions for potentially deductible expenses.

- **Sensitivity:** `medium` — row-level transaction data.
- **Unique parameters:** `tax_year: int` (required), `categories: list[str]?` (filter to specific categories, e.g., `["Charitable Donations", "Medical"]`).
- **Behavior:** Returns transactions in deduction-relevant categories for the tax year, with category totals. Includes a disclaimer that this is informational, not tax advice. Degraded response returns category totals only.
- **Service:** `TaxService.deductions() -> DeductionSearchResult`
- **CLI:** `moneybin tax deductions --year 2025 [--categories "Charitable Donations,Medical"]`
- **Note:** v1 filters by category name pattern. Future enhancement: deduction-relevant category flags.

---

## 11. `privacy.*` — Privacy & consent

**Service class:** `PrivacyService`

**Dependency:** All `privacy.*` tools depend on the consent management spec, audit log spec, and provider profiles spec. They ship with stubbed behavior until those specs are implemented.

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

## 12. `system_*` and `reports_health` — Data status meta-view + financial health snapshot (v2: split from `overview.*`)

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
- **CLI:** `moneybin doctor [--verbose] [--output json]`

### `reports_health`

Financial health snapshot — high-level summary across all domains.

- **Sensitivity:** `low` — aggregates only.
- **Unique parameters:** `months: int = 1` (period to summarize).
- **Behavior:** Returns `{net_worth, monthly_income, monthly_expenses, monthly_net, savings_rate, top_spending_categories, budget_compliance, recurring_total}`. Designed as a conversation opener — gives the AI enough context to ask informed follow-up questions.
- **Service:** `OverviewService.health() -> FinancialHealth`
- **CLI:** `moneybin reports health [--months 1]`

---

## 13. `sql.*` — Direct SQL access

`sql_query` uses `get_db()` directly with query validation from the privacy module. No dedicated service class — this is a power-user escape hatch, not a structured service.

### `sql_query`

Execute an arbitrary read-only SQL query against DuckDB.

- **Sensitivity:** `medium` — can return any row-level data from core tables.
- **Unique parameters:** `sql: str` (required).
- **Behavior:** Validates query is read-only (SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN). Blocks file-access functions (`read_csv`, `read_parquet`, etc.) and URL literals. Results capped at `MAX_ROWS` and `MAX_CHARS`. Returns results in the standard response envelope with column names as field keys. Degraded response rejects the query with a consent instruction — arbitrary SQL can't be meaningfully degraded to aggregates.
- **CLI:** `moneybin sql query "SELECT ..." [--output json]`

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

**Relevant tools:** `reports_spending_summary`, `reports_spending_by_category`, `reports_spending_compare`, `reports_cashflow_summary`, `reports_budget_status`, `transactions_recurring_list`, `reports_health`

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

**Relevant tools:** `transactions_categorize_stats`, `categories_list`, `transactions_categorize_pending_list`, `transactions_categorize_apply`, `transactions_categorize_rules_create`, `merchants_create`, `categories_create`

**Guardrails:**

- Defaults are seeded automatically by `db init`; no MCP-side seed step
- Fetch uncategorized transactions in manageable batches (50)
- Always use batch tools, never single-item equivalents
- Present proposed categorizations to the user for confirmation before applying
- After applying, propose merchant mappings and rules for patterns that appeared multiple times
- Track progress: "X of Y categorized, Z remaining"
- If `transactions_categorize_ml_status` shows a trained model, use `suggest=true` to leverage ML suggestions
- Stop when the user says stop, not when the queue is empty

**Decision points:** User confirms each batch of categorizations before `transactions_categorize_apply` is called. User confirms proposed rules before `transactions_categorize_rules_create` is called.

### `onboarding` (Setup)

**Goal:** Guide a first-time user from empty database to imported, transformed, and categorized data.

**Relevant tools:** `system_status`, `import_file`, `import_file_preview`, `import_list_formats`, `transactions_categorize_stats`

**Guardrails:**

- Start by checking `system_status` — if data already exists, acknowledge and ask what the user wants to do next rather than re-running onboarding
- Ask the user for file paths — don't assume locations
- For tabular files, guide through the format creation flow if auto-detection fails
- After import, explain what happened (records loaded, accounts discovered) and what's available next
- Seed categories and mention categorization as a natural next step, but don't force it
- Keep the tone welcoming, not overwhelming — this is a first impression

**Decision points:** User provides file paths. User confirms column mappings. User decides whether to proceed to categorization.

### `tax-prep` (Review)

**Goal:** Gather tax-relevant financial information for a tax year — W-2 data, deductible expenses, income summary.

**Parameters:** `tax_year: str` (defaults to prior year).

**Relevant tools:** `tax_w2`, `tax_deductions`, `reports_spending_by_category`, `reports_cashflow_income`, `transactions_get`

**Guardrails:**

- Always include the disclaimer: informational summary, not tax advice, consult a tax professional
- Start with W-2 data if available — wages, withholdings, employer info
- Summarize income sources beyond W-2 (interest, dividends, side income) if visible in transactions
- Search for potentially deductible expenses by category (charitable, medical, business)
- If multiple W-2s exist, show both individual and combined totals
- Highlight data gaps: "No W-2 data found for 2025" or "Medical expenses may be incomplete if not all accounts are imported"
- Do not attempt to calculate tax liability or suggest filing strategies

**Decision points:** None — read-only analysis. User decides what to share with their tax professional.

---

## 15. Resources

Four ambient context endpoints loaded when the AI connects. Resources provide background context the AI needs to make informed first tool calls. They are read-only, compact, and change infrequently.

### `moneybin://status`

Data freshness dashboard. Contains: row counts per source, date ranges, last import timestamp, categorization coverage percentage, pending match count. Lets the AI know what data exists without a tool call.

### `moneybin://accounts`

Account list with types, institutions, and currencies. Lets the AI reference accounts by name and filter by type without calling `accounts_list` first. Excludes balances and account numbers.

### `moneybin://privacy`

Active consent grants, configured AI backend (name, type, is_local), consent mode. Lets the AI know what sensitivity tiers are available before hitting a consent wall. Ships with static defaults (no grants, no backend configured) until consent infrastructure lands.

### `moneybin://schema`

Core and app table schemas with column names, types, and descriptions. Lets the AI write accurate SQL for `sql_query` without calling a discovery tool first.

### `accounts://summary`

Cross-account summary: list of accounts with display name, type, institution, currency, include_in_net_worth flag, archived status, and last known balance. Mirrors the `accounts_summary` tool response — available as ambient context so the AI can reference accounts without an extra tool call.

### `net-worth://summary`

Current net worth snapshot: total net worth, assets vs liabilities breakdown, and per-account balance contributions. Refreshed on each connection. Lets the AI answer "what's my net worth?" from ambient context without calling `reports_networth_get`.

### `moneybin://tools`

Available tool namespaces with one-line descriptions, tool counts, and loaded/unloaded status. Lets the AI know what capabilities exist without seeing every tool schema. Example:

```json
{
  "core": [
    {"namespace": "overview", "tools": 2, "loaded": true, "description": "Data status and financial health snapshot"},
    {"namespace": "spending", "tools": 4, "loaded": true, "description": "Expense analysis, trends, category breakdowns"},
    {"namespace": "accounts", "tools": 4, "loaded": true, "description": "Account listing, balances, net worth"}
  ],
  "extended": [
    {"namespace": "categorize", "tools": 15, "loaded": false, "description": "Rules, merchant mappings, categorization, auto-rule review, ML"},
    {"namespace": "budget", "tools": 4, "loaded": false, "description": "Budget targets, status, rollovers"},
    {"namespace": "tax", "tools": 2, "loaded": false, "description": "W-2 data, deductible expense search"}
  ],
  "discover_tool": "moneybin_discover"
}
```

Updated dynamically as namespaces are loaded during a session.

---

## 15b. `moneybin_discover` — Namespace discovery meta-tool

Always registered regardless of namespace configuration. Enables progressive disclosure of the full tool surface without overwhelming the AI's context at connection time. See `mcp-architecture.md` §3 "Progressive disclosure via namespace registration" for the design rationale.

- **Name:** `moneybin_discover`
- **Description:** "Load tools from a namespace. Call this when you need capabilities not in the currently loaded tools. Use the moneybin://tools resource to see available namespaces."
- **Sensitivity:** `low` — no financial data, just tool metadata.
- **Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `namespace` | `str` | (required) | Namespace to load (e.g., `categorize`, `budget`, `tax`, `matches`) |

- **Behavior:** Registers all tools in the requested namespace, sends `tools/list_changed` notification, and returns tool names with descriptions so the AI can immediately use them. Idempotent — calling discover on an already-loaded namespace returns the tool list without side effects.
- **Response `data` shape:**

```json
{
  "namespace": "categorize",
  "tools_loaded": [
    {"name": "transactions_categorize_pending_list", "description": "Fetch transactions that haven't been categorized yet"},
    {"name": "transactions_categorize_apply", "description": "Apply categories to multiple transactions at once"},
    {"name": "transactions_categorize_rules_list", "description": "List active categorization rules"}
  ],
  "already_loaded": false
}
```

- **CLI equivalent:** N/A — the CLI has all commands available via `--help`. Progressive disclosure is an MCP-specific optimization for AI context management.

---

## 16. Migration from prototype

Clean break — old tool names stop working when v1 ships. MoneyBin is pre-1.0; breaking changes are expected.

| Prototype tool | v1 equivalent | Notes |
|---|---|---|
| `list_tables` | `moneybin://schema` resource | Tool → resource |
| `describe_table` | `moneybin://schema` resource | Tool → resource |
| `list_accounts` | `accounts_list` | |
| `get_account_balances` | `accounts_balances` | |
| `query_transactions` | `transactions_get` | Richer filters, curation fields, cursor pagination |
| `get_w2_summary` | `tax_w2` | |
| `list_categories` | `categorize_categories` | |
| `list_categorization_rules` | `categorize_rules` | |
| `list_merchants` | `categorize_merchants` | |
| `get_categorization_stats` | `categorize_stats` | |
| `list_institutions` | `accounts_list` | Institution is a field on account |
| `run_read_query` | `sql_query` | |
| `import_file` | `import_file` | |
| `categorize_transaction` | `categorize_apply` | Single-item removed; use list of one |
| `get_uncategorized_transactions` | `categorize_uncategorized` | |
| `seed_categories` | _removed_ | Seeds run automatically via `db init` / `transform seed` |
| `toggle_category` | `categorize_toggle_category` | |
| `create_category` | `categorize_create_category` | |
| `create_merchant_mapping` | `categorize_create_merchants` | Single → bulk |
| `create_categorization_rule` | `categorize_create_rules` | Single → bulk |
| `delete_categorization_rule` | `categorize_delete_rule` | |
| `bulk_categorize` | `categorize_apply` | |
| `bulk_create_categorization_rules` | `categorize_create_rules` | |
| `bulk_create_merchant_mappings` | `categorize_create_merchants` | |
| `set_budget` | `budget_set` | |
| `get_budget_status` | `budget_status` | |
| `get_monthly_summary` | `spending_summary` | |
| `get_spending_by_category` | `spending_by_category` | |
| `find_recurring_transactions` | `transactions_recurring` | |
| `csv_preview_file` | `import_csv_preview` | |
| `csv_list_profiles` | `import_list_formats` | |
| `csv_save_profile` | Absorbed into `import_file` via `save_format` flag | |

### Prototype prompts

All prototype prompts are replaced by the four v1 prompts. The prototype's step-by-step prompts are superseded by goal-oriented templates.

### Prototype resources

| Prototype resource | v1 equivalent | Notes |
|---|---|---|
| `moneybin://schema/tables` | `moneybin://schema` | Consolidated |
| `moneybin://schema/{table_name}` | `moneybin://schema` | Consolidated |
| `moneybin://accounts/summary` | `moneybin://accounts` | Simplified |
| `moneybin://transactions/recent` | Removed | Too dynamic for ambient context; use `transactions_get` |
| `moneybin://w2/{tax_year}` | Removed | Parameterized data belongs as a tool (`tax_w2`) |

---

## 16b. Rename Map (v1 → v2)

Per [`cli-restructure.md`](cli-restructure.md) v2. Hard cut: rename in place, no aliases. Update the tool registry, regenerate `mcp install` output for all client configs, and update any AI agent prompts or external references.

### `accounts_*`

| v1 | v2 | Notes |
|---|---|---|
| `accounts_list` | `accounts_list` | Unchanged |
| `accounts_details` | `accounts_get` | Single-instance get |
| `accounts_balances` | `accounts_balance_list` | Singular type + explicit `_list` |
| `accounts_networth` | `reports_networth_get` | **Moves to `reports_*`** — cross-domain rollup (accounts + assets), no longer scoped to `accounts` namespace |
| (new — `net-worth.md`) | `accounts_balance_assert` | |
| (new) | `accounts_balance_history` | |
| (new) | `accounts_balance_reconcile` | |
| (new) | `accounts_balance_assertions_list` | |
| (new) | `accounts_balance_assertion_delete` | |
| (new) | `reports_networth_history` | Moved out of `accounts_*` |
| (new — `account-management.md`) | `accounts_summary` | Cross-account summary view |
| (new) | `accounts_rename` | Rename an account |
| (new) | `accounts_include` | Toggle include_in_net_worth |
| (new) | `accounts_archive` | Mark archived; cascades exclude from net worth |
| (new) | `accounts_unarchive` | Clear archived flag |
| (new) | `accounts_settings_update` | Metadata update (subtype, holder category, currency, credit limit, last four, official name) |

### `transactions_*` (entity ops)

| v1 | v2 | Notes |
|---|---|---|
| `transactions_get` | `transactions_get` | Verb already trailing |
| `transactions_correct` | `transactions_correct` | Verb already trailing |
| `transactions_annotate` | `transactions_annotate` | Verb already trailing |
| `transactions_recurring` | `transactions_recurring_list` | Add explicit `_list` |
| (new) | `transactions_review_status` | Returns counts of both review queues (matches pending + categorize pending). Orientation tool for "anything to review?" check; AI then calls `transactions_matches_pending` or `transactions_categorize_pending_list` to fetch items |

### `transactions_matches_*` (workflow under transactions)

| v1 | v2 | Notes |
|---|---|---|
| `transactions_matches_pending` | `transactions_matches_pending` | Unchanged — kept specialized; review queue is the dominant call. See judgment call #1 |
| `transactions_matches_confirm` | `transactions_matches_confirm` | Unchanged. CLI v2 also uses `matches confirm` for symmetry — see judgment call #2 |
| `transactions_matches_reject` | `transactions_matches_reject` | Unchanged |
| `transactions_matches_revoke` | `transactions_matches_undo` | Align verb with CLI (`matches undo`) |
| `transactions_matches_log` | `transactions_matches_log` | Noun "log" reads as the resource; keep |
| `transactions_matches_run` | `transactions_matches_run` | Unchanged |

### `transactions_categorize_*` (was `categorize_*` workflow + rules + auto + ml)

Workflow tools that operate on transaction state stay nested under `transactions_categorize_*`. Reference-data tools (categories, merchants) split into their own top-level groups — see `categories_*` and `merchants_*` below.

| v1 | v2 |
|---|---|
| `categorize_uncategorized` | `transactions_categorize_pending_list` |
| `categorize_bulk` | `transactions_categorize_apply` |
| `categorize_apply_rules` | `transactions_categorize_rules_apply` |
| `categorize_rules` | `transactions_categorize_rules_list` |
| `categorize_create_rules` | `transactions_categorize_rules_create` |
| `categorize_delete_rule` | `transactions_categorize_rule_delete` |
| `categorize_stats` | `transactions_categorize_stats` |
| `categorize_auto_review` | `transactions_categorize_auto_review` |
| `categorize_auto_confirm` | `transactions_categorize_auto_confirm` |
| `categorize_auto_stats` | `transactions_categorize_auto_stats` |
| `categorize_ml_status` | `transactions_categorize_ml_status` |
| `categorize_ml_train` | `transactions_categorize_ml_train` |
| `categorize_ml_apply` | `transactions_categorize_ml_apply` |

### `categories_*` (new top-level — taxonomy reference data)

Categories are reference data that transactions reference, not a workflow on transactions. Promoted to a top-level entity group so taxonomy management is independent of the categorization workflow.

| v1 | v2 |
|---|---|
| `categorize_categories` | `categories_list` |
| `categorize_create_category` | `categories_create` |
| `categorize_toggle_category` | `categories_toggle` |
| (new) | `categories_delete` |

### `merchants_*` (new top-level — merchant mapping reference data)

Merchants are reference data (canonical names + default categories). Same logic as categories: promoted to a top-level entity group.

| v1 | v2 |
|---|---|
| `categorize_merchants` | `merchants_list` |
| `categorize_create_merchants` | `merchants_create` |

### `reports_*` (new namespace — cross-domain analytical and aggregation views)

`spending_*` and `cashflow_*` tools move under `reports_*`. Read-only `budget_*` tools also move; mutation `budget_*` tools stay top-level. `accounts_networth_*` moves here too — net worth aggregates across accounts + assets, so it's structurally a cross-domain report.

`tax_*` does **not** move here — see "Unchanged top-level domains" below.

| v1 | v2 |
|---|---|
| `spending_summary` | `reports_spending_summary` |
| `spending_by_category` | `reports_spending_by_category` |
| `spending_merchants` | `reports_spending_merchants` |
| `spending_compare` | `reports_spending_compare` |
| `cashflow_summary` | `reports_cashflow_summary` |
| `cashflow_income` | `reports_cashflow_income` |
| `budget_status` | `reports_budget_status` |
| `budget_summary` | `reports_budget_summary` |
| `accounts_networth` (v1: `accounts_networth`, then proposed `accounts_networth_get`) | `reports_networth_get` |
| (new) | `reports_networth_history` |
| (new) | `reports_health` (was `overview_health`) |

### `budget_*` (mutation, stays top-level)

| v1 | v2 |
|---|---|
| `budget_set` | `budget_set` |
| `budget_delete` | `budget_delete` |

### `system_*` and split of `overview_*`

The v1 `overview_*` namespace bundled two conceptually different tools:
- `overview_status` — operational meta-view: data freshness, counts, pending queues
- `overview_health` — analytical financial summary (net worth + spending + budget compliance)

v2 splits them by their actual content. Status is a system meta-view; health is a cross-domain financial report.

| v1 | v2 | Rationale |
|---|---|---|
| `overview_status` | `system_status` | Operational/system meta — distinct from analytical reports |
| `overview_health` | `reports_health` | Cross-domain financial summary — sibling of `reports_spending_summary` |

`system_*` is a new top-level namespace for system meta-information. Currently a single tool; future system observability or diagnostics tools can land here.

### Unchanged top-level domains

| Namespace | Tools | Rationale |
|---|---|---|
| `import_*` | 6 of 7 unchanged; `import_csv_preview` → `import_file_preview` (format-agnostic) | Pipeline action, not entity-scoped |
| `privacy_*` | All 4 privacy tools unchanged | Cross-cutting consent/audit domain |
| `sql_*` | `sql_query` unchanged | Infrastructure escape hatch |
| `tax_*` | `tax_w2`, `tax_deductions` unchanged | Tax is its own domain — workflow that crosses spending, income, deductions, forms. Future tools (`tax_1099`, `tax_capital_gains`, `tax_estimate`) land here. |

### `assets_*` (new top-level — physical assets)

Reserved namespace for `asset-tracking.md`. Workflows (registration, valuation, liability linking, staleness) defined there. Initial v2 implementation creates the namespace with stubs only; full tool surface lands when `asset-tracking.md` is implemented.

### `sync_*` (new MCP exposure — was CLI-only)

Per the v2 MCP exposure principle, sync becomes nearly fully MCP-exposed. The AI may want to proactively pull recent data, check sync status, or initiate a connection — these are legitimate user-workflow operations. OAuth flows return redirect URLs; the client opens them.

| v2 tool | Behavior |
|---|---|
| `sync_login` | Initiates device-code flow with moneybin-server. Returns `{device_code, user_code, verification_url, polling_token}`. Client displays URL + code; tool polls until completion |
| `sync_logout` | Clears stored JWT |
| `sync_connect [--institution]` | Initiates OAuth with bank/aggregator. Returns redirect URL; tool polls for completion |
| `sync_disconnect <institution>` | Removes institution; idempotent |
| `sync_pull [--institution] [--force]` | Triggers sync for one or all institutions; runs full pipeline |
| `sync_status` | Read-only: connected institutions, last-sync times, errors |
| `sync_schedule_set --time HH:MM` | Installs daily sync (launchd/cron); writes scheduler entry |
| `sync_schedule_show` | Read-only schedule details |
| `sync_schedule_remove` | Uninstalls scheduled job |

**CLI-only (security-justified):** `sync_rotate_key` — passphrase material through LLM context window is a security model violation.

### `transform_*` (new MCP exposure — was CLI-only)

Routine pipeline operations the AI legitimately needs (e.g., ensure pipeline is up to date after a manual data change). Full surface except `_restate`.

| v2 tool | Behavior |
|---|---|
| `transform_status` | Current model state, environment |
| `transform_plan` | Preview pending SQLMesh changes (read-only) |
| `transform_validate` | Check model SQL parses and resolves |
| `transform_audit` | Run data quality assertions |
| `transform_apply` | Execute SQLMesh changes |

**CLI-only (operator-territory):** `transform_restate` — destructive force-recompute for a date range, used for bug fixes / late-data backfill / schema reinterpretation. Power-user / data-engineering territory; preceded by code changes the AI doesn't drive.

### Judgment calls flagged for review

A few renames make minor judgment calls. Flagged here so they can be revisited if needed:

1. **`transactions_matches_pending` → `transactions_matches_list` + `status` param.** Consolidates a status-filter into a parameter. Alternative: keep `_pending` as a specialized tool. Pro-consolidation: matches the v2 rule (one list operation per sub-resource). Pro-specialized: `pending` is the dominant query in practice and a dedicated tool reads cleaner.
2. **`transactions_matches_confirm` stays `_confirm`; CLI verb aligned to `confirm`.** Resolved. Both interfaces use `confirm` because it more accurately describes ratifying a system-proposed match (vs. picking from options).
3. **`overview_*` split into `system_status` and `reports_health`.** v1 bundled operational meta and financial summary under one namespace. v2 separates them by content type. Resolved.
4. **`tax_*` stays its own top-level domain.** Resolved. Tax is a workflow that crosses spending, income, deductions, and forms; future tools (1099, capital gains, estimates, carryforward) need a stable home. Two tools today, but the namespace is reserved for natural growth.
5. **`categorize_uncategorized` → `transactions_categorize_pending_list`.** Resolved. Symmetry with `transactions_matches_pending` — both are review queues, both read the same shape.

---

## 17. Dependency tracker

Tools that depend on unbuilt subsystems are included in the full catalog with dependency markers. They ship with stub or no-op behavior until their dependency is implemented.

| Dependency | Status | Blocked tools |
|---|---|---|
| **Consent management spec** | Not written | `privacy_grant`, `privacy_revoke`, `privacy_status`; all degraded response behavior across the surface |
| **Audit log spec** | Not written | `privacy_audit`; audit logging in middleware |
| **Redaction engine spec** | Not written | `accounts_get` field masking; `high` sensitivity behavior |
| **Provider profiles spec** | Not written | Verified-local bypass; `privacy_status` backend info |
| **Transaction matching (Pillars A+C)** | Draft (umbrella) | All `transactions_matches.*` tools |
| **Transaction matching (Pillar B)** | Draft (umbrella) | `transactions_matches.*` transfer-type filtering |
| **[Categorization overview](categorization-overview.md)** | Draft | `transactions_categorize_rules_apply`, `transactions_categorize_auto_review`, `transactions_categorize_auto_confirm`, `transactions_categorize_auto_stats`, `transactions_categorize_ml_status`, `transactions_categorize_ml_train`, `transactions_categorize_ml_apply` |
| **Smart Import (Pillar A)** | Not written | `import_folder` |
| **Smart Import (Pillar F) + Privacy** | Not written | `import_ai_preview`, `import_ai_parse` |
| **Corrections table schema** | Not written | `transactions_correct` |
| **Annotations table schema** | Not written | `transactions_annotate` |
| **Budget tracking spec** | Draft | `reports_budget_summary` rollover behavior |

### Tools shippable without dependencies

These tools can be fully implemented with the current codebase and existing infrastructure:

**Infrastructure**: `moneybin_discover`
**`spending.*`**: `summary`, `by_category`, `merchants`, `compare`
**`cashflow.*`**: `summary`, `income`
**`accounts.*`**: `list`, `balances`, `networth`, `resolve`
**`transactions.*`**: `search`, `recurring`
**`import.*`**: `file`, `status`, `csv_preview`, `list_formats`
**`categorize.*`**: `uncategorized`, `apply`, `rules`, `create_rules`, `delete_rule`, `merchants`, `create_merchants`, `categories`, `create_category`, `toggle_category`, `seed`, `stats`
**`budget.*`**: `set`, `status`, `delete`
**`tax.*`**: `w2`
**`overview.*`**: `status`, `health`
**`sql.*`**: `query`

This is a 35-tool surface (34 domain tools + `moneybin_discover`) that can ship independently of any pending spec work. With progressive disclosure, the AI sees ~20 core tools at connection time plus `moneybin_discover`; extended namespaces load on demand.

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
