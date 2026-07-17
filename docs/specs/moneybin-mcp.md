# MCP Tool Surface

> Last updated: 2026-05-20
> Status: in-progress
> Companion to: [`mcp-architecture.md`](mcp-architecture.md) (design philosophy, conventions, patterns), [`extension-contracts.md`](extension-contracts.md) (Analysis Packages and standalone Reports register `<pkg>_*`-prefixed tools and auto-generated reports into this server via entry points)
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
- Orientation pointers — which tool to call to "get oriented" (`system_status` for data status; `review` for pending review counts across all queues; `reports_networth` + `reports_spending` for a quick financial pulse)
- Response envelope shape (`{summary, data, actions}`) and pagination convention
- Batch-tool preference
- Sensitivity tiers and degraded-response behavior

Length budget: ~150–300 tokens. The text is loaded once per session, so the cost is amortized — but it competes with conversation and tool descriptions for working memory.

Keep the text in sync with the spec. Renames and new top-level groups must update both.

### First-run setup (elicitation)

When `moneybin mcp serve` starts with no configured profile (no `--profile`, no
`MONEYBIN_PROFILE`, no `active_profile` in `config.yaml`), it boots **anyway** —
it must never trigger the interactive first-run wizard, whose stdout prompts
would corrupt the stdio JSON-RPC stream. The server registers its tools plus
`FirstRunSetupMiddleware` and waits for the first tool call.

On that first call:
- **Elicitation-capable clients** (e.g. Claude Desktop) are asked for a profile
  name via `ctx.elicit`. MoneyBin creates the profile (encrypted DB + schema via
  `ProfileService.create`, key generated server-side into the keychain),
  activates it in-process, and proceeds with the original call — no restart.
- **Tools-only clients** (capability probed via
  `session.check_client_capability`) receive a single structured error envelope
  with `error.code = "infra_setup_required"`, directing the user to run
  `moneybin profile create <name>` and reconnect.

Only the profile **name** crosses the LLM context; the encryption key never
does, so the `profile_*`-is-CLI-only policy (above) is preserved — first-run is
startup/middleware behavior, not a new MCP tool. See
[`mcp-first-run-setup.md`](mcp-first-run-setup.md).

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
| **Elicitation** | optional | ✅ shipped (first-run setup); planned (PDF sign-convention confirm) | First use: `FirstRunSetupMiddleware` elicits a profile name on the first tool call when the server booted unconfigured (see "First-run setup" above). Capability probed via `session.check_client_capability`; tools-only clients get the `infra_setup_required` envelope fallback. **Planned second use:** in-place confirmation of a credit-card sign inversion during import (see [`import_confirm`](#import_confirm)) — today the agent is directed to the CLI because ratifying an inversion is a decision the agent must never self-accept. Still NOT used for general destructive-write confirmation — that stays on the `confirm` parameter pattern (revisit when destructive write-tool count > 5). |
| **Logging level negotiation** | optional | ⏳ not used | Server-side log level honors `MoneyBinSettings.logging.level`; not negotiated per session. |
| **Pagination cursors** | core | ⏳ partial | `summary.has_more` + `summary.total_count` flag truncation; `offset` parameter handles paging. No opaque cursor pattern; revisit if any tool needs server-side iteration state. |
| **Server `instructions`** | core | ✅ shipped | `FastMCP(instructions=...)` in `src/moneybin/mcp/server.py`. See above subsection. |
| **MCP capability negotiation** | core | ✅ shipped via FastMCP | FastMCP handles `initialize` capabilities; MoneyBin doesn't negotiate per-capability flags today. |

**Discipline:** when an MCP specification change, client constraint, or product
requirement surfaces a capability not in this table, add it before deciding what
to do—the table itself is the audit trail. PRs that add a row marked
`⏳ deliberate defer` must include the rationale; PRs that flip a row from `⏳`
to `📐` or `✅` must update the relevant subsection of this spec.

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
moneybin reports spending [--from-month YYYY-MM] [--to-month YYYY-MM] [--category SLUG] [--compare yoy|mom|trailing] [-o text|json] [-q]
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
- **CLI:** `moneybin reports cashflow [--from-month YYYY-MM] [--to-month YYYY-MM] [--by account|category|account-and-category]`.

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

### `accounts_links_pending`

List pending account-link decisions grouped by provisional account.

- **Sensitivity:** `medium` — surfaces account `display_name` (classified `USER_NOTE`, matching `accounts_summary`/`accounts_get`), so the proposal labels sit behind the same consent bar; opaque IDs, signal strings, and confidence scores otherwise. Never surfaces `ref_value` (which can be a full account number). Without consent the response degrades to counts.
- **Unique parameters:** None.
- **Behavior:** Returns all provisional accounts that have pending merge candidates, each grouped with their candidate list. Each candidate carries `decision_id`, `candidate_account_id`, `candidate_display_name`, `confidence`, and `signal`. Use `accounts_links_set(decision_id, action="accept", target_account_id=<candidate_account_id>)` to merge (the user is prompted to confirm) or `action="reject"` to keep the provisional account standalone.
- **Service:** `AccountLinksService.pending()` + `AccountLinksService.count_pending()`
- **CLI:** `moneybin accounts links pending`

### `accounts_links_set`

Accept (merge) or standalone-reject one pending account-link decision.

- **Sensitivity:** `low`
- **Unique parameters:** `decision_id: str` (required); `action: "accept" | "reject"` (required); `target_account_id: str | None = None` (required with `action="accept"` — the decision's own `candidate_account_id`; invalid with `action="reject"`).
- **Behavior:** Accept/reject is **explicit** in `action` — never inferred from the truthiness of `target_account_id` (an empty-string target from a malformed agent call must not become a permanent reject that suppresses a correct merge proposal forever; a rejected proposal is never re-proposed). `action="accept"` requires `target_account_id` = the decision's own `candidate_account_id` (confirming safety check; `MUTATION_INVALID_INPUT` on mismatch, on an empty target, or on a missing one) **and** explicit human agreement through an MCP elicitation → MERGE (re-points every accepted source reference from the provisional onto the survivor; auto-rejects every other pending decision touching the provisional). `action="reject"` (no target; passing one raises `MUTATION_INVALID_INPUT`) → STANDALONE-REJECT (rejects every pending decision for the provisional account; it stays its own canonical account).
- **Accept is gated by an MCP elicitation; there is no agent self-accept.** Same rule and same shared helper as `investments_securities_links_set` (§5d): a pending decision is by construction a weak inference — the resolver proposes a merge *only* when it cannot bind unambiguously — and a weak inference is "never eligible for agent self-accept, regardless of confidence score" (`.claude/rules/design-principles.md`, "Magic stays visible"). Accepting fuses two accounts' transaction histories and balances, so a wrong accept corrupts both the merged ledger and net worth. The confirmation names **both** accounts (id + display name) and the weak signal + confidence the resolver fired on. The confirming safety check is applied *before* the prompt, so a doomed merge never costs the user a confirmation. Reject stays agent-callable without a confirm — it is cheap and reversible.
- **Confirmation contract:** three paths, no fourth. **Accepted elicitation** → merge, `decided_by="user"`. **Declined / cancelled** → nothing written, decision stays `pending`, `MUTATION_CONFIRMATION_REQUIRED`. **Client cannot elicit / no active session** → nothing written, `MUTATION_CONFIRMATION_REQUIRED` whose `hint` carries the CLI command (`moneybin accounts links set <id> --into <account_id>`) and whose `details.reason` is `client_unsupported` / `no_session` / `declined`.
- **`decided_by` reflects reality:** `"user"` on accept (a human ratified it at the elicitation); `"auto"` on an MCP reject (the agent decided — no human ratified it; the column's CHECK admits only `auto`/`user`, and the MCP channel itself is recorded as `actor="mcp"` in `app.audit_log`). The CLI records `"user"` on both, because a human ran the command.
- **Mutation surface:** writes `app.account_links` and `app.account_link_decisions`. Reverse with `system_audit_undo(operation_id)`.
- **Service:** `AccountLinksService.set(decision_id, target_account_id=..., decided_by="user")` on accept / `.set(decision_id, target_account_id=None, decided_by="auto")` on reject
- **CLI:** `moneybin accounts links set <decision_id> --into <account_id>` (merge) or `--standalone` (standalone-reject)

### `accounts_links_history`

Show recent account-link decisions (all statuses), newest first.

- **Sensitivity:** `low`
- **Unique parameters:** `limit: int = 50`
- **Behavior:** Returns decisions across all statuses (pending, accepted, rejected). Useful for auditing what has already been decided.
- **Service:** `AccountLinksService.history()`
- **CLI:** `moneybin accounts links history`

### `accounts_links_run`

Backfill account-link proposals for accounts already in `core.dim_accounts` that have no pending proposal yet.

- **Sensitivity:** `low` — count only.
- **Unique parameters:** None.
- **Behavior:** Iterates all accounts in `core.dim_accounts`, calls the resolver candidate pass (institution+last4 or name fuzzy-match) excluding each account itself, and writes `pending` `app.account_link_decisions` rows for any new unordered pair not already proposed or decided. Skips pairs that already have a decision in either direction (any status). Returns `data.new_proposals` — count of new pending decisions written.
- **Mutation surface:** writes `app.account_link_decisions`; revert via `app.audit_log` (no undo tool; deferred to M1L).
- **Service:** `AccountLinksService.run()`
- **CLI:** `moneybin accounts links run`
- **read_only:** false

> **Note:** An undo variant (`accounts_links_undo`) remains deliberately out of scope, deferred to the M1L audit-undo consumer.

---

## 5b. `merchants_links_*` — Merchant-link review workflow (M1T)

**Service class:** `MerchantLinksService`

These four tools implement the merchant-link review surface: the agent-facing peer to the Task 10 CLI. Provider entity ids (e.g. Plaid `merchant_entity_id`) arrive in the pending queue; the agent reviews and binds each to a canonical merchant in `core.dim_merchants`, or rejects so the resolver mints a new merchant for the id on its next categorization pass.

> **Note:** An undo variant (`merchants_links_undo`) remains deliberately out of scope, deferred to the M1L audit-undo consumer.

### `merchants_links_pending`

- **Sensitivity:** `medium` — `ref_value` and `candidate_merchant_id` are `RECORD_ID` (low); `provider_merchant_name` and `candidate_canonical_name` are `MERCHANT_NAME` (medium).
- **Unique parameters:** None.
- **Behavior:** Returns all provider entity ids that have pending merge candidates, each grouped with their candidate list. Each candidate carries `decision_id`, `candidate_merchant_id`, `candidate_canonical_name`, and `confidence`. Use `merchants_links_set(decision_id, action="accept", target_merchant_id=<candidate_merchant_id>)` to bind (the user is prompted to confirm) or `action="reject"` to leave the entity id unbound.
- **Service:** `MerchantLinksService(db, actor="mcp").pending()` + `.count_pending()`
- **CLI:** `moneybin merchants links pending`
- **read_only:** true

### `merchants_links_set`

- **Sensitivity:** `low` — returns `decision_id` (RECORD_ID) and `status` (TXN_TYPE) only.
- **Parameters:** `decision_id: str`, `action: "accept" | "reject"`, `target_merchant_id: str | None = None`
- **Behavior:** Accept/reject is **explicit** in `action` — never inferred from the truthiness of `target_merchant_id` (an empty-string target from a malformed agent call must not become a permanent reject that suppresses a correct binding forever; a rejected pairing is never re-proposed). `action="accept"` requires `target_merchant_id` = the decision's own `candidate_merchant_id` (confirming safety check; `MUTATION_INVALID_INPUT` on mismatch, on an empty target, or on a missing one) **and** explicit human agreement through an MCP elicitation → BIND (writes the accepted binding in `app.merchant_links`; auto-rejects sibling candidates for the same `(source_type, ref_value)`). `action="reject"` (no target; passing one raises `MUTATION_INVALID_INPUT`) → REJECT (marks this decision and every pending sibling rejected; the declined pairing is not re-proposed, and the resolver mints a new merchant for the id on its next categorization pass).
- **Accept is gated by an MCP elicitation; there is no agent self-accept.** Same rule and same shared helper as `investments_securities_links_set` (§5d): a decision only reaches this queue because one provider entity id pointed at more than one canonical merchant — a weak inference, "never eligible for agent self-accept, regardless of confidence score" (`.claude/rules/design-principles.md`, "Magic stays visible"). Accepting attributes every transaction carrying that entity id to the chosen merchant, which also drives their categorization. The confirmation names **both** sides (provider entity id + provider name; canonical merchant id + canonical name) and the confidence the resolver could not clear. The confirming safety check is applied *before* the prompt. Reject stays agent-callable without a confirm.
- **Confirmation contract:** three paths, no fourth. **Accepted elicitation** → bind, `decided_by="user"`. **Declined / cancelled** → nothing written, decision stays `pending`, `MUTATION_CONFIRMATION_REQUIRED`. **Client cannot elicit / no active session** → nothing written, `MUTATION_CONFIRMATION_REQUIRED` whose `hint` carries the CLI command (`moneybin merchants links set <id> --into <merchant_id>`) and whose `details.reason` is `client_unsupported` / `no_session` / `declined`.
- **`decided_by` reflects reality:** `"user"` on accept (a human ratified it at the elicitation); `"auto"` on an MCP reject (the agent decided — no human ratified it; the column's CHECK admits only `auto`/`user`, and the MCP channel itself is recorded as `actor="mcp"` in `app.audit_log`). The CLI records `"user"` on both, because a human ran the command.
- **Mutation surface:** writes `app.merchant_link_decisions` + `app.merchant_links`; reverse with `system_audit_undo(operation_id)`.
- **Service:** `MerchantLinksService(db, actor="mcp").set(decision_id, target_merchant_id=..., decided_by="user")` on accept / `.set(decision_id, target_merchant_id=None, decided_by="auto")` on reject
- **CLI:** `moneybin merchants links set`
- **read_only:** false

### `merchants_links_history`

- **Sensitivity:** `medium` — history rows carry `provider_merchant_name` (MERCHANT_NAME); the envelope derives the tier from the highest field, so the row's id/status fields don't lower it.
- **Parameters:** `limit: int = 50`
- **Behavior:** Returns all merchant-link decisions (any status), newest first. Filter by `limit`.
- **Service:** `MerchantLinksService(db, actor="mcp").history(limit=limit)`
- **CLI:** `moneybin merchants links history`
- **read_only:** true

### `merchants_links_run`

- **Sensitivity:** `low` — returns counts only.
- **Unique parameters:** None.
- **Behavior:** Harvests merchant-link proposals from existing categorization facts. Binds provider entity ids that point unambiguously to a single canonical merchant; routes conflicts to the pending review queue. Returns `data.bound` (entity ids silently bound to one merchant) and `data.conflicts` (one-id-many-merchant cases queued for review) — bound bindings are not pending.
- **Mutation surface:** writes `app.merchant_links` + `app.merchant_link_decisions`; revert via `app.audit_log` (no undo tool; deferred to M1L).
- **Service:** `MerchantLinksService(db, actor="mcp").run()`
- **CLI:** `moneybin merchants links run`
- **read_only:** false

---

## 5c. `investments_*` — Investment ledger, cost basis, and securities catalog (M1J.1)

**Service class:** `InvestmentService`

Foundation child of the investments initiative ([`investments-data-model.md`](investments-data-model.md), Pillars A+B — implemented). The ledger (`investments`) is the one authored/ingested surface; lots, holdings, and realized gain/loss are derived from it in SQLMesh (Invariant 8) — never snapshotted into `app.*`. Free-text account/security references resolve at the service boundary (Guard 2, `identifiers.md`); an ambiguous or unresolved account reference is always a hard failure, while an unresolved security reference is soft in `investments_record` (see below). Sensitivity is DERIVED per tool from its payload's classified fields, not hardcoded (`src/moneybin/privacy/payloads/investments.py`): `investments` / `investments_holdings` / `investments_lots` / `investments_gains` carry `BALANCE`/`TXN_AMOUNT` fields and resolve to `high`; `investments_securities` carries only `TXN_TYPE`/`CURRENCY`/`RECORD_ID` fields and resolves to `low`.

### `investments`

List investment ledger events (buys, sells, dividends, corporate actions, ...).

- **Sensitivity:** `high` (see note above).
- **Unique parameters:** `account` (optional, free-text/id), `security` (optional, free-text/id), `type_filter` (optional), `from_date`/`to_date` (optional ISO date).
- **Behavior:** Returns rows from `core.fct_investment_transactions`. Amounts (`quantity`/`price`/`amount`/`fees`) use the per-type sign convention documented on `investments_record`; positive quantity = acquisition, negative = disposal. Amounts are in the currency named by `summary.display_currency`.
- **Service:** `InvestmentService.list_events()`
- **CLI:** `moneybin investments list`
- **read_only:** true

### `investments_holdings`

Current positions: quantity, cost basis, average cost per (account, security).

- **Sensitivity:** `high`.
- **Unique parameters:** `account` (optional).
- **Behavior:** Sum of open lots from `core.dim_holdings`. Market value and unrealized gain/loss require price feeds (Pillar C, not yet shipped) — response always carries a warning that only cost basis is available.
- **Service:** `InvestmentService.holdings()`
- **CLI:** `moneybin investments holdings`
- **read_only:** true

### `investments_lots`

Tax lots with remaining quantity and basis.

- **Sensitivity:** `high`.
- **Unique parameters:** `account` (optional), `security` (optional), `open_only` (bool, default `true`).
- **Behavior:** Returns rows from `core.fct_investment_lots`. Open lots only by default; `open_only=false` returns the full open+closed history.
- **Service:** `InvestmentService.lots()`
- **CLI:** `moneybin investments lots list`
- **read_only:** true

### `investments_gains`

Realized gain/loss (the 1099-B surface) from `core.fct_realized_gains`.

- **Sensitivity:** `high`.
- **Unique parameters:** `account` (optional), `security` (optional), `from_date`/`to_date` (optional), `term` (optional: `short`|`long`).
- **Behavior:** One row per (disposal, consumed lot). A row with `basis_incomplete=true` means the disposal exceeded tracked lots (oversold) or the acquisition lot is missing — its gain/loss is computed from zero cost basis and is conservative, not authoritative; `data.warnings` names the count when any row is incomplete.
- **Service:** `InvestmentService.gains()`
- **CLI:** `moneybin investments gains`
- **read_only:** true

### `investments_securities`

List the manually-maintained securities catalog.

- **Sensitivity:** `low` — reference data only, no amounts, no per-user holdings.
- **Unique parameters:** `security_type` (optional).
- **Behavior:** Returns rows from `core.dim_securities`.
- **Service:** `InvestmentService.list_securities()`
- **CLI:** `moneybin investments securities list`
- **read_only:** true

### `investments_record`

Record one or more investment ledger events (Shape 3 batch).

- **Sensitivity:** `high`.
- **Parameters:** `events: list[dict]` — each item: `account` (required), `type` (required — one of the 14-value closed taxonomy: `buy`, `sell`, `reinvest`, `dividend`, `interest`, `capital_gain_distribution`, `transfer_in`, `transfer_out`, `deposit`, `withdrawal`, `split`, `fee`, `return_of_capital`, `other`), `date` (required, ISO), `security` (required for buy/sell/reinvest/transfer_in/transfer_out/split/return_of_capital; forbidden for deposit/withdrawal), `quantity`/`price`/`amount`/`fees`/`basis` (decimal strings, never floats), `subtype`, `acquired` (ISO date, `transfer_in` only), `event_group_id`, `currency` (default `USD`), `description`.
- **Behavior:** Sign convention: `quantity` positive for acquisitions (buy/reinvest/transfer_in), negative for disposals (sell/transfer_out), absent for cash-only events; `amount` negative for cash out (buy/reinvest/withdrawal/fee), positive for cash in (sell/deposit/dividend/interest/capital_gain_distribution/return_of_capital). A `reinvest` event atomically writes the acquisition leg AND a paired income row sharing one `event_group_id` — both ids are returned. All events are validated and resolved BEFORE any row is written: a bad/ambiguous ACCOUNT reference (or a taxonomy/sign violation) is a HARD failure that aborts the entire call with nothing written; a bad/ambiguous SECURITY reference is a SOFT per-item failure reported in `data.error_details` while the rest of the batch is written.
- **Mutation surface:** writes `raw.manual_investment_transactions` (one row per event, two for `reinvest`). No revert tool; every write is recorded in `app.audit_log` under `action="investment.record"`.
- **Service:** `InvestmentService.record_event()`
- **CLI:** `moneybin investments add`
- **read_only:** false

### `investments_securities_set`

Create-or-update one securities-catalog entry (Shape 1b entity upsert).

- **Sensitivity:** `low`.
- **Parameters:** `security_id` (`None` creates; existing id partially updates), `name`, `security_type` (required on create: `equity`, `etf`, `mutual_fund`, `bond`, `crypto`, `cash`, `other`; immutable post-creation), `ticker`, `exchange`, `cusip`, `isin`, `figi`, `coingecko_id`, `is_cash_equivalent`, `cost_basis_method` (`fifo`/`hifo`/`specific`/`average`; `average` valid only for `mutual_fund`/`etf`, raises `mutation_invalid_input` otherwise), `currency_code` (default `USD` on create).
- **Behavior:** Unset (`None`) fields keep their current value on update. Updating a `security_id` that doesn't exist raises `mutation_not_found`.
- **Mutation surface:** writes `app.securities`. No delete tool for catalog entries in v1; revert by calling again with the prior values.
- **Service:** `InvestmentService.upsert_security()` / `InvestmentService.set_security()`
- **CLI:** `moneybin investments securities add` / `moneybin investments securities set <security_id>`
- **read_only:** false

### `investments_lots_select`

Set (or clear) the full specific-identification lot selection for a disposal (Shape 1a collection state-set).

- **Sensitivity:** `high`.
- **Parameters:** `disposal_txn_id` (must be a `sell`; other event types raise `mutation_invalid_input`), `selections: list[{"lot_id": str, "quantity": str}]`.
- **Behavior:** The listed `(lot_id, quantity)` pairs REPLACE any prior selection for this disposal in full — an omitted lot is dropped, not left in place. `selections=[]` clears all overrides and reverts the disposal to FIFO. Selected quantities must sum to no more than the disposal's magnitude.
- **Mutation surface:** writes `app.lot_selections`. No revert tool; call again with the prior selections (or `[]`) to undo.
- **Service:** `InvestmentService.select_lots()`
- **CLI:** `moneybin investments lots select <disposal_txn_id>`
- **read_only:** false

> **Per-account cost-basis default** is a field on `accounts_set` (`default_cost_basis_method`), not a separate tool — see [`account-management.md`](account-management.md).

---

## 5d. `investments_securities_links_*` — Security-identity merge review workflow (M1G.4)

**Service class:** `SecurityLinksService`

These three tools implement the security-identity review surface: the agent-facing peer to the `investments securities links` CLI (Task 12), mirroring `merchants_links_*` (§5b). `SecurityResolver` refuses to auto-merge two securities when their identity is ambiguous (an identifier tie, a stripped ticker, a cross-exchange contradiction, a fuzzy name match) — it files one pending decision per candidate instead.

**Accept is gated by an MCP elicitation; there is no agent self-accept.** Every pending decision is by construction a weak inference — the resolver proposes a merge *only* when it cannot decide — and a weak inference is "never eligible for agent self-accept, regardless of confidence score" (`.claude/rules/design-principles.md`, "Magic stays visible"). Accepting fuses two instruments' tax lots: it re-points every accepted provider ref and lot selection onto the survivor and deletes the provisional catalog row, so a wrong accept corrupts cost basis and every later realized gain. `investments_securities_links_set(action="accept")` therefore prompts the user through `ctx.elicit` — naming both securities and the `match_reason` — and merges only on explicit agreement (`.claude/rules/mcp.md`: "Destructive — use a `confirm` parameter or elicitation"). On a client that cannot elicit, or with no active session, accept **hard-fails** with `MUTATION_CONFIRMATION_REQUIRED` naming the CLI equivalent; it never falls through to accepting. A declined or cancelled elicitation likewise writes nothing. The confirming safety check (`into` must equal the decision's own `candidate_security_id`, mirroring `merchants_links_set`'s `target_merchant_id` guard) is applied *before* the prompt, so a doomed merge never costs the user a confirmation. Reject stays agent-callable without a confirm — it is cheap and reversible.

**The elicitation gate is shared, not per-tool.** `moneybin/mcp/elicitation.py` owns the whole pattern — `supports_elicitation` (the capability probe) and `confirm_or_raise` (resolve the session → probe the capability → `ctx.elicit` → raise `MUTATION_CONFIRMATION_REQUIRED` with the CLI equivalent on every ungranted path). All three link-merge accept gates (`investments_securities_links_set`, `accounts_links_set`, `merchants_links_set`) call it, and the first-run profile bootstrap (`mcp/first_run.py`) uses the capability probe. There is one elicitation pattern in the codebase, not one per tool: a new confirm-gated mutation calls `confirm_or_raise` with its own prompt and CLI equivalent, and inherits the contract. The three link tools are deliberately identical in shape — same `action` parameter, same accept gate, same `decided_by` semantics — because they do the same job on three entity types; a defect found in one is a defect in all three.

> **Note:** No dedicated undo variant (`investments_securities_links_undo`) exists — none is needed. `accept_merge` writes its whole cascade under one audited operation, so `system_audit_undo(operation_id)` reverses it atomically; the tool description names that as the recovery path.

### `investments_securities_links_pending`

- **Sensitivity:** derived from payload fields — `ref_value`/`decision_id`/`candidate_security_id` are `RECORD_ID`; `provider_ticker`/`provider_name`/`candidate_ticker`/`candidate_name` are `TXN_TYPE`; `match_reason` is `USER_NOTE`; `confidence` is `AGGREGATE`.
- **Unique parameters:** None.
- **Behavior:** Returns all provider refs (`plaid_security_id` or `institution_security_id`) with pending merge-survivor candidates, grouped by ref. Each group carries BOTH sides of the proposed merge — `provider_ticker`/`provider_name` (what's being merged) alongside each candidate's `candidate_ticker`/`candidate_name` (what it would merge into) — and each candidate carries `match_reason` (`identifier_tie`, `exchange_contradiction`, `fuzzy_name`, ...), the field that conveys how risky accepting is. Use `investments_securities_links_set` to decide each group.
- **Service:** `SecurityLinksService(db, actor="mcp").pending()` + `.count_pending()`
- **CLI:** `moneybin investments securities links pending`
- **read_only:** true

### `investments_securities_links_set`

- **Sensitivity:** `low` — returns `decision_id` (RECORD_ID) and `status` (TXN_TYPE) only.
- **Parameters:** `decision_id: str`, `action: "accept" | "reject"`, `into: str | None = None`
- **Behavior:** Accept/reject is **explicit** in `action` — never inferred from the truthiness of `into` (an empty-string `into` from a malformed agent call must not become a permanent reject that suppresses a correct merge proposal forever). `action="accept"` requires `into` = the decision's own `candidate_security_id` (confirming safety check; `MUTATION_INVALID_INPUT` on mismatch, on an empty `into`, or on a missing one) **and** explicit human agreement through an MCP elicitation → MERGE (re-points every accepted provider ref and tax lot from the provisional security onto the survivor in one transaction; auto-rejects sibling candidates for the same ref). `action="reject"` (no `into`; passing one raises `MUTATION_INVALID_INPUT`) → REJECT (marks only this decision rejected; sibling candidates for the same provider ref remain pending — unlike `merchants_links_set`'s reject-all, rejecting one security candidate does not answer whether another candidate is the correct match).
- **Confirmation contract:** three paths, no fourth. **Accepted elicitation** → merge, `decided_by="user"` (a human did decide). **Declined / cancelled** → nothing written, decision stays `pending`, `MUTATION_CONFIRMATION_REQUIRED`. **Client cannot elicit / no active session** → nothing written, `MUTATION_CONFIRMATION_REQUIRED` whose `hint` carries the CLI command (`moneybin investments securities links set <id> --accept --into <security_id>`) and whose `details.reason` is `client_unsupported` / `no_session` / `declined`.
- **`decided_by` reflects reality:** `"user"` on accept (a human ratified it at the elicitation); `"auto"` on an MCP reject (the agent decided — no human ratified it; the column's CHECK admits only `auto`/`user`, and the MCP channel itself is recorded as `actor="mcp"` in `app.audit_log`). The CLI records `"user"` on both, because a human ran the command.
- **Mutation surface:** writes `app.security_link_decisions` + `app.security_links` + `app.lot_selections` + `app.securities` (deletes the merged-away provisional row on accept); reverse the whole cascade with `system_audit_undo(operation_id)`.
- **Service:** `SecurityLinksService(db, actor="mcp").accept_merge(decision_id, into=..., decided_by="user")` / `.reject_merge(decision_id, decided_by="auto")`
- **CLI:** `moneybin investments securities links set <decision_id> --accept --into <candidate_security_id>` / `--reject`
- **read_only:** false; **destructive:** true

### `investments_securities_links_history`

- **Sensitivity:** derived from payload fields (same classification as `_pending`, plus `decided_at` as `TIMESTAMP_OBSERVABILITY`).
- **Parameters:** `limit: int = 50`
- **Behavior:** Returns all security-link decisions (any status), newest first. Filter by `limit`.
- **Service:** `SecurityLinksService(db, actor="mcp").history(limit=limit)`
- **CLI:** `moneybin investments securities links history`
- **read_only:** true

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

### `review`

Orientation tool: pending counts across **all five** review queues (matches + categorize + account-links + merchant-links + security-links).

- **Sensitivity:** `low` — counts only.
- **Unique parameters:** None.
- **Behavior:** Returns `{matches_pending: int, categorize_pending: int, account_links_pending: int, merchant_links_pending: int, security_links_pending: int, total: int}` so the agent can answer "what needs my attention?" in one sweep. Drill into `transactions_categorize_pending` for categorization items, `transactions_matches_pending` for match proposals, `accounts_links_pending` for account-link decisions, `merchants_links_pending` for merchant-link decisions, and `investments_securities_links_pending` for security-link decisions (see §5d).
- **Service:** `ReviewService(MatchingService, CategorizationService, AccountLinksService, MerchantLinksService, SecurityLinksService).status()`
- **CLI:** `moneybin review`

### `transactions_review` *(deprecated — removed after one minor release)*

**DEPRECATED alias for `review`.** Registered with a description starting with `"DEPRECATED: use review — removed after one minor release."` Identical behavior; prefer `review` in all new agent code.

- **CLI:** `moneybin transactions review` *(deprecated — use `moneybin review`)*

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
- **Behavior:** Replaces the import's label set; emits one full-row `import.set` audit row (Invariant 10, via `ImportsRepo.set`).
- **Mutation surface:** `app.imports` (labels overlay). Revert via another `_set` with the prior list (full before/after captured in `app.audit_log`).
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

Match review is a distinct workflow within the transactions domain. These tools operate on match proposals — pairs of transactions that the matching engine believes represent the same real-world event (dedup) or two sides of a transfer. All four tools are **live and registered**.

**Service class:** `MatchingService`

#### `transactions_matches_pending`

List match proposals awaiting a decision.

- **Sensitivity:** `low` — returns pair IDs and confidence scores; no transaction amounts, descriptions, or PII.
- **Unique parameters:** `match_type: str?` (`dedup` or `transfer`; omit for all pending), `limit: int?` (default 50, applied as SQL `LIMIT`).
- **Behavior:** Returns array of `{match_id, match_type, match_tier, confidence_score, source_type_a, source_transaction_id_a, source_type_b, source_transaction_id_b, match_status, component_key}` for proposals whose status is `pending`. No amounts/descriptions — call `transactions_get` on a source id for those. Use `transactions_matches_set` to accept or reject individual proposals. `component_key` groups all edges that belong to the same N-way dedup cluster (MIN packed member key per component, matching `match_group_id` semantics in the prep fold); transfer rows use their own `match_id` as the key. The `actions[]` summary hint reports `N pending dedup edges across M groups`.
- **Service:** `MatchingService.get_pending()`
- **CLI:** `moneybin transactions matches pending` (grouped pending display); `moneybin transactions review --type matches` (orientation + interactive queue); `moneybin transactions review --type matches --status` (counts only)
- **read_only:** true

#### `transactions_matches_set`

Accept or reject one pending match proposal by `match_id`.

- **Sensitivity:** `low` — the response carries only `{match_id, match_status}`; no amounts, descriptions, or PII. (The operation still mutates `app.match_decisions` — see Mutation surface.)
- **Unique parameters:** `match_id: str` (required), `status: Literal["accepted", "rejected"]` (required).
- **Behavior:** Shape-1b partial update — sets the decision status for one proposal. The read-validate-write runs in a single transaction (no TOCTOU window). Only `pending` decisions are settable; re-asserting a decision's current status is an idempotent no-op (hence `idempotent=True`), while any cross-status transition on an already-decided match errors with `recovery_actions` (e.g. rejecting an already-accepted proposal points at `system_audit_undo` — the audit-log undo, shipping in M1L; until it lands, the CLI `moneybin transactions matches undo` is the manual route). Accepted decisions collapse the matched pair on the next `refresh_run`; rejected decisions suppress the proposal from future review.
- **Mutation surface:** writes `app.match_decisions`. Revert via `moneybin transactions matches undo <match_id>` (CLI) until `system_audit_undo` ships.
- **Annotations:** `read_only=False`, `destructive=False`, `idempotent=True`.
- **Service:** `MatchingService.set_status(match_id, status)`
- **CLI:** `moneybin transactions matches set <match_id> --status accepted|rejected`

#### `transactions_matches_run`

Run the matching engine on-demand and propose new pending decisions.

- **Sensitivity:** `low` — triggers a pipeline step; returns counts, not financial data.
- **Unique parameters:** None in the current implementation (the engine scans all unmatched transactions).
- **Behavior:** Runs the matcher, writes new `pending` rows to `app.match_decisions` for newly-discovered pairs, and returns `{auto_merged: int, pending_review: int, pending_transfers: int}`. **Operator-territory granular alternative to `refresh_run(steps=["match"])`** — not promoted in the `instructions` field or user-facing `actions[]` hints; reach it via `refresh_run` for the standard path.
- **Annotations:** `read_only=False`, `destructive=False`, `idempotent=False`.
- **Service:** `MatchingService.run()`
- **CLI:** `moneybin transactions matches run`

#### `transactions_matches_history`

Recent match decisions (accepted and rejected).

- **Sensitivity:** `low` — decision metadata only (match IDs, type, status, timestamps); no financial data.
- **Unique parameters:** `limit: int?` (default 50), `match_type: str?` (`dedup` or `transfer`; omit for all).
- **Behavior:** Returns array of `{match_id, match_type, match_status, confidence_score, decided_by, decided_at}` ordered newest-first.
- **Service:** `MatchingService.get_log()`
- **CLI:** `moneybin transactions matches history [--type dedup|transfer] [--limit N]`
- **read_only:** true

---

## 7. `import.*` — Data ingestion

**Service class:** `ImportService`

### `import_files`

Import one or more financial data files into MoneyBin. Format detected automatically per file from extension (OFX/QFX/QBO, CSV/TSV/Excel/Parquet/Feather). Per-file failures do not abort the batch; transforms run once at end of batch by default.

- **Sensitivity:** `low` on success; `medium` on `confirmation_required` state (returns sample rows + proposed mapping).
- **Unique parameters:** `paths: list[str]` (required, each path must be within the user's home directory), `refresh: bool = True`, `force: bool = False`.
- **Behavior:** Validates each path, delegates to `ImportService.import_files()`. On success, returns envelope with `data.{imported_count, failed_count, total_count, transforms_applied, transforms_duration_seconds, files: list[{path, status, source_type, rows_loaded, import_id, error?}]}`. Amounts use accounting convention: negative=expense, positive=income; transfers exempt.

  **`confirmation_required` state (first-encounter unknown layout):** instead of importing, returns a `ResponseEnvelope` with `status="ok"`, `summary.status="confirmation_required"`, and per-file `confirmation_payload`. The `actions[]` field contains concrete invocation hints for `import_confirm`. The caller inspects the proposal (optionally via `import_preview`) and calls `import_confirm` to ratify. Sensitivity is `medium` for this state because the envelope contains row-level content.

  - **Tabular** unknown layout: `confirmation_payload.{proposed_mapping, samples, flagged, missing_required, unmapped_columns}`; ratify with `accept=True` / `mapping={...}`.
  - **PDF bridge** (Phase 2b): a native-text PDF the deterministic rung can't crack escalates to the driving agent instead of silently seeding (gated on the agent caller — `actor_kind="agent"`). `confirmation_payload.{channel="pdf", bridge_payload}` carries the document text + table preview + layout fingerprint + transparency notice + `request_kind`; the agent proposes a recipe + rows and ratifies via `import_confirm(bridge_response=...)`. With no agent present, the PDF falls back to the Phase 2a seed path.
  - **PDF sign convention** (credit-card statement): a PDF that names itself a credit card derives a `negative_is_income` recipe that inverts every amount's sign (charges → expenses, payments → credits). Because the convention is not recoverable from the numbers, it is never applied silently. `confirmation_payload.{channel="pdf", reason="sign_convention", sign_convention, sign_evidence, sign_sample_rows}` carries the card disclosures matched and printed-vs-recorded sample rows. The agent must NOT ratify it: **MCP cannot confirm a sign inversion in place yet** (elicitation-based confirm is planned — see [First-run setup (elicitation)](#first-run-setup-elicitation) for the pattern this will follow). Until then the `actions[]` direct the human to resolve it in a terminal — `moneybin import files <path> --confirm` if it IS a credit card, or `moneybin import files <path> --sign negative_is_expense` if it is not. A replay of an already-confirmed card format loads without re-asking.
- **Service:** `ImportService.import_files() -> BatchImportResult | ConfirmationRequiredResult`
- **CLI:** `moneybin import files PATHS... [--confirm/--no-confirm] [--mapping field=column] [--no-refresh] [--output json]`

Per-file overrides (`account_name`, `institution`, `format_name`) are not exposed on the batch MCP surface — use the CLI for those.

### `import_confirm`

Terminal `_confirm` step of the propose→review→confirm workflow. Two channels —
**tabular** (ratify a detected column mapping) and **PDF bridge** (apply an
agent-authored extraction recipe). `import_files`/`import_preview` returns a
`confirmation_required` envelope; the agent inspects the proposal and calls
`import_confirm` to ratify.

- **Sensitivity:** `medium` — returns row-shaped sample values / row counts.
- **Annotations:** `read_only=False`, `destructive=False`, `idempotent=False`.
- **Unique parameters:**

  | Parameter | Type | Notes |
  |---|---|---|
  | `file_path` | `str` | Path to the file to import. Must match the path from the `confirmation_required` envelope. |
  | `accept` | `bool` | **Tabular:** accept the detected mapping as-is. Default `False`. |
  | `mapping` | `dict[str, str] \| None` | **Tabular:** partial-merge override, dest field → source column. |
  | `bridge_response` | `dict \| None` | **PDF:** the agent's `{recipe, rows}` reply. Mutually exclusive with `accept`/`mapping` (conflict → `confirm_channel_conflict` error). |
  | `save_format` | `bool` | Pin the merged mapping / recipe as a saved `app.tabular_formats` / `app.pdf_formats` entry. Default `True`. |
  | `account_id` | `str \| None` | Pin rows to an existing account (single-account tabular; PDF with no account anchor). |
  | `account_name` | `str \| None` | Existing account name to resolve to an `account_id` (single-account tabular). |
  | `account_bindings` | `dict[str, str] \| None` | Ratify an `account_confirmation`: `source_account_key` → existing `account_id` (adopt) or `"new"` (mint a distinct account). Keys come from `confirmation_payload.account_proposals[].source_account_key`. For multi-account files; `account_id`/`account_name` also cover the single-account case. Also resolves the single-account no-identity case (where the source_key comes from `confirmation_payload.account_proposals[0].source_account_key`). |
  | `account_metadata` | `dict[str, dict[str, str]] \| None` | For `"new"`-bound accounts: `source_account_key` → `{display_name, account_subtype, last_four, iso_currency_code}` captured into the minted account's `app.account_settings`. Unknown fields raise; ignored for adopted accounts. |

- **Behavior (tabular):** Merges `mapping` over the detected proposal, validates, and executes the import.
- **`account_confirmation` state:** two cases return `confirmation_required` with `confirmation_payload.{reason="account_confirmation", account_proposals[]}` instead of loading. (a) A source account resolves to weak merge candidate(s) (`institution+last4` / name) and the caller has not bound it — **interactive human** imports gate here; **agent** (`actor_kind="agent"`) imports never gate here: they load and leave the proposal in the account-link review queue (`accounts_links_pending`). (b) A single-account tabular file is imported with no account identity (`account_id`, `account_name`, account-name column all absent) — the resolver finds no candidates and returns a one-entry no-candidate proposal; **both human and agent** callers receive `account_confirmation` for this case (no silent fallback exists). In both cases the column layout is settled; only the account identity needs ratifying via `account_bindings`. Strong-confirmer adoptions and `account_bindings`-resolved accounts load silently. See [`account-identity-resolution.md`](account-identity-resolution.md) Decision 7.
- **Behavior (PDF bridge):** Delegates to `ImportService.apply_pdf_bridge_response()` — re-runs the agent's recipe against the document, reconciles the **re-executed** rows against the statement balances (the authority; the agent's returned rows are verified against them and a row-count divergence is reported back), persists the recipe, and loads the transactions. Returns `data.{status="applied", import_id, rows_loaded, format_name, expected_row_count, actual_row_count, rows_diverged}`. A response whose re-executed rows fail reconciliation is rejected: `data.{status="invalid", reject_reason, …}` and **nothing loads**. A malformed response or an out-of-bounds recipe (Req 9b) returns a `bridge_response_invalid` error envelope.
- **Behavior (PDF sign convention):** `import_confirm` has **no** path to ratify a credit-card sign inversion — no `sign=` parameter, and `accept=`/`mapping=` on a `.pdf` returns a `confirm_channel_conflict` error whose message directs the human to the terminal (`moneybin import files <path> --confirm` / `--sign negative_is_expense`). Ratifying an inversion in place is deferred to an elicitation-based confirm (the MCP server asks the human directly; the agent never self-ratifies). This keeps the agent from silently reversing every amount in a statement.
- Amounts use accounting convention: negative=expense, positive=income; transfers exempt.
- **Mutation surface:** `raw.tabular_transactions` (load), `app.tabular_formats` / `app.pdf_formats` (when `save_format=True`). Revertible via `import_revert` (data rows) and `system_audit_undo` (format save).
- **Service:** `ImportService.confirm_import()` (tabular) / `ImportService.apply_pdf_bridge_response()` (PDF).
- **CLI:** `moneybin import confirm <file> [--accept] [--mapping field=column] [--save-format/--no-save-format] [--account-name NAME] [--account-id ID] [--account-binding source_key=ACCOUNT_ID|new] [--account-meta source_key:field=value] [--output text|json]`

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

Preview a file's structure before importing. Tabular (CSV, Excel, etc.) and PDF.

- **Sensitivity:** `medium` — the response carries row-level sample content (tabular `sample_values`; PDF deterministic row counts and, on escalation, the bridge payload's document text).
- **Annotations:** `read_only=False`, `idempotent=False` — the **PDF escalation branch writes a `smart_import_parse` egress audit row** (Req 14), so the tool is no longer side-effect-free. Tabular previews remain effectively read-only.
- **Unique parameters:** `file_path: str` (required).
- **Behavior (tabular):** Runs detect → read → map and returns the format info, column mapping, sample values, and confidence, plus row-accounting transparency fields: `has_header`, `skip_rows`, `rows_in_file` (the reader's reconciled row total — `skip_rows + header + rows_read + rows_skipped_trailing`), and `header_row_looks_like_data` (a red flag when the row consumed as the header parses as a transaction, e.g. an explicit `skip_rows` eating a data row or a headerless Excel sheet). A structural red flag forces `confidence` to `low` so a suspicious layout routes to the confirm gate instead of being self-accepted.
- **Behavior (PDF):** Runs the deterministic extraction rung via `ImportService.pdf_preview()`.
  - **Deterministic / non-escalating** — returns `data.{status="preview", channel="pdf", deterministic, decision_reason, confidence, row_count, fingerprint}`. `deterministic=True` means the rung cleanly structured the PDF as transactions; `deterministic=False` (e.g. `no_transaction_table`) means `import_files` would seed it.
  - **Bridge escalation** (low confidence, failed reconciliation, …) — returns `data.{status="confirmation_required", channel="pdf", reason, bridge_payload}` and writes the egress audit row. `bridge_payload` carries the document text + table preview + layout fingerprint + a **transparency notice** (proceeding surfaces the document's content to the agent) + a `request_kind`. The agent proposes a recipe + rows and ratifies via `import_confirm(bridge_response=...)`.
- **Service:** `ImportService.pdf_preview() -> PdfPreviewResult` (PDF); tabular detect/read/map (tabular).
- **CLI:** `moneybin import file-preview PATH`

### `import_formats`

List available import formats (tabular + PDF).

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns two arrays in one envelope. `formats` carries tabular formats (CSV/Excel/Parquet/etc.): `{name, institution_name, file_type, sign_convention, date_format, number_format, multi_account, header_signature}`. `pdf_formats` (Phase 2a) carries auto-derived PDF recipes keyed by layout fingerprint: `{name, institution_name, document_kind, routing, front_end, version, times_used, last_used_at}`. Both built-in and user-saved tabular formats appear; PDF formats are always system-derived.
- **Service:** `import_tools.import_formats() -> ResponseEnvelope[ImportFormatsPayload]` (returns `formats` + `pdf_formats` arrays as described above)
- **CLI:** `moneybin import formats list [--type tabular|pdf|all]` — CLI offers a `--type` filter; the MCP tool returns the union (agent filters by reading the relevant array).

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
- **Behavior:** Returns array of `{transaction_id, account_id, account_name, txn_date, amount, description, merchant_id, merchant_normalized, age_days, priority_score, source_type, source_id, pending_transfer_match}` from `reports.uncategorized_queue`. `pending_transfer_match` is `true` when the row carries an unresolved (pending, unreversed) `app.match_decisions` entry — categorizing it would double-count against the eventual transfer pair once matching resolves. Rows are flagged, never hidden; when any are flagged, `actions[]` adds a hint to resolve the match first via `transactions_matches_run` / `transactions_matches_set`. Amounts use the accounting convention: negative = expense, positive = income. Degraded response returns uncategorized count by account and time period.
- **Service:** `CategorizationService.list_uncategorized_transactions(limit, sort, min_amount, account_id)`
- **CLI:** `moneybin transactions categorize pending [--limit N] [--sort date|impact] [--min-amount N] [--account NAME]`

### `transactions_categorize_assist`

Return uncategorized transactions as PII-scrubbed rows for LLM-assisted categorization.

- **Sensitivity:** `medium` — descriptions leave the machine.
- **Unique parameters:** `limit: int | None = None` (defaults to `assist_default_batch_size`, 100), `account_filter: list[str] | None = None`, `date_range: dict[str, str] | None = None`.
- **Behavior:** Returns `RedactedTransaction` rows (see `categorization-matching-mechanics.md` §"`RedactedTransaction` schema") as `{transaction_id, description_scrubbed, memo_scrubbed, source_type, transaction_type, check_number, is_transfer, transfer_pair_id, payment_channel, amount_sign}`. Merchant text (`description`/`memo`) is sent **in full** — it is the categorization signal — with only embedded PII (e.g. account numbers in the memo) masked via `redact_for_llm()`. No amounts, dates, or account identifiers are ever sent; `amount_sign` (`'+'`/`'-'`/`'0'`) is the only direction hint. The LLM proposes `(category, subcategory, canonical_merchant_name)` per row; the user reviews; the LLM persists accepted decisions via `transactions_categorize_commit` with `categorized_by='ai'`.
- **Field history:** `description_scrubbed`/`memo_scrubbed` were named `description_redacted`/`memo_redacted` before this rename — the old names claimed descriptions were withheld, which was never true. Behavior is unchanged.
- **Service:** `categorize_assist()` (`src/moneybin/services/categorization/assist.py`)
- **CLI:** `moneybin transactions categorize assist [--limit N] [--account-filter a,b]`; `moneybin transactions categorize export-uncategorized` writes the same shape to a file for pipeline use.

### `transactions_categorize_commit`

*Exemplar — see section 2.3.*

### `transactions_categorize_run`

Run the categorization engine cascade over uncategorized transactions.

- **Sensitivity:** `medium` — writes categorizations to `app.transaction_categories`.
- **Unique parameters:** `methods: list[Literal["rules", "merchants"]] | None` (optional, default `["rules", "merchants"]`) — engines to run in order. A rule write blocks a merchant write at the same priority.
- **Behavior:** Returns `{applied_by_method: {rules: int, merchants: int}, total_applied: int}`. The canonical order `["rules", "merchants"]` takes an optimized shared-scan path; other orders run engines individually in the requested order. The `"ml"` literal will be added when ML categorization implementation lands.
- **Service:** `CategorizationService.categorize_run(methods=...) -> dict`
- **CLI:** `moneybin transactions categorize run [--methods rules,merchants] [--output json]`

### `transactions_categorize_improve_ai`

Re-categorize AI-guessed transactions to confident provider-native categories.

- **Sensitivity:** `low` — returns only an aggregate count.
- **Unique parameters:** None.
- **Behavior:** Reverse-looks-up every transaction currently `categorized_by='ai'` against the Plaid category bridge (`core.bridge_category_source_map`); upgrades it to `provider_native` only when the bridge match is at MEDIUM confidence or higher. Only rewrites rows currently `categorized_by='ai'` — user, rule, and merchant categorizations are never overwritten. Writes `app.transaction_categories`; revert by re-categorizing the transaction (a user edit wins at priority 1). Returns `{upgraded_count: int}`.
- **Service:** `CategorizationService.improve_ai_categories() -> int`
- **CLI:** `moneybin transactions categorize improve-ai [--output json]`

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
- **Unique parameters:** `rules: list[object]` (required) — list of `{name, merchant_pattern, category, subcategory?, match_type?, min_amount?, max_amount?, account_id?, priority?}`. `allow_broad: bool` (default `False`) — see Behavior.
- **Behavior:** Idempotent. Each item is deduped against active rules by the matcher+output tuple — `(merchant_pattern, match_type, min_amount, max_amount, account_id, category, subcategory)`. `name` and `priority` are metadata and excluded from the dedup key, so renaming a rule or shuffling priorities does not create a new row. If an active rule with the same key exists, the existing `rule_id` is returned. A `contains` item whose pattern is shorter than `auto_rule_min_contains_length` (default 4) is refused rather than inserted — it would match unrelated merchants (e.g. `contains "TO"` matches STORE/AUTO/TOTAL) — unless `allow_broad=True`; refused items are counted in `skipped` and explained in `error_details`. `exact` patterns are never gated. Returns `{created, existing, skipped, errors, error_details, rule_ids}`.
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
- **Behavior:** Base response: `{total_transactions, categorized, uncategorized, percent_categorized, by_source, plaid_unmapped}` where `by_source` breaks down by categorization source — one bucket per persisted `categorized_by` value (`user`, `rule`, `auto_rule`, `migration`, `ml`, `provider_native`, `ai`) plus a reporting-only `merchant_map` bucket split out of `rule` for rows written via merchant-pattern matching (the persisted `categorized_by` value on those rows is still `rule`; this split makes `by_source` reconcile with `transactions_categorize_rules`' rule list) — and `plaid_unmapped` counts Plaid transactions whose PFC code has no `core.bridge_category_source_map` mapping yet (omitted when no Plaid data is present). With `include_auto=True`, returns `{overall: <base>, auto: {active_auto_rules, pending_proposals, transactions_categorized}}`. The `auto` block absorbs what was previously the standalone `transactions_categorize_auto_stats` tool.
- **Service:** `CategorizationService.stats() -> CategorizationStats`; with `include_auto=True` also calls `AutoRuleService.stats() -> AutoStatsResult`.
- **CLI:** `moneybin transactions categorize stats`

### `transactions_categorize_rules_apply`

**Retired.** The standalone rule-engine apply tool is folded into the polymorphic engine cascade. Use `transactions_categorize_run(methods=["rules"])` (MCP) or `moneybin transactions categorize run --methods rules` (CLI) instead. See `transactions_categorize_run` above.

### `transactions_categorize_auto_review`

List auto-generated rules pending user approval.

- **Sensitivity:** `low`
- **Unique parameters:** None.
- **Behavior:** Returns array of `{proposed_rule_id, merchant_pattern, match_type, category, subcategory, trigger_count, sample_txn_ids, estimated_match_count, is_broad}`. `estimated_match_count` is how many transactions the proposed pattern would actually match today, computed with the live matcher's own predicate (not an approximation). `is_broad` is `true` when the blast radius outruns the evidence behind the proposal (both an absolute floor and a ratio against `trigger_count` — see `auto_rule_broad_match_min` / `auto_rule_broad_match_factor` in `config.py`). A broad proposal cannot be promoted via `transactions_categorize_auto_accept` without an explicit `allow_broad` override. When any returned proposal is broad, `actions[]` adds a hint naming the count and pointing at `estimated_match_count`.
- **Service:** `AutoRuleService.review() -> AutoReviewResult`
- **CLI:** `moneybin transactions categorize auto review`
- **Dependency:** [Categorization overview](categorization-overview.md) (Pillar E: auto-rule generation), [Auto-rule generation](categorization-auto-rules.md).

### `transactions_categorize_auto_accept`

Accept or reject proposed auto-generated rules. Takes two parallel ID lists.

- **Sensitivity:** `low`
- **Unique parameters:** `accept: list[str]` (proposed_rule_ids to promote), `reject: list[str]` (proposed_rule_ids to refuse), `allow_broad: bool = False` — required to accept a proposal that `transactions_categorize_auto_review` flagged `is_broad`; without it, such ids are skipped rather than promoted (they count toward the response's `skipped`, not `accepted`).
- **Behavior:** Accepted rules are promoted to active categorization rules in `app.categorization_rules` with `created_by='auto_rule'` and immediately evaluated against uncategorized transactions. Rejected rules are not re-proposed for the same pattern. IDs appearing in both `accept` and `reject` are dropped from `accept` — an explicit reject always wins. Returns `{approved, rejected, skipped, newly_categorized, rule_ids}`.
- **Mutation surface:** writes to `app.categorization_rules` (accept path) and `app.proposed_rules` (status transitions). Revert via `transactions_categorize_rules_delete` for promoted rules.
- **Service:** `AutoRuleService.accept(accept=..., reject=..., allow_broad=...) -> AutoConfirmResult`
- **CLI:** `moneybin transactions categorize auto accept --accept <id> [<id>...] --reject <id> [<id>...] [--allow-broad]`
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

**Status: de-registered / removed (2026-05-23).** The tool was removed because it
synthesized its result from `BudgetService` rather than reading a `reports.*`
view, violating the `reports_*` = reads-a-view convention now enforced by the
report framework (§12, §17d). It **re-registers automatically through the report
framework** once M3C ships a `reports.budget` view — at which point a `@report`
runner generates the tool, CLI command, and `TableRef` from the view's structured
comment block (no hand-written tool). The budget-vs-actual design intent below is
retained as the target shape for that view.

Budget vs actual spending comparison for a month.

- **Sensitivity:** `low` — returns aggregates (budget target, total spent, remaining).
- **Unique parameters:** `month: str?` (YYYY-MM, defaults to current month).
- **Behavior:** Returns array of `{category, budget, spent, remaining, percent_used, status}` where status is `OK`, `WARNING` (>90%), or `OVER`. Includes only categories with active budgets. At `detail=full`, includes per-week spending pace within the month.
- **Service:** `BudgetService.status() -> list[BudgetStatus]`
- **CLI:** `moneybin reports budget status [--month YYYY-MM]`

### `reports_budget_summary`

**Status: de-registered / removed (2026-05-23)** — same rationale as `reports_budget`
above (synthesized from `BudgetService`, not a `reports.*` view). Re-registers via
the report framework when M3C ships the backing `reports.budget` view. Design intent
below is retained as the target trend shape.

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

**Service class:** `ConsentService`

**Status:** The consent ledger shipped in PR 3. All four `privacy_*` tools below are registered today. The **enforcement gate** (degraded/aggregate responses without consent) is deferred — granting/revoking records and reports consent but does not yet gate data; CRITICAL fields remain always-masked (PR 2).

### `privacy_status`

Current consent state, configured AI backend, and privacy mode.

- **Sensitivity:** `low` — metadata about privacy configuration, not financial data.
- **Unique parameters:** None.
- **Behavior:** Returns active grants, configured backend, and `consent_policy` (the AIConfig standard/strict policy; distinct from each grant's per-grant `consent_mode`). Useful when the AI needs to check consent state before attempting a sensitive operation.
- **Service:** `ConsentService.status()`
- **CLI:** `moneybin privacy status`

### `privacy_consent_grant`

Grant consent for a privacy feature category.

- **Sensitivity:** `low` — modifying consent state, not accessing financial data.
- **Unique parameters:** `category: str` (required — e.g., `mcp-data-sharing`), `backend: str?` (override configured backend for this grant), `mode: "persistent" | "one-time"` (default `"persistent"`).
- **Behavior:** Writes to `app.ai_consent_grants` with a paired `app.audit_log` row (Invariant 10). Idempotent per `(category, backend)` — re-granting an active grant returns the existing grant. `read_only=False`. Revert via `privacy_consent_revoke`.
- **Service:** `ConsentService.grant_consent()`
- **CLI:** `moneybin privacy grant`

### `privacy_consent_revoke`

Revoke a previously granted consent.

- **Sensitivity:** `low`
- **Unique parameters:** `category: str` (required), `backend: str?`.
- **Behavior:** Sets `revoked_at` on the active grant row; the row is retained for audit. Returns confirmation with revocation timestamp. `read_only=False`. Revert via `privacy_consent_grant`.
- **Service:** `ConsentService.revoke_consent()`
- **CLI:** `moneybin privacy revoke`

### `privacy_log`

Query recent privacy-log events.

- **Sensitivity:** `low` — the privacy log records consent grants/revokes and MCP/CLI tool calls (metadata only: sensitivity tier, row count, actor). No financial data.
- **Unique parameters:** `last: int?` (number of recent events to return), `actor: str?` (filter by originating command or tool name).
- **Behavior:** Reads `<profile>/privacy.log.jsonl`; returns recent events in reverse-chronological order. `read_only=True`.
- **Service:** privacy log reader (`read_privacy_events`) over `privacy.log.jsonl`.
- **CLI:** `moneybin privacy log`

---

## 12. `system_*`, `reports_*`, `refresh_run` — Data status, reports projections, and the refresh umbrella

**Service class:** `SystemService`

### `system_status`

Data status dashboard — what data exists, how fresh it is, what's pending action.

- **Sensitivity:** `low` — counts and dates only.
- **Unique parameters:** None.
- **Behavior:** Returns `{accounts: {count}, transactions: {count, date_range, last_import_at}, matches: {pending_review}, account_links: {pending_review}, merchant_links: {pending_review}, security_links: {pending_review}, categorization: {uncategorized}, transforms: {pending, last_apply_at}, schema_drift: {tables[], remediation} | null, gsheet: {total_connections, by_status, needs_attention[]}, database_connections: {writers[], readers[]}}`. `security_links` was added M1G.4 Task 12 (2026-07-11), covering the fifth review queue alongside `system status`/`review`. Degrades to a zero-filled envelope with `summary.degraded=true` when a writer holds the database lock — `database_connections` still names the holder in that case (see `DatabaseLockError` recovery). This is the tool version of the `moneybin://status` resource with richer detail.
- **Service:** `SystemService.status() -> SystemStatus`
- **CLI:** `moneybin system status`

### `system_doctor`

Pipeline integrity check — confirms the data pipeline is self-consistent before analysis.

- **Sensitivity:** `low` — counts and status labels only.
- **Unique parameters:** `full: bool = False` — when true, the protected-`app.*` audit-coverage checks scan every row instead of the default sampled, recent-rows-only window.
- **Behavior:** Runs all SQLMesh named audits (FK integrity, sign convention, transfer balance) plus hardcoded checks (staging coverage, categorization coverage) and per-table `app.*` audit-coverage + uniqueness invariants. Returns pass/fail/warn per invariant and total transaction count. Always runs with `verbose=False` — agents can query `core.fct_transactions` or `core.bridge_transfers` directly for drill-down. Exit is informational only (no exception on fail).
- **Service:** `DoctorService.run_all(verbose=False, full=False) -> DoctorReport`
- **CLI:** `moneybin system doctor [--verbose] [--full] [--output json]`

### `system_audit_undo` / `system_audit_history` / `system_audit_get` — audit-log undo consumer

The undo consumer for any audited `app.*` mutation (REC-PR3 / Invariant 10 Phase 2). The read-only `system_audit` event-list tool stays in the curation module; these three undo-domain tools live in `mcp/tools/system.py` and share the `system_audit_*` prefix. All operator territory — positioned for reviewing and reversing recent agent changes, not promoted in the `instructions` enumeration. **Service class:** `UndoService`.

#### `system_audit_undo`

Reverse every `app.*` mutation in one operation as a unit, keyed on `operation_id`.

- **Sensitivity:** `low` — the response carries only ids, a row count, and table names; no financial values. (The operation still mutates `app.*` — see Mutation surface.)
- **Unique parameters:** `operation_id: str` (required).
- **Behavior:** Loads every audit row for the operation and synthesizes each row's inverse from its full before/after image (insert→delete, update→restore-before, delete→reinsert). Writes new audit rows with `is_undo=TRUE`, `undoes_operation_id=<original>`, and a fresh `operation_id` of its own, returned as `undo_operation_id` so the undo is itself undoable. Marker rows (`target_id IS NULL`, e.g. the `tag.rename` parent) are skipped; only the per-row children are reversed (an operation with *only* marker rows — e.g. a `tag.rename` that matched zero transactions — has nothing to reverse and is refused with `recovery_no_path` rather than returning an undo id that doesn't exist). **Block, don't cascade:** if a later live operation modified the same `(target_table, target_id)`, it refuses with `undo_cascade_blocked` and lists blocker operation ids in `recovery_actions` (newest first). Other refusals: `undo_operation_not_found`, `undo_already_undone` (suggests undoing the undo), and `recovery_no_path` (the operation touched a table outside the undoable `app.*` surface, e.g. `raw.manual_transactions` from manual entry — re-import to recover).
- **Mutation surface:** writes `app.audit_log` plus the reversed `app.*` rows. Revert by calling `system_audit_undo` again on the returned `undo_operation_id`.
- **Annotations:** `read_only=False`, `destructive=False`, `idempotent=False` (a second undo of the same op raises `undo_already_undone`).
- **Service:** `UndoService.undo(operation_id, actor) -> UndoResult`
- **CLI:** `moneybin system audit undo <operation_id>`

#### `system_audit_history`

List recent audited operations grouped by `operation_id`, newest first — the pull surface for reversing a change when no error preceded the regret.

- **Sensitivity:** `low` — operation ids, actor, action verbs, table names, counts, and undoability flags; no financial values.
- **Unique parameters:** `domain: str?` (action-family filter, e.g. `"tag"` → any `tag.*` row), `since: str?` (ISO timestamp lower bound), `actor: str?`, `limit: int = 50`, `include_undone: bool = False`.
- **Behavior:** Each entry carries `{operation_id, occurred_at, actor, actions[], tables[], row_count, is_undo, undoes_operation_id, can_undo, undo_blocked_by, recovery_actions[]}`. `can_undo` is false when the operation was already undone, is cascade-blocked (`undo_blocked_by` lists the blockers to undo first), or touched a non-undoable table. `recovery_actions[]` carries the pre-built `system_audit_undo` call(s) for the entry's state (undo it, undo the blockers, or undo the undo) — the same structured shape the error envelope uses, so an agent reading history executes the action directly instead of re-deriving it from the raw ids. By default the undo operations themselves (`is_undo=TRUE`) are hidden; `include_undone=True` shows them, and the originals they reversed always appear with `can_undo=False`. (The operation context records `operation_id`/`actor` but not the originating tool name/arguments, so entries summarize by action verb rather than the spec's `tool`/`arguments` fields.)
- **Service:** `UndoService.history(...) -> list[OperationSummary]`
- **CLI:** `moneybin system audit history [--domain X] [--since TS] [--actor A] [--limit N] [--include-undone]`

#### `system_audit_get`

Full before/after for every row of one operation — inspect exactly what an undo would change before running it.

- **Sensitivity:** `high` — `before_value`/`after_value` can carry financial amounts (`TXN_AMOUNT`), same tier as `system_audit`.
- **Unique parameters:** `operation_id: str` (required).
- **Behavior:** Returns `{operation_id, events[], can_undo, undo_blocked_by}` where each event is the full audit row (the `system_audit` event shape). Raises `undo_operation_not_found` for an unknown id.
- **Service:** `UndoService.get(operation_id) -> OperationDetail`
- **CLI:** `moneybin system audit get <operation_id>`

### `extension_validate`

**Status:** planned. Backing spec [`extension-contracts.md`](extension-contracts.md) is `draft` — the tool is documented here for design alignment but NOT registered on the MCP surface until the spec reaches `in-progress` and the validator lands. See §17c dependency tracker.

Validate an extension manifest (Analysis Package or standalone Report) against the framework's registration rules. Operator-territory: invoked by contributor skills, CI, and humans before opening a PR — not by user-facing agents in normal workflows.

- **Sensitivity:** `low` — inputs are extension manifest paths, not financial data; outputs are validation findings (rule IDs, file paths, severity).
- **Annotations:** `read_only=True`, `destructive=False`, `idempotent=True`, `open_world=False`.
- **Unique parameters:** `path: str` (required) — filesystem path to a package directory (containing `moneybin_package.yaml`) or to a standalone-report manifest file.
- **Behavior:** Runs the checks defined in [`extension-contracts.md`](extension-contracts.md) §"The extension validator": manifest schema validity, capability-vs-SQL match (every `CREATE TABLE`/`VIEW` covered by `capabilities.writes`), prefix discipline (tables, tools, CLI commands, schema files all start with `owns_prefix`), Quality Scale claim matches evidence, SQL compiles against the current canonical schema, no prefix collisions with already-registered extensions, and the package's own test suite passes. Returns the standard `ResponseEnvelope` so invoking agents can react programmatically; per-check results land in `data[]` as `{rule_id, severity, message, file?, line?}`.
- **Service:** `ExtensionValidatorService.validate(path) -> ValidationReport` (planned).
- **CLI:** `moneybin extension validate PATH` (per CLI↔MCP parity).

> Note: a unified `reports_health` snapshot tool is **planned, not registered** (its CLI stub was removed 2026-05-23). Today the agent composes the same view from `reports_networth` + `reports_spending` + `reports_cashflow`. Re-evaluate as a dedicated tool when agent-experience reports show the composition friction outweighs the surface cost.

> **Implementation note (report framework, 2026-05-23):** The six view-backed
> reports — `reports_cashflow`, `reports_spending`, `reports_recurring`,
> `reports_merchants`, `reports_large_transactions`, `reports_balance_drift` —
> are no longer hand-written service wrappers. They are generated from `@report`
> runners in `moneybin.reports.definitions` and registered via
> `register_reports_mcp` (see §17d and [`extension-contracts.md`](extension-contracts.md)
> §"Report contract"). **Tool names, parameters, sensitivity tiers, and response
> envelopes are unchanged** — only the implementation moved from `ReportsService`
> methods to the framework. `reports_networth` / `reports_networth_history` stay
> hand-written (they are `NetworthService`-backed, not single `reports.*` view
> reads — a documented exception).

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
- **Behavior:** Single user-facing entry point for the refresh domain. Idempotent; safe to retry after a failure. Matching and categorization are best-effort: a SQLMesh apply error fails the call (`error`), but a matcher/categorizer crash does not abort the pipeline — it is surfaced instead of swallowed. Returns `{applied, duration_seconds, error?, matching_error?, categorization_error?, self_heal_actions}`. `matching_error` / `categorization_error` carry a real crash's message (a first-load missing-view precondition is not a crash and leaves them absent); `self_heal_actions` lists self-heal recipes that ran (empty until the M1L self-heal safelist lands). When a step crashes, the envelope's `recovery_actions` carries the targeted retry (`refresh_run(steps=["match"])` and/or `refresh_run(steps=["categorize"])`) followed by a single `system_doctor` diagnostic. On apply failure, `actions[]` hints at `moneybin transform plan` (CLI operator tool) to inspect, or `refresh_run` to retry. When `steps` includes `match` but excludes `categorize`, `actions[]` includes a follow-up hint pointing at `refresh_run(steps=["categorize"])`. Unknown step names raise `UserError(code="UNKNOWN_REFRESH_STEP")`. Symmetric with `transactions_categorize_run(methods=...)`.
- **Service:** `moneybin.services.refresh.refresh(db, *, steps=None) -> RefreshResult`
- **CLI:** `moneybin refresh [--step STEP]... [--output json] [-q]`

---

## 13. `sql.*` — Direct SQL access

`sql_query` uses `get_db()` directly with query validation from the privacy module. No dedicated service class — this is a power-user escape hatch, not a structured service.

### `sql_query`

Execute a read-only SQL query against the `core` and `app` schemas.

- **Sensitivity:** per-call — derived from the query's output columns via sqlglot lineage. CRITICAL if any output column is `ACCOUNT_IDENTIFIER`, `INSTITUTION_ACCOUNT_NUMBER`, or `ROUTING_NUMBER`; otherwise the max tier across classified output columns. Metadata statements are `low`.
- **Unique parameters:** `query: str` (required).
- **Behavior:** Validates query is read-only (SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN). Blocks file-access functions (`read_csv`, `read_parquet`, etc.) and URL literals. **Data queries may reference only the `core` and `app` schemas** — the schemas the privacy registry classifies, so masking is sound; a query touching any other schema (`raw`/`prep`/`reports`/`meta`) is refused with `sql_schema_not_allowed` (use the `reports_*` tools for curated views). `DESCRIBE`/`SHOW`/`PRAGMA`/`EXPLAIN` return schema/plan text, not row data, and execute directly as low-sensitivity metadata (no lineage). Each data-query output column is resolved to its `DataClass` via sqlglot lineage against the live `core.*`/`app.*` schema snapshot — including across all branches of a `UNION`; CRITICAL-tier columns (account and routing numbers) are masked using the same rules as the typed tools (`****<last4>` for account numbers, `*****` for routing numbers). HIGH/MEDIUM/LOW columns (amounts, descriptions, dates) pass through in the clear — same behavior as `transactions_get` and other typed tools. Results are capped at `MAX_ROWS`; when truncated, `summary.has_more` is true and `summary.total_count` exceeds `returned_count`. Returns the standard response envelope with per-query `summary.sensitivity`; `summary.classes_returned` lists the resolved `DataClass` values for audit. For the privacy-safe agent path, prefer this tool over direct `moneybin db query` CLI access (which has no privacy middleware).
- **CLI:** `moneybin db query "SELECT ..." [-o text|json|csv|markdown|box]` — direct DB access, no privacy middleware; see operator-bypass banner in that command's help.

### `sql_schema`

Return the curated database schema for ad-hoc SQL composition. Equivalent to reading the `moneybin://schema` MCP resource — provided as a tool for hosts that don't surface MCP resources to the model (e.g. Claude.ai chat).

- **Sensitivity:** `low` — schema metadata, no row data.
- **Unique parameters:** `table: str | None`. `None` (default) returns the compact catalog (table names + purposes + column counts). A full name like `'core.fct_transactions'` returns columns, comments, and example queries for that one table. `'*'` returns the full ~50KB schema document.
- **Behavior:** Unknown `table` returns a `UserError(code='sql_unknown_table')` with the available-tables list as a hint.
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

**Relevant tools:** `reports_spending` (with `category` filter for drill-down), `reports_cashflow`, `reports_recurring`, `reports_merchants`, `reports_networth`

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
| `sync_link [institution]` | medium | Initiates Plaid Hosted Link flow. Returns `{session_id, link_url, expiration}`. `link_url` is a one-time bearer credential — treat as medium sensitivity. Pass `institution` to re-authenticate (Plaid update mode). |
| `sync_link_status <session_id>` | low | Single-shot check of a link session. Returns `{session_id, status, provider_item_id, institution_name, error, expiration}`. Does NOT poll internally — the agent invokes this when the user signals completion. |
| `sync_disconnect <institution>` | medium | Removes institution by name. No revert path. |
| `sync_pull [institution] [force] [refresh=true]` | medium | Triggers sync for one or all institutions; loads `raw.plaid_*` and propagates through SQLMesh. Amounts follow MoneyBin convention (negative = expense). When `refresh` (default true) and the sync changes raw state, the post-load refresh pipeline (matching + SQLMesh apply + categorization) runs once at end-of-pull so `core.dim_accounts` reflects new data before returning. Result envelope adds `transforms_applied`, `transforms_duration_seconds`, `transforms_error` (SQLMesh-step outcome — matching and categorization are log-only on failure) **and the investment outcome the CLI reports**: `securities_loaded`, `investment_transactions_loaded`, `holdings_loaded`, `holding_lots_loaded`, `opening_bootstrap_rows`, `investment_source_overlap_accounts`, `security_resolution` (per-outcome counts: adopted / auto_bound / minted / proposed / pending) and `security_resolution_error`. A pull that soft-failed resolution MUST NOT be reported as a clean success — there is no source-native fallback for `security_id` and `cost_basis.py` skips every NULL-security event, so those buys/sells silently vanish from lots and realized gains. The CLI warns and exits non-zero; MCP has no exit code, so the same signal leads the envelope's `actions[]`. |
| `sync_status` | low | Read-only: connected institutions, last-sync times, guidance for error states. |

**CLI-only (security-justified):** `sync_login`, `sync_logout` (browser interaction + credential handling routed through LLM context is a security-model violation); `sync_rotate_key` — passphrase material through LLM context is a security-model violation.

**Prompts:**

| Prompt | Behavior |
|---|---|
| `sync_review` | Agent-driven health check. Walks the agent through `sync_status` + a quick `reports_spending` pulse to flag errored institutions, stale connections (last_sync > 7 days), and volume anomalies. Output is constrained to counts / dates / status codes / institution names — no PII. |

**Verb rename (PR #197).** The mediated-provider verb was renamed from
`sync_connect` / `sync_connect_status` to `sync_link` / `sync_link_status`
to lock the `_link` (mediated provider) vs `_connect` (user-controlled
storage) semantic split before launch. The old names remain registered as
deprecated aliases with `logger.warning()` for one minor release; they
forward to the canonical implementation via `.__wrapped__` to avoid
double-firing the `@mcp_tool` decorator machinery (audit log, timeout
guard). Full rationale: [`surface-design.md`](../../.claude/rules/surface-design.md)
verb vocabulary, [`connect-gsheet.md`](connect-gsheet.md) §verb-rename.

---

## 16b. `gsheet_*` — Google Sheets (user-controlled storage)

User-controlled-storage `connect-*` family per
[`surface-design.md`](../../.claude/rules/surface-design.md) verb vocabulary:
the client speaks Google's API directly via OAuth tokens stored in
SecretStore (no server mediation). Distinct from `sync_*` which routes
through a mediated provider (Plaid). Full design:
[`connect-gsheet.md`](connect-gsheet.md).

| Tool | Sensitivity | Behavior |
|---|---|---|
| `gsheet_auth` [`force_reauth=False`] | medium | OAuth 2.0 PKCE installed-app flow. Opens the user's browser, listens on a 127.0.0.1 loopback callback, persists tokens to SecretStore. Tokens never enter the MCP wire or LLM context. Short-circuits with `status='already_authorized'` when a refresh token is on file unless `force_reauth=True`. Per-tool timeout raised to 180s for the consent click-through. Mutation surface: SecretStore. Revert: visit https://myaccount.google.com/permissions and revoke. |
| `gsheet_connect` [`url`, `adapter`, `alias`, `account_name`, `account_id`, `column_mapping`, `yes`, `accept_seed_fallback`, `no_initial_pull`] | medium | Bind a Google Sheet for live sync. Runs column detection, persists to `app.gsheet_connections` (audited), and (by default) executes the initial pull. Amounts use MoneyBin's accounting convention (negative = expense). Accepts `account_name` as free-text resolved via `AccountService.resolve_strict` (or `account_id` directly). Medium-confidence detection requires `yes=True`; low-confidence refuses unless `accept_seed_fallback=True` falls through to the seed adapter. Same 180s timeout as `gsheet_auth` for the first-run-before-auth path that triggers OAuth inline. Mutation surface: `app.gsheet_connections` + `raw.tabular_transactions` (transactions adapter) or `raw.gsheet_seeds` (seed adapter). Revert: `gsheet_disconnect(connection_id, purge=True)`. |
| `gsheet_pull` [`connection_id`] | medium | Pull latest content for one connection by ID, or every healthy connection when omitted. Per-connection isolation — one failure doesn't block siblings. Returns `{status, load_result, drift_reason, error_message}` per connection. `auth_expired` and `drift_detected` connections surface `actions[]` hints pointing at `gsheet_auth` / `gsheet_reconnect` respectively. Mutation surface: raw rows for the connection's adapter. |
| `gsheet` | low | List every Google Sheets connection (`connection_id`, adapter, status, last_pull_at, last_success_at, etc.). Drift-detected connections surface a `gsheet_reconnect` hint in `actions[]`. |
| `gsheet_status` [`connection_id`] | low | Snapshot status for one connection by ID, or a roll-up across all connections when omitted. Drift-detected connections surface a `gsheet_reconnect` hint. |
| `gsheet_reconnect` [`connection_id`, `yes`] | medium | Re-detect sheet structure, re-pin the column mapping, run a pull. Use after drift_detected. Medium-confidence remaps require `yes=True` (symmetric to `gsheet_connect`). Mutation surface: `app.gsheet_connections.column_mapping` + `header_signature` (audited). No revert — the connection re-binds to the sheet's current shape. |
| `gsheet_disconnect` [`connection_id`, `purge=False`] | medium | Soft-disconnect (default) marks `status='disconnected'` and retains raw rows for analytics. `purge=True` hard-deletes the connection row, drops the per-connection seed view (if any), and deletes raw rows. Purge is permanent — no revert. |

**No CLI-only carve-outs in this domain.** `gsheet_auth` was initially
designed CLI-only on the assumption that OAuth's browser flow "has no
MCP equivalent." That contradicted `mcp.md`'s explicit "Needs
OAuth / browser" guidance ("tools can return redirect URLs; clients open
them"). Since the MCP server runs locally, the loopback callback lands
on the same host — `gsheet_auth` runs the full PKCE flow in-process and
returns the result. The hosted-MCP variant (M3H+) will need a different
shape; tracked separately and does not block local launch.

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

Reserved for [`asset-tracking.md`](asset-tracking.md). Workflows (registration, valuation, liability linking, staleness) are defined there. Per the surface-discipline rule in `.claude/rules/mcp.md`, no `assets_*` tools register until the backing spec reaches `in-progress`; this entry exists so reviewers can confirm the namespace is reserved.

---

## 17c. Dependency tracker

Tools that depend on unbuilt subsystems are documented in the catalog with dependency markers but **are not registered on the MCP surface until their backing spec reaches `in-progress` or `implemented`** in `docs/specs/INDEX.md`. The "Blocked tools" column below names the future tool set; today only entries whose dependency is `in-progress`/`implemented` are live. See `.claude/rules/mcp.md` "Surface change discipline" for the rule.

| Dependency | Status | Blocked tools |
|---|---|---|
| **Consent management spec** | Ledger shipped (PR 3); enforcement gate deferred | degraded/aggregate response behavior across the surface (enforcement gate) |
| **Audit log spec** | Not written | audit logging in middleware |
| **Redaction engine spec** | Not written | `accounts_get` field masking; `high` sensitivity behavior |
| **Provider profiles spec** | Not written | Verified-local bypass; `privacy_status` backend info |
| **Transaction matching (Pillar B — transfer detection)** | Draft (umbrella) | `transactions_matches.*` transfer-type filtering (dedup-type proposals are live; transfer-type proposals pending the Pillar B spec) |
| **[Categorization overview](categorization-overview.md)** | Draft | ML tools deferred — see `categorization-ml.md` (planned) |
| **Smart Import (Pillar A)** | Not written | `import_folder` |
| **Smart Import (Pillar F) + Privacy** | Not written | `import_ai_preview`, `import_ai_parse` |
| **Corrections table schema** | Not written | `transactions_correct` |
| **Annotations table schema** | Not written | `transactions_annotate` |
| **Budget tracking spec** | Draft | `reports_budget` / `reports_budget_summary` (read tools removed 2026-05-23 — re-register via the report framework when M3C ships a `reports.budget` view; `reports_budget_summary` rollover behavior pending the spec); `budget_set` (de-registered 2026-05-17 — re-register when spec reaches `in-progress`) |
| **Tax spec (none yet)** | Not written | `tax_w2`, `tax_deductions`, `tax_prep` prompt — **removed entirely** 2026-05-19; W-2 extraction pipeline cut; tax data ingestion to be re-designed from scratch when a new tax spec is written |
| **[Extension contracts](extension-contracts.md)** | Draft | `extension_validate` (manifest + capability + prefix + Quality-Scale validator); `<pkg>_*` tools from Analysis Packages (e.g., `assets_*`, `us_tax_*` at M2M); standalone-Report auto-registered `reports_*` tools |

### Tools shippable without dependencies

> **Surface status (2026-05-19):** All entries in §16 not marked "NOT registered" or de-registered are live and visible at connect. See the dependency tracker above for tools that remain blocked. `budget.*` and `transform_*` tool modules remain implemented but are **de-registered** in `src/moneybin/mcp/server.py:register_core_tools()`. `budget.*` re-registers when its backing spec reaches `in-progress`/`implemented`; `transform_*` are operator territory (category 2) and remain CLI-only (see §17). `tax.*` tools were **removed entirely** — the W-2 PDF extraction pipeline was cut; tax data ingestion will be re-designed from scratch when a new tax spec is written. A working implementation alone does not justify exposing a tool on the public surface.

## 17d. Entry-points-registered tools

**Status: planned, pending [`extension-contracts.md`](extension-contracts.md).** [`extension-contracts.md`](extension-contracts.md) is currently `draft`; the entry-points discovery path is documented here for design alignment but is NOT active at runtime until the spec reaches `in-progress` and the framework implementation lands (Plan 2 of the extension-contracts implementation graph). As of 2026-05-20, the tool catalog above is the complete registered surface — every tool is wired in by an explicit `register_*_tools(mcp)` call in `src/moneybin/mcp/server.py`.

When the entry-points path ships: per [`mcp-architecture.md`](mcp-architecture.md) §"Tool registration paths" and [`extension-contracts.md`](extension-contracts.md), the framework will also enumerate the `moneybin.packages` setuptools entry points at startup and invoke each package's `tools.register(mcp)` hook after the explicit-call path completes.

Two extension shapes will contribute tools through this second path:

- **Analysis Packages** (`extension-contracts.md` §"Analysis Package contract") — `<pkg>_*`-prefixed tools registered programmatically. The M2M first-party lineup will be `assets_*` and `us_tax_*` (both shipped in the MoneyBin repo, both contributed via the entry-points path rather than explicit register-call imports — see [`asset-tracking.md`](asset-tracking.md) and §17b reservation for the assets namespace). Third-party packages installed via PyPI will register identically once they appear on the entry-point group.
- **Standalone Reports** (`extension-contracts.md` §"Report contract") — `reports_*` tools auto-generated from structured comments on a single SQLMesh view, including the paired `TableRef` constant, `ReportsService` method, MCP tool, and CLI command. The contributor writes only the SQL with the `@name` / `@description` / `@param` / `@example` block; the framework derives the registration trinity.

The §18 tool-catalog-discipline rule will apply symmetrically: a PR that adds a package- or report-contributed tool must update this spec (or its in-package equivalent) in the same change. The runtime-registered surface — explicit-register-call plus entry-points — must match what's documented.

---

## 18. Standing audits and review discipline

These are recurring review responsibilities, not one-shot work items. Each is owned by the spec and enforced via a documented PR-review checklist or — where the rule is mechanical — a structural test.

### Tool description audit (checklist)

Every tool whose description omits a relevant invariant fails review. Required content per tool class:

- Tools that accept an `amount` field — sign convention, decimal precision, date format (per [§Tool descriptions: invariants must be in the description, not just the rule files](#tool-descriptions-invariants-must-be-in-the-description-not-just-the-rule-files)).
- Tools that mutate state — reversibility statement, ID-composite requirements, `app.audit_log` reference.
- Tools that return currency-bearing data — currency-pairing invariant per `architecture-shared-primitives.md` Invariant 7.

Enforcement: a checklist applied during PR review on every change to an `@mcp_tool`-decorated function. Codified in [`.claude/rules/mcp.md`](../../.claude/rules/mcp.md) under "Description requirements" so future tool authors apply it at write-time. **No structural pytest** — description text is prose, and a regex-based check produces noise (false positives on intentional convention overrides, false negatives on synonyms). Graduate to a structural test only when ≥3 invariants become statable as `(signature predicate, literal-string requirement)` pairs.

Discharge: a one-time audit pass on the existing surface ran as part of the 2026-05 MCP gap-closure PR; subsequent enforcement is per-PR reviewer responsibility.

### Tool catalog discipline (PR-review rule)

The tool list documented in this spec must match the runtime-registered set.
Without an executable parity check, documentation and registration drift
independently and agents receive a surface different from the one contributors
reviewed.

- **Rule:** any PR that adds, renames, or removes an `@mcp_tool`-decorated function MUST update this spec in the same change. Codified in [`.claude/rules/mcp.md`](../../.claude/rules/mcp.md) under "Surface change discipline."
- **Enforcement:** PR review. Reviewers grep for `@mcp_tool` diffs and verify each touches a corresponding spec section.
- **Why no automated test:** a fixture-based drift test would detect code-vs-fixture drift but not code-vs-spec drift (the original problem). A spec-parsing test would detect code-vs-spec drift but is fragile to spec restructuring. The PR-review rule directly addresses the documentation-discipline problem without introducing a third synced artifact. Revisit if review attention proves insufficient.
- **One-time audit:** the 2026-05 MCP gap-closure PR ran the first runtime-vs-spec diff; findings (orphaned spec entries, undocumented tools, visibility-tagging discrepancies) were resolved or logged as deferred follow-ups in that PR's CHANGELOG.

### Protocol-coverage matrix freshness

The matrix in [§Protocol-standard capability coverage matrix](#protocol-standard-capability-coverage-matrix) must be re-reviewed:

1. **Whenever the MCP spec adds or changes a capability.** Track the MCP spec changelog (https://modelcontextprotocol.io/) on a quarterly cadence; add new rows for any new capability before deciding adopt vs. defer.
2. **Whenever protocol evolution, client testing, or a user workflow surfaces a
   missing capability.** Record the gap in the matrix before deciding whether to
   implement or deliberately defer it.
3. **Whenever a row flips status** (`⏳ deliberate defer` → `📐 designed` → `✅ shipped`). The matrix is the audit trail for these transitions.

Owner: whoever maintains this spec. Cadence: quarterly review minimum, plus ad-hoc on the triggers above.
