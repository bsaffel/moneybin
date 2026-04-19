# Privacy & AI Trust

> Last updated: 2026-04-19 — promoted to ready
> Status: Ready — framework spec for MoneyBin's privacy model across all AI data flows.
> Companions: [`privacy-security-roadmap.md`](privacy-security-roadmap.md) (data custody tiers), [`ADR-002: Privacy Tiers`](../decisions/002-privacy-tiers.md) (data custody architecture), [`smart-import-overview.md`](smart-import-overview.md) (pillar F depends on this), [`matching-overview.md`](matching-overview.md) (audit log shared), `CLAUDE.md` Security section, `.claude/rules/security.md`

### Relationship to existing privacy docs

[ADR-002](../decisions/002-privacy-tiers.md) defines MoneyBin's **data custody model** — where data lives at rest (Local Only, Encrypted Sync, Managed). That model was designed before AI-first features were planned. This spec fills the gap: even in the Local Only custody tier, AI features may send data to external services. This spec governs **data in motion** to AI backends — what leaves, how it's minimized, when consent is required, and how every flow is audited. The two models are complementary: custody tiers define where data *lives*; AI trust tiers define how data *moves*.

## Mission

> **Financial data is sensitive. MoneyBin treats every external data flow — whether you initiated it or the system did — as something you should know about and control.**

MoneyBin is an AI-first financial application. AI makes it better — smarter imports, better categorization, conversational analysis. But "AI-first" does not mean "privacy-last." This spec defines how MoneyBin balances the two, honestly and without pretending the tension doesn't exist.

### Governing principles

1. **Minimize by default.** Always try to mask, aggregate, or limit data before it leaves the machine. If a question can be answered with category totals instead of individual transactions, send totals.
2. **Consent when minimization isn't sufficient.** If the question can't be answered with masked data, get the user's consent — at least once per flow category. The user sees what's going, where, and why.
3. **Every byte that leaves has a receipt.** The audit log records what was sent, to which backend, when, and what came back. The user can query it at any time. The log stores metadata, not a second copy of sensitive data.
4. **Capabilities matter alongside privacy.** When a user chooses an AI backend, they should see both the provider's privacy stance and what features it unlocks. MoneyBin does not artificially limit features to the lowest common denominator across providers — it helps the user make an informed choice.

## Data sensitivity taxonomy

MoneyBin classifies financial data fields into sensitivity tiers. These tiers determine default redaction behavior across all AI data flows.

| Tier | Fields | Default treatment |
|---|---|---|
| **Critical (PII)** | Account numbers, routing numbers, SSNs, full names, addresses, phone numbers | **Always masked.** Never sent unmasked to any external service, even with consent. Replaced with deterministic synthetic values (e.g., `****1234`) or structural placeholders. |
| **High (financial)** | Exact transaction amounts, account balances, income/salary figures, net worth | **Masked by default.** Can be sent unmasked when the user grants tier-2 or tier-3 consent. Masking: round to nearest order of magnitude or replace with synthetic values preserving format. |
| **Medium (behavioral)** | Transaction descriptions, merchant names, memo fields, specific dates | **The signal AI needs.** Format detection can sometimes work with synthetic samples; categorization and analysis cannot. Sent when consent is granted. Can be partially masked (truncated, generalized) for some use cases. |
| **Low (structural)** | Category labels, transaction types (debit/credit), column headers, date formats, institution names, currency codes | **Low privacy risk.** Sent freely in masked/minimized data flows. No consent required. |

The key tension this taxonomy makes explicit: **medium-tier data — descriptions and merchant names — is exactly what makes AI useful for financial analysis.** The spec does not pretend this can always be masked. Instead, it ensures the user knows when it's being shared and has the ability to control it.

## AI data flow tiers

Every path where data might leave the user's machine falls into one of four tiers. The tier determines the consent model.

### Tier 0 — Local only

**Examples:** Heuristic format detection (smart-import pillars A–E), local ML categorization (scikit-learn), auto-rule generation, all SQLMesh transformations, all DuckDB queries.

**Data state:** Never leaves the machine.
**Consent:** None required.
**Audit:** Not logged (no external flow to audit).

### Tier 1 — Masked / minimized

**Examples:** MCP tool returns spending totals by category (no individual transactions). MCP tool returns account count and types (no account numbers). Smart-import sends file structure metadata (column headers, row count, date format patterns) with all values replaced by synthetic data.

**Data state:** Leaves the machine, but critical and high-sensitivity fields are stripped or replaced. Only low-sensitivity and structural data is sent.
**Consent:** None required — the system handles minimization automatically.
**Audit:** Logged in `app.ai_audit_log` with `flow_tier=1`.

### Tier 2 — User-initiated, real data

**Examples:** MCP tool returns actual transaction descriptions and amounts in response to a user's query ("show me my Amazon purchases last month"). MCP tool returns account balances. Any MCP interaction where the user's question can only be answered with real medium/high-tier data.

**Data state:** Real financial data (descriptions, amounts, dates) sent to the AI backend. Critical fields (account numbers, SSNs) remain masked.
**Consent:** One-time persistent consent per feature category. Fires the first time the feature would send unmasked data. Persists across sessions until explicitly revoked.

**Feature categories for tier-2 consent:**
- `mcp-data-sharing` — MCP tools returning real transaction/account data
- `smart-import-parsing` — Smart Import sending file content to AI for format detection or parsing
- `ml-categorization` — ML categorization service sending descriptions to a cloud model (if using a cloud backend instead of local scikit-learn)
- `matching-overview` — Matching engine sending transaction details to AI for fuzzy matching assistance

**Consent prompt example:**
> MoneyBin MCP tools can share transaction details (descriptions, amounts, dates) with **Anthropic (Claude)** to answer your questions.
>
> **Privacy:** Anthropic does not train on API data. Account numbers and SSNs are always masked.
>
> Allow? This can be revoked anytime with `moneybin privacy revoke mcp-data-sharing`. [y/N]

### Tier 3 — Programmatic, real data

**Examples:** Smart Import pillar F sends a file preview to AI for parsing when heuristics fail. Future: autonomous AI analysis runs (scheduled spending summaries, anomaly detection).

**Data state:** Real financial data, sent by the system — not in response to a direct user question.
**Consent:** Per-invocation with redacted preview. Every call shows:
- What will be sent (redacted preview of the actual payload)
- Where it will be sent (backend name and provider privacy stance)
- What response shape is expected (e.g., "column mapping," not raw transactions)

The user confirms, declines, or switches backend each time. No persistent consent for tier 3 in v1. If a single user action (e.g., one file import) would trigger multiple AI calls, the consent prompt covers the full set of calls for that action — not one prompt per internal API call.

**Consent prompt example:**
> Smart Import couldn't auto-detect the format of `chase_2024.csv`.
>
> To parse this file, MoneyBin can send a preview to **Anthropic (Claude)**:
> - **Rows:** 5 sample rows (of 1,240 total)
> - **Masked:** Account numbers replaced with `****XXXX`, amounts with synthetic values
> - **Visible to AI:** Column headers, date formats, description text, transaction structure
>
> **Privacy:** Anthropic does not train on API data.
>
> Send preview? [y/N] or switch backend [b]

## Provider profiles

MoneyBin supports multiple AI backends through an abstract `AIBackend` interface. Each provider gets a profile documenting both its privacy stance and its capabilities. Profiles are hardcoded in each MoneyBin release and updated with every major or minor version.

### Profile schema

Each provider profile includes:

- **Provider name and version** — e.g., "Anthropic (Claude)" with the model used
- **Privacy stance**
  - Data retention policy (e.g., "30-day retention for abuse monitoring, no permanent storage")
  - Training policy (e.g., "API data is never used for training")
  - Compliance certifications (SOC 2, GDPR, etc.)
  - Data residency (where data is processed)
- **Capabilities**
  - MCP Apps support (yes/no)
  - Function/tool calling (yes/no)
  - Vision/image input (yes/no — matters for PDF and image parsing)
  - Structured output / JSON mode (yes/no)
  - Context window size (affects how much of a file can be sent)
  - Streaming support (yes/no)
- **Quality tier** — expected quality for MoneyBin's use cases, based on testing:
  - Format detection accuracy
  - Categorization accuracy
  - Document parsing accuracy
- **Cost model** — per-token, subscription, or free (local)
- **Local?** — whether data stays entirely on the user's machine

### v1 adapters

| Provider | Privacy | Key capabilities | Local? |
|---|---|---|---|
| **Anthropic (Claude)** | No training on API data; 30-day retention for trust & safety | MCP Apps, function calling, vision, structured output, large context | No |
| **OpenAI** | API data not used for training by default (can be confirmed in settings) | Function calling, vision, structured output, JSON mode | No |
| **Ollama** | Fully local — no data leaves the machine | Varies by model; no MCP Apps; context window varies | Yes |

Additional providers (Google Gemini, local llama.cpp, etc.) can be added by implementing the `AIBackend` interface and registering a provider profile.

### Surfacing profiles in consent prompts

When a consent prompt fires (tier 2 or 3), it includes the relevant provider's privacy stance — not just the provider name. The user sees *why* they might trust (or distrust) this provider before deciding. Provider capabilities are surfaced in setup/configuration, not in consent prompts (consent is about privacy, not features).

## Redaction engine

The redaction engine transforms data before it leaves the machine. It is the mechanism that makes tier-1 flows possible and that generates the "redacted preview" for tier-3 consent prompts.

### Design

- **Input:** Raw financial data (DataFrame, dict, or file content) + target tier
- **Output:** Redacted data appropriate for that tier, plus a mapping table for reverse-lookup
- **Deterministic:** Same input → same masked output. Uses seeded hashing (keyed to the user's local profile, never transmitted), not random replacement. This allows the AI's response (which references masked values) to be mapped back to real records.
- **Field-level rules:** Keyed to the data sensitivity taxonomy. Each field has a redaction strategy per tier:

| Field | Tier 1 (masked) | Tier 2/3 (consented) |
|---|---|---|
| Account number | `****1234` (last 4 preserved) | `****1234` (always masked — critical tier) |
| Amount | Synthetic (e.g., `$12.34`) | Real value |
| Description | `DESCRIPTION_001` | Real value |
| Date | Shifted (preserve format, offset by random-but-consistent days) | Real value |
| Category | Real value (low sensitivity) | Real value |
| Institution name | Real value (low sensitivity) | Real value |

- **What the user sees is what gets sent.** The redacted preview in tier-3 consent prompts is generated by the same engine that produces the actual payload. No divergence.

### Open design question

**Redaction for format detection specifically.** Can Smart Import pillar F's format detection work with fully synthetic sample rows (tier 1), or does it need real descriptions and amounts (tier 2) to detect patterns like European decimal commas, institution-specific description formats, or date ambiguity (MM/DD vs DD/MM)? Deferred to `smart-import-tabular.md` — the answer depends on the detection algorithm's sensitivity to real vs synthetic data.

## Audit log — "the receipt"

Every external AI call is recorded. The audit log is a user-facing feature, not just infrastructure — it's how the user "checks the receipt."

### Schema: `app.ai_audit_log`

| Column | Type | Description |
|---|---|---|
| `audit_id` | `VARCHAR PRIMARY KEY` | Unique identifier for this audit entry |
| `timestamp` | `TIMESTAMP` | When the call was made |
| `flow_tier` | `INTEGER` | 1, 2, or 3 (tier 0 is not logged — no external flow) |
| `feature` | `VARCHAR` | Which feature initiated the call (e.g., `smart_import_parse`, `mcp_transaction_query`) |
| `backend` | `VARCHAR` | Provider used (e.g., `anthropic`, `openai`, `ollama`) |
| `model` | `VARCHAR` | Specific model (e.g., `claude-sonnet-4-6`, `gpt-4o`, `llama3`) |
| `data_sent_summary` | `VARCHAR` | Human-readable summary: row count, field list, redaction level. NOT the actual data. |
| `data_sent_hash` | `VARCHAR` | SHA-256 of the actual payload for forensic verification without storing the payload |
| `response_summary` | `VARCHAR` | What came back: schema/shape description, not content |
| `consent_reference` | `VARCHAR` | Which consent grant authorized this call (FK to `app.ai_consent_grants`) |
| `user_initiated` | `BOOLEAN` | True if the user directly triggered this (MCP query); false if system-initiated (Smart Import) |

### Why no payload storage

The audit log stores metadata, not data. Storing the actual payload would:
- Create a second copy of sensitive financial data in a less-protected location
- Make the audit log itself a privacy liability
- Grow large quickly with file-based smart-import calls

The `data_sent_hash` allows forensic verification ("was this specific payload sent?") without retaining the payload.

### User interface

- **CLI:** `moneybin privacy audit-log [--last N] [--feature X] [--backend Y] [--since DATE]`
- **MCP:** `get_ai_audit_log` tool with the same filter parameters
- **Example output:**
  ```
  2026-04-17 14:32:01 | tier 3 | smart_import_parse | anthropic/claude-sonnet-4-6
    Sent: 5 sample rows (descriptions, dates, amounts — account numbers masked)
    Received: column mapping (8 columns detected)
    Consent: per-invocation #a1b2c3

  2026-04-17 14:15:22 | tier 2 | mcp_transaction_query | anthropic/claude-sonnet-4-6
    Sent: 23 transactions (descriptions, amounts, dates)
    Received: spending analysis response
    Consent: mcp-data-sharing (granted 2026-04-10)
  ```

## MCP field minimization

The MCP server returns structured data to the AI host. This section defines how MCP tools minimize data exposure by default, keeping most interactions at tier 1 (no consent needed).

### Tool-level sensitivity declarations

Each MCP tool declares the maximum data sensitivity tier its response contains:

```python
@mcp_tool(sensitivity="low")  # tier 0/1 — aggregates only
def get_spending_by_category(month: str) -> dict: ...

@mcp_tool(sensitivity="medium")  # tier 2 — includes descriptions/amounts
def search_transactions(query: str, limit: int) -> list[dict]: ...
```

- **Low-sensitivity tools** return aggregates: totals, counts, averages, category breakdowns. No individual transaction details. These work without tier-2 consent.
- **Medium-sensitivity tools** return row-level data: transaction descriptions, amounts, dates. These require `mcp-data-sharing` consent before returning full results.
- **High-sensitivity tools** (if any exist) return data that includes fields normally in the critical tier. These are exceptional and require explicit justification.

### Response filtering

When a medium-sensitivity tool is called without tier-2 consent:
- The tool returns a **degraded response**: aggregate summary instead of row-level data, plus a notice: *"Detailed transaction data requires data-sharing consent. Run `moneybin privacy grant mcp-data-sharing` to enable."*
- The tool does NOT fail — it returns what it can within the current consent level.

When tier-2 consent is granted:
- Medium-sensitivity tools return full row-level data.
- Critical-tier fields (account numbers, SSNs) remain masked in all responses regardless of consent — unless the backend is verified-local AND `LOCAL_UNMASK_CRITICAL` is enabled (see [Verified-local mode](#verified-local-mode)).

### Aggregation preference

MCP tools prefer returning the minimum data needed to answer the query:
- "How much did I spend on groceries?" → total (low sensitivity, no consent needed)
- "Show me my grocery transactions" → row-level list (medium sensitivity, consent needed)
- "What's my checking account number?" → masked `****1234` (critical fields always masked)

## Consent management

### Schema: `app.ai_consent_grants`

| Column | Type | Description |
|---|---|---|
| `grant_id` | `VARCHAR PRIMARY KEY` | Unique identifier |
| `feature_category` | `VARCHAR` | e.g., `mcp-data-sharing`, `smart-import-parsing` |
| `backend` | `VARCHAR` | Provider this consent applies to |
| `granted_at` | `TIMESTAMP` | When consent was given |
| `revoked_at` | `TIMESTAMP` | When revoked; NULL if active |
| `grant_prompt` | `TEXT` | The exact prompt text the user saw and agreed to |
| `consent_mode` | `VARCHAR` | `persistent` (tier 2) or `one-time` (tier 3) |

### CLI commands

- `moneybin privacy status` — shows active consent grants, audit log summary, configured backend
- `moneybin privacy grant <category>` — proactively grant consent (e.g., scripting/automation)
- `moneybin privacy revoke <category>` — revoke a specific consent; takes effect immediately
- `moneybin privacy revoke-all` — revoke all consents; nuclear option
- `moneybin privacy audit-log [filters]` — query the audit log (see Audit Log section)

### MCP tools

- `get_privacy_status` — returns active consents and backend info
- `revoke_ai_consent` — revoke a consent by category
- `get_ai_audit_log` — query the audit log

### Config override

For maximum-paranoia users:

```
MONEYBIN_AI__CONSENT_MODE=strict
```

In `strict` mode, ALL AI calls (tier 2 and 3) require per-invocation consent. No persistence. Every call prompts. This is the "I trust nothing" escape hatch.

## Configuration model

New `AIConfig` section in `MoneyBinSettings`, following the existing pattern (`DatabaseConfig`, `SyncConfig`, etc.):

```python
class AIConfig(BaseModel):
    """AI backend and privacy configuration."""
    model_config = ConfigDict(frozen=True)

    default_backend: str | None = Field(
        default=None,
        description="Default AI backend (anthropic, openai, ollama). None = AI features disabled.",
    )
    consent_mode: Literal["standard", "strict"] = Field(
        default="standard",
        description="standard: tier-2 consent persists. strict: all AI calls prompt every time.",
    )

class AnthropicConfig(BaseModel):
    """Anthropic (Claude) backend configuration."""
    model_config = ConfigDict(frozen=True)

    api_key: SecretStr | None = Field(default=None, description="Anthropic API key")
    model: str = Field(default="claude-sonnet-4-6", description="Model to use for AI features")

class OpenAIConfig(BaseModel):
    """OpenAI backend configuration."""
    model_config = ConfigDict(frozen=True)

    api_key: SecretStr | None = Field(default=None, description="OpenAI API key")
    model: str = Field(default="gpt-4o", description="Model to use for AI features")

class OllamaConfig(BaseModel):
    """Ollama (local LLM) backend configuration."""
    model_config = ConfigDict(frozen=True)

    base_url: str = Field(default="http://localhost:11434", description="Ollama server URL")
    model: str = Field(default="llama3", description="Model name")
```

Environment variables follow the existing `MONEYBIN_` prefix with `__` nesting:

```bash
MONEYBIN_AI__DEFAULT_BACKEND=anthropic
MONEYBIN_AI__ANTHROPIC__API_KEY=sk-ant-...
MONEYBIN_AI__CONSENT_MODE=standard
MONEYBIN_AI__OLLAMA__MODEL=llama3
```

When `default_backend` is `None` (the default), all AI-dependent features gracefully degrade: Smart Import falls back to heuristic-only; MCP tools return what they can without AI enrichment; ML categorization uses local scikit-learn only.

## Verified-local mode

When the configured AI backend is **verified-local** — meaning the `base_url` resolves to `localhost` / `127.0.0.1` / `::1` — MoneyBin operates in a mode that preserves the full "nothing leaves this machine" guarantee from the Local Only custody tier ([ADR-002](../decisions/002-privacy-tiers.md)) while still accessing AI-powered features.

### What changes in verified-local mode

| Concern | Cloud backend | Verified-local backend |
|---|---|---|
| **Tier-2 consent** | One-time persistent consent required per feature category | **Skipped** — data stays on the machine; no consent gate needed |
| **Tier-3 consent** | Per-invocation with redacted preview | **Skipped** — still logged in audit log, but no confirmation prompt |
| **Critical-field masking** | Always masked (invariant for cloud) | **Optional** — user can set `MONEYBIN_AI__LOCAL_UNMASK_CRITICAL=true` to send account numbers, SSNs, etc. to the local model unmasked. Default is still masked. |
| **Redaction engine** | Always runs at the appropriate tier | Runs unless `LOCAL_UNMASK_CRITICAL` is enabled, in which case all fields are sent as-is |
| **Audit log** | Full logging | **Still logged** — even local calls get audit entries, but marked `backend_local=true` |

### Verification

A backend is verified-local when:
1. The provider type is explicitly local (e.g., `ollama`, `llama-cpp`)
2. The configured `base_url` resolves to a loopback address (`127.0.0.1`, `::1`, `localhost`)

A backend on `192.168.x.x` or a remote hostname is NOT verified-local — it's a network service that happens to be nearby. The distinction matters: "local" means "on this machine, in this process or a process only this user controls."

### Why this matters

The privacy spec's mission is awareness and control, not restriction. When data genuinely never leaves the machine, consent gates add friction without providing value. Verified-local mode is the "complete local-only experience" for advanced users who run their own models — they get full AI features with zero external data flows, and the audit log confirms it.

## Open questions

- **MCP Apps and provider capability framing.** MCP Apps currently requires Anthropic. The provider profile documents this as a capability. How should this be presented to users — as a neutral capability difference, or with guidance? The spec should avoid appearing to recommend a provider while being honest about feature gaps.
- **Redaction for format detection.** Deferred to `smart-import-tabular.md`. The detection algorithm's sensitivity to real vs synthetic data determines whether format detection is tier 1 (masked) or tier 2 (consent needed).
- **Future: per-provider consent.** v1 ties consent to feature categories, not providers. If a user consents to `mcp-data-sharing` with Anthropic, does that consent transfer when they switch to OpenAI? Probably not — different privacy stances. Future enhancement: consent is per (category, provider) tuple.
