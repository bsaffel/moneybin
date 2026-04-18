# Spec Index

Single source of truth for spec status. Update this table when a spec's status changes.

## Status definitions

| Status | Meaning |
|---|---|
| `draft` | Design written, not yet reviewed or ready for implementation |
| `ready` | Design reviewed and approved; ready for implementation |
| `in-progress` | Implementation underway |
| `implemented` | Shipped; spec moved to `implemented/` |

## Updating implemented features

Implemented specs are historical records of what was designed and shipped. They don't get edited after the fact.

| Change size | Action |
|---|---|
| **Bug fix / minor tweak** | No spec needed. Code change speaks for itself. |
| **Enhancement to existing feature** | New spec referencing the original as context. Original stays in `implemented/` untouched. |
| **Full redesign / replacement** | New spec. Original gets a note at the top pointing to the replacement. |

## Active specs

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Smart Import Overview](smart-import-overview.md) | Umbrella | draft | Six-pillar initiative: smart CSV/TSV detection, Excel, PDF, ML categorization, auto-rules, AI-assisted parsing |
| [Transaction Matching](transaction-matching.md) | Umbrella | draft | Cross-source dedup, transfer detection, golden-record merge rules; core as gold analytics layer |
| [Categorization](categorization-overview.md) | Umbrella | draft | Auto-rule generation, ML categorization, priority hierarchy, bootstrap strategies |
| [Privacy & AI Trust](privacy-and-ai-trust.md) | Framework | draft | AI data flow tiers, consent model, provider profiles, redaction engine, audit log |
| [CSV Import](csv-import.md) | Feature | ready | Profile-based CSV extraction and loading (existing system) |
| [MCP Architecture & Design](mcp-architecture.md) | Architecture | draft | MCP v1 design philosophy, tool taxonomy, privacy integration, CLI symmetry, Apps readiness |
| [MCP Tool Surface](mcp-tool-surface.md) | Architecture | draft | Concrete tool, prompt, resource, and service layer definitions for MCP v1 (45 tools, 4 prompts, 4 resources) |
| [Budget Tracking](budget-tracking.md) | Feature | draft | Monthly budgets with target-vs-actual and rollovers |
| [Data Protection](data-protection.md) | Feature | draft | DuckDB encryption at rest (AES-256-GCM), `Database` connection factory, key management, file permissions, PII log sanitization |
| [Database Migration](database-migration.md) | Feature | ready | Dual-path schema migration system: auto-upgrade on first invocation, SQL/Python migrations, rebaseline, SQLMesh version detection |
| [Plaid Integration](plaid-integration.md) | Feature | draft | Bank sync via Plaid through the Encrypted Sync tier |
| [Sync Client Integration](sync-client-integration.md) | Feature | draft | Client-side sync service integration (auth, data flow, CLI) |
| [Same-Record Dedup](same-record-dedup.md) | Feature | draft | Cross-source dedup + golden-record merge rules (transaction-matching pillars A+C) |

## Roadmaps (not feature specs)

| Doc | Summary |
|---|---|
| [Privacy & Security Roadmap](privacy-security-roadmap.md) | Three-tier data custody model overview |
| [Distribution Roadmap](distribution-roadmap.md) | Packaging and distribution strategy |

## Implemented

Specs in `implemented/` are complete. Listed here for reference.

| Spec | Summary |
|---|---|
| [OFX Import](implemented/ofx-import.md) | OFX/QFX file extraction and loading |
| [CSV Import (original)](implemented/ofx-import.md) | Initial CSV import implementation |
| [MCP Read Tools](implemented/mcp-read-tools.md) | 8 read-only MCP tools + 5 resources + 5 prompts |
| [MCP Write Tools](implemented/mcp-write-tools.md) | 9 write MCP tools (import, categorization, budgets) |
| [Transaction Categorization](implemented/transaction-categorization.md) | Rule-based + LLM categorization with merchant normalization |
| [W-2 Extraction](implemented/w2-extraction.md) | PDF W-2 form extraction and loading |

## Planned child specs (not yet written)

These are referenced by umbrella specs but don't exist yet. Create from `_template.md` when ready.

### MCP children
- MCP Apps spec (name TBD) — First MCP App MVP, consuming the tool surface

### Privacy infrastructure children
- Redaction engine spec — Field-level masking rules, deterministic redaction, reverse-lookup
- Consent management spec — `app.ai_consent_grants` schema, grant/revoke lifecycle
- Audit log spec — `app.ai_audit_log` schema, logging contract, query API
- Provider profiles spec — `AIBackend` interface, provider metadata, verified-local detection
- AIConfig spec — `MoneyBinSettings.ai` configuration block, backend selection

### Smart Import children
- `smart-csv-detection.md` — Pillar A: heuristic column inference for unknown CSVs
- `excel-import.md` — Pillar B: XLSX/XLS import
- `structured-pdf-import.md` — Pillar C: native-text PDF import
- `ai-assisted-parsing.md` — Pillar F: LLM fallback for file parsing

### Categorization children
- `auto-rule-generation.md` — Pillar E: auto-generate rules from user edits (absorbed from Smart Import)
- `ml-categorization.md` — Pillar D: local ML-powered categorization (absorbed from Smart Import)

### Transaction Matching children
- `same-record-dedup.md` — Pillars A + C: cross-source deduplication and per-field merge policy for golden records
- `transfer-detection.md` — Pillar B: transfer pair detection across accounts
