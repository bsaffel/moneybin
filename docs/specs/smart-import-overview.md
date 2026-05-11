# Smart Import — Overview

> Last updated: 2026-04-19 — promoted to ready; resolved open questions; renamed "profile" → "format" for tabular import column mappings
> Status: Ready — umbrella doc for the smart-import initiative. Child specs listed in [Pillars](#pillars) are written separately.
> Companions: [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) (AI data flow governance), [`matching-overview.md`](matching-overview.md) (peer initiative), [`categorization-overview.md`](categorization-overview.md) (owns pillars D & E), `CLAUDE.md` "Architecture: Data Layers"

## Purpose

Smart Import is MoneyBin's headline import experience. This doc is the umbrella: it fixes the vision, the scope boundary, the default privacy posture, and the build order. Design and implementation details live in the child specs it points to.

## Vision

> **Drop in any file, we figure it out, the tool learns — and if you want, your data stays on your machine.**

Three commitments, in order:

1. **Coverage.** The user hands over a file — CSV, TSV, Excel, native-text PDF — and MoneyBin imports it cleanly. When built-in profiles don't match, heuristics infer the mapping. When heuristics fall short, an opt-in AI path extracts what's present.
2. **Adaptation.** Every import makes the next one faster. Detected formats are reused. Categorizations seed auto-rules. Historical edits train a local ML categorizer. The tenth import requires less effort than the first.
3. **Optional local-only trust.** Every local path (heuristic detection, ML categorization, auto-rules) runs entirely on the user's machine. The one path that may send data elsewhere — AI-assisted parsing — is consent-gated per file and fully auditable. A user who never opts in never has data leave their machine.

## Target users

Smart Import touches all four MoneyBin user personas:

- **Trackers** (broadest appeal) — the first-import experience determines whether they adopt at all.
- **Power users** — the CLI flags, structured output, and local-LLM options that let them script around it.
- **Budgeters** — recurring monthly imports with minimal friction and good auto-categorization.
- **Wealth managers** — brokerage statements (PDF) and positions exports (Excel) fall into this surface.

The feature's biggest lift is for Trackers (time-to-first-import) and Budgeters (recurring import ergonomics).

## Pillars

Smart Import decomposes into six independent subsystems. Each has its own child spec; this doc fixes the shared vocabulary, sequencing, and privacy boundary.

| Pillar | Purpose | Touches cloud? | Child spec |
|---|---|---|---|
| **A+B.** Smart tabular import | Universal tabular importer: CSV, TSV, Excel, Parquet, Feather with heuristic column detection, multi-account support, and migration formats (Tiller, Mint, YNAB). Pillars A and B merged — Excel is just another file type reader feeding the same detection engine. JSON/JSONL deferred to a separate spec (nested types → DuckDB native STRUCT/LIST/MAP). | No | [`smart-import-tabular.md`](smart-import-tabular.md) |
| **C.** Structured PDF import | Native-text PDFs via `pdfplumber`/`camelot`. Extends the `w2_extractor` pattern to statements and brokerage reports. | No | `smart-import-pdf.md` |
| **D.** ML-powered categorization | Local scikit-learn (TF-IDF + SVM) trained on the user's own `transaction_categories`. High-confidence → auto-apply; medium → suggest; low → defer. | No | Owned by [`categorization-overview.md`](categorization-overview.md) |
| **E.** Auto-rule generation | Hook `categorize_transaction()` / `categorize_items()` to synthesize rules and merchant mappings from user edits and high-confidence ML picks. | No | Owned by [`categorization-overview.md`](categorization-overview.md) |
| **F.** AI-assisted parsing | LLM fallback for files A/B/C can't crack. Extracts structured data from document content. | **Yes — consent-gated** | `smart-import-ai-parsing.md`, gated by `docs/specs/privacy-and-ai-trust.md` |

All six pillars share one architectural property: they operate at or above the extractor layer. The `raw` / `prep` / `core` pipeline is unchanged. Every pillar's output is normalized to the canonical raw schema before anything hits DuckDB.

### Terminology note

Throughout this initiative, **"format"** refers to a saved column mapping + metadata for a specific institution's export layout (e.g., "Chase format", "Tiller format"). This is distinct from **"file type"** or **`source_type`** which refers to the file container (CSV, TSV, Excel, Parquet, etc.). Two files can have the same file type (both CSV) but different formats (Chase vs Citi column layouts).

## In scope

- CSV / TSV exports from any institution
- Excel (XLSX, XLS) with native tables, multi-sheet, non-row-1 headers
- Native-text PDFs (tabular: bank statements, brokerage statements, 1099s, etc.)
- Batch folder import — point at a directory, handle mixed CSV / XLSX / PDF files
- AI-assisted parsing as an opt-in fallback for all of the above

## Out of scope

Explicitly deferred until after v1. Revisit per pillar as the initiative matures.

- **Scanned PDFs and image-only PDFs** — requires OCR + vision model; different trust and accuracy profile from text parsing
- **Receipt photos** — different document shape (one transaction per doc, not a list); belongs with a separate receipt-capture feature
- **Email inbox scraping** — separate integration surface; belongs with Plaid / sync, not smart import
- **Non-financial documents** — generic invoices, utility bills; widens product beyond personal finance
- **Foreign-language statements** — i18n for dates, amounts, categories is its own effort; the multi-currency initiative handles the money side, document-language support is a later concern
- **Password-protected files** — user unlocks before import

Two semantic non-goals worth stating explicitly:

1. **Smart Import is file-based, not API-based.** Plaid and other live connections live in `sync-overview.md`. There is no overlap — Smart Import is what happens when the user has a file.
2. **Smart Import does not invent data.** If a column isn't in the source, the AI doesn't hallucinate it. Pillar F is bounded to *extracting* what's present, never *inferring* what's absent. This constraint is load-bearing for the privacy spec — it shapes what prompts are allowed.

## Adjacent initiatives

Two concerns touch Smart Import but are scoped to separate peer specs. Calling them out here so downstream child specs can assume the contract exists.

### Transaction matching — `matching-overview.md`

**Scope:** Cross-source deduplication of the same transaction (e.g., the same txn appears in a CSV statement, an OFX export, and Plaid), transfer pair detection (money out of account A matches money into account B), and golden-record merge rules when multiple sources describe the same transaction with slightly different fields.

**Why it's a peer, not a child:** Matching is shared infrastructure consumed by every ingestion surface — Smart Import, Plaid sync (`sync-overview.md`), and manual entry all feed it. Design choices in the matching spec constrain what each ingestion surface must produce (provenance columns, candidate keys, fuzzy-match signals). It also owns the `source_type` taxonomy.

**Contract with Smart Import:** Every pillar in this initiative must produce raw records that conform to whatever provenance schema the matching spec defines. Pillars A/B/C/F cannot finalize their raw-row output shape until the matching spec lands.

**Children (tentative):** `matching-same-record-dedup.md`, `matching-transfer-detection.md`, `mastered-record-merge-rules.md`.

### Privacy & AI trust — `docs/specs/privacy-and-ai-trust.md`

Already referenced throughout this doc. Constrains pillar F. Foundational alongside `matching-overview.md`.

## Default privacy posture

**Consent-gated cloud, per-file prompt with redacted preview.**

Every local path (A, B, C, D, E) has zero external traffic. On a fresh install with no AI backend configured, Smart Import can still import files the heuristics can handle and never makes a network call.

When Smart Import invokes AI-assisted parsing (pillar F), the user sees — before any data leaves the machine — a confirmation showing:

- What will be sent (a redacted preview of the file — account identifiers, exact amounts, and PII masked per `docs/specs/privacy-and-ai-trust.md`)
- Where it will be sent (backend name, e.g. `claude`, `openai`, `ollama://localhost`)
- What will be received (the expected shape of the response: column mapping, not raw transactions)

The user confirms, declines, or switches backend per file. No persistent "trust this source forever" mode in v1 — the consent spec may introduce one later with explicit revocation.

Detailed rules — redaction fields, supported backends, audit log schema, consent revocation, local-LLM support — live in `docs/specs/privacy-and-ai-trust.md`. That spec is written *before* pillar F is built.

## Build order & rationale

1. **`docs/specs/privacy-and-ai-trust.md`** — foundational. Defines the consent model and audit schema that pillar F must conform to. Worth writing even though F is built last: locks in the privacy contract before any AI-touching code exists.
2. **`matching-overview.md`** (umbrella + at least `matching-same-record-dedup.md` child) — foundational peer spec. Defines the provenance contract every ingestion surface must produce and owns the `source_type` taxonomy. Pillars A/B/C/F can't finalize their raw-row output until this lands.
3. **Pillars A+B — [`smart-import-tabular.md`](smart-import-tabular.md)** — merged into a universal tabular importer. CSV, TSV, Excel, Parquet, Feather all handled by one detection engine. Establishes the file-type-agnostic architecture, `TabularFormat` system, `ingest_dataframe()` Database primitive, and multi-account support. Zero privacy risk. Proves the architecture for everything downstream. JSON/JSONL deferred to a separate spec — nested types map better to DuckDB's native STRUCT/LIST/MAP than to tabular flattening.
4. **Pillars E & D** — now owned by [`categorization-overview.md`](categorization-overview.md). Build order: E (auto-rules) first, then D (ML). See that spec for rationale and sequencing. Migration-imported categories from the tabular importer serve as a bootstrap accelerator for both pillars.
5. **Pillar C — `smart-import-pdf.md`** — independent; follows the `w2_extractor` pattern.
6. **Pillar F — `smart-import-ai-parsing.md`** — last. Depends on the privacy framework and benefits from A+B/C being the trusted non-AI fallback when the user declines AI.

## Success criteria

Outcome-oriented, not metric-oriented. Per-pillar metrics live in child specs.

- **Time-to-first-import.** A first-time user gets their first file imported in under two minutes, from install to data-visible.
- **Mastery curve.** A user's tenth import is meaningfully less effortful than their first. Evidence comes from auto-rule hit rates, reused detected formats, and ML categorization coverage growing over time.
- **Trust preservation.** A user who never opts into AI parsing never has data leave their machine. Verifiable from the audit log — a grep for outbound calls should return zero rows.
- **Graceful degradation.** When detection fails, the user gets a clear path forward: manual mapping, skip the file, or escalate to AI. Never a dead end.
- **No silent failures.** Every import produces a visible outcome — success, partial (with flagged rows), or declined. A file is never "imported but wrong."

## Resolved questions

Previously open; resolved in child or peer specs.

- **Format persistence UX.** Auto-save by default; `--save-format` / `--no-save-format` to control. Resolved in `smart-import-tabular.md` requirement 10.
- **Format precedence.** User DB formats override built-ins of the same name; built-in YAML is fallback only. Resolved in `smart-import-tabular.md` Stage 3, Step 1.
- **MCP Apps wizard timing.** v1 ships basic MCP tools (`import_file`, `import_confirm`, `import_preview`, `list_formats`) via the shared service layer. The interactive wizard MCP App is Phase 2. Resolved in `smart-import-tabular.md` MCP Interface section.
- **Batch folder UX.** Batch folder import deferred to post-v1 (listed under Future Enhancements in `smart-import-tabular.md`). AI consent model for batch will be resolved when batch is designed.
- **Match-review hooks in import flow.** Import summary output includes match results ("3 auto-merged, 5 pending review. Run `moneybin matches review` when ready."). Review is a separate command, not inline. Resolved in `matching-overview.md` Default run model section.
