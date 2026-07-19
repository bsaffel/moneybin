<!-- Last reviewed: 2026-07-18 -->
# Roadmap

MoneyBin is pre-v1 and solo-maintained. The AGPL license guarantees the code
outlives the maintainer — anyone can fork, host, or continue development under
the same terms. This page shows direction, not dates; we don't commit to dates
pre-v1.

For what you can use now, see [What Works Today](features.md); the
[Changelog](../CHANGELOG.md) carries the dated record.

## Where it stands

The engine comes before the storefront: each milestone closes on a falsifiable
gate, not a calendar date. The Plaid link → sync → reconcile flow is
author-tested against a production account but still needs non-author
validation. Start with the synthetic `moneybin demo` profile to explore the
product without real financial data.

## Milestones and their gates

| Milestone | Focus | Closes when | State |
|---|---|---|---|
| **M0 — Foundation** | Secure local storage, CLI/MCP frameworks, privacy middleware | Shipped. | ✅ |
| **M1 — Ingestion Core** | Every way money gets in, landing in one trustworthy encrypted warehouse | Every import format passes an end-to-end scenario; a multi-currency round-trip reconciles to a bank statement within $0.01; investment cost basis ties to a real broker 1099-B for a full tax year; `system doctor` is clean. | 🚧 |
| **M2 — Analysis & Reports** | Every essential report, each answer traceable to source rows | Each report has a correctness scenario against known ground truth; categorization and transfer accuracy hold their thresholds; every number is explainable via lineage. | 🚧 |
| **M3 — Productization & Distribution** | Evaluation, packaging, web UI, and the opt-in hosted tier | The full suite is green; the anonymized real-data parity check passes; privacy and security checks pass. M3 close = v1. | 🚧 |

A quiet first public release precedes those gates. Its bar is narrower —
installable, not done: imports validated against real data, the Plaid
round-trip exercised beyond the author, a minimal web surface, and a PyPI
publish. Availability, not promotion.

## Current and planned increments

| Address | Outcome | Status |
|---|---|---|
| **M0** | Encryption, multi-profile storage, database coordination, CLI/MCP foundations, privacy controls, integrity checks | ✅ shipped |
| **M1A–M1F** | Tabular and financial-file imports, inbox workflow, matching, categorization, curation, Google Sheets | ✅ shipped |
| **M1G** | Plaid cash, credit-card, and investment sync | ✅ shipped; non-author validation open — [sync-plaid.md](specs/sync-plaid.md) |
| **M1I** | Native PDF import: deterministic recipe ladder, saved per-format replay, agent-bridge escalation | ✅ shipped — [smart-import-pdf.md](specs/smart-import-pdf.md) |
| **M1J** | Investment ledger, tax lots, cost basis, realized gains, Plaid investment ingestion | 🚧 ledger shipped; closes on a real-broker 1099-B tie-out; market prices and net-worth integration remain — [investments-data-model.md](specs/investments-data-model.md) |
| **M1H, M1Q** | Import confirmation and contributor extension contracts | 🚧 — [smart-import-confirmation.md](specs/smart-import-confirmation.md), [extension-contracts.md](specs/extension-contracts.md) |
| **M1K** | Multi-currency (capture shipped; conversion staged on top) | 🚧 — [multi-currency.md](specs/multi-currency.md) |
| **M1L–M1P** | Recovery completion, pipeline reconciliation, export bundle, anonymized fixtures | 🚧/📐/🗓️ per increment — see the [spec index](specs/INDEX.md) |
| **M1S–M1W** | Cross-source account, merchant, and category identity resolution; category taxonomy audit | ✅ shipped |
| **M1R, M1X** | Format-compatibility fixtures; account subtype detail and Plaid liabilities | 🗓️ planned |
| **M2A–M2B** | Curated reports plus net-worth and balance tracking | ✅ shipped |
| **M2C–M2P** | Budgets, recurring review, goals, projections, packages, richer report lineage | 📐/🗓️ per increment — see the [spec index](specs/INDEX.md) |
| **M3A** | Safe evaluator path: `moneybin demo` and first-run | 🚧 demo shipped; first-run work remains |
| **M3B** | Packaging and tester distribution (PyPI first; brew and `.mcpb` follow) | 🚧 release automation in place; no published package yet |
| **M3C** | Local web UI for review, data quality, accounts, reports | 📐 — [ui-architecture.md](specs/ui-architecture.md) |
| **M3D, M3H, M3J** | Remote MCP with auth (unlocks ChatGPT web and claude.ai connectors), opt-in hosted tier, self-host operations | 🗓️ planned |
| **M3E–M3G, M3I** | Migration guides, doc polish and landing, agent context resource, extension contributor UX | 🚧/🗓️ per increment |
| **M3K–M3M** | CLI/MCP UX standards, shared UI architecture, MCP App surface | 📐/🗓️ per increment |
| **M3N** | MCP first-run setup (elicitation-driven profile creation) | ✅ shipped |
| **M3O** | First-party directory listings (Claude Connectors Directory, ChatGPT Apps) | 📐 — [ai-client-compatibility.md](specs/ai-client-compatibility.md) |

Deliberately, the **front end is sequenced after the engine and the reports**. A narrow review console ships earlier (M3A, pulled forward) — but as a *testing and trust* surface, not a user-acquisition play. We'd rather be complete and correct than first.

### The first public release (pulled forward, deliberately small)

A quiet first public release precedes the milestone gates above. Its bar is deliberately narrower than Ingestion-Complete — it makes MoneyBin *installable* without declaring the engine *done*:

1. **Imports validated** — every shipped import format driven end-to-end against real data.
2. **Plaid round-trip** — link → sync → reconciled balances on a real account. Built and validated end-to-end against a Production Plaid account by the author; not yet exercised by a non-author user.
3. **A minimal web surface (M3A/M3C on M3L)** — the product can be seen and interacted with in a browser, not only queried. *(Originally a minimal MCP-app surface (M3M); re-pointed web-first on 2026-06-12 — a spike proved MoneyBin's MCP-App server contract works but shipping hosts don't render MCP Apps yet. See M3M below.)*
4. **Quiet distribution** — PyPI publish (that half of M3B) plus the first-run wizard and `moneybin demo` preset (from M3A). Availability, not promotion.

The full test suite, `system doctor`, and privacy/security checks must be green for the release artifacts. **Distribution is not launch**: no announcement, no landing push — marketing waits until the product proves itself in daily use.

The distribution channels split on one line: **does the channel require a vendor human-review workflow?** Self-serve channels that don't — PyPI, the `.mcpb` bundle, the Claude Code plugin, a Homebrew tap, the MCP Registry — are *tester distribution*: they may be prepared and made available to testers ahead of this release. Only first-party directory listings that do require human review (M3O) are held until the first public release is validated. (Full ladder: [`ai-client-compatibility.md`](specs/ai-client-compatibility.md).)

Investments (M1J) jumped this queue: its foundation (Pillars A+B — the investment-transaction ledger and four-method cost-basis engine) shipped 2026-07-09, ahead of the first public release. The remaining post-release wave is **investments Pillars C/D (price feeds, net-worth integration) → account subtype detail & Plaid liabilities (M1X) → multi-currency (M1K) → budgets (M2C)**. Those still close the Ingestion-Complete and Analysis-Complete gates and still gate v1 (M3H); they no longer gate first availability.

### How to read the addresses

Each milestone breaks into **increments** (`M1A`, `M1B`, …) — a coherent capability that closes on its own, roughly one spec. Where an increment is decomposed into discrete design/PR units, those are **work items** (`M1J.1`, `M1J.2`). The milestone is the gate; increments and work items are addresses within it, not separate gates.

> **Note (2026-05-30):** the milestone taxonomy was unified. Earlier versions used a flat M0–M3F grid where the numbers carried a different meaning; the four milestones above are now the scheme. `CHANGELOG.md` dated sections below the revision line use the old grid.

## Status legend

| Icon | Meaning |
|---|---|
| ✅ shipped | Merged to `main` and working end-to-end |
| 🚧 in flight | Implementation underway; partial surface available |
| 📐 designed | Spec exists; no implementation yet |
| 🗓️ planned | On the roadmap; no spec yet |

---

## M0 — Foundation ✅ shipped

The engine room — the generic substrate every later milestone builds on. No domain-specific features here.

| Address | Area | Status | Notes |
|---|---|---|---|
| **M0A** | Encryption at rest (AES-256-GCM), secret store, multi-profile isolation | ✅ | Passphrase or OS keychain. [`privacy-data-protection.md`](specs/privacy-data-protection.md). |
| **M0B** | Database engine: connection factory, migrations, writer coordination | ✅ | Short-lived per-call connections; writer coordination hardened (ADR-010 + 2026-Q2 hardening pass). |
| **M0C** | Observability: metrics + sanitized logging | ✅ | No PII or financial data in logs. |
| **M0D** | Shared primitives + `core.updated_at` convention | ✅ | The internal contract later features inherit. [`architecture.md`](architecture.md). |
| **M0E** | MCP framework: scaffold, v2 taxonomy, schema discoverability, timeouts | ✅ | `moneybin://schema`, 30s dispatch cap, response envelope. v2 naming ongoing. |
| **M0F** | CLI framework: command taxonomy | ✅ | v1 shipped; v2 taxonomy ongoing. |
| **M0G** | Testing infrastructure | ✅ | Synthetic data, e2e harness, scenario runner, five-tier assertion taxonomy. |
| **M0H** | Privacy middleware + data classification | ✅ | `DataClass` registry, redaction, consent ledger, SQL-lineage masking. AI-trust framework `ready`. |
| **M0I** | `system doctor` integrity command | ✅ | Named audits + coverage checks; the trust artifact. |

---

## M1 — Ingestion Core 🚧

Every planned way your money gets in lands cleanly — with the *ergonomics* (confidence/confirm + saved per-institution templates) — in a correct, auditable, multi-currency-aware warehouse, and a real-data parity harness proves it. Mostly built; completing now.

| Address | Area | Status | Notes |
|---|---|---|---|
| **M1A** | File import: tabular (CSV/TSV/Excel/Parquet) + OFX/QFX/QBO + watched-folder inbox + refresh pipeline | ✅ | Auto-detects Tiller/Mint/YNAB/Maybe. [`smart-import-tabular.md`](specs/smart-import-tabular.md). |
| **M1B** | Matching engine: cross-source dedup, transfer detection, golden-record, N-way collapse | ✅ | Content hashes + two-signal transfer scoring (date distance, keyword). |
| **M1C** | Categorization engine: auto-rules, bulk, cold-start, memo-aware matching | ✅ | Review queue surfaces proposed rules. |
| **M1D** | Account management: list/get/set/resolve, reversible merge | ✅ | [`account-management.md`](specs/account-management.md). |
| **M1E** | Transaction curation: notes, tags, splits, manual entry, audit log | ✅ | [`transaction-curation.md`](specs/transaction-curation.md). |
| **M1F** | Google Sheets connect (live tabular source via direct OAuth) | ✅ | Airtable/Smartsheet siblings planned. [`connect-gsheet.md`](specs/connect-gsheet.md). |
| **M1G** | Plaid sync | 🚧 | Phase 1 (cash + credit) ✅; the link → sync → reconcile round-trip is built and validated against a Production Plaid account by the author. **Plaid-Investments (M1G.4) ✅ shipped**: securities, investment transactions, and dated holdings snapshots ride the same `sync pull` job into the investment ledger via an adopt-or-mint security-identity resolver (review queue on any ambiguity) and an opening-lot bootstrap for pre-window positions; `system doctor` gains eight investment checks. Three behaviors (reinvest/corporate-action pairing, fee-inclusion convention, split-multiplier derivation) ship a conservative default pending Plaid Sandbox golden data, and end-to-end integration tests are gated on moneybin-sync implementing the contract. [`sync-plaid-investments.md`](specs/sync-plaid-investments.md). Remaining: validation by a non-author user; SimpleFIN planned. [`sync-plaid.md`](specs/sync-plaid.md). |
| **M1H** | Confirm-the-columns confidence layer | 🚧 | Confirmation & confidence contract + cross-channel `import_preview`→`import_confirm` implementation ✅ (PR #227); saved layouts reuse silently. Remaining channels wire in as M1I/M1Q land. [`smart-import-confirmation.md`](specs/smart-import-confirmation.md). |
| **M1I** | Native PDF import | ✅ | Phase 1 seed path ✅ (PR #228). Phase 2a deterministic recipe ladder + replay + transactions routing ✅ (PR #233): auto-derived recipes persist to `app.pdf_formats` keyed by layout fingerprint and replay for free on the next statement; reconciliation gate (±1¢) routes transaction-shaped PDFs to `raw.tabular_transactions`. Phase 2b ✅: a layout the deterministic rung can't crack escalates to the driving agent (`import_files`/`import_preview` return a bridge payload), and `import_confirm(bridge_response=…)` re-runs the agent's recipe, reconciles, persists, and loads (MCP-only; gated on the agent caller); a drifted saved recipe auto-`bump_version`s on replay-guard re-derive; a scanned/image-only PDF (no text layer) returns an explicit unsupported outcome. The in-process LLM/vision rung (reading scanned PDFs without a driving agent) is deferred (Out of Scope). [`smart-import-pdf.md`](specs/smart-import-pdf.md). |
| **M1J** | Investments core | 🚧 | **M1J.1 foundation (Pillars A+B) ✅ shipped**: securities catalog, investment-transaction ledger, derived lots/realized-gains/holdings, and the four-method cost-basis engine (FIFO/HIFO/specific-ID/average, Decimal precision) reconciled against a hand-labeled full-tax-year 1099-B fixture; top-level `investments` CLI/MCP group. **M1G.4 child ✅ shipped**: Plaid Investments sync feeds the same ledger automatically (see M1G above) — manual entry and Plaid sync are not yet deduped on one account (`system doctor` flags the overlap; investment dedup is a future matching child). **M1J.2 🗓️ planned**: dividend diff-detection — propose dividends missing from the ledger (held-quantity history × provider dividend data) through a visible confirm; deferred out of the Plaid Investments sync spec. Remaining: Pillar C (price feeds) and Pillar D (net-worth integration). **🔒 M1J closes only when cost basis ties to a real broker 1099-B for a full tax year** — still open; the shipped tie-out is a hand-labeled fixture, not yet a real broker statement. [`investments-data-model.md`](specs/investments-data-model.md). |
| **M1K** | Multi-currency schema wave | 📐 | Original currency canonical at every grain; conversion staged on top. Phased: capture + integrity (independent, may precede investments), display conversion (auditable Frankfurter rates, after investments), realized FX gain/loss (reuses the cost-basis engine). [`multi-currency.md`](specs/multi-currency.md). |
| **M1L** | Engine integrity & recovery completion | 🚧 | Paired audit writes + doctor coverage + undo consumer. [`app-integrity-invariant.md`](specs/app-integrity-invariant.md), [`data-recovery-contract.md`](specs/data-recovery-contract.md). |
| **M1M** | Source observations — vocabulary & canonical homes (positioning doc) | ✅ | [`source-observations.md`](specs/source-observations.md). Names where source-observed facts already live: `raw.*` + `meta.fct_transaction_provenance` for transactions; `core.fct_balances` + `app.balance_assertions` for balances; `app.match_decisions` for M:N curation. Forbids parallel `core.fct_source_observations` / `core.bridge_transaction_observations` / `app.observations`. Web UI read map. (Like M0D, this slot's deliverable is the doc itself; the underlying primitives shipped across `matching-*` and `reports-net-worth.md`.) |
| **M1N** | Data-pipeline reconciliation | 📐 | raw→prep→core accounting, orphan detection. [`data-reconciliation.md`](specs/data-reconciliation.md). |
| **M1O** | `moneybin export` bundle (CSV / Excel / Google Sheets) | 🗓️ | Local files as the canonical exit path: manifest, checksums, generated data dictionary. |
| **M1P** | Anonymizer (real data → reproducible test fixtures) | 📐 | The real-data parity enabler for the Ingestion-Complete gate. [`testing-anonymized-data.md`](specs/testing-anonymized-data.md). |
| **M1Q** | Extension framework (provider / report / package) | 🚧 | Powers customizable reports; strengthens the warehouse. **🔒 public contract locks at the M1→M2 boundary**, after the schema stabilizes. [`extension-contracts.md`](specs/extension-contracts.md). |
| **M1R** | Format-compatibility test scaffolding | 🗓️ | Curated bank-export fixtures + extractor verification; supports the Ingestion-Complete gate. |
| **M1S** | Cross-source account identity resolution | 🚧 | One real account = one canonical, opaque non-PII `account_id` across OFX/CSV/PDF/Plaid; `app.account_links` registry + resolution ladder (auto-adopt on full-number/token, review on `institution+last4`). Architecture + M1S.1–.6 ✅. **M1S.7–.9** (in progress): cross-source linking didn't fire in live testing — three capture gaps (last4 never derived into `dim_accounts`; mutable CSV labels keyed hard → duplicates on rename; exporter name used as institution → aggregator bridge dead). Fix: capture-into-`dim` + capture contract, CSV bind-first / format-account decoupling, exporter↔institution split. **Unblocks** cross-source txn dedup and the deferred account merge. [`account-identity-resolution.md`](specs/account-identity-resolution.md). |
| **M1T** | Cross-source merchant identity resolution | ✅ | The merchant twin of M1S. One real merchant = one canonical `merchant_id`; resolve by Plaid's stable `merchant_entity_id` **before** name matching. `app.merchant_links` (provider-id → merchant binding, provider-neutral, N:1) + `app.merchant_link_decisions` (fuzzy-review queue); adopt-or-mint ladder (adopt → auto-bind exact → review fuzzy → mint `created_by='plaid'`). Provider id stays in `raw`/`prep`, never `core.fct_transactions`; backfill harvests existing categorizations (conflicts-only review). Identity only — category is Tier-2b. Consumes Tier-1 (`merchant_entity_id` capture, PR #283). [`merchant-entity-resolution.md`](specs/merchant-entity-resolution.md). |
| **M1U** | Category source model + Plaid PFC categorizer | ✅ | The category twin of M1S/M1T. Splits `categorized_by` into *method* vs a new `source_type` (aggregator) on `app.transaction_categories` so provider-native categorization is a labelled source, not a laundered priority integer; keeps `categorized_by` = the *method* (merchant matches stamp `rule`; a provenance-aware demotion was built then reverted as inert and harmful to auto-rule health — provenance precision deferred) and lets a rule or merchant authored after the Plaid import override `provider_native` across runs; ships `apply_plaid_categories` — reverse-looks-up Plaid PFC codes against the **M1V category-source bridge** (two-tier detailed→primary, one canonical category/txn — deterministic), gated at ≥MEDIUM confidence, run last-before-AI in `categorize_pending` — plus provider-native metrics + a `plaid_unmapped` coverage stat. Consumer contract (one resolved row/txn) unchanged. The opt-in upgrade pass shipped as `improve-ai` (the immediate follow-up); the per-source candidates view remains deferred (with the axis-2 category-seed audit). Consumes Tier-1 (`category_detailed`/`category_confidence`, PR #283). [`categorization-source-model.md`](specs/categorization-source-model.md). |
| **M1V** | Category ↔ source mapping bridge | ✅ | Durable provider-code → canonical-category model. `seeds`/`app.category_source_map` → `core.bridge_category_source_map` (VIEW), keyed `(source_type, source_category_code)` → `category_id` — canonical-by-PK (one category per code, deterministic reverse lookup), two-tier detailed+primary via `code_level`, `source_taxonomy_version` drift marker; absorbs any aggregator with no schema change. Adds accounting `class` (income/expense/transfer/debt) to the category dim. Re-derives the seed against Plaid's verified PFC taxonomy (fixes 5 invalid tags, formalizes 16 primary-level rows; 29-code gap → axis-2). Hard-cuts `plaid_detailed` (migration `V032`). Coverage query, typed-payload `class`, and write-path metrics deferred to the Tier-2b categorizer that consumes this bridge. [`category-source-map.md`](specs/category-source-map.md). |
| **M1W** | Category taxonomy audit (axis-2 content) | ✅ | The content twin of M1V's mapping. Audited all 108 seed categories against four principles (earn-the-split granularity, class-by-accounting-nature, no redundant/orphan categories, provider-neutral) → **112 categories** (108 − 5 + 9): retired 5 duplicates/orphans (resolving the two-mortgage ambiguity to `LNP-MTG`), added 9 — 6 finer categories from the 29-code triage plus a 3-category **Family & Kids** group (`FAM`/`FAM-ACT`/`FAM-SUP`) folded in after a cross-aggregator comprehensiveness crosswalk (MX/Mint/Monarch/Maybe) validated the set — reconciled the accounting `class` (zero reclasses), hardened the seed-validation test. Purely additive on the M1V bridge (seed-content changes, no consumer rewrite). Internal 4-class scheme only; IRS/chart-of-accounts crosswalk → `us_tax` (M2M). [`category-taxonomy-audit.md`](specs/category-taxonomy-audit.md). |
| **M1X** | Account subtype detail + Plaid Liabilities | 🗓️ | Subtype-validated account fields — credit limit/payoff-date on credit cards, APR/servicer/escrow/maturity on loans & mortgages, appraisal data on property, vehicle metadata. Extends `account-management.md` (M1D) with subtype-aware schema; today's `app.account_settings` carries flat fields with no subtype validation. Table-stakes competitive gap: peers (including AI-native competitors) surface liability sub-type detail MoneyBin doesn't yet capture. Sequenced as the **next increment after M1J investments** — unblocks a Plaid Liabilities sync child spec (planned, mirrors `sync-plaid-investments.md`'s foundation/provider split off `investments-data-model.md`). Spec `account-subtype-detail.md` not yet written. |

> **Ingestion-Complete gate.** M1 closes when every import format (CSV/TSV/OFX/QFX/QBO/Excel/Parquet/PDF/gsheet/Plaid) passes an end-to-end scenario; a deliberate multi-currency round-trip reconciles to a bank-statement expectation within $0.01; investment cost basis ties to a real broker 1099-B for a full tax year; the pipeline reproduces a parity check against anonymized real data; and `system doctor` is clean.

---

## M2 — Analysis & Reports

Every essential analysis feature a serious user (and the PFM field) expects, built on the now-complete ingestion data — each answer traceable back to source rows via lineage. Partly shipped.

| Address | Area | Status | Notes |
|---|---|---|---|
| **M2A** | Reports recipe library (8 curated `reports.*` views) | ✅ | Net worth, cash flow, spending trend, recurring, uncategorized queue, merchants, large transactions, balance drift. |
| **M2B** | Net worth & balance tracking | ✅ | Daily carry-forward, reconciliation deltas. [`reports-net-worth.md`](specs/reports-net-worth.md). |
| **M2C** | Monthly budgets, target-vs-actual, **rollovers** | 📐 | Rewrite of [`budget-tracking.md`](specs/budget-tracking.md). |
| **M2D** | Recurring / subscription review workflow | 🗓️ | Accepted definitions + evidence + report impact — reviewable, with provenance. |
| **M2E** | Reimbursements / transaction links | 🗓️ | Reversible linking of refunds, reimbursements, shared expenses. |
| **M2F** | Goals | 🗓️ | Allocate balances toward named goals with progress + projected target dates. |
| **M2G** | Cash-flow projection | 🗓️ | Forward balance from scheduled/recurring items; bundles with M2D. |
| **M2H** | Anomaly detection | 🗓️ | A window compared against a trailing-N-month baseline. |
| **M2I** | "Show me the SQL" report lineage | 🗓️ | Every report exposes a stable result/lineage reference. The warehouse trust primitive. |
| **M2J** | Report subscriptions / digests | 🗓️ | Scheduled report recipes + params + lineage + optional cited prose. |
| **M2K** | Asset tracking (real estate, vehicles, valuables) | 📐 | Periodic valuations, net-worth integration. [`asset-tracking.md`](specs/asset-tracking.md). |
| **M2M** | Reference packages: `assets` + `us_tax` | 📐 | Ship at Platinum; `us_tax` builds on M1J investments. Worked examples for community packages. |
| **M2N** | LLM prose summaries | 🗓️ | Deterministic numbers; AI writes prose only from cited refs. |

> **Analysis-Complete gate.** M2 closes when each major report has a correctness scenario checked against synthetic ground truth; categorization and transfer-detection accuracy hold their thresholds; budget/recurring/reimbursement scenarios pass; and every report number is explainable through lineage.

---

## M3 — Productization & Distribution

Now that the engine and the analysis layer are complete and self-testable, make MoneyBin delightful and acquirable.

| Address | Area | Status | Notes |
|---|---|---|---|
| **M3A** | Evaluator/testing surface (**pulled forward**) | 🗓️ | `moneybin demo` preset + first-run wizard + a **narrow** Web review console (categorization/import/doctor/lineage), **built on M3L**. Ships early as a *testing/trust* surface so the M1 core is legible — but it's productization, hence M3. Demo preset + wizard are first-public-release items (see above). Demo preset shipped: [`demo-preset.md`](specs/demo-preset.md) (✅ `moneybin demo`). |
| **M3B** | Install & packaging | 🗓️ | PyPI Trusted Publishing + Homebrew formula + `.mcpb` bundle. The PyPI half is a first-public-release item; brew + `.mcpb` follow later. Packaging ladder + per-client blessed paths designed in [`ai-client-compatibility.md`](specs/ai-client-compatibility.md) (Claude Code plugin/marketplace, MCP Registry publish, install-badge deep links, Antigravity T1 install). |
| **M3C** | Full Web UI | 🗓️ | Extends the M3A console to the complete dashboard surface, backed by real domains; **built on M3L**. Same UI at `moneybin ui` (local) and the hosted tier. |
| **M3D** | Remote / HTTP MCP transport + auth | 🗓️ | Unlocks ChatGPT web (mobile MCP undocumented as of Jul 2026 — re-verify at M3D); identity via Auth0/OIDC, MoneyBin-owned authorization/consent. Auth design inputs (OAuth 2.1+PKCE floor, DCR→CIMD, Auth0-`OAuthProxy` vs WorkOS) in [`ai-client-compatibility.md`](specs/ai-client-compatibility.md). |
| **M3E** | Migration guides | 🗓️ | Mint/Tiller/YNAB/Actual/Maybe/OFX; each gated on its import path being real. |
| **M3F** | Doc polish + landing + screenshots + demo video | 🚧 | Earned positioning — after the core is real. [`user-facing-doc-polish.md`](specs/user-facing-doc-polish.md). |
| **M3G** | Generated agent financial-context resource | 🗓️ | `moneybin://context` briefing layer from warehouse state. |
| **M3H** | Hosted launch | 🗓️ | Auth + billing + per-user encrypted DuckDB + GDPR + on-call. **Deployment choice, not the headline.** M3H close = v1. |
| **M3I** | Extension contributor UX | 🗓️ | Scaffolders, validator, plugin bundle; in-tree provider Platinum sweep. |
| **M3J** | Self-host / headless operations | 🗓️ | Gated on `moneybin-sync`. Operator guides + any build specs. |
| **M3K** | CLI / MCP UX standards | 📐 | Interaction patterns, output formatting, prompt/resource conventions. **M3K.1** [`agent-visualization.md`](specs/agent-visualization.md) (draft) specifies chart-ready projections + presentation hints. **M3K.2** [`mcp-tool-surface-scaling.md`](specs/mcp-tool-surface-scaling.md) (in progress) defines one bounded, capability-complete registry of approximately 45 intent-shaped tools, a generic report runner, consumer-driven output-schema admission, and count/metadata/evaluation gates. Capable hosts may defer schemas from that same registry; generic clients receive it in full. Broader `mcp-ux-standards.md` umbrella still planned. |
| **M3L** | Shared UI architecture (foundation) | 📐 | One `ui-core` (React + shadcn/Tailwind/Tremor) behind two shells — Web UI and MCP App; transport-agnostic `MoneyBinClient`; bundle embedded in the Python wheel. Prerequisite for M3A/M3C/M3M. [`ui-architecture.md`](specs/ui-architecture.md) + [ADR-014](decisions/014-shared-ui-architecture.md). |
| **M3M** | MCP App surface | 🗓️ | MoneyBin's own dashboards rendered inside an MCP host (Claude, ChatGPT, …), built on M3L's `ui-core`. **Paused on an upstream host fix (2026-06-12):** a walking-skeleton spike verified MoneyBin's server side end-to-end (FastMCP `_meta.ui` + `ui://` serving), but shipping hosts don't render MCP Apps yet ([ext-apps #671](https://github.com/modelcontextprotocol/ext-apps/issues/671), [claude-ai-mcp #165](https://github.com/anthropics/claude-ai-mcp/issues/165)). Web shell (M3A/M3C) ships first; resumes when a host renders. Verdict: [`ui-architecture.md`](specs/ui-architecture.md) open question #1. |
| **M3N** | MCP first-run setup (**pulled forward**) | ✅ | `mcp serve` always boots with no profile; first tool call drives elicitation-based profile creation on capable clients (Claude Desktop), one structured `setup_required` envelope on tools-only clients. Fixes the interactive wizard corrupting the JSON-RPC stream. Pulled forward for the near-term distribution surface. [`mcp-first-run-setup.md`](specs/mcp-first-run-setup.md). |
| **M3O** | First-party directory listings | 📐 | Claude Connectors Directory + ChatGPT Apps SDK (distributed to users as "Plugins") — the only channels that reach ordinary consumer users. Gated on M3D (authenticated remote) + a human-review workflow, so **held until the first public release is validated** (no officially-reviewed listing before the product is tested). Self-serve channels (PyPI, `.mcpb`, Claude Code plugin, Homebrew, MCP Registry) ship earlier as tester distribution. [`ai-client-compatibility.md`](specs/ai-client-compatibility.md) (draft). |

> **Pre-Distribution gate.** M3 work proceeds once the full suite is green, the anonymized real-data parity check passes, `system doctor` is clean on a real profile, and privacy/PII/security checks pass. The first-public-release items (M3A demo/wizard + minimal web surface, M3B PyPI) — and the self-serve, no-vendor-review tester-distribution channels (`.mcpb`, Claude Code plugin, Homebrew, MCP Registry) — deliberately precede this gate; see "The first public release" above. The rest of M3, including the human-reviewed directory listings (M3O), waits. **Hosted launch (M3H) = v1.**

**A note on extensibility.** The contributor surface — adding reports, analysis packages, and providers — is a stated differentiator, deliberately narrower than a general plugin SDK. The framework is M1 engine work (M1Q); the reference packages (`assets`, `us_tax`) are M2 (M2M); the contributor-facing tooling and docs are M3 (M3I). Contract specified in [`extension-contracts.md`](specs/extension-contracts.md).

---

## Post-launch / Beyond v1

Designed or noted, but not gating launch. Listed without commitment.

- **Privacy tiers + consent model** deepening. Framework spec at [`privacy-and-ai-trust.md`](specs/privacy-and-ai-trust.md).
- **Connect: more live sources** — Airtable, Smartsheet, and Notion connectors under the same connection-lifecycle pattern as Google Sheets (M1F).
- **AI-assisted parsing of non-PDF file types** — the smart-import bridge (ships first for PDF in M1I) applied to other formats.
- **ML-powered categorization + merchant entity resolution.** Needs accumulated labeled data from real users.
- **FIRE / retirement projection** (Monte Carlo, Roth conversions, RMDs). A wealth analysis package on top of M1J — built only after the investment ledger is correct, never as a shallow dashboard.
- **Multi-account-holder sharing / household ownership.** Single-user is the v1 posture; if adopted, modeled as core ownership bridges, not app-only filters.
- **EU Open Banking / SimpleFIN** sync providers. After Plaid + one additional provider validate the sync framework.

---

## Explicitly out of scope

Solo capacity stays focused. These are not on the roadmap — many never will
be. Where one is a hard requirement, the noted alternative is the better fit
(the [audience guide](audience.md) has the full table).

- **Native mobile apps.** The planned web UI will run in a phone browser; account linking and editing stay on desktop.
- **Envelope budgeting.** Use YNAB or Actual Budget.
- **Direct broker APIs beyond Plaid.** CSV import covers the long tail.
- **Receipt scanning, per-item OCR, and email-forwarding ingestion.**
- **Tax-form generation** (Schedule D, Form 8949). The `us_tax` package ships reporting helpers, not official form output.
- **Public REST API.** Built when a real consumer requests it.
- **Windows native distribution.** macOS is the primary target; Linux runs from source.
- **Enterprise / SOC 2 path.** Consumer and indie tier; revisit on enterprise signal.
- **Crypto-heavy or DeFi-only tracking.** Use Rotki.
- **Small-business accounting with payroll.** Use QuickBooks.

## Design references

Specs live in [`docs/specs/`](specs/), indexed by [INDEX.md](specs/INDEX.md);
decision records in [decisions](decisions/). A spec can be more granular than
this page; the roadmap stays the canonical map of milestone addresses and
product direction.
