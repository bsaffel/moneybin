<!-- Last reviewed: 2026-05-24 -->
# Roadmap

Pre-v1 roadmap. Each milestone is a coherent slice of work, not a calendar date — we don't commit dates pre-v1. Statuses below reflect what's merged to `main` today; the dated record of individual changes lives in [`CHANGELOG.md`](../CHANGELOG.md). For "is this for me?" see [`audience.md`](audience.md); for head-to-head fit, [`comparison.md`](comparison.md); for shipped capability detail, [`features.md`](features.md).

**Maturity signal.** MoneyBin is pre-v1 and pre-launch. File-based usage (CSV/OFX/QFX/QBO import, categorization, net-worth, MCP query) has shipped through M2A and is in daily use by the author. Plaid sync (M3A) has shipped its first phase but has not yet been used against a Production-approved Plaid account by anyone other than the author.

**Your data, your file.** MoneyBin stores everything in a local encrypted DuckDB file on your machine. Today's exit path is direct SQL access to that file — the data is yours. A one-command `moneybin export` (CSV / Excel / Google Sheets) is on the post-launch list, not shipped.

## Status legend

| Icon | Meaning |
|---|---|
| ✅ shipped | Merged to `main` and working end-to-end |
| 🚧 in flight | Implementation underway; partial surface available |
| 📐 designed | Spec exists; no implementation yet |
| 🗓️ planned | On the roadmap; no spec yet |

## Milestone overview

Each milestone has a one-line gloss for what it means to a user. Details below.

| Code | Name — what it means | Status |
|---|---|---|
| **M0** | Infrastructure — the engine and safety rails. No user-facing features. | ✅ shipped |
| **M1** | Data Integrity — import works, dedup works, the numbers can be trusted. | ✅ shipped |
| **M2A** | Curator State — notes, tags, splits, manual entry; you can correct what was imported. | ✅ shipped |
| **M2B** | Architecture Reference — internal contract every later feature inherits. User impact: stability. | ✅ shipped |
| **M2C** | Install & Onboarding — `brew install moneybin` works; an evaluator gets to a first answer unaided. | 🚧 in flight |
| **M2D** | Recovery & Trust — every data failure is fixable through tools (never SQL surgery), and every edit is reversible. | 🚧 in flight |
| **M2E** | Smart Import & Connect — Google Sheets live sync, drop-any-PDF import, and one shared confirm step across every channel. | 🚧 in flight (gsheet shipped) |
| **M3A** | Plaid sync — connect your bank by login instead of CSV download. | 🚧 in flight (Phase 1) |
| **M3B** | Investments / cost basis — holdings, lots, gain/loss tied to a real 1099-B. | 🗓️ planned |
| **M3C** | Multi-currency + budgets — non-USD support and monthly budgets with rollovers. | 🗓️ planned |
| **M3D** | Web UI — the dashboard you'll actually look at, plus remote MCP for ChatGPT web/mobile. | 🗓️ planned |
| **M3E** | Hosted launch (v1) — opt-in cloud tier; reference packages (`assets`, `us_tax`) ship Platinum-quality. | 🗓️ planned |
| Post-launch | Anything after M3E. Listed without commitment. | 🗓️ planned |

**A note on extensibility.** The contributor surface — adding reports, analysis packages, and providers — is a stated differentiator, built on the premise that people (and the agents they drive) will extend MoneyBin to track money their own way. The contract is specified in [`extension-contracts.md`](specs/extension-contracts.md). The package framework core has shipped, and report auto-generation — a single `@report` runner that generates the CLI command, MCP tool, and column masking — is in flight (🚧), so packages and agents add reports onto both surfaces through one definition. Two reference packages ship at v1: `assets` (real estate, vehicles, valuables) and `us_tax` (locale-specific tax helpers built on top of M3B investments). Both at Platinum quality; both serve as worked examples for community packages post-launch.

M3E closing = v1 launch. **Phase 1 of M3A** = cash and credit-card accounts via Plaid. Phase 2 = investment accounts, which overlaps with M3B.

---

## Exit criteria — what "closes" means

The single best place to read this roadmap: every in-flight or planned milestone closes against a concrete, falsifiable bar.

| Milestone | Closes when… |
|---|---|
| M2C | `brew install moneybin && moneybin demo` works on a clean Mac with a clean `moneybin system doctor` output; the Web UI prototype runs at `moneybin ui` and lets a user review AI-categorization proposals end-to-end; the README quickstart + Claude Desktop guide carry a new user from zero to a first MCP answer. |
| M2D | Every documented failure mode returns structured `recovery_actions`; the doctor recipe registry and the refresh-time self-heal safelist are in place; no recovery path requires raw SQL. |
| M2E | Google Sheets sync, drop-any-PDF import, and the shared import-confirm flow all ship; nothing lands unconfirmed on first contact across tabular, Sheets, and PDF, and a confirmed layout replays silently. |
| M3A | Plaid Production is approved and a first user syncs from a real bank. |
| M3B | Investment cost-basis numbers tie to at least one broker's official 1099-B for a full tax year. |
| M3C | A non-USD user can import multi-currency transactions, see home-currency equivalents, and FX gain/loss on a deliberate round-trip ties to a bank-statement-derived expectation within $0.01. |
| M3D | The full UI surface ships at `moneybin ui` (local) and the hosted tier from the same codebase; ChatGPT web connects via the HTTP-based MCP transport. |
| M3E | Hosted ops + billing + GDPR + on-call all close; a beta user signs up, links a bank, asks Claude a question, and downloads their full encrypted DuckDB. **Launch.** |

---

## M0 — Infrastructure ✅ shipped

The foundation every later milestone builds on. No user-facing features land here — this is the engine room.

| Area | Status | Notes |
|---|---|---|
| Encryption at rest (AES-256-GCM) | ✅ | Passphrase-derived key or OS keychain. See [`privacy-data-protection.md`](specs/privacy-data-protection.md). |
| Database connection factory + secret store | ✅ | Short-lived per-call connections. |
| Schema migration system | ✅ | Auto-upgrade; self-heals on body change. |
| Multi-profile isolation | ✅ | `~/.moneybin/profiles/{name}/`. |
| Observability (metrics + sanitized logging) | ✅ | No PII or financial data in logs. |
| Synthetic data generator | ✅ | Three personas, ~200 merchants, ground-truth labels. |
| MCP server scaffolding | ✅ | Response envelope, tool decorator, namespace registry. |
| End-to-end test infrastructure | ✅ | Subprocess-based; smoke + golden-path. |

---

## M1 — Data Integrity ✅ shipped

Makes the numbers trustworthy. Spending totals match what you'd compute from bank statements.

| Area | Status | Notes |
|---|---|---|
| Smart tabular importer | ✅ | CSV/TSV/Excel/Parquet/Feather; auto-detects Tiller, Mint, YNAB, Maybe profiles. See [`smart-import-tabular.md`](specs/smart-import-tabular.md). |
| OFX/QFX/QBO import | ✅ | Same import infrastructure as tabular. |
| Watched-folder inbox | ✅ | Drop a statement in `~/Documents/MoneyBin/<profile>/inbox/` and it imports. |
| Cross-source dedup | ✅ | Content hashes + golden-record merge; N-way collapse merges 3+ copies across sources and overlapping within-source files into one record. |
| Transfer detection | ✅ | Two-signal scoring (date distance, keyword) across accounts. |
| Auto-rule learning from edits | ✅ | Review queue surfaces proposed rules before they apply broadly. |
| Account management | ✅ | List, get, set, resolve; reversible merging. |
| Net-worth + balance tracking | ✅ | Reconciliation deltas surface drift. |
| MCP tool surface (v2 taxonomy) | ✅ | Path-prefix-verb-suffix naming; install across nine clients. |
| 10-scenario integration test suite | ✅ | Five-tier assertion taxonomy. |

---

## M2A — Curator State ✅ shipped

Transaction-level user state — notes, tags, splits, manual entry, audit history — without touching imported data. Imports stay clean; your edits live in a separate user-state layer.

| Area | Status | Notes |
|---|---|---|
| Manual transaction entry (CLI + MCP) | ✅ | |
| Free-text notes (multi-note thread per transaction) | ✅ | |
| Multi-tag table | ✅ | |
| Edit-history audit log | ✅ | |
| Import-batch labels | ✅ | |
| Split-via-annotation | ✅ | First-class splits parked; interim form ships. |

Spec: [`transaction-curation.md`](specs/transaction-curation.md). Trust is surfaced through `moneybin system doctor` running continuous invariant checks, not through per-row "verified" flags.

---

## M2B — Architecture Reference ✅ shipped

Internal milestone. Codifies the shared primitives that crystallized through M0–M1 so M3 features inherit a single contract instead of re-deriving one. User impact: durability — surfaces shipped from M3 forward look the same from outside even as the engine evolves.

| Area | Status | Notes |
|---|---|---|
| Shared-primitives reference doc | ✅ | See [`architecture.md`](architecture.md) for the one-page user-facing distillation. |
| Writer-coordination contract | ✅ | Short-lived per-call connections; read-only sessions coexist across processes. |
| Per-row `updated_at` convention | ✅ | Model freshness surfaced through the doctor command. |

---

## M2C — Install & Onboarding 🚧 in flight

The installable beachhead. `brew install moneybin` works end-to-end on a clean Mac, and an evaluator landing on the repo gets to a useful first answer without external hand-holding.

| Area | Status | Notes |
|---|---|---|
| `moneybin system doctor` integrity command | ✅ | Pipeline audits + staging coverage + categorization coverage. Surfaces "✅ N invariants passing across M transactions." |
| Reports recipe library | ✅ | Eight curated views: net worth, cash flow, spending trend, recurring subscriptions, uncategorized queue, merchant activity, large transactions, balance drift. |
| Report auto-generation framework | 🚧 | One `@report` runner generates the CLI command, MCP tool, parameter flags, and column masking; analysis packages and agents contribute reports the same way. First concrete slice of the extension contract. |
| Agent ingest toolset (MCP `transform_*` + batch import) | ✅ | Closes the agent ingest loop. |
| `refresh` umbrella across MCP + CLI | ✅ | One command: matching → pipeline apply → categorization. |
| Public doc surface (README, audience, comparison, features, licensing) | ✅ | |
| Categorization correctness pass | ✅ | Memo-aware matching, exemplar accumulation, auto fan-out. |
| `moneybin demo` instant synthetic-data preset | 🗓️ | |
| First-run wizard | 🗓️ | |
| PyPI publish workflow + Homebrew formula | 🗓️ | `pyproject.toml` metadata ready; publish pipeline pending. |
| README quickstart + Claude Desktop setup guide | 🗓️ | |
| Web UI prototype (narrow scope) | 🗓️ | AI-categorization-proposal review queue + transactions list. First iteration of the M3D UI. |
| User-state integrity invariant | 📐 | Paired audit writes + doctor coverage. Gates the hosted tier (M3D/M3E). See [`app-integrity-invariant.md`](specs/app-integrity-invariant.md). |

---

## M2D — Recovery & Trust 🚧 in flight

Every data failure is fixable through tools, never SQL surgery — and every edit is reversible. The contract: the system *recomputes* but never *decides*; if it would have to choose between two reasonable outcomes, it surfaces the choice with structured `recovery_actions` instead of guessing. Spec: [`data-recovery-contract.md`](specs/data-recovery-contract.md).

| Area | Status | Notes |
|---|---|---|
| Structured `recovery_actions` on every error + audit failure | ✅ | Agents read the named recovery tool calls straight off any failure. |
| `operation_id` grouping on `app.audit_log` | ✅ | A tool call's mutations form one undoable unit. |
| Audit-log undo consumer | ✅ | `system_audit_undo` / `_history` / `_get` reverse any audited `app.*` mutation from its before/after image; the undo is itself undoable. Block-don't-cascade when later edits touched the same rows. |
| Reversible `app.*` mutations (repository routing) | ✅ | Every protected write pairs with an audit row in the same transaction (Invariant 10). |
| Error-code taxonomy (prefix-grouped) | ✅ | `infra_*`, `import_*`, `mutation_*`, `audit_*`, `refresh_*`, `undo_*`, `recovery_*`, `sync_*`. |
| Matches domain on MCP | ✅ | `transactions_matches_run` / `_pending` / `_set` / `_history` — dedup/transfer review reachable by agents, not just the CLI. |
| Refresh crash surfacing | ✅ | `RefreshResult` reports matcher/categorizer crashes instead of swallowing them at DEBUG. |
| AI consent ledger | ✅ | Grant / revoke / status / log which AI feature categories are authorized for which backend. |
| SQL-lineage column masking | ✅ | `sql_query` masks account/routing numbers via sqlglot lineage — raw SQL is not a privacy bypass. |
| Doctor recipe registry | 🚧 | Each invariant audit ships a recipe producing `recovery_actions` from the failure's affected IDs. |
| Refresh-time self-heal safelist | 📐 | Five reversible, information-loss-free recipes; strict criteria gate any addition. |
| `data-recovery.md` project rule | 📐 | Codifies the contract for future tools, audits, and refresh stages. |

---

## M2E — Smart Import & Connect 🚧 in flight (gsheet shipped)

Expanding what MoneyBin can ingest, pre-launch. Two threads: **Connect** — user-controlled live data sources via direct OAuth, distinct from `sync` (which mediates third-party financial providers through moneybin-server); the client speaks the provider's API directly and tokens live in the local `SecretStore` — and **Smart Import** — turning more file types into trustworthy rows. One shared confirmation contract governs every channel.

| Area | Status | Notes |
|---|---|---|
| Google Sheets connector | ✅ | Two adapters: `transactions` (Tiller-style → matching/categorization pipeline) and `seed` (catch-all → JSON storage + auto-generated typed views). OAuth via Google's "Desktop app" PKCE flow; no shared client secret. Soft-delete preserves audit history; per-connection drift detection refuses pulls on structural change until `gsheet reconnect`. See [`connect-gsheet.md`](specs/connect-gsheet.md). |
| Drop-any-PDF import | 🚧 | Generic PDF ingestion: native-text statements extract locally and free via `pdfplumber`; harder layouts escalate to the AI agent already driving MoneyBin; a learned recipe replays for free thereafter. Transaction-shaped rows route to `core`; everything else lands as queryable JSON seeds. Spec ready ([`smart-import-pdf.md`](specs/smart-import-pdf.md)); first phase underway. |
| Import confirmation & confidence | 🚧 | One trust step shared by every smart-import channel (tabular, Sheets, PDF): nothing lands unconfirmed on first contact, a confirmed layout replays silently, and a wrong guess is one obvious step to recover (`import_confirm`). Draft spec. |

Independent of M3A — no moneybin-server dependency. The Connect family grows post-launch with Airtable, Smartsheet, and Notion siblings under the same connection-lifecycle pattern; AI-assisted parsing of *non-PDF* file types also stays post-launch.

---

## M3A — Plaid sync 🚧 in flight (Phase 1 shipped)

Bank-login connection in place of monthly CSV exports. **Phase 1** (shipped) = cash and credit-card accounts. **Phase 2** = investment accounts, which overlaps with M3B.

| Area | Status | Notes |
|---|---|---|
| `moneybin sync` CLI + sync MCP tools | ✅ | Pull, status, connect, disconnect. |
| Plaid Hosted Link flow | ✅ | Long-running sync uses a job-handle pattern to fit the MCP timeout cap. |
| Raw → staging → canonical transactions/accounts | ✅ | Sign convention applied in staging. |
| Provider framework | ✅ | See [`sync-overview.md`](specs/sync-overview.md). |
| Plaid Production approval | 🗓️ | Multi-week paperwork window; starts when the investment spec lands. |
| Real-user Plaid round-trip | 🗓️ | M3A closes when a first user syncs from a real bank. |

---

## M3B — Investments / cost basis 🗓️ planned

The largest competitive moat. M3B does not ship until cost-basis output ties to a real 1099-B end-to-end.

| Area | Status | Notes |
|---|---|---|
| Securities, ledger, lots, realized gain/loss, holdings (cost basis) | 📐 | [`investments-data-model.md`](specs/investments-data-model.md) — foundation child (Pillars A+B) |
| Short-term / long-term classification | 📐 | Foundation child; oldest-first per-lot under every method |
| Unrealized gain/loss + Yahoo/CoinGecko price feeds | 🗓️ | Pillar C — `investments-price-feeds.md` (planned) |
| Holdings in net worth | 🗓️ | Pillar D — `investments-net-worth.md` (planned) |
| Plaid Investments product wiring | 🗓️ | Gated on the foundation contracts. |

---

## M3C — Multi-currency + budgets 🗓️ planned

Closes the last traditional-budgeting gap and adds non-USD support.

| Area | Status | Notes |
|---|---|---|
| Multi-currency transaction support + FX rates | 🗓️ | Realized FX gain/loss on conversions. |
| Monthly budgets, target-vs-actual, rollovers | 📐 | Spec [`budget-tracking.md`](specs/budget-tracking.md) is `draft`. |

---

## M3D — Web UI + remote MCP 🗓️ planned

Extends the M2C Web UI prototype to the full dashboard surface and ships the HTTP-based MCP transport that unlocks ChatGPT web/mobile and other remote clients.

| Area | Status | Notes |
|---|---|---|
| Full Web UI (dashboards, accounts, balances, investments, multi-currency) | 🗓️ | Same UI at `moneybin ui` (local) and the hosted tier. |
| HTTP-based MCP transport | 🗓️ | Unlocks ChatGPT web + mobile. |
| "Show me the SQL" report-lineage tool | 🗓️ | |
| Mobile read-only viewer | 🗓️ | **Web-based, not a native app.** Read-only; account linking and editing stay on desktop. |

---

## M3E — Hosted launch (v1) 🗓️ planned

The opt-in cloud tier. Local-first remains the default. M3E closing = v1 launch.

| Area | Status | Notes |
|---|---|---|
| Auth + billing + per-user encrypted DuckDB | 🗓️ | |
| Zero-knowledge passphrase + recovery codes | 🗓️ | |
| GDPR data export / delete | 🗓️ | Beta user downloads their full encrypted DuckDB. |
| On-call ready | 🗓️ | |
| Extension contract (Reports / Analysis Packages / Providers) | 📐 | [`extension-contracts.md`](specs/extension-contracts.md) — contributor-facing surface with Quality Scale (Bronze → Platinum). |
| Reference package: `assets` (real estate, vehicles, valuables) | 📐 | First reference package; ships at Platinum. Demonstrates the package contract. |
| Reference package: `us_tax` (locale-specific tax helpers) | 📐 | Second reference package; ships at Platinum. Depends on M3B investments-core. |
| In-tree provider framework Platinum sweep | 📐 | Existing providers (OFX, Plaid, tabular) brought to Platinum at launch. |

Pricing is not committed pre-launch. The local CLI + MCP stack will remain fully usable without a hosted account.

---

## Post-launch / Beyond v1

Designed but not gating launch. Listed without commitment.

- **Data export** (CSV, Excel, Google Sheets) as a one-command flow. Until this ships, the data-exit path is direct SQL access to your DuckDB file.
- **Privacy tiers + consent model.** Framework spec at [`privacy-and-ai-trust.md`](specs/privacy-and-ai-trust.md).
- **Connect: more live sources** — Airtable, Smartsheet, and Notion connectors under the same connection-lifecycle pattern as Google Sheets (which ships in M2E).
- **AI-assisted parsing of non-PDF file types** — the smart-import bridge (shipped first for PDF in M2E) applied to other formats.
- **ML-powered categorization + merchant entity resolution.** Needs accumulated labeled data from real users.
- **MCP Apps** (interactive UI inside Claude Desktop, VS Code). Revisit when client support widens.
- **Multi-account-holder sharing.** Single-user is the v1 posture; revisit on user demand.
- **Recurring-transaction storage** (storage table for scheduled definitions).

---

## Explicitly out of scope

To keep solo capacity focused, these are **not on the roadmap** — many never will be. When one of these is a hard requirement, the alternative noted is genuinely the better fit (see [`audience.md`](audience.md) for the full "not yet for you" table).

- **No native mobile apps.** A web-based read-only viewer is the most we'll ship; account linking and editing stay on desktop.
- **First-class split transactions** (parked; split-via-annotation ships in M2A).
- **Envelope budgeting** (zero-based). Use YNAB or Actual Budget.
- **Household / multi-user shared budgets.** Use Tiller, YNAB, or Lunch Money.
- **Direct broker APIs beyond Plaid.** CSV import covers the long tail.
- **Real estate / illiquid assets** beyond manual tracking.
- **Receipt scanning / per-item OCR.**
- **Email forwarding ingestion.**
- **Tax-form generation** (Schedule D, Form 8949). Use Beancount or a professional accountant. The `us_tax` reference package ships *reporting* helpers (realized gain/loss summaries, cost-basis snapshots) on top of M3B investments — not official form output.
- **Public REST API for third-party integrations.** Build when a real consumer requests it.
- **Windows native distribution.** Linux works via PyPI; Mac is the curator audience.
- **Enterprise / SOC 2 path.** Consumer + indie tier; revisit only on enterprise signal.
- **Crypto-heavy or DeFi-only tracking.** Use Rotki.
- **Small-business accounting with payroll.** Use QuickBooks.

---

## How milestone state changes

Statuses move when work merges and the relevant spec marks `implemented`. CHANGELOG records the dated merge; this page records the milestone shape. The README defers here.

MoneyBin is solo-maintained. The AGPL license guarantees the code outlives the maintainer — anyone can fork, host, or continue development under the same terms. See [`licensing.md`](licensing.md).
