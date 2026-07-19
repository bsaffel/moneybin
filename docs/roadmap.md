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
| **M1J** | Investment ledger, tax lots, cost basis, realized gains, Plaid investment ingestion | 🚧 ledger shipped; closes on a real-broker 1099-B tie-out; market prices and net-worth integration remain — [investments-data-model.md](specs/investments-data-model.md) |
| **M1H, M1Q** | Import confirmation and contributor extension contracts | 🚧 — [smart-import-confirmation.md](specs/smart-import-confirmation.md), [extension-contracts.md](specs/extension-contracts.md) |
| **M1K** | Multi-currency (capture shipped; conversion staged on top) | 🚧 — [multi-currency.md](specs/multi-currency.md) |
| **M1L–M1P** | Recovery completion, pipeline reconciliation, export bundle, anonymized fixtures | 🚧/📐/🗓️ per increment — see the [spec index](specs/INDEX.md) |
| **M2A–M2B** | Curated reports plus net-worth and balance tracking | ✅ shipped |
| **M2C–M2P** | Budgets, recurring review, goals, projections, packages, richer report lineage | 📐/🗓️ per increment — see the [spec index](specs/INDEX.md) |
| **M3A** | Safe evaluator path: `moneybin demo` and first-run | 🚧 demo shipped; first-run work remains |
| **M3B** | Packaging and tester distribution | 🚧 release automation in place; no published package yet |
| **M3C** | Local web UI for review, data quality, accounts, reports | 📐 — [ui-architecture.md](specs/ui-architecture.md) |
| **M3D, M3H, M3J** | Remote MCP, opt-in hosted tier, self-host operations | 🗓️ planned |

Status marks: ✅ shipped · 🚧 in progress · 📐 designed (a public spec exists) ·
🗓️ planned (direction set, implementation not started).

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
