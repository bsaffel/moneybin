<!-- Last reviewed: 2026-07-18 -->
# Roadmap

MoneyBin is pre-v1. This page shows direction, not dates or release promises.
For capabilities you can use now, see [What Works Today](features.md); for the
dated record of changes, see the [Changelog](../CHANGELOG.md).

## What works today

MoneyBin is a local-first financial data platform for people comfortable with a
CLI, SQL, or an MCP-enabled AI client. It imports financial files, supports
Plaid sync for cash, credit-card, and investment accounts, stores each profile
in an encrypted DuckDB database, and provides reports, categorization,
investment accounting, reversible edits, and data-quality checks.

The Plaid link, sync, and reconcile flow is author-tested against a production
account but still needs non-author validation. Start with the synthetic
`moneybin demo` profile to explore the product without real financial data.

## Status

| Mark | Meaning |
|---|---|
| ✅ shipped | Merged and usable today. |
| 🚧 in progress | Work has started; the outcome is not complete yet. |
| 📐 designed | A public technical design exists; implementation has not started. |
| 🗓️ planned | Direction is set, but implementation has not started. |

## Milestones

| Milestone | Focus | Current state |
|---|---|---|
| **M0 — Foundation** | Secure local storage and shared product infrastructure | ✅ shipped |
| **M1 — Ingestion Core** | Trustworthy financial data in one warehouse | 🚧 core imports and investment foundations are shipped; validation and completion work remains. |
| **M2 — Analysis & Reports** | Traceable answers built on the warehouse | 🚧 core reports are shipped; deeper analysis is planned. |
| **M3 — Productization & Distribution** | Easier evaluation, installation, and future product surfaces | 🚧 the demo path is shipped; broader distribution and product surfaces are planned. |

## Current and planned increments

| Address | Outcome | Status |
|---|---|---|
| **M0** | Encryption, multi-profile storage, database coordination, CLI/MCP foundations, privacy controls, and integrity checks | ✅ shipped |
| **M1A–M1F** | Tabular and financial-file imports, inbox workflow, matching, categorization, account/transaction curation, and Google Sheets | ✅ shipped |
| **M1G** | Plaid cash, credit-card, and investment sync | ✅ shipped; broader validation continues |
| **M1J** | Investment ledger, tax lots, cost basis, realized gains, and Plaid investment ingestion | ✅ shipped; market prices and net-worth integration remain planned |
| **M1H, M1Q** | Import confirmation and contributor extension contracts | 🚧 in progress |
| **M1K–M1P** | Multi-currency, recovery, export bundles, and anonymized fixtures | 📐 designed or 🗓️ planned |
| **M2A–M2B** | Curated reports plus net-worth and balance tracking | ✅ shipped |
| **M2C–M2P** | Budgets, recurring review, goals, projections, packages, and richer report lineage | 📐 designed or 🗓️ planned |
| **M3A** | Safe evaluator path with `moneybin demo` and first-run improvements | ✅ demo shipped; remaining first-run work is 🗓️ planned |
| **M3B** | Packaging and tester distribution | 🗓️ planned; release automation is in place, but a published package is not yet available |
| **M3C** | Local web UI for review, data quality, accounts, and reports | 📐 designed |
| **M3D, M3H, M3J** | Remote MCP, an optional hosted tier, and self-host operations | 🗓️ planned; none are part of today's local product |

## Boundaries

The local web UI and any hosted tier are planned milestones, not available
products or release promises. MoneyBin is also not planning a native mobile
app, household-shared budgets, or a general-purpose accounting system in the
near term. The [audience guide](audience.md) points to better-established
alternatives when those needs are primary.

## Design references

The repository publishes current contributor-facing contracts in
[`docs/specs/`](specs/) and [decisions](decisions/). Their status may be more
granular than this page; this roadmap is the canonical map of public milestone
addresses and product direction.
