<!-- Last reviewed: 2026-07-18 -->
<!-- markdownlint-disable MD033 MD041 -->
<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/moneybin-logo-on-dark.svg">
    <img src="docs/assets/moneybin-logo-on-light.svg" alt="MoneyBin" width="320">
  </picture>

  **Your finances, understood by AI.**

  A local-first financial data platform: one encrypted database, open
  interfaces, and answers you can trace back to their source.

  [Try the demo](#try-it-safely) · [What works today](docs/features.md) · [Read the architecture](docs/architecture.md)

  [![CI](https://github.com/bsaffel/moneybin/actions/workflows/ci.yml/badge.svg)](https://github.com/bsaffel/moneybin/actions/workflows/ci.yml)
  [![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-8A6A1C.svg)](LICENSE)
  [![Python 3.12+](https://img.shields.io/badge/Python-3.12+-1C1A16.svg)](https://www.python.org)
</div>
<!-- markdownlint-enable MD033 MD041 -->

MoneyBin turns bank files, spreadsheets, and connected-account data into one
encrypted [DuckDB](https://duckdb.org) database on your machine. Work with it
from the terminal, query it with SQL, or let an MCP-compatible assistant help
you investigate it. The database is yours; no separate hosted financial app
owns the canonical copy.

That matters when you want to change tools, rebuild a report, or audit an
answer instead of accepting a dashboard's conclusion.

It is for people who want more than a dashboard and less than a pile of
spreadsheets: a durable financial data layer that an AI assistant can
inspect without becoming the source of truth.

## Why this exists

Personal finance data tends to fragment. A bank export has history, a
spreadsheet has corrections, an aggregator has recent activity, and an AI
answer has no obvious way to show where it came from. MoneyBin gives those
inputs one modeled home and keeps the reasoning inspectable.

- **Own the data.** Each profile is an AES-256-GCM-encrypted DuckDB file. Use
  it locally without a vendor account, back it up like a file, and query it
  with tools you already trust.
- **Keep one canonical record.** Imports are recorded as batches, duplicate
  records are reconciled across sources, and corrections live separately from
  the source data. Re-importing overlapping history should not create a second
  ledger.
- **Ask better questions.** The CLI, SQL interface, and MCP server operate on
  the same modeled tables. An assistant can help investigate spending or net
  worth while you retain a direct route to the rows and queries behind it.

## The workflow

![Files, Linked Accounts, and Connected Sheets flow into an Encrypted Local
DuckDB database. The database serves CLI, SQL, and MCP.](docs/assets/moneybin-workflow.svg)

Start with files you already have, such as CSV, OFX/QFX/QBO, Excel, Parquet,
or a bank-statement PDF with selectable text. MoneyBin can also pull connected
bank data through Plaid and live tabular data from Google Sheets. Those inputs
flow through one pipeline before reports, SQL, and AI tools read them.

That separation is intentional. An import is evidence, derived tables are
rebuildable, and your notes, tags, categories, and other edits are auditable
state rather than hidden changes to the source file.

## Try it safely

MoneyBin currently installs from source. macOS is the primary target; Linux is
supported, and Windows has not yet been tested. You need Python 3.12+,
[uv](https://docs.astral.sh/uv/), and Git.

```bash
git clone https://github.com/bsaffel/moneybin.git
cd moneybin
make setup
moneybin demo
```

`moneybin demo` creates a dedicated profile populated only with deterministic
synthetic data. It runs the normal pipeline, checks the result, and prints a
first net-worth answer. Re-running it rebuilds that demo profile; it never
imports or changes a real profile.

Choose a demo shape with `--persona basic`, `--persona family`, or
`--persona freelancer`. Pass `--seed` to reproduce the same dataset while you
evaluate a query or MCP workflow before pointing it at personal history.

Use the demo to ask a few concrete questions:

```bash
moneybin reports spending
moneybin reports cashflow
moneybin sql query "SELECT * FROM reports.net_worth LIMIT 10"
```

The demo command switches the active profile to `demo` after a clean run. If
you already had one, it prints the command to switch back.

## Bring your own data

When you are ready to work with real data, create a separate profile and point
MoneyBin at an export you can keep:

```bash
moneybin profile create personal
moneybin import files ~/Downloads/checking.qfx
moneybin reports networth
```

The import command refreshes derived data automatically. You can safely repeat
an overlapping import: MoneyBin records the batch and uses source identifiers
and content-based matching to avoid double-counting. Before a large first
import, create an encrypted backup:

```bash
moneybin db backup
moneybin import files ~/Downloads/transactions.csv
moneybin reports spending
```

Use the [data import guide](docs/guides/data-import.md) for source-specific
paths, including migrations from Tiller, Mint, YNAB, and generic CSV exports.
It also documents what data survives an import and how to revert a batch.

You do not have to abandon your current workflow on day one. Start with an
export, inspect the imported rows, and keep the source files as your fallback.
MoneyBin preserves the source evidence and records how each import was handled,
so you can validate totals before making it the place you return to every
month. The goal is confidence, not a migration leap of faith.

## Use an assistant without hiding the data model

MoneyBin exposes a local MCP server for AI clients such as Claude Desktop,
Claude Code, Cursor, VS Code, Gemini CLI, and Codex. The installer writes the
appropriate local client configuration:

```bash
moneybin mcp install --client claude-code --profile personal
```

Once connected, use the assistant for questions that benefit from exploration:

> What changed in my dining spending over the last three months, and which
> merchants explain it?

> Show my current net worth and the accounts with the largest month-over-month
> change.

The MCP server, CLI, and read-only SQL interface are peers over the same data.
Tool responses include structured results and next-step hints; you can always
fall back to the CLI or SQL when you want to verify a conclusion yourself.

### Know the AI boundary

MoneyBin's MCP server runs locally and does not phone home. The AI client you
connect to may still send your prompt and MoneyBin tool results to its model
provider. Treat a question to a cloud-hosted assistant as sharing the returned
financial data with that provider.

LLM-assisted categorization is opt-in. Before it sends a categorization prompt,
MoneyBin removes amounts, dates, and account identifiers, but that redaction is
not a substitute for choosing an AI provider whose terms you accept. The
[threat model](docs/guides/threat-model.md) explains the boundary in detail.

## What you can rely on today

MoneyBin is pre-v1 and in daily use by its author. Its working center is a
local, source-backed ledger for people comfortable with a terminal or an
MCP-enabled client.

The everyday loop is working: import or sync data, let MoneyBin rebuild the
canonical tables, review what needs attention, and ask for reports or direct
queries. It covers balances, spending, cash flow, recurring activity, and net
worth without forcing you into a separate reporting store.

The model also carries curation state—categories, merchants, notes, tags, and
splits—without overwriting source data. An investment ledger supports positions,
tax lots, and realized gains; its limitations are documented plainly before you
rely on it for a tax workflow.

The detailed boundary—including source support, known limitations, and which
workflows have been exercised end to end—is in [What Works Today](docs/features.md).

## What it is not

MoneyBin is not yet a finished consumer app. It has no published package or
Homebrew install, no polished first-run onboarding, and no browser dashboard or
native mobile app. It is also not a replacement for collaborative household
budgeting, envelope budgeting, professional bookkeeping, or tax-form filing.

The investment ledger supports cost basis and realized gains, but it is not yet
a tax-preparation product. If you need a mature visual finance app, shared
budgets, plain-text double-entry accounting, or tax workflows today, read the
[honest fit guide](docs/audience.md) before migrating.

## Read next

- [What Works Today](docs/features.md) — the shipped capability boundary and
  known limits
- [Data import](docs/guides/data-import.md) — bring over history from files,
  spreadsheets, or another finance tool
- [MCP server](docs/guides/mcp-server.md) — connect an AI client and understand
  the tool contract
- [Database and security](docs/guides/database-security.md) — encryption,
  backups, profiles, and key management
- [Architecture](docs/architecture.md) — the guarantees and data layers behind
  the user-facing surfaces
- [Where MoneyBin Fits](docs/comparison.md) — a critical comparison with other
  tools

## Contributing

This is an open-source project under [AGPL-3.0](LICENSE). Bug reports, focused
feature proposals, and pull requests are welcome. Start with
[CONTRIBUTING.md](CONTRIBUTING.md), and use
[GitHub Discussions](https://github.com/bsaffel/moneybin/discussions) for
broader questions and design conversations.
