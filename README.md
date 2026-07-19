<!-- Last reviewed: 2026-07-18 -->
<!-- markdownlint-disable MD033 MD041 -->
<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/moneybin-logo-on-dark.svg">
    <img src="docs/assets/moneybin-logo-on-light.svg" alt="MoneyBin" width="320">
  </picture>

  **Your finances, understood by AI.**

  A personal finance platform built like a data warehouse: one encrypted
  DuckDB file on your machine, SQL all the way down, and a first-party MCP
  server for the AI you already use.

  [Run the demo](#sixty-seconds-on-synthetic-data) · [What works today](docs/features.md) · [Architecture](docs/architecture.md)

  [![CI](https://github.com/bsaffel/moneybin/actions/workflows/ci.yml/badge.svg)](https://github.com/bsaffel/moneybin/actions/workflows/ci.yml)
  [![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-8A6A1C.svg)](LICENSE)
  [![Python 3.12+](https://img.shields.io/badge/Python-3.12+-1C1A16.svg)](https://www.python.org)
  [![DuckDB](https://img.shields.io/badge/DuckDB-powered-FFF000.svg)](https://duckdb.org)
</div>
<!-- markdownlint-enable MD033 MD041 -->

MoneyBin imports bank files (CSV, OFX/QFX/QBO, Excel, Parquet, selectable-text
PDF), syncs Plaid-linked accounts, and connects Google Sheets — all into one
AES-256-GCM-encrypted [DuckDB](https://duckdb.org) file. Query it three ways:
the CLI, raw SQL, or an MCP server exposing more than 100 tools to Claude,
Cursor, VS Code, Gemini CLI, Codex, and other clients. Every surface reads the
same tables.

![Downloaded Files, Linked Accounts, and Connected Sheets flow into an
Encrypted Local DuckDB database. The database serves CLI, SQL, and MCP.](docs/assets/moneybin-workflow.svg)

Local only. No telemetry. No vendor account. Account and routing numbers in
typed fields leave the machine only as masked placeholders (`****1234`) — no
consent tier unlocks the real value, and what free text can carry is
[documented](docs/guides/what-the-ai-sees.md), not hand-waved. Every one of
those claims is verifiable in source — AGPL-3.0.

## Ask your money anything

```bash
uv run moneybin mcp install --client claude-code   # or claude-desktop, cursor, gemini-cli, codex, ...
```

One command — run from the project checkout the [next
section](#sixty-seconds-on-synthetic-data) sets up — wires the MCP server into
the AI client you already use. Then ask, in your own words:

- *"What changed in my dining spending over the last three months, and which
  merchants explain it?"*
- *"Find my recurring subscriptions and their annual cost."*
- *"Show me the SQL behind that number."*

That last one is the point: the assistant queries the same tables the CLI
reads, so an answer is a query you can rerun — not a paragraph you have to
trust.

The boundary, plainly: the MCP server runs locally and sends no telemetry and
no model calls of its own — the `sync` and Sheets tools reach only the
endpoints you configure. The AI client you connect is what talks to a model
provider: a question to a cloud-hosted assistant shares whatever data the
answer required. LLM-assisted
categorization is opt-in and strips amounts, dates, and account identifiers
before the prompt leaves. Details: [threat model](docs/guides/threat-model.md).

## Sixty seconds on synthetic data

You need Python 3.12+, [uv](https://docs.astral.sh/uv/), and Git.

```console
$ git clone https://github.com/bsaffel/moneybin.git && cd moneybin
$ make setup
$ uv run moneybin demo
Generated 995 transactions for persona 'basic' (seed=42, 2023-01-01 to 2025-12-31)
SQLMesh transforms completed in 4.00s
✅ Demo profile 'demo' ready (2 accounts, 995 transactions, 859 categorized).

$ uv run moneybin reports networth
Net worth as of 2025-12-27: 212913.05
  Assets:      212913.05
  Liabilities: 0.00

$ uv run moneybin sql query "
    SELECT category, COUNT(*) AS txns, SUM(amount) AS total
    FROM core.fct_transactions
    WHERE amount < 0 AND category IS NOT NULL
    GROUP BY 1 ORDER BY total ASC LIMIT 5"
category | txns | total
Housing & Utilities | 144 | -62676.66
Food & Drink | 304 | -14353.58
Services | 36 | -5112.00
Shopping | 88 | -4889.37
Transportation | 113 | -3609.95
```

The demo is deterministic synthetic data pushed through the real pipeline —
import, transform, dedup, categorization, integrity checks. Its window is the
three most recent complete years, so the dates and totals in your run roll
forward from those shown. `--seed` varies it; `--persona family` and
`--persona freelancer` change its shape. It builds
its own profile and never touches a real one, though it does make `demo` the
active profile and prints the command to switch back. Spending totals are
negative: the accounting sign convention holds across every surface.

## Should you trust it with your money yet?

MoneyBin is pre-v1 and installs from source; there is no published package
yet. The author's own finances run on it daily. macOS is the primary target,
Linux is supported, Windows is untested. The Plaid leg is author-tested
against a production account but has had no non-author validation. Calibrate
accordingly: start on the demo, then import file exports you keep anyway, and
run `moneybin db backup` before anything large.

## Bring your own data

Create a real profile and point it at an export:

```bash
uv run moneybin profile create personal
uv run moneybin profile switch personal                 # demo left itself active
uv run moneybin import files ~/Downloads/checking.qfx   # OFX / QFX / QBO
uv run moneybin import files ~/Downloads/history.csv    # CSV / Excel / Parquet
uv run moneybin reports spending
```

Imports are idempotent — re-import an overlapping month and source IDs plus
content matching keep the count right. Coming from Tiller, Mint, or YNAB, the
[data import guide](docs/guides/data-import.md) has a migration path per tool,
and documents how to revert a batch.

## What it is not

No web UI, no mobile app, no published package, no hosted service. Not
envelope budgeting, not double-entry accounting, not tax software. The
[audience page](docs/audience.md) names the better tool for each of those —
use it before migrating.

## Read next

- [What works today](docs/features.md) — the shipped capability boundary
- [Data import](docs/guides/data-import.md) — files, Plaid, Sheets, migrations
- [MCP server](docs/guides/mcp-server.md) — tool catalog, envelope, redaction
- [Database and security](docs/guides/database-security.md) — encryption, backups, profiles
- [Architecture](docs/architecture.md) — the data layers and the contracts they keep
- [Where MoneyBin fits](docs/comparison.md) — and where it doesn't

## Contributing

[AGPL-3.0](LICENSE). Bug reports, focused proposals, and pull requests are
welcome — start with [CONTRIBUTING.md](CONTRIBUTING.md); questions and design
conversations go to [GitHub Discussions](https://github.com/bsaffel/moneybin/discussions).
