<!-- Last reviewed: 2026-07-18 -->
# Where MoneyBin Fits

Choosing a personal-finance platform is a high-switching-cost decision — your data, your categorization history, the habits you build around it. This page is the honest version: what MoneyBin is, and who should use something else.

We don't keep a feature-by-feature scorecard of other tools here. Those tools are mature and move fast; a grid maintained by us would be both stale and self-serving. Instead, here's the lane MoneyBin is built for, stated on its own terms — and a straight list of where another tool is the better answer.

## What defines MoneyBin

Five properties, together. Plenty of tools have one or two; the **combination** is the point.

- **Local-first, and yours.** Everything lives in one encrypted DuckDB file on your machine. No vendor account is required to use the core product, and you can copy that file and walk away at any time.
- **Encrypted by default.** AES-256-GCM at rest, with no setup step — not "bring your own full-disk encryption."
- **AI-native.** A first-party [MCP](https://modelcontextprotocol.io) server, so the AI assistant you already use can answer real questions about your money — against the same local data the CLI and SQL see. Bring your own model; swap it whenever a better one ships.
- **Queryable like a data warehouse.** Real SQL over [DuckDB](https://duckdb.org), with every number traceable back through a versioned [SQLMesh](https://sqlmesh.com) model to your source file. Ask the AI for an answer, then ask it to show you the SQL.
- **Open source.** [AGPL-3.0](licensing.md). The license guarantees the code outlives its maintainer — anyone can fork, self-host, or continue development under the same terms.

If that intersection is what you've been looking for, the rest of the docs will feel like home — start with [What Works Today](features.md) or [Who MoneyBin Is For](audience.md).

## Where MoneyBin is not the best fit

Honest mismatches matter more than feature checklists. If any of these describe you, the alternative is genuinely the better choice — today, and possibly always:

- **You want a polished mobile app.** Use [Copilot](https://copilot.money/), [Monarch](https://www.monarchmoney.com/), or [Lunch Money](https://lunchmoney.app/). MoneyBin is desktop/CLI today; no native mobile app is planned for v1.
- **You share a budget with a partner or household.** Use [Tiller](https://www.tiller.com/), [YNAB](https://www.ynab.com/), or [Monarch](https://www.monarchmoney.com/). MoneyBin is single-user.
- **You want pure envelope budgeting, no AI in the loop.** Use [YNAB](https://www.ynab.com/) or [Actual Budget](https://actualbudget.org/).
- **You want plain-text, double-entry, git-diffable books.** Use [Beancount](https://beancount.github.io/) + [Fava](https://github.com/beancount/fava) or [hledger](https://hledger.org/). MoneyBin's encrypted DuckDB file is deliberately not a plain-text ledger.
- **You want market-priced holdings or a proven real-broker tie-out.** Use [Wealthfolio](https://wealthfolio.app/), [Beancount](https://beancount.github.io/), or [Portfolio Performance](https://www.portfolio-performance.info/) for those. MoneyBin supports Plaid investment ingestion, a manual-entry investment ledger, and market value from the close a connected broker already sends — but a security no connected broker prices stays unvalued until external feeds land, and a real-broker tie-out is still ahead.
- **You need multi-currency today.** Use [Firefly III](https://www.firefly-iii.org/) or [Beancount](https://beancount.github.io/).
- **You want a battle-tested self-host stack you can deploy this afternoon.** Use [Firefly III](https://www.firefly-iii.org/) or [Sure](https://github.com/we-promise/sure) — MoneyBin's container story is on the roadmap, not in the box.

## A note on maturity

MoneyBin is **pre-v1** and in daily use by its author. The tools above have been refined for years. If you need a finished, polished product right now, one of them is the safer bet — come back when v1 lands. If you'd rather get in early on the local-first, AI-native, fully-queryable approach and tolerate some rough edges, that's exactly who MoneyBin is for today.

For the persona-by-persona breakdown — including the migration paths from each tool — see [Who MoneyBin Is For](audience.md).
