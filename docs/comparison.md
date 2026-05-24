<!-- Last reviewed: 2026-05-24 -->
# How MoneyBin Compares

Choosing a personal-finance platform is a high-switching-cost decision: your data, your categorization history, the habits you build around the UI. This page is the honest comparison we'd write for ourselves. ✅ = yes, ❌ = no, 🟡 = partial, planned, or requires configuration.

## Comparison

The table is 8 columns wide rather than 7 because Firefly III is the most-deployed self-host finance tool and excluding it would misrepresent the landscape.

|  | MoneyBin | Tiller | Lunch Money | Monarch / Copilot | Maybe / Sure | Firefly III | Beancount / hledger | YNAB / Actual |
|---|---|---|---|---|---|---|---|---|
| Local-first (data on your machine) | ✅ | ❌ Sheets-hosted | ❌ hosted | ❌ hosted | ✅ self-host⁽¹⁾ | ✅ self-host | ✅ | 🟡 Actual only⁽²⁾ |
| Open source | ✅ AGPL-3.0 | ❌ | ❌ | ❌ | ✅ AGPL-3.0⁽¹⁾ | ✅ AGPL-3.0 | ✅ | 🟡 Actual MIT, YNAB ❌ |
| Plain-text / VCS-friendly storage | ❌⁽³⁾ | ❌ | ❌ | ❌ | ❌ Postgres | ❌ MySQL/Postgres | ✅ git-diffable | ❌ |
| Encrypted at rest by default | ✅ AES-256-GCM | ❌ | 🟡 server-side | 🟡 server-side | ❌⁽³⁾ | ❌ | ❌⁽³⁾ | 🟡 Actual E2E sync⁽⁶⁾ |
| AI / MCP-native | ✅ first-party | ❌ | 🟡 community MCP | ❌ in-app only | ❌ | ❌ | ❌ | ❌ |
| Agent-buildable extension contract⁽⁹⁾ | 🟡 reports / packages / providers (in flight) | 🟡 Sheets scripting | 🟡 public API | ❌ | 🟡 fork | 🟡 REST API | ✅ Python plugins | 🟡 Actual API |
| Direct SQL query access | ✅ DuckDB | ❌ | ❌ | ❌ | 🟡 Postgres | 🟡 MySQL/Postgres | ✅ BQL / hledger-query | 🟡 Actual SQLite⁽⁷⁾ |
| Automated categorization | ✅ rules + LLM-assist | 🟡 manual + AutoCat | ✅ rules | ✅ rules + ML | ✅ rules | ✅ rules | ✅ smart_importer / --auto | ✅ rules |
| Bank-direct sync | 🟡 Plaid | ❌ manual import | ✅ Plaid | ✅ Plaid | ✅ Plaid/SimpleFIN | ✅ SimpleFIN/Nordigen | ❌ | 🟡 YNAB only |
| Import format coverage | ✅ CSV/OFX/QFX/Excel/Parquet | 🟡 CSV via Sheets | ✅ CSV/OFX | 🟡 CSV | ✅ CSV/OFX | ✅ CSV/OFX/camt | 🟡 CSV + plugins | 🟡 CSV/OFX |
| Investment / cost-basis tracking | 🟡 planned⁽⁴⁾ | 🟡 add-on sheet | 🟡 basic | ✅ | ✅ lots | 🟡 basic | ✅ full | ❌ |
| Multi-currency | 🟡 planned⁽⁴⁾ | 🟡 manual | ✅ | 🟡 limited | ✅ | ✅ | ✅ | 🟡 Actual partial |
| Household / shared budget | ❌⁽⁵⁾ | ✅ | ✅ | ✅ | 🟡 self-host | 🟡 multi-user | ❌ | ✅ |
| Open-format data export | ✅ DuckDB + CSV | ✅ Sheets-native | ✅ CSV | 🟡 CSV | ✅ Postgres | ✅ SQL + CSV | ✅ plain text | 🟡 CSV / Actual file |
| Self-host posture⁽⁸⁾ | 🟡 CLI scriptable | ❌ N/A | ❌ N/A | ❌ N/A | ✅ Docker/Compose | ✅ Docker/Compose | 🟡 CLI scriptable | 🟡 Actual Docker |
| Maturity / time-in-market | ❌ pre-v1 | ✅ since 2017 | ✅ since 2018 | ✅ since 2018 | 🟡 fork active 2024+ | ✅ since 2015 | ✅ 15+ years | ✅ 10+ years |
| No telemetry by default | ✅ | ❌ | ❌ | ❌ | ✅ self-host | ✅ self-host | ✅ | 🟡 Actual ✅, YNAB ❌ |

## Notes

⁽¹⁾ **Maybe / Sure.** The original Maybe Finance company shut down in mid-2024; the codebase continues as AGPL-3.0, with the community fork **Sure** under active development and the path most self-hosters take today. Both are Postgres-backed and require you to run them yourself.

⁽²⁾ **YNAB / Actual.** YNAB is hosted-only. Actual Budget is local-first (or self-hostable sync server), MIT-licensed, and uses SQLite under the hood. The combined cell reflects the union; individual cells call out which side wins.

⁽³⁾ **At-rest encryption and plain-text trade-offs.** Beancount, hledger, Maybe/Sure, and Firefly III rely on whatever the host provides — full-disk encryption, ZFS-native encryption, `age`, or `git-crypt` over a ledger file are all common and fully sufficient. MoneyBin integrates AES-256-GCM into the DuckDB file by default; competing tools delegate the choice. The trade-off cuts both ways: MoneyBin's encrypted DuckDB file is **not** plain-text or `git diff`-able, which is the headline feature of Beancount/hledger. Different defaults, comparable end states for users who configure them deliberately.

⁽⁴⁾ **Planned, not shipped.** Investment + cost-basis tracking and multi-currency support are on the roadmap but not in today's build. See [features.md](features.md) for what works now and [roadmap.md](roadmap.md) for the order of arrival.

⁽⁵⁾ **MoneyBin is single-user.** No household-shared budget is planned for v1. If joint finances with a partner are a hard requirement today, Monarch, Tiller, or Lunch Money is the right answer — see below.

⁽⁶⁾ **Actual E2E encrypted sync.** Actual Budget's sync server supports end-to-end encrypted sync with a user-supplied key, so the server never sees plaintext. The local SQLite file itself isn't encrypted at rest unless you arrange that yourself.

⁽⁷⁾ **Actual's SQLite is app-managed.** The schema is internal to Actual and changes between releases; querying it works for read-only analysis but is not a stable contract the way DuckDB or Postgres access is.

⁽⁸⁾ **Self-host posture** rolls up containerization, headless / cron-friendly operation, reverse-proxy ergonomics, and outbound network posture into one cell. ✅ = published container image with Compose example and no required external calls; 🟡 = scriptable but no first-party container, or some required egress; ❌ = not designed to be self-hosted. MoneyBin today is a Python CLI suitable for cron and headless use, but ships no container image and has no daemon mode — a self-host guide is on the roadmap.

⁽⁹⁾ **Agent-buildable extension contract** measures whether there's a *stable, documented surface for adding your own reports, analyses, and data sources* that an AI agent can target — not merely "it's open source, fork it." ✅ = a first-class contract for new reports/providers that ships today (e.g. Beancount's Python plugin system). 🟡 = an API or scripting layer you can build against but no purpose-built extension contract yet (Sheets scripting, a public REST API, modify-the-source). ❌ = closed, no supported extension path. MoneyBin is 🟡 because its contract — a declarative `@report` runner plus the broader package/provider surface — is designed so the agent you already drive can scaffold an extension against the schema and runner shape, but it is **in flight, not shipped**.

## Where MoneyBin is not the best fit

Honest mismatches matter more than feature checklists. If any of these describe you, the alternative is genuinely better today:

- **You need a polished mobile app.** Use [Copilot](https://copilot.money/), [Monarch](https://www.monarchmoney.com/), or [Lunch Money](https://lunchmoney.app/).
- **You share a budget with a partner or household.** Use [Tiller](https://www.tiller.com/), [YNAB](https://www.ynab.com/), or [Monarch](https://www.monarchmoney.com/).
- **You want pure envelope budgeting, no AI.** Use [YNAB](https://www.ynab.com/) or [Actual Budget](https://actualbudget.org/).
- **You want plain-text, double-entry, git-diffable books.** Use [Beancount](https://beancount.github.io/) + [Fava](https://github.com/beancount/fava) or [hledger](https://hledger.org/).
- **You want a finished investment-tracking workflow today** (FIFO lots, realized/unrealized gain/loss, 1099-B reconciliation). Use [Wealthfolio](https://wealthfolio.app/), [Beancount](https://beancount.github.io/), or [Portfolio Performance](https://www.portfolio-performance.info/) until MoneyBin's investment work lands.
- **You need multi-currency today.** Use [Firefly III](https://www.firefly-iii.org/) or [Beancount](https://beancount.github.io/).
- **You want a battle-tested self-host stack you can deploy this afternoon.** Use [Firefly III](https://www.firefly-iii.org/) or [Sure](https://github.com/we-promise/sure) — MoneyBin's container story is on the roadmap, not in the box.

For the deeper persona-by-persona breakdown, see [audience.md](audience.md).
