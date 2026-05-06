# How MoneyBin Compares

A wider comparison than the ✓/✗ summary in the README. The peer set is the personal-finance / local-first / open-source / AI-native landscape MoneyBin actually competes in.

> The README's [Comparison](../README.md#comparison) section keeps a tight 5-row ✓/✗ table sized for any viewport. This page is the deeper dive for users evaluating MoneyBin against specific alternatives.

## Eight-way comparison

|  | Beancount/Fava | Firefly III | Actual Budget | Maybe/Sure | Era / BankSync | Lunch Money | Wealthfolio | MoneyBin |
|---|---|---|---|---|---|---|---|---|
| **Storage** | Plain-text ledger | MySQL/Postgres | Local SQLite | PostgreSQL | Hosted | Hosted | Local SQLite | Encrypted DuckDB |
| **Encrypted at rest** | ✗ (OS only) | ✗ | ✗ | ✗ | server-side | server-side | ✗ | ✓ default |
| **Bank sync** | OFX importers | Nordigen (6000+) | goCardless / SimpleFIN | Plaid / SimpleFIN | Plaid (hosted) | Plaid (hosted) | Manual + CSV | Designed (M3A) |
| **AI / MCP** | Community wrappers | — | Community wrappers | — | Hosted MCP | Community wrapper | — | First-party local + hosted |
| **SQL access** | — | API only | — | — | — | API only | — | DuckDB native |
| **Web UI** | Fava (localhost) | Yes | Electron | Yes | Yes | Yes | Tauri desktop | M3D |
| **Investments** | ✓ (lots, cost basis) | basic | basic | ✓ | — | basic | ✓ | M3B |
| **Multi-currency** | ✓ | ✓ | ✗ (workaround) | ✓ | — | ✓ | ✓ | M3C |
| **Open source** | ✓ | ✓ AGPL | ✓ MIT | ✓ AGPL | ✗ | ✗ | ✓ | ✓ AGPL |
| **Self-host** | ✓ files | ✓ Docker | ✓ | ✓ | ✗ | ✗ | ✓ desktop | ✓ + M3E hosted parity |
| **Maturity** | Years | Years | Years | Archived; "Sure" fork active | New (2026) | Years | Active | Pre-launch (M2 in flight) |

## Tier framing (from the strategic review)

These categories come from the May 2026 competitor landscape analysis. They help orient *why* a given tool feels close or distant.

### Tier 0 — Direct lane (AI-native + personal-finance vertical)

Same headline pitch ("AI-native personal finance via MCP") as MoneyBin.

- **[Era](https://era.app/)** — hosted MCP, ~33 tools across 7 groups, OAuth 2.1.
- **[BankSync](https://banksync.io/)** — hosted MCP at $7/mo, 36 tools, 10k+ banks.
- **[Truthifi](https://truthifi.com/)** — read-only-by-architecture portfolio MCP.
- **[Muntze](https://muntze.com/)** — crypto-only MCP co-pilot.
- **OpenAI / Hiro Finance** (acquired April 2026) — vertical AI personal finance shipping inside ChatGPT.

What separates MoneyBin: every Tier 0 competitor is hosted, closed-source, or both. None expose a SQL warehouse with lineage. None encrypt the user-owned database file at rest. None let the user "download your DuckDB and walk away."

### Tier 1 — Adjacent (one bet shared)

Some overlap with MoneyBin's bet, but missing one of the legs.

- **[Fina Money](https://fina.money/)** — "Notion for finance," modular blocks with AI insights. Closest spiritual neighbor for the curator posture; cloud + closed.
- **[Cleo](https://meetcleo.com/), [Copilot Money](https://copilot.money/), [Origin](https://useorigin.com/), [Monarch](https://www.monarchmoney.com/), [Rocket Money](https://www.rocketmoney.com/)** — AI-as-feature in closed cloud apps.
- **[Lunch Money](https://lunchmoney.app/)** — indie cloud PFM with public API; a community MCP wrapper exists.
- **[Wealthfolio](https://wealthfolio.app/)** — Tauri/Rust desktop investment tracker, true local-first, no AI angle.
- **[Sure](https://github.com/we-promise/sure)** (Maybe community fork) — AGPLv3, active, full PFM + investments. Reference data model for `investment-tracking.md`.

### Tier 2 — Conventional incumbents

Mainstream cloud PFM, where AI is bolted on.

- **[YNAB](https://www.ynab.com/), [Quicken Simplifi](https://www.quicken.com/products/simplifi/), [Empower](https://www.empower.com/personal-dashboard) (Personal Capital), [PocketSmith](https://www.pocketsmith.com/), [Banktivity](https://www.iggsoftware.com/banktivity/), [Tiller](https://www.tiller.com/).** AI is feature-checkbox, not interface.

### Tier 3 — Plain-text accounting & ledgers

Local, open, scriptable, but not AI-native.

- **[Beancount](https://beancount.github.io/) + [Fava](https://github.com/beancount/fava), [hledger](https://hledger.org/), [GnuCash](https://www.gnucash.org/), [BeanHub](https://beanhub.io/), [Costflow](https://www.costflow.io/).**

## Where MoneyBin lands

The empty quadrant in this landscape is **local-first AND AI-native AND open-source AND encrypted-by-default.** Every Tier 0 competitor is hosted, closed, or both. Tier 1 each picks one or two of those four properties. Tier 2 picks none. Tier 3 has the local-first + open-source pair but lacks AI-native and is built on a different paradigm (double-entry plain-text ledger vs SQL data warehouse).

MoneyBin is alone in the four-way intersection. The hosted tier (M3E) does not break this — same AGPL code anyone can self-host, zero-knowledge passphrase model, "download your DuckDB any time" walk-away guarantee.

## Honest sequencing notes

The comparison table reflects current state as of M2 in flight. Several rows that show "M3A" / "M3B" / etc. for MoneyBin will flip to ✓ at M3 close:

- Bank sync: `Designed (M3A)` → `✓ Plaid` after M3A closes
- Investments: `M3B` → `✓ FIFO lots, cost basis` after M3B closes
- Multi-currency: `M3C` → `✓ Phase 1` after M3C closes
- Web UI: `M3D` → `✓ local + hosted` after M3D closes

This page should be kept current as those milestones close.

## What MoneyBin doesn't try to be

The strategic review made some explicit "we are not for these audiences" calls. Listing them here so the comparison is honest:

- **Mass-market mobile-first consumers.** Monarch, Copilot, and Rocket Money are well-funded and mobile-polished. MoneyBin is desktop-and-web; native mobile is post-launch at earliest.
- **Professional accountants / CPAs.** Beancount and QuickBooks own this segment. The features they need (proper double-entry, tax-year-aware lot matching, audit trails for clients) are deep enough to warrant a different product.
- **Pure envelope budgeters.** YNAB and Actual Budget are excellent at envelope. MoneyBin parks envelope budgeting (M3C ships traditional + rollovers, which covers the 80% case).
- **Enterprise / B2B.** SOC 2 deferred. Self-host is the answer to "we need control."

If you fit one of the above, the alternatives noted are where you should look.
