# Who MoneyBin Is For

MoneyBin sits in a specific lane: local-first AND AI-native AND open-source AND encrypted-by-default. That combination shapes who fits well today, who'll fit at launch (M3), and who's better served by other tools.

## Today (during M2 development)

MoneyBin fits you if any of the following describes you.

- **You're already running a power-user finance tool** — Tiller in Sheets, Lunch Money, Beancount, hledger, GnuCash, or a heroic spreadsheet — and you want a real database with AI on top.
- **You want a finance MCP that runs against your own data file**, not someone else's hosted store. You've tried hosted MCP-finance products (Era, BankSync, etc.) and the vendor-locked data plane is a non-starter.
- **You self-host other infrastructure** — Vaultwarden, NextCloud, Photoprism, Jellyfin, Pi-hole — and the personal-finance gap has been on your todo list for a while.
- **You're comfortable with a CLI install today.** `brew install moneybin` ships at M2C; until then, `git clone` + [uv](https://docs.astral.sh/uv/) + `make setup` is the install path.
- **You're an AI-native developer** living in Claude Code, Cursor, or VS Code, who wants every domain of your data exposed via MCP.

## At launch (M3 close)

The audience widens once M3 closes. The hosted tier removes the install barrier; Plaid sync removes the manual-export ritual; the Web UI gives a visual surface; investments + multi-currency cover broader segments.

- **Trackers** — anyone who wants a polished visual dashboard for their spending and net worth, with an AI of their choice answering questions about it.
- **FIRE / wealth-builders** — net worth + investments in one queryable warehouse, with FIFO cost basis, realized/unrealized gain/loss, ST/LT classification.
- **Migrants from cloud PFM** — Mint refugees, Monarch evaluators, Copilot churners. Honest pricing, AI of your choice, walk-away guarantee on your data.
- **Privacy-conscious users on the hosted tier** — zero-knowledge passphrase model means even we can't read your data; download your DuckDB any time.

## Not yet for you (today)

If any of these is a hard requirement, MoneyBin isn't the right answer right now — try the alternatives noted.

| Need | Better fit today |
|---|---|
| One-click bank sync | [Monarch](https://www.monarchmoney.com/), [Copilot](https://www.copilot.money/), [YNAB](https://www.ynab.com/) — Plaid sync arrives in M3A |
| Polished mobile app | [Copilot](https://www.copilot.money/), [Monarch](https://www.monarchmoney.com/) — mobile read-only viewer is post-launch at earliest |
| Investment tracking with cost basis | [Wealthfolio](https://wealthfolio.app/), [Beancount](https://beancount.github.io/), [Portfolio Performance](https://www.portfolio-performance.info/) — investments arrive in M3B |
| Pure envelope budgeting | [YNAB](https://www.ynab.com/), [Actual Budget](https://actualbudget.org/) — envelope budgeting is a post-launch consideration |
| Tax-form generation (Schedule D, Form 8949) | [Beancount](https://beancount.github.io/), TurboTax, professional accountants — tax features are post-launch |
| Pure plain-text accounting (double-entry, postings) | [Beancount](https://beancount.github.io/), [hledger](https://hledger.org/) — MoneyBin uses a star-schema data warehouse, not a double-entry ledger |

## Honest framing

MoneyBin is **not** trying to be a one-app-fits-all replacement for the cloud-PFM market. The cloud PFMs have years of polish, polished mobile apps, deep marketing budgets. We compete on a different axis: ownership, lineage, AI-native architecture, and the same code running locally or hosted.

If that axis matters to you, the rest of the documentation will feel like home. If it doesn't, the alternatives above are excellent and we're happy to point you at them.
