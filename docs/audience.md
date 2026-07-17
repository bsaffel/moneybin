<!-- Last reviewed: 2026-07-09 -->
# Who MoneyBin Is For

MoneyBin is built for a specific set of people. This page tells you whether you're one of them — honestly, including who you should use instead if you're not.

The lane is narrow on purpose: your data stays on your machine, AI assists rather than runs the show, the code is open source, and every database file is encrypted at rest. Every persona below either already lives in that intersection or is being built toward it. If none of these describe you, the [comparison page](comparison.md) names the tool that probably should.

> **MoneyBin is pre-v1.** Dates aren't committed. Where a persona is listed under "Coming later" below, the honest answer to "when?" is "we're not promising a quarter." If you need a finished product today, the post-launch personas should stay on the tool they're using.

**Quick navigator.** MoneyBin's strongest fit today is power users comfortable in a terminal — the four personas under "Already a good fit." If that's not you, skip straight to **[Coming later](#personas--coming-later)** for the visual / FIRE / non-USD personas, or **[Not yet for you](#not-yet-for-you)** for the cases where another tool is genuinely better.

## Personas — already a good fit

These are the people MoneyBin already serves well in what's shipped now. Install is still `git clone` + `uv` until the Homebrew tap lands — see "What's still rough" under each persona.

### The Tiller-migrant CLI user

**Stack:** macOS, Tiller-in-Sheets for years, a folder of monthly CSV exports, comfortable in a terminal.

**Job to be done:** Get out of the spreadsheet. Keep the multi-account history. Stop hand-categorizing every month.

**Why MoneyBin fits today:**
- The smart tabular importer reads your Tiller export directly — column detection plus a Tiller migration profile, no field-mapping ceremony.
- Cross-source dedup means you can re-import overlapping months without double-counting; transfer detection stops transfers from showing up as spending.
- Auto-rule learning watches your category edits and proposes rules; by the third or fourth import, the LLM is barely involved.

**Migration paths from other tools:**
- **Tiller, Mint, YNAB** — first-class migration profiles, column detection handles their export shapes directly.
- **Lunch Money, Copilot, Monarch** — no first-class migration profile yet. Export to CSV and the generic tabular importer will read it, but expect to set column mappings the first time. No automated history pull from the vendor's API.
- **Beancount / hledger** — no direct ledger importer; export postings to CSV and import that. Round-tripping back to Beancount syntax isn't supported.
- **Bank OFX/QFX/QBO** — full first-class import alongside tabular.

**Data exit:** A plaintext-export command (CSV out of every core table) is planned but not shipped. Today the database itself is portable — one encrypted DuckDB file per profile, queryable with any DuckDB client.

**What's still rough:** The install path is still `git clone` + `uv` + `make setup`; a Homebrew tap is planned. No visual UI yet — see the tracker persona below.

### The agent-native developer

**Stack:** Claude Code or Cursor as the daily driver, comfortable curating a `~/.claude/` config, already pipes work through MCP servers. Wants their financial data inside that loop without trusting a hosted vendor with it.

**Job to be done:** Ask "what did I spend on AWS last quarter?" inside the same chat window where they write code. Get back a real answer with SQL behind it, not a hosted vendor's summary — and when the built-in answer isn't enough, have the agent *build* the missing piece.

**Why MoneyBin fits today:**
- A wide MCP surface across accounts, transactions, reports, categories, merchants, system, sync, and transform — installable via `moneybin mcp install --client claude-code` (nine clients supported).
- Every MCP tool has a CLI twin with `--output json` parity; the CLI is a first-class agent surface, not an afterthought.
- The agent answers with SQL it wrote against canonical fact and dimension tables — and you can read that SQL and verify it.
- It's built to be *extended* by the agent, not just queried by it. The schema and the import pipeline are stable surfaces an agent can build against today; a declarative report-runner contract and the broader extension contract (reports, packages, providers) are in flight. Together they make "vibe-code a custom report / importer / tracker on top of my own financial data" the intended workflow, not a hack — MoneyBin wants to be the first tool your agent reaches for.

**What's still rough:** Today's MCP transport requires the AI client and MoneyBin to run on the same machine. Remote-client support (so ChatGPT web or a hosted assistant can reach a MoneyBin running elsewhere) is planned.

### The self-hoster filling a finance gap

**Stack:** Synology or unRAID NAS running Vaultwarden, Immich, Jellyfin, Pi-hole. Each app owns its data, files live in directories they can `tar` and walk away with.

**Job to be done:** Close the personal-finance gap with something that follows the same rules — local data, encrypted at rest, no vendor account, AGPL.

**Why MoneyBin fits today:**
- The data plane is one encrypted DuckDB file per profile, AES-encrypted at rest. Back it up like any other file, walk away with it whenever you want.
- No hosted dependency for the core product. Bank-direct sync (when you opt in) goes through a server you can also self-host; OFX/QFX/CSV import is fully local.
- Same code in the eventual hosted tier is what you self-host — AGPL guarantees that.

**What's still rough:** No always-on daemon yet — MoneyBin runs when you invoke it. Linux works via PyPI but Mac is the primary target; Windows isn't on the roadmap.

### The privacy-conscious power user

**Stack:** Doesn't trust Plaid, doesn't want Mint-style data resale, runs Little Snitch, would rather download an OFX file every month than authorize a third party.

**Job to be done:** Keep using bank-direct OFX exports, get the modern analytics layer (categorization, recurring detection, net worth) without surrendering data to an aggregator.

**Why MoneyBin fits today:**
- OFX/QFX/QBO import has full parity with tabular — re-import detection, batch revert, institution auto-resolution. No Plaid required.
- LLM-assisted categorization strips amounts, dates, and account identifiers before any prompt leaves your machine. Structural signals only — the model sees the shape of the description, not your money.
- The threat model is documented — what encryption protects against, what it doesn't (forgotten passphrase, malware on your machine, the AI vendor's data flow).

**What's still rough:** Categorization assist still routes through whatever model your AI client is using — you choose the model, but you also accept its data-flow terms. The redaction layer narrows that exposure; it doesn't eliminate it. For exactly what reaches the provider on every tool — and how to run a fully local model so nothing leaves — see [What the AI Provider Sees](guides/what-the-ai-sees.md).

## Personas — coming later

These are the people MoneyBin is being built for but doesn't fully serve yet. No committed dates pre-v1; use the tool listed under "What's still rough" today.

### The tracker

**Stack:** Monarch or Copilot today. Likes a clean visual dashboard of net worth and spending trends. Doesn't want to think about SQL — wants charts, categories, and "where did the money go this month."

**Job to be done:** A polished visual surface for spending and net worth, with optional AI on the side — not a chatbot pretending to be an interface.

**Why MoneyBin will fit:** A browser-based web UI is planned — dashboards, account management, balance reconciliation, multi-currency views. It will work on a phone browser, but **there is no native mobile app planned for v1.** The web UI runs against your local MoneyBin or, once it exists, the hosted tier.

**Do you have to use AI?** No. AI is an optional layer over a deterministic SQL pipeline. Every number on the screen will exist whether or not you ever open a chat box; the AI is for asking questions about those numbers.

**Partner sharing:** MoneyBin is single-user. There's no household-shared budget on the v1 roadmap — if joint finances with a partner are a hard requirement, Monarch or Tiller is the right answer.

**What's still rough:** No production web UI yet. A CLI/MCP review queue exists for the AI-categorization workflow; there is no web surface yet. **Stay on Monarch or Copilot until the web UI ships.**

### The FIRE / wealth-builder

**Stack:** A taxable brokerage at Fidelity or Schwab, a Roth IRA, maybe a 401(k), maybe some crypto. Tracks net worth in a spreadsheet and updates it monthly. Cares about cost basis, realized/unrealized gain/loss, and short-term vs long-term classification at tax time.

**Job to be done:** One queryable warehouse holding cash + investments together. FIFO lot tracking. Numbers that tie to the 1099-B at year-end.

**Why MoneyBin will fit:** The core ledger is built and working: a manually-maintained securities catalog, an investment transaction ledger (buys, sells, reinvested dividends, interest, capital-gain distributions, transfers, splits, fees, return of capital), four cost-basis methods (FIFO, HIFO, specific identification, average cost), derived tax lots, and realized short-/long-term gain/loss reporting — the 1099-B surface. That math has been checked against a hand-labeled, full-tax-year fixture, not yet against a real broker's 1099-B — that reconciliation is the one gate left before this persona can trust the numbers at tax time, and partial trust is a worse outcome than none.

Automated broker sync isn't built yet, so entry is manual today via `investments add` (CSV import will fill remaining gaps once it ships). When broker sync lands, it'll cover whatever brokers the sync framework supports — not a separate promise. Real estate, private equity, and illiquid assets stay manual.

**What's still rough:** No real-broker 1099-B tie-out yet — the cost-basis engine has only been proven against a synthetic fixture. No market pricing, so `investments holdings` shows quantity and cost basis but not current market value or unrealized gain/loss, and investment positions don't yet fold into net-worth reports. No automated broker sync for holdings — entry is manual today. Lot selection for a disposal can be overridden by hand (`investments lots select`), but there's no automated tax-loss-harvesting workflow, and no IRR/TWR performance views. **Use [Wealthfolio](https://wealthfolio.app/), [Beancount](https://beancount.github.io/), or [Portfolio Performance](https://www.portfolio-performance.info/) in the meantime** — and track progress on the [roadmap](roadmap.md) (M1J).

### The non-USD user

**Stack:** Lives outside the US, holds accounts in two or three currencies, occasionally moves money across them and would like to know whether they gained or lost on the round-trip.

**Job to be done:** Import multi-currency transactions, see home-currency equivalents, get FX gain/loss on conversions.

**Why MoneyBin will fit:** Multi-currency support is planned — original-currency amounts preserved alongside home-currency conversions, daily FX rates, realized FX gain/loss on conversions. The work closes when a non-USD user can round-trip a deliberate conversion and the FX gain/loss ties to bank-statement-derived expectation within $0.01.

**What's still rough:** Today MoneyBin treats every amount as USD. **Use [Firefly III](https://www.firefly-iii.org/) or [Beancount](https://beancount.github.io/) in the meantime.**

## Not yet for you

If any of these is a hard requirement, MoneyBin isn't the right answer. The competitor noted is genuinely the better fit — these aren't disclaimers, they're recommendations.

| If you need… | Use instead |
|---|---|
| To share a household budget collaboratively with a partner | [Tiller](https://www.tiller.com/), [YNAB](https://www.ynab.com/), [Lunch Money](https://lunchmoney.app/) |
| Pure envelope budgeting (zero-based, every dollar a job) | [YNAB](https://www.ynab.com/), [Actual Budget](https://actualbudget.org/) |
| Plain-text double-entry accounting (postings, ledger files) | [Beancount](https://beancount.github.io/) + [Fava](https://github.com/beancount/fava), [hledger](https://hledger.org/) |
| To bookkeep client books as a CPA or tax preparer | [QuickBooks](https://quickbooks.intuit.com/), [Xero](https://www.xero.com/) |
| Small-business accounting with employees and payroll | [QuickBooks](https://quickbooks.intuit.com/) |
| Crypto-heavy or DeFi-only tracking with on-chain integrations | [Rotki](https://rotki.com/) |
| A polished mobile app available today | [Copilot](https://copilot.money/), [Monarch](https://www.monarchmoney.com/), [Lunch Money](https://lunchmoney.app/) |
| Budgeting only, no AI in the loop | [YNAB](https://www.ynab.com/), [Actual Budget](https://actualbudget.org/), [Beancount](https://beancount.github.io/) + Fava |
| Tax-form generation (Schedule D, Form 8949) | [Beancount](https://beancount.github.io/), TurboTax, a professional accountant |

---

If any of the "already a good fit" personas described you, the rest of the docs will feel like home — start with [What Works Today](features.md) or [Where MoneyBin Fits](comparison.md).
