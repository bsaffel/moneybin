<!-- Last reviewed: 2026-09-04 -->
# Getting started

From a clean machine to a first report and a first question to your AI assistant, in eight steps: install from source, try the synthetic demo, create a profile, import one bank file, check what landed, read the first reports, categorize, and wire the MCP server into a client. Budget about an hour, most of it on your bank's download page.

Every transcript below is real output from a fresh profile holding one synthetic 20-transaction January statement, trimmed only by whole lines (file paths, and one deprecation warning a dependency prints). Your numbers will differ; the shape will not.

## What you need

- **Python 3.12+, [uv](https://docs.astral.sh/uv/), Git, and GNU Make.** macOS is the primary target and Linux is supported; Windows is untested.
- **One export from your bank.** OFX, QFX, or QBO from the bank's download page is the best first file: it names its own institution and account, so nothing has to be confirmed. CSV and Excel work too, with a one-time column confirmation. The [data import guide](data-import.md) covers every format and the Tiller, Mint, and YNAB migration paths.
- **Optional: an MCP client** — Claude Desktop, Claude Code, Cursor, VS Code, Codex, or Gemini CLI — for the last step.

File import, the CLI, and SQL need no vendor account and no API key. [Plaid sync](data-import.md#live-banking-sync-plaid) and [Google Sheets](connect-gsheet.md) are separate opt-ins.

## 1. Install from source

There is no published package yet; MoneyBin runs from a checkout.

```bash
git clone https://github.com/bsaffel/moneybin.git
cd moneybin
make setup
```

`make setup` checks that a `python3` is installed, creates `.venv` on the pinned interpreter, locks and syncs the dependencies with `uv`, and installs the pre-commit hooks. Every command from here on is `uv run moneybin …`, run from inside the checkout. The [CLI command tree](../reference/cli/README.md) is one generated page per command group; `--help` on any command prints the same text and never touches a database.

## 2. Try it on synthetic data

Before touching real data, build the demo profile — three years of deterministic synthetic transactions pushed through the real import, transform, dedup, and categorization pipeline:

```bash
uv run moneybin demo
```

The [top-level README](../../README.md#sixty-seconds-on-synthetic-data) shows the full transcript and a first report against it. Two things to know before moving on: the demo makes `demo` the active profile and prints the command to switch back, and it never touches a real profile. `--persona family`, `--persona freelancer`, and `--persona international` change its shape; the [reports guide](reports.md) runs every report against the family persona.

## 3. Create your profile

A profile is one encrypted database, one keychain entry, one audit trail. Create one for your own data:

```console
$ uv run moneybin profile create personal
⚙️  Initializing MoneyBin schema...
```

About 200 lines of schema-migration and transform-plan output follow, then `✅ Created profile personal at …` naming the profile directory — `.moneybin/profiles/personal` inside the checkout when you run from it, as this guide does, and `~/.moneybin/profiles/personal` from anywhere else. The encrypted database file, its config, logs, and backups live under that directory. The import inbox is the one thing that does not: accept the prompt and `~/Documents/MoneyBin/personal/{inbox,processed,failed}` is created for files you drop in, so [moving the profile to another machine](profiles.md#multi-machine-workflows) is a copy plus the key, plus that directory if you use it.

The database exists and is encrypted from this moment: a random 256-bit key is generated and stored in the OS keychain under the service name `moneybin-personal`, and you never type a passphrase. On Linux the keychain is Secret Service (GNOME Keyring or KWallet); a headless box or container with no keyring takes the key from an environment variable instead — see [Headless and cron](database-security.md#headless-and-cron-deployments). `profile create` has no passphrase option, so to type a passphrase instead, decide now while the database is empty: switch to the profile, delete its database file, and run `moneybin db init --passphrase`. [Passphrase mode](database-security.md#passphrase-mode) says what the passphrase protects, and the same guide's Switching modes section wraps that sequence in a backup and a restore, because once data has landed there is no in-place conversion.

`profile create` does not make the new profile active. `profile list` still marks `demo`:

```console
$ uv run moneybin profile list
  demo (active)
  personal
```

Without a switch, the next command still runs against `demo` — or, if you skipped the demo and no profile has ever been active, opens the first-run setup wizard. Switch, then confirm:

```console
$ uv run moneybin profile switch personal
✅ Switched to profile: personal
$ uv run moneybin profile show
Profile: personal (active)
  DB state: exists
  Config (config.yaml):
    database.encryption_key_mode: auto
    logging.level: INFO
    logging.log_to_file: True
  Settings (database):
    home_currency: (not set)
```

`--profile personal` on any single command does the same job for one invocation. `home_currency` stays unset until you choose one — MoneyBin never assumes USD — and a single-currency profile never needs it; the [reports guide](reports.md#one-display-currency) says when it matters. Everything else about profiles — several of them, moving one between machines, deleting one — is in the [profiles guide](profiles.md).

## 4. Import your first file

Download a statement from your bank as OFX, QFX, or QBO — most banks list it as the Quicken format — and import it:

```console
$ uv run moneybin import files ~/Downloads/checking.qfx
Using profile: personal
Created import batch: ec107f8d...
Extracted 1 institution(s), 1 account(s), 20 transaction(s)
Import ec107f8d... finalized: complete (23 imported, 0 rejected)
Running transforms
Transforms completed in 4.60s
Account-link backfill wrote 0 new pending decisions
Merchant linking complete: 0 linked automatically, 0 sent for review.
  Institutions: 1
  Accounts: 1
  Transactions: 20
  Balances: 1
  Date range: 2026-01-02 to 2026-01-31
  Core tables rebuilt (dim_accounts, fct_transactions)
✅ checking.qfx [ofx] — 20 rows
👀 Created account: Example Bank checking …7890 (35aaf6929c62)
   Rename with 'moneybin accounts set <account_id> --display-name <name>'; if it duplicates an account you already have, 'moneybin accounts links run' proposes the merge — and if that proposes nothing, the pair shares no signal, so name it yourself with 'moneybin accounts links run <account_id> <candidate_account_id>'.
✅ Core tables rebuilt
```

In order: the file's rows landed untouched in `raw.ofx_*`; the transforms rebuilt the canonical `core.*` tables the built-in reports read (`sql query` and a saved report can also read `raw`, `prep`, and `app` directly); matching looked for an existing account the new one duplicates (none, on a first import); categorization ran with nothing to apply yet (step 7). The 23 imported rows are the 20 transactions plus the institution, the account, and the closing balance the file carries.

Imports are idempotent: the import log refuses a file it has already seen (`--force` overrides), and source ids plus content matching keep an overlapping month from double-counting. `import revert` undoes one batch.

Coming from Tiller, Mint, YNAB, or a bank's own CSV export, the command is the same with the CSV path, and several files import in one command (`import files ~/Downloads/*.csv`). The named exports are recognised by their column headers (`--format tiller` forces one); a CSV shape MoneyBin has not seen before gets a one-time column confirmation. Categories in the file come through verbatim in `category` and `subcategory` rather than mapped onto MoneyBin's own taxonomy, so a migrated history arrives categorized and step 7 layers rules on top. If the Tiller sheet is still live, [connect it as a Google Sheets source](connect-gsheet.md) instead of exporting. PDF statements, the watched inbox folder, and the per-tool migration profiles are in the [data import guide](data-import.md).

Take a backup now, before the history gets long. It writes an encrypted snapshot under the profile's `backups/` directory and prints the path and size; `db restore --from <path>` is the round trip:

```bash
uv run moneybin db backup
```

## 5. Check what landed

```console
$ uv run moneybin system status
Using profile: personal
System status: 1 accounts, 20 transactions, 0 matches pending, 20 uncategorized, transforms_pending=False
Accounts: 1
Transactions: 20 (2026-01-02 – 2026-01-31)
Last import: 2026-09-04
Matches pending: 0
Uncategorized: 20
Export local:exports (local): ready; write capable: True
```

```console
$ uv run moneybin accounts list
Using profile: personal
Listed 1 accounts
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ account                     ┃ account_id   ┃ institution  ┃ type       ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Example Bank checking …7890 │ 35aaf6929c62 │ Example Bank │ depository │
└─────────────────────────────┴──────────────┴──────────────┴────────────┘
```

The display name is institution, account type, and last four. The account-number field itself leaves the process as those four digits (`****7890`) and a routing number as `*****` in every MCP response and every `--output json` result, because masking follows a field's declared class. Two things that rule does not cover: the operator commands `moneybin db query`, `db shell`, and `db ui`, which print the raw columns behind a banner saying so, and a number that rides inside a description, a note, or an import sample, which travels under that field's own class and is not scrubbed. `sql_query` reads of the `raw` and `prep` schemas fall back to a value scan that catches an unbroken run of eight or more digits and misses shorter or hyphenated ones. [What the AI provider sees](what-the-ai-sees.md#not-masked-stated-plainly) states each case. `accounts set 35aaf6929c62 --display-name "Everyday checking"` renames it, and the twelve-character `account_id` is what every other command and the MCP tools take as a reference.

`system doctor` runs the integrity checks and says what to do about each warning:

```console
$ uv run moneybin system doctor
Using profile: personal
⚠️  categorization_coverage — 100% of non-transfer transactions are uncategorized
   💡 [suggested] transactions_categorize_run(methods=['rules', 'merchants']) — Run the deterministic categorization cascade (rules + merchants) to raise coverage above the 50% threshold. Suggested (not certain) because the cascade applies 0 rows when no active rules or merchant mappings match the remaining uncategorized transactions — re-run the doctor after to verify.

61 invariants checked across 20 transactions — 60 passing, 1 warn, 0 skipped
```

The one warning is expected on a first import and is step 7. The `💡` lines under a doctor check or a report name the MCP tool call an assistant would make next, with the equivalent flag on each command's [reference page](../reference/cli/README.md); the one exception is a report that masked one of its columns, which points at `moneybin reports explain` instead, and elsewhere a `💡` line is a plain hint, such as the command to run next.

## 6. First reports

```console
$ uv run moneybin reports networth
Using profile: personal
USD as of 2026-01-31
Net worth:   4,317.87
Assets:      4,317.87
Liabilities: 0.00
Accounts:    1
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━┓
┃ account                     ┃  balance ┃ currency ┃ source ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━┩
│ Example Bank checking …7890 │ 4,317.87 │ USD      │ ofx    │
└─────────────────────────────┴──────────┴──────────┴────────┘
💡 Run reports(report_id='core:networth_history', parameters={'from_date': 'YYYY-MM-DD', 'to_date': 'YYYY-MM-DD'}) for the time series
💡 Run accounts_balances(view='history', reference='<account>') to drill into one account
💡 Run accounts(include_closed=True) to inspect closed or excluded accounts
```

The as-of date is the latest balance the file carried, and `source` names where that balance was observed — `ofx` is the ledger balance the statement itself reports.

```console
$ uv run moneybin transactions list --limit 5
Using profile: personal
Transaction query returned 5 of 20 rows (has_more=True)
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ date       ┃ description          ┃    amount ┃ category      ┃ account_id   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ 2026-01-31 │ INTEREST PAID        │     +0.42 │ Uncategorized │ 35aaf6929c62 │
│ 2026-01-30 │ STATE FARM INSURANCE │   −118.00 │ Uncategorized │ 35aaf6929c62 │
│ 2026-01-28 │ CHIPOTLE 1188        │    −13.20 │ Uncategorized │ 35aaf6929c62 │
│ 2026-01-26 │ PARKSIDE APARTMENTS  │ −1,850.00 │ Uncategorized │ 35aaf6929c62 │
│ 2026-01-24 │ WHOLE FOODS MARKET   │    −92.15 │ Uncategorized │ 35aaf6929c62 │
└────────────┴──────────────────────┴───────────┴───────────────┴──────────────┘
5 of 20 shown · raise --limit for more · 5 uncategorized
```

Negative is money out, positive is money in, for every transaction-level amount — the CLI, JSON, SQL, and the MCP tools all carry the same sign on a row. The reports that total outflow (`reports spending`, `merchants`, `recurring`) print it as a positive absolute figure; the [Signs bullet](reports.md#reading-the-output) in the reports guide lists which is which. `reports cashflow` and `reports spending` run too, but on one uncategorized month they show a single row with an empty category. The [reports guide](reports.md) walks all eight built-in reports on a populated profile and shows how to save your own.

## 7. Categorize

A fresh profile ships with 112 seeded categories and no rules, so on a bank file — which carries no categories of its own — rule matching and merchant matching find nothing to apply:

```console
$ uv run moneybin transactions categorize run
Using profile: personal
  rules: 0
  merchants: 0
✅ Applied 0 total
```

Three ways to raise coverage, in the order most people use them:

1. **Rules.** `transactions categorize rules create` matches description text to a category and applies on the next refresh; a rule outranks every automated source.
2. **An assistant proposes, you commit.** Through MCP, `transactions_categorize_assist` hands the model, per uncategorized row, the scrubbed description and memo, the transaction id, the source and transaction types, the check number as stored, the transfer flag and pair id, the payment channel, and the sign of the amount — never the amount itself, the date, or an account id. The scrub is pattern-based and best-effort. It strips the shapes it recognizes — emails, phone-shaped numbers, dates written `MM/DD`, `#`-prefixed numbers and starred tokens, a trailing `City, ST` or zip code, an all-caps city followed by a two-letter state code and any bare state code, the recipient after `PAYMENT`, `ZELLE`, `VENMO`, or `CASHAPP` `TO`/`FROM`, standalone runs of three to five digits, and a trailing reference number — and anything else reaches the model as written: a run of six or more digits mid-description, a title-case city mid-description, a recipient after any other provider's name. The [what the AI provider sees](what-the-ai-sees.md#not-masked-stated-plainly) guide is the full statement. `transactions_categorize_commit` writes what you approve.
3. **Auto-rule learning.** Your own edits become proposed rules; accept them and the next import categorizes itself.

The [categorization guide](categorization.md) has the precedence ladder, the bulk-commit hazard, and the migration path for categories curated in another tool.

## 8. Ask your AI assistant

One command writes MoneyBin into a client's MCP config, embedding the active profile. It asks before writing; `--print` shows the entry without writing it, and `--client` takes any of the eight supported clients.

```bash
uv run moneybin mcp install --client claude-desktop
```

Restart the client fully, then ask in your own words:

- *"What's my net worth right now?"*
- *"What did I spend at Whole Foods in January?"*
- *"Show me the SQL behind that number."*

The assistant calls the same catalog the CLI reads — `reports`, `transactions`, `accounts`, `sql_query`, and 46 other tools — over local stdio. An answer that came through `sql_query` or a SQL-backed report is a query you can rerun with `moneybin sql query`; the net-worth answer is the exception, because `core:networth` runs through a service, and `reports explain core:networth` shows its lineage rather than SQL. Tools that write are flagged as such to the client, and the ones that delete or merge ask for MoneyBin's own exact confirmation; read a prompt before approving it. The [Claude Desktop guide](setting-up-claude-desktop.md) is the happy path, the [MCP clients guide](mcp-clients.md) covers the other seven clients and carries the troubleshooting table, and [What the AI provider sees](what-the-ai-sees.md) states exactly what leaves the machine.

## Next

- [Data import](data-import.md) — more formats, the watched inbox, migrating from Tiller, Mint, or YNAB.
- [Reports](reports.md) — the eight built-in reports and your own.
- [Categorization](categorization.md) — rules, merchant mappings, LLM assist.
- [Database and security](database-security.md) — backups, passphrase mode, and [env-var key injection](database-security.md#headless-and-cron-deployments) for a NAS, a container, or cron.
- [Direct SQL access](sql-access.md) — the encrypted file from DuckDB's own CLI or UI.
