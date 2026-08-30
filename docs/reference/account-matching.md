<!-- Last reviewed: 2026-07-24 -->
# Account Matching

One real-world account shows up as many records — a QFX statement this month, a
CSV export the next, a Plaid feed later, the same account from two banks' tools.
MoneyBin collapses those into **one canonical account** so balances and
transactions don't double-count. This page explains how it decides two records
are the same account, **what information it uses and where each piece comes from
per file format**, and the moments it stops to ask you.

If you just want to get files in, start with [Data Import](../guides/data-import.md);
this page is the "how does it actually decide?" companion.

## The canonical account

Every account gets an **opaque, minted `account_id`** — a 12-character handle
like `fc238e82f883`, never your bank's account number. It is:

- **Stable** — it doesn't change when you rename the account or when a new
  source for it arrives.
- **Safe to share** — no PII, so it's fine in logs, scripts, and agent prompts.
- **The key everything joins on** — reports, `core.fct_transactions`, and the
  MCP tools all reference the canonical id; your real account number is *never*
  the key.

Each source record binds to the canonical account through a per-source link.
Re-importing the same source reuses the same id rather than creating a twin.

## How a source record resolves to an account

On every import and sync, MoneyBin runs a resolution ladder — ordered from the
most reliable signal to the least, stopping at the first rung that fires:

```mermaid
flowchart TD
    A[New source account record] --> B{You named an account?<br/>--account-id / --account-name / binding}
    B -->|Yes| ADOPT[Adopt that account]
    B -->|No| C{Strong key already bound?<br/>same-source key · persistent token · scoped full number}
    C -->|Yes| AUTO[Auto-adopt silently]
    C -->|No| D{Weak match?<br/>same last 4 · fuzzy name}
    D -->|Yes| REVIEW[Propose - you confirm<br/>never an auto-merge]
    D -->|No| F{Did the file name an account?}
    F -->|Yes| MINT[Create it, and say so<br/>in the import result]
    F -->|No| E[Show your accounts as a pick-list<br/>choose one, or mint new]
```

1. **Explicit binding.** You pinned the identity (`--account-id`,
   `--account-name`, `import_confirm(preview_id=..., account_bindings=...)`, or "import into account
   X"). Adopted above all detection.
2. **Strong key → silent auto-adopt.** A stable, upstream-assigned key that's
   already bound to an account: the source's own account key on a same-source
   re-import (including Plaid's `account_id`, scoped to that connection), Plaid's
   `persistent_account_id` where the institution supplies one (this is the key
   that survives a relink, when `account_id` does not), or a full account number
   scoped by its routing/bank id. These are near-certain, so MoneyBin adopts
   silently.
3. **Weak match → always a confirm.** A shared **last 4 digits** — corroborated
   by a shared **institution** when both sides name one, and dropped outright
   when they name two different ones — or a **fuzzy name** match. Weak signals
   collide — two Wells Fargo accounts can both end in `1212` — so MoneyBin
   *proposes* and waits. **It never merges two accounts on a weak signal.**
4. **Nothing matched, but the file named an account.** An OFX `<ACCTID>`, a
   statement's issuer and last four, an account column in a spreadsheet — the
   file states an identity and nothing in your book resembles it. There is no
   second answer to give, so MoneyBin creates the account and **names it in the
   import result** rather than stopping to ask.
5. **Nothing matched, and the file named nothing.** MoneyBin shows a **pick-list
   of your existing accounts** to choose from (or "new"). A bare
   Date/Description/Amount CSV lands here: the only name available is the
   filename, and a guess is a question.

The governing rule is **"magic stays visible":** MoneyBin acts silently only on a
near-certain signal, and surfaces a confirm exactly where its inference could be
wrong. A wrong account *merge* is hard to notice and undo, so the bar for acting
without asking is deliberately high. A new account is the cheap mistake by
comparison — it shows up in `moneybin accounts` and corrects with a rename or a
merge — so rung 4 reports instead of asking. Each created account is listed by
name and id when the import finishes, together with both recoveries:

```console
$ moneybin import files statement.ofx
  Institutions: 1
  Accounts: 1
  Transactions: 2
✅ statement.ofx [ofx] — 2 rows
👀 Created account: SAMPLE BANK checking …1111 (e3a84714695d)
   Rename with 'moneybin accounts set <account_id> --display-name <name>'; if it duplicates an account you already have, 'moneybin accounts links run' proposes the merge.
```

The name is the one `moneybin accounts` will show for that account — MoneyBin
derives it at mint time by the same rules the accounts table is built from, so
the two never disagree. If your file named the account itself — a spreadsheet's
Account column, `--account-name`, or the name your bank shows in its own app —
that name is used, with any account number in it masked and the last four added
beside it (`Vacation Fund …1111`) so two accounts sharing a name stay
distinguishable — unless the name already shows four digits of its own, which
it then keeps unchanged. Otherwise MoneyBin assembles one from the institution and
account-type registries, as in the OFX example above, and reports
`Unnamed account` when it has nothing to assemble from. Rename any of them with
the command above.

The same two fields reach every other surface: `accounts_created` on the
`--output json` per-file rows, on the `import_files` MCP result, and on
`import_confirm`. It carries the id and the name only — never the file's own
account key. An OFX key can be an account number; a PDF key is an opaque
document digest.

## What information is used, per format

What MoneyBin can match on depends entirely on what the file carries. The
**strong key** is the only thing that auto-adopts; **last 4** corroborates and
makes a candidate recognizable but is never a key on its own; **institution** and
**name** feed the weak-match and pick-list rungs.

| Source | Strong key (auto-adopts) | Last 4 (corroborating) | Institution | Name |
|---|---|---|---|---|
| **OFX / QFX / QBO** | `<ACCTID>` scoped by `<BANKID>` | `RIGHT(<ACCTID>, 4)` | `<FID>` lookup, else `<ORG>`, else filename | account type / label |
| **Plaid** | `account_id` (same connection only) | `mask` | `institution_name` | official account name |
| **Tabular — aggregator export** (Tiller, Monarch, …, with account info) | *none* — labels are mutable | parsed from the account-label / `Account #` column | a per-row `Institution` column, or parsed from the label | the account label |
| **Tabular — bare bank export** (Date / Description / Amount only) | *none* | *none* | filename heuristic, or unknown | filename stem (a placeholder) |
| **PDF statement** | identifier proven complete and scoped by a validated routing number; exact document bytes for re-import only | captured suffix or mask | statement issuer / routing number | labelled account/product name when present |

Reading the table precisely requires four caveats:

- **Full numbers stay encrypted and scoped.** A complete identifier from OFX or
  PDF may be retained only as a validated-routing-scoped `full_number` account
  link inside the encrypted database. It never enters the canonical account
  dimension, raw source key, logs, or responses. Partial values remain last-four
  evidence and never auto-adopt.
- **Institution ≠ exporter.** The *exporter* (Tiller, Monarch, a bank's web
  export) decides how the file is parsed. The *institution* is a property of the
  account and comes from row data (an `Institution` column, OFX `<ORG>`, Plaid
  `institution_name`) — MoneyBin never treats the tool name as the institution.
- **OFX matching reads `<FID>` before `<ORG>`.** `<ORG>` is a routing code for
  some issuers — Chase publishes `B1`, Wells Fargo `WF` — so matching resolves
  the institution from the exact `<FID>` against `seeds.institutions`, falling
  back to `<ORG>` only for an unregistered FID. The import-time slug recorded on
  each transaction keeps the older `<ORG>`-first order, because changing it
  would re-key every transaction already imported.
- **Every source resolves to one registry slug before comparison.** Sources
  spell an institution differently — a `<FID>`, a sheet's hand-typed
  `U.S. Bank`, a filename heuristic's `us_bank` — and comparing those spellings
  directly splits a bank from itself, because the registry's slug is curated
  rather than derived (`U.S. Bank` slugifies to `u-s-bank`, never `us_bank`).
  Both sides of every institution comparison are therefore resolved through
  `seeds.institutions` first, matching case- and punctuation-stripped text
  against the registry's slug *and* its display name. An unregistered
  institution keeps its own text, which still compares consistently. When
  several sources merge into one account, a slug the registry resolved outranks
  unresolved text no matter which arrived most recently — otherwise one
  unrecognized spelling in a later spreadsheet would overwrite the canonical
  slug and stop that account matching itself on the next import.
- **A bare bank CSV carries no identity.** Date/Description/Amount alone can't
  tell MoneyBin which account it is, so binding is always explicit — which is why
  the pick-list (rung 5) exists.
- **Plaid's strong key crosses connections where the institution supplies one.**
  Re-authenticating the same bank through Plaid Link issues fresh `account_id`
  tokens, so the connection-scoped key cannot recognize a returning account.
  Plaid's `persistent_account_id` survives that boundary, and MoneyBin adopts on
  it directly (rung 2). Plaid populates it for depository accounts at the three
  institutions that use tokenized account numbers — Chase, PNC, and US Bank. An
  account without it — every credit card, and every account at the other
  institutions — resolves through the weak-match rungs (institution + last 4)
  like any other cross-source twin. Accounts synced before MoneyBin captured the
  field hold no value for it until their next sync.

## When MoneyBin asks you: the import gate

If an import is about to adopt an account you already have on a weak signal, or
the file names no account at all, it **pauses without loading any rows** and
returns an account confirmation. You see the proposed account(s) plus a pick-list
of your existing accounts. Bind it in one command:

```bash
# Adopt an existing account, or mint a distinct new one:
moneybin import confirm <file> --accept --account-binding @0=<account_id|new>

# Or name a brand-new account directly:
moneybin import confirm <file> --accept --account-name "WF Business Checking"
```

`@0` is the first account the confirmation listed, `@1` the second — the gate
prints the label beside each one, and the `.pending.yml` sidecar carries it when
the file came through the inbox. That account's own `source_account_key` works
in the same position. Supply a binding for **every** account the file contains
in that one command — the gate is all-or-nothing.

`import confirm` answers the account gate on every file type. OFX and PDF have
no column mapping to ratify, so `--accept` is a formality there — it satisfies
the command's require-an-action guard. Use this command rather than re-running
`import files`: only `import confirm` archives a file out of the inbox's
`pending/` bucket, so answering a pending file any other way leaves it to be
offered again on the next sync.

**Human vs. agent — the same gate.** Both are gated: rows don't land until the
account is bound, and neither self-accepts a weak match. An agent-driven import
used to pass through instead, minting a provisional account and filing weak
matches for later review; it now stops where you would.

**What the gate does not ask.** A file that names an account nothing in your book
resembles has one possible answer, so it loads and reports the account it created
(rung 4). Stopping there charged one confirm per file on a first import, each
with a single legal answer.

The MCP equivalent is the same propose → confirm loop: `import_files` /
`import_preview` return a confirmation, and `import_confirm(preview_id=..., account_bindings=...)`
ratifies it. Key each binding by the proposal's `proposal_ref` — `@0` is the
file's first source account. An assistant reads `source_account_key` masked
(`****1234`), so the ref is the half of the proposal it can name back.

## Cross-source twins found later: the review queue

When the same account arrives from a second source and only matches *weakly*
(institution + last 4), MoneyBin files a proposal instead of merging:

```bash
moneybin accounts links pending                    # see proposals: provisional account, candidate, signal, last4
moneybin accounts links set <decision_id> --into <account_id>   # accept the merge (asks first)
moneybin accounts links set <decision_id> --into <account_id> --yes  # answer the prompt in advance
moneybin accounts links set <decision_id> --standalone           # keep it as its own account
moneybin accounts links run                        # re-scan existing accounts for twins
```

The agent path uses `reviews(kind="account_links", status="pending")`. Accept with
`identity_links_decide(decisions=[{"kind":"account_link","decision_id":"<id>","decision":"accept","target_id":"<account_id>"}])`
or reject with
`identity_links_decide(decisions=[{"kind":"account_link","decision_id":"<id>","decision":"reject"}])`,
then run `refresh_run(steps=["identity"])`.
The `review` command (`moneybin review --type
account-links --status`) shows the pending count across queues. **You decide every merge**
— MoneyBin won't combine two accounts on a weak signal on its own.

## What happens after a match

- **Duplicates collapse.** Bind a CSV to the same account as its OFX twin and the
  overlapping transactions deduplicate: `core.fct_transactions` keeps one row per
  transaction with `source_count = 2` (both sources contributed). No
  double-counting, and the contributing sources stay recorded for provenance.
- **Your work survives.** Categorizations, notes, tags, and splits persist across
  re-imports — transaction identity is derived from the source row's content, not
  from the (mutable) account id, so re-binding an account never orphans them.

## Re-imports and what's remembered

- **Same source, re-imported → silent.** The source's own account key (or a
  remembered binding) adopts the same account with no prompt. Re-running the exact
  same bare file is matched on its content and adopts without re-asking.
- **A confirmed binding is remembered.** Once you bind a source account, the next
  import of that same account adopts it automatically rather than gating again.
- **A genuinely new file still asks.** A different bare file, or a source whose
  account can't be matched, surfaces the gate again rather than guessing.

## See also

- [Data Import](../guides/data-import.md) — how to import each format, step by step.
- [Data Sources](data-sources.md) — per-source identifiers, formats, and sign conventions.
