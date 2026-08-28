# Feature: Document–Account Binding and Transaction Identity Stability

## Status

`draft` — M1Y

## Goal

Stop `transaction_id` from rotating when a user renames, moves, or re-downloads
a file. Achieve it by separating two things the code currently conflates: the
identity of an **import artifact** (a document) and the identity of an
**account**. A document is identified by its content; an account is identified
by a minted opaque key bound to documents by an explicit human decision.

## Background

- [`transaction-identity-stability.md`](transaction-identity-stability.md) —
  the decision record this spec implements. Read it for the evidence; this
  spec carries only what governs the build.
- [`account-identity-resolution.md`](account-identity-resolution.md) — M1S.
  Establishes `app.account_links` as the native-ref → canonical registry and
  the adopt-or-confirm ladder. This spec extends it; it does not replace it.
- [ADR-015](../decisions/015-transaction-identity-content-derived.md) —
  Accepted. Transaction identity stays content-derived rather than a minted
  surrogate. This spec keeps that decision and repairs its premise.
- [`matching-same-record-dedup.md`](matching-same-record-dedup.md) — owns the
  match engine this spec modifies in R12.

### Two reversals from the decision record

Where the decision record and this spec disagree, this spec governs.

**1. The binding ref is a content digest, not a filename stem.**
`transaction-identity-stability.md` §"The one genuinely open sub-decision"
recommends a filename stem, user-confirmed and scoped by source. Withdrawn: a
filename is not a reliable signal of contents. Exports arrive as
`transactions.csv` and are renamed by hand, so the stem is both unstable and
non-unique.

**2. The changes ship as one slice, not change 1 alone first.**
`transaction-identity-stability.md:614-615` asks "Ship change 1 alone first? It
is free and forward-only" and recommends yes. Withdrawn: change 1 is only
forward-only in isolation. Every other change rotates `transaction_id`, so
shipping them separately means several rotations, several migrations, and
several windows in which curation can orphan. The migration is the atom (§Key
Decisions 3).

## The governing rule

> A name says where a value came from, and no value may ever fall back from one
> name into another.

Every defect below is a place where one word means two things depending on the
data. The requirements are that rule applied to five columns.

## What is true today (verified at `de040465`)

`transaction_id` is `SUBSTRING(SHA256(source_type || '|' || source_origin ||
'|' || source_account_key || '|' || source_transaction_id), 1, 16)` —
`int_transactions__matched.sql:128-134` and `:163-169`.

Three of its four inputs move under ordinary user action:

| Input | Why it moves | Ref |
|---|---|---|
| `source_account_key` | For an unregistered tabular file it is `f"{slugify(file_path.stem) or 'file'}-{digest}"` — filename stem plus a digest of file **bytes**. A rename moves it; so does next month's export. | `import_service.py:947` |
| `source_origin` | Falls back to `slugify(account_name or "unknown")` when no format matches, so passing or omitting `--account-name` re-namespaces identity. | `import_service.py:3222` (MB-147) |
| `source_transaction_id` | The union aliases the tabular *fallback* column into this name (`transaction_id AS source_transaction_id`) and discards the genuinely-native one, so it is the institution's id on some rows and a MoneyBin hash of `date\|amount\|description\|account_id` plus an occurrence index on others. | `raw_tabular_transactions.sql:6`, `int_transactions__unioned.sql:82` |

`source_file` is **already not a path** in most channels, which is why it
cannot serve as a document identity:

| Channel | `source_file` value | Ref |
|---|---|---|
| OFX / tabular | `str(canonical_path)` — a resolved filesystem path | `import_service.py:2044`, `:3518` |
| Plaid | `f"sync_{job_id}"` | `extractors/plaid/extractor.py:322` |
| Google Sheets | `f"gsheet://{spreadsheet_id}/{sheet_gid}"` | `gsheet/adapters/transactions.py:254` |
| Manual | `f"<{source_type}:{format_name}:{actor}>"`, then `NULL` in the union | `import_service.py:1826`, `int_transactions__unioned.sql:76` |

The re-import gate already reached this spec's conclusion and applies it in one
place only. `find_existing_import` matches on the content digest and treats the
path as "a legacy fallback that retires per path"
(`import_log.py:380-395`). `raw.import_log` therefore already carries both
`source_file` and `file_sha256` side by side. This spec generalizes that split.

## Requirements

### Document identity

**R1.** A document's identity is `source_document_key`: a truncated SHA-256 of
the artifact's bytes — the first 16 hex characters, the repo's content-hash
convention (`.claude/rules/identifiers.md`). No filename, path, mtime, size, or caller-supplied value
contributes to it. Two files with identical bytes have one document key
regardless of name or location; one path holding different bytes over time
yields different document keys.

**R2.** `source_document_key` is computed once per import from the same bytes
that are parsed, not by re-opening the path. (OFX already does this —
`import_service.py:1955-1961` hashes before a lossy decode so every channel's
digest describes the same thing.)

**R3.** Re-importing a document whose `source_document_key` is already recorded
against a live batch is idempotent: it is refused without `--force`, and under
`--force` it recomputes identical transaction ids. This is the one case that
may proceed with no human decision.

### Account identity

**R4.** An account key is never derived from document bytes, a filename, a
path, or a display label. For a file with no caller-stated account, the import
**mints** an opaque `source_account_key`.

**R5.** A minted account key is opaque and non-reproducible across databases. A
wiped database re-mints different keys, so re-loading the same source files
into a fresh database yields different transaction ids and no `app.*` curation
survives. That is accepted: wiping the database is a deliberate act, and the
recovery path is a database restore, not a re-import.

Note the precise scope, because it narrows §Key Decisions 1's recomputability
claim. `raw.*` stores the stamped account key, so `transaction_id` stays
recomputable from `raw` — re-running the transform after any derived-layer loss
reproduces every id exactly. What is *not* reproducible is regenerating `raw`
itself from the original files after `raw` is gone. Backups cover that; the
identity model does not.

**R6.** A file with no stated account **never binds silently**. It surfaces an
`account_confirmation` proposal through the existing gate and waits for a human
or ratifying agent decision. A new `source_document_key` may *suggest* an
account from in-file evidence; it never asserts one.

**R7.** On the unpinned path, a file that resolves to a known account stamps
the **remembered** key into `raw`, not a freshly computed one.
`AccountResolver.accepted_native_keys_for_account` (`account_resolver.py:782`)
already answers this and is ordered oldest-decision-first so the first accepted
key wins forever. It has exactly one caller today —
`import_service.py:917`, the `--account-id`-pinned path. Call it from the
unpinned branch too.

R7 fires only once the account is known. Its input is an `account_id`: it
answers "what does this account already call itself here?", never "which
account is this file?". A file that states an account supplies that input and
R7 stamps the remembered key. A file that states nothing supplies nothing, so
R7 cannot fire, and R6's gate is reached on every import of a recurring
export. R17 addresses that population; it is not a gap R7 closes, and an
earlier draft implied it was.

**R8.** `source_origin` for a file matching no registered format is a constant.
No caller flag may move a component of the identity hash. (MB-147.)

**R9.** Manual rows carry no `account_id` in the identity tuple. Manual rows
already hold an immutable `manual_<uuid4>` as `source_transaction_id`; the
account slot becomes a **minted per-account source key**.

An earlier draft also offered the constant `'user'`. That option is withdrawn as
unsafe: `stg_manual__transactions.sql:7-8` reads `t.account_id` as both
`source_account_key` and the link join's `ref_value`, so a constant would bind
every manual account to one `app.account_links` row and silently merge every
hand-entered account into a single account.

**R17.** A file that states no account identity reaches the gate carrying
**ledger evidence**, not a bare pick-list. The tabular gate passes its
extracted rows to `probe_incoming_ledger_overlap` (`ledger_overlap.py:95-176`)
exactly as the PDF channel already does (`import_service.py:4113,4781`), so
every candidate arrives with `matched`, `comparable`, and its date window, and
the candidate list is ordered by that evidence. The leading candidate may be
pre-selected. It stays a suggestion: R6 is unchanged, and a human answers the
gate.

This is a wiring change, not a new heuristic. The probe exists and is
calibrated — "amount within ±3 days matched 345 of 346 rows against the true
twin and 0 of 346 against each of two controls" (`ledger_overlap.py:39-40`) —
and the gate already attaches overlap to every candidate whenever the caller
supplies rows (`import_service.py:2296-2306`). Only the tabular and OFX call
sites omit `incoming_transactions`, which is why no CSV proposal carries
overlap today on *any* signal, not merely the fallback one.

**What it changes.** A second bare CSV has new bytes, so a new
`_bare_account_key`, so it gates again — and because `last_four is None`
forces the review open, it skips both the institution scope and the
25-candidate cap and offers every account in `core.dim_accounts`
(`account_resolver.py:1422-1442`). That completeness is deliberate and stays:
the method's own reasoning is that "a long list is the lesser cost;
institution matches still lead it" (`account_resolver.py:1419-1420`). R17
does not shorten the list — it orders it by evidence, so the likely answer
leads on a measured signal rather than on institution slug.

**What it does not change, deliberately.** Overlap compares amount, currency,
and transaction date within a ±3-day window against `core.fct_transactions`
(`ledger_overlap.py:151-161`), and `comparable` counts only rows falling
inside the existing ledger's span. A **disjoint** export — one file per
statement period, sharing no rows with what is already loaded — therefore has
zero comparable rows and yields no signal at all. That case still confirms
once per file, and no file-derived signal can change it (§Decisions Taken 5).

### Provenance vs. identity

**R10.** `source_document_key` **replaces** `source_file` in every position
where the schema or a query treats it as identity. `source_path` is what is
left over: informational metadata that MUST NOT appear in any join predicate,
equality test, hash input, `PARTITION BY`, or `ORDER BY` affecting a result. It
remains freely displayable and loggable (R11).

This is a substitution, not a rename-and-demote. `source_file` is load-bearing
in two ways that a demotion alone would break:

**It is a PRIMARY KEY component in eight raw tables.** A PK is a uniqueness
constraint that affects results and cannot be NULL, so "informational and
nullable" is not available until the key moves:

| Table | Current primary key | After R10 and R13 |
|---|---|---|
| `raw_tabular_transactions.sql:33` | `(transaction_id, account_id, source_file)` | `(source_row_key, account_id, source_document_key)` |
| `raw_tabular_accounts.sql:19` | `(account_id, source_file)` | `(account_id, source_document_key)` |
| `raw_ofx_transactions.sql:19` | `(source_transaction_id, account_id, source_file)` | `(source_transaction_id, account_id, source_document_key)` |
| `raw_ofx_accounts.sql:14` | `(account_id, source_file, extracted_at)` | `(account_id, source_document_key, extracted_at)` |
| `raw_ofx_balances.sql:16` | `(account_id, statement_end_date, source_file)` | `(account_id, statement_end_date, source_document_key)` |
| `raw_plaid_investment_holdings.sql:27` | `(account_id, security_id, source_origin, source_file)` | `(account_id, security_id, source_origin, source_document_key)` |
| `raw_plaid_investment_holdings_snapshots.sql:22` | `(source_origin, source_file)` | `(source_origin, source_document_key)` |
| `raw_plaid_investment_holding_lots.sql:18` | `(account_id, security_id, source_origin, lot_index, source_file)` | `(account_id, security_id, source_origin, lot_index, source_document_key)` |

Each of these reads "one row per business key **per document**" — the schema
has been asserting document identity all along and spelling it with a path.
That is why the substitution strengthens the model rather than working around
it: the PKs become true statements instead of accidentally-true ones.

**Seven of the eight are a positional swap. `raw_tabular_transactions` is
not**, and the third column above exists so that is stated rather than left to
be inferred. R13 drops `transaction_id` from that table, so a key written by
swapping only `source_file` would name a column that no longer exists.
`source_row_key` takes the vacated slot, because it is what `transaction_id`
held for a tabular row all along. `source_transaction_id` cannot take it: R13
makes that column legitimately NULL for a source that assigns no id, and a PK
component may not be NULL. The OFX key keeps `source_transaction_id` for the
opposite reason: it is already a key component there
(`raw_ofx_transactions.sql:19`), so the column cannot be NULL in that table
today and R13 does not change that. The extractor repairs a colliding `<FITID>`
rather than dropping it (`:18`), which is the same premise stated from the
other side.

**Nine query sites use it as a batch discriminator**, all of which move to
`source_document_key`:

`import_service.py:1862`, `:5026`, `:5257`, `:6112`;
`account_resolver.py:1318`; `doctor_service.py:141`;
`dim_holdings.sql:88`; `gsheet/connection_service.py:566`; and
`import_log.py:410,415` — the last being the legacy path fallback that
`find_existing_import` already documents as retiring, which this spec retires.

**R11.** A filename or path is **not** treated as a sensitive leak vector.
`source_path` keeps `DataClass.RECORD_ID` (`taxonomy.py:692`, `Tier.LOW`,
unmasked), and stays projected by `core.dim_accounts` (`dim_accounts.sql:269`)
and `meta.fct_transaction_provenance:14`.

This is a deliberate decision, recorded so a later reviewer does not "fix" it.
An incoming filename is chosen by the user on their own disk and typed into the
import command; if it embeds an account number, that number was already exposed
where MoneyBin cannot reach. Masking it would cost legibility in every import
error, doctor check, and provenance lookup, in exchange for closing nothing.
The masking rules that matter apply to values MoneyBin *extracts* — account
numbers, balances, descriptions — not to the name of the container they arrived
in.

**R12.** Wherever the match engine needs "did these two rows come from the same
physical import?", it keys on `source_document_key`, not on a path. Two sites:
the Tier 2b blocking guard `a.source_file != b.source_file`
(`scoring.py:284`) and the 1:1 assignment key
`(source_type, source_origin, source_file)` (`assignment.py:88`).

**R13.** `source_transaction_id` means **the source's own transaction id, or
NULL**. It is never synthesized. MoneyBin's own within-document row identity
moves to a separate column, `source_row_key`, and the value the identity hash
actually consumes is named `identity_component`.

This is the fifth fallback, and it is currently invisible because the two
columns already exist and are then collapsed. `raw.tabular_transactions` is
honest at the raw layer: `source_transaction_id` is *"Institution-assigned
unique transaction identifier if present"*, kept beside a distinct
`transaction_id` (`raw_tabular_transactions.sql:20,6`). Staging carries both
(`stg_tabular__transactions.sql:60`). Then `int_transactions__unioned.sql:82`
aliases `transaction_id AS source_transaction_id` — promoting the **fallback**
column into that name and discarding the honest one. Downstream, a
MoneyBin-synthesized surrogate is indistinguishable from an
institution-assigned id.

Plaid's equivalent aliasing at `:119` is *correct* and stays: its raw
`transaction_id` genuinely is Plaid's own id.

Required:

- `source_transaction_id` carries the institution's id where the source
  supplies one, and NULL where it does not. No `COALESCE` may fill it.
- `source_row_key` carries MoneyBin's within-document row identity. It keeps
  today's inputs — transaction date, amount, description, and the source
  account key, with an occurrence index counting repeats of that same content —
  and is therefore **byte-identical to the value `transaction_id` holds today**
  for every row with no native id. That identity is what keeps this change from
  rotating those rows.

  An earlier draft dropped `description` and the account key from this key, and
  justified it by saying the existing hash used `row_number`, raw file order.
  Both were wrong. `row_number` is assigned at `transforms.py:142` and never
  reaches the hash — the call site passes seven arguments and that is not among
  them (`transforms.py:228-236`) — and the index already counts repeats of the
  same content, as the module's own comment states. Dropping the account key
  would also bucket a multi-account file's rows together: `transforms.py:137`
  broadcasts a per-row account key precisely because one file can carry several
  accounts, so two rows alike on date, amount, and description but sitting in
  different accounts would take occurrence 0 and 1, and a reorder between
  exports would swap them. That is the failure this requirement exists to
  prevent. The stale claim traces to the schema comment at
  `raw_tabular_transactions.sql:6`, which was never updated when the scheme
  changed.
- Manual entries have no institution, so neither manual table carries a
  `source_transaction_id`: the minted `manual_<uuid4>` is a row key, and the
  column is renamed `source_row_key` in **both**
  `raw.manual_transactions` (`raw_manual_transactions.sql:6`) and
  `raw.manual_investment_transactions`
  (`raw_manual_investment_transactions.sql:8`), which mints the same value into
  a column of the same name. This resolves R9's conflict with this requirement
  — a MoneyBin-minted value sitting in a column named for the source's own id —
  and it has to resolve it in both places or R14's guard fails on day one
  against the table an earlier draft did not name.
- The identity hash consumes `source_transaction_id` when present and
  `source_row_key` otherwise. That selection happens **once**, explicitly, in
  `int_transactions__matched`, under the name `identity_component` — never by a
  fallback hidden inside a column's meaning.
- Staging's tabular dedup groups on that same expression.
  `stg_tabular__transactions.sql:35` partitions on `(transaction_id,
  account_id)` today and on `(COALESCE(source_transaction_id, source_row_key),
  account_id)` after this requirement, so staging and core agree by
  construction about what one row is. This is a grouping key, not a second
  selection, and it is the only other place the pair may appear; a guard pins
  the count at two. The two forms that are wrong, and why, are in *Files to
  Modify*.
- `app.match_decisions.source_transaction_id_a/_b` are renamed
  `identity_component_a/_b`, because that is what they have always held. They
  stay `NOT NULL`: `identity_component` is never NULL, so the matcher's node
  identity and its group-recovery join keep working unchanged
  (`int_transactions__matched.sql:130,209`).

The payoff is that "does this source give us stable ids?" becomes a question
the data answers. Today it cannot be asked: every row has a
`source_transaction_id` whether or not the institution supplied one.

**What rotates, and what does not.** Tabular rows carrying a native id rotate,
and only those: their identity component is `{account_key}:{native_id}` today
(`transforms.py:637`) and becomes the bare native id, matching what OFX and
Plaid already feed into that position. One hash slot stops meaning two
different things by channel. Rows with no native id do not rotate at all,
because `source_row_key` reproduces their existing value byte for byte.

### Invariants (testable as refusals)

**R14.** No `COALESCE`, `IFNULL`, or `CASE` may place a `source_account_key`
into an `account_id` column, a `display_name` into a `source_account_key`, a
`source_path` into any key, or a MoneyBin-derived value into
`source_transaction_id`. A source-scan guard enforces this across
`src/moneybin/sqlmesh/models/`, paired with a behavioural test that a resolved
account never carries a source-native value in `account_id`.

**R15.** `core.dim_accounts.account_id` holds only minted surrogates, making
its existing `DataClass.RECORD_ID` classification unconditionally true rather
than true-only-where-a-link-exists.

R4–R8 alone do **not** achieve this, and an earlier draft claimed they did. R4
mints a `source_account_key`, which still lands in `raw.*.account_id` and is
still passed through `COALESCE(links.account_id, <source>.account_id) AS
account_id` in **thirteen** prep models —
`stg_{ofx,tabular,plaid,manual}__transactions`,
`stg_{ofx,tabular,plaid}__accounts`, `stg_{ofx,plaid}__balances`, and the four
Plaid investment models (`stg_plaid__investment_holdings`,
`stg_plaid__investment_transactions`, `stg_plaid__opening_lots`,
`stg_plaid__opening_lot_review`). An unlinked account would keep publishing a
source-native value in `account_id`, which is precisely what R14's guard
forbids, so the guard would fail against unmodified files the day it lands.
Two of the investment models fall back to `source_account_key` **by name** —
the most explicit form of the defect, and the reason an earlier count of nine
was wrong rather than merely incomplete.

Removing the fallback requires that every account be linked, which R6's
always-propose rule delivers **for new imports**: a file that reaches the
confirm gate produces an `app.account_links` row before load. It does not cover
a link the user later reverses, or one still pending — the join matches only
`status = 'accepted'`. Those rows keep a NULL `account_id` instead of a
source-native one.

**The join therefore stays a `LEFT JOIN`.** Converting it to an inner join
would delete the row instead, which reads as a stricter contract and is the
opposite: `fct_transactions_fk_integrity` finds unresolvable accounts by
looking for them in `core`, so removing them upstream makes that audit pass by
destroying its evidence. A NULL is visible; a deleted row is not. A standalone
audit, `fct_transactions_account_linkage`, names the case so `moneybin doctor`
reports it — standalone, so a reversed link surfaces a finding rather than
halting every transform.

The thirteen models must still drop the fallback arm — otherwise a dead branch
keeps the ambiguity alive in the code and in `taxonomy.py`'s reasoning about
the column.

**R16.** `moneybin doctor` detects orphaned rows in all five `app.*` tables
hard-coupled to `transaction_id`, not the two it covers today
(`doctor_service.py:713`). V052 deliberately preserves no `app.*` state, and no
foreign key exists anywhere in `app.*` to complain, so doctor is the only
surface that can tell a user what the migration and re-import cost them. It
ships **with** the migration, not after it.

## Data Model

### New column

`source_document_key VARCHAR NOT NULL` on every `raw.*` transaction and account
table that today carries `source_file`, and on `raw.import_log`. `NOT NULL`
because it takes `source_file`'s place in eight primary keys (R10).

**Truncation: 16 hex characters**, matching `transaction_id` and
`migrations.py:35`'s documented 64-bit content-hash convention, rather than
`_bare_account_key`'s 12. The full digest stays in
`raw.import_log.file_sha256`, which the key is a prefix of **for file-backed
channels only** — Plaid, Google Sheets, and manual have no `file_sha256`, so
the two columns coexist rather than agree everywhere.

For channels with no file, the document key hashes the batch's existing stable
token, so the column holds one kind of value everywhere:

| Channel | Document key input |
|---|---|
| OFX / tabular / PDF | file bytes |
| Plaid | `sync_{job_id}` |
| Google Sheets | `gsheet://{spreadsheet_id}/{sheet_gid}` plus the pull's row digest |
| Manual | the minting batch token |

`raw.import_log.file_sha256` is the existing full digest and stays, retaining
its role in `find_existing_import`.

### Second new column

`source_row_key VARCHAR` on `raw.tabular_transactions` and any other raw
transaction table whose source may not supply a native id. Carries MoneyBin's
within-document row identity (R13). `source_transaction_id` becomes nullable
wherever it is not already, since NULL is now its honest answer for a source
that assigns no id.

The existing `raw.tabular_transactions.transaction_id` column — the
native-or-synthesized fallback — is **dropped**. Its two jobs are now the two
columns above, and leaving it would preserve the ambiguity this spec exists to
remove.

### Renamed column

`source_file` → `source_path`, nullable, informational — but only **after**
`source_document_key` has taken its place in the eight primary keys listed in
R10. Until that swap lands the column cannot be nullable, so the two changes
are one step, not two.

Retained rather than dropped: it is the only human-legible pointer back to the
artifact on disk, and dropping it would make a failed import unexplainable. For
the three channels whose `source_file` is already a synthetic batch token
rather than a path (Plaid, Google Sheets, manual), `source_path` becomes
redundant with `source_document_key` and may be left NULL.

Its privacy class is unchanged (R11).

### Unchanged

`app.account_links` needs no new `ref_kind`. A minted opaque account key is a
`source_native` ref, so the existing CHECK constraint
`('source_native', 'persistent_token', 'full_number')`
(`app_account_links.sql`) holds and no constraint migration is required.

### Migration

`V052__separate_document_and_account_identity.py`. One migration, because the
migration is the atom: R4, R7, R8, R9, and R13 each rotate `transaction_id`.

**Posture: schema only, no data preservation.** The migration changes shape and
recreates the re-derivable raw tables empty. It does not backfill identity
values, mint replacement account keys, or rewrite the `transaction_id` held in
`app.*`. Recovery is a re-import, which regenerates every `raw.*` identity
correctly by construction — a fresh import is the definition of the right
answer here, so reproducing it inside a migration is duplicated logic that can
only diverge.

This is a deliberate pre-launch trade. A data-preserving migration costs more
to write, review, and verify than a re-import costs to run, and the repo is
pre-launch with no user whose curation cannot be rebuilt. The migration is
written **to be deleted wholesale** in a future pre-release schema reset rather
than maintained: it carries no backfill or value-rewriting logic to unwind, and
nothing else may depend on having run it.

**"No data preservation" means no backfill, not no data.** The distinction is
load-bearing and is the one this migration is most likely to get wrong.
Clearing a table a re-import refills is free. Clearing a table nothing outside
the database can reproduce is permanent loss wearing the same syntax. The rule
is therefore a single test applied per table, not a blanket `DROP` over the
`raw` schema: **clear it when a re-import regenerates it by construction;
preserve it when nothing outside the database can.**

The migration must:

1. Recreate the eight `raw.*` tables in R10's table with the new shape —
   `source_document_key` and `source_row_key` added, `source_file` renamed to
   `source_path` and nullable, and the new primary key R10's table names for
   each. They are recreated **empty**; DuckDB cannot `ADD` or `DROP` a primary
   key, so each is a create-drop-rename regardless, and carrying rows across
   would require backfilling a `NOT NULL` key component that only a re-import
   can compute.
2. Clear the remaining re-derivable `raw.*` tables, which hold no identity the
   new shape changes but would otherwise carry a document key nothing can fill:
   `raw.plaid_*`, `raw.ofx_institutions`, `raw.pdf_seeds`, `raw.gsheet_seeds`,
   and `raw.import_preview_snapshots` — the last holding staged bytes that are
   deleted on consumption anyway.
3. Preserve the five `raw.*` tables a re-import does not rebuild. The list is
   enumerated here rather than left to a predicate, because the failure mode is
   a list that looks complete and is short by one:

   | Table | Why a re-import does not rebuild it |
   |---|---|
   | `raw.manual_transactions` | The user typed the rows. |
   | `raw.manual_investment_transactions` | The same, for investment events — the second manual table, easily missed because only the first is named elsewhere in this spec (`raw_manual_investment_transactions.sql:1-6`). |
   | `raw.exchange_rates` | Append-only by design: "a rate a provider published for a date is a historical fact, so a refetch never rewrites one." A refetch is also bounded by the provider's history window, so what falls outside it is simply gone. |
   | `raw.security_prices` | Append-only for the same reason — "a historical close is an immutable fact" — and the schema says so precisely to contrast with `raw.plaid_securities`, whose close price is overwritten on every pull and therefore *cannot* carry a history. |
   | `raw.import_log` | The batch parent of all four above: dropping it dangles their `import_id`. It is also the audit record of every import ever run, which a re-import appends to rather than reconstructs. |

4. On both manual tables, rename the minted `source_transaction_id` to
   `source_row_key` (R13). It stays the primary key and no value changes.
5. NULL the stored gold-key predictions on those two tables —
   `raw.manual_transactions.transaction_id` and
   `raw.manual_investment_transactions.investment_transaction_id`. Each was
   computed at INSERT from inputs that R9 and R13 rotate, so each is stale the
   moment this migration runs. NULL is deliberate rather than a recomputation:
   the prediction exists so `_run_orphan_app_state` can suppress a false
   positive on a row not yet materialized in `core`
   (`doctor_service.py:754-763` unions it into `valid_txn`), and a *stale*
   prediction makes that suppression hide a real orphan. NULL makes doctor
   report instead — the failure that is visible rather than the one that is
   silent. The column repopulates on the next entry, and the preserved rows
   stop being reported the moment a transform materializes them into `core`.
6. On `raw.import_log`, rename `source_file` to `source_path` and relax it to
   nullable, then add `source_document_key` as a **nullable** column, filled by
   truncating `file_sha256` where one exists. That is a derivation, not a
   backfill: R1's key is exactly that truncation of exactly those bytes. It is
   nullable because this table's key is `import_id` and the document key is
   informational here; NULL for a pre-V046 batch is the honest answer, and
   minting a stand-in would put a value into a column that names bytes nobody
   hashed.
7. Rename `app.match_decisions.source_transaction_id_a/_b` to
   `identity_component_a/_b`. Values are kept as-is: decisions on rows that do
   not rotate re-anchor on the next import, and decisions on tabular native-id
   rows orphan.
8. Log, at INFO, that a re-import is required, and that `moneybin doctor` lists
   whatever `app.*` curation no longer resolves (R16).

**The one thing a re-import cannot rebuild** that this migration does not
preserve is a Plaid batch older than the API's available history window. A user
in that position takes `moneybin db backup` before upgrading. Everything else
either re-imports from files the user still holds or is named in step 3.

**What the thin migration is betting on**, and the reason it is safe rather
than merely cheap: `app.*` is untouched, so `app.account_links` still holds
each account's accepted key. R7 stamps that remembered key on re-import, so the
transaction ids come back identical and the curation pointing at them is still
anchored. Rotation is confined to accounts that never had an accepted link.
That is a claim a test has to hold down, not an assumption — Testing Strategy
item 18.

## Implementation Plan

### Files to Create

- `src/moneybin/sql/migrations/V052__separate_document_and_account_identity.py`
- A source-scan guard for R14 plus its behavioural partner.

### Files to Modify

**Identity derivation**

- `src/moneybin/services/import_service.py` — split `_bare_account_key`
  (`:947`) into a document-key function and a minting account-key function;
  call `accepted_native_keys_for_account` from the unpinned branch (R7); pin
  `source_origin` (R8); stop seeding any key path from the filename stem
  (`:3302`, `:3408` keep the stem as a *suggestion* only).
- `src/moneybin/services/account_resolver.py` — confirm
  `accepted_native_keys_for_account` needs no scoping change for its new
  unpinned caller.
- `src/moneybin/extractors/tabular/schema/raw_tabular_transactions.sql` — R13:
  add `source_row_key`, drop the `transaction_id` fallback column.
- `src/moneybin/sqlmesh/models/prep/int_transactions__unioned.sql:82` — stop
  aliasing the fallback column into `source_transaction_id`; carry the honest
  `source_transaction_id` and `source_row_key` separately. `:119` (Plaid) is
  correct and stays.
- `src/moneybin/sqlmesh/models/prep/int_transactions__matched.sql` — the
  identity hash selects `source_transaction_id` when present and
  `source_row_key` otherwise, explicitly and in one place.

**Deletions**

- `rekey_bare_proposals_for_path` (`import_service.py:950-968`), its call site
  (`inbox_service.py:779`), its import (`inbox_service.py:43`), and its tests
  (`test_inbox_service.py:1835`, `test_import_binding.py:1089`). The function
  exists solely to repair the account key after the inbox appends a collision
  suffix (`statement.csv` → `statement-1.csv`). With no stem in the key there
  is nothing to repair.

**Matching**

- `src/moneybin/matching/scoring.py:284` — Tier 2b guard to
  `source_document_key`.
- `src/moneybin/matching/assignment.py:88,173,191,199-206` — physical-import
  key to `source_document_key`.

**Raw schemas** — the eight files in R10's table, each taking the new primary
key that table names. Seven are a positional swap;
`raw_tabular_transactions.sql` is not, because R13 also drops `transaction_id`
from it.

- `src/moneybin/sql/schema/raw_manual_investment_transactions.sql` — R13's
  `source_transaction_id` → `source_row_key` rename, the same one
  `raw.manual_transactions` takes.
- `src/moneybin/sql/schema/raw_import_log.sql` — `source_file` → `source_path`,
  relaxed to nullable, plus a nullable `source_document_key` (§Migration step
  6). Its own key is `import_id` and does not move.

**The nine query sites** in R10 that use `source_file` as a batch
discriminator.

**The thirteen `COALESCE` prep models** in R15, each dropping the fallback arm:
`stg_ofx__transactions.sql:102`, `stg_tabular__transactions.sql:45`,
`stg_plaid__transactions.sql:7`, `stg_manual__transactions.sql:7`,
`stg_ofx__accounts.sql:25`, `stg_tabular__accounts.sql:7`,
`stg_plaid__accounts.sql:29`, `stg_ofx__balances.sql:7`,
`stg_plaid__balances.sql:7`, `stg_plaid__investment_holdings.sql:18`,
`stg_plaid__investment_transactions.sql:241`,
`stg_plaid__opening_lots.sql:257`, `stg_plaid__opening_lot_review.sql:107`.

**`stg_tabular__transactions.sql:35`** — the staging dedup partition
`ROW_NUMBER() OVER (PARTITION BY transaction_id, account_id …)` names the
`transaction_id` column R13 drops. It repartitions on
`(COALESCE(source_transaction_id, source_row_key), account_id)`, the expression
R13 selects as `identity_component`. `account_id` here is the source-native key
read from `raw.tabular_transactions`; the resolved id is not in scope inside the
CTE, and that is unchanged.

Three other forms are wrong:

- **Adding `source_document_key` to the key.** This partition exists to *ignore*
  which file a row arrived in — that is what collapses the February rows of a
  Jan–Mar export and a Feb–Apr export into one. Adding the document puts the two
  copies in separate partitions, so both reach `core` carrying the same
  `transaction_id`. Testing Strategy item 3 is exactly this case.
- **`source_row_key` alone.** It fixes the duplicate above, but the row key is
  derived from the row's content, so a bank that restates a description between
  exports (`PENDING - AMAZON` → `AMAZON MKTPLACE`) changes it and the two copies
  stop grouping. The institution's own id survives that restatement, which is
  why it must win where it exists.
- **`source_transaction_id` alone.** After R13 that column is legitimately NULL
  for every no-native-id source, which would collapse an entire document into
  one partition and delete all but one row. The `COALESCE` is what makes NULL
  safe.

**Other models** — the remainder of the 22 referencing `source_file`, of which
the load-bearing ones are `int_transactions__unioned.sql`,
`int_transactions__matched.sql`, `stg_plaid__investment_holdings.sql`,
`int_plaid__opening_positions.sql`, `stg_plaid__opening_lots.sql`,
`core/dim_accounts.sql`, `core/dim_holdings.sql`, and
`meta/fct_transaction_provenance.sql`.

**Privacy**

- `src/moneybin/privacy/taxonomy.py:692` — the sole `source_file` entry,
  renamed to `source_path` with its class unchanged (R11). New entries are
  needed wherever `source_document_key` and `source_row_key` become
  publishable; both are `RECORD_ID`.

### Key Decisions

1. **Derived, not minted, transaction ids.** ADR-015 stands. The premise it
   rested on — that `source_account_key` is a stable immutable stand-in — was
   false; this spec makes it true rather than replacing the model. Minting
   transaction ids relocates the identity problem one level down to the lookup
   key and forfeits recomputability from raw files.
2. **Explicit human binding, not inference.** A document key can suggest an
   account. It cannot assert one. This is "magic stays visible" applied at the
   point the inference is weakest, and it is why R6 is a refusal rather than a
   confidence threshold.
3. **One slice, because the migration is the atom.** See §Migration.
4. **Keep `source_path`.** Informational, never load-bearing.

## CLI Interface

No new commands. Behavioural changes:

- `moneybin import <file>` with no `--account-id` and no account column now
  reaches the existing `account_confirmation` gate rather than silently
  minting a bytes-derived key. The gate itself already ships.
- Re-importing a renamed or moved file is recognized as the document it already
  is, and is refused as a duplicate rather than imported as a new account.
- Re-importing from a reused path with new contents is recognized as a new
  document.
- The gate for a file that states no account ranks its candidates by ledger
  overlap and shows that evidence, instead of listing every account in slug
  order (R17). The list is unchanged in length.

`--account-name` is unchanged by this spec; its decomposition is Out of Scope.

## MCP Interface

No new tools. `import_files` / `import_preview` / `import_confirm` response
envelopes gain `source_document_key` wherever they carry `source_file` today,
and `source_file` is renamed in those payloads. Because the repo is pre-launch
these are breaking envelope changes taken deliberately at the cheapest moment,
per `.claude/rules/design-principles.md`.

## Observability

Per `docs/specs/observability.md`, registered in
`src/moneybin/metrics/registry.py`:

| Metric | Type | Labels | Why |
|---|---|---|---|
| `import_document_rebinds_total` | Counter | `source_type`, `outcome` | How often a known document key resolves to an account vs. reaches the confirm gate. Measures whether R6's confirm burden is once-per-import or worse. |
| `import_account_keys_minted_total` | Counter | `source_type` | A rise without a matching import rise means keys are still churning. |
| `import_remembered_key_reuse_total` | Counter | `source_type`, `hit` | Directly measures R7, on the population R7 can serve — files that state an account. A `hit=false` rate near 1.0 there means the unpinned path is not reaching the resolver. It is silent on identity-unknown files, which have no account to look up. |
| `import_overlap_evidence_total` | Counter | `source_type`, `verdict` | R17's evidence on the identity-unknown path: `decisive` (one account, no near runner-up), `ambiguous`, or `absent` (a disjoint export). This is what §Open Questions turns on, and the only measurement that can answer it. |
| `matching_pairs_blocked_total` | Counter | `tier`, `reason` | R12 changes what Tier 2b can see; this makes the change observable rather than inferred. |

## Testing Strategy

**Identity stability** — the tests that would have caught this:

1. Import a file; rename it; import again → refused as a duplicate, and no new
   `transaction_id` exists.
2. Import a file; move it to another directory; import again → same.
3. Import an export covering Jan–Mar; import a second export covering Feb–Apr
   **written to the same path** → Feb and March rows carry the transaction ids
   they already had, and April's are new. This is the case the current Tier 2b
   guard is structurally blind to.
4. Import the same bytes under `--force` → identical transaction ids.
5. Pass `--account-name` and omit it across two imports of one file → identical
   transaction ids (R8).

**Binding**

6. A tabular file with no account signal reaches the `account_confirmation`
   gate and loads nothing until a decision (R6).
7. An agent path cannot self-accept that gate.
8. Two different-account files that happen to share a name do not collide.

**Source id honesty (R13)**

9. Import a file whose source assigns no transaction id → every row's
   `source_transaction_id` is NULL, and `source_row_key` is populated.
10. Import a file that *does* carry a native id column → `source_transaction_id`
    holds the institution's value verbatim, and the identity hash consumes it
    rather than `source_row_key`.
11. A file with a native id keeps its transaction ids when rows are reordered;
    a file without one does not (and relies on R3's document-key idempotence
    instead). This asserts the two paths are genuinely distinct rather than one
    path with a hidden fallback.

**Invariants**

12. Source-scan guard: no model places a source-native value into an
    `account_id`, and no `COALESCE` fills `source_transaction_id` (R14), with a
    behavioural partner asserting the same at runtime — a source scan alone
    cannot see a Python-side fallback.
13. `source_path` appears in no join, equality test, or hash input (R10).

**Migration**

14. Every re-derivable `raw.*` table is empty afterwards **and** each of the
    five preserved tables still holds every row it held before. Both halves
    are the test: an over-broad clear is silent data loss, and a missed clear
    leaves rows carrying a `NOT NULL` document key that nothing can fill. A
    fixture that seeds only the tables being cleared passes this vacuously,
    so it seeds all thirteen.
15. Partial-failure: an interrupted migration leaves the database on the old
    schema, not half-rotated.
16. The stored gold-key prediction is NULL on every preserved manual row of
    both manual tables, and `moneybin doctor` therefore reports those rows
    rather than suppressing them, until a transform materializes them into
    `core`.
17. `raw.import_log` keeps every batch row. `source_document_key` equals the
    truncation of `file_sha256` wherever that column is populated, and NULL
    for a pre-V046 batch — no batch acquires a minted stand-in.
18. A re-import after the migration reproduces the transaction ids that
    `app.*` curation already points at, for every account carrying an accepted
    `app.account_links` row (after the gate decision, for a file that states
    no account). This is the test that makes the thin migration safe rather
    than merely cheap: it asserts R7 does the work the migration declined to
    do. Its negative partner is an account with no accepted link, whose ids
    *do* rotate and whose curation doctor then lists.

**Binding evidence (R17)**

19. A second file from a recurring **overlapping** export reaches the gate
    with its true account ranked first, carrying a non-zero `overlap_matched`.
20. A file overlapping nothing reaches the gate with no overlap evidence and
    still loads nothing. Absence of a signal is not itself a signal and must
    not promote a candidate.
21. A tabular gate and a PDF gate over the same ledger produce the same
    overlap numbers. R17 is a wiring change; a divergence means the two
    channels have forked.

## Synthetic Data Requirements

The generator needs a **recurring export series**: three files for one account
covering overlapping date windows, with rows appended at the top in one variant
and at the bottom in another, so the occurrence-index component of R13 is exercised
in both directions. Ground truth is the set of distinct transactions across the
series, so a test can assert the count after importing all three.

The series is needed in **two flavours** — one carrying a native
transaction-id column and one without — because R13 makes those genuinely
different code paths rather than one path with a fallback. The with-id flavour
should survive row reordering; the without-id flavour should not, and tests 9
through 11 assert exactly that asymmetry.

Also needed: two files for *different* accounts sharing one filename, to
exercise test 8.

## Migration and On-Disk Impact

**This breaks existing databases and requires V052 to be run, followed by a
re-import.** The migration recreates the re-derivable `raw.*` tables empty
(§Migration), so every derived layer rebuilds from the re-imported sources.

**Expect `app.*` curation to orphan broadly, and treat that as the cost of the
posture rather than a defect.** Two things rotate `transaction_id`
independently: R13 rotates tabular rows carrying a native id, and R4 rotates
any row whose account slot held a bytes-derived key. How much the second one
touches on re-import depends on whether the resolver reuses the account key
remembered in `app.account_links` (R7) or mints a new one — `app.account_links`
survives the migration, so R7's remembered-binding path is what decides it.
**Verify this on a real database before relying on either answer**; it is the
difference between a handful of orphans and all of them.

Five `app.*` tables are hard-coupled to `transaction_id`:
`transaction_categories` (PK), `transaction_tags` (PK), `transaction_notes`
(`NOT NULL` + index), `transaction_splits` (`NOT NULL` + index), and
`categorization_decisions` (`UNIQUE (transaction_id, attempt_number)`). The
migration does not rewrite them. `app.match_decisions` is **not** insulated as
an earlier draft claimed: it anchors rows by the value the union called
`source_transaction_id`, both anchor columns are `NOT NULL`
(`app_match_decisions.sql:4,7`), and R13 renames them to
`identity_component_a/_b`. Of the rest, `lot_selections` follows a separate
`investment_transaction_id` lineage, `proposed_rules.sample_txn_ids` is
illustrative, and `audit_log.target_id` is a historical record where a stale id
is arguably correct.

**No foreign key is declared anywhere in `app.*`** — `FOREIGN KEY` and
`REFERENCES` appear in none of the schema files. Nothing cascades and nothing
errors, so orphaned curation is silent unless something goes looking.

**That makes the doctor gap load-bearing, not optional.**
`_run_orphan_app_state` (`doctor_service.py:713`) checks `transaction_notes`
and `transaction_tags` only. `transaction_categories`, `transaction_splits`,
and `categorization_decisions` have no orphan detection. Under a migration that
deliberately preserves nothing, doctor is the *only* thing that will tell a
user what they lost — so R16 closing that gap is a prerequisite of this
migration shipping, not a follow-up to it.

Recovery, if the migration is abandoned partway: restore from `moneybin db
backup`. The migration is not designed to be reversible in place, and is not
worth making so — see the collapsibility note in §Migration.

## Dependencies

No new packages. Prerequisite: none — `app.account_links`,
`accepted_native_keys_for_account`, and the `account_confirmation` gate all
ship today.

## Out of Scope

- **The Part 1 vocabulary work** — `nickname`, the `--account-name`
  decomposition, and the `reports.*.account_name` rename. Public-contract
  changes with their own review surface.
  ([`transaction-identity-stability.md`](transaction-identity-stability.md)
  Part 1.)
- **Tier 2b's silent-action posture.** `_classify_pair` returns `None` rather
  than `pending` for a within-source pair below the bar
  (`engine.py:294-307`), so an uncertain pair is dropped instead of reviewed. A
  blank description scores `0.30 × 1.0 + 0.70 × 0.0 = 0.30` and vanishes. That
  is a matcher-posture decision reaching well beyond re-imports.
- **`_pdf_alias`** (`import_service.py:989`) still builds `raw.pdf_<alias>`
  view names from the filename stem. A naming surface, not identity — but a
  rename does create a second view.
- **Promoting a recurring export to a registered format** at the moment the
  system notices a second import of the same shape. A good UX prompt and a
  natural successor; not required for identity stability. It is also where a
  human-declared import route would live — the only mechanism that could ever
  auto-confirm a *disjoint* recurring export (§Decisions Taken 5). A safe one
  needs a user-managed coordinate that does not exist yet, so it is not
  smuggled into this milestone.

## Decisions Taken

Recorded here because each was a live question during design, and a later
reader would otherwise reopen it.

1. **Filenames are not a sensitive leak vector** (R11). Treating an incoming
   filename as sensitive would cost legibility everywhere and close nothing: a
   name that embeds an account number was already exposed on the user's own
   disk before MoneyBin saw it. `source_path` stays unmasked.
2. **The source's transaction id stays independent of MoneyBin's** (R13).
   Whether a given institution supplies one is unknown and varies by source, so
   the design must not depend on the answer. Keeping `source_transaction_id`
   honest — the institution's value or NULL — and putting MoneyBin's row
   identity in `source_row_key` makes both cases explicit, instead of hiding the
   difference behind one column that means whichever the data happened to
   allow.
3. **The separation ships as a rotation, not as a data-preserving migration**
   (R13, §Migration). The alternative kept the existing never-null column and
   added an honest one beside it, rotating nothing — but it would have left one
   hash slot meaning `{account_key}:{id}` for tabular and the bare id for OFX
   and Plaid, permanently, and a per-channel exception in an identity contract
   is exactly what stops being fixable after launch. Pre-launch is the cheapest
   moment to end it, so the rotation is taken now. The migration that carries
   it is kept deliberately thin — schema only, re-import to recover, written to
   be deleted in a later pre-release reset — because a fresh import already
   computes the right answer, and reproducing that inside a migration is
   duplicated logic that can only diverge from it.
4. **An unlinked account yields a NULL, not a dropped row** (R15). Removing
   the `COALESCE` leaves one case unnamed: an account whose link is pending or
   reversed. Dropping those rows would make the data look clean and make
   `fct_transactions_fk_integrity` pass for the wrong reason, because that
   audit detects the condition by finding the rows in `core`. The `LEFT JOIN`
   stays, the rows keep a NULL `account_id`, and a standalone audit reports
   them.
5. **No file-derived signal can auto-confirm a file that states no account**
   (R6, R17). Auto-confirm needs a reference that *recurs* across imports of
   one account and does not *collide* across accounts. Every candidate the
   file itself offers fails one of the two. A content digest is unique but
   does not recur — "this key is the file's bytes, and a recurring export's
   bytes change every period" (`import_service.py:3322-3324`). A filename stem
   recurs but collides, because `statement.csv` is universal. A column
   signature recurs but collides, because two accounts at one institution
   export identically. The pinned path reached the same conclusion first:
   "No file-derived key escapes this: a content hash breaks when the file
   grows, a filename breaks when it is renamed" (`import_service.py:2392-2393`).

   So the signal has to come from outside the file, and there are exactly two
   sources: the accumulated ledger, which R17 wires up, and a human-declared
   route recorded once. **The route is rejected for this milestone**, and not
   only on scope. No original filesystem path is persisted anywhere.
   `raw.import_log.source_file` stores the path at import time, which under
   the inbox flow is the inbox path and goes stale the moment the file is
   moved (`inbox_service.py:569-574`, `import_log.py:126-143`). The only path
   actually available is therefore the inbox path — and every institution's
   `statement.csv` arrives at the same one, so binding on it would silently
   merge two banks' accounts. That is the exact failure `_bare_account_key`
   was written to prevent (`import_service.py:934-937`). A safe route needs a
   new user-managed coordinate and new plumbing to retain it, which is the
   registered-recurring-export work in §Out of Scope.

6. **A decisive ledger overlap still asks** (R6, R17). R17 stops at
   evidence: the leading candidate is ranked, its match count is shown, and it
   may be pre-selected — but a human answers the gate even when the overlap
   points at exactly one account and nothing else. Binding silently was
   considered and declined.

   The case for it was real, which is why it was put rather than assumed. The
   probe separated a true twin from two controls 345/346 to 0/346, and a
   confirm that is always answered the same way is confirm volume scaling with
   *items* rather than with uncertainty — something `design-principles.md`
   treats as a design failure in its own right, and a live cost here, because a
   recurring export re-asks every period forever.

   Two things outweighed it. This is a **merge onto an existing account**, the
   case `account_resolution_types.py:154-155` names as one where "a wrong merge
   is hard to notice and hard to undo" — the reason the gate exists at all. And
   the probe's own author already assigned it the opposite role: its window is
   deliberately kept off the user-tunable setting because "this one supplies the
   evidence a human ratifies an irreversible whole-ledger merge on"
   (`ledger_overlap.py:50-51`). Using it as grounds for acting *without* that
   human inverts what it was calibrated for.

   **What would reopen this**, and the reason `import_overlap_evidence_total`
   (§Observability) exists: nobody currently knows how often the
   identity-unknown path is `decisive` versus `ambiguous`. One dogfooding cycle
   produces that number. If decisive turns out to be the overwhelming majority,
   the trade changes and this is worth revisiting — but on data, not on the
   asymmetry of a single calibration run. If it is ever taken, the shape is
   narrow: decisive for exactly one account, no near runner-up, and `comparable`
   above a floor so a two-row file proves nothing — and the import result must
   show the binding it made and offer its reversal, because that is what "magic
   stays visible" requires of a silent action.

## Open Questions

One sizing question remains: what fraction of real files carry a
native id column. That changes how often the `source_row_key` path is
exercised, not whether either path is correct — and after R13 it becomes a
question the data can answer, because a NULL `source_transaction_id` will mean
exactly "this source assigns no id."
