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

- **The prior analysis** — an internal account-vocabulary and
  transaction-identity review that produced this spec. It is working material
  rather than a public record, so everything from it that governs the build is
  restated here rather than cited; §"Two reversals" records the two places
  this spec overrides it.
- [`account-identity-resolution.md`](account-identity-resolution.md) — M1S.
  Establishes `app.account_links` as the native-ref → canonical registry and
  the adopt-or-confirm ladder. This spec extends it; it does not replace it.
- [ADR-015](../decisions/015-transaction-identity-content-derived.md) —
  Accepted. Transaction identity stays content-derived rather than a minted
  surrogate. This spec keeps that decision and repairs its premise.
- [`matching-same-record-dedup.md`](matching-same-record-dedup.md) — owns the
  match engine this spec modifies in R12.
- [`moneybin-doctor.md`](moneybin-doctor.md) — owns the check surface this spec
  modifies in R16, which widens orphan detection from two `app.*` tables to six
  and adds `app.sync_recovery_state` as a reported state.

### Two reversals from the prior analysis

Where the prior analysis and this spec disagree, this spec governs. Both
reversals are stated with the position they overturn, so the disagreement is
legible without reading the earlier material.

**1. The *document* key is a content digest, not a filename stem.** The
prior analysis left this as its one genuinely open sub-decision and
recommended a filename stem, user-confirmed and scoped by source. Withdrawn: a
filename is not a reliable signal of contents. Exports arrive as
`transactions.csv` and are renamed by hand, so the stem is both unstable and
non-unique.

This reverses what identifies **a document**, and says nothing about what may
bind **an account**. §Decisions Taken 5 rejects the content digest for that
second job, and the two hold together rather than colliding: a digest is
unique-but-non-recurring, which is exactly right for naming one artifact and
exactly wrong for recognising the same account next period. The earlier
document called both jobs "the binding ref," which is the conflation this spec
exists to remove — so the word is avoided here.

**2. The changes ship as one slice, not change 1 alone first.** The prior
analysis asks "Ship change 1 alone first? It is free and forward-only" and
recommends yes. Withdrawn: that first change is only
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
| OFX | `str(canonical_path)` — a **resolved** filesystem path | `import_service.py:2044` |
| Tabular | `str(file_path)` — the path **as given**, never `.resolve()`d | `import_service.py:3518` |
| PDF | the original filename, **basename only, no path** | `raw_pdf_seeds.sql:9` |
| Plaid | `f"sync_{job_id}"` | `extractors/plaid/extractor.py:322` |
| Google Sheets | `f"gsheet://{spreadsheet_id}/{sheet_gid}"` | `gsheet/adapters/transactions.py:254` |
| Manual | `f"<{source_type}:{format_name}:{actor}>"`, then `NULL` in the union | `import_service.py:1826`, `int_transactions__unioned.sql:76` |

Six channels, one column, four different kinds of value: a synthetic batch
token in three of them, a filesystem path in two — and those two do not even
agree, since OFX canonicalises and tabular does not, so the same file reached
by two paths is one document to OFX and two to tabular. PDF is the sharpest
case of all: a bare basename is neither unique nor stable, and two statements
downloaded a month apart routinely share one.

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

**The account key is recovered by lookup, not recomputation**, and saying so is
load-bearing rather than pedantic: for a document that states no account,
nothing else in this spec can supply it. R4 mints a *non-reproducible* key, so
minting a second time yields a different one and rotates every id. R7 cannot
help either — its input is an `account_id`, and a file that states nothing
supplies none (see R7). What does supply it is the batch this document is
already recorded against: the `raw.*` rows stamped with this
`source_document_key` carry the account key chosen on the first import, and the
`--force` path re-stamps that stored value instead of deriving a new one. R5's
note is the same mechanism seen from the other side — `raw.*` stores the
stamped key, which is exactly what keeps `transaction_id` recomputable.

This is also what makes "no human decision" a replay rather than a silent bind.
The binding was decided once, by a human, at first import; the document key is
what remembers that decision, so a forced re-import repeats it instead of
re-asking. The precondition carries the whole guarantee: if those rows are
gone, the document is no longer recorded against a live batch, R3 does not
apply, and the import returns to R6's gate like any other.

**The replay revalidates the binding; it does not merely find it.** A stamped
key in `raw` records what was decided, not what is still true — the user may
have reversed that link since. So `--force` proceeds without a decision only
when the recovered key still carries a **currently accepted**
`app.account_links` row. If the link was reversed or is pending, the import
returns to R6's gate rather than re-stamping it.

Without that check the guarantee inverts into the failure it exists to
prevent. R15 resolves only accepted links, so replaying a reversed binding
would re-stamp the old key, produce the promised identical transaction ids,
and then materialize every one of them against a NULL canonical account — a
silent reinstatement of a decision the user explicitly undid, which is the
class of wrong silent action `.claude/rules/design-principles.md` weighs most
heavily because it is the hardest to notice and undo.

### Account identity

**R4.** An account key is never derived from document bytes, a filename, or a
path. For a file with no caller-stated account, the import **mints** an opaque
`source_account_key`.

**Two branches derive an account key from a label, and only one of them is
an exception.** An earlier draft called `--account-name` "the one label-derived
key in the codebase". That is false, and the branch it missed is the one R4 is
actually about.

*The exception, named rather than implied: `--account-name`.*
`_label_account_key` (`import_service.py:878`, called at `:2424` and `:3331`)
derives an account key from the **caller's** display label, and this spec
leaves that path alone — its decomposition is Out of Scope (§Testing Strategy).
Stating R4 as an unqualified "never … or a display label" would make it an
invariant the shipping code violates on day one, which is worse than an honest
exception: a guard nobody can turn green gets disabled, and then it guards
nothing. It is bounded and self-retiring — it fires only when a caller passes
`--account-name`, and it retires with the account-vocabulary work that
decomposes the flag.

*In scope, and not an exception: the multi-account tabular branch.* When a
format is multi-account and an account-name column is mapped,
`import_service.py:3358` keys every enumerated account on `slugify(name)` read
**from the file's own column**, and `:3380` slugifies the same label again to
attach the per-account institution. Nothing there is caller-supplied, so it
sits squarely inside "every key the *file* produces" — the population R4
governs. Leaving it alone would make R4 false for exactly the Tiller- and
Monarch-shaped exports that make a format multi-account, and a renamed account
in next month's export would go on silently rotating every transaction id
beneath it.

So this branch mints like the rest: each account the file states gets a minted
`source_account_key` on first contact, and the binding is remembered so next
month's export reuses it. The gate infrastructure is already there — phase 1
enumerates one `SourceAccount` per native key precisely so the account-binding
gate can run before the writing `resolve()` pass. What changes is which key
each enumerated account carries, not when the user is asked: it stays **one**
gate per file listing that file's accounts, never one gate per account. Confirm
volume tracks the uncertainty, which is per file, not the row count.

**"Remembered" needs storage, and the storage does not exist yet.** This is the
half an earlier draft asserted without building, and without it the change is a
regression rather than a fix: the existing ladder can go native key → account
(`accepted_native_account_id`) and account → native keys
(`accepted_native_keys_for_account`, which takes an `account_id`
(`account_resolver.py:782-784`) and so presupposes the answer). Neither goes
*label → key*. A minted key is opaque and a fresh parse cannot re-derive it, so
every recurring multi-account export would reach the confirm gate on every
file, forever — and unattended imports would stop on all of them. `slugify`
at least reused its key.

So `app.account_links` gains a nullable `source_label`: the in-file label the
binding was made under, giving a `(source_type, source_origin, source_label)`
lookup that returns the account and its already-minted key. R4 still holds —
the key is minted, and the label is a lookup handle, never an input to the
hash. A renamed account misses the lookup and re-asks **once**, then the new
label is remembered too; that is the promised behaviour, and it is the whole
difference from `slugify`, where a rename silently rotated every id beneath the
account instead of asking.

**The lookup must be unique, or it is not allowed to bind.** Nothing constrains
`source_label` — `account_links` guards uniqueness on `ref_value`
(`account_links_repo.py:75-81`) and never on the label — so two accounts under
one `(source_type, source_origin)` can both be labelled `Checking`, and R8 makes
that *likelier* by collapsing every unregistered document import onto a single
constant origin. The lookup therefore requires **exactly one** accepted match.
Zero is the ordinary first-import path and reaches the confirm gate. Two or more
is ambiguity, and it reaches the same gate carrying both candidates — never a
silent pick of the first row, never an unattended bind.

This is *Magic stays visible* applied literally: a weak or ambiguous inference
is never eligible for silent action regardless of confidence, and the wrong
action here is an account *merge*, which that rule names as the case where a
wrong silent action is hardest to notice and undo. R7's unattended promise is
scoped to an unambiguous label, and saying so is the honest form of it — a
promise kept by guessing is worse than the gate it skipped.

Two boundaries, because both are easy to get wrong later. This column does not
retroactively fix R8's collision residual — pre-migration rows carry no label,
so that residual stands exactly as scoped. And it does not contradict R8's
refusal to add a discriminator column: that one would have existed solely to
disambiguate a legacy population for a migration written to be deleted, while
this is permanent product state every future import reads. `source_label`
is `DataClass.ACCOUNT_IDENTIFIER`, named explicitly rather than
left as "the account name's class" to resolve on its own. The column is copied
from the tabular/PDF in-file account-name column — `("raw",
"tabular_accounts").account_name`, which `taxonomy.py:1072` already classifies
`ACCOUNT_IDENTIFIER` precisely because the mapped account column can *be* the
account number. Naming a class here matters more than usual because
`source_label` is designed to be shown at the confirm gate: an implementer who
resolved "the account name's class" to one of the lower classes the taxonomy
also uses for account names would unmask what is effectively a raw account
number on the one surface built to display it. It is not `ref_value`'s class
only in that `ref_value` is never projected at all.

**Remembering a renamed label is an UPDATE, and the repository has no verb for
it.** `AccountLinksRepo` exposes `insert` and `repoint`
(`account_links_repo.py:120,176`) and nothing else. On a rename the account's
`ref_value` is unchanged — reusing it is what preserves the transaction ids
testing item 26 asserts — so re-inserting an accepted row for the same tuple
trips `_guard_uniqueness`, and `repoint` is the wrong verb: it moves a link to
a different `account_id` by reversing and re-inserting, when the account here is
the same and only its label changed. Without a third verb the rename path either
raises on uniqueness or mints a new key and rotates every id beneath the
account — the precise `slugify` failure R4 exists to end.

So the repo gains `relabel`: an audited UPDATE of `source_label` on one accepted
link, paired with its `app.audit_log` row like every other mutation
(Invariant 10), with `before_value` carrying the prior label rather than NULL —
it is an update, not a creation. It is deliberately not folded into `repoint`;
one changes which account a ref means, the other changes only what the file
called it, and collapsing them would let a relabel silently carry an account
move.

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

**The constant strands the links written under the old fallback, so V052
re-points them.** Today an unregistered file takes
`slugify(account_name or "unknown")` as its origin
(`import_service.py:3219-3222`), and that value is what lands in
`app.account_links.source_origin`. `accepted_native_keys_for_account` filters
on it (`account_resolver.py:802-808`), so after R8 the lookup asks for the
constant while the stored rows still say `slugify(account_name)` — and R7 finds
nothing, for precisely the identity-unknown population R17 exists to serve. The
user is re-asked a question they already answered, which is the failure this
spec is about.

Migration step 6 therefore re-points those rows in the same statement block
that mints. **The predicate is scoped by `source_type` first, and only then by
the format table of that same family.** "Names no registered format" is not a
predicate on its own: `source_origin` holds a different kind of value per
channel — Plaid stores the account's `provider_item_id`
(`sync_service.py:307`), OFX the institution, Google Sheets the connection id —
and none of those is a row in either format table. An unscoped `NOT IN` would
therefore match **every** Plaid, OFX, and gsheet link and overwrite a valid
origin with the tabular fallback constant, which is strictly worse than the
defect it fixes: `stg_plaid__accounts.sql:55` joins
`links.source_origin = a.source_origin`, so the rewrite would make every Plaid
account unresolvable rather than merely re-asked.

The rewrite is confined to the population the old fallback actually wrote — the
document-import channels, which are the only ones that put a *format name*
there (`import_service.py:3219-3222`). Each family is checked against its own
table: rows with a tabular `source_type` against `app.tabular_formats.name`,
rows with `source_type='pdf'` against `app.pdf_formats.name`. Not the union of
the two — a PDF link whose origin happens to collide with a tabular format name
is still fallback-origin, and the union would spare it. Both columns are
`VARCHAR PRIMARY KEY`, so within a family the membership test itself is exact.

**One collision survives, and it is bounded rather than fixed.** Both sides are
arbitrary user-controlled strings, so an old unregistered import whose
`slugify(account_name)` happens to equal a saved format name in the same family
is indistinguishable from a genuine format-origin row — no column records which
path wrote it. That row keeps its old origin and R7 will not find it, so the
user is asked to bind that one account again. This is accepted, not overlooked:
the cost is exactly one re-confirmation of a binding the user can re-answer,
which is the pre-migration behaviour for the whole population and is the
failure mode this spec treats as recoverable. It is never a wrong binding and
never data loss, because a stranded link is invisible rather than misapplied.
Persisting a discriminator would mean adding a column to `app.account_links`
for the sole benefit of a migration written to be deleted, which the
thin-migration posture rules out. Doctor's account-links check surfaces an
accepted link on a non-constant origin after V052, so the residual is
reportable even though it is not repairable.

This is a link-table rewrite, not an identity backfill: the transaction ids
still rotate, which R8 already accepts. What it preserves is the *binding
decision*.

**R9.** Manual rows carry no `account_id` in the identity tuple. Manual rows
already hold an immutable `manual_<uuid4>` as `source_transaction_id`; the
account slot becomes a **minted per-account source key**. §Data Model defines
its shape and §Migration step 6 mints it for rows that already exist.

R9 governs `raw.manual_transactions` only. `raw.manual_investment_transactions`
is excluded, for a reason §Data Model states rather than leaves to inference.

An earlier draft also offered the constant `'user'`. That option is withdrawn as
unsafe: `stg_manual__transactions.sql:7-8` reads `t.account_id` as both
`source_account_key` and the link join's `ref_value`, so a constant would bind
every manual account to one `app.account_links` row and silently merge every
hand-entered account into a single account.

**R17** is numbered out of sequence deliberately: it was added after R10–R16
existed, and requirement numbers are append-only so a citation never silently
changes meaning (`design-principles.md`'s addressing rule). It sits here rather
than at the end because it is about gate evidence and belongs beside R9.

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
| `raw_ofx_transactions.sql:19` | `(source_transaction_id, account_id, source_file)` | `(source_row_key, account_id, source_document_key)` |
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
component may not be NULL.

**The OFX key moves the same way, and an earlier draft got this backwards.** It
kept `source_transaction_id` in the OFX key because the column is already a key
component there and so cannot be NULL, citing the extractor's FITID repair as
"the same premise stated from the other side." The repair is the *opposite*
premise. `_disambiguate_colliding_fitids` (`extractor.py:137-205`) does not
merely keep a colliding `<FITID>`; at `:196-198` it appends a content-hash
suffix **to `source_transaction_id` itself**, which is exactly the synthesis R13 forbids
and R14 guards. Left as drafted, OFX violates the new contract on the first
Chase file and R14's guard can never be turned green — the failure mode R4's
exception paragraph exists to avoid.

Deleting the repair is not the alternative. The raw primary key and the
`stg_ofx__transactions` dedup window both key on that column, so two distinct
rows sharing a FITID collapse and one is silently lost — the data-loss bug the
repair was written to fix, on a real supported case (Chase stamps a foreign
purchase and its foreign-transaction fee with one shared FITID).

So OFX splits the way tabular does. `source_transaction_id` holds the
institution's `<FITID>` **verbatim, repaired or not**, which is both honest and
what R13 and R14 require. A `source_row_key NOT NULL` holds the
within-document row identity and carries the repair suffix when one fired. The
raw key becomes `(source_row_key, account_id, source_document_key)` — tabular's
shape — so the two file channels stop running on two different key grammars.

**The identity selection needs OFX's existing flag, not a new one.** R13's
"`source_transaction_id` when present, `source_row_key` otherwise" is wrong for
a repaired OFX row: the FITID *is* present and is *not* unique, so identity
would re-collapse the two rows the repair just separated — reintroducing the
loss through the front door. `fitid_repaired` (`raw_ofx_transactions.sql:18`,
added by V047) already marks exactly those rows, and its column comment already
calls it "the only proof staging may use to retire the id this row superseded."
The discriminator exists and is already persisted, so nothing new is invented
for this.

**These enumerations are a floor, not a census.** Two of them were verified
short, both in the same way: a name was grepped once, the first hit was
recorded, and the rest of the file went unread. Take every list below as the
sites that are known to move, and re-derive the full set with
`grep -rn source_file src/moneybin` at implementation time — the same
instruction §`source_transaction_id_a/_b` consumers already carries, for the
same reason.

**Nine query sites use it as a batch discriminator** — ten line references,
because `import_log.py:410,415` is a single site spanning two lines of one
query (both bound by the same parameters at `:426`) — all of which move to
`source_document_key`:

`import_service.py:1862`, `:5026`, `:5257`, `:6112`;
`account_resolver.py:1318`; `doctor_service.py:141`;
`dim_holdings.sql:88`; `gsheet/connection_service.py:566`; and
`import_log.py:410,415` — the last being the legacy path fallback that
`find_existing_import` already documents as retiring, which this spec retires.

**`doctor_service.py` is six sites, not the one listed.** Beyond `:141`:
`_NEWEST_HOLDINGS_SNAPSHOT_CTE` both selects and orders by the column
(`:134`, `:138`), `_run_investment_unreported_holdings` joins two snapshots on
it (`:1218`), and `_run_unproposed_cross_source_duplicates` projects it twice
as the matcher's physical-source key (`:2389`, `:2393`); a comment at `:2584`
describes the NULL semantics and moves with them. Missing these is worse than
a crash: these checks catch a catalog error and return `skipped`, so a migrated
database would quietly lose its holdings and duplicate diagnostics while doctor
still reported a clean run.

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

One counter deserves an answer rather than silence, because it is the reason a
reviewer keeps re-raising this: MoneyBin's own log is a **new** surface. The
path on disk was visible only to whoever held the disk; a log file concentrates
many paths into one artifact a user may attach to a bug report. That is a real
difference from "it was already exposed."

It does not change the decision, for two reasons. The log is still the user's
own local artifact, written under the same roof as the database it describes,
and MoneyBin never transmits it. And the alternative fails on its own terms: a
masked path in an import error is a path the user cannot match to the file they
just passed, which is the single most common thing an import error has to
communicate. What this does change is the advice — a shared log is a shared
artifact, and `moneybin doctor` output pasted into an issue carries whatever
the user named their files. That belongs in support guidance, not in a masking
rule that buys nothing where the file already sits.

**This is not a departure from the privacy rules — it is what they already
say.** `privacy-data-protection.md` §"What CAN appear" lists "File paths (not
file contents)" beside record counts, entity ids, masked identifiers, and
institution names. R11 restates that for `source_path`; it carves no exception
out of it.

What does need correcting is AGENTS.md's one-line summary — "Log record counts,
IDs, and status codes only" — which is narrower than the canonical spec it
points at and drops four of the six permitted categories. Read literally it
forbids the masked account label its own next sentence permits. A reviewer
holding the summary against this column will read a deliberate decision as a
violation, and has. The summary moves to the canonical list; the canonical list
does not move to the summary.

**And the advice above gets an owner.** `privacy-data-protection.md` gains it
next to the permitted list, where a reader meets it: doctor and import output
carry file paths verbatim, and are worth a glance before being pasted into an
issue. A mitigation named in a rationale and assigned to no file is a sentence
nobody implements.

**R12.** Wherever the match engine needs "did these two rows come from the same
physical import?", it keys on `source_document_key`, not on a path. Two sites:
the Tier 2b blocking guard `a.source_file != b.source_file`
(`scoring.py:284`) and the `SourceKey` cardinality unit
`(source_type, source_origin, source_file)` (`assignment.py:88`).

**`assignment.py:88` is not the 1:1 assignment key**, and an earlier draft
called it that. It is `SourceKey`, the cardinality unit inside
`assign_components`' union-find: two rows sharing it came from the same file
and are therefore distinct transactions rather than duplicates of each other.
The actual 1:1 key is `_claim_key` (`assignment.py:34-52`), built from
`(source_type, account_id, source_transaction_id)` with no `source_file` in it
at all. The distinction decides which mechanism an implementer edits.

**`_claim_key` does need a change — from R13, not R12.** It reads
`pair.source_transaction_id_a/_b`, which R13 makes legitimately NULL for a
tabular row carrying no native id. Every such row within one account would then
build the identical claim key and collapse into a single claim, silently
suppressing matches. `_claim_key` must consume `identity_component` instead —
the value R13 defines precisely so that a total key exists. This is the one
place R13's honest NULL is actively dangerous rather than merely more truthful,
so it is named here rather than left to fall out of the rename.

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
- The identity hash consumes `source_transaction_id` when it is present **and
  not marked unreliable**, and `source_row_key` otherwise. "Unreliable" has
  exactly one producer today — `fitid_repaired` on a repaired OFX row, where
  the institution reused one FITID for two distinct transactions — and it is a
  third case inside the one expression, not a second selection site. That
  selection happens **once**, explicitly, in `int_transactions__matched`, under
  the name `identity_component` — never by a fallback hidden inside a column's
  meaning.
- **Both staging dedups group on that same expression** —
  `stg_tabular__transactions.sql:35`, which partitions on `(transaction_id,
  account_id)` today, and `stg_ofx__transactions.sql:61`, which partitions on
  `(source_transaction_id, account_id)`. Both move to
  `(identity_component, account_id)`, so staging and core agree by construction
  about what one row is. These are grouping keys, not further selections, and
  they are the only other places the expression may appear; a guard pins the
  count at **three**. The forms that are wrong, and why, are in *Files to
  Modify*.

  **OFX staging is not optional here, and an earlier draft left it out.** Once
  a repaired row carries the bare `<FITID>` in `source_transaction_id`, the
  existing OFX partition collapses the two rows the repair separated — in
  `prep`, before `int_transactions__matched` ever gets to consult
  `source_row_key`. Fixing the raw key alone moves the loss one layer later
  instead of removing it: raw would hold both rows and core would still show
  one.
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

The `--account-name` path (R4's named exception) is the guard's **only**
exemption, and the guard asserts its exemption list by **set equality** rather
than by membership. A membership check silently absorbs the next exemption
somebody adds; set equality fails the moment the list changes, which turns
adding one into a decision somebody has to make on purpose. The list empties
itself when the `--account-name` decomposition lands, and the guard then fails until the
exemption is removed — which is the intended way for it to be retired.

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

**`core.dim_accounts` carries a fourteenth fallback, and it is the one that
actually publishes.** `dim_accounts.sql:199` computes `COALESCE(account_id,
source_account_key) AS grain_key` and `:231` projects `grain_key AS
account_id`. Dropping the thirteen prep arms without this one achieves nothing
observable: the NULL those thirteen now produce is caught here and replaced by
the source-native key, which is then published as the canonical
`core.dim_accounts.account_id` — a `DataClass.RECORD_ID` column, unmasked. R15
would be false on the exact surface it names, and R14's guard would still fail.

**Deleting this COALESCE naively is worse than leaving it, and the model's own
comment says so.** `:185-193` records that the fallback "never fires: all three
`stg_*__accounts` models already project `COALESCE(links.account_id,
a.account_id)`", kept "as a second line of defence — were a staging model to
stop falling back, every NULL would collapse into one bad row." R15 *is* that
event. Remove the fallback and every unlinked account grains on NULL, so the
`merged` GROUP BY collapses them into a single row — silently merging unrelated
accounts, which is strictly worse than publishing a source-native id.

The defect is that one expression serves two jobs. Split them: `grain_key`
remains the internal grouping key and may still fall back to the source-native
value, so unlinked accounts stay *distinct* rows; the published column stops
being `grain_key` and becomes the minted `account_id` itself — NULL when no
accepted link exists. Grouping by a value is not publishing it, and only the
projection was ever the contract. That keeps R15 true, keeps the collapse from
happening, and keeps R15's own NULL-over-deletion choice: several unlinked rows
publishing NULL `account_id` is visible to
`fct_transactions_account_linkage`, where a merged-away row is not.

**R16.** `moneybin doctor` detects orphaned rows in all six `app.*` tables
hard-coupled to `transaction_id`, not the two it covers today
(`doctor_service.py:713`). Five take the same check — a row whose
`transaction_id` no longer resolves. `transaction_id_aliases` takes a different
one, because its rows are never orphaned in that sense: check that each
`new_transaction_id` still resolves, since a preserved alias forwarding to a
rotated id is the failure it can suffer. V052 deliberately preserves no `app.*` curation, and no
foreign key exists anywhere in `app.*` to complain, so doctor is the only
surface that can tell a user what the migration and re-import cost them. It
ships **with** the migration, not after it.

**`app.match_decisions` needs a seventh check, because the other six cannot
reach it.** R16's six are the tables hard-coupled to `transaction_id`, and
`match_decisions` is not one of them: it anchors on the source-native pair
(`source_transaction_id_a/_b` + type + origin), which R13 renames to
`identity_component_a/_b` and whose *values* rotate for tabular native-id rows.
Migration step 8 already says those decisions orphan. What is missing is that
nothing detects it. The existing `_run_match_decisions_account_fk`
(`doctor_service.py:1702-1715`) validates only `account_id`/`account_id_b`
against `dim_accounts` — its docstring states outright that "there is no clean
transaction FK here" — so it stays green while every anchor dangles.

The consequence is not cosmetic. `int_transactions__matched.sql:10-34` builds
its dedup edges from accepted decisions keyed on `(source_type,
source_transaction_id)` scoped by `account_id`. An anchor that no longer names
a node contributes no edge, the accepted dedup silently stops collapsing its
pair, and both rows are counted — a user's *accepted* de-duplication reverting
itself, with no error and no doctor finding. So the seventh check tests anchor
resolvability against the staging identity set, not `transaction_id`. It
reports rather than repairs, consistent with this migration preserving no
`app.*` curation: re-anchoring inside V052 would mean rewriting a human
decision's subject from a value the user never saw, which is the silent-merge
class again.

## Data Model

### New column

`source_document_key VARCHAR NOT NULL` on every `raw.*` table that today
carries `source_file`. `NOT NULL` because it takes `source_file`'s place in
eight primary keys (R10).

**`raw.import_log` takes the same column nullable**, and the exception is
deliberate rather than an inconsistency. That table keys on `import_id`, so the
document key is informational there rather than a key component, and it holds
rows for pre-V046 batches with no `file_sha256` to derive one from. NULL is the
honest answer for those; minting a stand-in would put a value into a column
that names bytes nobody hashed. §Migration step 7 adds it on those terms.

**The column is wider than R10's table, and the difference is what it does.**
Sixteen `raw.*` schema files carry `source_file` today. R10 names the eight
whose *primary key* changes — three of them already Plaid tables
(`raw_plaid_investment_holdings`, `_snapshots`, `_holding_lots`). The other
eight take `source_document_key` as a plain descriptive column and keep their
existing keys, which are built on the source's own ids rather than on a
document:

| Table | Where it lives |
|---|---|
| `raw_plaid_transactions` | `extractors/plaid/schema/` |
| `raw_plaid_accounts` | `extractors/plaid/schema/` |
| `raw_plaid_balances` | `extractors/plaid/schema/` |
| `raw_plaid_investment_transactions` | `extractors/plaid/schema/` |
| `raw_plaid_securities` | `extractors/plaid/schema/` |
| `raw_ofx_institutions` | `extractors/ofx/schema/` |
| `raw_pdf_seeds` | `sql/schema/` |
| `raw_import_log` | `sql/schema/` (already named above) |

**Enumerate rather than search, because the files are not where a reader
looks.** Only `raw_import_log.sql` and `raw_pdf_seeds.sql` live under
`sql/schema/`; the other fourteen live under `extractors/<channel>/schema/`. A
`grep` of `sql/schema/` therefore returns two of sixteen, and the resulting
list looks complete long before it is — which is how the five Plaid tables and
`raw_ofx_institutions` go missing from a change that must touch all of them.

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

`source_row_key VARCHAR NOT NULL` on `raw.tabular_transactions`, on
`raw.ofx_transactions`, and on any other raw transaction table whose source may
not supply a native id — OFX for the separate reason above: its source always
supplies an id, but that id is not always unique, and the repair for that must
not land in the column that means "the institution's own value". Carries
MoneyBin's within-document row identity (R13). `source_transaction_id` becomes
nullable wherever it is not already, since NULL is now its honest answer for a
source that assigns no id.

**`NOT NULL`, and computed for every tabular row — including rows that carry a
native id.** This is the one place the two columns are not symmetric, and
getting it backwards breaks native-id imports on the first file. R10 makes
`source_row_key` the leading component of `raw.tabular_transactions`' new
primary key, and a key component cannot be NULL. "The column for rows without a
native id" is therefore the wrong reading: `source_row_key` is *always*
populated, and `source_transaction_id` is the one that may be NULL. Which of
the two the identity hash consumes is a separate question, answered once by
`identity_component` (R13) — a populated `source_row_key` on a native-id row is
simply unused by the hash, not a contradiction.

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

### Repurposed column

`raw.manual_transactions.account_id` stops holding a canonical
`core.dim_accounts` id and starts holding a **minted per-account source key**
(R9): `src_` followed by 12 hex, opaque and not reproducible across databases.
One key per manual account, minted when the account is created and persisted —
never per row, never per batch.

The `src_` prefix is load-bearing. Canonical ids are bare 12 hex
(`account_resolver.py:463`), so the prefix keeps the two kinds visually
distinct in a column that used to hold the other one, and makes a minted key
greppable for R14's guard.

The join does not change shape. `stg_manual__transactions.sql:26-31` already
resolves this column through `app.account_links` on `ref_value = t.account_id`;
after R9 that join matches a minted key instead of a canonical one, and the
`COALESCE` at `:7` yields the same canonical account it always did. The column
keeps its name here. Renaming it to `source_account_key` — which is what the
staging model already aliases it to — is the honest end state by this spec's
own rule that a name says where a value came from, but it is a separate change
to a public column name and is deliberately not folded in.

**`raw.manual_investment_transactions.account_id` is excluded.** Its staging
model has no link join at all: `stg_manual__investment_transactions.sql:11`
passes the column straight through as already-canonical. Minting into it would
therefore emit a `src_` key *as* the canonical `account_id` and orphan every
row from `core.dim_accounts`. Giving that table an honest source slot means
first giving its model a link join, which is a larger change than R9 and is not
in this spec. R9 governs the table whose column is read as
`source_account_key`; the investment table's column is read as the account
itself.

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
values or rewrite the `transaction_id` held in `app.*`. Recovery is a
re-import, which regenerates every `raw.*` identity correctly by construction —
a fresh import is the definition of the right answer here, so reproducing it
inside a migration is duplicated logic that can only diverge.

**It writes exactly three things beyond the schema change**, all named here so
none is discovered later as an inconsistency. A minted source key per existing
manual account, on rows it preserves rather than clears (step 6). A pre-clear
row-count baseline with an `incomplete` marker for the Plaid tables it empties
(step 2). And a re-pointing of `app.account_links.source_origin` for links
written under the old `--account-name` fallback (step 6, R8). Everything else
is shape.

All three survive the posture for the same reason: each is the minimum durable
state without which the migration fails *silently*. Drop the mint and the
defect R9 exists to remove survives permanently on the rows the migration went
out of its way to save. Drop the baseline and the cleared Plaid history is not
merely unrecoverable but unreportable, because the counts doctor would compare
against are exactly what step 2 destroys. Drop the re-pointing and R7 stops
finding remembered bindings for the identity-unknown accounts, so the migration
silently re-opens every confirm the user has already answered.

None of the three is a backfill and none rewrites an identity value — which is
the line this posture actually draws. Transaction ids still rotate exactly as
R4, R7, R8, R9, and R13 say they do; what these three preserve is the state a
re-import cannot reconstruct.

This is a deliberate pre-launch trade. A data-preserving migration costs more
to write, review, and verify than a re-import costs to run, and the repo is
pre-launch with no user whose curation cannot be rebuilt. The migration is
written **to be deleted wholesale** in a future pre-release schema reset rather
than maintained: nothing else may depend on having run it, and the one
derivation it does carry — computing `source_document_key` from the
`sync_{job_id}` already in `source_file` for the three Plaid investment tables
(step 1) — is a pure function of a column that is present, not a reconstruction
of anything absent. Deleting the migration later deletes that too, with no
unwind. The three writes above are consistent with that. The minted keys and the re-pointed origins become ordinary
`app.account_links` rows indistinguishable from any other, and the recovery
baseline is self-retiring — cleared by a completed forced pull — so a database
that has finished recovering carries no residue of the migration at all.

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
   each. DuckDB cannot `ADD` or `DROP` a primary key, so each is a
   create-drop-rename regardless of what happens to the rows.

   **Five are recreated empty; three carry their rows across.** The split is
   not a concession to the posture — it is the posture's own test ("preserve it
   when nothing outside the database can reproduce it") applied per table. Five
   of the eight need a `NOT NULL` key component only a re-import can compute,
   so they are recreated empty. The three Plaid investment tables —
   `raw.plaid_investment_holdings`, `raw.plaid_investment_holdings_snapshots`,
   `raw.plaid_investment_holding_lots` — are different on both halves of the
   test, and clearing them was the single most damaging thing an earlier draft
   of this migration did.

   **They are not reproducible.** `prep.int_plaid__opening_positions` anchors
   the opening-lot bootstrap to the **first** snapshot per (account, item)
   (`:24-45`), and its own comment says why that is safe: "raw.plaid_investment_holdings
   keeps every snapshot (source_file is part of its PK), so a later sale that
   drops a lot from the NEWEST snapshot never retroactively rewrites a
   pre-window lot whose basis was known at connect." A forced pull returns the
   holdings snapshot **as of that sync** and nothing earlier, so it can add a
   new newest snapshot and can never restore the first one. Clearing these
   tables therefore does not cost history that comes back — it silently and
   permanently changes opening positions and cost basis, which is tax-relevant
   and which no doctor warning can undo.

   **And no re-import is needed to reshape them.** Their document key is
   derivable in place: it hashes `sync_{job_id}`, which is exactly the value
   `source_file` already holds (`raw_plaid_transactions.sql:27`) — the same
   derivation §step 2 notes for `raw.plaid_*` generally. `source_row_key` is an
   R13 transaction-identity column and these three are holdings tables, so it
   does not apply to them. The create-drop-rename therefore selects rows across
   with `source_document_key` computed from the old column, and the new primary
   key is satisfied without an API call.
2. Clear the remaining re-derivable `raw.*` tables, which hold no identity the
   new shape changes: the five `raw.plaid_*` tables named in §Data Model,
   `raw.ofx_institutions`, `raw.pdf_seeds`, and
   `raw.import_preview_snapshots` — the last holding staged bytes that are
   deleted on consumption anyway.

   **Eight cleared, but only seven take the new column.** Those seven carry
   `source_file` today, so all seven fall under §Data Model's rule and acquire
   a `NOT NULL` `source_document_key` that only a re-import can fill — which is
   what makes clearing the thing that satisfies it. `raw.import_preview_snapshots`
   is the exception on both counts: it has no `source_file` (it keys on
   `preview_id` and holds only staged bytes), so it takes no new column, and it
   is cleared because its rows are deleted on consumption anyway. Do not read
   "cleared" and "takes the column" as the same set.

   Clearing is a cost decision, not an impossibility. For `raw.plaid_*` the
   document key *is* derivable without an API call — the key hashes
   `sync_{job_id}`, which is the exact value `source_file` already holds
   (`raw_plaid_transactions.sql:27`) — so a backfill could be written.

   **But recovering the cleared Plaid rows is not automatic, and the migration
   must not pretend otherwise.** "A re-sync rebuilds them" is false for the
   default command: `moneybin sync pull` leaves `force=False`
   (`cli/commands/sync.py:334-336`), which passes `reset_cursor=False`
   (`sync_service.py:117-119`), and the server contract makes that
   incremental-only from the last stored cursor
   (`docs/reference/server-api-contract.md:256`). An ordinary pull after V052
   returns only what is newer than the cursor and leaves the cleared history
   missing — silently, because nothing treats an empty raw table as an error.

   **The migration cannot fix this itself.** The cursor is server-side — the
   contract says `reset_cursor: true` makes "the server discard *its* cursor"
   — and V052 is a local DuckDB migration with no reach into it. So the
   requirement is a reporting one, not a mechanical one: V052 completes with an
   explicit instruction to run `moneybin sync pull --force`, and `moneybin
   doctor` reports the Plaid tables as incomplete until their history returns.
   What the migration may not do is clear the tables and describe the recovery
   as automatic.

   **Doctor cannot report that from the tables alone, so V052 must persist a
   baseline before it clears.** Steps 1–2 destroy the very row counts the check
   would compare against, and once an incremental pull adds a single row the
   tables are non-empty — at which point doctor cannot distinguish partially
   restored history from fully restored history, or from a table that was
   legitimately empty all along. The gap goes silent again, one row after the
   migration.

   Before clearing, V052 writes the pre-clear row count for each affected table
   plus an `incomplete` marker into `app.*`, and doctor reports every marked
   table until the marker is retired. The marker is what makes this fail-closed
   rather than a guess: a missing baseline reads as "not yet recovered," never
   as "fine." This is the one piece of durable state the thin-migration posture
   requires, and it exists precisely because everything else about the posture
   is deliberately disposable.

   **The count is evidence, not the resolving condition.** Resolving on "count
   met or exceeded" would report a false recovery. A forced pull cannot return
   history older than the provider's window, and ordinary new transactions
   accumulate afterwards, so a table can regain its old count — or exceed it —
   while every row the migration destroyed stays permanently gone. A count
   cannot separate those two states, because they are not the same rows.

   The marker is therefore retired only by a **completed forced pull** for
   that connection, and doctor reports the pre-clear count beside the current
   one instead of asserting they are equivalent. Where the forced pull returns
   fewer rows than the baseline, that shortfall is the provider window and it
   is permanent — doctor says so plainly rather than waiting on a count that
   will never arrive or clearing on one that means nothing.

   **A completed pull resolves the row; it does not delete it.** That
   distinction is the whole mechanism, and an earlier draft got it backwards.
   The completed pull is the first moment the shortfall is *knowable* — only
   then is there a post-recovery count to set beside the baseline — so deleting
   the row on completion would destroy the evidence in the same instant it
   comes into existence, and doctor could never report the permanent loss the
   paragraph above promises. The pull instead stamps the recovered count and a
   resolution timestamp, moving the row from `incomplete` to `recovered` when
   the count is whole or `short` when it is not. A `recovered` row goes quiet.
   A `short` row is reported as a settled fact — history the provider window
   will not return — and no later pull reopens it, because no later pull can
   change it.

   **A `short` row carries no acknowledgement**, and an earlier draft's "the
   user can acknowledge it" was a surface this spec never defines: §CLI and
   §MCP add no operations, so the acknowledgement would have had no way to be
   made. Inventing one also gets the polarity backwards. The shortfall is a
   permanent property of this database, so doctor reports it the way it reports
   any other standing fact about the data — an informational line, not a
   finding that asks for an action no action can satisfy. What retires is the
   *warning*: an `incomplete` row is an open recovery item, a `short` row is
   closed history.

   **The clearing path is named, or the marker is permanent.** A migration that
   writes state nothing else can clear leaves doctor reporting every recovered
   database as incomplete forever, which is the same silent-gap failure with
   the sign flipped. So this state gets the ordinary treatment rather than an
   ad-hoc table: `app.sync_recovery_state` (one row per affected
   `(table, source_origin)`, holding the pre-clear count and the migration's
   timestamp, then the recovered count, the resolution timestamp, and the
   `incomplete` / `recovered` / `short` state the pull stamps on it), written
   through a
   `SyncRecoveryRepo` like every other `app.*` table (Invariant 10), and
   resolved from exactly one
   place — `SyncService.pull` (`sync_service.py:85`), at the end of a run whose
   `force=True` reached a successful load. `force` is already what sets
   `reset_cursor` (`:119`), so the flag that causes the recovery is the flag
   that resolves the marker; no second notion of "forced" is introduced. Doctor
   reports nothing once every row is `recovered`, and reports each `short` row
   as standing history for as long as the database exists.
3. Preserve the six `raw.*` tables a re-import does not rebuild. The list is
   enumerated here rather than left to a predicate, because the failure mode is
   a list that looks complete and is short by one:

   | Table | Why a re-import does not rebuild it |
   |---|---|
   | `raw.manual_transactions` | The user typed the rows. |
   | `raw.manual_investment_transactions` | The same, for investment events — the second manual table, easily missed because only the first is named elsewhere in this spec (`raw_manual_investment_transactions.sql:1-6`). |
   | `raw.exchange_rates` | Append-only by design: "a rate a provider published for a date is a historical fact, so a refetch never rewrites one." A refetch is also bounded by the provider's history window, so what falls outside it is simply gone. |
   | `raw.security_prices` | Append-only for the same reason — "a historical close is an immutable fact" — and the schema says so precisely to contrast with `raw.plaid_securities`, whose close price is overwritten on every pull and therefore *cannot* carry a history. |
   | `raw.gsheet_seeds` | Holds **soft-deleted** rows a re-pull cannot return. Each pull sets `deleted_from_source_at` on rows that have vanished from the sheet and keeps their `data` (`raw_gsheet_seeds.sql:8`), but the adapter can only read rows the sheet still has — so a row the user deleted upstream exists nowhere else. Clearing this table is unrecoverable by any command, `--force` included, which is what separates it from `raw.plaid_*`. It carries no `source_file` and takes no new column, so preserving it needs no reshape. |
   | `raw.import_log` | The batch parent of all five above: dropping it dangles their `import_id`. It is also the audit record of every import ever run, which a re-import appends to rather than reconstructs. |

4. On both manual tables, rename the minted `source_transaction_id` to
   `source_row_key` (R13). It stays the primary key and no value changes.
5. NULL the stored gold-key prediction on `raw.manual_transactions` —
   `transaction_id`, and **only** that one. It was computed at INSERT from
   inputs step 6 rotates, so it is stale the moment this migration runs. NULL
   is deliberate rather than a recomputation: the prediction exists so
   `_run_orphan_app_state` can suppress a false positive on a row not yet
   materialized in `core` (`doctor_service.py:754-763` unions it into
   `valid_txn`), and a *stale* prediction makes that suppression hide a real
   orphan. NULL makes doctor report instead — the failure that is visible
   rather than the one that is silent. The column repopulates on the next
   entry, and the preserved rows stop being reported the moment a transform
   materializes them into `core`.

   **`raw.manual_investment_transactions.investment_transaction_id` is
   preserved, not NULLed**, and the two tables are not symmetric here even
   though they look it. On the transactions side the stored value is a
   *prediction* that `int_transactions__matched` recomputes. On the investment
   side there is no matcher pipeline, so the stored value is the canonical id
   itself: `stg_manual__investment_transactions.sql:28` passes it straight
   through, and `fct_investment_transactions.sql:16` declares
   `grain investment_transaction_id`. NULLing it would not make doctor report a
   recoverable orphan — it would materialize every preserved investment event
   with a NULL grain key and break every lot reference pointing at one, with
   nothing anywhere able to recompute the value.

   Nothing forces it stale, either. Its hash inputs are
   `manual|user|account_id|source_transaction_id`; R9 excludes this table's
   `account_id` (§Data Model), and step 4's rename carries the same value under
   a new name. Both inputs survive the migration unchanged, so the stored id is
   still correct.
6. Mint a source key for every manual account that already has rows. For each
   distinct `account_id` in `raw.manual_transactions`: mint one `src_` key,
   INSERT the matching `app.account_links` row (`ref_kind='source_native'`,
   `source_type='manual'`, `source_origin='user'`, `status='accepted'`,
   `decided_by='system'`) pointing at the canonical id currently stored, then
   UPDATE that account's rows to the minted key.

   **Mint and link are one step, not two.**
   `stg_manual__transactions.sql:31` joins on `ref_value`, so rewriting the
   column without writing the link orphans every preserved manual row from
   `core.dim_accounts` — the rows the migration preserved precisely because
   nobody can retype them. Writing the link without rewriting the column does
   nothing at all.

   Without this step the column holds two kinds of value forever: a canonical
   id on every preserved row, a minted key on every row entered afterwards. The
   preserved rows would keep exactly the defect R9 exists to remove, and the
   R14 guard could not tell a violation from a pre-migration survivor.

   **The same block re-points the fallback-origin links (R8).** In one
   `UPDATE`, every `app.account_links` row written under the old tabular/PDF
   fallback takes R8's constant. Without it those links become unreachable the
   moment R8 lands, because `accepted_native_keys_for_account` filters on
   `source_origin` (`account_resolver.py:802-808`), and the identity-unknown
   accounts get re-asked a question they already answered. It belongs here
   rather than in its own step because it touches the same table in the same
   transaction as the mint.

   **Scoped by `source_type`, then by that family's format table** — per R8. A
   tabular `source_type` is checked against
   `NOT IN (SELECT name FROM app.tabular_formats)`, `source_type='pdf'` against
   `NOT IN (SELECT name FROM app.pdf_formats)`, and every other `source_type`
   is left alone. The unscoped form is a data-loss bug, not a wider net: Plaid,
   OFX, and gsheet origins are item ids, institutions, and connection ids, none
   of which appear in either format table, so an unscoped `NOT IN` rewrites all
   of them. The migration test asserts the negative directly — a seeded Plaid
   link and a seeded OFX link carry byte-identical `source_origin` values
   before and after V052.

   **The rewrite must not collapse two accepted links onto one key.**
   `_guard_uniqueness` keys on `(source_type, source_origin, ref_kind,
   ref_value)` among accepted rows (`account_links_repo.py:75-81`) — it includes
   `source_origin`, which the table comment's narrower `(source_type, ref_kind,
   ref_value)` phrasing omits (`app_account_links.sql:6-9`). Two accepted links
   sharing a `ref_value` under *different* fallback origins are therefore legal
   today, and re-pointing both at the one constant makes them collide on the full
   key. Nothing catches it, for the reason this step already gives: a migration
   writes below the repository layer, so the guard never runs.

   The damage is not confined to the link table.
   `stg_tabular__transactions.sql:73-78` joins on exactly that tuple, so two
   surviving accepted rows turn a 1:1 translation into a one-to-many join and
   **duplicate every transaction** in the affected account — silently, in `prep`,
   with no error anywhere.

   So step 6 checks before it writes: group the rewrite population by its
   *post-rewrite* key, and if any group holds more than one accepted row,
   **refuse the migration** and report the affected pairs — `ref_value` masked
   per its classification, never printed raw. Refusing is deliberate over
   auto-consolidating. Picking a survivor means discarding one of two human
   binding decisions, which is precisely the silent merge *Magic stays visible*
   forbids without a confirm, and a migration has no confirm surface to offer. A
   refusal is cheap and recoverable — reverse one link, re-run — while a wrong
   merge surfaces months later as doubled spending. The refusal also keeps the
   thin-migration posture honest: detecting the collision is a `GROUP BY … HAVING
   COUNT(*) > 1`, not a consolidation engine.

   The same step adds `app.account_links.source_label` and backfills
   **nothing** into it. A pre-migration row's label is not recoverable — the
   old key was `slugify(name)`, and a slug does not invert — so every existing
   row keeps NULL and the first import after V052 re-asks that account once,
   then remembers. Guessing a label back from a slug is the one thing worse
   than asking: it would silently bind an account under a name the file never
   used.

   These are V052's largest `app.*` mutation but **not its only one**: step 2
   writes an `app.sync_recovery_state` baseline row per affected
   `(table, source_origin)`, and that table is not one of Invariant 10's exempt
   system tables either. It takes the identical treatment — an `app.audit_log`
   row paired with each marker insertion, in the same transaction,
   `actor='system'`, `before_value` NULL because it is an INSERT. Skipping it
   would ship a protected app-state mutation the migration's own audit routing
   cannot account for, which is the precise hole Invariant 10 exists to close.

   Each of these writes departs from its table's repository-only rule —
   `AccountLinksRepo` for the links (`app_account_links.sql:5`),
   `SyncRecoveryRepo` for the baseline — in the same way and for the same
   reason: a migration runs below the repository layer. V052 therefore writes
   the paired `app.audit_log` rows itself, under one shared `operation_id` with
   `actor='system'`, because the invariant is about the audit row existing, not
   about which layer emitted it. **`before_value` is
   NULL only for the mint.** `app_audit_log.sql:15` defines it as the "full
   prior row state; NULL on creation (INSERT)", so the minted link — an INSERT
   — carries NULL, while each re-pointed origin is an UPDATE and carries the
   complete pre-mutation row. Recording an update as though it were an
   insertion would leave the audit unable to say which origin was replaced,
   which is precisely what makes the change reviewable and undoable
   (Invariant 11).

   Preserved manual rows rotate their `transaction_id` once here, since the
   account slot feeding the hash changes value. That is the rotation step 5
   NULLs the stale predictions for, and it happens once: after this migration
   the minted key never changes, so a manual row's identity is stable for good.

7. On `raw.import_log`, rename `source_file` to `source_path` and relax it to
   nullable, then add `source_document_key` as a **nullable** column, filled by
   truncating `file_sha256` where one exists. That is a derivation, not a
   backfill: R1's key is exactly that truncation of exactly those bytes. It is
   nullable because this table's key is `import_id` and the document key is
   informational here; NULL for a pre-V046 batch is the honest answer, and
   minting a stand-in would put a value into a column that names bytes nobody
   hashed.
8. Rename `app.match_decisions.source_transaction_id_a/_b` to
   `identity_component_a/_b`. Values are kept as-is: decisions on rows that do
   not rotate re-anchor on the next import, and decisions on tabular native-id
   rows orphan.
9. Log, at INFO, that a re-import is required, and that `moneybin doctor` lists
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
  — **every structural step is guarded on the shape actually present, because a
  fresh profile reaches this migration already current.** `Database` calls
  `init_schemas` before it runs the migration runner (`database.py:656-681`), so
  a brand-new database is built straight from the canonical DDL — `source_path`
  and both new keys already in place — and only *then* does the runner find V052
  unapplied and execute it against a schema that needs none of it. An
  unconditional `ALTER TABLE … RENAME COLUMN source_file TO source_path` fails on
  a column that does not exist, and a failed migration raises `MigrationError`
  and aborts the open (`database.py:700-704`). So the failure lands on **every
  new profile**, not on the legacy databases the step was written for — the one
  population that has nothing to migrate is the one that cannot open.

  The `ADD COLUMN IF NOT EXISTS` idiom the recent migrations use
  (`V050__add_plaid_account_identity_fields.py:31-36`) has no rename counterpart
  in DuckDB, so each rename, drop, and recreate probes the current catalog and
  no-ops when its target shape is already there. Note this is the one part of the
  migration path that is *not* cruft to be deleted later: the thin-migration
  posture is about not over-engineering the legacy path, and this is what makes a
  fresh install work at all.
- `src/moneybin/sql/schema/app_sync_recovery_state.sql` — the pre-clear
  baseline and `incomplete` marker V052 writes before step 2 clears the Plaid
  tables.
- `src/moneybin/schema.py` — add `app_sync_recovery_state.sql` to
  `_NON_PROVIDER_SCHEMA_FILES` (`:57`), the closed list `create_all_schemas`
  reads (`:127`). A DDL file that is not on this list is never executed, so
  V052's baseline write fails on a table that does not exist — and the table
  vanishes again on the promised reset that deletes V052.
- `src/moneybin/tables.py` — a `TableRef` for `app.sync_recovery_state`.
  `SyncRecoveryRepo`, `SyncService`, and doctor all reach it through
  `moneybin.tables` rather than a literal (AGENTS.md's Key Abstractions), so
  without the constant there is no sanctioned way to name it.
- `src/moneybin/repositories/sync_recovery_repo.py` — `SyncRecoveryRepo`, so
  the marker is read and cleared through the repository layer like every other
  `app.*` table (Invariant 10). V052 itself writes below it, as it does for
  `app.account_links`.
- A source-scan guard for R14 plus its behavioural partner.

### Files to Modify

**Identity derivation**

- `src/moneybin/services/import_service.py` — split `_bare_account_key`
  (`:947`) into a document-key function and a minting account-key function;
  call `accepted_native_keys_for_account` from the unpinned branch (R7); pin
  `source_origin` (R8); stop seeding any key path from the filename stem
  (`:3302`, `:3408` keep the stem as a *suggestion* only); and mint or remember
  a key per file-stated account in the **multi-account branch** instead of
  `slugify(name)` (`:3358`, and `:3380` for the per-account institution) — R4
  governs it, and it is the branch an earlier draft mistook for the
  `--account-name` exception.
- `src/moneybin/services/account_resolver.py` — confirm
  `accepted_native_keys_for_account` needs no scoping change for its new
  unpinned caller.
- `src/moneybin/services/sync_service.py` — `pull` (`:85`) resolves the
  `app.sync_recovery_state` row for that connection after a `force=True` run
  completes its load, stamping the recovered count and marking it `recovered`
  or `short`. It does not delete the row: the completed pull is the moment the
  shortfall becomes knowable, so deleting here destroys the evidence as it
  appears. Without the resolve the marker never retires and doctor reports
  every recovered database as incomplete forever.
- `src/moneybin/synthetic/writer.py` — emits `source_file` on five raw tables
  (`:143`, `:175`, `:202`, `:223`, `:260`) and the removed tabular
  `transaction_id` (`:253`), and supplies neither new column. `Database.ingest`
  inserts `BY NAME` (`database.py:980`), so after V052 every synthetic frame
  fails on an unknown column or a `NOT NULL` violation — which takes `moneybin
  demo` down with it. This is a producer, and §Raw-table producers applies to
  it in full.
- `src/moneybin/synthetic/reset.py` — **this one is not a rename, and treating
  it as one opens a data-loss hole.** `_SYNTHETIC_ROWS` predicates on
  `source_file LIKE 'synthetic://%'` (`:36`) under a comment (`:37-39`) that
  states the exact failure R12 is about to cause: "Relies on `source_file`
  being NOT NULL in every table above… If that constraint is ever relaxed, this
  negation starts evaluating to NULL — not TRUE — for those rows, and they go
  invisible to the real-data guard below."

  Follow the rename naively and that prediction comes true, in the fail-**open**
  direction. `_NON_SYNTHETIC_ROWS` (`:41`) is `NOT (…)`, and it is what
  `has_any_user_content` counts to decide whether a database holds anything the
  generator did not write (`:150-152`). A real row with a NULL `source_path`
  makes that predicate NULL rather than TRUE, so it is not counted, so a
  database holding real user data can report **no user content** — and the
  destructive reset proceeds. The docstring already states the opposite
  posture: "For a database we CANNOT attribute to the generator, there is no
  such thing as a safe table."

  So the two directions get fixed asymmetrically, each failing closed.
  `_SYNTHETIC_ROWS` stays a bare `LIKE`, which already excludes NULL, so only a
  proven-synthetic row is ever deleted. `_NON_SYNTHETIC_ROWS` becomes
  `source_path IS NULL OR NOT (source_path LIKE 'synthetic://%')`, so an
  unattributable row counts as user content and blocks the reset. Absence of
  evidence that a row is ours must never be read as evidence that it is not
  the user's.

  This also answers R10 rather than contradicting it. R10 forbids `source_path`
  as an *identity* discriminator — "did these two rows come from the same
  physical import?" — which is `source_document_key`'s job now. "Did our own
  generator write this row?" is a different question about a `synthetic://`
  sentinel the generator itself writes and no user path ever produces. What the
  column may not do is answer that question by silence.
- `src/moneybin/services/doctor_service.py` — report each still-marked table
  with its pre-clear count beside its current one, and say plainly that a
  shortfall after a completed forced pull is the provider window and is
  permanent.
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

- The seven further schema files that carry `source_file` without holding it
  in a primary key, each taking `source_document_key` as a plain column and
  R10's `source_file` → `source_path` rename (§Data Model lists all eight,
  `raw_import_log.sql` being the eighth and already named above):
  `extractors/plaid/schema/raw_plaid_transactions.sql`,
  `raw_plaid_accounts.sql`, `raw_plaid_balances.sql`,
  `raw_plaid_investment_transactions.sql`, `raw_plaid_securities.sql`;
  `extractors/ofx/schema/raw_ofx_institutions.sql`; and
  `sql/schema/raw_pdf_seeds.sql`. Note the two directories: **only**
  `raw_import_log.sql` and `raw_pdf_seeds.sql` are under `sql/schema/`, and
  every other raw schema this spec touches is under
  `extractors/<channel>/schema/`.

**Raw-table producers** — a schema change alone does not load. `Database.ingest`
inserts `BY NAME`, so every extractor and transform that builds a DataFrame for
a changed table must emit the new columns, or the first import after V052 fails
on an unknown column or a missing `NOT NULL`. Each channel's producer moves
together with its schema: the tabular and OFX transforms, the Plaid extractor,
and the Google Sheets adapters. Treat "the schema file is edited" as half the
change for every table listed above.

**`.claude/rules/identifiers.md` § Source-Provided IDs** — a binding rule
file, and R13 puts it in direct contradiction rather than merely out of date.
It currently sanctions the behaviour R13 forbids, in as many words: the suffix
"is disambiguation of a broken source id, not a switch to strategy #2"
(`:197`). It then documents the superseding CTE's mechanism as settled design
(`:199`) and derives three generalized rules from it (`:203-205`), one of which
— "Split the id by position and join on equality" — describes machinery R13
deletes. Left alone, the repo would hold a rule file calling the same write
legitimate that this spec calls synthesis, plus rules generalized from code
that no longer exists.

Rewrite it **as part of implementing R13**, not before: until the change ships,
that section accurately describes what the extractor does, and editing it early
would make a binding rule describe code that does not yet exist. What changes:
the exception moves from "append to `source_transaction_id`" to "write the
repair into `source_row_key` and leave the source's id verbatim"; the
supersession paragraph loses the suffix-splitting and keys on `fitid_repaired`;
and the `LIKE`-wildcard warning survives as the general rule it always was,
detached from this instance. The R11 precedent applies — a summary that
contradicts the shipped design is corrected in the change that causes the
contradiction.

**`stg_ofx__transactions` supersession** — the OFX staging model does two
things with the repair suffix, and R13 removes the suffix from under both.

Its dedup window (`:61`) partitions on `(source_transaction_id, account_id)`
and must move to `(identity_component, account_id)` for the reason above.

Its `superseding` CTE (`:27-43`) is the subtler half. It exists for a
cross-import case: a file imported *before* a collision appeared leaves a bare
row that a later import cannot overwrite, because the two carry different raw
keys, so both reach core and one real transaction is counted twice. The CTE
finds repaired rows and derives the id they supersede by splitting at the last
`#` — and its own comment carefully justifies splitting by position rather than
by pattern, since `_` and `%` are `LIKE` wildcards and a prefix test would be
quadratic.

**All of that machinery disappears, because it existed only to undo the
suffix.** After R13 a repaired row already holds the superseded id verbatim in
`source_transaction_id`, so `superseded_transaction_id` *is* that column: no
`LEFT`/`STRPOS`/`REVERSE`, no `#` search, and none of the reasoning about
reserved characters. Detection is `fitid_repaired` alone, which the comment
already calls the only admissible proof. Note the direction of the change —
this is not a simplification bought by accepting risk; the column now means
what it says, and the string surgery was the cost of it not meaning that.

One new condition is required, and omitting it inverts the fix: the anti-join
(`:84-98`) must apply only to rows where `fitid_repaired` is false. A repaired
row now matches its own superseded id and its own content, so an unguarded
anti-join makes every repaired row suppress *itself* — turning a
double-counting bug into a deletion bug. The guard also states the intent
better than the old suffix test did: only a stale **bare** row is ever
suppressed.

**`moneybin import history` table output** — `import_cmd.py:2311` reads
`rec.get("source_file", "")` and feeds it to the displayed path. It is a
`.get` with a default, so the rename does not raise here; the column simply
renders blank for every record while the JSON output beside it carries the
populated `source_path`. A silent blank column is the failure mode a rename
sweep that greps only for subscript access misses, and the heading and help
text on the same command carry the old word too.

**Import-log producers and readers** — `raw.import_log` is written by every
channel, and the enumeration above named only the `find_existing_import`
predicate. The writers break first and hardest: `loaders/import_log.py:129`
names `source_file` in its `INSERT` column list (bound at `:135`, declared at
`:89`, and listed in the module's column tuple at `:45`), and
`connectors/gsheet/pull_service.py:282` has its own `INSERT` naming the same
column. Both raise a missing-column error on the **first** file, manual, or
Google Sheets import after V052 — before any data loads. Three history readers
project it as well (`import_log.py:244`, `:255`, `:333`), and a second
signature carries it at `:374`. The rename is a module-wide change to
`loaders/import_log.py` plus the gsheet writer, not an edit to one predicate.

**`AccountLinksRepo` persists `source_label`** — R4's new column needs a write
path or it is decorative. `_ACCOUNT_LINKS_COLUMNS`
(`repositories/account_links_repo.py:26-38`) lists eleven columns and does not
include it, and `insert` has a fixed signature and `INSERT` column list that
likewise omit it. Left alone, every new link stores NULL, the label lookup
misses every time, and the unattended re-import R4 promises never happens —
the same defect as having no column at all, one layer down. The column tuple
feeds the audited row snapshot, so adding it there also keeps
`before_value`/`after_value` complete (Invariant 11); a repo that writes a
column it does not serialize would make the audit silently partial.

The same file gains `relabel` per R4 — the audited UPDATE that attaches a new
label to an existing accepted link. `insert` cannot serve the rename (the tuple
is unchanged, so `_guard_uniqueness` rejects it) and `repoint` is a different
verb (it moves the link to another `account_id`), so without it the remembered
rename has no write path at all.

**Per-account overlap evidence** — `_gate_account_proposals`
(`import_service.py:2214`) applies its one `incoming_transactions` sequence to
every proposal's candidates (`:2296-2309`), so each source account is scored
against the whole file's rows. That is harmless today: only the two PDF paths
pass the argument (`:4113`, `:4781`), and a PDF states one account. It stops
being harmless the moment the multi-account tabular branch above gates with
evidence, because a file holding accounts A and B would offer A's proposal
matches drawn from B's rows — overlap evidence that reads as corroboration and
is nothing of the kind, on the exact surface §Magic stays visible says must be
calibrated to certainty. Filter the sequence by `source_account_key` per
proposal.

**Google Sheets soft-delete state machine** — the gsheet transactions adapter
is not only a producer, and treating it as one leaves the channel broken. R13
drops `raw.tabular_transactions.transaction_id`, and
`connectors/gsheet/adapters/transactions.py` uses that column as the identity
of its diff: `:287` builds `current_ids` from `df["transaction_id"]`, `:291`
selects the connection's still-active ids by it, `:317` soft-deletes by it, and
`:270` documents the state machine in its terms. `connection_service.py:566`
orders by it as well — the same line §R10 already lists for its `source_file`
half, so that one line takes both edits, not one. A producer-only instruction
leaves all five, and the first Google Sheets pull after V052 fails on a missing
column before it loads a row.

The replacement is `source_row_key`, **not** the identity expression. A review
proposed `COALESCE(source_transaction_id, source_row_key)` here; that is the
wrong value twice over. `source_row_key` is `NOT NULL` and computed for every
tabular row (§Added columns), so it is total on its own, and it is the leading
component of the table's new primary key — which is exactly the
within-document row identity a soft-delete diff needs. The `COALESCE` pair, by
contrast, is the *identity hash* input, which R13 confines to one selection
plus the two staging dedup windows, with a guard pinning the occurrence count
at three; a fourth one here would fail that guard.

**`source_transaction_id_a/_b` consumers** — R13 renames both anchor columns of
`app.match_decisions` to `identity_component_a/_b`, and the name appears in
eighteen files. The repository is the mandatory one — `MatchDecisionsRepo`
(`repositories/match_decisions_repo.py:28,31,103,106,131`) names the columns in
its column tuple, its signature, and its `INSERT`. Beyond it: `matching/`
(`persistence.py`, `reconciliation.py`, `engine.py`, `scoring.py`,
`assignment.py`, `transfer.py`), `services/matching_service.py`,
`services/doctor_service.py`, `services/categorization/queries.py`,
`mcp/tools/reviews.py`, `mcp/tools/transactions.py`, `privacy/taxonomy.py`,
`privacy/payloads/transactions.py`, and three SQLMesh models
(`core/bridge_transfers.sql`, `meta/fct_transaction_provenance.sql`,
`prep/int_transactions__matched.sql`). Enumerate with
`grep -rln source_transaction_id_a src/moneybin` rather than from this list —
it is accurate at the SHA in the header and nowhere else.

**Manual investment write path** — R13 renames
`raw.manual_investment_transactions.source_transaction_id` to
`source_row_key`, and the column is that table's primary key, so its writers
break. `InvestmentService` mints the value and inserts it
(`investment_service.py:1224-1239`), and `ManualInvestmentTransactionsRepo`
selects and updates by it. §Files to Modify previously named only
`_predict_investment_gold_key` (`:234`), which is the one site the rename does
*not* break — it is listed for R9's exclusion, not for this rename.

**The privacy classification registry** — `privacy/taxonomy.py`'s
`CLASSIFICATION` is compared against the live catalog by
`test_classification_registry_covers_every_app_and_core_column`
(`tests/moneybin/test_privacy/test_classification_registry_coverage.py:45`), so
an unclassified column is a failing gate, not a deferred to-do. The renamed
`core.dim_accounts.source_path` and the two new raw keys are already covered
above; the slice also adds `app.account_links.source_label` and **every column
of `app.sync_recovery_state`**, and neither is enumerated anywhere else in this
spec. `source_label` takes the account name's class, not `ref_value`'s (R4);
the recovery table holds table names, row counts, states, and timestamps, which
is `RECORD_ID`/`Tier.LOW` throughout.

**`EXPECTED_CORE_COLUMNS`** (`database.py:319-329`) — the core schema-drift
registry still requires `source_file` on `core.dim_accounts`. R10 renames that
column, and `check_schema_at_boot` (`mcp/server.py:97-160`) raises
`SchemaDriftError` when a registered column is missing after one self-heal
attempt. Leaving the registry stale therefore does not degrade gracefully: the
MCP server refuses to boot on every migrated database, and the recovery tool
lives inside the server that will not start.

**Manual write path** — the services that author manual rows, without which
R9's minted key exists in the schema and nothing ever writes it.

- `src/moneybin/services/transaction_service.py:1077` — `create_manual_batch`
  writes `entry["account_id"]` straight into the account slot (`:1150`) and
  feeds the same value to `_predict_manual_gold_key` (`:110,1127`). After R9 it
  must resolve or mint the account's `src_` key, persist the accepted
  `manual`/`user` link through `AccountLinksRepo` in the same transaction, and
  hash the minted key. Without this, every post-migration manual row resolves
  to a NULL canonical account once R15 removes the staging fallback.
- `src/moneybin/services/investment_service.py:234` — `_predict_investment_gold_key`
  keeps hashing the canonical `account_id`, because R9 excludes its table
  (§Data Model). It is listed so the exclusion is a decision on the record
  rather than a file nobody checked.

- `src/moneybin/sql/schema/raw_manual_transactions.sql` — R13's
  `source_transaction_id` → `source_row_key` rename, plus R9's repurposing of
  `account_id` to a minted source key. Neither R10's eight-file table nor the
  bullet below covers it, because manual carries no `source_file`; R13's prose
  warns that this exact omission fails R14's guard on day one, so it is listed
  in its own right.
- `src/moneybin/sql/schema/raw_manual_investment_transactions.sql` — the same
  `source_transaction_id` → `source_row_key` rename. Its `account_id` is **not**
  repurposed (§Data Model).
- `src/moneybin/sql/schema/raw_import_log.sql` — `source_file` → `source_path`,
  relaxed to nullable, plus a nullable `source_document_key` (§Migration step
  6). Its own key is `import_id` and does not move.
- `src/moneybin/sql/schema/app_account_links.sql` — a nullable `source_label`
  recording the in-file account label a binding was made under, so a recurring
  multi-account export can find its already-minted key (R4). Nullable because
  every pre-migration row has one and because the channels that state no label
  never will. It is a lookup handle, never an identity input, and it carries
  the sensitivity of the account name it was copied from — not `ref_value`'s.

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
| `moneybin_import_document_rebinds_total` | Counter | `source_type`, `outcome` | How often a known document key resolves to an account vs. reaches the confirm gate. Measures whether R6's confirm burden is once-per-import or worse. |
| `moneybin_import_account_keys_minted_total` | Counter | `source_type` | A rise without a matching import rise means keys are still churning. |
| `moneybin_import_remembered_key_reuse_total` | Counter | `source_type`, `hit` | Directly measures R7, on the population R7 can serve — files that state an account. A `hit=false` rate near 1.0 there means the unpinned path is not reaching the resolver. It is silent on identity-unknown files, which have no account to look up. |
| `moneybin_import_overlap_evidence_total` | Counter | `source_type`, `verdict` | R17's evidence on the identity-unknown path: `decisive` (one account, no near runner-up), `ambiguous`, or `absent` (a disjoint export). This is what §Open Questions turns on, and the only measurement that can answer it. |
| `moneybin_matching_pairs_blocked_total` | Counter | `tier`, `reason` | R12 changes what Tier 2b can see; this makes the change observable rather than inferred. |

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
    holds the institution's value verbatim, `source_row_key` is **also**
    populated (it is a primary-key component, so a native id does not excuse
    it), and the identity hash consumes the native id rather than the row key.
11. **Both** paths keep their transaction ids when rows are reordered. A row
    key counts occurrences within a group of identical rows, so reordering
    permutes interchangeable rows and leaves every key intact; asserting
    otherwise would push the implementation toward physical row position, which
    would rotate ids on an ordinary export reshuffle.

    What separates the two paths is **restatement**, so test that instead: an
    export that rewords a description or corrects an amount keeps its
    transaction id on the native-id path (the institution's id did not change)
    and rotates it on the row-key path (a hashed field did). That asserts the
    two paths are genuinely distinct rather than one path with a hidden
    fallback, and it asserts the thing that actually differs.

**Invariants**

12. Source-scan guard: no model places a source-native value into an
    `account_id`, and no `COALESCE` fills `source_transaction_id` (R14), with a
    behavioural partner asserting the same at runtime — a source scan alone
    cannot see a Python-side fallback.
13. `source_path` appears in no join, equality test, or hash input (R10).

**Migration**

14. Every re-derivable `raw.*` table is empty afterwards **and** each of the
    six preserved tables still holds every row it held before. Both halves
    are the test: an over-broad clear is silent data loss, and a missed clear
    leaves rows carrying a `NOT NULL` document key that nothing can fill.

    **The fixture seeds all 22 affected tables** — the eight reshaped in step
    1 (five emptied, three carrying rows), the eight cleared in step 2, and
    the six preserved in step 3. Seeding only some of them makes one half
    vacuous without failing: seed only the survivors and "every cleared table
    is empty" is trivially true of a table that started empty, which is
    exactly how an over-broad clear passes. Every destructive path in the
    migration must run against rows that were actually there.

    The three Plaid investment tables need both halves asserted on that same
    fixture: their rows survive step 1, **and** each row's
    `source_document_key` equals the hash of the `sync_{job_id}` its old
    `source_file` held. A test that only counts rows cannot tell a correct
    derivation from a constant.
15. Partial-failure: an interrupted migration leaves the database on the old
    schema, not half-rotated.
16. The stored gold-key prediction is NULL on every preserved
    `raw.manual_transactions` row, and `moneybin doctor` therefore reports
    those rows rather than suppressing them, until a transform materializes
    them into `core`. Its partner asserts the asymmetry rather than assuming
    it: every preserved `raw.manual_investment_transactions` row still holds
    the **same non-NULL** `investment_transaction_id` it held before the
    migration. That column is the canonical grain key, not a prediction, and
    nothing downstream can recompute it — a test that accepts NULL here would
    pass while every preserved investment event loses its identity.
17. `raw.import_log` keeps every batch row. `source_document_key` equals the
    truncation of `file_sha256` wherever that column is populated, and NULL
    for a pre-V046 batch — no batch acquires a minted stand-in.
18. A re-import after the migration reproduces the transaction ids that
    `app.*` curation already points at, for every **row carrying no native
    transaction id** whose account holds an accepted `app.account_links` row
    (after the gate decision, for a file that states no account). This is the
    test that makes the thin migration safe rather than merely cheap: it
    asserts R7 does the work the migration declined to do.

    **Scope this by row, not by account.** A remembered account key is
    necessary but not sufficient: R13 independently rotates tabular rows that
    *do* carry a native id, by moving their identity component off the
    MoneyBin-synthesized value and onto the institution's own. Those rows
    rotate however well R7 works — §Migration and On-Disk Impact names R13 and
    R4 as two independent rotation causes for exactly this reason. Written as
    "every account," this test would fail on correct behaviour the first time
    it met a native-id file.

    Two negative partners, then, one per cause: an account with no accepted
    link, whose ids rotate because R7 had nothing to remember; and a native-id
    tabular file under a fully remembered account, whose ids rotate because
    R13 changed the component. Doctor lists the orphaned curation in both.
19. Every preserved manual row's account slot is a `src_` key afterwards, and
    each distinct pre-migration account has exactly one accepted
    `app.account_links` row pointing back at the canonical id it had before.
    Its negative partner is the one that catches a half-applied step: joining
    `raw.manual_transactions` through `prep.stg_manual__transactions` returns
    the same canonical `account_id` per row as it did before the migration, and
    no row resolves to a `src_` value. `raw.manual_investment_transactions` is
    asserted **unchanged** in the same test, so the exclusion is pinned rather
    than assumed.

**Binding evidence (R17)**

20. A second file from a recurring **overlapping** export reaches the gate
    with its true account ranked first, carrying a non-zero `overlap_matched`.
21. A file overlapping nothing reaches the gate with no overlap evidence and
    still loads nothing. Absence of a signal is not itself a signal and must
    not promote a candidate.
22. A tabular gate and a PDF gate over the same ledger produce the same
    overlap numbers. R17 is a wiring change; a divergence means the two
    channels have forked.

**Migration `app.*` writes**

23. A seeded Plaid link and a seeded OFX link carry byte-identical
    `source_origin` values before and after V052, while a seeded
    fallback-origin tabular link takes R8's constant. The negative half is the
    point: an unscoped predicate passes the positive half alone.
24. V052 marks the cleared Plaid tables incomplete; a `pull(force=True)` that
    completes and returns the whole baseline resolves the row to `recovered`
    and doctor goes quiet; a `pull(force=True)` that completes and returns
    fewer rows resolves it to `short` and doctor still reports the shortfall.
    Both halves are required — asserting only the first cannot tell a resolving
    marker from one that deletes the evidence of a permanent loss, and cannot
    tell either from a marker that never retires.
25. A Google Sheets pull round-trips soft-delete across V052: seed a
    connection, drop a row from the sheet, pull, and the vanished row carries
    `deleted_from_source_at` while the survivors do not. This test passes today
    against `transaction_id`; the point is that it still passes with the column
    gone.
26. A multi-account tabular export re-imported after one of its in-file account
    labels is renamed yields byte-identical `transaction_id` values for the
    overlapping rows. Under `slugify(name)` keying, every id beneath that
    account changes, so this test fails against today's branch — which is why
    the branch is in scope rather than exempt.
27. An **unchanged** multi-account export re-imported a second time loads
    unattended: the `source_label` lookup returns each account's minted key and
    no proposal reaches the confirm gate. This is the half that makes minting
    an improvement rather than a regression, and it fails against a mint with
    no lookup behind it — which is what an earlier draft specified.
28. An OFX file with two distinct rows sharing one FITID keeps both rows
    after V052, and each row's `source_transaction_id` equals the institution's
    `<FITID>` **unsuffixed** while their `source_row_key` values differ. Both
    halves matter: asserting only that both rows survive passes against the
    current repair, which is what R13 forbids.
29. Both rows also survive `prep.stg_ofx__transactions`, and their
    `transaction_id` values differ in `core`. Assert at both layers: the rows
    are distinct in `raw` regardless, so a raw-only test passes while staging
    silently drops one, and that is exactly the defect an earlier draft
    shipped.
30. A bare OFX row from an import that predates the collision is still
    suppressed once the repaired rows arrive, and neither repaired row
    suppresses itself. The second half is the regression this round could have
    introduced: with the suffix gone, a repaired row matches its own superseded
    id and its own content, so an unguarded anti-join deletes it.
31. A confirmed multi-account import writes `source_label` on the link, and the
    paired `app.audit_log` `after_value` contains it. The second assertion is
    the one that catches a repo updated in its `INSERT` but not in its column
    tuple.
32. In a two-account file, the overlap evidence offered for account A's
    proposal is computed from A's rows alone. Seed B with rows that would match
    A's candidate and assert they do not appear in A's overlap: a test that
    only checks A's own rows are present passes against the unfiltered
    sequence.
33. A **fresh** profile initializes cleanly with V052 in the migration set:
    create a new database, assert the open completes and V052 records success.
    This is what an unconditional rename fails, and it fails on every new
    install — so a suite that only exercises upgrade-from-legacy fixtures
    reports green while no new user can open a database at all.
34. V052 refuses, **with no partial write**, when two accepted links share a
    `ref_value` under different fallback origins. Assert both links still carry
    their original `source_origin` afterwards: a migration that detects the
    collision only after rewriting the first row leaves exactly the duplicate
    state the refusal exists to prevent.
35. Two accounts sharing one in-file label under the same `(source_type,
    source_origin)` send **both** candidates to the confirm gate rather than
    binding either. Assert on the gate, not on a returned key — a lookup that
    silently returns its first match passes any test that only checks some key
    came back, which is the failure mode this requirement exists to stop.
36. Two accounts with **no** accepted link publish two distinct
    `core.dim_accounts` rows, each with a NULL `account_id`. Both halves are
    load-bearing: asserting only the NULL passes against a model that collapsed
    them into one row, and asserting only the row count passes against one that
    published their source-native keys.
37. An accepted dedup decision whose anchor rotates is reported by `moneybin
    doctor`. Assert the finding, not the row's survival — the row survives
    either way, and `app_match_decisions_account_fk` stays green throughout
    because both accounts still resolve.
38. A renamed in-file label re-binds to the **same** link row: `source_label`
    holds the new value, `link_id` and `ref_value` are unchanged, and an
    `app.audit_log` row carries the prior label in `before_value`. The
    unchanged `ref_value` is what makes item 26's ids stable; the audit row is
    what catches a relabel written as a raw UPDATE below the repository.
39. `moneybin import history` renders a non-empty source column for a migrated
    record in **table** output. The JSON path passes with or without the fix,
    so a test that only parses JSON is the one that misses this entirely.

## Synthetic Data Requirements

The generator needs a **recurring export series**: three files for one account
covering overlapping date windows, with rows appended at the top in one variant
and at the bottom in another, so the occurrence-index component of R13 is exercised
in both directions. Ground truth is the set of distinct transactions across the
series, so a test can assert the count after importing all three.

The series is needed in **two flavours** — one carrying a native
transaction-id column and one without — because R13 makes those genuinely
different code paths rather than one path with a fallback.

**Both flavours must survive row reordering** (test 11). A row key counts
occurrences within a group of identical rows, so a reshuffle permutes
interchangeable rows and changes no key; a generator built to make the
without-id flavour rotate on reorder would encode the wrong invariant and push
the implementation toward physical row position. What separates the flavours is
**restatement**, so the generator needs one further variant of each series: a
file with a reworded description and a file with a corrected amount. Those keep
the id on the with-id flavour (the institution's id did not change) and rotate
it on the without-id flavour (a hashed field did). Tests 9 through 11 assert
that asymmetry.

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

Six `app.*` tables are hard-coupled to `transaction_id`:
`transaction_categories` (PK), `transaction_tags` (PK), `transaction_notes`
(`NOT NULL` + index), `transaction_splits` (`NOT NULL` + index), and
`categorization_decisions` (`UNIQUE (transaction_id, attempt_number)`), and
`transaction_id_aliases`, whose `new_transaction_id` is a `NOT NULL` forwarding
target. The migration does not rewrite them.

`transaction_id_aliases` is the one worth stating separately, because the
migration silently falsifies the promise its own schema makes: the table exists
so "a held id stays resolvable -- never an orphan"
(`app_transaction_id_aliases.sql:1-5`). V052 preserves the alias rows while
R4 and R13 rotate the ids they forward *to*, so a preserved alias resolves an
old id to a `new_transaction_id` that no longer exists — a dangling forward
that reads as success at every call site that trusts the table's contract.
R16's doctor coverage includes it for that reason. `app.match_decisions` is **not** insulated as
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

- **The account-vocabulary work** — `nickname`, the `--account-name`
  decomposition, and the `reports.*.account_name` rename. Public-contract
  changes with their own review surface, tracked separately from this spec.
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
