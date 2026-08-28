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

**One exception, named rather than implied: `--account-name`.**
`_label_account_key` (`import_service.py:878`, called at `:2424` and `:3331`)
derives an account key from the caller's display label, and this spec leaves
that path alone — its decomposition is Out of Scope (§Testing Strategy). Stating
R4 as an unqualified "never … or a display label" would make it an invariant
the shipping code violates on day one, which is worse than an honest exception:
a guard nobody can turn green gets disabled, and then it guards nothing.

The exception is bounded and self-retiring. It fires only when a caller passes
`--account-name`, it is the one label-derived key in the codebase, and it
disappears with the account-vocabulary work that decomposes `--account-name`.
Until then R4 governs every key the *file* produces, which is the population
this spec is about.

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
that mints: any `app.account_links` row whose `source_origin` names no
registered format takes the constant. The predicate is decidable rather than a
guess, because registered names are enumerable — `app.tabular_formats.name` and
`app.pdf_formats.name` are both `VARCHAR PRIMARY KEY`. This is a link-table
rewrite, not an identity backfill: the transaction ids still rotate, which R8
already accepts. What it preserves is the *binding decision*.

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

**Nine query sites use it as a batch discriminator** — ten line references,
because `import_log.py:410,415` is a single site spanning two lines of one
query (both bound by the same parameters at `:426`) — all of which move to
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

`source_row_key VARCHAR NOT NULL` on `raw.tabular_transactions` and any other
raw transaction table whose source may not supply a native id. Carries
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
than maintained: it carries no backfill or value-rewriting logic to unwind, and
nothing else may depend on having run it. The three writes above are consistent
with that. The minted keys and the re-pointed origins become ordinary
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
   each. They are recreated **empty**; DuckDB cannot `ADD` or `DROP` a primary
   key, so each is a create-drop-rename regardless, and carrying rows across
   would require backfilling a `NOT NULL` key component that only a re-import
   can compute.
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
   table until the marker is cleared. The marker is what makes this fail-closed
   rather than a guess: a missing baseline reads as "not yet recovered," never
   as "fine." This is the one piece of durable state the thin-migration posture
   requires, and it exists precisely because everything else about the posture
   is deliberately disposable.

   **The count is evidence, not the clearing condition.** Clearing on "count
   met or exceeded" would report a false recovery. A forced pull cannot return
   history older than the provider's window, and ordinary new transactions
   accumulate afterwards, so a table can regain its old count — or exceed it —
   while every row the migration destroyed stays permanently gone. A count
   cannot separate those two states, because they are not the same rows.

   The marker is therefore cleared only by a **completed forced pull** for that
   connection, and doctor reports the pre-clear count beside the current one
   instead of asserting they are equivalent. Where the forced pull returns
   fewer rows than the baseline, that shortfall is the provider window and it
   is permanent: doctor says so once, plainly, rather than waiting on a count
   that will never arrive or clearing on one that means nothing.
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
   `UPDATE`, every `app.account_links` row whose `source_origin` names no
   registered format — `NOT IN (SELECT name FROM app.tabular_formats)` and
   likewise for `app.pdf_formats` — takes R8's constant. Without it those links
   become unreachable the moment R8 lands, because
   `accepted_native_keys_for_account` filters on `source_origin`
   (`account_resolver.py:802-808`), and the identity-unknown accounts get
   re-asked a question they already answered. It belongs here rather than in
   its own step because it touches the same table in the same transaction as
   the mint.

   These writes are V052's only `app.*` writes and its only departure from
   Invariant 10's "written only through `AccountLinksRepo`"
   (`app_account_links.sql:5`) — a migration runs below the repository layer.
   It writes the paired `app.audit_log` row itself (`actor='system'`,
   `before_value` NULL, one shared `operation_id`), because the invariant is
   about the audit row existing, not about which layer emitted it.

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

    **The fixture seeds all 22 affected tables** — the eight recreated in step
    1, the eight cleared in step 2, and the six preserved in step 3. Seeding only some of them makes one half vacuous
    without failing: seed only the survivors and "every cleared table is empty"
    is trivially true of a table that started empty, which is exactly how an
    over-broad clear passes. Every destructive path in the migration must run
    against rows that were actually there.
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
