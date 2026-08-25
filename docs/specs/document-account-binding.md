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
| `source_transaction_id` | The union aliases the tabular *fallback* column into this name (`transaction_id AS source_transaction_id`) and discards the genuinely-native one, so it is the institution's id on some rows and a MoneyBin hash of `date\|amount\|description\|account_id\|row_number` on others — re-embedding the account key a second time. | `raw_tabular_transactions.sql:6`, `int_transactions__unioned.sql:82` |

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
the artifact's bytes. No filename, path, mtime, size, or caller-supplied value
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

| Table | Current primary key |
|---|---|
| `raw_tabular_transactions.sql:33` | `(transaction_id, account_id, source_file)` |
| `raw_tabular_accounts.sql:19` | `(account_id, source_file)` |
| `raw_ofx_transactions.sql:19` | `(source_transaction_id, account_id, source_file)` |
| `raw_ofx_accounts.sql:14` | `(account_id, source_file, extracted_at)` |
| `raw_ofx_balances.sql:16` | `(account_id, statement_end_date, source_file)` |
| `raw_plaid_investment_holdings.sql:27` | `(account_id, security_id, source_origin, source_file)` |
| `raw_plaid_investment_holdings_snapshots.sql:22` | `(source_origin, source_file)` |
| `raw_plaid_investment_holding_lots.sql:18` | `(account_id, security_id, source_origin, lot_index, source_file)` |

Each of these reads "one row per business key **per document**" — the schema
has been asserting document identity all along and spelling it with a path.
`source_document_key` takes the `source_file` slot in all eight. That is why
the substitution strengthens the model rather than working around it: the PKs
become true statements instead of accidentally-true ones.

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
moves to a separate column, `source_row_key`.

This is the fifth fallback, and it is currently invisible because the two
columns already exist and are then collapsed. `raw.tabular_transactions` is
honest at the raw layer: `source_transaction_id` is *"Institution-assigned
unique transaction identifier if present"*, kept beside a distinct
`transaction_id` that is *"source_transaction_id when available, else SHA-256
hash of `date|amount|description|account_id|row_number`"*
(`raw_tabular_transactions.sql:20,6`). Staging carries both
(`stg_tabular__transactions.sql:60`). Then
`int_transactions__unioned.sql:82` aliases `transaction_id AS
source_transaction_id` — promoting the **fallback** column into that name and
discarding the honest one. Downstream, a MoneyBin-synthesized surrogate is
indistinguishable from an institution-assigned id.

Plaid's equivalent aliasing at `:119` is *correct* and stays: its raw
`transaction_id` genuinely is Plaid's own id.

Required:

- `source_transaction_id` carries the institution's id where the source
  supplies one, and NULL where it does not. No `COALESCE` may fill it.
- `source_row_key` carries MoneyBin's within-document row identity, derived
  from transaction date, amount, and an **occurrence index scoped to that
  `(date, amount)` pair** — dropping `description` (which a bank may re-render)
  and the account key (already an identity-tuple component, so embedding it
  twice compounds any wobble). This is the hardening ADR-015 already names as a
  follow-up.

  **The occurrence index must not be a row position.** Today the row hash uses
  `row_number`, raw file order (`transforms.py:142`, no sort anywhere in the
  module). A position moves whenever the export window shifts, so a Feb row
  sitting at offset 40 in a Jan–Mar export sits at offset 8 in a Feb–Apr one —
  and a position-derived key would rotate its id, defeating this spec's Goal
  for precisely the recurring-export case it exists to fix. An occurrence index
  is stable under that shift: a Feb 15 / −52.30 row that is the only one of its
  `(date, amount)` is occurrence 1 in both files.

  The cost, stated plainly: two genuinely distinct transactions sharing a date
  and an amount become interchangeable under this key, because `description` is
  gone. They still both exist and still both get ids — but which id attaches to
  which row is arbitrary, and if their relative order flips between exports the
  two ids swap. That is acceptable only because the two rows are, by
  construction, indistinguishable on every field the key retains; a user who
  curated one and not the other could see that curation move. Sources with a
  native id never touch this path.
- The identity hash consumes `source_transaction_id` when present and
  `source_row_key` otherwise. That selection happens **once**, explicitly, in
  the identity derivation — not by a fallback hidden inside a column's meaning.

The payoff is that "does this source give us stable ids?" becomes a question
the data answers. Today it cannot be asked: every row has a
`source_transaction_id` whether or not the institution supplied one.

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
always-propose rule delivers: a file that reaches the confirm gate produces an
`app.account_links` row before load, so the join always hits and the `COALESCE`
has nothing left to fall back to. The thirteen models must then drop the
arm — otherwise a dead branch keeps the ambiguity alive in the code and in
`taxonomy.py`'s reasoning about the column.

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
Shipped separately they would mean five rotations, five migrations, and five
windows in which a user's curation can orphan.

The migration must:

1. Add `source_document_key` and backfill it. For rows whose source artifact is
   gone, backfill from `raw.import_log.file_sha256` where present; where absent
   (pre-V046 batches), mint a per-batch key so the column is never NULL.
2. Add `source_row_key` and **recompute** it from the raw columns
   (`transaction_date`, `amount`, occurrence index), not by copying the existing
   `transaction_id` hash — that hash still contains `description` and
   `account_id`, the two components R13 removes, so copying it would leave
   every migrated row carrying a key no fresh import can reproduce and break
   the `--force` idempotence test. Then drop the fallback `transaction_id`
   column.

   `source_transaction_id` needs no null-out pass: in
   `raw.tabular_transactions` it is *already* NULL for exactly the rows whose
   id was MoneyBin-derived (`raw_tabular_transactions.sql:20`) — the derived
   value only ever lived in the separate `transaction_id` column. The
   correction is at the union (`int_transactions__unioned.sql:82`), not in the
   data.
3. **Mint replacement account keys.** For every distinct existing
   bytes-derived `source_account_key` in `raw.*`, mint an opaque key, rewrite
   the `raw.*.account_id` values that carry it, and update the matching
   `app.account_links.ref_value` rows in the same transaction. Without this
   step R4 and R15 are not achieved on any existing database and step 5's
   rotation is largely a no-op, because the account slot would rehash to the
   same value. Where two bytes-derived keys already resolve to one
   `account_id`, they collapse to one minted key — which is the duplicate this
   spec exists to stop creating.
4. Swap `source_document_key` for `source_file` in the eight primary keys
   (R10), then rename the leftover to `source_path` and make it nullable.
5. Recompute `transaction_id` for every affected row and rewrite the
   `transaction_id` held by the five hard-coupled `app.*` tables named in
   §Migration and On-Disk Impact, verifying row counts before and after —
   nothing cascades, so a missed table fails silently.
6. Refuse to run partially: either every table is rotated or none is.

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

**Raw schemas** — the eight files in R10's table, each swapping
`source_document_key` for `source_file` in its PRIMARY KEY.

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
`(source_document_key, source_row_key, account_id)`. It must **not** repartition
on `source_transaction_id` alone: after R13 that column is legitimately NULL for
every no-native-id source, which would collapse an entire document into one
partition and delete all but one row.

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
| `import_remembered_key_reuse_total` | Counter | `source_type`, `hit` | Directly measures R7. A `hit=false` rate near 1.0 means the unpinned path is not reaching the resolver. |
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

14. Round-trip: a database with curation on rotated ids retains every
    category, note, tag, split, and match decision.
15. Partial-failure: an interrupted migration leaves the database on the old
    schema, not half-rotated.
16. Backfill: a pre-V046 batch with no `file_sha256` gets a minted per-batch
    document key rather than a NULL.

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

**This breaks existing databases and requires V052 to be run.** Every
transaction id derived from an unregistered tabular file rotates. Databases
whose transactions come only from OFX, Plaid, or registered tabular formats
with native transaction ids are unaffected in their ids but still take the
column rename.

Five `app.*` tables are hard-coupled to `transaction_id` and must be rewritten
in the same transaction as the rotation: `transaction_categories` (PK),
`transaction_tags` (PK), `transaction_notes` (`NOT NULL` + index),
`transaction_splits` (`NOT NULL` + index), and `categorization_decisions`
(`UNIQUE (transaction_id, attempt_number)`). The other four tables the brief
listed are insulated — `match_decisions` keys on
`(source_type, source_transaction_id, account_id)` rather than the gold id,
`lot_selections` follows a separate `investment_transaction_id` lineage,
`proposed_rules.sample_txn_ids` is illustrative, and `audit_log.target_id` is a
historical record where a stale id is arguably correct.

**No foreign key is declared anywhere in `app.*`** — `FOREIGN KEY` and
`REFERENCES` appear in none of the schema files. So nothing cascades and
nothing errors: a rotation that misses a table orphans that curation silently.
The migration must enumerate the five tables explicitly and verify row counts
before and after, rather than relying on the database to complain.

**Doctor coverage is thinner than the coupling.** `_run_orphan_app_state`
(`doctor_service.py:713`) checks `transaction_notes` and `transaction_tags`
only. `transaction_categories`, `transaction_splits`, and
`categorization_decisions` have no orphan detection, so today a bad re-key
drops the highest-value curation in the system with nothing flagging it.
Closing that gap is worthwhile independent of this spec, and it is what makes
the migration's before/after verification checkable afterwards.

Recovery, if the migration is abandoned: the raw layer is unchanged by identity
rotation, so re-running the transform reproduces the derived layers. Curation
in `app.*` is the only thing that cannot be recomputed, which is why step 6 of
the migration refuses partial application.

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
  natural successor; not required for identity stability.

## Decisions Taken

Recorded here because both were live questions during design, and a later
reader would otherwise reopen them.

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

## Open Questions

None blocking. One sizing question remains: what fraction of real files carry a
native id column. That changes how often the `source_row_key` path is
exercised, not whether either path is correct — and after R13 it becomes a
question the data can answer, because a NULL `source_transaction_id` will mean
exactly "this source assigns no id."
