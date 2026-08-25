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

### One reversal from the decision record

`transaction-identity-stability.md` §"The one genuinely open sub-decision"
recommends a **filename stem**, user-confirmed and scoped by source, as the
durable binding ref. **That recommendation is withdrawn.** A filename is not a
reliable signal of contents: exports arrive as `transactions.csv` and are
renamed by hand, so the stem is both unstable and non-unique. Document identity
is a content digest. Where the decision record and this spec disagree, this
spec governs.

## The governing rule

> A name says where a value came from, and no value may ever fall back from one
> name into another.

Every defect below is a place where one word means two things depending on the
data. The requirements are that rule applied to four columns.

## What is true today (verified at `de040465`)

`transaction_id` is `SUBSTRING(SHA256(source_type || '|' || source_origin ||
'|' || source_account_key || '|' || source_transaction_id), 1, 16)` —
`int_transactions__matched.sql:128-134` and `:163-169`.

Three of its four inputs move under ordinary user action:

| Input | Why it moves | Ref |
|---|---|---|
| `source_account_key` | For an unregistered tabular file it is `f"{slugify(file_path.stem) or 'file'}-{digest}"` — filename stem plus a digest of file **bytes**. A rename moves it; so does next month's export. | `import_service.py:947` |
| `source_origin` | Falls back to `slugify(account_name or "unknown")` when no format matches, so passing or omitting `--account-name` re-namespaces identity. | `import_service.py:3222` (MB-147) |
| `source_transaction_id` | For tabular without a native id, hashes `date\|amount\|description\|account_id\|row_number` — embedding the account key a second time. | `raw_tabular_transactions.sql:6` |

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
wiped database re-mints different keys; that is intended, and re-loading the
source files is the recovery path.

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
account slot becomes a minted per-account source key or the constant `'user'`.

### Provenance vs. identity

**R10.** `source_file` is renamed `source_path` and becomes informational
metadata. It MUST NOT appear in any join predicate, equality test, hash input,
`PARTITION BY`, or `ORDER BY` that affects a result. It may be displayed and
logged subject to R11.

**R11.** `source_path`'s privacy class is re-derived. Today `source_file` is
`DataClass.RECORD_ID` (`taxonomy.py:692`), which maps to `Tier.LOW` —
unmasked — while `core.dim_accounts` projects it at `dim_accounts.sql:269`
with the comment *"Path to the source file from which the winning record was
loaded"* and `meta.fct_transaction_provenance:14` projects the
transaction-level one. A path naming an institution and a last four is
therefore published unmasked into an agent-readable schema. The classification
is right about the column's name and wrong about what two of five channels put
in it.

**R12.** Wherever the match engine needs "did these two rows come from the same
physical import?", it keys on `source_document_key`, not on a path. Two sites:
the Tier 2b blocking guard `a.source_file != b.source_file`
(`scoring.py:284`) and the 1:1 assignment key
`(source_type, source_origin, source_file)` (`assignment.py:88`).

**R13.** The tabular row hash drops `description` and the account key, leaving
transaction date, amount, and a within-file ordinal. This is the hardening
ADR-015 already names as a follow-up, and it removes the account key's double
embedding identified in the table above.

### Invariants (testable as refusals)

**R14.** No `COALESCE`, `IFNULL`, or `CASE` may place a `source_account_key`
into an `account_id` column, a `display_name` into a `source_account_key`, or a
`source_path` into any key. A source-scan guard enforces this across
`src/moneybin/sqlmesh/models/`, paired with a behavioural test that a resolved
account never carries a source-native value in `account_id`.

**R15.** After R4–R8, `core.dim_accounts.account_id` holds only minted
surrogates, making its existing `DataClass.RECORD_ID` classification
unconditionally true rather than true-only-where-a-link-exists.

## Data Model

### New column

`source_document_key VARCHAR` on every `raw.*` transaction and account table
that today carries `source_file`, and on `raw.import_log`.

For channels with no file, the document key is the batch's existing stable
token, hashed the same way, so the column holds one kind of value everywhere:

| Channel | Document key input |
|---|---|
| OFX / tabular / PDF | file bytes |
| Plaid | `sync_{job_id}` |
| Google Sheets | `gsheet://{spreadsheet_id}/{sheet_gid}` plus the pull's row digest |
| Manual | the minting batch token |

`raw.import_log.file_sha256` is the existing full digest and stays; the new
column is the truncated form used for joins, so the two agree by construction.

### Renamed column

`source_file` → `source_path`, nullable, informational. Retained rather than
dropped: it is the only human-legible pointer back to the artifact on disk, and
dropping it would make a failed import unexplainable.

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
2. Rename `source_file` to `source_path`.
3. Recompute `transaction_id` for every affected row and rewrite the
   `transaction_id` held by the five hard-coupled `app.*` tables named in
   §Migration and On-Disk Impact, verifying row counts before and after —
   nothing cascades, so a missed table fails silently.
4. Refuse to run partially: either every table is rotated or none is.

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
- `src/moneybin/extractors/tabular/schema/raw_tabular_transactions.sql` — R13.

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

**Models** — the 22 models referencing `source_file`, of which the load-bearing
ones are `int_transactions__unioned.sql`, `int_transactions__matched.sql`,
`stg_tabular__transactions.sql`, `stg_plaid__investment_holdings.sql`,
`int_plaid__opening_positions.sql`, `stg_plaid__opening_lots.sql`,
`core/dim_accounts.sql`, and `meta/fct_transaction_provenance.sql`. For Plaid,
gsheet, and manual the *value* does not change — those columns already hold a
batch token — so those edits are a rename.

**Privacy**

- `src/moneybin/privacy/taxonomy.py:692` and every other `source_file` entry —
  R11.

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

**Invariants**

9. Source-scan guard: no model places a source-native value into an
   `account_id` (R14), with a behavioural partner asserting the same at
   runtime — a source scan alone cannot see a Python-side fallback.
10. `source_path` appears in no join, equality test, or hash input (R10).

**Migration**

11. Round-trip: a database with curation on rotated ids retains every
    category, note, tag, split, and match decision.
12. Partial-failure: an interrupted migration leaves the database on the old
    schema, not half-rotated.

**Privacy**

13. A `source_path` containing an institution name and four digits does not
    reach an unmasked read surface (R11).

## Synthetic Data Requirements

The generator needs a **recurring export series**: three files for one account
covering overlapping date windows, with rows appended at the top in one variant
and at the bottom in another, so the row-ordinal component of R13 is exercised
in both directions. Ground truth is the set of distinct transactions across the
series, so a test can assert the count after importing all three.

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
in `app.*` is the only thing that cannot be recomputed, which is why step 4 of
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

## Open Questions

1. Does any path currently stored in a live `raw.*.source_file` contain an
   institution name plus digits? R11 is a latent gap or an active one depending
   on the answer, and that changes only its urgency, not its fix.
2. How many of the real files in use carry a native transaction-id column?
   Those files never touch the derived row hash, so the answer sizes R13's
   blast radius — it does not change R13's correctness.
