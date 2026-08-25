# Account Vocabulary and Transaction Identity Stability

> Last updated: 2026-08-24
> Status: draft — decision material, **not a filed spec**. Deliberately absent
> from `INDEX.md` and `docs/roadmap.md`, and carrying no milestone address:
> filing it (address, 📐 roadmap entry, INDEX row per `.claude/rules/shipping.md`)
> commits the work and is the owner's call, not this document's.
> Relates to: M1S (Ingestion Core), refining
> [ADR-015](../decisions/015-transaction-identity-content-derived.md)
> Type: Architecture
> Owns: nothing. Proposes amendments to the `source_account_key` clause of
> ADR-015 and to the account-vocabulary contract in
> [`account-identity-resolution.md`](account-identity-resolution.md)
> Verified against: `de040465` (origin/main at time of writing)

## Recommendation in one page

**One rule settles both parts: a name says where a value came from, and no value
may ever fall back from one name into another.** Every defect below is a place
where a fallback lets one word mean two things depending on the data.

Three fallbacks blur the vocabulary today:

| Fallback | Blurs | Consequence |
|---|---|---|
| `COALESCE(links.account_id, <source>.account_id)` in nine `stg_*` models (4 transaction, 3 account, 2 balance) | `account_id` | The canonical surrogate silently becomes a source-native key — which for OFX **is the institution's account number** |
| `--account-name` seeding `_label_account_key` and `source_origin` | `account_name` | A display label becomes an identity input; passing or omitting the flag moves two of the four hash components |
| `_bare_account_key` synthesizing an account key from file bytes | `source_account_key` | A *document* key impersonates an *account* key, so it changes every time a recurring export grows |

Fix the fallbacks and both questions resolve, because the id hash's inputs become
stable exactly when each word has exactly one meaning.

**Part 1 — vocabulary.** The owner's five primitives are correct but incomplete
and one sensitivity label is wrong. Add a sixth, `source_document_key` (the
identity of one import artifact, which is what `_bare_account_key` and
`pdf_doc_*` actually produce). Correct `account_id` from "internal, safe" to
"safe **only where an accepted link exists**" — and then make that
unconditionally true by removing the fallback, rather than reclassifying the
column.

**Part 2 — identity.** Neither A nor B. Take **C: keep the id derived, and make
its inputs stable by remembering the account key instead of recomputing it.**
The mechanism already ships — PR #438 built `accepted_native_keys_for_account`
for exactly this — but only fires when `--account-id` pins the import. Extending
it to the unpinned path is a lookup that already exists, called from one more
branch.

The decisive argument is not a preference between shapes. It is that **A cannot
work**: the alias map forwards a *reference*, and the tabular failure produces
*two rows*, not one row with two ids. There is nothing for an alias to forward.
See [§Why the alias map cannot fix this](#why-the-alias-map-cannot-fix-this).

---

# Part 1 — The account vocabulary

## What is true today (verified at `de040465`)

Each claim below was checked against the code, not carried from the brief.

1. **`nickname` does not exist.** `grep -rn "nickname" src/` returns nothing.
   Confirmed — it is a proposal, not drift to reconcile.
2. **`account_name` already means two things on two shipped surfaces.**
   - `reports/balance_drift.sql:23`, `reports/cash_flow.sql:11`,
     `reports/large_transactions.sql:14` each publish
     `a.display_name AS account_name` — a rendered label.
   - `--account-name` on `moneybin import` is an identity seed:
     `_label_account_key` (`import_service.py:878`) turns it into the
     `source_account_key`.
3. **`--account-name` does four jobs plus a channel gate.** In
   `services/import_service.py`: display name (`:3299`), key seed (`:3331`,
   `:2426`), `source_origin` fallback (`:3222`), last-four extraction (`:3300`,
   `:3334`).
4. **Correction — the flag is not silently ignored off-channel.**
   `_HONORED_ACCOUNT_SIGNALS` (`:722`) lists `account_name` for `tabular` only,
   but `reject_unhonored_account_signals` (`:767`) **refuses** the import on the
   first unhonored signal. Its docstring names the silent-drop behaviour as the
   prior defect that "cannot be noticed." A user passing `--account-name` to OFX
   or PDF gets an error, not a silent discard. This is already correct.
5. **`display_name` cannot serve as a handle.** `dim_accounts.sql:273-287` is a
   nine-arm `COALESCE` terminating in the literal `'Unnamed account'`. Never
   NULL, not unique, and its own comment says "never an id."
6. **Correction — the `alias` at `dim_accounts.sql:62-68` is not an account
   alias.** It is built from `seeds.institutions` (`slug` and `display_name`,
   lowercased with non-alphanumerics stripped) and is used for **institution**
   matching. It has nothing to do with account display names.
7. **`last_four` is published** (`dim_accounts.sql:289`), derived per source,
   classified `INSTITUTION_ACCOUNT_NUMBER` in the taxonomy — i.e. masked.
   Confirmed never the full number.

## The sensitivity label on `account_id` is wrong

This is the most important Part 1 finding, and it is grounded in the taxonomy's
own stated rule rather than in judgement.

`privacy/taxonomy.py:672-675` classifies `core.dim_accounts.account_id` as
`RECORD_ID` (unmasked), justified by the comment *"Opaque minted canonical
surrogate (spec D6) — not PII."*

That justification is unconditional. The value is not:

- `dim_accounts.sql:199` computes `COALESCE(account_id, source_account_key) AS
  grain_key`, projected as `account_id` at `:231`.
- The dim's own comment (`:185-193`) states the COALESCE never fires *there*
  because "all three `stg_*__accounts` models already project
  `COALESCE(links.account_id, a.account_id)`" — the fallback happens one layer
  earlier, not never.
- So an account with **no accepted `app.account_links` row** publishes its
  source-native key in `dim_accounts.account_id`.
- For OFX that key is the `<ACCTID>`. The same taxonomy classifies it
  `INSTITUTION_ACCOUNT_NUMBER` in `raw.ofx_accounts` (`:1015-1019`), noting
  `source_account_key` "is that same column re-aliased by every `stg_ofx__`
  model."

The taxonomy states the deciding rule itself (`:1007-1011`): use
`ACCOUNT_IDENTIFIER` "where the column may hold a number or an opaque token and
**cannot be told apart**." Under the fallback, `dim_accounts.account_id` is
precisely that column. The `display_name` terminal arm is explicitly fail-closed
for this reason (`:284-287`) — the *label* refuses to name an account by its id,
while the *id column beside it* publishes the number unmasked.

**Recommendation: remove the fallback rather than reclassify the column.**
Reclassifying to `ACCOUNT_IDENTIFIER` would mask a column the entire design
wants unmasked and that agents use as a handle. Minting unconditionally — every
account gets a surrogate at resolution time, and an unresolved account is
represented by a provisional minted id, never by its source key — makes the
existing `RECORD_ID` classification true rather than aspirational.

> **Not yet proven:** whether an unresolved account can still reach `core` now
> that every channel gates before load on an unratified identity. If the gate is
> airtight the exposure is unreachable in practice — but the classification's
> premise is still conditional, and a fallback that "never fires" is one refactor
> away from firing. This needs a live check against a database with an
> unresolved account; I did not have one and did not manufacture the claim.

## The missing primitive: `source_document_key`

`_bare_account_key` and `pdf_doc_*` both hash bytes, and both are described as
account keys. Neither is. They identify **one import artifact**.

PDF gets away with it: a statement is one immutable document covering one
account, so document identity and account identity coincide, and the code is
honest about which it means (`pdf_doc_` prefix, `source_origin` pinned to the
constant `'document'` — `pdf_account_identity.py:12,74`).

Tabular does not: a recurring export is a *series* of documents for one account,
and each has different bytes. Calling its document key an account key is what
makes the key wobble. The `_bare_account_key` docstring already knows —
*"The digest is a disambiguator, NOT an identity claim."*

So the sixth primitive is not new machinery; it is a name for something two
channels already build. Naming it lets the two jobs separate: a document key is
excellent at idempotent re-import detection (which is what
`test_ofx_reimport_identity.py` exercises) and unfit for transaction identity.

## Settled vocabulary

| Primitive | The one meaning | Stored | User-editable | Unique | Privacy class |
|---|---|---|---|---|---|
| `account_id` | Canonical surrogate minted by MoneyBin. Never a source value. | `app.account_links.account_id`, projected to `core.dim_accounts` | No | Yes | `RECORD_ID` — true once the fallback is removed |
| `account_number` | The institution's real number | `app.account_links.ref_value` only | No | — | `INSTITUTION_ACCOUNT_NUMBER` / `ACCOUNT_IDENTIFIER`; never projected to `core` |
| `last_four` | Last four of the number, for disambiguation | `core.dim_accounts.last_four`, `app.account_settings` | Yes | No | `INSTITUTION_ACCOUNT_NUMBER` (masked) |
| `display_name` | Human-readable label for rendering. Never an id. | `core.dim_accounts.display_name`, override in `app.account_settings` | Yes | No | `USER_NOTE` |
| `nickname` | Short slug a person or agent types to *refer* to an account | `app.account_settings` | Yes | Yes (enforced) | `USER_NOTE` |
| `source_account_key` | What **one source** calls this account | `raw.*.account_id`, `app.account_links.ref_value` | No | Within `(source_type, source_origin)` | `ACCOUNT_IDENTIFIER` — may hold a real number |
| `source_document_key` | What identifies **one import artifact** | `raw.*.source_file` + content digest | No | Per artifact | `RECORD_ID` |

Two rules make the table hold:

- **Nothing falls back across a row.** No `COALESCE` may put a
  `source_account_key` into an `account_id`, or a `display_name` into a
  `source_account_key`.
- **Mutable names live at the boundary; immutable ids live in storage.**
  `display_name` and `nickname` are input/output conveniences.
  `account_id` and `transaction_id` are what artifacts store.

## The nickname's mutability question

The brief asks what happens on rename, and whether a nickname appears in output
that outlives the session. Answering the second settles the first.

**Recommendation: a nickname resolves at the input boundary and is never stored
in any artifact that outlives the session.** Exports, curation tables, the audit
log, and MCP response identity fields carry `account_id`. A nickname may be
*returned alongside* the id for display; it may never *be* the id.

Then rename is free — no alias table, no forwarding, no stale-handle errors —
because nothing durable ever pointed at it. This is the same rule as Part 2,
applied to accounts: make the mutable thing non-durable and the durable thing
non-mutable, and the forwarding problem disappears instead of being managed.

The alternative (nickname as a durable reference) recreates
`transaction_id_aliases` one level up, for an entity users rename casually.
Reject it.

## Decomposing `--account-name`

The owner's guidance — *"account name should be just one thing, and should
accept a display or slug form depending on context"* — resolves into two flags:

| Flag | The one job | Form |
|---|---|---|
| `--account-name` | Display label for an account being **created** | Display form: `"Everyday Checking"` |
| `--account` (generalizes today's `--account-id`) | Bind to an account that **exists** | Slug form: a `nickname` or an `account_id` |

The three identity jobs leave `--account-name`:

- **Key seed** (`_label_account_key`) — removed. The key comes from the source,
  or from the remembered binding (Part 2), or from the document key on genuine
  first contact with an anonymous file.
- **`source_origin` fallback** (`slugify(account_name or "unknown")`) — removed.
  This is MB-147, and it is the same defect as the key seed: a caller flag
  re-namespacing identity. For an unregistered file there **is** no exporter
  identity, and inventing one from a display label is the bug. Use a constant,
  mirroring PDF's `'document'`.
- **Last-four extraction** (`parse_account_label`) — kept, but demoted to what it
  is: a best-effort parse of a human label into `last_four` metadata, used only
  when the row data carries no account-number column. Display → metadata is a
  legitimate derivation; display → identity is not.

**`reports.*.account_name` needs no rename.** Once the identity jobs are gone,
both surfaces mean "human-readable label," which is one meaning. That is the
cheapest coherent outcome and it breaks no public contract.

> Optional sub-decision, not required by coherence: reports currently publish
> `display_name AS account_name`, so core and reports spell one concept two ways.
> Publishing `display_name` unaliased in those three files would make the word
> identical across layers, at the cost of a `reports.*` column rename — a
> user-facing contract, cheap pre-launch but not free. Recommend deferring; the
> alias is defensible as layer-local presentation.

---

# Part 2 — What a transaction's permanent id is built from

## The formula and its premise

`prep/int_transactions__matched.sql:130` (matched group anchor) and `:165`
(unmatched fallback):

```
SUBSTRING(SHA256(source_type || '|' || source_origin || '|'
                 || source_account_key || '|' || source_transaction_id), 1, 16)
```

The comment at `:88-95` states `account_id` is deliberately absent because it is
a mutable surrogate, and that `source_account_key` is "always present and is the
NULL-safe immutable stand-in."

**The stand-in is not immutable, and the exclusion is not honoured.** Both halves
of that sentence fail, on different channels.

## Stability audit per source

| Source | `source_type` | `source_origin` | `source_account_key` | `source_transaction_id` | Verdict |
|---|---|---|---|---|---|
| OFX | constant | raw column (V028/V029) | `<ACCTID>` — institution's own | `FITID` — institution's own | **stable** |
| Plaid | constant | `item_id` | Plaid account id | Plaid txn id | **stable**, modulo the documented pending→posted re-mint |
| PDF | constant | constant `'document'` | `pdf_doc_<sha[:16]>` — bytes of an immutable artifact | — | **stable in practice** |
| Manual | constant | constant `'user'` | **the canonical `account_id`** | `manual_<uuid4>`, persisted | **unstable** — re-keys on every account re-mint or merge |
| Tabular, registered format | `csv` | `matched_format.name` | per-format key | native id, else row hash | mostly stable |
| Tabular, **unregistered** | `csv` | `slugify(account_name or "unknown")` | `slug(stem)-sha256(bytes)[:12]` | row hash over `date\|amount\|description\|account_id\|row_number` | **unstable in three of four components** |

Sources for the two unstable rows:

- **Manual.** `stg_manual__transactions.sql:8` projects `t.account_id AS
  source_account_key`, and `transaction_service.py:110-128` confirms it in a
  docstring that, in the same breath, claims to hash "the immutable source
  identity (NOT the mutable canonical `account_id`)" while interpolating
  `account_id` into the third slot. ADR-015's stated invariant is violated
  literally, in the one channel whose rows a user typed by hand.
- **Unregistered tabular.** `_bare_account_key` (`import_service.py:928`) binds
  the key to file bytes. Worse, the *inner* row hash embeds the same wobbly key
  a second time: `extractors/tabular/schema/raw_tabular_transactions.sql:6`
  defines `transaction_id` as "`source_transaction_id` when available, else
  SHA-256 hash of `date|amount|description|account_id|row_number`", and
  `int_transactions__unioned.sql:82` maps that column into
  `source_transaction_id`. So the file digest enters the gold hash twice, and
  `description` — which ADR-015 Requirement 1 explicitly excludes from identity —
  enters transitively.

`source_origin` is caller-influenced for the unregistered case
(`import_service.py:3219-3223`), which is MB-147 and Part 1's decomposition.

**Answer to the premise question: `source_account_key` is immutable for OFX,
Plaid and PDF; it is a mutable canonical surrogate for manual; and it is a
document key wearing an account key's name for unregistered tabular.**

## What actually breaks

Both branches are bad, in different ways. Neither is theoretical.

**Branch 1 — recurring export saved to a new path each period** (the common
case: `transactions (3).csv`).

New bytes → new `_bare_account_key` → both the `source_account_key` and the inner
row hash change → the raw dedup partition `(transaction_id, account_id)` in
`stg_tabular__transactions.sql:35` misses on **both** columns, so every
overlapping row survives twice.

The only same-source net is Tier 2b. `matching/persistence.py:21` defines
`MatchTier = Literal["2b", "3"]` — there are exactly two tiers, and Tier 3 is
cross-source only. Tier 2b (`scoring.py:239-260`) requires same `source_type`
and `source_origin` with **different** `source_file`, and blocks on
`a.account_id = b.account_id` (`scoring.py:362`). It has no auto-merge floor by
deliberate design (`scoring.py:272-278`: inside one source "two rows written
differently are two transactions — the floor there would silently delete one").

So every overlapping row goes to **review**. A twelve-month rolling export
re-imported monthly puts eleven months of history in the review queue every
month. That is a per-item confirm gate, which
`.claude/rules/design-principles.md` names as a design failure in its own terms —
confirm volume must scale with uncertainty, not with row count.

It also only works *after* the user has bound the new key to the same canonical
account, because the blocking predicate needs `account_id` to match. Until then
the `COALESCE` at `stg_tabular__transactions.sql:45` falls through to the wobbled
key, the two imports carry different `account_id`s, and Tier 2b cannot even
generate the pair. **This is the circularity the brief flags, confirmed
exactly.**

**Branch 2 — recurring export overwritten at the same path** (equally common:
a browser writing `~/Downloads/transactions.csv` every time).

`account-identity-resolution.md:1459-1461` states the shipped rule, validated by
`test_ofx_reimport_identity.py`: identical bytes at a new path are refused as
already-imported, and **different bytes at a reused path import as new**.

Different bytes → the key wobbles as in Branch 1 → raw dedup misses. But
`source_file` is now *identical*, so Tier 2b's `a.source_file != b.source_file`
predicate excludes the pair. The duplicate is invisible to the matcher
entirely.

**Result: silent, permanent double-counting with nothing in the system able to
flag it.** No review, no doctor check, no metric. This is the sharpest finding in
this document.

For scale, the spec records the measured shape of this class of failure at
`INDEX.md:188`: five accounts across two sources yielded 558 rows / 10 ids where
279 / 5 was correct.

## Why the alias map cannot fix this

PR #438's own docstring (`import_service.py:2386-2394`) already states the
problem and the reason no file-derived key escapes it:

> `_bare_account_key` hashes the file's bytes, which identifies a *document*:
> stable for an unchanged file, different the moment a recurring export grows by
> a row. `account_id` is folded into `transaction_id`, so a rotated key re-keys
> every row already imported and both copies clear the `(transaction_id,
> account_id)` dedup — double-counting the overlap… **No file-derived key escapes
> this: a content hash breaks when the file grows, a filename breaks when it is
> renamed.**

That last sentence is the refutation of candidate A, written by the codebase
itself.

`app.transaction_id_aliases` maps `old_id → new_id`. It presumes **one
transaction that changed its id**. The tabular failure produces **two rows that
were never merged**. There is no `old_id`, no `new_id`, and no merge event to
write an alias row at. Wiring the alias table — writers, resolution in the
models, `app.*` update behaviour, doctor checks — would be real work that
leaves the double-count exactly where it is.

The alias map remains the right answer to the problem ADR-015 actually
anticipated: a merged group re-anchoring when a more-stable source backfills
history. That is worth building. It is not this.

Confirming the brief's correction: the table
(`sql/schema/app_transaction_id_aliases.sql`) and `TransactionIdAliasesRepo`
exist and are unit-tested, but there is **no production writer** and **no SQLMesh
model resolves through it** (`grep -rn "transaction_id_aliases"
src/moneybin/sqlmesh/` returns nothing). Its only non-test consumers are the
table constant, schema registration, the privacy taxonomy, and a
`doctor_service.py` audit-coverage check for bypass writes — not id forwarding.

## The three shapes

**A — keep it derived, migrate on re-bind.** Rejected: it cannot address the
dominant failure, for the reason above. Costed anyway: writers at every
id-changing merge; resolution wired into the 13 models that reference a bare
`transaction_id`; update behaviour in the five hard-coupled `app.*` tables;
chain collapse across successive merges (an open follow-up ADR-015 already
names); doctor coverage. An
id held mid-session by an agent or sitting in an exported CSV resolves only if
every read path resolves through the map — and the export round-trip
(§Blast radius) reads the file back through
`_ALLOWED_ITEM_KEYS = {transaction_id, category, subcategory}`, which would need
resolution too. A partial migration leaves some references forwarded and some
not, with no way to tell which.

**B — mint once, store the mapping.** Rejected, and already rejected by an
Accepted ADR whose reasoning still holds: hot mutable state at transaction
volume, and transaction identity becomes backup-critical in a layer whose whole
value is being rebuildable from `raw`.

The brief calls the lookup key "the hard part — the same identity problem one
level down," and that is exactly right and exactly disqualifying. To look up a
minted transaction id you need a stable key for the transaction. The stable key
you lack **is `source_account_key`**. B does not answer the question; it
relocates it to a table with a hundred thousand rows instead of a hundred.

**C — keep the id derived; stabilise its inputs by remembering the account key.**

ADR-015 already states the rule that decides this:

> Use a **surrogate + registry** when the entity is few, long-lived, and
> externally *referenced*… Use a **content-derived id + alias forwarding** when
> the entity is high-volume and internal.

An account key is few and long-lived. `app.account_links` **is** that registry,
it already exists, and it is already user-confirmed. C is not a new decision — it
is finishing the one ADR-015 made, in the one place a channel still synthesises
from content what it should be reading from the registry.

C is the same trade B offers, applied where the entity count is three orders of
magnitude smaller and where the repo already decided a registry is correct.

## What C changes

1. **Extend remembered-key reuse to the unpinned path.**
   `AccountResolver.accepted_native_keys_for_account` (`account_resolver.py:782`)
   already answers "what does this account call itself for this source?", ordered
   by `decided_at` so the first-accepted key wins forever. Its docstring states
   the requirement outright: *"two imports of one statement have to land on the
   same key or staging cannot dedup them."* Today only `--account-id` pins reach
   it. Call it from the unpinned branch: once a file resolves to a known account,
   the import stamps the **remembered** key into `raw`, not the freshly computed
   one.
2. **Take `account_id` out of the manual identity tuple.** Manual rows already
   carry an immutable `manual_<uuid4>` as `source_transaction_id`; the account
   slot should be a minted per-account source key or the constant `'user'`. Small
   and local, and it makes ADR-015's stated invariant true.
3. **Drop `description` and the account key from the tabular row hash**, leaving
   date, amount, and a within-file ordinal. This is the follow-up ADR-015 already
   names ("hardening the CSV per-source content hash (drop `description`)") and it
   removes the double-embedding.
4. **Pin `source_origin` for unregistered files to a constant** (Part 1's
   decomposition; MB-147), so no caller flag moves a hash component.

**How C breaks the circle.** The brief requires any proposal that dedups on the
resolved account to say exactly how it escapes the circular join. C does not
break the circle at the read side — it removes the wobble at the **write** side.
The resolver already runs before load and already gates on an unratified
identity; C makes it stamp the remembered key into `raw.tabular_transactions.
account_id`. The staging join at `stg_tabular__transactions.sql:73-78` then finds
the link on the first try, and the `COALESCE` fallback fires only for genuinely
unresolved accounts — which Part 1 recommends eliminating separately by always
minting.

## The one genuinely open sub-decision

C stabilises the key *once the file→account binding is known*. On the second
import of a recurring export, the resolver still has to decide **which account
this file is** — and it cannot use the content key, because that is the thing
that moved.

The ladder's weak signals (`institution + last4`, name) reach a confirm and never
auto-adopt, by deliberate design. So without a stable binding ref, C reduces the
confirm burden from *once per transaction* to *once per import* — a large win,
but not silence.

To reach silence, the confirm that binds a file to an account must record a ref
that survives the next export. Options, in the order I would take them:

1. **A user-confirmed stable ref for the recurring source** — the filename stem
   alone, scoped by `(source_type, source_origin)` and written as an
   `account_links` row under its own `ref_kind` at confirm time. The
   `_bare_account_key` docstring's objection to stems (two banks'
   `statement.csv` colliding) is an objection to using a stem as a *global*
   identity; as a ref the user explicitly confirmed once, scoped by source, it
   carries the same trust as every other remembered ref in the ladder.
2. **Promote the file to a registered format**, which already has stable
   per-account bindings. The right answer for a genuinely recurring export, and
   arguably the UX prompt to surface at the second import.
3. Leave it at once-per-import. Honest, and still a large improvement.

I recommend (1) with (2) offered at the moment the system notices a second import
from the same shape — that is a well-targeted confirm at real uncertainty, which
is what "magic stays visible" asks for.

---

# Blast radius (measured at `de040465`)

**`app.*` tables — the brief's "9 tables key on `transaction_id`" is materially
overstated. Exposure is not uniform:**

| Table | Actual coupling | Exposure |
|---|---|---|
| `transaction_categories` | `transaction_id` is the PK | **hard** |
| `transaction_tags` | PK `(transaction_id, tag)` | **hard** |
| `transaction_notes` | `NOT NULL` + index | **hard** |
| `transaction_splits` | `NOT NULL` + index | **hard** |
| `categorization_decisions` | `UNIQUE (transaction_id, attempt_number)` | **hard** |
| `match_decisions` | keys on `(source_type, source_transaction_id, account_id)` — **not** the gold id | insulated from gold churn; exposed to account and source-id churn |
| `lot_selections` | `investment_transaction_id` → `core.fct_investment_transactions` | separate id lineage |
| `proposed_rules` | `sample_txn_ids VARCHAR[]`, "up to 5" illustrative samples | non-authoritative |
| `audit_log` | `target_id`, free-form historical record | a stale id there is arguably correct |

Five tables are hard-coupled, not nine — and **none of them by a declared
foreign key**. A repo-wide grep for `FOREIGN KEY|REFERENCES` across
`src/moneybin/**/*.sql` returns zero: the coupling above is PK, `NOT NULL`, and
`UNIQUE` only. Nothing cascades, so a re-key that misses a table orphans its
rows silently instead of erroring — which is what makes the doctor gap below
load-bearing rather than cosmetic. `match_decisions` — which the brief lists —
is the one table structurally immune to gold-id churn.

**Doctor coverage is thinner than the coupling count.** `_run_orphan_app_state`
(`doctor_service.py:713`) checks `transaction_notes` and `transaction_tags`
**only**. `transaction_categories`, `transaction_splits`, and
`categorization_decisions` have no orphan detection at all — so a re-key silently
drops the highest-value curation in the system with nothing flagging it. That
gap is independent of which shape is chosen and worth closing either way.

**SQLMesh models.** 21 files under `src/moneybin/sqlmesh/models/` mention any
`*transaction_id` (the brief says 22); 13 mention a bare `transaction_id`. 28
across all of `src/moneybin/sqlmesh/`.

**MCP and CLI.** 11 files under `src/moneybin/mcp/` and 11 under
`src/moneybin/cli/commands/` reference `transaction_id`.
`mcp/write_contracts.py:189,212,220` types it as `IdentifierString` on three
write contracts.

**Export round-trip — this is a contract, not an internal.**
`moneybin transactions categorize export` emits `transaction_id` (plus scrubbed
description/memo); `commit-from-file` reads it back with
`_ALLOWED_ITEM_KEYS = {"transaction_id", "category", "subcategory"}`
(`cli/commands/transactions/categorize/commit_from_file.py:21`). Ids leave the
system in a file, sit there while a human or an LLM categorises offline, and come
back. **Any re-import between export and commit invalidates the entire batch**
under today's behaviour. Under C, ids stop moving on re-import and the round-trip
stops breaking — that is the concrete user-visible win.

# Does this break an existing on-disk database?

Stated plainly, per the constraint:

- **Change 1 (remembered-key reuse) requires no migration.** The remembered key
  *is* the key existing rows already carry, because
  `accepted_native_keys_for_account` orders by `decided_at` and the first import's
  key sorts first. Future imports converge onto ids already in the database.
  Forward-only.
- **Changes 2, 3 and 4 re-key existing rows** — manual transactions, unregistered
  tabular transactions, and anything whose `source_origin` came from a caller
  flag. Existing `app.*` curation on those rows orphans.
- The spec's declared migration posture already covers this
  (`account-identity-resolution.md:1315-1345`): pre-launch, with only the
  maintainer's dogfooding data, **re-import into a clean database**. A re-mint
  migration was prototyped and deliberately removed.

Recommendation: ship change 1 alone first, since it is free and forward-only, and
land 2–4 together behind one re-import.

# The strongest argument against C

**C makes `transaction_id` depend on import history, not only on file bytes.**
Today, re-importing the same files into a clean database reproduces the same ids
from the bytes alone. Under C the id depends on which file was imported *first*,
because that is the import whose key gets remembered. Import the same set in a
different order and you get different ids. That is a real loss of a real
property, and it is the honest cost.

Three things bound it, but they do not erase it:

1. **`core` is already not a function of `raw` alone.**
   `stg_tabular__transactions` already joins `app.account_links`, and
   `int_transactions__matched` already reads `app.match_decisions`. `core` is
   already `raw + app`. C does not introduce the dependency.
2. **C moves the dependency from read-time to write-time**, which is *stronger*
   for derive-from-raw than today: once the remembered key is stamped into
   `raw`, the raw row is self-sufficient and the model needs no lookup to
   reproduce the id.
3. **The property is already gone for accounts.** `account_id` is a minted
   uuid — not reproducible from `raw` at any price — and PR #438 already accepted
   history-dependent keys on the pinned path.

Counter-argument to my own rebuttal, stated so a reader can weigh it: (2) means
the raw layer now records a *decision*, not just an observation, which is a
meaningful widening of what `raw` means in the medallion architecture
([ADR-001](../decisions/001-medallion-data-layers.md)). Someone could
reasonably hold that `raw` must stay observation-only and pay the review cost
instead. I think that trades a purity property for a silent double-count and is
the wrong trade, but it is the strongest version of the case against.

# Corrections to the brief

Recorded so they are not repeated:

1. **"Dedup is per-source" is half right.** `stg_ofx__transactions:61` and
   `stg_tabular__transactions:35` dedup with a `ROW_NUMBER` partition.
   `stg_plaid__transactions` and `stg_manual__transactions` have **no** staging
   dedup at all — a repo-wide grep for `ROW_NUMBER|PARTITION BY` across the four
   matches only the first two. The conclusion the brief draws from it (a staging
   partition-key change cannot collapse OFX against CSV) still holds.
2. **Nine `app.*` tables do not uniformly key on `transaction_id`** — five are
   hard-coupled by PK/`NOT NULL`/`UNIQUE`. No `FOREIGN KEY` is declared
   anywhere, so nothing cascades; `match_decisions` keys on source identity
   instead. See §Blast radius.
3. **22 models → measured 21** under `models/` (28 across all of `sqlmesh/`).
4. **`--account-name` is not silently ignored off-channel** — it is refused by
   `reject_unhonored_account_signals`. Part 1 item 4.
5. **The `alias` at `dim_accounts.sql:62-68` is an institution alias**, built
   from `seeds.institutions`, not a normalized account `display_name`.
6. **PDF's `source_origin` is the constant `'document'`**, not caller-influenced —
   so PDF is stable in *both* components, which is a cleaner asymmetry than the
   brief describes.

# Open questions for Brandon

1. **The binding-ref sub-decision** (§The one genuinely open sub-decision):
   confirm-and-remember a stem-based ref, prompt to register a format, or accept
   once-per-import confirms. I recommend the first with the second offered.
2. **Ship change 1 alone first?** It is free and forward-only; 2–4 need a
   re-import. I recommend yes.
3. **Is the unresolved-account privacy exposure reachable?** Needs a live check
   against a database with an unresolved account (§Part 1). I did not have one.
4. **Doctor orphan coverage** for `transaction_categories`, `transaction_splits`,
   `categorization_decisions` — worth closing independently of this decision.
   File separately?
