# Data-layer touchpoints — what breaks when you change a core fact or an app write

On-demand companion to [`.claude/rules/database.md`](../rules/database.md). Read
this before **adding a column that reaches a core fact table**, **unioning a new
balance or price source**, **carrying a provider-specific id through the merge**,
or **reusing a guarded-UPSERT write helper for a new caller**.

Each section is a defect that shipped or was caught late. None of them produce an
error at the point of the mistake — they produce wrong numbers, silently split
entities, or NULLed columns.

## Adding a column that flows to `core.fct_transactions`

The model chain is longer than it looks, and several test fixtures independently
**hardcode** the column list.

**The chain — the `matched` layer is the one that gets missed:**

`prep.stg_<source>__transactions` → `prep.int_transactions__unioned` (all four
source CTEs: ofx / manual / tabular / plaid; `SELECT * UNION ALL`, so positions
must align) → **`prep.int_transactions__matched`** (lists columns explicitly, so
it drops anything not added here) → `prep.int_transactions__merged` (per-column
`ARG_MIN`) → `core.fct_transactions`.

**Hardcoded column lists that also need the new column** — none of these fail
until tests run:

- `src/moneybin/privacy/taxonomy.py` → `CLASSIFICATION[("core","fct_transactions")]`;
  every `core`/`app` column must be classified and a completeness test enforces it.
- `tests/moneybin/db_helpers.py` → `CORE_FCT_TRANSACTIONS_DDL`, the static stub
  behind `schema_catalog_db`.
- `tests/moneybin/matching/test_int_matched_model.py` → `_UNIONED_STUB_DDL`, and
  `tests/moneybin/test_services/test_doctor_service.py`, which carries its own
  `prep.int_transactions__unioned` stub. Both build the `matched` view against a
  hand-written `unioned` table.
- `tests/scenarios/_tier1_backfill.py` → `FCT_TRANSACTIONS_SCHEMA`, asserted by
  the scenario `schema_snapshot`.

**Gates:** `make check test` catches the binder errors and the `db_helpers` /
matching stubs; `make test-scenarios` catches the `FCT_TRANSACTIONS_SCHEMA`
snapshot. Run **both** — scenarios are not in `make check test`.
`tests/moneybin/test_sync_e2e.py` is the right place to assert a new field
actually reaches core. (Learned shipping Plaid `original_description`, #283.)

## Unioning a new balance source into `core.fct_balances`

`reports.net_worth` is **sign-based**: `net_worth = SUM(balance)`,
`total_assets = SUM(balance WHERE > 0)`,
`total_liabilities = SUM(balance WHERE < 0, kept negative)`. So
`core.fct_balances.balance` must arrive **pre-signed — liabilities negative,
assets positive**. There is no account-type sign normalization in the union; each
source is trusted to provide the right sign.

**The trap (#299):** OFX and tabular happen to provide pre-signed balances, but
**Plaid reports `current_balance` as a positive magnitude** — for `credit` and
`loan` accounts, a positive amount *owed*. Unioning it straight books a
credit-card balance as an asset and inflates net worth. The `plaid_balances` CTE
in `fct_balances.sql` joins `stg_plaid__accounts` and negates when
`account_type IN ('credit','loan')`.

Two rules for **any** future balance source (a PDF statement balance, a new
provider):

1. Check the source's sign convention against the liabilities-negative contract
   before unioning. A positive liability fails silently — no error, just a wrong
   net worth.
2. **Filter** nullable balance columns (`WHERE NOT <col> IS NULL`) rather than
   passing them through. `fct_balances_daily._to_decimal()` coerces NULL/NaN to
   `0`, planting a false $0 anchor in balance history. Plaid `current_balance` is
   nullable, and `available_balance` is **not** a safe fallback — its semantics
   differ per account type.

## Carrying a provider-specific id through the merge

A provider-specific attribute (e.g. Plaid `merchant_entity_id`) carried through
`prep.int_transactions__merged` does **not** align with that merge group's
`canonical_source_type`. `canonical_source_type = ARG_MIN(source_type by priority)`
picks the highest-priority member of a cross-source-deduped group — the default
`source_priority` ranks `ofx` above `plaid` — while the provider id exists only on
the member that issued it. An OFX+Plaid-deduped transaction therefore carries the
Plaid id with `canonical_source_type = 'ofx'`.

**Consequence:** key an `app.*` binding or lookup on `canonical_source_type` and a
Plaid id riding a deduped transaction binds under `('ofx', id)` while a Plaid-only
sibling looks up `('plaid', id)` — one real entity **silently splits into two**.

**Fix pattern:** carry an **entity-paired `source_type`** alongside the id, derived
by the *same* `ARG_MIN`-with-non-null filter the id uses, and make any
entity-bearing member outrank entity-less ones even when its source is absent from
`seed_source_priority` (sentinel `2147483646` beats the entity-less `2147483647` —
the validator only requires `source_priority` to be non-empty, so a user can omit
`plaid` entirely). Resolve and harvest on the paired column, never on
`canonical_source_type`. Precedent: `merchant_entity_source_type` in
`int_transactions__merged.sql` (#284). Applies to any future provider-specific id.

## Reusing a guarded-UPSERT write helper

`upsert_guarded` runs `ON CONFLICT (…) DO UPDATE SET <every column> = EXCLUDED.<col>`
on **every** permitted write, not just inserts. A write helper that omits an
argument (defaulting it to `None`) is harmless on the INSERT path and silently
**NULLs that column** on the UPDATE path.

**Concretely (#294):** `_write_plaid_rows` omitted `merchant_id=`. Safe for
`apply_plaid_categories` (new rows only, `WHERE tc.transaction_id IS NULL`), but
`improve_ai_categories` reused the same helper to UPDATE existing
`categorized_by='ai'` rows that routinely carry a `merchant_id` — nulling the
canonical merchant FK. Both a final self-review and an opus whole-branch review
missed it; both fixated on the precedence guard rather than the adjacent column
loss.

**How to apply:** when reusing a guarded-UPSERT helper for a caller that UPDATEs
existing rows, enumerate **every** column the `SET` clause overwrites and confirm
the new caller threads through the ones it does not mean to change — then add a
test that populates those columns on the *pre-existing* row before the update.
"I only set the fields I care about" is not how a guarded UPSERT behaves.
