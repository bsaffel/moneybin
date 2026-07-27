# Feature: Multi-Currency

## Status

in-progress

## Goal

Make every monetary value in MoneyBin carry an explicit currency, let a user
declare a **home currency**, and present amounts in that home currency through
**auditable conversion** — without ever silently mixing currencies or mutating the
original-currency source of truth. This is the foundational M1K schema wave: the
*original* currency is canonical at every grain; conversion is a presentation
layer staged on top.

The wave is phased so the **capture + integrity** layer (M1K.1) can land early —
independent of investments — while **live conversion** (M1K.2) and **realized FX
gain/loss** (M1K.3) follow the investment cost-basis engine they depend on.

## Background

Today amounts are *implicitly* USD, inconsistently:

- `prep.int_transactions__unioned` already carries a `currency_code`, but every arm
  defaults unknown to USD: tabular, manual, and Plaid all `COALESCE(..., 'USD')`
  (Plaid's `iso_currency_code` **is** read here — captured all the way from
  `SyncTransaction` through `stg_plaid__transactions` already — just badly
  defaulted to USD when null); **OFX hardcodes `'USD'` outright** with no read at
  all — `CURDEF` isn't captured anywhere in the OFX pipeline (raw parse, staging,
  or union) yet.
- `core.fct_transactions` exposes `currency_code`; `core.dim_accounts` has
  `currency_code` — but **balances carry no currency at all** (`core.fct_balances`,
  `reports.net_worth` have no currency column).
- Reports `SUM` across rows with no currency dimension. A user who imports EUR rows
  via CSV (currency lands in `fct_transactions`) gets every report summing EUR+USD
  into a single number while the envelope claims USD. **This is a live correctness
  bug**, not a hypothetical — M1K.1's guard closes it.

**Update, 2026-07-17:** the capture gaps and the `dim_accounts` naming described
above are closed (Requirements 1–3, 8).

**Update, 2026-07-26:** the no-silent-blend guard (Requirement 5) shipped, closing
the live correctness bug: every `reports.*` model that sums money segments per
`currency_code`, and the envelope no longer claims USD for rows that are not.
The account grain stopped defaulting to `'USD'` in the same pass, which is what
makes an unknown currency representable end-to-end (Requirement 3).

The design move is the same one [`investments-data-model.md`](investments-data-model.md)
makes: **lock the schema, stage the algorithm.** The investments spec (implemented,
PR #300) lands a `currency_code` column on its ledger/lots/gains/holdings now and
explicitly defers conversion to M1K (*"currency column now, conversion later"*) —
already adopting this spec's canonical `currency_code` naming (see §Key Decisions).
So investments is self-contained in its own denominating currency; M1K is the layer
that adds conversion across both cash and investments.

Related specs:

- [`investments-data-model.md`](investments-data-model.md) — implemented (PR #300);
  carries per-instrument `currency_code` natively; defers FX conversion to this spec.
  **M1K.3 reuses its cost-basis engine** for realized FX gain/loss.
- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — layer
  conventions; **Invariant 8** (derivations live in SQLMesh, never snapshotted into
  `app.*`); the `ResponseEnvelope.summary.display_currency` contract.
- [`reports-net-worth.md`](reports-net-worth.md) and the report recipe library —
  consumers that must segment-or-convert, never silently blend.
- [`account-management.md`](account-management.md) — `app.account_settings`; per-account
  `currency_code` already validated against ISO 4217.
- [`smart-import-financial.md`](smart-import-financial.md),
  [`smart-import-tabular.md`](smart-import-tabular.md), [`sync-plaid.md`](sync-plaid.md) —
  the capture points where currency must stop being dropped.
- [`identifiers.md`](../../.claude/rules/identifiers.md) (agent rules, not a spec) — the
  surrogate-key guideline for the M1K.3 conversion-pair identity; `raw.exchange_rates`
  itself keys naturally on `(from_currency, to_currency, rate_date, source)`.

## The core decision (one-way door)

**Original currency is the canonical stored amount at every grain. Conversion to a
home/display currency happens at presentation time — never by mutating stored
originals.**

Concretely: `amount` is always in `currency_code` (the row's original currency).
Reports, queries, and MCP envelopes convert *on read* to a requested display
currency, recording which rate they used. No converted amount is ever persisted at
row grain.

Why this is the durable path:

- **Home currency stays mutable for free.** Changing it re-presents; it never
  re-converts stored data, because nothing converted was stored.
- **Raw stays the source of truth.** Core is rebuilt from raw, not patched
  (consistent with the medallion architecture). The migration is *additive columns*,
  not a backfill of derived values.
- **Rate corrections don't strand data.** A corrected historical rate changes future
  presentation; it doesn't require rewriting millions of stored converted amounts.
- **It matches the investments pattern** ("lock the schema, stage the algorithm"),
  so the two foundational waves are coherent rather than divergent.

The rejected alternative — storing `home_amount`/`home_currency` at row grain —
forces conversion to exist before any row lands, freezes the home currency, and
makes rate corrections a data-rewrite. Not chosen.

**Rejected alternatives.** Persisting converted amounts at row grain makes a
corrected historical rate a data rewrite. Maintaining derived valuation
snapshots in mutable tables adds rebuild and orphan-cleanup obligations on every
recompute. Converting at presentation time and deriving through SQLMesh avoid
both failure modes while preserving original-currency facts.

## Phasing

| Phase | Scope | Depends on | Notes |
|---|---|---|---|
| **M1K.1** | Currency capture & integrity (no conversion) | nothing | Independent of investments; **may be pulled into the first public release** (see [`roadmap.md`](../roadmap.md) §"The first public release"). Closes the live silent-blend bug. Requirements 1, 2, 3, 8 (capture, schema, account-currency inheritance) implemented 2026-07-17; Requirements 4–7 (home currency, no-silent-blend guard, doctor check, report guard) implemented 2026-07-25, and Requirement 3's account-grain `'USD'` fallback removed 2026-07-26 — **M1K.1 closed**, except the first-run-wizard locale default explicitly descoped under Requirement 4. |
| **M1K.2** | Display conversion (auditable rates) | M1K.1 + **investments (M1J)** | The unifying conversion layer over both cash and investment grains. Sequenced after investments so it converts *everything* in one coherent pass. |
| **M1K.3** | Realized FX gain/loss | M1K.2 + investments cost-basis engine | Reuses the investments lot/cost-basis machinery; the genuinely investment-shaped part. |

**Sequencing rule:** investments (M1J) lands before M1K.2/M1K.3. The dependency runs
one direction only — realized FX gain/loss is currency-lot accounting, i.e. the same
engine securities use; it cannot precede it. M1K.1 carries no such dependency.

## Requirements

Numbered, testable. Tagged by phase.

### M1K.1 — Currency capture & integrity

1. **Currency captured at every ingestion grain.** OFX import records `CURDEF`
   end-to-end (raw parse → `stg_ofx__transactions` → union) — today none of these
   three stages capture it at all. Plaid **transaction** currency is already
   partially captured: `SyncTransaction.iso_currency_code` and
   `stg_plaid__transactions` both exist and the union already reads it (Requirement
   2 fixes how the read value gets defaulted). What's still missing on the Plaid
   side is **balances**: `SyncBalance` (`src/moneybin/connectors/sync_models.py`)
   has no `iso_currency_code`/`unofficial_currency_code` field, so balance currency
   isn't captured at all yet. Adding it to `SyncBalance` and moneybin-sync's mapping
   is an **additive, optional** contract change (one-way door: additive only).
   **Implemented 2026-07-17:** OFX `CURDEF` now flows raw parse → `stg_ofx__transactions`
   → union; `SyncBalance` gained `iso_currency_code`/`unofficial_currency_code` and
   moneybin-sync's mapping populates it.
2. **The union stops hardcoding `'USD'`.** `int_transactions__unioned.sql` reads the
   captured currency for the OFX and Plaid arms and leaves it `NULL` when the source omits
   it — it does **not** `COALESCE` to a literal `'USD'`, which would relabel a non-USD
   account's rows before account-currency inheritance (Req 3) can run. The tabular/manual
   arms' blind-`'USD'` fallback is the same class of bug (they do read source currency when
   present) and is dropped in the same pass; account inheritance (Req 3) fills currency where
   known, and anything still unknown is segmented, not guessed (Req 8).
   **Implemented 2026-07-17:** no arm (OFX/manual/tabular/Plaid) defaults to a literal
   `'USD'` anymore, including the CLI `transactions create --currency` entry point that
   fed the manual arm.
3. **Currency at every core monetary grain.** `core.fct_balances` **and the derived
   `core.fct_balances_daily`** (the model `reports.net_worth` actually aggregates) gain a
   `currency_code`; `core.fct_transactions` already carries it. `core.dim_accounts` also
   carries it today, but under the legacy name `iso_currency_code` — this phase renames it
   to `currency_code` end-to-end per the resolved naming decision and scope (§Key
   Decisions, Decision 5). Where a grain genuinely cannot know its currency, it inherits
   the account's `currency_code`, never a blind `'USD'`.
   **Implemented 2026-07-17:** `dim_accounts.currency_code` (renamed), `fct_balances`/
   `fct_balances_daily.currency_code`, and account-currency inheritance on
   `fct_transactions`/`fct_balances` all shipped.
   **Completed 2026-07-26:** the account grain itself kept a `COALESCE(..., 'USD')`
   until this pass — the one place the "never a blind `'USD'`" rule was still
   broken, and the terminal fallback of the whole chain, so it silently supplied a
   currency to every row that lacked one. `core.dim_accounts` now resolves
   user override → the currency the account's own source reported (OFX `CURDEF` via
   `prep.stg_ofx__balances`, Plaid `iso_currency_code`/`unofficial_currency_code` via
   `prep.stg_plaid__balances`, the tabular `currency` column) → `NULL`. Until this
   landed, Requirement 8's unknown-currency rule and Requirement 6's `fail` branch
   were both unreachable in production.
4. **Home currency setting.** A profile-level `home_currency` (ISO 4217), **mutable**,
   defaulted by **locale auto-detection with explicit user confirmation** in the
   [first-run wizard](mcp-first-run-setup.md). Distinct from per-account currency. It is
   **`app.*` state (DB-resident)**, not YAML config — the no-blend guard and report views
   are SQLMesh models that must read it to segment home vs. foreign — so it is written
   through a `*Repo` (Invariant 10), not the generic YAML `profile set`.
   **Implemented 2026-07-25** as `app.profile_settings` (singleton row, V044) behind
   `ProfileSettingsRepo`. `moneybin profile set home_currency EUR` dispatches the
   managed key to the repo while dotted `section.field` keys still write `config.yaml`;
   `profile show` splits `Config (config.yaml)` from `Settings (database)`. MCP gained
   `profile` and `profile_set`. Unset reports null — never an implied USD. The
   first-run-wizard locale default is **not** built: the setting is mutable and
   segmentation does not depend on it, so nothing reads it yet.
5. **No-silent-blend invariant.** An aggregation across rows of differing
   `currency_code` MUST NOT emit a single combined figure unless an explicit
   conversion with recorded rate provenance is applied. Absent conversion (all of
   M1K.1), results are **segmented per currency** (a sub-total per currency), never
   blended.
   **Implemented 2026-07-25.** Every `reports.*` model that sums money projects and
   groups by `currency_code`; `reports.balance_drift` projects it without regrouping
   (asserted and computed balances are the same account's, so the comparison is
   single-currency by construction). Two consumers re-aggregate the segmented views
   and had to segment too: the `core:cashflow` runner (`currency_code` is in
   `select_cols`/`group_cols` unconditionally, for every `by` value) and
   `NetworthService` (see Requirement 7). `reports.large_transactions` additionally
   scopes its median/MAD baselines and its top-100 rank per currency — a pooled
   baseline compares unlike units and scores a typical charge in the
   smaller-denominated currency as an anomaly.
   **Ranking and reachability, 2026-07-26.** Segmentation makes each figure
   correct; two follow-on defects left a correct figure unreadable or absent.
   The framework truncates with `records[:max_rows]`, so the *first* sort key
   decides what a capped response can contain: a report that emitted one
   currency's rows before the next let a single currency fill the cap and drop
   the others out of the response entirely — missing, not merely ranked lower.
   Four of the six runners did this — `balance_drift` ordered globally by raw
   `drift_abs`; `large_transactions`, `merchant_activity`, and
   `recurring_subscriptions` ranked correctly per currency and then re-grouped
   by `currency_code` at the top level, undoing it. All four now order by
   rank-within-currency first, so any prefix represents every currency.
   `cash_flow` and `spending_trend` lead with `year_month` and are unaffected:
   truncating a time series drops tail months across all currencies alike.
   `test_no_runner_leads_its_sort_with_currency_code` scans the runner sources
   rather than waiting for someone to build a mixed-currency fixture per
   report — the defect reached four runners because each was fixed where it was
   found rather than swept for. Separately, `core.fct_transaction_lines` — the
   canonical split-expanded grain — projected every parent column except the
   denomination, so an agent on it could not tell a EUR line from a USD one; it
   now carries `currency_code`, as does `core.uncategorized_queue`, whose review
   surface asks a user to act on the amount it shows. The curated `sql_schema`
   examples that sum money across `core.*` group by currency, held by
   `test_every_money_aggregating_example_names_its_currency`, which derives its
   eligible set from `CLASSIFICATION` so a new money example on a
   currency-bearing table inherits the guard rather than escaping it.
   The prefix-truncation defect had a second channel the runner scan cannot
   see: `sql_query` also keeps a prefix (`rows[:max_rows]`), and so does an
   agent's own `LIMIT`, which examples asking for "top" anything invite. Twelve
   curated examples led their sort with `currency_code` and now rank within
   each currency first, or lead with the non-currency dimension where the query
   is a time series or a closed vocabulary. Two keep `ORDER BY currency_code`
   because they return exactly one row per currency, where ordering is not the
   lever — any prefix of *k* rows holds *k* currencies whatever the sort key
   is. `test_no_example_leads_its_sort_with_currency_code` asserts set equality
   against those two, so a new offender fails and so does a stale exemption.
   **Which payloads owe a currency, 2026-07-26.** The MCP-side enumeration
   (`test_money_tools_name_their_currency`) asked whether a payload carries a
   money-classed field and no `DataClass.CURRENCY`, and six tools answered yes.
   That question is wrong: `TXN_AMOUNT` marks a field *masking-sensitive*, not
   *denominable*. Walking each payload to the exact leaf, only two were real.
   `transactions` was — the flagship read returned bare amounts, so a
   mixed-currency page (`display_currency` null by design) had no denomination
   anywhere in the response; its rows now carry `currency_code` from
   `core.fct_transactions` through both the MCP and CLI construction sites.
   The other real one is `transactions_categorize_rules`: `min_amount`/
   `max_amount` are genuine bounds, but `app.categorization_rules` has no
   currency column, so a rule cannot say which currency it bounds — schema work,
   deferred to M1K.2. The remaining four are classification artifacts with no
   currency to state: `import_files` and `import_preview` expose
   `as_printed`/`as_recorded`, a single magnitude printed and then negated as
   `str` so the caller can confirm a sign convention; `investments_lots_select`
   exposes `quantity`, a share count; `system_audit` exposes
   `before_value`/`after_value`, polymorphic audit evidence whose audited column
   differs per event. The guard now splits three ways instead of two, and each
   not-denominable entry pins the money-classed `(field, type)` leaves its
   reason rests on — a payload that grows a real `Decimal` amount fails rather
   than inheriting an exemption written for a string sample.
   **Known limit — unknown pools into one segment.** `GROUP BY currency_code`
   puts every row whose currency is unknown into a single `NULL` segment and
   sums it. Two accounts denominated in genuinely different currencies that
   both lack one are therefore added together, producing a figure in no unit.
   This is not fixable by grouping: nothing distinguishes two unknowns from
   each other. It is why Requirement 6 makes an unknown currency a **failure**
   rather than a warning — the remedy is `accounts set --currency`, not an
   aggregate MoneyBin could compute. Withholding the segment's totals instead
   was considered and rejected: the population this creates is tabular imports
   whose file carried no currency column, who are almost always single-currency,
   and blanking net worth and cash flow for all of them to guard a case their
   own doctor output already fails on trades a certain harm for a rare one.
   **The invariant binds `core.*` too, not only `reports.*`.**
   `core.fct_balances_daily` carries a balance forward adjusted by intervening
   transactions, and `fct_transactions.currency_code` resolves per row — so an
   account whose statements are USD can hold a transaction of its own in EUR.
   Summing them and adding the result to the carry blends currencies one layer
   *below* every guard: the row still reports the observation's currency, so
   `balance_drift`'s `currency_mismatch` check compares two matching `USD`
   values and passes the blended number through to `reports.net_worth`. The
   carry therefore applies only the transactions denominated in the currency it
   is carrying. The excluded movement is not dropped silently — it lands in the
   next observation's `reconciliation_delta` and reads as drift, and the
   Requirement 6 warn names the behaviour. The same reasoning governs the
   *carry itself*: `core.fct_balances.currency_code` resolves per row, so a
   corrected re-import can move an account from USD to EUR between two
   consecutive observations. `reconciliation_delta` subtracts the prior carry
   from the new observation, so across that boundary it would difference two
   units and label the result with the newer one — invisible to the same
   `currency_mismatch` check, which would again see two agreeing codes. The
   delta is therefore `NULL` whenever the observed currency differs from the
   carried one, and `balance_drift` already renders a null delta as `no-data`.
   **Guard placement rule:** a currency guard on a derived value must sit where
   the arithmetic happens. A guard downstream of the blend can only compare
   labels, and both labels survive a blend intact.

6. **Doctor check.** `system doctor` reports when a profile holds more than one
   distinct currency across transactions/accounts/balances, **flags accounts/rows whose
   currency is unknown (`NULL`) so the user can assign one before it can blend**, and flags
   any report path that would violate Requirement 5.
   **Implemented 2026-07-25** as the `currency_integrity` invariant: **fail** on any
   unknown-currency account/transaction/balance (with the `accounts set --currency`
   fix in the detail, the `moneybin transform` that makes it take effect in `core.*`,
   and the affected ids attached), **warn** on two or more known
   currencies with nothing unknown — naming both consequences a user would
   otherwise read as a bug: reports sub-total per currency, and a transaction
   denominated differently from its account sits out of that account's carried
   balance and shows up as its drift — **pass** otherwise. It publishes
   `moneybin_profile_currencies` and `moneybin_unknown_currency_rows{grain}`.
   The third clause — "any report path that would violate Requirement 5" — is a
   **build-time** guard rather than a runtime one, because the set of report paths is
   code, not data: `test_every_money_bearing_report_projects_the_currency_it_is_denominated_in`
   enumerates the live report catalog and fails CI for any registered report that
   declares a `TXN_AMOUNT`/`BALANCE` column without a `currency_code` one. A runtime
   check could only re-assert what CI already proved, and would go stale against a
   report added later.
7. **Report guard.** Report views that sum money detect mixed currency and either
   segment (default) or return an explicit "cross-currency total unavailable until
   conversion ships" signal — never a silent blend. Single-currency profiles (the
   common case, including USD-only users) see **zero behavior change**.
   **Implemented 2026-07-25.** Segmentation is the default everywhere. The one place
   that takes the explicit-signal branch is `core:networth`'s scalar headline, which
   has no room for a sub-total per currency: `NetWorthSnapshotPayload` nulls
   `net_worth`/`total_assets`/`total_liabilities`/`currency_code` when more than one
   currency contributes and carries each currency's totals in `per_currency`; its
   report records attach each account row to its own currency's totals.
   `NetworthService.history` partitions both its bucketing and its period-over-period
   `LAG` by `currency_code`, so a change is never the difference between two
   currencies' positions. Zero-behavior-change is held by fixtures, not assertion: the
   pre-existing single-currency report and net-worth tests assert the same figures
   unchanged, and `tests/scenarios/test_multi_currency_report_segmentation.py` proves
   the mixed case is what discriminates a segmented model from a blending one
   (restoring the blend in `reports.cash_flow` fails it; a single-currency fixture
   cannot).
8. **Migration is additive.** New currency columns are nullable additions to raw tables;
   core is rebuilt from raw (no in-place core patch). The migration does **not** depend on
   `home_currency` (also introduced in M1K.1): a row with no captured currency inherits its
   account's `currency_code` (Req 3); a value still `NULL` after that is treated as
   **unknown currency** — never silently resolved to the home currency (that would be a guess
   the no-blend guard couldn't see). Unknown-currency rows are segmented out (Req 5) and
   surfaced by `system doctor` (Req 6) for the user to assign. `home_currency` itself is
   established by the first-run wizard (Req 4), not the migration. Versioned migration under
   `src/moneybin/sql/migrations/`.
   **Implemented 2026-07-17** for the capture/inheritance columns above.
   **Completed 2026-07-26:** segmentation and doctor surfacing shipped with Requirements
   5–7, and the account-grain `'USD'` fallback that made "still `NULL`" impossible is
   gone (see Requirement 3). `tests/scenarios/test_multi_currency_report_segmentation.py::test_a_transaction_with_no_captured_currency_stays_unknown`
   holds the whole chain: an uncaptured currency stays `NULL` through core, gets its own
   report segment, and fails `system doctor`.

### M1K.2 — Display conversion

9. **Display currency on read.** Reports/queries/MCP accept an optional display
   currency (default: home currency) and convert original amounts to it on
   presentation, populating `ResponseEnvelope.summary.display_currency`. Original
   amounts remain available for drilldown.
10. **Auditable rate provenance.** Every converted figure traces to a stored rate — a
    provider rate in `raw.exchange_rates` (`(from_currency, to_currency, rate_date, rate,
    source, fetched_at)`, unique on `(from_currency, to_currency, rate_date, source)`) or a
    user override in `app.*` (Req 14). A "show me the rate" path exposes the exact rate
    behind any converted number (consistent with the lineage promise).
11. **Free reference-rate source.** Rates fetch lazily on first need from **Frankfurter**
    (ECB-backed, no auth, historical to 1999), cached in `raw.exchange_rates`.
    `ExchangeRate.host`/`open.er-api.com` are documented fallbacks. **Only currency
    codes and dates leave the machine — never amounts or PII** (same structural-signal
    posture as categorization redaction).
12. **Offline fails loud.** A needed historical rate that is neither cached nor
    fetchable causes an explicit, surfaced error — never a silent substitution of
    today's rate or `1.0`.
13. **Weekend/holiday handling.** A non-trading date resolves to the ECB last-published
    business day; the resolution is recorded in `rate_date` provenance, not hidden.
14. **User rate override.** A user may override an auto-fetched rate (the bank's actual rate
    differs from the ECB mid-rate). An override is **mutable user-authored state**, so it
    lives in `app.*` (e.g. `app.exchange_rate_overrides`), mutated only via a `*Repo` with
    paired audit (Invariant 10) — **not** in `raw.exchange_rates` (the immutable provider
    cache). The conversion layer prefers an app override over the cached provider rate, and a
    later provider refresh never silently overwrites it. **Scope:** an override is a daily
    reference-rate correction — one per `(from, to, rate_date)`, matching the
    `app.exchange_rate_overrides` key — *not* a per-transaction bank-spread capture. Two
    same-day transactions at different effective rates (spreads/fees) are out of scope here; a
    genuine per-transaction rate belongs on the conversion-pair model (M1K.3).
15. **Segmentation becomes the fallback.** For currency pairs the source can't provide
    (exotics, crypto-as-currency) or while offline, reports fall back to M1K.1
    segmentation rather than guessing.
16. **Investment bridge (display).** Investment holdings (which carry their own
    `currency_code` per `investments-data-model.md`) convert to home currency for a
    unified net-worth number through this same layer.

### M1K.3 — Realized FX gain/loss

17. **Conversion-pair identity.** A currency conversion event (e.g. a EUR debit paired
    with a USD credit) is modeled as a first-class pair, not inferred from two
    unrelated rows.

    **Reserved import shape — single-row FX transfer.** Some source formats express
    an FX transfer as one row carrying *both* legs (the sent amount/currency plus a
    received amount and target currency), rather than two rows. Reserve a
    received-leg column pair — `to_amount` + `to_currency` — on the raw import tables
    (`raw.*`, e.g. `raw.tabular_transactions`) where a source row lands, so an
    importer that meets this shape has somewhere to put the second leg instead of
    dropping it or fabricating a paired row; the row's own `amount`/`currency_code`
    is then the sent leg, and the conversion-pair model consumes either shape.
    `to_amount`/`to_currency` follow the directional `from_currency`/`to_currency`
    prefix convention already used on `raw.exchange_rates` — reserving them now
    (schema reservation, not yet built) keeps a later importer from coining an
    ad-hoc, differently-ordered name and compounding the currency-column naming
    drift §Key Decisions already flags.
18. **Currency-lot accounting.** Realized FX gain/loss on disposing a foreign-currency
    holding is computed via the **investments cost-basis engine** (FIFO / average per
    the elected method in `investments-data-model.md`), treating currency holdings as
    lots. Realized FX gain/loss on a foreign-denominated *security* sale is the same
    engine applied to the currency leg.
19. **Decimal throughout.** All amounts and rates are `DECIMAL`, never `FLOAT`
    (`DECIMAL(18,2)` amounts; `DECIMAL(18,8)` rates per the `database.md` precision
    convention).

## Data Model

Sketch; exact DDL settled per phase during implementation planning.

### M1K.1

```sql
-- core.fct_balances: add original currency (mirrors fct_transactions)
ALTER TABLE ... ADD COLUMN currency_code VARCHAR;   -- ISO 4217; inherits account currency
-- core.fct_balances_daily (Python model feeding reports.net_worth): propagate currency_code
--   through the daily rollup so net_worth can segment/convert per currency

-- raw OFX/Plaid transaction + balance tables: capture original currency
--   OFX: CURDEF;  Plaid: iso_currency_code / unofficial_currency_code
-- prep.int_transactions__unioned: every arm reads captured currency and leaves NULL when
--   the source omits it (no COALESCE to literal 'USD'); account inheritance fills where known,
--   anything still unknown stays NULL (segmented, not guessed)

-- profile-level home currency: app.* state (DB-resident so SQLMesh guard/views can read it
-- to segment home vs. foreign), mutated via a *Repo (Invariant 10). If a profile-settings
-- app table already exists, add the field there; otherwise introduce app.profile_settings.
--   home_currency VARCHAR NOT NULL  -- ISO 4217, mutable, ISO-validated
```

### M1K.2

```sql
-- raw.exchange_rates: the auditable provider rate cache (immutable; overrides live in app.*)
CREATE TABLE raw.exchange_rates (
    from_currency  VARCHAR NOT NULL,   -- ISO 4217
    to_currency    VARCHAR NOT NULL,   -- ISO 4217
    rate_date      DATE    NOT NULL,   -- resolved trading day
    rate           DECIMAL(18,8) NOT NULL,  -- from->to multiplier; precision per database.md
    source         VARCHAR NOT NULL,   -- provider only: 'frankfurter' | 'exchangerate_host' | ...
    fetched_at     TIMESTAMP NOT NULL,
    UNIQUE (from_currency, to_currency, rate_date, source)
);

-- app.exchange_rate_overrides: user-authored overrides (mutable user state, NOT raw).
-- Mutated only via a *Repo with paired audit (Invariant 10); the conversion layer prefers
-- an override here over the cached provider rate above.
CREATE TABLE app.exchange_rate_overrides (
    from_currency  VARCHAR NOT NULL,           -- ISO 4217
    to_currency    VARCHAR NOT NULL,           -- ISO 4217
    rate_date      DATE    NOT NULL,
    rate           DECIMAL(18,8) NOT NULL,     -- user-entered; precision per database.md
    note           VARCHAR,                    -- why the user overrode the provider rate
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL,
    UNIQUE (from_currency, to_currency, rate_date)
);
```

### M1K.3

Conversion-pair + realized-FX model derived in SQLMesh (Invariant 8), reusing the
`core.fct_investment_lots` / cost-basis derivation from `investments-data-model.md`.
DDL fixed when M1K.3 is planned.

## Sequencing & Dependencies

```mermaid
flowchart LR
    M1K1["M1K.1 — capture &<br/>integrity (no conversion)"]
    M1J["M1J — investments<br/>(cost-basis engine)"]
    M1K2["M1K.2 — display<br/>conversion"]
    M1K3["M1K.3 — realized<br/>FX gain/loss"]
    M1K1 --> M1K2
    M1J --> M1K2
    M1J --> M1K3
    M1K2 --> M1K3
```

- **M1K.1** depends on nothing; independent of investments. Eligible to ride into the
  first public release.
- **M1K.2 / M1K.3** follow investments (M1J): M1K.2 is the unifying conversion layer
  over cash *and* investment grains; M1K.3 reuses the cost-basis engine.
- External: Frankfurter (M1K.2). Cross-repo: the sync-contract currency field (M1K.1)
  touches moneybin-sync.

## CLI Interface

- Set/adjust the home currency (M1K.1) — locale-detected default confirmed in the first-run
  wizard, mutable thereafter. Surfaced through the existing `profile` command group (alongside
  `profile show`), not a new `settings` group; exact invocation settles with `moneybin-cli.md`.
  Because `home_currency` is `app.*` state, the command routes through a `*Repo`, **not** the
  generic YAML `profile set` (whose `section.field` keys don't write `app.*`).
- Reports accept `--display-currency <ISO>` (M1K.2; default home).
- `moneybin fx rate <FROM> <TO> [DATE]` (M1K.2) — inspect/seed a cached rate.
- `moneybin fx override <FROM> <TO> <DATE> <RATE>` (M1K.2) — auditable user override.
  The `fx` group is a **new top-level CLI namespace**; like the MCP names above, its exact
  shape settles with the surface specs — `moneybin-cli.md` and the capabilities map must
  register it when M1K.2 is planned, not be invented standalone from this spec.
- `system doctor` gains the mixed-currency integrity checks (M1K.1).

## MCP Interface

- `ResponseEnvelope.summary.display_currency` populated whenever money is returned
  (the profile's home currency for single-currency profiles — never a hardcoded `USD`,
  which would mislabel a EUR/GBP-only user; the requested/home currency under conversion).
- **The envelope derives it; call sites do not have to remember.** `build_envelope`
  reads `currency_code` off the payload it is given — the record itself, or the rows
  of its single primary list — and applies `resolve_display_currency`: one agreed
  known code, else null. An explicit argument still wins, for the caller that
  resolved a currency the payload cannot show (the reports framework resolves across
  every matching row, not just the returned page).

  This is placement, not preference. A default of `"USD"` on the parameter is
  inherited silently by every call site that omits it, so the guard has to sit
  where the envelope is *constructed*, not at each of the 251 places that build
  one. Rounds 10 and 11 of the M1K.1 review fixed named call sites twice and the
  class reopened both times — nine of the eleven money-bearing tools were still
  claiming USD when the third round found `accounts_set`. Same lesson as the
  reconciliation guard below: a currency guard must sit where the value is
  produced, because downstream nothing can tell an inherited default from a
  deliberate one.

  A payload that records no currency anywhere reports null rather than a guess.
  That is honest but uninformative, so the set of such tools is pinned by
  `tests/moneybin/test_mcp/test_money_tools_name_their_currency.py` — enumerated
  from the live registry with set equality, so a new money tool cannot join it
  silently.
- M1K.2 rate / conversion / exposure operations follow the existing MCP taxonomy —
  multi-currency is a **crosscutting service-layer concern, not its own tool namespace**
  (`mcp-architecture.md`), and tool names use the noun=query / path-prefix-verb-suffix
  contract (no verb-first `get_*` / `record_*`); exact names settle with the surface specs.
  Same envelope, sensitivity, audit, and confirmation rules as every other tool.
- Per-currency segmentation surfaces in report tool output under M1K.1 (so an agent
  can see *why* there is no single total yet).

## Testing Strategy

- **Scenario fixtures (YAML):** add a multi-currency profile (e.g. USD + EUR + GBP
  cash, plus a foreign-denominated holding now that investments exist) with ground-truth
  per-currency sub-totals. Existing single-currency scenarios must be **unchanged**
  (Requirement 7: zero behavior change for single-currency profiles).
- **Guard tests (M1K.1):** a mixed-currency profile makes summing reports segment or
  flag — never emit a blended number; the doctor check fires.
- **Property-based (M1K.2/3):** round-trip conversion identity within tolerance;
  realized FX gain/loss conserves value across lots (Hypothesis, mirroring the
  investments lot-conservation tests).
- **Offline/edge (M1K.2):** missing-rate-offline fails loud; weekend/holiday resolves
  to the recorded business day; an override survives a refresh.

## Synthetic Data Requirements

The generator should be able to emit a multi-currency persona: accounts in ≥2
currencies, cross-currency transfer pairs (for M1K.3 conversion-pair ground truth),
and at least one currency outside Frankfurter's set (to exercise the segmentation
fallback). Ground truth includes per-currency sub-totals and, for M1K.3, expected
realized FX gain/loss on the conversion pairs.

## Dependencies

- **Investments (M1J)** — prerequisite for M1K.2 and M1K.3 (not M1K.1).
- **Frankfurter** (ECB reference rates; free, no auth) — M1K.2; fallbacks documented.
- **moneybin-sync** — the M1K.1 sync-contract currency field (additive).
- DuckDB / SQLMesh migration tooling — additive schema migration (M1K.1).

## Key Decisions

1. **Original currency is canonical; conversion is presentation-time** (the §"core
   decision" one-way door): convert-at-view, not store-both; additive columns, core
   rebuilt from raw.
2. **Home currency default = locale auto-detect with confirm; mutable.**
   Mutability is cheap *because* of decision 1.
3. **Rates lazy-fetch + cache, never pre-populated.**
4. **Realized FX gain/loss lives in a dedicated conversion-pair model, not a column on
   `fct_transactions`** — a conversion is a relationship between two
   events, and reuses the investments cost-basis engine.
5. **Canonical currency column name = `currency_code`** *(coherence decision — resolved
   2026-07-17).* **Decision: rename `dim_accounts.iso_currency_code` → `currency_code`
   end-to-end, as a direct rename with no deprecation shim.** Confirmed with Brandon
   2026-07-17. Exact call sites and migration steps are M1K.1 implementation-plan detail
   (see Requirement 3), not spec content — the scope in brief: the `app.account_settings`
   schema + repo, `AccountService`, `core.dim_accounts`, the privacy taxonomy/payloads,
   and the `accounts_set` MCP tool's parameter name. The CLI is already clean (`accounts
   set --currency`, not `--iso-currency-code`) and is unaffected.

   Three names existed when this spec was drafted: `currency_code` (`fct_transactions`),
   `iso_currency_code` (`dim_accounts`), `currency` (investments spec, then unbuilt).
   Investments shipped (PR #300) and adopted **`currency_code`** directly
   (`investments-data-model.md` Requirement 15). Plaid Investments sync then shipped too
   (PR #318 / moneybin-sync #29) and reinforced the same split three more times: the
   broker wire contract (`SyncSecurity`, `SyncInvestmentTransaction`, `SyncHolding`)
   deliberately keeps `iso_currency_code`/`unofficial_currency_code` (mirroring Plaid's
   own field names, consistent with how every other passthrough field is handled), while
   staging translates it and every `core.*` table — `dim_securities`, `dim_holdings`,
   `fct_investment_transactions`, `fct_transactions` — normalizes to `currency_code`.
   `core.dim_accounts` was the one remaining holdout in `core` against a pattern now
   established four times over. The raw `iso_currency_code` grep count across `src/` is
   large (dozens) but mostly irrelevant to this decision — nearly all of it is the wire
   layer above, which correctly keeps the provider's name; only the account-currency
   surface listed above is actually in scope.

   The original text here assumed the MCP-parameter rename needed the
   ship-alongside-the-old-name-for-one-release protocol from
   `design-principles-depth.md`. That protocol is explicitly **post-launch
   only**; per the launch trigger in `design-principles.md` (**M3H** hosted
   launch, or the first tagged release adopted by a non-author user — that rule
   currently misstates this as "M3E," a separate stale milestone-code reference
   worth fixing there independently of this spec), MoneyBin is
   still pre-launch — no tag has been cut and no non-author user has adopted the MCP
   contract yet. So the rename is a direct, one-time change: no shim, no follow-up removal
   PR. Implementation is scoped to M1K.1, not this spec pass — see Requirement 3.

   **Implemented as decided, 2026-07-17:** `dim_accounts.iso_currency_code` (and
   `app.account_settings.iso_currency_code`, the `accounts_set` MCP parameter, and every
   internal reference) renamed to `currency_code` end-to-end; no shim.

## Out of Scope

- **Crypto-as-currency** (BTC/ETH *as* a denominating currency) — use the investment
  model (`investments-data-model.md`, `crypto` security type), not the FX path.
- **Intraday / real-time rates** — daily ECB granularity only.
- **Currencies ECB does not publish** beyond the documented fallback — segmentation,
  not a guessed rate.
- **IRS election rules for FX gain/loss** (e.g. §988/§987) — MoneyBin mirrors the
  mechanics, it does not police tax policy (same stance as the investments spec on
  1099-B).
- **Multi-currency *budgets*** — M2C concern; this spec provides the currency grain it
  builds on, not the budgeting semantics.
