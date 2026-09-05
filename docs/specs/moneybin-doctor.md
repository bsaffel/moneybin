# Feature: moneybin system doctor

## Status

implemented

## Goal

Provide a single command — `moneybin system doctor` — that asserts MoneyBin's pipeline invariants and produces a trust artifact: "✅ N invariants passing across M transactions." The command checks the pipeline, not the user's data. It replaces the dropped `verified` curator flag as MoneyBin's integrity-by-construction signal.

## Background

The `verified` flag was dropped from `transaction-curation.md` (PR #120) because per-row user assertions conflict with the brand promise that MoneyBin's data is trustworthy by construction. The replacement is a system-asserted check: MoneyBin proves its own pipeline is self-consistent.

`moneybin system doctor` is the entry point for that proof. It is read-only, zero-argument (no date ranges, no filters), and produces a clear pass/fail summary. The longer-form ETL reconciliation vision (row accounting, amount sums, temporal coverage) lives in `data-reconciliation.md` and is out of scope here.

Related specs:
- [`transaction-curation.md`](transaction-curation.md) §"Dropped: verified flag" — original motivation
- [`data-reconciliation.md`](data-reconciliation.md) — broader ETL integrity checks; doctor is a focused, user-facing subset
- [`moneybin-cli.md`](moneybin-cli.md) — CLI v2 taxonomy; doctor is top-level, parallel to `transform`
- [`moneybin-mcp.md`](moneybin-mcp.md) — the `system_status` doctor-section contract

## Design

### Invariant execution: SQLMesh named audits + DoctorService extras

Row-level invariants are defined as SQLMesh standalone named audits in `src/moneybin/sqlmesh/audits/`. Each audit is a `SELECT` query that returns violation rows — SQLMesh's convention. `DoctorService` auto-discovers all named audits via `ctx.standalone_audits`, renders each query with `audit.render_audit_query().sql(dialect="duckdb")`, and executes it against the open database connection.

Two additional checks that don't fit the "return violation rows" model (percentage thresholds, cross-layer counts) live as direct SQL in `DoctorService`.

Adding a new invariant in the future: add a `.sql` file to `src/moneybin/sqlmesh/audits/` — `DoctorService` picks it up automatically with no Python changes.

### Connection model

`DoctorService` is read-only by design. It takes a `Database` via constructor; current call sites (`mcp/tools/system.py` and `cli/commands/system/doctor.py`) still open with `get_database()` (write mode), but [ADR-010](../decisions/010-writer-coordination.md) has shipped `get_database(read_only=True)`, so flipping the call sites is a small follow-up the design unlocks. `sqlmesh_context()` (used by `DoctorService._run_sqlmesh_audits()`) accepts an explicit `db: Database` parameter — the caller passes its connection.

## Invariants

### SQLMesh named audits (auto-discovered)

| Audit file | Name | What it checks | Fails when |
|---|---|---|---|
| `fct_transactions_fk_integrity.sql` | `fct_transactions_fk_integrity` | Every `fct_transactions.account_id` resolves to `dim_accounts` | Any orphaned account_id |
| `fct_transactions_sign_convention.sql` | `fct_transactions_sign_convention` | `amount` is non-NULL and both derived columns agree with it: `transaction_direction` matches the sign, `amount_absolute` matches `ABS(amount)` (zero is a modeled 'zero' direction, not a violation; category is deliberately not policed against the sign) | Any NULL amount, or either derived column disagreeing |
| `bridge_transfers_balanced.sql` | `bridge_transfers_balanced` | Every transfer pair's legs cancel exactly, and both legs are still present in `core.fct_transactions` | Any pair whose legs do not sum to `0`, including a pair with a missing leg |
| `fct_investment_transactions_fk_integrity.sql` | `fct_investment_transactions_fk_integrity` | Every `fct_investment_transactions.account_id` resolves to `dim_accounts` | Any orphaned account_id |
| `fct_investment_transactions_sign_convention.sql` | `fct_investment_transactions_sign_convention` | The investment ledger obeys the accounting sign convention | Any violation of the convention |
| `fct_investment_transactions_uniqueness.sql` | `fct_investment_transactions_uniqueness` | Every `investment_transaction_id` appears once | Any duplicate id |

Each audit returns the offending `transaction_id` (or `debit_transaction_id` for transfers, `investment_transaction_id` for the investment ledger) as the first column. `DoctorService` uses this column for `--verbose` affected-ID output.

**Every audit file must declare `standalone TRUE`.** Without it SQLMesh loads the file as a *generic* `ModelAudit` that only executes when a model names it in an `audits (...)` property — so it silently never runs, and the suite stays green while the invariant goes unchecked. Standalone audits are also structurally non-blocking (a failure warns rather than raising), so a passing test run is never evidence that an audit holds.

### DoctorService extras (hardcoded)

**`dedup_reconciliation`** — Cross-layer count check that every imported row which disappears between the unioned staging layer and the core fact table is explained by recorded dedup decisions. The invariant is `raw_total - core_count == dedup_absorbed`, where `raw_total` is the row count of `prep.int_transactions__unioned`, `core_count` is the distinct `transaction_id` count of `core.fct_transactions`, and `dedup_absorbed` is `Σ(group_size - 1)` over every connected component in `prep.int_transactions__matched` — computed as `COUNT(*) - COUNT(DISTINCT match_group_id)` over rows where `match_group_id IS NOT NULL`. This formula is exact for any group topology: N-way merges, cyclic accepted-edge sets (e.g. three edges over a 3-node group still absorbs only 2 rows), and the common 1:1 pair case. `fail` when the counts disagree (a leak: rows vanished without a decision; or an un-applied match: a recorded decision didn't collapse its rows); `skipped` before the first transform (prep/core views absent). See `_run_dedup_reconciliation()` in `src/moneybin/services/doctor_service.py`.

**`duplicate_account_overlap`** — One real account imported under two canonical identities. `warn` when two accounts at the same `institution_slug` mirror each other's transactions; `pass` otherwise; `skipped` before the first transform. Reported once per unordered pair, at the higher of the two directional coverage ratios.

Nothing else in the pipeline can see this failure. The transaction matcher blocks candidate pairs on `account_id`, so a split identity produces no candidate pair at all — not a rejected one. `dedup_reconciliation` reconciles perfectly throughout, because nothing was lost: the same money was counted twice. The account-identity resolver is what should have prevented the split, and by the time the doctor runs, it already didn't.

A pair qualifies when a transaction in one account has a counterpart in the other with the **exact same amount** (sign included) within `matching.date_window_days`, and:

| Condition | Setting | Default | Rejects |
|---|---|---|---|
| Coverage of the smaller side | `doctor.duplicate_account_overlap_ratio` | 0.5 | Two real accounts sharing a few amounts by coincidence |
| Distinct amounts among mirrored rows | `doctor.duplicate_account_min_distinct_amounts` | 10 | Twin savings accounts posting identical interest — 100% mirrored on one amount |

Three details are load-bearing:

- **The window is the matcher's, not exact-date.** On the split that motivated this check, 80 of 346 mirrored pairs (23%) shared a date; the rest were spread across posting lag. An exact-date formulation would have missed it.
- **Amount equality carries the sign.** A transfer between two accounts at one institution is equal in magnitude and opposite in sign, so the most common same-institution pair shape is excluded without a special case.
- **`warn`, never `fail`.** Only the user knows whether two accounts at one bank are one account, and merging them is not undone by re-running anything.

Remedy: `moneybin accounts links run` raises a proposal for existing accounts, `accounts links pending` shows it, `accounts links set <decision_id> --into <account_id>` merges. Deciding the proposal standalone leaves the warning in place.

**The remedy is not guaranteed to reach this check's pairs.** `AccountLinksService.run()` delegates to `AccountResolver.propose_existing()`, which searches institution+last-four and fuzzy name only — never the transaction-overlap signal this invariant measures. A pair flagged here was flagged *because* identity resolution failed to bind it, so `run()` can legitimately write zero decisions and leave `pending` empty. Closing that gap — materializing a detected overlap pair as a review decision, or accepting two account ids directly — is tracked as follow-up work, not shipped here.

**`unproposed_cross_source_duplicates`** — Two sources co-resident in one account that the matcher has never considered. `warn` when a cross-source pair matches on amount and date and no live `app.match_decisions` edge explains the matcher's silence about it; `pass` otherwise; `skipped` before the first transform. Reported once per account, with the pair count.

This is the state `duplicate_account_overlap` leaves behind. That check sees the split while it is still two accounts and stops applying the instant the link is accepted — which is exactly when the rows become co-resident and matchable. `dedup_reconciliation` never applied: its `raw_total - core_count == dedup_absorbed` identity balances whether or not a duplicate was ever *proposed*, so a pair nobody looked at moves both sides together. On 2026-08-08 both checks were green across 377 silently duplicated rows.

A pair qualifies when it is one the matcher's own blocking join would have produced — same `account_id`, exact amount (sign included), within `matching.date_window_days`, neither side `manual`, compatible `currency_code` — and none of the three suppressions below applies. Each mirrors one reason the matcher is legitimately silent; together they are what makes the remaining silence diagnostic.

| Suppression | Grain | Mirrors |
|---|---|---|
| `source_type` **or** `source_origin` differs on the two sides | pair | The Tier 3 blocking test in `scoring.py::_get_candidates`, verbatim. Within-source pairs are Tier 2b's, and Tier 2b declines by writing no row at all (`engine._classify_pair` returns `None`), so silence there is the normal resting state. `source_type` alone would exclude two CSV bank integrations and two Plaid connections, which the matcher does treat as cross-source |
| The two rows are already in one component | component | `assign_components` skips an edge on `find(a) == find(b)`. Components are the transitive closure of `accepted`/`pending` dedup edges, keyed on the matcher's own `NodeKey` — `(source_type, source_transaction_id)` scoped per account (`assignment.py::_node_a`); a row in no component is a component of one |
| The two components already hold a row from one `(source_type, source_origin, source_file)` | component | `assign_components`' cardinality guard. Two rows of a single import file are distinct transactions by construction, so an edge joining their components is refused. Without this the check warns about pairs no refresh can clear, since the remedy it recommends is the pass that re-drops them |
| The exact pair was `rejected` | pair | The matcher excludes only the exact rejected pair, so a row rejected against one partner stays a live candidate against every other. Node grain here would hide a genuinely unproposed pair behind an unrelated rejection |

Two grains, deliberately. `accepted`/`pending` rows are union-find seeds and read at component grain; `rejected` rows are not seeds and read at pair grain. `reversed` rows suppress nothing — the decision was undone. `match_type = 'dedup'` throughout: Tier 4 transfers are cross-account and never run through the blocking join this mirrors.

The two grains also key differently, because the matcher does. Components use `NodeKey`, which has no `source_origin`: two rows alike on `(source_type, source_transaction_id)` under one account are a *single* node to the matcher, so `find(a) == find(b)` holds and `assign_components` drops the candidate without writing anything. Splitting that node on origin would warn about a pair no refresh can clear. Rejected pairs drop origin for the same reason: `get_rejected_pairs` selects it (`matching/persistence.py:192-194`), but `scoring.py` discards it when building the tuple it actually tests (`matching/scoring.py:399-414,441`), so the matcher skips a rejected pair whatever origin the rows now carry. Requiring origin here would warn about a pair the recommended refresh cannot clear — the identical failure, one grain over.

The `account_id` scoping is load-bearing in both. A source-native id is unique only within its account (`identifiers.md`), so an un-namespaced FITID reused by a second account would otherwise mark a row "already decided" and suppress the warning — most likely on two accounts at one institution, which is exactly the pair an account-link merge just joined.

**The component mirror is close, not exact.** `assign_components` registers physical sources only for the nodes appearing in a given run's candidate list, and that per-run state is never persisted. The SQL rebuilds it from the same list: the endpoints of the blocking join's pairs, minus the rejected ones. Both exclusions carry weight. A seed-only member of a component contributes no `source_file`; neither does a pair the user dismissed, because `scoring.py` skips a rejected pair before `assign_components` ever sees it, which makes both of its endpoints seed-only too. Nothing else narrows the list — both dedup tiers pass `excluded_ids=None` and no score cutoff applies — so every surviving pair registers on both sides. What is left approximate is the rejected-pair key itself: this check compares `source_origin` alongside source type and id where the matcher's key compares type and id alone, so a decision whose recorded origin no longer matches its row's diverges. Getting an exclusion wrong is not a cosmetic mismatch — a source registered here that the matcher would not register is silence about an unproposed duplicate, the exact failure this check exists to remove.

`warn`, never `fail` — equal amounts days apart can be two real charges, and only a match pass can tell. A user-rejected pair keeps its row, so dismissing a proposal silences this permanently rather than nagging.

Remedy: `moneybin refresh --step match --step transform` proposes the pairs and reflects any auto-merges into the ledger; `moneybin review --type matches` decides the rest. An accepted account-link merge now re-runs matching automatically (`AccountLinksService.rematch_after_merge()`), so this check should only fire on ledgers whose merges predate that behavior, or where a match pass failed.

**`categorization_coverage`** — What percentage of non-transfer transactions have a category. Status is `warn` (not `fail`) when below 50%; `pass` otherwise. Never blocks exit 0 on its own.

### Investment reconciliation (M1G.4)

Nine checks covering the Plaid investment ledger. They split into two families: **refusals surfaced** (staging declined to guess and filed the row for review — these must be visible, not silently dropped) and **divergence from the broker** (MoneyBin's derived position disagrees with what the provider reports).

| Name | What it checks |
|---|---|
| `investment_staging_rejects` | Rows staging routed to review rather than the ledger. Three reasons today — `split_underivable`, `transfer_direction_underivable`, `unmapped_subtype` — all deliberate refusals. The query is deliberately open (`review_reason IS NOT NULL`), so a new reason added upstream surfaces without a code change. |
| `investment_opening_lot_review` | Positions the opening-lot bootstrap refused to synthesize: short/non-positive quantity, NULL basis, and `sold_out_prewindow` gaps it declined to reconstruct rather than guess. |
| `investment_unmodeled_legs` | Legs in the ledger stripped of lot-affecting quantity (option and short legs MoneyBin models no book for). An assignment that exercises away a covered-call position disposes of real shares; the held lot never closes. |
| `investment_holdings_divergence` | Engine-derived held lots that disagree with the broker's newest snapshot, on positions MoneyBin *does* hold a lot for. |
| `investment_unreported_holdings` | Broker-reported positions with no `core.dim_holdings` row — the opposite direction, and the more dangerous one. |
| `investment_phantom_holdings` | Open lots MoneyBin holds that the broker's newest snapshot no longer reports. Keyed on the per-pull holdings-snapshot receipt (below), not on the presence of holdings rows. |
| `investment_unresolved_securities` | Ledger rows whose provider security key never resolved to a canonical security. These are dropped from cost basis entirely, so they must not stay silent. |
| `investment_source_overlap` | Accounts carrying both manual and Plaid investment history. **The one investment check that `fail`s** — see below. |
| `investment_conflicting_security_refs` | One provider security bound to two different canonical securities. The resolver refuses to repoint either binding on its own — a repoint is a reviewed merge, never a sync-time side effect — so it logs and moves on, which made the conflict visible only to whoever was reading server logs. |

**`investment_source_overlap` is the only investment check that `fail`s, and
the only one whose remedy is outside the pipeline.** Every other investment
check `warn`s: it reports a position MoneyBin can still describe honestly, and
`core.dim_holdings` withholds only the figures it cannot stand behind. Source
overlap is different in kind — the account has two ledgers rather than one, so
every event exists twice and lots, cost basis, gains and holdings are *all*
wrong at once. There is no investment dedup to run (transactions have
`prep.int_transactions__matched`; investments have no equivalent, and it is a
future matching child), so no refresh, price pull, or reconciliation clears it.
Only removing one of the two feeds does.

`core.dim_holdings` withholds accordingly: every position in such an account
carries `valuation_status = 'source_overlap'` and publishes no market value,
unrealized gain, or pricing at all. That status is deliberately **not** spelled
`withheld` — the four `withheld` clauses each say one position's share count is
wrong and want it reconciled, and a reader who cannot tell the two apart cannot
tell which repair applies.

The check reads the RAW tables rather than the ledger, so it still fires before
a first transform has run — the point at which the withhold does not yet exist.
Its recipe emits exactly one `RecoveryAction`, `import_revert`, because that is
the only remedy MoneyBin can run: `REVERT_TABLES['manual']` covers
`raw.manual_investment_transactions`, so reverting the batch deletes those rows
and leaves the account with one ledger. `sync_disconnect` is deliberately **not**
offered beside it. It is a remote operation — `SyncService.disconnect_confirmed`
calls `client.disconnect` and deletes nothing locally, as its own confirmation
says ("Previously pulled local rows remain") — and this check joins exactly
those retained rows, as does `core.dim_holdings`'s `source_overlap_accounts`.
Following it would cost the user their connection permanently and leave the
check failing and the holdings withheld, which is worse than no suggestion: a
`RecoveryAction` is a claim that running it fixes the failure. The fact still
reaches the user, in the remedy's rationale and this check's `detail`, because
someone whose file import is the ledger they want will reach for a disconnect
on their own.

**Open gap:** there is no local counterpart for the synced feed —
`raw.plaid_investment_transactions` carries no `import_id` and no tool deletes
it — so a user who wants to keep the file import and drop the connector has no
remedy today. Closing it needs a way to remove locally-retained rows for one
connection.

The remedy does not carry its identifying argument: the check knows account ids
and not an `import_id`, so it names the missing argument in the rationale at
`confidence: suggested`, which is the shape `RecoveryAction` prescribes for a
value unknown at construction time.

**The phantom check depends on `raw.plaid_investment_holdings_snapshots`.** Holdings *rows* cannot distinguish "this item reported and holds nothing" from "this item never reported" — an item whose pull returns an empty holdings array writes no rows at all, so a newest-snapshot join keyed on those rows silently keeps the last non-empty snapshot from an earlier pull. That reads a fully-liquidated broker as still holding its old positions: the largest possible net-worth overstatement, and precisely the phantom this check exists to catch. The receipt is written per (item, pull) **even when zero positions come back**, and both `core.dim_holdings` and this check derive "newest snapshot" from it.

### Investment pricing (M1J.3 C.2)

Four checks covering the price series holdings are valued from. C.2 is the
first phase in which one security can carry more than one price source, and the
first in which a price can come from somewhere other than the broker that
reports the position.

| Name | What it checks |
|---|---|
| `investment_price_disagreement` | Two provider feeds holding closes for the same security, date, and quote currency that differ by more than `investments.price_disagreement_tolerance_pct`. Resolution picks a winner by source rank; this is where that choice becomes visible instead of silent. Recording a mark for that grain settles it and drops it from the check. |
| `investment_unpriced_holdings` | Open positions whose `valuation_status` is `unpriced` — no usable price, so they report no market value and are absent from every total that sums one. |
| `investment_stale_prices` | Open positions whose `valuation_status` is `carried_forward` and whose close is older than its security type allows. The threshold resolves per type through `moneybin.staleness` — 4 days for exchange-traded, 1 for crypto, `investments.price_staleness_default_days` for a type the table does not name. |
| `investment_unmapped_price_source` | Price rows whose `source_type` `prep.stg_security_prices` has no `ref_kind` mapping for, detected as rows with an accepted matching binding that still never reach staging. |

**`investment_stale_prices` is the only surface that judges a price's age.**
`core.dim_holdings` publishes `days_since_observed` and `holdings` publishes
`max_days_since_observed`, both as numbers rather than warnings — a boolean flag
on every position would fire on the ~114 days a year markets are closed. Per-type
thresholds are what make a warning affordable, and this check is where they
apply. Without it a feedless position keeps a years-old `trade_implied` purchase
price indefinitely and is summed into portfolio totals as though it were current,
which is why `investment_unpriced_holdings` deliberately scopes to `unpriced`
alone and leaves `carried_forward` here.

**The disagreement check reads `prep.stg_security_prices`, not the resolved fact
table.** The fact table has already collapsed each conflicting pair to one
winner, so the disagreement is no longer visible there. Staging also carries
provider observations *only* — overrides and trade-implied prices are derived at
model build and never land in `raw.security_prices` — which is what restricts
the comparison to sources that are supposed to agree. Both derived sources are
*expected* to differ from a provider close: an override exists precisely to
correct one, and a trade-implied price reflects a single execution's size and
spread rather than the day's close. Comparing them would raise a standing
warning on every ordinary correction and every intraday fill.

The tolerance is sized to the failure the check actually catches — a feed key
bound to the wrong security, which produces order-of-magnitude differences — not
to the precision two correct feeds agree to. Legitimate differences exist and
must not fire: a broker strikes its crypto valuation at its own snapshot time
while CoinGecko's is a 00:00 UTC close, so a volatile day separates two correct
figures by more than a percent.

**`investment_unmapped_price_source` detects a mapping failure without parsing
the model's SQL.** `prep.stg_security_prices` takes each `source_type`'s
`ref_kind` from `seeds.price_source_map` and INNER JOINs on the result, so a
source absent from that registry — or carrying no `ref_kind` — matches nothing
and the join drops the row, with no error and no counter. That drop is permanent rather than deferred:
unlike an unresolved binding, which waits in raw and reappears once its security
binds, no number of later accepted bindings will ever surface it, because the
failure is in the registry rather than the binding.

The check separates those two conditions with an accepted-binding join. A row it
reports already has an accepted binding whose `source_type` and `ref_value`
match the staging join exactly, leaving `ref_kind` as the only condition that
can still be failing. Without that clause the check would fire on every ordinary
first pull, since the Plaid extractor writes prices during ingestion, before the
resolver has minted a canonical security.

This check exists because the failure it describes shipped: C.2 added a writer
for `tiingo` and `coingecko` rows one commit ahead of the staging mapping, and
every row written in between was discarded silently. That particular split is
now structural rather than guarded — `seeds.price_source_map` declares the
source PriceService dispatches on and the `ref_kind` staging joins in one row,
so declaring either declares both. This check remains the run-time half, and it
is the only one that covers rows already written and a registry row someone
deletes.

### Dropped invariant

**`reconciliation_deltas`** — deferred. Requires a unified balance-evidence model spanning `app.balance_assertions`, OFX `LEDGERBAL`, and future Plaid sync balances. That model doesn't exist yet. See `data-reconciliation.md` for the longer-term design.

## Data Model

No new tables or migrations. All checks are read-only queries against existing schemas.

```python
@dataclass(frozen=True)
class InvariantResult:
    name: str
    status: Literal["pass", "fail", "warn", "skipped"]
    detail: str | None  # human-readable description; None on pass
    affected_ids: list[str]  # populated only when verbose=True; empty otherwise


@dataclass(frozen=True)
class DoctorReport:
    invariants: list[InvariantResult]
    transaction_count: int  # total rows in fct_transactions; used in summary line
```

`DoctorService.run_all(verbose=False) -> DoctorReport`. The transaction count is fetched by a dedicated `_get_transaction_count()` query against `core.fct_transactions`; returns `0` if the schema doesn't exist yet (pre-first-transform).

## CLI Interface

Top-level command, parallel to `moneybin transform`:

```
moneybin system doctor [--verbose] [--output text|json]
```

**Human output (default):**

```
✅ fct_transactions_fk_integrity
✅ fct_transactions_sign_convention
❌ bridge_transfers_balanced — 2 violation(s)
   Run with --verbose for affected pair IDs
⚠️  categorization_coverage — 43% of non-transfer transactions are uncategorized
✅ dedup_reconciliation

5 invariants checked across 14,203 transactions — 1 failing
```

With `--verbose`, affected IDs appear under each failing line:
```
❌ bridge_transfers_balanced — 2 violation(s)
   Affected: a1b2c3d4e5f6, b7c8d9e0f1a2
```

**Exit codes:** `0` = all pass or warn-only, `1` = any invariant fails.

**`--output json`** returns the standard `ResponseEnvelope` with all invariants included (agents need the full picture, not just failures):

```json
{
  "summary": {"total_count": 5, "returned_count": 5, "sensitivity": "low"},
  "data": {
    "passing": 3, "failing": 1, "warning": 1,
    "transaction_count": 14203,
    "invariants": [
      {"name": "fct_transactions_fk_integrity", "status": "pass", "detail": null, "affected_ids": []},
      {"name": "fct_transactions_sign_convention", "status": "pass", "detail": null, "affected_ids": []},
      {"name": "bridge_transfers_balanced", "status": "fail", "detail": "2 violation(s)", "affected_ids": []},
      {"name": "categorization_coverage", "status": "warn", "detail": "43% of non-transfer transactions are uncategorized", "affected_ids": []},
      {"name": "dedup_reconciliation", "status": "pass", "detail": null, "affected_ids": []}
    ]
  },
  "actions": ["Run with --verbose to see affected transaction IDs"]
}
```

`affected_ids` is always `[]` unless `--verbose` is also passed.

## MCP Interface

**`system_status(sections=["doctor"], detail="full")`** — the selected doctor
section of the standard system-status contract in `src/moneybin/mcp/tools/system.py`.

```python
async def system_status_coarse(
    sections: list[Literal["overview", "doctor", "categorization"]] | None = None,
    detail: Literal["summary", "full"] = "summary",
) -> ResponseEnvelope:
    """Return selected system overview, integrity, and categorization sections."""
```

The full doctor selection returns affected IDs where the doctor contract permits them. It is reached through the standard system registration.

## Implementation Notes

**`dedup_reconciliation` SQL:** Three queries inside one `try/except` — `raw_total` from `prep.int_transactions__unioned`, `core_count` as `COUNT(DISTINCT transaction_id)` from `core.fct_transactions`, and `dedup_absorbed` as `COUNT(*) - COUNT(DISTINCT match_group_id)` from `prep.int_transactions__matched` where `match_group_id IS NOT NULL`. This equals `Σ(group_size - 1)` over every connected component and is exact for any group topology including N-way merges and cyclic accepted-edge sets. All three queries are wrapped in one `try/except` so the invariant reports `skipped` (not errored) before the first transform, when the `prep`/`core` views don't yet exist.

**Audit SQL column contract:** Each named audit's SELECT must return the violation entity's ID as the first column (e.g., `transaction_id`, `debit_transaction_id`). `DoctorService` uses `row[0]` for `affected_ids` — this is a convention, not schema-enforced. Document it in `src/moneybin/sqlmesh/audits/README.md` or a comment in `DoctorService`.

**SQLMesh context in tests:** Unit tests mock `sqlmesh_context()` and inject pre-rendered SQL to avoid loading the full SQLMesh project. E2E tests use a real profile with a test database.

## Files to Create

- `src/moneybin/sqlmesh/audits/fct_transactions_fk_integrity.sql`
- `src/moneybin/sqlmesh/audits/fct_transactions_sign_convention.sql`
- `src/moneybin/sqlmesh/audits/bridge_transfers_balanced.sql`
- `src/moneybin/services/doctor_service.py` — `InvariantResult`, `DoctorService`
- `src/moneybin/cli/commands/system/doctor.py` — Typer command under the `system` group
- `tests/moneybin/test_services/test_doctor_service.py`
- `tests/moneybin/test_cli/test_doctor.py`
- `tests/e2e/test_e2e_doctor.py`

## Files to Modify

- `src/moneybin/cli/commands/system/__init__.py` — register the `doctor` command on the existing `system` Typer group via `app.command(name="doctor")(_doctor.doctor_command)`
- `src/moneybin/mcp/tools/system.py` — expose doctor through the `system_status` section selector
- `docs/specs/INDEX.md` — add this spec; update `data-reconciliation.md` entry with cross-reference
- `docs/specs/moneybin-mcp.md` — document the `system_status` doctor section
- `CHANGELOG.md` — `Added` entry under `Unreleased`
- `docs/roadmap.md` — move to `✅ shipped` when complete

## Testing Strategy

**Unit** (`tests/moneybin/test_services/test_doctor_service.py`):
- Each invariant: one test with clean fixture data (pass), one with deliberate violation (fail)
- `run_all()` aggregates all results correctly
- `verbose=True` populates `affected_ids`; `verbose=False` returns empty list
- `sqlmesh_context()` mocked; audit SQL injected directly

**CLI** (`tests/moneybin/test_cli/test_doctor.py`):
- Clean pipeline → exit 0, `--output json` shape valid
- Failing invariant → exit 1
- `--verbose` adds affected IDs to human output
- `--output json --verbose` includes affected IDs in JSON

**E2E** (`tests/e2e/test_e2e_doctor.py`):
- Clean test profile → all invariants pass, exit 0
- Unbalanced transfer inserted → `bridge_transfers_balanced` fails, exit 1, `--verbose` shows pair ID

## Out of Scope

- `reconciliation_deltas` — requires unified balance-evidence model; deferred
- Broader ETL checks (raw→prep row accounting, amount sums, temporal gaps) — `data-reconciliation.md`
- Writing any state — this command is permanently read-only
- Scheduled or CI-triggered doctor runs — use `make doctor` or a cron wrapper
