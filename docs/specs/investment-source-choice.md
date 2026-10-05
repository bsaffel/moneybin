# Investment Source Choice

> Last updated: 2026-10-04
> Status: implemented
> Address: M1J.8 (Investments — per-account investment source choice)
> Type: Feature
> Owns: the per-account `investment_source_type` setting, the ledger filter it
> drives, and the `investment_source_overlap` remedy
> Refines: [`investments-data-model.md`](investments-data-model.md),
> [`account-management.md`](account-management.md), and
> [`moneybin-doctor.md`](moneybin-doctor.md)
> Constrained by:
> [`investment-event-matching.md`](investment-event-matching.md),
> [`sync-plaid-investments.md`](sync-plaid-investments.md),
> [`core-updated-at-convention.md`](core-updated-at-convention.md), and
> [`observability.md`](observability.md)
> Closes: GitHub issue #546

## One-line goal

When one investment account has investment history from two sources, let the
user choose which one counts, without deleting anything and without losing
the connection.

## Why this exists

An account can collect investment history from two places. A person records
trades by hand, or has an agent record them from statements
(`investments add`, `investments_record`, source type `manual`), and later
connects the same brokerage through sync (source type `plaid`). MoneyBin does
not yet match investment events across sources, so the two histories
interleave. Every trade appears twice, lots double-count, and cost basis mixes
two separate accountings.

MoneyBin contains that today rather than tolerating it:

- `core.dim_holdings` withholds every figure for every position in the account
  (`valuation_status = 'source_overlap'`).
- `system doctor`'s `investment_source_overlap` check fails.
- The holdings, lots and gains reads set `summary.degraded_reason`.

The only remedy the check offers is `import_revert`. It deletes the recorded
history permanently, and it is the wrong choice whenever the recorded history
is the one the user wants. There is no remedy that goes the other way:

- **Deleting synced rows doesn't last.** Plaid sends a full holdings snapshot
  on every pull and re-sends investment transactions in overlapping date
  windows, so the next pull puts the rows back. The detector also counts
  holdings rows, so the overlap would come back even if no trade did.
- **Disconnecting doesn't fit.** `sync_disconnect` is remote only and keeps
  every pulled row. It also acts on the whole connection, not one account.
  A disconnect that also purged the connection's rows would throw away
  cash transactions and balances for every account on the connection, can't
  be undone, and loses history older than the provider's window.

What the user is actually trying to express is a choice about one account:
which history to trust. This spec makes that choice a setting.

M1J.7 ([`investment-event-matching.md`](investment-event-matching.md)) is the
eventual answer for people who want both histories combined, with the recorded
past and the synced present matched event by event. That spec names the cost
of the interim posture: "Suppressing one source wholesale is safer than a
wrong ledger, but it also hides legitimate non-overlapping history." This
feature is that wholesale choice, made deliberately by the user, reversible,
and available now. It does not replace matching. Once matching can accept
events, a user who wants both histories clears the setting.

## Goals

1. One per-account setting chooses which investment source type feeds the
   ledger: unset (every source, today's behavior), `manual`, or `plaid`.
2. Choosing a source clears the overlap at once: the ledger rebuilds, holdings
   are valued again, and the doctor check passes for that account.
3. Nothing is deleted. Clearing or changing the setting brings the excluded
   history back exactly as it was.
4. Sync keeps running for the account. Balances, cash transactions, prices and
   the broker's holdings snapshot keep arriving.
5. The doctor's fix shows the user what each choice keeps, as per-source trade
   counts and date ranges, and an agent can't make the choice on the user's
   behalf.

## Non-goals

- **Matching or merging the two histories.** That is M1J.7.
- **A cutoff date** ("recorded before 2024-09, synced after"). Rejected
  because a manual history dated by trade date and a synced one that can
  arrive dated by settlement date disagree for a few days around any
  boundary. A trade on the boundary is then counted twice or dropped, with
  nothing to show it happened. Stitching histories at a boundary is
  whole-event matching, which is M1J.7's job.
- **Deleting excluded rows.** Use `import_revert` to delete recorded history
  on purpose.
- **Choosing per security, or per connection inside one account.** The
  overlap is a property of the account's ledger, so that is where the choice
  sits.
- **Investment file import.** None exists today. When one ships, its rows
  carry their own source type and that value becomes a valid choice (see
  §Vocabulary).

## User experience

The overlap starts with the sync warning that already exists, with its pointer
corrected (§Messages that change):

```
! Needs attention
  Investment sources   1 account has both manual and Plaid history
→ moneybin system doctor
```

The doctor then explains the overlap and lists one fix per source. The account
id and figures below are synthetic:

```
✗ investment_source_overlap   1 account has investment history from two sources
   → Consider keeping the recorded history (412 trades, 2019-03-04 → 2025-11-21)
     and ignoring the connection's (96 trades, 2024-09-03 → 2026-09-30):
     moneybin accounts set acct_7f3a9c --investment-source-type manual
   → Consider keeping the connection's history (96 trades, 2024-09-03 → 2026-09-30)
     and ignoring the recorded one (412 trades, 2019-03-04 → 2025-11-21):
     moneybin accounts set acct_7f3a9c --investment-source-type plaid
```

Running either command saves the setting and rebuilds the ledger before it
returns, with the standard settings receipt carrying an "Investment source"
row:

```
Account settings updated
Account ID:         acct_7f3a9c
Updated fields:     investment_source_type
Investment source:  Using 412 recorded trades; ignoring 96 synced trades (kept, not deleted)
```

After the rebuild the doctor passes, holdings show values again, and the next
sync doesn't bring the warning back. `moneybin accounts get` lists the
setting. `--clear-investment-source-type` goes back to using both sources;
until M1J.7 acceptance ships, that brings the overlap and its warning back.

Over an MCP connection, the doctor's result carries the same two fixes as
`accounts_set` recovery actions with `confidence: "suggested"`, so an agent
must ask which history to keep before calling `accounts_set`.

Two consequences the user lives with afterwards:

- **Under `manual`, recorded history ends where recording stopped.** The
  broker's holdings snapshot still arrives, so the existing reconciliation
  checks point at the gap. `investment_holdings_divergence`,
  `investment_unreported_holdings` and `investment_phantom_holdings` report
  where the recorded history and the broker disagree, and `core.dim_holdings`
  keeps withholding figures for a position whose share count disagrees, as it
  does today. Keeping current means recording new trades.
- **Under `plaid`, new recorded trades are refused** with
  `investment_source_excluded`, because they would never reach the ledger.
  The message names the setting and how to change it.

## Vocabulary

- **Investment source type** (`investment_source_type`). The one
  [Source type](../../CONTEXT.md) whose investment rows feed the account's
  ledger. It is not a new vocabulary. Its values are exactly the source types
  that `core.fct_investment_transactions` unions, which today are `manual` and
  `plaid`. NULL means every source type, which is today's behavior.
- The Plaid opening-lot bootstrap rows (`subtype = 'opening_bootstrap'`) carry
  source type `plaid`. They follow the setting with no special case: `plaid`
  keeps them and `manual` drops them. That is correct in both directions,
  because the bootstrap reconstructs the gap in the *synced* history, and a
  recorded history has no such gap to fill.

The validator's accepted set is the source types the ledger unions, kept as a
single constant next to the model. There is no DDL `CHECK`, because a new
investment importer must become choosable without a migration. That breaks
from `default_cost_basis_method`'s `CHECK` on purpose: that column's
vocabulary is fixed by tax law, while this one grows with each importer.

## Design

### Durable state: `app.account_settings`

Two columns, appended last because `ALTER TABLE ADD COLUMN` always appends and
`init_schemas()` must produce the same column order as an upgraded database:

| Column | Type | Meaning |
|---|---|---|
| `investment_source_type` | `VARCHAR` | The source type whose investment rows feed this account's ledger; NULL uses every source |
| `investment_source_type_changed_at` | `TIMESTAMP` | When `investment_source_type` last changed, including a clear and an undo; NULL if it never changed |

`investment_source_type_changed_at` is not redundant with `updated_at`. That
column moves on any settings edit, so folding it into the ledger's watermark
would advance every investment row on every rename. This one moves only when
the set of ledger rows changes. It survives a clear, so rows that come back
into the ledger after a clear carry a timestamp newer than the clear (see
§Freshness).

Delivered by one migration that adds both columns. Existing rows get NULL,
which keeps today's behavior.

### Surface through `core.dim_accounts`

No model joins `app.account_settings` directly; `core.dim_accounts` is the
single resolved view (`database.md`). It gains both columns.
`core.fct_investment_transactions` already joins `core.dim_accounts` for the
currency fallback, so the filter reuses that join.

### The ledger filter

`core.fct_investment_transactions` keeps a unioned row only when the account
has no choice set, or when the row's source type is the choice:

```sql
WHERE a.investment_source_type IS NULL
   OR u.source_type = a.investment_source_type
```

That is the only place the setting changes data. Every model that derives from
the ledger inherits the filter: lots, realized gains, `core.dim_holdings`'s
`source_overlap_accounts`, and `InvestmentService._source_overlap_accounts`.
None of them is edited. An account whose ledger now has one source type no
longer satisfies `COUNT(DISTINCT source_type) > 1`, so the withholding lifts by
the same predicate that imposed it.

Staging models and the raw tables are untouched. Excluded rows stay in
`prep.stg_plaid__investment_transactions` and
`prep.stg_manual__investment_transactions`, readable through `sql_query` and
restored by a clear.

### Freshness

[`core-updated-at-convention.md`](core-updated-at-convention.md) requires
`updated_at` to advance whenever a row's values change. A choice changes which
rows exist, and whether the account's positions are withheld:

- **`core.fct_investment_transactions.updated_at`** folds
  `investment_source_type_changed_at` for the account's rows. A row that
  re-enters the ledger after a clear or a change reports a timestamp newer than
  the change, rather than its original `created_at`, which an incremental
  reader has already passed.
- **`core.dim_holdings.updated_at`** folds the same term per account. Today,
  clearing an overlap rewinds that watermark: the convention spec's
  §"`dim_holdings` can rewind when a source overlap clears" names a missing
  "persisted, account-scoped signal" as the reason. This column is that signal
  for the source-choice remedy, so that section is narrowed: the rewind
  remains only for `import_revert`.

### Restating on change

`AccountService` treats a change to `investment_source_type` the way it treats
`default_cost_basis_method`. After the settings write commits, it restates the
derived models from `core.dim_accounts` down, through the same
`TransformService.restate_models` path `restate_fx_accounting` uses. The set
returns only after the restate, so the confirmation describes the rebuilt
ledger. `undo_service` sets `needs_restatement` for this field too, and an
undo also advances `investment_source_type_changed_at`, because undoing the
setting changes the ledger just as setting it did. If the restate fails, the
setting stays saved, and the error says so and names `moneybin refresh` as the
retry. That matches how the cost-basis setting handles a failed restate.

Undoing the account's first-ever settings write would delete the row and its
change time with it. The refusal reads the live row, not the write's audit
image: it covers any account whose settings row has ever carried a source
choice (a non-NULL `investment_source_type_changed_at`, which a cleared or
undone choice keeps). That undo is refused with `recovery_no_path`, and the hint
says the row must stay and to change the other fields instead. An account that
never had a choice still deletes the row as before.

### Detection skips a chosen account

`moneybin.investments.source_overlap.investment_source_overlap` excludes every
account whose `investment_source_type` is set. A chosen account no longer
reaches the sync warning (`PullResult.investment_source_overlap_accounts`) or
the doctor check. Choosing a source settles the account until the user clears
the choice, and a later pull doesn't raise it again. The detector reads
`app.account_settings` directly because it runs before any transform, which is
also why it reads raw. The setting is user state, not derived state, so there
is no `core` copy to wait on.

The same module gains the evidence that the doctor's fix needs:

```python
@dataclass(frozen=True)
class SourceEvidence:
    source_type: str  # 'manual' | 'plaid'
    trade_count: (
        int  # current raw observations; review routing happens later in staging
    )
    first_trade_date: date | None
    last_trade_date: date | None
    holdings_only: bool  # plaid evidence is a holdings snapshot with no trades


def investment_source_evidence(
    db: Database, account_ids: Sequence[str]
) -> dict[str, list[SourceEvidence]]: ...
```

Counts come from the same raw scope the detector already uses: the current
Plaid receipts, and manual rows resolved through
`prep.int_manual__investment_identity`. They don't come from the ledger,
because the doctor has to describe an account that hasn't been transformed
yet. They count current raw observations, not ledger rows: staging may later
route a Plaid row to review instead of the ledger, and the opening-lot
bootstrap rows exist only in the ledger, so a count can differ from the ledger
by those rows. A holdings-only Plaid source reads as "a holdings snapshot, no trades"
instead of "0 trades".

### Doctor: `investment_source_overlap`

- `affected_ids` switch to the masked `account:<id>` form every other
  account-scoped check emits (`_masked_account_affected_ids`). Today the check
  returns raw ids, which is a pre-existing gap that this change closes.
- `detail` drops the "revert the redundant import batch" prose and says each
  overlapping account needs a source chosen, and that choosing deletes nothing.
- The recipe replaces the single `import_revert` action with two `accounts_set`
  actions per affected account, one per source present:

  ```python
  RecoveryAction(
      tool="accounts_set",
      arguments={"account_id": <command id>, "investment_source_type": "manual"},
      rationale=(
          "Keep the recorded history (412 trades, 2019-03-04 → 2025-11-21) and "
          "ignore the connection's (96 trades, 2024-09-03 → 2026-09-30); "
          "nothing is deleted and clearing the setting restores both"
      ),
      confidence="suggested",
      idempotent=True,
  )
  ```

  - Both actions are `suggested`. MoneyBin cannot know which history the user
    trusts, so no agent may pick one on its own
    (`.claude/rules/design-principles.md` → "Magic stays visible"). The
    rationale carries counts and date ranges instead of a recommendation.
  - The account argument follows `_command_account_id`. An id that would be
    altered by sanitizing or masking becomes a placeholder, and the rationale
    asks for the id.
  - `import_revert` is no longer offered. It still works as a deliberate
    delete, but as the suggested fix it was a destructive answer to a question
    the setting answers without loss.
- The CLI's `_recovery_command` maps an `accounts_set` fix carrying
  `account_id` and `investment_source_type` to
  `moneybin accounts set <id> --investment-source-type <value>`. With two fixes
  per account, the CLI lists both fixes for every overlapping account. The
  JSON output and MCP list the same fixes.
- The recipe-round-trip test covers the new actions, so each one is a
  runnable call.

### Other doctor checks

A check that reports on rows the choice excluded skips them:

- `investment_staging_rejects` and `investment_opening_lot_review` read Plaid
  staging or bootstrap rows directly. Each one skips Plaid rows in an account
  set to `manual`. On a `core.dim_accounts` that predates the column (migrated
  but not yet refreshed) no choice is readable, so both run without the filter,
  as the match planner does.
- `investment_unmodeled_legs` and `investment_unresolved_securities` read the
  ledger, so they inherit the filter and need no change.
- `investment_holdings_divergence`, `investment_unreported_holdings` and
  `investment_phantom_holdings` compare the ledger to the broker's snapshot.
  They are unchanged and keep running under both choices. Under `manual`,
  these checks are what tell the user that recorded history has fallen behind.

### M1J.7 planner

The review-only matching planner skips accounts with a choice set and
proposes no matches there. A proposal on an account whose ledger has one
source is noise, and when acceptance ships, accepting a match on such an
account would conflict with the user's choice. Clearing the setting brings the
account back into the planner. Existing pending proposals for a newly chosen
account go stale under the planner's existing freshness rules. This spec
deletes none of them.

### Recording into an excluded source

`InvestmentService.record_event` and `record_events` refuse an account whose
`investment_source_type` is set and isn't `manual`. They raise `UserError` with
the new code `investment_source_excluded` and write nothing. For
`record_events` this is a hard failure (pass 1 aborts the batch), because one
refused event among others is a mistake about the account, not about the
event. The message names the setting and the command that changes it, and
leaves out the account's label:

```
This account's investment history comes from plaid (investment_source_type),
so a recorded trade would not reach its ledger. To record trades here:
moneybin accounts set <account> --investment-source-type manual
```

Sync is not refused under `manual`. Its rows are still captured, so a later
change of mind restores them without a re-pull.

### Settings surfaces

| Surface | Change |
|---|---|
| `accounts set` (CLI) | `--investment-source-type {manual,plaid}`, `--clear-investment-source-type` |
| `accounts_set` (MCP) | `investment_source_type` parameter; `"investment_source_type"` added to `_CLEARABLE_FIELDS`; tool description gains one sentence |
| `accounts get` / `accounts_get` | the setting appears in `AccountDetail`; the `accounts_set` result carries it in `AccountSettingsPayload` |
| Confirmation (CLI + MCP) | the per-source counts after the rebuild, trades used and trades ignored: an "Investment source" row in the CLI receipt, and the single `actions[]` entry of the MCP envelope |

The CLI flag accepts the value lowercase only, matching how
`--default-cost-basis-method` validates. An unknown value fails with
`mutation_invalid_input` and lists the accepted set.

Adding a parameter to `accounts_set` keeps the MCP tool count at 50.

### Messages that change

Every message that names `import_revert` as the overlap remedy now names the
setting instead:

- the `investment_source_overlap` doctor `detail` and recipe;
- `InvestmentService._source_overlap_reason`, the `degraded_reason` on
  holdings, lots and gains: "…choose one source for the account with
  `moneybin accounts set <account> --investment-source-type manual|plaid`…";
- the MCP `sync_pull` action hint, which already says "until one source is
  chosen per account" and now names `accounts_set`;
- the CLI sync attention block. Its pointer today is `→ moneybin doctor`, which
  is not a command (it exits 2, `No such command 'doctor'`). It becomes
  `→ moneybin system doctor`, because the doctor is where the counts are. The
  row also gets correct singular and plural forms ("1 account has").

## Observability

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `moneybin_investment_source_choice_accounts` | Gauge | `investment_source_type` (`manual`, `plaid`) | Accounts with each choice set, refreshed when `investment_source_overlap` runs |

Setting it during the doctor run follows the pattern of the
`moneybin_net_worth_*` gauges, a read-path refresh that takes no write lock.
Changes to the setting are already in `app.audit_log` through
`account_settings.set`, so no counter is added.

Logs may include account ids, source types, and counts. They must not include
account labels, trade descriptions, or amounts.

## Scenario matrix

| Starting state | Action | Expected |
|---|---|---|
| Manual + Plaid trades, no choice | doctor | `fail`; two `accounts_set` actions with correct counts and ranges |
| same | set `manual` | ledger has only manual rows; positions `valued` (or `withheld` where the share count disagrees with the snapshot); doctor `investment_source_overlap` `pass` |
| same | set `plaid` | ledger has Plaid rows plus the opening bootstrap; doctor `pass` |
| set `manual` | `sync pull` re-delivers the same window | no overlap warning; Plaid raw rows refreshed; ledger unchanged |
| set `manual` | clear | ledger is the union again; re-entered rows' `updated_at` ≥ the clear; doctor `fail` again |
| set `plaid` | `investments add` | refused, `investment_source_excluded`, nothing written |
| set `manual` | undo the set | restated; `investment_source_type_changed_at` advanced; overlap back |
| an account whose settings row has ever carried a source choice (set, then cleared or undone) | undo the row's first-ever settings write | refused, `recovery_no_path`, row and change time kept; nothing changes |
| Manual trades + Plaid holdings only | doctor | `fail`; Plaid side reads "holdings snapshot, no trades" |
| No overlap | set `plaid` up front | accepted; later recorded trades refused; later sync never warns |
| Unknown value | `accounts set … --investment-source-type ofx` | `mutation_invalid_input` naming `manual`, `plaid` |

## Verification

- **Unit.** Service validation, clear, and restate trigger. The recipe output
  (action count, arguments, the masked placeholder path, counts in the
  rationale). The detector skipping a chosen account. The refusal in
  `record_event` and `record_events`. CLI flag parsing and the
  recovery-command mapping. MCP parameter and clearable-field round-trip.
- **SQLMesh.** The `fct_investment_transactions` filter in both directions,
  including the opening bootstrap following `plaid`. The `dim_holdings`
  withholding lifting through the unchanged predicate. Both `updated_at` folds,
  including the clear case.
- **Integration.** Pull, record, overlap, set, pull again: no warning, ledger
  stable. Then clear and confirm the overlap returns. Migration upgrade parity
  for the two appended columns.
- **Docs.** In [`moneybin-doctor.md`](moneybin-doctor.md), remove the "Open
  gap" note and rewrite the `import_revert` paragraphs. Narrow the
  [`core-updated-at-convention.md`](core-updated-at-convention.md) rewind
  section. Update the M1J row in [`../roadmap.md`](../roadmap.md) and add a
  changelog fragment.

## Deferred decisions

- **The `source_overlap` valuation status name.** It still describes an
  account with two sources and no choice set. A choice makes it unreachable
  for that account, so no rename is needed.
- **Combining a choice with M1J.7.** Once acceptance ships, a later spec
  decides whether a chosen account can opt back into matching without
  clearing the setting first. Until then, clear the setting to return the
  account to matching.
- **A choice does not follow an account merge.** When an account link merges
  the account holding the choice into another account, the setting stays under
  the absorbed id. The survivor then shows no choice, both histories return,
  and `investment_source_overlap` fails again until the choice is set on the
  survivor. Every other `app.account_settings` field has the same gap today.
  [#655](https://github.com/bsaffel/moneybin/issues/655) makes settings follow
  a merge under one conflict rule.
