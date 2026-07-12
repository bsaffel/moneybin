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
- [`moneybin-mcp.md`](moneybin-mcp.md) — `system_doctor` tool registration

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
| `fct_transactions_sign_convention.sql` | `fct_transactions_sign_convention` | No amount is NULL (zero is a modeled 'zero' direction, not a violation) | Any NULL amount |
| `bridge_transfers_balanced.sql` | `bridge_transfers_balanced` | Every transfer pair sums to within $0.01 | Any pair with `ABS(SUM(amount)) > 0.01` |
| `fct_investment_transactions_fk_integrity.sql` | `fct_investment_transactions_fk_integrity` | Every `fct_investment_transactions.account_id` resolves to `dim_accounts` | Any orphaned account_id |
| `fct_investment_transactions_sign_convention.sql` | `fct_investment_transactions_sign_convention` | The investment ledger obeys the accounting sign convention | Any violation of the convention |
| `fct_investment_transactions_uniqueness.sql` | `fct_investment_transactions_uniqueness` | Every `investment_transaction_id` appears once | Any duplicate id |

Each audit returns the offending `transaction_id` (or `debit_transaction_id` for transfers, `investment_transaction_id` for the investment ledger) as the first column. `DoctorService` uses this column for `--verbose` affected-ID output.

**Every audit file must declare `standalone TRUE`.** Without it SQLMesh loads the file as a *generic* `ModelAudit` that only executes when a model names it in an `audits (...)` property — so it silently never runs, and the suite stays green while the invariant goes unchecked. Standalone audits are also structurally non-blocking (a failure warns rather than raising), so a passing test run is never evidence that an audit holds.

### DoctorService extras (hardcoded)

**`dedup_reconciliation`** — Cross-layer count check that every imported row which disappears between the unioned staging layer and the core fact table is explained by recorded dedup decisions. The invariant is `raw_total - core_count == dedup_absorbed`, where `raw_total` is the row count of `prep.int_transactions__unioned`, `core_count` is the distinct `transaction_id` count of `core.fct_transactions`, and `dedup_absorbed` is `Σ(group_size - 1)` over every connected component in `prep.int_transactions__matched` — computed as `COUNT(*) - COUNT(DISTINCT match_group_id)` over rows where `match_group_id IS NOT NULL`. This formula is exact for any group topology: N-way merges, cyclic accepted-edge sets (e.g. three edges over a 3-node group still absorbs only 2 rows), and the common 1:1 pair case. `fail` when the counts disagree (a leak: rows vanished without a decision; or an un-applied match: a recorded decision didn't collapse its rows); `skipped` before the first transform (prep/core views absent). See `_run_dedup_reconciliation()` in `src/moneybin/services/doctor_service.py`.

**`categorization_coverage`** — What percentage of non-transfer transactions have a category. Status is `warn` (not `fail`) when below 50%; `pass` otherwise. Never blocks exit 0 on its own.

### Investment reconciliation (M1G.4)

Eight checks covering the Plaid investment ledger. They split into two families: **refusals surfaced** (staging declined to guess and filed the row for review — these must be visible, not silently dropped) and **divergence from the broker** (MoneyBin's derived position disagrees with what the provider reports).

| Name | What it checks |
|---|---|
| `investment_staging_rejects` | Rows staging routed to review rather than the ledger. Three reasons today — `split_underivable`, `transfer_direction_underivable`, `unmapped_subtype` — all deliberate refusals. The query is deliberately open (`review_reason IS NOT NULL`), so a new reason added upstream surfaces without a code change. |
| `investment_opening_lot_review` | Positions the opening-lot bootstrap refused to synthesize: short/non-positive quantity, NULL basis, and `sold_out_prewindow` gaps it declined to reconstruct rather than guess. |
| `investment_unmodeled_legs` | Legs in the ledger stripped of lot-affecting quantity (option and short legs MoneyBin models no book for). An assignment that exercises away a covered-call position disposes of real shares; the held lot never closes. |
| `investment_holdings_divergence` | Engine-derived held lots that disagree with the broker's newest snapshot, on positions MoneyBin *does* hold a lot for. |
| `investment_unreported_holdings` | Broker-reported positions with no `core.dim_holdings` row — the opposite direction, and the more dangerous one. |
| `investment_phantom_holdings` | Open lots MoneyBin holds that the broker's newest snapshot no longer reports. Keyed on the per-pull holdings-snapshot receipt (below), not on the presence of holdings rows. |
| `investment_unresolved_securities` | Ledger rows whose provider security key never resolved to a canonical security. These are dropped from cost basis entirely, so they must not stay silent. |
| `investment_source_overlap` | Accounts carrying both manual and Plaid investment history, where double-counting is possible. |

**The phantom check depends on `raw.plaid_investment_holdings_snapshots`.** Holdings *rows* cannot distinguish "this item reported and holds nothing" from "this item never reported" — an item whose pull returns an empty holdings array writes no rows at all, so a newest-snapshot join keyed on those rows silently keeps the last non-empty snapshot from an earlier pull. That reads a fully-liquidated broker as still holding its old positions: the largest possible net-worth overstatement, and precisely the phantom this check exists to catch. The receipt is written per (item, pull) **even when zero positions come back**, and both `core.dim_holdings` and this check derive "newest snapshot" from it.

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
❌ bridge_transfers_balanced — 2 transfer pairs sum to > $0.01
   Run with --verbose for affected pair IDs
⚠️  categorization_coverage — 43% of non-transfer transactions are uncategorized
✅ dedup_reconciliation

5 invariants checked across 14,203 transactions — 1 failing
```

With `--verbose`, affected IDs appear under each failing line:
```
❌ bridge_transfers_balanced — 2 transfer pairs sum to > $0.01
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
      {"name": "bridge_transfers_balanced", "status": "fail", "detail": "2 transfer pairs sum to > $0.01", "affected_ids": []},
      {"name": "categorization_coverage", "status": "warn", "detail": "43% of non-transfer transactions are uncategorized", "affected_ids": []},
      {"name": "dedup_reconciliation", "status": "pass", "detail": null, "affected_ids": []}
    ]
  },
  "actions": ["Run with --verbose to see affected transaction IDs"]
}
```

`affected_ids` is always `[]` unless `--verbose` is also passed.

## MCP Interface

**`system_doctor`** — registered alongside `system_status` in `src/moneybin/mcp/tools/system.py`.

```python
@mcp_tool(sensitivity="low", read_only=True)
def system_doctor() -> ResponseEnvelope:
    """Run pipeline integrity checks across all SQLMesh named audits.
    Returns pass/fail/warn per invariant plus a transaction count.
    Read-only — never writes. Call before relying on analytical results
    to confirm the pipeline is self-consistent."""
```

Always runs with `verbose=False` — affected IDs are omitted (agents can query `core.fct_transactions` or `core.bridge_transfers` directly for drill-down). Registered in `register_system_tools()`.

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
- `src/moneybin/mcp/tools/system.py` — add `system_doctor`, register in `register_system_tools()`
- `docs/specs/INDEX.md` — add this spec; update `data-reconciliation.md` entry with cross-reference
- `docs/specs/moneybin-mcp.md` — document `system_doctor`
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
