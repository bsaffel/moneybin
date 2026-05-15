# Core `updated_at` Convention

## Status

- **Type:** Architecture
- **Status:** implemented
- **Authority:** Defines the row-level freshness contract for `core.*` models and the model-level freshness surface in `meta.*`. Cited by future specs that probe pipeline or row freshness (e.g., the eventual `agent-ingest-completion.md`).

## Goal

Make "how fresh is this row?" answerable from any `core.*` model with a single column read, and "how fresh is this model?" answerable from one stable view, without misleading semantics or per-table heuristics.

## Background

A `private/followups.md` audit found that `updated_at` is present on `core.dim_accounts` (populated via `CURRENT_TIMESTAMP` at SQLMesh refresh) but absent from `core.fct_transactions`, `core.dim_categories`, and `core.dim_merchants`. The asymmetry creates two failure modes:

1. **Downstream tools assume a uniform per-row freshness column and fall back to per-table heuristics** (e.g., reading `loaded_at` here, `created_at` there). Each new consumer relearns the inventory.
2. **The existing `dim_accounts.updated_at` works only by coincidence of materialization.** `CURRENT_TIMESTAMP` evaluates at write time when the model is `kind FULL`. If `dim_accounts` ever becomes incremental, the column silently stops meaning what its name says — every row would carry the latest partition's write time rather than the row's own change time. The same expression in a view (`fct_transactions`, `dim_categories`, `dim_merchants` are all `kind VIEW`) evaluates at `SELECT` time and is meaningless as a freshness signal.

A separate concern is that two different questions get conflated under the name "freshness":

- **Row freshness** — when did any input contributing to *this row's current values* most recently change? Per-row fact.
- **Model freshness** — when did SQLMesh last apply (or last change the data for) *this model*? Per-event, per-model fact.

`CURRENT_TIMESTAMP AS updated_at` on a FULL table technically answers the first question but is used as the second. They should be separated.

### Related specs

- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — Owns the data-layer table and the `core.*` / `meta.*` schema definitions. Receives a small scope-clarification edit from this spec (see [Cascading edits](#cascading-edits)).
- [`account-management.md`](account-management.md) — Establishes `app.account_settings.updated_at` (with service-managed `NOW()` on UPDATE). This spec extends the same pattern to other `app.*` reference tables.
- [`categorization-cold-start.md`](categorization-cold-start.md) — Establishes the seed/user/override layering for categories and merchants that this spec's per-row formulas reflect.

### Non-goals

- Building the `agent-ingest-completion.md` consumer or a CLI `freshness` subcommand. Those land when downstream specs need them; this spec defines the surface they read from.
- Reconciliation tooling that compares row freshness to expected SLAs. Out of scope.
- Backfill of historical `updated_at` for rows that pre-date this convention. Pre-existing rows get the value computed from the inputs they currently reference (which may be the first apply after this lands).

## Convention

**`core.<entity>.updated_at` = `MAX` of timestamps from inputs that change *per row*. Inputs that change *per model* (seeds, reference tables) contribute `NULL` to the per-row formula and are surfaced separately as model-level freshness in `meta.model_freshness`.**

Two corollaries:

1. **Never use `CURRENT_TIMESTAMP AS updated_at` inside a view.** It evaluates at `SELECT` time and conveys nothing about row provenance.
2. **Never use `CURRENT_TIMESTAMP AS updated_at` inside a FULL table either** unless the intent really is "every row gets the same timestamp on every apply." For the dim models in scope here, that's the wrong semantic — replace with a `GREATEST(...)` over real per-row inputs.

### Per-model formulas

All `updated_at` columns are `TIMESTAMP` and may be `NULL` only where the row's inputs all contribute `NULL` (pure-seed rows in `dim_categories` / `dim_merchants`).

| Model | `kind` | `updated_at` expression |
|---|---|---|
| `core.dim_accounts` | FULL | `GREATEST(winner.loaded_at, settings.updated_at)` |
| `core.fct_transactions` | VIEW | `GREATEST(t.loaded_at, c.categorized_at, notes_latest, tags_latest, splits_latest)` where `notes_latest = MAX(notes.created_at)`, `tags_latest = MAX(tags.applied_at)`, `splits_latest = MAX(splits.created_at)`, each grouped per transaction. Notes/tags/splits are insert-only today, so their per-row freshness column is the create-time column. `c` is `app.transaction_categories`, whose write-time column is `categorized_at`. |
| `core.dim_categories` | VIEW | `COALESCE(user_categories.updated_at, override.updated_at)` — `NULL` for pure-seed rows. |
| `core.dim_merchants` | VIEW | `COALESCE(user_merchants.updated_at, override.updated_at)` — `NULL` for pure-seed rows. |
| `core.fct_balances` | VIEW | The contributing observation's freshness column: `loaded_at` from OFX/tabular staging sources, `created_at` from `app.balance_assertions`. Requires extending `fct_balances` CTEs to project these timestamps through to a single `updated_at` output column. |

### Semantics for consumers

- A row's `updated_at` advances if and only if a real input to that row changed (raw data reloaded, user edit, override toggled). It does *not* advance just because SQLMesh re-applied with no input change. This is the desired property — it lets consumers cheaply ask "what changed since timestamp X?" without false positives from idempotent reruns.
- `NULL` `updated_at` means "this row's freshness is the model's freshness." Consumers needing a non-`NULL` answer should `COALESCE(updated_at, <meta.model_freshness.last_changed_at for this row's contributing seed model>)`. The model name is derivable from row-level fields where it matters (e.g., `is_user = FALSE` rows in `dim_categories` came from `seeds.categories`).
- `updated_at` does not imply causality across rows. Two rows with the same `updated_at` may have changed for unrelated reasons.

## Model-level freshness: `meta.model_freshness`

`meta.model_freshness` is a SQLMesh view that wraps SQLMesh's internal state schema and exposes a stable public contract:

```sql
-- meta.model_freshness columns
model_name        VARCHAR   -- fully-qualified name, e.g. 'core.dim_accounts', 'seeds.categories'
last_changed_at   TIMESTAMP -- when SQLMesh last created a new snapshot for this model (i.e., the model's content or definition changed)
last_applied_at   TIMESTAMP -- when SQLMesh last applied (refreshed/materialized) this model, regardless of whether its content changed
```

The view reads from SQLMesh's `_snapshots` (and, if needed, `_intervals`) tables. The exact source-column mapping is determined at implementation time by inspecting a SQLMesh-applied database — this spec commits only to the public column shape so the view can absorb upstream renames without breaking consumers.

### Schema placement: why `meta`, not `reports`

`reports.*` is for **user-facing curated presentation models** (`reports.net_worth`, `reports.spending`, ...). Pipeline metadata is internal-state plumbing — not a report. `meta` already houses provenance (`meta.fct_transaction_provenance`), and pipeline freshness is the same conceptual family: facts about how data got to where it is.

This expands `meta`'s scope description in `architecture-shared-primitives.md` from "cross-source provenance" to "provenance and pipeline metadata" — see [Cascading edits](#cascading-edits).

### Typed programmatic accessor

A `SystemService.model_freshness()` method provides a typed alternative to ad-hoc SQL, alongside the existing `SystemService.status()` data-inventory snapshot:

```python
# src/moneybin/services/system_service.py
@dataclass(slots=True)
class ModelFreshness:
    model_name: str
    last_changed_at: datetime | None
    last_applied_at: datetime | None

class SystemService:
    def model_freshness(self, model_name: str) -> ModelFreshness | None:
        """Return freshness for one SQLMesh model, or None if not yet applied."""
```

A thin wrapper over `meta.model_freshness`. Lives on `SystemService` because model-level freshness is the same conceptual family as data inventory and `last_import_at` — answered with the same service surface that other "system status" queries already use.

## App-table schema changes

The per-row formulas above require that every `app.*` reference table contributing to a `core.dim_*` row carries an `updated_at` column. Five already do (`app.account_settings`, `app.budgets`, `app.imports`, `app.category_overrides`, `app.merchant_overrides`); two do not. This spec adds:

| Table | DDL change |
|---|---|
| `app.user_categories` | Add `updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`. |
| `app.user_merchants` | Add `updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`. |

DuckDB has no `ON UPDATE` trigger, so services that write to these tables must set `updated_at = NOW()` on `UPDATE` (and `INSERT … ON CONFLICT DO UPDATE` statements). This is already the established pattern — see `account_service.py:276` and `categorization_service.py:1106`. The exact call sites needing updates are enumerated during plan execution.

The DDL change ships as a SQL migration under `src/moneybin/sql/migrations/`. Backfill: existing rows take `CURRENT_TIMESTAMP` at migration time (via the default). This is a one-time approximation — pre-existing rows lose their true last-edit time, which the project does not track today anyway.

For the two existing `*_overrides` tables, the live schema is `updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP` (nullable). The DDL migration also tightens these to `NOT NULL` for uniform per-row freshness guarantees — all existing rows are already populated by the default, so the tightening succeeds without backfill.

## What ships

1. **DDL migration** — `app.user_categories` and `app.user_merchants` gain `updated_at`. `app.category_overrides` and `app.merchant_overrides` tighten existing `updated_at` to `NOT NULL`.
2. **Service writes** — every `INSERT … ON CONFLICT DO UPDATE` and bare `UPDATE` against the four tables above sets `updated_at = NOW()`. Specific call sites identified during plan execution.
3. **SQLMesh model edits** — five core models (`dim_accounts`, `fct_transactions`, `dim_categories`, `dim_merchants`, `fct_balances`) gain/replace `updated_at` per the formulas above. The misleading `CURRENT_TIMESTAMP AS updated_at` on `dim_accounts` is replaced. `fct_balances` also gains `loaded_at` propagation through its source CTEs.
4. **`meta.model_freshness` view** — new SQLMesh model in `sqlmesh/models/meta/` exposing the public column contract above.
5. **`SystemService.model_freshness()` method** — typed wrapper over the view; lives alongside `SystemService.status()` and the `ModelFreshness` dataclass in `src/moneybin/services/system_service.py`.
6. **Per-column comments** — every `updated_at` column on the five core models gets a comment matching the convention (see [Documentation](#documentation)).
7. **Cascading edits** — see below.

## What does NOT ship

- Materialization changes to `fct_transactions`, `dim_categories`, `dim_merchants`. They stay views; the `GREATEST(...)` / `COALESCE(...)` expressions work in views.
- A `meta.transform_apply_log` table or any other custom event log. SQLMesh state already tracks this; we wrap it via `meta.model_freshness`.
- Any user-facing CLI surface (`moneybin freshness`, etc.). Surfaces are added when consuming specs (`agent-ingest-completion.md` or similar) need them.
- Reconciliation logic that compares `updated_at` against SLAs or alerts on stale data. Out of scope.
- **`core.fct_balances_daily`.** It is a Python carry-forward model that synthesizes one row per account-day from first to last observation. Interpolated days have no per-row "input that changed" — assigning them an `updated_at` would either propagate the contributing observation's timestamp (defensible, but adds complexity) or fall back to model-level rebuild time (misleading). Defer until a consumer needs per-day balance freshness; for now, callers needing balance-table freshness use `core.fct_balances.updated_at` directly or `meta.model_freshness` for the daily model.

## Cascading edits

These ride along with the spec landing:

1. **`architecture-shared-primitives.md` — `meta` schema row.** Update the "Purpose" column in the data-layer table from "Cross-source provenance. Tracks which source row(s) contributed to each canonical row in `core`." to "Provenance and pipeline metadata. Cross-source row lineage (`fct_*_provenance`) and model-level freshness (`model_freshness`)."
2. **`architecture-shared-primitives.md` — column-comment convention.** Add a one-line note under "SQLMesh Layer Conventions" naming the `updated_at` convention so future model authors don't re-derive it.
3. **`private/followups.md` — remove the `core-updated-at-consistency` entry.** Resolved by this spec.

## Documentation

Per-column comments on every `updated_at` column:

> `-- Latest of all per-row input timestamps contributing to this row's current values. NULL when all contributing inputs are model-level (seeds, reference tables) — query meta.model_freshness for those. Does not advance on idempotent SQLMesh re-applies.`

Both consumers and reviewers should be able to read this without opening this spec.

## Testing surface

| Layer | Coverage |
|---|---|
| **Unit** | `SystemService.model_freshness()` returns the right dataclass; returns `None` for unknown models. |
| **SQLMesh model** | Each touched core model has an audit (or scenario assertion) that `updated_at` is non-`NULL` for rows whose inputs all have timestamps, and is `NULL` only where expected (pure-seed rows in `dim_categories` / `dim_merchants`). |
| **Migration** | DDL migration adds the column with the documented default; existing rows get `CURRENT_TIMESTAMP` at migration time. |
| **Service** | Each updated write path sets `updated_at = NOW()` on UPDATE; verified by inspecting the row after edit. |
| **Scenario** | One end-to-end scenario: edit a user-category, run `transform apply`, verify `core.dim_categories.updated_at` advances for that row and not for unrelated rows. |
| **`meta.model_freshness`** | Smoke test: applying the pipeline, then querying the view, returns a row per registered SQLMesh model with non-`NULL` `last_applied_at`. |

## Open verification — resolved

The four verification items deferred to plan time were resolved during plan preparation:

1. **SQLMesh state schema** — confirmed as `sqlmesh._snapshots` (SQLMesh `c.SQLMESH` constant = `"sqlmesh"`; default `state_schema`). Relevant columns: `name` (quoted FQN like `"core"."dim_accounts"`), `version` (content fingerprint), `updated_ts` (BIGINT, Unix milliseconds). The `meta.model_freshness` view strips the quotes from `name` and converts `updated_ts` from millis to `TIMESTAMP`.
2. **`fct_balances` aggregation shape** — confirmed: one row per observation today, three UNION ALL CTEs (OFX, tabular, user assertion). Source CTEs do not currently project `loaded_at`/`created_at` to the final SELECT; the plan extends them.
3. **Service call sites** — enumerated in the plan's File Structure section.
4. **Curation-table timestamp columns** — confirmed via live schema: `app.transaction_notes.created_at`, `app.transaction_tags.applied_at`, `app.transaction_splits.created_at`, `app.transaction_categories.categorized_at`. All four tables are insert-only or have a single write-time column; the formulas use those columns directly.

## Risk

- **Largest risk: a query consumer somewhere does `SELECT *` from a touched view and breaks when the column shape changes.** `SELECT *` against `core.*` is discouraged but not forbidden. Plan execution greps for `SELECT \*` against the five models and either narrows the projection or accepts the new column.
- **Second-largest risk: the SQLMesh state schema differs from expectation.** Mitigated by verifying against an applied database before writing the view definition.
- **Third: a service write path is missed and `updated_at` goes stale for some rows.** Mitigated by the service-level test surface above and by the `NOT NULL DEFAULT` constraint preventing accidental `NULL` inserts.
