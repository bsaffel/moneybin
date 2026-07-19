# Reports Foundation — One Contract, Coherent Surface

> Child spec of [`reports-overview.md`](reports-overview.md) (milestone **M2P.1**).
> Status: implemented
> Type: Feature
> Last updated: 2026-07-18 — implemented; R1–R6 shipped on `feat/reports-foundation`.
> Companions: [ADR-013](../decisions/013-report-classification-declared.md)
> (declared classification), [`privacy-data-classification.md`](privacy-data-classification.md),
> [`queryable-internal-schemas.md`](queryable-internal-schemas.md),
> [`extension-contracts.md`](extension-contracts.md),
> [`reports-recipe-library.md`](reports-recipe-library.md).

## Goal

Make the `reports.*` surface honest before anything is built on top of it.
Today the schema is fully queryable through `sql_query`, but its privacy
coverage rests on a hand-maintained bridge and a fallback that fails **open**.
This spec closes that permanently and gives report authors — human or agent — a
single stated contract.

Implements decisions **D3**, **D4**, and **D5** from the umbrella.

## Background: what actually went wrong in #330

PR #330 widened `sql_query` to the whole `reports` schema. The declared-class
map covered 6 of 8 deployed views — `net_worth` and `uncategorized_queue` were
uncovered.

The missing declaration was the symptom, but not on the column the original
telling of this story named. `uncategorized_queue.account_id` returning
unmasked was not itself wrong: `account_id` is a deliberately opaque minted
surrogate classified `RECORD_ID` (LOW) everywhere in `CLASSIFICATION` (see
[`account-identity-resolution.md`](account-identity-resolution.md) Decisions 1
and 6), so passing it through unmasked is correct, not a leak. What the
coverage gap actually let through was **tier**, on different columns: every
column in the two uncovered views fell through to `AGGREGATE` (LOW), including
`net_worth`/`total_assets`/`total_liabilities` (`BALANCE`, HIGH) on
`net_worth` and `amount`/`priority_score` (`TXN_AMOUNT`, HIGH) on
`uncategorized_queue` — five HIGH-tier financial columns served at LOW.

The mechanism is that **two fallbacks disagreed about what an undeclared
column means**:

| Path | Undeclared column resolves to | Where |
|---|---|---|
| Report tool (`reports_*`) | `ACCOUNT_IDENTIFIER` → masked | `reports/_framework/classify.py` (`_FAIL_CLOSED`) |
| `sql_query` (pre-fix) | `AGGREGATE` (LOW) → **clear** | `privacy/sql_lineage.py`'s single fallback (R1 below splits it in two) |

The report framework already reasoned about this correctly and called its
fallback "defense in depth." `sql_query` did not. Until that asymmetry was
fixed, every future coverage gap was a silent leak rather than a visible
annoyance — and wiring `discover_reports()` for package reports (M2M) is
exactly the event that would have re-armed it.

## Requirements

### R1 — Fail closed on coverage gaps, not on unresolved expressions

`sql_lineage.py`'s undeclared-column fallback must distinguish two cases that
it previously conflated:

1. **An expression we could not resolve** — an alias we couldn't trace, a
   computed projection. Keep today's behaviour: take the max tier of the
   scope's resolvable input columns, floor `AGGREGATE`. This is normal and
   must stay permissive, or the BI surface over-masks constantly.
2. **A column that resolved to a real table in a schema owed complete
   declarations, which has no declaration.** This is a *coverage gap*: our
   registry is wrong, not the query. Fail closed and log a warning naming the
   `(schema, table, column)`.

The distinction is the requirement. A blanket fail-closed would break case 1
and is explicitly **not** what this asks for.

**Shipped as:** case 1 stays `_scope_input_max`/`_conservative_floor`
(unchanged); case 2 is the new `_coverage_gap_class`, both in
`src/moneybin/privacy/sql_lineage.py`. Case 2 fails closed to
`DataClass.UNRESOLVED` (`FAIL_CLOSED_CLASS`), not `ACCOUNT_IDENTIFIER` as
first proposed here — `UNRESOLVED` masks the value WHOLE rather than
partially, which is the right answer for a column lineage never positively
classified (a partial `"****" + value[-4:]` mask would surface characters of a
value we cannot name). See the `DataClass.UNRESOLVED` docstring in
`taxonomy.py`.

**Why this comes first:** it converts every subsequent derivation miss from a
silent leak into visible over-masking. Everything after R1 is an ergonomics
problem rather than a security one, and can be iterated on safely.

### R2 — Derive column classes from model source, verified in CI

Classes for `reports.*` views are **derived**, not hand-asserted. The
declaration remains the runtime authority (ADR-013: SQLMesh deploys a
`kind VIEW` model as a `SELECT *` pointer, so runtime introspection of the
deployed view sees the pointer, not the logic) — but it becomes a *derived,
checked* artifact.

**The deriver is the existing classifier, not a new one.**
`resolve_output_classes(tree, snapshot)` already maps a SELECT's output columns
to `DataClass`, handling counting aggregates, literals, and UNION max-tier
combination. `SchemaSnapshot` is a plain dataclass; `get_current_schema_snapshot(db)`
is one way to build one, not the only way. So:

```
resolve_output_classes(parse(model.query), snapshot_built_from_model_sources)
```

This is a coherence requirement, not merely a convenience: the classifier that
masks a user's ad-hoc SQL at runtime is the same one that derives a report's
classes at build time. Do **not** introduce a second classification path.

Feasibility is proven, not assumed — a spike resolved all 8 shipped models with
7/7 expectations passing, including `uncategorized_queue.account_id →
core.fct_transactions.account_id` (the exact column #330 leaked).

Mechanism, with the constraints the spike established:

- **Parse connectionless.** `sqlmesh.core.model.load_sql_based_model` parses the
  `MODEL(...)` DDL with no `Context`, no state connection, and no encrypted
  database; `model.query` yields the bare SELECT. No string surgery on the
  `MODEL(...)` block.
- **Build the snapshot from the same on-disk model sources**, never by hand. A
  *stale* schema entry fails loudly (`OptimizeError: Unknown column`), but a
  *missing* table degrades silently to no dependencies. Hand-maintenance
  reintroduces exactly the drift class this spec exists to remove.
- **Python models need a second path.** `core.fct_balances_daily` is a Python
  model; its columns come from the `columns={...}` declaration via `ast`, not
  from SQL parsing.
- **Reject `SELECT *` in a final projection.** The snapshot builder reads
  upstream column lists from each model's final SELECT. `core.dim_accounts`
  uses `SELECT *` inside CTEs and only works because its final SELECT is
  explicit; a future model ending in `SELECT *` would silently yield a
  one-entry schema. Fail loudly instead.
- **Assert the graph is acyclic.** Deriving `reports.*` classes from
  `CLASSIFICATION` is only well-founded because every `reports.*` model reads
  exclusively from `core.*`/`app.*`. A report reading another report makes the
  map self-referential. CI must assert no `reports.*` model depends on
  `reports.*`.

**Failure to derive is a CI failure, not a silent fallback.** If a model's
columns cannot be resolved, the check fails; it does not quietly emit
`AGGREGATE`.

**Shipped as:** the upstream snapshot is built from `CLASSIFICATION`
(`_upstream_snapshot()` in `report_class_derivation.py`), not by walking
on-disk model sources as prescribed above. This is sound, not a shortcut:
`CLASSIFICATION` completeness against the live DuckDB catalog is already
CI-enforced, so for every `core`/`app` column it already *is* the catalog —
and it is necessary, not merely convenient, because `app.balance_assertions`
(read by `balance_drift`) is a migration-created table with no SQLMesh model
at all, so there is no on-disk model source to walk for it. Python models get
no separate `ast`-based path either: `_load_model()` raises
`ReportDerivationError` on any non-`SqlModel`, the same "out of scope for a
connectionless, source-parsing deriver" treatment `derive_core_view_classes`
gives every Python model under `core/*.py`. The requirement this section
exists to satisfy — one classifier, derived not hand-asserted, CI-verified,
with no silent fallback — is unchanged; only the prescribed mechanism for
building the upstream snapshot and handling non-SQL models was revised during
implementation.

### R3 — Provenance sets a floor an author may lower with a reason

Derivation is right for pass-through columns and systematically
*over*-classifies computed ones. Over-masking a BI surface is its own failure
mode, so an author may downgrade a column below its derived floor **with an
explicit inline reason**. CI fails unless every declared column is either
derivation-matched **or** carries an explicit downgrade.

Scope note: the existing classifier already handles the cases that would
otherwise need downgrades most often. `COUNT(*)`/`COUNT(DISTINCT x)` resolve to
`AGGREGATE` through the counting-aggregate rule rather than inheriting the
counted column's class, and literals resolve to `AGGREGATE`. Of ~80 columns
across the 8 shipped models, only 5 resolve to no upstream at all (four
`COUNT(*)`, one `NULL::TEXT`). Expect downgrades to be rare; if the
implementation finds them common, that is a signal the deriver is wrong, not
that the escape hatch needs widening.

### R4 — Delete the transitional bridge

`reports/definitions/_bridged_classes.py` is deleted. Derivation **subsumes**
it — it is not replaced by a different hand-authored declaration mechanism.
The `RuntimeError` duplicate-guard in `reports_class_map()` goes with it.

Runner-backed reports keep their `classes=` map as the runtime authority, now
CI-verified against derivation. Views without a runner (`net_worth` today) get
their classes from a **generated, checked-in** artifact that CI verifies is
current — mechanically produced, so it cannot drift the way the bridge could.

### R5 — `reports.*` contains only user-facing reports

Per D3, membership in `reports.*` *is* the definition of "is a report."
`uncategorized_queue` is service-internal — its only runtime reader is
`services/categorization/queries.py`, backing `transactions_categorize_pending`
— so it moved out of `reports.*` into `core.uncategorized_queue`.

`core` rather than `prep`, because `prep` is not in `_ALLOWED_QUERY_SCHEMAS`;
moving there would silently remove a view users can query today. `core` keeps
it queryable and shifts its coverage to the `CLASSIFICATION` registry.
`account_id` is declared `RECORD_ID` there, matching all 15 other `account_id`
columns in `CLASSIFICATION` (spec D6): it is a deliberately opaque minted
surrogate, not PII, so it is correct for it to pass through unmasked — the
bridge's `ACCOUNT_IDENTIFIER` declaration for this column was a mistaken
premise, not the thing #330 actually needed fixed. What #330's coverage gap
genuinely broke was never having a declaration to compare against AT ALL,
which R2/R3's derivation-and-verification now forecloses structurally.

Touch points the move updated (mechanical but wide):

- `src/moneybin/tables.py` — the `TableRef` constant's schema, and therefore its
  `full_name`. `tests/moneybin/test_tables.py` pins `EXPECTED_INTERFACE` by
  `full_name`.
- `services/categorization/queries.py` — the read, plus a `UserError` payload
  that embeds `full_name`, so observable error content changes.
- ~20 hardcoded `reports.uncategorized_queue` strings across test fixture DDL,
  scenario data, and docs were updated to `core.uncategorized_queue` —
  including a stale `docs/guides/data-pipeline.md` reference to a CLI command
  and MCP tool that no longer existed, which is now corrected.
- `services/schema_catalog.py` — a hardcoded example query.

### R6 — A report-authoring rule

`.claude/rules/reports.md`, path-scoped to the reports and SQLMesh reports
directories, stating what a complete report requires: the SQLMesh model, the
`@report` runner, the declared class map and how it is verified, the
completeness guard, and the `reports.*`-means-user-facing boundary.

This is documentation of a contract that R1–R4 already enforce mechanically.
It is not the safety net — that is the point. The umbrella's D6 (materialization
mechanically writes the class map) is what makes the rule unskippable, and lands
in M2P.3.

## Out of scope

- **`net_worth`'s tool status.** Derivation covers its classes like any other
  view, so it is no longer a privacy question. Whether its bespoke
  `NetworthService`-backed tools should become generated ones is a
  tool-ergonomics question, decided in M2P.3 alongside the
  `extension-contracts.md` M3I addressing reconciliation. The umbrella's open
  question is reworded accordingly: *is a bespoke-tool report a permanent
  sanctioned category, or a migration state?* — which matters because M2P.2 and
  M2P.3 create reports at runtime, and those cannot have hand-written tools.
- **Wiring `discover_reports()`** for package-contributed reports (M2M). When it
  is wired, it must feed the class map; R1's fail-closed behaviour is what makes
  that safe to get wrong once.
- **Build-time *writing* of class maps** (umbrella D6) — M2P.3. This spec
  derives and verifies; it does not generate runner source.
- **Dynamic reports** and `app.user_reports` — M2P.2.

## Testing

- **R1:** a resolved-but-undeclared column masks; an unresolvable *expression*
  still classifies by input columns and does not over-mask. Both directions are
  required — a test that only checks the masking direction would pass for a
  blanket fail-closed implementation, which R1 explicitly rejects.
- **R2:** every deployed `reports.*` view's declared classes match derivation.
  The existing completeness scenario already enumerates deployed views from the
  DuckDB catalog rather than the declared registry — preserve that property; it
  is what makes the guard capable of catching an *undeclared* view.
- **R3:** a deliberately downgraded column passes CI; an undeclared mismatch
  fails.
- **R5:** `core.uncategorized_queue.account_id` passes through unmasked via
  `sql_query`, same as every other `account_id` column (`RECORD_ID`, spec D6)
  — it does NOT mask; the categorization surface still works end-to-end.
- Scenarios are not in the default gate — this changes data shape and schema
  membership, so `make test-scenarios` is required alongside `make check test`.

## Open questions (resolved)

- **Where does the generated artifact for runner-less views live?** Shipped as
  `src/moneybin/reports/definitions/_derived_classes.py`, mirroring the bridge
  it replaces, regenerated via `make generate-report-classes`. If M2P.3
  resolves `net_worth` toward a runner, the file empties out and should be
  deleted rather than kept as a permanent seam.
- **Does the derived floor belong in the runner or beside it?** Shipped
  inline: `class_downgrades={...}` is a keyword on the same `@report`
  decorator as `classes={...}` (`reports/_framework/contract.py`) — one place
  to look, per the original lean.
