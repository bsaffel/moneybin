# Reports Foundation — One Contract, Coherent Surface

> Child spec of [`reports-overview.md`](reports-overview.md) (milestone **M2P.1**).
> Status: draft
> Type: Feature
> Last updated: 2026-07-18 — initial spec.
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
map covered 6 of 8 deployed views, and `reports.uncategorized_queue.account_id`
came back unmasked.

The missing declaration was the symptom. The mechanism is that **two fallbacks
disagree about what an undeclared column means**:

| Path | Undeclared column resolves to | Where |
|---|---|---|
| Report tool (`reports_*`) | `ACCOUNT_IDENTIFIER` → masked | `reports/_framework/classify.py` (`_FAIL_CLOSED`) |
| `sql_query` | `AGGREGATE` (LOW) → **clear** | `privacy/sql_lineage.py` (`_fallback_class`) |

The report framework already reasons about this correctly and calls its
fallback "defense in depth." `sql_query` does not. Until that asymmetry is
fixed, every future coverage gap is a silent leak rather than a visible
annoyance — and wiring `discover_reports()` for package reports (M2M) is
exactly the event that re-arms it.

## Requirements

### R1 — Fail closed on coverage gaps, not on unresolved expressions

`_fallback_class` must distinguish two cases that it currently conflates:

1. **An expression we could not resolve** — an alias we couldn't trace, a
   computed projection. Keep today's behaviour: take the max tier of the
   scope's resolvable input columns, floor `AGGREGATE`. This is normal and
   must stay permissive, or the BI surface over-masks constantly.
2. **A column that resolved to a real table in a schema owed complete
   declarations, which has no declaration.** This is a *coverage gap*: our
   registry is wrong, not the query. Fail closed — classify as
   `ACCOUNT_IDENTIFIER` (the `Tier.CRITICAL` class `redact_records` masks) and
   log a warning naming the `(schema, table, column)`.

The distinction is the requirement. A blanket fail-closed would break case 1
and is explicitly **not** what this asks for.

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
`reports.uncategorized_queue` is service-internal — its only runtime reader is
`services/categorization/queries.py`, backing `transactions_categorize_pending`.
It moves to `core`.

`core` rather than `prep`, because `prep` is not in `_ALLOWED_QUERY_SCHEMAS`;
moving there would silently remove a view users can query today. `core` keeps
it queryable and shifts its coverage to the `CLASSIFICATION` registry, where
`account_id` must be declared `ACCOUNT_IDENTIFIER` — verified by a test, since
this is the exact column that leaked.

Known touch points (the move is mechanical but wide):

- `src/moneybin/tables.py` — the `TableRef` constant's schema, and therefore its
  `full_name`. `tests/moneybin/test_tables.py` pins `EXPECTED_INTERFACE` by
  `full_name`.
- `services/categorization/queries.py` — the read, plus a `UserError` payload
  that embeds `full_name`, so observable error content changes.
- ~20 hardcoded `reports.uncategorized_queue` strings across test fixture DDL,
  scenario data, and docs — including a stale `docs/guides/data-pipeline.md`
  reference to a CLI command and MCP tool that no longer exist.
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
- **R5:** `core.uncategorized_queue.account_id` masks through `sql_query`; the
  categorization surface still works end-to-end.
- Scenarios are not in the default gate — this changes data shape and schema
  membership, so `make test-scenarios` is required alongside `make check test`.

## Open questions

- **Where does the generated artifact for runner-less views live?** A generated
  module under `reports/definitions/` mirrors the bridge it replaces, but if
  M2P.3 resolves `net_worth` toward a runner, the file empties out and should be
  deleted rather than kept as a permanent seam. Decide during implementation;
  prefer whichever leaves less behind.
- **Does the derived floor belong in the runner or beside it?** Inline in
  `classes=` keeps one place to look; a separate generated baseline makes the
  CI diff clearer. Lean inline — one place to look wins for authors.
