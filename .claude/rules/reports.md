---
description: "Report authoring: the @report contract, declared privacy classes, derivation/CI verification, the reports.* boundary"
paths: ["src/moneybin/reports/**", "src/moneybin/sqlmesh/models/reports/**"]
---

# Report Authoring

**A report is a `ReportSpec` in the one catalog.** That is the whole contract:
an id, a name, declared parameters, declared output columns, a `DataClass` per
column, financial semantics, and something that produces rows. Every consumer —
the `reports` MCP tool, the CLI, `export report`, `reports explain` — reads that
spec and nothing else. What varies between report *kinds* is only where the spec
comes from.

Four kinds ship today. Pick the row you are writing, then read its section.

| Kind | Where the spec comes from | Who declares the classes |
|---|---|---|
| **Materialized** SQL-backed | An `@report` runner in the repo | The author, verified against derivation in CI |
| **Runner-less view** | The generated `_derived_classes.py` | Derivation, checked in |
| **Service-backed** | A hand-written `ServiceReportSpec` | The author, against an independently reviewed map |
| **User-created** (dynamic) | A row in `app.user_reports`, via `spec_from_row` | Derivation, at save time — the user never declares one |

## Materialized reports need three parts, all required

"Materialized" means the report is a relation in the graph, refreshed by
SQLMesh, and therefore a thing the rest of the platform can schedule, export,
and depend on. That is what the three parts buy — a dynamic report has none of
them and still runs:

1. A SQLMesh model at `src/moneybin/sqlmesh/models/reports/<name>.sql`.
2. An `@report`-decorated runner in `src/moneybin/reports/definitions/<name>.py`
   (`ReportQuery`, `report` from `reports/_framework/contract.py`) that returns
   the parameterized SELECT. The framework introspects the runner's signature
   and docstring into a `ReportSpec` and generates the CLI command and `TableRef`
   wiring from that one definition — see
   `docs/specs/extension-contracts.md` §"Report contract".
3. A declared `classes={...}` map on `@report` naming every output column's
   `DataClass`, plus a `class_downgrades={...}` entry (with a real reason) for
   any column whose declared class sits below its derived floor.

A service-backed report instead declares one `ServiceReportSpec` with its
parameters, output columns, privacy classes, semantics, executor, and
validator. It has no SQL model to derive lineage from, so its complete expected
parameter and output class maps must also be added to
`test_service_report_privacy_maps_match_independent_contract`; see
"Service-backed reports" below.

A SQL view with no runner instead gets its classes from the generated module —
see "Runner-less views" below.

## User-created reports declare nothing, and that is the point

A user-created report is a row: name, SQL, declared parameters. Its privacy
contract is **derived from the SQL at save time** and stored
(`reports/_framework/derive.py`), and `spec_from_row` rebuilds a `ReportSpec`
from that row on every read. Nothing about authoring one belongs in this file —
there is no file to author. What a contributor touching that path must know:

- **Never let a user-supplied class into the map.** A parameter's class comes
  from the column it is compared against; an output column's from lineage. A
  declared class would be a user widening their own masking floor.
- **The stored map is authoritative at run time, and drift-checked.** A
  `class_fingerprint` over derivation's inputs decides whether the stored map is
  still current; a mismatch re-derives and fails closed on any column that moved
  upward (`docs/specs/reports-dynamic.md` R4).
- **`reports reclassify` is the only path that lowers a stored floor**, it
  requires a human confirmation, and it is audited. Do not add a second one.
- **Not every saved report can graduate** into the materialized form above:
  `report_materialization.py::materialization_blockers` answers why not, and
  `reports explain` reports it.

Full contract: [`reports-dynamic.md`](../../docs/specs/reports-dynamic.md).

## Classes are declared, then mechanically verified — never hand-waved

Everything in this section is about the two kinds where a human writes the class
map. The user tier inverts it — derivation *is* the author there, so there is no
declaration to verify against, and the equivalent guard is the drift fingerprint.

The declaration is the **runtime authority** (ADR-013:
[`013-report-classification-declared.md`](../../docs/decisions/013-report-classification-declared.md)
— SQLMesh deploys a `kind VIEW` report model as a `SELECT * FROM <internal
physical table>` pointer, so runtime introspection of the deployed view sees
only the pointer, never the logic). But it is not merely trusted: CI derives
the same answer independently from the model's **source**, where lineage is
complete, and fails the build if the two disagree.

- `derive_report_classes()` (`src/moneybin/privacy/report_class_derivation.py`)
  parses every `src/moneybin/sqlmesh/models/reports/*.sql` file with
  `sqlmesh.core.model.load_sql_based_model` (no `Context`, no database) and
  calls `resolve_output_classes(model.query, snapshot, strict=True)` —
  **the same classifier** that masks ad-hoc `sql_query` SQL at runtime. Do not
  write a second classification path for reports.
- `tests/privacy/test_report_class_derivation.py::test_declared_classes_match_derivation`
  compares declared vs. derived on **`(tier, mask strength)`** — not class
  identity, and not tier alone. Over-declaring across tiers (e.g. declaring a
  passthrough column `ACCOUNT_IDENTIFIER` when derivation would say
  `RECORD_ID`) always passes. A declaration that masks more weakly than
  derivation needs a `class_downgrades` reason; an unreasoned one fails CI —
  and so does a `class_downgrades` reason for a column that is no longer
  genuinely weaker than its derived floor (a stale entry must be deleted, not
  left in the tree). Staleness is checked in **both directions**: the
  comparison walks derivation's output, so a separate pass
  (`_orphaned_downgrades`) walks the declared downgrades and fails any entry
  naming a column the model no longer selects — otherwise a renamed or dropped
  column's reason would survive unvisited forever, and pre-authorize whatever
  a future column of the same name declared. Needs no database, so it runs in
  the default `make check test` gate, not `make test-scenarios`.
- **A `class_downgrades` reason cannot waive an *equal-tier* weakening.** The
  mechanism exists because derivation over-classifies *computed* columns — an
  author asserts "this z-score reveals no amount", a claim about information
  content. That argument is unavailable when both classes already agree on the
  tier and differ only in transform: waiving there would not correct an
  over-classification, it would elect to publish the last four characters of a
  value everyone agrees is CRITICAL. Declare the derived class instead. The
  legitimate case needs no waiver — `dim_accounts.last_four` genuinely *is* an
  institution account number, so derivation returns the partial-masking class
  too and there is no mismatch to excuse.
- **At CRITICAL, over-declaring is not automatically safe — the transform
  matters.** Below CRITICAL every transform is passthrough, so a higher tier is
  strictly more masking and tier alone decides. At CRITICAL it does not: all
  five classes share `Tier.CRITICAL` but `ROUTING_NUMBER`, `UNRESOLVED`, and
  `COMPOSITE_IDENTIFIER` mask **wholly** (`'021000021'` → `'*****'`) while
  `ACCOUNT_IDENTIFIER` and `INSTITUTION_ACCOUNT_NUMBER` mask **partially**
  (`'021000021'` → `'****0021'`). Runtime masking keys off the **declared**
  class, so declaring
  `ACCOUNT_IDENTIFIER` for a column that derives to `ROUTING_NUMBER` publishes
  the real routing number's last four digits. **Never replace a whole-masking
  class with a partial-masking one.** The guard measures each class's strength
  from `redaction.py`'s `_TRANSFORMS` (`redaction.mask_strength`) rather than
  from a hand-kept list, so a new `DataClass` cannot silently weaken it.
- `tests/scenarios/test_reports_classification.py::test_reports_declared_classes_cover_real_views`
  enumerates every **deployed** `reports.*` view from the live DuckDB catalog
  (not the declared registry) and fails if any real column is undeclared —
  this is what makes the guard capable of catching a view nobody declared at
  all, the exact shape of gap that let a coverage hole through undetected
  before this branch.

**Runner-less views (no `@report`).** Their classes live in the generated,
checked-in `src/moneybin/reports/definitions/_derived_classes.py` — never edit
it by hand. Regenerate after adding or changing a runner-less model with:

```bash
make generate-report-classes
```

then commit the resulting `_derived_classes.py`.
`tests/privacy/test_sql_query.py::test_generated_classes_are_current` fails CI
if the checked-in file has drifted from what derivation produces now.

## `reports.*` membership means user-facing — nothing else

Per the reports-overview umbrella's D3: **being in `reports.*` *is* the
definition of "is a report."** A view that exists only to back an internal
service (not a CLI/MCP report surface) does not belong in `reports.*` — put it
in `core` or `prep`. Precedent: `core.uncategorized_queue` (the categorization
curator queue) moved out of `reports.*` because its only reader is
`services/categorization/queries.py`, not the public `reports` catalog.

## A reports.* model reads only `core.*` / `app.*`

Derivation builds its upstream schema snapshot from the `CLASSIFICATION`
registry, which covers `core`/`app` only. A `reports.*` model that reads
another `reports.*` table would make the derived class map self-referential —
`report_materialization.py::assert_acyclic` rejects any such model outright
(`ReportDerivationError`), rather than silently deriving a wrong answer. The
same function answers a *saved* report's graduation eligibility, so a dynamic
report reading `reports.*` runs correctly and reports itself unmaterializable
for exactly this reason — one rule, two callers.

## No `SELECT *`, anywhere in the model

`report_materialization.py::assert_no_star` rejects a bare `*` or `t.*`
projection in **any** `SELECT` in the model — including inside a CTE, not just
the final projection. Nothing expands a star for a connectionless deriver, so
an unresolved star would silently degrade a column's class instead of failing
loudly. Name every column explicitly.

## `account_id` is `RECORD_ID`, not `ACCOUNT_IDENTIFIER`

`account_id` is a deliberately opaque, minted surrogate (`uuid4[:12]`) — not an
account number, not PII. Every `account_id` column in `CLASSIFICATION` is
`DataClass.RECORD_ID` (LOW tier); see
[`account-identity-resolution.md`](../../docs/specs/account-identity-resolution.md)
Decision 1 (opaque-by-construction) and Decision 6 (the reclassification from
`ACCOUNT_IDENTIFIER` to `RECORD_ID` this depends on). Declare a report's
`account_id` output column `RECORD_ID` to match. Declaring it
`ACCOUNT_IDENTIFIER` still passes CI (`RECORD_ID` is LOW, so declaring a
CRITICAL class for it over-declares across tiers, which the comparison above
allows) but masks a column that is safe to expose and is not the pattern to
copy into a new report.

This applies to `ServiceReportSpec` parameters as well as output columns:
exact `account_id` / `account_ids` parameters are `RECORD_ID`. Service-backed
reports have no SQL model for source-lineage derivation, so they require an
independently written expected column and parameter class map in
`tests/moneybin/test_reports/test_catalog.py::test_service_report_privacy_maps_match_independent_contract`.
That test enumerates the complete service-backed registry, so adding a report
without reviewing its privacy contract fails CI. The registry-wide
`test_registered_account_id_metadata_uses_opaque_record_id_class` separately
checks the opaque-ID naming invariant across both report kinds.

## Service-backed reports use an independent reviewed class map

`ServiceReportSpec.__post_init__` checks internal consistency, but its columns
and runtime class map can share the same mistaken declaration. Because there
is no SQL source for independent lineage derivation,
`test_service_report_privacy_maps_match_independent_contract` is the second
source of truth: it names every service report and every parameter/output
class explicitly. Additions or classification changes require updating that
reviewed map in the same change.

## This rule documents a contract CI already enforces

R1-R4 of `docs/specs/reports-foundation.md` ship the mechanical checks this
file describes: `report_class_derivation.py`'s asserts, the two scenario tests
above, and `build_spec`'s validation of `classes`/`class_downgrades`
(`src/moneybin/reports/_framework/introspect.py`) all run whether or not this
rule was read. **This file is the map, not the fence** — skipping it costs you
a slower first attempt at a new report, not a silent privacy hole.
