# ADR-013: Report column classification is declared, not lineage-derived

**Status:** Accepted

## Context

Every `reports.*` surface (MCP tool + CLI command) returns rows whose columns
must be privacy-classified so `redact_records` masks CRITICAL fields (account
identifiers) before the data reaches an agent. For `core`/`app` columns this
classification is **declared** in the `CLASSIFICATION` registry
(`privacy/taxonomy.py`); the `sql_query` lineage engine then *propagates* those
declared base classes to the columns of an arbitrary, agent-written query.

The report framework initially tried to reuse that lineage engine to **derive**
each report's column classes automatically — run lineage on the report view's
SQL body, trace each output column back to a classified `core`/`app` column.
The appeal was zero hand-authored classification.

It does not work, in two independent ways:

1. **The deployed view is a pointer, not the logic.** SQLMesh's
   virtual/physical layer deploys each `kind VIEW` model as
   `CREATE VIEW reports.x AS SELECT * FROM moneybin.sqlmesh__reports.x__<hash>`
   — the real query is materialized into an internal physical table, and the
   public view is a thin `SELECT *` pointer at it. Lineage reads the deployed
   view SQL (`duckdb_views().sql`), so it classifies `SELECT * FROM <internal>`,
   not the logic. The internal schema isn't in `CLASSIFICATION`, so `expand_star`
   can't expand the `*`; lineage returns `{'*': aggregate}`. At runtime the real
   columns (`account_id`, …) aren't in that map, fall back to the max tier
   present (`aggregate`/LOW → passthrough), and **leak in the clear**. Confirmed
   against all eight real built views.

2. **Provenance ≠ sensitivity for derived columns.** Even given the logical
   query, lineage answers "where did this column come from," not "how sensitive
   is it." A `SUM(amount)` is `TXN_AMOUNT`, but a z-score derived from `amount`
   is a unitless statistic (`AGGREGATE`); a confidence score is `AGGREGATE`.
   Lineage would propagate the source tier and over-classify, requiring a human
   judgment for the derived half regardless.

Reports are also the *inverse* of the case lineage was built for: a **fixed,
first-party, curated** surface known at design time — not an arbitrary
agent-written query. For a fixed surface, deriving classification at runtime is
both unreliable (1) and unnecessary (you can declare it once).

## Decision

A report **declares** its output-column→`DataClass` map on the `@report`
decorator (`classes=`); `ReportSpec.classes` carries it, and
`classify_columns` masks output columns by it. An undeclared column **fails
closed** (masked). The declaration is the report's privacy contract, on the
same footing as the `CLASSIFICATION` registry that declares `core`/`app`
base truth.

**Declaration site:** the `@report` decorator (Python), because every report
already has a Python runner — the class map lives with it, is typed/greppable,
and an extension package ships its report and its classes together. (If
runner-less pure-SQL reports are ever supported, revisit declaring classes as
SQLMesh column-comment sigils.)

**Regression guard:** a scenario test builds the real SQLMesh views and asserts
every report's declared `classes` cover the deployed view's columns and that
`account_id` is `ACCOUNT_IDENTIFIER`. A trivial hand-written fixture view (as
the unit tests use) cannot catch a gap against the real multi-CTE views — that
gap was exactly how the lineage approach leaked.

This is a quality-spec requirement for reports (in-tree and extension): a
report that does not declare a complete column map is invalid.

## Consequences

- Report classification is deterministic and auditable; no dependence on
  SQLMesh internals, view-body shape, or lineage-engine coverage.
- Each report carries a small hand-authored map (the in-tree maps mirror the
  pre-migration typed payloads). Adding/renaming a view column requires
  updating the map — the completeness test enforces this.
- `classify.py` drops all lineage/cache machinery for reports.
- The `sql_query` surface keeps using lineage — that is its correct home (an
  arbitrary query that reads `core`/`app` directly, with a short, declared-base
  lineage chain).

## Future work (not this change)

Lineage can return as a **recommendation engine**, not the authority: at build
time, derive *proposed* report-column classes from the logical model query and
surface them for a human to confirm into the declared map. That keeps the
declared-map as the source of truth (resolving consequence #2 — a human
confirms the derived/aggregate judgments) while removing the tedium of the
passthrough columns. Tracked as a follow-up.

## Alternatives considered

- **Lineage on the deployed view (the original approach).** Rejected — leaks
  (consequence 1).
- **Resolve through the SQLMesh indirection** to the physical table. Rejected —
  the physical table is a materialized result with no lineage back to `core`;
  its columns are unclassifiable without the declaration we're adding anyway.
- **Central `CLASSIFICATION` registry for `reports.*` columns.** Viable, but a
  central dict is awkward for extension packages to contribute to, and it
  separates a report's classes from the report. The decorator colocates them.
