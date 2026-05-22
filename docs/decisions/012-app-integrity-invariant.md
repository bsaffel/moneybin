# ADR-012: `app.*` Integrity Invariant — Repository + Audit Routing

## Status
accepted

## Context

`app.*` holds the only non-reconstructible state in MoneyBin — user categories,
merchant patterns, categorization rules, account settings, balance assertions,
budgets, curation notes/tags/splits, match decisions, tabular-format profiles.
Everything in `raw`, `prep`, `core`, and `reports` can be rebuilt from source
files plus `app.*`; `app.*` cannot. A bad service mutation can silently corrupt
any of it.

Today audit routing is enforced by **convention, not structure**:
`audit_service.record_audit_event()` is called at only a fraction of the mutation
sites (verified bypass map in [`app-integrity-invariant.md`](../specs/app-integrity-invariant.md)
§Background). The doctor has no `app.*` invariants — only pipeline ones. Two
forces make this untenable:

1. **Hosted tier (M3D/M3E)** cannot ship without per-user `app.*` integrity.
2. **The agent-first thesis** means bulk LLM-driven mutations must be trustworthy
   and recoverable — an agent recategorizing 30 transactions needs an undo path,
   and undo needs complete, reliable pre-image capture at every mutation site.

This ADR records how MoneyBin makes `app.*` mutation routing structural, and the
forward-compatibility contract that lets a future undo consumer (Phase 2,
[`data-recovery-contract.md`](../specs/data-recovery-contract.md)) be strictly
additive.

### Options considered

| Approach | Audit guarantee | Cost profile |
|---|---|---|
| **Repository layer** (chosen) | Structural — each protected table's `*Repo` emits audit unconditionally in the same txn | +1 INSERT per mutation; preserves today's read performance |
| `@audited` decorator on service methods | Disciplinary — still per-method; breaks on multi-row / multi-table mutations; invisible at SQL-grep time | Low, but doesn't close the gap |
| Event sourcing | Structural, with full history | Write amplification, projection maintenance, operational complexity — wrong for a single-user local app |
| DB triggers | Structural | Not available — DuckDB has no triggers; no ORM either |

## Decision

### Repository pattern owns every protected `app.*` write

Each protected `app.*` table gets a tiny `*Repo` class in
`src/moneybin/repositories/` whose mutation methods (`upsert`, `delete`, …) emit
audit unconditionally in the same DuckDB transaction. Services **compose**
repositories instead of executing raw mutation SQL. Reads stay free — services
may continue to `SELECT` directly from `app.*`.

This makes audit structural rather than disciplinary: there is no mutation path
that doesn't go through a repository, and the repository cannot mutate without
auditing. A lint rule (per Req 8) rejects `INSERT`/`UPDATE`/`DELETE` against a
protected `app.*` table outside `*_repo.py` and `audit_service.py`; the doctor
verifies the runtime invariant (every recent mutation has an audit row).

**Why not a decorator:** a `@audited` decorator still requires per-method
discipline, breaks on multi-row or multi-table mutations, and is invisible when
grepping for raw SQL. The repository moves the guarantee from "every author
remembers" to "the structure makes it impossible to forget."

**Why not event sourcing:** right pattern, wrong cost profile for a single-user
local app. The repository preserves today's read performance (only +1 INSERT per
mutation) while achieving the same forensic property — every state change
recorded with a full pre-image.

### Full-row pre-image capture (not a diff)

Every repository mutation captures the **complete pre-mutation row** in
`before_value` (full row state, not a diff or changed-columns subset). For
INSERT, `before_value=None`; for UPDATE/DELETE, the full prior row read in the
same transaction immediately before the write. `after_value` is the resulting
row for INSERT/UPDATE and `None` for DELETE.

This **supersedes** the earlier column-subset optimization
(`transaction-curation.md` Req 29, now amended). The forward-compat cost is ~10%
extra discipline per repository method; the retrofit cost — once mutations have
written partial `before_value` rows — is a 3–4 week refactor and the bulk of
Phase 2. Capturing the full row now is the cheapest moment. It is non-negotiable
and explicitly not subject to "optimize to diffs" review feedback.

### Cascade threading

When one user action triggers multiple `app.*` mutations (e.g. deleting a
category cascades to recategorizing referencing transactions), the cascaded
mutations share a `parent_audit_id` pointing at the originating audit row.
`TransactionService`'s `tag.rename` / `tag.rename_row` chain is the reference
implementation.

### Phased delivery

- **Phase 1 (this ADR / spec):** the contract + plumbing — repositories, full
  pre-image capture, cascade threading, Invariant 10, the lint rule, and doctor
  invariants. No undo surface.
- **Phase 2 ([`data-recovery-contract.md`](../specs/data-recovery-contract.md)):**
  the undo *consumer* — `UndoService`, `system_audit_undo` / `_history` / `_get`,
  `operation_id` grouping. Deferred deliberately: the undo UX (granularity,
  confirmation thresholds, discoverability) is best designed against real
  agent-usage data, and Phase 1's full-row capture is exactly what makes Phase 2
  strictly additive.

### Invariant 10

Codified in [`architecture-shared-primitives.md`](../specs/architecture-shared-primitives.md)
§Architecture Invariants so reviewers reject violating changes and future specs
inherit the contract:

> **Invariant 10 — `app.*` mutation routing.** All mutations of `app.*` tables
> MUST emit a paired `app.audit_log` row via `audit_service.record_audit_event()`
> inside the same DuckDB transaction, except for: (a) `app.audit_log` itself,
> (b) `app.metrics` (observability data, not user state), (c) seed-loaded
> configuration tables written only at install/migration time (currently
> `app.seed_source_priority`), and (d) migration-system tables
> (`app.schema_migrations`, `app.versions`). Direct `INSERT`/`UPDATE`/`DELETE`
> against `app.*` from outside `audit_service.py` or `*_repo.py` modules is a
> contract violation. The doctor MUST verify routing via per-table invariants.

## Consequences

- Every protected `app.*` mutation is audited with a full pre-image — the data
  foundation for Phase 2 undo is in place without re-instrumenting later.
- Audit coverage becomes structural; the lint rule + doctor invariants catch
  regressions a future refactor might introduce.
- Read performance is unchanged; write cost is +1 INSERT per mutation
  (acceptable for personal-finance volumes).
- Services get slightly more verbose — they compose repositories instead of
  writing inline SQL — in exchange for a guarantee that can't be forgotten.
- The migration lands per-table across small reviewable PRs (one repo + one
  writer + one doctor invariant each); the lint rule lands last, after every
  protected table has coverage, so it doesn't block the migration PRs.
- Non-service writers (`extractors/tabular/formats.py`, `matching/persistence.py`)
  import their repo across packages. The cleaner refactor (loader/service split)
  is a tracked follow-up, kept distinct from audit-routing to avoid inflating
  Phase 1.
- `app.audit_log` grows with every mutation plus per-row cascade children; the
  schema is sized for >100K rows per `identifiers.md`.

## References

- [`app-integrity-invariant.md`](../specs/app-integrity-invariant.md) — the
  Phase 1 spec (bypass map, repository contract, lint rule, doctor invariants,
  PR-by-PR plan)
- [`data-recovery-contract.md`](../specs/data-recovery-contract.md) — Phase 2,
  the undo consumer (Invariant 11)
- [`architecture-shared-primitives.md`](../specs/architecture-shared-primitives.md) —
  Invariant 10; Service-Layer Contract
- [`transaction-curation.md`](../specs/transaction-curation.md) — introduced
  `AuditService` + `app.audit_log`; Req 29 amended here for full-row capture
- [ADR-001: Medallion Data Layers](001-medallion-data-layers.md) — establishes
  `raw`/`prep`/`core`/`app` that this invariant protects
- [ADR-010: Writer Coordination](010-writer-coordination.md) — single
  read-write process per profile, the transaction model repositories rely on
