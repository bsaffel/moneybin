# Design Principles: Durable Path Selection

Companion to AGENTS.md's "Guiding Principle." Defines what "durable" means
concretely, when the protocol applies, and how to record the outcome.

## The primary lens: reversibility

AGENTS.md establishes the one-way / two-way door classifier. This file
fills in: which surfaces are which, what "durable" means for one-way
doors, what it does NOT mean, and how an outcome lands in the repo. When
in doubt, treat as one-way and invoke the protocol.

## Public contracts vs internal abstractions

The two categories require different defaults.

### Public contracts (one-way doors) — the trigger list

Observable from outside the repo. Breaking them breaks users or
downstream agents.

- Schemas in `core` and `app` — column names, types, semantics
- MCP tool names, parameter shapes, response envelopes
- CLI command names, subcommand structure, output formats
- On-disk formats — database files, exports, config files
- Encryption parameters and key-derivation choices
- Critical-path dependency choices that leak into public types

**Pre-launch posture (current — M2A/M2B):** iterate aggressively to find
the right shape. This is the cheapest moment to fix mistakes. Don't
prematurely freeze a surface before you know it's right.

**Post-launch posture:** lock hard. Treat changes as breaking. Require
an explicit migration or deprecation path; record the rationale in an
ADR if it meets the bar below.

**Launch trigger.** Lock at the earlier of: M3E hosted launch, or the
first tagged release adopted by any non-author user.

### Evolving a public contract post-launch

Locked contracts still need to change sometimes. By surface:

- **Schemas:** versioned column additions only; never rename or retype
  in place. Deprecate-then-remove for column removal across two
  releases.
- **MCP tools:** add the new tool alongside, mark the old one
  deprecated in its description, remove after one minor release.
- **CLI commands:** add the new command, keep the old one as an alias
  for one minor release with a deprecation warning, remove on the next.
- **On-disk formats:** include a format version field; readers handle
  N-1 and N, writers emit N.

Pattern-changing breaks need an ADR; routine breaks get a CHANGELOG
entry under `Changed` or `Deprecated`.

### Internal abstractions (two-way doors)

Only the team sees them. Refactoring is a mechanical change verified by
types and tests.

- Module boundaries, factory functions, helper classes
- Internal naming, file layout, import structure
- Implementation choices behind a stable public contract

Apply Simplicity First. Don't invoke the protocol. Refactor freely when
the right shape becomes obvious.

## What "durable" means for one-way doors

- **Public contracts outlive the code.** Pay the rename cost now, not
  later.
- **Security and privacy are non-negotiable.** Finance + AI = zero trust
  budget. Never trade a security property for a week of schedule.
- **Boring dependencies, fewer of them.** Each dep on the critical path
  is a multi-year bet. Preference order: stdlib → DuckDB / SQLMesh /
  Pydantic ecosystem → well-known PyPA-blessed libraries. Avoid pre-1.0,
  single-maintainer, or rapidly-churning packages on the critical path.
- **Abstractions remove ambiguity; they don't add flexibility.**
  `TableRef`, `SecretStore`, `Database`, `ResponseEnvelope` make wrong
  code hard to write — that compounds. Plugin systems and config-driven
  toggles add surface without removing ambiguity — they rot.

## Coherence: one way to do each thing

Coherence makes the foundation feel consistent from outside: every part
of the codebase looks designed by the same hand. Applies to every
change, not just one-way doors.

**Operational test: when you add new X, does it look like existing X?**

- A new MCP tool uses the same response envelope, sensitivity tier, and
  error shape as every other tool.
- A new CLI command follows the existing taxonomy and argument
  conventions before introducing new ones.
- A new schema table uses the same naming, sign conventions, and dedup
  pattern as existing tables.
- A new module lands in an existing layer (`extractors/`, `services/`,
  `mcp/`) instead of inventing a new location.

If the existing pattern is wrong, **fix it everywhere** — don't
introduce a second pattern beside it. Two patterns for the same job is
the single largest source of codebase rot. When the migration can't
land in one PR, the introducing PR must mark the old pattern with a
grep-able deprecation comment (`# DEPRECATED: pattern-name`) and link
the removal plan or tracking issue gated to a milestone.

This is NOT a license to gold-plate. "Elegant" and "architecturally
pure" are not goals — they are post-hoc descriptions of code that is
coherent and durable.

## What it does NOT mean

- Not gold-plating, perfectionism, or refusing to ship.
- Not rewriting in another language for elegance. The stack — Python,
  DuckDB, SQLMesh, Typer, MCP — is fixed; "inevitable" means inevitable
  *within that stack*.
- Not blocking on hypothetical future requirements.
- Not freezing the public surface before launch. Pre-launch is when
  iteration is cheap; use it.

## Example: applying the protocol

**Decision:** Adding a merchant attribute to `core.fct_transactions`.
`merchant TEXT` (raw string on the fact table) vs `merchant_id BIGINT`
(FK to a new `core.dim_merchants`)?

**Classification:** One-way door. `core.fct_transactions` is exposed via
MCP and CLI; consumers will write queries against this shape.

**Option A (durable):** `merchant_id` + `dim_merchants`. Cost: ~3 extra
days to build the dim table, dedup logic, and joins in `reports`. Locks
the right shape — merchants are a real entity with attributes
(normalized name, aliases, category) that will grow.

**Option B (fast):** `merchant TEXT`. Cost: ~half a day. Works for the
M2B demo. Breaks the moment a second merchant attribute is needed —
forcing a dim-table introduction later plus migration of every consumer
query already written.

**Recommend A.** Pays 3 days now to avoid a breaking migration of the
public schema later. **No new ADR needed** — this applies the existing
dim-table pattern from ADR-001. Capture in the merchants spec and PR
description; reference ADR-001 for the dim/fact rationale.

## Recording the outcome

Most one-way-door decisions do NOT need their own ADR. ADRs are for
decisions that *establish* a pattern others will inherit from, not
decisions that *apply* an existing pattern. The ADR bar is deliberately
high — sprawl devalues the format and trains contributors to skip them.

**Record an ADR only when all three are true:**

1. The decision **establishes or changes a pattern** others will inherit
   from (not just applies an existing one).
2. The **"why" isn't recoverable from reading the code** — only the
   "what" is.
3. A reasonable future contributor might **propose undoing it** without
   that context.

If any one fails: capture the decision in the PR description, the
relevant spec, or an inline comment. Don't create an ADR.

**ADR-worthy:**
- DuckDB as the embedded analytical store (ADR-000) — establishes the
  storage pattern every other choice inherits.
- Medallion data layers (ADR-001) — establishes raw/prep/core/app.
- Privacy tiers and sensitivity model (ADR-002) — establishes the
  classification every MCP tool inherits.
- Encryption key management (ADR-009) — establishes the crypto pattern.

**Not ADR-worthy:**
- Adding a `merchant_id` column or a new dim table — applies ADR-001.
- Renaming a CLI command or restructuring a subgroup — applies the CLI
  taxonomy in the relevant spec.
- Adding a new MCP tool — applies the MCP architecture spec.
- Bumping a dependency, even a critical one — no pattern change.
- Choosing a column type (`DECIMAL(18,2)` vs `DECIMAL(20,4)`) — applies
  the accounting-precision convention.

When in doubt: don't create the ADR. The principle's job is to make
durable choices, not to generate paperwork about them.
