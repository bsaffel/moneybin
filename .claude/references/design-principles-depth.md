# Design Principles — depth

On-demand companion to [`.claude/rules/design-principles.md`](../rules/design-principles.md),
which stays always-loaded. Read this when you are actually evolving a locked
public contract, addressing a milestone, checking what the principle does NOT
mean, wanting the worked example, or deciding whether a decision earns an ADR.

## Evolving a public contract post-launch

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

## Milestone addressing — one scheme, append don't reinvent

All roadmap work is tracked under a single phase-aligned address scheme.
**Use it; never fork a parallel numbering or sequencing scheme** — we have
migrated twice already (Level/Wave → flat M-grid → this), and each migration
was pure churn.

- **`M0`–`M3`** = the four milestones / build phases: **M0 Foundation ·
  M1 Ingestion Core · M2 Analysis & Reports · M3 Productization &
  Distribution**. The milestone *is* the test-functionality gate — testing
  batches at the phase, not per-item.
- **`M1J`** = an *increment*: a coherent capability ≈ one spec, closes on its
  own.
- **`M1J.2`** = a *work item*: a discrete design / PR / plan within an
  increment.

When planning, brainstorming, or writing a new spec or plan: attach the work
to an existing increment, or **append the next free letter (new increment) or
`.n` (new work item)** — then register it in the canonical reference,
[`docs/roadmap.md`](../../docs/roadmap.md) (public source of truth). Put durable
private rationale in `bsaffel/moneybin-private`, and coordinate or navigate it
from the MoneyBin Linear project instead of maintaining a second roadmap. Don't
mint a new top-level number for a sub-task, and don't add a per-increment gate.
If the scheme genuinely can't express the work, raise it — don't invent a
second one.

## What it does NOT mean

- Not gold-plating, perfectionism, or refusing to ship.
- Not rewriting in another language for elegance. The stack — Python,
  DuckDB, SQLMesh, Typer, MCP — is fixed; "inevitable" means inevitable
  *within that stack*.
- Not blocking on hypothetical future requirements.
- Not freezing the public surface before launch. Pre-launch is when
  iteration is cheap; use it.

## Example: applying the protocol

**Decision:** Add a merchant attribute to `core.fct_transactions` —
`merchant TEXT` on the fact table, or `merchant_id` FK to a new
`core.dim_merchants`?

**Classification:** One-way door. `core.fct_transactions` is exposed via
MCP and CLI; consumers will write queries against this shape.

**Option A (durable) — `merchant_id` + `dim_merchants`:** ~3 days. Locks
the right shape; merchants are a real entity with attributes (aliases,
category) that will grow.

**Option B (fast) — `merchant TEXT`:** ~½ day. Breaks the moment a
second merchant attribute is needed — forces dim-table introduction
later plus migration of every consumer query.

**Recommend A.** Applies the existing dim-table pattern from ADR-001;
no new ADR. Capture in the merchants spec and PR description.

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

**ADR-worthy** (establishes a pattern others inherit): embedded analytical
store (ADR-000), medallion data layers (ADR-001), privacy tiers (ADR-002),
encryption key management (ADR-009).

**Not ADR-worthy** (applies an existing pattern): a new `merchant_id` column
or dim table (ADR-001), renaming/restructuring a CLI command (CLI taxonomy
spec), a new MCP tool (MCP architecture spec), a dependency bump (no pattern
change), a column-type choice like `DECIMAL(18,2)` (accounting-precision
convention).

When in doubt: don't create the ADR. The principle's job is to make
durable choices, not to generate paperwork about them.
