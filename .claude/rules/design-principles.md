---
description: "Durable path selection: heuristics for one-way-door decisions, public-contract triggers, coherence rule"
---

# Design Principles: Durable Path Selection

Companion to AGENTS.md's "Guiding Principle." Defines what "durable" means
concretely and when the protocol applies. Depth — evolving a locked contract,
milestone addressing, the worked example, and the ADR bar:
[`.claude/references/design-principles-depth.md`](../references/design-principles-depth.md).

## The primary lens: reversibility

AGENTS.md establishes the one-way / two-way door classifier. This file
fills in: which surfaces are which, what "durable" means for one-way
doors, what it does NOT mean, and how an outcome lands in the repo. When
in doubt, treat as one-way and invoke the protocol.

### The protocol (one-way doors)

Surface both paths, name each cost concretely, recommend the durable path
and state what it costs; wait for explicit override before taking the fast
path. With no user (subagent / autonomous), default to durable and document
the choice.

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

**Pre-launch posture (current):** iterate aggressively to find
the right shape. This is the cheapest moment to fix mistakes. Don't
prematurely freeze a surface before you know it's right.

**Post-launch posture:** lock hard. Treat changes as breaking. Require
an explicit migration or deprecation path; record the rationale in an
ADR if it meets the bar below.

**Launch trigger.** Lock at the earlier of: M3E hosted launch, or the
first tagged release adopted by any non-author user.

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

## Magic stays visible

Automation that acts without showing its work erodes the trust the
durable path is built on. **Every increment of "magic" — detection,
auto-matching, silent defaulting, agent self-accept — must be matched by
a visible, dismissible confirm**, targeted at the moment the inference
could be wrong. The bar is **better-targeted confirmation, not less of it
than the competition**: tools that bind explicitly (you pick the account,
you write the rule) never surprise the user, so a detection-first tool
earns that trust only by surfacing a confirm exactly where its inference
is uncertain — and staying silent only where it is genuinely certain.

Two rules fall out:

- **Calibrate visibility to certainty.** Silent action is allowed only on
  a strong, near-certain signal (exact id, remembered binding). A weak or
  ambiguous inference always surfaces — and is never eligible for agent
  self-accept, regardless of confidence score.
- **Weigh the cost of a wrong silent action.** The harder a wrong
  inference is to notice and undo (a silent account *merge*, a silent
  destructive write), the higher the bar for acting without a confirm.
  Cheap, self-evident mistakes (a column guess visible in the result) can
  lean more automatic.

A confirm/review surface is therefore part of a feature's durable design,
not a nicety bolted on later. Operational home for the confirm pattern
itself (the `_confirm` verb, propose→review→confirm): `surface-design.md`.
Origin: the account-identity work (`account-identity-resolution.md`),
where a column-mapping confirm that *existed* went unseen because the
agent path self-accepted it silently.

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

## Depth

Evolving a locked public contract, milestone addressing (`M{phase}{letter}.{n}`), the worked example, and the ADR bar:
[`.claude/references/design-principles-depth.md`](../references/design-principles-depth.md).
