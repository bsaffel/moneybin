---
description: "Documentation: diagram conventions when authoring Markdown"
paths: ["**/*.md"]
---

# Documentation

## Visibility

Public documentation explains what MoneyBin does, how to use or contribute to
it, and the durable technical contracts that govern it. Keep public:

- user, operator, contributor, API, CLI, MCP, and architecture documentation;
- ADRs and other durable decisions; and
- implemented specs or pre-implementation specs that intentionally invite
  external feedback or establish a contributor-facing contract.

Keep private by default: strategy, pricing, commercial or hosted-service
planning, competitor and market research, internal reviews, work queues,
implementation plans, agent scratch work, and uncommitted design exploration.
Promote a private design into public docs only when it becomes a durable public
contract or materially helps an external user or contributor make a decision.

Never link a public document to `private/`. Replace the reference with a public
issue, roadmap item, or an honest statement that the work is planned. Treat any
information already committed publicly as public history: moving it to
`private/` prevents future exposure but does not erase it from Git history.

## External product references

Public comparison, audience, compatibility, migration, and integration guides
may name an external product when that helps a user make a practical choice.
Do not make an external product, competitor, or market study the authority for
a MoneyBin technical decision. Specs and ADRs must state the actual rationale:
a user need, source or protocol constraint, observed MoneyBin failure,
invariant, security property, or validation result.

Keep competitor feature grids, market research, and notes of the form “they do
X, so MoneyBin should do Y” private. When a provider or dependency contract is
relevant, cite its primary documentation and state the required behavior
directly.

## Voice — ledger-grade prose

Public docs (README, `docs/`) speak in the product's voice, defined in
`design-system/readme.md` → Content fundamentals: exact, calm, auditable. The
two failure modes to avoid are equally bad — marketing cadence (superlatives,
"not just X but Y", emoji-templated structure) and compliance cadence
(hedge-stacking, mechanism descriptions that hide the claim). Testable rules:

- **Numbers first.** Every claim that can carry a checkable number does
  ("eight clients", "within $0.01") — never a bare adjective ("powerful",
  "seamless"). If the number would go stale fast, use a bounded one
  ("more than 100 tools").
- **No global hedges.** Never "pre-v1, expect roughness" disclaimers. A
  limitation is a scoped fact plus the next action ("Windows is untested";
  "consent gating is designed but not yet enforced"). One qualifier per
  paragraph, maximum.
- **Ban the negation-restatement tic.** "X is planned; it is not available
  today" — "planned" already says that. Grep before shipping:
  `is planned\..* not|planned; .*not|is not available today`.
- **Trust as negation.** State what the product does NOT do, flatly ("No
  telemetry. No vendor account."). Promises of absence are checkable.
- **Concede real categories by name** (audience.md is the pattern). Candor
  with specifics is the strongest trust signal; no template generates it.
- **Transcripts over prose.** A real command with real output persuades more
  than a paragraph. Never fabricate or edit output; trimming whole lines is
  fine.
- **Imperative mood** over chatty second person. Sentence-case headings. No
  exclamation points, no superlatives; emoji only as status marks in tables.
- **Would this sentence be false if a user checked it today?** If yes, cut
  it. If no, don't hedge it — the hedge costs the reader information and buys
  nothing.

## Diagrams

- **Mermaid over ASCII**: When generating `.md` files that include diagrams, use Mermaid code blocks (` ```mermaid `) instead of ASCII art.
