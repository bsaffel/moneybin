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

## Diagrams

- **Mermaid over ASCII**: When generating `.md` files that include diagrams, use Mermaid code blocks (` ```mermaid `) instead of ASCII art.
