---
description: "Agent-experience reporting rule for MoneyBin MCP testing"
---

# Agent Experience Reports for MoneyBin's MCP

Whenever you **invoke MCP tools** against MoneyBin's running MCP server in
a session — **for any reason** — you MUST produce a short
**agent-experience report** at the end. That includes:

- Smoke checks, new-tool validation, comparing surfaces by actually calling
  tools.
- Production-style use (answering a real financial question with MoneyBin).
- Read-only lookups during unrelated implementation work.
- One-off "just checking" calls.

If you invoked MCP tools as an agent consumer, write the report.

### What does NOT trigger a report

The trigger is **first-person tool invocation as an agent consumer**. The
following do **not** count, even though they touch MCP-related code or
state:

- **Running the project test suite** (`pytest`, `make test`,
  `tests/moneybin/test_mcp/...`) — the harness drives the calls, not you.
  Test failures are captured by pytest output, not AX prose.
- **Editing MCP code or its tests** without manually invoking the tools
  afterward — code review, refactors, type fixes, dependency bumps.
- **Reading MCP code, specs, or fixtures** to answer a question.
- **Asking another agent / subagent to do MCP work** on your behalf —
  if the work warrants a report, that agent owes it (or its summary
  surfaces friction); you don't double-bill.

The signal we care about is *what it felt like to use the surface as an
agent.* Mechanical test execution and code editing don't produce that
signal.

The report does not need to be long. A few honest bullets per section beat
a polished essay. Quote the exact tool name, parameter, or returned string
when calling something out — vague "the spending endpoint was confusing"
reports aren't actionable.

## Structure and workflow

Follow the six-section structure and the session-internal reporting workflow in
[`.claude/references/agent-experience-report-template.md`](../references/agent-experience-report-template.md).
Read it before writing the report — do not improvise the sections.
