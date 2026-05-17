---
description: "Agent-experience reporting rule for MoneyBin MCP testing"
---

# Agent Experience Reports for MoneyBin's MCP

Whenever you interact with MoneyBin's MCP server in a session — **for any
reason** — you MUST produce a short **agent-experience report** at the end.
That includes:

- Test probing, smoke checks, new-tool validation, comparing surfaces.
- Production-style use (answering a real financial question with MoneyBin).
- Read-only lookups during unrelated implementation work.
- One-off "just checking" calls.

If you touched the MCP server, write the report. The only exception is
fully automated test suites, which have their own contracts.

This rule exists because the MCP server is a **first-class agent surface**
(see `mcp-server.md`), and the only reliable way to keep it that way is to
treat every interaction as feedback. Agents like Claude, Codex CLI, and
Gemini CLI will be the dominant consumers; their friction — captured *as
it happens*, not reconstructed from memory — is the metric.

The report does not need to be long. A few honest bullets per section beat
a polished essay. Quote the exact tool name, parameter, or returned string
when calling something out — vague "the spending endpoint was confusing"
reports aren't actionable.

## Required report structure

```
## Agent experience report

**Blockers / sharp edges** — anything that forced multiple retries, returned
a cryptic error, or hid the right path forward.

**Data quality / consistency** — schema oddities, type drift, double-encoded
text, opaque IDs that turn out to be PII, fields that promise more than they
deliver.

**Defaults** — anything that returned far more or far less than was useful by
default; cases where the agent couldn't tell that results were truncated or
how to widen.

**Strengths (worth keeping)** — what worked well on first contact. This is
not filler: it documents the bar future changes must not regress past.

**What would have made this easier or more intuitive** — concrete suggestions
from the agent's perspective. Examples: a parameter that should default
differently, a tool that should accept an alias, a description string that
should mention the sign convention, an error message that should list valid
values, an `actions[]` hint that's missing, a workflow that needed three
tools when it should have needed one. Be specific — name the tool and the
suggested change.

**The single biggest fix** — one concrete next change that would most
improve the agent experience.
```

## Reporting workflow

AX reports are **session-internal** — they go to Brandon in the conversation,
not into public artifacts. Workflow:

1. At the end of any session that touched MoneyBin's MCP server, present the
   report directly in chat using the structure above.
2. Brandon triages each finding. Approved findings get filed as one-line
   entries in `private/followups.md`; the rest are dropped.
3. The PR shipping the underlying change describes the change only — **never
   paste the AX report (or a link to it) into the PR body, commit message,
   CHANGELOG, ADR, or any other checked-in artifact**.

The report is raw feedback for prioritizing future work, not a deliverable.
Brandon's filtering is what gives it signal value; surfacing every friction
note publicly would invert that.

This rule is paired with `mcp-server.md` (Architecture and Description
Requirements) and with the `using-superpowers` skill's emphasis on
intellectual honesty: report friction even when the change you shipped
caused it. Honesty is the standard for the in-chat report — public artifacts
remain scoped to what shipped.
