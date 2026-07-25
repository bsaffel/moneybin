# Agent-experience report — structure and workflow

On-demand companion to [`.claude/rules/agent-experience.md`](../rules/agent-experience.md), which carries the trigger and stays
always-loaded. Read this when you are actually writing a report.

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

AX reports are **session-internal** — they go to the developer in the conversation,
not into public artifacts. Workflow:

1. At the end of any session that touched MoneyBin's MCP server, present the
   report directly in chat using the structure above.
2. The developer triages each finding. Approved findings get filed as one-line
   entries in `private/followups.md`; the rest are dropped.
3. The PR shipping the underlying change describes the change only — **never
   paste the AX report (or a link to it) into the PR body, commit message,
   CHANGELOG, ADR, or any other checked-in artifact**.

The report is raw feedback for prioritizing future work, not a deliverable.
The developer's filtering is what gives it signal value.
