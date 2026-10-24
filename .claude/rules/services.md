---
description: "Service layer: data access, error messages, entity resolution, destructive confirmation"
paths: ["src/moneybin/services/**", "src/moneybin/mcp/**"]
---

# Service Layer

Services hold business logic and parameterized SQL and return typed Python objects (dataclasses or Pydantic models); MCP tools and CLI commands are thin wrappers over them. Layering and the MCP surface: [`mcp.md`](mcp.md).

## Data Access

- Read from **core schema** via `TableRef` constants for analytics queries.
- Write to **app schema** for user-authored state (categories, budgets, consent, annotations).
- Write to **raw schema** for import operations only.
- Use parameterized SQL with `?` placeholders for all values.

## Error Messages

- **Minimize data in errors** — no account numbers, balances, or PII in error messages. Classification and critical-field masking are middleware concerns.

## Entity resolution

All coarse operations use one shared reference-resolution contract:

1. explicit stable ID;
2. exact alias or name;
3. unambiguous normalized match;
4. structured `not_found` or `ambiguous` result with candidate IDs.

Writes never select the first fuzzy match. Resolution belongs in the service
layer, not individual tool wrappers.

## Destructive confirmation

A destructive preview binds approval to canonical validated arguments, resolved
stable IDs, actor/profile and authorization context, operation kind, and a
blast-radius summary. Approval is short-lived and single-use. Recompute the
canonical representation immediately before commit and distinguish expired,
replayed, mismatched, and nothing-to-do results.

Use MCP elicitation when the client supports it. Request an explicit boolean
and confirm only an accepted `true`, then compare a freshly recomputed binding.
Do not use FastMCP's deprecated empty-schema elicitation. This elicited boolean
is not a bare `confirm` tool argument: degraded clients receive an opaque token
carrying the same binding, and no operation proceeds without exact binding
verification.

**Exception — operations that forgo the token entirely.** The fallback token is
returned to the *calling agent*, so it confirms nothing about a person for an
operation whose wrong outcome is both hard to notice and hard to undo. Such an
operation passes `elicitation_only=True` to `grant_confirmation_or_raise`: it
refuses a supplied `confirmation_token` (without consuming it) and refuses a
client that cannot prompt, naming the CLI equivalent instead of minting a key to
its own unconfirmed write. An accepted account link is the current member —
adding one is a design decision, not a local call-site choice.

Treat approval as an immutable digest grant; verify that grant
against live state inside the same write transaction, immediately before the
first mutation. Never verify and then open a separate mutation transaction.
Compatible same-intent writes may share one conservatively annotated tool;
classify validated arguments and confirm only the destructive branch. Do not
combine reads with writes or operations whose authorization, sensitivity,
audit, or recovery contracts differ.
