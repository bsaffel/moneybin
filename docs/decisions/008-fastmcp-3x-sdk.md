# ADR-008: Upgrade FastMCP to 3.x

## Status
proposed

## Context

MoneyBin's MCP server is already built on FastMCP — we import `FastMCP` from `mcp.server.fastmcp` (pinned via `mcp[cli]>=1.9.0`). What's not always obvious is that **the FastMCP bundled inside the official `mcp` Python SDK is a 2024 snapshot of FastMCP v1**, not an independent reimplementation. FastMCP 1.0 was incorporated into the SDK in 2024; the original project (now under `PrefectHQ/fastmcp`) kept developing and is currently at **3.1.x stable**. Per the project README, "some version of FastMCP powers 70% of MCP servers across all languages." Anthropic donated MCP itself to the Linux Foundation in December 2025; there is no stated commitment to keep the bundled `mcp.server.fastmcp` in feature-parity with the standalone codebase.

This is a version-upgrade decision, not an SDK swap. The same project has shipped two major versions of features since the snapshot — features we have been planning to hand-roll because we never upgraded.

Two MoneyBin needs are converging on this decision:

1. **Unified error handling.** The `chore/quality-cleanup` branch (2026-04-28) shipped a `UserError` shape, an error classifier, and CLI adapters — but only the infrastructure landed. MCP tools still mix per-call `except duckdb.CatalogException` / `except Exception: # noqa BLE001` blocks. The plan was to follow up with a hand-rolled `handle_mcp_errors` decorator. Uncaught exceptions today surface their `str(exc)` to MCP clients — DuckDB error text containing SQL fragments and file paths can leak.
2. **Architectural fit for sensitivity tiers and progressive disclosure.** [ADR-002](002-privacy-tiers.md) defines `low`/`medium`/`high` tiers enforced by privacy middleware. [ADR-003](003-mcp-primary-interface.md) makes MCP the primary interface, with progressive disclosure (~19 core tools at connect, extended namespaces loaded via a `moneybin.discover` meta-tool + `tools/list_changed`) implemented by a custom `NamespaceRegistry`. Both fight v1: middleware doesn't compose cleanly across tool calls, sensitivity tiers live in a parallel decorator system, and `NamespaceRegistry` is a hand-rolled approximation of primitives FastMCP added in 2.x and 3.x.

What the v3.x line ships in-box that we are missing:

- **`mask_error_details=True` constructor flag** — when set on the `FastMCP` server, unclassified exceptions are wrapped as `ToolError("Error calling tool 'X'")` with no stack trace before reaching the client. Real PII improvement over today. The flag is server-level only; per-tool overrides do not exist. Domain exceptions that should reach the client unmasked must be raised as `fastmcp.exceptions.ToolError("user-safe message")` or caught in our own decorator and converted to error envelopes.
- **No `@handle_tool_errors` decorator** — masking is constructor-only, so we still need our own catch-and-build-envelope logic for domain exceptions (`UserError`, `DatabaseKeyError`, etc.) that should return a `ResponseEnvelope` shape rather than a generic `ToolError`. We fold this into the existing `mcp_tool` decorator (always paired with the old `handle_mcp_errors` anyway) and delete `handle_mcp_errors`. The constructor flag complements the decorator: classified exceptions → envelope; unclassified → masked.
- **Composable middleware system** — `AuthMiddleware`, `ResponseLimitingMiddleware`, `PingMiddleware` ship in-box. Custom middleware survives across tool calls (a v1 limitation 3.x fixes).
- **`@tool(auth=require_scopes(...))`** — auth/authz consolidated into the decorator. Maps to MoneyBin's sensitivity tiers more cleanly than the current decorator + privacy-middleware split.
- **Tag-based visibility transforms + `await enable_components(ctx, tags={domain})`** — first-class primitive for progressive disclosure. Tools are tagged at registration (`mcp.tool(name=..., tags={"categorize"})`); a server-level `Visibility(False, tags={"categorize"})` transform hides the tagged set globally; per-session calls to `enable_components` re-enable for that session only, and the framework auto-emits `ToolListChangedNotification`. Direct replacement for `NamespaceRegistry`'s manual mutation pattern. (Note: `enable_components` takes `names: set[str]` / `tags: set[str]` — keyword-only, not varargs.)
- **Provider/transform architecture** — `FileSystemProvider`, `OpenAPIProvider`, `ProxyProvider`, `LocalProvider`, `FastMCPProvider`, `SkillsProvider` (the last exposes skill instruction files as MCP *resources*, not a dynamic tool registry — this ADR previously misidentified `SkillsProvider` as the progressive-disclosure primitive; the actual primitive is the visibility system).
- **Structured `outputSchema` / typed Pydantic returns** — formalizes the success path the way `ToolError` formalizes the error path. Aligns with our `ResponseEnvelope` shape.
- **Decorators return functions, not component objects** — decorated tools stay directly callable from tests and non-MCP contexts.

## Decision

**Upgrade the existing FastMCP dependency from the v1 snapshot bundled in `mcp[cli]` to the standalone `fastmcp` package at v3.x.** Replace `from mcp.server.fastmcp import FastMCP` with `from fastmcp import FastMCP`. Enable `mask_error_details=True` on the constructor for unclassified exceptions; fold envelope-on-classified-exception into the existing `mcp_tool` decorator and delete `handle_mcp_errors`.

Bundle the `NamespaceRegistry` migration into the same PR, replacing it with the **visibility system** (per-session component enabling). The architectural change — server-global namespace state → per-session visibility — is a deliberate behavior improvement: it matches MCP's session model and gives correct isolation for clients with different consent or disclosure state.

Do **not** bundle the privacy-middleware-to-transform migration. That's security-critical code (sensitivity gates, consent enforcement, audit logging) and earns its own focused review.

### Scope of the migration PR (`refactor/migrate-fastmcp-3x`)

1. **Relocate `ResponseEnvelope` out of `mcp/`.** Move `src/moneybin/mcp/envelope.py` to `src/moneybin/protocol/envelope.py` (or top-level `src/moneybin/envelope.py` — final location TBD in the plan). Update all imports. The envelope is the shared response contract across CLI (`--output json`), MCP, and any future HTTP/FastAPI surface — it should not live under any single transport. Doing this first means subsequent steps already use the new import path. Same rationale applies to any other cross-transport types currently colocated with MCP code (e.g., `UserError` if it's under `mcp/`).
2. **SDK swap.** Replace `mcp.server.fastmcp.FastMCP` import with `fastmcp.FastMCP`. Audit `src/moneybin/mcp/server.py` for lifespan and transport API differences.
3. **Error handling.** Set `mask_error_details=True` on the `FastMCP` constructor. Fold envelope-on-classified-exception into the `mcp_tool` decorator (catch `UserError`, `DatabaseKeyError`, `FileNotFoundError`; build `ResponseEnvelope`; let other exceptions propagate to fastmcp's masking). Sweep tools for redundant per-call `except` blocks; convert tool-specific catches to `raise UserError(...)`. Delete `src/moneybin/mcp/error_handler.py`. Validate the test suite under masking — audit any test asserting on raw exception strings reaching the client.
4. **Adapter extraction.** Move `to_envelope()` off the `AutoRuleService` dataclasses into MCP adapters (closes the dependency-direction violation flagged in PR #60). Co-locate with the SDK upgrade because 3.x's typed Pydantic returns may shape the adapter signature.
5. **NamespaceRegistry → tag-based visibility transforms.**
   - Tools declare progressive-disclosure intent via the `mcp_tool` decorator: `@mcp_tool(sensitivity="medium", domain="categorize")`. The registration layer translates `domain` into `mcp.tool(tags={"categorize"})`; a server-level `Visibility(False, tags={"categorize"})` transform hides the tagged set globally. There is no per-tool `enabled=False` decorator kwarg in fastmcp 3.x — tag-based transforms are the canonical pattern.
   - `moneybin.discover(domain)` becomes a thin wrapper around `await enable_components(ctx, tags={domain})`. No enumeration of tool names; the tag does the matching. The canonical extended-namespace list (`EXTENDED_DOMAINS`) lives in `server.py` as the single source of truth.
   - Per-session semantics replace today's server-global mutation. Different MCP clients connected to the same server have independent visibility — an `async with Client(mcp)` context maintains its own session state.
   - Hidden tools are uncallable via `tools/call` (not just filtered from `tools/list`) — verified experimentally against `fastmcp 3.2.4` during the spike. No backup gate needed.
6. **Test fixtures for per-session model.** New tests explicitly call `discover` for the domains they touch (matches production flow). Existing tests that assumed all tools available at connect get a migration shim that pre-discovers all domains; carry a TODO to convert them to explicit-discover.

### Deferred to follow-up ADRs

- **`NamespaceRegistry` improvements beyond visibility.** Once the visibility migration lands, evaluate whether the provider model (custom `MoneyBinNamespaceProvider`) buys anything beyond what visibility alone gives us. Probably no, but worth a follow-up review.
- **Privacy middleware → transform model.** Security-critical migration. Wants its own ADR, its own PR, its own test sweep.
- **`ProxyProvider` for `moneybin-server`.** Precondition not met — `moneybin-server` doesn't expose an MCP surface today. Revisit when it does.

## Alternatives Considered

### 1. Stay pinned to the bundled v1 snapshot, ship hand-rolled infrastructure

Build `handle_mcp_errors` as designed, extend the existing middleware shim, keep `NamespaceRegistry` as a custom registry.

**Rejected because:**

- The classifier and call sites we already shipped are SDK-agnostic, but the decorator and middleware are throwaway scaffolding. Doing the migration twice (hand-rolled now, replace later) is more work than doing it once.
- `mask_error_details` is a meaningful PII improvement we don't get on v1 — replicating it means writing and maintaining our own exception filter at every tool boundary.
- The bundled snapshot is essentially frozen. Anthropic ingested v1 and moved on; no signal it will track 2.x or 3.x. Every day on v1 is technical debt accumulating against features that already exist upstream.

### 2. Drop FastMCP, use the lower-level `mcp` server API directly

Skip both versions; build directly on `mcp.server.Server` with our own decorator layer.

**Rejected because:**

- We'd reimplement parameter validation, decorator ergonomics, structured returns, and middleware composition — exactly what FastMCP exists to provide.
- No ecosystem benefit. Every example, tutorial, and integration assumes FastMCP-style decorators.

### 3. Upgrade only as far as 2.x

The previous stable line, more conservative.

**Rejected because:**

- 2.x is in security-backport-only mode. New features (provider/transform model, `mask_error_details`, composable middleware, visibility system) are 3.x-only.
- A 2.x → 3.x migration would still be required later. Skipping the intermediate hop avoids two migrations.

### 4. Wait for the bundled SDK to catch up

Defer the decision; track upstream.

**Rejected because:**

- The bundled `mcp.server.fastmcp` is a frozen snapshot of FastMCP v1, not a living port of the project. There is no stated plan to track 2.x or 3.x features.
- Hand-rolled error handling and registry code accrues maintenance cost while we wait for an upstream that may never arrive.

### 5. Bundle the privacy-middleware-to-transform migration into this PR

Migrate sensitivity gates, consent enforcement, and audit logging onto the 3.x transform model in the same PR.

**Rejected because:**

- Security-critical code. If a transform-model migration drops a check or reorders enforcement, the failure mode is a privacy violation, not a broken tool.
- Multi-purpose PRs make security review harder. Whoever reviews this PR should be reviewing the SDK upgrade and the visibility migration — adding consent-system changes to the same diff fragments their attention.
- The transform-model migration is a behavior-preserving refactor of code that already works. It can wait without accruing the same hand-rolled scaffolding cost as the items in scope.

## Consequences

### Positive

- **PII safety at the SDK layer.** `mask_error_details=True` removes the per-tool discipline burden of sanitizing exception strings before they reach MCP clients.
- **One decorator, less scaffolding.** `mask_error_details=True` covers the unclassified-exception leak path; envelope-on-classified-exception folds into the existing `mcp_tool` decorator. `error_handler.py` is deleted, and tools no longer carry a paired `@handle_mcp_errors` decoration.
- **Cleaner architectural fit for sensitivity tiers.** `@tool(auth=require_scopes(...))` aligns with the privacy-tier model in [ADR-002](002-privacy-tiers.md) more cleanly than the current decorator + middleware split.
- **Progressive disclosure becomes a one-liner per tool.** `domain="categorize"` on the `mcp_tool` decorator (translated to `tags={"categorize"}` at registration) plus one `Visibility(False, tags={domain})` transform per extended namespace replaces the entire `NamespaceRegistry` class. `moneybin.discover` is reduced to a single `enable_components(ctx, tags={domain})` call — no enumeration, no parallel constant list.
- **Per-session visibility matches MCP semantics.** Different clients with different consent or disclosure state are tracked independently. The current global-mutation behavior is arguably a latent bug; this fixes it.
- **Decorated tools remain callable from tests.** 3.x's decorators return functions, not component objects — keeps non-MCP test paths simple.
- **Path forward for future migrations.** Provider model unlocks `ProxyProvider` for `moneybin-server`; transform model unlocks privacy-middleware migration. No commitment yet, but the option is open.
- **Cleaner FastAPI prep.** Relocating `ResponseEnvelope` out of `mcp/` makes the cross-transport contract explicit. A future local-webapp HTTP surface becomes a parallel adapter layer (HTTP → service-layer dataclasses → envelope), not a copy-paste of MCP code. `OpenAPIProvider` in 3.x is also available as a hedge — if FastAPI lands later, its OpenAPI spec can be projected back into MCP tools without per-tool re-registration.

### Negative / accepted tradeoffs

- **Migration churn.** v1 → v3 is two major versions of API change across `mcp/server.py`, the namespace registry, decorators, middleware, lifespan handling, and entry points. Bounded to the PR scope above.
- **Behavior change: per-session visibility.** Tests that assumed server-global tool availability need updating. Power users with multiple clients see correct isolation that they might find surprising at first. Mitigated by the migration-shim approach for legacy tests and a clear note in release docs.
- **Test-suite revalidation.** `mask_error_details` changes the strings clients see. Tests asserting on raw exception text need rewriting.
- **Standalone-package release cadence.** `fastmcp` releases more frequently than the bundled `mcp` SDK. Mitigated by pinning a compatible range in `pyproject.toml` and exercising the integration in CI.
- **The bundled `mcp` package stays a transitive dependency.** `mcp` is still pulled in via `fastmcp`'s own dependency graph for protocol primitives. We don't lose access to those types — we stop importing `mcp.server.fastmcp` directly.

## References

- [ADR-002: Privacy Tiers](002-privacy-tiers.md)
- [ADR-003: MCP Server as Primary Interface](003-mcp-primary-interface.md)
- [`docs/specs/mcp-architecture.md`](../specs/mcp-architecture.md) — current MCP server design
- [`private/followups.md`](../../private/followups.md) — original deferred-work entry capturing the option-A vs option-B decision logic from the `chore/quality-cleanup` branch (2026-04-28); this ADR supersedes the "Migrate from `mcp.server.fastmcp` to `fastmcp` 3.x" section
- [PrefectHQ/fastmcp](https://github.com/PrefectHQ/fastmcp) — the standalone, actively maintained continuation of the FastMCP project
- [What's New in FastMCP 3.0](https://jlowin.dev/blog/fastmcp-3-whats-new) — provider/transform architecture, visibility system, error masking
