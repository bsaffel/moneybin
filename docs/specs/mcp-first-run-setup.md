# Feature: MCP First-Run Setup

## Status

implemented

## Address

M3N (Productization & Distribution — onboarding; pulled forward for the
near-term MCP-app distribution surface).

## Goal

When a user connects an MCP client (Claude Desktop, etc.) to a freshly
installed MoneyBin that has **no profile configured**, the server must come up
cleanly and guide the user through one-question setup — never corrupt the
JSON-RPC stream, never require a manual terminal step before the surface works.

On clients that support elicitation, the agent asks the user for a profile
name, MoneyBin creates the profile, and the original tool call proceeds — no
restart. On clients that don't, the server returns one clear, structured
"run this command, then reconnect" message instead of failing.

## Background

`moneybin mcp serve` with no `--profile`, no `MONEYBIN_PROFILE`, and no
`active_profile` in `config.yaml` triggers the lazy profile resolver, which
runs the **interactive first-run wizard**. The wizard writes a welcome banner
and prompts (`👋 Welcome…`, `First name:`, `⚠️  Setup…`) to **stdout** via
`typer.echo` / `input()`. Under stdio transport, stdout is the JSON-RPC
channel, so every banner line becomes a separate parse error in the host. The
observed failure was ~8 consecutive "is not valid JSON" / "Unexpected end of
JSON input" errors in Claude Desktop, with the server then `Aborted.` on the
blocking `input()` call.

The narrow bug is "the interactive wizard runs on the MCP path." The durable
fix is "MCP first-run is a first-class, agent-driven flow," which this spec
delivers.

Requiring config to pre-exist before the server starts is the ecosystem norm
for local MCP servers. MoneyBin's UX/AX bias points the other way: the agent
is already in session, so the session itself should finish setup — an
elicitation-driven first run rather than an out-of-band config step.

Related code: `src/moneybin/cli/commands/mcp.py` (`serve`),
`src/moneybin/cli/utils.py` (`resolve_profile`, `_flags`),
`src/moneybin/mcp/middleware.py` (existing `on_call_tool` middleware),
`src/moneybin/services/profile_service.py` (`ProfileService.create`),
`src/moneybin/utils/user_config.py` (`load_user_config`,
`set_default_profile`).

## Key enabling facts (verified against installed deps)

- **FastMCP 3.3.1 / mcp 1.27.1.** `ctx.elicit(message, response_type=...)`
  is available (FastMCP since 2.10.0). Claude Desktop renders the dialog with
  no client-side config.
- **Clean capability detection.** `session.check_client_capability(
  ClientCapabilities(elicitation=ElicitationCapability()))` returns `bool`
  (`mcp/server/session.py:120`). The fallback is deterministic — no
  try/except guessing.
- **Elicitation needs an active request context.** It cannot run at
  `mcp.run()` startup, only during a tool call. Setup therefore triggers on
  the **first tool call**, not at boot.
- **Tool registration is DB-independent.** `init_db()` is just
  `register_core_tools()`; it does not open the database. `get_database()`
  opens lazily per call. So the server can fully boot and register tools with
  no profile.
- **Middleware can drive elicitation.** `MiddlewareContext.fastmcp_context`
  exposes the `Context` (`fastmcp/server/middleware/middleware.py:54`), and
  MoneyBin already uses an `on_call_tool` middleware
  (`ValidationErrorMiddleware`). First-run setup is a coherent sibling.

## Requirements

1. `moneybin mcp serve` **always boots**, with or without a configured
   profile. The interactive wizard is never reachable from the MCP path.
2. When no profile is configured, tools are still registered so the surface is
   visible and the first call can drive setup.
3. On the first tool call with no profile, on an **elicitation-capable**
   client: elicit a profile name, create the profile + encrypted DB, set it
   active in `config.yaml` and in-process, then proceed with the original
   call. No restart.
4. On a client that does **not** support elicitation (or when the user
   declines/cancels): return the standard MoneyBin response envelope with
   `error.hint` telling the user to run `moneybin profile create <name>` and
   reconnect. Do not execute the original call.
5. No secret material crosses the LLM context: only the profile **name** is
   elicited. The encryption key is generated server-side into the keychain by
   `ProfileService.create` and never appears in any tool argument or response.
6. No new `profile_*` MCP tool is registered. First-run is startup/middleware
   behavior, so the `profile_*`-is-CLI-only rule (`.claude/rules/mcp.md`)
   is honored, not bent.
7. After setup, subsequent tool calls pass straight through with no
   re-check cost beyond a cached boolean.
8. The original stdout-corruption bug is regression-locked: a no-profile
   server driven over real stdio by a non-eliciting client produces **zero**
   stdout JSON-parse errors.

## Design

### Component 1 — `serve()` boots unconfigured

Detect the unconfigured state before any eager profile/DB call:
`_flags.profile is None and not os.environ.get("MONEYBIN_PROFILE") and
load_user_config().active_profile is None`.

- **Configured** → unchanged: resolve profile, `setup_observability`,
  `check_schema_at_boot()`, `mcp.run()`.
- **Unconfigured** → **clear the process-wide profile resolver**
  (`register_profile_resolver(None)`), `register_core_tools()`, set up
  observability without a profile (default/no-profile log target), skip
  `get_database_path()` / `get_current_profile()` / `check_schema_at_boot()`,
  register `FirstRunSetupMiddleware`, and `mcp.run()`. The server waits for
  the first tool call.

  Clearing the resolver is what makes requirement 1 hold for **every** MCP
  entry point, not just tool calls. The resolver (registered in
  `main_callback`) is the single chokepoint that runs the interactive wizard;
  `FirstRunSetupMiddleware` only guards `on_call_tool`, so a **resource read**
  (e.g. `moneybin://schema`) or prompt fetch would otherwise reach
  `get_database()` → `get_settings()` → the resolver → wizard → stdout
  corruption. With the resolver cleared, `get_settings()` raises a clean
  `RuntimeError` (surfaced by FastMCP as an MCP error, no stdout write); the
  middleware does the real elicitation-based setup, calling
  `set_current_profile()` directly, so the happy path never needs the
  resolver.

### Component 2 — `FirstRunSetupMiddleware.on_call_tool`

Lives in `src/moneybin/mcp/middleware.py` alongside `ValidationErrorMiddleware`.

```
on_call_tool(context, call_next):
    if _profile_configured:            # cached bool, set True after bootstrap
        return await call_next(context)
    ctx = context.fastmcp_context
    if client supports elicitation:
        result = await ctx.elicit("What should we name your MoneyBin "
                                  "profile?", response_type=str)
        if result.action != "accept":
            return _setup_needed_envelope()
        name = result.data
        _bootstrap_profile(name)       # may re-elicit once on invalid name
        _profile_configured = True
        return await call_next(context)  # original call proceeds
    return _setup_needed_envelope()
```

Capability check: `ctx.session.check_client_capability(ClientCapabilities(
elicitation=ElicitationCapability()))`.

### Component 3 — `_bootstrap_profile(name)`

The single mutation point:

1. `ProfileService().create(name)` — creates the profile dir, generates the
   encryption key into the keychain, initializes the encrypted DB + schema.
   (`create` already rolls back a partial dir on failure.)
2. `set_default_profile(name)` — writes `active_profile` to `config.yaml`
   (written only after `create` succeeds, so no dangling pointer).
3. `set_current_profile(name)` + re-init observability in-process, mirroring
   `resolve_profile()`, so the same process is fully configured without a
   restart.

### Error handling

- **Decline / cancel** → `_setup_needed_envelope()`; no partial profile.
- **`create` fails midway** → `ProfileService.create`'s own rollback removes
  the partial dir; `config.yaml` is untouched; surface a clean envelope.
- **Invalid name** (`normalize_profile_name` raises `ValueError`) → re-elicit
  once with a hint, then fall back to the envelope. Bounded loop.
- **Name collides with an existing profile** (`ProfileExistsError`) → adopt
  the existing profile (set it active) rather than erroring — the user named
  something real.
- **`--profile X` passed but X doesn't exist** → unchanged from today: clean
  `logger.error` to stderr + exit. Operator misconfiguration, not first-run;
  stderr is safe for MCP.
- **Concurrency** — stdio is single-client/serial per session; no locking
  needed at personal-finance scale.

### Fallback envelope shape

Standard MoneyBin error envelope (per `build_error_envelope`):
`{summary, data: null, actions: [...], error: {kind: "setup_required",
hint: "Run 'moneybin profile create <name>', then reconnect the MCP
server."}}`.

## Data Model

No schema changes. No new configuration fields. Reuses
`config.yaml`'s existing `active_profile`.

## Taxonomy & security reconciliation

`.claude/rules/mcp.md` classifies `profile_*` as CLI-only for two reasons,
both honored here: (1) **no secret material through the context** — only the
name is elicited; the key is generated server-side into the keychain; (2)
**no `profile_*` tool is registered** — this is startup/middleware behavior,
so the tool taxonomy and operator-territory rule are untouched. This spec
records the reconciliation so the design is not later misread as a violation.

## Implementation Plan

### Files to modify

- `src/moneybin/cli/commands/mcp.py` — `serve()` unconfigured-boot branch;
  remove the interim stderr-exit guard.
- `src/moneybin/mcp/middleware.py` — add `FirstRunSetupMiddleware` +
  `_bootstrap_profile` helper (or a small `mcp/first_run.py` if the
  middleware module grows too large).
- `src/moneybin/mcp/server.py` — register the middleware on the unconfigured
  path (or always, gated internally on configured state).

### Files to create

- `tests/moneybin/test_mcp/test_first_run_setup.py` — unit coverage.
- E2E additions in `tests/e2e/test_e2e_mcp.py`.

## Testing

- **Unit:** unconfigured `serve()` boots without firing the wizard;
  middleware bootstraps on elicit-accept; returns the envelope when
  capability is absent; decline / invalid-name / collision paths. Mock
  `ProfileService`, `ctx.elicit`, and the capability check.
- **E2E (the regression lock):** boot `moneybin mcp serve` with an empty
  `MONEYBIN_HOME` over real stdio, driven by the MCP SDK client.
  (a) Client **without** elicitation → first tool call returns the
  setup-required envelope, **zero stdout JSON-parse errors**.
  (b) Client **with** an elicitation handler supplying a name → profile is
  created, the call succeeds, a second call passes straight through.

## Not in scope

- No new MCP tool. No Web-UI first-run (that's M3A). No ADR — this applies
  the existing middleware + elicitation patterns rather than establishing a
  new inheritable one.
