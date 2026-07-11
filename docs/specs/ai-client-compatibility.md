# Roadmap: AI Client Compatibility & Distribution

## Status

draft

## Address

M3B (packaging work items) · M3D (remote MCP + auth) · **M3O (new increment:
first-party directory listings)** · Phase-0 corrections carry no milestone
(routine doc/CLI fixes). M3O and the new M3B/M3D work items below get
registered in [`roadmap.md`](../roadmap.md) when this spec is promoted to
`ready`.

## Goal

MoneyBin's MCP server should reach users through the **vendor-blessed install
path of every AI client worth supporting** — as close to one-click as each
client allows — and should support every client that any major competitor
supports, more ergonomically. This spec fixes the support matrix (which
clients, at which tier, via which mechanism), corrects stale install guidance,
and sequences the build plan onto the milestone grid.

Findings are grounded in a July 2026 review of vendor primary docs
(code.claude.com, support.claude.com, claude.com/docs/connectors,
developers.openai.com, learn.chatgpt.com, cursor.com, code.visualstudio.com,
docs.windsurf.com, zed.dev, jetbrains.com, geminicli.com,
modelcontextprotocol.io, docs.docker.com), with decision-critical claims
adversarially verified. The ecosystem churns monthly; re-verify gating claims
before executing later phases.

## The landscape in one paragraph

Every serious agentic client speaks MCP as of July 2026, but they bifurcate
hard on transport. **Local stdio** is first-class in the developer/desktop
tools (Claude Code, Claude Desktop, Codex, Cursor, VS Code Copilot, Windsurf,
Gemini CLI, Zed, JetBrains, and the long tail). **Remote streamable-HTTP +
OAuth** is the only way into the consumer cloud surfaces (claude.ai web/mobile,
every ChatGPT surface, remote Cowork sessions) — and SSE is deprecated
ecosystem-wide (still accepted only in legacy corners). Discovery is
consolidating into first-party channels: `.mcpb` one-click bundles and plugin
marketplaces on the Anthropic side, the Apps SDK directory on the OpenAI side,
GitHub's registry for VS Code, and per-client marketplaces (Cursor deep links,
Windsurf, Cline). The official MCP Registry is a preview-stage metadata
backbone that aggregators consume — publish to it, but don't build on it.

## Support matrix

**Tier definitions.**
- **T1 — Supported**: install automation (`mcp install` or packaged artifact),
  documented, smoke-tested each release. Breakage is a bug.
- **T2 — Documented**: config snippet in the clients guide, `mcp install
  --print` template where trivial. Not release-gated; community-reported
  breakage triaged best-effort.
- **T3 — Remote-gated**: requires M3D (authenticated remote MCP). Supported
  posture-gated (see Egress posture) once the transport ships.
- **✗ — Intentionally unsupported**: with a named revisit trigger.

| Client | Transports (Jul 2026) | Blessed install path | Tier | Notes |
|---|---|---|---|---|
| Claude Code (CLI + IDE ext) | stdio, HTTP (SSE deprecated) | `claude mcp add`; **plugin via marketplace repo**; `.mcpb` also accepted | **T1** | Plugin bundles server + skills; self-hosted marketplace needs no Anthropic approval |
| Claude Desktop (Chat) | stdio (`.mcpb` blessed; config JSON legacy), remote connectors | **`.mcpb` Desktop Extension** (Settings → Extensions); config file still supported | **T1** | Python via `server.type="uv"`. Extension blocklists + MDM flags exist on managed orgs |
| Cowork (local session) | inherits Desktop local MCP | same as Desktop | T1 (inherited) | **Remote/cloud Cowork sessions cannot reach local MCP by design** — docs: "Local MCP servers don't run in remote sessions" |
| Codex (CLI/IDE/desktop) | **stdio + streamable HTTP** | `codex mcp add` / `~/.codex/config.toml` | **T1** | Set `startup_timeout_sec` > default 10s (uvx cold start 3–15s); per-server tool allow/deny useful for our surface |
| Cursor | stdio, SSE, streamable HTTP | `~/.cursor/mcp.json`; **`cursor://` install deep link** ("Add to Cursor" badge) | **T1** | Deep-link badge in README is near-zero cost |
| VS Code (Copilot agent) | stdio, http | `.vscode/mcp.json`; **`vscode:mcp/install` deep link**; GitHub MCP Registry one-click | **T1** | Org allowlist governance exists (registry-only policies) |
| Gemini CLI | stdio, SSE, streamable HTTP + OAuth | `~/.gemini/settings.json`; `gemini extensions install <repo>` | **T1** | Extension gallery is un-vetted/community by design |
| Windsurf | stdio, streamable HTTP, SSE (OAuth on all) | `mcp_config.json`; in-app marketplace | **T1 ⚠** | **100-active-tool cap vs our 102 registered** — verify connect-time visible count; keep headroom |
| Zed | stdio (`context_servers`); remote unconfirmed | `settings.json` or packaged Zed extension | T2 | Extension packaging is a later nicety, not required |
| JetBrains AI Assistant / Junie | stdio, streamable HTTP, SSE | IDE Settings → MCP; Junie `.junie/mcp/mcp.json` | T2 | |
| Cline | stdio | in-app MCP Marketplace (one-click) | T2 | Marketplace submission is free/community — cheap reach when wanted |
| Goose | stdio | `config.yaml` / in-app Add Extension | T2 | |
| Continue.dev | stdio | `config.json` / workspace YAML | T2 | Agent-mode only |
| LibreChat | stdio, sse, streamable-http | `librechat.yaml` / in-app panel | T2 | |
| Warp | stdio, SSE URL | UI add; auto-detects `.warp/.mcp.json` + reads Claude Code/Codex config | T2 | Often works with zero MoneyBin effort via config pickup |
| Open WebUI | streamable-http (native ≥0.6.31); stdio via `mcpo` | admin config | T2 | Localhost streamable-http intersects our `--insecure` gate — document carefully |
| claude.ai web + mobile (custom connectors) | remote MCP (OAuth optional platform-side) | Settings → Connectors (Free capped at 1) | **T3** | M3D. Available on all plans incl. Free |
| ChatGPT web/desktop/mobile (Developer Mode) | **remote-only** (HTTPS `/mcp`; SSE+streamable) | Developer Mode → add connector | **T3** | Plus/Pro/Business/Enterprise/Edu; Free excluded. Write-permission tiering ambiguous in vendor docs — re-verify at M3D |
| Cowork remote sessions | remote MCP via connectors | claude.ai connectors | **T3** | Same M3D unlock |
| Claude Connectors Directory / ChatGPT App Directory | hosted remote + review | vendor submission portals | **T3 (M3O)** | Both require org accounts + human review; see M3O |
| Gemini Code Assist | MCP in Private Preview | account-team request | **✗** | Revisit: GA announcement |
| Consumer Gemini app | no custom MCP (3 built-in Spark connectors) | — | **✗** | Revisit: Google opens Spark/MCP to third parties |
| Raycast | community extension-mediated | community `mcp-config.json` | **✗** | Revisit: first-party built-in MCP client ships |
| GitHub Copilot cloud coding agent | no OAuth remote MCP | — | **✗** | Revisit: OAuth support lands |
| SSE / WebSocket transports | deprecated / no-OAuth niche | — | **✗** | We ship `--transport sse` today: deprecate the flag at M3D, remove per the CLI deprecation policy |

## Blessed-path corrections (Phase 0 — immediate)

Stale guidance found during the review; all are routine fixes:

1. **`docs/guides/mcp-clients.md` Claude Desktop section** presents config-file
   JSON as the primary path. Vendor docs now bless `.mcpb` extensions and frame
   manual JSON as legacy (still supported). Rewrite the section; add the
   Cowork caveat (remote sessions never see local MCP; local sessions do) and
   the managed-org flags (`isLocalDevMcpEnabled`, `isDesktopExtensionEnabled`).
2. **`mcp install --client chatgpt-desktop` prints instructions for a
   local/stdio connector option that does not exist.** All ChatGPT surfaces
   are remote-only (HTTPS + public `/mcp`; OpenAI recommends tunnels for local
   dev). Replace the instructions with an honest "requires remote MCP —
   arriving at M3D" message (and keep the client id reserved).
3. **Snippet hardening** in `mcp install`: emit the absolute `uv` path (macOS
   GUI-launched clients drop shell PATH — documented recurring failure);
   include `startup_timeout_sec` for Codex (uvx cold start 3–15s vs 10s
   default); note Gemini CLI trust settings.
4. **File the upstream Claude Desktop bug**: connector toggle ON in Chat with
   tools never reaching the model (observed 2026-07-10) contradicts Anthropic's
   documented behavior — file with logs against claude-ai-mcp.
5. **Verify the connect-time visible tool count** against Windsurf's
   100-active-tool cap (102 registered; extended-namespace tools are hidden by
   default — confirm the visible number and record headroom policy).

## Packaging ladder (what we ship, in order)

| Rung | Artifact | Reaches | Address |
|---|---|---|---|
| 1 | **PyPI + `uvx moneybin`** as the canonical config command (repo-checkout `uv run` stays the dev path) | every stdio client | M3B (existing first-public-release item) |
| 2 | **`.mcpb` bundle** via `server.type="uv"` (manifest ≥0.2 with privacy policy, so it's directory-submittable later) | Claude Desktop one-click; Claude Code; MCP for Windows | M3B (existing item) |
| 3 | **Claude Code plugin + self-hosted marketplace** (`.claude-plugin/marketplace.json` in a MoneyBin repo; bundles the MCP server now, skills later) | all Claude Code users, `/plugin install` | **M3B.new** |
| 4 | **Official MCP Registry publish** (`server.json`, `pypi` registryType, namespace via GitHub OIDC or DNS) → aggregators (Smithery, Glama) pick it up; **install deep-link badges** (Cursor, VS Code) in README | discovery everywhere | **M3B.new** |
| 5 | **Docker image**; Docker MCP Catalog when demand justifies (Gateway is invite-only — catalog listing alone is fine) | self-host crowd; Docker Desktop users | **M3B.new** (later) |
| 6 | **Hosted remote MCP + OAuth** | claude.ai, ChatGPT, Cowork remote, mobile | **M3D** |
| 7 | **Directory listings** (Claude Connectors Directory; ChatGPT App Directory) | ordinary consumer users | **M3O** |

Existing strengths this plan builds on (do not regress): per-tool
`readOnlyHint`/`destructiveHint`/`idempotentHint`/`openWorldHint` annotations
already ship (`src/moneybin/mcp/_registration.py`) — a hard requirement for
both directories and the input to ChatGPT's read-vs-write confirmation UX;
portable `[A-Za-z0-9_-]` tool names; the response envelope; `mcp install`
config-writing, which remains a fully current pattern (Anthropic's
`claude mcp add` and FastMCP's `fastmcp install` do the same).

The discrete execution breakdown of this ladder for the Tier-1 clients (the
W1–W11 build inventory: what ships, in what order, with dependencies) lives in
`private/strategy/distribution-roadmap.md` §1.0 — kept out of this tracked spec
because it churns monthly and gets a full `writing-plans` decomposition only when
M3B executes.

## M3D — remote MCP + auth (design inputs sharpened)

The existing M3D row stands ("identity via Auth0/OIDC, MoneyBin-owned
authorization/consent"). This review sharpens the shape:

- **Spec floor:** OAuth 2.1 + PKCE, RFC 9728 protected-resource metadata,
  RFC 8414 AS metadata, RFC 8707 resource indicators. **DCR is a SHOULD** with
  ~4% real-world AS support; **CIMD** (spec 2025-11-25) is the practical
  alternative both Claude and ChatGPT prefer. Track the 2026-07-28 spec
  release's six auth-hardening SEPs.
- **Provider decision (one-way door).** Two viable paths: (a) **Auth0 via
  FastMCP `OAuthProxy`** — Auth0 lacks native DCR, the proxy bridges it; keeps
  one identity stack across MoneyBin surfaces (sync broker already validates
  Auth0-minted OIDC JWTs). (b) **WorkOS AuthKit via FastMCP
  `RemoteAuthProvider`** — the most MCP-spec-complete turnkey IdP
  (DCR + PKCE + resource indicators + PRM + CIMD) but a second identity
  system. **Recommendation: (a)** — identity-stack coherence is worth more
  than spec-surface completeness, and FastMCP's proxy is the documented
  pattern for exactly this. Cost: the proxy layer is one more moving part, and
  CIMD support should be verified against Auth0 + FastMCP before commit.
- **Client-validation work items:** claude.ai custom connector (all plans,
  Free capped at 1 — a real trial channel) and ChatGPT Developer Mode
  (re-verify the read-vs-write plan tiering, which vendor docs state
  inconsistently as of July 2026).
- **Consent gate is a prerequisite, not a follow-up** (see Egress posture).
- **Deprecate `--transport sse`** when authenticated HTTP ships (CLI
  deprecation policy: alias + warning one minor release).

## M3O — first-party directory listings (new increment)

The only channels that reach ordinary consumer users. Both are gated on M3D
plus organizational prerequisites — sequenced last deliberately.

- **Claude Connectors Directory:** submission requires a **Team/Enterprise
  claude.ai org** (owner-submitted), HTTPS remote server (streamable HTTP or
  SSE), OAuth 2.0 for authenticated services, per-tool title + annotations
  (✅ already shipped), privacy policy (instant rejection if missing), human
  review with self-test attestation. Local one-click distribution submits
  separately as an **MCPB** (manifest ≥0.2 with privacy policy) — available
  as soon as rung 2 ships, independent of M3D.
- **ChatGPT App Directory:** Apps SDK submission (Python/FastMCP is a blessed
  SDK), manual review, privacy policy + verified website + screenshots;
  regional exclusions (EEA/CH/UK at launch) may affect reach.
- Prerequisite to name in roadmap: acquiring/holding the submitting org
  accounts is a real cost, not a formality.

## Egress posture

Per the privacy framework: **cloud clients get remote MCP only behind
authentication plus the sensitivity consent gate** (the planned
medium/high-tier downgrade for cloud consumers in
[`privacy-data-protection.md`](privacy-data-protection.md) becomes an M3D
prerequisite, not a fast-follow). Local-first remains the headline; remote is
opt-in with visible consent. The unauthenticated transports stay behind
`--insecure` until removed. No tunnel-based workaround (ngrok/Cloudflare to a
live profile) is ever documented as a recommended path — dev-only, ephemeral,
demo data.

## Competitive bar

Wealthfolio sets the best-in-class install story (MCP embedded in the app the
user already runs — zero external server). Copilot Money ships the hosted
read-only pattern (waitlisted). The AI-native cluster (Finlynq, Syllogic,
Alderfi, Tuskledger) treats first-party MCP as core design but installs via
Docker+config. Nobody in the category has: one-click `.mcpb`, a plugin
marketplace presence, registry listings, *and* an authenticated hosted tier.
Rungs 1–4 alone put MoneyBin ahead of every incumbent's install ergonomics;
M3D+M3O match the only two hosted plays while keeping the local-first
posture they lack.

## Out of scope

- The first-run wizard / in-product onboarding (M3A, M3N — already specced).
- MCP Apps interactive UI (M3M — paused on upstream host rendering).
- Homebrew formula (already in M3B, unchanged by this review).
- REST API surface (future, `surface-design.md` §REST).

## Open questions

1. Windsurf cap: what is the connect-time *visible* tool count, and do we
   adopt a headroom policy (e.g., visible surface ≤ 80)?
2. ChatGPT write-access plan tiering (Business/Enterprise-only vs broader) —
   vendor docs conflict; re-verify at M3D execution.
3. Google Antigravity's MCP story — thin sourcing (aggregator-only); verify
   against Google primary docs before assigning a tier.
4. Which org account (and whose) submits to the two directories at M3O.
5. Does `.mcpb`'s `server.type="uv"` mode meet our cold-start budget on a
   clean machine (no uv preinstalled)? Prototype early in M3B.
6. **Claude Code plugin classification (rung 3) — founder call.** The distribution
   roadmap deliberately classifies Claude Code / Codex plugins as *contributor UX*
   (extension authoring, M3I), **not** user distribution, and lists them out of
   scope for M3B. This spec's rung 3 treats a Claude Code plugin + self-hosted
   marketplace as an MCP-*server*-distribution vector in M3B — a distinct use
   (`/plugin install` adds the server, more discoverable than `claude mcp add`, and
   the later skills-bundling channel). Reconcile before promotion: keep M3I's
   contributor plugin as-is; confirm whether the server-distribution plugin is
   worth building as an M3B rung. Recommendation: yes, it's cheap. See
   `private/strategy/distribution-roadmap.md` 2026-07-10 banner.
