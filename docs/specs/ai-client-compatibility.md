# Roadmap: AI Client Compatibility & Distribution

## Status

draft

## Address

M3B (packaging work items) · M3D (remote MCP + auth) · **M3O (new increment:
first-party directory listings)** · Phase-0 corrections carry no milestone
(routine doc/CLI fixes). M3O and the new M3B/M3D work items are registered in
[`roadmap.md`](../roadmap.md) in this PR (📐 M3O row + M3B/M3D notes), per
`.claude/rules/shipping.md`'s new-spec checklist — not deferred to `ready`
promotion.

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
Gemini CLI, Zed, JetBrains, and the long tail — plus the **ChatGPT desktop
app**, which takes a local stdio server via the shared Codex host). **Remote
streamable-HTTP + OAuth** is the only way into the consumer cloud surfaces
(claude.ai web/mobile, ChatGPT **web/mobile**, remote Cowork sessions) — and SSE
is deprecated
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
| Gemini CLI | stdio, SSE, streamable HTTP + OAuth | `~/.gemini/settings.json`; `gemini extensions install <repo>` | **T1** | Extension gallery is un-vetted/community by design. **Sunsetting into Antigravity CLI** — Antigravity carries the forward "Google agent surface" T1 slot. **Deliberately kept T1 (founder decision 2026-07-11), not demoted:** still widely installed today; revisit at actual sunset |
| Antigravity (Google) | stdio, SSE, streamable HTTP | desktop app + `antigravity` CLI MCP config; Python SDK `skills_paths` | **T1** | Google's first-party agent surface — **peer to Codex (OpenAI) / Claude Code (Anthropic)**; its CLI is the Gemini-CLI successor. Surfaces in flux (desktop primary; Nov-2025 IDE on a deprecation track) → target desktop + CLI, re-verify the config path each release |
| Zed | stdio (`context_servers`); remote unconfirmed | `settings.json` or packaged Zed extension | T2 | Extension packaging is a later nicety, not required |
| JetBrains AI Assistant / Junie | stdio, streamable HTTP, SSE | IDE Settings → MCP; Junie `.junie/mcp/mcp.json` | T2 | |
| Cline | stdio | in-app MCP Marketplace (one-click) | T2 | Marketplace submission is free/community — cheap reach when wanted |
| Goose | stdio | `config.yaml` / in-app Add Extension | T2 | |
| Continue.dev | stdio | `config.json` / workspace YAML | T2 | Agent-mode only |
| LibreChat | stdio, sse, streamable-http | `librechat.yaml` / in-app panel | T2 | |
| Warp | stdio, SSE URL | UI add; auto-detects `.warp/.mcp.json` + reads Claude Code/Codex config | T2 | Often works with zero MoneyBin effort via config pickup |
| Open WebUI | streamable-http (native ≥0.6.31); stdio via `mcpo` | admin config | T2 | Localhost streamable-http intersects our `--insecure` gate — document carefully |
| Windsurf | stdio, streamable HTTP, SSE (OAuth on all) | `mcp_config.json`; in-app marketplace | **T2** | **Demoted from T1 2026-07-11**: works via stdio, but momentum faded post-Cognition-acquisition (~$82M ARR vs Cursor ~$2B) and the **100-active-tool cap vs our 102** is a per-release headroom tax not worth paying. Document only; revisit if it re-enters the momentum tier |
| claude.ai web + mobile (custom connectors) | remote MCP (OAuth optional platform-side) | Settings → Connectors (Free capped at 1) | **T3** | M3D. Available on all plans incl. Free |
| ChatGPT desktop app (Codex host) | **stdio + streamable HTTP** | Settings → MCP servers → Add (STDIO); shares `~/.codex/config.toml`; `mcp install --client chatgpt-desktop` writes it (PR #315) | **T1** (pending #315) | Same local host as Codex — configure once, use in ChatGPT desktop + Codex CLI + IDE extension. **Until #315 merges, `chatgpt-desktop` is manual-config only** (still in `_NO_INSTALL_CLIENTS` on `main`) |
| ChatGPT web (Developer Mode) | **remote-only** (HTTPS `/mcp`; SSE+streamable) | Developer Mode → add connector | **T3** | Web doesn't read local Codex config. **Mobile MCP support undocumented** (Jul 2026). Plus/Pro/Business/Enterprise/Edu; Free excluded. Write-tiering ambiguous — re-verify at M3D |
| Cowork remote sessions | remote MCP via connectors | claude.ai connectors | **T3** | Same M3D unlock |
| Claude Connectors Directory / ChatGPT Apps SDK (→ "Plugins") | hosted remote + review | vendor submission portals | **T3 (M3O)** | Both require org accounts + human review; see M3O |
| Gemini Code Assist | MCP in Private Preview | account-team request | **✗** | Revisit: GA announcement |
| Consumer Gemini app | no custom MCP (3 built-in Spark connectors) | — | **✗** | Revisit: Google opens Spark/MCP to third parties |
| Raycast | community extension-mediated | community `mcp-config.json` | **✗** | Revisit: first-party built-in MCP client ships |
| GitHub Copilot cloud coding agent | no OAuth remote MCP | — | **✗** | Revisit: OAuth support lands |
| SSE / WebSocket transports | deprecated / no-OAuth niche | — | **✗** | We ship `--transport sse` today: deprecate the flag at M3D, remove per the CLI deprecation policy |

## Tiering rationale (momentum review, 2026-07-11)

Nearly every serious client speaks **local stdio MCP**, so the marginal cost of
*coverage* is near zero — one MoneyBin server + a documented config snippet
reaches most of the field. The real cost is the **per-client tax**: install
automation we smoke-test every release (T1) plus quirks like tool caps. So the
tier line is drawn on *momentum × tax*, not on whether a client technically
works.

- **T1 = the frontier-lab agent surfaces + reach leaders.** Each major lab now
  ships a first-party agent surface: **Codex** (OpenAI), **Claude Code**
  (Anthropic), **Antigravity** (Google) — all T1, for coherence. Plus **Cursor**
  (~$2B ARR, revenue leader), **VS Code / Copilot** (largest install base),
  **Claude Desktop** (our consumer home), and **Gemini CLI** (T1 through its
  sunset into Antigravity CLI).
- **Windsurf → T2.** A top-3 name in 2024/early-2025, but Google poached its
  founders and Cognition acquired the remainder (Dec 2025); it now trails badly
  on adoption (~$82M vs Cursor's ~$2B ARR). Combined with the 100-tool-cap tax
  against our 102-tool surface, it doesn't earn a release-gated T1 commitment.
  Still documented (stdio works); revisit on a momentum change.
- **Antigravity → T1.** Google-backed, MCP over stdio/SSE/HTTP, its CLI inherits
  Gemini CLI's large base. Adoption is early and its surfaces are still moving,
  so T1 targets the stable desktop + CLI and re-verifies config each release.

Sources: AI-coding market-share reviews (Cursor ~$2B ARR / Claude Code most-loved
/ Copilot largest base), Cognition–Windsurf acquisition coverage, and the
Antigravity 2.0 four-surface launch (Google I/O 2026) confirming stdio MCP. Full
session research: `private/research/2026-07-10-ai-client-compatibility.md`.

## Blessed-path corrections (Phase 0 — immediate)

Stale guidance found during the review; all are routine fixes:

1. **`docs/guides/mcp-clients.md` Claude Desktop section** presents config-file
   JSON as the primary path. Vendor docs now bless `.mcpb` extensions and frame
   manual JSON as legacy (still supported). Rewrite the section; add the
   Cowork caveat (remote sessions never see local MCP; local sessions do) and
   the managed-org flags (`isLocalDevMcpEnabled`, `isDesktopExtensionEnabled`).
2. **`mcp install --client chatgpt-desktop` should write the shared Codex TOML
   config, not a remote-only disclaimer.** The ChatGPT desktop app hosts Codex
   and reads `~/.codex/config.toml` — the same file `mcp install --client codex`
   writes — so it takes an ordinary local stdio server (Settings → MCP servers →
   Add → STDIO). Write that config, as **PR #315** implements. Only ChatGPT
   **web/mobile** is remote-only (needs M3D). *(Corrects an earlier draft of this
   spec that wrongly called all ChatGPT surfaces remote-only — its evidence was
   the Apps SDK / Developer Mode "apps in ChatGPT" path, a different feature from
   the desktop app's Codex-host MCP-servers setting.)*
3. **Snippet hardening** in `mcp install`: emit the absolute `uv` path (macOS
   GUI-launched clients drop shell PATH — documented recurring failure);
   include `startup_timeout_sec` for Codex (uvx cold start 3–15s vs 10s
   default); note Gemini CLI trust settings.
4. **File the upstream Claude Desktop bug**: connector toggle ON in Chat with
   tools never reaching the model (observed 2026-07-10) contradicts Anthropic's
   documented behavior — file with logs against claude-ai-mcp.
5. **Windsurf tool-cap overflow is a shipped defect, not informational.**
   Progressive disclosure was retired (`mcp-architecture.md` §3): the full
   registered surface is visible at connect, so all **102** tools count against
   Cascade's hard **100-active-tool ceiling**. Count confirmed against the live
   served surface — `moneybin mcp list-tools` → `list_tools()` reports
   `total_count: 102`, `0 hidden`. (Static `@mcp_tool`-decorator counts undercount
   — e.g. 100 if you subtract the intentionally-unregistered budget/transform
   modules — because the served surface includes tools registered outside those
   modules; only a live `list_tools()` is authoritative.) We are 2 over. **PR
   #315** warns at install time and pins the count with a test; the durable fix
   is getting the served surface back under 100.
6. **`docs/features.md` and `docs/specs/user-facing-doc-polish.md` are CORRECT
   about ChatGPT Desktop — do not "fix" them.** Both say the ChatGPT desktop app
   takes a local stdio server, and item 17 keeps a Desktop-vs-web/mobile split;
   that is right (desktop = Codex host + `~/.codex/config.toml`, stdio;
   web/mobile = remote-only). An earlier draft of this spec wrongly flagged them
   as stale — recorded here so the error is not reintroduced.

## Packaging ladder (what we ship, in order)

**The ladder splits on one line: does the channel require a human-review
workflow?** Self-serve channels (rungs 1–4, plus the Homebrew tap) are
**tester-distribution-eligible now** — they put MoneyBin in testers' hands with
no external gatekeeper. The human-reviewed app directories (rung 7) are **held
until the first public release is validated** (founder directive 2026-07-11;
`docs/roadmap.md`'s pre-v1 bar): no officially-reviewed public listing ships
before the product is tested.
Shipping the `.mcpb` *file* to testers (rung 2) is self-serve and fine now;
*submitting* that `.mcpb` to the Connectors Directory is the human-review step,
so it lives at rung 7's gate, not rung 2's.

| Rung | Artifact | Reaches | Review gate | Address |
|---|---|---|---|---|
| 1 | **PyPI + `uvx moneybin`** as the canonical config command (repo-checkout `uv run` stays the dev path) | every stdio client | none (quiet publish) | M3B — **tester-eligible** |
| 2 | **`.mcpb` bundle** via `server.type="uv"` (manifest ≥0.2 w/ privacy policy, so it's directory-submittable later) | Claude Desktop one-click; Claude Code; MCP for Windows | none (self-install) | M3B — **tester-eligible** |
| 3 | **Claude Code plugin + self-hosted marketplace** (`.claude-plugin/marketplace.json` in a MoneyBin repo; bundles the MCP server now, skills later) | all Claude Code users, `/plugin install` | none (self-hosted marketplace needs no Anthropic approval) | M3B — **tester-eligible** |
| 4 | **Official MCP Registry publish** (`server.json`, `pypi` registryType, namespace via GitHub OIDC or DNS) → aggregators (Smithery, Glama) pick it up; **install deep-link badges** (Cursor, VS Code) in README | discovery everywhere | none (self-serve namespace verification, like PyPI) | M3B — **tester-eligible** |
| 5 | **Docker image**; Docker MCP Catalog when demand justifies (Gateway is invite-only — catalog listing alone is fine) | self-host crowd; Docker Desktop users | catalog submission is reviewed → treat as gated | M3B (later) / gated |
| 6 | **Hosted remote MCP + OAuth** | claude.ai, ChatGPT web/mobile, Cowork remote, mobile | n/a (our own infra) | **M3D** |
| 7 | **Directory listings** (Claude Connectors Directory; ChatGPT Apps SDK → "Plugins"; `.mcpb` directory submission) | ordinary consumer users | **human review** | **M3O — gated on the first public release** |

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

- **Spec floor:** OAuth 2.1 + PKCE, RFC 9728 protected-resource metadata (PRM),
  RFC 8414 AS metadata, RFC 8707 resource indicators. **Dynamic Client
  Registration (DCR) is a SHOULD** with ~4% real-world AS support; the **Client
  ID Metadata Document (CIMD)** approach (spec 2025-11-25) is the practical
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

The only channels that reach ordinary consumer users. All require a **human
review** workflow, so per the 2026-07-11 founder directive M3O is **gated on
the first public release being validated** — no officially-reviewed public
listing before the product is tested — *and* on M3D (authenticated remote) plus
organizational prerequisites. Sequenced last, deliberately.

- **Claude Connectors Directory:** submission requires a **Team/Enterprise
  claude.ai org** (owner-submitted), HTTPS remote server (streamable HTTP or
  SSE), OAuth 2.0 for authenticated services, per-tool annotations
  (✅ shipped — readOnly/destructive/idempotent/openWorld hints; per-tool `title`
  not yet wired), privacy policy (instant rejection if missing), human
  review with self-test attestation.
- **Local one-click via `.mcpb` directory submission:** the `.mcpb` *file* ships to
  testers at rung 2 now, but *submitting* it to the Connectors Directory
  (manifest ≥0.2 with privacy policy) is itself a human-review step — so it
  belongs to this first-public-release-gated increment, not to rung 2.
- **ChatGPT (Apps SDK → "Plugins"):** developers submit via the **Apps SDK**
  (Python/FastMCP is a blessed SDK); end users install and browse it as a
  **"Plugin" in ChatGPT Work**. Manual review, privacy policy + verified website
  + screenshots;
  regional exclusions (EEA/CH/UK at launch) may affect reach.
- Prerequisite to name in roadmap: the submitting org account is a dedicated
  MoneyBin organization (provisional default; finalized at M3O — see Decisions);
  acquiring/holding it is a
  real cost, not a formality.

## Egress posture

Per the privacy framework: **cloud clients get remote MCP only behind
authentication plus the sensitivity consent gate** (the planned
medium/high-tier downgrade for cloud consumers in
[`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) becomes an M3D
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
- REST API surface (future, `.claude/rules/surface-design.md` §REST).

## Decisions (resolved 2026-07-11)

The initial review's open questions (OQ1–OQ6) are resolved by the **founder directive of
2026-07-11** — a momentum review plus three calls (Antigravity → T1; ship the
Claude Code distribution plugin; gate officially-reviewed distribution on the
first public release). Content decisions:

- **Antigravity → T1** (was OQ3). It is Google's first-party agent surface — the
  peer to Codex and Claude Code — supports stdio MCP, and its CLI succeeds the
  sunsetting Gemini CLI. T1 targets the stable desktop + CLI surfaces and
  re-verifies the config path each release while the surfaces settle.
- **Windsurf → T2** (was OQ1). Momentum faded after the Cognition acquisition,
  and the 100-active-tool cap vs our 102 is a per-release headroom tax not worth
  paying for a distant follower. Documented, not release-gated; no headroom
  policy. Revisit on a momentum change. (The Phase-0 item to *verify* the visible
  count stays — still worth knowing — but it no longer gates a T1 commitment.)
- **Ship the Claude Code distribution plugin in M3B** (was OQ6). It is a
  server-distribution vector (`/plugin install moneybin` via a self-hosted
  marketplace, no review) — distinct from, and additive to, the M3I
  contributor/authoring plugin. Cheap, high-leverage for the most-loved client,
  and needs no human-review workflow, so it fits tester distribution. Milestone
  ordering is not immutable: it lands with the launch packaging set (rung 3), not
  deferred to M3I.

Deferred with a named trigger (deliberately not pre-resolved):

- **ChatGPT read-vs-write plan tiering** (was OQ2) — verify at M3D execution;
  vendor docs conflict and may change by then. Gates nothing now.
- **Which org account submits to the directories** (was OQ4) — resolved at M3O;
  provisionally a dedicated MoneyBin organization account (cleaner than personal for a public
  listing). Holding the org account is a real prerequisite, named not costed.
- **`.mcpb` clean-machine cold-start budget** (was OQ5) — an M3B spike acceptance
  test; measure on a machine with no uv preinstalled, don't opine.
