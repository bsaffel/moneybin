<!-- Last reviewed: 2026-07-18 -->
# Configuring MCP Clients

MoneyBin's MCP server runs over stdio today and connects to any MCP-spec-compliant client. This guide covers the clients we test against and the install steps for each. For the protocol-level details (envelope shape, tool catalog, sensitivity tiers), see the [MCP server guide](mcp-server.md).

## `moneybin mcp install` — one command per client

`moneybin mcp install --client <name>` writes the MoneyBin server entry into the client's canonical config file (creating the file if it doesn't exist), preserving any unrelated entries already in it. The command does *not* start the server — it only edits config; the client launches MoneyBin on its own when it needs the tools.

```bash
# Default client is claude-desktop. -y skips the confirmation prompt.
moneybin mcp install --client claude-desktop -y

# Print the snippet without writing — useful for inspection or for clients
# with no programmatic install path.
moneybin mcp install --client cursor --print

# Embed a specific profile in the generated entry. Each profile installed
# this way appears as a separate server (e.g. "MoneyBin (alice)").
moneybin mcp install --client claude-code --profile alice -y
```

The supported `--client` values are:

- `claude-desktop`
- `claude-code`
- `cursor`
- `windsurf`
- `vscode`
- `gemini-cli`
- `codex` (CLI, Desktop app, IDE extension — all share `~/.codex/config.toml`)
- `chatgpt-desktop` (the ChatGPT desktop app hosts Codex and shares that same file — installing for either covers both)

ChatGPT on the **web** cannot reach a local MoneyBin. See [ChatGPT desktop app](#chatgpt-desktop-app).

If `--client` is omitted, `claude-desktop` is the default. To look up the install path the command would write to (without writing), use:

```bash
moneybin mcp config path --client <name> [--profile <name>]
```

### Preview the snippet

`--print` emits the exact bytes the command would write, without touching any file. Useful for review before merging into a shared config. The shape varies by client:

```json
// Claude Desktop / Cursor / Windsurf / Gemini CLI / Claude Code — JSON, "mcpServers" key
{
  "mcpServers": {
    "MoneyBin": {
      "command": "/opt/homebrew/bin/uv",
      "args": ["run", "--directory", "/path/to/repo", "moneybin", "--profile", "default", "mcp", "serve"]
    }
  }
}
```

`command` is the **absolute path** to `uv`, resolved when you run install. That is deliberate: macOS clients launched from the GUI (Claude Desktop, Cursor) do not inherit your shell's `PATH`, so a bare `uv` resolves to nothing and the server dies at launch with an error the client reports as a generic failure. If `uv` isn't on your `PATH` at install time either, the bare name is emitted and the client will tell you it couldn't start.

VS Code uses `{"servers": {...}}` with an explicit `"type": "stdio"`. Codex uses TOML under `[mcp_servers.MoneyBin]`, and carries a `startup_timeout_sec = 30` — Codex defaults to 10s, but a cold `uv run` (building the environment on first launch) routinely takes longer, so the very first connection is the one most likely to time out. If `MONEYBIN_HOME` is set when you run install, an `env` block pinning it lands inside the server entry so the client launches the server with the same home directory you used.

Installing a non-default profile generates a distinct entry name (e.g. `MoneyBin (alice)`); see [Switching profiles](#switching-profiles) below.

## Where data goes

The MCP transport is local-only and the MoneyBin server itself does not phone home, but the **client** you connect to is almost certainly cloud-hosted. Be deliberate about which surface is which.

- **`moneybin mcp serve` (the server side).** Makes no outbound network calls of its own — no telemetry, no update checks, no license pings, no merchant-enrichment fetches. It reads and writes only the local DuckDB profile. The egress posture is "zero by default."

- **The MCP client (Claude Desktop, Cursor, Codex, …).** Sends your prompt and the tool-result payloads MoneyBin returns to its own hosted LLM provider, per the client's privacy policy. When you ask "what did I spend on groceries?", the agent receives row-level transaction data from MoneyBin and forwards it upstream as ordinary tool-result context.
- **Sensitivity tiers.** Every MoneyBin tool declares `low` / `medium` / `high` per [`mcp-server.md`](mcp-server.md). A consent gate that downgrades `medium`/`high` responses for cloud clients is planned. Until it lands, **treat anything you ask the agent as if you sent it directly to the model provider** — because effectively, you did.
- **Other MoneyBin surfaces.** Plaid sync, OAuth, and any future hosted-server features do make outbound calls when you use them. Those flow through `moneybin-sync`, not the MCP server — see [`docs/reference/server-api-contract.md`](../reference/server-api-contract.md) for that contract.
- **Local-LLM clients.** No first-class MCP-compatible local-LLM agent is shipping today (Ollama doesn't expose MCP; LM Studio's support is experimental). When one becomes stable, MoneyBin will connect to it the same way it connects to Claude Desktop — the server side doesn't care which LLM is on the other end of the stdio pipe.

## Bounded tool surface

MoneyBin exposes one **47-tool standard registry**. Generic clients receive all
47 tools. A supported host may defer schemas from that same registry to reduce
prompt cost, without reconnect, packs, or profiles; tool names, approvals,
allowlists, annotations, and audit identity do not change. Reports are catalog
entries behind `reports`, not additional tool slots. The initial registry
advertises zero output schemas; a future schema or tool must pass the admission
record in [`mcp-tool-surface-scaling.md`](../specs/mcp-tool-surface-scaling.md).

## Per-client setup

### Claude Desktop

Anthropic's desktop app for macOS and Windows.

Anthropic now blesses **desktop extensions (`.mcpb` bundles)** as the primary way to add a local MCP server: one file, installed through the app's own UI, no JSON editing. Hand-editing `claude_desktop_config.json` still works and is still supported — it is simply the legacy path now.

**MoneyBin does not ship an `.mcpb` bundle.** The config-file path below is the supported way to install it.

```bash
moneybin mcp install --client claude-desktop -y
```

- **Config file:** `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS). Windows uses the equivalent `%APPDATA%\Claude\claude_desktop_config.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** Yes. Quit Claude Desktop entirely (menu bar → Quit, not just closing the window) and reopen it. The app reads its MCP config only at launch.
- **Server lifecycle:** One server process per app instance, spawned at launch and reused across all chats. Opening "New chat" does not spawn another server.
- **Confirmation UI:** Renders Claude's standard tool-call approval prompt. Tools marked `destructiveHint=true` (categorization commits, rule deletes, refresh runs) render with a more explicit confirmation than read-only tools.

**Cowork sessions can't see MoneyBin.** Claude's Cowork surface runs *remote* sessions in Anthropic's cloud, and a remote session cannot reach an MCP server running on your machine — it will behave as though MoneyBin isn't installed. Local Claude Desktop sessions see it normally. This is not a misconfiguration; a local stdio server is simply unreachable from a cloud session.

**Managed / work devices.** If Claude Desktop is administered by an organization, two admin flags decide whether any of this is available to you: `isLocalDevMcpEnabled` (local MCP servers at all) and `isDesktopExtensionEnabled` (`.mcpb` extensions). With them off, MoneyBin cannot be installed into that Claude Desktop, and the failure looks like the server silently never appearing. Check with whoever administers the device before debugging further.

See [Verifying the connection](#verifying-the-connection) for a smoke test.

### Claude Code

Anthropic's CLI agent. Unlike every other client here, MoneyBin's Claude Code config lives in the **MoneyBin profile directory** (`~/.moneybin/profiles/<profile>/claude-code-mcp.json` by default), not Claude Code's own MCP config. That way plain `claude` invocations in your repo don't load MoneyBin and don't take the database lock — MoneyBin only attaches when you launch a session that explicitly opts in.

```bash
moneybin mcp install --client claude-code --profile <name> -y
```

To launch Claude Code **with** MoneyBin attached, run one of:

```bash
make claude-mcp                       # active profile
make claude-mcp PROFILE=<name>        # explicit profile
./scripts/claude-mcp.sh <name>        # equivalent without Make
```

These resolve to `claude --strict-mcp-config --mcp-config <profile-config-path>`, telling Claude Code to ignore every other configured MCP server and load only MoneyBin for that one session.

- **Config file:** `~/.moneybin/profiles/<profile>/claude-code-mcp.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** None — each `make claude-mcp` invocation is a fresh session.
- **Server lifecycle:** Per-invocation. Each new `make claude-mcp` launches a new MoneyBin server bound to that one session.

Inside a `make claude-mcp` session, the `/mcp` slash command shows MoneyBin's tool list as a quick sanity check.

### Cursor

AI-first editor with native MCP support.

```bash
moneybin mcp install --client cursor -y
```

- **Config file:** `~/.cursor/mcp.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** Yes — quit Cursor and reopen. The MCP server list is read at launch.
- **Server lifecycle:** One server process per Cursor instance.
- **Confirmation UI:** Cursor surfaces tool calls in its agent chat panel. It honors `readOnlyHint` for the auto-approve UI but does not render distinct treatment for `destructiveHint`-flagged tools today — be deliberate with what you let it auto-approve. The Cursor **Settings → MCP Servers** panel shows MoneyBin with its tool count populated when the server starts cleanly.

### Windsurf

Codeium's editor.

```bash
moneybin mcp install --client windsurf -y
```

- **Config file:** `~/.codeium/windsurf/mcp_config.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** Yes — quit Windsurf and reopen.
- **Server lifecycle:** One server process per Windsurf instance.
- **Confirmation UI:** Tool-call approval is shown in the Cascade chat panel. Like Cursor, Windsurf reads `readOnlyHint` but doesn't currently distinguish `destructiveHint` in its UI.

> **MoneyBin’s 47-tool registry fits Windsurf’s limit.** Cascade holds a maximum
> of **100 tools at any one time**, across *all* connected MCP servers. MoneyBin
> uses 47 of those slots, so other servers still share the remaining budget.
> Windsurf gives no warning when the combined total crosses the limit; disable
> unused servers in **Settings → MCP Servers** if your overall configuration
> exceeds 100.

### VS Code (Copilot Chat, agent mode)

GitHub Copilot Chat in agent mode reads workspace-scoped MCP config from `.vscode/mcp.json`.

```bash
# Must be run inside a repo checkout — VS Code MCP config is workspace-local
moneybin mcp install --client vscode -y
```

- **Config file:** `<repo>/.vscode/mcp.json` (workspace-local; created in whichever repo you ran the command from).
- **Format:** JSON, under the `servers` key, with an explicit `"type": "stdio"` field per VS Code's schema.
- **Restart required:** Reload the VS Code window (Command Palette → "Developer: Reload Window") after install. New servers are not picked up live.
- **Server lifecycle:** One server process per workspace, loaded only when that workspace is open. Closing the workspace stops the server.
- **Confirmation UI:** Copilot Chat agent mode shows a tool-call confirmation card. It honors `readOnlyHint` for auto-approval defaults; `destructiveHint` rendering depends on the Copilot Chat version.

Note: this install path requires a git repo (`find_repo_root()` must resolve). Outside a repo, the command fails with a clear error — that's intentional, since `.vscode/mcp.json` is meaningful only in a workspace.

### Gemini CLI

Google's `gemini` command-line agent.

```bash
moneybin mcp install --client gemini-cli -y
```

- **Config file:** `~/.gemini/settings.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** No — `gemini` reads settings on each invocation.
- **Server lifecycle:** Per-invocation. Every `gemini` command in any terminal spawns its own MoneyBin server. Multiple `gemini` sessions on the same profile coexist — reads usually coexist and writes serialize. A write-mode tool call fails only when another session holds a conflicting lock past the retry window (a long write, or a long read holding the read lock); a read-mode call fails only when it lands during a long write — see [Concurrency](#concurrency-which-clients-share-a-server) below.
- **Confirmation UI:** `gemini` prompts in the terminal before invoking tools by default. Tool annotations are not currently surfaced in the prompt text.
- **`trust` is deliberately not set.** Gemini CLI supports a per-server `"trust": true` setting that, in its own words, will "trust this server and bypass all tool call confirmations." MoneyBin does not write it. Our surface includes write tools — import, categorize, delete, refresh — and those should ask before they act on your financial data. Add it by hand only if you accept that every MoneyBin tool call runs unprompted.

### Codex (CLI, Desktop app, IDE extension)

OpenAI's Codex products share `~/.codex/config.toml`. A single install covers all three surfaces.

```bash
moneybin mcp install --client codex -y
```

- **Config file:** `~/.codex/config.toml`.
- **Format:** TOML, under `[mcp_servers.<name>]`. The merge is done through `tomlkit`, so existing comments, key ordering, and unrelated settings in `config.toml` survive.
- **Surfaces covered:**
  - **Codex CLI** — the `codex` command.
  - **Codex Desktop app** — macOS / Windows app from [developers.openai.com/codex/app](https://developers.openai.com/codex/app).
  - **Codex IDE extension** — VS Code and JetBrains.
- **Restart required:** Restart the Codex app or IDE extension after install. The CLI re-reads `config.toml` on each invocation.
- **Server lifecycle:** Per-invocation for the CLI; per-app-instance for Desktop and the IDE extension.
- **Confirmation UI:** Codex Desktop and the IDE extension render an approval dialog for tool calls. Today they don't visibly distinguish `destructiveHint` from non-destructive read tools — assume every approval is "yes, run this" without a softer "read-only" path.

As an alternative install path, OpenAI also documents `codex mcp add` for managing servers from the CLI. The block `moneybin mcp install --client codex` writes is equivalent.

### ChatGPT desktop app

The ChatGPT desktop app **hosts Codex**, and shares its MCP configuration: per OpenAI's docs, "The ChatGPT desktop app, Codex CLI, and IDE extension support MCP servers and share MCP configuration for the same Codex host." So it takes an ordinary local stdio server — the same `~/.codex/config.toml` entry the Codex CLI uses.

```bash
moneybin mcp install --client chatgpt-desktop -y
```

- **Config file:** `~/.codex/config.toml` — **the same file as `--client codex`.** Installing for either one covers the ChatGPT desktop app, the Codex CLI, and the Codex IDE extension. There is no need to run both.
- **Format:** TOML, under `[mcp_servers.<name>]`, carrying `startup_timeout_sec` (see Codex below).
- **Restart required:** Yes. In ChatGPT, the server appears under **Settings → MCP servers**; select **Restart** there to pick it up. You can also add it through that UI by hand (Add server → choose **STDIO** → paste the command).
- **Server lifecycle:** One server per app instance; the shared config also means every `codex` shell invocation auto-loads MoneyBin — see [Concurrency](#concurrency-which-clients-share-a-server).

> **ChatGPT on the web cannot see this.** "ChatGPT web doesn't read local Codex configuration files" — it reaches MCP only through *remote* connectors over HTTPS, and MoneyBin's server is local.
>
> **Do not** reach for `moneybin mcp serve --transport streamable-http --insecure` to bridge that gap. It has no authentication at all — anyone who can reach the port can read and write your finances — and ChatGPT's cloud can't reach a port on your laptop anyway. See [Transport](#transport).

## Concurrency: which clients share a server

MoneyBin stores each profile's data in a single-writer DuckDB file, but each MoneyBin process opens **short-lived, per-operation connections** rather than holding the file open for its whole lifetime. As a result, **multiple MoneyBin processes (MCP servers and CLI commands) can run against the same profile at once:** reads attach in shared mode and coexist with other reads; writes take the exclusive lock one at a time, retrying briefly (up to 5 s) on contention. Read and write opens that cross each other retry on the same backoff in both directions. A call fails only when whichever operation holds the conflicting lock — a long write (large import or transform) or a long-running read (a slow `sql_query` or large `reports` call) — exceeds the retry window. Only that one operation fails, not the whole session.

| Pattern | Clients | Behavior |
|---|---|---|
| App-shared connection | Claude Desktop, ChatGPT desktop app, Cursor, VS Code, Windsurf | One server process per app instance, spawned at launch and reused across all chats. |
| Per-invocation | Claude Code (via `make claude-mcp`), Codex CLI, Gemini CLI | One server process per CLI invocation. Each new shell session spawns a fresh server; servers coexist on the same profile and contend only when operations need conflicting locks — concurrent writes, or a write racing a long-running read. |

Different *profiles* never collide — each has its own DB and lock — so `MoneyBin (alice)` and `MoneyBin (bob)` can coexist in the same client without issue. Write contention only ever arises between concurrent sessions on the **same** profile.

Practical guidance:

- **One app-based client per profile is still simplest.** Two app instances on the same profile both run fine, but their writes serialize — a write issued while the other instance is mid-import or mid-transform can wait and then fail with a lock-timeout error.
- **Concurrent writes serialize.** Running a MoneyBin CLI write command (`moneybin import`, `moneybin transform apply`, etc.) while a desktop client is writing to the same profile can make one of them wait or time out. Read commands (`moneybin transactions`, `moneybin reports`, etc.) rarely contend with writes; if a read fails with a lock error, a long write in progress is the cause.
- **CLI clients (codex, gemini-cli) auto-load on every invocation.** Installing into them means every `codex` or `gemini` command in any terminal will try to spawn the MoneyBin server. For occasional use, prefer `mcp install --print` and paste the snippet manually only when you want it. Claude Code is the deliberate exception — `make claude-mcp` makes it explicit per-session.

## Verifying the connection

After installing and restarting the client, run one low-risk tool:

- `system_status` — data inventory and freshness snapshot. Low sensitivity, no PII.
- `accounts` — lists configured accounts.

Both return the standard MoneyBin envelope. `system_status` looks roughly like:

```json
{
  "summary": {"sensitivity": "low", "display_currency": "USD", "degraded": false},
  "data": {
    "accounts": {"count": 6},
    "transactions": {"count": 12483, "date_range": ["2023-01-04", "2026-05-14"], "last_import_at": "2026-05-17T09:12:33"},
    "categorization": {"uncategorized": 17},
    "transforms": {"pending": false, "last_apply_at": "2026-05-17T09:13:01"}
  },
  "actions": ["Use reviews for per-queue review counts", "Use reports(report_id=\"core:spending\") for a monthly spending trend snapshot"]
}
```

If the response is missing fields, has `degraded: true` unexpectedly, or surfaces a raw error, check that the server actually started — each client writes its own log; consult that client's documentation for log paths, since MoneyBin's stderr is forwarded into the client's process logs.

You can cross-check the same payload from the CLI:

```bash
moneybin system status --output json
moneybin accounts --output json
```

The envelope shape is identical. See the [CLI reference](cli-reference.md) for the full command list.

For direct stdio inspection without going through a client (useful for debugging tool schemas or reproducing client-side issues), run `moneybin mcp serve` in the foreground and drive it with the MCP inspector or any JSON-RPC client.

## Uninstall and reset

There is no `moneybin mcp uninstall` command today — removal is a manual edit to the client's config file.

1. Run `moneybin mcp config path --client <name>` to print the resolved config path.
2. Open the file in an editor.
3. For JSON clients (Claude Desktop, Cursor, Windsurf, VS Code, Gemini CLI), delete the `"MoneyBin"` (or `"MoneyBin (<profile>)"`) entry under `mcpServers` / `servers`. For Codex, delete the `[mcp_servers.MoneyBin]` table. For Claude Code, delete the per-profile MoneyBin config file under `~/.moneybin/profiles/<profile>/claude-code-mcp.json` outright — that file holds only MoneyBin's entry.
4. Restart the client (per the per-client restart rules above).

If `mcp install` previously errored out with `Cannot parse existing config`, the safe recovery is to rename the broken file to `<name>.bak` and re-run `moneybin mcp install --client <name> --print` to get a clean snippet, then merge your prior unrelated entries back by hand. Renaming preserves the original for forensics; the install path will create a fresh file alongside.

## Switching profiles

Re-running `moneybin mcp install` with a different `--profile <name>` adds a **second** entry to the client config (e.g. `MoneyBin` and `MoneyBin (alice)`) — it does not replace the previous one. The client sees both servers; some clients let you toggle them, others always start both. To switch which profile is "primary," uninstall the unwanted entry per the steps above. See the [profiles guide](profiles.md) for the durable model.

## Tool annotations and client rendering

Every MoneyBin tool emits the four MCP protocol annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`). What each client actually renders varies:

| Client | `readOnlyHint` | `destructiveHint` |
|---|---|---|
| Claude Desktop | Honored (affects auto-approve) | Confirmation prompt is more explicit on destructive tools |
| Claude Code | Honored | Honored |
| Cursor | Honored | Not currently distinguished in UI |
| Windsurf | Honored | Not currently distinguished in UI |
| VS Code Copilot Chat | Honored | Version-dependent |
| Gemini CLI | Not surfaced in prompt | Not surfaced in prompt |
| Codex (CLI / Desktop / IDE) | Approval dialog uniform | Approval dialog uniform |
| ChatGPT desktop app | Approval dialog uniform (Codex host) | Approval dialog uniform (Codex host) |

Where the client doesn't render a distinct destructive-tool confirmation, treat every tool-call approval as "yes, run this." MoneyBin's tool descriptions name the mutation surface explicitly — read them before approving.

## Transport

Today MoneyBin's MCP server speaks **stdio only** for the install paths above — the client launches MoneyBin as a child process and communicates over stdin/stdout. One server process per client session; the server's lifetime is bound to the client's.

The network transports (`sse`, `streamable-http`) exist in the underlying FastMCP runtime, but MoneyBin ships **no HTTP authentication** — an HTTP listener would let anyone who can reach the port read and write your financial data. So `moneybin mcp serve` refuses to start any non-stdio transport unless you pass `--insecure`, and even then only as a localhost-only escape hatch (e.g. ChatGPT Desktop builds that accept no stdio connector), printing a loud startup warning. A fully-supported HTTP transport — with real authentication, tunneling, and a remote-client story — is planned alongside the web UI. Never expose the `--insecure` listener to an untrusted network.

### Headless and daemon use

Because the transport is stdio, "MoneyBin as a long-running daemon with remote clients connecting in" isn't a supported deployment shape yet — the client process needs to be on the same host so it can fork-and-pipe `moneybin mcp serve`. What works today on a headless box:

- **Headless MCP clients on the same host.** Codex CLI, Gemini CLI, and Claude Code (via `make claude-mcp`) run without a GUI. Drop them in a tmux session on a NAS / homelab box and they'll spawn MoneyBin per invocation against the local DuckDB profile.
- **Desktop client on a workstation, data on the same workstation.** Standard install path; no networking involved.

What does not work today: running `moneybin mcp serve` as a systemd unit or Docker container with a Claude Desktop on a separate laptop connecting in. The `--insecure` HTTP transport is unauthenticated and localhost-only — safe remote access waits on the planned authenticated HTTP transport.

## Troubleshooting

**Server doesn't start.** Most common: the database is locked. Run `moneybin db unlock` to unlock the active profile's database before launching the client. If the unlock prompt errors out, check that you've created a profile (`moneybin profile create <name>`) and that the profile passphrase is set up.

**Client doesn't see any tools.** Restart the client after install — most clients read MCP config only at launch. If the client is restarted and still empty, run `moneybin mcp config path --client <name>` to print the resolved config path, then verify the file exists and contains a `MoneyBin` entry under `mcpServers` (or `servers` for VS Code, `[mcp_servers.<name>]` for Codex).

**Tools error with "no profile" or similar.** The install snippet embeds whichever profile was active when you ran `mcp install`. To change it, re-run with `--profile <name>` (see [Switching profiles](#switching-profiles)).

**"Database is locked" (server fails to start, or a tool call errors).** Another process is holding a conflicting lock on the same profile past the ~5 s retry window — typically a long operation in progress: (a) a `moneybin transform apply` or large `import` running in another terminal; (b) a desktop client mid-import or mid-transform on the same profile; (c) a long-running read (a slow `sql_query` or large `reports` call) holding the read lock — only blocks writers, not other readers; (d) a stuck process that never released the lock. If a read fails with a lock error, only a long write is the cause; if a write fails, either a long write or a long read can be. Run `moneybin db ps` to see who holds the file and `moneybin db kill` to clear stuck processes, or switch profiles.

**"Cannot parse existing config" on install.** The target file has invalid JSON or TOML. Fix the syntax in your editor and re-run, or use the `<name>.bak` recovery path in [Uninstall and reset](#uninstall-and-reset).

**Slow first call after launch.** Cold start imports the MCP runtime and loads settings. Subsequent calls in the same session reuse the connection.

**`make claude-mcp` reports "No active profile and --profile not supplied".** Either run `moneybin profile create <name>` first, or pass `PROFILE=<name>` to the make target.

## Stability and licensing

Stability of the MCP surface (tool names, parameter shapes, envelope fields) is documented alongside the protocol in the [MCP server guide](mcp-server.md). MoneyBin is AGPL-licensed; see [`docs/licensing.md`](../licensing.md) for what that means for your deployment.
