<!-- Last reviewed: 2026-05-17 -->
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
- `chatgpt-desktop`

If `--client` is omitted, `claude-desktop` is the default. To look up the install path the command would write to (without writing), use:

```bash
moneybin mcp config path --client <name> [--profile <name>]
```

## Per-client setup

### Claude Desktop

Anthropic's desktop app for macOS and Windows.

```bash
moneybin mcp install --client claude-desktop -y
```

- **Config file:** `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS). Windows uses the equivalent `%APPDATA%\Claude\claude_desktop_config.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** Yes. Quit Claude Desktop entirely (menu bar → Quit, not just closing the window) and reopen it. The app reads its MCP config only at launch.
- **Server lifecycle:** One server process per app instance, spawned at launch and reused across all chats. Opening "New chat" does not spawn another server.
- **Confirmation UI:** Renders Claude's standard tool-call approval prompt. Tools marked `destructiveHint=true` (categorization commits, rule deletes, refresh runs) render with a more explicit confirmation than read-only tools.

Verify the connection by asking: *"What's my account balance?"* — Claude should call `accounts_balances` and show the response.

### Claude Code

Anthropic's CLI agent.

```bash
moneybin mcp install --client claude-code --profile <name> -y
```

- **Config file:** `<base>/profiles/<profile>/claude-code-mcp.json` (a per-profile MoneyBin file under `~/.moneybin/profiles/<profile>/` by default, not Claude Code's own config).
- **Format:** JSON, under the `mcpServers` key.
- **Per-session opt-in.** Claude Code is the only client today that supports launching with an MCP config override. We use that on purpose: plain `claude` invocations don't load MoneyBin, so the database lock isn't taken when you're doing unrelated work. To launch *with* MoneyBin, run one of:

  ```bash
  make claude-mcp                       # active profile
  make claude-mcp PROFILE=<name>        # explicit profile
  ./scripts/claude-mcp.sh <name>        # equivalent without Make
  ```

  These resolve to `claude --strict-mcp-config --mcp-config <profile-config-path>`, which tells Claude Code to ignore every other configured MCP server and load only MoneyBin for that one session.

- **Restart required:** Each `make claude-mcp` invocation is a fresh session — no restart concept.
- **Server lifecycle:** Per-invocation. Each new `make claude-mcp` launches a new MoneyBin server bound to that one session.

Verify with the `/mcp` slash command in Claude Code, or ask the agent to call `system_status`.

### Cursor

AI-first editor with native MCP support.

```bash
moneybin mcp install --client cursor -y
```

- **Config file:** `~/.cursor/mcp.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** Yes — quit Cursor and reopen. The MCP server list is read at launch.
- **Server lifecycle:** One server process per Cursor instance.
- **Confirmation UI:** Cursor surfaces tool calls in its agent chat panel. It honors `readOnlyHint` for the auto-approve UI but does not render distinct treatment for `destructiveHint`-flagged tools today — be deliberate with what you let it auto-approve.

Verify with the Cursor settings panel under **MCP Servers**; MoneyBin should show up with its tool count populated.

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

Verify by opening Cascade and asking it to list your accounts; it should call `accounts`.

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

Verify by opening the Copilot Chat agent panel, switching to "Agent" mode, and asking it to call `system_status`.

### Gemini CLI

Google's `gemini` command-line agent.

```bash
moneybin mcp install --client gemini-cli -y
```

- **Config file:** `~/.gemini/settings.json`.
- **Format:** JSON, under the `mcpServers` key.
- **Restart required:** No — `gemini` reads settings on each invocation.
- **Server lifecycle:** Per-invocation. Every `gemini` command in any terminal will spawn MoneyBin and take the DB lock. If you keep two `gemini` sessions open on the same profile, the second will fail to acquire the lock — see [Concurrency](#concurrency-which-clients-share-a-server) below.
- **Confirmation UI:** `gemini` prompts in the terminal before invoking tools by default. Tool annotations are not currently surfaced in the prompt text.

Verify by running `gemini` and asking it to call `accounts`.

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

Verify by running `codex` and asking it to call `system_status`, or by checking the Codex Desktop's MCP servers panel.

### ChatGPT Desktop

ChatGPT Desktop adds MCP servers through **Settings → Connectors** (Developer Mode), not a JSON config file we can write programmatically.

```bash
moneybin mcp install --client chatgpt-desktop
```

This invocation always prints the canonical snippet plus a numbered Connector-setup checklist (which fields to fill, where to paste them, and the HTTP-connector fallback). `--print` is implicit; there is no file to write.

- **Config file:** none — the snippet is informational. Copy fields into the Connector UI by hand.
- **Restart required:** Yes — restart ChatGPT Desktop after adding the connector.
- **Server lifecycle:** One server process per app instance.
- **MCP support gating:** ChatGPT Desktop's MCP support depends on app version and account plan. If your build only accepts HTTP connectors, run `moneybin mcp serve --transport streamable-http` and register the resulting URL as a custom connector. (HTTP transport is supported by FastMCP today but is not the default install path — see [Transport](#transport) below.)

Verify by opening the Connectors panel and confirming MoneyBin shows the registered tool count.

## Concurrency: which clients share a server

MoneyBin stores each profile's data in a single-writer DuckDB file. **Only one MCP server (or any other MoneyBin process) can hold the database open against a given profile at a time.** A second connection on the same profile fails to acquire the write lock and exits.

| Pattern | Clients | Behavior |
|---|---|---|
| App-shared connection | Claude Desktop, ChatGPT Desktop, Cursor, VS Code, Windsurf | One server process per app instance, spawned at launch and reused across all chats. |
| Per-invocation | Claude Code (via `make claude-mcp`), Codex CLI, Gemini CLI | One server process per CLI invocation. Each new shell session spawns a fresh server and tries to claim the lock. |

Different *profiles* never collide — each has its own DB and lock — so `MoneyBin (alice)` and `MoneyBin (bob)` can coexist in the same client without issue. The lock fight is only between concurrent sessions on the **same** profile.

Practical guidance:

- **Pick one app-based client per profile** as the primary MoneyBin host. Two running app instances configured against the same profile will fight over the lock; the second to start fails.
- **Avoid running MoneyBin CLI commands** (`moneybin transactions`, `moneybin reports`, etc.) while a desktop client is using the same profile. The CLI opens its own DB connection.
- **CLI clients (codex, gemini-cli) auto-load on every invocation.** Installing into them means every `codex` or `gemini` command in any terminal will try to spawn the MoneyBin server. For occasional use, prefer `mcp install --print` and paste the snippet manually only when you want it. Claude Code is the deliberate exception — `make claude-mcp` makes it explicit per-session.

## Verifying the connection

After installing and restarting the client, run one low-risk tool:

- `system_status` — returns the data inventory and freshness snapshot. Low sensitivity, no PII.
- `accounts` — lists configured accounts.

Both should return the standard envelope: `summary` (counts, sensitivity tier, currency), `data` (the payload), and `actions` (next-step hints). If the response is missing fields or returns a raw error, check that the server actually started (the client log usually surfaces stderr from `moneybin mcp serve`).

You can cross-check the same payload from the CLI:

```bash
moneybin system status --output json
moneybin accounts --output json
```

The envelope shape is identical. See the [CLI reference](cli-reference.md) for the full command list.

## Hand-testing without a client

To inspect the server directly without going through an AI client:

```bash
moneybin mcp serve
```

This starts the server on stdio in the foreground; you can drive it with the MCP inspector or any JSON-RPC client. Useful for debugging tool schemas or reproducing a client-side issue against a known-good transport.

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
| ChatGPT Desktop | Per Connector UI behavior | Per Connector UI behavior |

Where the client doesn't render a distinct destructive-tool confirmation, treat every tool-call approval as "yes, run this." MoneyBin's tool descriptions name the mutation surface explicitly — read them before approving.

## Transport

Today MoneyBin's MCP server speaks **stdio only** for the install paths above — the client launches MoneyBin as a child process and communicates over stdin/stdout. This means one server process per client session, and the server's lifetime is bound to the client's.

`moneybin mcp serve --transport streamable-http` is supported by the underlying FastMCP runtime today and is the path ChatGPT Desktop's HTTP-connector fallback uses, but the install snippets above all assume stdio. A fully-supported HTTP transport (with proper auth, tunneling, and a remote-client story) is planned alongside the web UI.

## Troubleshooting

**Server doesn't start.** Most common: the database is locked. Run `moneybin db unlock` to unlock the active profile's database before launching the client. If the unlock prompt errors out, check that you've created a profile (`moneybin profile create <name>`) and that the profile passphrase is set up.

**Client doesn't see any tools.** Restart the client after install — most clients read MCP config only at launch. If the client is restarted and still empty, run `moneybin mcp config path --client <name>` to print the resolved config path, then verify the file exists and contains a `MoneyBin` entry under `mcpServers` (or `servers` for VS Code, `[mcp_servers.<name>]` for Codex).

**Tools error with "no profile" or similar.** The install snippet embeds whichever profile was active when you ran `mcp install`. To change it, re-run with `--profile <name>`. Different profiles are added as separate entries (e.g. `MoneyBin (alice)` and `MoneyBin (bob)`); we don't auto-replace because silently losing access to a previous profile is worse than leaving it visible.

**"Database is locked" / server exits immediately.** Another process is holding the same profile's DB. Most common: (a) a desktop client is already running with the same profile installed; (b) a `moneybin` CLI command is still running in another terminal; (c) two `codex` / `gemini` / `make claude-mcp` invocations are racing. Quit the offender or switch profiles.

**"Cannot parse existing config" on install.** The target file has invalid JSON or TOML. Fix the syntax in your editor and re-run.

**Slow first call after launch.** Cold start imports the MCP runtime and loads settings. Subsequent calls in the same session reuse the connection.

**`make claude-mcp` reports "No active profile and --profile not supplied".** Either run `moneybin profile create <name>` first, or pass `PROFILE=<name>` to the make target.

## Stability

MoneyBin is pre-v1. Tool names, parameter shapes, and envelope fields may change before the first tagged release — clients with cached tool lists may need to reconnect after a MoneyBin upgrade. Once v1 lands, the MCP surface locks under the deprecation rules in the design-principles guide.
