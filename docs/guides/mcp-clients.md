# Configuring MCP Clients

This guide covers wiring MoneyBin's MCP server into AI clients. For what the server *exposes* (tools, prompts, resources), see [MCP Server](mcp-server.md).

## Quickstart

```bash
# Print a snippet (default client: claude-desktop)
moneybin mcp config generate

# Install directly into a client's config file
moneybin mcp config generate --client claude-desktop --install --yes

# Profile-aware: install for a specific MoneyBin profile
moneybin mcp config generate --client cursor --profile alice --install --yes
```

Each invocation prints the JSON or TOML snippet first; `--install` then merges it into the client's canonical config (creating the file if absent). Existing settings — including unrelated MCP servers, comments, and key ordering — are preserved.

## Supported clients

| Client | Format | Install path | Notes |
|---|---|---|---|
| `claude-desktop` | JSON `mcpServers` | `~/Library/Application Support/Claude/claude_desktop_config.json` | One server process per app, shared across all chats |
| `claude-code` | JSON `mcpServers` | `<base>/profiles/<profile>/claude-code-mcp.json` | Per-profile file; loaded only when launched with `--strict-mcp-config --mcp-config <file>` (see [Per-session opt-in](#per-session-opt-in-claude-code)) |
| `cursor` | JSON `mcpServers` | `~/.cursor/mcp.json` | One server process per Cursor instance |
| `windsurf` | JSON `mcpServers` | `~/.codeium/windsurf/mcp_config.json` | One server process per Windsurf instance |
| `vscode` | JSON `servers` (with `"type": "stdio"`) | `<repo>/.vscode/mcp.json` | Workspace-local; only loads when the workspace is open in VS Code |
| `gemini-cli` | JSON `mcpServers` | `~/.gemini/settings.json` | Auto-loads on every `gemini` invocation |
| `codex` | TOML `[mcp_servers.<name>]` | `~/.codex/config.toml` | Shared across **Codex CLI**, **Codex Desktop app**, and the **Codex IDE extension** |
| `chatgpt-desktop` | n/a (UI) | n/a | Uses the in-app **Connectors** UI; `--install` is unsupported. The command prints step-by-step setup instructions |

## Concurrency model

MoneyBin stores each profile's data in a single-writer DuckDB file. **Only one MCP server (or any other process) can hold the database open against a given profile at a time.** A second connection on the same profile fails to acquire the write lock and exits.

How that interacts with each client depends on its server-lifecycle model:

| Pattern | Clients | Behavior |
|---|---|---|
| **App-shared connection** | Claude Desktop, ChatGPT Desktop, Cursor, VS Code, Windsurf | One server process per app instance, spawned at launch and reused across all chats. Opening "New chat" doesn't start another connection. |
| **Per-invocation** | Claude Code, Codex CLI, Gemini CLI | One server process per CLI invocation. Each new shell session spawns a fresh server, and each one tries to claim the lock. |

Different *profiles* never collide — each has its own DB and lock — so you can install `MoneyBin (alice)` and `MoneyBin (bob)` side-by-side in the same client without issue. The lock fight is only between concurrent sessions on the **same** profile.

### What this means in practice

- **Pick one app-based client per profile** as the primary MoneyBin host. If you keep Claude Desktop and Cursor both running with `MoneyBin (alice)`, the second one to start will fail to load.
- **Avoid running MoneyBin CLI commands while a desktop client is using the same profile.** The CLI opens its own DB connection and will collide with whichever app is holding the lock. Quit the client first, or run the CLI against a different profile.
- **CLI clients (codex, gemini-cli) auto-load on every invocation.** If you `--install` MoneyBin into them, every `codex` or `gemini` command in any terminal will try to spawn the server. For occasional use, prefer pasting the snippet manually instead of `--install`. (Claude Code is the exception; see below.)

## Per-session opt-in (Claude Code)

Claude Code is the only client that supports a true per-launch MCP config override. You can install MoneyBin without forcing it to start in every Claude Code session.

```bash
# One-time setup
moneybin profile create <name>
moneybin mcp config generate --client claude-code --profile <name> --install --yes

# Launch a Claude Code session with MoneyBin loaded
make claude-mcp                       # uses the active profile
make claude-mcp PROFILE=<name>        # explicit profile
./scripts/claude-mcp.sh <name>        # equivalent, no Make
```

`make claude-mcp` runs `claude --strict-mcp-config --mcp-config <profile-config-path>`, which tells Claude Code to ignore every other configured MCP server and load only MoneyBin for that one session. Plain `claude` invocations don't include MoneyBin and don't take the lock.

This is the recommended setup if you do non-MoneyBin work in Claude Code on the same machine — it keeps the server (and its lock) off until you ask for it.

## Codex (CLI, Desktop, IDE)

OpenAI's Codex products share `~/.codex/config.toml`. A single `--install --client codex` covers all three:

- **Codex CLI** — `codex` command in the terminal
- **Codex Desktop app** — macOS / Windows app from [developers.openai.com/codex/app](https://developers.openai.com/codex/app)
- **Codex IDE extension** — VS Code / JetBrains

There's no per-launch override flag. Once installed, Codex auto-loads MoneyBin every time. As an alternative install path, OpenAI also documents `codex mcp add` for managing servers from the CLI; `moneybin mcp config generate --client codex --install` produces an equivalent TOML block via tomlkit (which preserves any inline comments and unrelated settings in your `config.toml`).

## ChatGPT Desktop

ChatGPT Desktop adds MCP servers through **Settings → Connectors** (Developer Mode), not a JSON config file we can write. Running:

```bash
moneybin mcp config generate --client chatgpt-desktop --profile <name>
```

prints the canonical snippet plus a numbered checklist for the Connectors UI: which fields to fill (Name, Command, Arguments, env vars), where to paste them, and a fallback for builds that only accept HTTP connectors (`moneybin mcp serve --transport streamable-http` + tunnel URL).

## Switching profiles

Re-running `--install` for a different profile **adds an additional entry** rather than replacing the previous one. So `moneybin mcp config generate --client cursor --profile alice --install` followed by `moneybin mcp config generate --client cursor --profile bob --install` leaves both `MoneyBin (alice)` and `MoneyBin (bob)` in `~/.cursor/mcp.json`. That's safe — different profiles don't collide on the lock — and lets you see all configured profiles in the client's UI.

If you'd rather have a single MoneyBin entry, edit the client's config file directly to remove the stale entry. We don't auto-replace because losing access to the previous profile silently is worse than leaving it visible.

## Troubleshooting

**"Database is locked" / MCP server exits immediately on startup.** Another process is holding the same profile's DB. Most common causes: (a) a desktop client (Claude Desktop, Cursor, VS Code) is already running with the same profile installed; (b) you ran a `moneybin` CLI command in another terminal and it's still holding the connection; (c) two `claude`/`codex`/`gemini` invocations are racing. Quit the offender or switch to a different `--profile`.

**Install command says "Cannot parse existing config".** The target file has invalid JSON or TOML. Open it in your editor, fix the syntax, then re-run `--install`.

**`make claude-mcp` reports "No active profile and --profile not supplied".** Either run `moneybin profile create <name>` first, or pass `PROFILE=<name>` to the make target.

**MCP server starts but the client doesn't see any tools.** Verify the client is actually pointing at the right config: `moneybin mcp config path --client <name> [--profile <name>]` prints the resolved install path. Cross-check that the file exists, contains a `MoneyBin` entry, and that the entry's `command` and `args` are runnable from the client's launch context.
