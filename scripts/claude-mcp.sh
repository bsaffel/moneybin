#!/usr/bin/env bash
# Launch Claude Code with the MoneyBin MCP server for a single session.
#
# Resolves the per-profile claude-code-mcp.json path, verifies prerequisites,
# then execs `claude --strict-mcp-config --mcp-config <path>` so the user's
# shell becomes the controlling process of the TUI. This must run from a
# real shell (not as a Make recipe) so claude can grab the TTY directly.
#
# Usage:
#   scripts/claude-mcp.sh                  # uses active MoneyBin profile
#   scripts/claude-mcp.sh <profile-name>   # uses a specific profile
#   PROFILE=<name> scripts/claude-mcp.sh   # same, via env

set -u

profile="${1:-${PROFILE:-}}"
profile_arg=()
if [[ -n "$profile" ]]; then
  profile_arg=(--profile "$profile")
fi

err=$(mktemp)
trap 'rm -f "$err"' EXIT

config_path=$(uv run --quiet moneybin mcp config path --client claude-code "${profile_arg[@]}" 2>"$err")
rc=$?

if [[ $rc -ne 0 || -z "$config_path" ]]; then
  echo "❌ Could not resolve MoneyBin MCP config path." >&2
  cat "$err" >&2
  echo "" >&2
  echo "Hint: moneybin profile create <name>   (if no profile exists)" >&2
  echo "      moneybin mcp config generate --client claude-code --install --yes --profile <name>" >&2
  echo "      $0 <name>   (or PROFILE=<name> make claude-mcp)" >&2
  exit 1
fi

if [[ ! -f "$config_path" ]]; then
  echo "❌ MoneyBin MCP config not found at $config_path." >&2
  echo "Run: moneybin mcp config generate --client claude-code --install --yes${profile:+ --profile \"$profile\"}" >&2
  exit 1
fi

if ! command -v claude >/dev/null 2>&1; then
  echo "❌ 'claude' CLI not found on PATH." >&2
  echo "Install Claude Code first: https://docs.claude.com/en/docs/claude-code" >&2
  exit 1
fi

echo "🚀 Launching Claude Code with MoneyBin MCP ($config_path)"
exec claude --strict-mcp-config --mcp-config "$config_path"
