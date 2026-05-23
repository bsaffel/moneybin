---
description: "Bash invocation patterns: single commands, allowlisted pipelines, structured-output filtering, policy denials"
---

# Sandboxed Bash Patterns

Shape bash invocations to run silently and efficiently in this project's sandbox + permission setup.

**Precondition:** these patterns assume the OS sandbox is **enabled** (`sandbox.enabled: true` with `autoAllowBashIfSandboxed: true` in `.claude/settings.local.json`). When the sandbox is off, commands fall through to the permission allow/deny flow rather than auto-approving — and the allow/deny rules, not the sandbox, become the only boundary.

**Editing this config:** before changing `.claude/settings*.json` (permissions, sandbox, or hooks), state in chat exactly what the change does and why. A `PreToolUse` guard forces an approval prompt on every edit to those files — the up-front narration is what makes Brandon's approval informed rather than rote. The guard is intentionally an `ask`, not a deny, so agent-assisted edits stay possible.

## Single commands sandbox automatically

Single bash commands — including those with arguments, globs, brace expansion, or stdin heredocs — run inside the OS sandbox and auto-approve without prompting:

```bash
grep PATTERN src/**/*.py
cat src/{cli,config}.py
cat <<EOF > /tmp/msg.txt
```

Prefer this shape when the work fits.

## Use the Read tool for file content, not bash `cat`

For reading a file into context, use the `Read` tool — not `cat`. `Read` takes a single absolute path (e.g. `Read(/Users/.../src/foo.py)`), is sandbox-independent, supports `offset`/`limit` for large files, and avoids the bash command-string matcher entirely. Reserve `cat` for cases that genuinely need shell interpretation: piping into another command, multi-file concatenation, or building files via heredoc. To find files by pattern, use `Glob` (or `Grep`), then `Read` the specific paths.

## Pipelines and chains run silently when components are allowlisted

The project allowlist covers common downstream filters: `head`, `tail`, `grep` (only when the searched paths are inside the project directory — outside paths are denied by the sandbox), `wc`, `jq`, `sort`, `uniq`, `awk`, `sed`, `cut`, `tee`. Pipelines and `&&`/`||`/`;` chains around these pass without prompts and save context tokens vs. reading full output:

```bash
make test 2>&1 | tail -100
ruff check src/ | grep D107
gh api repos/x/y/issues | jq '.[].title'
git add src/ && git commit -m "subject" -m "body"
```

If a pipeline prompts, it usually means one component (or a path it touches) isn't covered. Surface that to the user; don't restructure the workflow to avoid it.

## Prefer tool-native structured output over regex filtering

When the goal is "find specific items in tool output," reach for the tool's own filtering before grep. Single command, sandbox-eligible, denser output, more reliable to parse:

- `ruff check --output-format json` or `--output-format concise` instead of `ruff | grep ...`
- `pyright --outputjson` instead of piping pyright through grep
- `pytest --tb=short -q` for compact failure summaries
- `gh api ... --jq '.field'` instead of piping gh output through jq
- `git log --pretty=format:'%h %s'` instead of piping git log through awk/cut

This is also a token savings: structured output is denser than verbose text + filter.

## Don't reach for these

These are policy denials, not shape problems — they'll be blocked regardless of how you write them:

- `python -m foo`, `pip install` — use `uv run foo` per AGENTS.md.
- `HOME=/tmp git ...`, `GIT_CONFIG_GLOBAL=/dev/null git ...` and similar env-var prefix workarounds — these paper over real configuration read errors. Surface the underlying error to the user rather than working around it.
- `dangerouslyDisableSandbox` — see the next section for the protocol. Never reach for it without first identifying the exact denied path and proposing an allowlist fix.

## Reaching for `dangerouslyDisableSandbox`

The sandbox is a guard, not an obstacle. Default to fixing the policy, not bypassing it.

**Before bypassing the sandbox, always:**

1. **Quote the exact denied path** from the error (e.g., `Operation not permitted: <path>`). If you can't quote a specific path from the failure output, you don't know the sandbox is the cause — re-read the error before assuming it.
2. **Propose what would let it run safely inside the sandbox** — typically the smallest allowlist addition (read/write/network) that closes the failure. Never propose adding sensitive paths (`~/.ssh/`, `~/.bashrc`, credential files, `.env*`, `.claude/settings*.json`, `.claude/skills/*`).
3. **Then** ask Brandon for one of:
   - "Add `<path>` to the sandbox write allowlist" — preferred, durable fix.
   - "Run this one command unsandboxed: `<cmd>`" — only when the allowlist change would be too broad, too sensitive, or genuinely one-off.

**Exception — `rm`:** `rm` traversals routinely touch paths outside the write-allowlist; sandboxing them adds friction without value. Use `dangerouslyDisableSandbox` for `rm` without the protocol.

**Hook failures are NOT an allowlist problem.** When a pre-commit / lint hook (`end-of-file-fixer`, `trailing-whitespace`) fails because it touched a denied path (`.claude/settings.json`, `.claude/skills/*.md`, `.env*`), the deny is intentional. Two real fixes:
- Scope the hook to exclude the denied directory in its config (e.g., `.pre-commit-config.yaml` `exclude:`).
- For an intentional edit to a guarded file, accept the approval prompt the guard raises.

`--no-verify` is the current escape hatch but skips ALL hooks, including the legitimate ruff/pyright ones. Prefer scoping the offending hook over bypassing the whole chain.
