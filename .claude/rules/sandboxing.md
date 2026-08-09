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

`Read` is sandbox-independent and avoids the bash command-string matcher entirely; it also takes `offset`/`limit` for large files. Reserve `cat` for cases needing shell interpretation: piping, multi-file concatenation, heredocs. To find files by pattern use `Glob`/`Grep`, then `Read` the specific paths.

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

To find specific items in tool output, use the tool's own filtering before grep — single command, sandbox-eligible, denser and more reliably parsed:

- `ruff check --output-format json` (or `concise`)
- `pyright --outputjson`
- `pytest --tb=short -q`
- `gh api ... --jq '.field'`
- `git log --pretty=format:'%h %s'`

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

## Known cases — don't re-derive these

Each of these has already cost a debugging detour. The first seven are failures that **look** like they need a bypass and do not.

| Case | What actually happens | What to do |
|---|---|---|
| `gh auth status` fails in-sandbox | Cosmetic. `GH_TOKEN` comes from the keychain wrapper and resolves for real API calls. | Ignore the status output; judge by whether the API call worked. |
| `gh run view --log-failed` | Works in-sandbox — the `gh` cache path is allowlisted. | Read CI failure logs directly; no bypass. |
| Bare `git push` after the sandbox blocked the `-u` write | Reports success and **no-ops**. | Push `HEAD:<branch>` explicitly, then verify `origin/<branch>` actually moved. |
| `git branch -m` on a fresh branch → `could not lock config file .git/config` | The ref rename **succeeded**; only the (nonexistent) upstream-config write failed. | Confirm with `git symbolic-ref HEAD` and `git worktree list`. Don't retry unsandboxed. |
| `git checkout -b <new> origin/main` → same `could not lock config file` | Worse than the rename case: the branch ref **is** created and the worktree **is** updated, but HEAD stays on the old branch, so `git status` shows the whole base-vs-old-branch delta as staged. Nothing is lost; the pointer just didn't move. | Just `git checkout <new>` — plain, no `-b`. The branch already exists, so this writes no config and moves HEAD correctly (verified). Don't `git reset --hard`, and don't retry `checkout -b`. If plain checkout also fails, fall back to `git symbolic-ref HEAD refs/heads/<new>` plus a bare `git reset`. Either way the branch has **no upstream** (`git rev-parse @{upstream}` → `fatal: no upstream configured`) — that is the denied write, and it is unrestorable in-sandbox. Harmless for the agent path only: the shipping skills push `HEAD:refs/heads/<branch>` explicitly and are forbidden from `-u`. The human flow is not the same — `CONTRIBUTING.md:157-161` tells contributors to run `git push -u origin <branch>` — so before handing the branch back, run exactly that outside the sandbox: `git push -u origin <branch>` sets upstream whether or not the branch has already been pushed, because `-u` applies to every branch "up to date or successfully pushed". Don't reach for `git branch --set-upstream-to=origin/<branch>` instead — in this scenario the remote counterpart does not exist yet, so it exits 128 with `fatal: the requested upstream branch 'origin/<branch>' does not exist`. |
| `failed to store: -60008` printed by `git fetch` / `git push` / `git ls-remote` | Cosmetic — same denied keychain **write** as the row below, from the credential helper caching the token. The remote operation itself succeeds and prints its normal result on the next line. | Ignore it; judge by the command's actual output. It does **not** excuse skipping the `git ls-remote origin <ref>` check that the bare-`git push` row above requires — that row is about a real no-op, this one is noise. |
| `moneybin demo` / `accounts links run` → `PasswordSetError: Can't store password on keychain: (-60008)` | The keychain **write** is denied. Reads are unaffected and `moneybin db backup` completes normally (34.5 MB written), so only commands that store a secret fail. | Not an allowlist problem. For `demo`, supply the in-memory keyring: `PYTHON_KEYRING_BACKEND=tests.e2e.memory_keyring.MemoryKeyring PYTHONPATH="$PWD" uv run moneybin demo …`. For `accounts links run`, go via MCP — the server holds its own keyring session. |
| Compound bash — several `&&`/`;` statements or subshells | Defeats the static analyzer and prompts even when every component is allowlisted. | One statement per call, or move the logic into a single `python3` / `uv run python` helper. |
| Bare `uv run sqlmesh -p … format` | Forks a worker pool the encrypted-DB design disallows. | `make format-sql` (sets `MAX_FORK_WORKERS=1`). |
| Reading repos under `~/Workspace/` other than `moneybin*` (sibling clones, external projects) | Outside the read allowlist. | Genuine one-off bypass — quote the denied path first. |
| `moneybin-sync` tests | testcontainers needs the Docker socket, outside the allowlist. | Genuine one-off bypass. |
| `~/Documents/MoneyBin/` → `Operation not permitted` | macOS TCC, **not** the sandbox. | A bypass won't help; grant Terminal/Claude Code Documents access. |
