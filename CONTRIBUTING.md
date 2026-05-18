<!-- Last reviewed: 2026-05-17 -->
# Contributing to MoneyBin

Thanks for helping out. This file gets you to a landed change without a reading
detour. For project conventions and the bigger picture, see [`AGENTS.md`](AGENTS.md).

MoneyBin is pre-v1. Public surfaces — CLI commands, MCP (Model Context
Protocol — the protocol your AI assistant speaks to local tools) tools, and
the DuckDB schema — may rename or break between merges; the `CHANGELOG.md`
records every rename. If you're touching one of those surfaces, expect a
review turnaround on the order of days, not hours.

## Quick start

Requires Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/bsaffel/moneybin.git
cd moneybin
make setup        # venv + lockfile sync + pre-commit hooks
make check test   # format, lint, type-check, unit tests
```

If `make test` is green, your environment is good.

**No external accounts required for the dev loop.** `make test` runs without
Plaid credentials, without an unlocked profile keychain, and without network
access — it uses synthetic fixtures and a per-worker `MONEYBIN_HOME` tempdir
(see `tests/conftest.py`). `make test-all` adds the integration, e2e, and
scenario tiers, which spin up real DuckDB + SQLMesh against synthetic data
but still need no external services.

## Project shape

MoneyBin is a Python + DuckDB + SQLMesh + Typer + MCP application. Code lives
under `src/moneybin/`; SQL transformations under `sqlmesh/`; tests under
`tests/`. Read these as you need them:

- [`AGENTS.md`](AGENTS.md) — critical rules, key abstractions, code standards.
- [`docs/architecture.md`](docs/architecture.md) — system shape and data layers.
- [`docs/specs/INDEX.md`](docs/specs/INDEX.md) — every feature spec and its status.
- [`.claude/rules/`](.claude/rules/) — per-domain rules (security, database,
  MCP, CLI, testing, data-extraction, identifiers).

## Workflow

### 1. Branch

Use the prefix that matches your change. See [`.claude/rules/branching.md`](.claude/rules/branching.md)
for the full table; the prefix becomes the PR label.

```
feat/   fix/   docs/   refactor/   chore/
deps/   ci/    security/  test/   perf/
```

Example: `feat/plaid-incremental-sync`.

### 2. Decide whether you need a spec

If your change **adds or renames a CLI command, MCP tool, or schema column**,
write a one-page spec first under `docs/specs/<name>.md`, register it in
[`docs/specs/INDEX.md`](docs/specs/INDEX.md) as `draft`, and link it from the
PR description. Existing specs in `docs/specs/` are the format reference —
one page is enough for most cases.

Bug fixes, internal refactors, doc updates, and test additions don't need a
spec. Open the PR directly.

### 3. Write the change

Find the rule file for the domain you're touching and follow it:

| Touching... | Read |
|---|---|
| MCP tools or resources | [`.claude/rules/mcp-server.md`](.claude/rules/mcp-server.md) |
| Typer commands | [`.claude/rules/cli.md`](.claude/rules/cli.md) |
| DuckDB / SQL / schemas | [`.claude/rules/database.md`](.claude/rules/database.md) |
| Loaders, extractors, sync | [`.claude/rules/data-extraction.md`](.claude/rules/data-extraction.md) |
| IDs, hashes, slugs | [`.claude/rules/identifiers.md`](.claude/rules/identifiers.md) |
| Anything that touches user data | [`.claude/rules/security.md`](.claude/rules/security.md) |
| Tests | [`.claude/rules/testing.md`](.claude/rules/testing.md) |

#### Adding a new MCP tool

The shape is the same for every tool — pick an existing one in the same
domain and match it.

1. **Pick a domain file.** `src/moneybin/mcp/tools/<domain>.py`. Read an
   existing tool in that file to match the shape; create a new file only if
   no domain fits.
2. **Build the service first.** Put the business logic in
   `src/moneybin/services/` returning a typed dataclass. The tool is a thin
   wrapper around the service.
3. **Decorate the tool.** `@mcp_tool(sensitivity="<tier>", domain="<domain>", read_only=<bool>)`.
   Sensitivity tiers and the response-envelope contract are in
   [`.claude/rules/mcp-server.md`](.claude/rules/mcp-server.md).
4. **Return a `ResponseEnvelope`.** Use the helpers in
   `src/moneybin/protocol/envelope.py`. Never return a bare dict.
5. **Add a CLI peer.** Every MCP tool needs a CLI command that produces the
   same data with `--output json`. See `src/moneybin/cli/commands/` and
   [`.claude/rules/cli.md`](.claude/rules/cli.md).
6. **Tests:** unit test on the service; integration test on the MCP tool;
   E2E test on the CLI peer including a `--output json` parity check. See
   the "Mock Boundaries" and "Test Coverage by Layer" sections of
   [`.claude/rules/testing.md`](.claude/rules/testing.md).
7. **Hand-test against your IDE.** Run `moneybin mcp serve` and connect from
   Claude Desktop / Cursor via `moneybin mcp install --client <name>`. See
   [`docs/guides/mcp-clients.md`](docs/guides/mcp-clients.md).

#### Adding a new CLI command

1. **Pick a command group.** `src/moneybin/cli/commands/<group>/`. Match the
   naming convention in [`.claude/rules/cli.md`](.claude/rules/cli.md) —
   subgroup commands are `<group>_<verb>`.
2. **Use the standard flags and exit codes.** `--output {table,json}` for any
   command that returns data; the JSON envelope contract is the same one
   MCP tools use.
3. **Tests:** unit test argument parsing and exit codes; E2E subprocess test
   in the appropriate tier file (see [`.claude/rules/testing.md`](.claude/rules/testing.md)
   "E2E Test Coverage Requirement"). Every CLI command needs an E2E test
   unless it's interactive-only (`db shell`, `db ui`).

### 4. Test

```bash
make test                                       # fast unit loop
uv run pytest tests/path/to/test_file.py -v    # single file
make test-all                                   # unit + integration + e2e
make test-scenarios                             # whole-pipeline scenarios
```

Scenario expectations must be derived from the fixture, persona config, or
hand-authored ground truth — never observe-and-paste from program output.
The full rule is in [`.claude/rules/testing.md`](.claude/rules/testing.md).

### 5. Pre-commit gate

```bash
make check test
```

Runs Ruff format, Ruff lint, Pyright, and unit tests. CI runs the same checks
plus integration, e2e, and scenarios. SQL changes also need
`uv run sqlmesh -p sqlmesh format`.

### 6. Commit and PR

Imperative-mood subject under 72 chars; body explains *why*, not which files
changed. No `Co-Authored-By: Claude` or similar AI-attribution trailers —
see [`.claude/rules/branching.md`](.claude/rules/branching.md).

```bash
git add <specific files>     # prefer over `git add -A`
git commit
git push -u origin <branch>
gh pr create
```

CI must pass before merge. Commits land as authored (no mandatory squash);
force-pushes during review are fine for fixups.

### 7. After it ships

If the change is user-visible, update the docs surface per
[`.claude/rules/shipping.md`](.claude/rules/shipping.md):

- Add a line to `CHANGELOG.md` under `Unreleased`. New MCP tools and new CLI
  commands go under `Added`; renames and behavior changes go under `Changed`.
- Move the spec to `implemented` in both the spec file and
  [`docs/specs/INDEX.md`](docs/specs/INDEX.md).
- Update [`docs/roadmap.md`](docs/roadmap.md) and
  [`docs/features.md`](docs/features.md) if the capability surface changed.
- Add or update a guide under `docs/guides/` for new user-facing features.

Internal refactors, CI tweaks, and code-style changes don't need a CHANGELOG
entry.

## Specs and roadmap

Feature work is tracked in [`docs/specs/`](docs/specs/) with statuses
`draft` → `ready` → `in-progress` → `implemented`.
[`docs/specs/INDEX.md`](docs/specs/INDEX.md) is the single source of truth —
check it before writing a new spec. Milestone status lives in
[`docs/roadmap.md`](docs/roadmap.md).

## Repo conventions

- **Licensing.** MoneyBin is AGPL-3.0; your contributions are licensed under
  AGPL-3.0 by submission. No CLA, no DCO. See [`LICENSE`](LICENSE) and
  [`docs/licensing.md`](docs/licensing.md).
- **Commits land as authored.** No mandatory squash. Keep the history
  readable.
- **Dependabot.** Grouped PRs weekly; a human reviews and merges.

## What we don't do

- **No pre-merge AI-generated test bodies you haven't read.** AI assistance is
  fine; landing code or tests you don't understand is not.
- **No skipping hooks.** If a hook fails, fix the cause; don't pass
  `--no-verify`.
- **No secrets in commits.** Never stage `.env`, credentials, or local DBs.

## Getting help

- [`README.md`](README.md) — what MoneyBin is and what it does today.
- [`AGENTS.md`](AGENTS.md) — project conventions, key abstractions, full rule
  index.
- [`.claude/rules/`](.claude/rules/) — per-domain detail when a rule file is
  referenced above.

Open an issue if you're stuck on setup or scope.
