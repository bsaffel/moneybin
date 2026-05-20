# MoneyBin

Personal financial data platform. Python + DuckDB + SQLMesh + Typer CLI + MCP server.

## Guiding Principle: Build the Inevitable Choice

Be what `uv` and `ruff` are for Python tooling — the option a serious user
converges on because the foundation is rock-solid. Not first, not fastest to
ship. **Inevitable.** A longer development lifecycle is acceptable when it
buys durability.

Three axes, distinct from "Simplicity First" below:

- **Path selection (this principle):** For one-way-door choices (public
  contracts, security postures, critical-path deps — see
  `.claude/rules/design-principles.md` for the trigger list and launch
  trigger), default to the path that still feels right in five years,
  even if it costs months more. Pre-launch: iterate. Post-launch: lock
  hard.
- **Coherence (every change):** When adding new X, follow the existing
  pattern for X. If the pattern is wrong, fix it everywhere — don't
  introduce a parallel pattern beside it. Two patterns for the same job is
  the single largest source of codebase rot.
- **Scope discipline ("Simplicity First"):** Two-way doors (internal
  abstractions, module boundaries, refactors behind a stable contract) get
  the minimum code that solves the problem. No protocol.

**Agent protocol for one-way-door decisions:** Surface both paths. Name
each cost concretely (time, surface area, migration risk). Recommend the
durable path; state what durability costs. Use the worked example in
`.claude/rules/design-principles.md` as the output template. Wait for
explicit override before taking the fast path — or, in subagent /
autonomous-loop contexts with no user, default to durable and document
the choice for the parent. If you can't tell whether a decision is
one-way: treat it as one-way and invoke.

Heuristics, trigger list, the public-contract / internal-abstraction split,
and the coherence rule: `.claude/rules/design-principles.md`.

## Bias Toward UX / DX / AX

Better user experience, developer experience, and agent experience is
almost always the right choice. When tradeoffs are visible, default to the
path that's nicer to use, build on, or drive from an LLM — and state the
cost explicitly so the choice is informed, not silent. "Simpler to build,"
"smaller v1 surface," or "less code" are not winning arguments if the
result is meaningfully worse to use, develop against, or operate as an
agent.

The three audiences:

- **UX (end user):** the human running the CLI, reading reports, or
  driving MCP through their AI tool. Magic, predictability, recoverability.
- **DX (developer):** the contributor reading the code, writing the next
  feature, debugging a regression. Clear boundaries, named primitives,
  small focused files.
- **AX (agent):** the LLM consuming the MCP / CLI surface (Claude Code,
  Codex, Gemini CLI, hosted Claude). Tool names that telegraph intent,
  error envelopes that prescribe recovery via `actions[]`, response
  envelopes that expose what the agent needs without a follow-up call,
  taxonomy that lets the agent pick a tool confidently without
  disambiguation. MoneyBin's CLI and MCP are first-class agent surfaces
  (see [`feedback_cli_agent_surface`] memory + `.claude/rules/mcp-server.md`)
  — AX is a peer, not a poor cousin.

This is not a contradiction of "Simplicity First" (which is about scope
discipline — don't ship features that weren't asked for) or the Guiding
Principle (which is about durable path selection). It's a tiebreaker:
when two viable paths exist and one has noticeably better ergonomics for
any of the three audiences, take the ergonomics. The durable choice is
usually also the one that feels good five years later — for all three.

**Agent protocol:** When presenting options, lead with the better-XX path
as the recommendation. Name what the XX advantage actually buys, per
audience where relevant — one-click vs. 15 min of console clicking (UX);
one named primitive vs. two parallel patterns (DX); one tool the agent
picks confidently vs. two it disambiguates between (AX). Name the cost
honestly (verification process, more code, larger blast radius). Default
to the better-XX path unless the cost is genuinely disqualifying — and
"more work for me" is not disqualifying. Reinforces the
`.claude/rules/agent-experience.md` report's "what would have made this
easier" question — answer it the first time, don't wait for the AX
report. When XX trades against XX (e.g., AX wants verbose response
envelopes, UX wants terse CLI output), surface the conflict and let the
user pick.

## Think Before Coding
Don't assume. Don't hide confusion. Surface tradeoffs.

### Before implementing
State your assumptions explicitly. If uncertain, ask.
If multiple interpretations exist, present them - don't pick silently.
If a simpler approach exists, say so. Push back when warranted.
If something is unclear, stop. Name what's confusing. Ask.

## Simplicity First
Minimum code that solves the problem. Nothing speculative.

No features beyond what was asked.
No abstractions for single-use code.
No "flexibility" or "configurability" that wasn't requested.
No error handling for impossible scenarios.
If you write 200 lines and it could be 50, rewrite it.
Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## Design Philosophy

- **Sync server is opaque.** The client communicates only with moneybin-server's API surface. External service providers are implementation details hidden behind the server.

## Critical Rules

- **Package manager**: `uv` only. Never `pip install`, `uv pip install`, or `python -m`.
- **Linting/formatting**: `make format && make lint` (Ruff, line length 88).
- **Type checking**: `uv run pyright` on modified files (not mypy).
- **Tests**: Dev: `uv run pytest tests/path/to/test_file.py -v`. Pre-commit: `make test`. Always `uv run pytest` — never `uv run python -m pytest` (sandbox-denied per the `python -m` ban above). If `uv run pytest` resolves to the wrong interpreter, the venv has stale shebangs from a worktree move; fix with `uv sync --reinstall` rather than working around it via `python -m`.
- **Pre-commit checklist**: `make check test` — format, lint, type-check, tests. Run once before committing.
- **SQL formatting**: `uv run sqlmesh -p sqlmesh format`.
- **Check library docs first**: Before implementing patterns with SQLMesh, DuckDB, Pydantic, etc., verify the correct API in official docs. Training knowledge may be outdated.

## Key Abstractions

| Need | Use | Never |
|------|-----|-------|
| Database access | `get_database()` → `Database` | `duckdb.connect()` |
| Configuration | `get_settings()` → `MoneyBinSettings` | `os.getenv()`, hardcoded values |
| Secrets/keys | `SecretStore` | `os.getenv()`, plain `str` fields |
| Table references | `from moneybin.tables import FCT_TRANSACTIONS`, etc. | Hardcoded table name strings |
| DataFrames | DuckDB > Polars > Pandas | Pandas (unless required for library compat — document why) |

## Code Standards

- **Logging**: `logger = logging.getLogger(__name__)`. Always f-strings — never `%s`/`%d` lazy formatting (bypasses `SanitizedLogFormatter`).
- **Inline SQL**: Triple-quoted strings (`"""..."""`).
- **Suppression comments**: Always include a reason: `# noqa: S608  # test input, not executing SQL`.
- **Acronyms**: ALL CAPS in class names: `OFXExtractor`, `CSVReader`, `PDFExtractor`.
- **Comments and docstrings**: Default to one short line. A longer comment or
  multi-paragraph module docstring is warranted when it documents a
  *non-obvious why* a future reader would otherwise undo — a workaround for an
  upstream bug, a hidden constraint, a platform-specific quirk, or an
  invariant the code relies on but doesn't enforce. Don't restate what the
  code already says; do explain context that lives outside the code.

## Architecture: Data Layers

| Layer | Schema | Materialized | Purpose |
|-------|--------|-------------|---------|
| Raw | `raw` | Table | Untouched data from loaders (Python) |
| Staging | `prep` | View | Light cleaning, type casting (SQLMesh `stg_*`) |
| Core | `core` | Table / View | Canonical, deduplicated, multi-source (`fct_*`, `dim_*`, `bridge_*`) |
| App | `app` | Table | User-state and application-managed metadata (mutable; not derivable from raw) |
| Reports | `reports` | View | Curated presentation models, one per CLI/MCP report |

Full schema reference (including `meta`, `seeds`, `synthetic`, prefix conventions, layer rules, and consumer access patterns): [`architecture-shared-primitives.md`](docs/specs/architecture-shared-primitives.md).

1. **One canonical table per entity** — `dim_accounts`, `fct_transactions`, etc. Consumers read from `core` and `reports` only.
2. **Multi-source union** — Core models `UNION ALL` from every staging source with `source_type` column.
3. **Dedup in core** — `ROW_NUMBER()` windows for duplicates; mapping tables for cross-source dedup.
4. **Accounting sign convention** — negative = expense, positive = income. `DECIMAL(18,2)` for amounts, `DATE` for dates.
5. **Source-agnostic consumers** — MCP server, CLI use `moneybin.tables` constants, never source-specific logic.

## Specs & Implementation Tracking

Feature specs live in `docs/specs/`. The **[Spec Index](docs/specs/INDEX.md)** is the single source of truth.

- **Before implementing**, check `INDEX.md` for existing specs.
- **When starting**, update status to `in-progress` (spec file + `INDEX.md`).
- **When complete**, update to `implemented`. See `.claude/rules/shipping.md` for README updates.
- **Observability wiring**: Specs touching app code must include metrics. See `docs/specs/observability.md` and `src/moneybin/metrics/registry.py`.
- Statuses: `draft` → `ready` → `in-progress` → `implemented`.

## Plans vs Specs

Specs (intent, durable) live in `docs/specs/` and are tracked. Implementation plans (step-by-step scaffolding from `superpowers:writing-plans` and similar) are ephemeral and **do not belong in the repo** — write them to `private/plans/` (gitignored). From a worktree, that is `../../private/plans/`. Before discarding a plan, lift any durable design rationale into the relevant spec or an ADR.

## Configuration

All config in `src/moneybin/config.py` — one `MoneyBinSettings` root via Pydantic Settings. Never hardcode paths, credentials, or tunable parameters. Env vars use `MONEYBIN_` prefix with `__` for nesting: `MONEYBIN_DATABASE__PATH`.

## Constants

Security-critical parameters (crypto cost factors, key lengths, salt sizes) defined once — module-level `_CONSTANTS` or config fields. Never duplicate across call sites.

## Security

- **Encryption at rest**: AES-256-GCM on all DuckDB databases. See [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md).
- **No PII or financial data in logs.** Log record counts, IDs, and status codes only.
- **Parameterized SQL** with `?` placeholders. See `.claude/rules/security.md` for full standards.

## Rules Index

Files in `.claude/rules/` auto-load via Claude Code's `paths:` frontmatter — path-scoped rules load when Claude reads a matching file; unscoped rules load every session. The table below is for discoverability when planning work that hasn't touched matching files yet. Read a rule directly if you need it before editing.

### Path-scoped

| Rule | Covers |
|------|--------|
| `security.md` | SQL injection, input validation, XSS, PII, exception wrapping |
| `database.md` | DuckDB patterns, SQL conventions, schema, column comments |
| `mcp-server.md` | Tool taxonomy, response envelope, sensitivity tiers, services |
| `cli.md` | Typer patterns, error handling, command registration, icons |
| `testing.md` | Pytest patterns, fixtures, mocking strategy, DB test helpers |
| `data-extraction.md` | Incremental sync, dedup, parameter design, new data sources |
| `identifiers.md` | Content hashes, truncated UUIDs, source IDs, semantic slugs |
| `documentation.md` | Diagram conventions (Mermaid over ASCII) |

### Always loaded (workflow rules)

| Rule | Covers |
|------|--------|
| `design-principles.md` | Durable path selection: heuristics for "inevitable choice" decisions and the trigger list for the agent protocol |
| `surface-design.md` | Cross-surface (MCP / CLI / REST) operation-shape taxonomy, verb vocabulary, audience layering. Consult before adding, renaming, or restructuring a tool/command/endpoint |
| `shipping.md` | Post-implementation checklist: README updates, roadmap icons, `/simplify` pre-push pass |
| `branching.md` | Branch prefix → PR label mapping, commit message style |
| `sandboxing.md` | Bash invocation patterns: single commands, allowlisted pipelines, structured-output filtering, policy denials |
| `agent-experience.md` | Required agent-experience report whenever you interact with MoneyBin's MCP server in a session |
