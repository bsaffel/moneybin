# MoneyBin

Personal financial data platform. Python + DuckDB + SQLMesh + Typer CLI + MCP server.

## Guiding Principle: Build the Inevitable Choice

Be the option a serious user converges on because the foundation is
rock-solid — inevitable, not first or fastest. A longer development
lifecycle is acceptable when it buys durability.

Three axes, distinct from "Simplicity First" below:

- **Path selection (this principle):** For one-way-door choices (public
  contracts, security postures, critical-path deps), default to the path
  that still feels right in five years, even if it costs months more.
  Pre-launch: iterate. Post-launch: lock hard.
- **Coherence (every change):** When adding new X, follow the existing
  pattern for X. If the pattern is wrong, fix it everywhere — don't
  introduce a parallel pattern beside it. Two patterns for the same job is
  the single largest source of codebase rot.
- **Scope discipline ("Simplicity First"):** Two-way doors (internal
  abstractions, module boundaries, refactors behind a stable contract) get
  the minimum code that solves the problem. No protocol.

**Agent protocol (one-way doors):** surface both paths, name each cost
concretely, recommend the durable path and state what it costs; wait for
explicit override before taking the fast path. With no user (subagent /
autonomous), default to durable and document the choice. If you can't tell
whether a decision is one-way, treat it as one-way. Full protocol, trigger
list, worked example, and the public-contract / internal-abstraction split:
`.claude/rules/design-principles.md`.

## Bias Toward UX / DX / AX

When two viable paths exist and one is noticeably nicer for the **user**
(UX — the human running the CLI, reading reports, driving MCP), the
**developer** (DX — the contributor reading and extending the code), or
the **agent** (AX — the LLM driving the MCP / CLI surface), take the
ergonomics and name the cost explicitly. "Simpler to build," "smaller v1
surface," or "less code" don't win if the result is meaningfully worse to
use, build on, or operate as an agent. AX is a peer, not a poor cousin.
This is a tiebreaker between viable paths — not a contradiction of
Simplicity First (scope discipline) or the Guiding Principle (durable path
selection), and not a license to gold-plate.

**Agent protocol:** lead with the better-ergonomics path; name what it buys
per audience (one-click vs. 15 min of clicking; one named primitive vs. two
parallel patterns; one tool the agent picks confidently vs. two it
disambiguates) and name the cost honestly ("more work for me" is not
disqualifying). When audiences conflict (e.g. AX wants verbose envelopes,
UX wants terse output), surface it and let the user pick.

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

- **Sync server is opaque.** The client communicates only with moneybin-sync's API surface. External service providers are implementation details hidden behind the server.

## Design System

MoneyBin's visual language lives in `design-system/` — the source of truth (it also backs the `/moneybin-design` skill and the synced claude.ai/design project). Before any UI, artifact, or frontend work, read `design-system/readme.md`, `design-system/tokens/`, and `design-system/guidelines/`. Non-negotiables: dark theme leads; brass (`--accent-brass`) is the only accent, never blue; money is always JetBrains Mono via the `Amount` component, with explicit +/− signs on income/expense flows (balances stay unsigned); hairline borders, no resting shadows; every data widget carries a SQL provenance chip; linear chart interpolation only; no emoji, no exclamation points.

## Critical Rules

- **Package manager**: `uv` only. Never `pip install`, `uv pip install`, or `python -m`.
- **Linting/formatting**: `make format && make lint` (Ruff, line length 88).
- **Type checking**: `uv run pyright` on modified files (not mypy).
- **Tests**: Dev `uv run pytest <path> -v`; pre-commit `make test`. Always `uv run pytest`; wrong interpreter → `uv sync --reinstall`.
- **Pre-commit checklist**: `make check test` — format, lint, type-check, tests. Run once before committing.
- **SQL formatting**: `make format-sql` (sets `MAX_FORK_WORKERS=1`; the bare `uv run sqlmesh -p sqlmesh format` forks a worker pool the encrypted-DB design disallows and the sandbox blocks).
- **Check library docs first**: Before implementing patterns with SQLMesh, DuckDB, Pydantic, etc., verify the correct API in official docs. Training knowledge may be outdated.

## Key Abstractions

| Need | Use | Never |
|------|-----|-------|
| Database access | `get_database()` → `Database` | `duckdb.connect()` |
| Configuration | `get_settings()` → `MoneyBinSettings` | `os.getenv()`, hardcoded values |
| Secrets/keys | `SecretStore` | `os.getenv()`, plain `str` fields |
| Table references | `from moneybin.tables import FCT_TRANSACTIONS`, etc. | Hardcoded table name strings |
| Protected `app.*` mutation | compose a `*Repo` (`src/moneybin/repositories/`) | raw `INSERT`/`UPDATE`/`DELETE` in a service (Invariant 10) |
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
- **Verify the spec against the code before building** — specs (especially `draft` ones) can describe a model that was never built or has drifted. Reconcile spec ↔ code at the `draft → ready` promotion, not mid-implementation.
- **When starting**, update status to `in-progress` (spec file + `INDEX.md`).
- **When complete**, update to `implemented`. See `.claude/rules/shipping.md` for README updates.
- **Observability wiring**: Specs touching app code must include metrics. See `docs/specs/observability.md` and `src/moneybin/metrics/registry.py`.
- Statuses: `draft` → `ready` → `in-progress` → `implemented`.

## Plans vs Specs

Specs (intent, durable) live in `docs/specs/` and are tracked. Implementation plans (e.g. from `superpowers:writing-plans`) are ephemeral — write them to `private/plans/` (gitignored; `../../private/plans/` from a worktree), never the repo. Before discarding one, lift durable design rationale into a spec or ADR.

## Configuration

All config in `src/moneybin/config.py` — one `MoneyBinSettings` root via Pydantic Settings. Never hardcode paths, credentials, or tunable parameters. Env vars use `MONEYBIN_` prefix with `__` for nesting: `MONEYBIN_DATABASE__PATH`.

## Constants

Security-critical parameters (crypto cost factors, key lengths, salt sizes) defined once — module-level `_CONSTANTS` or config fields. Never duplicate across call sites.

## Security

- **Encryption at rest**: AES-256-GCM on all DuckDB databases. See [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md).
- **No PII or financial data in logs.** Log record counts, IDs, and status codes only.
- **Parameterized SQL** with `?` placeholders. See `.claude/rules/security.md` for full standards.

## Rules Index

Files in `.claude/rules/` auto-load via `paths:` frontmatter — path-scoped load on matching-file reads, unscoped load every session. This table aids discoverability before you've touched a matching file; read a rule directly if you need it sooner.

### Path-scoped

| Rule | Covers |
|------|--------|
| `security.md` | SQL injection, input validation, XSS, PII, exception wrapping |
| `database.md` | DuckDB patterns, SQL conventions, schema, column comments |
| `mcp.md` | Tool taxonomy, response envelope, sensitivity tiers, services |
| `cli.md` | Typer patterns, error handling, command registration, icons |
| `testing.md` | Pytest patterns, fixtures, mocking strategy, DB test helpers |
| `data-extraction.md` | Incremental sync, dedup, parameter design, new data sources |
| `identifiers.md` | Content hashes, truncated UUIDs, source IDs, semantic slugs |
| `documentation.md` | Diagram conventions (Mermaid over ASCII) |
| `shipping.md` | Post-implementation checklist (CHANGELOG, roadmap, features, README, INDEX) — loads when editing those |
| `surface-design.md` | Cross-surface operation-shape taxonomy, verb vocabulary, audience layering — loads when touching mcp/cli/services code or the moneybin-mcp/cli/capabilities + mcp-architecture specs |

### Always loaded (workflow rules)

| Rule | Covers |
|------|--------|
| `design-principles.md` | Durable path selection: heuristics for "inevitable choice" decisions, the agent-protocol trigger list, and the milestone addressing scheme (`M{phase}{letter}.{n}` — append, don't reinvent) |
| `branching.md` | Branch prefix → PR label mapping, commit message style |
| `sandboxing.md` | Bash invocation patterns: single commands, allowlisted pipelines, structured-output filtering, policy denials |
| `agent-experience.md` | Required agent-experience report whenever you interact with MoneyBin's MCP server in a session |
