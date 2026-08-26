# MoneyBin

Personal financial data platform. Python + DuckDB + SQLMesh + Typer CLI + MCP server.

## Guiding Principle: Build the Inevitable Choice

Be the option a serious user converges on because the foundation is
rock-solid — inevitable, not first or fastest. A longer development
lifecycle is acceptable when it buys durability.

Three axes, distinct from "Simplicity First" below: **path selection**
(one-way doors), **coherence** (every change), **scope discipline**
(two-way doors). If you can't tell whether a decision is one-way, treat it
as one-way. Classifier, trigger list, the agent protocol, and what
"durable" means: `.claude/rules/design-principles.md`.

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

State assumptions explicitly; if uncertain, ask. If multiple interpretations
exist, present them — don't pick one silently. If a simpler approach exists,
say so; push back when warranted. When something is unclear, stop and name
what's confusing.

## Simplicity First

Minimum code that solves the problem. No features beyond what was asked. No
abstractions for single-use code. No "flexibility" or "configurability" that
wasn't requested. No error handling for impossible scenarios. If you write 200
lines and it could be 50, rewrite it.

## Design Philosophy

- **Sync server is opaque.** The client communicates only with moneybin-sync's API surface. External service providers are implementation details hidden behind the server.

## Shared Vocabulary

[`CONTEXT.md`](CONTEXT.md) is the glossary: the canonical word for each concept,
and the resolution for overloaded ones (`provider`, `tier`, `audit`, `source`).
Consult it when naming or writing. Where code disagrees, the glossary wins for
new writing and internal names migrate as they are touched; renaming a shipped
schema column, MCP tool, or CLI command is a public-contract change first.

## Design System

MoneyBin's visual language lives in `design-system/`. The repo is canonical;
the claude.ai Design System project is a generated mirror — never hand-edit it
as the source. Before any UI, artifact, or frontend work, invoke the
`moneybin-design` skill: it carries the tokens, the four binding grammar docs,
and the non-negotiables. Update flow: `design-system/readme.md` → "Updating
the design system".

## Critical Rules

- **Package manager**: `uv` only. Never `pip install`, `uv pip install`, or `python -m`.
- **Linting/formatting**: `make format && make lint` (Ruff, line length 88).
- **Type checking**: `uv run pyright` (not mypy) — the gate is the bare, repo-wide run. A scoped `uv run pyright <path>` is a fast inner loop, never a substitute: it skips the tests you just edited, which is how a green local run becomes a red CI type check.
- **Tests**: Dev `uv run pytest <path> -v`; pre-commit `make test`. Always `uv run pytest`; wrong interpreter → `uv sync --reinstall`.
- **Pre-commit checklist**: `make check test` — format, lint, type-check, tests. Run once before committing. **Scope the gate to what the diff touches, and never to less:** a diff containing no `.py` files runs its own layer's gate instead (e.g. `uv run pytest tests/design_system` for `design-system/`), and relies on CI for the rest — the 5,800-test Python suite returns no signal on a markdown-only change. Any diff touching `src/` or `tests/` runs the full checklist. This selects the covering gate; it is not licence to skip one. **`make check test` does not include scenarios** — a diff that changes data shapes, matching/categorization heuristics, or migrations also runs `make test-scenarios`.
- **SQL formatting**: `make format-sql` (sets `MAX_FORK_WORKERS=1`; the bare `uv run sqlmesh -p src/moneybin/sqlmesh format` forks a worker pool the encrypted-DB design disallows and the sandbox blocks).
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
- **Comments and docstrings**: Default to one short line. Go longer only for a
  *non-obvious why* a future reader would otherwise undo — an upstream-bug
  workaround, a hidden constraint, a platform quirk, an unenforced invariant.

## Architecture: Data Layers

| Layer | Schema | Materialized | Purpose |
|-------|--------|-------------|---------|
| Raw | `raw` | Table | Untouched data from loaders (Python) |
| Staging | `prep` | View | Light cleaning, type casting (SQLMesh `stg_*`) |
| Core | `core` | Table / View | Canonical, deduplicated, multi-source (`fct_*`, `dim_*`, `bridge_*`) |
| App | `app` | Table | User-state and application-managed metadata (mutable; not derivable from raw) |
| Reports | `reports` | View | Curated presentation models, one per CLI/MCP report |

Full schema reference (including `meta`, `seeds`, `synthetic`, prefix conventions, layer rules, and consumer access patterns): [`architecture-shared-primitives.md`](docs/specs/architecture-shared-primitives.md).

1. **One canonical table per entity** — `dim_accounts`, `fct_transactions`, etc. Consumers read from `core` and `reports` for analysis; the agent-safe SQL surface (`sql_query`, `moneybin sql query`) also reads `raw` and `prep` for inspection, masked by value shape rather than by column declaration.
2. **Multi-source union** — Core models `UNION ALL` from every staging source with `source_type` column.
3. **Dedup in core** — `ROW_NUMBER()` windows for duplicates; mapping tables for cross-source dedup.
4. **Accounting sign convention** — negative = expense, positive = income. `DECIMAL(18,2)` for amounts, `DATE` for dates.
5. **Source-agnostic consumers** — MCP server, CLI use `moneybin.tables` constants, never source-specific logic.

## Specs & Implementation Tracking

Feature specs live in `docs/specs/`. The **[Spec Index](docs/specs/INDEX.md)** is the single source of truth.

- **Before implementing**, check `INDEX.md` for existing specs.
- **Verify the spec against the code before building** — specs (especially
  `draft` ones) can describe a model that was never built or has drifted. Keep
  the spec current as constraints surface during initial planning and
  implementation.
- **When starting**, update status to `in-progress` (spec file + `INDEX.md`).
- **When complete**, reconcile the spec to the delivered implementation, then
  update it to `implemented`. An implemented spec records the shipped decision
  boundary; supersede it only for a material replacement, not ordinary
  follow-up work. See `.claude/rules/shipping.md` for README updates.
- **Observability wiring**: Specs touching app code must include metrics. See `docs/specs/observability.md` and `src/moneybin/metrics/registry.py`.
- Statuses: `draft` → `ready` → `in-progress` → `implemented`.

## Plans vs Specs

Specs are public, durable design records in `docs/specs/`; revise them through
the initial implementation and freeze them when it ships. Plans and session
evidence are execution records, not current truth. Keep disposable plans in the
harness's scratch space. Store plans or evidence that must survive sessions in
`bsaffel/moneybin-private` through a pull request, and link them from the
MoneyBin Linear project. Never write new work to the legacy local `private/` <!-- retired-route-description-ok -->
tree. If either GitHub or Linear is
unreachable, complete the step supported by the reachable service, say exactly
which persistence or navigation step is blocked, and hand the blocked material
or link to the user; do not invent another tracker. Public delivery status and
implementation-ready work belong in this repository's issues and pull requests.

PAO reads `.agents/project-tracking.json`; `declaration_mode: locator` means it
only locates the canonical declaration in `bsaffel/moneybin-private`, while
coordination, knowledge, and archive state remain private and are not
duplicated here.

## Configuration

All config in `src/moneybin/config.py` — one `MoneyBinSettings` root via Pydantic Settings. Never hardcode paths, credentials, or tunable parameters. Env vars use `MONEYBIN_` prefix with `__` for nesting: `MONEYBIN_DATABASE__PATH`.

## Constants

Security-critical parameters (crypto cost factors, key lengths, salt sizes) defined once — module-level `_CONSTANTS` or config fields. Never duplicate across call sites.

## Security

- **Encryption at rest**: AES-256-GCM on all DuckDB databases. See [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md).
- **No PII or financial data in logs.** Log record counts, IDs, and status codes only. One exception: an account label already reduced to its masked form (`****1098`) may appear in a refusal message, because a caller who passed several keys cannot otherwise tell which one was rejected. That mask is digit-pattern based, so it fires only on keys carrying five or more digits — a shorter key reaches the refusal, and the log, verbatim. Treat that as a known gap, not as licence to widen the exception. See `.claude/rules/identifiers.md` → "Account identifiers".
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
| `reports.md` | The `@report` contract, declared privacy classes + derivation/CI verification, the `reports.*`-means-user-facing boundary — loads when touching `reports/**` or SQLMesh `reports` models |
| `shipping.md` | Post-implementation checklist (CHANGELOG, roadmap, features, README, INDEX) — loads when editing those |
| `surface-design.md` | Cross-surface operation-shape taxonomy, verb vocabulary, audience layering — loads when touching mcp/cli/services code or the moneybin-mcp/cli/capabilities + mcp-architecture specs |

### Always loaded (workflow rules)

| Rule | Covers |
|------|--------|
| `design-principles.md` | Durable path selection: one-way-door classifier, public-contract trigger list, the agent protocol, coherence rule. Depth — post-launch contract evolution, the milestone addressing scheme (`M{phase}{letter}.{n}` — append, don't reinvent), what it does NOT mean, the ADR bar: `.claude/references/design-principles-depth.md` |
| `branching.md` | Branch prefix → PR label mapping, commit message style |
| `sandboxing.md` | Bash invocation patterns: single commands, allowlisted pipelines, structured-output filtering, policy denials |
| `agent-experience.md` | Required agent-experience report whenever you interact with MoneyBin's MCP server in a session |
