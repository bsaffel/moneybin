# Follow-ups

Tracking deferred work and known limitations from shipped features.

## Draft `account-management.md` spec (post-cli-restructure v2)

The v2 CLI restructure places the `accounts` top-level entity namespace but no spec owns it. Net-worth.md owns balance/networth surfaces; asset-tracking.md owns assets; nothing owns the entity-management surface (`list`, `show`, `rename`, `archive`, `include`).

Surfaces left without a home:
- `accounts list / show / rename / archive / include` CLI commands
- `accounts_list / accounts_get / accounts_rename / accounts_archive / accounts_include` MCP tools
- `app.account_settings` table (currently specced inside `net-worth.md` but conceptually broader than net-worth inclusion)
- Account merging workflow (when import discovers the same account from two sources)
- Per-account display preferences (color, icon, display order) — future, not yet specced anywhere

Plan:
1. Write `docs/specs/account-management.md` covering the scope above.
2. Move `app.account_settings` ownership from `net-worth.md` to the new spec; net-worth.md continues to read `include_in_net_worth` from it.
3. Update INDEX.md status `planned` → `draft` once written.

## Dynamic MCP server instructions for progressive disclosure (post-cli-restructure v2)

The MCP `FastMCP(instructions=...)` text in `src/moneybin/mcp/server.py` is currently static. v1 advertised `moneybin_discover` for loading extended namespaces, but progressive disclosure (per-session tag-based tool visibility) is not honored by all MCP clients. v2 leaves the discover advertisement out of the instructions to keep the message correct across clients.

When client support for FastMCP visibility transforms broadens — or when we identify the connecting client by name on `initialize` — revisit:

- **Conditional injection.** Inspect the connecting client (FastMCP exposes the client name) and append a "Load extended namespaces via `moneybin_discover`" line only for clients that honor visibility transforms.
- **Static re-add.** If the broader ecosystem catches up, just put the line back and drop the conditional.

The `moneybin_discover` tool itself remains active and discoverable via `mcp list-tools`; this is purely about whether the instructions field calls it out at session start.

## Auto-rule splitting (post-PR #58)

The current auto-rule generator proposes one `(merchant_pattern, category, subcategory)` per
normalized merchant. When a single merchant gets categorized multiple ways by the user
(e.g., Amazon → Groceries for food orders, Amazon → Shopping for everything else), the
proposal pipeline picks the dominant category and the others are abandoned or override the
proposed rule later.

A richer model would let one merchant produce **multiple** proposals, each scoped by an
additional discriminator beyond the merchant pattern.

### Discriminator detection

Candidate signals to mine from `fct_transactions` per merchant cluster:

- **Amount band**: bimodal/multimodal amounts (small grocery vs large electronics on Amazon)
- **Account type**: credit-card recurring vs debit one-off
- **Day-of-week / day-of-month**: payroll on the 15th, gym on the 1st
- **Description fragments**: tokens that co-occur with one category but not another
  (e.g., `AMZN MKTP` vs `AMAZON PRIME`)

Detection algorithm sketch:
1. Group user-categorized transactions by normalized merchant.
2. If categories disagree, try splits along each candidate discriminator.
3. Accept a split if each branch has high category purity (>=90%) and meets the
   trigger threshold (current `auto_rule_min_count`).

### Richer proposal model

Replace the single `(pattern, category)` tuple with a list of `(pattern, filters, category)`
tuples. `filters` is a structured predicate — initially a small allowlist
(`amount_lt`, `amount_gte`, `account_id_in`, `description_contains`) so it can be
serialized into a rule and re-applied deterministically by `CategorizationService`.

Schema impact:
- `app_proposed_rules` already keys on `proposed_rule_id`; adding a `filters` JSON
  column is additive.
- `app_categorization_rules` would need the same column. `_match_rules_for_uncategorized`
  in `CategorizationService` would join the filter predicate into its WHERE clause.

### Review UX

Today `auto-review` lists each proposal as one line. With splits, a single merchant
could produce 2–4 proposals — they should be grouped under the merchant in the table
view, with the discriminator shown alongside the pattern (e.g.
`AMAZON  amount<$50  → Groceries  ×42` /
`AMAZON  amount>=$50 → Shopping   ×17`).

### Why `find_matching_rule` is the substrate

`CategorizationService.find_matching_rule(transaction_id)` (added in PR #58) returns
the first active rule that would match a transaction. Splitting requires asking
"would this rule cover that transaction?" for many candidate rules during proposal
mining — `find_matching_rule` is the single SQL surface that answers it, so the
splitter can be built on top without re-implementing match semantics.

## Restore `categorization-priority-hierarchy` scenario

Removed during PR #59 review: the scenario referenced
`tests/fixtures/categorization/user_overrides.csv` which was never committed, so
`load_fixtures` raised `FileNotFoundError` whenever `moneybin synthetic verify --all`
ran. To restore:

1. Author the CSV with rows whose `source_transaction_id` is `USER_OVERRIDE_2024_03_01`
   plus enough surrounding context for matching.
2. Wire a way to mark the loaded row as `categorized_by='user'` before the auto-rule
   step runs — the current `fixture_loader.py` doesn't carry a category column, and
   the `categorize` step will overwrite anything it considers uncategorized. Likely
   needs either an `expectations`-style pre-load hook or a small extension to
   `FixtureSpec` so the YAML can declare per-row category overrides.
3. Re-add `src/moneybin/testing/scenarios/data/categorization-priority-hierarchy.yaml`
   with the `category_for_transaction` expectation that `categorized_by` stays
   `"user"` after auto-rule promotion.

The expectation matters for correctness — auto-rule promotion must never overwrite a
human-set category — but it can't be checked end-to-end until the fixture format
supports user categorization.

## Scenario runner: skipped /simplify items (post testing-scenario-runner)

Surfaced during the `/simplify` pre-push pass on `feat/testing-scenario-runner` and
intentionally deferred. Each is an enhancement, not a defect — feature works as shipped.

### Reuse `TabularLoader` in `fixture_loader.load_fixture_into_db`

`src/moneybin/testing/scenarios/fixture_loader.py` reads a CSV with `pl.read_csv`,
enriches it (transaction_id, account_id, source_*, import_id, row_number) via Polars
expressions, and calls `db.ingest_dataframe("raw.tabular_transactions", ...)`. This
duplicates the column-mapping work `TabularLoader`
(`src/moneybin/loaders/tabular_loader.py:19`) already does for the production import
path.

Why deferred: the scenario fixture format is a hand-authored 4-column schema
(`date,description,amount,source_transaction_id`) chosen to keep fixture YAML readable
— not a real bank export. `TabularLoader` expects to drive the format-detection +
mapping pipeline against a realistic file. Wiring fixtures through it would require
either (a) writing the mapping config inline per fixture or (b) shaping fixtures like
real exports, which defeats the readability goal. Revisit when we add the second
fixture source type (OFX) — at that point a tiny shared helper that delegates to the
production loader probably wins over two parallel mini-loaders.

### Type `ResponseEnvelope.data` more precisely

Several call sites — `src/moneybin/cli/commands/synthetic.py` `verify_cmd`, all three
tests in `tests/integration/test_scenario_runner.py` — narrow `env.data` with
`cast("dict[str, Any]", env.data)` because the field is typed as `list | dict`. A
`TypedDict` for the scenario-envelope shape (or a `Generic[T]` parameterization on
`ResponseEnvelope`) would remove the cast pattern.

Why deferred: `ResponseEnvelope` is shared with the MCP layer (envelope contract is
defined in `docs/specs/mcp-architecture.md` §7). Tightening its `data` field touches
every MCP tool and CLI consumer, not just the scenario surface — out of scope for the
testing branch.

### CI: matrix-by-scenario for parallelism

`.github/workflows/scenarios.yml` runs `moneybin synthetic verify --all` serially in
one job. With seven scenarios the wall-clock is acceptable today, but a GitHub Actions
matrix (one job per scenario name) would parallelize cleanly and surface per-scenario
failure status as separate red checks instead of one rolled-up artifact line.

Why deferred: premature for 7 fast scenarios; the JSONL summary already gives
per-scenario PASS/FAIL in `$GITHUB_STEP_SUMMARY`. Revisit if the suite grows past ~15
scenarios or the wall-clock exceeds the 10-minute job budget.

### Trim `uv sync --all-extras` in scenarios CI

The workflow installs all extras to satisfy the runner. Most extras (e.g. dev tooling)
aren't needed for `synthetic verify`. A targeted `uv sync --extra <group>` would cut
cold-cache install time.

Why deferred: needs an audit of which extras the scenario pipeline actually pulls in
(SQLMesh, encryption, polars are required; reportlab/pdf likely not). Cheap to do but
risk of breaking CI if an extra is silently transitive — pair with the matrix work
above.

## claude[bot] cannot dismiss its own CHANGES_REQUESTED reviews

The GitHub Action running claude[bot] is sandboxed and blocks
`gh pr review` / `gh api` write calls without explicit permission. As a
result, the bot can flag CHANGES_REQUESTED but cannot clear it after a
re-review confirms fixes — the author has to dismiss it manually
(`gh api -X PUT /repos/{owner}/{repo}/pulls/{n}/reviews/{id}/dismissals`).

Fix: grant the workflow permission to call `gh pr review` by adding to
`.claude/settings.json` (or the workflow's allowlist):

```json
{ "permissions": { "allow": ["Bash(gh pr review:*)", "Bash(gh api:*)"] } }
```

See PR #58 conversation for the exchange where this came up.

## `db key {export,import,verify}` (post-PR for CLI symmetry refactor)

The CLI symmetry refactor introduced the `db key` sub-group with stubs for three operations that are not yet implemented. They exist to reserve the command namespace and exit with code 1 + a "not yet implemented" message.

### `db key export`

Export the active profile's encryption key, wrapped in a user-supplied passphrase, to a backup file. Use case: disaster recovery when the keychain is lost.

Design considerations:
- Wrap with Argon2id-derived KEK + AES-256-GCM (same primitives as data-protection).
- Output format should be portable: a small JSON envelope with `version`, `kdf_params`, `nonce`, `ciphertext`, `tag`.
- File should be marked `0600`.
- Must NOT print the unwrapped key to stdout.

### `db key import`

The inverse: read a backup file, prompt for the passphrase, restore into the keychain for the active profile.

Design considerations:
- Detect collision with an existing keychain entry; require `--force` to overwrite.
- After import, run `db key verify` automatically.

### `db key verify`

Confirm the stored key actually decrypts the active profile's database (open + read a single row from a known table). Useful after restore, or as a periodic check.

Design considerations:
- Must not modify the database.
- Should differentiate "key wrong" from "DB missing" in the error message.

### Why deferred

Each requires a small spec covering the file format, passphrase prompt UX, and rotation interaction. Better to land the CLI shape now and iterate the implementations against a reference spec than to inline-design under PR pressure.

## CLI shadowing trap: `cli/__init__.py` re-exports `main`

`from .main import main` in `src/moneybin/cli/__init__.py` makes `moneybin.cli.main` resolve to the function, not the module. Tests that monkeypatch attributes on the module must use `sys.modules["moneybin.cli.main"]` or import via a different path. Consider renaming the entry-point function (e.g., `cli_entry`) so the module shape is unambiguous.

## City-token stripping in `normalize_description` (post-PR #66)

`_TRAILING_LOCATION` in `src/moneybin/services/_text.py` strips bare
`ST ZIP` only — it deliberately leaves trailing city tokens in place
(e.g. `WHOLEFDS MKT AUSTIN TX 78701` → `WHOLEFDS MKT AUSTIN`). The
optional city group was removed in PR #66 review because it produced
false positives on merchant tokens (`TARGET STORE NY 10001` → `TARGET`,
`SHELL MART NY 10001` → `SHELL`). At the lexical level a trailing
all-caps token is indistinguishable from a merchant descriptor.

A correct fix likely needs one of:
- A known-cities allowlist (US Census places ≥10k population is ~5k entries)
- Two-pass: detect city using state+zip as anchor, then validate that
  what remains is a plausible merchant token (heuristics on token
  length, presence of known suffixes like `MKT`, `STORE`, etc.)

Add goldens for both directions when fixed:
- `WHOLEFDS MKT AUSTIN TX 78701` → `WHOLEFDS MKT` (city stripped)
- `TARGET STORE NY 10001` → `TARGET STORE` (merchant token preserved)

Flagged by claude[bot] and chatgpt-codex-connector in PR #66 review.

## CLI simplify pass — deferred findings (refactor/cli-simplify)

Surfaced by `/simplify` review agents on the cli/ pass. Each was deemed
out of scope for the narrow PR (which only collapses N+1 COUNT queries
in `db info`) and parked here for future, focused changes.

- **Lazy command-group imports in `cli/main.py`** — top-level imports of
  every command module make `moneybin --help` pay the cost of loading
  DuckDB, Plaid, etc. Defer with `typer.Typer(callback=...)` or
  per-subcommand lazy imports. Quantify startup before/after first.
- **`ImportConfig` dataclass for `import file`** — the command currently
  takes ~16 typer options that get plumbed through. A frozen dataclass
  would tighten the signature and make the call sites in tests less
  brittle. Watch out for typer's introspection of parameter defaults.
- **`render_or_json()` helper across `cli/commands/`** — the
  `if json_output: typer.echo(json.dumps(...)) else: <table>` pattern
  is repeated in `db.py`, `accounts.py`, and others. A small helper
  taking `(payload, table_renderer)` would collapse it.
- **Table-formatter helpers** — Rich `Table` construction is
  copy-pasted with column-name-only differences. Consider a
  `render_rows(rows, columns)` utility once a third command needs it
  (rule of three).
- **Broad `except Exception` audit in `synthetic.py` / `transform.py`** —
  several handlers swallow all exceptions and re-raise as `typer.Exit`,
  losing tracebacks. Narrow to expected types (`duckdb.Error`,
  `OSError`, `pydantic.ValidationError`) where feasible.
- **`_with_encryption_key` context manager for db key commands** — the
  rotate/rekey/unlock paths each repeat the "load key → open db → run
  op → zero key" dance. Extract a `with _encryption_key(...) as db:`
  helper. Touches privacy-sensitive code; needs careful review.
- **Stringly-typed validation flags → `Literal`** — several CLI options
  accept free-form strings then validate against a hardcoded set
  (`--format`, `--mode`, etc.). Switch to `Literal[...]` types so typer
  generates the choices and pyright catches typos at call sites.
## Schema examples co-location (post-`mcp-sql-discoverability`)

Example queries currently live in `src/moneybin/services/schema_catalog.py`
(`EXAMPLES` dict) with one-line pointer comments in each interface model
and DDL file. If example drift becomes a real maintenance problem
(examples that reference dropped columns, examples that contradict model
logic, examples that lag behind schema changes), revisit the **sibling
`.examples.sql`** approach: one file per table next to the model, parsed
at startup. See `docs/specs/mcp-sql-discoverability.md` Section "Out of
Scope" and the brainstorming session that produced it.

## MCP schema discoverability — app.* drift coverage

The drift tests in `tests/moneybin/test_services/test_schema_catalog.py`
only exercise `core.*` interface tables because the `schema_catalog_db`
fixture only seeds `core.dim_accounts`, `core.fct_transactions`, and
`core.bridge_transfers`. Six `app.*` interface tables (`categories`,
`budgets`, `transaction_notes`, `merchants`, `categorization_rules`,
`transaction_categories`) are silently skipped — a column rename in any
of them is not caught by CI until the schema doc is read at runtime.

Add a `create_app_interface_tables_raw()` helper to
`tests/moneybin/db_helpers.py` (parallel to `create_core_tables_raw`)
and extend the `schema_catalog_db` fixture to call it. The DDL exists in
`src/moneybin/sql/schema/app_*.sql` and could be loaded directly via
`schema.py:init_schemas()` if a non-private interface is exposed, or
mirrored as Python constants like the core helpers do today.

## Revisit migration auto-apply gate closer to MVP

`Database.__init__` now applies pending migrations whenever
`MigrationRunner(self).pending()` is non-empty, instead of gating on
`stored_pkg_version != current_pkg_version`. The previous gate was
silently broken — `pyproject.toml` `version` was static at `0.1.0`, so
DBs created pre-V003 never re-entered the migration runner on subsequent
opens, and the V003 column adds (e.g. `raw.ofx_institutions.import_id`)
never landed on existing personal DBs. The pending-based gate fixes that
without requiring a manual version bump.

Trade-off to revisit before MVP / first non-author user: the new gate
removes the implicit "migrations only run when I cut a release" boundary.
For single-user personal MoneyBin that's a feature; for a packaged
release where users may have read-only DB snapshots, copy on drag, or
multiple processes opening the same DB, we may want migration application
to be an explicit step (e.g. `moneybin db migrate` at install time, with
the runtime gate being either advisory or off). Decide based on the
deploy model we settle on for MVP — keeping pending-driven, switching to
explicit-only, or some both/and (apply automatically if exactly one
process owns the file, otherwise warn).

Touchpoint: `src/moneybin/database.py` `__init__` migration block, around
the `runner.pending()` check.

## CLI-restructure v2 simplify-pass deferrals

Findings surfaced during the post-implementation `/simplify` review on PR
#96 that were skipped to keep the PR scoped. Each is a real cleanup with
a documented reason for deferring.

- **Lift `mcp_db` fixture from `tests/moneybin/test_mcp/conftest.py` up
  to `tests/moneybin/conftest.py`.** `test_system_service.py` re-implements
  ~25 lines of the same encrypted-DB / singleton-injection setup that
  `mcp_db` already provides. Lifting the fixture lets the service test
  reuse it. Skipped because moving a shared fixture has blast-radius across
  every `test_mcp/*.py` file and benefits from a focused PR.

- **Drop deferred imports inside `system_status` and
  `transactions_review_status` tool bodies.** Both functions defer
  `get_database` and service imports inside the function body even though
  every other tool in the same files imports at module scope. There is no
  documented circular-import reason. Skipped on the suspicion that the
  pattern may have been introduced to keep MCP server boot fast; verify
  there's no real cycle and lift to module scope.

- **Consolidate `SystemService.status()` queries into one CTE.** Currently
  fires 5 sequential DuckDB queries (accounts count, txn aggregate, last
  import, matches pending, categorize uncategorized). For a local in-process
  DuckDB the absolute cost is microseconds; not worth it today. Worth
  revisiting if `system_status` ever becomes a hot path or if we add more
  inventory dimensions.

- **Decide whether `transactions_review_status` should remain a separate
  tool from `system_status`.** Both surface `matches_pending` /
  `categorize_pending`. They were intentionally split during the v2
  brainstorm — `system_status` for full data inventory, `transactions_review_status`
  as a lighter orientation tool — but the duplication means the two paths
  could silently diverge. Either consolidate (call `SystemService` and
  project) or formalize the split with a comment.

- **Bulk-insert in `merchants_create` and `transactions_categorize_rules_create`.**
  Both iterate one INSERT per item (N+1). For 50-item batches that's
  ~10-100 ms locally; the bigger risk is partial-state on mid-batch
  failure (current code counts and continues, no rollback). Switch to
  `executemany` or a single VALUES insert wrapped in BEGIN/COMMIT.
  Pre-existing pattern, not introduced by the restructure.

- **Reconcile `count_uncategorized` vs `categorization_stats` query
  shapes.** `count_uncategorized` uses a LEFT JOIN; `categorization_stats`
  uses subtraction from `COUNT(*)` — which over- or undercounts if any
  transaction has multiple category rows. They should produce the same
  number in well-formed data; the LEFT JOIN shape is more correct. Audit
  for actual divergence and unify, or document why the subtraction shape
  is acceptable.
