# Follow-ups

Tracking deferred work and known limitations from shipped features.

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
