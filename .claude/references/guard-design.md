# Guard design — making a guard actually guard

On-demand companion to [`.claude/rules/testing.md`](../rules/testing.md), which
carries the day-to-day pytest conventions. Read this when you are **writing or
changing a guard** — a test, an invariant, a gate, a tripwire — and want to know
how this codebase's guards have failed before.

Every rule below is a defect that shipped or nearly shipped in MoneyBin. The
common thread: a guard's authority extends exactly as far as the thing it
actually inspected, and the failure is always invisible because both the guard
and the code look correct in isolation.

## 1. Before writing it: what does this guard actually read?

**Classify and execute the same bytes.** Any transformation between check and
action — normalizing, re-rendering, re-serializing, interpolating — voids the
guarantee for whatever that transformation touched. `parse_cached` collapsed
whitespace before parsing, so `sql_query` classified one string and handed DuckDB
another; three divergences fell out of that one transform (a `--` comment losing
its terminating newline so a smuggled second statement became comment text to the
parser but still ran; `"routing  number"` classifying as a different column than
DuckDB read; `'a  b'` → `'a b'`). Only the first was reported. **Prefer deleting
the transformation over patching the reported case** — the invariant then holds by
construction. Watch the same shape wherever a value is *rendered* rather than
*bound*. (#346)

**A guard reading one side of a two-sided contract cannot fire when the other
side moves** — and "the other side moved" is how these contracts break. A
tripwire asserted `"tiingo" not in _ref_kind_mapping()` by parsing a `CASE` out
of a model file, so it fired only when the **CASE** changed. The adapter shipped
its *writer* one commit ahead of the mapping: CASE untouched, test green, and
every row the feature wrote was discarded by an INNER JOIN for three commits.
Ask "which side does my guard read?" and write the opposite one — *every
`source_type` the service writes must appear in the model's CASE with the same
`ref_kind` it binds.* Put the producer-side guard in the **fast gate**: a guard
that only runs under `pytest.mark.integration` never runs in `make test`, which
is a weaker version of the same mistake.

**A guard does not inherit correctness from its name.** Before re-applying an
existing check at a new call site, state what its predicate returns on the
adversarial input. `recipe_polarity_fits` re-applied `_has_any_negative_amount`
at the replay gate — faithfully, and that was the bug: the check only catches a
card statement with *zero* payment rows, and every real card statement has a
payment or refund. The corruption was reachable on the plain derive path with no
replay involved. (#313)

**The complement of an allowlist bounds nothing.** `not is_X()` routing into a
branch that *executes* is default-open: every type the parser learns to produce
becomes a bypass. Add the positive `is_Y()` allowlist and refuse anything that is
neither. MoneyBin went through this door twice — a top-level `EXCEPT`/`INTERSECT`,
then `;`-separated statements parsing to `exp.Block`. Beware catch-all parser
nodes: sqlglot lowers `EXPLAIN` to the same `exp.Command` it uses for syntax it
cannot parse, so allowlisting that node wholesale reopens what you just closed —
match the command word instead. (#346)

**Enumerate the exposed set, never the declared set.** A guard that iterates the
registry you are trusting can never reveal what you failed to declare. Widening
`sql_query`'s gate to the whole `reports` schema shipped a masking hole: the
declared-class map covered 6 of 8 *deployed* `reports.*` views, and the other two
fell through a fail-open fallback returning `account_id` unmasked. The
"completeness test" iterated `ALL_REPORTS` — the declared set. Do the arithmetic:
what does this gate admit that the safety net does not cover? Prefer fail-closed;
where fail-open is required for usability, the coverage guard is load-bearing.
(#330)

**Guard a hand-maintained list with set equality — not a count, not a subset.**
`assert len(report.invariants) == 48` fires when you **add** an invariant and
never when you **forget** one. Measured consequence: 33 repos discovered, 21
covered, **12 silently uncovered**. A subset assertion
(`uncovered - exempt == set()`) closes only that direction and creates a new rot
channel, because nothing fails when you wire real coverage and leave the
exemption behind. Instead compute the discovered truth, compute what the
hand-maintained side actually produces — from the **live** surface, real names
from a real run, not a regex over source — and
`assert discovered_gap == set(_EXEMPTIONS)`. One assertion closes three decay
directions: a forgotten registration, a stale exemption, and an exemption naming
something renamed or deleted. Every exemption carries a one-line reason naming
*what closing it needs*, never "TODO".

Hand-maintained lists in MoneyBin: `_NON_PROVIDER_SCHEMA_FILES` (`schema.py`),
the doctor's `_run_app_audit_coverage` call list, `EXPECTED_INTERFACE`
(`test_tables.py`). `undo_dispatch` is the counter-example worth copying — it
*discovers* `BaseRepo` subclasses by `table_ref`. When adding a registration
point, prefer discovery; when you can't, add the equality guard in the same
change.

**Derive an expectation from the constant, never a literal.** A guard that
hardcodes the value it checks does not detect drift, it **locks it in**.
`test_final_review_host_and_report_wording_is_current` asserted the literal
`"registry of 45 intent-shaped tools"` and passed, while
`STANDARD_TOOL_COUNT = 47`. Docs and test agreed with each other and both
disagreed with the code — and the test named "is_current" was the reason the doc
*stayed* stale, because any correct edit to the roadmap turned the suite red. Two
tells: the name contains "current" / "matches" / "in sync" while the body
contains a magic number or copied phrase; and fixing the artifact requires
editing the guard in the same commit. **If a correct change to the artifact breaks
the guard, the guard points the wrong way.** Corollary: the doc fix and the guard
fix must land together, or the correct edit lands red and gets reverted.

## 2. After writing it: prove it fails

**Restore the bug.** When a defect is found by *reading* rather than by a failing
test, the order inverts TDD — fix first, test second — so the test never gets its
RED, and the common outcome is a test that passes against the original buggy code.
Mutate the fix back and run the new test; if it passes, it is not a regression
test. One edit, one targeted `pytest -k`.

**Ask what input distinguishes the two versions.** Boundary values (all /
nothing / zero) usually do **not** — the bug lives in the middle.
`PRICE_ROWS_WRITTEN_TOTAL` incremented only when `written == len(observations)`,
so a *partial* write recorded zero. Both new tests passed against the bug: one
asserted the counter didn't move when nothing was written, the other used a batch
where everything was new. Only a genuinely mixed batch distinguishes them.
Coverage cannot see this class at all — every such test executes the fixed line
and reports green.

**One isolating fixture per guard.** Delete each scoping clause and confirm a
*specific* test fails. See testing.md § "A Fixture That Trips Two Guards Isolates
Neither" — a cash-only fixture (NULL `security_id` *and* NULL price) passed with
its guard removed because `price > 0` already excluded it; the isolating case was
a *priced* trade whose security never bound.

**A MISSED in a restoration matrix has two causes, and they need opposite
fixes.** Trace the patched value forward to the assertion to tell them apart:

- *The guard observes a state the fail-safe path also produces.* A recoverable
  defect leaves the healthy-looking flag healthy: storing a drift fingerprint
  computed from the wrong inputs sends every later read down the mismatch branch,
  which re-resolves, agrees, and reports `degraded=False` — correct result,
  full re-derivation forever. **Fix:** assert the expensive path was *not taken*
  (monkeypatch the costly function to raise), not that the outcome was good. This
  is the shape whenever a defect degrades cost or robustness while the result
  stays correct.
- *Code downstream of the patch site neutralizes the patch.* Swapping constructor
  kwargs changed nothing because the function's last line re-keys on `report_id`.
  The guard was fine; the **patch** did not express the bug. **Fix:** write the
  patch as the implementation someone would plausibly have written, not a minimal
  token edit — a token edit near a normalizing step often cannot survive to the
  observable.

**An inert patch is a lead, not a dead end — always ask why it did nothing.**
Removing `list()` around sqlglot's `find_all` changed no output, because
`find_all` is `bfs()` and materializes a queue — and *that* meant the renderer
was ordering positional `?` bindings breadth-first, so three placeholders
rendered into each other's slots. The fake bug was inert because a real bug sat
underneath it. Deleting the case as unrealistic would have shipped it.

**A CAUGHT verdict is also a claim.** Two ways a matrix reports green while
lying:

- *A failed revert leaves the defect in the tree.* Patched text collided with
  text already in the file, so the revert's uniqueness check refused and printed
  FATAL **to the log only** — the loop never checked revert's exit code. The
  matrix said 10/10 while the source still carried the bug. Make replacement text
  unique by including a neighbouring line, check revert's status, and end the run
  by diffing `git diff src/ | shasum` against a baseline taken before the loop.
  **The harness needs its own restoration check — trust git, not the harness's
  report.**
- *`pytest` exits 5 when `-k` matches nothing*, which is non-zero, so a typo'd
  selector reads as CAUGHT. Assert the run did not say "no tests ran".

The danger in a verification harness is not that it fails; it is that it succeeds
at nothing.

## 3. Strength is not coverage

A restoration matrix proves each guard fails against a defect **you thought of
and injected**. It cannot reveal a defect class you never modelled. A 10/10
matrix over the wrong assertions is 10/10.

An xhigh multi-agent review found 15 verified defects in a branch with **7,743
passing tests and a 10/10 restoration matrix**. Not fifteen independent misses —
one blind spot repeated: *every guard asserted the layer being written, never the
boundary a caller reaches.* Drift tests asserted `DynamicReport.degraded`, the
intermediate object, so `degraded` being dropped before `to_envelope()` was
invisible. The parity test exercised only `str`, so `date` and `decimal` being
unrunnable through MCP passed. Nothing archived a report and then ran it. Nothing
saved a table name in non-lowercase.

After the unit guards pass, add one assertion per **boundary**:

- the response the caller actually receives (`to_envelope()` / `to_dict()`), not
  the object that feeds it;
- every value of an enumerated input domain the surface advertises — all declared
  parameter types, not the one the fixture used;
- the state transition a user performs and then acts on (archive **then** run;
  save **then** re-read);
- an input in the case / whitespace / encoding form a real user would type.

**When a promise appears in more than one place — spec, DDL comment, docstring —
that repetition is a signal nobody tested it.** Prose is where a claim goes when
no assertion holds it.

## 4. When you widen the inputs

Widening a model's input set hands every predicate already in it subjects it was
never written for. Nobody moved the guard; its **meaning changed underneath it**,
and a diff-scoped review sees an untouched predicate and moves on.
`core.fct_security_prices` withheld a grain when two rows in one pull carried
different provider keys and different closes — written for Plaid retiring a
`security_id` on a corporate action. Unioning trade-implied prices handed that
same predicate two **partial fills of one security on one day**: every condition
matched, so the routine case would have emitted no row and the position would
have read `unpriced`. Partial fills are common.

- After adding a UNION branch, a new `source_type`, or a new writer, list every
  `WHERE`, `QUALIFY`, window, and `CASE` in the model and say what each now means
  for the new rows. The dangerous ones reference *source-shaped* columns —
  `source_type`, `source_origin`, `extracted_at`, a provider key.
- Check the schema constraints the old inputs carried that the new one doesn't.
  `raw.security_prices` and `app.security_price_overrides` both
  `CHECK (close > 0)`; `fct_investment_transactions.price` has no such constraint
  and legitimately records `0` for a vesting grant — which would have valued a
  whole position at nothing while reporting `valued`.
- Scope such guards by **source identity enumerated explicitly**, never a
  rank/threshold range. A range looks equivalent and breaks the first time a new
  source takes the next free rank.
- **A spec-described guard can be a no-op.** A "first-available floor" filtered on
  `MIN(price_date)` per `(security_id, source)` taken over the very set it
  filtered, so every row satisfied it by construction. Check a predicate against
  the set it ranges over before implementing it.

The touchpoint list for the transactions fact, the `fct_balances` sign contract,
and the provider-id keying rule live in
[`data-layer-touchpoints.md`](data-layer-touchpoints.md).

## 5. When you remove or narrow a precondition

Making an existing side effect conditional **inverts the burden of proof**: every
consumer that silently depended on it is now a suspect, and none of them are in
the diff, so a diff-scoped review cannot see them.

A grep enumerates the consumers; it does not establish a property over them. The
dangerous step is the sentence turning *n* observations into a claim about *N*
sites — it reads as diligence, which is why it survives self-review. On #363 a
self-review grepped `auto_resolve=False`, listed every call site *including the
one that broke*, and concluded they were all safe; two had actually been opened.
`moneybin mcp config path --client claude-code` — the launcher behind
`make claude-mcp` — went from exit 0 to exit 1 under an ambient
`MONEYBIN_PROFILE`.

- Open **each** consumer, or state the count you actually checked ("verified 2 of
  8"). Prefer the honest partial over the confident generalization.
- **Look for the conflated signal before widening the condition list.** Patching
  the one broken call site fixes the instance, duplicates precedence logic, and
  leaves the others exposed. The durable fix on #363 was noticing the change had
  conflated *a name is set* with *the profile is fully resolved*, and giving the
  second its own flag — restoring the eager set for everyone.
