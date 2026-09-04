# Feature: CLI Output Coherence

## Status
<!-- draft | ready | in-progress | implemented -->
in-progress

## Milestone

M3K.3 — the third increment under M3K (CLI / MCP UX standards), after M3K.1
([`agent-visualization.md`](agent-visualization.md)) and M3K.2
([`mcp-tool-surface-scaling.md`](mcp-tool-surface-scaling.md)). Those two govern
what the MCP surface returns; this one governs what the CLI's text branch prints.

## Goal

Give the CLI one way to render each kind of result, so every command looks like it
was written by the same hand and every line printed is worth reading. Today three
commands returning rows use three different idioms, four commands print internal
function or MCP tool names at the user, and `reports spending` is illegible at
default terminal width.

## Background

Origin: a 2026-07-25 audit of the CLI's text output, conducted by running every
representative command against a populated profile and reading what came back. It
produced twelve reproducible defects, referenced throughout this spec as F1–F12
and each named at the requirement that closes it. Five of them are worth stating
up front, because they set the scope:

- **F1** — `reports spending` renders eleven columns into an 80-column terminal.
  Every header and nearly every value is elided; not one figure is legible.
- **F2** — `accounts list`, `transactions list`, and `reports networth` are all
  read-projections returning rows, and each renders in a different idiom.
- **F3** — four commands print MCP tool names, Python function names, `repr`
  fragments, or `key=value` debug output at the user.
- **F11** — every invocation prints a profile banner naming two possible sources
  rather than the one that resolved, costing a line per command and telling the
  reader nothing actionable.
- **F12** — `moneybin refresh` narrates one pipeline stage of three, so a run that
  changed nothing and a run that recategorized 400 transactions are nearly
  indistinguishable from its output.

The remaining findings — F4 through F10 — are each named at the requirement that
closes them, in the Requirements section below.

**Reconciled against `e9c9ab15` (2026-08-29).** The findings above record the
audit as taken; two have since moved and the requirements say so at the point
of use. **F11 is closed** — the profile banner now names one source
(requirement 19, which stays as a regression guard). **F3 is two-thirds
closed**, with one leak surviving in a service rather than the CLI
(requirement 16). **F10 moved the other way:** the total the finding wanted is
now free to render, and requirement 34 renders it — but the base64 cursor F10
reported is still printed at 229, so that requirement now deletes a line
rather than merely declining to add one. Every `file:line` citation in this
spec was re-verified at that revision.

Governing rules, none of which this spec supersedes:

- [`.claude/rules/cli.md`](../../.claude/rules/cli.md) — the `-o/--output`,
  `-q/--quiet`, `--json-fields` contract; exit codes; stdout/stderr split; the
  icon table. This spec adds *how the text branch renders*, which `cli.md` has
  never specified.
- [`.claude/rules/design-principles.md`](../../.claude/rules/design-principles.md)
  → Coherence — "when you add new X, does it look like existing X?" F2 is a
  standing violation of that rule.
- [`.claude/rules/surface-design.md`](../../.claude/rules/surface-design.md) —
  operation shapes. Every command in scope is Shape 5 (read-projection) or Shape 3
  (discrete-verb) and renders accordingly.
- [`.claude/rules/identifiers.md`](../../.claude/rules/identifiers.md) → Guard 1 —
  "the display column is for rendering; the ID is for joining." F7 violates it in
  both directions at once.
- [`design-system/readme.md`](../../design-system/readme.md) → Content
  fundamentals + Visual foundations — the CLI is named there as a surface sharing
  the product's language. Money is always mono with redundant sign; balances
  unsigned; numbers first. Requirement 12 reads "balances unsigned" as its intent
  — no decorative `+` on a positive position — and keeps the `−` on a negative
  one, which that rule was never meant to suppress.
- [`moneybin-cli.md`](moneybin-cli.md) — command taxonomy. **Unchanged by this
  spec.** No command is added, removed, renamed, or re-parented.

The same audit surfaced one defect that is **not** a rendering problem, tracked
separately and referenced here as F0: `reports networth` sums an account once per
balance source, so an account carrying rows from two sources is counted twice.
Liabilities dedup correctly and assets do not, which is the diagnostic lead. It is
named in this spec for one reason — a renderer that silently collapsed the
duplicate rows would make the output *look* correct while the total stayed wrong.
Requirement 35 exists to prevent exactly that.

## Requirements

Numbered, each independently testable.

**Renderers (F2)**

1. A module `moneybin.cli.render` exposes exactly three text renderers —
   `render_rows`, `render_summary`, `render_note` — and no command in
   `src/moneybin/cli/` constructs a `rich.Table` or hand-formats an indented list
   directly.

   **The hand-formatted set was larger than the audit found.** The audit named
   the review queues and list commands below under "Files to Modify"; an AST
   scan for an alignment format spec inside a `typer.echo` — the signature of a
   hand-built column — returned **thirteen** modules. Eight of them were not in
   that list: `db.py` (the `ps` process table, twice), `demo.py`, `fx.py`,
   `import_cmd.py`, and four under `investments/` (`__init__.py`, `lots.py`,
   `prices.py`, `securities.py`). They were the same defect and in scope for
   this requirement, excused by a named set in
   `tests/moneybin/test_cli/test_render.py` (`_AWAITING_RENDER_ROWS`) asserted
   by set equality in both directions. **All eight have migrated and the set is
   gone** — with no exemption left, one guard holds every CLI module
   unconditionally and the second one, which existed only to keep the list from
   rotting, was removed with it.

   **A per-unit price is not an amount.** Three of those migrations render a
   figure stored `DECIMAL(28, 10)` — `fx list`'s rate, `investments prices
   list`'s close, `investments holdings`' average cost. Requirement 11's
   `format_money` rounds to two decimal places, which renders a sub-cent
   crypto close as `0.00`, so those columns declare no money kind and print as
   stored. Requirement 11 governs *amounts* — what something is worth — not
   the per-unit prices an amount is computed from.

   **That exclusion covers formatting only.** A column left out of `money=`
   still holds a number, and the fold below reads a fragment of one as a whole
   value whether or not it is an amount: `8.2987654321` folded to `8.298` is
   wrong the same way `1,200.00` folded to `1,200.` is. `render_rows` therefore
   takes a second declaration, `numeric=`, carrying atomicity without
   formatting — share counts, per-unit prices, FX rates, match scores, the
   counts in `import history`. Every column holding a bare number declares one
   of the two. Alignment is *not* part of it: requirement 13 covers amounts,
   and the left-versus-right split recorded below stands until a percent
   contract settles it.

   **A migrated table wider than 80 columns curates its default view.** Six of
   the eight — `investments holdings`, `gains`, `list`, `lots list`,
   `import history`, `import formats list --type=pdf` — declare their columns
   once as `(name, extractor)` pairs plus a `_DEFAULT` subset, extending
   requirement 9's mechanism from `reports` to the list commands. Five of those
   six take `--wide`. `investments list` does not: its six columns fit 80
   together, so its `_DEFAULT` is the whole declaration and nothing is held
   back — a flag there would promise columns already on screen, and the table
   at the end of this document scopes `--wide` to a default narrower than its
   projection. The trigger is that Rich folds an over-narrow cell: a folded
   amount reads as a smaller number rather than a wrapped one, which is a
   correctness failure and not a cosmetic one. Width-based fitting
   (`render_rows(fit=True)`) is the fallback where no author judgement exists
   to encode — it keeps the first and last columns, so on `holdings` it drops
   `market value`, the figure the command reports. `column_view` derives the
   header and every row from the single declaration, so the two cannot drift;
   a contract test pins each default set to names that exist and to a
   header width within 80.

   The scan is the requirement's enforcement, not a one-off: two more guards
   confine Rich to `render.py` and forbid `typer.secho` / `typer.style`
   anywhere else, which is what makes "no second table idiom" and requirement
   36's "no colour literal at a call site" checkable rather than reviewable.
2. `render_rows` is the only way a command prints a collection of records. It
   emits a Rich table to **stdout**.
3. `render_summary` is the only way a command prints a labelled scalar block
   (`Net worth: …`, `Assets: …`). It emits aligned label/value pairs to
   **stdout**.
4. `render_note` is the only way a command prints an informational status line. It
   emits to **stderr** and is suppressed by `-q/--quiet`. Result framing
   (requirement 10) is not a note and does not travel this path.

    **A fidelity disclosure is not an informational status line.** `-q` reaches
    a next-step hint — "run `moneybin reports explain …`" — because that is
    chatter in the sense this requirement means. It does not reach the three
    statements `echo_report_notes` makes about how far the numbers above can be
    trusted: the truncation warning, the degraded-report warning, and the
    applied-rates conversion disclosure. Asking for less chatter is not a claim
    that masking, truncation, or a currency conversion stopped happening, and
    `moneybin reports <x> -q > out.txt` must not capture a capped table that
    reads as the whole answer. That is requirement 10's silent truncation
    arriving through the quiet flag instead of through the stream split.
5. Result data is never suppressed by `-q` (restates `cli.md`; asserted here
   because the renderers are now the enforcement point). The same guarantee
   covers any statement about what the result omits — see requirement 10.

**Column policy (F1)**

6. Every `reports` command declares its default column set, and `render_rows`
   renders only those. The declaration is **parameter-aware**: a static tuple
   where the columns that answer the report do not move, and a callable of the
   report's own effective parameters where they do.

   The resolver intersects the declaration with the columns the result actually
   carries, in the declaration's order, and drops a name the result does not
   have. That ordering is a display decision the declaration owns: an author
   putting the identifying column first must not have to reorder the SQL
   projection, which `--wide`, `--output json`, and every MCP caller also read.

   **What each of those two orderings must be** is settled by
   [`column-ordering.md`](../../.claude/rules/column-ordering.md), not here:
   the decoupling this requirement establishes says the declaration *need not*
   mirror the projection, never that it is unruled. Both follow the same
   grain-first rule independently, and the rule file carries what is guarded.

   **`cash_flow` needs only a static tuple, and the intersection is why.** It
   accepts three groupings — `account | category | account-and-category`
   (`src/moneybin/reports/definitions/_shared.py:34`) — and each selects a
   different set of dimension columns (`cash_flow.py:148-158`): `account` adds
   `account_id, account_name` and no `category`, `category` the reverse, and
   the default both. One tuple naming `account_name, category, currency_code,
   year_month, net` therefore renders five columns in the default mode and
   four in each of the others, because the dimension the mode did not group by
   is simply absent from the result. Naming a field one mode does not return is
   the mechanism, not a violation.

   **`spending`'s `compare` is the case a tuple cannot express.** Its
   projection is fixed — the view returns all three comparisons regardless
   (`spending_trend.py:171-174`) — so the parameter changes which columns are
   *relevant* rather than which exist. `mom` wants `mom_pct`, `yoy` wants
   `yoy_pct`, `trailing` wants `trailing_3mo_avg`, and all three together do
   not fit requirement 9's bar. A static tuple would have to pick one and
   ignore the parameter. This also gives `compare` its first observable effect:
   before this it was documented as "caller-side intent only" and changed
   nothing any caller could see.

   A report whose default set resolves to nothing present in the result is a
   spec violation, caught by the contract test in requirement 9. At run time
   the resolver fails open to the whole projection rather than rendering an
   empty table, because `0 of 9 columns shown` reads as a report that returned
   nothing.
   **Extension reports** (`register_extension_reports`,
   `src/moneybin/reports/_framework/registry.py:53`) share the same `@report` /
   `ReportSpec` contract and the same `register_report_cli` path, so the field is
   **optional** on `ReportSpec`: an extension that declares nothing keeps working
   unchanged. An undeclared report is **fitted to the terminal** rather than
   capped: `render_rows(fit=True)` measures the rendered cells, keeps the first
   and last columns, and drops from the middle outwards until the table fits the
   real console width, marking the gap with a `…` column. This is what DuckDB's
   box renderer and pandas both do, verified against both
   (`14 columns (6 shown)`, `[1 rows x 14 columns]`), and it is strictly better
   than the fixed count first specified here: a fixed six under-fills a 200-column
   window and still overflows a 40-column one, and it cannot know a column's
   display width, which depends on the values rather than on `OutputColumn`
   (`src/moneybin/reports/_framework/contract.py:139-144`). Measuring at render
   time is exactly where the values are.

   The ends are kept because they are what identify a row and carry its answer —
   a spending table reads as `month … total`, and dropping either end for two
   middle dimensions keeps the qualifiers and loses the question. A column too
   wide to keep costs only itself: the walk closes that side and keeps taking
   from the other, so widths `1, 100, 1, 1, 1` render four columns rather than
   the two an all-or-nothing stop would leave in an 80-column window. Requirement
   10's framing line reports the fit from what was actually printed, so a
   narrowing the caller never requested is disclosed by the same line as a
   declared one. **Requirement 9's 80-column guarantee remains contract-tested
   for in-tree reports only** — a declared set is a curated answer that the fit
   must not re-decide, so a report that declares columns is never fitted.
   `docs/specs/extension-contracts.md` gains the optional field and its fallback
   in the same change.
7. Every command with a `DEFAULT_COLUMNS` narrower than its full projection
   accepts `--wide`, which renders all columns. It is injected on every
   generated report command, including one whose default set is its whole
   projection: the flag is then a no-op, which is cheaper than a signature that
   varies per report and a `--help` where the option comes and goes.
   `reports run` carries it too, so a report renders the same columns whichever
   command ran it. The two hand-written commands — `reports networth` and
   `networth-history` — do not: they already choose their columns in code, and
   networth's full projection mixes per-currency totals rows with per-account
   rows that a single table cannot widen into coherently.
8. `--output json` is unaffected by `DEFAULT_COLUMNS` and by `--wide`; it returns
   the full projection, filtered only by `--json-fields`.
9. `DEFAULT_COLUMNS` for every report fits in **80 columns** with no elision of any
    header, and no elision of any value that is not free text. 80 is the width F1
    was reproduced at, so a wider bar would let an implementation satisfy this
    spec while the reported defect persists. Fitting is necessary but not
    sufficient: a column's rendered form must still preserve **the distinction
    the column exists to reveal**. A date abbreviated until a year-old value
    and a four-month-old value render identically clears the width bar while
    destroying the column's purpose; so does a merchant name truncated until
    two distinct payees collapse into one string. Where the two bars conflict,
    drop the column from `DEFAULT_COLUMNS` and disclose the omission under
    requirement 10 — a named absence beats a present-but-uninformative
    column.
10. When columns are omitted, `render_rows` emits one **result-framing** line
    naming the count and the flag: `12 of 23 columns shown — --wide for all`.
    Silent truncation is prohibited. This line is part of the result, not
    chatter: it emits to **stdout** immediately after the table, and `-q` never
    suppresses it. Both properties are load-bearing — routing it to stderr would
    let `moneybin reports spending > report.txt` capture a truncated table with
    no indication it was truncated, and suppressing it under `-q` would do the
    same for `--quiet`. Either alone reintroduces the silent truncation this
    requirement forbids.

**Money and numbers (F9)**

11. A single `format_money` is the only place amounts are stringified for text
    output. Thousands separators always; two decimal places always.

    **Non-money numerics are not covered, and it shows.** This requirement and
    the money kinds below reach only columns that declare a `money_kind`;
    everything else reaches the table through `str()`. `reports spending`
    therefore prints its ratio columns at full binary-float precision — cells
    running to eighteen fractional digits (`0.009336226303145232`,
    `0.024842005788199163`) — in a report whose money columns are now
    formatted to two places.

    **The visible cost is a table with two minus signs in it.** One row of
    `reports spending` renders `mom_delta` as `−8.04` (U+2212, requirement 12)
    and `mom_pct` as `-0.018468747846461304` (hyphen-minus) side by side,
    because only the first column reaches `format_money`. Two glyphs one cell
    apart is exactly the difference a reader is entitled to read as meaning.
    Zero tells the same story — `0.00` in the money column, `0.0` in the ratio
    column — and so does alignment: the ratio columns sit left while the money
    columns sit right (requirement 13), so their digits never line up.

    `reports networth-history` splits the same way for the same reason, but
    only on the glyph and alignment halves: its `change_pct` is already
    formatted to two places (`f"{...:.2%}"`), so it costs a hyphen-minus beside
    `change_abs`'s U+2212 and a left-aligned column beside a right-aligned one,
    without the runaway precision above. Fixing that one column's glyph alone
    would be worse than leaving it: `mom_pct` would still print hyphen-minus,
    and the reader would face two ratio columns disagreeing about the same
    character with no rule saying which is right. Both wait on the percent
    contract.

    The same gap costs alignment in five more tables. `render_rows`
    right-aligns a column only when it is declared money, so every non-money
    numeric that a hand-built table used to right-align now sits left:
    `conf` in the security-link and merchant-link queues, `score` in
    `transactions matches`, the ledger overlap in `accounts links`, and the tag
    counts in `transactions tags`. Requirement 13 covers amounts only, so this
    is conformant — and it is the same left-versus-right split described above,
    reproduced by the migration rather than inherited.

    Both ratio columns are declared at
    `src/moneybin/reports/definitions/spending_trend.py:77,95`.
    No requirement in this spec fixes it; it is recorded here because the
    milestone's own transcript surfaced it, and a percent contract (places,
    and whether the column carries `%` or a ratio) is the natural companion to
    the money contract rather than a second idiom invented later.
12. Every money column declares a **money kind**, and the renderer never infers
    meaning from the raw number. Four kinds:
    - `flow` — signed under the AGENTS.md accounting convention (negative =
      expense, positive = income). Renders an explicit `+` / `−` (U+2212).
    - `magnitude` — a positive absolute quantity whose polarity is carried by the
      column, not the value: `spending_trend.total_spend` is
      `SUM(ABS(t.amount))`
      (`src/moneybin/sqlmesh/models/reports/spending_trend.sql:14`), a positive
      absolute outflow. Renders unsigned; never colored as income.
    - `delta` — a signed *change in a magnitude*, where the sign means direction
      (more / less) rather than income / expense. Declares the polarity of the
      thing it measures, because that determines whether an increase is good or
      bad. `spending_trend.mom_delta` is `total_spend - prev_month_spend`
      (`src/moneybin/sqlmesh/models/reports/spending_trend.sql:89`), so a
      positive value means spending *rose*. The computation lives in the
      SQLMesh model, not the Python definition — `spending_trend.py` only
      selects the column (line 62), and reading that declaration alone leaves
      the sign direction unestablished, which is this kind's entire basis. It renders signed — the direction is the
      column's entire purpose — and colors against the declared polarity, so a
      rise in spending is `--neg-expense`, not `--pos-income`.
    - `balance` — a position, not a movement. Renders **unsigned when
      non-negative, and always retains `−` when negative**. The design system's
      "balances unsigned" rule exists so a checking balance is not decorated with
      a `+`; it does not license dropping a minus. `reports.net_worth` is
      `SUM(d.balance)` over accounts whose liabilities are kept negative
      (`src/moneybin/sqlmesh/models/reports/net_worth.sql:13`), so a net worth of
      −50,000.00 is reachable and would otherwise render identically to
      +50,000.00 — the single worst misread this spec could ship.

    `delta` exists because neither of the other kinds fits a signed delta:
    `flow` would color a spending increase green, and `magnitude` renders
    unsigned, erasing the increase-versus-decrease distinction the column exists
    to convey.

    **One column in the catalog the four kinds do not name.**
    `balance_drift.drift` is asserted balance minus computed balance — a signed
    discrepancy where *neither* direction is favourable. `flow` would paint a
    positive drift green; `delta` demands a polarity that does not exist,
    because drifting either way is equally wrong and only `drift_abs` beside it
    says how wrong. It is declared `balance`, which is the right *rendering*
    contract — signed, uncolored — while not literally being a position. Left
    as a declared stretch rather than resolved by minting a fifth kind for one
    column; revisit if a second such column appears.

    The kind is declared **per column**, as `money_kind` on `OutputColumn`. It is
    deliberately *not* named `kind`, because `ReportSemantics.kind` already exists
    with the values `position | flow | ratio | count | unknown`
    (`src/moneybin/reports/_framework/contract.py:163`). That one is **report**-level
    and cannot serve here: `spending_trend` carries a `magnitude` and a `delta` in
    the same result, so one report-level value cannot describe both columns. The
    two overlap in vocabulary (`flow`, and `position` ≈ `balance`) without being
    the same thing, so the distinct field name is load-bearing — a shared `kind`
    would read as one concept and rot into two.

    For **extension reports**, `money_kind` is optional and has **no default
    kind**. Requiring it would break every existing `@report` extension, since
    `OutputColumn` had no such field; defaulting it would mean the renderer
    deciding which columns hold amounts, which is the inference this
    requirement exists to remove. An undeclared column is therefore not money
    to the renderer and reaches the table through `str()` — exactly how it
    rendered before the field existed, so nothing breaks and an extension opts
    in per column.

    An earlier draft of this requirement specified a `flow` default, on the
    reasoning that a needless `+` is cosmetic while a dropped `−` is a misread.
    That reasoning is sound about *which* default would be safer and silent
    about the prior question: nothing tells the renderer that an undeclared
    column is money at all. The only candidate signal, `DataClass`, answers a
    privacy question — `TXN_AMOUNT` marks what must be masked, not what must be
    formatted — and keying rendering to it would couple the two the moment one
    of them moved. `docs/specs/extension-contracts.md` documents the optional
    field alongside `DEFAULT_COLUMNS`.

    Optional is not unchecked. `OutputColumn.__post_init__` refuses a
    `money_kind` or `polarity` outside its declared set, and a polarity on any
    kind but `delta`. Both are `Literal` types, which bind a type checker and
    not the interpreter, so the extension author this field exists for gets no
    runtime signal otherwise — and a wrong value is silent rather than loud: an
    unrecognized kind renders unsigned and uncoloured, and `Money.style_for`
    reads every polarity that is not `income` as `expense`.
13. Amounts are right-aligned in `render_rows` columns.
14. Color is driven by the money kind plus the value, never by the value alone.
    A `flow` colors `--pos-income` when positive and `--neg-expense` when
    negative; a `magnitude` in a spend-oriented column is never green; a `delta`
    colors against its declared polarity, so a rise in an expense magnitude reads
    `--neg-expense`; a `balance` is uncolored. The sign glyph — where the kind has
    one — is present regardless of color, so the encoding survives a pipe, a
    non-TTY, and `NO_COLOR`. A negative `balance` is the load-bearing case: it
    carries `−` with no color, so the sign is the only channel and must never be
    dropped.
    **Rationale:** `spending_trend.sql` computes `total_spend` as
    `SUM(ABS(t.amount))` (line 14) and `mom_delta` as
    `total_spend - prev_month_spend` (line 89). Coloring on raw sign would
    render spending green as income and invert the meaning of a rising
    `mom_delta`, contradicting AGENTS.md's sign convention.
15. Color is emitted only when stdout is a TTY and `NO_COLOR` is unset.

**Message hygiene (F3, F11, F12)**

16. No user-facing message *leaks* an internal identifier. The audit matches
    **compound internal identifiers and tool-call syntax**, not bare words:
    - a `snake_case` identifier of two or more segments (`transactions_get`,
      `accounts_links_run`), whether bare or as a `name:` prefix;
    - `key=value` debug fragments outside a recovery action.

    It deliberately does **not** match single-word registered tool names. The
    registry contains tools literally named `accounts` and `reports`
    (`src/moneybin/mcp/surface.py:30-31`), which are also unavoidable product
    vocabulary — a test rejecting every registered name would fail on
    `Accounts: 5` and force unnatural prose for no privacy or clarity gain.

    **Audit state at spec time.** Two of F3's four leaks are already gone:
    the `transactions_get returned …` line and the `accounts_links_run:`
    prefix no longer appear. Two survive. The second was missed by the audit
    and by the first automated guard alike, because both looked only at
    `logger.*` and `typer.echo` arguments: `system doctor` prints
    `InvariantResult.name` verbatim
    (`src/moneybin/cli/commands/system/doctor.py:118-120`), so
    `name="sqlmesh_model_presence"` reached the user as a compound snake_case
    identifier through a constructor, one rendering layer away from any call
    this requirement's scan visits. Requirement 17 renames it; the scan now
    reads `InvariantResult` fields for exactly this reason. The first sits
    **below the CLI layer** — `src/moneybin/services/merchant_links_service.py:301` logs
    `merchant_links_run: bound={n} conflicts={n}`, which is a compound
    snake_case identifier *and* two `key=value` fragments, and it reaches the
    user through the log handler. Closing it means editing a service, so this
    requirement is the **second** deliberate below-CLI reach in this spec;
    requirement 18 is the other. The reach is bounded to the message: the
    string is rephrased into prose, the counts the step already computes are
    kept, and no signature, result carrier, or call site changes. Retaining
    the leak instead would leave requirement 16 asserting a property the
    codebase does not have.

    **Explicit exception — `RecoveryAction` rendering.** `system doctor` and the
    error path render `action.tool(key=value, …)` deliberately
    (`src/moneybin/cli/commands/system/doctor.py:141`), because
    `RecoveryAction.tool` *is* an MCP tool name by contract
    (`src/moneybin/errors.py:60`) and the rendered call is meant to be pasted
    directly by an agent. This is a designed AX affordance, not a leak, and
    requirement 22 preserves it. The audit skips recovery-action lines.
17. No user-facing message names an internal dependency. `SQLMesh` is not a user
    concept; the stage is "transforms".

    **The surface is every console-visible module, not `moneybin.cli`.** The
    user reads one stderr stream, and the services behind a command write to it
    on the same terms — `_CONSOLE_SUPPRESSED_PREFIXES` is a denylist, so a
    module's INFO reaches the console unless it is named there. A CLI-only
    rename leaves `transform apply` printing "Running SQLMesh transforms"
    directly above "Transforms applied", and pairs a renamed
    "Transform state migrated" in `db migrate apply` with
    `database.py`'s "SQLMesh state updated" on the adjacent path. Seven strings
    across five modules below the CLI carry it:
    `services/transform_service.py` (start, failure, completion),
    `database.py`, `seeds.py`, `services/doctor_service.py`, and
    `sqlmesh_registry.py`. Contributor-facing prose — comments, module
    docstrings, the `sqlmesh_registry` module name, the private
    `_run_sqlmesh_model_presence` method — is out of scope.

    **Those seven were not the whole set, and the shortfall was structural.**
    Three further strings live in `migrations.py`'s `sqlmesh_state_assessment`,
    which returns them for `db migrate` to log one call away; six more are
    `InvariantResult` `name` and `detail` fields that `system doctor` prints
    verbatim. Neither family is an argument to a `logger.*` or `typer.echo`
    call, so the hand audit and the first automated guard missed both by the
    same mechanism rather than by carelessness. A message is in scope because
    the user reads it, not because of the call it is written at.

    **Residual, deliberately out of scope: exception text.** `RuntimeError`
    and `ImportRefreshError` messages name SQLMesh at
    `services/import_service.py:1886` and `:5725`,
    `services/transform_service.py:311`, and `mcp/tools/import_tools.py:425`.
    Whether these are user-facing prose or a contributor-facing crash report
    depends on which are rendered and which reach a traceback, and that is a
    decision about the CLI's error contract rather than about vocabulary.
    Requirement 17 does not cover them; the guard's docstring names the gap so
    a reader does not mistake its scan for a stronger guarantee.
18. `refresh` emits one `render_note` per pipeline stage naming the stage and its
    observable outcome, including stages whose outcome is zero. A run that changed
    nothing and a run that recategorized 400 transactions are distinguishable from
    stderr alone. This requires a **result-carrier change**, not a renderer-only
    one: `RefreshResult` (`src/moneybin/orchestration/refresh.py:109`) carries
    error-or-`None` per step, and `_run_categorize_step` (line 486) computes its
    counts, logs them, and returns `str | None`, discarding them. The renderer
    cannot recover a count the service already dropped, and must not re-query for
    it. `RefreshResult` therefore gains a per-stage outcome carrying the counts the
    steps already compute. This is **one of the three** requirements in this
    spec that reach below the CLI layer, and it is deliberate: the outcome per
    stage is the payload, so no render-layer-only change can satisfy it.
    Requirement 16's `merchant_links_run` leak and requirement 17's five
    service-layer modules are the others; both reach less far — a message
    rewrite rather than a carrier change.
19. The profile banner names the source that actually resolved, or says
    nothing. Two lines carry it, and only one was clean at spec time.
    `src/moneybin/cli/utils.py` logs a bare `Using profile: {profile_name}`,
    which is fine; the *source* line beside it read
    `Profile resolved from config.yaml or first-run wizard` — it told the
    reader that one of two things happened without saying which, which is
    exactly the ambiguity this requirement forbids. Reading
    `get_default_profile()` before `ensure_default_profile()` resolves it:
    that is the same check `ensure_default_profile` makes first, so a
    non-`None` result means the config path and `None` means the wizard.
    The other half of F11, that the banner costs a line per invocation to say
    something the reader rarely needs, is a separate call from whether it is
    ambiguous and is **not** decided here.

**Quiet on success (F6)**

20. `system doctor` prints per-invariant lines only for invariants that need
    attention. On a fully-passing run it prints its summary line and nothing
    else.

    **The suppressed status is `pass`, not "everything but `fail`."** A `warn`
    and a `skipped` are not passes: the summary counts them without naming
    them, so hiding either would leave a reader knowing something is off and
    unable to see what — the silent masking the rest of this spec exists to
    prevent, arriving through the change meant to reduce noise. Four statuses
    exist (`pass`, `fail`, `warn`, `skipped`) and exactly one is suppressed.
21. `--verbose` restores the full per-invariant roll. It already meant "show
    the affected transaction IDs"; it now also means "show every invariant that
    ran", which is one flag for one question — what actually happened — rather
    than two flags a reader has to choose between.
22. The failure path is unchanged in content: a failing invariant prints its name
    and its detail.

    **The summary survives `-q`.** Once requirement 20 stops narrating a
    passing invariant, the summary is the only thing a clean run prints, and
    the pre-existing `if not quiet` gate on it would make
    `moneybin system doctor -q` succeed in total silence — a command whose
    whole job is to report on the ledger saying nothing about it. The summary
    is doctor's result, not a status line about producing one, and requirement
    5 already forbids `-q` suppressing result data. What `-q` does silence is
    the 💡 recovery-action hints, which is the line `echo_report_notes` already
    draws for reports: quiet reaches next-step hints and nothing else. A
    failing invariant still prints under `-q`.

**Stats (F5)**

23. `stats` renders each metric with its distinguishing dimension, such that no
    two rendered lines share a label.
24. Histogram metrics render an **explicitly declared** unit, carried as a `unit`
    field on the metric declaration in `src/moneybin/metrics/registry.py`. A
    metric that is not a duration does not render `s`.
    **Name-suffix derivation was tried and rejected.** It holds for
    `..._duration_seconds` and `..._rate`, but nine registered metrics end in
    suffixes that name a *dimension* rather than a unit — `..._batch_size`,
    `..._score`, `..._pending`, `..._count`, `..._rows_affected`. The decisive
    case is `moneybin_import_batch_size`, whose observations are **files**: that
    fact lives only in the declaration's description string, which `app.metrics`
    does not persist. Derivation would force `stats` to omit the unit or guess
    it. One declared field applied uniformly is also the coherent choice — a
    derive-here / declare-there split would be two mechanisms for one job.
25. `stats` groups metrics by domain with a header per group, rather than one
    alphabetical list.

**Identity display (F7)**

26. `accounts list` renders the account ID alongside the display name, so rows
    that share a display name are distinguishable. **Balance is deliberately not
    in scope:** `AccountSummary`
    (`src/moneybin/privacy/payloads/accounts.py`) carries no current-balance
    field and `AccountService.list_accounts()` reads only `dim_accounts`, so
    adding one would change the JSON/MCP contract requirement 8 keeps untouched
    — and F0 shows balance data is currently summed once per source, which a new
    surface would reproduce rather than reveal. Tracked as a follow-up.
27. `transactions list` continues to render the account ID. **The display name is
    deliberately not in scope**, for the same reason as requirement 26's balance:
    `TransactionRow` carries only `account_id`
    (`src/moneybin/privacy/payloads/transactions.py:44`) and
    `TransactionGetResult` carries no account name
    (`src/moneybin/services/transaction_service.py:178-197`), so a name would
    require a service and payload change — the JSON/MCP contract requirement 8
    keeps untouched. Tracked as a follow-up alongside the balance column.
28. `accounts list` and `transactions list` share the `account_id` column, with
    equal values for the same account, so the two outputs join. **This is what
    closes F7:** the reported defect was that `accounts list` showed names with no
    ID while `transactions list` showed an ID with no name, leaving no shared key.
    Requirement 26 supplies the missing ID, which makes the join possible on the
    column both sides already agree on. Display names on both surfaces are an
    ergonomic improvement on top of that, not the fix.

**Categories (F8)**

29. The provider vocabulary never enters the category column, so no single
    rendered column contains both `Food & Drink` and `FOOD_AND_DRINK`. **The
    boundary is below the CLI**, in `prep.int_transactions__unioned`: its Plaid
    branch aliased the raw PFC code into `category` (`plaid_category AS
    category`), which `core.fct_transactions` then reached as the third arm of
    `COALESCE(dc.category, c.category, t.category)`. The branch now contributes
    `NULL::TEXT AS category`; the PFC code was already carried separately as
    `plaid_category`, so nothing is lost and no column is added.

    **Revised during implementation.** This requirement originally placed the
    resolution in the CLI renderer. Reading the code first showed that wrong
    twice over. The mixed column is a data-layer fact every consumer sees —
    reports, `sql_query`, MCP — so a renderer fix would leave it everywhere
    except the CLI. And all four report models group by `category`, so mapping
    unknown values to a placeholder *at render time* would turn one grouping key
    into several rows all labelled `Uncategorized` with different amounts —
    which requirement 35 forbids the renderer collapsing. The renderer boundary
    trades a mixed column for a duplicated one.

    **Scope is the provider vocabulary only.** `category` still falls back to
    the source's own text where a person wrote it — a tabular CSV's category
    column (`stg_tabular__transactions`), a manual entry (`stg_manual__`) —
    because that is a category the user chose, not a machine code. `t.category`
    meant two different things depending on source, and only one of them was the
    defect; dropping the fallback wholesale would have discarded user-authored
    categories, a regression this scoping avoids.

    **Two further defects fall out of the same one-line change**, and are the
    evidence the boundary belongs here rather than in the renderer:
    `core.uncategorized_queue` selects `WHERE category IS NULL`, so a
    never-categorized Plaid row carrying a raw code was **excluded from the
    curation queue** — while `transactions list --uncategorized`
    (`categorized_by IS NULL`, `transaction_service.py:1007`) included it. Two
    definitions of "uncategorized" disagreeing on exactly the rows F8 is about.
    And an unmapped code formed its own row in a spending breakdown, splitting
    a category's total. Neither is a rendering bug and neither was in F8's
    report; both are now covered by `test_categorize_plaid_e2e.py`.
30. A row with no category renders a single consistent placeholder —
    `Uncategorized` — in **text output only**. Requirement 29 means the raw
    provider code can no longer reach this column, so the placeholder now stands
    for a genuine absence rather than masking a vocabulary mismatch. The count of
    such rows rides the **result framing** (requirement 10), not a `render_note`:
    requirement 4 suppresses notes under `--quiet`, and a taxonomy gap the user
    cannot see is exactly what requirement 29 exists to prevent. This **widens
    requirement 10's trigger**: result framing is emitted when columns are
    omitted *or* when the placeholder appears in the column declared to carry
    it. Without that widening the disclosure would vanish in exactly the cases
    it matters — under `--wide`, and for any report whose full projection
    already fits 80 columns, neither of which omits a column. The framing
    clauses share one line.

    **The renderer substitutes, and counts what it substituted (surfaced
    during implementation).** The caller declares the column and the word, then
    passes the stored value through — NULL included — rather than substituting
    and letting the renderer match the string back. Two things fall out that
    the string match got wrong. A description or a category a person *authored*
    as `Uncategorized` is a value, not a gap: `tabular` and `manual` keep
    whatever text was written, and plenty of tools export that exact word as
    their own placeholder, so matching the rendered string would count an
    authored value as a missing one — collapsing the very distinction the
    paragraph below promises `--output json` preserves. And the count is
    confined to one column, because a taxonomy gap lives in one column while
    every other cell in the row is data.

    Absent means NULL, and only NULL. A whitespace-only category is reachable —
    Polars reads an empty CSV cell as NULL but keeps a cell holding spaces —
    and treating it as a gap is the tempting mistake. `core.uncategorized_queue`
    selects `WHERE category IS NULL`, so such a row is not in the queue, and
    counting it would advertise a gap `transactions categorize run` cannot act
    on: the same class of lie as the provider code requirement 29 removes from
    this column. Making the two agree means normalizing blanks in staging
    (`NULLIF(TRIM(category), '')` in `stg_tabular__` and `stg_manual__`), which
    changes what the queue *contains* rather than how it renders, and is
    therefore not this requirement's to make.

    A declared column absent from the table is refused rather than skipped, for
    the reason `column_view` already gives: a disclosure that silently counts
    nothing renders exactly like a table with no gaps. A declared column the
    *width fit* dropped is skipped, which is the different case — the gap is
    real but not on screen to be misread.

    `--output json` carries the underlying NULL untouched, so a caller can still
    tell an uncategorized row from one categorized as the literal string
    `Uncategorized`. The raw provider code remains readable at
    `prep.int_transactions__merged.plaid_category` through `sql_query` /
    `moneybin sql query`, which AGENTS.md already designates as the surface for
    inspecting `raw` and `prep`. It is deliberately **not** added to
    `TransactionRow`: that is the payload change requirements 26 and 27 declined,
    for the same reason — requirement 8 keeps the JSON/MCP contract untouched.
    The cost, named rather than hidden: an agent debugging why a row failed to
    categorize runs one SQL query instead of reading the list payload.

    One placeholder, tree-wide. `transactions splits list` rendered `-` for the
    same absence; it now renders the same word, because two placeholders for one
    condition is the second pattern the coherence rule prohibits.

**Stubs (F4)**

31. Unimplemented commands are hidden from `--help` output at every level
    (`hidden=True`), while remaining invocable so the namespace stays reserved and
    any existing script keeps its exit code.
    **Scope is whole-command stubs only.** A command that reaches
    `_not_implemented` on *some* paths is not a stub and is not hidden — hiding it
    would remove working behavior from `--help`, the opposite of the trust repair
    F4 exists for. The enumeration is the four `sync` stubs (`key rotate`,
    `schedule set|show|remove`), the two `budget` stubs (`set`, `delete`), the
    three `transactions categorize ml` stubs, and the three `db key` stubs
    (`export`, `import`, `verify`) — **not** a grep for
    `_not_implemented`, which both over- and under-matches. It over-matches on
    `review`'s `--interactive` branch
    (`src/moneybin/cli/commands/transactions/review.py:96`) and any future
    partial: that branch is deliberate, since PR #358 stopped routing users
    into it while leaving it unbuilt, so `review` is a working command with one
    pending path — exactly the case hiding would break. It under-matches on the
    `db key` trio, which predates the helper and inlines its own message and
    `typer.Exit(1)` (`src/moneybin/cli/commands/db.py`); they are whole-command
    stubs by every other measure, and an enumeration derived from the grep
    silently omitted them.

    **A group whose every command is a stub is hidden too.** Hiding only the
    leaves leaves `sync key`, `sync schedule`, `budget`, and
    `transactions categorize ml` advertising a group that lists nothing, which
    is the same promise F4 exists to withdraw. `db key` is the counter-case and
    stays visible: `show` and `rotate` work, so hiding the group would take
    them out of `--help` too. Both directions are asserted — an implementation
    that hides every group containing any stub passes the first and fails the
    second.
32. The not-implemented message names a user-facing next action, not a repo path.
     `docs/specs/*.md` does not appear in any message reachable by an installed
     user. Both stub families share one message: two shapes for one situation
     is the coherence failure "one way to do each thing" forbids.

     **One message, level-independent.** The shared helper writes with
     `typer.echo(..., err=True)`, so no `LoggingConfig.level` can suppress it.
     A logger cannot carry this message: `ERROR` and `CRITICAL` are two of the
     five supported values and both drop a `WARNING` record, which would leave
     the three `db key` stubs exiting `1` having printed nothing — a bare
     failure code with no reason, which is the whole of what this requirement
     forbids. Those three explained themselves through an unconditional
     `typer.echo(..., err=True)` before requirement 31 moved them onto the
     shared helper, so a logger-backed helper would have regressed exactly the
     range they were immune to. Routing every stub the same way closes that for
     all twelve and still leaves one message shape, which is what requirement 32
     asks for; special-casing `db key` is what would have produced two. The next
     action rides in the same call rather than a second one, because splitting
     it would drop exactly the half this requirement demands.

     The cost is that stub invocations no longer reach the log file —
     `.claude/rules/cli.md` already accepts that for a line a `typer.echo`
     carries.
33. Exit codes are unchanged. The tree carries **two** policies, and this spec
    records rather than unifies them: stubs routed through `stubs.py` exit `0`
    (the reasoning is in its docstring — `1` means "ran and failed"), while the
    three `db key` stubs exit `1` because they shipped that way. Unifying them
    would be a public-contract change to a published command, which the
    milestone's "preserve exit codes" boundary excludes. Revisit only behind an
    explicit stub-exit-code decision.

**Pagination (F10)**

34. The text branch renders a human paging line stating how many rows were
    shown, **the total matching the filters**, and a continuation the reader
    can actually type — `8 of 2,046 shown · raise --limit for more`.

    **The total is rendered, and costs nothing to obtain.**
    `TransactionGetResult` carries `total_count`
    (`src/moneybin/services/transaction_service.py:189`), documented there as
    "every row matching the filters, not the page length — the same meaning
    `summary.total_count` carries on the MCP surface", and `transactions list`
    already passes it (`src/moneybin/cli/commands/transactions/list_.py:173`).
    No service or payload change is required, so requirement 8's exclusion
    does not bite. This is the count F10 actually wanted: the finding was that
    a human asking "how much is there?" got a base64 cursor instead.

    **The count and the continuation are gated separately (surfaced during
    implementation).** `total_count` is every row matching the filters and does
    not shrink as a walk advances, so the remainder it implies is not evidence
    that a further page exists: on the last page of a `--cursor` walk the total
    still exceeds the rows shown, and `--limit` there would fetch nothing. The
    count follows the remainder; the continuation follows `next_cursor`, the
    same fact the JSON envelope publishes as `summary.has_more`. A last page
    therefore reads `6 of 2,046 shown` and offers nothing — the slice is still
    disclosed, because requirement 34 exists so a partial result cannot read as
    a whole one, and that is true of a walk's last page as much as its first.

    **The text branch must stop printing the cursor — a deletion, not an
    omission.** `list_.py:229` currently emits
    `Next page: --cursor {result.next_cursor}` to stderr. That line *is* F10:
    the base64 token rendered at a human. This requirement removes it. Stating
    only that the token must not be *added* would leave the reported defect
    untouched, which is how a spec ships green against a live finding.

    **The text branch must not name `--cursor` at all.** The option takes a
    token value (`list_.py:117-118`) that text output no longer supplies, and
    instructing a reader to pass a flag whose value they were not given
    produces a usage error.

    **The agent path is unchanged and asymmetric on purpose.** `--cursor`
    remains the continuation over `--output json`, where
    `_continuation_command` (`list_.py:25`) already builds a complete,
    typeable invocation carrying the token and every bound filter, surfaced
    through the envelope's `actions` (`list_.py:83`). The asymmetry is the
    design: the agent receives the token because it can act on it, the human
    receives the count because that is what they asked for. Neither surface
    gets the other's answer.

    **The same defect is live on the report surface, from the other
    direction — and it is systemic, not confined to one report.** Five sites
    declare `actions` written as MCP calls:
    `service_reports.py:342` and `:400`, `balance_drift.py:295`,
    `cash_flow.py:198`, and `spending_trend.py:271`. Read them at those
    citations rather than here: quoting a tool call in a public doc binds it to
    the live schema, and this is a note about the CLI's prose, not about the
    MCP contract. `cli_register.py:155` prints every one of them verbatim to a
    CLI reader who has no such commands to type, while
    `mcp/tools/reports.py:97` sends the same list to MCP, where it is correct.
    It is this requirement's rule with the surfaces swapped: there, a token the
    human cannot use; here, a call signature the human cannot use.

    **The framework already contains the answer, applied once.**
    `inspection_hint` (`execute.py:512`) produces an action naming
    `moneybin reports explain`, and its docstring states the reason: an action
    must bind to an admitted surface, and the verify surface has no MCP
    identity. One producer follows that; five do not. Two patterns for one job
    is what the coherence rule in
    [`design-principles.md`](../../.claude/rules/design-principles.md) forbids,
    so the fix is to make the declaration carry both vocabularies rather than
    to teach the CLI to translate MCP strings.

    **Scope note (decided during implementation).** This half does *not* land
    with requirement 34's first half. Rendering it correctly means adding a
    declaration to the report contract — documented in `extension-contracts.md`
    — so one declared action renders a CLI invocation on one surface and a tool
    call on the other, across 5 definitions and 2 render paths. That is a
    contract change on top of a rendering change, and larger than the rest of
    requirement 34. It lands with the increment covering requirements 18 and
    23–25, which already reaches below the CLI into `orchestration/refresh.py` and
    `metrics/registry.py`; grouping by the layer touched keeps each review to
    one shape.

**Non-interference with data correctness (F0)**

35. No renderer deduplicates, merges, or suppresses rows. `render_rows` emits one
    line per record it is given. A regression test asserts that a payload
    containing two rows identical in every rendered column still renders two
    lines — so a future "tidier output" change cannot mask a data defect behind a
    presentation fix.

**Color as a named palette**

36. Color is declared once, semantically, in `moneybin.cli.render` — a
    positive/income color, a negative/expense color, a warning color, and a
    neutral default — and no command or renderer branch writes a Rich color
    literal inline. Requirements 14 and 15 govern *when* color appears and
    under what conditions; this one governs **where it is defined**.

    It is stated as a requirement rather than left to implementation because
    the CLI has no centralized color today — `src/moneybin/cli/` contains zero
    Rich color markup — so the render layer is being written from scratch and
    will otherwise scatter literals across every call site as it grows. Once
    that happens requirement 14 is unenforceable by inspection: verifying that
    a `magnitude` is never green means auditing every literal rather than
    reading one table. A single named palette also makes a colorblind-safe or
    high-contrast variant a one-file change instead of a survey, which matters
    because requirement 14 already commits to color being redundant with the
    sign glyph rather than load-bearing.

## Data Model

No schema changes. No migration. This spec touches presentation only.

One registry change: requirement 24's `unit` field on each metric declaration in
`src/moneybin/metrics/registry.py`. A middle draft of this spec removed it in
favour of deriving the unit from the Prometheus name suffix, on the reasoning
that the convention was already universal. It is not: `..._duration_seconds` and
`..._rate` are self-describing, but `..._batch_size`, `..._score`, `..._pending`,
`..._count`, and `..._rows_affected` name a dimension, not a unit.
`moneybin_import_batch_size` counts **files**, a fact recorded only in the
description string that `app.metrics` does not persist. Declaring the unit is
therefore the smaller of the two changes, not the larger one — derivation buys a
removed field and costs a `stats` surface that cannot label nine of its metrics.

## Implementation Plan

### Files to Create

| Path | Purpose |
|---|---|
| `src/moneybin/cli/render.py` | `render_rows`, `render_summary`, `render_note`, `format_money`, the semantic color palette (36), color/TTY gating |
| `tests/moneybin/test_cli/test_render.py` | Renderer unit tests — alignment, sign, separators, TTY gating, quiet |
| `tests/moneybin/test_cli/test_message_hygiene.py` | Requirements 16, 17, 32 — the grep-shaped audits |
| `tests/moneybin/test_cli/test_column_policy.py` | Requirements 6–10 across every `reports` command |

### Files to Modify

| Path | Change |
|---|---|
| `src/moneybin/cli/output.py` | `render_or_json` delegates its text branch to the new renderers; `--wide` joins the shared option set |
| `src/moneybin/reports/_framework/contract.py` (`ReportSpec`, line 192) | Carry the default column set as spec metadata — **this is where the column policy lives**. Parameter-aware per requirement 6 |
| `src/moneybin/reports/_framework/cli_register.py` | `build_cli_command(spec)` (line 162, called by `register_report_cli`, line 209) builds each report's Typer signature — this is where the generated `--wide` option and the `DEFAULT_COLUMNS` application land. Note `register_reports_cli`, the plural fan-out, lives in `registry.py:98` and only loops specs; it likely needs no change |
| `src/moneybin/reports/definitions/*.py` | Declare each report's `DEFAULT_COLUMNS`, `spending_trend.py` first (F1) |
| `src/moneybin/cli/commands/reports/networth.py` | The two hand-written NetworthService-backed commands; adopt `render_summary` / `render_rows` |
| `src/moneybin/cli/commands/accounts/__init__.py` | Account ID column (26); adopt `render_rows` |
| `src/moneybin/cli/commands/transactions/list_.py` | Keep rendering the account ID only — requirement 27 excludes the display name (27); rename its header `account` → `account_id`, the key `accounts list` now shares (28); **delete the `Next page: --cursor …` line at 229** and render `N of M shown` from the `total_count` already passed at line 173 (34); pass `placeholder=` so an absent category renders and is counted (29, 30) |
| `src/moneybin/sqlmesh/models/prep/int_transactions__unioned.sql` | The Plaid branch contributes `NULL::TEXT AS category` instead of `plaid_category AS category` — **this is requirement 29's boundary** (29). One line; the PFC code already flows separately as `plaid_category` |
| `src/moneybin/sqlmesh/models/core/fct_transactions.sql` | Column comment only: the `category` fallback chain no longer reaches a provider taxonomy code, and the comment said it did (29) |
| `src/moneybin/cli/commands/transactions/splits.py` | Render the shared `Uncategorized` placeholder instead of `-` — one word for one condition (30) |
| `src/moneybin/cli/commands/transactions/categorize/__init__.py` | Uncategorized queue is a Shape-5 read-projection — migrate off `render_rich_table` (1) |
| `src/moneybin/cli/commands/accounts/links.py` | `links pending` (line 55) and `links history` (line 512) hand-format an aligned table via `typer.echo`; there is no `links list` subcommand — requirement 1 applies from day one |
| `src/moneybin/cli/commands/investments/security_links.py` | `links pending` (line 42) / `links history` (line 202) hand-format the same padded-column table |
| `src/moneybin/cli/commands/transactions/notes.py` | `notes list` emits one `typer.echo` per note (line 103) |
| `src/moneybin/cli/commands/transactions/tags.py` | `tags list` emits a tab-separated tag/count list (line 150) |
| `src/moneybin/cli/commands/merchants/links.py` | `links pending` (line 34) / `links history` (line 140) — same hand-formatted-table pattern as its accounts twin; migrate both together per the coherence rule |
| `src/moneybin/cli/commands/transactions/matches.py` | `matches pending` (line 35) hand-formats a padded f-string table at lines 70-85 — the third of the three review-queue renderers |
| `src/moneybin/cli/commands/refresh.py` | Per-stage notes (18); drop function-name prefixes and `SQLMesh` (16, 17) |
| `src/moneybin/orchestration/refresh.py` | `RefreshResult` gains per-stage outcomes so the counts `_run_categorize_step` already computes reach the renderer instead of only the log (18) |
| `src/moneybin/services/merchant_links_service.py` | Line 301 — rephrase the `merchant_links_run: bound=… conflicts=…` log into prose, keeping the counts (16). The second and smaller of this spec's two below-CLI reaches |
| `docs/specs/extension-contracts.md` | Document `DEFAULT_COLUMNS` as an optional `ReportSpec` field and its width-bounded fallback (6) |
| `src/moneybin/cli/commands/system/doctor.py` | Quiet on success (20–22); recovery-action rendering unchanged per req 16's exception |
| `src/moneybin/cli/commands/stats.py` | Dimensions, units, grouping (23–25) |
| `src/moneybin/cli/utils.py` | Profile banner (19); **retire `render_rich_table`** into `render_rows` — it is the shared `rich.Table` builder req 1 supersedes |
| `src/moneybin/cli/commands/stubs.py` | Message copy (32); the parameter carries a user-facing feature name, never a spec filename |
| `src/moneybin/cli/main.py` + group modules | `hidden=True` on stub registrations, and on the four groups whose every command is a stub (31) |
| `src/moneybin/cli/commands/db.py` | Route the three `db key` stubs through `_not_implemented` and hide them, keeping their `typer.Exit(1)` (31–33). Also the `ps` process roll, printed twice from one format string — one renderer serves `ps` and `kill`'s preamble (1) |
| `src/moneybin/cli/commands/demo.py`, `fx.py`, `import_cmd.py`, `investments/{__init__,lots,prices,securities}.py` | The eight modules the audit's file list did not name, found by the guard rather than by the audit. All migrated; `_AWAITING_RENDER_ROWS` is empty and the guard that policed it is retired (1) |
| `src/moneybin/cli/commands/accounts/__init__.py`, `reports/networth.py` | The only two already-migrated commands that still rendered an empty result as a header box. Not part of the eight, but the empty-result rule holds tree-wide or not at all (1) |
| `src/moneybin/metrics/registry.py` | Add a `unit` field to each histogram declaration (24); add the three counters in Observability below |
| `.claude/rules/cli.md` | Add a "Text rendering" section pointing at this spec — the rule file is where a future contributor looks first |

### Key Decisions

**Three renderers, not a framework.** `render_rows` / `render_summary` /
`render_note` cover every text branch in the CLI today. This is deliberately not a
pluggable formatter registry: per Simplicity First, the abstraction exists to
remove ambiguity about which shape to use, not to add flexibility. If a fourth
shape is genuinely needed later, it is added then.

**Column policy lives with the command, not in the renderer.** `DEFAULT_COLUMNS`
is declared per report because only the report's author knows which columns answer
the question. A width-based auto-selector would be magic — it would silently pick
different columns on different terminals, which
`design-principles.md` → "Magic stays visible" rules out.

**Truncation is always disclosed (10).** Reporting fewer columns than exist is a
bounded view, and per the review's own standard a bounded view that does not say
so reads as complete when it is not.

**Stubs are hidden, not removed (31).** Removing them frees the namespace for a
future collision and breaks any script that tolerates the `0` exit. Hiding them
fixes the trust problem — root `--help` stops promising what it cannot deliver —
at no compatibility cost. This is a pre-launch surface per
`design-principles.md`, so the change is cheap now and expensive later.

**Sign encoding is redundant by construction (14).** Color is an enhancement, never
the encoding. This matches the design system's colorblind rule and survives the
CLI's non-TTY consumers — scripts and agents — which `cli.md` treats as peers.

**The renderers do not dedup (35).** Stated as a requirement, not left to
judgement, because F0 makes it tempting: an account appearing once per balance
source produces visibly repeated rows, and collapsing them in the renderer would
make the net-worth output *look* correct while the total stayed wrong. Rendering
is not the place to repair data — a presentation fix that hides a model defect is
worse than the defect, because it removes the symptom that would have found it.

## CLI Interface

No command is added, removed, or renamed. One new flag:

| Flag | Attaches to | Purpose |
|---|---|---|
| `--wide` | Read-only commands whose `DEFAULT_COLUMNS` is narrower than their projection | Render all columns |

`--wide` is ignored with `--output json` (which is always full), matching how
`--json-fields` is ignored with `--output text`.

Before and after for the worst case (F1). The "before" is real output at 80
columns; the "after" is a **mock** against the `demo` persona, showing the shape
of the change rather than the final column choices:

```
# before — 11 columns at 80 cols, nothing legible
│ 202… │ LOA… │ 117… │ 4    │ 0.00 │ 117… │      │ 0.00  │ 117… │       │ 391… │

# after — DEFAULT_COLUMNS (mock)
  Month     Category            Total      Txns      MoM      YoY
  2026-07   Groceries         −412.55        18    −0.04x   +0.11x
  2026-07   Utilities         −208.00         3    +0.02x   −0.06x

  6 of 11 columns shown — --wide for all
```

## MCP Interface

No change. MCP tools return the envelope; this spec governs the CLI's **text**
branch only. `render_or_json`'s JSON path — redaction, `derive_tier` stamping,
the `privacy.log` audit event — is untouched, and requirement 8 exists to keep it
that way.

## Observability

Per AGENTS.md ("Specs touching app code must include metrics") and
[`observability.md`](observability.md). Three counters, registered in
`src/moneybin/metrics/registry.py` alongside the existing declarations and
following the same Prometheus naming convention the rest of the registry
uses:

```python
CLI_WIDE_REQUESTED_TOTAL = Counter(
    "moneybin_cli_wide_requested_total",
    "Times --wide was passed, by command",
    ["command"],
)

CLI_COLUMNS_OMITTED_TOTAL = Counter(
    "moneybin_cli_columns_omitted_total",
    "Times a text render omitted columns from the full projection, by command",
    ["command"],
)

CLI_STUB_INVOKED_TOTAL = Counter(
    "moneybin_cli_stub_invoked_total",
    "Times an unimplemented command was invoked, by command",
    ["command"],
)
```

What each is for — a metric with no question behind it is noise:

- **`wide_requested` vs. `columns_omitted`** flag a report whose `DEFAULT_COLUMNS`
  may be wrong: one whose `--wide` rate approaches its omission rate is hiding
  something readers want. Read as a pointer, not a verdict — see the persistence
  constraint below.
- **`stub_invoked`** answers which hidden stubs users still reach (requirement
  31 hides them from `--help` but keeps them invocable), which is the demand
  signal for implementing one — and detects any surviving path that still
  advertises a stub.

No metric records row contents, amounts, or identifiers — only counts and the
command label, per the PII-in-logs rule.

**These counters persist only on sessions that also write business data.**
`flush_metrics()` returns without flushing when `database_was_written()` is false
(`src/moneybin/observability.py:116`), so a read-only `reports` or stub
invocation discards its observations at exit. That is deliberate and stays:
turning an otherwise read-only command into a write-lock holder is not a trade
this project makes for telemetry, and `--wide` is common enough that the lock
would land on nearly every read. The consequence is stated rather than worked
around: the `--wide`-versus-omission ratio is a **directional** signal drawn from
write-bearing sessions, not a census, and it does not by itself settle whether a
report's `DEFAULT_COLUMNS` is right. Requirement 9's contract test is what
*enforces* the column policy; these counters only suggest where to look next.

## Testing Strategy

Per `.claude/rules/testing.md` and the project's TDD requirement, each numbered
requirement gets a failing test before its implementation.

**Unit** — `format_money` across all four money kinds (`flow` / `magnitude` /
`delta` / `balance`), asserting a spend `magnitude` never renders green and a
positive `delta` on an expense column renders `--neg-expense` (12, 14); TTY and
`NO_COLOR` gating (15); and the stream/suppression matrix — `-q` suppresses
notes, never rows, and never result framing, which lands on stdout (4, 5, 10).
The redirect case is its own assertion: capturing stdout alone must still
contain the omission line.

**Contract, parameterized across every command** — this is where coherence is
actually enforced, and the tests must enumerate commands from the live registry
rather than a hand-maintained list, so a new command cannot silently skip the
contract:

- Every read-only command's text branch renders through one of the three
  renderers (1).
- Every `reports` command's default column set renders within **80** columns with
  no header elided (9) — the same threshold requirement 9 sets, and the width F1
  was reproduced at. Parameter-aware sets are exercised across every legal
  parameter combination, not just the default one (6).
- **Renderer-authored** output — static status copy, labels, headers, notes, and
  result framing — contains no **multi-segment `snake_case` identifier** and no
  `key=value` fragment outside a recovery action (16). The audit is scoped to
  strings the renderer owns and **excludes table-cell payloads**: a transaction
  description reading `order_id=123` or a user-supplied snake_case category label
  is the user's own data, and an assertion over all rendered bytes would demand
  mangling financial data to satisfy a contract about status copy. The test also
  carries requirement 16's exceptions verbatim — single-word registered tool names
  (`accounts`, `reports`) and `RecoveryAction` lines are **not** matched. Stating
  the assertion more broadly than the requirement would fail on legitimate output
  like `Accounts: 5`.
- No reachable message contains `docs/specs/` (32).
- No renderer or command module contains an inline Rich color literal; every
  color resolves through the named palette (36). This is a source-shaped
  assertion, so it needs the behavioural partner below — a palette can be
  declared and then bypassed at one call site without the source scan
  noticing, if the literal is spelled as a style string rather than a color
  name.
- No stub command appears in any `--help` output, **and every
  partially-implemented command still does** (31). Both directions are required:
  the one-sided assertion passes trivially against an implementation that hides
  everything matching `_not_implemented`, which is exactly the over-broad reading
  requirement 31 rules out. The same pairing applies one level up — a group whose
  every command is a stub is hidden, **and `db key`, which holds both stubs and
  working commands, is not.**

**Regression, F-numbered** — one test per finding, each written to fail against
today's code. Two need specific shapes:

- **F6** — a doctor run with one failing invariant must print that invariant and
  not the 48 passing ones. A fixture where everything passes cannot distinguish
  quiet-on-success from a renderer that prints nothing at all.
- **F8** — two tiers, because requirement 29's boundary moved below the CLI.
  At the data layer, `test_categorize_plaid_e2e.py` asserts an uncategorized
  Plaid row carries `category IS NULL` — **and reaches
  `core.uncategorized_queue`**, the behavioural partner that proves the raw code
  was not merely hidden from a column but was emptying the curation queue. Two
  assertions in that file previously pinned the defect
  (`assert row[0] == "FOOD_AND_DRINK", "raw Plaid category text still passes
  through"`); they were incidental to what the test exists to prove, and now
  assert the NULL. At the CLI, a fixture containing both a mapped and an unmapped
  category asserts the column is homogeneous, the unmapped row is counted in the
  result framing — not a `render_note`, which requirement 4 suppresses under `-q`
  — and that `--output json` still carries the NULL. A fully-mapped fixture
  passes trivially.
- **F10** — three assertions, because requirement 34 adds, deletes, and gates.
  The first: a paged text run renders `N of M shown` with `M` equal to
  `total_count`, not to the page length, which requires a fixture whose match
  count exceeds its `--limit`. The second: **no text-branch output contains
  the cursor token**, asserted against stdout *and* stderr together, because
  the line being deleted (`list_.py:229`) writes to stderr — an assertion over
  stdout alone passes against the unfixed code. A third followed from the gate
  above: a result with `next_cursor=None` and a `total_count` above the page
  length still frames the slice and offers no `--limit`, which no fixture
  asserting only the common case can distinguish from the ungated line.
- **F9 / req 36** — the behavioural partner to the source scan above: render a
  `magnitude`, a negative `flow`, and a negative `balance` with color forced
  on, and assert the emitted ANSI codes match the palette's declared values.
  This catches a bypassed palette that the source scan misses.

**Gate.** Most of these are CLI-surface tests in `tests/moneybin/test_cli/`,
where `make check test` is correct. **Requirement 29 is the exception and
changes the answer:** it edits a `prep` model that feeds `core.fct_transactions`,
which is a data shape, so the change carrying it also runs
`make test-integration` and `make test-scenarios` per AGENTS.md's blast-radius
rule. The original note here read "no scenario-suite run is required — no data
shape changes," which was true of the renderer-boundary design and false of the
one that shipped.

## Synthetic Data Requirements

None new. The existing demo persona exercises every renderer. The F8 test needs a
category with no display mapping, which is a fixture concern rather than a
generator one.

## Dependencies

None added. Rich is already a dependency and already renders
`transactions list`.

## Out of Scope

- **F0**, `reports networth` summing an account once per balance source. A
  correctness defect in the model, tracked separately and fixed on its own branch
  with its own test. Requirement 35 keeps this spec from masking it.
- **A TUI or any second front-end.** Considered and declined: a persistent
  full-screen app serves humans only, while `cli.md` treats scripts and agents as
  peer consumers of the same surface. Nothing here forecloses one later.
- **New reports or report capabilities** — charts, sparklines, financial-health
  projections, spending-flexibility tagging, regex search, local backups.
  Considered and declined for this increment; each is additive to a coherent
  render layer rather than blocked by its absence.
- **Command taxonomy.** `moneybin-cli.md` owns it and is unchanged.
- **Implementing any stubbed command.** F4 is about visibility only.
