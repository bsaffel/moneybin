---
description: "CLI development: Typer patterns, error handling, command registration, non-interactive parity"
paths: ["src/moneybin/cli/**", "src/moneybin/main.py"]
---

# CLI Development

**Surface-shape rules:** [`surface-design.md`](surface-design.md) — operation-shape taxonomy, verb vocabulary, audience layering. Cross-surface (governs CLI, MCP, and future REST endpoints). Consult before adding, renaming, or restructuring a command.

## Core Principle

CLI commands are **thin wrappers** around tested business logic. Delegate complex work to business logic classes.

**Enforcement:** `tests/moneybin/test_architecture/test_adapter_layering.py` fails CI when CLI commands (or MCP tools) import write-callable symbols from `moneybin.loaders`, `moneybin.extractors`, or `moneybin.matching` without an allowlist entry. The fix is almost always a new service method; allowlist entries are reserved for pure constants, pure read helpers, DI targets, and type/format descriptors, each with a `# why` comment.

## Consumer Model

CLI serves three peer consumers, not just one:

1. **Humans at terminals** — the obvious case. Get nice output, helpful errors, interactive prompts where useful.
2. **Shell scripts and pipelines** — `--output json` + `jq`, exit codes, stdout/stderr separation.
3. **AI agents** — Claude Code, Codex CLI, Gemini CLI, and similar agents drive CLI commands directly as a peer pathway to MCP. They pipe and chain commands the way humans use shells, and parse JSON output the way scripts do.

**The CLI is a first-class agent surface, not a fallback for users without MCP.** When MoneyBin offers a capability via MCP, it ships with a CLI equivalent (per `mcp.md` principle 5) — and that CLI is designed for both humans and agents from the start.

What this means in practice:

- Data primitives (export commands, file-based inputs, stdin/stdout JSON) are designed once and serve all three consumers.
- Redaction contracts apply identically across CLI and MCP — never assume CLI users are "trusted enough to skip redaction."
- Every interactive prompt must have a flag equivalent (see Non-Interactive Parity below) — agents cannot navigate prompts.
- `--output json` returns the same envelope shape MCP returns (see `mcp.md` Response Envelope).

When designing a new command, ask: "Could an agent drive this end-to-end without a human?" If not, redesign — that's a flag-equivalence gap or a JSON-output gap, not an acceptable limitation.

## Standard Pattern

```python
@app.command("command-name")
def command_function(
    output: OutputFormat = output_option,
    source_path: Path = typer.Option(..., help="Description"),
) -> None:
    """Clear command description."""
    with handle_cli_errors(cli_actor="command_name", payload_type=CommandPayload):
        with get_database(read_only=True) as db:
            result = BusinessClass(db).main_operation(source_path)
    render_or_json(build_envelope(data=result), output, cli_actor="command_name")
```

## Error Handling

`handle_cli_errors` is the single error boundary. It runs every exception
through `classify_user_error` (`src/moneybin/errors.py`), exits 1 with the
classified message — or a structured error envelope under `--output json` — and
re-raises whatever the classifier does not recognize, so a programmer error
still surfaces as a failure. `typer.Exit` passes through untouched.

- **Do not re-implement it per command.** `DatabaseKeyError`,
  `DatabaseNotInitializedError`, `DatabaseLockError`, `SchemaDriftError`, the
  `moneybin.secrets` families, `FileNotFoundError`, `PermissionError`, and bare
  `ValueError` / `LookupError` are already classified, with hints and recovery
  actions attached centrally.
- **Catch a specific exception only to do something the classifier cannot** —
  recover, fall back, or add context only the failure site holds. Then raise
  `UserError(...)` with the right `error_codes` constant rather than logging a
  bare message: a write site that means "invalid" or "not found" names a
  `MUTATION_*` code the classifier is not positioned to infer (see the note in
  `error_codes.py`).
- **A new exception family needs a `classify_user_error` branch**, or it reaches
  the user as a traceback and an agent as `infra_unclassified_error`.
  `tests/moneybin/test_errors/test_exception_family_coverage.py` enumerates the
  families off their modules and fails when one drifts out of coverage.
- Use `raise typer.Exit(code) from e` for early exits the classifier should not
  see (mutually exclusive flags, a declined prompt).
- Exit codes: 0 = success, 1 = general error, 2+ = command-specific.

## Secrets in Error Output

Recovery messages containing keys, tokens, or credentials must go to stderr via `typer.echo(..., err=True)` — **never through `logger.*()`**. The log pipeline persists to files and hex keys won't match PII regex patterns.

## Multi-State Operations

When a command modifies multiple persistent stores in sequence (e.g., file move + keychain update), wrap later steps in try/except with recovery guidance: tell the user what state they're in, where the backup is, and don't delete backups until all steps succeed.

## Command Group Registration

- **Workflow ordering**: Top-level commands in `main.py` are registered in workflow order: setup → ingest → enrich → pipeline → analyze → output → integrations → ops. New commands should be inserted at the appropriate workflow stage.
- **`no_args_is_help=True`**: Every `typer.Typer()` *group* must set this flag so bare invocation shows help text consistently. Leaf commands (registered via `app.command()` directly on the root app, like `stats` and `logs`) follow a different convention — see "Leaf Commands vs Sub-Groups" below. Do not use `invoke_without_command=True` callbacks as a substitute — that flag runs the callback even when a subcommand is provided, causing confusing side effects like duplicate setup or output.
## Cold-Start Hygiene

Every E2E test, every shell autocomplete, and every CLI invocation pays the full module-import cost for `moneybin.cli.main`. Keep that path light.

- **Defer heavy transitive imports inside command bodies.** `fastmcp`, `sqlmesh`, `polars`, and similar (anything that pulls in a parser, ORM, or large package graph) must not be imported at module top in `src/moneybin/cli/commands/*` or any module those imports load. Put the import inside the function that uses it:

  ```python
  @app.command("serve")
  def serve(...) -> None:
      from moneybin.mcp.server import build_server  # noqa: PLC0415 — defer import
      build_server(...).run()
  ```

- **Verify with `importtime`.** When adding a new command module, confirm the cold-start path stays clean:

  ```bash
  uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | grep -iE "<heavy-dep>"
  ```

  Should produce no output for `fastmcp`, `sqlmesh`, or `polars`.

## Leaf Commands vs Sub-Groups

A **leaf command** is a top-level command with no subcommands (e.g., `moneybin stats`, `moneybin logs <stream>`). A **sub-group** is a `typer.Typer()` parent with multiple registered actions (e.g., `moneybin db ...`, `moneybin import formats ...`).

**Choose leaf when:**
- The command represents a single action with no plausible siblings (`stats`, `logs`).
- Auxiliary modes can be expressed as flags (`--print-path`, `--prune`) without crowding help text.

**Choose sub-group when:**
- 2+ distinct actions exist on the same noun (`db key {show,rotate,export,import,verify}`, `import formats {list,show,delete}`).
- Future actions are likely (reserve the namespace).

**Naming convention for leaf functions:** Free-function leaf commands use `<name>_command` (e.g., `stats_command`, `logs_command`) to avoid shadowing the surrounding module name. Sub-group commands continue to use `<group>_<verb>` (e.g., `db_key_show`, `db_key_rotate`).

**Required arguments for leaf commands:** Leaf commands MAY require arguments and exit non-zero (code `2`) with a usage error when invoked bare. This is the convention of `docker logs CONTAINER`, `kubectl logs POD`, `tail FILE`. The `no_args_is_help=True` rule applies to **groups**, not leaves; a leaf with required positionals must surface a usage error, not help, so scripts can detect mis-invocation.

## Help Surface Contract

`--help` and `-h` MUST be **side-effect free**. They MUST NOT:

- Trigger first-run wizards
- Read or write profile data
- Open database connections
- Hit external services

`main_callback` (in `src/moneybin/cli/main.py`) MUST stay inert — only register the lazy profile resolver, never call `resolve_profile()` directly. Help paths exit before any command body runs, so the lazy path is what keeps them side-effect free.

## Exit Codes & stderr

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Runtime error (operation ran and failed: file not found, DB locked, API 500) |
| `2` | Usage error (missing arg, invalid flag, unknown subcommand, bad argument value) |

Diagnostic output (errors, warnings, progress, status) goes to **stderr** (fd 2). Data output (rows, JSON, the thing the user asked for) goes to **stdout** (fd 1). Help text from `--help` goes to stdout — it's documentation the user requested, and pipes (`| less`) must work.

Use `typer.echo(msg, err=True)` for direct error echoes. The project logger's `StreamHandler` already targets `sys.stderr` (see `src/moneybin/logging/config.py`). `logger.error()` and `logger.warning()` reach fd 2; `logger.info()` may reach either as long as it doesn't pollute scripts capturing stdout. Locked by `tests/moneybin/test_cli/test_error_routing.py`.

### Keeping the console readable

WARNING and above always reach the console. Below that, a record is hidden only
if its logger matches `_CONSOLE_SUPPRESSED_PREFIXES` in `logging/config.py`.
Everything else prints. That list holds two kinds of entry — third-party
libraries that narrate every call (`sqlmesh`, `httpx`, …) and MoneyBin modules
whose INFO is per-run bookkeeping the log file should keep and the terminal
should not. Read the constant for the current membership; each entry carries
the reason it earned a place.

**`logger.debug` is not "hide from console" — it is "drop everywhere."** The
root logger sits at INFO, so a DEBUG record is never emitted and never reaches
the log file either. That distinction decides which of two tools to reach for:

| The line is… | Use | Why |
|---|---|---|
| Already said by a `typer.echo` or another surviving INFO line | `logger.debug` | The file keeps the other copy. `sync pull` reported its categorization total three times from three layers. |
| Detail worth keeping in the file, but in the subsystem's vocabulary rather than the user's | denylist prefix | The file keeps it; the console does not. "Tier 4: 5 potential transfers found" — the user has no tiers; "Loaded 5 Plaid accounts" — their Chase card is not a Plaid account. |

Getting this backwards is easy and quiet: demoting the per-tier match counts to
debug looked like console cleanup, but `MatchResult.summary()` reports only
run-wide totals, so the per-tier split left the log file entirely. Before
demoting, name the other place the information survives.

**`typer.echo` does not reach the log file.** It writes to stderr directly. A
count that exists only in a `typer.echo` is absent from the log, which is why
the Plaid row counts stay at INFO behind a denylist prefix instead.

**A denylist is the deliberate choice here.** An allowlist would be quieter as
new dependencies arrive, but it inverts the default for ~168 `logger.info` sites
and turns every one whose output a user needs into a silent regression — MCP's
host stderr, `log_to_file: false`, schema-migration progress, and "your
`--institution` flag was ignored" each broke that way when it was tried in #356.
What must be hidden is enumerable; what must stay visible is not.

**Adding a prefix hides it from every stream at every level**, including
`--verbose` — but only while a log file exists to hold the copy. Under
`log_to_file: false`, or when the log directory is missing, stderr is the only
sink and the filter stands down entirely, because
`docs/guides/observability.md` and `threat-model.md` both promise stderr is
unaffected by that setting.

Locked by `tests/moneybin/test_logging_config.py::TestConsoleNoiseFilter`, which
checks both directions — denylisted prefixes are hidden, an unnamed logger still
prints — and by `TestMcpStreamKeepsInfoOnStderr` for the host channel.

## Standard Flags on Read-Only Commands

Every command that **reads but does not mutate** state MUST accept:

- `-o, --output {text,json}` — output format. `text` is human-readable, `json` is machine-readable. The `json` branch must serialize the same **records and values** the text branch displays: the same rows, the same amounts, the same masking. It may carry more *fields* — where a text table renders a declared subset of its columns, JSON still carries every one (see `--wide` below). Narrowing is a reading aid for a terminal, never a difference in what the two branches know.
- `-q, --quiet` — suppress informational output (status lines, progress, `✅`). Result rows are NEVER suppressed by `-q` — they are the data.
- `--wide` — on a command whose text table renders a declared subset of its columns, restore the full projection. Text-only: `--output json` always carries every column. A command that renders everything by default does not need it.
- `--json-fields` — comma-separated field projection for `--output json` (e.g. `--json-fields id,date,amount`). Only applies when `--output json` is active; silently ignored otherwise. Added progressively as each read-only command is extended — declare as `json_fields: str | None = json_fields_option` and pass to `render_or_json(json_fields=json_fields)`. Commands that implement it MUST enumerate available field names in their `--help` text (e.g. `"Available fields: id, date, amount, description, category, account_id"`).

`db query` extends `--output` to `text|json|csv|markdown|box` since DuckDB's CLI supports all five natively.

A read-only command that pages MUST expose `--cursor` carrying the shared keyset
envelope from `moneybin.protocol.pagination` — never an offset. Offset paging
skips a row when anything above the boundary is deleted and repeats one when
anything prepends, and on a ledger both are silent. See `mcp.md` → Pagination
for the binding rules; they are cross-surface, not MCP-specific.

**Operator-bypass banner on direct-DB commands.** `db query`, `db shell`, and `db ui` are direct database access with no privacy middleware — CRITICAL-tier fields (account/routing numbers) are NOT masked. Each command emits a banner on stderr at invocation and includes the banner text in its `--help` output, directing operators to `moneybin sql query` for the privacy-safe MCP-backed path. Agents should use the `sql_query` MCP tool or `moneybin sql query` CLI command, not `moneybin db query`, when privacy enforcement is required.

This makes every read command pipeable into `jq`, scripts, and AI agents. Audit-tested by `tests/moneybin/test_cli/test_cli_output_quiet.py`.

## Text rendering

Text output goes through `moneybin.cli.render` — never a `rich.Table` built at
the call site, never a hand-padded f-string column. Three renderers, one per
shape of result:

| Shape | Renderer | Stream | `-q` |
|---|---|---|---|
| A collection of records | `render_rows(columns, rows, money=...)` | stdout | never suppressed |
| A labelled scalar block | `render_summary(pairs, title=...)` | stdout | never suppressed |
| An informational status line | `render_note(message, quiet=..., warn=...)` | stderr | suppressed |

Neither result renderer takes a `quiet` parameter, so there is no way to route
data through this module and have it silenced.

A command that accepts `-q` must forward it: `render_note` defaults to
`quiet=False`, so a dropped flag is a flag that silently does nothing. Forward
it to the chatter only — a next-step hint, a progress line, a `✅`. A statement
about how far the numbers can be trusted (truncated, degraded, converted)
keeps printing under `-q`, because asking for less chatter is not a claim that
the truncation stopped.

**Amounts.** `format_money` is the only place an amount becomes text, and every
money column declares a **money kind** — `flow`, `magnitude`, `delta`, or
`balance` — that decides its sign glyph and colour. The renderer never reads
meaning off the raw number: `spending_trend.total_spend` is `SUM(ABS(amount))`,
so colouring on sign alone would render spending as green income. Pass the
declaration as `render_rows(..., money={"amount": Money("flow")})`; a report
declares it on its `OutputColumn` instead and the framework passes it through.

**Narrowed tables.** A report declares `default_columns` on `@report` — the
columns a text reader sees before `--wide` — and the generated command resolves
it against the result. When anything is omitted, `render_rows` prints one
result-framing line to **stdout** beneath the table (`4 of 11 columns shown —
--wide for all`), which `-q` never suppresses: routing it to stderr or silencing
it would let a redirected file record a truncated table that reads as whole.
Pass `total_columns=` to `render_rows` to get that line; leave it out and
nothing is framed.

**Colour** is defined once, semantically, as `render.Style` — no colour literal
belongs at a call site — and is emitted only when stdout is a TTY and `NO_COLOR`
is unset. The sign glyph is always present, so the encoding survives a pipe.

Three guards in `tests/moneybin/test_cli/test_render.py` enforce this
structurally: Rich may be imported only by `render.py`, no `typer.echo` outside
it carries an alignment format spec, and nothing calls `typer.secho`/`typer.style`.

**No module is exempt.** The second guard carried an `_AWAITING_RENDER_ROWS`
set for the eight modules the audit's file list did not name; all eight have
migrated and the set is gone. Every CLI module is held to the rule
unconditionally.

**A per-unit price is not an amount.** `fx list`'s rate, `investments prices
list`'s close, and `investments holdings`' average cost are stored to ten
decimal places, and `format_money` rounds to two — routing a sub-cent price
through it renders `0.00`. Those columns declare no `Money` and print as
stored. Requirement 11 governs amounts: a transaction total, a balance, a
gain. If the column answers "how much is this worth", format it; if it
answers "what does one unit cost", do not.

**Curate a wide table; do not let the renderer measure one.** A list command
with more columns than an 80-column terminal holds declares its columns once as
`(name, extractor)` pairs, a `_DEFAULT` subset, and takes `--wide` — the same
shape `reports` uses, via `column_view` in `render.py`. Header and rows are then
derived from one declaration, so a moved column cannot desynchronize them.

Reach for `render_rows(fit=True)` only where no author judgement exists to
encode: it keeps the first and last columns, which on `investments holdings`
elides `market value` — the figure the command exists to report. The default
set is curated rather than measured because width knows nothing about meaning.

The reason either is needed: Rich folds an over-narrow cell, and a folded
amount is *misread*, not merely ugly — `1,200.00` becomes `1,200.` above `00`.
Folding an identifier is the accepted degradation; folding a number is the bug.

`render_rows` already holds that line for you, and you get it by declaring the
column rather than by touching Rich: a declared column is `no_wrap=True` with
`overflow="ellipsis"`, so Rich spends a narrow terminal's squeeze on the *text*
columns first and numbers survive whole.

**Two declarations, because formatting and atomicity are different questions.**
`money=` says how to render an amount — two places, a sign glyph, a colour.
`numeric=` says only that the cell is a number and must not break across lines.
A per-unit price, a share count, an FX rate and a match score all belong in
`numeric=` precisely *because* they are excluded from `money=` — rounding a
`DECIMAL(28,10)` close to two places would print `0.00` — and that exclusion
used to take the no-fold guarantee with it. Every column holding a bare number
declares one of the two; there is no third state. Do not copy
`no_wrap=True` onto the text columns to make a table fit — applied to every
column it leaves Rich nothing wrappable to give up, and it then crops
`1,234,567.89` to `1,234`, which is the same misread by another route. The
ellipsis is the floor under that: a cell too narrow even after the squeeze
reads `1,234,5…`, which cannot pass for a whole number.

Curation is still the answer for a table that is simply too wide. The renderer
guarantees no amount is *wrong*; only the author can decide which columns are
worth showing, and a nine-column projection squeezed into 80 is legible in the
sense that nothing lies and unreadable in every other sense.

**Guard an empty result.** `render_rows([], ...)` draws a header box with no
rows under it, so a command that renders unconditionally prints a table for a
result that has nothing in it — where the `for` loop it replaced printed
nothing at all. Wrap the call in `if rows:`, as `render_report_result` does
with `if result.records:`. The guard looks removable and is not.

Full contract: [`cli-output-coherence.md`](../../docs/specs/cli-output-coherence.md).

## Conventions

- Kebab-case for command names
- Clear help text for all commands and options
- Progress updates for long operations

## Non-Interactive Parity

Every interactive prompt (confirmation, selection, wizard step) must have a flag equivalent that expresses the same intent in a single invocation. AI agents and scripts cannot navigate interactive prompts.

- **Confirmations** → `--yes` / `-y` to auto-accept
- **Field selection** → named flags (e.g., `--date-col=X`, `--amount-col=Y`)
- **Declining/skipping** → `--skip` or equivalent
- **Multi-step wizards** → each step's choice expressible as a flag; all flags combinable in one invocation

Combined with `--output json` (see `mcp-architecture.md` §7), this makes every CLI command fully automatable by AI agents (Claude Code, Codex) and shell scripts.

## Icon Usage

Use icons **sparingly** — only where they add scanability, not decoration.

| Signal | Icon | When to use |
|--------|------|-------------|
| Success | `✅` | Final line of a successful action command |
| Error | `❌` | `logger.error(...)` messages |
| Warning | `⚠️` | `logger.warning(...)` messages |
| Working | `⚙️` | Start of a long-running operation (sync, load, transform) |
| Hint | `💡` | Optional follow-up tips after an error |
| Bug report | `🐛` | Link to issue tracker after an unexpected error |
| Review | `👀` | Items that need user attention or review |

Do **not** add icons to ordinary informational log lines (paths, counts, results rows). Query/display commands (`status`, `stats`, `list-*`) don't need a trailing ✅ — they just display data. No decorative icons (📈📊📁) — only the semantic icons in the table above.
