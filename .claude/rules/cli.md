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
      from moneybin.mcp.server import build_server  # fastmcp is not cold-start cheap
      build_server(...).run()
  ```

  Say *why* in a plain comment, and never reach for `# noqa: PLC0415`: ruff's
  `select` omits `PL`, so the marker suppresses nothing and only looks
  official — which is how 17 wrong justifications rode unchallenged until
  MB-168 tested them. `RUF100` now rejects any inert `noqa` in `ruff check .`.

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

WARNING and above always reach the console. Normal CLI invocations suppress
routine INFO and DEBUG records; `--verbose` restores diagnostics. File handlers
remain unfiltered, and non-CLI streams retain their own console behavior.

**`logger.debug` is not "hide from console" — it is "drop everywhere" unless
`--verbose` enables it.** Before demoting a line, name the result or recovery
presenter that preserves the user-facing fact. `typer.echo` is not a durable
log record, so use INFO when the detail belongs in configured file logs and
make it visible with `--verbose`. Command results never depend on either INFO
or DEBUG. Locked by `tests/moneybin/test_logging_config.py::TestConsoleNoiseFilter`.

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

**Terminal policy** belongs to `moneybin.cli.terminal`. It resolves actual
stdin/stdout/stderr capabilities and the supplied `CLISettings` without loading
a profile or database. JSON disables human presentation; reduced motion keeps
static stage labels while disabling animation; quiet suppresses those labels.
`NO_COLOR` disables every style, including bold. `render.Style` names semantic
roles (hierarchy, context, action, and states) with terminal palette names; no
colour literal belongs at a call site. Rich may appear only in the centralized
presentation helpers (`render.py`, `terminal.py`, and `progress.py`), never in a command-local
renderer. The sign glyph is always present, so the encoding survives a pipe.

`TerminalPolicy.symbols` provides `✓`, `!`, `×`, and `›` with `OK`, `!`, `X`,
and `>` ASCII fallbacks. Use its `minus` when a terminal-facing formatter needs
the portable hyphen-minus; preserve the number and sign meaning. Commands with
legacy direct human output carry the searchable
`DEPRECATED: direct-human-output` marker until they migrate through the shared
presentation boundary.

Three guards in `tests/moneybin/test_cli/test_render.py` enforce this
structurally: Rich may be imported only by centralized presentation helpers,
no `typer.echo` outside them carries an alignment format spec, and nothing calls
`typer.secho`/`typer.style`.

**No module is exempt** — every CLI module is held to these three guards
unconditionally. The `_AWAITING_RENDER_ROWS` set that once carried eight
unmigrated modules is gone; do not reintroduce a waiting list.

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

## Terminal symbols and logging

Normal CLI logging shows warnings and errors only; `--verbose` adds routine
diagnostics. Command results belong to their stdout presenter and progress to
stderr, so a result must never depend on `logger.info`. This stays true when
file logging is unavailable. File handlers retain their configured records.

Use the active `TerminalPolicy` symbols for shared error and progress helpers:
`OK`/`X`/`>` in ASCII mode and their functional Unicode counterparts otherwise.
Do not hard-code pictographic prefixes in new terminal output. Essential
recovery text goes directly to stderr and JSON commands keep stdout exclusively
for their structured document or envelope.
