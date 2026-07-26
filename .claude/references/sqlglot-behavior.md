# sqlglot behavior that surprises — traversal order and SQLMesh's tokenizer patch

On-demand companion to [`.claude/rules/database.md`](../rules/database.md). Read
this before walking a parsed SQL tree, matching sqlglot node types, or pairing
values with placeholders. Both facts below would silently mis-bind values in code
that pairs a value to a placeholder by tree position, or that matches one
placeholder node type. No current call site does either — treat this as the
design to follow, not a bug report.

Both claims re-verified by direct measurement on sqlglot 30.8.0 (2026-07-26);
the reproductions below are the actual observed output, not illustrations.

## No tree walk yields source order

`Expression.find_all()` calls `walk(bfs=True)`, so nodes arrive
**breadth-first**. `bfs=False` does not fix it — DFS follows sqlglot's own arg
order, which visits a `Select`'s `limit` before its `where`:

```
SELECT a FROM t WHERE x >= ? AND y = ? LIMIT ?
                          ^0        ^1       ^2   <- source-text order

placeholder parents, in visit order:
  find_all / walk(bfs=True)  -> Limit, GTE, EQ    == 2, 0, 1
  walk(bfs=False)            -> Limit, GTE, EQ    == 2, 0, 1
```

The `LIMIT` placeholder is visited **first**, ahead of both `WHERE` predicates.

**Consequence.** DuckDB binds positional `?` in *source text* order, so anything
pairing a value to a placeholder by counting a tree walk mis-slots them. That is
the silent mis-binding named in `reports-dynamic.md` R8, reached through the
traversal rather than through storage.

**Use the tokenizer instead.** `sqlglot.tokenize(sql, read="duckdb")` yields
tokens in text order with `start`/`end` offsets, plus three properties the tree
lacks:

- a `?` inside a string literal is a `STRING` token, not a `PLACEHOLDER`, so a
  naive textual scan's worst bug is impossible;
- `$name` is `PARAMETER('$')` plus the name token, and token types and offsets
  are **identical whether or not SQLMesh has been imported** — unlike the parse
  tree (next section);
- splicing by offset preserves the author's formatting, so a rendered query stays
  recognizable instead of being regenerated.

Splice right-to-left so earlier offsets stay valid. **No precedent to copy:**
`sqlglot.tokenize` appears nowhere under `src/moneybin/` — every current call
site parses instead. The first tokenizer-based splice will be establishing this
pattern, not following it.

## Importing SQLMesh changes how `$name` parses, process-wide

`sqlglot.parse_one("… = $acct", dialect="duckdb")` yields **two different node
types** depending on whether `sqlmesh` has been imported into the process:

- bare sqlglot → `exp.Placeholder(this="acct")`
- after any `import sqlmesh` → `exp.Parameter(this=exp.Var(this="acct"))`

SQLMesh's dialect extensions claim `$name` for macro parameters and patch the
tokenizer **process-wide**, retroactively for every later parse. Both shapes
render back to `$acct`, so generated SQL looks identical and only a node-type
check sees the difference. Eleven modules under `src/moneybin/` import SQLMesh —
among them `database.py` (`sqlmesh_context`), `schema.py`, `migrations.py`,
`cli/commands/transform.py`, and `privacy/report_class_derivation.py` — so which
shape a process sees depends on import order, not on anything visible at the
call site.

**How to apply:**

- Any `find_all` / `isinstance` over placeholder nodes must match **both**
  `exp.Placeholder` and `exp.Parameter`, reading the name through a helper
  (`Placeholder.this` is the bare string; `Parameter.this` is a `Var`). No
  placeholder walk exists yet — every current `find_all` in `privacy/` targets
  `exp.Table` / `CTE` / `Subquery` / `Column` / `Select` / `AggFunc`. For the
  node-tuple pattern to copy, see `_OPAQUE_PROJECTION_NODES`
  (`privacy/sql_lineage.py:913`), spread into `find_all` at `:941`.
- Write the guard by **constructing both node shapes explicitly** and swapping
  them into a parsed tree — never by trusting what the current process imported,
  or the test asserts one shape and silently skips the other.
- **Suspect this whenever a sqlglot-walking test passes on its own file and fails
  only in the full suite.** Any test whose module imports sqlmesh flips the shape
  for every later parse in that process. Bisect by pairing the target test with
  one other **file** at a time — a per-test bisect finds nothing, because no
  single test causes it, the import does. Note that
  `privacy/report_class_derivation.py` imports sqlmesh itself, so inside
  `privacy/` the patched `Parameter` shape is the *normal* one once that module
  loads; bare `Placeholder` is what you get only in a process that never
  touched it.
- **A missed node type does not fail loudly.** Both shapes render back to
  `$acct` byte-identically, so generated SQL looks correct and only a node-type
  check sees the difference; an unmatched placeholder silently takes whatever
  default the caller applies. Before assuming fail-closed makes that benign, ask
  what else consumes the match — in `privacy/`, an unresolved node reaches
  `DataClass.UNRESOLVED` (`privacy/redaction.py`), which masks WHOLE.
