# sqlglot behavior that surprises — traversal order and SQLMesh's tokenizer patch

On-demand companion to [`.claude/rules/database.md`](../rules/database.md). Read
this before walking a parsed SQL tree, matching sqlglot node types, or pairing
values with placeholders. Both facts below produced defects that rendered or
executed SQL claiming values it never used.

Measured on sqlglot 30.8.0 / DuckDB 1.5.4 (2026-07-26).

## No tree walk yields source order

`Expression.find_all()` calls `walk(bfs=True)`, so nodes arrive
**breadth-first**. `bfs=False` does not fix it — DFS follows sqlglot's own arg
order, which visits a `Select`'s `limit` before its `where`:

```
SELECT a FROM t WHERE x >= ? AND y = ? LIMIT ?
  bfs=True  -> 1, 2, 0     # by find_all order
  bfs=False -> 1, 2, 0
```

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

Splice right-to-left so earlier offsets stay valid. Live example:
`reports/_framework/provenance.py::_slots`.

## Importing SQLMesh changes how `$name` parses, process-wide

`sqlglot.parse_one("… = $acct", dialect="duckdb")` yields **two different node
types** depending on whether `sqlmesh` has been imported into the process:

- bare sqlglot → `exp.Placeholder(this="acct")`
- after any `import sqlmesh` → `exp.Parameter(this=exp.Var(this="acct"))`

SQLMesh's dialect extensions claim `$name` for macro parameters and patch the
tokenizer **process-wide**, retroactively for every later parse. Both shapes
render back to `$acct`, so generated SQL looks identical and only a node-type
check sees the difference. MoneyBin imports SQLMesh on several paths
(`transform`, `doctor`, `sqlmesh_context`), so which shape a process sees depends
on import order — not on anything visible at the call site.

**How to apply:**

- Any `find_all` / `isinstance` over placeholder nodes must match **both**
  `exp.Placeholder` and `exp.Parameter`, reading the name through a helper
  (`Placeholder.this` is the bare string; `Parameter.this` is a `Var`). See
  `_PLACEHOLDER_NODES` / `_placeholder_name` in `privacy/sql_lineage.py`.
- Write the guard by **constructing both node shapes explicitly** and swapping
  them into a parsed tree — never by trusting what the current process imported,
  or the test asserts one shape and silently skips the other.
- **Suspect this whenever a sqlglot-walking test passes on its own file and fails
  only in the full suite.** The poisoners were `tests/privacy/test_sql_query.py`
  and `test_report_class_derivation.py`, both of which import sqlmesh. Bisect by
  pairing the target test with one other **file** at a time: no single *test* did
  it, so a per-test bisect finds nothing. A file-scoped run would have shipped it.
- The failure was not a merely missed class. The unmatched placeholder fell to
  `UNRESOLVED`, which feeds the dynamic-report `class_fingerprint`, so
  match-vs-mismatch on a durable drift key flipped with import order. Ask what
  *else* consumes a node-type match before assuming fail-closed makes it benign.
