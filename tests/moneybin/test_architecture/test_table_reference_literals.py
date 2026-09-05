r"""Structural guardrail: no hardcoded schema-qualified table literal reaches SQL.

AGENTS.md's Key Abstractions table requires every table reference to come from
``moneybin.tables`` (``from moneybin.tables import FCT_TRANSACTIONS``, etc.) —
never a hardcoded ``"core.fct_transactions"``-shaped string. #519 swept 95
pre-existing literals to ``TableRef`` constants; nothing stopped the next PR
from reintroducing one. This test closes that gap.

## Threat model

This guard defends against **accidental** reintroduction of a hardcoded
schema-qualified table reference — a contributor who copy-pasted a query,
wrote a quick fix without reaching for the existing TableRef constant, or
didn't notice a sibling literal already existed. It does **not** defend
against deliberate evasion: a string assembled via concatenation or
``.join()``, dynamic dispatch onto an execute-shaped method (``getattr(db,
name)(sql)``), a literal split across an encoding, or a reference assembled
across module boundaries a per-file AST scan can't see. Those are
undecidable in general for a bounded, file-local heuristic (point 1 below),
and an adversarial reviewer motivated to keep finding them never runs out —
a near-identical guard added in #512 absorbed sixteen review rounds before
its residual ticket (MB-160) named this limit explicitly. **A finding is in
scope only if it describes a plausible accidental shape** — a bypass a
contributor could trigger by ordinary refactoring, not one requiring them to
choose evasion. Round 3 findings (quoted identifiers, ``COPY``/``DESCRIBE``,
name-to-name assignment — see below) all clear that bar, and so do round 4's
(``SHOW``/``PRAGMA``, the f-string interpolation-hole misparse, stale
literal bindings — see module docstring points 1-2 below); a fifth round
proposing runtime string construction or similar would not, and should be
declined rather than patched.

Why not a plain regex over the source text? A regex for
``\\b(schema)\\.[a-z][a-z0-9_]+`` matches ~1900 times across ``src/**/*.py`` —
almost all of it Python attribute access (``app.config``, ``core.foo``) and
prose, not SQL. That noise floor makes a bare regex either deafening (flag
everything) or vacuous (exempt everything). Three design choices bring the
signal back:

1. **AST first, not text first.** Only ``ast.Constant``/``ast.JoinedStr``
   string literals that *reach* a ``.execute()`` / ``.executemany()`` /
   ``.sql()`` call are candidates — not every string in the tree. "Reaches" means: passed
   directly as an argument, OR bound to a local name — via ``=``, an
   annotated ``x: str = ...``, an augmented ``x += ...``, a walrus
   ``(x := ...)``, or a plain name-to-name alias (``sql = query``) —
   (``ast.Assign``/``ast.AnnAssign``/``ast.AugAssign``/``ast.NamedExpr``) —
   that is later passed as an argument (including transitively, through an
   f-string interpolation inside another candidate literal, OR through a
   chain of aliases of any length — see ``_NEWEST_HOLDINGS_SNAPSHOT_CTE`` in
   ``doctor_service.py`` for the interpolation shape this exists to catch: a
   module-level query fragment assigned once, then spliced into other
   queries via ``f"...{_CTE}..."``). Name-to-name aliasing closes a gap
   found in round 3 of this guard's review: ``query = "SELECT * FROM
   core.x"; sql = query; db.execute(sql)`` is ordinary refactoring, not
   evasion, and a scan that only recorded literal RHS values missed it. The
   full enumeration of name-binding AST nodes considered, and why the other
   binding forms (loop targets, ``with``/``except`` targets, function
   parameters, tuple/list-destructured assignment) are excluded, lives as a
   comment above the binding-collection loop in ``_scan_file``. This is a
   bounded, file-local heuristic, not a full dataflow analysis — deciding
   "does this string reach a DB call" is undecidable in general (a query
   built via ``.join()``, string concatenation across branches, or a helper
   function's return value can evade it — see Threat model above). The
   tradeoff is deliberate: false negatives here are a coverage gap to close
   by hand if discovered; false positives are a source of guard fatigue that
   erodes trust in the whole check.

2. **sqlglot's parse tree, not a hand-maintained keyword list, decides what
   counts as a table reference — for everything sqlglot's DuckDB grammar can
   represent.** A round-3 review found two gaps in the earlier
   regex-and-keyword-list version of this check: a quoted identifier
   (``FROM "core"."fct_transactions"``) doesn't match a bare-identifier
   regex, and a clause word missing from the hand-maintained keyword list
   (``COPY``, ``DESCRIBE``) let a real reference through uncaught — the
   generator of an endless "you missed one" series, not a one-off miss.
   Parsing with ``sqlglot.parse(text, dialect="duckdb",
   error_level=ErrorLevel.IGNORE)`` and walking ``find_all(exp.Table)``
   handles quoting and every clause that can introduce a table (``FROM``,
   ``JOIN``, ``COPY``, ``DESCRIBE``, ``DROP``, ``TRUNCATE TABLE``,
   ``UPDATE``, ``INSERT INTO``, ...) structurally, via DuckDB's grammar
   rather than an enumeration this file has to keep current. The alias
   false-positive (``FROM raw.tabular_accounts AS raw`` making a later
   ``raw.account_id`` look like a table) disappears structurally too:
   sqlglot parses ``raw.account_id`` to a ``Column`` node, never a
   ``Table``. Measured before choosing this: a strict ``parse_one`` over
   every one of the 654 candidate literals in this tree (2026-09) succeeds
   only 21% of the time — most candidate literals are f-string SQL whose
   ``TABLE.full_name`` interpolation, at the time of that measurement, was
   stripped by ``_literal_text`` to an empty string, leaving a hole exactly
   where a table name would sit (``CREATE OR REPLACE VIEW  AS ...``), which
   a strict parser rejects. (``_literal_text`` now substitutes a neutral
   placeholder identifier for that hole instead of dropping it — see
   finding B in that function's own docstring — for an unrelated reason:
   an empty hole can splice the literal segments on either side of it into
   a *different*, still tolerantly-parseable statement that hides an
   adjacent hardcoded reference. This 2026-09 measurement predates that
   change and is not re-derived here; the placeholder only shrinks the set
   of fragments a strict parser would reject, so the case for the tolerant
   parse below stands either way.) The tolerant
   ``ErrorLevel.IGNORE`` parse used here recovers all but ~4% of those, and
   — the number that actually matters — every literal that contains a
   genuine schema-qualified reference in this tree parses successfully.
   ``exp.Table`` nodes with an **empty name** are excluded (``if not
   table.db or not table.name``) rather than the regex's implicit floor of
   ``[a-z][a-z0-9_]+`` after a schema: an interpolation hole (``DROP VIEW IF
   EXISTS raw.;``) and DuckDB's own ``CREATE SCHEMA IF NOT EXISTS seeds``
   (which sqlglot represents as a ``Table`` with the schema name in ``db``
   and an empty ``name``) both produce that shape, and neither is a table
   reference — filtered by the general rule, not by naming either literal in
   an allowlist.

   **The ``exp.Command`` bucket is the exception, and it is handled, not
   ignored.** sqlglot lowers any statement its DuckDB grammar cannot fully
   parse to ``exp.Command`` with the payload left entirely unparsed —
   DuckDB's ``EXPLAIN`` / ``EXPLAIN ANALYZE`` have no sqlglot node and
   always land here (see ``.claude/references/guard-design.md`` and
   ``src/moneybin/privacy/sql_query.py``'s ``is_metadata_query``: "sqlglot
   lowers EXPLAIN to the same exp.Command it uses for syntax it cannot
   parse"). ``find_all(exp.Table)`` on a ``Command`` always returns
   nothing — an UNEXAMINED payload, not an absence of tables — so a first
   version of this rewrite silently dropped coverage the old regex had:
   ``db.execute("EXPLAIN SELECT * FROM core.fct_transactions")`` (an
   ordinary query-plan diagnostic, not evasion — squarely inside the Threat
   model above) went from caught to missed. The fix treats **any**
   ``exp.Command`` — not just the ones whose payload happens to start with
   ``EXPLAIN`` — as a signal that the payload needs a fallback pass, not as
   a signal that no tables exist: `_fallback_regex_tables` re-runs the
   pre-round-3 keyword/regex matcher on that one statement's own
   reconstructed text. This is the sole remaining use of that matcher in
   this file — it is a narrow backstop for the fraction of statements
   sqlglot's DuckDB grammar cannot represent at all, not the primary path,
   and it carries the same bounded false-positive exposure the old
   regex-only design had (documented on ``_fallback_regex_tables`` itself).

   **Round 4 found the ``exp.Command`` fix was still verb-specific, just at
   a different granularity.** ``db.execute("SHOW app.foo")`` lands in the
   same ``exp.Command`` bucket as ``EXPLAIN``, but the keyword-anchored
   fallback regex required a clause word (``FROM``, ``JOIN``, ...)
   immediately before the schema-qualified name — ``SHOW`` isn't one, and
   there is no other keyword in ``SHOW app.foo`` for it to anchor on, so
   the literal passed uncaught. Patching in ``SHOW`` would only repeat the
   round-3 COPY/DESCRIBE pattern: fix the symptom, leave the next verb
   open. The actual fix drops the keyword requirement — the rule is
   ``<known-schema>.<identifier>`` IS a table reference regardless of what
   verb (if any) precedes it, with ``_SCHEMA_NAMES`` (not a keyword list)
   doing the real gating. A second, structurally different gap surfaced
   alongside it: ``db.execute("PRAGMA table_info('core.fct_transactions')")``
   parses successfully (it is NOT an ``exp.Command``), but the target sits
   inside a quoted string-literal *argument* — an ``exp.Literal``, not a
   ``Table`` — so ``find_all(exp.Table)`` structurally cannot see it no
   matter what keyword logic wraps it. `_tables_in_text` closes this by
   additionally scanning a parsed statement's own string-literal arguments
   with the same (now keyword-optional) fallback regex, but ONLY when that
   statement's structural table search came up empty — never against the
   statement's full reconstructed text, which is what keeps a `-- schema.table`
   SQL comment (never represented as an `exp.Literal`, even though sqlglot
   round-trips it back into reconstructed SQL as `/* ... */`) from becoming
   a new false positive.

3. **Function-scoped name binding, with module-level fallback.** A candidate
   name is resolved within the module or function/method body that binds it
   — NOT across the whole file. Two unrelated functions in the same module
   reusing a conventional local name (``query``, ``sql``) no longer
   contaminate each other: a literal assigned to ``query`` in function ``f``
   is invisible when a *different* ``query`` (e.g. a parameter) reaches
   ``execute()`` in function ``g``. A name with no local binding falls back
   to the module-level scope — this is what lets a module-level constant
   like ``_NEWEST_HOLDINGS_SNAPSHOT_CTE`` (point 1) still resolve from
   inside a method. This is Local + Global, not full Python LEGB: a name
   bound only in an *enclosing function* (a closure) is not resolved,
   consistent with this being a bounded, file-local heuristic. See the
   scoping comment above ``_scan_file``'s per-scope loop.

A SQL comment mentioning a table by name (``-- core.fct_transactions is
expensive``) is not a residual false-positive risk under the sqlglot design:
a comment is not tokenized as SQL content, so it can never produce an
``exp.Table`` node — see ``test_keyword_gate_excludes_sql_comment_prose``
below, which is the old regex-era name kept because the *behavior* it pins
(comment text is not a violation) is unchanged even though the mechanism
that guarantees it changed.

Exemptions:

- ``src/moneybin/sql/migrations/`` is skipped wholesale (a directory-level
  structural rule, not a maintained list): each migration is frozen
  point-in-time DDL that must reflect the schema exactly as it was when it
  ran, so binding to ``moneybin.tables`` — a live, current-schema registry —
  would be wrong, not merely undesirable. ``src/moneybin/migrations.py`` (the
  runner) is a different file, lives one directory up, and is NOT exempt —
  ``test_migrations_runner_is_not_exempt`` pins that.
- Everything else is an individual ``TABLE_LITERAL_ALLOWLIST`` entry with a
  ``# why`` comment. Prose, docstrings, comments, and the
  ``schema_catalog.py`` ``EXAMPLES``/hint-text strings from the #519 sweep
  needed no entries here: none of them are arguments to a recognized SQL
  sink call, so the AST-first design excludes them by construction.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src" / "moneybin"
MIGRATIONS_DIR = SRC_ROOT / "sql" / "migrations"

# Method names treated as SQL-execution sinks. `sql` covers
# `Database.sql()` (`database.py`) — a real, parameter-free execution path
# (e.g. `db.sql("SELECT version()")` in `cli/commands/db.py`) distinct from
# `.execute()`/`.executemany()` but reaching the same DuckDB connection.
EXECUTE_METHOD_NAMES = frozenset({"execute", "executemany", "sql"})

# Method names treated as table-name-argument sinks — structurally different
# from EXECUTE_METHOD_NAMES: the recognized argument (first positional, or
# the `table=` keyword) is a bare `schema.table` string, NOT SQL text.
# `Database.ingest_dataframe(table, df, ...)` (`database.py`) interpolates
# `table` straight into `INSERT INTO {safe_ref} ...` / `CREATE OR REPLACE
# TABLE {safe_ref} ...`, so a hardcoded literal here is exactly the same
# defect this guard exists to catch — but running it through `_tables_in_text`
# (the sqlglot-based SQL matcher) would not reliably parse a bare dotted
# identifier to an `exp.Table` and would silently stop catching anything.
# `_table_arg_match` below tests the string directly against `_SCHEMA_NAMES`
# instead. See the Call-node handling in `_collect_scope`.
TABLE_ARG_METHOD_NAMES = frozenset({"ingest_dataframe"})

_SCHEMA_NAMES = (
    "raw",
    "prep",
    "core",
    "app",
    "reports",
    "meta",
    "seeds",
    "synthetic",
)

# The clause_type reported for a TABLE_ARG_METHOD_NAMES violation. Not a SQL
# clause (there is no SQL here) — a fixed label distinguishing this sink
# family from the FROM/JOIN/... clause types the execute-sink path reports,
# shared by every member of TABLE_ARG_METHOD_NAMES rather than the method
# name itself, so the allowlist key shape doesn't have to change if a second
# member is ever added.
_TABLE_ARG_CLAUSE_TYPE = "TABLE_ARG"

# `ingest_dataframe`'s `table` argument is a bare `schema.table` string, not
# SQL — quoting doesn't apply the way it does in `_FALLBACK_SCHEMA_TABLE_PATTERN`,
# and the match must consume the WHOLE string (`fullmatch`, not `search`) since
# there is no surrounding SQL for a partial match to be extracted from.
_TABLE_ARG_PATTERN = re.compile(
    r"(" + "|".join(_SCHEMA_NAMES) + r")\.([a-z][a-z0-9_]+)",
    re.IGNORECASE,
)


def _table_arg_match(text: str) -> str | None:
    """Direct (non-SQL) match of a `schema.table` string against `_SCHEMA_NAMES`.

    Used for `TABLE_ARG_METHOD_NAMES` sinks only — see that constant's
    comment for why this does NOT go through `_tables_in_text`/sqlglot.

    Case-insensitive, reporting the lowercased pair, for the same two reasons
    the structural path lowercases at `exp.Table`: DuckDB resolves unquoted
    identifiers case-insensitively, so `"RAW.foo"` writes the same table as
    `"raw.foo"` and must not evade the guard; and reporting the source
    spelling would split one table across two `TABLE_LITERAL_ALLOWLIST` keys.
    """
    match = _TABLE_ARG_PATTERN.fullmatch(text.strip())
    return f"{match.group(1).lower()}.{match.group(2).lower()}" if match else None


# Fallback matcher for text sqlglot's structural walk can't see into — NOT
# the primary matcher. Two round-4 findings share one root cause a
# keyword-by-keyword patch (COPY, then DESCRIBE, then EXPLAIN) would never
# close: ``SHOW app.foo`` lowers to an unparsed ``exp.Command`` whose payload
# this regex used to require a clause keyword (``FROM``, ``JOIN``, ...)
# immediately before a schema-qualified name — ``SHOW`` isn't one of them, so
# the reference slipped through; ``PRAGMA table_info('core.fct_transactions')``
# parses just fine, but the target sits inside a quoted string-literal
# *argument*, a shape ``find_all(exp.Table)`` never walks into regardless of
# what clause keyword (if any) sits nearby. The fix is the same for both: the
# rule is ``<known-schema>.<identifier>`` IS a table reference regardless of
# what verb precedes it — ``_SCHEMA_NAMES`` (not a keyword list) is what
# keeps the match from firing on arbitrary dotted attribute access, so the
# keyword requirement was never doing the gating work. The keyword group
# below is now OPTIONAL: it still reports a real clause word when one
# happens to precede the match (e.g. the ``FROM`` inside ``EXPLAIN SELECT *
# FROM core.fct_transactions``, which existing tests pin), but a match no
# longer requires one. See ``_tables_in_text`` for the two call sites this
# feeds — the ``exp.Command`` payload text, and (new) each string-literal
# argument of a statement sqlglot parsed but found no real table in.
_FALLBACK_TABLE_KEYWORDS = (
    "FROM",
    "JOIN",
    "INTO",
    "UPDATE",
    "TABLE",
    "VIEW",
    "EXISTS",
    "REFERENCES",
    "TRUNCATE",
)
# Each identifier position accepts an optional double quote, because quoting
# is a property of *every* identifier in SQL, not of particular ones: DuckDB
# reads `core.t`, `"core".t`, `core."t"` and `"core"."t"` as the same table.
# The primary parse path gets this for free (sqlglot strips quoting when it
# exposes `Table.db`/`.name`); the fallback has to state the rule itself, or
# the two matchers in this file would disagree about what an identifier is.
_FALLBACK_SCHEMA_TABLE_PATTERN = re.compile(
    r"(?:\b(" + "|".join(_FALLBACK_TABLE_KEYWORDS) + r")\s+)?"
    r"\"?(" + "|".join(_SCHEMA_NAMES) + r")\"?\.\"?([a-z][a-z0-9_]+)\b\"?",
    re.IGNORECASE,
)


def _fallback_regex_tables(text: str, default_clause: str) -> list[tuple[str, str]]:
    """Regex-based table matching for text sqlglot's structural walk can't see into.

    Called from two bounded contexts in ``_tables_in_text`` — never as the
    primary matcher — an ``exp.Command`` node's own reconstructed SQL text,
    or a single string-literal argument of a statement sqlglot otherwise
    parsed successfully. `default_clause` is the `clause_type` reported when
    no ``_FALLBACK_TABLE_KEYWORDS`` word immediately precedes the match (the
    normal case for a literal-argument scan, and for a Command payload like
    ``SHOW app.foo`` that has no clause keyword at all): the caller passes
    the statement's own verb/node-type name, so the report still says
    *something* structurally derived rather than a hardcoded label. Shares
    the same false-positive exposure the original regex-only design had (an
    aliased-to-schema-name column reference, a SQL comment mentioning a
    table in prose) — acceptable here because both call sites are narrow,
    bounded fallbacks for text sqlglot's own parse tree can't structurally
    resolve, not the file's main path. Quoted identifiers are *not* in that
    exposure list: the pattern handles them, so neither call site silently
    reopens the quoting hole the parse path closed.
    """
    found: list[tuple[str, str]] = []
    for match in _FALLBACK_SCHEMA_TABLE_PATTERN.finditer(text):
        clause = match.group(1).upper() if match.group(1) else default_clause
        found.append((clause, f"{match.group(2)}.{match.group(3)}"))
    return found


# Allowlist entries are (file_relpath, clause_type, "schema.table",
# occurrence) 4-tuples. `file_relpath` is relative to src/moneybin/ for
# stability across moves. `clause_type` is the upper-cased sqlglot node type
# that directly parents the `exp.Table` — `FROM`/`JOIN`/`DROP`/`COPY`/
# `DESCRIBE`/`UPDATE`/`INSERT`/... (see `_tables_in_text`, module docstring
# point 2) — keying on it, not just the table name, separates a
# `DROP VIEW ... app.merchants` from an unrelated `FROM app.merchants`
# naming the same table for a different reason.
# `occurrence` is a 0-based count of prior matches of that same
# (clause_type, table) pair *within the file*, assigned in source order
# (`_scan_file` sorts by lineno before counting) — this, not a line number,
# disambiguates two genuinely distinct occurrences that share both the same
# clause type and the same table (e.g. two separate `FROM app.merchants`
# reads in the same file). It is deliberately NOT the line number: a line
# number shifts on any unrelated edit above it, turning every such edit into
# a guard failure. The occurrence count only changes when an occurrence of
# that exact (clause, table) pair is itself added, removed, or reordered
# relative to its siblings — reordering is safe too, since same-pair
# occurrences are interchangeable by construction (each gets a distinct
# index regardless of which physical occurrence holds it).
#
# Collision behavior: two occurrences collide (compute to the same key) only
# if they share file, clause type, table, AND relative order — which,
# given the sort-by-lineno step, requires two distinct occurrences to start
# on the exact same source line. No such case exists in the tree today; if
# one arises, this scheme cannot order them and the fix is to additionally
# key on the enclosing statement's text or function name.
#
# Every entry carries a `# why` comment; entries are asserted by set
# equality against the live scan (`test_allowlist_has_no_dead_entries`), so
# a stale entry fails as loudly as a new violation.
TABLE_LITERAL_ALLOWLIST: frozenset[tuple[str, str, str, int]] = frozenset({
    # `refresh_views` drops two pre-migration legacy view names during its
    # backward-compat upgrade path. `app.categories` and `app.merchants`
    # predate today's `core.dim_categories` / `core.dim_merchants` (CATEGORIES
    # / MERCHANTS) and were retired by the V006/V032 migrations — they have
    # no current TableRef *by design*: tables.py is a registry of live,
    # current names, and the whole point of these two statements is to strip
    # away a name that no longer exists in that registry. Giving them a
    # TableRef would misrepresent them as live tables. Clause type is `DROP`
    # (both are `DROP VIEW IF EXISTS ...`) — sqlglot represents `IF EXISTS`
    # as a modifier on the `Drop` node, not a distinct clause.
    ("seeds.py", "DROP", "app.categories", 0),
    ("seeds.py", "DROP", "app.merchants", 0),
    # NOT a retired-view drop — `app.merchants` here is read live, inside the
    # pre-V006 backward-compat passthrough that wraps the legacy TABLE (still
    # a BASE TABLE, not yet migrated to `app.user_merchants`) so
    # categorization reads keep working before V006 runs. It has no
    # TableRef because tables.py registers only the *current* schema shape;
    # this statement exists specifically to read the pre-migration one.
    ("seeds.py", "FROM", "app.merchants", 0),
})


# Placeholder substituted for each f-string interpolation hole in
# `_literal_text` — see that function's docstring. Two properties keep it
# from becoming a false-positive source of its own:
#   1. It cannot appear as the SCHEMA half of a match: `_SCHEMA_NAMES` is a
#      closed, known list and this identifier isn't a member of it.
#   2. A dynamic TABLE name IS a realistic shape it must not be mistaken
#      for one of (`f"DROP VIEW IF EXISTS raw.{view_name}"` — a real
#      pattern in this tree, e.g. `connection_service.py`'s
#      `gsheet_{alias}` view drop): `_tables_in_text` explicitly excludes
#      an `exp.Table` whose `name` exactly equals this placeholder, the
#      same way it excludes an empty name. The leading underscore is a
#      second, independent line of defense for the OTHER fallback path —
#      `_FALLBACK_SCHEMA_TABLE_PATTERN`'s table-name group is
#      `[a-z][a-z0-9_]+`, which never matches a leading `_`, so this value
#      can't slip through the regex-based fallback either.
_INTERPOLATION_PLACEHOLDER = "_mb_ph"


def _literal_text(node: ast.expr) -> str | None:
    """Static string content of a Constant or JoinedStr (f-string) node.

    For a JoinedStr, each interpolation hole (``{TABLE.full_name}``, etc.)
    is replaced with a neutral placeholder identifier
    (``_INTERPOLATION_PLACEHOLDER``) rather than dropped. Dropping it is
    NOT equivalent to "no hardcoded schema.table text in its static shape"
    — it can splice the literal segments on either side of the hole into a
    *different*, still tolerantly-parseable statement that hides an
    adjacent hardcoded reference. Concretely: ``f"UPDATE {TARGET.full_name}
    SET x = 1 FROM core.fct_transactions"`` naively stripped becomes
    ``"UPDATE  SET x = 1 FROM core.fct_transactions"`` — sqlglot's tolerant
    parser doesn't reject this, it MISPARSES it (the bare ``SET`` token
    after two spaces becomes the ``UPDATE`` target's table name, consuming
    the parse before it reaches the real ``FROM`` clause), so
    ``find_all(exp.Table)`` never sees the hardcoded
    ``core.fct_transactions`` at all. A placeholder identifier keeps the
    clause boundary (`UPDATE <something> SET ...`) syntactically intact, so
    the parser reaches the real ``FROM core.fct_transactions`` unharmed.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value
            if isinstance(v, ast.Constant) and isinstance(v.value, str)
            else _INTERPOLATION_PLACEHOLDER
            for v in node.values
        )
    return None


def _embedded_names(node: ast.expr) -> set[str]:
    """Names referenced inside a JoinedStr's f-string interpolations.

    Lets the scan follow one query fragment spliced into another via
    ``f"...{_SOME_CTE}..."`` — see module docstring point 1.
    """
    if not isinstance(node, ast.JoinedStr):
        return set()
    return {
        n.id
        for v in node.values
        if isinstance(v, ast.FormattedValue)
        for n in ast.walk(v.value)
        if isinstance(n, ast.Name)
    }


def _record_literal_binding(
    var_literals: dict[str, list[tuple[int, str, set[str]]]],
    name: str,
    value: ast.expr,
    lineno: int,
) -> None:
    """Record `name`'s literal text (if any) as a candidate reaching execute().

    Flow-insensitive by construction: every binding is APPENDED to
    `name`'s list — never replaced — so if `execute()` is later seeded
    with `name` (module docstring point 1's worklist), ALL literals ever
    bound to `name` in this scope are reported, including one a later,
    non-literal rebind superseded:

        if cond:
            query = "SELECT * FROM core.x"   # still flagged even if cond
                                               # is always False at runtime
        else:
            query = build()
        db.execute(query)

    This is a deliberate, NOT a fixable, false-positive: a bounded,
    flow-insensitive scan cannot tell a superseding rebind (dead code, safe
    to drop) apart from a branch (both live, must keep). Clearing on rebind
    would silently turn the branch case into a false NEGATIVE — a rebind
    that looks identical in the AST but is reached at runtime — which is
    strictly worse for a guard than an occasional false positive. The
    remedy for a genuine false positive from this shape is the existing
    `TABLE_LITERAL_ALLOWLIST`, the same escape hatch every other accepted
    false-positive class in this file uses.
    """
    text = _literal_text(value)
    if text is not None:
        var_literals.setdefault(name, []).append((lineno, text, _embedded_names(value)))


def _record_binding(
    var_literals: dict[str, list[tuple[int, str, set[str]]]],
    var_aliases: dict[str, list[str]],
    name: str,
    value: ast.expr,
    lineno: int,
) -> None:
    """Record `name`'s literal text, or its alias target, as reaching execute().

    Extends `_record_literal_binding` with one more RHS shape: a bare
    `ast.Name` — `sql = query` — which makes `name` an ALIAS of `value.id`
    rather than a literal itself. `_resolve_worklist` follows an alias chain
    of any length the same way it already follows an f-string's embedded
    name (module docstring point 1) — this is what closes the round-3
    finding on `query = "..."; sql = query; db.execute(sql)`. Not used for
    `ast.AugAssign` (`x += y`): that produces a NEW value (the concatenation
    of the old `x` and `y`), not an alias of `y`, so it stays on the
    literal-only recorder.
    """
    if _literal_text(value) is not None:
        _record_literal_binding(var_literals, name, value, lineno)
    elif isinstance(value, ast.Name):
        var_aliases.setdefault(name, []).append(value.id)


_ScopeLiterals = dict[str, list[tuple[int, str, set[str]]]]
_ScopeAliases = dict[str, list[str]]


def _direct_scope_nodes(root: ast.AST) -> list[ast.AST]:
    """All descendant nodes of `root`, not descending into a nested function.

    A nested ``ast.FunctionDef``/``ast.AsyncFunctionDef`` is itself included
    in the returned list (so the traversal below knows to stop there) but
    its own body is NOT descended into here — that subtree is a separate
    scope, walked independently by ``_scan_file``. Including the bare
    ``FunctionDef``/``AsyncFunctionDef`` node contributes nothing to
    ``_collect_scope``'s OWN scan of *this* list: every check there
    (``isinstance(node, (ast.Assign, ...))``, ``isinstance(node, ast.Call)``)
    matches a specific statement/expression shape, and none matches a bare
    ``FunctionDef`` node — it is inert filler in this scope's binding
    collection, not a carrier of the nested function's decorator or
    default-argument literals. Those DO still get scanned, but through a
    different path than "included here": ``_scan_file``'s own
    ``ast.walk(tree)`` reaches that same node independently of this
    function's choice not to descend into it, and when it does,
    ``_direct_scope_nodes(node)`` is called with the ``FunctionDef`` node
    itself as `root` — whose *direct children* (via
    ``ast.iter_child_nodes``) include its `decorator_list` and argument
    defaults, so those literals surface as part of the function's OWN
    scope, not the enclosing one. Everything else (``if``/``for``/``with``/
    ``try``/class bodies, comprehensions) is not a distinct variable scope
    in this heuristic and is walked through, mirroring Python's actual
    scoping rule that only ``def``/``lambda`` introduce a new local
    namespace.
    """
    nodes: list[ast.AST] = []
    stack = list(ast.iter_child_nodes(root))
    while stack:
        node = stack.pop()
        nodes.append(node)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        stack.extend(ast.iter_child_nodes(node))
    return nodes


def _collect_scope(
    nodes: list[ast.AST],
) -> tuple[
    _ScopeLiterals,
    _ScopeAliases,
    set[str],
    list[tuple[int, str]],
    set[str],
    list[tuple[int, str]],
]:
    """Collect literal bindings, name aliases, sink-seed names, and direct literal args.

    Returns `(var_literals, var_aliases, seed_names, scanned,
    table_arg_seed_names, table_arg_scanned)`. The last two mirror the two
    before them but for `TABLE_ARG_METHOD_NAMES` sinks (`ingest_dataframe`)
    instead of `EXECUTE_METHOD_NAMES` sinks — see the Call-node handling
    below for why they need separate collection.

    `nodes` is one lexical scope's node list from `_direct_scope_nodes` — the
    module body, or one function/method body. Enumeration of AST nodes that
    bind a name to a value, and whether each can carry a hardcoded string
    literal (or a reference to a tracked name) into an execute() call:

    HANDLED — the value is written inline at the binding site, so a
    literal there is a real candidate, and a bare-name RHS makes the
    target an alias `_resolve_worklist` follows transitively:
      - ast.Assign        x = "..."        or  x = y
      - ast.AnnAssign     x: str = "..."   or  x: str = y   (skipped when
                                             .value is None, i.e. a bare
                                             annotation)
      - ast.AugAssign     x += "..."       (records the added text; does
                                             NOT alias `x` to a Name RHS —
                                             `x += y` produces the
                                             concatenation of the prior `x`
                                             and `y`, not `y`'s value, so
                                             treating it as an alias would
                                             be wrong, not just imprecise —
                                             and does not track prior
                                             concatenation either, same
                                             file-local-heuristic limit as
                                             module docstring point 1)
      - ast.NamedExpr     (x := "...")     or  (x := y)   (walrus; target is
                                             always a plain Name per grammar).
                                             This records the binding for ANY
                                             NamedExpr node in this scope's
                                             node list, including one nested
                                             inside a sink call's own
                                             arguments (`db.execute(query :=
                                             "...")`)  —  `_direct_scope_nodes`
                                             does not stop descending at a
                                             Call node, only at a nested
                                             function boundary. That binding
                                             alone does not make `query`
                                             reach the sink, though: seeding
                                             is a separate step, handled in
                                             the Call-node branch below.

    CONSCIOUSLY EXCLUDED — no realistic or in-scope path to a hardcoded
    literal:
      - ast.For / ast.AsyncFor / comprehension `for` clauses: the target
        is bound to elements of an *iterable*, not a literal written at
        the binding site. Extracting a string from a List/Tuple-of-
        Constants iterable is a materially different shape than "value is
        a Constant/JoinedStr" and falls under the same disclaimed gap as
        ".join()"/concatenation in module docstring point 1.
      - ast.With / ast.AsyncWith `as` targets: bound to a context
        manager's __enter__() return value — never a literal present in
        source at the binding site.
      - ast.ExceptHandler `as` targets: bound to a raised exception
        instance, never a literal.
      - Function/lambda parameters (incl. defaults): the runtime value
        comes from the call site, not the default text — tracking it
        requires interprocedural analysis, explicitly out of scope
        ("a helper function's return value can evade it", docstring
        point 1).
      - ast.Global / ast.Nonlocal: declarations with no value to record.
      - Tuple/list-destructured Assign (`a, b = "core.x", "core.y"`):
        still an ast.Assign node (no separate node type), but
        `_literal_text` only extracts Constant/JoinedStr — a destructured
        Tuple/List RHS is silently not unpacked. Noted as a residual gap
        on the existing Assign handler, not a new node type to enumerate.
      - ast.Attribute assignment targets (`self.query = "..."`,
        `Cls.QUERY = "..."`): every `isinstance(target, ast.Name)` guard
        above only matches a bare local name, so an attribute target is
        silently skipped rather than recorded under some other name — this
        is the same file-local-heuristic limit as a helper function's
        return value (module docstring point 1): tracking an attribute
        write would require knowing whether every later read of that
        attribute, from anywhere, reaches this one.
    """
    var_literals: _ScopeLiterals = {}
    var_aliases: _ScopeAliases = {}
    seed_names: set[str] = set()
    scanned: list[tuple[int, str]] = []  # (lineno, text) reaching execute()
    table_arg_seed_names: set[str] = set()
    table_arg_scanned: list[
        tuple[int, str]
    ] = []  # reaching a TABLE_ARG_METHOD_NAMES sink

    for node in nodes:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    _record_binding(
                        var_literals, var_aliases, target.id, node.value, node.lineno
                    )
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.value is not None:
                _record_binding(
                    var_literals,
                    var_aliases,
                    node.target.id,
                    node.value,
                    node.lineno,
                )
        elif isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name):
                _record_literal_binding(
                    var_literals, node.target.id, node.value, node.lineno
                )
        elif isinstance(node, ast.NamedExpr):
            # node.target is always ast.Name per the walrus grammar — no
            # isinstance check needed (and pyright flags one as redundant).
            _record_binding(
                var_literals, var_aliases, node.target.id, node.value, node.lineno
            )
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else func.id
                if isinstance(func, ast.Name)
                else None
            )
            if name in EXECUTE_METHOD_NAMES:
                for arg in (*node.args, *(kw.value for kw in node.keywords)):
                    if isinstance(arg, ast.Name):
                        seed_names.add(arg.id)
                        continue
                    if isinstance(arg, ast.NamedExpr):
                        # `db.execute(query := "...")` — the walrus target,
                        # not the NamedExpr node itself, is what a bare-Name
                        # argument would look like; seed it the same way.
                        # The literal binding itself is already recorded by
                        # this same scope's node walk, above, via the
                        # `elif isinstance(node, ast.NamedExpr)` branch — a
                        # NamedExpr nested inside a Call's arguments is still
                        # a direct child of this scope's node list
                        # (`_direct_scope_nodes` only stops descending at a
                        # nested function boundary), so that branch already
                        # fires on it independently of this one.
                        seed_names.add(arg.target.id)
                        continue
                    text = _literal_text(arg)
                    if text is not None:
                        scanned.append((arg.lineno, text))
                        seed_names |= _embedded_names(arg)
            elif name in TABLE_ARG_METHOD_NAMES:
                # Unlike EXECUTE_METHOD_NAMES, only ONE argument position is
                # the table name — the rest (`df`, `on_conflict`) are never
                # SQL/table text, so scanning every arg the way the execute
                # loop does would be wrong, not just unnecessary.
                arg = (
                    node.args[0]
                    if node.args
                    else next(
                        (kw.value for kw in node.keywords if kw.arg == "table"),
                        None,
                    )
                )
                if isinstance(arg, ast.Name):
                    table_arg_seed_names.add(arg.id)
                elif isinstance(arg, ast.NamedExpr):
                    # `db.ingest_dataframe(table := "raw.foo", df)` — the same
                    # shape the execute loop above handles, seeded the same
                    # way. Leaving it out would put two behaviours on one
                    # syntax depending only on which sink it reached.
                    table_arg_seed_names.add(arg.target.id)
                elif arg is not None:
                    text = _literal_text(arg)
                    if text is not None:
                        table_arg_scanned.append((arg.lineno, text))

    return (
        var_literals,
        var_aliases,
        seed_names,
        scanned,
        table_arg_seed_names,
        table_arg_scanned,
    )


def _resolve_worklist(
    seed_names: set[str],
    local_literals: _ScopeLiterals,
    local_aliases: _ScopeAliases,
    fallback_literals: _ScopeLiterals,
    fallback_aliases: _ScopeAliases,
) -> list[tuple[int, str]]:
    """Fixed-point worklist: resolve seed names to their literal text.

    Checks `local_literals`/`local_aliases` (this scope) first; a name with
    NEITHER a local literal NOR a local alias falls back to
    `fallback_literals`/`fallback_aliases` (the module scope) — module
    docstring point 3; this is one "local binding shadows the module scope"
    decision covering both dicts, not two independent ones, so a name bound
    locally only as an alias still doesn't see a same-named module literal.
    A name "reaches" execute() directly, transitively through an f-string
    interpolation inside another reaching literal (docstring point 1), or
    transitively through a chain of plain-name aliases of any length
    (`sql = query` — the round-3 finding this closes).
    """
    worklist = list(seed_names)
    visited: set[str] = set()
    scanned: list[tuple[int, str]] = []
    while worklist:
        name = worklist.pop()
        if name in visited:
            continue
        visited.add(name)
        is_local = name in local_literals or name in local_aliases
        literal_entries = (
            local_literals.get(name, [])
            if is_local
            else fallback_literals.get(name, [])
        )
        alias_entries = (
            local_aliases.get(name, []) if is_local else fallback_aliases.get(name, [])
        )
        for lineno, text, embedded in literal_entries:
            scanned.append((lineno, text))
            worklist.extend(embedded - visited)
        worklist.extend(set(alias_entries) - visited)
    return scanned


def _tables_in_text(text: str) -> list[tuple[str, str]]:
    """Every schema-qualified table reference sqlglot finds in `text`.

    Returns (clause_type, "schema.table") pairs. Parses with
    ``error_level=ErrorLevel.IGNORE`` because most candidate literals in this
    tree are incomplete SQL fragments by construction — an f-string whose
    ``{TABLE.full_name}`` interpolation ``_literal_text`` stripped leaves a
    hole exactly where a table name would sit (``CREATE OR REPLACE VIEW  AS
    ...``), which a strict parser rejects outright. Measured over every
    candidate literal in this tree (module docstring point 2): a strict
    ``parse_one`` succeeds only 21% of the time, almost entirely on that
    interpolation-hole shape; the tolerant parse used here recovers the
    rest, and every literal containing a genuine hardcoded reference parses
    successfully either way.

    A table needs a non-empty `db` AND a non-empty `name` to count — see
    module docstring point 2 for the two shapes (an interpolation hole; a
    bare ``CREATE SCHEMA`` statement) that produce a schema-only `exp.Table`
    with no real table name, which is not a table reference. A `name`
    exactly equal to `_INTERPOLATION_PLACEHOLDER` is excluded the same way:
    `_literal_text`'s placeholder keeps a genuinely dynamic identifier
    non-empty (so the surrounding SQL still parses — see that function's
    own docstring), which reopens exactly the interpolation-hole problem
    the empty-name check exists for unless this exclusion mirrors it —
    ``f"DROP VIEW IF EXISTS raw.{view_name}"`` becomes ``raw._mb_ph`` after
    substitution: a hardcoded schema prefixed to a fully dynamic name, not
    a hardcoded schema.table reference, and this is a real, live shape in
    this tree (``connection_service.py``'s ``gsheet_{alias}`` view drop).

    An ``exp.Command`` node is sqlglot's catch-all for a statement it could
    not fully parse for the ``duckdb`` dialect (DuckDB's ``EXPLAIN`` /
    ``EXPLAIN ANALYZE`` / ``SHOW`` are the known members today; the set is a
    moving target across sqlglot versions, not a fixed list — matched by
    node type, never by scraping the "Falling back to parsing as a
    'Command'" warning sqlglot logs). Its payload is left completely
    unparsed, so ``find_all(exp.Table)`` on it always returns nothing
    regardless of what the payload actually contains — an UNEXAMINED
    payload, not an absence of tables (see
    ``.claude/references/guard-design.md`` and
    ``src/moneybin/privacy/sql_query.py``'s ``is_metadata_query`` docstring
    for the identical trap in a sibling guard). Silently treating "no tables
    found" as "no tables present" here would be a coverage regression versus
    the pre-sqlglot regex, which matched ``EXPLAIN``'s ``FROM
    schema.table`` textually with no parse step to fail. `_fallback_regex_tables`
    runs on exactly this bucket — every ``exp.Command``, regardless of which
    unsupported construct produced it — using the statement's own
    reconstructed SQL text (``statement.sql(dialect="duckdb")``) so a
    fallback triggered by one statement in a multi-statement literal never
    re-scans a sibling statement sqlglot parsed successfully.

    A statement sqlglot DOES fully parse can still hide a schema-qualified
    reference from ``find_all(exp.Table)``: ``PRAGMA
    table_info('core.fct_transactions')`` (round-4 finding 2) parses to an
    ``exp.Pragma`` with the target as a quoted string ``exp.Literal``
    argument, not a `Table` — structurally invisible to the walk above no
    matter how it's filtered. When that walk finds NO real table for a
    parsed (non-Command) statement, this function additionally scans that
    statement's own string-literal arguments (``find_all(exp.Literal)``,
    ``literal.is_string``) with the same fallback regex — never the
    statement's full reconstructed text, which is what keeps a `-- schema.table`
    comment out of this net even though sqlglot round-trips such a comment
    back into the reconstructed SQL as `/* ... */` (see
    ``test_keyword_gate_excludes_sql_comment_prose``): a SQL comment is
    never represented as an `exp.Literal` node, so it is structurally
    invisible to `find_all(exp.Literal)` regardless of what the regex
    itself would match. This literal-argument scan only runs when the
    statement's structural table search came up empty — a statement with a
    real target (`UPDATE app.merchants SET note = '...core.foo...' ...`)
    is not also re-scanned for a coincidental schema-shaped substring in an
    unrelated string literal.
    """
    try:
        statements = sqlglot.parse(
            text, dialect="duckdb", error_level=ErrorLevel.IGNORE
        )
    except Exception:  # noqa: BLE001  # tolerant parse over arbitrary fragments is best-effort
        return []

    found: list[tuple[str, str]] = []
    for statement in statements:
        if statement is None:
            continue
        if isinstance(statement, exp.Command):
            default_clause = (
                statement.this if isinstance(statement.this, str) else "COMMAND"
            )
            found.extend(
                _fallback_regex_tables(
                    statement.sql(dialect="duckdb"), default_clause.upper()
                )
            )
            continue
        statement_found: list[tuple[str, str]] = []
        for table in statement.find_all(exp.Table):
            schema = table.db.lower()
            name = table.name.lower()
            if (
                not schema
                or not name
                or schema not in _SCHEMA_NAMES
                or name == _INTERPOLATION_PLACEHOLDER
            ):
                continue
            clause_node = table.parent if table.parent is not None else statement
            statement_found.append((
                type(clause_node).__name__.upper(),
                f"{schema}.{name}",
            ))
        if not statement_found:
            for literal in statement.find_all(exp.Literal):
                if not literal.is_string:
                    continue
                statement_found.extend(
                    _fallback_regex_tables(
                        literal.this, type(statement).__name__.upper()
                    )
                )
        found.extend(statement_found)
    return found


def _scan_file(path: Path) -> list[tuple[int, str, str]]:
    """Return (lineno, clause_type, "schema.table") violations for one file.

    Scans the module scope and every function/method body as independent
    scopes (module docstring point 3), each falling back to the module scope
    for names it doesn't bind locally. Returned in source order (sorted by
    lineno) so a caller can assign stable per-(clause, table) occurrence
    indices — see TABLE_LITERAL_ALLOWLIST's key-shape comment.

    `lineno` is the enclosing string literal's own ``ast.Constant``/
    ``ast.JoinedStr`` node's ``.lineno`` — for a multi-line triple-quoted
    string, that is the line the literal's OPENING quote sits on, not the
    line where the matched clause's text physically appears inside it (a
    ``FROM core.fct_transactions`` clause ten lines into a long triple-
    quoted query reports the string's start line, not line-plus-ten).
    Sqlglot's own line/column info for the match inside the parsed fragment
    is not surfaced here — the goal is "which literal", not "which
    character".
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []

    (
        module_literals,
        module_aliases,
        module_seeds,
        all_scanned,
        module_table_arg_seeds,
        all_table_arg_scanned,
    ) = _collect_scope(_direct_scope_nodes(tree))
    all_scanned = list(all_scanned)
    all_scanned.extend(
        _resolve_worklist(module_seeds, module_literals, module_aliases, {}, {})
    )
    all_table_arg_scanned = list(all_table_arg_scanned)
    all_table_arg_scanned.extend(
        _resolve_worklist(
            module_table_arg_seeds, module_literals, module_aliases, {}, {}
        )
    )

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            (
                local_literals,
                local_aliases,
                local_seeds,
                local_scanned,
                local_table_arg_seeds,
                local_table_arg_scanned,
            ) = _collect_scope(_direct_scope_nodes(node))
            all_scanned.extend(local_scanned)
            all_scanned.extend(
                _resolve_worklist(
                    local_seeds,
                    local_literals,
                    local_aliases,
                    module_literals,
                    module_aliases,
                )
            )
            all_table_arg_scanned.extend(local_table_arg_scanned)
            all_table_arg_scanned.extend(
                _resolve_worklist(
                    local_table_arg_seeds,
                    local_literals,
                    local_aliases,
                    module_literals,
                    module_aliases,
                )
            )

    violations: list[tuple[int, str, str]] = []
    for lineno, text in all_scanned:
        for clause, table in _tables_in_text(text):
            violations.append((lineno, clause, table))
    for lineno, text in all_table_arg_scanned:
        table = _table_arg_match(text)
        if table is not None:
            violations.append((lineno, _TABLE_ARG_CLAUSE_TYPE, table))
    # Sort by lineno so occurrence indices (assigned by the caller) reflect
    # top-to-bottom source order rather than scan-order — see
    # TABLE_LITERAL_ALLOWLIST's key-shape comment. `find_all` walks
    # breadth-first (see .claude/references/sqlglot-behavior.md), so two
    # DIFFERENT (clause, table) pairs sharing one literal's lineno may sort
    # arbitrarily relative to each other here — harmless, since occurrence
    # counting (in `_scan_source_tree`) is keyed per (clause, table) pair and
    # unaffected by the order of unrelated pairs. Two matches of the SAME
    # pair on one lineno are interchangeable by construction regardless of
    # order (see TABLE_LITERAL_ALLOWLIST's collision-behavior comment).
    violations.sort(key=lambda item: item[0])
    return violations


def _is_exempt_migration(path: Path) -> bool:
    """True for any file under the frozen migrations directory."""
    return path.is_relative_to(MIGRATIONS_DIR)


def _scan_source_tree() -> list[tuple[str, int, str, str, int]]:
    """Walk src/moneybin/**/*.py, collecting every occurrence.

    Returns (relpath, lineno, clause_type, table, occurrence) 5-tuples.
    `occurrence` is a 0-based count of prior matches of the same
    (clause_type, table) pair within this file, assigned in the source
    order `_scan_file` returns (see TABLE_LITERAL_ALLOWLIST's key-shape
    comment for why this — not the line number — is the stable identity).
    """
    found: list[tuple[str, int, str, str, int]] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if _is_exempt_migration(path):
            continue
        relpath = path.relative_to(SRC_ROOT).as_posix()
        occurrence_counts: dict[tuple[str, str], int] = {}
        for lineno, clause, table in _scan_file(path):
            key = (clause, table)
            occurrence = occurrence_counts.get(key, 0)
            occurrence_counts[key] = occurrence + 1
            found.append((relpath, lineno, clause, table, occurrence))
    return found


def test_no_hardcoded_table_literals_reach_execute() -> None:
    """Every schema-qualified literal reaching execute() must be allowlisted.

    A failure here means a new call site hardcoded a schema.table string
    instead of importing the TableRef constant from moneybin.tables. Fix by
    routing through the constant; add an allowlist entry with a `# why`
    comment only for a genuine non-executed or historically-frozen exception.
    """
    found = _scan_source_tree()
    violations = [
        (relpath, lineno, clause, table, occurrence)
        for relpath, lineno, clause, table, occurrence in found
        if (relpath, clause, table, occurrence) not in TABLE_LITERAL_ALLOWLIST
    ]
    if violations:
        formatted = "\n".join(
            f"  - {relpath}:{lineno}: {clause} {table} (occurrence {occurrence})"
            for relpath, lineno, clause, table, occurrence in violations
        )
        pytest.fail(
            "Hardcoded schema-qualified table literal(s) reach a SQL execute "
            "call. Import the TableRef constant from moneybin.tables instead, "
            'or add (file, clause_type, "schema.table", occurrence) to '
            f"TABLE_LITERAL_ALLOWLIST with a `# why` comment.\n\n"
            f"Violations:\n{formatted}"
        )


def test_allowlist_has_no_dead_entries() -> None:
    """Every allowlist entry must match a real occurrence in the tree.

    A stale entry silently widens the exception surface — if the swept
    literal it names is removed or fixed, the entry should go too.
    """
    found = {
        (relpath, clause, table, occurrence)
        for relpath, _lineno, clause, table, occurrence in _scan_source_tree()
    }
    stale = TABLE_LITERAL_ALLOWLIST - found
    if stale:
        formatted = "\n".join(
            f"  - {relpath}: {clause} {table} (occurrence {occurrence})"
            for relpath, clause, table, occurrence in stale
        )
        pytest.fail(
            "TABLE_LITERAL_ALLOWLIST contains entries with no matching "
            f"occurrence in the tree — remove them.\n\nStale entries:\n{formatted}"
        )


def test_migrations_runner_is_not_exempt() -> None:
    """The migration *runner* is a different file from the frozen scripts.

    ``migrations.py`` (the runner) vs. ``sql/migrations/*.py`` (the frozen
    scripts) must stay in scope. Guards against the exemption predicate
    accidentally widening to match it — e.g. a substring check on
    "migrations" instead of a directory membership check.
    """
    runner = SRC_ROOT / "migrations.py"
    assert runner.is_file(), "expected src/moneybin/migrations.py to exist"
    assert not _is_exempt_migration(runner)


# --- Synthetic-fixture scanner unit tests -----------------------------------
#
# The tests above assert against whatever src/moneybin currently contains —
# real coverage of the scanner's own core logic (sqlglot table parsing,
# alias exclusion, CTE-splice tracing, function-scope boundary, name-to-name
# aliasing) is incidental to that, not guaranteed. These exercise
# `_scan_file` directly against small synthetic snippets written to
# `tmp_path`, so each mechanism is pinned independently of what the live
# tree happens to contain.


def _scan_source(tmp_path: Path, source: str) -> list[tuple[int, str, str]]:
    """Write `source` to a temp module and return `_scan_file`'s violations."""
    module = tmp_path / "_synthetic_module.py"
    module.write_text(source, encoding="utf-8")
    return _scan_file(module)


def test_genuine_table_reference_is_flagged(tmp_path: Path) -> None:
    """A schema.table used as a real FROM target is a violation."""
    source = 'db.execute("SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_column_reference_through_alias_is_not_flagged(tmp_path: Path) -> None:
    """A table aliased to its own schema name is not a false positive on its column refs.

    ``raw.account_id`` parses to an ``exp.Column`` (table=``raw``, this=
    ``account_id``) through the ``AS raw`` alias, never an ``exp.Table`` — so
    only the genuine ``FROM raw.tabular_accounts`` target is flagged (module
    docstring point 2). A regex-based matcher needed an explicit keyword
    gate to exclude this case; the AST distinguishes Column from Table by
    grammar position, so there is nothing to gate here.
    """
    source = 'db.execute("SELECT raw.account_id FROM raw.tabular_accounts AS raw")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "raw.tabular_accounts")]


def test_quoted_schema_and_table_are_flagged(tmp_path: Path) -> None:
    """A fully double-quoted schema.table is still a genuine reference (round-3 finding 1).

    A bare-identifier regex does not match ``"core"."fct_transactions"`` —
    sqlglot strips quoting when exposing ``exp.Table.db``/``.name``, so
    quoting has no effect on detection.
    """
    source = 'db.execute(\'SELECT * FROM "core"."fct_transactions"\')\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_partially_quoted_table_is_flagged(tmp_path: Path) -> None:
    """Only the table half quoted (`core."fct_transactions"`) is still flagged (finding 1)."""
    source = "db.execute('SELECT * FROM core.\"fct_transactions\"')\n"
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_copy_statement_is_flagged(tmp_path: Path) -> None:
    """`COPY schema.table TO ...` is a violation (round-3 finding 2).

    `COPY` was missing from the old regex's hand-maintained clause-keyword
    list. sqlglot parses it to an `exp.Copy` node with the source table
    directly reachable via `find_all(exp.Table)` — no keyword list to keep
    current.
    """
    source = "db.execute(\"COPY core.fct_transactions TO 'export.csv'\")\n"
    assert _scan_source(tmp_path, source) == [(1, "COPY", "core.fct_transactions")]


def test_describe_statement_is_flagged(tmp_path: Path) -> None:
    """`DESCRIBE schema.table` is a violation (round-3 finding 2, second keyword)."""
    source = 'db.execute("DESCRIBE core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "DESCRIBE", "core.fct_transactions")]


def test_create_schema_statement_is_not_a_false_positive(tmp_path: Path) -> None:
    """`CREATE SCHEMA IF NOT EXISTS x` must not be flagged as a table reference.

    sqlglot represents a `CREATE SCHEMA` statement's target as an
    `exp.Table` carrying the schema name in `db` and an EMPTY `name` — the
    same shape an interpolation hole produces (module docstring point 2).
    Neither is a real table reference; both are excluded by requiring a
    non-empty `name`, not by naming this literal in the allowlist.
    """
    source = 'db.execute("CREATE SCHEMA IF NOT EXISTS seeds")\n'
    assert _scan_source(tmp_path, source) == []


def test_explain_statement_is_flagged(tmp_path: Path) -> None:
    """`EXPLAIN SELECT ...` is a violation — the exp.Command coverage regression.

    sqlglot has no DuckDB `EXPLAIN` node, so the whole statement lowers to
    `exp.Command` with its payload left unparsed — `find_all(exp.Table)`
    finds nothing on it regardless of what the payload contains. The
    sqlglot-primary rewrite silently dropped this (the pre-round-3 regex
    matched `FROM schema.table` textually with no parse step to fail);
    `_fallback_regex_tables` closes it by re-running that matcher on any
    `exp.Command` node's own text.
    """
    source = 'db.execute("EXPLAIN SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_explain_analyze_statement_is_flagged(tmp_path: Path) -> None:
    """`EXPLAIN ANALYZE SELECT ...` is a violation too — same `exp.Command` bucket."""
    source = 'db.execute("EXPLAIN ANALYZE SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


@pytest.mark.parametrize(
    "source",
    [
        'db.execute("EXPLAIN SELECT * FROM \\"core\\".\\"fct_transactions\\"")\n',
        'db.execute("EXPLAIN SELECT * FROM core.\\"fct_transactions\\"")\n',
        'db.execute("EXPLAIN SELECT * FROM \\"core\\".fct_transactions")\n',
    ],
    ids=["both-quoted", "table-quoted", "schema-quoted"],
)
def test_explain_flags_quoted_identifiers(tmp_path: Path, source: str) -> None:
    """Quoting an identifier inside an `EXPLAIN` does not hide it.

    The composition case the two paths make easy to miss: `EXPLAIN` always
    routes to `_fallback_regex_tables`, so a quote-blind fallback would
    reopen — on exactly that path — the hole the sqlglot parse path closed.
    Both matchers have to agree that an identifier may be quoted.
    """
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_normal_parse_does_not_use_the_command_fallback(tmp_path: Path) -> None:
    """A statement sqlglot parses normally never reaches the exp.Command fallback.

    Positive control for the previous two tests: proves the fallback is
    scoped to the unparsed-payload bucket and does not fire — or change the
    result — for ordinary, fully-parseable SQL.
    """
    source = 'db.execute("SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_show_statement_is_flagged(tmp_path: Path) -> None:
    """`SHOW schema.table` is a violation (round-4 finding 1).

    `SHOW app.foo` has no sqlglot DuckDB node at all — it lowers to an
    unparsed `exp.Command`, same bucket as `EXPLAIN`. The pre-fix fallback
    regex required a clause keyword (`FROM`, `JOIN`, ...) immediately
    before a schema-qualified name; `SHOW` isn't one, and there is no other
    keyword in this statement for it to anchor on, so the whole hardcoded
    reference passed uncaught. The keyword is now optional, so the bare
    schema-qualified shape matches on its own; `default_clause` falls back
    to the Command's own verb (`SHOW`) since no keyword precedes the match.
    """
    source = 'db.execute("SHOW app.foo")\n'
    assert _scan_source(tmp_path, source) == [(1, "SHOW", "app.foo")]


def test_pragma_table_info_statement_is_flagged(tmp_path: Path) -> None:
    """`PRAGMA table_info('schema.table')` is a violation (round-4 finding 2).

    Unlike `SHOW`, `PRAGMA` is NOT an `exp.Command` — sqlglot parses it
    fully to an `exp.Pragma` node. The schema-qualified target lives inside
    a quoted string-literal *argument*, which `find_all(exp.Table)` never
    walks into no matter how it's filtered. `_tables_in_text` closes this
    by falling back to scanning the statement's own string-literal
    arguments once its structural table search comes up empty.
    """
    source = "db.execute(\"PRAGMA table_info('core.fct_transactions')\")\n"
    assert _scan_source(tmp_path, source) == [(1, "PRAGMA", "core.fct_transactions")]


def test_pragma_storage_info_statement_is_flagged(tmp_path: Path) -> None:
    """`PRAGMA storage_info(...)` is flagged too — same exp.Literal shape, a different pragma name.

    Proves the fix is verb-agnostic (matches module docstring point 2's
    round-4 addendum) rather than a `table_info`-specific patch.
    """
    source = "db.execute(\"PRAGMA storage_info('core.fct_transactions')\")\n"
    assert _scan_source(tmp_path, source) == [(1, "PRAGMA", "core.fct_transactions")]


def test_literal_argument_fallback_does_not_fire_when_a_real_table_exists(
    tmp_path: Path,
) -> None:
    """The literal-argument fallback (round-4 finding 2) only fires when the structural scan found nothing.

    An `UPDATE` with a genuine target table also carries an unrelated
    string literal that coincidentally looks like a schema-qualified name
    (`'see core.fct_transactions'` in a `SET` value). That literal must NOT
    also be scanned and flagged a second time — the fallback is scoped to
    statements with zero real structural matches, not layered on top of a
    successful one.
    """
    source = (
        "def f(db):\n"
        '    db.execute("UPDATE app.merchants SET note = '
        "'see core.fct_transactions' WHERE id = 1\")\n"
    )
    assert _scan_source(tmp_path, source) == [(2, "UPDATE", "app.merchants")]


def test_keyword_gate_excludes_sql_comment_prose(tmp_path: Path) -> None:
    """A schema.table mentioned in a SQL comment, not after a clause keyword, is not flagged."""
    source = 'db.execute("-- core.fct_transactions is expensive\\nSELECT 1")\n'
    assert _scan_source(tmp_path, source) == []


def test_sql_method_is_a_recognized_sink(tmp_path: Path) -> None:
    """`.sql()` (Database.sql()) is scanned like `.execute()`/`.executemany()`."""
    source = 'db.sql("SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_cte_splice_transitively_reaches_execute(tmp_path: Path) -> None:
    """A module-level literal spliced via f-string into a method's query is traced.

    Mirrors ``_NEWEST_HOLDINGS_SNAPSHOT_CTE`` in ``doctor_service.py``
    (module docstring point 1): a module-level fragment assigned once, then
    interpolated into a query built inside a function.
    """
    source = (
        '_CTE = "SELECT * FROM core.fct_transactions"\n'
        "\n\n"
        "def f(db):\n"
        '    db.execute(f"WITH x AS ({_CTE}) SELECT 1")\n'
    )
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_local_binding_reaches_execute_within_its_own_function(tmp_path: Path) -> None:
    """A literal assigned and used within one function is caught (positive control)."""
    source = (
        "def f(db):\n"
        '    query = "SELECT * FROM core.fct_transactions"\n'
        "    db.execute(query)\n"
    )
    assert _scan_source(tmp_path, source) == [(2, "FROM", "core.fct_transactions")]


def test_function_scope_does_not_leak_a_same_named_local_across_functions(
    tmp_path: Path,
) -> None:
    """A local reused as a name in an unrelated function is not a false positive.

    ``f``'s ``query`` local is a real literal; ``g``'s ``query`` is a
    parameter with no local binding of its own. Before function-scoping
    (module docstring point 3), the file-global name lookup let ``g``'s
    ``execute(query)`` pick up ``f``'s unrelated literal.
    """
    source = (
        "def f():\n"
        '    query = "SELECT * FROM core.fct_transactions"\n'
        "    return query\n"
        "\n\n"
        "def g(db, query):\n"
        "    db.execute(query)\n"
    )
    assert _scan_source(tmp_path, source) == []


def test_name_to_name_alias_reaches_execute(tmp_path: Path) -> None:
    """A bare-name reassignment (`sql = query`) still taints the literal (round-3 finding 3).

    Ordinary local refactoring — introducing a second name for the same
    value — must not hide a hardcoded literal from the scan. Before this
    fix, only a literal or f-string RHS seeded a name's binding, so `sql`
    had no recorded literal of its own and the chain broke at `sql = query`.
    """
    source = (
        "def f(db):\n"
        '    query = "SELECT * FROM core.fct_transactions"\n'
        "    sql = query\n"
        "    db.execute(sql)\n"
    )
    assert _scan_source(tmp_path, source) == [(2, "FROM", "core.fct_transactions")]


def test_name_to_name_alias_chain_of_two_reaches_execute(tmp_path: Path) -> None:
    """A two-hop alias chain (`b = a; c = b`) still resolves transitively."""
    source = (
        "def f(db):\n"
        '    a = "SELECT * FROM core.fct_transactions"\n'
        "    b = a\n"
        "    c = b\n"
        "    db.execute(c)\n"
    )
    assert _scan_source(tmp_path, source) == [(2, "FROM", "core.fct_transactions")]


def test_augassign_does_not_alias_its_rhs_name(tmp_path: Path) -> None:
    """`x += y` is concatenation, not aliasing — `y`'s own literal must not leak onto `x`.

    Confirms `_record_binding` is not used for `ast.AugAssign`: `x`'s
    resulting value is the OLD `x` plus `y`, never `y` alone, so treating
    `x` as an alias of `y` here would misattribute `y`'s literal to a
    different (concatenated, in this case unresolvable) value.
    """
    source = (
        "def f(db):\n"
        '    y = "SELECT * FROM core.fct_transactions"\n'
        '    x = "SELECT 1"\n'
        "    x += y\n"
        "    db.execute(x)\n"
    )
    assert _scan_source(tmp_path, source) == []


def test_fstring_interpolation_placeholder_does_not_hide_adjacent_literal(
    tmp_path: Path,
) -> None:
    """An f-string mixing a TableRef interpolation with an accidental literal is still caught (round-4 finding B).

    Naively dropping the interpolation hole (the pre-fix `_literal_text`
    behavior) turns
    ``f"UPDATE {target.full_name} SET x = 1 FROM core.fct_transactions"``
    into ``"UPDATE  SET x = 1 FROM core.fct_transactions"`` — sqlglot's
    tolerant parser doesn't reject this, it MISPARSES it: the bare `SET`
    token after the double space becomes the `UPDATE` target's table name,
    and the parse never reaches the real `FROM core.fct_transactions`
    clause, so `find_all(exp.Table)` finds nothing. `_literal_text` now
    substitutes a neutral placeholder identifier for the hole instead,
    which keeps the `UPDATE <target> SET ...` clause boundary intact so the
    parser reaches the hardcoded `FROM` target unharmed.
    """
    source = (
        "def f(db, target):\n"
        '    db.execute(f"UPDATE {target.full_name} SET x = 1 '
        'FROM core.fct_transactions")\n'
    )
    assert _scan_source(tmp_path, source) == [(2, "FROM", "core.fct_transactions")]


# --- TABLE_ARG_METHOD_NAMES (ingest_dataframe) sink -------------------------


def test_ingest_dataframe_hardcoded_literal_is_flagged(tmp_path: Path) -> None:
    """A hardcoded `schema.table` string reaching `ingest_dataframe` is a violation.

    `Database.ingest_dataframe(table, df, ...)` interpolates `table` straight
    into `INSERT INTO`/`CREATE OR REPLACE TABLE` — the same hardcoding defect
    this guard exists to catch, but the argument is a bare dotted identifier,
    not SQL, so it must be recognized directly rather than via `_tables_in_text`.
    """
    source = 'db.ingest_dataframe("raw.new_table", df)\n'
    assert _scan_source(tmp_path, source) == [(1, "TABLE_ARG", "raw.new_table")]


def test_ingest_dataframe_table_keyword_is_flagged(tmp_path: Path) -> None:
    """The `table=` keyword form is recognized the same way as the positional form."""
    source = 'db.ingest_dataframe(df=df, table="raw.new_table")\n'
    assert _scan_source(tmp_path, source) == [(1, "TABLE_ARG", "raw.new_table")]


def test_ingest_dataframe_tableref_full_name_is_not_flagged(tmp_path: Path) -> None:
    """A `TableRef.full_name` call site — the required pattern — is NOT flagged (negative control).

    `RAW_NEW_TABLE.full_name` is an `ast.Attribute`, neither an `ast.Name`
    (which would be tracked as a seed) nor a string literal — the same shape
    every real `ingest_dataframe` call site in the tree uses today.
    """
    source = "db.ingest_dataframe(RAW_NEW_TABLE.full_name, df)\n"
    assert _scan_source(tmp_path, source) == []


def test_ingest_dataframe_variable_bound_literal_is_flagged(tmp_path: Path) -> None:
    """A hardcoded literal reaching `ingest_dataframe` through a local variable is still caught.

    `t = "raw.foo"; db.ingest_dataframe(t, df)` reuses the same worklist
    machinery as the execute-sink path (`_resolve_worklist` over the scope's
    `var_literals`/`var_aliases`) — NOT `_tables_in_text` — to resolve `t`
    back to its literal text before testing it against `_TABLE_ARG_PATTERN`.
    """
    source = 'def f(db, df):\n    t = "raw.foo"\n    db.ingest_dataframe(t, df)\n'
    assert _scan_source(tmp_path, source) == [(2, "TABLE_ARG", "raw.foo")]


def test_ingest_dataframe_other_positional_args_are_not_scanned(tmp_path: Path) -> None:
    """Only the first positional / `table=` argument is inspected — not every argument.

    Unlike an EXECUTE_METHOD_NAMES sink, `ingest_dataframe`'s later positional
    arguments (`df`, `on_conflict`) are never table names; a Name there must
    not be seeded, and a schema-shaped string there must not be flagged.
    """
    source = 'db.ingest_dataframe(RAW_NEW_TABLE.full_name, df, "raw.decoy")\n'
    assert _scan_source(tmp_path, source) == []


# --- Walrus used directly as a call argument (not just a statement) --------


def test_walrus_statement_binding_reaches_execute(tmp_path: Path) -> None:
    """A walrus bound in a statement (e.g. an `if` test), then passed by name, is caught.

    Positive control: proves the ordinary walrus-binding path already works
    before testing the narrower inline-argument gap below.
    """
    source = (
        "def f(db):\n"
        '    if (query := "SELECT * FROM core.fct_transactions"):\n'
        "        db.execute(query)\n"
    )
    assert _scan_source(tmp_path, source) == [(2, "FROM", "core.fct_transactions")]


def test_walrus_used_directly_as_call_argument_is_flagged(tmp_path: Path) -> None:
    """`db.execute(query := "...")` — the walrus target used inline as the argument — is caught.

    Before this fix, the argument loop only recognized a bare `ast.Name` or a
    literal (`_literal_text`) — an `ast.NamedExpr` argument matched neither
    branch and fell through unseeded, even though the binding itself (the
    `elif isinstance(node, ast.NamedExpr)` branch a few lines above the
    argument loop) was already recorded.
    """
    source = 'db.execute(query := "SELECT * FROM core.fct_transactions")\n'
    assert _scan_source(tmp_path, source) == [(1, "FROM", "core.fct_transactions")]


def test_ingest_dataframe_uppercase_schema_is_flagged(tmp_path: Path) -> None:
    """An uppercase spelling reaches the same DuckDB table, so it must not evade.

    DuckDB resolves unquoted identifiers case-insensitively — `"RAW.Foo"`
    writes exactly what `"raw.foo"` writes. The structural path already
    lowercases at `exp.Table`; a case-sensitive matcher here would have left
    the new sink with an evasion its sibling does not have. The reported
    table is lowercased for the same reason, so one table cannot occupy two
    `TABLE_LITERAL_ALLOWLIST` keys depending on how it was typed.
    """
    source = 'db.ingest_dataframe("RAW.New_Table", df)\n'
    assert _scan_source(tmp_path, source) == [(1, "TABLE_ARG", "raw.new_table")]


def test_ingest_dataframe_walrus_argument_is_flagged(tmp_path: Path) -> None:
    """A walrus bound inline as the table argument is seeded like a bare name.

    The execute sink grew `ast.NamedExpr` handling in the same round; without
    the matching branch here, one syntax would behave two ways depending only
    on which sink it reached.
    """
    source = 'db.ingest_dataframe(table := "raw.foo", df)\n'
    assert _scan_source(tmp_path, source) == [(1, "TABLE_ARG", "raw.foo")]
