"""Tests for the schema catalog service."""

from __future__ import annotations

from typing import Any

import duckdb
import pytest

import moneybin.services.schema_catalog as schema_catalog_module
from moneybin.database import (  # noqa: PLC2701  # the single definition of "point a new cursor at the attached DB"
    Database,
    _pin_cursor_to_moneybin,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.privacy.sql_query import ALLOWED_QUERY_SCHEMAS, execute_sql_query
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass
from moneybin.reports._framework.registry import spec_of
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.services.schema_catalog import (
    CONVENTIONS,
    EXAMPLES,
    Example,
    build_live_catalog,
    build_schema_doc,
)
from moneybin.tables import IMPORT_LOG, INTERFACE_TABLES

pytestmark = pytest.mark.unit


def test_conventions_has_required_keys() -> None:
    """CONVENTIONS must define exactly the four canonical keys."""
    assert set(CONVENTIONS.keys()) == {
        "amount_sign",
        "currency",
        "dates",
        "ids",
    }


def test_example_dataclass_shape() -> None:
    """Example is a frozen dataclass with question and sql fields."""
    ex = Example(question="q?", sql="SELECT 1")
    assert ex.question == "q?"
    assert ex.sql == "SELECT 1"


def test_every_currency_segmented_report_example_names_its_currency() -> None:
    """An example on a per-currency report must project its currency column.

    These examples are what an agent copies. A report whose rows are segmented
    per currency (`ReportSemantics.currency`) but whose example ranks or
    aggregates without naming that column hands back an apparently-global
    answer that is really per-currency-but-unlabelled — the presentation half
    of the no-blend invariant (multi-currency.md Requirement 5).

    Derived from each report's own declared semantics rather than a list kept
    here, so a new currency-segmented report inherits the guard instead of
    silently escaping it.
    """
    # A view-less spec (a saved report, whose class map is derived from stored SQL
    # rather than a SQLMesh model) is skipped rather than keyed some other way:
    # `EXAMPLES` is keyed by view name, so it holds no entry for one, and there is
    # nothing here to check. Keying on `report_id` instead would make every lookup
    # below miss and quietly empty the guard.
    segmented = {
        spec.view.full_name: spec.semantics.currency
        for spec in map(spec_of, ALL_REPORTS)
        if spec.semantics.currency and spec.view is not None
    }
    assert segmented, "expected at least one currency-segmented report"

    offenders = [
        f"{view} example {ex.question!r} never names {column}"
        for view, column in segmented.items()
        for ex in EXAMPLES.get(view, ())
        if column not in ex.sql
    ]
    assert not offenders, "\n".join(offenders)


def test_every_money_aggregating_example_names_its_currency() -> None:
    """An example that sums money must name the currency it summed.

    The sibling guard above derives from `ReportSemantics.currency`, so it only
    reaches `reports.*` views. A `core.*` table has no report semantics and
    escaped it entirely — which is how seven curated examples came to blend
    denominations while every `reports.*` example was correct.

    Derived from `CLASSIFICATION` on both sides: a table is eligible when it
    declares a `CURRENCY` column, so a table that genuinely has no currency
    (`core.dim_categories`) never trips, and a new money example on a
    currency-bearing table inherits the guard rather than escaping it.

    Restricted to SUM/AVG because those combine rows into one figure. COUNT is
    currency-agnostic, and MIN/MAX here fall on dates.
    """
    currency_tables = {
        f"{schema}.{table}"
        for (schema, table), columns in CLASSIFICATION.items()
        if DataClass.CURRENCY in columns.values()
    }
    assert currency_tables, "expected at least one currency-bearing table"

    offenders = [
        f"{view} example {ex.question!r} aggregates money without currency_code"
        for view in currency_tables
        for ex in EXAMPLES.get(view, ())
        if ("SUM(" in ex.sql or "AVG(" in ex.sql) and "currency_code" not in ex.sql
    ]
    assert not offenders, "\n".join(offenders)


# Curated examples that deliberately lead their sort with currency_code, keyed
# by (view, question). Each value argues why interleaving is not the lever for
# that query — not that truncation is unlikely to reach it.
_CURRENCY_FIRST_SORT_OK: dict[tuple[str, str], str] = {
    (
        "reports.net_worth",
        "Net worth today, one row per currency",
    ): (
        "reports.net_worth is grained (balance_date, currency_code) and this "
        "example pins one date, so the result is exactly one row per currency. "
        "No ordering survives truncation better: any prefix of k rows holds k "
        "currencies whatever the sort key is."
    ),
    (
        "core.fct_investment_lots",
        "Total remaining cost basis across all open lots in an account "
        "(substitute YOUR_ACCOUNT_ID)",
    ): (
        "GROUP BY currency_code alone, so the result is one row per currency — "
        "the same argument as above. Ordering cannot recover a currency that a "
        "cap dropped when every currency costs one row."
    ),
}


def test_no_example_leads_its_sort_with_currency_code() -> None:
    """A curated example must not hand a row cap one currency before the next.

    `sql_query` truncates with a prefix — `sql_query.py` keeps `rows[:max_rows]`
    — and so does an agent's own `LIMIT`, which these examples invite by asking
    for "top" anything. Leading with `currency_code` therefore fills the whole
    budget with the lexicographically-first currency and the rest are absent,
    not merely ranked lower, with nothing in the response saying so.

    That is the same defect `test_no_runner_leads_its_sort_with_currency_code`
    removed from the report runners. This is the second channel: the SQL we
    *teach* agents to write. A guard over `reports/definitions/*.py` cannot see
    it, so fixing one channel and leaving the other is how two patterns for the
    same job survive side by side.

    Set equality, not a subset: a new example that leads with `currency_code`
    fails, and so does a stale exemption for an example that was since fixed.
    """
    offenders = {
        (view, ex.question)
        for view, examples in EXAMPLES.items()
        for ex in examples
        if "ORDER BY currency_code" in " ".join(ex.sql.split())
    }

    assert offenders == set(_CURRENCY_FIRST_SORT_OK), (
        "curated examples leading their sort with currency_code let a truncated "
        "response omit a whole currency; rank within each currency and order by "
        "that rank first, or lead with the non-currency dimension:\n"
        + "\n".join(
            f"  {view}: {question!r}"
            for view, question in sorted(offenders ^ set(_CURRENCY_FIRST_SORT_OK))
        )
    )


def test_examples_only_reference_interface_tables() -> None:
    """Every key in EXAMPLES must be a known interface table."""
    interface_names = {t.full_name for t in INTERFACE_TABLES}
    for table_name in EXAMPLES.keys():
        assert table_name in interface_names, (
            f"EXAMPLES key {table_name!r} is not an interface table"
        )


def test_every_interface_table_has_at_least_one_example() -> None:
    """Every interface table must have at least one entry in EXAMPLES."""
    interface_names = {t.full_name for t in INTERFACE_TABLES}
    missing = interface_names - set(EXAMPLES.keys())
    assert not missing, f"Interface tables missing examples: {sorted(missing)}"


def _present_tables(db: Database) -> set[str]:
    """Return fully-qualified names of all tables and views in the test DB."""
    rows = db.execute(
        "SELECT schema_name || '.' || table_name FROM duckdb_tables() "
        "UNION ALL "
        "SELECT schema_name || '.' || view_name FROM duckdb_views() "
        "WHERE NOT internal"
    ).fetchall()
    return {r[0] for r in rows}


def test_build_schema_doc_top_level_keys(schema_catalog_db: Database) -> None:
    """The returned dict must have all expected top-level keys with correct types."""
    doc = build_schema_doc()
    assert doc["version"] == 1
    assert "generated_at" in doc
    assert doc["conventions"]["amount_sign"].startswith("negative")
    assert isinstance(doc["tables"], list)
    assert "beyond_the_interface" in doc
    assert "catalog_query" in doc["beyond_the_interface"]


def test_build_schema_doc_includes_present_interface_tables(
    schema_catalog_db: Database,
) -> None:
    """Core interface tables present in the DB must appear in the output."""
    doc = build_schema_doc()
    names = {t["name"] for t in doc["tables"]}
    # The test DB only creates core.* via create_core_tables_raw; app tables
    # are absent, so build_schema_doc should silently skip them rather than
    # error. Core interface tables must be present.
    assert "core.fct_transactions" in names
    assert "core.dim_accounts" in names


def test_build_schema_doc_includes_interface_views(
    schema_catalog_db: Database,
) -> None:
    """Interface objects that are views (not tables) must appear too.

    Regression test: `duckdb_tables()` excludes views, so the catalog
    query must union it with `duckdb_views()` to surface objects like
    `core.dim_categories` (a SQLMesh-managed view; stubbed in the fixture
    for tests).
    """
    doc = build_schema_doc()
    names = {t["name"] for t in doc["tables"]}
    assert "core.dim_categories" in names


def test_live_catalog_lists_a_queryable_table_the_curated_doc_omits(
    schema_catalog_db: Database,
) -> None:
    """A queryable-but-uncurated relation must be discoverable.

    `app.match_decisions` is created by `init_schemas` and readable through
    `sql_query`, but its `TableRef` carries no `audience="interface"`, so it is
    absent from `INTERFACE_TABLES` and therefore from `build_schema_doc`. That
    gap is the defect: an agent is told `app` is queryable, asks the schema
    surface about an `app` table, and is told it is unknown.
    """
    curated = {t["name"] for t in build_schema_doc()["tables"]}
    assert "app.match_decisions" not in curated

    live = {r["name"]: r for r in build_live_catalog()}
    assert "app.match_decisions" in live
    assert live["app.match_decisions"]["curated"] is False
    assert live["core.fct_transactions"]["curated"] is True


def test_live_catalog_never_contradicts_the_curated_doc(
    schema_catalog_db: Database,
) -> None:
    """The `curated` flag must agree with what `build_schema_doc` actually curates.

    `build_schema_doc` curates two families that can never be `TableRef`s: the
    `raw.gsheet_<alias>` / `raw.pdf_<alias>` seed views, whose alias is chosen at
    connection time. Deriving the flag from `INTERFACE_TABLES` alone reports
    `curated: false` for a relation `sql_schema(table='raw.gsheet_<alias>')`
    answers with a full entry — and the listing's own hint then routes the agent
    to a bare `DESCRIBE` instead of the richer entry that already exists.

    The seed view is the fixture that makes the predicate non-vacuous; the
    assertion is the rule, so a third runtime-curated family cannot reintroduce
    the same contradiction.
    """
    schema_catalog_db.execute(
        "CREATE OR REPLACE VIEW raw.gsheet_probe AS SELECT 1 AS id"
    )
    schema_catalog_db.execute(
        """
        INSERT INTO app.gsheet_connections (
            connection_id, spreadsheet_id, sheet_gid, sheet_name, workbook_name,
            adapter, column_mapping, header_signature, status, alias
        ) VALUES ('conn-probe', 'sheet-1', 0, 'Sheet1', 'Workbook', 'seed',
                  '{}', '[]', 'healthy', 'probe')
        """
    )

    curated_doc = {t["name"] for t in build_schema_doc()["tables"]}
    assert "raw.gsheet_probe" in curated_doc, "fixture must reach the predicate"

    live = {r["name"]: r for r in build_live_catalog()}
    assert live["raw.gsheet_probe"]["curated"] is True

    contradictions = {
        name for name, row in live.items() if name in curated_doc and not row["curated"]
    }
    assert not contradictions


def test_live_catalog_reads_both_of_its_sets_from_one_snapshot(
    schema_catalog_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The `curated` names and the relation rows must come from one connection.

    They came from two: `build_schema_doc` closed its connection before the
    listing query opened another. A `raw.gsheet_<alias>` view created in that
    window is missing from the curated names but present in the rows, so the
    listing reports `curated: false` for a relation `sql_schema(table=...)`
    answers with a full entry — the contradiction the test above rules out,
    reached through snapshot skew rather than a stale name set.

    Counting connection requests is what makes the guarantee testable: the
    fixture hands every caller one shared `Database`, so comparing the two
    connections would compare an object with itself and pass either way.
    """
    opened = 0
    connect = schema_catalog_module.get_database

    def counting_get_database(*args: object, **kwargs: object) -> Any:
        nonlocal opened
        opened += 1
        return connect(*args, **kwargs)

    monkeypatch.setattr(schema_catalog_module, "get_database", counting_get_database)
    assert build_live_catalog(schema="raw"), "fixture must reach the predicate"
    assert opened == 1


def test_live_catalog_holds_one_transaction_across_both_of_its_sets(
    schema_catalog_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One connection is not one snapshot: DuckDB autocommits every statement.

    Pinning both sets to a single connection (the test above) is necessary and
    not sufficient. Each statement on that connection opens and closes its own
    transaction, so a relation another connection commits in between still lands
    in the rows while the curated names predate it — the same `curated: false`
    on a curated relation, one layer below the two-connection race.

    A second cursor on the same DuckDB instance is the concurrent writer the
    fixture's one shared `Database` otherwise cannot supply.
    """
    writer = schema_catalog_db.conn.cursor()
    _pin_cursor_to_moneybin(writer)
    # The seam the race opens at.
    schema_doc = schema_catalog_module._schema_doc  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    def doc_then_commit_a_view(db: Database) -> dict[str, Any]:
        doc = schema_doc(db)
        writer.execute("CREATE VIEW raw.committed_mid_read AS SELECT 1 AS n")
        return doc

    monkeypatch.setattr(schema_catalog_module, "_schema_doc", doc_then_commit_a_view)
    names = {r["name"] for r in build_live_catalog(schema="raw")}
    assert "raw.committed_mid_read" not in names


def test_schema_doc_holds_one_transaction_across_its_reads(
    schema_catalog_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The curated document reads the database three times, and needs the rule too.

    Its interface relations come from one query and its runtime seed views from
    two more. A gsheet connection committed after the first read gives it a seed
    view the interface query never saw — a document describing a state the
    database was never in. Same defect as the listing above, so the module gets
    one rule rather than a transaction at whichever call site was reviewed.
    """
    writer = schema_catalog_db.conn.cursor()
    _pin_cursor_to_moneybin(writer)
    # The read the race lands in front of.
    gsheet_seed_views = schema_catalog_module._gsheet_seed_views  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    def commit_a_seed_view_then_read(db: Database) -> list[dict[str, Any]]:
        writer.execute("CREATE OR REPLACE VIEW raw.gsheet_probe AS SELECT 1 AS id")
        writer.execute(
            """
            INSERT INTO app.gsheet_connections (
                connection_id, spreadsheet_id, sheet_gid, sheet_name, workbook_name,
                adapter, column_mapping, header_signature, status, alias
            ) VALUES ('conn-probe', 'sheet-1', 0, 'Sheet1', 'Workbook', 'seed',
                      '{}', '[]', 'healthy', 'probe')
            """
        )
        return gsheet_seed_views(db)

    monkeypatch.setattr(
        schema_catalog_module, "_gsheet_seed_views", commit_a_seed_view_then_read
    )
    names = {t["name"] for t in build_schema_doc()["tables"]}
    assert "raw.gsheet_probe" not in names


def test_catalog_survives_a_database_missing_its_optional_tables(
    schema_catalog_db: Database,
) -> None:
    """A missing optional table degrades the catalog; it must not abort the read.

    Both seed-view helpers swallow `CatalogException` so a database that never
    ran `init_schemas` still gets a document. That recovery now happens inside
    an explicit transaction, and an engine that poisons a transaction on a
    failed statement would turn a silently-skipped table into the failure of
    the whole document. DuckDB does not — a binder error never reaches the
    transaction — but nothing else in the suite would notice if that changed,
    and the recovery is invisible in the passing case.

    Dropping the two optional tables and proving one of them really raises is
    what keeps this from passing on a database that still has them.
    """
    schema_catalog_db.execute("DROP TABLE app.gsheet_connections")
    schema_catalog_db.execute(f"DROP TABLE {IMPORT_LOG.full_name}")
    with pytest.raises(duckdb.CatalogException):
        schema_catalog_db.execute("SELECT 1 FROM app.gsheet_connections")

    assert build_schema_doc()["tables"]
    assert build_live_catalog(schema="raw")


def test_live_catalog_distinguishes_a_view_from_a_table(
    schema_catalog_db: Database,
) -> None:
    """Every entry carries the relation kind the catalog UNION currently drops.

    `build_schema_doc` unions `duckdb_tables()` with `duckdb_views()` and keeps
    no column saying which side a row came from, so nothing on the surface
    answers "is this relation ALTER-able?".
    """
    live = {r["name"]: r for r in build_live_catalog()}
    assert live["core.dim_categories"]["kind"] == "view"
    assert live["core.fct_transactions"]["kind"] == "table"


def test_live_catalog_is_bounded_by_the_queryable_schemas(
    schema_catalog_db: Database,
) -> None:
    """Listing what exists must not exceed what `sql_query` will read.

    `SHOW ALL TABLES` — the path the catalog footer currently recommends —
    discloses the shape of `meta` and `seeds`, which `sql_query` refuses
    outright. This listing is bounded by the same set the gate enforces, so it
    is a strictly narrower disclosure than the query it replaces.
    """
    schemas = {r["schema"] for r in build_live_catalog()}
    assert schemas <= ALLOWED_QUERY_SCHEMAS
    assert "meta" not in schemas
    assert "seeds" not in schemas


def test_live_catalog_can_be_scoped_to_one_schema(
    schema_catalog_db: Database,
) -> None:
    """A schema filter is what keeps the enumeration inside a context window."""
    only_core = build_live_catalog(schema="core")
    assert only_core
    assert {r["schema"] for r in only_core} == {"core"}


def test_beyond_the_interface_query_passes_the_sql_query_gate(
    schema_catalog_db: Database,
) -> None:
    """The catalog query must run through `execute_sql_query`, not just DuckDB.

    Regression test for the defect this footer shipped with for the life of the
    feature: the query was a `duckdb_tables()` SELECT, which is valid DuckDB but
    is REFUSED by `sql_query`'s schema gate — a table-valued function parses to a
    table node with an empty schema and an empty name, and `tables_outside_schemas`
    can never resolve that to an allowed schema. The footer is served only on the
    agent path (`moneybin://schema`, `sql_schema`), so the surface it is advertised
    on is the one it did not run on.

    The predecessor of this test called `schema_catalog_db.execute(query)` — the
    raw `Database` connection — which reaches DuckDB without ever reaching the
    gate, so it stayed green through two rewrites of the string it covers
    (`.claude/rules/testing.md`, "A Fixture That Never Reaches the Predicate
    Proves Nothing"). Drive the real primitive instead.
    """
    doc = build_schema_doc()
    query = doc["beyond_the_interface"]["catalog_query"]

    result = execute_sql_query(schema_catalog_db, query, max_rows=1000)

    assert result.records, "the catalog query must return the objects that exist"


def test_beyond_the_interface_query_lists_raw_and_prep_views(
    schema_catalog_db: Database,
) -> None:
    """The catalog query must surface the objects the footer exists to open.

    `raw` and `prep` are deliberately absent from the curated `tables` list, so
    this query is an agent's only route to them — and nearly all of them are
    VIEWS: every `prep` staging model, plus the `raw.gsheet_<alias>` /
    `raw.pdf_<alias>` seed views minted at runtime. A discovery query that lists
    tables but not views would name none of them.
    """
    schema_catalog_db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    schema_catalog_db.execute(
        "CREATE OR REPLACE VIEW raw.gsheet_probe AS SELECT 1 AS id"
    )
    schema_catalog_db.execute("CREATE OR REPLACE VIEW prep.stg_probe AS SELECT 1 AS id")

    doc = build_schema_doc()
    result = execute_sql_query(
        schema_catalog_db, doc["beyond_the_interface"]["catalog_query"], max_rows=1000
    )

    listed = {f"{r['schema']}.{r['name']}" for r in result.records}
    assert "raw.gsheet_probe" in listed
    assert "prep.stg_probe" in listed


def test_beyond_the_interface_note_does_not_route_agents_to_the_bypass(
    schema_catalog_db: Database,
) -> None:
    """The footer must not hand an agent an unmasked operator command.

    This note is served only to an MCP agent. `moneybin db query`, `db shell`,
    and `db ui` are direct database access with no privacy middleware
    (`.claude/rules/mcp.md`, "When CLI-only is justified"), so naming one here as
    the way to read what `sql_query` refuses turns a refusal into a signpost.
    """
    note = build_schema_doc()["beyond_the_interface"]["note"]

    assert "db query" not in note
    assert "db shell" not in note
    assert "db ui" not in note


def test_build_schema_doc_columns_carry_type_and_comment(
    schema_catalog_db: Database,
) -> None:
    """Each column entry must include data_type and the applied comment."""
    doc = build_schema_doc()
    fct = next(t for t in doc["tables"] if t["name"] == "core.fct_transactions")
    cols_by_name = {c["name"]: c for c in fct["columns"]}
    assert "amount" in cols_by_name
    assert "DECIMAL" in cols_by_name["amount"]["type"].upper()
    assert "negative" in cols_by_name["amount"]["comment"].lower()


def test_build_schema_doc_includes_examples_for_present_tables(
    schema_catalog_db: Database,
) -> None:
    """Each table entry must carry at least one example with question and sql."""
    doc = build_schema_doc()
    fct = next(t for t in doc["tables"] if t["name"] == "core.fct_transactions")
    assert len(fct["examples"]) >= 1
    first = fct["examples"][0]
    assert "question" in first
    assert "sql" in first


def test_interface_tables_present_in_catalog(schema_catalog_db: Database) -> None:
    """Stale-entry drift: every interface-tagged table must exist in the DB.

    Catches removals or renames of any INTERFACE_TABLES entry — including the
    six app.* interface tables (categories, budgets, merchants,
    categorization_rules, transaction_categories, transaction_notes), which
    were previously skipped because the fixture did not seed them.
    """
    present = _present_tables(schema_catalog_db)
    missing = [t.full_name for t in INTERFACE_TABLES if t.full_name not in present]
    assert not missing, f"INTERFACE_TABLES entries missing from test DB: {missing}"


def test_examples_parse_and_execute(schema_catalog_db: Database) -> None:
    """Examples must parse and execute against the live schema.

    Catches column-renamed-but-example-not-updated drift. Now exercises
    examples for app.* interface tables (previously skipped when the
    fixture did not seed them).
    """
    for examples in EXAMPLES.values():
        for ex in examples:
            schema_catalog_db.execute(ex.sql).fetchall()
