"""Scenario: a seed sheet in ``raw`` reads through ``sql_query``, floor and all.

M2O.2 made ``raw`` and ``prep`` readable through the privacy-gated ``sql_query``
surface behind a content floor: 33 hand-declared CRITICAL columns, and a
value-shape scan for every other column. This scenario is the whole-pipeline
check on that floor over a table nothing declares — a ``raw.gsheet_<alias>``
seed view, minted at connect time by the same code path a real pull uses
(``connectors/gsheet/adapters/raw_seed.py`` calls ``generate_seed_view_sql`` on
every load).

A standalone scenario test, not driven through the YAML scenario runner, for the
reason ``gsheet/test_scenario_gsheet.py`` gives: gsheet is a step
``STEP_REGISTRY`` does not model. It drives the real
``GSheetConnectionService.connect`` against the in-memory sheets client, so the
view under test is minted rather than hand-written.

Every expected value is hand-derived from ``gsheet/seed_sheet_content_floor.yaml``
against the two rules that file quotes, before the pipeline runs — see
``.claude/rules/testing.md`` "Scenario Expectations Must Be Independently
Derived". The fixture is a second file rather than a row added to
``synthetic_workbook.yaml``, whose counts back the cross-source matching
scenario.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from moneybin.connectors.gsheet.connection_service import (
    ConnectionRequest,
    GSheetConnectionService,
)
from moneybin.connectors.gsheet.testing import (
    FakeSheetTab,
    FakeWorkbook,
    TestOAuthClient,
    TestSheetsClient,
)
from moneybin.database import Database
from moneybin.privacy.sql_query import execute_sql_query
from moneybin.privacy.taxonomy import DataClass
from tests.scenarios._runner import Scenario, scenario_env

pytestmark = [pytest.mark.scenarios, pytest.mark.slow]

_FIXTURE_PATH = Path(__file__).parent / "gsheet" / "seed_sheet_content_floor.yaml"

_COLUMNS = (
    "date",
    "category",
    "account_ref",
    "member_id",
    "ledger_code",
    "card_ref",
    "balance",
)


def _load_fixture() -> dict[str, Any]:
    return yaml.safe_load(_FIXTURE_PATH.read_text())


def _build_workbook(spec: dict[str, Any]) -> FakeWorkbook:
    """Render the fixture's gsheet block into a TestSheetsClient workbook."""
    gs = spec["gsheet"]
    headers: list[str] = list(gs["headers"])
    keys = [_normalize(header) for header in headers]
    return FakeWorkbook(
        title=gs["workbook_title"],
        tabs=[
            FakeSheetTab(
                name=gs["tab_name"],
                gid=gs["gid"],
                headers=headers,
                rows=[[str(row[key]) for key in keys] for row in gs["rows"]],
            ),
        ],
    )


def _normalize(header: str) -> str:
    """The fixture's row keys are the sheet headers, lowercased and joined."""
    return header.lower().replace(" ", "_")


def _empty_scenario() -> Scenario:
    """Minimal Scenario shell so ``scenario_env`` will bootstrap the DB.

    No SQLMesh plan and no pipeline: the seed view is minted by the connector
    itself, and ``raw.gsheet_seeds`` is schema DDL rather than a SQLMesh model.
    """
    return Scenario.model_validate({
        "scenario": "sql-query-internal-schemas",
        "setup": {"persona": "basic"},
        "pipeline": [],
    })


def _connect_seed_sheet(db: Database, fixture: dict[str, Any]) -> None:
    """Connect the fixture workbook with the seed adapter, minting the view.

    ``yes`` and ``accept_seed_fallback`` are both left at their defaults: the
    ambiguous-detection confirm and the seed fall-through are gated on
    ``target_adapter == "transactions"``, and this request names ``seed``
    explicitly, so neither gate is on this path.
    """
    gs = fixture["gsheet"]
    sheets = TestSheetsClient()
    sheets.register_workbook(gs["spreadsheet_id"], _build_workbook(fixture))
    service = GSheetConnectionService(
        db=db, sheets_client=sheets, oauth_client=TestOAuthClient(authorized=True)
    )
    result = service.connect(
        ConnectionRequest(
            url=(
                f"https://docs.google.com/spreadsheets/d/{gs['spreadsheet_id']}"
                f"/edit#gid={gs['gid']}"
            ),
            adapter="seed",
            alias=gs["alias"],
        )
    )
    assert result.initial_pull is not None, "seed connect did not pull the sheet"


def test_seed_view_reads_through_sql_query_under_the_content_floor() -> None:
    """One row of a minted ``raw`` seed view, seven columns, seven derived cells.

    Two columns must come back masked and five must come back intact, each for a
    reason the fixture names. Both directions are load-bearing: a floor that
    masked everything would satisfy the masked half while making the schema
    useless, and a floor that masked nothing would satisfy the passthrough half
    while publishing an account number.
    """
    fixture = _load_fixture()
    expected = fixture["expectations"]

    with scenario_env(_empty_scenario()) as (db, _tmp, _env):
        _connect_seed_sheet(db, fixture)

        # Hand-counted from the fixture: three data rows, no others.
        rows = db.execute("SELECT COUNT(*) FROM raw.gsheet_budget").fetchone()
        assert rows is not None
        assert rows[0] == expected["rows_in_view"]

        # The view's own inferred types, checked before the masking assertions:
        # the DECIMAL passthrough below is a fixture property, and reading it as
        # a masking result would be the exact confusion this fixture is built to
        # prevent.
        types = {
            name: str(sql_type)
            for name, sql_type in db.execute(
                "SELECT column_name, data_type FROM duckdb_columns() "
                "WHERE schema_name = 'raw' AND table_name = 'gsheet_budget' "
                "AND NOT starts_with(column_name, '_')"
            ).fetchall()
        }
        assert types == expected["column_types"]

        projection = ", ".join(_COLUMNS)
        query = (
            f"SELECT {projection} FROM raw.gsheet_budget "  # noqa: S608  # own constant
            "WHERE _row_number = 1"
        )
        # The same query without the middleware. Proves the masking below is the
        # surface's doing rather than something the fixture or the view already
        # did — an assertion that a masked value came back is otherwise
        # indistinguishable from a fixture that never held the real one.
        stored = db.execute(query).fetchone()
        assert stored is not None
        assert (
            stored[_COLUMNS.index("account_ref")]
            == fixture["gsheet"]["rows"][0]["account_ref"]
        )

        result = execute_sql_query(db, query, max_rows=10)

    # Nothing declares this view, so every column must resolve to the floor. An
    # AGGREGATE here would mean the lineage snapshot never saw the view — the
    # failure `get_current_schema_snapshot` warns about, where a gate-admitted
    # schema returns LOW passthrough for columns nobody classified.
    assert set(result.output_classes.values()) == {DataClass.FLOORED}

    (row,) = result.records
    masked = expected["masked"]
    passthrough = expected["passthrough"]
    assert row["account_ref"] == masked["account_ref"]
    assert row["member_id"] == masked["member_id"]
    assert row["category"] == passthrough["category"]
    assert row["ledger_code"] == passthrough["ledger_code"]
    assert row["card_ref"] == passthrough["card_ref"]
    assert row["balance"] == Decimal(passthrough["balance"])
    assert row["date"] == date.fromisoformat(passthrough["date"])
