"""Scenario: gsheet pull + OFX side-load + transform + match collapses cross-source.

A standalone scenario test (not driven through the YAML scenario runner) —
gsheet is a new step the runner doesn't model, and adding a ``gsheet_pull``
entry to ``STEP_REGISTRY`` would be larger than the MVP for this PR. The
test is marked ``@pytest.mark.scenarios`` so ``make test-scenarios`` picks
it up.

Hand-derived expectations live alongside the fixture in
``synthetic_workbook.yaml`` — every count is verifiable by reading that
file, not by running the pipeline. See ``.claude/rules/testing.md``
"Scenario Expectations Must Be Independently Derived".
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
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
from moneybin.database import Database, sqlmesh_context
from moneybin.services.matching_service import MatchingService
from moneybin.tables import (
    OFX_ACCOUNTS,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
)
from tests.scenarios._runner import Scenario, scenario_env

pytestmark = [pytest.mark.scenarios, pytest.mark.slow]

_FIXTURE_PATH = Path(__file__).parent / "synthetic_workbook.yaml"


def _load_fixture() -> dict[str, Any]:
    return yaml.safe_load(_FIXTURE_PATH.read_text())


def _build_workbook(spec: dict[str, Any]) -> FakeWorkbook:
    """Render the YAML's gsheet block into a TestSheetsClient workbook."""
    gs = spec["gsheet"]
    rows = [
        [row["date"], row["description"], row["amount"], row["account"]]
        for row in gs["rows"]
    ]
    return FakeWorkbook(
        title=gs["workbook_title"],
        tabs=[
            FakeSheetTab(
                name=gs["tab_name"],
                gid=gs["gid"],
                headers=list(gs["headers"]),
                rows=rows,
            ),
        ],
    )


def _seed_tabular_account(db: Database, account_id: str, account_name: str) -> None:
    """Mirror ``fixture_loader._seed_tabular_account`` — gsheet doesn't write here.

    Gsheet's transactions adapter writes only to ``raw.tabular_transactions``;
    the matching ``dim_accounts`` row is populated either via the import
    pipeline (CSVs) or via this seed for gsheet-only scenarios.
    """
    df = pl.DataFrame([
        {
            "account_id": account_id,
            "account_name": account_name,
            "account_type": "checking",
            "institution_name": "scenario",
            "source_file": "scenario://gsheet/synthetic_workbook",
            "source_type": "gsheet",
            "source_origin": "scenario",
            "import_id": uuid.uuid4().hex[:12],
            "extracted_at": datetime.now(UTC),
        }
    ])
    db.ingest_dataframe(TABULAR_ACCOUNTS.full_name, df, on_conflict="upsert")


def _seed_ofx_account(db: Database, account_id: str) -> None:
    """Mirror ``fixture_loader._seed_ofx_account`` so dim_accounts has FK target."""
    df = pl.DataFrame([
        {
            "account_id": account_id,
            "routing_number": None,
            "account_type": "CHECKING",
            "institution_org": "scenario",
            "institution_fid": None,
            "source_file": "scenario://ofx/synthetic_workbook",
            "extracted_at": datetime.now(UTC),
        }
    ])
    db.ingest_dataframe(OFX_ACCOUNTS.full_name, df, on_conflict="upsert")


def _seed_ofx_rows(db: Database, account_id: str, rows: list[dict[str, str]]) -> None:
    """Insert OFX rows into ``raw.ofx_transactions`` directly."""
    frame = pl.DataFrame([
        {
            "source_transaction_id": r["source_transaction_id"],
            "account_id": account_id,
            "transaction_type": r["transaction_type"],
            "date_posted": datetime.fromisoformat(r["transaction_date"]),
            "amount": pl.Decimal(18, 2),  # placeholder for typing — overwritten below
            "payee": r["payee"],
            "memo": None,
            "check_number": None,
            "source_file": "scenario://ofx/synthetic_workbook",
            "extracted_at": datetime.now(UTC),
        }
        for r in rows
    ])
    # Cast amount column explicitly — placeholder above set dtype only.
    frame = frame.with_columns(
        pl.Series(
            "amount",
            [str(r["amount"]) for r in rows],
        ).cast(pl.Decimal(18, 2))
    )
    db.ingest_dataframe(OFX_TRANSACTIONS.full_name, frame, on_conflict="insert")


def _empty_scenario() -> Scenario:
    """Minimal Scenario shell so ``scenario_env`` will bootstrap the DB.

    The scenario runner's bootstrap path provisions a profile, opens an
    encrypted Database, and seeds the SQLMesh catalog — all things this
    test needs. We bypass the YAML-defined pipeline by passing an empty
    list and driving the gsheet + OFX flow imperatively below.
    """
    return Scenario.model_validate({
        "scenario": "gsheet-cross-source-matching",
        "setup": {"persona": "basic"},
        "pipeline": [],
    })


def test_gsheet_cross_source_matching_with_ofx() -> None:
    """Three gsheet rows + one OFX row collapse to three gold records.

    Hand-derived: 3 gsheet rows total, of which 1 (Whole Foods 2026-01-15)
    matches the single OFX row by date + amount + account → cross-source
    dedup collapses (gsheet, OFX) for that one row, leaving 3 gold rows.
    """
    fixture = _load_fixture()
    expected = fixture["expectations"]
    account_id: str = fixture["account"]["id"]
    account_name: str = fixture["account"]["name"]

    with scenario_env(_empty_scenario()) as (db, _tmp, _env):
        # ── seed ──────────────────────────────────────────────────────────
        _seed_tabular_account(db, account_id, account_name)
        _seed_ofx_account(db, account_id)
        _seed_ofx_rows(db, account_id, fixture["ofx"]["rows"])

        # ── gsheet connect → loads 3 rows into raw.tabular_transactions ──
        sheets = TestSheetsClient()
        sheets.register_workbook(
            fixture["gsheet"]["spreadsheet_id"], _build_workbook(fixture)
        )
        oauth = TestOAuthClient(authorized=True)
        conn_svc = GSheetConnectionService(
            db=db, sheets_client=sheets, oauth_client=oauth
        )
        gs = fixture["gsheet"]
        url = (
            f"https://docs.google.com/spreadsheets/d/{gs['spreadsheet_id']}"
            f"/edit#gid={gs['gid']}"
        )
        result = conn_svc.connect(
            ConnectionRequest(
                url=url,
                adapter="transactions",
                account_name=account_name,
                account_id=account_id,
                yes=True,
            )
        )
        assert result.initial_pull is not None

        # Sanity (independent of pipeline): hand-counted from YAML.
        raw_tabular = db.execute(
            "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'gsheet'"
        ).fetchone()
        raw_ofx = db.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()
        assert raw_tabular is not None and raw_ofx is not None
        assert raw_tabular[0] == expected["rows_in_raw_tabular"]
        assert raw_ofx[0] == expected["rows_in_raw_ofx"]

        # ── transform via SQLMesh ──────────────────────────────────────────
        with sqlmesh_context(db) as ctx:
            ctx.plan(auto_apply=True, no_prompts=True)

        # ── match (auto-accept transfers; cross-source dedup is automatic) ─
        MatchingService(db).run(auto_accept_transfers=True)

        # ── assert hand-derived gold count ────────────────────────────────
        gold_count = db.execute("SELECT COUNT(*) FROM core.fct_transactions").fetchone()
        assert gold_count is not None
        assert gold_count[0] == expected["gold_record_count"], (
            f"Expected {expected['gold_record_count']} gold rows, "
            f"got {gold_count[0]} — cross-source collapse mis-counted."
        )

        # Negative expectation: raw rows must exceed gold rows — otherwise
        # the 3-gold count could pass with zero matches if both sources
        # contributed 3 disjoint rows. This locks in that the collapse
        # actually happened.
        raw_total = db.execute(
            "SELECT (SELECT COUNT(*) FROM raw.tabular_transactions "
            "        WHERE source_type='gsheet')"
            " + (SELECT COUNT(*) FROM raw.ofx_transactions)"
        ).fetchone()
        assert raw_total is not None
        assert raw_total[0] > gold_count[0], (
            f"Raw rows ({raw_total[0]}) must exceed gold rows ({gold_count[0]}) — "
            "otherwise no cross-source collapse happened."
        )
