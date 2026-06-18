"""Capture contract (Decision 8): per-source last_four / institution_name pin.

Each detect-capable import source must land the matcher's identity input
(last_four) into core.dim_accounts, OR be honestly declared binding-only. A
source that silently lands neither is the original "nothing captures the last 4
in the wild" bug — this test fails CI if it returns.

Scoped to M1S.7's guarantee: LAST4 capture end-to-end (import -> transform ->
dim_accounts.last_four). Per-account institution for multi-account aggregator
formats is contracted separately by M1S.9 (exporter/institution split), so this
contract only asserts institution for OFX, where institution_org is unambiguous.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from moneybin.services.import_service import ImportService
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

_XSRC = Path(__file__).parent / "data" / "fixtures" / "account-identity-cross-source"
_CAPTURE = Path(__file__).parent / "data" / "fixtures" / "capture-contract"


def _import_ofx(svc: ImportService) -> None:
    """OFX statement: last4 derives from <ACCTID> (1111), institution from <ORG>."""
    svc.import_file(_XSRC / "wf_checking.qfx", refresh=False)


def _import_csv_with_label(svc: ImportService) -> None:
    """A CSV whose account label embeds the last 4 — captured via the label parser."""
    svc.import_file(
        _CAPTURE / "transactions.csv",
        account_name="Daily Expense (...1789)",
        confirm=True,
        actor_kind="human",
        refresh=False,
    )


def _import_bare_csv(svc: ImportService) -> None:
    """A bare CSV with no last4 anywhere — honestly binding-only."""
    svc.import_file(
        _CAPTURE / "transactions.csv",
        account_name="Plain Savings",
        confirm=True,
        actor_kind="human",
        refresh=False,
    )


# (source_label, importer, expects_last4, expects_institution)
# Every detect-capable source MUST appear here with expects_last4=True; a
# binding-only source is listed with expects_last4=False and MUST assert NO last4
# (proving it does not silently fabricate one). A new format added without a row
# here is a coverage gap the reviewer catches.
_CONTRACT: list[tuple[str, Callable[[ImportService], None], bool, bool]] = [
    ("ofx", _import_ofx, True, True),
    ("csv_with_label", _import_csv_with_label, True, False),
    ("bare_csv", _import_bare_csv, False, False),
]


@pytest.mark.scenarios
@pytest.mark.slow
@pytest.mark.parametrize(
    ("source", "importer", "expects_last4", "expects_institution"),
    _CONTRACT,
    ids=[c[0] for c in _CONTRACT],
)
def test_capture_contract(
    source: str,
    importer: Callable[[ImportService], None],
    expects_last4: bool,
    expects_institution: bool,
) -> None:
    scenario = Scenario(
        scenario="account-identity-capture-contract",
        setup=SetupSpec(persona="family"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        importer(ImportService(db))
        run_step("transform", scenario.setup, db, env=env)
        rows = db.execute(
            "SELECT last_four, institution_name FROM core.dim_accounts"
        ).fetchall()
        assert rows, f"{source}: no dim_accounts row after import"
        has_last4 = any(r[0] for r in rows)
        if expects_last4:
            assert has_last4, (
                f"{source}: detect-capable source landed NO last_four in dim_accounts "
                "— capture gap (the original 'nothing captures last4' bug)"
            )
        else:
            assert not has_last4, (
                f"{source}: binding-only source fabricated a last_four {rows!r} "
                "— it must land NONE, not a spurious value"
            )
        if expects_institution:
            assert any(r[1] for r in rows), f"{source}: landed no institution_name"
