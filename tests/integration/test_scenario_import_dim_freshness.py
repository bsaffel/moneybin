# ruff: noqa: S101
"""Regression scenario: multi-file import refreshes core.dim_accounts.

Guards against the 2026-05-15 finding: a multi-file import previously left
core.dim_accounts stale because transforms ran per-file (last-write-wins on
side effects) instead of once after the batch landed.  With import_files()
applying transforms at end-of-batch via ImportService.apply_post_import_hooks,
all distinct accounts from every imported file must be visible after one call,
and ``updated_at`` must advance.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.import_service import ImportService
from moneybin.services.system_service import SystemService

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "ofx"
_KEY = "scenario-dim-freshness-key-0123456789ab"


def _secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _KEY
    return store


def _build_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    db_path = tmp_path / "dim_freshness.duckdb"
    db = Database(db_path, secret_store=_secret_store())
    settings = MagicMock()
    settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: settings)
    return db


@pytest.mark.integration
@pytest.mark.slow
def test_multifile_import_makes_all_accounts_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Four OFX files → five distinct accounts visible after one import_files call.

    Derived from hand-inspection of the chosen fixtures' ``<ACCTID>`` values:
      - sample_minimal.ofx       → 1111
      - multi_account_sample.ofx → CHECKING1, SAVINGS1   (two accounts)
      - qbo_bank_sample.qbo      → 4444555566667777
      - qbo_intuit_sample.qbo    → 2222
    Total distinct ACCTIDs: 5.  This count is derived from the fixtures, not
    from observing the program output (per .claude/rules/testing.md §Scenario
    Expectations Must Be Independently Derived).

    ``missing_institution_sample.ofx`` is deliberately excluded: it requires
    interactive institution resolution and therefore fails non-interactive
    batch import.  Its single account (NOFI001) is not exercised here.
    """
    db = _build_db(tmp_path, monkeypatch)
    paths = [
        FIXTURES_DIR / "sample_minimal.ofx",
        FIXTURES_DIR / "multi_account_sample.ofx",
        FIXTURES_DIR / "qbo_bank_sample.qbo",
        FIXTURES_DIR / "qbo_intuit_sample.qbo",
    ]
    for p in paths:
        assert p.exists(), f"missing fixture: {p}"

    batch = ImportService(db).import_files(list(paths), apply_transforms=True)
    assert batch.imported_count == 4
    assert batch.failed_count == 0
    assert batch.transforms_applied is True

    account_count = db.execute("SELECT COUNT(*) FROM core.dim_accounts").fetchone()
    assert account_count is not None
    # Five distinct ACCTIDs from the four fixtures listed above.
    assert account_count[0] == 5, (
        f"Expected 5 accounts in core.dim_accounts, found {account_count[0]}. "
        "Regression: this is the 2026-05-15 multi-file dim-staleness pattern."
    )

    post_max = db.execute(
        "SELECT MAX(updated_at)::TIMESTAMP FROM core.dim_accounts"
    ).fetchone()
    assert post_max is not None
    assert post_max[0] is not None, "core.dim_accounts.updated_at is NULL"

    status = SystemService(db).status()
    assert status.transforms_pending is False
    assert status.transforms_last_apply_at is not None
