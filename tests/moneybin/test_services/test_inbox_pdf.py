"""Integration test: InboxService routes PDFs through the sync drain.

Confirms that a PDF dropped in the inbox folder is picked up by
InboxService.sync(), imported via ImportService._import_pdf(), and moved
to processed/YYYY-MM/ on success — without requiring any allowlist change
in the inbox layer (_classify accepts every regular file).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import moneybin.extractors.pdf.bridge as bridge_mod
from moneybin.config import ImportSettings, MoneyBinSettings
from moneybin.database import Database
from moneybin.privacy.disclosure import host_for_recipient_name
from moneybin.services.consent_service import ConsentService
from moneybin.services.inbox_service import InboxService


def _make_settings(tmp_path: Path) -> MoneyBinSettings:
    return MoneyBinSettings(
        profile="test",
        import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
    )


@pytest.mark.integration
def test_inbox_processes_pdf(
    db: Database, tmp_path: Path, simple_statement_pdf: Path
) -> None:
    """PDF dropped in inbox/ is imported and moved to processed/YYYY-MM/."""
    year_month = "2026-05"
    svc = InboxService(db=db, settings=_make_settings(tmp_path))
    svc.ensure_layout()

    shutil.copy(simple_statement_pdf, svc.inbox_dir / "simple_statement.pdf")

    result = svc.sync(year_month=year_month, refresh=False)

    # File must have left the inbox root.
    assert not (svc.inbox_dir / "simple_statement.pdf").exists()

    # Exactly one file processed, none failed.
    assert len(result.processed) == 1
    assert len(result.failed) == 0

    entry = result.processed[0]
    assert entry["filename"] == "simple_statement.pdf"
    assert entry["file_type"] == "pdf"

    # File landed in processed/YYYY-MM/.
    processed_path = svc.processed_dir / year_month / "simple_statement.pdf"
    assert processed_path.exists(), f"expected file at {processed_path}"

    # Verify the seed was written to raw.pdf_seeds.
    seed_row = db.execute("SELECT COUNT(*) FROM raw.pdf_seeds").fetchone()
    assert seed_row is not None
    assert seed_row[0] > 0, (
        "inbox import should have written at least one row to raw.pdf_seeds"
    )


@pytest.fixture()
def remembered_grant(db: Database) -> str:
    """A remembered ai_host consent, present while the inbox drains."""
    host = host_for_recipient_name("claude-code")
    assert host is not None
    grant, created = ConsentService(db).grant_host_consent(
        host=host, approved_via="cli_prompt", actor="test"
    )
    assert created
    return grant.grant_id


def test_inbox_drain_never_builds_bridge_request(
    db: Database,
    tmp_path: Path,
    canary_pdf: Path,
    remembered_grant: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unattended drain seeds a bridge-eligible PDF even with a remembered grant."""

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("inbox drain built a bridge request")

    monkeypatch.setattr(bridge_mod, "build_bridge_request", _boom)
    svc = InboxService(db=db, settings=_make_settings(tmp_path))
    svc.ensure_layout()
    shutil.copy(canary_pdf, svc.inbox_dir / "canary_statement.pdf")

    result = svc.sync(year_month="2026-05", refresh=False)

    assert len(result.failed) == 0
    assert len(result.processed) == 1
    parse_rows = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'smart_import_parse'"
    ).fetchone()
    assert parse_rows is not None and parse_rows[0] == 0
