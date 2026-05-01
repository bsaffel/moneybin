"""Integration tests for ImportService._import_ofx via the new pipeline."""

from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportService


class TestImportOFXBatchLifecycle:
    """Import batch lifecycle tests for OFX files."""

    def test_import_creates_committed_batch(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        result = service.import_file(fixture, apply_transforms=False)

        assert result.transactions > 0

        history = import_log.get_import_history(db, limit=5)
        ofx_imports = [h for h in history if h["source_type"] == "ofx"]
        assert len(ofx_imports) >= 1
        latest = ofx_imports[0]
        assert latest["status"] in ("complete", "partial")
        # rows_imported sums all four OFX tables (institutions, accounts,
        # transactions, balances) so balance-only statements still report > 0.
        expected_total = (
            result.institutions
            + result.accounts
            + result.transactions
            + result.balances
        )
        assert latest["rows_imported"] == expected_total

    def test_reverting_ofx_batch_deletes_rows(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        service.import_file(fixture, apply_transforms=False)

        history = import_log.get_import_history(db, limit=5)
        latest = [h for h in history if h["source_type"] == "ofx"][0]
        import_id = latest["import_id"]
        assert isinstance(import_id, str)

        result = import_log.revert_import(db, import_id)
        assert result["status"] == "reverted"

        remaining_row = db.execute(
            "SELECT COUNT(*) FROM raw.ofx_transactions WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert remaining_row is not None
        assert remaining_row[0] == 0

    def test_reimport_without_force_raises(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        service.import_file(fixture, apply_transforms=False)

        with pytest.raises(ValueError, match="already imported"):
            service.import_file(fixture, apply_transforms=False)

    def test_reimport_with_force_creates_new_batch(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        service.import_file(fixture, apply_transforms=False)
        service.import_file(fixture, apply_transforms=False, force=True)

        canonical = str(fixture.resolve())
        history = import_log.get_import_history(db, limit=10)
        ofx_for_file = [
            h
            for h in history
            if h["source_type"] == "ofx" and h["source_file"] == canonical
        ]
        assert len(ofx_for_file) == 2
