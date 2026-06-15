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
        result = service.import_file(fixture, refresh=False)

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
        service.import_file(fixture, refresh=False)

        history = import_log.get_import_history(db, limit=5)
        latest = [h for h in history if h["source_type"] == "ofx"][0]
        import_id = latest["import_id"]
        assert isinstance(import_id, str)

        result = ImportService(db).revert(import_id)
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
        service.import_file(fixture, refresh=False)

        with pytest.raises(ValueError, match="already imported"):
            service.import_file(fixture, refresh=False)

    def test_reimport_with_force_creates_new_batch(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        service.import_file(fixture, refresh=False)
        service.import_file(fixture, refresh=False, force=True)

        canonical = str(fixture.resolve())
        history = import_log.get_import_history(db, limit=10)
        ofx_for_file = [
            h
            for h in history
            if h["source_type"] == "ofx" and h["source_file"] == canonical
        ]
        assert len(ofx_for_file) == 2


class TestImportOFXAccountResolution:
    """OFX import populates app.account_links via AccountResolver (A7)."""

    def test_import_writes_accepted_source_native_link(self, db: Database) -> None:
        """Each OFX account yields an accepted source_native link in app.account_links.

        sample_minimal.ofx has one account: ACCTID=1111, BANKID(routing)=123456789.
        The resolver mints a canonical account and writes the accepted
        source_native mapping (ref_value = the ACCTID, source_type='ofx') — this
        is the ref the B1 staging translation JOIN keys on, so it must be total
        for new OFX imports. refresh=False skips the SQLMesh apply (no
        core.dim_accounts needed here).

        Per account-identity-resolution.md Decision 3 step 2, the mint path writes
        ONLY source_native; the scoped full_number is passed into the resolver (so
        a later same-number import can adopt via step 1) but is NOT itself written
        as an accepted strong ref on a fresh mint — asserted negatively below to
        pin that contract. See the A7 report concern re: mint-time strong-ref write.
        """
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        ImportService(db).import_file(fixture, refresh=False)

        native = db.execute(
            """
            SELECT account_id FROM app.account_links
            WHERE status = 'accepted' AND ref_kind = 'source_native'
              AND source_type = 'ofx' AND ref_value = ?
            """,
            ["1111"],
        ).fetchall()
        assert len(native) == 1
        assert len(native[0][0]) == 12  # minted canonical uuid4[:12]

        # Spec Decision 3 step 2: a fresh mint records source_native only; the
        # scoped full_number is not yet an accepted strong ref.
        full_number = db.execute(
            """
            SELECT COUNT(*) FROM app.account_links
            WHERE status = 'accepted' AND ref_kind = 'full_number'
              AND source_type = 'ofx'
            """,
        ).fetchone()
        assert full_number is not None and full_number[0] == 0
