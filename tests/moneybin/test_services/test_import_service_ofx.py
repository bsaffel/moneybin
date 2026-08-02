"""Integration tests for ImportService._import_ofx via the new pipeline."""

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportService
from tests.moneybin.import_helpers import import_answering_gate


class TestImportOFXBatchLifecycle:
    """Import batch lifecycle tests for OFX files."""

    def test_import_creates_committed_batch(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        result = import_answering_gate(service, fixture, refresh=False)

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

    def test_shared_fitid_rows_all_survive_import(self, db: Database) -> None:
        """F1 regression: two distinct transactions sharing a FITID both land.

        The fixture has a Chase-style foreign purchase (-13.12) and its
        foreign-transaction fee (-0.39) stamped with one shared FITID, plus a
        third unique-FITID row. Before the extractor's content-based
        disambiguation, the raw primary key
        ``(source_transaction_id, account_id, source_file)`` collapsed the shared
        pair via the ``on_conflict="upsert"`` (``INSERT OR REPLACE``) write,
        silently dropping the $13.12 purchase. All three rows must now survive in
        raw.
        """
        fixture = Path("tests/fixtures/ofx/duplicate_fitid_sample.ofx")
        assert fixture.exists()

        service = ImportService(db)
        import_answering_gate(service, fixture, refresh=False)

        rows = db.execute(
            "SELECT amount FROM raw.ofx_transactions ORDER BY amount"
        ).fetchall()
        amounts = [r[0] for r in rows]
        # All three transactions survive — the shared-FITID pair is not collapsed.
        assert len(amounts) == 3
        assert Decimal("-13.12") in amounts
        assert Decimal("-0.39") in amounts
        assert Decimal("-42.00") in amounts

    def test_reimport_over_legacy_row_does_not_self_heal(self, db: Database) -> None:
        """Characterizes the documented recovery limitation (see CHANGELOG #304).

        A file imported *before* the FITID-collision fix leaves one plain
        ``SHAREDFITID999`` row in raw (the surviving half of the dropped pair);
        that pre-fix state is simulated here by seeding the plain row directly,
        so the single ``import_file`` call below proceeds without needing the
        ``force`` gate or a prior ``raw.import_log`` row. Importing the file
        post-fix emits two content-suffixed rows with new primary keys; because
        the OFX write path upserts by PK (``on_conflict="upsert"``), it inserts
        them and leaves the stale plain row untouched — so the surviving
        transaction would be double-counted. This is why recovering
        already-affected data requires ``import revert`` first, not a bare
        re-import (the write path upserts by PK regardless of ``force``). If
        re-import is ever made replace-by-file (self-healing), flip this test to
        assert the plain row is gone.
        """
        fixture = Path("tests/fixtures/ofx/duplicate_fitid_sample.ofx")

        # Simulate a pre-fix import: one plain-FITID row already in raw.
        db.execute(
            """
            INSERT INTO raw.ofx_transactions
                (source_transaction_id, account_id, transaction_type,
                 date_posted, amount, payee, source_file, source_type)
            VALUES ('SHAREDFITID999', '4387', 'DEBIT', TIMESTAMP '2025-11-19',
                    -0.39, 'FOREIGN TRANSACTION FEE', ?, 'ofx')
            """,
            [str(fixture.resolve())],
        )

        import_answering_gate(ImportService(db), fixture, refresh=False)

        # The stale plain row is NOT replaced by the suffixed rows (no self-heal).
        stale = db.execute(
            "SELECT COUNT(*) FROM raw.ofx_transactions "
            "WHERE source_transaction_id = 'SHAREDFITID999'"
        ).fetchone()
        assert stale is not None and stale[0] == 1
        # The two content-suffixed rows land alongside it.
        suffixed = db.execute(
            "SELECT COUNT(*) FROM raw.ofx_transactions "
            "WHERE source_transaction_id LIKE 'SHAREDFITID999#%'"
        ).fetchone()
        assert suffixed is not None and suffixed[0] == 2

    def test_reverting_ofx_batch_deletes_rows(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        import_answering_gate(service, fixture, refresh=False)

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
        import_answering_gate(service, fixture, refresh=False)

        with pytest.raises(ValueError, match="already imported"):
            service.import_file(fixture, refresh=False)

    def test_reimport_of_a_renamed_copy_raises(
        self, db: Database, tmp_path: Path
    ) -> None:
        """Same bytes, new path — the browser's "(1)" copy is not a new document.

        Path-only matching let this through as a fresh batch, and the import
        then re-asked which account the file belongs to. A different answer
        re-keys every row (transaction ids embed ``account_id``), so both
        answers survive dedup and the statement double-counts.
        """
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )
        renamed = tmp_path / "sample_minimal (1).ofx"
        renamed.write_bytes(fixture.read_bytes())

        service = ImportService(db)
        import_answering_gate(service, fixture, refresh=False)

        with pytest.raises(ValueError, match="already imported"):
            service.import_file(renamed, refresh=False)

    def test_reimport_with_force_creates_new_batch(self, db: Database) -> None:
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        service = ImportService(db)
        import_answering_gate(service, fixture, refresh=False)
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

        Per account-identity-resolution.md Decision 3 step 2, the mint path also
        claims an accepted strong ref for every scoped confirmer the source carries
        — here the OFX `full_number` (BANKID+ACCTID) — so a later source bearing the
        same scoped number auto-adopts via step 1 instead of minting a duplicate.
        Asserted positively below to pin that contract (resolves the A7 report
        concern re: mint-time strong-ref write).
        """
        fixture = Path("tests/fixtures/ofx/sample_minimal.ofx")
        if not fixture.exists():
            pytest.fail(
                "Sample OFX fixture missing at tests/fixtures/ofx/sample_minimal.ofx"
            )

        import_answering_gate(ImportService(db), fixture, refresh=False)

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

        # Spec Decision 3 step 2: a fresh mint also claims the scoped full_number
        # strong ref so a later same-number source auto-adopts (no duplicate).
        full_number = db.execute(
            """
            SELECT COUNT(*) FROM app.account_links
            WHERE status = 'accepted' AND ref_kind = 'full_number'
              AND source_type = 'ofx'
            """,
        ).fetchone()
        assert full_number is not None and full_number[0] == 1


class TestImportOFXPermissionDenied:
    """An unreadable OFX must classify as a permission failure, not a parse one."""

    def test_permission_error_survives_the_ofx_read_boundary(
        self, db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OFX is a core format; the recovery advice must reach it too.

        `_import_ofx` wraps read failures as `ValueError` so MCP's error
        envelope catches them, but that flattening also erased
        `PermissionError` — `classify_user_error` keys the
        `infra_permission_denied` code and its Full-Disk-Access hint off the
        exception TYPE, so a TCC-blocked .qfx reported `infra_invalid_input`
        with no hint while an identically-blocked .csv reported the right one.
        """
        import builtins

        from moneybin.errors import classify_user_error

        target = tmp_path / "statement.ofx"
        target.write_text("<OFX></OFX>")
        real_open = builtins.open

        def _deny(file: object, *args: object, **kwargs: object) -> Any:
            # Scoped to the target so DuckDB and the import log keep working.
            if str(file) == str(target):
                raise PermissionError(1, "Operation not permitted", str(target))
            return real_open(file, *args, **kwargs)  # pyright: ignore[reportCallIssue,reportArgumentType]

        monkeypatch.setattr(builtins, "open", _deny)

        service = ImportService(db)
        with pytest.raises(PermissionError) as raised:
            service.import_file(target, refresh=False)

        # Classify the exception the boundary actually produced — that is the
        # claim under test, not that PermissionError classifies in isolation.
        classified = classify_user_error(raised.value)
        assert classified is not None
        assert classified.code == "infra_permission_denied"
        assert classified.hint is not None


class TestExtractionParsesTheHashedBytes:
    """The digest and the loaded rows must describe one version of the file."""

    def test_a_rewrite_after_the_read_does_not_reach_the_loaded_rows(
        self, db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A synced folder replacing the file mid-import must not split identity.

        The service reads the file once, hashes those bytes, and stamps the
        digest on the batch. Reopening the path to extract puts a window between
        the two: a file that changes inside it is recorded as version A and
        loaded as version B, so B's rows land under A's content identity and a
        later genuine import of B is dismissed as a duplicate. Rewriting the
        file the instant the digest is taken makes that window deterministic.
        """
        from moneybin.services import import_service as import_service_module

        original = Path("tests/fixtures/ofx/sample_minimal.ofx").read_bytes()
        replacement = Path("tests/fixtures/ofx/duplicate_fitid_sample.ofx").read_bytes()
        target = tmp_path / "statement.ofx"
        target.write_bytes(original)

        real_source_sha256 = import_service_module.source_sha256

        def _rewrite_then_hash(path: Path, source_bytes: bytes | None) -> str:
            target.write_bytes(replacement)
            return real_source_sha256(path, source_bytes)

        monkeypatch.setattr(import_service_module, "source_sha256", _rewrite_then_hash)

        # Bound up front rather than via import_answering_gate: the helper
        # re-imports to answer, and the second read would see the replacement —
        # which is the very swap this test exists to keep out of the rows.
        # "1111" is sample_minimal.ofx's <ACCTID>; the replacement's is 4387.
        ImportService(db).import_file(
            target, refresh=False, account_bindings={"1111": "new"}
        )

        loaded = {
            row[0]
            for row in db.execute(
                "SELECT source_transaction_id FROM raw.ofx_transactions"
            ).fetchall()
        }
        # Hand-derived from sample_minimal.ofx: two <FITID> elements. The
        # replacement carries SHAREDFITID999/UNIQUEFITID001, so either file is
        # unambiguous in the result.
        assert loaded == {"FITID001", "FITID002"}
