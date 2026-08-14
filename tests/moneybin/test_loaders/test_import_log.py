"""Tests for the generic import_log module."""

import json
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportService


def test_get_import_history_preserves_legacy_started_at_only_query() -> None:
    """The live import-status helper keeps its frozen ordering behavior."""
    db = MagicMock(spec=Database)
    db.execute.return_value.fetchall.return_value = []

    import_log.get_import_history(db, limit=7)

    query, params = db.execute.call_args.args
    assert "ORDER BY started_at DESC\n            LIMIT ?" in query
    assert "import_id DESC" not in query
    assert "OFFSET" not in query
    assert params == [7]


class TestBeginImport:
    """begin_import creates a 'importing' status row and returns a UUID."""

    def test_returns_uuid_string(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking", "savings"],
        )
        assert len(import_id) == 36
        assert import_id.count("-") == 4

    def test_writes_pending_row(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        row = db.execute(
            "SELECT source_file, source_type, source_origin, status, account_names "
            "FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert row is not None
        assert row[0] == "/tmp/test.ofx"  # noqa: S108  # test fixture path in assertion
        assert row[1] == "ofx"
        assert row[2] == "wells_fargo"
        assert row[3] == "importing"
        assert json.loads(row[4]) == ["checking"]


class TestFinalizeImport:
    """finalize_import updates status, counts, and completed_at."""

    def test_marks_complete(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db,
            import_id,
            status="complete",
            rows_total=100,
            rows_imported=100,
        )
        row = db.execute(
            "SELECT status, rows_imported, completed_at "
            "FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert row is not None
        assert row[0] == "complete"
        assert row[1] == 100
        assert row[2] is not None


class TestFindExistingImport:
    """find_existing_import detects prior imports of the same source_file."""

    def test_returns_none_for_new_file(self, db: Database) -> None:
        result = import_log.find_existing_import(db, "/tmp/never_imported.ofx")  # noqa: S108  # test fixture path
        assert result is None

    def test_returns_import_id_and_status_for_imported_file(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/once.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(db, "/tmp/once.ofx")  # noqa: S108  # test fixture path
        assert result == (import_id, "complete")

    def test_returns_importing_status_for_in_progress_batch(self, db: Database) -> None:
        """A crashed/in-progress batch is detectable so callers can craft a clear error."""
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/in_progress.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        # Don't finalize — simulate a crash mid-import.
        result = import_log.find_existing_import(db, "/tmp/in_progress.ofx")  # noqa: S108  # test fixture path
        assert result == (import_id, "importing")

    def test_skips_reverted_imports(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/reverted.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=0, rows_imported=0
        )
        # Revert lives on the service, not the loader; use it as setup so the
        # real assertion (find_existing_import skips reverted batches) stays
        # focused on this module's behavior.
        ImportService(db).revert_confirmed(import_id, verify=lambda _live: None)
        result = import_log.find_existing_import(db, "/tmp/reverted.ofx")  # noqa: S108  # test fixture path
        assert result is None


class TestFindExistingImportByContent:
    """A moved, renamed, or re-downloaded copy is still the same document.

    Path-only matching misses the ordinary cases: the browser saving a second
    download as ``statement (1).pdf``, or the user filing the statement out of
    Downloads before importing. Both land as a fresh batch, and the import then
    re-asks which account the statement belongs to — a second decision point on
    a document already answered, where a different answer re-keys every row.
    """

    _DIGEST = "a" * 64
    _OTHER_DIGEST = "b" * 64

    def test_matches_a_renamed_copy_by_content(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/statement.pdf",  # noqa: S108  # test fixture path
            source_type="pdf",
            source_origin="chase",
            account_names=["sapphire"],
            file_sha256=self._DIGEST,
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(
            db,
            "/tmp/Statements/2026-06 chase.pdf",  # noqa: S108  # test fixture path
            file_sha256=self._DIGEST,
        )
        assert result == (import_id, "complete")

    def test_a_changed_file_at_the_same_path_is_a_new_import(
        self, db: Database
    ) -> None:
        """Same name, different bytes — July's statement saved over June's.

        Reusing one filename is ordinary: a fixed download-manager target, or
        just overwriting last month's file. The path predicate exists for rows
        that predate ``file_sha256`` and therefore carry NULL; once a row has a
        real digest, content is what identifies it, and different bytes are a
        different document. Matching such a row on path alone would reject a
        genuinely new statement as "already imported".
        """
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
            file_sha256=self._DIGEST,
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(
            db,
            "/tmp/Downloads/statement.ofx",  # noqa: S108  # same path...
            file_sha256=self._OTHER_DIGEST,  # ...different bytes
        )
        assert result is None

    def test_does_not_match_different_content_at_a_new_path(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/june.pdf",  # noqa: S108  # test fixture path
            source_type="pdf",
            source_origin="chase",
            account_names=["sapphire"],
            file_sha256=self._DIGEST,
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(
            db,
            "/tmp/Downloads/july.pdf",  # noqa: S108  # test fixture path
            file_sha256=self._OTHER_DIGEST,
        )
        assert result is None

    def test_a_real_digest_never_matches_a_row_imported_before_the_column(
        self, db: Database
    ) -> None:
        """Batches predating file_sha256 carry NULL — not a wildcard."""
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/legacy.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(
            db,
            "/tmp/Downloads/legacy-copy.ofx",  # noqa: S108  # test fixture path
            file_sha256=self._DIGEST,
        )
        assert result is None

    def test_an_unknown_digest_does_not_collapse_two_legacy_rows(
        self, db: Database
    ) -> None:
        """NULL == NULL must not match: a caller with no digest matches on path only."""
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/legacy.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(
            db,
            "/tmp/Downloads/unrelated.ofx",  # noqa: S108  # test fixture path
            file_sha256=None,
        )
        assert result is None

    def test_a_legacy_path_blocks_until_that_path_has_a_digest(
        self, db: Database
    ) -> None:
        """The first re-import at a legacy path is still refused.

        A row predating ``file_sha256`` carries no content to compare against,
        so whether this is the same document is genuinely unknowable. Refusing
        and letting ``--force`` decide is the conservative answer, and this
        pins it: only the *permanence* below is the defect.
        """
        legacy = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, legacy, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(
            db,
            "/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            file_sha256=self._DIGEST,
        )
        assert result == (legacy, "complete")

    def test_a_legacy_row_stops_answering_once_its_path_has_a_digest(
        self, db: Database
    ) -> None:
        """A legacy row must not veto every future import at its path.

        Nothing backfills that NULL — a ``--force`` re-import writes a new
        digest-backed row *beside* the legacy one rather than retiring it. So
        while the path fallback stays live, the legacy row keeps answering for
        its path forever, and every later statement saved to a reused download
        target is rejected as already imported. Once the same path has been
        imported with a real digest, content is available for that path and the
        legacy row's "something was imported here" is subsumed by it.
        """
        legacy = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, legacy, status="complete", rows_total=1, rows_imported=1
        )
        digest_backed = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
            file_sha256=self._DIGEST,
        )
        import_log.finalize_import(
            db, digest_backed, status="complete", rows_total=1, rows_imported=1
        )

        # Next month's statement, saved over the same filename: neither row
        # describes these bytes.
        result = import_log.find_existing_import(
            db,
            "/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            file_sha256=self._OTHER_DIGEST,
        )
        assert result is None

    def test_a_digest_backed_path_still_recognizes_its_own_content(
        self, db: Database
    ) -> None:
        """Retiring the legacy fallback must not cost real duplicate detection."""
        legacy = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, legacy, status="complete", rows_total=1, rows_imported=1
        )
        digest_backed = import_log.begin_import(
            db,
            source_file="/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
            file_sha256=self._DIGEST,
        )
        import_log.finalize_import(
            db, digest_backed, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(
            db,
            "/tmp/Downloads/statement.ofx",  # noqa: S108  # test fixture path
            file_sha256=self._DIGEST,
        )
        assert result == (digest_backed, "complete")


class TestBeginImportValidatesSourceType:
    """begin_import raises ValueError for unrecognized source_type values."""

    def test_rejects_unknown_source_type(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown source_type"):
            import_log.begin_import(
                db,
                source_file="/tmp/x",  # noqa: S108  # test fixture path
                source_type="nope",  # type: ignore[arg-type]  # intentional: testing runtime validation
                source_origin="x",
                account_names=[],
            )
