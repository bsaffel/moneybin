"""Integration tests for ImportService PDF import path (Phase 1: seed-only)."""

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from moneybin.database import Database
from moneybin.services.import_service import (
    ImportService,
    _pdf_alias,  # type: ignore[reportPrivateUsage]
)


@pytest.mark.integration
def test_import_pdf_lands_as_seed(db: Database, simple_statement_pdf: Path) -> None:
    result = ImportService(db).import_file(simple_statement_pdf, refresh=False)
    assert result.file_type == "pdf"
    assert result.import_id is not None
    # View name = "pdf_" + _pdf_alias(None, fixture).
    # For "simple_statement.pdf", that resolves to "pdf_simple_statement".
    row = db.execute("SELECT COUNT(*) FROM raw.pdf_simple_statement").fetchone()
    assert row is not None
    assert row[0] == 3  # 3 transaction rows in the simple_statement.pdf fixture


@pytest.mark.integration
def test_import_pdf_is_revertible(db: Database, simple_statement_pdf: Path) -> None:
    svc = ImportService(db)
    result = svc.import_file(simple_statement_pdf, refresh=False)
    assert result.import_id is not None
    out = svc.revert(result.import_id)
    assert out["status"] == "reverted"
    row = db.execute("SELECT COUNT(*) FROM raw.pdf_seeds").fetchone()
    assert row is not None
    assert row[0] == 0
    # Revert must also drop the auto-generated raw.pdf_<alias> view.
    view_exists = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name = 'pdf_simple_statement'"
    ).fetchone()
    assert view_exists is not None
    assert view_exists[0] == 0, (
        "revert should drop the auto-generated raw.pdf_<alias> view"
    )


@pytest.mark.integration
def test_import_pdf_zero_rows_raises(db: Database, empty_statement_pdf: Path) -> None:
    """Importing a text-only PDF with no tables must raise, not silently succeed."""
    with pytest.raises(ValueError, match="No tables extracted"):
        ImportService(db).import_file(empty_statement_pdf, refresh=False)
    # No degenerate view should have been created during the failed import.
    view_count = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name LIKE 'pdf_%'"
    ).fetchone()
    assert view_count is not None
    assert view_count[0] == 0, "failed import must not leave orphan views"


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("simple_statement.pdf", "simple_statement"),
        ("2024_Q4.pdf", "2024_q4"),
        (".pdfrc", "pdfrc"),
        # 80-char stem → 54-char truncation + "_" + 4-char content hash
        # (deterministic via SHA-256 of the full slug).
        (("a" * 80) + ".pdf", ("a" * 54) + "_0f45"),
    ],
    ids=[
        "stem_clean_letter_start",
        "stem_leading_digit_no_prefix",
        "stem_leading_dot_stripped_letter_start",
        "long_stem_truncated_with_hash_suffix",
    ],
)
def test_pdf_alias_resolves(filename: str, expected: str) -> None:
    assert _pdf_alias(Path(filename)) == expected


def test_pdf_alias_long_stems_avoid_collision() -> None:
    """Distinct long filenames sharing a 59-char prefix get distinct aliases."""
    a = _pdf_alias(Path("bank_statement_checking_account_january_2024.pdf"))
    b = _pdf_alias(Path("bank_statement_checking_account_january_2025.pdf"))
    # Both inputs are <60 chars, so neither triggers truncation — they
    # naturally diverge. The collision case is for >59-char stems.
    assert a != b

    # Now exercise the truncation path explicitly.
    long_a = "x" * 56 + "_january_2024"
    long_b = "x" * 56 + "_january_2025"
    alias_a = _pdf_alias(Path(long_a + ".pdf"))
    alias_b = _pdf_alias(Path(long_b + ".pdf"))
    assert len(alias_a) <= 59
    assert len(alias_b) <= 59
    assert alias_a != alias_b, (
        f"long stems sharing the first 59 chars must hash to distinct aliases; "
        f"got alias_a={alias_a!r} alias_b={alias_b!r}"
    )


@pytest.mark.integration
def test_import_pdf_cleans_orphans_on_view_failure(
    db: Database, simple_statement_pdf: Path
) -> None:
    """Rows written to raw.pdf_seeds must be deleted if view creation fails after the seed insert."""
    with patch(
        "moneybin.extractors.pdf.seed_store.generate_seed_view_sql",
        side_effect=ValueError("forced for test"),
    ):
        with pytest.raises(ValueError, match="forced for test"):
            ImportService(db).import_file(simple_statement_pdf, refresh=False)
    # The just-inserted rows must be cleaned up — no orphan rows should remain.
    row = db.execute("SELECT COUNT(*) FROM raw.pdf_seeds").fetchone()
    assert row is not None
    assert row[0] == 0


@pytest.mark.integration
def test_revert_preserves_view_when_other_imports_remain(
    db: Database, simple_statement_pdf: Path, tmp_path: Path
) -> None:
    """Reverting one PDF import should not drop the view if another import shares its alias.

    With on_conflict='ignore', import A owns all rows for identical content.
    After revoking A the view still exists (import B's log entry is still
    complete) but contains no rows — B never acquired ownership of any rows.
    The key invariant is view persistence, not row count.
    """
    # Two physical files that resolve to the same alias (same stem)
    a = tmp_path / "a" / "simple_statement.pdf"
    b = tmp_path / "b" / "simple_statement.pdf"
    a.parent.mkdir()
    b.parent.mkdir()
    shutil.copy(simple_statement_pdf, a)
    shutil.copy(simple_statement_pdf, b)

    svc = ImportService(db)
    result_a = svc.import_file(a, refresh=False)
    result_b = svc.import_file(b, refresh=False)
    assert result_a.import_id is not None
    assert result_b.import_id is not None

    svc.revert(result_a.import_id)

    # View must still exist because result_b's import remains complete.
    view_count = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name = 'pdf_simple_statement'"
    ).fetchone()
    assert view_count is not None
    assert view_count[0] == 1, (
        "view should remain when another import still references the alias"
    )


@pytest.mark.integration
def test_reimport_preserves_first_import_id_ownership(
    db: Database, simple_statement_pdf: Path, tmp_path: Path
) -> None:
    """Re-importing identical content keeps rows owned by the first import_id.

    Otherwise reverting the second import would orphan the first import's
    log entry — its rows would be gone but its status stays 'complete'.
    """
    a = tmp_path / "a" / "simple_statement.pdf"
    b = tmp_path / "b" / "simple_statement.pdf"
    a.parent.mkdir()
    b.parent.mkdir()
    shutil.copy(simple_statement_pdf, a)
    shutil.copy(simple_statement_pdf, b)

    svc = ImportService(db)
    result_a = svc.import_file(a, refresh=False)
    assert result_a.import_id is not None
    rows_after_a = db.execute(
        "SELECT import_id FROM raw.pdf_seeds WHERE alias = 'simple_statement'"
    ).fetchall()

    result_b = svc.import_file(b, refresh=False)
    assert result_b.import_id is not None
    rows_after_b = db.execute(
        "SELECT import_id FROM raw.pdf_seeds WHERE alias = 'simple_statement'"
    ).fetchall()

    # Same number of rows (no duplicates created by import B)
    assert len(rows_after_b) == len(rows_after_a)
    # All rows still owned by import A — ignore preserved ownership
    for (import_id,) in rows_after_b:
        assert import_id == result_a.import_id

    # Reverting B leaves A's rows intact
    svc.revert(result_b.import_id)
    rows_after_revert_b = db.execute(
        "SELECT COUNT(*) FROM raw.pdf_seeds WHERE alias = 'simple_statement'"
    ).fetchone()
    assert rows_after_revert_b is not None
    assert rows_after_revert_b[0] == len(rows_after_a)


@pytest.mark.integration
def test_reimport_does_not_inflate_rows_imported(
    db: Database, simple_statement_pdf: Path, tmp_path: Path
) -> None:
    """Re-importing identical content must record rows_imported=0 in the audit log.

    The pre-fix code passed the extracted count as both rows_total and
    rows_imported, so a re-import of an existing statement showed
    rows_imported=N in the audit log even though on_conflict='ignore'
    inserted 0 new rows. Two "successful 3-row imports" implied 6 rows in
    the DB when there were only 3.
    """
    a = tmp_path / "a" / "simple_statement.pdf"
    b = tmp_path / "b" / "simple_statement.pdf"
    a.parent.mkdir()
    b.parent.mkdir()
    shutil.copy(simple_statement_pdf, a)
    shutil.copy(simple_statement_pdf, b)

    svc = ImportService(db)
    result_a = svc.import_file(a, refresh=False)
    result_b = svc.import_file(b, refresh=False)
    assert result_a.import_id is not None
    assert result_b.import_id is not None

    log_a = db.execute(
        "SELECT rows_total, rows_imported FROM raw.import_log WHERE import_id = ?",
        [result_a.import_id],
    ).fetchone()
    log_b = db.execute(
        "SELECT rows_total, rows_imported FROM raw.import_log WHERE import_id = ?",
        [result_b.import_id],
    ).fetchone()
    assert log_a is not None
    assert log_b is not None
    # A persisted all rows; B persisted none (all hashes already present).
    assert log_a[0] == log_a[1]  # rows_total == rows_imported for first import
    assert log_a[1] > 0
    assert log_b[0] == log_a[1]  # rows_total still reflects extraction count
    assert log_b[1] == 0  # rows_imported == 0 — nothing new written


@pytest.mark.integration
def test_cleanup_failure_still_finalizes_import_log(
    db: Database, simple_statement_pdf: Path
) -> None:
    """A failing cleanup DELETE must not leave import_log stuck in 'in_progress'.

    Without the finally-block guard, a DB error during the post-failure
    DELETE would propagate and skip finalize_import, leaving the audit
    entry permanently in 'in_progress' with no CLI recovery path.
    """
    svc = ImportService(db)
    real_execute = db.execute
    delete_attempts: list[str] = []

    def flaky_execute(sql: str, *args: object, **kwargs: object) -> object:
        if "DELETE FROM raw.pdf_seeds" in sql:
            delete_attempts.append(sql)
            raise RuntimeError("simulated cleanup-time DB error")
        return real_execute(sql, *args, **kwargs)  # type: ignore[arg-type]

    with patch(
        "moneybin.extractors.pdf.seed_store.generate_seed_view_sql",
        side_effect=ValueError("forced view-creation failure"),
    ):
        with patch.object(db, "execute", side_effect=flaky_execute):
            with pytest.raises(ValueError, match="forced view-creation failure"):
                svc.import_file(simple_statement_pdf, refresh=False)

    # The original exception (ValueError) propagated, not the cleanup error.
    assert delete_attempts, "cleanup DELETE must have been attempted"
    # The import_log entry was finalized as 'failed' despite the cleanup error.
    log = db.execute(
        "SELECT status FROM raw.import_log "
        "WHERE source_type = 'pdf' ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    assert log is not None
    assert log[0] == "failed", (
        f"expected import_log.status='failed', got {log[0]!r} "
        "— finalize_import must run even when cleanup DELETE raises"
    )
