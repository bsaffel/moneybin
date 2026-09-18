"""Auto-saved tabular formats must carry columns only, never an account binding."""

from __future__ import annotations

from pathlib import Path

from moneybin.database import Database
from tests.import_helpers import import_answering_gate


def test_autosaved_format_does_not_store_account_name_as_institution(
    db: Database, tmp_path: Path
) -> None:
    """Auto-saved format.institution_name must never contain the per-account label (bug #5)."""
    from moneybin.services.import_service import ImportService

    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2026-01-15,Coffee,-12.50\n")
    svc = ImportService(db)
    import_answering_gate(
        svc,
        csv,
        account_name="WF Checking (...1212)",
        confirm=True,
        actor_kind="human",
        save_format=True,
        refresh=False,
    )
    row = db.execute("SELECT institution_name FROM app.tabular_formats").fetchone()
    assert row is not None, "expected an auto-saved format row"
    assert "1212" not in row[0] and "WF Checking" not in row[0], (
        f"account label leaked into format.institution_name: {row[0]!r}"
    )


def test_explicit_account_name_overrides_saved_format_binding(
    db: Database, tmp_path: Path
) -> None:
    """Two structurally-identical CSVs imported with DIFFERENT explicit account names.

    They land on DISTINCT accounts, even though the second matches the saved
    format. A saved format carries columns only, never an account binding (#5).
    """
    from moneybin.services.import_service import ImportService

    svc = ImportService(db)
    a = tmp_path / "a.csv"
    a.write_text("Date,Description,Amount\n2026-01-01,X,-1.00\n")
    b = tmp_path / "b.csv"
    b.write_text("Date,Description,Amount\n2026-02-01,Y,-2.00\n")
    # First import saves the format (header signature = these columns).
    import_answering_gate(
        svc,
        a,
        account_name="WF Checking",
        confirm=True,
        actor_kind="human",
        save_format=True,
        refresh=False,
    )
    # Second import is structurally identical -> matches the saved format,
    # but carries a DIFFERENT explicit account name.
    import_answering_gate(
        svc,
        b,
        account_name="WF Savings",
        confirm=True,
        actor_kind="human",
        refresh=False,
    )
    keys = {
        r[0]
        for r in db.execute(
            "SELECT ref_value FROM app.account_links WHERE ref_kind = 'source_native' "
            "AND source_type IN ('csv', 'tsv', 'excel')"
        ).fetchall()
    }
    assert keys == {"wf-checking", "wf-savings"}, keys


def test_autosaved_format_never_pins_a_header_position(
    db: Database, tmp_path: Path
) -> None:
    """The auto-save omits ``skip_rows`` even for a file with a preamble.

    This pins a DELIBERATE omission, not an oversight: a saved format
    describes the column layout, not the header's position, so every read
    re-detects the header fresh. Pinning the position this import detected
    would instead consume a transaction as the header the first time a
    future export of this same layout grows one more preamble line.
    Changing this assertion changes import behavior — see the comment beside
    the ``TabularFormat(...)`` construction in ``import_service.py``.
    """
    from moneybin.services.import_service import ImportService

    csv = tmp_path / "with_preamble.csv"
    csv.write_text(
        "Bank Summary Report\n"
        "Generated: 2026-01-15\n"
        "Date,Amount,Description\n"
        "2026-01-05,-4.50,Coffee\n"
        "2026-01-06,100.00,Payroll\n"
    )
    svc = ImportService(db)
    result = import_answering_gate(
        svc,
        csv,
        account_name="Checking",
        confirm=True,
        actor_kind="human",
        save_format=True,
        refresh=False,
    )
    # Load the two transactions and neither preamble line: this is what makes
    # the skip_rows assertion below mean something. A stored 0 is also what a
    # file with no preamble produces, so without proving detection actually
    # found and skipped two rows, the assertion would pass for the wrong
    # reason.
    assert result.transactions == 2, result

    row = db.execute("SELECT skip_rows FROM app.tabular_formats").fetchone()
    assert row is not None, "expected an auto-saved format row"
    assert row[0] == 0


def test_autosaved_format_records_resolved_institution(
    db: Database, tmp_path: Path
) -> None:
    """An auto-saved format records the filename-resolved institution, not "unknown".

    A future import matching the format then inherits a real institution hint for
    the cross-source bridge. The account-label leak stays fixed; only a
    genuinely-resolved institution lands here.
    """
    from moneybin.services.import_service import ImportService

    csv = tmp_path / "wells_fargo_export.csv"
    csv.write_text("Date,Description,Amount\n2026-01-15,Coffee,-12.50\n")
    svc = ImportService(db)
    import_answering_gate(
        svc,
        csv,
        account_name="Checking",
        confirm=True,
        actor_kind="human",
        save_format=True,
        refresh=False,
    )
    row = db.execute("SELECT institution_name FROM app.tabular_formats").fetchone()
    assert row is not None and row[0] == "wells_fargo", row
