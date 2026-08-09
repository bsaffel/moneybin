"""Raw third-party exception text must not reach the wire."""

from pathlib import Path
from typing import Any

import pytest

from moneybin.services.import_service import ImportService, per_file_failure
from tests.import_helpers import import_answering_gate

# Stands in for the payee/amount/memo content ofxparse and Polars embed in
# their exception strings when they choke on a row.
PII = "SAFEWAY #1234 CARDHOLDER JANE Q PUBLIC 4111111111111111"


def test_ofx_parse_failure_does_not_put_parser_text_on_the_wire(
    db: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ofxparse

    ofx = tmp_path / "statement.ofx"
    ofx.write_text("OFXHEADER:100\n<OFX></OFX>")

    def _boom(*args: object, **kwargs: object) -> Any:
        raise ValueError(f"could not parse line: {PII}")

    monkeypatch.setattr(ofxparse.OfxParser, "parse", staticmethod(_boom))

    with pytest.raises(ValueError) as raised:
        ImportService(db).import_file(ofx, refresh=False)

    message, _code, _hint, _details = per_file_failure(raised.value)
    assert PII not in message, f"parser text reached the wire: {message}"
    assert PII not in str(raised.value)
    # Still actionable: the user must learn the file could not be parsed.
    assert "OFX" in message


def test_transform_failure_does_not_put_polars_text_on_the_wire(
    db: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The tabular twin of the OFX guard.

    Polars conversion errors quote the offending cell, so a failed transform
    would otherwise publish a payee or amount through `PerFileResult.error`.
    The reviewer named only the OFX site; this path has the same shape.
    """

    def _boom(**kwargs: object) -> Any:
        raise ValueError(f"could not convert '{PII}' to date")

    # Patched at its definition module: import_service imports it inside the
    # function body, so the module-attribute route never resolves.
    monkeypatch.setattr(
        "moneybin.extractors.tabular.transforms.transform_dataframe", _boom
    )

    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2024-01-15,Coffee,-4.50\n")

    # auto_accept clears the unknown-layout confirmation gate, which is
    # upstream of the transform stage under test and has its own coverage.
    with pytest.raises(ValueError) as raised:
        import_answering_gate(
            ImportService(db),
            csv,
            refresh=False,
            account_name="Checking",
            confirm=True,
            auto_accept=True,
        )

    message, _code, _hint, _details = per_file_failure(raised.value)
    assert PII not in message, f"parser text reached the wire: {message}"
    assert "Transform failed" in str(raised.value)


# An <ACCTID> long enough that no other field could produce it by accident.
ACCTID = "987654321098"


def _ofx_with_acctid(tmp_path: Path, acctid: str = ACCTID) -> Path:
    """The minimal fixture, re-keyed to an account number worth protecting."""
    source = Path("tests/fixtures/ofx/sample_minimal.ofx").read_text(encoding="utf-8")
    ofx = tmp_path / "statement.ofx"
    ofx.write_text(
        source.replace("<ACCTID>1111</ACCTID>", f"<ACCTID>{acctid}</ACCTID>"),
        encoding="utf-8",
    )
    return ofx


def test_empty_binding_value_does_not_echo_the_files_account_number(
    db: Any, tmp_path: Path
) -> None:
    """Rejecting a blank binding must not quote the OFX ``<ACCTID>`` back.

    ``_resolve_binding_targets`` already states the rule — echoing the caller's
    own unknown keys is safe, listing the file's real ones is not — but the
    sibling raise in ``_apply_account_bindings`` named ``source_account_key``,
    which on this channel IS an account number. ``classify_user_error`` maps a
    bare ValueError to ``UserError(str(exc))`` with the message intact, so it
    reaches an MCP caller verbatim; ``per_file_failure``'s scrubbing only covers
    UNclassified exceptions.
    """
    ofx = _ofx_with_acctid(tmp_path)

    with pytest.raises(ValueError) as raised:
        ImportService(db).import_file(
            ofx, refresh=False, account_bindings={"@0": "   "}
        )

    message, _code, _hint, _details = per_file_failure(raised.value)
    assert ACCTID not in message, f"account number reached the wire: {message}"
    assert ACCTID not in str(raised.value)
    # Still answerable: the caller learns which proposal it fumbled.
    assert "@0" in message


def test_a_minted_accounts_display_name_does_not_carry_an_account_number(
    db: Any, tmp_path: Path
) -> None:
    """The mint report names the account without republishing its number.

    ``CreatedAccount.display_name`` is declared ``USER_NOTE`` (MEDIUM) and
    documented as safe to show, and it reaches the terminal, the CLI/MCP
    ``accounts_created`` rows and the inbox drain unmasked. For tabular it is
    derived from the file's own account column via ``parse_account_label``,
    which strips only a *recognized masked* last-four — ``(...1789)``, ``x1789``,
    a bare trailing group. A genuinely unmasked full number matches none of
    those patterns and passed through whole, so the one shape that actually
    needed stripping was the one that survived.

    The label stays readable: only the digit run is masked, because the point
    of this field is to name what was created.
    """
    csv = tmp_path / "txns.csv"
    csv.write_text(
        f"Date,Description,Amount,Account\n2024-01-15,Coffee,-4.50,Checking {ACCTID}\n"
    )

    result = import_answering_gate(
        ImportService(db),
        csv,
        refresh=False,
        confirm=True,
        auto_accept=True,
    )

    created = result.accounts_created
    assert len(created) == 1, created
    assert ACCTID not in created[0].display_name, created[0].display_name
    # Still names the account — masking the whole label would defeat the field.
    assert created[0].display_name.startswith("Checking")
    assert created[0].display_name.endswith(ACCTID[-4:])


def test_unknown_account_metadata_key_does_not_list_the_real_keys(
    db: Any, tmp_path: Path
) -> None:
    """The typo error names what the caller sent, never what the file holds.

    A tabular source key is ``slugify(account_name)``, and a real account label
    routinely carries the number ("Checking 987654321098"), so enumerating this
    file's keys publishes it. The caller's own unknown key stays — they sent it.
    """
    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2024-01-15,Coffee,-4.50\n")

    with pytest.raises(ValueError) as raised:
        import_answering_gate(
            ImportService(db),
            csv,
            refresh=False,
            account_name=f"Checking {ACCTID}",
            confirm=True,
            auto_accept=True,
            account_metadata={"mistyped-key": {"display_name": "Joint"}},
        )

    message, _code, _hint, _details = per_file_failure(raised.value)
    assert ACCTID not in message, f"account number reached the wire: {message}"
    assert "mistyped-key" in message
