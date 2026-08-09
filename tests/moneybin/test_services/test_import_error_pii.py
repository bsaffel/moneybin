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


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        # Contiguous — the shape parse_account_label leaves entirely alone.
        (f"Checking {ACCTID}", "Checking ****1098"),
        # Grouped. parse_account_label strips only the trailing four-digit
        # token, so the *prefix* of the number survives into the label — and a
        # contiguous-run mask matches none of it, because every remaining run
        # is four digits.
        ("Checking 1234-5678-9012", "Checking ****5678"),
        ("Checking 4111 1111 1111 1111", "Checking ****1111"),
        # Every separator, not an enumerated few: the label parser and the mask
        # have to agree on what a separator is, and each time they did not, the
        # gap was a run of account digits. The parser's trailing-token strip
        # accepts these, so the mask has to as well.
        ("Checking 1234.5678.9012", "Checking ****5678"),
        ("Checking 1234/5678/9012", "Checking ****5678/"),
        ("Checking 1234_5678_9012", "Checking ****5678_"),
        # A three-character separator, which the parser's own trailing-token
        # strip accepts: it lifts `9012` out and hands the mask `1234 - 5678`.
        ("Checking 1234 - 5678 - 9012", "Checking ****5678"),
        # Letters *inside* the identifier, the shape brokerage and investment
        # accounts actually use. The run is bounded by separator length, not by
        # character class, so letters no longer end it.
        ("Brokerage 12AB34CD56", "Brokerage ****3456"),
        ("IRA 12X3456789", "IRA ****6789"),
        # Letter groups of any length. Bounding the gap at three characters was
        # the third enumeration to leak: `ABCD` is not a word, it is the middle
        # of an identifier, and no digit-count-per-group rule can tell them
        # apart. The rule is about words, so the gap length is unbounded.
        ("Brokerage 12ABCD34EFGH56", "Brokerage ****3456"),
        # Trailing text, so the parser's last-four strip does not fire first and
        # the mask is the only thing standing between this and the wire.
        ("Acct 1234WXYZ5678 IRA", "Acct ****5678 IRA"),
        # A letter prefix or suffix belongs to the identifier, not to the name,
        # so the mask swallows the whole token rather than leaving a stub.
        ("Brokerage X12345678", "Brokerage ****5678"),
        ("Checking 12345XY", "Checking ****2345"),
        # A whole *word* breaks the run — two separate four-digit tokens are not
        # one eight-digit number. This is the boundary, and it is the entire
        # rule: `Savings` is whitespace-delimited and alphabetic, `ABCD` above is
        # neither. A label that starts with digits survives for the same reason.
        ("Checking 1234 Savings 5678", "Checking 1234 Savings"),
        # Seven digits, and none of it an account number. Masking from the first
        # digit to the last would leave "****2024" — no name at all, which
        # defeats the field. The word between them is what keeps it whole.
        ("401K Plan 2024 Rewards", "401K Plan 2024 Rewards"),
        # A bare trailing four-digit group is the masked last-four banks print,
        # and parse_account_label lifts it out into `last_four` — so the label
        # arrives already stripped and there is nothing left for the mask to
        # find. Pinned because it is the boundary: one digit more and the mask
        # has to act.
        ("Checking 1789", "Checking"),
        ("Savings 2024", "Savings"),
    ],
)
def test_a_minted_accounts_display_name_masks_every_account_number_shape(
    db: Any, tmp_path: Path, label: str, expected: str
) -> None:
    """Grouped numbers are the shape that survives both stages.

    ``parse_account_label`` removes a recognized trailing four-digit token, so
    ``Checking 4111 1111 1111 1111`` arrives as ``Checking 4111 1111 1111`` —
    twelve digits of a card number, in a field declared safe to show. Masking
    only contiguous runs of five or more leaves that untouched, because each
    group is four.

    Exact equality on both directions: masking too little publishes a number,
    masking too much turns the mint report into ``****1098`` and defeats the
    field's purpose, which is to name what was created.
    """
    csv = tmp_path / "txns.csv"
    csv.write_text(
        f"Date,Description,Amount,Account\n2024-01-15,Coffee,-4.50,{label}\n"
    )

    result = import_answering_gate(
        ImportService(db), csv, refresh=False, confirm=True, auto_accept=True
    )

    created = result.accounts_created
    assert len(created) == 1, created
    assert created[0].display_name == expected


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
