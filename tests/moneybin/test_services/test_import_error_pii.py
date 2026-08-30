"""Raw third-party exception text must not reach the wire."""

import time
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
    ("hostile", "shape"),
    [
        (("1" + " " * 64) * 4 + "x", "whitespace-run-alternation"),
        ("A" * 30_000 + "1x2x3x4xZ", "long-alphabetic-prefix"),
    ],
    ids=["whitespace-run-alternation", "long-alphabetic-prefix"],
)
def test_masking_a_hostile_account_label_terminates(
    db: Any, tmp_path: Path, hostile: str, shape: str
) -> None:
    """The mask runs on untrusted file content, so it must not be exponential.

    ``_created_account`` masks ``src.account_name``, which on tabular is a cell
    the file supplied — so a label is attacker-controlled in the same sense any
    imported field is.

    Two distinct blow-ups, found a round apart, which is why this is a grid:

    - *whitespace-run-alternation* — the gap's second branch could place its
      whitespace atom at any of N positions in a run of N spaces, so a label
      that ultimately failed to match cost 2^N. 165 characters took 0.53s.
    - *long-alphabetic-prefix* — the leading ``[A-Za-z]*`` was retried from
      every character of a long letter run, rescanning it each time: quadratic,
      1.2s at 20k characters. Fixed by a lookbehind that lets a match begin only
      at a token boundary, so the retry costs O(1) instead of O(n).

    Asserted as a wall-clock bound rather than a pattern property because that
    is the failure a user experiences — a hung import.
    """
    from moneybin.services.import_service import mask_embedded_account_number

    start = time.perf_counter()
    mask_embedded_account_number(hostile)
    elapsed = time.perf_counter() - start

    assert elapsed < 1.0, f"masking took {elapsed:.2f}s — the gap is ambiguous again"


# A key the caller typed that is shaped like an account number — a mistyped
# <ACCTID>, or one pasted from the wrong statement. One digit off `ACCTID`, so
# it is genuinely unknown to the file while still being the sensitive shape.
MISTYPED_ACCTID = "987654321099"


def _refusal_message(exc: Exception) -> str:
    """What the caller actually receives for this failure.

    Through ``per_file_failure``, because that is the CLI's and MCP's shared
    verdict path — and a bare ``ValueError`` is *classified*, so its message
    survives verbatim rather than being scrubbed to the class name.
    """
    message, _code, _hint, _details = per_file_failure(exc)
    return message


def test_an_unknown_binding_key_is_not_echoed_in_full(db: Any, tmp_path: Path) -> None:
    """A refusal must not persist the account number the caller mistyped.

    Echoing the caller's own key back reads as safe — they already have it — but
    the CLI writes this message through ``logger.error``, and a log file is
    exactly the "artifact that outlives the session" `.claude/rules/security.md`
    names as a boundary. The sanitizer masks recognized shapes, not every
    issuer's numbering, so the honest fix is not to put it there.

    Masked rather than dropped: the caller still has to learn *which* of their
    keys was wrong, and on a file bound by source key the ref list alone cannot
    tell them.
    """
    ofx = _ofx_with_acctid(tmp_path)

    with pytest.raises(ValueError) as raised:
        ImportService(db).import_file(
            ofx, refresh=False, account_bindings={MISTYPED_ACCTID: "new"}
        )

    message = _refusal_message(raised.value)
    assert MISTYPED_ACCTID not in message, f"caller key reached the wire: {message}"
    assert MISTYPED_ACCTID not in str(raised.value)
    # Still answerable: the masked tail identifies which key, the refs say what
    # to send instead.
    assert "****1099" in message, message
    assert "@0" in message, message


def test_an_unknown_binding_key_is_not_echoed_in_full_on_pdf(
    db: Any, tmp_path: Path
) -> None:
    """The third channel, which this file otherwise never exercises.

    ``_mask_caller_keys`` is channel-agnostic, so this is coverage of the claim
    rather than of a suspected second bug — but this file's whole purpose is
    proving account identifiers never reach error text, and PDF is one of the
    three channels this PR newly gates. A file that proves it for two of three
    reads as proving it for all three.
    """
    from tests.moneybin.pdf_statement_fixtures import write_card_statement_pdf

    pdf = write_card_statement_pdf(tmp_path)

    with pytest.raises(ValueError) as raised:
        ImportService(db).import_file(
            pdf, refresh=False, confirm=True, account_bindings={MISTYPED_ACCTID: "new"}
        )

    message = _refusal_message(raised.value)
    assert MISTYPED_ACCTID not in message, f"caller key reached the wire: {message}"
    assert "****1099" in message, message


def test_an_unknown_metadata_key_is_not_echoed_in_full(db: Any, tmp_path: Path) -> None:
    """The ``account_metadata`` twin of the binding refusal above.

    Same class, same sink, different parameter — and the reason to fix it in the
    same change is that a refusal fixed on one parameter and not the other is
    the shape of gap this PR has already shipped twice.
    """
    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2024-01-15,Coffee,-4.50\n")

    with pytest.raises(ValueError) as raised:
        ImportService(db).import_file(
            csv,
            refresh=False,
            confirm=True,
            account_bindings={"@0": "new"},
            account_metadata={MISTYPED_ACCTID: {"display_name": "X"}},
        )

    message = _refusal_message(raised.value)
    assert MISTYPED_ACCTID not in message, f"caller key reached the wire: {message}"
    assert "****1099" in message, message


def test_a_duplicated_metadata_referent_is_not_echoed_in_full(
    db: Any, tmp_path: Path
) -> None:
    """The same-account-twice refusal names a key the caller sent, too.

    Reachable with an account column that *is* an account number, which is a
    real tabular export shape — so this refusal can quote one even though the
    caller supplied it.
    """
    csv = tmp_path / "txns.csv"
    csv.write_text(
        f"Date,Description,Amount,Account\n2024-01-15,Coffee,-4.50,{ACCTID}\n"
    )
    svc = ImportService(db)

    with pytest.raises(ValueError) as raised:
        svc.import_file(
            csv,
            refresh=False,
            confirm=True,
            account_bindings={"@0": "new"},
            account_metadata={
                "@0": {"display_name": "A"},
                ACCTID: {"display_name": "B"},
            },
        )
    # The refusal has to be the duplicate-referent one, not unknown-key: if the
    # column no longer slugified to the number itself this would silently become
    # a different test.
    assert "same account twice" in str(raised.value), str(raised.value)

    message = _refusal_message(raised.value)
    assert ACCTID not in message, f"account number reached the wire: {message}"


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        # Contiguous — the shape parse_account_label leaves entirely alone, so
        # the mask collapses the whole run and the parser lifts the surviving
        # four into last_four, where the ladder re-attaches them.
        (f"Checking {ACCTID}", "Checking …1098"),
        # Grouped, and the reason masking now runs BEFORE the last-four strip.
        # Stripping first left the number's *prefix* in the label, so the mask
        # kept that prefix's tail: "1234-5678-9012" lost 9012, and the label
        # then announced ****5678 — four digits from the middle of the number,
        # in the slot every surface fills with the last four. Wrong digits, and
        # a second slice of the number beside the real last four the account
        # already shows. Masking first collapses the run to its true tail.
        ("Checking 1234-5678-9012", "Checking …9012"),
        ("Checking 4111 1111 1111 1111", "Checking …1111"),
        # Every separator, not an enumerated few: the label parser and the mask
        # have to agree on what a separator is, and each time they did not, the
        # gap was a run of account digits. Ordering matters here too — the
        # slash and underscore cases used to leave the separator itself dangling
        # after the masked prefix ("****5678/"), because the strip had already
        # taken the group that followed it.
        ("Checking 1234.5678.9012", "Checking …9012"),
        ("Checking 1234/5678/9012", "Checking …9012"),
        ("Checking 1234_5678_9012", "Checking …9012"),
        # A three-character separator, which the parser's own trailing-token
        # strip accepts. Masking first means the strip never sees the raw run.
        ("Checking 1234 - 5678 - 9012", "Checking …9012"),
        # Letters *inside* the identifier, the shape brokerage and investment
        # accounts actually use. The run is bounded by separator length, not by
        # character class, so letters no longer end it.
        ("Brokerage 12AB34CD56", "Brokerage …3456"),
        ("IRA 12X3456789", "IRA …6789"),
        # Letter groups of any length. Bounding the gap at three characters was
        # the third enumeration to leak: `ABCD` is not a word, it is the middle
        # of an identifier, and no digit-count-per-group rule can tell them
        # apart. The rule is about words, so the gap length is unbounded.
        ("Brokerage 12ABCD34EFGH56", "Brokerage …3456"),
        # Trailing text, so the parser's last-four strip does not fire first and
        # the mask is the only thing standing between this and the wire.
        ("Acct 1234WXYZ5678 IRA", "Acct ****5678 IRA"),
        # A letter prefix or suffix belongs to the identifier, not to the name,
        # so the mask swallows the whole token rather than leaving a stub.
        ("Brokerage X12345678", "Brokerage …5678"),
        ("Checking 12345XY", "Checking …2345"),
        # A whole *word* breaks the run — two separate four-digit tokens are not
        # one eight-digit number. This is the boundary, and it is the entire
        # rule: `Savings` is whitespace-delimited and alphabetic, `ABCD` above is
        # neither. A label that starts with digits survives for the same reason.
        ("Checking 1234 Savings 5678", "Checking 1234 Savings"),
        # Seven digits, and none of it an account number. Masking from the first
        # digit to the last would leave "****2024" — no name at all, which
        # defeats the field. The word between them is what keeps it whole.
        ("Retirement Plan 2024 Rewards", "Retirement Plan 2024 Rewards"),
        # The same identifier, spaced and unspaced. A formatted one puts a
        # *word* between its digit groups -- a bank code is letters -- so the
        # run ends there and `CC00 BANK` survives as the residue. That residue
        # carries letters, which is exactly the test dim_accounts uses to decide
        # a label is a name, so it reached the wire as the account's name with
        # the last four appended. Masking cannot swallow the word without
        # swallowing `Checking 1234 Savings 5678` above; what it can do is
        # refuse the remainder once it has fired, and a digit surviving outside
        # the mask is what says the remainder is still the identifier. The
        # unspaced form already rendered this way -- pinned beside it because
        # the two spellings of one number must not disclose to different depths.
        ("CC00 BANK 1234 5678 9012", "…9012"),
        ("CC00BANK123456789012", "…9012"),
        # A bare trailing four-digit group is the masked last-four banks print,
        # and parse_account_label lifts it out into `last_four` — so the label
        # arrives already stripped and there is nothing left for the mask to
        # find. Pinned because it is the boundary: one digit more and the mask
        # has to act. The ladder then puts that same last four back in its
        # canonical position, which is a round trip through the field the
        # parser moved it to, not a second slice of the number.
        ("Checking 7777", "Checking …7777"),
        ("Savings 2024", "Savings …2024"),
    ],
)
def test_a_minted_accounts_display_name_masks_every_account_number_shape(
    db: Any, tmp_path: Path, label: str, expected: str
) -> None:
    """Grouped numbers are the shape that survives both stages.

    ``mask_embedded_account_number`` runs first and ``parse_account_label``
    second, which is what makes the surviving four digits the account's own.
    The other order strips a recognized trailing token before masking, leaving
    the number's prefix behind for the mask to take *its* tail from — so
    ``Checking 1234-5678-9012`` announced ``****5678``, the middle of the
    number, where the last four belongs.

    Exact equality on both directions: masking too little publishes a number,
    masking too much turns the mint report into ``…1098`` and defeats the
    field's purpose, which is to name what was created. And the four digits
    that do survive must be the last four, because that is the only slice the
    account already shows everywhere else — any other four is a second,
    independent disclosure of the same number.
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
    which strips only a *recognized masked* last-four — ``(...7777)``, ``x7777``,
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


def test_the_pinned_key_ambiguity_refusal_does_not_echo_the_pinned_account_id(
    db: Any, tmp_path: Path
) -> None:
    """``--account-id`` is caller input, and caller input is not automatically safe.

    The value arrives verbatim from the command line, and ``account_id`` is not
    always a minted surrogate: ``stg_tabular__transactions`` falls back to the
    source-native key when no link resolves, so the id a caller reads back out
    of a report and re-pins can be the institution's own ``<ACCTID>``.
    ``.claude/rules/identifiers.md`` decides masking per field, once, on the
    worst case the field can hold — never per value.
    """
    from moneybin.repositories.account_links_repo import AccountLinksRepo
    from moneybin.services.account_resolver import AccountResolver

    repo = AccountLinksRepo(db)
    for link_id, ref_value in (("lnk_a", "statement-aaa"), ("lnk_b", "statement-bbb")):
        repo.insert(
            link_id=link_id,
            account_id=MISTYPED_ACCTID,
            ref_kind="source_native",
            ref_value=ref_value,
            source_type="csv",
            source_origin="monarch",
            decided_by="user",
            actor="cli",
        )
    export = tmp_path / "export.csv"
    export.write_text("Date,Description,Amount\n2026-01-05,COFFEE,-4.75\n")

    with pytest.raises(ValueError) as raised:
        ImportService(db)._pinned_native_key(  # pyright: ignore[reportPrivateUsage]  # the refusal under test has no other entry point
            resolver=AccountResolver(db, actor="test"),
            account_id=MISTYPED_ACCTID,
            file_path=export,
            source_bytes=None,
            source_type="csv",
            source_origin="monarch",
        )

    message = _refusal_message(raised.value)
    assert MISTYPED_ACCTID not in message, f"pinned id reached the wire: {message}"
    # Still answerable: the masked tail says which pin, the flag says what to send.
    assert "****1099" in message, message
    assert "--account-name" in message, message


def test_the_contradicted_binding_refusal_masks_both_account_ids(
    db: Any, tmp_path: Path
) -> None:
    """The other half of the pin refusal pair, decided per field on the worst case.

    ``_refuse_contradicted_bindings`` names two ids — the one the caller pinned
    and the one the file's key is already accepted onto — and neither is
    guaranteed to be a minted surrogate, for the reason the ambiguity refusal
    above documents. The remembered owner is worse than caller input: the caller
    never typed it, so echoing it verbatim discloses an id they had not seen.
    """
    from moneybin.repositories.account_links_repo import AccountLinksRepo
    from moneybin.services import import_service
    from moneybin.services.account_resolution_types import SourceAccount
    from moneybin.services.account_resolver import AccountResolver

    owner_acctid = "123456789012"
    AccountLinksRepo(db).insert(
        link_id="lnk_owner",
        account_id=owner_acctid,
        ref_kind="source_native",
        ref_value="statement-aaa",
        source_type="csv",
        source_origin="monarch",
        decided_by="user",
        actor="cli",
    )
    pinned = SourceAccount(
        source_type="csv",
        source_origin="monarch",
        source_account_key="statement-aaa",
        account_name="Checking",
        explicit_account_id=MISTYPED_ACCTID,
    )

    with pytest.raises(ValueError) as raised:
        import_service._refuse_contradicted_bindings(  # pyright: ignore[reportPrivateUsage]  # the refusal under test has no other entry point
            AccountResolver(db, actor="test"), [pinned], [None]
        )

    message = _refusal_message(raised.value)
    assert MISTYPED_ACCTID not in message, f"pinned id reached the wire: {message}"
    assert owner_acctid not in message, f"remembered owner reached the wire: {message}"
    # Still answerable: each masked tail says which account the refusal means.
    assert "****1099" in message, message
    assert "****9012" in message, message
