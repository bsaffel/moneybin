# ruff: noqa: S101
"""The mint report names an account the way every other surface names it.

Guards issue #446: ``accounts_created[].display_name`` was a second, weaker
label built from the OFX ``<ORG>`` routing code plus the file's raw account-type
spelling, never written anywhere. It disagreed with
``core.dim_accounts.display_name`` — the name ``accounts`` returns — and, having
no per-account discriminator, two distinct accounts collided on one string.

Real refresh, real ``core.dim_accounts``: agreement between a Python-side
derivation and a SQL-side one can only be pinned by building both.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.import_service import ImportService

_FIXTURES = Path(__file__).parent.parent / "fixtures"
_KEY = "mint-report-names-key-0123456789abcdef"


def _build_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    store = MagicMock()
    store.get_key.return_value = _KEY
    db_path = tmp_path / "mint_report_names.duckdb"
    db = Database(db_path, secret_store=store, read_only=False)
    settings = MagicMock()
    settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: settings)
    return db


def _standard_csv(_tmp: Path) -> Path:
    return _FIXTURES / "tabular" / "standard.csv"


def _minimal_ofx(_tmp: Path) -> Path:
    return _FIXTURES / "ofx" / "sample_minimal.ofx"


def _card_statement_pdf(tmp_path: Path) -> Path:
    from tests.moneybin.pdf_statement_fixtures import write_card_statement_pdf

    return write_card_statement_pdf(tmp_path)


_CHANNELS: list[tuple[str, Callable[[Path], Path], dict[str, Any]]] = [
    ("tabular", _standard_csv, {"account_name": "WF Checking"}),
    ("ofx", _minimal_ofx, {}),
    ("pdf", _card_statement_pdf, {}),
]


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize(("channel", "make_file", "import_kwargs"), _CHANNELS)
def test_the_mint_report_names_the_account_the_way_dim_accounts_does(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    channel: str,
    make_file: Callable[[Path], Path],
    import_kwargs: dict[str, Any],
) -> None:
    """Every minted account is announced under the name it is stored under.

    The tool's own ``actions[]`` tells the agent to report ``accounts_created``
    to the user, so a label that no later surface can reproduce is worse than
    no label: it is the first thing a new user is told about an account, and
    ``accounts`` then answers under a different name.
    """
    db = _build_db(tmp_path, monkeypatch)
    result = ImportService(db).import_file(
        make_file(tmp_path),
        refresh=True,
        confirm=True,
        actor_kind="human",
        **import_kwargs,
    )
    assert result.accounts_created, f"{channel}: minted nothing to compare"
    stored = {
        str(row[0]): str(row[1])
        for row in db.execute(
            "SELECT account_id, display_name FROM core.dim_accounts"
        ).fetchall()
    }
    reported = {a.account_id: a.display_name for a in result.accounts_created}
    assert reported == {account_id: stored[account_id] for account_id in reported}, (
        f"{channel}: the mint report disagrees with core.dim_accounts"
    )


@pytest.mark.integration
@pytest.mark.slow
def test_two_accounts_of_one_type_at_one_institution_get_distinct_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The label must carry a per-account discriminator, not just org + type.

    The reported defect: two cards at one issuer each collapsed onto one string,
    so the label could not even distinguish the accounts it described.
    Institution and account type are shared by construction; the last four is
    what tells two siblings apart.
    """
    db = _build_db(tmp_path, monkeypatch)
    template = (_FIXTURES / "ofx" / "sample_minimal.ofx").read_text()
    paths: list[str | Path] = []
    for acctid in ("4242", "7777"):
        path = tmp_path / f"card_{acctid}.ofx"
        path.write_text(template.replace("<ACCTID>1111<", f"<ACCTID>{acctid}<"))
        paths.append(path)

    batch = ImportService(db).import_files(paths, refresh=True)
    assert batch.failed_count == 0
    labels = [
        account.display_name
        for entry in batch.per_file
        for account in entry.accounts_created
    ]
    assert len(labels) == 2, f"expected two mints, got {labels}"
    assert len(set(labels)) == 2, f"two distinct accounts share one label: {labels}"


def _import(db: Database, path: Path, **kwargs: Any) -> Any:
    """Import through the first-contact gate; these tests are about the name."""
    from tests.import_helpers import import_answering_gate

    return import_answering_gate(
        ImportService(db),
        path,
        refresh=True,
        confirm=True,
        actor_kind="human",
        **kwargs,
    )


def _stored(db: Database) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in db.execute(
            "SELECT account_id, display_name FROM core.dim_accounts"
        ).fetchall()
    }


@pytest.mark.integration
@pytest.mark.slow
def test_the_files_own_account_label_becomes_the_stored_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A human-authored account name outranks one assembled from bank fields.

    ``tiller.csv`` supplies all three at once — Account ``Personal Checking``,
    Institution ``Test Bank``, Account # ``9876`` — so it separates the two
    candidate rules. The label is the only name a person actually wrote, and
    ``moneybin accounts`` already prints institution and type in their own
    columns beside it, so naming the row ``Test Bank …9876`` spends the display
    on facts already on screen and discards the one that is not.

    The last four stays: it is the discriminator, not the name.
    """
    db = _build_db(tmp_path, monkeypatch)
    result = _import(db, _FIXTURES / "tabular" / "tiller.csv")

    [created] = result.accounts_created
    assert created.display_name == "Personal Checking …9876"
    assert _stored(db)[created.account_id] == "Personal Checking …9876"


@pytest.mark.integration
@pytest.mark.slow
def test_a_label_holding_only_an_account_number_is_not_used_as_the_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A number is an identifier, not a name, so the bank fields still win.

    An Account column mapped straight from the account-number field is ordinary
    in hand-rolled exports. Masking makes it safe to show but not meaningful:
    ``****1098`` names the account strictly worse than ``Test Bank …1098``, so
    the label arm stands down when the label carries no letter.
    """
    csv = tmp_path / "numeric_label.csv"
    csv.write_text(
        "Date,Description,Amount,Account,Institution\n"
        "2026-01-05,GROCERY STORE,-52.30,987654321098,Test Bank\n"
    )
    db = _build_db(tmp_path, monkeypatch)
    result = _import(db, csv)

    [created] = result.accounts_created
    assert "Test Bank" in created.display_name, created.display_name
    assert _stored(db)[created.account_id] == created.display_name


@pytest.mark.integration
@pytest.mark.slow
def test_a_blank_account_cell_is_not_named_by_the_placeholder_that_replaces_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row with no account still needs a key; the filler is not a name.

    A blank Account cell reaches the importer as NULL, and the multi-account
    branch substitutes the literal ``"unknown"`` so ``slugify`` still yields a
    key for those rows. That string reads exactly like a name a person wrote,
    which is the one thing the top rung is reserved for — so the account falls
    through to its bank fields, as it did before the rung existed.
    """
    csv = tmp_path / "one_blank_account.csv"
    csv.write_text(
        "Date,Description,Amount,Account,Institution\n"
        "2026-01-05,GROCERY STORE,-52.30,Personal Checking,Test Bank\n"
        "2026-01-06,ELECTRIC CO,-120.00,,Test Bank\n"
    )
    db = _build_db(tmp_path, monkeypatch)
    result = _import(db, csv)

    labels = {a.display_name for a in result.accounts_created}
    assert "unknown" not in labels, labels
    assert "Personal Checking" in labels, labels
    stored = _stored(db)
    assert {a.display_name for a in result.accounts_created} == {
        stored[a.account_id] for a in result.accounts_created
    }


@pytest.mark.integration
@pytest.mark.slow
def test_a_file_with_no_account_label_is_not_named_after_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only an authored label earns the top rung; a synthesized one does not.

    With no ``--account-name`` and no account column the importer still needs a
    key, and it builds a placeholder from the file stem. That string names the
    upload, not the account, and promoting it would let a rename of the file
    rename the account — so it must never reach ``display_name``.
    """
    csv = tmp_path / "january_download.csv"
    csv.write_text("Date,Description,Amount\n2026-01-05,GROCERY STORE,-52.30\n")
    db = _build_db(tmp_path, monkeypatch)
    result = _import(db, csv)

    [created] = result.accounts_created
    assert "january_download" not in created.display_name, created.display_name
    assert _stored(db)[created.account_id] == created.display_name
