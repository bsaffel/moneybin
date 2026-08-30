"""Integration tests: account_type normalizes to one canonical vocabulary.

Every source used to write its own spelling into the same column — OFX wrote
``CHECKING``/``CREDITLINE``, Plaid wrote ``depository``/``credit``, the PDF
importer wrote ``credit``, and a CSV column mapping wrote whatever the file said.
Nothing normalized anywhere, so ``core.dim_accounts.account_type`` carried four
vocabularies at once. That broke ``accounts --type credit`` (exact-match, so it
silently omitted OFX cards), split the by-type histogram into synonym buckets,
and let ``display_name`` flip spelling on every re-sync because the merge picks
by recency across sources.

The canonical set is the Plaid-style one — it is what the only value-branching
consumer (``core.fct_balances``) already keys on, and what ``account_subtype`` is
already documented in. The finer source distinction is preserved in
``account_subtype`` rather than discarded, so normalizing loses no information.

Seeding mirrors test_dim_accounts_merge.py: INSERT into raw.* + app.account_links,
materialize via sqlmesh, assert the projected dim columns.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _link(
    db: Database,
    *,
    link_id: str,
    account_id: str,
    ref_value: str,
    source_type: str,
    source_origin: str,
) -> None:
    db.execute(
        """
        INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type,
             source_origin, status, decided_by, decided_at)
        VALUES (?, ?, 'source_native', ?, ?, ?, 'accepted', 'auto', CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [link_id, account_id, ref_value, source_type, source_origin],
    )


def _ofx_account(
    db: Database,
    *,
    native_key: str,
    account_type: str | None,
    institution_org: str = "Vocab Bank",
    institution_fid: str = "fid-v",
    routing_number: str | None = "111000025",
    source_origin: str,
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, routing_number, account_type, institution_org,
             institution_fid, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, ?, ?, ?, ?, '/tmp/v.ofx', 'ofx',
                ?, '2024-01-01'::TIMESTAMP, '2024-01-01'::TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            native_key,
            routing_number,
            account_type,
            institution_org,
            institution_fid,
            source_origin,
        ],
    )


def _tabular_account(
    db: Database,
    *,
    native_key: str,
    account_type: str | None,
    source_origin: str,
) -> None:
    db.execute(
        """
        INSERT INTO raw.tabular_accounts
            (account_id, account_name, account_type, institution_name,
             source_file, source_type, source_origin, import_id,
             extracted_at, loaded_at)
        VALUES (?, 'Vocab Acct', ?, 'Vocab Bank', '/tmp/v.csv', 'csv',
                ?, 'imp-v-001', '2024-01-01'::TIMESTAMP,
                '2024-01-01'::TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key, account_type, source_origin],
    )


def _dim_type(db: Database, account_id: str) -> tuple[str | None, str | None]:
    row = db.execute(
        "SELECT account_type, account_subtype FROM core.dim_accounts WHERE account_id = ?",
        [account_id],
    ).fetchone()
    assert row is not None, f"no core.dim_accounts row for {account_id!r}"
    return row[0], row[1]


def _case_id(name: str) -> str:
    return f"mb21_account_type_{name}"


def _case_origin(name: str) -> str:
    return f"mb21_account_type_{name}_origin"


def _plaid_account(
    db: Database,
    *,
    native_key: str,
    account_type: str | None,
    account_subtype: str | None = None,
    institution_name: str | None = "Vocab Bank",
    mask: str = "4242",
    official_name: str | None = "Vocab Account",
    source_origin: str,
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, institution_name, mask,
             official_name, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, 'plaid://mb21-account-type', 'plaid', ?,
                '2024-01-01'::TIMESTAMP, '2024-01-01'::TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            native_key,
            account_type,
            account_subtype,
            institution_name,
            mask,
            official_name,
            source_origin,
        ],
    )


@pytest.fixture(scope="module")
def account_type_cases_template(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> Path:
    """One materialization over independently namespaced account-type cases."""
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path_factory.mktemp("account_type_normalization") / "test.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    request.addfinalizer(db.close)

    for raw_value in (
        "CHECKING",
        "SAVINGS",
        "MONEYMRKT",
        "CD",
        "CREDITLINE",
        "CREDITCARD",
    ):
        name = f"ofx_{raw_value.lower()}"
        native = _case_id(f"{name}_native")
        origin = _case_origin(name)
        _ofx_account(
            db, native_key=native, account_type=raw_value, source_origin=origin
        )
        _link(
            db,
            link_id=_case_id(f"{name}_link"),
            account_id=_case_id(name),
            ref_value=native,
            source_type="ofx",
            source_origin=origin,
        )

    for name, account_type in (
        ("tabular_credit_card", "credit_card"),
        ("tabular_unmapped", "Christmas Club"),
        ("tabular_generic_credit", "credit"),
    ):
        native = _case_id(f"{name}_native")
        origin = _case_origin(name)
        _tabular_account(
            db, native_key=native, account_type=account_type, source_origin=origin
        )
        _link(
            db,
            link_id=_case_id(f"{name}_link"),
            account_id=_case_id(name),
            ref_value=native,
            source_type="csv",
            source_origin=origin,
        )

    for name, suffix in (("typeless_a", "4242"), ("typeless_b", "7080")):
        # ``dim_accounts`` derives last_four only from a numeric native account id.
        # Keep that real input shape while the source origin and canonical id isolate
        # this template case from every other row.
        native = f"200111111111{suffix}"
        origin = _case_origin(name)
        _ofx_account(db, native_key=native, account_type=None, source_origin=origin)
        _link(
            db,
            link_id=_case_id(f"{name}_link"),
            account_id=_case_id(name),
            ref_value=native,
            source_type="ofx",
            source_origin=origin,
        )

    for name, account_type, institution_org, institution_fid, routing_number in (
        ("opaque_org", "CREDITCARD", "B1", "10898", "111000025"),
        ("unknown_fid", "CHECKING", "SOME CREDIT UNION", "99999", "111000025"),
        ("legacy_empty_type", "", "Vocab Bank", "fid-v", "111000025"),
        ("legacy_empty_routing", "CREDITCARD", "Vocab Bank", "fid-v", ""),
        ("subtype_override", "SAVINGS", "Vocab Bank", "fid-v", "111000025"),
    ):
        # The Chase display assertion needs the provider's numeric suffix in its
        # native id; the source origin and canonical id still namespace this case.
        native = "ofx-b1-4242" if name == "opaque_org" else _case_id(f"{name}_native")
        origin = _case_origin(name)
        _ofx_account(
            db,
            native_key=native,
            account_type=account_type,
            institution_org=institution_org,
            institution_fid=institution_fid,
            routing_number=routing_number,
            source_origin=origin,
        )
        _link(
            db,
            link_id=_case_id(f"{name}_link"),
            account_id=_case_id(name),
            ref_value=native,
            source_type="ofx",
            source_origin=origin,
        )

    db.execute(
        """
        INSERT INTO app.account_settings (account_id, account_subtype, updated_at)
        VALUES (?, 'money market', CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [_case_id("subtype_override")],
    )

    for name, account_type, account_subtype, institution_name, official_name in (
        ("plaid_unmapped", "crypto_wallet", None, "Novel Bank", "Novel"),
        ("plaid_null", None, None, "Untyped Bank", "Untyped"),
        ("plaid_blank_empty", "credit", "", "", ""),
        ("plaid_blank_spaces", "credit", "   ", "   ", "   "),
    ):
        native = _case_id(f"{name}_native")
        origin = _case_origin(name)
        _plaid_account(
            db,
            native_key=native,
            account_type=account_type,
            account_subtype=account_subtype,
            institution_name=institution_name,
            official_name=official_name,
            source_origin=origin,
        )
        _link(
            db,
            link_id=_case_id(f"{name}_link"),
            account_id=_case_id(name),
            ref_value=native,
            source_type="plaid",
            source_origin=origin,
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    db.close()
    return db.path


@pytest.fixture()
def account_type_cases(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    account_type_cases_template: Path,
) -> Database:
    """An isolated planned snapshot for one account-type assertion case."""
    path = tmp_path / "test.duckdb"
    shutil.copy(account_type_cases_template, path)
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        path,
        secret_store=secret_store,
        no_auto_upgrade=True,
        assume_initialized=True,
        read_only=False,
    )
    request.addfinalizer(db.close)
    return db


@pytest.mark.slow
@pytest.mark.parametrize(
    ("raw_value", "expected_type", "expected_subtype"),
    [
        ("CHECKING", "depository", "checking"),
        ("SAVINGS", "depository", "savings"),
        ("MONEYMRKT", "depository", "money market"),
        ("CD", "depository", "cd"),
        # The OFX spelling for a line of credit, and the one the synthetic
        # writer emits — neither of which `accounts --type credit` matched.
        ("CREDITLINE", "credit", "line of credit"),
        ("CREDITCARD", "credit", "credit card"),
    ],
)
def test_ofx_account_type_normalizes_to_canonical_vocabulary(
    account_type_cases: Database,
    raw_value: str,
    expected_type: str,
    expected_subtype: str,
) -> None:
    """OFX <ACCTTYPE> spellings collapse to the canonical set, keeping detail in subtype."""
    assert _dim_type(account_type_cases, _case_id(f"ofx_{raw_value.lower()}")) == (
        expected_type,
        expected_subtype,
    )


@pytest.mark.slow
def test_tabular_free_text_account_type_normalizes(
    account_type_cases: Database,
) -> None:
    """A CSV column mapping writes free text; it must land in the canonical set too."""
    assert _dim_type(account_type_cases, _case_id("tabular_credit_card")) == (
        "credit",
        "credit card",
    )


@pytest.mark.slow
def test_unmapped_account_type_is_null_not_guessed(
    account_type_cases: Database,
) -> None:
    """An unrecognized spelling yields NULL type, preserving the original in subtype.

    NULL is the honest answer and it is also the useful one: the dim's merge
    skips NULLs, so a stronger source can still supply the type. Defaulting to
    'other' would out-rank that real value on recency.
    """
    assert _dim_type(account_type_cases, _case_id("tabular_unmapped")) == (
        None,
        "christmas club",
    )


@pytest.mark.slow
def test_typeless_accounts_stay_distinguishable_by_last_four(
    account_type_cases: Database,
) -> None:
    """Two typeless accounts at one institution must not share a display_name.

    The COALESCE chain assumed account_type was always present: with it NULL,
    both the type+last4 branch and the type-only branch go NULL and the chain
    falls through to the bare institution name, so every card at one bank
    renders identically. last_four is what distinguishes them.
    """
    rows = account_type_cases.execute(
        "SELECT account_id, display_name FROM core.dim_accounts "
        "WHERE account_id IN (?, ?) "
        "ORDER BY account_id",
        [_case_id("typeless_a"), _case_id("typeless_b")],
    ).fetchall()
    names = [r[1] for r in rows]

    assert len(set(names)) == 2, (
        f"typeless accounts collided on display_name: {names!r}"
    )
    assert "4242" in names[0], names[0]
    assert "7080" in names[1], names[1]
    # And no double space where the absent type used to be interpolated.
    assert all("  " not in n for n in names), names


@pytest.mark.slow
def test_opaque_ofx_org_code_resolves_to_a_readable_institution_name(
    account_type_cases: Database,
) -> None:
    """<ORG> is a routing code, not a name — resolve the display name by FID.

    Chase publishes <ORG>B1</ORG> (FID 10898) and Wells Fargo <ORG>WF</ORG>
    (3000). Aliasing <ORG> straight through showed users "B1" in a column
    documented as the human-readable institution name.
    """
    row = account_type_cases.execute(
        "SELECT institution_name, display_name FROM core.dim_accounts WHERE account_id = ?",
        [_case_id("opaque_org")],
    ).fetchone()
    assert row is not None
    assert row[0] == "Chase", f"expected the FID to resolve a name, got {row[0]!r}"
    assert row[1] == "Chase credit card …4242", row[1]


@pytest.mark.slow
def test_unknown_fid_falls_back_to_the_raw_org(account_type_cases: Database) -> None:
    """An institution absent from the registry keeps its <ORG> — never blank."""
    row = account_type_cases.execute(
        "SELECT institution_name FROM core.dim_accounts WHERE account_id = ?",
        [_case_id("unknown_fid")],
    ).fetchone()
    assert row is not None
    assert row[0] == "SOME CREDIT UNION"


@pytest.mark.slow
def test_legacy_empty_string_account_type_normalizes_to_null(
    account_type_cases: Database,
) -> None:
    """Rows imported before the extractor fix hold '', not NULL.

    The extractor now writes NULL for an absent <ACCTTYPE>, but raw rows
    already on disk keep the empty string ofxparse produced. Staging must
    treat those as absent too, or the subtype fallback (LOWER(account_type))
    just relocates the empty string into account_subtype.
    """
    assert _dim_type(account_type_cases, _case_id("legacy_empty_type")) == (None, None)


@pytest.mark.slow
def test_unmapped_plaid_type_resolves_to_null_not_a_default(
    account_type_cases: Database,
) -> None:
    """An unrecognized Plaid type must resolve to NULL, like every other source.

    A non-NULL default looks safer than NULL here and is not. core.fct_balances
    drops Plaid balances it cannot sign; any placeholder value falls through to
    the positive ELSE branch, booking a possible liability as an asset and
    overstating net worth by twice the balance. Dropping understates instead.
    That tradeoff is deliberate and already guarded by
    test_plaid_null_account_type_dropped — this test pins the staging half so a
    future "helpful" fallback cannot reintroduce it from above.
    """
    account_type, account_subtype = _dim_type(
        account_type_cases, _case_id("plaid_unmapped")
    )
    assert account_type is None, (
        f"expected NULL, got {account_type!r} — a non-NULL default lets "
        "fct_balances sign an unsignable balance as an asset"
    )
    assert account_subtype == "crypto_wallet", (
        "the unmapped source spelling must survive in account_subtype so the "
        "information needed to extend the registry is not lost"
    )


@pytest.mark.slow
def test_mapped_alias_without_a_finer_subtype_stays_null(
    account_type_cases: Database,
) -> None:
    """A registry hit with no finer subtype must yield NULL, not the raw alias.

    `CREDIT` maps to account_type 'credit' with a blank account_subtype, so
    m.account_subtype is NULL and a bare COALESCE falls through to the raw text
    — producing account_subtype='credit'. Because the dim merges account_subtype
    by recency alone, a later generic import would then silently downgrade an
    existing 'credit card' to 'credit' and regress display_name.
    """
    assert _dim_type(account_type_cases, _case_id("tabular_generic_credit")) == (
        "credit",
        None,
    )


@pytest.mark.slow
def test_display_name_honors_a_user_subtype_override(
    account_type_cases: Database,
) -> None:
    """display_name must render the same subtype the subtype column reports.

    The output column is COALESCE(s.account_subtype, w.account_subtype), but the
    display chain read only the pre-override merged value — so overriding the
    subtype without also setting a display_name made the two disagree.
    """
    row = account_type_cases.execute(
        "SELECT account_subtype, display_name FROM core.dim_accounts WHERE account_id = ?",
        [_case_id("subtype_override")],
    ).fetchone()
    assert row is not None
    assert row[0] == "money market"
    assert "money market" in row[1], (
        f"display_name {row[1]!r} ignores the user's subtype override"
    )


@pytest.mark.slow
def test_genuinely_null_plaid_type_stays_null(account_type_cases: Database) -> None:
    """A NULL Plaid account_type is distinct from an unmapped one — both stay NULL.

    `SyncAccount.account_type` is `str | None`, so NULL is reachable rather than
    hypothetical. Defaulting it to any placeholder would let the balance pass
    fct_balances' `NOT account_type IS NULL` guard and book un-negated as an
    asset. Pairs with test_unmapped_plaid_type_resolves_to_null_not_a_default:
    the two inputs differ, the required outcome does not.
    """
    account_type, _ = _dim_type(account_type_cases, _case_id("plaid_null"))
    assert account_type is None, (
        f"expected NULL, got {account_type!r} — fct_balances would then sign an "
        "unsignable balance as a positive asset"
    )


@pytest.mark.slow
def test_legacy_empty_string_routing_number_normalizes_to_null(
    account_type_cases: Database,
) -> None:
    """routing_number has the same legacy-'' failure mode as account_type.

    A credit-card statement's <CCACCTFROM> never carries <BANKID>, so pre-fix
    rows hold ''. The extractor now writes NULL, but raw.ofx_accounts keys on
    extracted_at, so a re-import ADDS a row rather than replacing the stale one
    — and dim_accounts merges routing_number with
    `FILTER(WHERE NOT routing_number IS NULL)`, which excludes the fresh correct
    NULL and lets the stale '' win permanently. Verified live: both real Chase
    cards carried routing_number='' in core.dim_accounts before this guard.
    """
    row = account_type_cases.execute(
        "SELECT routing_number FROM core.dim_accounts WHERE account_id = ?",
        [_case_id("legacy_empty_routing")],
    ).fetchone()
    assert row is not None
    assert row[0] is None, f"expected NULL, got {row[0]!r} — the '' leak persists"


@pytest.mark.slow
@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_plaid_text_fields_normalize_to_null(
    account_type_cases: Database, blank: str
) -> None:
    """Blank Plaid text columns must reach core as NULL, not ''.

    `COALESCE` only replaces NULL, so a '' subtype short-circuits the registry
    fallback; a '' institution_name passes the merge's NOT-NULL filter and lands
    in display_name's concat, yielding a malformed leading-space label. Same
    empty-string-vs-NULL class this PR fixes for OFX, on the Plaid side.
    """
    name = "plaid_blank_empty" if blank == "" else "plaid_blank_spaces"
    row = account_type_cases.execute(
        "SELECT account_subtype, institution_name, official_name, display_name "
        "FROM core.dim_accounts WHERE account_id = ?",
        [_case_id(name)],
    ).fetchone()
    assert row is not None
    assert row[0] is None, f"blank subtype survived as {row[0]!r}"
    assert row[1] is None, f"blank institution_name survived as {row[1]!r}"
    assert row[2] is None, f"blank official_name survived as {row[2]!r}"
    assert "  " not in row[3] and not row[3].startswith(" "), (
        f"display_name is malformed: {row[3]!r}"
    )
