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
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, routing_number, account_type, institution_org,
             institution_fid, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, '111000025', ?, ?, ?, '/tmp/v.ofx', 'ofx',
                'vocab_ofx', '2024-01-01'::TIMESTAMP, '2024-01-01'::TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key, account_type, institution_org, institution_fid],
    )


def _tabular_account(
    db: Database, *, native_key: str, account_type: str | None
) -> None:
    db.execute(
        """
        INSERT INTO raw.tabular_accounts
            (account_id, account_name, account_type, institution_name,
             source_file, source_type, source_origin, import_id,
             extracted_at, loaded_at)
        VALUES (?, 'Vocab Acct', ?, 'Vocab Bank', '/tmp/v.csv', 'csv',
                'vocab_tab', 'imp-v-001', '2024-01-01'::TIMESTAMP,
                '2024-01-01'::TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key, account_type],
    )


def _dim_type(db: Database, account_id: str) -> tuple[str | None, str | None]:
    row = db.execute(
        "SELECT account_type, account_subtype FROM core.dim_accounts WHERE account_id = ?",
        [account_id],
    ).fetchone()
    assert row is not None, f"no core.dim_accounts row for {account_id!r}"
    return row[0], row[1]


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
    db: Database, raw_value: str, expected_type: str, expected_subtype: str
) -> None:
    """OFX <ACCTTYPE> spellings collapse to the canonical set, keeping detail in subtype."""
    native = f"ofx-{raw_value.lower()}"
    canonical = f"canon-{raw_value.lower()}"[:15]
    _ofx_account(db, native_key=native, account_type=raw_value)
    _link(
        db,
        link_id=f"lnk-{raw_value.lower()}"[:12],
        account_id=canonical,
        ref_value=native,
        source_type="ofx",
        source_origin="vocab_ofx",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _dim_type(db, canonical) == (expected_type, expected_subtype)


@pytest.mark.slow
def test_tabular_free_text_account_type_normalizes(db: Database) -> None:
    """A CSV column mapping writes free text; it must land in the canonical set too."""
    _tabular_account(db, native_key="tab-cc", account_type="credit_card")
    _link(
        db,
        link_id="lnk-tab-cc",
        account_id="canon-tab-cc",
        ref_value="tab-cc",
        source_type="csv",
        source_origin="vocab_tab",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _dim_type(db, "canon-tab-cc") == ("credit", "credit card")


@pytest.mark.slow
def test_unmapped_account_type_is_null_not_guessed(db: Database) -> None:
    """An unrecognized spelling yields NULL type, preserving the original in subtype.

    NULL is the honest answer and it is also the useful one: the dim's merge
    skips NULLs, so a stronger source can still supply the type. Defaulting to
    'other' would out-rank that real value on recency.
    """
    _tabular_account(db, native_key="tab-weird", account_type="Christmas Club")
    _link(
        db,
        link_id="lnk-tab-wd",
        account_id="canon-tab-wd",
        ref_value="tab-weird",
        source_type="csv",
        source_origin="vocab_tab",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _dim_type(db, "canon-tab-wd") == (None, "christmas club")


@pytest.mark.slow
def test_typeless_accounts_stay_distinguishable_by_last_four(db: Database) -> None:
    """Two typeless accounts at one institution must not share a display_name.

    The COALESCE chain assumed account_type was always present: with it NULL,
    both the type+last4 branch and the type-only branch go NULL and the chain
    falls through to the bare institution name, so every card at one bank
    renders identically. last_four is what distinguishes them.
    """
    for native, canonical, link in (
        ("2001111111114387", "canon-typeless-a", "lnk-tl-a"),
        ("2001111111113431", "canon-typeless-b", "lnk-tl-b"),
    ):
        _ofx_account(db, native_key=native, account_type=None)
        _link(
            db,
            link_id=link,
            account_id=canonical,
            ref_value=native,
            source_type="ofx",
            source_origin="vocab_ofx",
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT account_id, display_name FROM core.dim_accounts "
        "WHERE account_id IN ('canon-typeless-a', 'canon-typeless-b') "
        "ORDER BY account_id"
    ).fetchall()
    names = [r[1] for r in rows]

    assert len(set(names)) == 2, (
        f"typeless accounts collided on display_name: {names!r}"
    )
    assert "4387" in names[0], names[0]
    assert "3431" in names[1], names[1]
    # And no double space where the absent type used to be interpolated.
    assert all("  " not in n for n in names), names


@pytest.mark.slow
def test_opaque_ofx_org_code_resolves_to_a_readable_institution_name(
    db: Database,
) -> None:
    """<ORG> is a routing code, not a name — resolve the display name by FID.

    Chase publishes <ORG>B1</ORG> (FID 10898) and Wells Fargo <ORG>WF</ORG>
    (3000). Aliasing <ORG> straight through showed users "B1" in a column
    documented as the human-readable institution name.
    """
    _ofx_account(
        db,
        native_key="ofx-b1-4387",
        account_type="CREDITCARD",
        institution_org="B1",
        institution_fid="10898",
    )
    _link(
        db,
        link_id="lnk-b1-1",
        account_id="canon-b1-4387",
        ref_value="ofx-b1-4387",
        source_type="ofx",
        source_origin="vocab_ofx",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT institution_name, display_name FROM core.dim_accounts WHERE account_id = ?",
        ["canon-b1-4387"],
    ).fetchone()
    assert row is not None
    assert row[0] == "Chase", f"expected the FID to resolve a name, got {row[0]!r}"
    assert row[1] == "Chase credit card …4387", row[1]


@pytest.mark.slow
def test_unknown_fid_falls_back_to_the_raw_org(db: Database) -> None:
    """An institution absent from the registry keeps its <ORG> — never blank."""
    _ofx_account(
        db,
        native_key="ofx-unknown-1",
        account_type="CHECKING",
        institution_org="SOME CREDIT UNION",
        institution_fid="99999",
    )
    _link(
        db,
        link_id="lnk-unk-1",
        account_id="canon-unknown-1",
        ref_value="ofx-unknown-1",
        source_type="ofx",
        source_origin="vocab_ofx",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT institution_name FROM core.dim_accounts WHERE account_id = ?",
        ["canon-unknown-1"],
    ).fetchone()
    assert row is not None
    assert row[0] == "SOME CREDIT UNION"


@pytest.mark.slow
def test_legacy_empty_string_account_type_normalizes_to_null(db: Database) -> None:
    """Rows imported before the extractor fix hold '', not NULL.

    The extractor now writes NULL for an absent <ACCTTYPE>, but raw rows
    already on disk keep the empty string ofxparse produced. Staging must
    treat those as absent too, or the subtype fallback (LOWER(account_type))
    just relocates the empty string into account_subtype.
    """
    _ofx_account(db, native_key="ofx-legacy-empty", account_type="")
    _link(
        db,
        link_id="lnk-legacy-1",
        account_id="canon-legacy-em",
        ref_value="ofx-legacy-empty",
        source_type="ofx",
        source_origin="vocab_ofx",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _dim_type(db, "canon-legacy-em") == (None, None)


@pytest.mark.slow
def test_unmapped_plaid_type_still_yields_a_type(db: Database) -> None:
    """An unrecognized Plaid type must not become NULL.

    core.fct_balances filters Plaid balances on `NOT a.account_type IS NULL`
    and signs liabilities from that same column. Resolving an unmapped alias to
    NULL would drop every balance for that account out of fct_balances — and so
    out of net worth — silently. Plaid's own vocabulary is the canonical one, so
    its raw value is a safe fallback; that is not true of the other sources.
    """
    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, institution_name, mask,
             official_name, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES ('plaid-novel', 'crypto_wallet', NULL, 'Novel Bank', '4242',
                'Novel', 'plaid://novel', 'plaid', 'novel_inst',
                '2024-01-01'::TIMESTAMP, '2024-01-01'::TIMESTAMP)
        """  # noqa: S608  # test fixture
    )
    _link(
        db,
        link_id="lnk-plaid-nv",
        account_id="canon-plaid-nv",
        ref_value="plaid-novel",
        source_type="plaid",
        source_origin="novel_inst",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    account_type, _ = _dim_type(db, "canon-plaid-nv")
    assert account_type is not None, (
        "an unmapped Plaid type resolved to NULL, which drops its balances "
        "from fct_balances and therefore from net worth"
    )


@pytest.mark.slow
def test_mapped_alias_without_a_finer_subtype_stays_null(db: Database) -> None:
    """A registry hit with no finer subtype must yield NULL, not the raw alias.

    `CREDIT` maps to account_type 'credit' with a blank account_subtype, so
    m.account_subtype is NULL and a bare COALESCE falls through to the raw text
    — producing account_subtype='credit'. Because the dim merges account_subtype
    by recency alone, a later generic import would then silently downgrade an
    existing 'credit card' to 'credit' and regress display_name.
    """
    _tabular_account(db, native_key="tab-generic", account_type="credit")
    _link(
        db,
        link_id="lnk-tab-gen",
        account_id="canon-tab-gen",
        ref_value="tab-generic",
        source_type="csv",
        source_origin="vocab_tab",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    assert _dim_type(db, "canon-tab-gen") == ("credit", None)


@pytest.mark.slow
def test_display_name_honors_a_user_subtype_override(db: Database) -> None:
    """display_name must render the same subtype the subtype column reports.

    The output column is COALESCE(s.account_subtype, w.account_subtype), but the
    display chain read only the pre-override merged value — so overriding the
    subtype without also setting a display_name made the two disagree.
    """
    _ofx_account(db, native_key="ofx-override-1", account_type="SAVINGS")
    _link(
        db,
        link_id="lnk-ovr-1",
        account_id="canon-override-1",
        ref_value="ofx-override-1",
        source_type="ofx",
        source_origin="vocab_ofx",
    )
    db.execute(
        """
        INSERT INTO app.account_settings (account_id, account_subtype, updated_at)
        VALUES ('canon-override-1', 'money market', CURRENT_TIMESTAMP)
        """  # noqa: S608  # test fixture
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT account_subtype, display_name FROM core.dim_accounts WHERE account_id = ?",
        ["canon-override-1"],
    ).fetchone()
    assert row is not None
    assert row[0] == "money market"
    assert "money market" in row[1], (
        f"display_name {row[1]!r} ignores the user's subtype override"
    )
