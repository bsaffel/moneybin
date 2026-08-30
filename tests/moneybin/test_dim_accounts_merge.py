"""Integration tests: core.dim_accounts canonical-id grain + COALESCE merge (B5).

Decision 4: the dim is keyed on COALESCE(account_id, source_account_key) and
merges each field across the grain group instead of last-write-wins.

- No-null-clobber: a stronger source's structured field survives a later
  weaker-source NULL (old last-write-wins would null it).
- Collapse: ofx + csv rows sharing one canonical id produce exactly one row.
- Unlinked safety net: a row whose canonical account_id is still NULL (no
  accepted link yet) stays DISTINCT under its source-native key rather than
  collapsing every NULL account into one bad row.

Seeding mirrors test_stg_account_links_join.py: INSERT directly into raw.* +
app.account_links (bypassing AccountLinksRepo / audit-log pairing), then
materialize via sqlmesh and assert the projected dim columns.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.services.account_resolution_types import UNNAMED_ACCOUNT_LABEL

pytestmark = pytest.mark.integration


def _insert_accepted_source_native(
    db: Database,
    *,
    link_id: str,
    account_id: str,
    ref_value: str,
    source_type: str,
    source_origin: str,
) -> None:
    """Seed one accepted source_native row in app.account_links."""
    db.execute(
        """
        INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type,
             source_origin, status, decided_by, decided_at)
        VALUES (?, ?, 'source_native', ?, ?, ?, 'accepted', 'auto', CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [link_id, account_id, ref_value, source_type, source_origin],
    )


def _insert_ofx_account(
    db: Database,
    *,
    native_key: str,
    routing_number: str | None,
    institution_org: str,
    account_type: str,
    extracted_at: str,
    source_origin: str = "test_bank_ofx",
    institution_fid: str = "fid-ofx",
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, routing_number, account_type, institution_org,
             institution_fid, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, ?, ?, ?, ?, '/tmp/test.ofx', 'ofx', ?,
                ?::TIMESTAMP, ?::TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            native_key,
            routing_number,
            account_type,
            institution_org,
            institution_fid,
            source_origin,
            extracted_at,
            extracted_at,
        ],
    )


def _insert_tabular_account(
    db: Database,
    *,
    native_key: str,
    account_name: str,
    institution_name: str | None,
    account_type: str | None,
    extracted_at: str,
    source_origin: str = "test_bank_tab",
    account_number: str | None = None,
    account_label: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO raw.tabular_accounts
            (account_id, account_name, account_label, account_type,
             institution_name, account_number, source_file, source_type,
             source_origin, import_id, extracted_at, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, '/tmp/test.csv', 'csv', ?, 'imp-tab-001',
                ?::TIMESTAMP, ?::TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            native_key,
            account_name,
            account_label,
            account_type,
            institution_name,
            account_number,
            source_origin,
            extracted_at,
            extracted_at,
        ],
    )


def _seed_shared_canonical_ofx_and_tabular(db: Database) -> str:
    """Seed OFX (with routing) + a later tabular row (no routing) sharing one canonical id."""
    canonical_id = "canonshared0001"
    ofx_native = "ofx-acctid-shr01"
    tab_native = "tab-acctid-shr01"

    # OFX: earlier, carries the routing number.
    _insert_ofx_account(
        db,
        native_key=ofx_native,
        routing_number="111000025",
        institution_org="Shared Bank OFX",
        account_type="CHECKING",
        extracted_at="2024-01-01 00:00:00",
    )
    _insert_accepted_source_native(
        db,
        link_id="link-ofx-shr",
        account_id=canonical_id,
        ref_value=ofx_native,
        source_type="ofx",
        source_origin="test_bank_ofx",
    )

    # Tabular: later, no routing (tabular staging always projects routing NULL).
    _insert_tabular_account(
        db,
        native_key=tab_native,
        account_name="Shared Checking",
        institution_name="Shared Bank CSV",
        account_type="checking",
        extracted_at="2024-06-01 00:00:00",
    )
    _insert_accepted_source_native(
        db,
        link_id="link-tab-shr",
        account_id=canonical_id,
        ref_value=tab_native,
        source_type="csv",
        source_origin="test_bank_tab",
    )
    return canonical_id


@pytest.mark.slow
def test_dim_accounts_no_null_clobber(db: Database) -> None:
    """A later tabular row (routing NULL) must NOT null the OFX routing_number.

    Old last-write-wins (ROW_NUMBER ORDER BY extracted_at DESC) would pick the
    later tabular row and emit routing_number = NULL. The per-field merge keeps
    the OFX value.
    """
    canonical_id = _seed_shared_canonical_ofx_and_tabular(db)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT routing_number, institution_fid FROM core.dim_accounts WHERE account_id = ?",
        [canonical_id],
    ).fetchone()

    assert row is not None, "merged canonical row missing from core.dim_accounts"
    assert row[0] == "111000025", (
        f"routing_number: expected OFX value to survive, got {row[0]!r}"
    )
    assert row[1] == "fid-ofx", (
        f"institution_fid: expected OFX value to survive, got {row[1]!r}"
    )


@pytest.mark.slow
def test_dim_accounts_collapses_sources_to_one_row(db: Database) -> None:
    """OFX + CSV rows sharing one canonical id collapse to exactly one dim row."""
    canonical_id = _seed_shared_canonical_ofx_and_tabular(db)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    count = db.execute(
        "SELECT COUNT(*) FROM core.dim_accounts WHERE account_id = ?",
        [canonical_id],
    ).fetchone()
    assert count is not None
    assert count[0] == 1, f"expected exactly one merged row, got {count[0]}"


@pytest.mark.slow
def test_dim_accounts_unlinked_account_keyed_by_source_native(db: Database) -> None:
    """An unlinked account (canonical id NULL) stays distinct under its source-native key."""
    native_key = "tab-unlinked-0001"
    _insert_tabular_account(
        db,
        native_key=native_key,
        account_name="Orphan Checking",
        institution_name="Orphan Bank",
        account_type="checking",
        extracted_at="2024-02-01 00:00:00",
    )
    # Deliberately NO app.account_links row. stg does NOT project a NULL
    # account_id — it COALESCEs the source-native key in itself — so the row
    # reaches the dim keyed by native_key and grain_key passes it through.

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        "SELECT account_id FROM core.dim_accounts WHERE account_id = ?",
        [native_key],
    ).fetchall()
    assert len(rows) == 1, (
        f"unlinked account should appear once under its native key, got {len(rows)}"
    )
    assert rows[0][0] == native_key

    # And the dim must never emit a NULL account_id.
    null_count = db.execute(
        "SELECT COUNT(*) FROM core.dim_accounts WHERE account_id IS NULL"
    ).fetchone()
    assert null_count is not None
    assert null_count[0] == 0, "core.dim_accounts must never emit a NULL account_id"


@pytest.mark.slow
def test_a_resolved_institution_slug_outranks_later_unresolved_text(
    db: Database,
) -> None:
    """A registry-resolved slug must survive a later source that only has raw text.

    institution_slug is the column account matching compares, and only some
    sources can resolve it: OFX has a <FID>, a spreadsheet has whatever its
    Institution column was typed as. When an unregistered spelling merges into
    an account whose slug came from the registry, ordering that merge by pure
    recency lets the raw text overwrite the canonical slug — and the next
    import for that bank then compares 'us_bank' against 'Shared Bank CSV',
    misses the account, and mints a duplicate.

    Every other authority-sensitive field in this merge already orders by
    (source_rank, recency); institution_slug ranks on whether the value
    resolved, which is the property that actually makes it comparable.

    The fixture isolates that ranking alone: the OFX row is *earlier*, so
    recency on its own would pick the tabular text. 'Shared Bank CSV' is
    deliberately absent from seeds.institutions — a registered spelling would
    resolve and the two branches would agree.
    """
    canonical_id = "canoninstslug01"
    ofx_native = "ofx-acctid-inst1"
    tab_native = "tab-acctid-inst1"

    # OFX: earlier, FID 5950 resolves through seeds.institutions to 'us_bank'.
    _insert_ofx_account(
        db,
        native_key=ofx_native,
        routing_number="123000220",
        institution_org="USB",
        account_type="CHECKING",
        extracted_at="2024-01-01 00:00:00",
        institution_fid="5950",
    )
    _insert_accepted_source_native(
        db,
        link_id="link-ofx-inst",
        account_id=canonical_id,
        ref_value=ofx_native,
        source_type="ofx",
        source_origin="test_bank_ofx",
    )

    # Tabular: later, and its institution text matches no registry spelling.
    _insert_tabular_account(
        db,
        native_key=tab_native,
        account_name="Shared Checking",
        institution_name="Shared Bank CSV",
        account_type="checking",
        extracted_at="2024-06-01 00:00:00",
    )
    _insert_accepted_source_native(
        db,
        link_id="link-tab-inst",
        account_id=canonical_id,
        ref_value=tab_native,
        source_type="csv",
        source_origin="test_bank_tab",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT institution_slug FROM core.dim_accounts WHERE account_id = ?",
        [canonical_id],
    ).fetchone()

    assert row is not None, "merged canonical row missing from core.dim_accounts"
    assert row[0] == "us_bank", (
        f"institution_slug: expected the registry-resolved slug to survive the "
        f"later unresolved text, got {row[0]!r}"
    )


@pytest.mark.slow
def test_last_four_derived_for_ofx_without_account_settings(db: Database) -> None:
    """OFX account without app.account_settings gets last_four derived from ACCTID digits.

    Verifies the Decision 8 capture layer: last_four is derived from source fields
    (OFX source_account_key) when no user-set app.account_settings row exists.
    """
    canonical_id = "canonofxlast401"
    ofx_native = "123456781212"  # ACCTID ending 1212
    _insert_ofx_account(
        db,
        native_key=ofx_native,
        routing_number="121000248",
        institution_org="WELLS FARGO",
        account_type="CHECKING",
        extracted_at="2024-01-01 00:00:00",
    )
    _insert_accepted_source_native(
        db,
        link_id="link-ofx-last4",
        account_id=canonical_id,
        ref_value=ofx_native,
        source_type="ofx",
        source_origin="test_bank_ofx",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT last_four, display_name FROM core.dim_accounts WHERE account_id = ?",
        [canonical_id],
    ).fetchone()
    assert row is not None, "derived-last4 canonical row missing from core.dim_accounts"
    assert row[0] == "1212", f"expected derived last_four 1212, got {row[0]!r}"
    assert "1212" in row[1], f"display_name should include last4: {row[1]!r}"


@pytest.mark.slow
def test_an_unnameable_unlinked_account_is_not_named_by_its_source_key(
    db: Database,
) -> None:
    """The terminal display_name branch never emits a source-native key.

    An account with no institution, subtype, type or last four falls through
    every naming arm to the terminal one. For an account with no accepted link
    the grain key IS the source-native key -- for a bank file, the institution's
    own account number -- so a terminal label built from it would put an account
    number in a column the taxonomy declares USER_NOTE, and from there into
    reports.* as account_name. The label degrades instead.
    """
    native_key = "471166339912"  # digits, as a real ACCTID would be
    _insert_tabular_account(
        db,
        native_key=native_key,
        # account_name is NOT NULL in raw and the dim's tabular CTE never
        # selects it, so it cannot reach display_name either way.
        account_name="Row With No Bank Fields",
        institution_name=None,
        account_type=None,
        extracted_at="2024-03-01 00:00:00",
    )
    # Deliberately NO app.account_links row. stg COALESCEs the source-native
    # key into account_id itself, so the row reaches the dim keyed by
    # native_key -- which is exactly what the terminal label must not print.

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id = ?",
        [native_key],
    ).fetchone()
    assert row is not None, "unlinked account missing from core.dim_accounts"
    assert native_key not in row[0], (
        f"display_name leaked the source-native key: {row[0]!r}"
    )
    # Against the constant, not a literal: this is the only place the SQL
    # terminal arm and the Python fallback every surface uses are compared
    # after a real plan, so a drift in either copy has to fail here.
    assert row[0] == UNNAMED_ACCOUNT_LABEL


@pytest.mark.slow
def test_the_terminal_label_omits_the_id_even_when_the_id_is_safe(
    db: Database,
) -> None:
    """A linked account with nothing to name it is unnamed too, not id-labelled.

    The terminal arm could have kept ``'Account ' || account_id`` for a linked
    account, whose grain key IS the canonical opaque id and safe to print. It
    does not, because telling the two cases apart needs a fact the dim does not
    hold: all three stg_*__accounts models COALESCE the source-native key into
    ``account_id`` before the dim sees it, so a NULL test finds nothing and a
    ``account_id <> source_account_key`` test is a proxy a fourth source could
    silently break. Dropping the id outright is fail-closed by construction.
    The label it costs applied to no account.
    """
    canonical_id = "canonnameless01"
    native_key = "tab-nameless-001"
    _insert_tabular_account(
        db,
        native_key=native_key,
        account_name="Row With No Bank Fields",
        institution_name=None,
        account_type=None,
        extracted_at="2024-03-01 00:00:00",
    )
    _insert_accepted_source_native(
        db,
        link_id="link-tab-nameless",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin="test_bank_tab",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id = ?",
        [canonical_id],
    ).fetchone()
    assert row is not None, "linked account missing from core.dim_accounts"
    assert canonical_id not in row[0], (
        f"terminal label should carry no id at all: {row[0]!r}"
    )
    # Against the constant, not a literal: this is the only place the SQL
    # terminal arm and the Python fallback every surface uses are compared
    # after a real plan, so a drift in either copy has to fail here.
    assert row[0] == UNNAMED_ACCOUNT_LABEL


@pytest.mark.slow
def test_a_last_four_alone_names_an_account_the_sentinel_would_have_taken(
    db: Database,
) -> None:
    """A last four outranks the sentinel: it is the discriminator, and it is safe.

    Every naming arm above the terminal needs an institution or a subtype, so an
    account carrying only a last four -- a real tabular-import shape -- fell all
    the way through and was labelled ``Unnamed account`` while the adjacent
    ``last_four`` column still held the digits. Two such accounts then rendered
    identically everywhere the name is all the user sees.

    The last four is not the id the terminal arm exists to withhold. It is a
    masked fragment the confirm flow already prints as evidence and the dim
    already publishes in its own column, so naming by it discloses nothing new.
    """
    _insert_tabular_account(
        db,
        native_key="tab-lastfour-only",
        account_name="Row With No Bank Fields",
        institution_name=None,
        account_type=None,
        account_number="9876554521",
        extracted_at="2024-03-01 00:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id = ?",
        ["tab-lastfour-only"],
    ).fetchone()
    assert row is not None, "account missing from core.dim_accounts"
    assert row[0] == "…4521"
    assert "9876554521" not in row[0], (
        f"only the last four may appear, never the number: {row[0]!r}"
    )


@pytest.mark.slow
def test_a_subtype_with_no_institution_still_keeps_its_last_four(
    db: Database,
) -> None:
    """The subtype arm gained a last four for the same reason the top arm has one.

    ``checking`` is a category, not an identity -- every checking account at
    every institution shares it. Before, an account with no institution stopped
    at the bare subtype and dropped a last four it had, so the arm that was
    supposed to name the account instead guaranteed a collision. This mirrors
    the institution arms, where the with-last-four variant precedes the without.
    """
    _insert_tabular_account(
        db,
        native_key="tab-subtype-lastfour",
        account_name="Row With No Bank Fields",
        institution_name=None,
        account_type="CHECKING",
        account_number="9876554521",
        extracted_at="2024-03-01 00:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id = ?",
        ["tab-subtype-lastfour"],
    ).fetchone()
    assert row is not None, "account missing from core.dim_accounts"
    assert row[0] == "checking …4521"


def test_two_accounts_sharing_one_label_keep_distinct_names(db: Database) -> None:
    """The account-label arm carries a last four for the reason every arm does.

    The label is the one name a person chose, but choosing it does not make it
    unique: Plaid sends the institution's own per-account name, and a
    household's two checking accounts routinely carry one product name. An
    arm that named both of them that would collide two accounts onto one
    string — the defect the arm was added to fix — and
    ``AccountService.resolve_strict`` raises ``AmbiguousAccountError`` on the
    duplicate, refusing a name reference that resolved before.
    """
    for native_key, number in (("tab-label-a", "4001111"), ("tab-label-b", "4002222")):
        _insert_tabular_account(
            db,
            native_key=native_key,
            account_name="HOUSEHOLD CHECKING",
            account_label="HOUSEHOLD CHECKING",
            institution_name="Test Bank",
            account_type="CHECKING",
            account_number=number,
            extracted_at="2024-03-01 00:00:00",
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    names = [
        str(row[0])
        for row in db.execute(
            "SELECT display_name FROM core.dim_accounts "
            "WHERE account_id IN ('tab-label-a', 'tab-label-b') "
            "ORDER BY account_id"
        ).fetchall()
    ]
    assert names == ["HOUSEHOLD CHECKING …1111", "HOUSEHOLD CHECKING …2222"], names


def test_a_label_alone_names_an_account_with_no_number(db: Database) -> None:
    """The discriminator is appended when there is one, never invented.

    SQL ``||`` yields NULL when any operand is NULL, so the with-last-four arm
    simply does not fire for an account whose source stated no number, and the
    bare arm below it names the account by what it does have.
    """
    _insert_tabular_account(
        db,
        native_key="tab-label-no-number",
        account_name="Vacation Fund",
        account_label="Vacation Fund",
        institution_name="Test Bank",
        account_type="SAVINGS",
        account_number=None,
        extracted_at="2024-03-01 00:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id = ?",
        ["tab-label-no-number"],
    ).fetchone()
    assert row is not None, "account missing from core.dim_accounts"
    assert row[0] == "Vacation Fund"


def test_a_non_latin_label_names_the_account_through_real_duckdb(
    db: Database,
) -> None:
    r"""The letter test is Unicode-aware in the model, not only in the mirror.

    ``\p{L}`` replaced ``[A-Za-z]`` here after a review round found the ASCII
    class silently discarding every non-Latin label — the account fell to its
    institution-derived name and the label a person wrote was dropped. The
    Python mirror pins that with non-Latin fixtures; this module's own contract
    is that the two ladders never drift, and until now nothing ran the branch
    against real DuckDB, whose regex dialect is the reason the class had to
    change in the first place.

    Discriminating on purpose: with ``[A-Za-z]`` restored, both label arms miss
    and this account renders "Test Bank savings …1111".
    """
    _insert_tabular_account(
        db,
        native_key="tab-label-non-latin",
        account_name="Row With A Non-Latin Label",
        account_label="储蓄账户",
        institution_name="Test Bank",
        account_type="SAVINGS",
        account_number="4001111",
        extracted_at="2024-03-01 00:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id = ?",
        ["tab-label-non-latin"],
    ).fetchone()
    assert row is not None, "account missing from core.dim_accounts"
    assert row[0] == "储蓄账户 …1111"


def test_the_unnamed_sentinel_is_never_promoted_as_a_source_label(
    db: Database,
) -> None:
    """The one string that means "no name" must not be taken for one.

    ``UNNAMED_ACCOUNT_LABEL`` is this ladder's own terminal arm, and
    ``is_a_name`` rejects it precisely because it compares equal to itself
    across unrelated accounts. It reaches ``account_label`` by an ordinary
    route: ``reports.*`` publish it as ``account_name``, so re-importing a
    MoneyBin export puts the literal in the Account column. Promoting it would
    hand ``resolve_strict`` and the merge matcher a name they must then discard,
    leaving the account unresolvable by what it displays — strictly worse than
    the institution-derived name it would otherwise have carried.
    """
    _insert_tabular_account(
        db,
        native_key="tab-label-sentinel",
        account_name="Row Whose Label Says Nothing Names It",
        account_label=UNNAMED_ACCOUNT_LABEL,
        institution_name="Test Bank",
        account_type="SAVINGS",
        account_number="4001111",
        extracted_at="2024-03-01 00:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT display_name FROM core.dim_accounts WHERE account_id = ?",
        ["tab-label-sentinel"],
    ).fetchone()
    assert row is not None, "account missing from core.dim_accounts"
    assert row[0] == "Test Bank savings …1111"
