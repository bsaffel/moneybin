# tests/moneybin/test_services/test_account_service.py
"""Tests for AccountService, soft-validation classifier, and canonical lists."""

# Persistence assertions read back via AccountService._load_settings (the
# service owns app.account_settings reads after the Invariant 10 migration).
# pyright: reportPrivateUsage=false
from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.payloads.accounts import AccountListPayload
from moneybin.protocol.envelope import build_envelope
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.services.account_resolution_types import UNNAMED_ACCOUNT_LABEL
from moneybin.services.account_service import (
    CLEAR,
    PLAID_CANONICAL_HOLDER_CATEGORIES,
    PLAID_CANONICAL_SUBTYPES,
    AccountService,
    AccountSettings,
    is_canonical_holder_category,
    is_canonical_subtype,
    suggest_holder_category,
    suggest_subtype,
)
from tests.moneybin.db_helpers import create_core_tables, create_core_tables_raw


class TestSubtypeClassifier:
    """Tests for Plaid subtype canonical list and soft-validation helpers."""

    @pytest.mark.unit
    def test_canonical_subtypes_present(self) -> None:
        assert "checking" in PLAID_CANONICAL_SUBTYPES
        assert "savings" in PLAID_CANONICAL_SUBTYPES
        assert "credit card" in PLAID_CANONICAL_SUBTYPES
        assert "mortgage" in PLAID_CANONICAL_SUBTYPES

    @pytest.mark.unit
    def test_is_canonical_true_for_known(self) -> None:
        assert is_canonical_subtype("checking") is True

    @pytest.mark.unit
    def test_is_canonical_false_for_unknown(self) -> None:
        assert is_canonical_subtype("chequing") is False

    @pytest.mark.unit
    def test_is_canonical_case_insensitive(self) -> None:
        assert is_canonical_subtype("CHECKING") is True

    @pytest.mark.unit
    def test_suggest_near_miss(self) -> None:
        assert suggest_subtype("chequing") == "checking"

    @pytest.mark.unit
    def test_suggest_returns_none_for_far_miss(self) -> None:
        assert suggest_subtype("xyz_garbage") is None


class TestHolderCategoryClassifier:
    """Tests for holder-category canonical set and soft-validation helpers."""

    @pytest.mark.unit
    def test_canonical_set(self) -> None:
        assert PLAID_CANONICAL_HOLDER_CATEGORIES == frozenset({
            "personal",
            "business",
            "joint",
        })

    @pytest.mark.unit
    def test_is_canonical(self) -> None:
        assert is_canonical_holder_category("personal") is True
        assert is_canonical_holder_category("corporate") is False

    @pytest.mark.unit
    def test_suggest_near_miss(self) -> None:
        assert suggest_holder_category("persoanl") == "personal"


@pytest.fixture()
def account_db(db: Database) -> Database:
    """Return a Database with core + raw tables and test data seeded."""
    conn = db.conn
    create_core_tables_raw(conn)

    # Insert test accounts — use named columns so DDL-added defaults apply to
    # Phase-2 columns (display_name, archived, etc.) without breaking this fixture.
    conn.execute("""
        INSERT INTO core.dim_accounts
            (account_id, routing_number, account_type, institution_name,
             institution_fid, source_type, source_file, extracted_at,
             loaded_at, updated_at)
        VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)  # noqa: S608  # test input, not executing SQL

    # Insert test balances
    conn.execute("""
        INSERT INTO raw.ofx_balances
            (account_id, statement_start_date, statement_end_date, ledger_balance,
             ledger_balance_date, available_balance, source_file,
             extracted_at, loaded_at, import_id, source_type)
        VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00, '2025-06-30',
         4800.00, 'test.qfx', '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx'),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00, '2025-06-30',
         15000.00, 'other.qfx', '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx')
    """)  # noqa: S608  # test input, not executing SQL

    return db


class TestListAccounts:
    """Tests for AccountService.list_accounts()."""

    @pytest.mark.unit
    def test_returns_account_list_payload(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        assert isinstance(result, AccountListPayload)
        assert len(result.rows) == 2

    @pytest.mark.unit
    def test_accounts_ordered_by_institution(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        names = [str(a.institution_name) for a in result.rows]
        assert names == sorted(names)

    @pytest.mark.unit
    def test_account_fields(self, account_db: Database) -> None:
        service = AccountService(account_db)
        result = service.list_accounts()
        acct = result.rows[0]
        assert acct.account_id in ("ACC001", "ACC002")
        assert acct.account_type in ("CHECKING", "SAVINGS")

    @pytest.mark.unit
    def test_envelope_sensitivity_medium(self, account_db: Database) -> None:
        # Default list returns medium sensitivity since it includes account metadata.
        service = AccountService(account_db)
        result = service.list_accounts()
        envelope = build_envelope(
            data=result,
            sensitivity="medium",
            actions=[
                "Use accounts_balances for current balances",
                "Use reports_spending with a category filter to drill in by account",
            ],
        )
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"
        # total_count=1 because AccountListPayload is a single dataclass (no bare list)
        # returned_count = len(rows) = 2 via _count_typed_payload
        assert d["summary"]["total_count"] == 2
        actions: list[str] = d["actions"]
        assert len(actions) > 0


class TestAccountSettingsModel:
    """Tests for AccountSettings dataclass construction and validation."""

    @pytest.mark.unit
    def test_full_construction(self) -> None:
        s = AccountSettings(
            account_id="acct_abc",
            display_name="Checking",
            official_name="PLATINUM CHECKING ACCOUNT",
            last_four="1234",
            account_subtype="checking",
            holder_category="personal",
            currency_code="USD",
            credit_limit=Decimal("5000.00"),
            archived=False,
            include_in_net_worth=True,
        )
        assert s.display_name == "Checking"
        assert s.credit_limit == Decimal("5000.00")
        assert s.account_subtype == "checking"

    @pytest.mark.unit
    def test_display_name_too_long(self) -> None:
        with pytest.raises(ValueError, match="display_name"):
            AccountSettings(account_id="a", display_name="x" * 81)

    @pytest.mark.unit
    def test_last_four_format(self) -> None:
        with pytest.raises(ValueError, match="last_four"):
            AccountSettings(account_id="a", last_four="abcd")
        with pytest.raises(ValueError, match="last_four"):
            AccountSettings(account_id="a", last_four="123")

    @pytest.mark.unit
    def test_currency_code_format(self) -> None:
        with pytest.raises(ValueError, match="currency_code"):
            AccountSettings(account_id="a", currency_code="usd")  # lowercase
        with pytest.raises(ValueError, match="currency_code"):
            AccountSettings(account_id="a", currency_code="USDD")

    @pytest.mark.unit
    def test_credit_limit_non_negative(self) -> None:
        with pytest.raises(ValueError, match="credit_limit"):
            AccountSettings(account_id="a", credit_limit=Decimal("-1.00"))

    @pytest.mark.unit
    def test_official_name_too_long(self) -> None:
        with pytest.raises(ValueError, match="official_name"):
            AccountSettings(account_id="a", official_name="x" * 201)

    @pytest.mark.unit
    def test_subtype_too_long(self) -> None:
        with pytest.raises(ValueError, match="account_subtype"):
            AccountSettings(account_id="a", account_subtype="x" * 33)

    @pytest.mark.unit
    def test_free_text_is_trimmed_before_it_is_stored(self) -> None:
        """Padding here becomes padding inside a rendered account name.

        ``core.dim_accounts`` reads each of these columns with no ``TRIM`` while
        the mint report's Python mirror trims, so one stray space from a caller
        split the announced name from the stored one. Normalizing at this
        boundary is what keeps both readers on the same string.
        """
        s = AccountSettings(
            account_id="acct_abc",
            display_name="  Joint Checking  ",
            official_name="  PLATINUM CHECKING  ",
            account_subtype="  checking  ",
            holder_category="  personal  ",
        )
        assert s.display_name == "Joint Checking"
        assert s.official_name == "PLATINUM CHECKING"
        assert s.account_subtype == "checking"
        assert s.holder_category == "personal"

    @pytest.mark.unit
    def test_whitespace_only_free_text_is_rejected_like_an_empty_string(self) -> None:
        """A blank is not a value. ``""`` already raised; ``"  "`` slipped past.

        The gap mattered because a non-NULL blank wins the model's ``COALESCE``
        outright: the account was named by an empty subtype instead of by the
        one its own source stated.
        """
        with pytest.raises(ValueError, match="account_subtype"):
            AccountSettings(account_id="a", account_subtype="   ")
        with pytest.raises(ValueError, match="display_name"):
            AccountSettings(account_id="a", display_name="   ")


@pytest.fixture()
def test_db(db: Database) -> Database:
    """Test database with all schemas + a seeded dim_accounts row for mutator tests.

    Seeds account_id='acct_a' so mutator tests (rename, archive, etc.) can call
    _assert_account_exists without failing on a missing row.
    """
    create_core_tables(db)
    db.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, account_type, institution_name, source_type)
        VALUES ('acct_a', 'CHECKING', 'Test Bank', 'ofx')
        """
    )
    return db


class TestAccountSettingsLoad:
    """Tests for AccountService._load_settings (app.account_settings read path).

    Audited write coverage (upsert/delete + audit-row pairing) lives in
    tests/moneybin/test_repositories/test_account_settings_repo.py.
    """

    @pytest.mark.unit
    def test_load_returns_none_when_absent(self, test_db: Database) -> None:
        assert AccountService(test_db)._load_settings("acct_missing") is None

    @pytest.mark.unit
    def test_settings_update_then_load(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.settings_update("acct_a", actor="cli", display_name="Checking")
        loaded = svc._load_settings("acct_a")
        assert loaded is not None
        assert loaded.display_name == "Checking"

    @pytest.mark.unit
    def test_a_legacy_blank_value_loads_as_the_absence_it_meant(
        self, test_db: Database
    ) -> None:
        """A blank written before the trim existed must not lock the account.

        `AccountSettings` validates on construction expressly so a row from a
        looser era still reads. Trimming ahead of that validation turned the
        accommodation inside out: `"   "` passed the length check when it was
        written and fails it now, and every settings mutator loads the row
        first, so the account becomes unreadable and unwritable together.
        """
        _seed_blank_settings_row(test_db)
        loaded = AccountService(test_db)._load_settings("acct_a")
        assert loaded is not None
        assert loaded.display_name is None
        assert loaded.official_name is None
        assert loaded.account_subtype is None
        assert loaded.holder_category is None

    @pytest.mark.unit
    def test_a_legacy_blank_row_can_still_be_updated(self, test_db: Database) -> None:
        """The break is not read-only; `_load_or_default` gates every mutator."""
        _seed_blank_settings_row(test_db)
        updated, _ = AccountService(test_db).settings_update(
            "acct_a", actor="cli", display_name="Everyday Spending"
        )
        assert updated.display_name == "Everyday Spending"


def _seed_blank_settings_row(db: Database) -> None:
    """Write the whitespace-only row a pre-trim MoneyBin accepted."""
    AccountSettingsRepo(db).set(
        account_id="acct_a",
        display_name="   ",
        official_name="  ",
        last_four=None,
        account_subtype=" ",
        holder_category="\t",
        currency_code=None,
        credit_limit=None,
        archived=False,
        include_in_net_worth=True,
        default_cost_basis_method=None,
        actor="test",
    )


class TestEmptyResults:
    """Tests for service behavior with no data in tables."""

    @pytest.fixture()
    def empty_db(self, db: Database) -> Database:
        create_core_tables_raw(db.conn)
        return db

    @pytest.mark.unit
    def test_list_accounts_empty_db(self, empty_db: Database) -> None:
        service = AccountService(empty_db)
        result = service.list_accounts()
        assert isinstance(result, AccountListPayload)
        assert result.rows == []


class TestAccountServiceMutators:
    """Tests for AccountService mutator methods."""

    @pytest.mark.unit
    def test_rename_inserts(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result = svc.rename("acct_a", "Checking")
        assert result.display_name == "Checking"

    @pytest.mark.unit
    def test_rename_clears_with_empty_string(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.rename("acct_a", "Checking")
        result = svc.rename("acct_a", "")
        assert result.display_name is None

    @pytest.mark.unit
    def test_include_idempotent(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.set_include_in_net_worth("acct_a", True)
        svc.set_include_in_net_worth("acct_a", True)
        loaded = svc._load_settings("acct_a")
        assert loaded is not None
        assert loaded.include_in_net_worth is True

    @pytest.mark.unit
    def test_archive_cascades_to_include(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result = svc.archive("acct_a")
        assert result.archived is True
        assert result.include_in_net_worth is False

    @pytest.mark.unit
    def test_unarchive_does_not_restore_include(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.archive("acct_a")
        result = svc.unarchive("acct_a")
        assert result.archived is False
        assert result.include_in_net_worth is False  # NOT restored

    @pytest.mark.unit
    def test_settings_update_partial(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update(
            "acct_a",
            actor="cli",
            account_subtype="checking",
            credit_limit=Decimal("5000.00"),
        )
        assert updated.account_subtype == "checking"
        assert updated.credit_limit == Decimal("5000.00")
        assert warnings == []  # canonical subtype, no warning
        # Verify persisted, not just returned
        loaded = svc._load_settings("acct_a")
        assert loaded is not None
        assert loaded.account_subtype == "checking"
        assert loaded.credit_limit == Decimal("5000.00")

    @pytest.mark.unit
    def test_settings_update_clears_with_clear_sentinel(
        self, test_db: Database
    ) -> None:
        svc = AccountService(test_db)
        svc.settings_update("acct_a", actor="cli", credit_limit=Decimal("5000.00"))
        updated, _ = svc.settings_update("acct_a", actor="cli", credit_limit=CLEAR)
        assert updated.credit_limit is None

    @pytest.mark.unit
    def test_settings_update_soft_validation_warning(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update(
            "acct_a", actor="cli", account_subtype="chequing"
        )
        assert updated.account_subtype == "chequing"  # write succeeded
        assert len(warnings) == 1
        assert warnings[0]["field"] == "account_subtype"
        assert "chequing" in warnings[0]["message"]
        assert warnings[0]["suggestion"] == "checking"

    @pytest.mark.unit
    def test_settings_update_holder_category_warning(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update(
            "acct_a", actor="cli", holder_category="corporate"
        )
        assert updated.holder_category == "corporate"
        assert len(warnings) == 1
        assert warnings[0]["field"] == "holder_category"

    @pytest.mark.unit
    def test_a_padded_canonical_value_warns_about_nothing(
        self, test_db: Database
    ) -> None:
        """The warning has to describe the value that actually gets stored.

        `AccountSettings` trims on construction, so `"  checking  "` is written
        as the canonical `"checking"`. Reading the soft-canonical check off the
        raw caller value instead reports that MoneyBin does not know a subtype
        it just recognized, normalized, and stored.
        """
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update(
            "acct_a", actor="cli", account_subtype="  checking  "
        )
        assert updated.account_subtype == "checking"
        assert warnings == []

    @pytest.mark.unit
    def test_a_padded_unknown_value_is_reported_by_its_stored_spelling(
        self, test_db: Database
    ) -> None:
        """Padding must not reach the message either.

        The warning is the only place a caller learns which spelling was
        doubted, so quoting the untrimmed input points them at a string the
        database does not hold.
        """
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update(
            "acct_a", actor="cli", holder_category="  corporate  "
        )
        assert updated.holder_category == "corporate"
        assert len(warnings) == 1
        assert warnings[0]["message"] == "'corporate' is not a known holder category"

    @pytest.mark.unit
    def test_settings_update_default_cost_basis_method_persists(
        self, test_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        restated: list[tuple[Database, bool]] = []

        def record_restatement(
            db: Database, *, account_currency_changed: bool = False
        ) -> None:
            restated.append((db, account_currency_changed))

        monkeypatch.setattr(
            "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
            record_restatement,
        )
        svc = AccountService(test_db)
        updated, warnings = svc.settings_update(
            "acct_a", actor="cli", default_cost_basis_method="hifo"
        )
        assert updated.default_cost_basis_method == "hifo"
        assert warnings == []
        loaded = svc._load_settings("acct_a")
        assert loaded is not None
        assert loaded.default_cost_basis_method == "hifo"
        assert restated == [(test_db, False)]

    @pytest.mark.unit
    def test_currency_update_restates_from_the_account_dimension(
        self, test_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        restated: list[tuple[Database, bool]] = []

        def record_restatement(
            db: Database, *, account_currency_changed: bool = False
        ) -> None:
            restated.append((db, account_currency_changed))

        monkeypatch.setattr(
            "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
            record_restatement,
        )

        AccountService(test_db).settings_update(
            "acct_a", actor="cli", currency_code="EUR"
        )

        assert restated == [(test_db, True)]

    @pytest.mark.unit
    def test_unrelated_account_setting_does_not_restate_fx_accounting(
        self, test_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        restated: list[Database] = []
        monkeypatch.setattr(
            "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
            restated.append,
        )

        AccountService(test_db).settings_update(
            "acct_a", actor="cli", display_name="Everyday"
        )

        assert restated == []

    @pytest.mark.unit
    def test_settings_update_default_cost_basis_method_clear_sentinel(
        self, test_db: Database
    ) -> None:
        svc = AccountService(test_db)
        svc.settings_update("acct_a", actor="cli", default_cost_basis_method="fifo")
        updated, _ = svc.settings_update(
            "acct_a", actor="cli", default_cost_basis_method=CLEAR
        )
        assert updated.default_cost_basis_method is None

    @pytest.mark.unit
    def test_settings_update_invalid_default_cost_basis_method_raises_before_db(
        self, test_db: Database
    ) -> None:
        svc = AccountService(test_db)
        with pytest.raises(UserError, match="Invalid cost-basis method"):
            svc.settings_update("acct_a", actor="cli", default_cost_basis_method="lifo")
        # Rejected before the DB write: no settings row was ever created.
        assert svc._load_settings("acct_a") is None


class TestSettingsUpdateExtended:
    """Tests for the Group 13 settings_update extension.

    settings_update absorbs display_name, include_in_net_worth, and archived
    so the prior single-field mutators (rename, set_include_in_net_worth,
    archive, unarchive) become thin delegates over one write path.
    """

    @pytest.mark.unit
    def test_set_display_name(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result, _ = svc.settings_update(
            "acct_a", actor="cli", display_name="My Renamed Account"
        )
        assert result.display_name == "My Renamed Account"

    @pytest.mark.unit
    def test_set_include_in_net_worth_false(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result, _ = svc.settings_update(
            "acct_a", actor="cli", include_in_net_worth=False
        )
        assert result.include_in_net_worth is False

    @pytest.mark.unit
    def test_set_archived_true_cascades_include_false(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        # Start from the default include_in_net_worth=True.
        result, _ = svc.settings_update("acct_a", actor="cli", archived=True)
        assert result.archived is True
        assert result.include_in_net_worth is False, (
            "Archiving must cascade include_in_net_worth to False"
        )

    @pytest.mark.unit
    def test_set_archived_false_does_not_restore_include(
        self, test_db: Database
    ) -> None:
        svc = AccountService(test_db)
        # Archive (cascades include=False).
        svc.settings_update("acct_a", actor="cli", archived=True)
        # Unarchive — include stays False, matching the prior unarchive() contract.
        result, _ = svc.settings_update("acct_a", actor="cli", archived=False)
        assert result.archived is False
        assert result.include_in_net_worth is False

    @pytest.mark.unit
    def test_clear_display_name_via_clear_sentinel(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        svc.settings_update("acct_a", actor="cli", display_name="Temporary Name")
        result, _ = svc.settings_update("acct_a", actor="cli", display_name=CLEAR)
        assert result.display_name is None

    @pytest.mark.unit
    def test_multi_field_partial_update(self, test_db: Database) -> None:
        svc = AccountService(test_db)
        result, _ = svc.settings_update(
            "acct_a",
            actor="cli",
            display_name="Multi Field",
            include_in_net_worth=False,
        )
        assert result.display_name == "Multi Field"
        assert result.include_in_net_worth is False

    @pytest.mark.unit
    def test_empty_diff_is_noop_no_audit(self, test_db: Database) -> None:
        """No field changes → no write and no phantom account_settings.set audit row."""
        svc = AccountService(test_db)
        _settings, warnings = svc.settings_update("acct_a", actor="cli")
        assert warnings == []
        # No write happened: no settings row created, no audit row emitted.
        assert svc._load_settings("acct_a") is None
        audit_count = test_db.execute(
            "SELECT COUNT(*) FROM app.audit_log "
            "WHERE target_id = ? AND action = 'account_settings.set'",
            ["acct_a"],
        ).fetchone()
        assert audit_count[0] == 0  # type: ignore[index]

    @pytest.mark.unit
    def test_empty_diff_still_rejects_unknown_account(self, test_db: Database) -> None:
        """Account existence is checked before the empty-diff early-return.

        A no-op (empty diff) against a nonexistent account must still raise, not
        silently return defaults.
        """
        svc = AccountService(test_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.settings_update("ACCTO1_typo", actor="cli")

    @pytest.mark.unit
    def test_the_unnamed_label_is_refused_as_a_display_name(
        self, test_db: Database
    ) -> None:
        """The label core uses to mean "no name" may not be set as one.

        `is_a_name` treats this exact string as the absence of a name, so an
        account wearing it drops out of fuzzy resolution, `resolve_strict` and
        merge-name matching. Accepting it here would let a user name an account
        something MoneyBin then refuses to look up, with nothing said.
        """
        svc = AccountService(test_db)
        with pytest.raises(UserError, match="reserved"):
            svc.settings_update(
                "acct_a", actor="cli", display_name=UNNAMED_ACCOUNT_LABEL
            )
        # Refused before the DB write, like the cost-basis vocabulary above.
        assert svc._load_settings("acct_a") is None

    @pytest.mark.unit
    def test_a_case_variant_of_the_unnamed_label_is_refused_too(
        self, test_db: Database
    ) -> None:
        """A case variant resolves other accounts' labels to this one.

        `resolve_strict` compares `LOWER(display_name) = LOWER(?)`, so with a
        generated-sentinel account present, asking for the exact label that
        account displays matches both rows; the sentinel row is then filtered
        as nameless and the *user's* account is returned as a unique hit. The
        reservation is case-insensitive because the collision it prevents is.
        """
        svc = AccountService(test_db)
        with pytest.raises(UserError, match="reserved"):
            svc.settings_update(
                "acct_a", actor="cli", display_name=UNNAMED_ACCOUNT_LABEL.lower()
            )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "variant",
        [" Unnamed account ", "Unnamed  account", "Unnamed\taccount"],
        ids=["padded", "doubled-space", "tab"],
    )
    def test_a_whitespace_variant_of_the_unnamed_label_is_refused(
        self, test_db: Database, variant: str
    ) -> None:
        """The reservation must use the normalization the matcher uses.

        `resolve_entity_reference`'s third rung compares `_normalize` forms,
        which NFKC-fold and collapse whitespace. Guarding on `casefold()` alone
        let these through, and because generated placeholders are filtered out
        of the candidate-name slot, a request for the exact label MoneyBin
        displays for some *other* account then resolved uniquely to this one --
        the collision the reservation exists to prevent, reached by whitespace
        instead of case.
        """
        svc = AccountService(test_db)
        with pytest.raises(UserError, match="reserved"):
            svc.settings_update("acct_a", actor="cli", display_name=variant)

    @pytest.mark.unit
    def test_a_row_already_holding_the_label_stays_readable(
        self, test_db: Database
    ) -> None:
        """Reserving the label must not strand a row that already carries it.

        `_load_settings` builds an `AccountSettings`, so any rule enforced in
        `__post_init__` runs on reads too and would make such a row raise
        instead of load. The reservation belongs on the write path for that
        reason; this pins it there.
        """
        AccountSettingsRepo(test_db).set(
            account_id="acct_a",
            display_name=UNNAMED_ACCOUNT_LABEL,
            official_name=None,
            last_four=None,
            account_subtype=None,
            holder_category=None,
            currency_code=None,
            credit_limit=None,
            archived=False,
            include_in_net_worth=True,
            default_cost_basis_method=None,
            actor="test",
        )
        loaded = AccountService(test_db)._load_settings("acct_a")
        assert loaded is not None
        assert loaded.display_name == UNNAMED_ACCOUNT_LABEL


# ---------------------------------------------------------------------------
# Helpers for new extended-read tests
# ---------------------------------------------------------------------------


def _insert_dim_account(
    db: Database,
    account_id: str,
    account_type: str = "CHECKING",
    institution_name: str | None = "Test Bank",
    source_type: str = "ofx",
    display_name: str | None = None,
    last_four: str | None = None,
    account_subtype: str | None = None,
    holder_category: str | None = None,
    currency_code: str = "USD",
    credit_limit: Decimal | None = None,
    archived: bool = False,
    include_in_net_worth: bool = True,
    routing_number: str | None = None,
    official_name: str | None = None,
) -> None:
    """Insert a row directly into core.dim_accounts for unit testing.

    Bypasses SQLMesh (which is not run in unit tests) and inserts a fully
    resolved row with both source-derived and settings-derived columns.
    Setting archived=TRUE directly here is intentional — it lets tests that
    need to verify archive filtering do so without running the LEFT JOIN
    through app.account_settings.
    """
    db.execute(
        """
        INSERT INTO core.dim_accounts (
            account_id, routing_number, account_type, institution_name,
            institution_fid, source_type, source_file, extracted_at,
            loaded_at, updated_at,
            display_name, official_name, last_four, account_subtype,
            holder_category, currency_code, credit_limit,
            archived, include_in_net_worth
        ) VALUES (?, ?, ?, ?, NULL, ?, 'test.qfx', '2025-01-01',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            account_id,
            routing_number,
            account_type,
            institution_name,
            source_type,
            display_name,
            official_name,
            last_four,
            account_subtype,
            holder_category,
            currency_code,
            credit_limit,
            archived,
            include_in_net_worth,
        ],
    )


@pytest.fixture()
def extended_db(db: Database) -> Database:
    """Database with full Phase-2 dim_accounts DDL for extended-read tests."""
    create_core_tables(db)
    return db


class TestAccountServiceListExtended:
    """Extended list_accounts tests: new columns, archiving, redaction, type filter."""

    @pytest.mark.unit
    def test_list_includes_new_columns(self, extended_db: Database) -> None:
        _insert_dim_account(
            extended_db,
            "acct_ext1",
            display_name="My Checking",
            account_subtype="checking",
            holder_category="personal",
            last_four="9999",
            credit_limit=None,
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts()
        assert len(result.rows) == 1
        acct = result.rows[0]
        assert acct.account_id == "acct_ext1"
        assert acct.display_name == "My Checking"
        assert acct.account_subtype == "checking"
        assert acct.holder_category == "personal"
        assert acct.archived is False
        assert acct.include_in_net_worth is True

    @pytest.mark.unit
    def test_list_hides_archived_by_default(self, extended_db: Database) -> None:
        _insert_dim_account(extended_db, "acct_active", institution_name="Alpha Bank")
        _insert_dim_account(
            extended_db, "acct_archived", institution_name="Beta Bank", archived=True
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts()  # default: include_archived=False
        ids = [a.account_id for a in result.rows]
        assert "acct_active" in ids
        assert "acct_archived" not in ids

    @pytest.mark.unit
    def test_list_include_archived_returns_all(self, extended_db: Database) -> None:
        _insert_dim_account(extended_db, "acct_active", institution_name="Alpha Bank")
        _insert_dim_account(
            extended_db, "acct_archived", institution_name="Beta Bank", archived=True
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts(include_archived=True)
        ids = [a.account_id for a in result.rows]
        assert "acct_active" in ids
        assert "acct_archived" in ids
        assert len(ids) == 2

    @pytest.mark.unit
    def test_list_always_includes_last_four_and_credit_limit(
        self, extended_db: Database
    ) -> None:
        """list_accounts always returns last_four and credit_limit fields.

        Middleware handles masking of CRITICAL-tier fields; the service no longer
        accepts a ``redacted`` kwarg.
        """
        _insert_dim_account(
            extended_db,
            "acct_pii",
            last_four="1234",
            credit_limit=Decimal("5000.00"),
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts()
        acct = result.rows[0]
        assert acct.last_four == "1234"
        assert acct.credit_limit == Decimal("5000.00")

    @pytest.mark.unit
    def test_list_type_filter(self, extended_db: Database) -> None:
        _insert_dim_account(
            extended_db,
            "acct_checking",
            account_type="CHECKING",
            institution_name="Alpha Bank",
        )
        _insert_dim_account(
            extended_db,
            "acct_savings",
            account_type="SAVINGS",
            institution_name="Alpha Bank",
        )
        svc = AccountService(extended_db)
        result = svc.list_accounts(type_filter="CHECKING")
        ids = [a.account_id for a in result.rows]
        assert "acct_checking" in ids
        assert "acct_savings" not in ids
        assert len(ids) == 1

    @pytest.mark.unit
    def test_list_type_filter_case_insensitive(self, extended_db: Database) -> None:
        # Seed an OFX-style account (uppercase account_type) and a user-set subtype
        # (lowercase). Filter with mixed casing should match.
        _insert_dim_account(
            extended_db,
            "acct_a",
            account_type="CHECKING",
            account_subtype="checking",
        )
        svc = AccountService(extended_db)
        # User filter "checking" should match account_type "CHECKING"
        result = svc.list_accounts(type_filter="checking")
        assert len(result.rows) == 1
        # User filter "CHECKING" should also match
        result_upper = svc.list_accounts(type_filter="CHECKING")
        assert len(result_upper.rows) == 1


class TestAccountServiceGetAccount:
    """Tests for AccountService.get_account()."""

    @pytest.mark.unit
    def test_get_returns_full_record(self, extended_db: Database) -> None:
        _insert_dim_account(
            extended_db,
            "acct_get1",
            display_name="Premium Checking",
            last_four="4321",
            account_subtype="checking",
            institution_name="First National",
        )
        svc = AccountService(extended_db)
        result = svc.get_account("acct_get1")
        assert result is not None
        assert result.account_id == "acct_get1"
        assert result.display_name == "Premium Checking"
        assert result.last_four == "4321"
        assert result.account_subtype == "checking"
        assert result.institution_name == "First National"
        assert result.archived is False
        assert result.include_in_net_worth is True

    @pytest.mark.unit
    def test_get_returns_none_for_missing(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        assert svc.get_account("acct_missing") is None


class TestAccountServiceSummary:
    """Tests for AccountService.summary()."""

    @pytest.mark.unit
    def test_summary_aggregates_by_type_and_subtype(
        self, extended_db: Database
    ) -> None:
        # 2 checking (one archived), 1 savings
        _insert_dim_account(
            extended_db,
            "acct_chk1",
            account_type="CHECKING",
            institution_name="Alpha Bank",
        )
        _insert_dim_account(
            extended_db,
            "acct_chk2",
            account_type="CHECKING",
            institution_name="Beta Bank",
            archived=True,
        )
        _insert_dim_account(
            extended_db,
            "acct_sav1",
            account_type="SAVINGS",
            institution_name="Gamma Bank",
            include_in_net_worth=False,
        )
        svc = AccountService(extended_db)
        result = svc.summary()
        # total_accounts counts all rows including archived
        assert result.total_accounts == 3
        # count_by_type excludes archived
        assert result.count_by_type == {"CHECKING": 1, "SAVINGS": 1}
        assert result.count_archived == 1
        assert result.count_excluded_from_net_worth == 1
        # recent activity is 0 (no transactions seeded)
        assert result.count_with_recent_activity == 0

    @pytest.mark.unit
    def test_summary_empty(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        result = svc.summary()
        assert result.total_accounts == 0
        assert result.count_by_type == {}


class TestMutatorAccountValidation:
    """Tests that mutators reject unknown account_ids."""

    @pytest.mark.unit
    def test_rename_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.rename("ACCTO1_typo", "new name")

    @pytest.mark.unit
    def test_set_include_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.set_include_in_net_worth("ACCTO1_typo", False)

    @pytest.mark.unit
    def test_archive_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.archive("ACCTO1_typo")

    @pytest.mark.unit
    def test_unarchive_rejects_unknown_account(self, extended_db: Database) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.unarchive("ACCTO1_typo")

    @pytest.mark.unit
    def test_settings_update_rejects_unknown_account(
        self, extended_db: Database
    ) -> None:
        svc = AccountService(extended_db)
        with pytest.raises(UserError, match="Account not found"):
            svc.settings_update("ACCTO1_typo", actor="cli", official_name="New Name")


class TestAccountResolution:
    """Tests for the AccountResolution dataclass."""

    @pytest.mark.unit
    def test_to_dict_shape(self) -> None:
        """AccountResolution.to_dict produces the agent-facing JSON shape."""
        from moneybin.services.account_service import AccountResolution

        r = AccountResolution(
            account_id="abc123",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
            confidence=0.876,
        )
        assert r.to_dict() == {
            "account_id": "abc123",
            "display_name": "Chase Checking",
            "account_subtype": "checking",
            "institution_name": "Chase",
            "confidence": 0.876,
        }

    @pytest.mark.unit
    def test_rounds_confidence_to_three_decimals(self) -> None:
        """Confidence is rounded to 3 decimals at serialization."""
        from moneybin.services.account_service import AccountResolution

        r = AccountResolution(
            account_id="x",
            display_name="X",
            account_subtype=None,
            institution_name=None,
            confidence=0.123456789,
        )
        assert r.to_dict()["confidence"] == 0.123

    @pytest.mark.unit
    def test_preserves_nulls(self) -> None:
        """Null subtypes/institution serialize as null (not omitted)."""
        from moneybin.services.account_service import AccountResolution

        r = AccountResolution(
            account_id="x",
            display_name="X",
            account_subtype=None,
            institution_name=None,
            confidence=1.0,
        )
        d = r.to_dict()
        assert d["account_subtype"] is None
        assert d["institution_name"] is None


class TestAccountServiceResolve:
    """Tests for AccountService.resolve()."""

    @pytest.mark.unit
    def test_exact_display_name_match_returns_top_confidence(
        self, extended_db: Database
    ) -> None:
        """Exact display_name match returns top-confidence candidate."""
        _insert_dim_account(
            extended_db,
            "a1",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
        )
        _insert_dim_account(
            extended_db,
            "a2",
            display_name="Schwab Brokerage",
            account_subtype="brokerage",
            institution_name="Schwab",
        )
        payload = AccountService(extended_db).resolve("Chase Checking")
        assert len(payload.matches) >= 1
        assert payload.matches[0].account_id == "a1"
        assert payload.matches[0].confidence == 1.0

    @pytest.mark.unit
    def test_fuzzy_match_handles_typos(self, extended_db: Database) -> None:
        """Typo'd query still finds the right account."""
        _insert_dim_account(
            extended_db,
            "a1",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
        )
        payload = AccountService(extended_db).resolve("Chse Chking")
        assert len(payload.matches) == 1
        assert payload.matches[0].account_id == "a1"
        assert 0.5 < payload.matches[0].confidence < 1.0

    @pytest.mark.unit
    def test_no_match_returns_empty_payload(self, extended_db: Database) -> None:
        """Query with no candidates returns a payload with empty matches."""
        payload = AccountService(extended_db).resolve("nonexistent")
        assert payload.matches == []

    @pytest.mark.unit
    def test_limit_caps_results(self, extended_db: Database) -> None:
        """Limit caps the number of results."""
        _insert_dim_account(extended_db, "a1", display_name="Account One")
        _insert_dim_account(extended_db, "a2", display_name="Account Two")
        _insert_dim_account(extended_db, "a3", display_name="Account Three")
        _insert_dim_account(extended_db, "a4", display_name="Account Four")
        payload = AccountService(extended_db).resolve("account", limit=2)
        assert len(payload.matches) == 2

    @pytest.mark.unit
    def test_unlimited_results_have_stable_account_id_tiebreak(
        self, extended_db: Database
    ) -> None:
        """Equal-confidence matches are complete and ordered by stable ID."""
        for account_id in ("tie_c", "tie_a", "tie_b"):
            _insert_dim_account(extended_db, account_id, display_name="zzzz")

        payload = AccountService(extended_db).resolve("zzzz", limit=None)

        assert [match.account_id for match in payload.matches] == [
            "tie_a",
            "tie_b",
            "tie_c",
        ]

    @pytest.mark.unit
    def test_negative_limit_returns_empty(self, extended_db: Database) -> None:
        """Negative limit returns empty payload.

        Never the Python slice semantics ('all but the last N') that callers
        would not expect from a max-candidates parameter.
        """
        _insert_dim_account(extended_db, "a1", display_name="Account One")
        _insert_dim_account(extended_db, "a2", display_name="Account Two")
        _insert_dim_account(extended_db, "a3", display_name="Account Three")
        assert AccountService(extended_db).resolve("account", limit=-1).matches == []
        assert AccountService(extended_db).resolve("account", limit=0).matches == []

    @pytest.mark.unit
    def test_matches_against_subtype(self, extended_db: Database) -> None:
        """Match against account_subtype, not just display_name."""
        _insert_dim_account(
            extended_db,
            "a1",
            display_name="Account 1234",
            account_subtype="checking",
            institution_name="Chase",
        )
        payload = AccountService(extended_db).resolve("checking")
        assert len(payload.matches) == 1
        assert payload.matches[0].account_id == "a1"

    @pytest.mark.unit
    def test_matches_against_institution_name(self, extended_db: Database) -> None:
        """Match against institution_name."""
        _insert_dim_account(
            extended_db,
            "a1",
            display_name="XYZ Account",
            account_subtype="checking",
            institution_name="Schwab Bank",
        )
        payload = AccountService(extended_db).resolve("schwab")
        assert len(payload.matches) == 1
        assert payload.matches[0].account_id == "a1"

    @pytest.mark.unit
    def test_results_sort_by_confidence_descending(self, extended_db: Database) -> None:
        """Results sort by confidence descending."""
        _insert_dim_account(
            extended_db,
            "a1",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
        )
        _insert_dim_account(
            extended_db,
            "a2",
            display_name="Bank of America",
            account_subtype="checking",
            institution_name="BofA",
        )
        payload = AccountService(extended_db).resolve("chase")
        assert len(payload.matches) >= 1
        assert payload.matches[0].account_id == "a1"  # better fuzzy match
        if len(payload.matches) > 1:
            assert payload.matches[0].confidence > payload.matches[1].confidence

    @pytest.mark.unit
    def test_empty_query_returns_empty_payload(self, extended_db: Database) -> None:
        """Whitespace-only or empty query short-circuits to empty payload."""
        _insert_dim_account(extended_db, "a1", display_name="Anything")
        assert AccountService(extended_db).resolve("").matches == []
        assert AccountService(extended_db).resolve("   ").matches == []

    @pytest.mark.unit
    def test_the_unnamed_sentinel_resolves_to_nothing(
        self, extended_db: Database
    ) -> None:
        """Typing back the sentinel MoneyBin displayed must not pick an account.

        Every account the dim cannot name carries the same label, so a string
        comparison scores each of them 1.0 against it and the tiebreak hands
        back whichever account id sorts first -- a maximally-confident answer
        built on the one fact that distinguishes nothing. Returning no match is
        the honest reply: the sentinel says an account exists, never which one.
        """
        for account_id in ("nameless_a", "nameless_b"):
            _insert_dim_account(
                extended_db,
                account_id,
                display_name=UNNAMED_ACCOUNT_LABEL,
                institution_name=None,
                account_subtype=None,
            )

        payload = AccountService(extended_db).resolve(UNNAMED_ACCOUNT_LABEL)

        assert payload.matches == []

    @pytest.mark.unit
    def test_a_real_name_still_resolves_when_a_sentinel_row_exists(
        self, extended_db: Database
    ) -> None:
        """The refusal is scoped to the sentinel, not to unnameable accounts.

        Guards the obvious over-correction: dropping every row whose label the
        dim generated, rather than only the one label that identifies nothing.
        """
        _insert_dim_account(
            extended_db,
            "nameless",
            display_name=UNNAMED_ACCOUNT_LABEL,
            institution_name=None,
            account_subtype=None,
        )
        _insert_dim_account(
            extended_db,
            "named",
            display_name="Chase Checking",
            account_subtype="checking",
            institution_name="Chase",
        )

        payload = AccountService(extended_db).resolve("Chase Checking")

        assert [m.account_id for m in payload.matches] == ["named"]
        assert payload.matches[0].confidence == 1.0


class TestAccountServiceResolveStrict:
    """Tests for AccountService.resolve_strict() — strict id-or-name lookup."""

    @pytest.mark.unit
    def test_returns_account_id_for_exact_id_match(self, extended_db: Database) -> None:
        """Exact account_id pass-through returns the same id."""
        from moneybin.services.account_service import AccountService

        _insert_dim_account(extended_db, "acct_abc123", display_name="Chase Checking")
        assert (
            AccountService(extended_db).resolve_strict("acct_abc123") == "acct_abc123"
        )

    @pytest.mark.unit
    def test_resolves_display_name_case_insensitive(
        self, extended_db: Database
    ) -> None:
        """Exact case-insensitive match on display_name returns its account_id."""
        from moneybin.services.account_service import AccountService

        _insert_dim_account(extended_db, "acct_abc123", display_name="Chase Checking")
        svc = AccountService(extended_db)
        assert svc.resolve_strict("Chase Checking") == "acct_abc123"
        assert svc.resolve_strict("chase checking") == "acct_abc123"
        assert svc.resolve_strict("CHASE CHECKING") == "acct_abc123"

    @pytest.mark.unit
    def test_raises_not_found_with_candidates(self, extended_db: Database) -> None:
        """Unknown reference raises AccountNotFoundError listing candidates."""
        from moneybin.services.account_service import (
            AccountNotFoundError,
            AccountService,
        )

        _insert_dim_account(extended_db, "acct_a1", display_name="Chase Checking")
        _insert_dim_account(extended_db, "acct_a2", display_name="Schwab Brokerage")
        with pytest.raises(AccountNotFoundError) as excinfo:
            AccountService(extended_db).resolve_strict("Nonexistent Account")
        assert excinfo.value.query == "Nonexistent Account"
        candidate_names = {name for _, name in excinfo.value.candidates}
        assert candidate_names == {"Chase Checking", "Schwab Brokerage"}

    @pytest.mark.unit
    def test_raises_ambiguous_on_display_name_collision(
        self, extended_db: Database
    ) -> None:
        """Two accounts with the same display_name raise AmbiguousAccountError.

        The COALESCE defaults in dim_accounts (institution + type + last-4)
        can collide across sources; the resolver must surface that instead
        of silently doubling.
        """
        from moneybin.services.account_service import (
            AccountService,
            AmbiguousAccountError,
        )

        _insert_dim_account(extended_db, "acct_a1", display_name="Joint Account")
        _insert_dim_account(extended_db, "acct_a2", display_name="Joint Account")
        with pytest.raises(AmbiguousAccountError) as excinfo:
            AccountService(extended_db).resolve_strict("Joint Account")
        assert excinfo.value.query == "Joint Account"
        assert set(excinfo.value.account_ids) == {"acct_a1", "acct_a2"}

    @pytest.mark.unit
    def test_id_match_wins_over_name_match(self, extended_db: Database) -> None:
        """Id-exact match wins over display_name match when they collide.

        Guarantees the resolution order: account_id check runs first, so a
        valid id never falls through to the case-insensitive name lookup.
        """
        from moneybin.services.account_service import AccountService

        # Pathological setup: account A's id is "checking"; account B's
        # display_name is also "checking". The id match must win.
        _insert_dim_account(extended_db, "checking", display_name="Primary")
        _insert_dim_account(extended_db, "acct_b2", display_name="checking")
        assert AccountService(extended_db).resolve_strict("checking") == "checking"

    @pytest.mark.unit
    def test_skips_archived_account_on_display_name_collision(
        self, extended_db: Database
    ) -> None:
        """Display-name shared by an archived and an active account resolves to active.

        Mirrors the ``NOT a.archived`` filter on the reports views — an
        archived old account reusing a name shouldn't trigger
        AmbiguousAccountError.
        """
        from moneybin.services.account_service import AccountService

        _insert_dim_account(
            extended_db, "acct_old", display_name="Chase Checking", archived=True
        )
        _insert_dim_account(
            extended_db, "acct_new", display_name="Chase Checking", archived=False
        )
        assert (
            AccountService(extended_db).resolve_strict("Chase Checking") == "acct_new"
        )

    @pytest.mark.unit
    def test_archived_account_id_not_resolvable(self, extended_db: Database) -> None:
        """Explicit archived account_id raises AccountNotFoundError.

        The resolver mirrors report-view semantics; callers that need
        archived-account access should reach for a non-strict lookup.
        """
        from moneybin.services.account_service import (
            AccountNotFoundError,
            AccountService,
        )

        _insert_dim_account(
            extended_db, "acct_old", display_name="Old Account", archived=True
        )
        _insert_dim_account(
            extended_db, "acct_active", display_name="Active", archived=False
        )
        with pytest.raises(AccountNotFoundError):
            AccountService(extended_db).resolve_strict("acct_old")

    @pytest.mark.unit
    def test_the_unnamed_sentinel_is_not_a_strict_reference(
        self, extended_db: Database
    ) -> None:
        """A lone unnameable account must not answer to the label it displays.

        The ambiguity guard only fires from two sentinel rows up; one is the
        ordinary case, and there the label reads as a unique exact match. This
        resolver feeds investment mutations, transaction categorization and
        sheet bindings, so a silent hit writes to an account nobody picked.
        """
        from moneybin.services.account_service import AccountNotFoundError

        _insert_dim_account(
            extended_db,
            "nameless",
            display_name=UNNAMED_ACCOUNT_LABEL,
            institution_name=None,
        )
        with pytest.raises(AccountNotFoundError):
            AccountService(extended_db).resolve_strict(UNNAMED_ACCOUNT_LABEL)

    @pytest.mark.unit
    def test_an_unnameable_account_still_resolves_by_its_id(
        self, extended_db: Database
    ) -> None:
        """Refusing the label must not stop the account being addressable.

        The id is the caller's remaining handle on an account core could not
        name; taking that away would make the row unusable rather than safe.
        """
        _insert_dim_account(
            extended_db,
            "nameless",
            display_name=UNNAMED_ACCOUNT_LABEL,
            institution_name=None,
        )
        assert AccountService(extended_db).resolve_strict("nameless") == "nameless"

    @pytest.mark.unit
    def test_a_candidate_with_no_name_is_listed_as_the_placeholder(self) -> None:
        """A nameless candidate must never be listed by its id instead.

        The candidate builders behind the `accounts`, `transactions` and
        `investments` tools hand this error a name per row, and an account
        nothing can name hands it an empty one. `handle_cli_errors` logs this
        message to the durable `cli_YYYY-MM-DD.log` on the strength of it being
        a fixed MoneyBin string -- and an account with no resolver link carries
        its source-native key as its id, on OFX a real `<ACCTID>`.
        """
        from moneybin.services.account_service import AccountNotFoundError

        error = AccountNotFoundError("typo", [("acct_secret", "")])

        assert "acct_secret" not in error.message
        assert UNNAMED_ACCOUNT_LABEL in error.message


class TestNullAccountType:
    """A NULL account_type must not surface as the string "None".

    Normalizing account_type to a canonical vocabulary made NULL a routine,
    documented outcome (an unrecognized source spelling resolves to NULL rather
    than a guess). The read path coerced the column with str(), which turns SQL
    NULL into the four-character string "None" — shown to the user and returned
    to the agent as though it were a real classification.
    """

    @pytest.fixture
    def untyped_account_db(self, db: Database) -> Database:
        conn = db.conn
        create_core_tables_raw(conn)
        conn.execute("""
            INSERT INTO core.dim_accounts
                (account_id, routing_number, account_type, institution_name,
                 institution_fid, source_type, source_file, extracted_at,
                 loaded_at, updated_at)
            VALUES
            ('ACCNULL', '333000075', NULL, 'Untyped Bank', '9999', 'ofx',
             'untyped.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        return db

    @pytest.mark.unit
    def test_list_accounts_preserves_null_account_type(
        self, untyped_account_db: Database
    ) -> None:
        service = AccountService(untyped_account_db)
        acct = service.list_accounts().rows[0]
        assert acct.account_type is None, (
            f"NULL rendered as {acct.account_type!r} instead of None"
        )

    @pytest.mark.unit
    def test_get_account_preserves_null_account_type(
        self, untyped_account_db: Database
    ) -> None:
        service = AccountService(untyped_account_db)
        acct = service.get_account("ACCNULL")
        assert acct is not None
        assert acct.account_type is None, (
            f"NULL rendered as {acct.account_type!r} instead of None"
        )

    @pytest.mark.unit
    def test_summary_buckets_null_account_type(
        self, untyped_account_db: Database
    ) -> None:
        """count_by_type must label NULL, not carry a None key.

        AccountSummaryStats.count_by_type is declared dict[str, int]; a raw None
        key violates that contract and renders as a bare "null" in JSON output.
        count_by_subtype has always used the <unset> label — count_by_type now
        does too.
        """
        service = AccountService(untyped_account_db)
        counts = service.summary().count_by_type
        assert None not in counts, f"count_by_type carries a None key: {counts!r}"
        assert counts.get("<unset>") == 1, counts


class TestNullCurrencyCode:
    """A NULL currency_code must not surface as the string "None".

    The same defect as TestNullAccountType, in the same two read paths, on the
    column beside it: `str(row[6])` renders SQL NULL as "None". Removing
    `dim_accounts`' blind `'USD'` fallback (multi-currency.md Requirement 3)
    made NULL a routine outcome rather than an unreachable one — an account
    whose source never stated a currency now keeps it unknown, and
    `system doctor`'s `currency_integrity` check exists precisely to fail until
    the user resolves those rows. "None" reads to an agent as a denomination.
    """

    @pytest.fixture
    def uncurrencied_account_db(self, db: Database) -> Database:
        """One account with a known type and an unknown currency.

        account_type is deliberately populated: a row that is NULL in both
        columns would pass these tests off the account_type fix and prove
        nothing about currency_code.
        """
        conn = db.conn
        create_core_tables_raw(conn)
        conn.execute("""
            INSERT INTO core.dim_accounts
                (account_id, routing_number, account_type, institution_name,
                 institution_fid, source_type, source_file, extracted_at,
                 loaded_at, updated_at)
            VALUES
            ('ACCNOCUR', '333000075', 'checking', 'Unstated Bank', '9999',
             'tabular', 'unstated.csv', '2025-01-01', CURRENT_TIMESTAMP,
             CURRENT_TIMESTAMP)
        """)
        return db

    @pytest.mark.unit
    def test_list_accounts_preserves_null_currency_code(
        self, uncurrencied_account_db: Database
    ) -> None:
        service = AccountService(uncurrencied_account_db)
        acct = service.list_accounts().rows[0]
        assert acct.currency_code is None, (
            f"NULL rendered as {acct.currency_code!r} instead of None"
        )

    @pytest.mark.unit
    def test_get_account_preserves_null_currency_code(
        self, uncurrencied_account_db: Database
    ) -> None:
        service = AccountService(uncurrencied_account_db)
        acct = service.get_account("ACCNOCUR")
        assert acct is not None
        assert acct.currency_code is None, (
            f"NULL rendered as {acct.currency_code!r} instead of None"
        )
