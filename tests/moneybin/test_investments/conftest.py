"""Canonical identities shared by investment comparison tests."""

import pytest

from moneybin.database import Database
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo


@pytest.fixture
def comparison_db(db: Database) -> Database:
    db.execute(
        "CREATE TABLE core.dim_accounts (account_id VARCHAR, currency_code VARCHAR)"
    )
    db.execute("INSERT INTO core.dim_accounts VALUES ('account', 'USD')")
    SecuritiesRepo(db).upsert(
        security_id="security",
        name="Test Security",
        security_type="equity",
        actor="test",
    )
    AccountLinksRepo(db).insert(
        link_id="account_link",
        account_id="account",
        ref_kind="source_native",
        ref_value="native_account",
        source_type="plaid",
        source_origin="origin",
        decided_by="user",
        actor="test",
    )
    SecurityLinksRepo(db).insert(
        security_id="security",
        ref_kind="plaid_security_id",
        ref_value="native_security",
        source_type="plaid",
        decided_by="user",
        actor="test",
    )
    return db
