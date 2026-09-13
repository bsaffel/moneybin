"""Identity evidence changes independently from immutable Source observations."""

import pytest

from moneybin.database import Database
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


@pytest.mark.parametrize(
    "route", ["manual_security", "plaid_security", "plaid_account"]
)
def test_same_target_route_generation_changes_evidence_not_source_version(
    comparison_db: Database, route: str
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "plaid")
    install_comparison_models(comparison_db)
    source_type = "manual" if route == "manual_security" else "plaid"
    query = """
        SELECT l.source_event_key, l.observation_version, h.event_fingerprint
        FROM prep.int_investment_events__legs AS l
        JOIN prep.int_investment_events__headers AS h USING (source_event_key)
        WHERE l.source_type = ?
    """
    before = comparison_db.execute(query, [source_type]).fetchone()
    assert before is not None
    if route == "manual_security":
        SecurityLinksRepo(comparison_db).insert(
            security_id="security",
            ref_kind="manual_investment_transaction_id",
            ref_value="manual",
            source_type="manual",
            decided_by="user",
            actor="test",
        )
    elif route == "plaid_security":
        SecuritiesRepo(comparison_db).upsert(
            security_id="intermediate_security",
            name="Intermediate Security",
            security_type="equity",
            actor="test",
        )
        for security_id in ("intermediate_security", "security"):
            link = comparison_db.execute("""
                SELECT link_id FROM app.security_links
                WHERE source_type = 'plaid' AND status = 'accepted'
            """).fetchone()
            assert link is not None
            SecurityLinksRepo(comparison_db).repoint(
                link_id=str(link[0]),
                new_security_id=security_id,
                decided_by="user",
                actor="test",
            )
    else:
        comparison_db.execute("""
            INSERT INTO core.dim_accounts VALUES ('intermediate_account', 'USD')
        """)
        for account_id in ("intermediate_account", "account"):
            link = comparison_db.execute("""
                SELECT link_id FROM app.account_links
                WHERE source_type = 'plaid' AND status = 'accepted'
            """).fetchone()
            assert link is not None
            AccountLinksRepo(comparison_db).repoint(
                link_id=str(link[0]),
                new_account_id=account_id,
                decided_by="user",
                actor="test",
            )
    after = comparison_db.execute(query, [source_type]).fetchone()
    assert after is not None
    assert after[:2] == before[:2]
    assert after[2] != before[2]
