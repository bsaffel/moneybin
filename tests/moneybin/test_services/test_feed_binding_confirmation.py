"""Feed binding confirms its own reference, not the candidate's manual ledger."""

from datetime import UTC, datetime

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.mcp.confirmation import ConfirmationBroker
from moneybin.mcp.tools.reviews import (
    _identity_binding,  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam
)
from moneybin.protocol.write_contracts import (
    IdentityDecisionRequest,
    SecurityLinkDecisionRequest,
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import SecurityLinkDecisionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.review_decisions_service import ReviewDecisionsService
from tests.moneybin.test_services.test_manual_identity_projection import (
    add_manual_observation,
)
from tests.moneybin.test_services.test_security_links_service import (
    merge_setup as merge_setup,
)


@pytest.mark.parametrize("material_merge", [False, True])
@pytest.mark.parametrize("change", ["record", "create", "repoint", "reverse"])
def test_security_approval_scopes_manual_changes_to_material_merges(
    db: Database, merge_setup: dict[str, str], material_merge: bool, change: str
) -> None:
    source = merge_setup["provisional"] if material_merge else merge_setup["survivor"]
    target = merge_setup["survivor"]
    decision = merge_setup["decision_id"]
    if not material_merge:
        decision = "feed_decision"
        SecurityLinkDecisionsRepo(db).insert(
            decision_id=decision,
            ref_kind="tiingo_ticker",
            ref_value="SYNTH",
            source_type="tiingo",
            candidate_security_id=target,
            actor="cli",
        )
    SecuritiesRepo(db).upsert(
        security_id="other", name="Synthetic other", security_type="equity", actor="cli"
    )
    add_manual_observation(db)
    db.execute(
        "UPDATE raw.manual_investment_transactions SET security_id = ?", [source]
    )
    links = SecurityLinksRepo(db)
    if change in ("repoint", "reverse"):
        links.insert(
            link_id="manual_link",
            security_id=source,
            ref_kind="manual_investment_transaction_id",
            ref_value="m",
            source_type="manual",
            decided_by="user",
            actor="cli",
        )
    requests: list[IdentityDecisionRequest] = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id=decision,
            decision="accept",
            target_id=target,
        )
    ]
    service = ReviewDecisionsService(db, actor="mcp")
    plan = service.plan_identity(requests)
    broker = ConfirmationBroker()
    now = datetime.now(UTC)
    grant = broker.consume(
        broker.issue(_identity_binding(requests, plan), now=now), now=now
    )
    if change == "record":
        add_manual_observation(db, native="new_manual")
        db.execute(
            "UPDATE raw.manual_investment_transactions SET security_id = ? WHERE source_transaction_id = 'new_manual'",
            [source],
        )
    elif change == "create":
        links.insert(
            link_id="manual_link",
            security_id="other",
            ref_kind="manual_investment_transaction_id",
            ref_value="m",
            source_type="manual",
            decided_by="user",
            actor="cli",
        )
    elif change == "repoint":
        links.repoint(
            link_id="manual_link",
            new_security_id="other",
            decided_by="user",
            actor="cli",
        )
    else:
        links.reverse(link_id="manual_link", reversed_by="user", actor="cli")
    before = {
        table: db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()  # noqa: S608  # fixed test table names
        for table in (
            "raw.manual_investment_transactions",
            "app.security_links",
            "app.security_link_decisions",
            "app.audit_log",
        )
    }
    if material_merge:
        with pytest.raises(UserError) as exc:
            service.apply_identity(
                requests,
                verify=lambda current: grant.verify(
                    _identity_binding(requests, current)
                ),
            )
        assert exc.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
        for table, rows in before.items():
            assert db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() == rows  # noqa: S608  # fixed test table names
    else:
        service.apply_identity(
            requests,
            verify=lambda current: grant.verify(_identity_binding(requests, current)),
        )
        assert (
            links.lookup(
                ref_kind="tiingo_ticker", ref_value="SYNTH", source_type="tiingo"
            )
            == target
        )
        assert (
            db.execute(
                "SELECT * FROM raw.manual_investment_transactions ORDER BY ALL"
            ).fetchall()
            == before["raw.manual_investment_transactions"]
        )


def test_feed_binding_still_rejects_new_exact_reference_collision(
    db: Database, merge_setup: dict[str, str]
) -> None:
    target = merge_setup["survivor"]
    SecurityLinkDecisionsRepo(db).insert(
        decision_id="feed",
        ref_kind="tiingo_ticker",
        ref_value="SYNTH",
        source_type="tiingo",
        candidate_security_id=target,
        actor="cli",
    )
    requests: list[IdentityDecisionRequest] = [
        SecurityLinkDecisionRequest(
            kind="security_link",
            decision_id="feed",
            decision="accept",
            target_id=target,
        )
    ]
    service = ReviewDecisionsService(db, actor="mcp")
    broker = ConfirmationBroker()
    now = datetime.now(UTC)
    grant = broker.consume(
        broker.issue(
            _identity_binding(requests, service.plan_identity(requests)), now=now
        ),
        now=now,
    )
    SecurityLinksRepo(db).insert(
        security_id=merge_setup["provisional"],
        ref_kind="tiingo_ticker",
        ref_value="SYNTH",
        source_type="tiingo",
        decided_by="user",
        actor="cli",
    )
    audits = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError) as exc:
        service.apply_identity(
            requests,
            verify=lambda current: grant.verify(_identity_binding(requests, current)),
        )
    assert exc.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audits
