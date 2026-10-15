"""Interactive limits select complete groups before hydration and enrichment."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from typing import Any, cast
from unittest.mock import patch

import duckdb
import pytest

from moneybin.cli.commands.interactive_review import (
    _current_item,  # pyright: ignore[reportPrivateUsage]  # exact live revalidation regression
    load_items,
)
from moneybin.database import Database
from moneybin.matching.persistence import get_match_decision
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.merchant_link_decisions_repo import MerchantLinkDecisionsRepo
from moneybin.repositories.security_link_decisions_repo import SecurityLinkDecisionsRepo
from tests.moneybin.db_helpers import create_core_tables


class _MeasuredRows:
    def __init__(self, result: duckdb.DuckDBPyConnection, sizes: list[int]) -> None:
        self.result = result
        self.sizes = sizes

    def fetchall(self) -> list[Any]:
        rows = self.result.fetchall()
        columns = [column[0] for column in self.result.description]
        if columns and columns[0] in {"decision_id", "match_id"}:
            self.sizes.append(len(rows))
        return rows

    def __getattr__(self, name: str) -> Any:
        return getattr(self.result, name)


def _measure(db: Database, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    execute = db.execute
    sizes: list[int] = []

    def measured_execute(
        query: str, params: list[Any] | Mapping[str, Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        return cast(
            duckdb.DuckDBPyConnection, _MeasuredRows(execute(query, params), sizes)
        )

    monkeypatch.setattr(db, "execute", measured_execute)
    return sizes


def _identity(db: Database, queue: str, group: str, decision: str) -> None:
    if queue == "account-links":
        AccountLinkDecisionsRepo(db).insert(
            decision_id=decision,
            provisional_account_id=group,
            candidate_account_id=decision,
            confidence_score=0.8,
            match_signals={"signal": "institution_last4", "value": "synthetic"},
            decided_by="auto",
            actor="test",
        )
    elif queue == "merchant-links":
        MerchantLinkDecisionsRepo(db).insert(
            decision_id=decision,
            source_type="csv",
            ref_kind="merchant_entity_id",
            ref_value=group,
            candidate_merchant_id=decision,
            confidence_score=0.8,
            match_signals={},
            decided_by="auto",
            actor="test",
        )
    else:
        SecurityLinkDecisionsRepo(db).insert(
            decision_id=decision,
            source_type="csv",
            ref_kind="tiingo_ticker",
            ref_value=group,
            candidate_security_id=decision,
            actor="test",
        )


@pytest.mark.parametrize("queue", ["account-links", "merchant-links", "security-links"])
def test_identity_limit_and_exact_revalidation_hydrate_only_complete_selected_group(
    db: Database, monkeypatch: pytest.MonkeyPatch, queue: str
) -> None:
    create_core_tables(db)
    for index in range(20):
        for candidate in range(2):
            _identity(
                db, queue, f"group:{index:02}", f"decision-{index:02}-{candidate}"
            )
    sizes = _measure(db, monkeypatch)
    with patch(
        "moneybin.cli.commands.interactive_review.get_database",
        return_value=nullcontext(db),
    ):
        selected = load_items(queue, 1)
        assert len(selected) == 1
        item = selected[0]
        assert {candidate.decision_id for candidate in item.candidates} == {
            "decision-00-0",
            "decision-00-1",
        }
        assert sizes and max(sizes) <= 2
        sizes.clear()
        assert _current_item(item) == item
        assert sizes and max(sizes) <= 2
        _identity(db, queue, "group:00", "new-sibling")
        sizes.clear()
        current = _current_item(item)
        assert current is not None and current != item
        assert {candidate.decision_id for candidate in current.candidates} == {
            "decision-00-0",
            "decision-00-1",
            "new-sibling",
        }
        assert sizes and max(sizes) <= 3


def _match(db: Database, index: int) -> None:
    MatchDecisionsRepo(db).insert(
        match_id=f"match-{index:02}",
        source_transaction_id_a=f"a-{index}",
        source_type_a="csv",
        source_origin_a="synthetic",
        source_transaction_id_b=f"b-{index}",
        source_type_b="ofx",
        source_origin_b="synthetic",
        account_id="account",
        confidence_score=0.8,
        match_signals={},
        match_status="pending",
        decided_by="auto",
        actor="test",
        match_type="transfer",
    )
    db.execute(
        """INSERT INTO prep.int_transactions__unioned VALUES
        ('csv', ?, 'account', DATE '2026-10-01', -5, 'USD', 'first observation'),
        ('ofx', ?, 'account', DATE '2026-10-01', 5, 'USD', 'second observation')""",
        [f"a-{index}", f"b-{index}"],
    )


def test_match_exact_revalidation_hydrates_one_pair_and_retains_source_drift(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_core_tables(db)
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute("""CREATE TABLE prep.int_transactions__unioned (
        source_type VARCHAR, source_transaction_id VARCHAR, account_id VARCHAR,
        transaction_date DATE, amount DECIMAL(18,2), currency_code VARCHAR,
        description VARCHAR)""")
    for index in range(20):
        _match(db, index)
    sizes = _measure(db, monkeypatch)
    observations: list[str] = []
    from moneybin.services.matching_service import MatchingService

    observation = MatchingService.get_source_observation

    def measured_observation(
        self: MatchingService, **keys: str
    ) -> dict[str, Any] | None:
        observations.append(keys["source_transaction_id"])
        return observation(self, **keys)

    monkeypatch.setattr(MatchingService, "get_source_observation", measured_observation)
    with patch(
        "moneybin.cli.commands.interactive_review.get_database",
        return_value=nullcontext(db),
    ):
        item = load_items("matches", 1)[0]
        sizes.clear()
        observations.clear()
        assert _current_item(item) == item
        assert sizes == [1]
        assert observations == ["a-0", "b-0"]
        db.execute("""UPDATE prep.int_transactions__unioned SET description = 'changed'
                      WHERE source_transaction_id = 'a-0'""")
        assert _current_item(item) != item


@pytest.mark.parametrize("queue", ["account-links", "merchant-links", "security-links"])
def test_bounded_identity_preserves_legacy_order_and_anchor_guards(
    db: Database, queue: str
) -> None:
    from moneybin.services.account_links_service import AccountLinksService
    from moneybin.services.merchant_links_service import MerchantLinksService
    from moneybin.services.security_links_service import SecurityLinksService

    create_core_tables(db)
    for group in ("z:group", "a:group", "a0"):
        _identity(db, queue, group, f"{group}-decision")
    if queue == "account-links":
        service = AccountLinksService(db)
        legacy = service.pending()
        expected = sorted(legacy, key=lambda group: group.provisional_account_id)
        assert service.pending(limit=2) == expected[:2]
        repo = AccountLinkDecisionsRepo(db)
    elif queue == "merchant-links":
        service = MerchantLinksService(db)
        # Source and ref are separate fields even when either contains colons.
        repo = MerchantLinkDecisionsRepo(db)
        repo.insert(
            decision_id="colon-source",
            source_type="a:csv",
            ref_kind="merchant_entity_id",
            ref_value="z:ref",
            candidate_merchant_id="candidate",
            confidence_score=0.8,
            match_signals={},
            decided_by="auto",
            actor="test",
        )
        legacy = service.pending()
        expected = sorted(
            legacy, key=lambda group: f"{group.source_type}:{group.ref_value}"
        )
        actual = sorted(
            service.pending(limit=2),
            key=lambda group: f"{group.source_type}:{group.ref_value}",
        )
        assert actual == expected[:2]
    else:
        service = SecurityLinksService(db)
        repo = SecurityLinkDecisionsRepo(db)
        repo.insert(
            decision_id="000-other-source",
            source_type="a:csv",
            ref_kind="tiingo_ticker",
            ref_value="z:group",
            candidate_security_id="other",
            actor="test",
        )
        legacy = service.pending()
        expected = sorted(
            legacy,
            key=lambda group: f"{group.source_type}:{group.ref_kind}:{group.ref_value}",
        )
        actual = sorted(
            service.pending(limit=2),
            key=lambda group: f"{group.source_type}:{group.ref_kind}:{group.ref_value}",
        )
        assert actual == expected[:2]
        # Existing security groups span sources; preserve every sibling.
        exact = service.pending(limit=1, decision_id="z:group-decision")
        assert exact == [expected[0]]
        assert len(exact[0].candidates) == 2
    assert service.pending(limit=0) == []
    assert service.pending(limit=1, decision_id="missing") == []
    if isinstance(repo, AccountLinkDecisionsRepo):
        repo.update_status(
            "a:group-decision",
            status="rejected",
            decided_by="user",
            actor="test",
            provisional_display_name="Synthetic provisional",
            candidate_display_name="Synthetic candidate",
        )
    else:
        repo.update_status(
            "a:group-decision", status="rejected", decided_by="user", actor="test"
        )
    assert service.pending(limit=1, decision_id="a:group-decision") == []


@pytest.mark.parametrize("change", ["description", "remove", "rekey"])
def test_match_revalidates_source_after_acquiring_writable_connection(
    db: Database, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from moneybin.cli.commands.interactive_review import apply_decision
    from moneybin.cli.utils import get_terminal_policy
    from moneybin.errors import UserError
    from moneybin.services.matching_service import MatchDecisionOutcome, MatchingService

    create_core_tables(db)
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute("""CREATE TABLE prep.int_transactions__unioned (
        source_type VARCHAR, source_transaction_id VARCHAR, account_id VARCHAR,
        transaction_date DATE, amount DECIMAL(18,2), currency_code VARCHAR,
        description VARCHAR)""")
    _match(db, 0)
    writable_opened = False

    def open_database(*, read_only: bool) -> Any:
        nonlocal writable_opened
        if not read_only:
            writable_opened = True
            # Simulate a refresh completing between the final read and writer acquisition.
            if change == "description":
                db.execute("""UPDATE prep.int_transactions__unioned SET description = 'Changed source'
                              WHERE source_transaction_id = 'a-0'""")
            elif change == "rekey":
                db.execute("""UPDATE prep.int_transactions__unioned SET source_transaction_id = 'new-key'
                              WHERE source_transaction_id = 'a-0'""")
            else:
                db.execute("""DELETE FROM prep.int_transactions__unioned
                              WHERE source_transaction_id = 'a-0'""")
        return nullcontext(db)

    monkeypatch.setattr(
        "moneybin.cli.commands.interactive_review.get_database", open_database
    )
    with patch.object(
        MatchingService, "set_status", return_value=MatchDecisionOutcome("accepted", 0)
    ) as mutation:
        item = load_items("matches", 1)[0]
        if change == "description":
            assert apply_decision(item, "accept", get_terminal_policy()) is False
        else:
            with pytest.raises(UserError, match="source evidence is unavailable"):
                apply_decision(item, "accept", get_terminal_policy())
        assert writable_opened
        mutation.assert_not_called()
    decision = get_match_decision(db, "match-00")
    assert decision is not None and decision["match_status"] == "pending"


def test_unchanged_match_evidence_commits_once_on_existing_connection(
    db: Database,
) -> None:
    from moneybin.cli.commands.interactive_review import apply_decision
    from moneybin.cli.utils import get_terminal_policy

    create_core_tables(db)
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute("""CREATE TABLE prep.int_transactions__unioned (
        source_type VARCHAR, source_transaction_id VARCHAR, account_id VARCHAR,
        transaction_date DATE, amount DECIMAL(18,2), currency_code VARCHAR,
        description VARCHAR)""")
    _match(db, 0)
    with (
        patch(
            "moneybin.cli.commands.interactive_review.get_database",
            return_value=nullcontext(db),
        ) as opens,
        patch.object(db, "commit", wraps=db.commit) as commit,
    ):
        item = load_items("matches", 1)[0]
        assert apply_decision(item, "reject", get_terminal_policy())
        commit.assert_called_once()
        assert [call.kwargs["read_only"] for call in opens.call_args_list] == [
            True,
            True,
            False,
        ]
    decision = get_match_decision(db, "match-00")
    assert decision is not None and decision["match_status"] == "rejected"


@pytest.mark.parametrize("queue", ["account-links", "merchant-links", "security-links"])
def test_identity_revalidates_complete_group_after_writer_acquisition(
    db: Database, monkeypatch: pytest.MonkeyPatch, queue: str
) -> None:
    from moneybin.cli.commands.interactive_review import apply_decision
    from moneybin.cli.utils import get_terminal_policy
    from moneybin.services.account_links_service import AccountLinksService
    from moneybin.services.merchant_links_service import MerchantLinksService
    from moneybin.services.security_links_service import SecurityLinksService

    create_core_tables(db)
    _identity(db, queue, "provider:group", "original")

    def open_database(*, read_only: bool) -> Any:
        if not read_only:
            _identity(db, queue, "provider:group", "new-sibling")
        return nullcontext(db)

    monkeypatch.setattr(
        "moneybin.cli.commands.interactive_review.get_database", open_database
    )
    with (
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="original",
        ),
        patch.object(AccountLinksService, "set", return_value=None) as accounts,
        patch.object(MerchantLinksService, "set") as merchants,
        patch.object(SecurityLinksService, "reject_merge") as securities,
    ):
        item = load_items(queue, 1)[0]
        assert apply_decision(item, "reject", get_terminal_policy()) is False
        accounts.assert_not_called()
        merchants.assert_not_called()
        securities.assert_not_called()
    repo = (
        AccountLinkDecisionsRepo(db)
        if queue == "account-links"
        else MerchantLinkDecisionsRepo(db)
        if queue == "merchant-links"
        else SecurityLinkDecisionsRepo(db)
    )
    original = repo.fetch_by_id("original")
    sibling = repo.fetch_by_id("new-sibling")
    assert original is not None and original["status"] == "pending"
    assert sibling is not None and sibling["status"] == "pending"
