"""Real comparison, human review, and exact Golden eligibility lifecycles."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.investments.event_projection import read_rows
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.investment_match_field_resolutions_repo import (
    InvestmentMatchFieldResolutionsRepo,
)
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
)
from tests.moneybin.test_investments.test_investment_golden_projection import (
    install_golden_models,
    ledger,
)


@dataclass(frozen=True)
class EventCase:
    """Hand-authored source taxonomy and exact economic inputs."""

    type: str
    provider_type: str
    provider_subtype: str
    days: int
    amount: str
    quantity: str | None = None
    subtype: str | None = None

    @property
    def role(self) -> str:
        if self.type in {"buy", "reinvest", "transfer_in"}:
            return "acquisition"
        if self.type in {"sell", "transfer_out"}:
            return "disposal"
        if self.type in {"dividend", "interest", "capital_gain_distribution"}:
            return "income"
        return "basis_reduction" if self.type == "return_of_capital" else "cash"

    @property
    def security(self) -> str | None:
        return None if self.type in {"deposit", "withdrawal"} else "security"

    @property
    def price(self) -> str | None:
        return "100" if self.quantity is not None else None


# Hand-authored contract: signed ledger cash and units, never observed output.
SIMPLE_EVENTS = (
    EventCase("buy", "buy", "buy", 5, "-100", "1"),
    EventCase("sell", "sell", "sell", 5, "100", "-1"),
    EventCase("dividend", "cash", "dividend", 3, "100"),
    EventCase("interest", "cash", "interest", 3, "100"),
    EventCase(
        "capital_gain_distribution",
        "cash",
        "long-term capital gain",
        3,
        "100",
        subtype="long_term",
    ),
    EventCase("return_of_capital", "cash", "return of principal", 3, "100"),
    EventCase("fee", "fee", "account fee", 3, "-100"),
    EventCase("deposit", "cash", "deposit", 7, "100"),
    EventCase("withdrawal", "cash", "withdrawal", 7, "-100"),
    EventCase("transfer_in", "transfer", "transfer", 7, "-100", "1"),
    EventCase("transfer_out", "transfer", "transfer", 7, "100", "-1"),
)


def _manual(db: Database, case: EventCase) -> None:
    if case.security is not None:
        seed_manual_event(
            db,
            "manual",
            type_=case.type,
            subtype=case.subtype,
            quantity=case.quantity,
            price=case.price,
            amount=case.amount,
        )
        return
    # Cash movement inputs must have no Security; the shared factory requires one.
    db.execute(
        """INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, type, trade_date,
            amount, fees, description, created_by, investment_transaction_id
        ) VALUES ('manual', 'import', 'account', ?, DATE '2026-01-10',
                  ?, 0, 'Manual context', 'cli', 'manual')""",
        [case.type, case.amount],
    )


def _plaid(db: Database, native: str, case: EventCase, *, day: int = 10) -> None:
    row = SyncInvestmentTransaction(
        investment_transaction_id=native,
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security" if case.security is not None else None,
        date=date(2026, 1, day),
        transaction_datetime=datetime(2026, 1, day, tzinfo=UTC),
        name="Aggregator context",
        quantity=Decimal(case.quantity) if case.quantity is not None else None,
        price=Decimal(case.price) if case.price is not None else None,
        amount=-Decimal(case.amount),
        fees=Decimal("0"),
        type=case.provider_type,
        subtype=case.provider_subtype,
    )
    at = datetime(2026, 2, 1, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # real synthetic revision ingestion
        [row], f"sync_{native}", at, at
    )


def _start(db: Database) -> InvestmentMembershipService:
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    install_golden_models(db)
    return membership


def _revisions(db: Database) -> set[tuple[Any, ...]]:
    # Capture comparison inputs before planning, independently of accepted members.
    return set(
        db.execute("""SELECT source_type, source_origin, native_reference,
        observation_version FROM prep.int_investment_events__legs""").fetchall()
    )


def _raw(db: Database) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        read_rows(
            db,
            """SELECT * FROM raw.manual_investment_transactions
            ORDER BY source_transaction_id""",
        ),
        read_rows(
            db,
            """SELECT * FROM raw.plaid_investment_transactions
            ORDER BY investment_transaction_id, observation_version""",
        ),
    )


def _accept_manual_dates(
    db: Database,
    matching: InvestmentMatchingService,
    proposal: dict[str, Any],
    *,
    roles: set[str],
) -> None:
    conflicts = proposal["field_choices"]
    assert {(c["leg_role"], c["field"]) for c in conflicts} == {
        (role, "trade_date") for role in roles
    }
    selected = [
        {
            "conflict_id": conflict["conflict_id"],
            "choice_id": next(
                c["choice_id"]
                for c in conflict["choices"]
                if c["source_type"] == "manual"
            ),
        }
        for conflict in conflicts
    ]
    with operation():
        matching.decide(
            proposal["proposal_id"],
            decision="accept",
            actor="test",
            field_choices=selected,
        )
    expected = {
        (
            "trade_date",
            c["source_type"],
            c["source_origin"],
            c["native_reference"],
            c["observation_version"],
        )
        for conflict in conflicts
        for c in conflict["choices"]
        if c["source_type"] == "manual"
    }
    assert {
        (
            r["field_name"],
            r["source_type"],
            r["source_origin"],
            r["native_reference"],
            r["observation_version"],
        )
        for r in InvestmentMatchFieldResolutionsRepo(db).active_resolutions()
    } == expected


def _assert_members(
    db: Database,
    revisions: set[tuple[Any, ...]],
    proposal_id: str,
    *,
    security: str | None,
) -> None:
    members = InvestmentEventMembersRepo(db).active_members()
    assert len(members) == len(revisions)
    assert {
        (
            m["source_type"],
            m["source_origin"],
            m["native_reference"],
            m["observation_version"],
        )
        for m in members
    } == revisions
    assert {m["decision_id"] for m in members} == {proposal_id}
    assert {m["bound_account_id"] for m in members} == {"account"}
    assert {m["bound_security_id"] for m in members} == {security}
    assert {m["bound_effective_currency"] for m in members} == {"USD"}


def _assert_stable(
    db: Database,
    membership: InvestmentMembershipService,
    matching: InvestmentMatchingService,
) -> None:
    rows = ledger(db)
    members = InvestmentEventMembersRepo(db).history()
    resolutions = InvestmentMatchFieldResolutionsRepo(db).active_resolutions()
    decisions = matching.history()
    raw = _raw(db)
    membership.reconcile(actor="test")
    matching.run(actor="test")
    assert ledger(db) == rows
    assert InvestmentEventMembersRepo(db).history() == members
    assert InvestmentMatchFieldResolutionsRepo(db).active_resolutions() == resolutions
    assert matching.history() == decisions
    assert matching.pending() == []
    assert _raw(db) == raw


def _economics(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: row[key]
        for key in (
            "account_id",
            "security_id",
            "type",
            "subtype",
            "currency_code",
            "quantity",
            "price",
            "amount",
            "fees",
            "trade_date",
            "settlement_date",
            "original_acquisition_date",
            "source_type",
            "source_origin",
            "provider_type",
            "provider_subtype",
            "description",
        )
    }


def _expected(case: EventCase, *, day: int, trade_day: int = 10) -> dict[str, Any]:
    return {
        "account_id": "account",
        "security_id": case.security,
        "type": case.type,
        "subtype": case.subtype,
        "currency_code": "USD",
        "quantity": Decimal(case.quantity) if case.quantity is not None else None,
        "price": Decimal(case.price) if case.price is not None else None,
        "amount": Decimal(case.amount),
        "fees": Decimal("0"),
        "trade_date": date(2026, 1, trade_day),
        "settlement_date": date(2026, 1, day),
        "original_acquisition_date": None,
        "source_type": "plaid",
        "source_origin": "origin",
        "provider_type": case.provider_type,
        "provider_subtype": case.provider_subtype,
        "description": "Aggregator context",
    }


def _expected_manual(case: EventCase) -> dict[str, Any]:
    return {
        **_expected(case, day=10),
        "source_type": "manual",
        "source_origin": "user",
        "provider_type": None,
        "provider_subtype": None,
        "description": "Manual context",
        "settlement_date": None,
    }


@pytest.mark.parametrize("case", SIMPLE_EVENTS, ids=lambda c: c.type)
@pytest.mark.parametrize("position", ["same", "inside", "boundary", "outside"])
def test_simple_eligibility_reaches_exact_reviewed_golden(
    comparison_db: Database, case: EventCase, position: str
) -> None:
    db = comparison_db
    offset = {
        "same": 0,
        "inside": case.days - 1,
        "boundary": case.days,
        "outside": case.days + 1,
    }[position]
    _manual(db, case)
    membership = _start(db)
    oldest = ledger(db)[0]
    _plaid(db, "plaid", case, day=10 + offset)
    membership.reconcile(actor="test")
    before = ledger(db)
    assert len(before) == 2
    assert {r["source_type"]: _economics(r) for r in before} == {
        "manual": _expected_manual(case),
        "plaid": _expected(case, day=10 + offset, trade_day=10 + offset),
    }
    assert len({r["event_group_id"] for r in before}) == 2
    revisions, raw = _revisions(db), _raw(db)
    matching = InvestmentMatchingService(db)
    matching.run(actor="test")
    assert ledger(db) == before  # Planning never accepts, even if auto_eligible.
    if position == "outside":
        assert matching.pending() == []
        assert matching.history() == []
        _assert_stable(db, membership, matching)
        return
    (proposal,) = matching.pending()
    assert proposal["status"] == "pending"
    assert proposal["confidence_band"] == ("exact" if offset == 0 else "fuzzy")
    assert proposal["auto_eligible"] is (offset == 0)
    assert proposal["is_competing"] is False
    roles: set[str] = {case.role} if offset else set()
    _accept_manual_dates(db, matching, proposal, roles=roles)
    (row,) = ledger(db)
    assert _economics(row) == _expected(case, day=10 + offset)
    assert (row["event_group_id"], row["investment_transaction_id"]) == (
        oldest["event_group_id"],
        oldest["investment_transaction_id"],
    )
    _assert_members(db, revisions, proposal["proposal_id"], security=case.security)
    assert _raw(db) == raw
    assert matching.history()[0]["status"] == "accepted"
    _assert_stable(db, membership, matching)


REINVEST_EVENTS = (
    ("dividend", "dividend", "dividend reinvestment", "dividend", None),
    ("interest", "interest", "interest reinvestment", "interest", None),
    (
        "capital_gain",
        "capital_gain_distribution",
        "long-term capital gain reinvestment",
        "long-term capital gain",
        "long_term",
    ),
)


def _manual_reinvest(db: Database, funding: str, income: str) -> None:
    # Retained supported manual two-leg input; the real adapter validates its group.
    seed_manual_event(
        db, "manual_acquisition", type_="reinvest", subtype=funding, group="group"
    )
    seed_manual_event(
        db,
        "manual_income",
        type_=income,
        group="group",
        quantity=None,
        price=None,
        amount="100",
    )


@pytest.mark.parametrize(
    ("funding", "income", "provider_acquisition", "provider_income", "income_subtype"),
    REINVEST_EVENTS,
    ids=["dividend", "interest", "capital-gain"],
)
@pytest.mark.parametrize("offset", [4, 5, 6])
def test_paired_reinvest_cross_source_boundary_is_atomic_through_golden(
    comparison_db: Database,
    funding: str,
    income: str,
    provider_acquisition: str,
    provider_income: str,
    income_subtype: str | None,
    offset: int,
) -> None:
    db = comparison_db
    _manual_reinvest(db, funding, income)
    membership = _start(db)
    oldest = {
        r["type"]: (r["event_group_id"], r["investment_transaction_id"])
        for r in ledger(db)
    }
    acquisition = EventCase(
        "reinvest", "buy", provider_acquisition, 5, "-100", "1", funding
    )
    distribution = EventCase(
        income, "cash", provider_income, 5, "100", subtype=income_subtype
    )
    _plaid(db, "plaid_acquisition", acquisition, day=10 + offset)
    _plaid(db, "plaid_income", distribution, day=10 + offset)
    membership.reconcile(actor="test")
    before = ledger(db)
    assert len(before) == 4
    assert {(r["source_type"], r["type"]): _economics(r) for r in before} == {
        ("manual", "reinvest"): _expected_manual(acquisition),
        ("manual", income): {**_expected_manual(distribution), "subtype": None},
        ("plaid", "reinvest"): _expected(
            acquisition, day=10 + offset, trade_day=10 + offset
        ),
        ("plaid", income): _expected(
            distribution, day=10 + offset, trade_day=10 + offset
        ),
    }
    assert len({r["event_group_id"] for r in before}) == 2
    revisions, raw = _revisions(db), _raw(db)
    matching = InvestmentMatchingService(db)
    matching.run(actor="test")
    assert ledger(db) == before
    if offset == 6:
        assert matching.pending() == []
        assert matching.history() == []
        _assert_stable(db, membership, matching)
        return
    (proposal,) = matching.pending()
    assert proposal["confidence_band"] == "fuzzy"
    assert proposal["auto_eligible"] is False
    assert proposal["is_competing"] is False
    assert len(proposal["members"]) == 2
    assert len(proposal["legs"]) == 4
    _accept_manual_dates(db, matching, proposal, roles={"acquisition", "income"})
    rows = ledger(db)
    assert len(rows) == 2
    assert {r["type"] for r in rows} == {"reinvest", income}
    assert len({r["event_group_id"] for r in rows}) == 1
    for row in rows:
        case = acquisition if row["type"] == "reinvest" else distribution
        assert _economics(row) == _expected(case, day=10 + offset)
        assert (row["event_group_id"], row["investment_transaction_id"]) == oldest[
            row["type"]
        ]
    _assert_members(db, revisions, proposal["proposal_id"], security="security")
    assert _raw(db) == raw
    _assert_stable(db, membership, matching)


@pytest.mark.parametrize(
    "case", ["missing", "ambiguous", "four_days", "mistyped", "cash"]
)
def test_unpaired_plaid_reinvest_never_partially_consolidates_manual_event(
    comparison_db: Database, case: str
) -> None:
    db = comparison_db
    _manual_reinvest(db, "dividend", "dividend")
    acquisition = EventCase(
        "reinvest", "buy", "dividend reinvestment", 5, "-100", "1", "dividend"
    )
    _plaid(db, "plaid_acquisition", acquisition)
    if case != "missing":
        income_type = "interest" if case == "mistyped" else "dividend"
        distribution = EventCase(
            income_type, "cash", income_type, 3, "90" if case == "cash" else "100"
        )
        _plaid(db, "plaid_income", distribution, day=14 if case == "four_days" else 10)
        if case == "ambiguous":
            _plaid(db, "plaid_second_income", distribution)
    membership = _start(db)
    before, raw = ledger(db), _raw(db)
    assert len(before) == {"missing": 3, "ambiguous": 5}.get(case, 4)
    assert len({r["event_group_id"] for r in before}) == len(before) - 1
    assert sum(r["type"] == "reinvest" for r in before) == 2
    for row in before:
        if row["source_type"] == "manual":
            expected = _expected_manual(
                acquisition
                if row["type"] == "reinvest"
                else EventCase("dividend", "cash", "dividend", 3, "100")
            )
        else:
            income_type = "interest" if case == "mistyped" else "dividend"
            source_case = (
                acquisition
                if row["type"] == "reinvest"
                else EventCase(
                    income_type,
                    "cash",
                    income_type,
                    3,
                    "90" if case == "cash" else "100",
                )
            )
            day = 14 if case == "four_days" and row["type"] != "reinvest" else 10
            expected = _expected(source_case, day=day, trade_day=day)
        assert _economics(row) == expected
    matching = InvestmentMatchingService(db)
    matching.run(actor="test")
    assert matching.pending() == []
    assert matching.history() == []
    assert ledger(db) == before
    assert {
        m["decision_id"] for m in InvestmentEventMembersRepo(db).active_members()
    } == {None}
    assert _raw(db) == raw
    _assert_stable(db, membership, matching)


@pytest.mark.parametrize("type_", ["other", "reinvest"])
def test_ineligible_singletons_keep_both_golden_events(
    comparison_db: Database, type_: str
) -> None:
    db = comparison_db
    is_reinvest = type_ == "reinvest"
    seed_manual_event(
        db,
        "manual",
        type_=type_,
        subtype="dividend" if is_reinvest else None,
        quantity="1" if is_reinvest else None,
        price="100" if is_reinvest else None,
    )
    case = EventCase(
        type_,
        "buy" if is_reinvest else "cash",
        "dividend reinvestment" if is_reinvest else "adjustment",
        5,
        "-100",
        "1" if is_reinvest else None,
        "dividend" if is_reinvest else None,
    )
    _plaid(db, "plaid", case)
    membership = _start(db)
    before = ledger(db)
    assert len(before) == 2
    assert {r["type"] for r in before} == {type_}
    assert {r["amount"] for r in before} == {Decimal("-100")}
    assert len({r["event_group_id"] for r in before}) == 2
    matching = InvestmentMatchingService(db)
    matching.run(actor="test")
    assert matching.pending() == []
    assert ledger(db) == before
    _assert_stable(db, membership, matching)


def test_indistinguishable_alternatives_stay_pending_without_ledger_mutation(
    comparison_db: Database,
) -> None:
    db = comparison_db
    case = SIMPLE_EVENTS[0]
    _manual(db, case)
    _plaid(db, "plaid_a", case)
    _plaid(db, "plaid_b", case)
    membership = _start(db)
    before, members, raw = (
        ledger(db),
        InvestmentEventMembersRepo(db).history(),
        _raw(db),
    )
    assert len(before) == 3
    assert len({r["event_group_id"] for r in before}) == 3
    for row in before:
        expected = (
            _expected_manual(case)
            if row["source_type"] == "manual"
            else _expected(case, day=10)
        )
        assert _economics(row) == expected
    matching = InvestmentMatchingService(db)
    matching.run(actor="test")
    proposals = matching.pending()
    assert len(proposals) == 2
    assert all(p["is_competing"] and not p["auto_eligible"] for p in proposals)
    assert all(
        p["status"] == "pending" and p["confidence_band"] == "exact" for p in proposals
    )
    assert {
        frozenset(leg["native_reference"] for leg in p["legs"]) for p in proposals
    } == {frozenset({"manual", "plaid_a"}), frozenset({"manual", "plaid_b"})}
    membership.reconcile(actor="test")
    matching.run(actor="test")
    assert matching.pending() == proposals
    assert matching.history() == []
    assert ledger(db) == before
    assert InvestmentEventMembersRepo(db).history() == members
    assert _raw(db) == raw
