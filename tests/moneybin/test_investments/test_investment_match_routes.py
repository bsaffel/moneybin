"""Existing refresh and reviews routes expose durable investment Proposals."""

import json
import re
from collections.abc import Generator
from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace

import duckdb
import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.orchestration.refresh import expand_steps, refresh
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def _seed(db: Database) -> None:
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "native")
    install_comparison_models(db)


def test_catalog_failure_is_a_real_planner_error(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)

    def fail(*args: object, **kwargs: object) -> None:
        raise duckdb.CatalogException("injected missing repository object")

    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        fail,
    )
    stage = refresh(comparison_db, steps=["investment_match"]).stages[0]
    assert stage.ran
    assert stage.error is not None


def test_cli_pending_text_displays_review_evidence(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)
    refresh(comparison_db, steps=["investment_match"])

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )
    result = CliRunner().invoke(app, ["investments", "matches", "pending"])
    assert result.exit_code == 0, result.output
    for field in (
        "quantity",
        "amount",
        "Evidence",
        "Downstream",
        "golden_membership_changed",
    ):
        assert field in result.output
    # seed_manual_event leaves raw.manual_investment_transactions.account_id
    # = 'account' unlinked (no accepted app.account_link_decisions row), so
    # int_manual__investment_identity's walk CTE terminates on that raw,
    # user-authored value — the real mechanism the task's account_id
    # classification is about, not a fixture standing in for it. The manual
    # leg's account_id must reach the CLI text table masked, never bare. The
    # word-boundary regex (not a bare substring check) excludes field-name
    # cells like "account_id"/"account_currency_code", which legitimately
    # contain "account" as a label, not a value.
    assert "****ount" in result.output
    assert re.search(r"\baccount\b", result.output) is None


@pytest.mark.parametrize("command", ["pending", "history"])
@pytest.mark.parametrize("quiet", [False, True], ids=["stdout-only", "quiet"])
def test_cli_review_stdout_preserves_proposal_and_choice_context(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    quiet: bool,
) -> None:
    from moneybin.privacy.payloads.reviews import (
        InvestmentMatchDetails,
        InvestmentMatchReviewRow,
        ReviewsInvestmentMatchesView,
        ReviewStatus,
    )

    view = ReviewsInvestmentMatchesView(
        status="pending" if command == "pending" else "history",
        rows=[
            InvestmentMatchReviewRow(
                decision_id="proposal-review-123",
                status="pending" if command == "pending" else "superseded",
                created_at="2026-01-12T00:00:00",
                summary="Investment Proposal",
                # model_validate (not the constructor) so pyright accepts the
                # plain nested dicts below — Pydantic validates and coerces
                # them into InvestmentLegRecord/InvestmentEvidenceRecord/
                # InvestmentFieldChoice at runtime regardless of which entry
                # point is used.
                details=InvestmentMatchDetails.model_validate({
                    "members": ["event-manual", "event-plaid"],
                    "confidence_band": "fuzzy",
                    "is_competing": False,
                    "auto_eligible": False,
                    "relationship_fingerprint": "relationship-123",
                    "candidate_graph_fingerprint": "graph-123",
                    "algorithm_version": "test-version",
                    "legs": [
                        {
                            "source_event_key": "event-manual",
                            "native_reference": "native-manual",
                            "observation_version": "obsver-manual",
                            "original_investment_transaction_id": "native-manual",
                            # Raw, user-authored manual account_id (the
                            # unlinked case) — must reach every surface
                            # masked, never bare.
                            "account_id": "raw_account_9999",
                            "security_id": "security-manual",
                            "source_group_reference": None,
                            "leg_role": "primary",
                            "source_type": "manual",
                            "source_origin": "import",
                            "account_identity_generation": "gen-a",
                            "security_identity_generation": "gen-s",
                            "type": "buy",
                            "subtype": None,
                            "event_type": "buy",
                            "description": "Manual context",
                            "trade_date_basis": "explicit",
                            "quantity": "1",
                            "price": "100",
                            "amount": "-100",
                            "fees": "0",
                            "source_currency_code": "USD",
                            "account_currency_code": "USD",
                            "currency_code": "USD",
                            "source_group_size": 1,
                            "supports_split": True,
                            "supports_native_relationships": False,
                            "supports_corrections": False,
                            "supports_reversals": False,
                            "is_unsupported_compound": False,
                            "has_source_security": True,
                            "has_resolved_identity": True,
                            "is_paired_reinvest": False,
                            "is_match_eligible": True,
                            "trade_date": "2026-01-10",
                            "settlement_date": None,
                            "original_acquisition_date": None,
                        }
                    ],
                    "evidence": [
                        {
                            "left_source_event_key": "event-manual",
                            "right_source_event_key": "event-plaid",
                            "left_native_reference": "native-manual",
                            "right_native_reference": "native-plaid",
                            "left_observation_version": "obsver-manual",
                            "right_observation_version": "obsver-plaid",
                            "left_source_type": "manual",
                            "right_source_type": "plaid",
                            "left_source_origin": "import",
                            "right_source_origin": "sync",
                            "left_account_identity_generation": "gen-a",
                            "right_account_identity_generation": "gen-a2",
                            "left_security_identity_generation": "gen-s",
                            "right_security_identity_generation": "gen-s2",
                            "event_type": "buy",
                            "leg_role": "primary",
                            "type": "buy",
                            "left_trade_date_basis": "explicit",
                            "right_trade_date_basis": "explicit",
                            "date_threshold_days": 5,
                            "date_distance_days": 1,
                            "structure_agrees": True,
                            "has_required_economics": True,
                            "quantity_within_tolerance": True,
                            "amount_within_tolerance": True,
                            "fees_within_tolerance": True,
                            "price_within_tolerance": True,
                            "date_within_tolerance": True,
                            "is_exact_economic_identity": False,
                            "subtype_conflict": False,
                            "trade_date_conflict": True,
                            "original_acquisition_date_conflict": False,
                            "has_validated_native_relationship": False,
                            "is_candidate": True,
                            "left_trade_date": "2026-01-10",
                            "right_trade_date": "2026-01-11",
                            "preferred_trade_date": "2026-01-11",
                            "left_original_acquisition_date": None,
                            "right_original_acquisition_date": None,
                        }
                    ],
                    "field_choices": [
                        {
                            "field": "trade_date",
                            "leg_role": "primary",
                            "conflict_id": "conflict-trade-date-123",
                            "choices": [
                                {
                                    "choice_id": "choice-manual",
                                    "value": "2026-01-10",
                                    "source_type": "manual",
                                    "source_origin": "import",
                                    "native_reference": "native-manual",
                                    "observation_version": "obsver-manual",
                                },
                                {
                                    "choice_id": "choice-plaid",
                                    "value": "2026-01-11",
                                    "source_type": "plaid",
                                    "source_origin": "sync",
                                    "native_reference": "native-plaid",
                                    "observation_version": "obsver-plaid",
                                },
                            ],
                        }
                    ],
                    "supersedes_decision_ids": [],
                    "supersession": [],
                    "downstream_effects": {"golden_membership_changed": False},
                }),
            )
        ],
    )

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )

    def review_view(db: Database, status: ReviewStatus) -> ReviewsInvestmentMatchesView:
        return view

    monkeypatch.setattr(
        "moneybin.adapters.investment_matching_adapters.investment_review_view",
        review_view,
    )
    result = CliRunner().invoke(
        app, ["investments", "matches", command, *(["--quiet"] if quiet else [])]
    )
    assert result.exit_code == 0, result.output
    for value in (
        "proposal-review-123",
        "fuzzy",
        "conflict-trade-date-123",
        "trade_date",
        "choice-manual",
        "2026-01-10",
        "choice-plaid",
        "2026-01-11",
        "quantity",
        "trade_date_conflict",
        "golden_membership_changed",
    ):
        assert value in result.stdout
    # The leg's raw, user-authored account_id must reach the text table
    # masked, never bare — the CLI text path applies no redaction by design
    # (render_or_json's docstring), so the command itself must mask it.
    assert "****9999" in result.stdout
    assert "raw_account_9999" not in result.stdout


def test_bounded_refresh_persists_proposals_without_transform(
    comparison_db: Database,
) -> None:
    _seed(comparison_db)
    result = refresh(comparison_db, steps=["investment_match"])
    assert not result.applied
    assert [stage.step for stage in result.stages] == ["investment_match"]
    assert result.stages[0].ran
    assert result.stages[0].counts == {
        "pending_unique": 1,
        "pending_competing": 0,
        "stale": 0,
        "suppressed": 0,
    }
    assert comparison_db.execute(
        "SELECT status FROM app.investment_match_decisions"
    ).fetchall() == [("pending",)]


def test_transform_selector_transitively_plans(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)

    def apply_transform(_service: object) -> SimpleNamespace:
        return SimpleNamespace(applied=True, error=None, duration_seconds=0)

    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService.apply", apply_transform
    )
    assert "investment_match" in expand_steps(["transform"])
    result = refresh(comparison_db, steps=["transform"])
    assert [stage.step for stage in result.stages][:2] == [
        "investment_match",
        "transform",
    ]
    assert comparison_db.execute(
        "SELECT COUNT(*) FROM app.investment_match_decisions"
    ).fetchone() == (1,)


def test_cli_run_uses_same_bounded_planner(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(comparison_db)

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )
    result = CliRunner().invoke(
        app, ["investments", "matches", "run", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["stages"][0]["step"] == "investment_match"
    assert comparison_db.execute(
        "SELECT COUNT(*) FROM app.investment_match_decisions"
    ).fetchone() == (1,)


@pytest.mark.asyncio
async def test_reviews_kind_reads_persisted_evidence(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.mcp.tools.reviews import reviews_coarse

    _seed(comparison_db)
    refresh(comparison_db, steps=["investment_match"])

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr("moneybin.mcp.tools.reviews.get_database", database_context)
    response = await reviews_coarse(kind="investment_matches")
    assert response.data is not None
    payload = response.data.model_dump()
    assert payload["kind"] == "investment_matches"
    assert len(payload["rows"]) == 1
    legs = payload["rows"][0]["details"]["legs"]
    assert legs
    assert payload["rows"][0]["details"]["evidence"]

    manual_leg = next(leg for leg in legs if leg["source_type"] == "manual")
    plaid_leg = next(leg for leg in legs if leg["source_type"] == "plaid")
    # The `comparison_db` fixture accepts a plaid account_link onto
    # account_id='account' and seeds core.dim_accounts with that same id, so
    # the manual leg's unresolved raw account_id (carried through unchanged —
    # no app.account_link_decisions row exists to walk) and the plaid leg's
    # resolved canonical id happen to collide on the literal 'account'. Both
    # are ACCOUNT_IDENTIFIER regardless of which case produced them (the
    # field is classified by worst case, not per row), so both must arrive
    # masked here — reviews_coarse's dynamic_classification=True still
    # redacts via build_classified_envelope.
    assert manual_leg["account_id"] == "****ount"
    assert plaid_leg["account_id"] == "****ount"
    # P2 regression: repo storage stringifies Decimal via
    # json.dumps(default=str); the typed leg model must coerce amounts back
    # to real Decimal rather than publish quoted strings.
    assert manual_leg["amount"] == Decimal("-100")
    assert isinstance(manual_leg["amount"], Decimal)


def test_pending_proposals_advertise_the_existing_reviews_route(
    comparison_db: Database,
) -> None:
    from moneybin.adapters.refresh_adapters import refresh_envelope

    _seed(comparison_db)
    result = refresh(comparison_db, steps=["investment_match"])
    envelope = refresh_envelope(result, requested=frozenset({"investment_match"}))
    assert any(
        'reviews(kind="investment_matches")' in action for action in envelope.actions
    )
