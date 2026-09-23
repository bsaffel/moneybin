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
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
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
@pytest.mark.parametrize("width", [40, 80])
def test_cli_review_stdout_preserves_proposal_and_choice_context(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    quiet: bool,
    width: int,
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
                decision_id="proposal-review-1234567890",
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
                            "source_event_key": "event-manual-1234567890",
                            "native_reference": "native-manual-1234567890",
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
                            "amount": "-123456789012345.6789012345",
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
                    "supersedes_decision_ids": ["proposal-prior-122"],
                    # Both are populated so the two render branches they gate
                    # actually execute. `alternatives` reaches the ` / ` join
                    # and `supersession` reaches `prior.model_dump()`; left at
                    # their empty defaults, a shape mismatch in either ships
                    # undetected. `supersession` is [] in the shipped planner
                    # (review-only refuses every decision, so nothing is ever
                    # accepted), which is exactly why the renderer needs a
                    # fixture rather than a caller to exercise it.
                    "alternatives": [["native-manual", "native-plaid-alt"]],
                    "supersession": [
                        {
                            "decision_id": "proposal-prior-122",
                            "status": "accepted",
                            "members": ["native-manual"],
                            "reserved_rows": [["plaid", "origin", "native-plaid"]],
                            "current_successors": ["proposal-review-123"],
                        }
                    ],
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

    policy = TerminalPolicy(
        output="text",
        interactive=False,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=width,
        height=24,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )

    def policy_factory(*, no_pager: bool = False) -> TerminalPolicy:
        return policy

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_terminal_policy",
        policy_factory,
    )
    result = CliRunner().invoke(
        app,
        [
            "investments",
            "matches",
            command,
            "--wide",
            "--no-pager",
            *(["--quiet"] if quiet else []),
        ],
    )
    assert result.exit_code == 0, result.output
    normalized_stdout = re.sub(r"\s+", " ", result.stdout)
    for value in (
        "proposal-review-1234567890",
        "fuzzy",
        "conflict-trade-date-123",
        "trade_date",
        "choice-manual",
        "2026-01-10",
        "choice-plaid",
        "2026-01-11",
        "event-manual-1234567890",
        "native-manual-1234567890",
        "-123456789012345.6789012345",
        "USD",
        "trade_date_conflict",
        "golden_membership_changed",
    ):
        assert value in normalized_stdout
    # Without the lifecycle status a terminal reader cannot tell why a settled
    # Proposal is in the history at all — every row there renders the same id
    # and confidence, and only JSON carried the state that separates them. The
    # label is asserted too: "pending" reaches stdout from the view's own status
    # either way, so the value alone proves nothing on the pending route.
    assert "Status" in normalized_stdout
    assert ("pending" if command == "pending" else "superseded") in normalized_stdout
    # The two branches the fixture above exists to reach.
    assert "native-manual / native-plaid-alt" in normalized_stdout
    assert "proposal-prior-122" in normalized_stdout
    # The leg's raw, user-authored account_id must reach the text table
    # masked, never bare — the CLI text path applies no redaction by design
    # (render_or_json's docstring), so the command itself must mask it.
    assert "****9999" in result.stdout
    assert "raw_account_9999" not in result.stdout


@pytest.mark.parametrize("command", ["pending", "history"])
def test_cli_review_empty_scope_stays_visible_without_its_routine_hint_under_quiet(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    """Quiet keeps the requested empty result while dropping the next-step hint."""

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )
    result = CliRunner().invoke(
        app, ["investments", "matches", command, "--quiet", "--no-pager"]
    )

    assert result.exit_code == 0, result.output
    expected_scope = "pending" if command == "pending" else "historical"
    assert f"No {expected_scope} investment Proposals." in result.stdout
    assert "moneybin investments matches run" not in result.stdout


def test_cli_pending_pages_one_complete_answer_and_no_pager_prints_it(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pager receives scope, proposal evidence, and context as one answer."""
    from moneybin.cli import pager

    _seed(comparison_db)
    refresh(comparison_db, steps=["investment_match"])

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    pages: list[str] = []

    def policy_factory(*, no_pager: bool = False) -> TerminalPolicy:
        return policy

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_terminal_policy",
        policy_factory,
    )
    monkeypatch.setattr(
        pager,
        "page_text",
        capture_page,
    )

    paged = CliRunner().invoke(app, ["investments", "matches", "pending"])
    direct = CliRunner().invoke(
        app, ["investments", "matches", "pending", "--no-pager"]
    )

    assert paged.exit_code == 0, paged.output
    assert direct.exit_code == 0, direct.output
    assert len(pages) == 1
    for text in (pages[0], direct.stdout):
        assert "All 1 pending investment Proposals" in text
        assert "Evidence" in text
        assert "Downstream effects" in text


def test_cli_pending_json_keeps_the_typed_envelope_and_masks_account_ids(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The text migration leaves the JSON contract and its redaction untouched."""
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
    result = CliRunner().invoke(
        app, ["investments", "matches", "pending", "--output", "json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["data"]["status"] == "pending"
    assert payload["data"]["rows"][0]["details"]["legs"][0]["account_id"] == "****ount"


def test_cli_run_no_input_prints_a_factual_receipt(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty planner run does not claim proposals were prepared."""

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr(
        "moneybin.cli.commands.investments.matches.get_database", database_context
    )
    result = CliRunner().invoke(app, ["investments", "matches", "run"])

    assert result.exit_code == 0, result.output
    assert "Investment match planning" in result.stdout
    assert "Comparison inputs are not ready; run refresh first." in result.stdout
    assert "prepared for review" not in result.stdout


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
