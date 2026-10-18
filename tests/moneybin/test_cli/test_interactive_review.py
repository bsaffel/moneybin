"""Bounded review sessions never apply an implicit decision."""

import re
from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.utils import get_terminal_policy

runner = CliRunner()


@pytest.mark.parametrize("args", [["--output", "json"], [], ["--limit", "0"]])
def test_interactive_rejects_invalid_mode_before_database(args: list[str]) -> None:
    with patch("moneybin.cli.commands.transactions.review.get_database") as database:
        result = runner.invoke(app, ["review", "--interactive", *args])
    assert result.exit_code == 2
    database.assert_not_called()


def test_interactive_skip_counts_toward_limit() -> None:
    from moneybin.cli.commands.interactive_review import ReviewItem, run_review

    items = [
        ReviewItem("matches", "one", (("Match", "one"),)),
        ReviewItem("matches", "two", (("Match", "two"),)),
    ]
    policy = replace(get_terminal_policy(), interactive=True)
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=items
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="skip",
        ),
        patch("moneybin.cli.commands.interactive_review.apply_decision") as apply,
    ):
        run_review("all", 1, policy)
    apply.assert_not_called()


@pytest.mark.parametrize(
    "queue",
    ["matches", "categorize", "account-links", "merchant-links", "security-links"],
)
def test_review_revalidates_before_deciding(queue: str) -> None:
    from moneybin.cli.commands.interactive_review import ReviewItem, run_review

    item = ReviewItem(queue, "one", (("Evidence", "original"),))
    changed = replace(item, evidence=(("Evidence", "changed"),))
    policy = replace(get_terminal_policy(), interactive=True)
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items",
            side_effect=[[item], [changed]],
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="accept",
        ),
        patch("moneybin.cli.commands.interactive_review.apply_decision") as apply,
    ):
        run_review(queue, 1, policy)
    apply.assert_not_called()


def test_cancel_reports_earlier_persisted_decision(
    capsys: pytest.CaptureFixture[str],
) -> None:
    import typer

    from moneybin.cli.commands.interactive_review import ReviewItem, run_review

    one = ReviewItem("matches", "one", (("Match", "one"),))
    two = replace(one, item_id="two")
    policy = replace(get_terminal_policy(), interactive=True)
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items",
            return_value=[one, two],
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            side_effect=["accept", typer.Abort()],
        ),
        patch(
            "moneybin.cli.commands.interactive_review.apply_decision", return_value=True
        ),
    ):
        with pytest.raises(typer.Abort):
            run_review("matches", 2, policy)
    out = capsys.readouterr().out
    assert "Decisions saved" in out
    assert re.search(r"Decisions saved:?\s+1\b", out)
    assert "cancelled" in out.lower()


def test_empty_review_does_not_prompt() -> None:
    from moneybin.cli.commands.interactive_review import run_review

    policy = replace(get_terminal_policy(), interactive=True)
    with (
        patch("moneybin.cli.commands.interactive_review.load_items", return_value=[]),
        patch("moneybin.cli.commands.interactive_review.choose_required") as choose,
    ):
        run_review("all", 50, policy)
    choose.assert_not_called()


def test_failed_account_refresh_reports_saved_then_exits(
    capsys: pytest.CaptureFixture[str],
) -> None:
    import typer

    from moneybin.cli.commands.interactive_review import (
        ReviewCandidate,
        ReviewItem,
        run_review,
    )

    item = ReviewItem(
        "account-links",
        "group",
        (),
        candidates=(ReviewCandidate("decision", "target", "Candidate"),),
    )
    policy = replace(get_terminal_policy(), interactive=True)
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            side_effect=["accept", "decision"],
        ),
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch("moneybin.cli.commands.accounts.links.confirm_merge"),
        patch("moneybin.cli.commands.accounts.links.merge_drift_check"),
        patch("moneybin.cli.commands.accounts.links.report_rematch", return_value=[]),
        patch("moneybin.services.account_links_service.AccountLinksService") as service,
    ):
        service.return_value.set.return_value = MagicMock(error="rebuild failed")
        with pytest.raises(typer.Exit) as stopped:
            run_review("account-links", 1, policy)
    assert stopped.value.exit_code == 1
    out = capsys.readouterr().out
    assert "Decisions saved" in out
    assert re.search(r"Decisions saved:?\s+1\b", out)


@pytest.mark.parametrize(
    "queue,target,method",
    [
        ("merchant-links", "merchant", "set"),
        ("security-links", "security", "accept"),
    ],
)
def test_explicit_identity_targets_reach_domain_service(
    queue: str, target: str, method: str
) -> None:
    from moneybin.cli.commands.interactive_review import (
        ReviewCandidate,
        ReviewItem,
        apply_decision,
    )

    item = ReviewItem(
        queue,
        "group",
        (),
        candidates=(ReviewCandidate("decision", target, "Candidate"),),
    )
    module = (
        "merchant_links_service.MerchantLinksService"
        if queue == "merchant-links"
        else "security_links_service.SecurityLinksService"
    )
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="decision",
        ) as choose,
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch(f"moneybin.services.{module}") as service,
    ):
        assert apply_decision(
            item, "accept", replace(get_terminal_policy(), interactive=True)
        )
    assert choose.call_args.kwargs["choices"][0].value == "decision"
    called = getattr(service.return_value, method).call_args
    assert called.args == ("decision",)
    assert target in called.kwargs.values()


def test_categorization_uses_versioned_id_and_explicit_category() -> None:
    from moneybin.cli.commands.interactive_review import (
        ReviewItem,
        apply_decision,
    )

    item = ReviewItem("categorize", "cat_versioned_attempt", ())
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="cat-id",
        ),
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch("moneybin.services.categorization.CategorizationService") as categories,
        patch(
            "moneybin.services.review_decisions_service.ReviewDecisionsService"
        ) as decisions,
    ):
        categories.return_value.get_active_categories.return_value = [
            {"category_id": "cat-id", "category": "Food", "subcategory": "Dining"}
        ]
        assert apply_decision(
            item, "accept", replace(get_terminal_policy(), interactive=True)
        )
    request = decisions.return_value.apply_ordinary.call_args.args[0][0]
    assert request.decision_id == "cat_versioned_attempt"
    assert request.category == "Food"
    assert request.subcategory == "Dining"


@pytest.mark.parametrize("committed,saved", [("accepted", True), ("reversed", False)])
def test_match_counts_committed_outcome(committed: str, saved: bool) -> None:
    from moneybin.cli.commands.interactive_review import (
        ReviewItem,
        apply_decision,
    )

    item = ReviewItem("matches", "match", ())
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch("moneybin.cli.utils.warn_transfers_retired"),
        patch("moneybin.services.matching_service.MatchingService") as service,
    ):
        service.return_value.set_status.return_value = MagicMock(
            match_status=committed, transfers_retired=0
        )
        if saved:
            assert apply_decision(
                item, "accept", replace(get_terminal_policy(), interactive=True)
            )
        else:
            import typer

            with pytest.raises(typer.Exit) as stopped:
                apply_decision(
                    item, "accept", replace(get_terminal_policy(), interactive=True)
                )
            assert stopped.value.exit_code == 1


def test_account_decline_never_opens_writable_database() -> None:
    import typer

    from moneybin.cli.commands.interactive_review import (
        ReviewCandidate,
        ReviewItem,
        apply_decision,
    )

    item = ReviewItem(
        "account-links",
        "group",
        (),
        candidates=(ReviewCandidate("decision", "target", "Candidate"),),
    )
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="decision",
        ),
        patch(
            "moneybin.cli.commands.accounts.links.confirm_merge",
            side_effect=typer.Abort(),
        ),
        patch("moneybin.cli.commands.interactive_review.get_database") as database,
    ):
        with pytest.raises(typer.Abort):
            apply_decision(
                item, "accept", replace(get_terminal_policy(), interactive=True)
            )
    database.assert_not_called()


def test_identity_group_chooses_one_of_all_candidates() -> None:
    from moneybin.cli.commands.interactive_review import (
        ReviewCandidate,
        ReviewItem,
        apply_decision,
    )

    item = ReviewItem(
        "merchant-links",
        "group",
        (),
        candidates=(
            ReviewCandidate("decision-a", "merchant-a", "A"),
            ReviewCandidate("decision-b", "merchant-b", "B"),
        ),
    )
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="decision-b",
        ) as choose,
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch(
            "moneybin.services.merchant_links_service.MerchantLinksService"
        ) as service,
    ):
        assert apply_decision(
            item, "accept", replace(get_terminal_policy(), interactive=True)
        )
    assert [choice.value for choice in choose.call_args.kwargs["choices"]] == [
        "decision-a",
        "decision-b",
    ]
    service.return_value.set.assert_called_once_with(
        "decision-b", target_merchant_id="merchant-b", decided_by="user"
    )


def test_fx_refresh_failure_keeps_committed_match_receipt(
    capsys: pytest.CaptureFixture[str],
) -> None:
    import typer

    from moneybin import error_codes
    from moneybin.cli.commands.interactive_review import ReviewItem, run_review
    from moneybin.errors import UserError

    item = ReviewItem("matches", "match", ())
    policy = replace(get_terminal_policy(), interactive=True)
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="accept",
        ),
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch(
            "moneybin.matching.persistence.get_match_decision",
            return_value={"match_status": "accepted"},
        ),
        patch("moneybin.services.matching_service.MatchingService") as service,
    ):
        service.return_value.set_status.side_effect = UserError(
            "Match committed; FX rebuild failed",
            code=error_codes.REFRESH_MODEL_FAILED,
            hint="Run moneybin refresh",
        )
        with pytest.raises(typer.Exit) as stopped:
            run_review("matches", 1, policy)
    assert stopped.value.exit_code == 1
    out = capsys.readouterr().out
    assert "Decisions saved" in out
    assert re.search(r"Decisions saved:?\s+1\b", out)


def test_categorization_reject_omits_result_fields() -> None:
    from moneybin.cli.commands.interactive_review import (
        ReviewItem,
        apply_decision,
    )

    item = ReviewItem("categorize", "cat_attempt", ())
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch(
            "moneybin.services.review_decisions_service.ReviewDecisionsService"
        ) as decisions,
    ):
        assert apply_decision(
            item, "reject", replace(get_terminal_policy(), interactive=True)
        )
    request = decisions.return_value.apply_ordinary.call_args.args[0][0]
    assert request.model_fields_set == {"kind", "decision_id", "decision"}


@pytest.mark.parametrize(
    "queue,module",
    [
        ("account-links", "account_links_service.AccountLinksService"),
        ("merchant-links", "merchant_links_service.MerchantLinksService"),
        ("security-links", "security_links_service.SecurityLinksService"),
    ],
)
def test_identity_loader_keeps_siblings_in_one_bounded_item(
    queue: str, module: str
) -> None:
    from moneybin.cli.commands.interactive_review import load_items

    candidates: list[MagicMock] = []
    for suffix in ("a", "b"):
        candidate = MagicMock()
        candidate.decision_id = f"decision-{suffix}"
        candidate.overlap = MagicMock(measurable=False)
        candidate.candidate_account_id = f"account-{suffix}"
        candidate.candidate_display_name = f"Account {suffix}"
        candidate.candidate_merchant_id = f"merchant-{suffix}"
        candidate.candidate_canonical_name = f"Merchant {suffix}"
        candidate.candidate_security_id = f"security-{suffix}"
        candidate.candidate_name = f"Security {suffix}"
        candidates.append(candidate)
    group = MagicMock(
        candidates=tuple(candidates),
        provisional_account_id="provisional",
        provisional_display_name="Provisional",
        transactions=2,
        ref_value="provider-ref",
        ref_kind="plaid_security_id",
        source_type="synthetic",
    )
    with (
        patch("moneybin.cli.commands.interactive_review.get_database") as database,
        patch(f"moneybin.services.{module}") as service,
    ):
        service.return_value.pending.return_value = [group]
        items = load_items(queue, 1)
    assert len(items) == 1
    assert [c.decision_id for c in items[0].candidates] == ["decision-a", "decision-b"]
    assert all(c.decision_id in str(items[0].evidence) for c in items[0].candidates)
    database.assert_called_once_with(read_only=True)


def test_categorization_loader_projects_attempt_without_persisting() -> None:
    from moneybin.cli.commands.interactive_review import load_items

    with (
        patch("moneybin.cli.commands.interactive_review.get_database") as database,
        patch("moneybin.services.categorization.CategorizationService") as service,
    ):
        service.return_value.list_pending_review_transactions.return_value = [
            {
                "transaction_id": "transaction",
                "decision_id": "cat_attempt",
                "description": "Synthetic purchase",
                "amount": "-1.00",
                "currency_code": "USD",
            }
        ]
        items = load_items("categorize", 1)
    assert items[0].item_id == "cat_attempt"
    assert items[0].transaction_id == "transaction"
    database.assert_called_once_with(read_only=True)
    service.return_value.apply_review_categorization_in_active_txn.assert_not_called()


def test_quit_leaves_all_decisions_untouched(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from moneybin.cli.commands.interactive_review import ReviewItem, run_review

    item = ReviewItem("matches", "match", ())
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="quit",
        ),
        patch("moneybin.cli.commands.interactive_review.apply_decision") as apply,
    ):
        run_review("all", 5, replace(get_terminal_policy(), interactive=True))
    apply.assert_not_called()
    assert re.search(r"Decisions saved:?\s+0\b", capsys.readouterr().out)


def test_target_time_staleness_counts_in_session_receipt(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from moneybin.cli.commands.interactive_review import ReviewItem, run_review

    item = ReviewItem("matches", "match", ())
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="accept",
        ),
        patch(
            "moneybin.cli.commands.interactive_review.apply_decision",
            return_value=False,
        ),
    ):
        run_review("all", 1, replace(get_terminal_policy(), interactive=True))
    out = capsys.readouterr().out
    assert re.search(r"Stale items:?\s+1\b", out)
    assert re.search(r"Decisions saved:?\s+0\b", out)


def test_rejected_newest_does_not_hide_older_pending_next_session(db: object) -> None:
    from contextlib import nullcontext
    from typing import cast

    from moneybin.cli.commands.interactive_review import apply_decision, load_items
    from moneybin.database import Database
    from tests.moneybin.db_helpers import create_core_tables

    database = cast(Database, db)
    create_core_tables(database)
    database.execute("""
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount,
             currency_code, description, source_type)
        VALUES ('a', 'acct', DATE '2026-10-02', -2.00, 'USD', 'Newest purchase', 'manual'),
               ('b', 'acct', DATE '2026-10-01', -1.00, 'USD', 'Older purchase', 'manual')
    """)
    database.execute("""INSERT INTO core.dim_accounts (account_id, display_name, archived)
                       VALUES ('acct', 'Synthetic account', FALSE)""")
    from tests.moneybin.db_helpers import derive_is_categorizable

    database.execute("UPDATE core.fct_transactions SET is_transfer = FALSE")
    derive_is_categorizable(database)
    with patch(
        "moneybin.cli.commands.interactive_review.get_database",
        return_value=nullcontext(database),
    ):
        first = load_items("categorize", 1)
        assert len(first) == 1
        assert dict(first[0].evidence)["transaction id"] == "a"
        assert apply_decision(
            first[0], "reject", replace(get_terminal_policy(), interactive=True)
        )
        next_session = load_items("categorize", 1)
    assert len(next_session) == 1
    assert dict(next_session[0].evidence)["transaction id"] == "b"


def test_match_load_and_render_show_both_actual_source_observations(
    db: object, capsys: pytest.CaptureFixture[str]
) -> None:
    from contextlib import nullcontext
    from typing import cast

    from moneybin.cli.commands.interactive_review import run_review
    from moneybin.database import Database
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
    from tests.moneybin.db_helpers import create_core_tables

    database = cast(Database, db)
    create_core_tables(database)
    database.execute("CREATE SCHEMA IF NOT EXISTS prep")
    database.execute("""
        CREATE TABLE prep.int_transactions__unioned (
            source_transaction_id VARCHAR, account_id VARCHAR, source_type VARCHAR,
            transaction_date DATE, amount DECIMAL(18,2), currency_code VARCHAR,
            description VARCHAR
        )
    """)
    database.execute("""
        INSERT INTO prep.int_transactions__unioned VALUES
        ('shared-id', 'first', 'manual', DATE '2026-10-01', -12.34, 'USD', 'First source purchase'),
        ('shared-id', 'second', 'csv', DATE '2026-10-02', 12.34, 'USD', 'Second source receipt'),
        ('shared-id', 'wrong', 'manual', DATE '2026-10-03', 999.00, 'EUR', 'Wrong account decoy')
    """)
    database.execute("""
        INSERT INTO core.dim_accounts (account_id, display_name)
        VALUES ('first', 'Synthetic checking'), ('second', 'Synthetic savings')
    """)
    MatchDecisionsRepo(database).insert(
        match_id="pair",
        source_transaction_id_a="shared-id",
        source_type_a="manual",
        source_origin_a="user",
        source_transaction_id_b="shared-id",
        source_type_b="csv",
        source_origin_b="bank",
        account_id="first",
        account_id_b="second",
        confidence_score=0.9,
        match_signals={},
        match_tier=None,
        match_type="transfer",
        match_status="pending",
        decided_by="matcher",
        actor="test",
    )
    with (
        patch(
            "moneybin.cli.commands.interactive_review.get_database",
            return_value=nullcontext(database),
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="skip",
        ),
    ):
        run_review("matches", 1, replace(get_terminal_policy(), interactive=True))
    output = capsys.readouterr().out
    for expected in (
        "First source purchase",
        "Second source receipt",
        "12.34",
        "USD",
        "2026-10-01",
        "2026-10-02",
        "Synthetic checking",
        "Synthetic savings",
    ):
        assert expected in output
    assert "Wrong account decoy" not in output


@pytest.mark.parametrize("missing_at", ["initial", "after_display", "before_write"])
def test_unavailable_match_observations_never_authorize_writes(missing_at: str) -> None:
    import typer

    from moneybin.cli.commands.interactive_review import run_review
    from moneybin.services.matching_service import MatchDecisionOutcome

    row = {
        "match_id": "pending",
        "account_id": "account",
        "source_type_a": "manual",
        "source_type_b": "csv",
        "source_transaction_id_a": "a",
        "source_transaction_id_b": "b",
    }
    observation = {
        "amount": "-1.00",
        "currency_code": "USD",
        "description": "Synthetic observation",
    }
    observation_options: dict[str, list[dict[str, str] | None]] = {
        "initial": [None, None],
        "after_display": [observation, observation, None, None],
        "before_write": [
            observation,
            observation,
            observation,
            observation,
            None,
            None,
        ],
    }
    observations = observation_options[missing_at]
    with (
        patch("moneybin.cli.commands.interactive_review.get_database") as database,
        patch("moneybin.services.matching_service.MatchingService") as service,
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="accept",
        ),
    ):
        service.return_value.get_pending.return_value = [row]

        def source_observation(
            *, source_type: str, source_transaction_id: str, account_id: str
        ) -> dict[str, str] | None:
            return observations.pop(0) if observations else None

        service.return_value.get_source_observation.side_effect = source_observation
        service.return_value.set_status.return_value = MatchDecisionOutcome(
            "accepted", 0
        )
        with pytest.raises(typer.Exit) as refused:
            run_review("matches", 1, replace(get_terminal_policy(), interactive=True))
    assert refused.value.exit_code == 1
    service.return_value.set_status.assert_not_called()
    assert all(call.kwargs.get("read_only") is True for call in database.call_args_list)


def test_stale_decision_diagnostic_goes_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from moneybin.cli.commands.interactive_review import ReviewItem, run_review

    item = ReviewItem("matches", "match", (("Description", "Original"),))
    changed = replace(item, evidence=(("Description", "Changed"),))
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items",
            side_effect=[[item], [changed]],
        ),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="accept",
        ),
    ):
        run_review("matches", 1, replace(get_terminal_policy(), interactive=True))
    captured = capsys.readouterr()
    assert "Decision changed since display" in captured.err
    assert "Decision changed since display" not in captured.out


def test_target_change_diagnostic_goes_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from moneybin.cli.commands.interactive_review import ReviewItem, apply_decision

    item = ReviewItem("matches", "match", ())
    with (
        patch("moneybin.cli.commands.interactive_review.load_items", return_value=[]),
        patch("moneybin.cli.commands.interactive_review.get_database") as database,
    ):
        assert not apply_decision(
            item, "accept", replace(get_terminal_policy(), interactive=True)
        )
    captured = capsys.readouterr()
    assert "Decision changed while choosing" in captured.err
    assert "Decision changed while choosing" not in captured.out
    database.assert_not_called()


@pytest.mark.parametrize(
    "field,updated", [("category", "Renamed"), ("class", "income")]
)
def test_selected_category_metadata_drift_is_refused(field: str, updated: str) -> None:
    from moneybin.cli.commands.interactive_review import ReviewItem, apply_decision

    selected = {
        "category_id": "chosen",
        "category": "Synthetic",
        "subcategory": None,
        "class": "expense",
    }
    current = {**selected, field: updated}
    item = ReviewItem("categorize", "pending", ())
    with (
        patch(
            "moneybin.cli.commands.interactive_review.load_items", return_value=[item]
        ),
        patch("moneybin.cli.commands.interactive_review.get_database"),
        patch(
            "moneybin.cli.commands.interactive_review.choose_required",
            return_value="chosen",
        ),
        patch("moneybin.services.categorization.CategorizationService") as categories,
        patch(
            "moneybin.services.review_decisions_service.ReviewDecisionsService"
        ) as decisions,
    ):
        categories.return_value.get_active_categories.side_effect = [
            [selected],
            [current],
        ]
        assert apply_decision(item, "accept", get_terminal_policy()) is False
        decisions.return_value.apply_ordinary.assert_not_called()
