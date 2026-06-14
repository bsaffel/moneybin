"""Schema-initialization tests for the M1S account-identity tables.

Mirrors test_matching_schema.py: every test is read-only against the app
schema, so the module-scoped ``module_db`` fixture applies. Covers the three
new tables (account_links, account_link_decisions, transaction_id_aliases),
their columns, and the TableRef constants.
"""

from moneybin.database import Database
from moneybin.privacy.redaction import redact_records
from moneybin.privacy.taxonomy import CLASSIFICATION
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    TRANSACTION_ID_ALIASES,
)


class TestAccountLinksRedaction:
    """Finding #6: account_links.ref_value masks by default (can be an account number).

    The coverage test only proves ref_value is *classified*; this pins it to a
    *masked* outcome so a future reclassification to a passthrough class can't
    silently leak account numbers at the dynamic-SQL surface.
    """

    def test_ref_value_is_masked(self) -> None:
        classes = CLASSIFICATION[("app", "account_links")]
        rows = [{"ref_value": "987654321", "ref_kind": "full_number"}]
        out = redact_records(
            rows,
            {"ref_value": classes["ref_value"], "ref_kind": classes["ref_kind"]},
        )
        assert out[0]["ref_value"] == "****4321"

    def test_match_signals_is_masked(self) -> None:
        """match_signals can carry institution_last4 — must not be LOW passthrough.

        Unlike match_decisions.match_signals (scores, no PII), this column's
        weak-signal values include account digits, so the dynamic-SQL redactor
        must not pass it through. Asserts the value is changed (masked), not the
        exact form (JSON masking is coarse; structured presentation is M1S.5).
        """
        classes = CLASSIFICATION[("app", "account_link_decisions")]
        raw = '{"signal": "institution_last4", "value": "4267"}'
        out = redact_records(
            [{"match_signals": raw}], {"match_signals": classes["match_signals"]}
        )
        assert out[0]["match_signals"] != raw

    def test_canonical_account_id_is_not_masked(self) -> None:
        """The opaque minted canonical account_id is the agent handle (spec D1/D6).

        It must pass through the redactor, not be masked to ****<last4> — masking
        the handle agents/users pass back as a parameter would defeat its purpose.
        """
        links = CLASSIFICATION[("app", "account_links")]
        decisions = CLASSIFICATION[("app", "account_link_decisions")]
        rows = [{"account_id": "a1b2c3d4e5f6"}]
        out = redact_records(rows, {"account_id": links["account_id"]})
        assert out[0]["account_id"] == "a1b2c3d4e5f6"
        # the decisions table's two account-id columns carry the same opaque id
        assert decisions["provisional_account_id"] == links["account_id"]
        assert decisions["candidate_account_id"] == links["account_id"]


class TestAccountIdentityTableRefs:
    """TableRef constants for the three account-identity tables."""

    def test_table_ref_full_names(self) -> None:
        assert ACCOUNT_LINKS.full_name == "app.account_links"
        assert ACCOUNT_LINK_DECISIONS.full_name == "app.account_link_decisions"
        assert TRANSACTION_ID_ALIASES.full_name == "app.transaction_id_aliases"


class TestAccountLinksSchema:
    """app.account_links — the native-ref -> canonical mapping (spec Decision 2)."""

    def test_account_links_table_exists(self, module_db: Database) -> None:
        result = module_db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'account_links'"
        ).fetchone()
        assert result is not None
        assert result[0] == 1

    def test_account_links_columns(self, module_db: Database) -> None:
        cols = module_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'account_links' "
            "ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        for expected in (
            "link_id",
            "account_id",
            "ref_kind",
            "ref_value",
            "source_type",
            "source_origin",
            "status",
            "decided_by",
            "decided_at",
            "reversed_at",
            "reversed_by",
        ):
            assert expected in col_names, f"missing column: {expected}"


class TestAccountLinkDecisionsSchema:
    """app.account_link_decisions — the merge-proposal review queue (spec Decision 2)."""

    def test_account_link_decisions_table_exists(self, module_db: Database) -> None:
        result = module_db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'account_link_decisions'"
        ).fetchone()
        assert result is not None
        assert result[0] == 1

    def test_account_link_decisions_columns(self, module_db: Database) -> None:
        cols = module_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'account_link_decisions' "
            "ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        for expected in (
            "decision_id",
            "provisional_account_id",
            "candidate_account_id",
            "confidence_score",
            "match_signals",
            "status",
            "decided_by",
            "match_reason",
            "decided_at",
            "reversed_at",
            "reversed_by",
        ):
            assert expected in col_names, f"missing column: {expected}"


class TestTransactionIdAliasesSchema:
    """app.transaction_id_aliases — old_id -> new_id forwarding map (spec Decision 4 / ADR-015)."""

    def test_transaction_id_aliases_table_exists(self, module_db: Database) -> None:
        result = module_db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'transaction_id_aliases'"
        ).fetchone()
        assert result is not None
        assert result[0] == 1

    def test_transaction_id_aliases_columns(self, module_db: Database) -> None:
        cols = module_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'transaction_id_aliases' "
            "ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        for expected in (
            "old_transaction_id",
            "new_transaction_id",
            "created_at",
        ):
            assert expected in col_names, f"missing column: {expected}"
