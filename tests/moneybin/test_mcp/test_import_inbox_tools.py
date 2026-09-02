"""Tests for import_inbox_sync / import_inbox_pending MCP tools."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.mcp.tools.import_inbox import (
    _uncategorized_count,  # pyright: ignore[reportPrivateUsage]  # module-private helper under test
    import_inbox_pending,
    import_inbox_sync,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.services.inbox_service import (
    InboxListResult,
    InboxSyncResult,
)
from tests.moneybin.db_helpers import (
    create_core_tables,
    install_uncategorized_queue_view,
)


@pytest.fixture
def patch_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> MagicMock:
    """Patch InboxService and get_database so MCP tool tests don't open a real DB."""
    fake = MagicMock()
    fake.root = tmp_path / "inbox-root"

    @contextmanager
    def _fake_get_database(*args, **kwargs):  # type: ignore[misc]
        yield MagicMock()

    monkeypatch.setattr(
        "moneybin.mcp.tools.import_inbox.get_database",
        _fake_get_database,  # pyright: ignore[reportUnknownArgumentType]
    )

    # Patch InboxService in the tool module so both the constructor call in
    # import_inbox_sync AND for_active_profile_no_db in import_inbox_pending
    # return `fake`.
    fake_cls = MagicMock(return_value=fake)
    fake_cls.for_active_profile_no_db = MagicMock(return_value=fake)
    monkeypatch.setattr(
        "moneybin.mcp.tools.import_inbox.InboxService",
        fake_cls,
    )
    return fake


class TestImportInboxSync:
    """import_inbox_sync envelope shape and actions."""

    async def test_returns_low_sensitivity_envelope(
        self, patch_service: MagicMock
    ) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 3}],
        )
        envelope = import_inbox_sync()
        assert envelope.summary.sensitivity == "low"
        # .get, not []: every key on the entry is optional by design — omission
        # is the row's contract, not an oversight.
        assert envelope.data.processed[0].get("filename") == "a.csv"

    async def test_account_confirmation_pending_includes_binding_hint(
        self, patch_service: MagicMock
    ) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            pending=[
                {
                    "filename": "statement.csv",
                    "channel": "tabular",
                    "tier": "high",
                    "score": 1.0,
                    "reason": "account_confirmation",
                    "moved_to": "pending/2026-05/statement.csv",
                }
            ],
        )
        envelope = import_inbox_sync()
        assert any("inbox/<account-slug>" in a for a in envelope.actions)
        # account_confirmation pairs --accept (ratifies the settled mapping) with
        # --account-binding; it never offers a standalone --mapping override.
        assert any(
            "--accept" in a and "--account-binding" in a for a in envelope.actions
        )
        assert not any("--mapping" in a for a in envelope.actions)

    async def test_minted_accounts_are_announced_with_their_recoveries(
        self, patch_service: MagicMock
    ) -> None:
        """An account the drain minted must reach the agent, with its correction.

        The drain gates the merge, not the mint, so nothing stopped to ask — the
        announcement is the whole of "magic stays visible" here. Same helper
        ``import_files`` uses, so the agent reads one wording for one event.
        """
        patch_service.sync.return_value = InboxSyncResult(
            processed=[
                {
                    "filename": "march.ofx",
                    "transactions": 47,
                    "accounts_created": [
                        {"account_id": "9f8e7d6c5b4a", "display_name": "Chase Checking"}
                    ],
                }
            ],
        )
        envelope = import_inbox_sync()
        assert any(
            "account" in a.lower() and "created" in a.lower() for a in envelope.actions
        ), envelope.actions
        created = envelope.data.processed[0].get("accounts_created") or []
        assert created[0].get("account_id") == "9f8e7d6c5b4a"

    async def test_no_mint_no_account_creation_hint(
        self, patch_service: MagicMock
    ) -> None:
        """The hint is conditional, not constant.

        Re-import adopts every account and is the common case; an action emitted
        on every drain is one the agent learns to skip on the drain that means it.
        """
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "march.ofx", "transactions": 47}],
        )
        envelope = import_inbox_sync()
        assert not any(
            "account" in a.lower() and "created" in a.lower() for a in envelope.actions
        ), envelope.actions

    async def test_pending_masks_the_institutions_account_number(
        self, patch_service: MagicMock
    ) -> None:
        """The redaction walk must reach proposals nested inside a pending entry.

        ``pending`` was declared ``list[dict[str, object]]`` — one DESCRIPTION
        class covering the whole list — so the walk stopped at the entry and
        never saw that ``account_proposals[].source_account_key`` is
        ACCOUNT_IDENTIFIER. On the OFX channel that key is the ``<ACCTID>`` the
        institution issued, and this envelope goes to the model provider.
        """
        acctid = "000123456789"
        patch_service.sync.return_value = InboxSyncResult(
            pending=[
                {
                    "filename": "statement.ofx",
                    "reason": "account_confirmation",
                    "account_proposals": [
                        {
                            "source_account_key": acctid,
                            "proposal_ref": "@0",
                            "proposed_account_id": "prov12345678",
                            "candidates": [],
                        }
                    ],
                }
            ],
        )

        envelope = import_inbox_sync()
        masked = redact_typed(envelope.data, consent=None)

        blob = json.dumps(masked, default=str)
        assert acctid not in blob, blob
        proposal = masked.pending[0]["account_proposals"][0]
        assert proposal["source_account_key"] == "****6789"
        # Ours, not the institution's — RECORD_ID, and the only half an agent
        # can act on once the institution's key is masked.
        assert proposal["proposal_ref"] == "@0"
        assert proposal["proposed_account_id"] == "prov12345678"
        # Keys absent from the entry stay absent: the omission is the contract,
        # and a dataclass here would start emitting `moved_to: null` on every row.
        assert "moved_to" not in masked.pending[0]

    async def test_no_failure_no_resolution_hint(
        self, patch_service: MagicMock
    ) -> None:
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 1}],
        )
        envelope = import_inbox_sync()
        assert not any("inbox/<account-slug>" in a for a in envelope.actions)

    async def test_categorize_hint_appears_when_above_threshold(
        self, patch_service: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Hint referencing categorize_assist is appended when uncategorized >= threshold."""
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 5}],
        )
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_inbox._uncategorized_count",
            lambda: 50,
        )
        envelope = import_inbox_sync()
        assert any("categorize_assist" in a for a in envelope.actions)
        assert any("50" in a for a in envelope.actions)

    async def test_uncategorized_count_reads_the_canonical_queue(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The drain's assist hint counts the rows the review queue lists.

        A confirmed transfer leg is not curator work, so
        core.uncategorized_queue drops it — and the hint that offers to
        categorize "N transactions" must not quote a larger N than the queue
        the user is then sent to.
        """
        create_core_tables(db)
        db.execute(
            "INSERT INTO core.dim_accounts (account_id, display_name, archived) "
            "VALUES ('acct_open', 'Checking', false)"
        )
        db.execute(
            "INSERT INTO core.fct_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            "category, is_transfer) VALUES "
            "('t_pending', 'acct_open', DATE '2026-04-01', -12.00, 'Cafe', "
            "NULL, false), "
            "('t_transfer', 'acct_open', DATE '2026-04-02', -500.00, 'Transfer', "
            "NULL, true)"
        )
        install_uncategorized_queue_view(db)

        @contextmanager
        def _bound_database(*args: object, **kwargs: object):  # type: ignore[misc]
            yield db

        monkeypatch.setattr(
            "moneybin.mcp.tools.import_inbox.get_database",
            _bound_database,  # pyright: ignore[reportUnknownArgumentType]
        )

        assert _uncategorized_count() == 1

    async def test_categorize_hint_absent_below_threshold(
        self, patch_service: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No hint when uncategorized count is below the configured threshold."""
        patch_service.sync.return_value = InboxSyncResult(
            processed=[{"filename": "a.csv", "transactions": 1}],
        )
        monkeypatch.setattr(
            "moneybin.mcp.tools.import_inbox._uncategorized_count",
            lambda: 0,
        )
        envelope = import_inbox_sync()
        assert not any("categorize_assist" in a for a in envelope.actions)


class TestImportInboxPending:
    """import_inbox_pending envelope shape."""

    async def test_returns_would_process_shape(self, patch_service: MagicMock) -> None:
        patch_service.enumerate.return_value = InboxListResult(
            would_process=[{"filename": "a.csv", "account_hint": None}],
        )
        envelope = import_inbox_pending()
        assert envelope.summary.sensitivity == "low"
        assert envelope.data.would_process[0]["filename"] == "a.csv"
