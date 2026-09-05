"""Curation stranded by *deleted* source rows is healed from the orphan side.

The forwarding derivation in ``moneybin.matching.aliasing`` builds its candidate
set from source rows that are still present, so it cannot see a re-key caused by
rows disappearing. Two production paths delete them: ``revert_confirmed`` drops
an import's raw rows while the accepted ``app.match_decisions`` row survives
(``REVERT_TABLES`` lists raw tables only), and ``handle_removed_transactions``
deletes rows on an ordinary Plaid sync. Either way the merge group re-anchors to
a surviving member and the canonical id flips back to one that already forwards
away, stranding the curation on an id present in no view.

Unlike ``test_transaction_id_aliasing.py``, which stubs
``prep.int_transactions__unioned`` as a table, this module builds the **real**
chain from ``raw.*`` up to ``core.fct_transactions`` out of the shipped model
files. Both causes delete rows from ``raw.*`` and the healing pass reads
``core.fct_transactions``; a stub at either end would let these tests pass
without either mechanism working.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Generator
from datetime import date, datetime
from decimal import Decimal

import pytest
from pytest_mock import MockerFixture

import moneybin.services.matching_service as matching_service
from moneybin.database import SQLMESH_ROOT, Database
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.matching.aliasing import (
    AliasForwardResult,
    forward_rekeyed_transaction_ids,
)
from moneybin.matching.engine import MatchResult, MatchRunError
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.transaction_categories_repo import TransactionCategoriesRepo
from moneybin.repositories.transaction_id_aliases_repo import TransactionIdAliasesRepo
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.repositories.transaction_splits_repo import TransactionSplitsRepo
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services.doctor_service import DoctorService
from moneybin.services.import_service import ImportService
from moneybin.services.matching_service import MatchingService
from tests.moneybin.db_helpers import (
    CORE_BRIDGE_TRANSFERS_DDL,
    CORE_DIM_ACCOUNTS_DDL,
    CORE_DIM_CATEGORIES_STUB_DDL,
    CORE_DIM_MERCHANTS_STUB_DDL,
)

_ACCOUNT = "acct_canonical"
_ORIGIN = "testbank"

# One source-native account key per channel; app.account_links maps all of them
# onto _ACCOUNT so rows from different channels land in one dedup-able account.
# Account identity is not what these tests exercise — the link is written
# through its own repo rather than forced into the staging join.
_CSV_ACCOUNT_KEY = "tab_acct_0001"
_OFX_ACCOUNT_KEY = "ofx_acct_0001"
_PLAID_ACCOUNT_KEY = "plaid_acct_0001"

_CURATION_TABLES = (
    "transaction_categories",
    "transaction_notes",
    "transaction_tags",
    "transaction_splits",
)

# Every prep model between raw and core, in dependency order.
_PREP_MODELS = (
    "stg_ofx__transactions",
    "stg_manual__transactions",
    "stg_tabular__transactions",
    "stg_plaid__transactions",
    "int_transactions__unioned",
    "int_transactions__matched",
    "int_transactions__merged",
)


def _model_body(layer: str, name: str) -> str:
    """Strip the ``MODEL()`` header off a shipped model file, leaving its query.

    Anchored on the header rather than the file start: several models carry a
    leading ``/* */`` table comment that SQLMesh reads and DuckDB will not parse.
    """
    raw = (SQLMESH_ROOT / "models" / layer / f"{name}.sql").read_text()
    return re.sub(r"^.*?\bMODEL\s*\(.*?\)\s*;\s*", "", raw, flags=re.DOTALL).strip()


def _identity_hash(
    source_type: str, source_account_key: str, source_transaction_id: str
) -> str:
    """The ADR-015 canonical id for one source row's identity tuple.

    Derived from the rule rather than read back from the model, so a change to
    the hashed tuple fails these tests instead of silently redefining them.
    """
    raw = f"{source_type}|{_ORIGIN}|{source_account_key}|{source_transaction_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@pytest.fixture()
def pipeline_db(db: Database) -> Generator[Database, None, None]:
    """A database carrying the shipped raw -> prep -> core transaction chain."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    for name in _PREP_MODELS:
        db.execute(  # noqa: S608  # model body read from the repo file, not user input
            f"CREATE OR REPLACE VIEW prep.{name} AS\n{_model_body('prep', name)}"
        )
    # core.fct_transactions' dimension joins are all LEFT and irrelevant here, so
    # they carry the same shape-only stubs the rest of the suite uses; the fact
    # view itself is the shipped model.
    db.execute(CORE_DIM_ACCOUNTS_DDL)
    db.execute(CORE_BRIDGE_TRANSFERS_DDL)
    db.execute(CORE_DIM_CATEGORIES_STUB_DDL)
    db.execute(CORE_DIM_MERCHANTS_STUB_DDL)
    db.execute(
        "CREATE OR REPLACE VIEW core.fct_transactions AS\n"  # noqa: S608  # model body read from the repo file
        f"{_model_body('core', 'fct_transactions')}"
    )
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, currency_code) VALUES (?, 'USD')",
        [_ACCOUNT],
    )
    links = AccountLinksRepo(db)
    for source_type, ref_value in (
        ("csv", _CSV_ACCOUNT_KEY),
        ("ofx", _OFX_ACCOUNT_KEY),
        ("plaid", _PLAID_ACCOUNT_KEY),
    ):
        links.insert(
            link_id=uuid.uuid4().hex[:12],
            account_id=_ACCOUNT,
            ref_kind="source_native",
            ref_value=ref_value,
            source_type=source_type,
            source_origin=_ORIGIN,
            decided_by="system",
            actor="system",
        )
    yield db


# --------------------------------------------------------------------------
# Raw-row helpers — one per channel, writing the columns staging actually reads.
# --------------------------------------------------------------------------


def _log_import(
    db: Database, *, import_id: str, source_type: str, source_file: str
) -> None:
    """Record the batch in ``raw.import_log`` so ``plan_revert`` can find it."""
    db.execute(
        """
        INSERT INTO raw.import_log (
            import_id, source_file, source_type, source_origin, account_names,
            status, rows_imported, started_at, completed_at
        ) VALUES (?, ?, ?, ?, '[]', 'complete', 1, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [
            import_id,
            source_file,
            source_type,
            _ORIGIN,
            datetime(2024, 3, 20, 9, 0, 0),
            datetime(2024, 3, 20, 9, 1, 0),
        ],
    )


def _load_csv_row(
    db: Database,
    *,
    transaction_id: str,
    import_id: str,
    description: str = "Coffee Roasters",
    amount: str = "-12.34",
    txn_date: date = date(2024, 3, 15),
) -> str:
    """Land one tabular row and return the canonical id it derives."""
    db.execute(
        """
        INSERT INTO raw.tabular_transactions (
            transaction_id, account_id, transaction_date, amount, description,
            currency, source_file, source_type, source_origin, import_id,
            row_number, extracted_at, loaded_at
        ) VALUES (?, ?, ?, ?, ?, 'USD', 'export.csv', 'csv', ?, ?, 1, ?, ?)
        """,
        [
            transaction_id,
            _CSV_ACCOUNT_KEY,
            txn_date,
            Decimal(amount),
            description,
            _ORIGIN,
            import_id,
            datetime(2024, 3, 16, 9, 0, 0),
            datetime(2024, 3, 16, 9, 0, 0),
        ],
    )
    _log_import(db, import_id=import_id, source_type="csv", source_file="export.csv")
    return _identity_hash("csv", _CSV_ACCOUNT_KEY, transaction_id)


def _load_ofx_row(
    db: Database,
    *,
    source_transaction_id: str,
    import_id: str,
    description: str = "Coffee Roasters",
    amount: str = "-12.34",
    txn_date: date = date(2024, 3, 15),
) -> str:
    """Land one OFX row plus its import-log batch; return the canonical id."""
    db.execute(
        """
        INSERT INTO raw.ofx_transactions (
            source_transaction_id, account_id, transaction_type, date_posted,
            amount, payee, source_file, extracted_at, loaded_at, import_id,
            source_type, source_origin, currency_code
        ) VALUES (?, ?, 'DEBIT', ?, ?, ?, 'statement.qfx', ?, ?, ?, 'ofx', ?, 'USD')
        """,
        [
            source_transaction_id,
            _OFX_ACCOUNT_KEY,
            txn_date,
            Decimal(amount),
            description,
            datetime(2024, 3, 20, 9, 0, 0),
            datetime(2024, 3, 20, 9, 0, 0),
            import_id,
            _ORIGIN,
        ],
    )
    _log_import(db, import_id=import_id, source_type="ofx", source_file="statement.qfx")
    return _identity_hash("ofx", _OFX_ACCOUNT_KEY, source_transaction_id)


def _load_plaid_row(
    db: Database,
    *,
    transaction_id: str,
    loaded_at: datetime,
    description: str = "Coffee Roasters",
    amount: str = "12.34",
    txn_date: date = date(2024, 3, 15),
) -> str:
    """Land one Plaid row (raw sign convention) and return the canonical id."""
    db.execute(
        """
        INSERT INTO raw.plaid_transactions (
            transaction_id, account_id, transaction_date, amount, description,
            iso_currency_code, pending, source_file, source_type, source_origin,
            extracted_at, loaded_at
        ) VALUES (?, ?, ?, ?, ?, 'USD', FALSE, 'sync_1', 'plaid', ?, ?, ?)
        """,
        [
            transaction_id,
            _PLAID_ACCOUNT_KEY,
            txn_date,
            Decimal(amount),
            description,
            _ORIGIN,
            loaded_at,
            loaded_at,
        ],
    )
    return _identity_hash("plaid", _PLAID_ACCOUNT_KEY, transaction_id)


# --------------------------------------------------------------------------
# Curation + assertion helpers.
# --------------------------------------------------------------------------


def _curate(db: Database, transaction_id: str, *, suffix: str = "0001") -> None:
    """Attach one of every kind of user curation to ``transaction_id``."""
    TransactionCategoriesRepo(db).set(
        transaction_id,
        category="Food & Drink",
        subcategory="Coffee",
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    TransactionNotesRepo(db).add(
        transaction_id=transaction_id,
        note_id=f"note{suffix}0000",
        text="reimbursable",
        actor="cli",
    )
    TransactionTagsRepo(db).add(transaction_id=transaction_id, tag="work", actor="cli")
    TransactionSplitsRepo(db).insert(
        split_id=f"split{suffix}000",
        transaction_id=transaction_id,
        amount=Decimal("-12.34"),
        category="Food & Drink",
        subcategory="Coffee",
        category_id=None,
        note=None,
        ord=0,
        actor="cli",
    )


def _curation_ids(db: Database) -> dict[str, list[str]]:
    """Every transaction id each curation table currently points at."""
    return {
        table: [
            str(row[0])
            for row in db.execute(
                f"SELECT transaction_id FROM app.{table} ORDER BY transaction_id"  # noqa: S608  # code-supplied table names
            ).fetchall()
        ]
        for table in _CURATION_TABLES
    }


def _live_ids(db: Database) -> set[str]:
    return {
        str(row[0])
        for row in db.execute(
            "SELECT DISTINCT transaction_id FROM core.fct_transactions"
        ).fetchall()
    }


def _aliases(db: Database) -> dict[str, str]:
    return {
        str(old): str(new)
        for old, new in db.execute(
            "SELECT old_transaction_id, new_transaction_id "
            "FROM app.transaction_id_aliases"
        ).fetchall()
    }


def _categories_fk_status(db: Database) -> str:
    """The doctor's verdict on ``app_transaction_categories_fk``."""
    return DoctorService(db)._run_transaction_categories_fk().status  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


def _accept_dedup(
    db: Database,
    *,
    match_id: str,
    side_a: tuple[str, str],
    side_b: tuple[str, str],
) -> None:
    """Propose and confirm one dedup pair through the production entry point."""
    MatchDecisionsRepo(db).insert(
        match_id=match_id,
        source_transaction_id_a=side_a[1],
        source_type_a=side_a[0],
        source_origin_a=_ORIGIN,
        source_transaction_id_b=side_b[1],
        source_type_b=side_b[0],
        source_origin_b=_ORIGIN,
        account_id=_ACCOUNT,
        confidence_score=0.91,
        match_signals={},
        match_tier="3",
        match_status="pending",
        decided_by="auto",
        actor="system",
    )
    MatchingService(db).set_status(match_id, status="accepted", actor="cli")


class TestRevertStrandsCurationAndTheHealPassRecoversIt:
    """``revert_confirmed`` deletes the anchor's rows; the merge decision stays."""

    @pytest.mark.unit
    def test_reverting_the_anchors_import_returns_curation_to_the_live_id(
        self, pipeline_db: Database
    ) -> None:
        """Curate the CSV row, merge onto an OFX anchor, revert the OFX import.

        The canonical id flips back to the CSV row's hash, which already
        forwards away, so the append-only map cannot be corrected and the
        curation is left on an id no view serves. The healing pass walks the
        map backwards and puts all four curation kinds back.
        """
        csv_id = _load_csv_row(pipeline_db, transaction_id="csv_1234", import_id="imp1")
        assert _live_ids(pipeline_db) == {csv_id}
        _curate(pipeline_db, csv_id)

        ofx_id = _load_ofx_row(
            pipeline_db, source_transaction_id="ofx_5678", import_id="imp2"
        )
        _accept_dedup(
            pipeline_db,
            match_id="match0000001",
            side_a=("csv", "csv_1234"),
            side_b=("ofx", "ofx_5678"),
        )
        assert _live_ids(pipeline_db) == {ofx_id}, "the OFX row anchors the group"
        assert _aliases(pipeline_db) == {csv_id: ofx_id}
        assert _curation_ids(pipeline_db) == dict.fromkeys(_CURATION_TABLES, [ofx_id])

        ImportService(pipeline_db).revert_confirmed("imp2", verify=lambda _plan: None)

        assert _live_ids(pipeline_db) == {csv_id}, "the group re-anchors to the CSV row"
        assert _curation_ids(pipeline_db) == dict.fromkeys(
            _CURATION_TABLES, [ofx_id]
        ), "the curation is stranded until the healing pass runs"
        assert _categories_fk_status(pipeline_db) == "fail"

        MatchingService(pipeline_db).run(actor="system")

        assert _curation_ids(pipeline_db) == dict.fromkeys(_CURATION_TABLES, [csv_id])
        assert _categories_fk_status(pipeline_db) == "pass"
        assert _aliases(pipeline_db) == {csv_id: ofx_id}, "no alias row is appended"

    @pytest.mark.unit
    def test_a_second_pass_over_the_healed_state_writes_nothing(
        self, pipeline_db: Database
    ) -> None:
        """Idempotence: a repointed row is no longer stranded, so nothing moves."""
        csv_id = _load_csv_row(pipeline_db, transaction_id="csv_1234", import_id="imp1")
        _curate(pipeline_db, csv_id)
        _load_ofx_row(pipeline_db, source_transaction_id="ofx_5678", import_id="imp2")
        _accept_dedup(
            pipeline_db,
            match_id="match0000001",
            side_a=("csv", "csv_1234"),
            side_b=("ofx", "ofx_5678"),
        )
        ImportService(pipeline_db).revert_confirmed("imp2", verify=lambda _plan: None)
        MatchingService(pipeline_db).run(actor="system")
        healed = _curation_ids(pipeline_db)
        aliases = _aliases(pipeline_db)
        audit_rows = pipeline_db.execute(
            "SELECT COUNT(*) FROM app.audit_log"
        ).fetchone()

        MatchingService(pipeline_db).run(actor="system")

        assert (
            _curation_ids(pipeline_db)
            == healed
            == dict.fromkeys(_CURATION_TABLES, [csv_id])
        )
        assert _aliases(pipeline_db) == aliases
        assert (
            pipeline_db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
            == audit_rows
        ), "a second pass writes no audit rows because it moves nothing"


class TestPlaidRemovalStrandsCuration:
    """An ordinary Plaid sync deletes rows with no user action at all."""

    @pytest.mark.unit
    def test_removing_the_anchor_row_returns_curation_to_the_surviving_twin(
        self, pipeline_db: Database
    ) -> None:
        """Driven through ``handle_removed_transactions``, not a hand-written DELETE.

        Both members are Plaid rows, so the anchor is the earlier ``loaded_at``;
        removing it re-anchors the group onto the later twin, whose id the alias
        map already points *away* from. The walk has to follow that edge
        backwards to find the live id.
        """
        anchor_id = _load_plaid_row(
            pipeline_db,
            transaction_id="plaid_anchor",
            loaded_at=datetime(2024, 3, 16, 9, 0, 0),
        )
        twin_id = _load_plaid_row(
            pipeline_db,
            transaction_id="plaid_twin",
            loaded_at=datetime(2024, 3, 17, 9, 0, 0),
        )
        _curate(pipeline_db, twin_id)
        _accept_dedup(
            pipeline_db,
            match_id="match0000002",
            side_a=("plaid", "plaid_anchor"),
            side_b=("plaid", "plaid_twin"),
        )
        assert _live_ids(pipeline_db) == {anchor_id}
        assert _aliases(pipeline_db) == {twin_id: anchor_id}
        assert _curation_ids(pipeline_db) == dict.fromkeys(
            _CURATION_TABLES, [anchor_id]
        )

        removed = PlaidExtractor(pipeline_db).handle_removed_transactions([
            "plaid_anchor"
        ])

        assert removed == 1
        assert _live_ids(pipeline_db) == {twin_id}
        assert _categories_fk_status(pipeline_db) == "fail"

        MatchingService(pipeline_db).run(actor="system")

        assert _curation_ids(pipeline_db) == dict.fromkeys(_CURATION_TABLES, [twin_id])
        assert _categories_fk_status(pipeline_db) == "pass"
        assert _aliases(pipeline_db) == {twin_id: anchor_id}, "no alias row is appended"


class TestComponentsThatCannotBeResolved:
    """Zero or several live members: report, never guess."""

    @pytest.mark.unit
    def test_a_component_with_no_live_member_is_left_alone(
        self, pipeline_db: Database
    ) -> None:
        """Every id that ever named the transaction is gone; nothing to move to.

        A second, resolvable transaction rides along in the same run: "nothing
        moved" would otherwise be the same observation as "the pass never ran".
        """
        dead_csv_id = _load_csv_row(
            pipeline_db, transaction_id="csv_dead", import_id="imp-csv-dead"
        )
        _curate(pipeline_db, dead_csv_id, suffix="0001")
        dead_ofx_id = _load_ofx_row(
            pipeline_db, source_transaction_id="ofx_dead", import_id="imp-ofx-dead"
        )
        _accept_dedup(
            pipeline_db,
            match_id="match0000001",
            side_a=("csv", "csv_dead"),
            side_b=("ofx", "ofx_dead"),
        )

        control_csv_id = _load_csv_row(
            pipeline_db,
            transaction_id="csv_live",
            import_id="imp-csv-live",
            description="Hardware Store",
            amount="-98.76",
            txn_date=date(2024, 4, 20),
        )
        _curate(pipeline_db, control_csv_id, suffix="0002")
        _load_ofx_row(
            pipeline_db,
            source_transaction_id="ofx_live",
            import_id="imp-ofx-live",
            description="Hardware Store",
            amount="-98.76",
            txn_date=date(2024, 4, 20),
        )
        _accept_dedup(
            pipeline_db,
            match_id="match0000002",
            side_a=("csv", "csv_live"),
            side_b=("ofx", "ofx_live"),
        )

        service = ImportService(pipeline_db)
        # Both of the first transaction's imports go, so its whole component is
        # dead; only the control's anchor goes, so the control can be healed.
        service.revert_confirmed("imp-ofx-dead", verify=lambda _plan: None)
        service.revert_confirmed("imp-csv-dead", verify=lambda _plan: None)
        service.revert_confirmed("imp-ofx-live", verify=lambda _plan: None)
        assert _live_ids(pipeline_db) == {control_csv_id}

        MatchingService(pipeline_db).run(actor="system")

        assert _curation_ids(pipeline_db) == dict.fromkeys(
            _CURATION_TABLES, sorted((dead_ofx_id, control_csv_id))
        ), "the control healed in this same pass; the dead component did not move"
        assert _categories_fk_status(pipeline_db) == "fail"

    @pytest.mark.unit
    def test_a_component_with_two_live_members_is_left_alone(
        self, pipeline_db: Database
    ) -> None:
        """Two live ids have named this transaction; nothing says which owns it.

        A third transaction with an unambiguous stranded id rides along, so the
        untouched rows prove the pass ran and declined rather than never ran.
        """
        first_id = _load_csv_row(
            pipeline_db, transaction_id="csv_1234", import_id="imp1"
        )
        # Deliberately different transactions, not twins: the matcher would
        # otherwise dedup them and leave one live id, which is the case above
        # rather than the ambiguity under test here.
        second_id = _load_ofx_row(
            pipeline_db,
            source_transaction_id="ofx_5678",
            import_id="imp2",
            description="Hardware Store",
            amount="-98.76",
            txn_date=date(2024, 4, 20),
        )
        third_id = _load_plaid_row(
            pipeline_db,
            transaction_id="plaid_9012",
            loaded_at=datetime(2024, 5, 2, 9, 0, 0),
            description="Bookshop",
            amount="31.50",
            txn_date=date(2024, 5, 1),
        )
        assert _live_ids(pipeline_db) == {first_id, second_id, third_id}

        ambiguous_id = "deadbeefdeadbeef"
        resolvable_id = "cafebabecafebabe"
        _curate(pipeline_db, ambiguous_id, suffix="0001")
        _curate(pipeline_db, resolvable_id, suffix="0002")
        aliases = TransactionIdAliasesRepo(pipeline_db)
        for old, new in (
            (ambiguous_id, first_id),
            (first_id, second_id),
            (resolvable_id, third_id),
        ):
            aliases.insert(
                old_transaction_id=old, new_transaction_id=new, actor="system"
            )

        MatchingService(pipeline_db).run(actor="system")

        assert _curation_ids(pipeline_db) == dict.fromkeys(
            _CURATION_TABLES, sorted((ambiguous_id, third_id))
        ), "the unambiguous component healed; the two-live-member one did not"
        assert _categories_fk_status(pipeline_db) == "fail", (
            "the doctor reports the ambiguity rather than the pass guessing at it"
        )

    @pytest.mark.unit
    def test_a_cyclic_alias_chain_terminates_and_still_heals(
        self, pipeline_db: Database
    ) -> None:
        """The map is append-only per ``old_transaction_id``, not acyclic.

        ``a -> b`` and ``b -> a`` are two distinct primary keys, so both can be
        present. An undirected walk without a visited set would loop forever on
        that pair; the fixpoint join terminates and still finds the live id.
        """
        live_id = _load_csv_row(
            pipeline_db, transaction_id="csv_1234", import_id="imp1"
        )
        stranded_id = "deadbeefdeadbeef"
        _curate(pipeline_db, stranded_id)
        aliases = TransactionIdAliasesRepo(pipeline_db)
        aliases.insert(
            old_transaction_id=stranded_id,
            new_transaction_id=live_id,
            actor="system",
        )
        aliases.insert(
            old_transaction_id=live_id,
            new_transaction_id=stranded_id,
            actor="system",
        )

        MatchingService(pipeline_db).run(actor="system")

        assert _curation_ids(pipeline_db) == dict.fromkeys(_CURATION_TABLES, [live_id])
        assert _categories_fk_status(pipeline_db) == "pass"


class TestHealingRunsWhenTheMatcherFails:
    """``MatchingService.run`` forwards in a ``finally``; the heal must be safe there."""

    @pytest.mark.unit
    def test_a_raising_matcher_still_heals_stranded_curation(
        self, pipeline_db: Database, mocker: MockerFixture
    ) -> None:
        """A matcher run that raises leaves durable writes; the heal still lands."""
        csv_id = _load_csv_row(pipeline_db, transaction_id="csv_1234", import_id="imp1")
        _curate(pipeline_db, csv_id)
        ofx_id = _load_ofx_row(
            pipeline_db, source_transaction_id="ofx_5678", import_id="imp2"
        )
        _accept_dedup(
            pipeline_db,
            match_id="match0000001",
            side_a=("csv", "csv_1234"),
            side_b=("ofx", "ofx_5678"),
        )
        ImportService(pipeline_db).revert_confirmed("imp2", verify=lambda _plan: None)
        assert _curation_ids(pipeline_db)["transaction_categories"] == [ofx_id]

        def _explode(*_args: object, **_kwargs: object) -> None:
            raise MatchRunError(RuntimeError("tier 4 blew up"), partial=MatchResult())

        mocker.patch.object(matching_service.TransactionMatcher, "run", _explode)
        with pytest.raises(MatchRunError):
            MatchingService(pipeline_db).run(actor="system")

        assert _curation_ids(pipeline_db) == dict.fromkeys(_CURATION_TABLES, [csv_id])
        assert _categories_fk_status(pipeline_db) == "pass"


class TestHealingSkipsBeforeTheFactViewExists:
    """A first load precedes the transform that builds ``core.fct_transactions``."""

    @pytest.mark.unit
    def test_the_pass_skips_cleanly_when_the_fact_view_is_absent(
        self, db: Database
    ) -> None:
        """The catalog is asked, not the view: reaching the view would raise.

        Only the prep chain exists here, which is the state after a first load
        and before the first ``transform apply``. Curation sits on an id that no
        view can confirm, and the pass must neither raise nor touch it — the
        query it would run names ``core.fct_transactions``, so completing
        without a ``CatalogException`` is what proves the guard fired.
        """
        db.execute("CREATE SCHEMA IF NOT EXISTS prep")
        for name in _PREP_MODELS:
            db.execute(  # noqa: S608  # model body read from the repo file
                f"CREATE OR REPLACE VIEW prep.{name} AS\n{_model_body('prep', name)}"
            )
        _curate(db, "deadbeefdeadbeef")

        result = forward_rekeyed_transaction_ids(db, actor="system")

        assert result == AliasForwardResult()
        assert _curation_ids(db) == dict.fromkeys(
            _CURATION_TABLES, ["deadbeefdeadbeef"]
        )


# --------------------------------------------------------------------------
# Split-collision helpers — the fact view's split expansion is the consumer
# that turns a merged allocation into double-counted money.
# --------------------------------------------------------------------------


def _add_split(
    db: Database, transaction_id: str, *, split_id: str, amount: str
) -> None:
    """Attach one split covering ``amount`` to ``transaction_id``."""
    TransactionSplitsRepo(db).insert(
        split_id=split_id,
        transaction_id=transaction_id,
        amount=Decimal(amount),
        category="Food & Drink",
        subcategory="Coffee",
        category_id=None,
        note=None,
        ord=0,
        actor="cli",
    )


def _splits_by_transaction(db: Database) -> dict[str, list[str]]:
    """Every split id currently attached to each transaction id."""
    grouped: dict[str, list[str]] = {}
    for transaction_id, split_id in db.execute(
        "SELECT transaction_id, split_id FROM app.transaction_splits "
        "ORDER BY transaction_id, split_id"
    ).fetchall():
        grouped.setdefault(str(transaction_id), []).append(str(split_id))
    return grouped


def _build_transaction_lines_view(db: Database) -> None:
    """Add the shipped split-expansion view on top of the fact model."""
    db.execute(
        "CREATE OR REPLACE VIEW core.fct_transaction_lines AS\n"  # noqa: S608  # model body read from the repo file
        f"{_model_body('core', 'fct_transaction_lines')}"
    )


def _published_line_total(db: Database) -> Decimal:
    """What ``core.fct_transaction_lines`` publishes as the sum of every line."""
    row = db.execute(
        "SELECT COALESCE(SUM(line_amount), 0) FROM core.fct_transaction_lines"
    ).fetchone()
    assert row is not None
    return Decimal(str(row[0]))


def _splits_fk_status(db: Database) -> str:
    """The doctor's verdict on ``app_transaction_splits_fk``."""
    return DoctorService(db)._run_transaction_splits_fk().status  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


class TestSplitsAreNeverMovedOntoAnAlreadySplitTransaction:
    """Two complete allocations must never become one transaction's union.

    ``core.fct_transaction_lines`` drops the whole-transaction row as soon as a
    transaction has any split, so a union of two full allocations publishes
    double the real amount to every spending report. Moving nothing is the only
    option that neither double-counts nor destroys a curation the user entered.
    """

    @pytest.mark.unit
    def test_both_sides_fully_split_leaves_the_superseded_allocation_in_place(
        self, pipeline_db: Database
    ) -> None:
        """The survivor keeps exactly its own splits; the merge publishes -12.34."""
        csv_id = _load_csv_row(pipeline_db, transaction_id="csv_1234", import_id="imp1")
        _add_split(pipeline_db, csv_id, split_id="splitcsv0001", amount="-12.34")
        ofx_id = _load_ofx_row(
            pipeline_db, source_transaction_id="ofx_5678", import_id="imp2"
        )
        _add_split(pipeline_db, ofx_id, split_id="splitofx0001", amount="-12.34")
        _build_transaction_lines_view(pipeline_db)

        _accept_dedup(
            pipeline_db,
            match_id="match0000001",
            side_a=("csv", "csv_1234"),
            side_b=("ofx", "ofx_5678"),
        )

        assert _live_ids(pipeline_db) == {ofx_id}, "the OFX row anchors the group"
        # One transaction of -12.34 exists; the ledger must publish -12.34, not
        # the -24.68 a unioned pair of complete allocations would.
        assert _published_line_total(pipeline_db) == Decimal("-12.34")
        assert _splits_by_transaction(pipeline_db) == {
            csv_id: ["splitcsv0001"],
            ofx_id: ["splitofx0001"],
        }
        # The refused split now sits on an id core.fct_transactions does not
        # carry — invisible unless the doctor reports it.
        assert _splits_fk_status(pipeline_db) == "fail"

    @pytest.mark.unit
    def test_an_unsplit_destination_still_receives_the_move(
        self, pipeline_db: Database
    ) -> None:
        """Guard against over-correction: the ordinary forward still happens."""
        csv_id = _load_csv_row(pipeline_db, transaction_id="csv_1234", import_id="imp1")
        _add_split(pipeline_db, csv_id, split_id="splitcsv0001", amount="-12.34")
        ofx_id = _load_ofx_row(
            pipeline_db, source_transaction_id="ofx_5678", import_id="imp2"
        )
        _build_transaction_lines_view(pipeline_db)

        _accept_dedup(
            pipeline_db,
            match_id="match0000002",
            side_a=("csv", "csv_1234"),
            side_b=("ofx", "ofx_5678"),
        )

        assert _splits_by_transaction(pipeline_db) == {ofx_id: ["splitcsv0001"]}
        assert _published_line_total(pipeline_db) == Decimal("-12.34")
        assert _splits_fk_status(pipeline_db) == "pass"

    @pytest.mark.unit
    def test_the_heal_pass_inherits_the_refusal(self, pipeline_db: Database) -> None:
        """A deliberately stranded split must not be moved later by the self-heal.

        The heal pass calls the same repo method, so it can re-introduce the
        double-count on its own schedule if the refusal lives only in the
        forwarding derivation. Here the user re-splits the live transaction
        while a split sits stranded on the superseded id; every other kind of
        curation heals and the split alone stays put.
        """
        csv_id = _load_csv_row(pipeline_db, transaction_id="csv_1234", import_id="imp1")
        _curate(pipeline_db, csv_id)
        ofx_id = _load_ofx_row(
            pipeline_db, source_transaction_id="ofx_5678", import_id="imp2"
        )
        _accept_dedup(
            pipeline_db,
            match_id="match0000003",
            side_a=("csv", "csv_1234"),
            side_b=("ofx", "ofx_5678"),
        )
        assert _splits_by_transaction(pipeline_db) == {ofx_id: ["split0001000"]}

        # Reverting the anchor's import re-anchors the group onto the CSV row,
        # stranding every curation on the OFX id the map already forwards to.
        ImportService(pipeline_db).revert_confirmed("imp2", verify=lambda _plan: None)
        assert _live_ids(pipeline_db) == {csv_id}
        # The user splits the live transaction while the old allocation is
        # stranded, so the heal's destination is already fully allocated.
        _add_split(pipeline_db, csv_id, split_id="splitlive001", amount="-12.34")
        _build_transaction_lines_view(pipeline_db)

        MatchingService(pipeline_db).run(actor="system")

        assert _curation_ids(pipeline_db)["transaction_categories"] == [csv_id]
        assert _curation_ids(pipeline_db)["transaction_notes"] == [csv_id]
        assert _curation_ids(pipeline_db)["transaction_tags"] == [csv_id]
        assert _splits_by_transaction(pipeline_db) == {
            csv_id: ["splitlive001"],
            ofx_id: ["split0001000"],
        }
        assert _published_line_total(pipeline_db) == Decimal("-12.34")
