"""Tests for the tabular import service layer."""

from pathlib import Path
from unittest.mock import patch

import pytest
from _pytest.logging import LogCaptureFixture

from moneybin.database import Database
from moneybin.services.import_service import (
    _detect_file_type,  # type: ignore[reportPrivateUsage]  # testing private function
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXTURES = Path(__file__).parents[2] / "fixtures" / "tabular"
_STANDARD_CSV = _FIXTURES / "standard.csv"  # high-confidence (Date,Description,Amount)
_CHASE_CSV = _FIXTURES / "chase_credit.csv"  # high-confidence known format
_CITI_CSV = (
    _FIXTURES / "citi_credit.csv"
)  # split debit/credit (Status,Date,Description,Debit,Credit,Member Name)


def _make_mapping_result(
    *,
    score: float,
    confidence: str,
    field_mapping: dict[str, str] | None = None,
    sign_needs_confirmation: bool = False,
    sign_convention: str = "negative_is_expense",
    sign_evidence_header: str | None = None,
) -> object:
    """Return a MappingResult-like object with the given confidence and score."""
    from moneybin.extractors.tabular.column_mapper import MappingResult

    if field_mapping is None:
        field_mapping = {
            "transaction_date": "Date",
            "amount": "Amount",
            "description": "Description",
        }
    return MappingResult(
        field_mapping=field_mapping,
        confidence=confidence,  # type: ignore[arg-type]
        date_format="%Y-%m-%d",
        number_format="us",
        sign_convention=sign_convention,  # type: ignore[arg-type]  # test fixture accepts every supported convention
        sign_needs_confirmation=sign_needs_confirmation,
        sign_evidence_header=sign_evidence_header,
        is_multi_account=False,
        unmapped_columns=["Balance"],
        flagged_fields=[],
        sample_values={"transaction_date": ["2026-01-05"], "amount": ["-52.30"]},
        score=score,
        missing_required=(),
    )


class TestDetectFileType:
    """Test that file extensions are detected correctly."""

    def test_csv_detected(self) -> None:
        assert _detect_file_type(Path("test.csv")) == "tabular"

    def test_tsv_detected(self) -> None:
        assert _detect_file_type(Path("test.tsv")) == "tabular"

    def test_xlsx_detected(self) -> None:
        assert _detect_file_type(Path("test.xlsx")) == "tabular"

    def test_parquet_detected(self) -> None:
        assert _detect_file_type(Path("test.parquet")) == "tabular"

    def test_feather_detected(self) -> None:
        assert _detect_file_type(Path("test.feather")) == "tabular"

    def test_txt_detected(self) -> None:
        assert _detect_file_type(Path("test.txt")) == "tabular"

    def test_ofx_still_works(self) -> None:
        assert _detect_file_type(Path("test.ofx")) == "ofx"

    def test_pdf_detected(self) -> None:
        """PDF is supported via the seed import path (Phase 1)."""
        assert _detect_file_type(Path("test.pdf")) == "pdf"

    def test_unsupported_extension_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported file type"):
            _detect_file_type(Path("test.jpg"))


def test_resolved_mapping_round_trip() -> None:
    """ResolvedMapping is constructible and exposes the resolved tabular fields."""
    from moneybin.services.import_service import ResolvedMapping

    rm = ResolvedMapping(
        field_mapping={"transaction_date": "Date", "amount": "Amt"},
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        is_multi_account=False,
        confidence="high",
    )
    assert rm.field_mapping["amount"] == "Amt"
    assert rm.sign_convention == "negative_is_expense"
    # Frozen — assignment must raise
    import dataclasses

    try:
        rm.confidence = "low"  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        pass
    else:
        raise AssertionError("ResolvedMapping must be frozen")


@pytest.mark.parametrize(
    ("plan_overrides", "message"),
    [
        (
            {"header_signature": ["Amount", "Date", "Wrong"]},
            "header signature",
        ),
        ({"rows_in_file": 3}, "row accounting"),
        (
            {
                "field_mapping": {
                    "transaction_date": "Date",
                    "amount": "Missing",
                    "description": "Description",
                }
            },
            "unavailable columns",
        ),
    ],
)
def test_reviewed_plan_rejects_parse_or_mapping_drift(
    db: Database,
    tmp_path: Path,
    plan_overrides: dict[str, object],
    message: str,
) -> None:
    import polars as pl

    from moneybin.errors import UserError
    from moneybin.services.import_service import ImportService, ReviewedTabularPlan

    csv_file = tmp_path / "reviewed.csv"
    csv_file.write_text(
        "Date,Description,Amount\n2026-01-05,Coffee,-4.75\n",
        encoding="utf-8",
    )
    plan_kwargs: dict[str, object] = {
        "file_type": "csv",
        "delimiter": ",",
        "encoding": "utf-8",
        "file_size": csv_file.stat().st_size,
        "field_mapping": {
            "transaction_date": "Date",
            "amount": "Amount",
            "description": "Description",
        },
        "date_format": "%Y-%m-%d",
        "sign_convention": "negative_is_expense",
        "number_format": "us",
        "is_multi_account": False,
        "confidence": "high",
        "skip_rows": 0,
        "has_header": True,
        "rows_in_file": 2,
        "rows_skipped_trailing": 0,
        "header_row_looks_like_data": False,
        "header_signature": ["Amount", "Date", "Description"],
    }
    plan_kwargs.update(plan_overrides)
    reviewed_plan = ReviewedTabularPlan(**plan_kwargs)  # type: ignore[arg-type]  # parametrized valid dataclass fields
    read_result = type(
        "ReadResult",
        (),
        {
            "df": pl.DataFrame({
                "Date": ["2026-01-05"],
                "Description": ["Coffee"],
                "Amount": ["-4.75"],
            }),
            "rows_in_file": 2,
        },
    )()

    with (
        patch(
            "moneybin.extractors.tabular.readers.read_file",
            return_value=read_result,
        ),
        pytest.raises(UserError, match=message) as exc,
    ):
        ImportService(db).import_file(
            csv_file,
            reviewed_plan=reviewed_plan,
            refresh=False,
            save_format=False,
        )

    assert exc.value.code == "IMPORT_PREVIEW_PLAN_MISMATCH"


def test_reimport_writes_single_accepted_source_native_link(
    db: Database,
) -> None:
    """Re-importing the same single-account CSV is idempotent in app.account_links.

    The account block now routes through AccountResolver for its side effect
    (the native->canonical source_native mapping). On the second import the
    resolver's strong-confirmer step adopts the existing link instead of
    minting a new one, so exactly one accepted source_native row persists for
    (source_type, source_origin, ref_value) — the wiring-level idempotency
    guarantee. confirm=True bypasses the first-encounter mapping gate;
    refresh=False skips the SQLMesh apply (no core.dim_accounts needed here).
    """
    from moneybin.services.import_service import ImportService

    svc = ImportService(db)
    for _ in range(2):
        result = svc.import_file(
            _STANDARD_CSV,
            account_name="Reimport Test",
            refresh=False,
            confirm=True,
            auto_accept=True,
        )
        assert result.import_id is not None

    # slugify("Reimport Test") is the native key; source_origin falls back
    # to the same slug when no registered format matched.
    row = db.execute(
        """
        SELECT COUNT(*) FROM app.account_links
        WHERE status = 'accepted' AND ref_kind = 'source_native'
          AND source_type = 'csv' AND ref_value = ?
        """,
        ["reimport-test"],
    ).fetchone()
    assert row is not None and row[0] == 1


def test_single_account_csv_captures_last4_from_label(
    db: Database,
) -> None:
    """Parsed last4 from account label lands in raw.tabular_accounts.account_number_masked.

    A single-account CSV imported with an account label embedding the last 4
    lands ****NNNN in raw.tabular_accounts.account_number_masked (Decision 8
    capture), so dim_accounts can derive last_four even before any user edit.
    """
    from moneybin.services.import_service import ImportService

    svc = ImportService(db)
    svc.import_file(
        _STANDARD_CSV,
        account_name="WF Checking (...4267)",
        refresh=False,
        confirm=True,
        auto_accept=True,
    )
    masked = db.execute(
        """
        SELECT account_number_masked FROM raw.tabular_accounts
        WHERE source_type IN ('csv', 'tsv', 'excel')
        """
    ).fetchone()
    assert masked is not None and masked[0] == "****4267", masked


# ---------------------------------------------------------------------------
# TestTabularConfirmationFlow
# ---------------------------------------------------------------------------


class TestTabularConfirmationFlow:
    """Verify that _import_tabular surfaces ImportConfirmationRequiredError.

    Each test patches map_columns to inject a controlled MappingResult so
    the service logic under test is the resolve_or_confirm routing, not the
    detection heuristics.
    """

    def test_low_confidence_raises_confirmation_required(self, db: Database) -> None:
        """Low-tier detection must raise ImportConfirmationRequiredError."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        low_result = _make_mapping_result(score=0.3, confidence="low")
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=low_result,
        ):
            with pytest.raises(ImportConfirmationRequiredError) as exc_info:
                ImportService(db).import_file(
                    _STANDARD_CSV, account_name="test", refresh=False
                )
        assert exc_info.value.outcome.channel == "tabular"
        assert exc_info.value.outcome.confidence.tier == "low"

    def test_medium_confidence_now_gates(self, db: Database) -> None:
        """Medium-tier no longer waves through; must raise ImportConfirmationRequiredError."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        med_result = _make_mapping_result(score=0.75, confidence="medium")
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=med_result,
        ):
            with pytest.raises(ImportConfirmationRequiredError) as exc_info:
                ImportService(db).import_file(
                    _STANDARD_CSV, account_name="test", refresh=False
                )
        assert exc_info.value.outcome.confidence.tier == "medium"

    def test_high_confidence_human_still_gates(self, db: Database) -> None:
        """High-tier, human caller, no signal -> ConfirmationRequired (first encounter)."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        high_result = _make_mapping_result(score=0.95, confidence="high")
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=high_result,
        ):
            with pytest.raises(ImportConfirmationRequiredError):
                ImportService(db).import_file(
                    _STANDARD_CSV, account_name="test", refresh=False
                )

    def test_agent_actor_kind_no_self_accept_when_gate_closed(
        self, db: Database
    ) -> None:
        """actor_kind='agent' with self_accept_high=False still surfaces."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        high_result = _make_mapping_result(score=0.95, confidence="high")
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=high_result,
        ):
            with pytest.raises(ImportConfirmationRequiredError):
                ImportService(db).import_file(
                    _STANDARD_CSV,
                    account_name="test",
                    refresh=False,
                    actor_kind="agent",
                )

    def test_agent_self_accepts_when_gate_open(
        self,
        db: Database,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """actor_kind='agent' + self_accept_high=True via settings + high -> data loads.

        Exercises the real settings → self_accept_enabled wiring (not by patching
        resolve_or_confirm) so a config misconfiguration would surface here.
        """
        from moneybin import config as config_module
        from moneybin.config import (
            clear_settings_cache,
            get_settings,
            set_current_profile,
        )
        from moneybin.services.import_service import ImportService

        monkeypatch.setenv("MONEYBIN_IMPORT___SELF_ACCEPT_HIGH", "true")
        clear_settings_cache()
        monkeypatch.setattr(config_module, "_current_profile", None)
        monkeypatch.setattr(config_module, "_current_settings", None)
        set_current_profile("test")
        assert get_settings().import_.self_accept_high is True

        high_result = _make_mapping_result(score=0.95, confidence="high")
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=high_result,
        ):
            result = ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                actor_kind="agent",
            )
        assert result.import_id is not None

    def test_confirm_true_accepts_high(self, db: Database) -> None:
        """confirm=True acts as Accept signal; Resolved -> data loads."""
        from moneybin.services.import_service import ImportService

        high_result = _make_mapping_result(score=0.95, confidence="high")
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=high_result,
        ):
            result = ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
            )
        assert result.import_id is not None

    def test_partial_mapping_override_loads(self, db: Database) -> None:
        """overrides= acts as Override signal; partial-merge resolves -> data loads."""
        from moneybin.services.import_service import ImportService

        med_result = _make_mapping_result(score=0.75, confidence="medium")
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=med_result,
        ):
            result = ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                overrides={"description": "Description"},
            )
        assert result.import_id is not None

    def test_split_debit_credit_passes_required_fields_validation(
        self, db: Database
    ) -> None:
        """Layouts with debit_amount + credit_amount (no single 'amount') must validate.

        _score_mapping treats debit_amount + credit_amount as satisfying the
        amount requirement (returns score=1.0), so _import_tabular must pass
        the matching required_fields tuple to resolve_or_confirm instead of
        the literal ('transaction_date', 'amount', 'description') — otherwise
        the validator rejects the mapping the scorer just blessed.
        """
        from moneybin.services.import_service import ImportService

        # citi_credit.csv: Status,Date,Description,Debit,Credit,Member Name
        split_result = _make_mapping_result(
            score=1.0,
            confidence="high",
            field_mapping={
                "transaction_date": "Date",
                "debit_amount": "Debit",
                "credit_amount": "Credit",
                "description": "Description",
            },
        )
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=split_result,
        ):
            result = ImportService(db).import_file(
                _CITI_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
            )
        assert result.import_id is not None

    def test_sign_convention_warning_still_present(
        self,
        db: Database,
        caplog: LogCaptureFixture,
    ) -> None:
        """Sign-convention warning still fires when sign is ambiguous (confirm=True path)."""
        from moneybin.services.import_service import ImportService

        high_ambig = _make_mapping_result(
            score=0.95, confidence="high", sign_needs_confirmation=True
        )
        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=high_ambig,
            ),
            caplog.at_level("WARNING"),
        ):
            ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
            )
        assert (
            "sign convention" in caplog.text.lower()
            or "ambiguous" in caplog.text.lower()
        )

    def test_agent_cannot_confirm_an_inferred_credit_card_inversion(
        self, db: Database
    ) -> None:
        """A generic MCP accept signal cannot ratify a whole-ledger flip."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
            SignConventionProposal,
        )
        from moneybin.services.import_service import ImportService

        inverted = _make_mapping_result(
            score=0.95,
            confidence="high",
            sign_convention="negative_is_income",
            sign_needs_confirmation=True,
            sign_evidence_header="Transaction Credit",
        )
        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=inverted,
            ),
            pytest.raises(ImportConfirmationRequiredError) as exc,
        ):
            ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
                actor_kind="agent",
            )

        assert exc.value.outcome.channel == "tabular"
        assert exc.value.outcome.reason == "sign_convention"
        from moneybin.services.import_confirmation import confirmation_payload_dict

        assert confirmation_payload_dict(exc.value.outcome)["sign_evidence"] == [
            "Transaction Credit"
        ]
        assert isinstance(exc.value.outcome.proposed, SignConventionProposal)
        assert exc.value.outcome.proposed.sign_convention == "negative_is_income"
        rows = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
        assert rows is not None and rows[0] == 0

    def test_mapping_accept_does_not_confirm_an_inferred_credit_card_inversion(
        self, db: Database
    ) -> None:
        """The mapping accept signal cannot silently ratify the sign flip."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        inverted = _make_mapping_result(
            score=0.95,
            confidence="high",
            sign_convention="negative_is_income",
            sign_needs_confirmation=True,
        )
        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=inverted,
            ),
            pytest.raises(ImportConfirmationRequiredError) as exc,
        ):
            ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
            )

        assert exc.value.outcome.reason == "sign_convention"

    def test_tabular_card_requires_mapping_then_sign_confirmation(
        self, db: Database
    ) -> None:
        """The real three-step retry flow keeps the two decisions separate."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        inverted = _make_mapping_result(
            score=0.95,
            confidence="high",
            sign_convention="negative_is_income",
            sign_needs_confirmation=True,
        )
        service = ImportService(db)
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=inverted,
        ):
            with pytest.raises(ImportConfirmationRequiredError) as mapping:
                service.import_file(_STANDARD_CSV, account_name="test", refresh=False)
            with pytest.raises(ImportConfirmationRequiredError) as sign:
                service.import_file(
                    _STANDARD_CSV,
                    account_name="test",
                    refresh=False,
                    confirm=True,
                )
            result = service.import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
                human_sign_confirmation=True,
            )

        assert mapping.value.outcome.reason == "unknown_layout"
        assert sign.value.outcome.reason == "sign_convention"
        assert result.import_id is not None

    def test_human_can_confirm_an_inferred_credit_card_inversion(
        self, db: Database
    ) -> None:
        """A separate human sign confirmation permits the already-accepted mapping."""
        from moneybin.services.import_service import ImportService

        inverted = _make_mapping_result(
            score=0.95,
            confidence="high",
            sign_convention="negative_is_income",
            sign_needs_confirmation=True,
        )
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=inverted,
        ):
            result = ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
                human_sign_confirmation=True,
            )

        assert result.import_id is not None

    def test_inferred_sign_proposal_metric_is_buffered(
        self,
        db: Database,
    ) -> None:
        from moneybin.metrics.observations import MetricObservations
        from moneybin.metrics.registry import TABULAR_SIGN_GATE_TOTAL
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        inverted = _make_mapping_result(
            score=0.95,
            confidence="high",
            sign_convention="negative_is_income",
            sign_needs_confirmation=True,
        )
        observations = MetricObservations()
        metric = TABULAR_SIGN_GATE_TOTAL.labels(outcome="proposed")
        before = metric._value.get()  # type: ignore[reportPrivateUsage]
        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=inverted,
            ),
            pytest.raises(ImportConfirmationRequiredError),
        ):
            ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
                emit_metrics=False,
                observations=observations,
            )

        assert metric._value.get() == before  # type: ignore[reportPrivateUsage]
        observations.flush("rollback")
        assert metric._value.get() == before + 1  # type: ignore[reportPrivateUsage]

    def test_explicit_sign_override_loads_and_records_gate_metric(
        self, db: Database
    ) -> None:
        """A CLI-level explicit sign choice bypasses the inferred-sign proposal."""
        from moneybin.metrics.registry import TABULAR_SIGN_GATE_TOTAL
        from moneybin.services.import_service import ImportService

        inverted = _make_mapping_result(
            score=0.95,
            confidence="high",
            sign_convention="negative_is_income",
            sign_needs_confirmation=True,
        )
        before = TABULAR_SIGN_GATE_TOTAL.labels(outcome="overridden")._value.get()  # type: ignore[reportPrivateUsage]
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=inverted,
        ):
            result = ImportService(db).import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
                sign="negative_is_expense",
            )

        assert result.import_id is not None
        assert (
            TABULAR_SIGN_GATE_TOTAL.labels(outcome="overridden")._value.get()  # type: ignore[reportPrivateUsage]  # testing prometheus internals
            == before + 1
        )

    def test_single_amount_mapping_rejects_split_sign_before_batch(
        self, db: Database, tmp_path: Path
    ) -> None:
        """A split override cannot reach a transform that only has ``amount``."""
        from moneybin.errors import UserError
        from moneybin.services.import_service import ImportService

        csv_file = tmp_path / "single.csv"
        csv_file.write_text(
            "Date,Description,Amount\n2026-01-05,Coffee,-4.75\n",
            encoding="utf-8",
        )
        single_result = _make_mapping_result(score=1.0, confidence="high")

        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=single_result,
            ),
            pytest.raises(UserError, match="single amount column") as exc,
        ):
            ImportService(db).import_file(
                csv_file,
                account_id="acct-single",
                refresh=False,
                confirm=True,
                sign="split_debit_credit",
                save_format=False,
            )

        assert exc.value.code == "invalid_sign_convention"
        assert "--sign negative_is_expense" in exc.value.message
        log_rows = db.execute("SELECT COUNT(*) FROM raw.import_log").fetchone()
        assert log_rows is not None and log_rows[0] == 0

    @pytest.mark.parametrize("sign", ["negative_is_expense", "negative_is_income"])
    def test_split_mapping_rejects_single_sign_before_batch(
        self, db: Database, tmp_path: Path, sign: str
    ) -> None:
        """Single-column conventions cannot finalize a split mapping as rejected."""
        from moneybin.errors import UserError
        from moneybin.services.import_service import ImportService

        csv_file = tmp_path / "split.csv"
        csv_file.write_text(
            "Date,Description,Debit,Credit\n2026-01-05,Coffee,4.75,\n",
            encoding="utf-8",
        )
        split_result = _make_mapping_result(
            score=1.0,
            confidence="high",
            field_mapping={
                "transaction_date": "Date",
                "debit_amount": "Debit",
                "credit_amount": "Credit",
                "description": "Description",
            },
            sign_convention="split_debit_credit",
        )

        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=split_result,
            ),
            pytest.raises(UserError, match="debit/credit pair") as exc,
        ):
            ImportService(db).import_file(
                csv_file,
                account_id="acct-split",
                refresh=False,
                confirm=True,
                sign=sign,
                save_format=False,
            )

        assert exc.value.code == "invalid_sign_convention"
        assert "--sign split_debit_credit" in exc.value.message
        log_rows = db.execute("SELECT COUNT(*) FROM raw.import_log").fetchone()
        assert log_rows is not None and log_rows[0] == 0

    @pytest.mark.parametrize(
        ("columns", "row", "mapping", "sign"),
        [
            (
                "Date,Description,Amount",
                "2026-01-05,Coffee,-4.75",
                {
                    "transaction_date": "Date",
                    "amount": "Amount",
                    "description": "Description",
                },
                "negative_is_expense",
            ),
            (
                "Date,Description,Amount",
                "2026-01-05,Coffee,4.75",
                {
                    "transaction_date": "Date",
                    "amount": "Amount",
                    "description": "Description",
                },
                "negative_is_income",
            ),
            (
                "Date,Description,Debit,Credit",
                "2026-01-05,Coffee,4.75,",
                {
                    "transaction_date": "Date",
                    "debit_amount": "Debit",
                    "credit_amount": "Credit",
                    "description": "Description",
                },
                "split_debit_credit",
            ),
        ],
    )
    def test_explicit_sign_matching_mapping_shape_loads(
        self,
        db: Database,
        tmp_path: Path,
        columns: str,
        row: str,
        mapping: dict[str, str],
        sign: str,
    ) -> None:
        """Every explicit convention still loads when its required columns exist."""
        from moneybin.services.import_service import ImportService

        csv_file = tmp_path / "matching.csv"
        csv_file.write_text(f"{columns}\n{row}\n", encoding="utf-8")
        mapping_result = _make_mapping_result(
            score=1.0,
            confidence="high",
            field_mapping=mapping,
            sign_convention=sign,
        )

        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=mapping_result,
        ):
            result = ImportService(db).import_file(
                csv_file,
                account_id="acct-matching",
                refresh=False,
                confirm=True,
                sign=sign,
                save_format=False,
            )

        assert result.rows_loaded == 1
        log_row = db.execute(
            "SELECT status, rows_imported, rows_rejected FROM raw.import_log "
            "WHERE import_id = ?",
            [result.import_id],
        ).fetchone()
        assert log_row == ("complete", 1, 0)

    def test_confirmed_credit_card_format_replays_without_confirmation(
        self, db: Database
    ) -> None:
        """A human-confirmed format is trusted on each later statement."""
        from moneybin.services.import_service import ImportService

        inverted = _make_mapping_result(
            score=0.95,
            confidence="high",
            sign_convention="negative_is_income",
            sign_needs_confirmation=True,
        )
        service = ImportService(db)
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=inverted,
        ):
            first = service.import_file(
                _STANDARD_CSV,
                account_name="test",
                refresh=False,
                confirm=True,
                human_sign_confirmation=True,
            )

        replay = service.import_file(
            _STANDARD_CSV,
            account_name="test",
            refresh=False,
        )

        assert first.import_id is not None
        assert replay.import_id is not None
