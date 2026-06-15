"""Tests for the tabular import service layer."""

from pathlib import Path
from unittest.mock import MagicMock, patch

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
        sign_convention="negative_is_expense",
        sign_needs_confirmation=sign_needs_confirmation,
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


def test_reimport_writes_single_accepted_source_native_link(
    mock_secret_store: MagicMock, tmp_path: Path
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

    db = Database(
        tmp_path / "reimport.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    try:
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
    finally:
        db.close()


# ---------------------------------------------------------------------------
# TestTabularConfirmationFlow
# ---------------------------------------------------------------------------


class TestTabularConfirmationFlow:
    """Verify that _import_tabular surfaces ImportConfirmationRequiredError.

    Each test patches map_columns to inject a controlled MappingResult so
    the service logic under test is the resolve_or_confirm routing, not the
    detection heuristics.
    """

    def _make_db(self, mock_secret_store: MagicMock, tmp_path: Path) -> Database:
        return Database(
            tmp_path / "conf_flow.duckdb",
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
            read_only=False,
        )

    def test_low_confidence_raises_confirmation_required(
        self, mock_secret_store: MagicMock, tmp_path: Path
    ) -> None:
        """Low-tier detection must raise ImportConfirmationRequiredError."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()

    def test_medium_confidence_now_gates(
        self, mock_secret_store: MagicMock, tmp_path: Path
    ) -> None:
        """Medium-tier no longer waves through; must raise ImportConfirmationRequiredError."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()

    def test_high_confidence_human_still_gates(
        self, mock_secret_store: MagicMock, tmp_path: Path
    ) -> None:
        """High-tier, human caller, no signal -> ConfirmationRequired (first encounter)."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
            high_result = _make_mapping_result(score=0.95, confidence="high")
            with patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=high_result,
            ):
                with pytest.raises(ImportConfirmationRequiredError):
                    ImportService(db).import_file(
                        _STANDARD_CSV, account_name="test", refresh=False
                    )
        finally:
            db.close()

    def test_agent_actor_kind_no_self_accept_when_gate_closed(
        self, mock_secret_store: MagicMock, tmp_path: Path
    ) -> None:
        """actor_kind='agent' with self_accept_high=False still surfaces."""
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()

    def test_agent_self_accepts_when_gate_open(
        self,
        mock_secret_store: MagicMock,
        tmp_path: Path,
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

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()

    def test_confirm_true_accepts_high(
        self, mock_secret_store: MagicMock, tmp_path: Path
    ) -> None:
        """confirm=True acts as Accept signal; Resolved -> data loads."""
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()

    def test_partial_mapping_override_loads(
        self, mock_secret_store: MagicMock, tmp_path: Path
    ) -> None:
        """overrides= acts as Override signal; partial-merge resolves -> data loads."""
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()

    def test_split_debit_credit_passes_required_fields_validation(
        self, mock_secret_store: MagicMock, tmp_path: Path
    ) -> None:
        """Layouts with debit_amount + credit_amount (no single 'amount') must validate.

        _score_mapping treats debit_amount + credit_amount as satisfying the
        amount requirement (returns score=1.0), so _import_tabular must pass
        the matching required_fields tuple to resolve_or_confirm instead of
        the literal ('transaction_date', 'amount', 'description') — otherwise
        the validator rejects the mapping the scorer just blessed.
        """
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()

    def test_sign_convention_warning_still_present(
        self,
        mock_secret_store: MagicMock,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ) -> None:
        """Sign-convention warning still fires when sign is ambiguous (confirm=True path)."""
        from moneybin.services.import_service import ImportService

        db = self._make_db(mock_secret_store, tmp_path)
        try:
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
        finally:
            db.close()
