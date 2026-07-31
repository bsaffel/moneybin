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
_TILLER_CSV = _FIXTURES / "tiller.csv"  # headers match the built-in `tiller` format


def _make_mapping_result(
    *,
    score: float,
    confidence: str,
    field_mapping: dict[str, str] | None = None,
    sign_needs_confirmation: bool = False,
    sign_convention: str = "negative_is_expense",
    sign_evidence_header: str | None = None,
    date_format: str | None = "%Y-%m-%d",
) -> object:
    """Return a MappingResult-like object with the given confidence and score.

    ``date_format=None`` is the detector saying it never read the date column —
    the case that used to be papered over with a fabricated ``"%Y-%m-%d"``.
    """
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
        date_format=date_format,
        number_format="us",
        sign_convention=sign_convention,  # type: ignore[arg-type]  # test fixture accepts every supported convention
        sign_needs_confirmation=sign_needs_confirmation,
        sign_evidence_header=sign_evidence_header,
        is_multi_account=False,
        unmapped_columns=["Balance"],
        flagged_fields=[],
        sample_values={
            "transaction_date": ["2026-01-05"],
            "amount": ["-52.30"],
        },
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
        "flagged_fields": [],
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

    assert exc.value.code == "import_preview_plan_mismatch"


def test_a_missing_required_field_outranks_an_unreadable_date(
    db: Database, tmp_path: Path
) -> None:
    """The reason must name a cause the caller's next action can answer.

    Both unreadable_date actions only remap the date column or supply a
    format. When another required destination is still missing, neither
    supplies it, so the caller corrects the date, re-previews, and is refused
    again — a loop with no instruction that resolves it. unknown_layout's hint
    asks for the mapping actually needed.
    """
    import polars as pl

    from moneybin.services.import_confirmation import ImportConfirmationRequiredError
    from moneybin.services.import_service import ImportService, ReviewedTabularPlan

    csv_file = tmp_path / "no_amount.csv"
    csv_file.write_text("Date,Description\nnot-a-date,Coffee\n", encoding="utf-8")
    reviewed_plan = ReviewedTabularPlan(
        file_type="csv",
        delimiter=",",
        encoding="utf-8",
        file_size=csv_file.stat().st_size,
        # transaction_date IS mapped and unreadable, and `amount` is absent.
        field_mapping={"transaction_date": "Date", "description": "Description"},
        date_format=None,
        sign_convention="negative_is_expense",
        number_format="us",
        is_multi_account=False,
        confidence="low",
        skip_rows=0,
        has_header=True,
        rows_in_file=2,
        rows_skipped_trailing=0,
        header_row_looks_like_data=False,
        header_signature=["Date", "Description"],
        flagged_fields=[],
    )
    read_result = type(
        "ReadResult",
        (),
        {
            "df": pl.DataFrame({"Date": ["not-a-date"], "Description": ["Coffee"]}),
            "rows_in_file": 2,
            "header_row_looks_like_data": False,
        },
    )()

    with (
        patch(
            "moneybin.extractors.tabular.readers.read_file",
            return_value=read_result,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        ImportService(db).import_file(
            csv_file,
            reviewed_plan=reviewed_plan,
            refresh=False,
            save_format=False,
        )

    assert exc.value.outcome.reason == "unknown_layout"
    assert "amount" in exc.value.outcome.confidence.missing_required


def test_a_declined_reviewed_plan_keeps_the_preview_s_flagged_evidence(
    db: Database, tmp_path: Path
) -> None:
    """The refusal must re-score against the flagged set the caller reviewed.

    Regression: the re-score passed an empty flagged list, so a plan the
    preview scored 0.85 came back as score=1.0 beside tier="low" — an envelope
    that contradicts itself and names none of the fields that earned the tier,
    leaving the agent nothing to correct.
    """
    import polars as pl

    from moneybin.services.import_confirmation import ImportConfirmationRequiredError
    from moneybin.services.import_service import ImportService, ReviewedTabularPlan

    csv_file = tmp_path / "flagged.csv"
    csv_file.write_text(
        "Date,Description,Amount\n2026-01-05,Coffee,-4.75\n",
        encoding="utf-8",
    )
    reviewed_plan = ReviewedTabularPlan(
        file_type="csv",
        delimiter=",",
        encoding="utf-8",
        file_size=csv_file.stat().st_size,
        field_mapping={
            "transaction_date": "Date",
            "amount": "Amount",
            "description": "Description",
        },
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        is_multi_account=False,
        confidence="low",
        skip_rows=0,
        has_header=True,
        rows_in_file=2,
        rows_skipped_trailing=0,
        header_row_looks_like_data=False,
        header_signature=["Amount", "Date", "Description"],
        flagged_fields=["description"],
    )
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
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        ImportService(db).import_file(
            csv_file,
            reviewed_plan=reviewed_plan,
            refresh=False,
            save_format=False,
        )

    confidence = exc.value.outcome.confidence
    assert confidence.tier == "low"
    # A complete mapping with a readable date and a flagged field scores 0.85;
    # dropping the flagged set re-scores it as a clean 1.0.
    assert confidence.score == 0.85
    assert confidence.flagged == ("description",)


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

    def test_confirm_refuses_a_plan_whose_date_column_was_never_read(
        self, db: Database
    ) -> None:
        """confirm=True must not resolve a plan the loader parses to zero rows.

        Regression: resolve_or_confirm's Accept branch only special-cases `low`,
        so a medium plan whose date column header-matched but whose values
        detect_date_format could not read resolved unconditionally, and a
        fabricated "%Y-%m-%d" carried it into the loader — every row dropped
        while the import reported success. Live via
        `moneybin import confirm <file> --accept`.
        """
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        undated = _make_mapping_result(
            score=0.75, confidence="medium", date_format=None
        )
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=undated,
        ):
            with pytest.raises(ImportConfirmationRequiredError) as exc_info:
                ImportService(db).import_file(
                    _STANDARD_CSV,
                    account_name="test",
                    refresh=False,
                    confirm=True,
                )
        outcome = exc_info.value.outcome
        # The date column IS mapped; its values are what nothing could read, so
        # the refusal names the one recovery that changes them.
        assert outcome.reason == "unreadable_date"
        # Keep the detected tier: filing a medium failure under "low" would
        # contradict the preview the caller already holds.
        assert outcome.confidence.tier == "medium"

    def test_first_contact_xlsx_reports_the_structural_cause(
        self, db: Database, tmp_path: Path
    ) -> None:
        """A headerless XLSX sets the flag on first contact — no skip_rows needed.

        _read_excel computes header_row_looks_like_data unconditionally,
        because pl.read_excel always consumes row 0 as the header. resolve_or_
        confirm then refuses the forced-low tier with its own generic reason
        and raises before the convergence guard, so every surface prescribed a
        mapping retry for the one cause no mapping answers. (An earlier round
        of this PR asserted first contact could never set the flag — true for
        the CSV reader, wrong for Excel.)
        """
        import openpyxl

        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["2026-07-01", -4.50, "Coffee"])
        ws.append(["2026-07-02", 100.00, "Salary"])
        xlsx = tmp_path / "headerless.xlsx"
        wb.save(xlsx)

        with pytest.raises(ImportConfirmationRequiredError) as exc_info:
            ImportService(db).import_file(
                xlsx,
                account_name="test",
                refresh=False,
                confirm=True,
                save_format=False,
            )

        assert exc_info.value.outcome.reason == "header_row_consumed"

    def test_a_saved_format_cannot_commit_a_consumed_header_row(
        self, db: Database, tmp_path: Path
    ) -> None:
        """The structural gate must cover the branch where the flag can be set.

        `header_row_looks_like_data` is computed only for an explicit
        `skip_rows` (readers.py — auto-detection never picks a data-looking
        row), so the only branch that can see it true is `elif matched_format:`,
        which asserts confidence="high" and commits. `--format <saved>` on a
        file whose post-skip header line is a transaction therefore imported a
        plan with one record already consumed as column names, while
        `import_preview` refuses the same plan. No caller input clears it:
        resolve_or_confirm honours an Override at every tier by design, and no
        column mapping un-consumes a header row.
        """
        from moneybin.extractors.tabular.formats import TabularFormat, save_format_to_db
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        # Row 1 is a preamble the format skips; row 2 then becomes the header
        # and is itself a transaction.
        csv = tmp_path / "preamble_then_data.csv"
        csv.write_text(
            "Statement export\n2026-01-05,-4.50,Coffee\n2026-01-06,100.00,Payroll\n",
            encoding="utf-8",
        )
        save_format_to_db(
            db,
            TabularFormat(
                name="skiprows_fixture",
                institution_name="Test",
                file_type="csv",
                delimiter=",",
                encoding="utf-8",
                header_signature=["2026-01-05", "-4.50", "Coffee"],
                field_mapping={
                    "transaction_date": "2026-01-05",
                    "amount": "-4.50",
                    "description": "Coffee",
                },
                sign_convention="negative_is_expense",
                date_format="%Y-%m-%d",
                number_format="us",
                skip_rows=1,
            ),
            actor="test",
        )

        with pytest.raises(ImportConfirmationRequiredError) as exc_info:
            ImportService(db).import_file(
                csv,
                account_name="test",
                refresh=False,
                confirm=True,
                format_name="skiprows_fixture",
                save_format=False,
            )

        assert exc_info.value.outcome.reason == "header_row_consumed"

    def test_a_consumed_header_refusal_counts_as_a_revalidation_failure(
        self, db: Database, tmp_path: Path
    ) -> None:
        """A refusal must not first record the saved format as a silent reuse.

        The guard sits on the matched_format path, so placing it after the
        metrics block counted a mastery KPI (IMPORT_KNOWN_FORMAT_REUSE_TOTAL)
        for a layout that then refused, and filed the decline under
        resolved.confidence — hardcoded "high" on that branch — while the
        envelope said low. IMPORT_REVALIDATION_FAILURE_TOTAL was declared for
        exactly this guard and went unincremented.
        """
        from moneybin.extractors.tabular.formats import TabularFormat, save_format_to_db
        from moneybin.metrics.observations import MetricObservations
        from moneybin.metrics.registry import (
            IMPORT_KNOWN_FORMAT_REUSE_TOTAL,
            IMPORT_REVALIDATION_FAILURE_TOTAL,
        )
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        csv = tmp_path / "preamble_metrics.csv"
        csv.write_text(
            "Statement export\n2026-01-05,-4.50,Coffee\n2026-01-06,100.00,Payroll\n",
            encoding="utf-8",
        )
        save_format_to_db(
            db,
            TabularFormat(
                name="skiprows_metrics",
                institution_name="Test",
                file_type="csv",
                delimiter=",",
                encoding="utf-8",
                header_signature=["2026-01-05", "-4.50", "Coffee"],
                field_mapping={
                    "transaction_date": "2026-01-05",
                    "amount": "-4.50",
                    "description": "Coffee",
                },
                sign_convention="negative_is_expense",
                date_format="%Y-%m-%d",
                number_format="us",
                skip_rows=1,
            ),
            actor="test",
        )

        observations = MetricObservations()
        reuse_before = IMPORT_KNOWN_FORMAT_REUSE_TOTAL.labels(
            channel="tabular"
        )._value.get()  # type: ignore[reportPrivateUsage]
        fail_before = IMPORT_REVALIDATION_FAILURE_TOTAL.labels(
            channel="tabular"
        )._value.get()  # type: ignore[reportPrivateUsage]

        with pytest.raises(ImportConfirmationRequiredError):
            ImportService(db).import_file(
                csv,
                account_name="test",
                refresh=False,
                confirm=True,
                format_name="skiprows_metrics",
                save_format=False,
                emit_metrics=False,
                observations=observations,
            )
        # The disposition the real MCP caller uses on this path: it wraps the
        # channel call in `except BaseException: db.rollback();
        # observations.flush("rollback")`. Flushing "commit" here asserted
        # against a disposition no caller ever reaches on a refusal, so the
        # test stayed green while the metrics were being discarded.
        observations.flush("rollback")

        assert (
            IMPORT_REVALIDATION_FAILURE_TOTAL.labels(channel="tabular")._value.get()  # type: ignore[reportPrivateUsage]
            == fail_before + 1
        )
        # The refusal must not have been counted as a successful reuse.
        assert (
            IMPORT_KNOWN_FORMAT_REUSE_TOTAL.labels(channel="tabular")._value.get()  # type: ignore[reportPrivateUsage]
            == reuse_before
        )

    def test_an_override_cannot_resolve_an_unreadable_date_column(
        self, db: Database
    ) -> None:
        """The unreadable-date gate must sit ahead of resolve_or_confirm.

        An Override short-circuits resolve_or_confirm at *every* tier, including
        low, so a gate placed inside it would never see this case: a mapping
        correction that does not fix the date column would still load zero rows.
        map_columns applies overrides before detecting the format, so a genuine
        correction clears this on its own.
        """
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        undated = _make_mapping_result(
            score=0.75, confidence="medium", date_format=None
        )
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=undated,
        ):
            with pytest.raises(ImportConfirmationRequiredError):
                ImportService(db).import_file(
                    _STANDARD_CSV,
                    account_name="test",
                    refresh=False,
                    overrides={"description": "Description"},
                )

    def test_an_explicit_date_format_recovers_a_layout_the_detector_cannot_read(
        self, db: Database, tmp_path: Path
    ) -> None:
        """--date-format is the documented escape hatch; the gate must not eat it.

        Regression: the unreadable-date gate raised before import_file applied
        date_format_override, so `moneybin import files <file> --confirm
        --date-format %Y%m%d` refused a file that imported before this PR.
        %Y%m%d is genuinely absent from _DATE_FORMATS, so the override is the
        only way such a file gets in at all. (`--date-format` lives on `import
        files`; `import confirm` does not take one.)
        """
        from moneybin.services.import_service import ImportService

        csv = tmp_path / "compact-dates.csv"
        csv.write_text(
            "Date,Description,Amount\n20260105,Coffee,-4.50\n20260212,Deposit,100.00\n"
        )
        undated = _make_mapping_result(
            score=0.75, confidence="medium", date_format=None
        )
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=undated,
        ):
            result = ImportService(db).import_file(
                csv,
                account_name="test",
                refresh=False,
                confirm=True,
                date_format="%Y%m%d",
            )
        assert result.import_id is not None

    def test_a_dirty_prefix_does_not_refuse_a_file_the_format_reads(
        self, db: Database, tmp_path: Path
    ) -> None:
        """The gate must read the column, not collect_samples' 20-row head.

        A sample answers "what does this look like"; this gate answers "does
        this format read the column". Validating the head made a run of
        malformed leading values refuse a file whose remaining rows parse
        fine — and the transform would have imported them, counting the bad
        ones in rows_rejected.
        """
        from moneybin.services.import_service import ImportService

        rows = ["Date,Description,Amount"]
        rows += [f"not-a-date,Row{i},-1.00" for i in range(11)]
        rows += [f"2026010{i % 9 + 1},Row{i},-2.00" for i in range(200)]
        csv = tmp_path / "dirty_prefix.csv"
        csv.write_text("\n".join(rows) + "\n", encoding="utf-8")

        undated = _make_mapping_result(
            score=0.75, confidence="medium", date_format=None
        )
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=undated,
        ):
            result = ImportService(db).import_file(
                csv,
                account_name="test",
                refresh=False,
                confirm=True,
                date_format="%Y%m%d",
            )

        assert result.import_id is not None

    def test_a_refused_date_override_records_no_success_metrics(
        self, db: Database, tmp_path: Path
    ) -> None:
        """A refusal must not leave a confirmation on the dashboards.

        The gate sat below the success counters, and on the CLI path
        `observations` is None — so those mutations apply immediately and no
        rollback undoes them. A refused import reported an accepted or
        overridden confirmation that never loaded anything.
        """
        from moneybin.errors import UserError
        from moneybin.metrics.registry import IMPORT_CONFIRMATIONS_TOTAL
        from moneybin.services.import_service import ImportService

        csv = tmp_path / "compact.csv"
        csv.write_text(
            "Date,Description,Amount\n20260105,Coffee,-4.50\n20260212,Rent,-9.00\n",
            encoding="utf-8",
        )
        undated = _make_mapping_result(
            score=0.75, confidence="medium", date_format=None
        )
        before = {
            outcome: IMPORT_CONFIRMATIONS_TOTAL.labels(
                channel="tabular", tier="medium", outcome=outcome
            )._value.get()  # type: ignore[reportPrivateUsage]
            for outcome in ("accepted", "overridden")
        }

        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=undated,
            ),
            pytest.raises(UserError),
        ):
            # CLI shape: no observations buffer, so counters apply immediately.
            ImportService(db).import_file(
                csv,
                account_name="test",
                refresh=False,
                confirm=True,
                date_format="%d/%m/%Y",
            )

        for outcome, prior in before.items():
            assert (
                IMPORT_CONFIRMATIONS_TOTAL.labels(
                    channel="tabular", tier="medium", outcome=outcome
                )._value.get()  # type: ignore[reportPrivateUsage]
                == prior
            ), outcome

    def test_a_date_format_override_that_cannot_read_the_column_is_refused(
        self, db: Database, tmp_path: Path
    ) -> None:
        """Honouring the override must not reopen the zero-row hole behind it.

        Skipping the gate whenever date_format_override is set would let a wrong
        format through to the loader, which drops every row and reports success
        — the same silent failure from the other side. The override is held to
        the parse bar the detector applies to its own candidates.
        """
        from moneybin import error_codes
        from moneybin.errors import UserError
        from moneybin.services.import_service import ImportService

        csv = tmp_path / "compact-dates.csv"
        csv.write_text(
            "Date,Description,Amount\n20260105,Coffee,-4.50\n20260212,Deposit,100.00\n"
        )
        undated = _make_mapping_result(
            score=0.75, confidence="medium", date_format=None
        )
        with patch(
            "moneybin.extractors.tabular.column_mapper.map_columns",
            return_value=undated,
        ):
            with pytest.raises(UserError) as exc_info:
                ImportService(db).import_file(
                    csv,
                    account_name="test",
                    refresh=False,
                    confirm=True,
                    date_format="%d/%m/%Y",
                )
        assert exc_info.value.code == error_codes.IMPORT_INVALID_DATE_FORMAT

    def test_a_date_format_override_is_validated_when_a_known_format_matched(
        self, db: Database
    ) -> None:
        """The parse check must not live in the first-contact branch alone.

        Regression: validation sat inside the no-matched-format branch, but the
        override is honored for *every* branch by the dataclasses.replace that
        rebuilds ResolvedMapping. tiller.csv's headers match the built-in
        `tiller` format, so a bare `moneybin import files <file> --confirm
        --date-format %Y%m%d` — no --format flag needed — skipped the check
        entirely and carried an unreadable format into the transform, which
        dropped every row while the import reported success.
        """
        from moneybin import error_codes
        from moneybin.errors import UserError
        from moneybin.services.import_service import ImportService

        with pytest.raises(UserError) as exc_info:
            ImportService(db).import_file(
                _TILLER_CSV,  # Date column is %m/%d/%Y; %Y%m%d reads none of it
                account_name="test",
                refresh=False,
                confirm=True,
                date_format="%Y%m%d",
            )
        assert exc_info.value.code == error_codes.IMPORT_INVALID_DATE_FORMAT

    def test_a_buffered_successful_detection_reaches_the_histogram(
        self, db: Database, tmp_path: Path
    ) -> None:
        """The other half of "all detections" — successes, not just refusals.

        Tagging the observation `rollback` fixed the refusal path and broke
        this one: a buffered caller flushes "commit" on success, and flush()
        discards every item whose disposition does not match. That moved the
        bias from "successes only" to "failures only" rather than removing it.
        """
        from moneybin.metrics.observations import MetricObservations
        from moneybin.metrics.registry import IMPORT_DETECTION_SCORE
        from moneybin.services.import_service import ImportService

        csv = tmp_path / "clean.csv"
        csv.write_text(
            "Date,Description,Amount\n"
            "2026-01-05,Coffee,-4.50\n"
            "2026-01-06,Payroll,100.00\n",
            encoding="utf-8",
        )
        observations = MetricObservations()
        before = IMPORT_DETECTION_SCORE._sum.get()  # type: ignore[reportPrivateUsage]

        result = ImportService(db).import_file(
            csv,
            account_name="test",
            refresh=False,
            confirm=True,
            save_format=False,
            emit_metrics=False,
            observations=observations,
        )
        observations.flush("commit")

        assert result.import_id is not None
        after = IMPORT_DETECTION_SCORE._sum.get()  # type: ignore[reportPrivateUsage]
        assert after > before, "a successful detection never reached the histogram"

    def test_an_unreadable_date_still_reaches_the_calibration_histogram(
        self, db: Database
    ) -> None:
        """The refusal must not drop its score from IMPORT_DETECTION_SCORE.

        The histogram is the primary calibration signal for the confidence
        bands. An early exit that records the declined confirmation but skips
        the observation biases every future threshold toward layouts that
        happened to succeed.
        """
        from moneybin.metrics.observations import MetricObservations
        from moneybin.metrics.registry import IMPORT_DETECTION_SCORE
        from moneybin.services.import_confirmation import (
            ImportConfirmationRequiredError,
        )
        from moneybin.services.import_service import ImportService

        undated = _make_mapping_result(
            score=0.75, confidence="medium", date_format=None
        )
        observations = MetricObservations()
        before = IMPORT_DETECTION_SCORE._sum.get()  # type: ignore[reportPrivateUsage]
        with (
            patch(
                "moneybin.extractors.tabular.column_mapper.map_columns",
                return_value=undated,
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

        # The disposition the real MCP caller uses on this path: it wraps the
        # channel call in `except BaseException: db.rollback();
        # observations.flush("rollback")`. Flushing "commit" here asserted
        # against a disposition no caller ever reaches on a refusal, so the
        # test stayed green while the metrics were being discarded.
        observations.flush("rollback")
        assert (
            IMPORT_DETECTION_SCORE._sum.get()  # type: ignore[reportPrivateUsage]
            == before + 0.75
        )

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

        score_mapping treats debit_amount + credit_amount as satisfying the
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

        assert exc.value.code == "import_invalid_sign_convention"
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

        assert exc.value.code == "import_invalid_sign_convention"
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
