"""Tests for ImportSettings and the profile_inbox_dir derived property."""

from pathlib import Path

import pytest

from moneybin.config import ConfidenceBands, ImportSettings, MoneyBinSettings


class TestImportSettings:
    """Tests for the ImportSettings model defaults and overrides."""

    def test_default_inbox_root_is_documents_moneybin(self) -> None:
        settings = ImportSettings()
        assert settings.inbox_root == Path.home() / "Documents" / "MoneyBin"

    def test_inbox_root_overridable_via_init(self, tmp_path: Path) -> None:
        settings = ImportSettings(inbox_root=tmp_path / "custom")
        assert settings.inbox_root == tmp_path / "custom"

    def test_default_self_accept_high_false(self) -> None:
        assert ImportSettings().self_accept_high is False

    def test_default_pdf_preview_size_limit_matches_binary_import_limit(self) -> None:
        assert ImportSettings().pdf_preview_size_limit_mb == 100

    def test_pdf_preview_size_limit_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            ImportSettings(pdf_preview_size_limit_mb=0)

    def test_pdf_preview_size_limit_uses_nested_environment_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(
            "MONEYBIN_IMPORT___PDF_PREVIEW_SIZE_LIMIT_MB",
            "7",
        )

        settings = MoneyBinSettings(_env_file=None)

        assert settings.import_.pdf_preview_size_limit_mb == 7

    def test_self_accept_high_can_enable(self) -> None:
        assert ImportSettings(self_accept_high=True).self_accept_high is True

    def test_confidence_bands_default(self) -> None:
        s = ImportSettings()
        assert s.confidence.t_high == 0.90
        assert s.confidence.t_med == 0.70


class TestConfidenceBands:
    """Validate confidence band thresholds and constraints."""

    def test_defaults_per_spec(self) -> None:
        b = ConfidenceBands()
        assert b.t_high == 0.90
        assert b.t_med == 0.70

    def test_rejects_inverted(self) -> None:
        with pytest.raises(ValueError, match="t_high must be >= t_med"):
            ConfidenceBands(t_high=0.5, t_med=0.7)

    def test_rejects_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            ConfidenceBands(t_high=1.5, t_med=0.7)
        with pytest.raises(ValueError):
            ConfidenceBands(t_high=0.9, t_med=-0.1)

    def test_accepts_equal_bands(self) -> None:
        b = ConfidenceBands(t_high=0.8, t_med=0.8)
        assert b.t_high == b.t_med


class TestProfileInboxDir:
    """Tests for the MoneyBinSettings.profile_inbox_dir derived property."""

    def test_derived_from_active_profile(self, tmp_path: Path) -> None:
        s = MoneyBinSettings(
            profile="alice",
            import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
        )
        assert s.profile_inbox_dir == tmp_path / "MoneyBin" / "alice"

    def test_switches_when_profile_changes(self, tmp_path: Path) -> None:
        a = MoneyBinSettings(
            profile="alice",
            import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
        )
        b = MoneyBinSettings(
            profile="bob",
            import_=ImportSettings(inbox_root=tmp_path / "MoneyBin"),
        )
        assert a.profile_inbox_dir != b.profile_inbox_dir
