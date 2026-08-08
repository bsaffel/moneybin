"""Tests for DoctorSettings configuration."""

import pytest

from moneybin.config import DoctorSettings, MoneyBinSettings


class TestDoctorSettings:
    """Tests for DoctorSettings defaults, validation, and env override."""

    def test_defaults(self) -> None:
        settings = DoctorSettings()
        assert settings.audit_coverage_lookback_days == 7
        assert settings.audit_coverage_sample_cap == 1000
        assert settings.duplicate_account_overlap_ratio == 0.5
        assert settings.duplicate_account_min_distinct_amounts == 10

    def test_lookback_days_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="audit_coverage_lookback_days"):
            DoctorSettings(audit_coverage_lookback_days=0)

    def test_sample_cap_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="audit_coverage_sample_cap"):
            DoctorSettings(audit_coverage_sample_cap=0)

    def test_overlap_ratio_must_be_a_reachable_fraction(self) -> None:
        # 0.0 would warn on a single mirrored row; above 1.0 never fires at all.
        with pytest.raises(ValueError, match="duplicate_account_overlap_ratio"):
            DoctorSettings(duplicate_account_overlap_ratio=0.0)
        with pytest.raises(ValueError, match="duplicate_account_overlap_ratio"):
            DoctorSettings(duplicate_account_overlap_ratio=1.5)

    def test_min_distinct_amounts_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="duplicate_account_min_distinct_amounts"):
            DoctorSettings(duplicate_account_min_distinct_amounts=0)

    def test_available_on_root_settings(self) -> None:
        settings = MoneyBinSettings(profile="test")
        assert settings.doctor.audit_coverage_lookback_days == 7

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MONEYBIN_DOCTOR__AUDIT_COVERAGE_LOOKBACK_DAYS", "14")
        settings = MoneyBinSettings(profile="test")
        assert settings.doctor.audit_coverage_lookback_days == 14
