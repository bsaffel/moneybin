"""Tests for DoctorSettings configuration."""

import pytest

from moneybin.config import DoctorSettings, MoneyBinSettings


class TestDoctorSettings:
    """Tests for DoctorSettings defaults, validation, and env override."""

    def test_defaults(self) -> None:
        settings = DoctorSettings()
        assert settings.audit_coverage_lookback_days == 7
        assert settings.audit_coverage_sample_cap == 1000

    def test_lookback_days_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="audit_coverage_lookback_days"):
            DoctorSettings(audit_coverage_lookback_days=0)

    def test_sample_cap_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="audit_coverage_sample_cap"):
            DoctorSettings(audit_coverage_sample_cap=0)

    def test_available_on_root_settings(self) -> None:
        settings = MoneyBinSettings(profile="test")
        assert settings.doctor.audit_coverage_lookback_days == 7

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MONEYBIN_DOCTOR__AUDIT_COVERAGE_LOOKBACK_DAYS", "14")
        settings = MoneyBinSettings(profile="test")
        assert settings.doctor.audit_coverage_lookback_days == 14
