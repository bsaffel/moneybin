"""Tests for ImportSettings and the profile_inbox_dir derived property."""

from pathlib import Path

from moneybin.config import ImportSettings, MoneyBinSettings


class TestImportSettings:
    """Tests for the ImportSettings model defaults and overrides."""

    def test_default_inbox_root_is_documents_moneybin(self) -> None:
        settings = ImportSettings()
        assert settings.inbox_root == Path.home() / "Documents" / "MoneyBin"

    def test_inbox_root_overridable_via_init(self, tmp_path: Path) -> None:
        settings = ImportSettings(inbox_root=tmp_path / "custom")
        assert settings.inbox_root == tmp_path / "custom"


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
