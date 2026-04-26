"""Tests for get_base_dir() resolution logic."""

from pathlib import Path

import pytest

from moneybin.config import get_base_dir


class TestGetBaseDir:
    """Test get_base_dir() resolution priority."""

    def test_moneybin_home_env_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Priority 1: MONEYBIN_HOME env var takes precedence over everything."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        assert get_base_dir() == tmp_path

    def test_moneybin_home_expands_tilde(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """MONEYBIN_HOME expands ~ to home directory."""
        monkeypatch.setenv("MONEYBIN_HOME", "~/custom-moneybin")
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        assert get_base_dir() == (Path.home() / "custom-moneybin").resolve()

    def test_development_env_uses_cwd(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Priority 2: MONEYBIN_ENVIRONMENT=development uses <cwd>/.moneybin."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.setenv("MONEYBIN_ENVIRONMENT", "development")
        assert get_base_dir() == (Path.cwd() / ".moneybin").resolve()

    def test_repo_checkout_detected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Priority 3: .git + pyproject.toml with name='moneybin' uses <cwd>/.moneybin."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        (tmp_path / ".git").mkdir()
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (tmp_path / ".moneybin").resolve()

    def test_repo_checkout_wrong_project_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Repo checkout detection rejects non-moneybin projects."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        (tmp_path / ".git").mkdir()
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "other-project"\n')
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (Path.home() / ".moneybin").resolve()

    def test_no_git_falls_through_to_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No .git directory means not a repo checkout — use default."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (Path.home() / ".moneybin").resolve()

    def test_default_is_dot_moneybin(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Priority 4: Default is ~/.moneybin/."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (Path.home() / ".moneybin").resolve()
