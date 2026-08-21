"""Tests for get_base_dir() resolution logic."""

from pathlib import Path

import pytest

from moneybin.config import canonical_checkout_root, get_base_dir


def _make_worktree(tmp_path: Path) -> tuple[Path, Path]:
    """Build a main moneybin checkout and a linked worktree of it.

    Mirrors the real layout: the worktree's ``.git`` is a *file* pointing into
    ``<main>/.git/worktrees/<name>``.
    """
    main = tmp_path / "main"
    (main / ".git" / "worktrees" / "wt").mkdir(parents=True)
    (main / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
    worktree = tmp_path / "wt"
    worktree.mkdir()
    (worktree / ".git").write_text(f"gitdir: {main / '.git' / 'worktrees' / 'wt'}\n")
    (worktree / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
    return main, worktree


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

    def test_development_env_outside_a_checkout_uses_cwd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Priority 2 with no checkout above CWD falls back to <cwd>/.moneybin.

        Must chdir somewhere that is provably not a checkout: reading the
        ambient CWD conflates this branch with the repo-root one below, and
        they only agree when the suite happens to run from a checkout root.
        """
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.setenv("MONEYBIN_ENVIRONMENT", "development")
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (tmp_path / ".moneybin").resolve()

    def test_development_env_uses_repo_root_from_subdirectory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Development mode keeps a checkout's state in its root directory."""
        (tmp_path / ".git").mkdir()
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
        subdirectory = tmp_path / "src" / "moneybin"
        subdirectory.mkdir(parents=True)
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.setenv("MONEYBIN_ENVIRONMENT", "development")
        monkeypatch.chdir(subdirectory)

        assert get_base_dir() == (tmp_path / ".moneybin").resolve()

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

    def test_worktree_uses_the_main_checkouts_data_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Priority 3 in a worktree resolves the main checkout, not the worktree."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        main, worktree = _make_worktree(tmp_path)
        monkeypatch.chdir(worktree)
        assert get_base_dir() == (main / ".moneybin").resolve()

    def test_development_env_in_a_worktree_uses_the_main_checkout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Priority 2 has its own ancestor walk and needs the same mapping.

        Fixing only priority 3 would leave every development-mode invocation —
        which is how the repo is normally driven — still on the empty home.
        """
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.setenv("MONEYBIN_ENVIRONMENT", "development")
        main, worktree = _make_worktree(tmp_path)
        subdirectory = worktree / "src" / "moneybin"
        subdirectory.mkdir(parents=True)
        monkeypatch.chdir(subdirectory)
        assert get_base_dir() == (main / ".moneybin").resolve()

    def test_moneybin_home_still_wins_inside_a_worktree(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An explicit MONEYBIN_HOME outranks the worktree mapping."""
        _, worktree = _make_worktree(tmp_path)
        override = tmp_path / "explicit"
        monkeypatch.setenv("MONEYBIN_HOME", str(override))
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        monkeypatch.chdir(worktree)
        assert get_base_dir() == override.resolve()

    def test_git_file_that_is_not_a_worktree_falls_through_to_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The production shape: no checkout anywhere means ~/.moneybin.

        Guards the case this change newly touches — a `.git` file that is not a
        worktree pointer must not invent a root.
        """
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        (tmp_path / ".git").write_text("not a gitdir pointer\n")
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "other"\n')
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (Path.home() / ".moneybin").resolve()


class TestCanonicalCheckoutRoot:
    """A linked worktree shares the main checkout's data home."""

    def test_linked_worktree_resolves_to_the_main_checkout(
        self, tmp_path: Path
    ) -> None:
        """The whole point: a worktree is a second checkout, not a second install."""
        main, worktree = _make_worktree(tmp_path)
        assert canonical_checkout_root(worktree) == main

    def test_normal_checkout_is_returned_unchanged(self, tmp_path: Path) -> None:
        """A `.git` directory is the main working tree already."""
        (tmp_path / ".git").mkdir()
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
        assert canonical_checkout_root(tmp_path) == tmp_path

    def test_submodule_is_not_treated_as_a_worktree(self, tmp_path: Path) -> None:
        """A submodule's `.git` file points at `.git/modules/<name>`, not a worktree.

        Resolving it to the superproject would move a submodule's data home into
        an unrelated repository. The `worktrees` path segment is the discriminator.
        """
        super_root = tmp_path / "super"
        (super_root / ".git" / "modules" / "sub").mkdir(parents=True)
        (super_root / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / ".git").write_text(
            f"gitdir: {super_root / '.git' / 'modules' / 'sub'}\n"
        )
        assert canonical_checkout_root(sub) == sub

    def test_worktree_of_a_non_moneybin_repo_is_unchanged(self, tmp_path: Path) -> None:
        """Only adopt a main checkout that is itself a moneybin checkout."""
        main, worktree = _make_worktree(tmp_path)
        (main / "pyproject.toml").write_text('[project]\nname = "something-else"\n')
        assert canonical_checkout_root(worktree) == worktree

    def test_missing_main_checkout_is_unchanged(self, tmp_path: Path) -> None:
        """A worktree whose main checkout was deleted degrades to today's behaviour."""
        _, worktree = _make_worktree(tmp_path)
        (worktree / ".git").write_text("gitdir: /nonexistent/.git/worktrees/wt\n")
        assert canonical_checkout_root(worktree) == worktree

    def test_malformed_git_file_is_unchanged(self, tmp_path: Path) -> None:
        """Anything that is not a `gitdir:` pointer falls back to the input."""
        (tmp_path / ".git").write_text("not a gitdir pointer\n")
        assert canonical_checkout_root(tmp_path) == tmp_path
