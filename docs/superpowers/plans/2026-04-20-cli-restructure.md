# CLI Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign MoneyBin's CLI command tree: profile system as first-class entity, domain commands at top level, `get_base_dir()` default inverted to `~/.moneybin/`, and all future spec CLI surfaces pre-wired.

**Architecture:** The profile system becomes the organizational unit — each profile has its own directory containing database, config, logs, and temp files under `<base>/profiles/<name>/`. `get_base_dir()` is rewritten to default to `~/.moneybin/` (detecting dev repo checkouts automatically). The `config` and `data` command groups are dissolved; `categorize`, `transform`, and new domain commands become top-level.

**Tech Stack:** Python 3.12+, Typer, Pydantic Settings, PyYAML, DuckDB, FastMCP

**Spec:** `docs/specs/cli-restructure.md`

---

## File Structure

### New files

| File | Responsibility |
|---|---|
| `src/moneybin/services/profile_service.py` | Profile lifecycle business logic (create, list, switch, delete, show, set) |
| `src/moneybin/cli/commands/profile.py` | Profile CLI commands (thin wrappers around profile_service) |
| `src/moneybin/cli/commands/logs.py` | Logs CLI commands (clean, path, tail) |
| `src/moneybin/cli/commands/stubs.py` | Phase 2 stub commands (matches, track, export, stats, db migrate) |
| `tests/moneybin/test_services/test_profile_service.py` | Profile service tests |
| `tests/moneybin/test_cli/test_cli_profile_commands.py` | Profile CLI tests |
| `tests/moneybin/test_cli/test_cli_restructure.py` | CLI routing tests (moved/removed/stubbed commands) |
| `tests/moneybin/test_cli/test_cli_transform.py` | Transform command tests |
| `tests/moneybin/test_cli/test_cli_mcp_enhancements.py` | MCP enhancement tests |
| `tests/moneybin/test_cli/test_cli_logs.py` | Logs command tests |
| `tests/moneybin/test_migration.py` | Old-format migration tests |

### Modified files

| File | Changes |
|---|---|
| `src/moneybin/config.py` | `get_base_dir()` rewrite, `MoneyBinSettings` profile directory layout |
| `src/moneybin/utils/user_config.py` | `active_profile` (rename from `default_profile`), profile config YAML generation |
| `src/moneybin/cli/main.py` | New command groups, remove config/data/extract |
| `src/moneybin/cli/commands/db.py` | Add `ps`/`kill` commands (moved from mcp) |
| `src/moneybin/cli/commands/mcp.py` | Remove `show`/`kill`, add `list-tools`/`list-prompts`/`config`/`config generate` |
| `src/moneybin/cli/commands/transform.py` | Add `status`/`validate`/`audit`/`restate` |
| `src/moneybin/cli/commands/sync.py` | Replace `all` with Phase 2 stubs |
| `tests/moneybin/conftest.py` | Update `temp_profile` for new directory layout |
| `tests/moneybin/test_config_profiles.py` | Update for new `get_base_dir()` behavior and directory layout |
| `tests/moneybin/test_cli/test_cli_profiles.py` | Update for new command paths |
| `sqlmesh/config.py` | Update if needed for new profile paths |

### Deleted files

| File | Reason |
|---|---|
| `src/moneybin/cli/commands/config.py` | Replaced by `profile` command group |
| `src/moneybin/cli/commands/extract.py` | Superseded by `import file` |
| `src/moneybin/cli/commands/credentials.py` | Removed with config group |
| `src/moneybin/cli/commands/data.py` | Dissolved — children promoted |

---

## Task 1: Rewrite `get_base_dir()`

**Files:**
- Modify: `src/moneybin/config.py:16-35`
- Test: `tests/moneybin/test_config_profiles.py`

This rewrites the base directory resolution to default to `~/.moneybin/` instead of `cwd`, with automatic repo checkout detection for developers.

- [ ] **Step 1: Write failing tests for new `get_base_dir()` behavior**

```python
# tests/moneybin/test_get_base_dir.py
"""Tests for get_base_dir() resolution logic."""

import os
from pathlib import Path

from pytest_mock import MockerFixture

from moneybin.config import get_base_dir


class TestGetBaseDir:
    """Test get_base_dir() resolution priority."""

    def test_moneybin_home_env_wins(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Priority 1: MONEYBIN_HOME env var takes precedence over everything."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        assert get_base_dir() == tmp_path

    def test_moneybin_home_expands_tilde(
        self, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """MONEYBIN_HOME expands ~ to home directory."""
        monkeypatch.setenv("MONEYBIN_HOME", "~/custom-moneybin")
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        assert get_base_dir() == (Path.home() / "custom-moneybin").resolve()

    def test_development_env_uses_cwd(self, monkeypatch: "pytest.MonkeyPatch") -> None:
        """Priority 2: MONEYBIN_ENVIRONMENT=development uses cwd."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.setenv("MONEYBIN_ENVIRONMENT", "development")
        assert get_base_dir() == Path.cwd().resolve()

    def test_repo_checkout_detected(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Priority 3: .git + pyproject.toml with name='moneybin' uses cwd."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        # Create fake repo indicators
        (tmp_path / ".git").mkdir()
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == tmp_path.resolve()

    def test_repo_checkout_wrong_project_name(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Repo checkout detection rejects non-moneybin projects."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        (tmp_path / ".git").mkdir()
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "other-project"\n')
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (Path.home() / ".moneybin").resolve()

    def test_no_git_falls_through_to_default(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """No .git directory means not a repo checkout — use default."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "moneybin"\n')
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (Path.home() / ".moneybin").resolve()

    def test_default_is_dot_moneybin(
        self, monkeypatch: "pytest.MonkeyPatch", tmp_path: Path
    ) -> None:
        """Priority 4: Default is ~/.moneybin/."""
        monkeypatch.delenv("MONEYBIN_HOME", raising=False)
        monkeypatch.delenv("MONEYBIN_ENVIRONMENT", raising=False)
        # chdir to a directory without .git
        monkeypatch.chdir(tmp_path)
        assert get_base_dir() == (Path.home() / ".moneybin").resolve()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_get_base_dir.py -v`
Expected: Multiple FAIL — current `get_base_dir()` defaults to `cwd` not `~/.moneybin/`, and doesn't do repo detection.

- [ ] **Step 3: Implement new `get_base_dir()`**

Replace the `get_base_dir()` function in `src/moneybin/config.py:16-35`:

```python
def _is_moneybin_repo(path: Path) -> bool:
    """Check if path is a moneybin repo checkout.

    Detects the moneybin repository by checking for .git directory and
    pyproject.toml with name = "moneybin".

    Args:
        path: Directory to check.

    Returns:
        True if path appears to be a moneybin repo checkout.
    """
    if not (path / ".git").exists():
        return False
    pyproject = path / "pyproject.toml"
    if not pyproject.exists():
        return False
    try:
        content = pyproject.read_text()
        return 'name = "moneybin"' in content
    except OSError:
        return False


def get_base_dir() -> Path:
    """Determine the base directory for MoneyBin data and configuration.

    Resolution order:
        1. MONEYBIN_HOME env var (explicit override, always wins)
        2. MONEYBIN_ENVIRONMENT=development: current working directory
        3. Repo checkout detection (.git + pyproject.toml name=moneybin): cwd
        4. Default: ~/.moneybin/

    Returns:
        Path: Absolute base directory for the application.
    """
    moneybin_home = os.getenv("MONEYBIN_HOME")
    if moneybin_home:
        return Path(moneybin_home).expanduser().resolve()

    environment = os.getenv("MONEYBIN_ENVIRONMENT")
    if environment == "development":
        return Path.cwd().resolve()

    if _is_moneybin_repo(Path.cwd()):
        return Path.cwd().resolve()

    return (Path.home() / ".moneybin").resolve()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_get_base_dir.py -v`
Expected: All PASS.

- [ ] **Step 5: Run existing tests to check for regressions**

Run: `uv run pytest tests/moneybin/test_config_profiles.py -v`

Note: Some existing tests may fail because they were written assuming `get_base_dir()` returns `cwd` in test environments. The tests run inside the moneybin repo, so repo detection (priority 3) should return `cwd`. If any tests fail because the environment doesn't look like a repo checkout (e.g. they `chdir` to `tmp_path`), they need to be updated to set `MONEYBIN_HOME` or `MONEYBIN_ENVIRONMENT=development`.

- [ ] **Step 6: Fix any broken existing tests**

For any existing test that calls `get_base_dir()` inside `tmp_path`, set `MONEYBIN_HOME`:

```python
# In tests/moneybin/conftest.py or individual test files, add monkeypatch:
monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
```

Or update the `clean_profile_state` fixture in `conftest.py` to always set `MONEYBIN_ENVIRONMENT=development` for test runs.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_get_base_dir.py tests/moneybin/conftest.py tests/moneybin/test_config_profiles.py
git commit -m "feat: rewrite get_base_dir() to default to ~/.moneybin/

Inverts the default from cwd to ~/.moneybin/ for installed users.
Adds repo checkout detection so developers get cwd automatically.

Resolution: MONEYBIN_HOME > MONEYBIN_ENVIRONMENT=development > repo detection > ~/.moneybin/

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 2: Profile Directory Layout

**Files:**
- Modify: `src/moneybin/config.py:256-308` (MoneyBinSettings.__init__)
- Modify: `tests/moneybin/conftest.py`
- Test: `tests/moneybin/test_config_profiles.py`

Change `MoneyBinSettings` path construction to use `<base>/profiles/<name>/` layout instead of `<base>/data/<name>/` + `<base>/logs/<name>/`.

- [ ] **Step 1: Write failing tests for new directory layout**

```python
# Add to tests/moneybin/test_config_profiles.py
class TestProfileDirectoryLayout:
    """Test that settings resolve paths under profiles/<name>/ directory."""

    def test_database_path_under_profiles(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Database lives at <base>/profiles/<name>/moneybin.duckdb."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        clear_settings_cache()
        set_current_profile("alice")
        settings = get_settings()
        assert (
            settings.database.path
            == tmp_path / "profiles" / "alice" / "moneybin.duckdb"
        )

    def test_log_path_under_profiles(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Logs live at <base>/profiles/<name>/logs/moneybin.log."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        clear_settings_cache()
        set_current_profile("alice")
        settings = get_settings()
        assert (
            settings.logging.log_file_path
            == tmp_path / "profiles" / "alice" / "logs" / "moneybin.log"
        )

    def test_temp_dir_under_profiles(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Temp directory at <base>/profiles/<name>/temp/."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        clear_settings_cache()
        set_current_profile("alice")
        settings = get_settings()
        assert (
            settings.database.temp_directory == tmp_path / "profiles" / "alice" / "temp"
        )

    def test_backup_path_under_profiles(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Backup directory at <base>/profiles/<name>/backups/."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        clear_settings_cache()
        set_current_profile("alice")
        settings = get_settings()
        assert (
            settings.database.backup_path == tmp_path / "profiles" / "alice" / "backups"
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_config_profiles.py::TestProfileDirectoryLayout -v`
Expected: FAIL — paths still use `data/<name>/` layout.

- [ ] **Step 3: Update `MoneyBinSettings.__init__` for new layout**

In `src/moneybin/config.py`, update the `__init__` method to use `profiles/<name>/` directory:

```python
def __init__(self, **kwargs: Any):
    """Initialize settings with profile-based directory layout."""
    from moneybin.utils.user_config import normalize_profile_name

    raw_profile = kwargs.get("profile", "default")
    profile = normalize_profile_name(raw_profile)
    kwargs["profile"] = profile

    base = get_base_dir()
    profile_dir = base / "profiles" / profile

    # Check for legacy DUCKDB_PATH environment variable
    duckdb_path = os.getenv("DUCKDB_PATH")

    if "database" not in kwargs or (
        "database" in kwargs
        and kwargs["database"].path == Path("data/default/moneybin.duckdb")
    ):
        if duckdb_path:
            kwargs["database"] = DatabaseConfig(
                path=_resolve_path(base, Path(duckdb_path)),
                backup_path=profile_dir / "backups",
                temp_directory=profile_dir / "temp",
            )
        else:
            kwargs["database"] = DatabaseConfig(
                path=profile_dir / "moneybin.duckdb",
                backup_path=profile_dir / "backups",
                temp_directory=profile_dir / "temp",
            )

    if "data" not in kwargs:
        kwargs["data"] = DataConfig(
            raw_data_path=profile_dir / "raw",
            temp_data_path=profile_dir / "temp",
        )

    if "logging" not in kwargs:
        kwargs["logging"] = LoggingConfig(
            log_file_path=profile_dir / "logs" / "moneybin.log"
        )

    super().__init__(**kwargs)
```

- [ ] **Step 4: Update `temp_profile` in conftest.py for new layout**

```python
# tests/moneybin/conftest.py — update temp_profile
@contextmanager
def temp_profile(profile: str) -> Generator[str, None, None]:
    """Context manager for automatic profile cleanup."""
    from moneybin.utils.user_config import normalize_profile_name

    normalized = normalize_profile_name(profile)
    try:
        yield normalized
    finally:
        base = get_base_dir()
        profile_dir = base / "profiles" / normalized
        try:
            shutil.rmtree(profile_dir)
        except FileNotFoundError:
            pass
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_config_profiles.py -v`
Expected: All PASS.

- [ ] **Step 6: Update `create_directories` if needed**

Verify `MoneyBinSettings.create_directories()` still works with the new paths. The existing implementation creates parent directories for each path, which should work with the new layout. No changes expected.

- [ ] **Step 7: Run full test suite to catch regressions**

Run: `uv run pytest tests/ -v`
Fix any failures caused by path layout changes.

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/conftest.py tests/moneybin/test_config_profiles.py
git commit -m "feat: move profile data to profiles/<name>/ directory layout

Consolidates database, logs, temp, and backups under a single
profiles/<name>/ directory instead of data/<name>/ + logs/<name>/.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 3: UserConfig Refactor and Profile Config YAML

**Files:**
- Modify: `src/moneybin/utils/user_config.py`
- Test: `tests/moneybin/test_utils/test_user_config.py`

Rename `default_profile` to `active_profile` in global config. Add per-profile `config.yaml` generation.

- [ ] **Step 1: Write failing tests for `active_profile` rename**

```python
# tests/moneybin/test_utils/test_user_config.py — add or update tests
class TestActiveProfile:
    """Test active_profile in global config."""

    def test_load_config_with_active_profile(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Global config uses active_profile key."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text("active_profile: alice\n")
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        from moneybin.utils.user_config import load_user_config

        config = load_user_config()
        assert config.active_profile == "alice"

    def test_save_config_writes_active_profile(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Saving config writes active_profile key."""
        config_path = tmp_path / "config.yaml"
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        from moneybin.utils.user_config import UserConfig, save_user_config

        save_user_config(UserConfig(active_profile="bob"))
        content = config_path.read_text()
        assert "active_profile: bob" in content

    def test_migrate_default_profile_to_active(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Old default_profile key is read as active_profile."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text("default_profile: alice\n")
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        from moneybin.utils.user_config import load_user_config

        config = load_user_config()
        assert config.active_profile == "alice"


class TestProfileConfigYaml:
    """Test per-profile config.yaml generation."""

    def test_generate_profile_config(self, tmp_path: Path) -> None:
        """generate_profile_config creates a config.yaml with defaults."""
        from moneybin.utils.user_config import generate_profile_config

        profile_dir = tmp_path / "profiles" / "alice"
        generate_profile_config(profile_dir, "alice")
        config_path = profile_dir / "config.yaml"
        assert config_path.exists()
        import yaml

        data = yaml.safe_load(config_path.read_text())
        assert data["database"]["encryption_key_mode"] == "auto"
        assert data["logging"]["level"] == "INFO"
        assert data["sync"]["enabled"] is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_utils/test_user_config.py -v`
Expected: FAIL — `active_profile` doesn't exist, `generate_profile_config` doesn't exist.

- [ ] **Step 3: Update `UserConfig` to use `active_profile`**

In `src/moneybin/utils/user_config.py`:

```python
class UserConfig(BaseModel):
    """User-level configuration stored in ~/.moneybin/config.yaml."""

    active_profile: str | None = Field(
        default=None,
        description="Active profile name",
    )

    @field_validator("active_profile")
    @classmethod
    def validate_active_profile(cls, v: str | None) -> str | None:
        """Validate and normalize profile name."""
        if v is None:
            return None
        return normalize_profile_name(v)
```

Update `load_user_config` to handle the old `default_profile` key:

```python
def load_user_config() -> UserConfig:
    """Load user configuration from ~/.moneybin/config.yaml."""
    config_path = get_user_config_path()

    if not config_path.exists():
        return UserConfig()

    try:
        with open(config_path) as f:
            raw_data = yaml.safe_load(f)
            data: dict[str, str | None] = raw_data if isinstance(raw_data, dict) else {}

            # Migrate old default_profile key to active_profile
            if "default_profile" in data and "active_profile" not in data:
                data["active_profile"] = data.pop("default_profile")

            return UserConfig(**data)
    except Exception as e:
        logger.warning(f"Failed to load user config from {config_path}: {e}")
        return UserConfig()
```

Update all functions that reference `default_profile`:

```python
def get_default_profile() -> str | None:
    """Get the active profile name from user config."""
    config = load_user_config()
    return config.active_profile


def set_default_profile(profile_name: str) -> None:
    """Set the active profile name in user config."""
    normalized = normalize_profile_name(profile_name)
    config = load_user_config()
    config.active_profile = normalized
    save_user_config(config)
    logger.info(f"Set active profile to: {normalized}")
```

Add `generate_profile_config`:

```python
def generate_profile_config(profile_dir: Path, profile_name: str) -> Path:
    """Generate a per-profile config.yaml with sensible defaults.

    Args:
        profile_dir: Directory for the profile (will be created).
        profile_name: Profile name (for header comment).

    Returns:
        Path to the created config.yaml.
    """
    from datetime import date

    profile_dir.mkdir(parents=True, exist_ok=True)
    config_path = profile_dir / "config.yaml"

    config_data = {
        "database": {
            "encryption_key_mode": "auto",
        },
        "logging": {
            "level": "INFO",
            "log_to_file": True,
            "max_file_size_mb": 50,
        },
        "sync": {
            "enabled": False,
        },
    }

    header = f"# Profile: {profile_name}\n# Created: {date.today()}\n\n"
    with open(config_path, "w") as f:
        f.write(header)
        yaml.safe_dump(config_data, f, default_flow_style=False, sort_keys=False)

    return config_path
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_utils/test_user_config.py -v`
Expected: All PASS.

- [ ] **Step 5: Update references to `default_profile` across codebase**

Search for remaining `default_profile` references in non-test code and update. The `UserConfig.active_profile` field replaces `default_profile`, but the public API functions (`get_default_profile`, `set_default_profile`) keep their names for backward compatibility with CLI code until the profile commands are added.

- [ ] **Step 6: Run full test suite**

Run: `uv run pytest tests/ -v`

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/utils/user_config.py tests/moneybin/test_utils/test_user_config.py
git commit -m "feat: rename default_profile to active_profile, add profile config generation

Renames the global config key from default_profile to active_profile.
Auto-migrates old key on load. Adds generate_profile_config() for
creating per-profile config.yaml files with sensible defaults.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 4: Profile Service

**Files:**
- Create: `src/moneybin/services/profile_service.py`
- Test: `tests/moneybin/test_services/test_profile_service.py`

Business logic for profile lifecycle: create, list, switch, delete, show, set.

- [ ] **Step 1: Write failing tests for profile service**

```python
# tests/moneybin/test_services/test_profile_service.py
"""Tests for profile lifecycle service."""

import os
from pathlib import Path

import pytest
import yaml

from moneybin.services.profile_service import (
    ProfileExistsError,
    ProfileNotFoundError,
    ProfileService,
)


class TestProfileCreate:
    """Test profile creation."""

    def test_create_profile(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Creating a profile creates directory structure and config."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        profile_dir = tmp_path / "profiles" / "alice"
        assert profile_dir.exists()
        assert (profile_dir / "config.yaml").exists()
        assert (profile_dir / "logs").exists()
        assert (profile_dir / "temp").exists()

    def test_create_duplicate_raises(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Creating an existing profile raises ProfileExistsError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        with pytest.raises(ProfileExistsError):
            svc.create("alice")

    def test_create_normalizes_name(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Profile name is normalized (lowercase, hyphens)."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("Alice Work")
        assert (tmp_path / "profiles" / "alice-work").exists()


class TestProfileList:
    """Test profile listing."""

    def test_list_profiles(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Lists all profiles with active marker."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.create("bob")
        svc.switch("alice")
        profiles = svc.list()
        names = [p["name"] for p in profiles]
        assert "alice" in names
        assert "bob" in names
        alice = next(p for p in profiles if p["name"] == "alice")
        assert alice["active"] is True

    def test_list_empty(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Empty list when no profiles exist."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        assert svc.list() == []


class TestProfileSwitch:
    """Test profile switching."""

    def test_switch_profile(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Switching updates global config active_profile."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.create("bob")
        svc.switch("bob")
        config_path = tmp_path / "config.yaml"
        # Note: get_user_config_path is always ~/.moneybin/config.yaml,
        # so mock it for tests
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        from moneybin.utils.user_config import load_user_config

        assert load_user_config().active_profile == "bob"

    def test_switch_nonexistent_raises(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Switching to nonexistent profile raises."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        with pytest.raises(ProfileNotFoundError):
            svc.switch("nonexistent")


class TestProfileDelete:
    """Test profile deletion."""

    def test_delete_profile(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Deleting removes profile directory."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.delete("alice")
        assert not (tmp_path / "profiles" / "alice").exists()

    def test_delete_nonexistent_raises(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Deleting nonexistent profile raises."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        with pytest.raises(ProfileNotFoundError):
            svc.delete("nonexistent")


class TestProfileShow:
    """Test profile show (resolved settings)."""

    def test_show_profile(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Show returns resolved settings for a profile."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        info = svc.show("alice")
        assert info["name"] == "alice"
        assert "database_path" in info
        assert "alice" in str(info["database_path"])


class TestProfileSet:
    """Test setting config values on a profile."""

    def test_set_logging_level(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Set a config value in profile config.yaml."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.set("alice", "logging.level", "DEBUG")
        config_path = tmp_path / "profiles" / "alice" / "config.yaml"
        data = yaml.safe_load(config_path.read_text())
        assert data["logging"]["level"] == "DEBUG"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_profile_service.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Create `__init__.py` for test_services if needed**

```bash
mkdir -p tests/moneybin/test_services
touch tests/moneybin/test_services/__init__.py
```

- [ ] **Step 4: Implement `ProfileService`**

```python
# src/moneybin/services/profile_service.py
"""Profile lifecycle management for MoneyBin.

Handles creation, listing, switching, deletion, display, and
configuration of user profiles. Each profile is an isolation boundary
with its own database, logs, and configuration.
"""

import logging
import shutil
from pathlib import Path

import yaml

from moneybin.config import get_base_dir
from moneybin.utils.user_config import (
    generate_profile_config,
    get_default_profile,
    normalize_profile_name,
    set_default_profile,
)

logger = logging.getLogger(__name__)


class ProfileExistsError(Exception):
    """Raised when attempting to create a profile that already exists."""


class ProfileNotFoundError(Exception):
    """Raised when a profile does not exist."""


class ProfileService:
    """Manages profile lifecycle operations.

    All methods work against the base directory returned by get_base_dir().
    """

    def __init__(self) -> None:
        self._base = get_base_dir()
        self._profiles_dir = self._base / "profiles"

    def _profile_dir(self, name: str) -> Path:
        """Get the directory for a profile.

        Args:
            name: Raw profile name (will be normalized).

        Returns:
            Path to the profile directory.
        """
        return self._profiles_dir / normalize_profile_name(name)

    def create(self, name: str) -> Path:
        """Create a new profile with directory structure and config.

        Args:
            name: Profile name (will be normalized).

        Returns:
            Path to the created profile directory.

        Raises:
            ProfileExistsError: If profile already exists.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profiles_dir / normalized

        if profile_dir.exists():
            raise ProfileExistsError(f"Profile '{normalized}' already exists")

        # Create directory structure
        profile_dir.mkdir(parents=True)
        (profile_dir / "logs").mkdir()
        (profile_dir / "temp").mkdir()

        # Generate config.yaml
        generate_profile_config(profile_dir, normalized)

        logger.info(f"Created profile: {normalized}")
        return profile_dir

    def list(self) -> list[dict[str, str | bool]]:
        """List all profiles with active status.

        Returns:
            List of dicts with keys: name, active, path.
        """
        if not self._profiles_dir.exists():
            return []

        active = get_default_profile()
        profiles = []

        for entry in sorted(self._profiles_dir.iterdir()):
            if entry.is_dir() and (entry / "config.yaml").exists():
                profiles.append({
                    "name": entry.name,
                    "active": entry.name == active,
                    "path": str(entry),
                })

        return profiles

    def switch(self, name: str) -> None:
        """Switch the active profile.

        Args:
            name: Profile name to switch to.

        Raises:
            ProfileNotFoundError: If profile doesn't exist.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profiles_dir / normalized

        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")

        set_default_profile(normalized)
        logger.info(f"Switched to profile: {normalized}")

    def delete(self, name: str) -> None:
        """Delete a profile and all its data.

        Args:
            name: Profile name to delete.

        Raises:
            ProfileNotFoundError: If profile doesn't exist.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profiles_dir / normalized

        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")

        shutil.rmtree(profile_dir)
        logger.info(f"Deleted profile: {normalized}")

    def show(self, name: str | None = None) -> dict[str, str | bool]:
        """Show resolved settings for a profile.

        Args:
            name: Profile name. Defaults to active profile.

        Returns:
            Dict with resolved profile information.

        Raises:
            ProfileNotFoundError: If profile doesn't exist.
        """
        if name is None:
            name = get_default_profile() or "default"

        normalized = normalize_profile_name(name)
        profile_dir = self._profiles_dir / normalized

        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")

        config_path = profile_dir / "config.yaml"
        config_data: dict[str, object] = {}
        if config_path.exists():
            with open(config_path) as f:
                config_data = yaml.safe_load(f) or {}

        active = get_default_profile()
        db_path = profile_dir / "moneybin.duckdb"

        return {
            "name": normalized,
            "active": normalized == active,
            "path": str(profile_dir),
            "database_path": str(db_path),
            "database_exists": db_path.exists(),
            "config": config_data,
        }

    def set(self, name: str, key: str, value: str) -> None:
        """Set a config value in a profile's config.yaml.

        Args:
            name: Profile name.
            key: Dot-separated config key (e.g., "logging.level").
            value: Value to set.

        Raises:
            ProfileNotFoundError: If profile doesn't exist.
            ValueError: If key is invalid.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profiles_dir / normalized

        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")

        config_path = profile_dir / "config.yaml"
        data: dict[str, object] = {}
        if config_path.exists():
            with open(config_path) as f:
                loaded = yaml.safe_load(f)
                if isinstance(loaded, dict):
                    data = loaded

        # Parse dot-separated key
        parts = key.split(".")
        if len(parts) != 2:
            raise ValueError(
                f"Key must be section.field (e.g., 'logging.level'), got: {key}"
            )

        section, field = parts

        if section not in data:
            data[section] = {}
        section_dict = data[section]
        if not isinstance(section_dict, dict):
            data[section] = {}
            section_dict = data[section]

        # Coerce booleans and numbers
        if value.lower() in ("true", "false"):
            section_dict[field] = value.lower() == "true"
        elif value.isdigit():
            section_dict[field] = int(value)
        else:
            section_dict[field] = value

        with open(config_path, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Set {key}={value} for profile {normalized}")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_profile_service.py -v`
Expected: All PASS (some tests may need adjustments to mock `get_user_config_path` properly).

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/services/profile_service.py tests/moneybin/test_services/
git commit -m "feat: add ProfileService for profile lifecycle management

Implements create, list, switch, delete, show, and set operations.
Each profile gets its own directory with config.yaml, logs, and temp.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 5: Profile CLI Commands

**Files:**
- Create: `src/moneybin/cli/commands/profile.py`
- Test: `tests/moneybin/test_cli/test_cli_profile_commands.py`

Thin CLI wrappers around `ProfileService`.

- [ ] **Step 1: Write failing tests for profile CLI commands**

```python
# tests/moneybin/test_cli/test_cli_profile_commands.py
"""Tests for profile CLI commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.profile import app

runner = CliRunner()


class TestProfileCreate:
    """Test profile create command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_success(self, mock_cls: MagicMock) -> None:
        """profile create <name> calls service.create()."""
        mock_svc = mock_cls.return_value
        mock_svc.create.return_value = Path("/fake/profiles/alice")
        result = runner.invoke(app, ["create", "alice"])
        assert result.exit_code == 0
        mock_svc.create.assert_called_once_with("alice")

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_duplicate_fails(self, mock_cls: MagicMock) -> None:
        """profile create fails for existing profile."""
        from moneybin.services.profile_service import ProfileExistsError

        mock_svc = mock_cls.return_value
        mock_svc.create.side_effect = ProfileExistsError("exists")
        result = runner.invoke(app, ["create", "alice"])
        assert result.exit_code == 1


class TestProfileList:
    """Test profile list command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_list_profiles(self, mock_cls: MagicMock) -> None:
        """profile list displays profiles."""
        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = [
            {"name": "alice", "active": True, "path": "/fake"},
            {"name": "bob", "active": False, "path": "/fake"},
        ]
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "alice" in result.output


class TestProfileSwitch:
    """Test profile switch command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_switch_success(self, mock_cls: MagicMock) -> None:
        """profile switch calls service.switch()."""
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["switch", "bob"])
        assert result.exit_code == 0
        mock_svc.switch.assert_called_once_with("bob")


class TestProfileDelete:
    """Test profile delete command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_delete_requires_confirmation(self, mock_cls: MagicMock) -> None:
        """profile delete prompts for confirmation."""
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["delete", "alice"], input="n\n")
        assert result.exit_code == 0
        mock_svc.delete.assert_not_called()

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_delete_with_yes_flag(self, mock_cls: MagicMock) -> None:
        """profile delete --yes skips confirmation."""
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["delete", "alice", "--yes"])
        assert result.exit_code == 0
        mock_svc.delete.assert_called_once_with("alice")


class TestProfileShow:
    """Test profile show command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_show_active_profile(self, mock_cls: MagicMock) -> None:
        """profile show displays active profile info."""
        mock_svc = mock_cls.return_value
        mock_svc.show.return_value = {
            "name": "alice",
            "active": True,
            "path": "/fake/profiles/alice",
            "database_path": "/fake/profiles/alice/moneybin.duckdb",
            "database_exists": True,
            "config": {"logging": {"level": "INFO"}},
        }
        result = runner.invoke(app, ["show"])
        assert result.exit_code == 0
        assert "alice" in result.output


class TestProfileSet:
    """Test profile set command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_set_value(self, mock_cls: MagicMock) -> None:
        """profile set calls service.set()."""
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["set", "logging.level", "DEBUG"])
        assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_profile_commands.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement profile CLI commands**

```python
# src/moneybin/cli/commands/profile.py
"""Profile management commands for MoneyBin CLI.

Commands for creating, listing, switching, deleting, showing, and
configuring user profiles. Each profile isolates databases, configuration,
logs, and data.
"""

import logging
from typing import Annotated

import typer

from moneybin.services.profile_service import (
    ProfileExistsError,
    ProfileNotFoundError,
    ProfileService,
)

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage user profiles (create, list, switch, delete, show, set)",
    no_args_is_help=True,
)


@app.command("create")
def profile_create(
    name: Annotated[str, typer.Argument(help="Profile name (will be normalized)")],
) -> None:
    """Create a new profile with directory structure and config.

    Creates the profile directory, config.yaml, database placeholder,
    logs directory, and temp directory.

    Examples:
        moneybin profile create alice
        moneybin profile create "Business Account"
    """
    svc = ProfileService()
    try:
        profile_dir = svc.create(name)
        logger.info(f"✅ Created profile at {profile_dir}")
        logger.info("💡 Run 'moneybin db init' to create the encrypted database")
    except ProfileExistsError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("list")
def profile_list() -> None:
    """List all profiles, marking the active one.

    Example:
        moneybin profile list
    """
    svc = ProfileService()
    profiles = svc.list()

    if not profiles:
        logger.info("No profiles found")
        logger.info("💡 Run 'moneybin profile create <name>' to create one")
        return

    for p in profiles:
        marker = " (active)" if p["active"] else ""
        logger.info(f"  {p['name']}{marker}")


@app.command("switch")
def profile_switch(
    name: Annotated[str, typer.Argument(help="Profile name to switch to")],
) -> None:
    """Set a different profile as the active default.

    Examples:
        moneybin profile switch bob
        moneybin profile switch business
    """
    svc = ProfileService()
    try:
        svc.switch(name)
        logger.info(f"✅ Switched to profile: {name}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("delete")
def profile_delete(
    name: Annotated[str, typer.Argument(help="Profile name to delete")],
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt"),
    ] = False,
) -> None:
    """Delete a profile and all its data (database, logs, config).

    Examples:
        moneybin profile delete old-profile
        moneybin profile delete old-profile --yes
    """
    svc = ProfileService()

    if not yes:
        confirm = typer.confirm(
            f"Delete profile '{name}' and ALL its data? This cannot be undone."
        )
        if not confirm:
            return

    try:
        svc.delete(name)
        logger.info(f"✅ Deleted profile: {name}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("show")
def profile_show(
    name: Annotated[
        str | None,
        typer.Argument(help="Profile name (defaults to active profile)"),
    ] = None,
) -> None:
    """Show resolved settings for a profile.

    Examples:
        moneybin profile show
        moneybin profile show alice
    """
    svc = ProfileService()
    try:
        info = svc.show(name)
        marker = " (active)" if info["active"] else ""
        logger.info(f"Profile: {info['name']}{marker}")
        logger.info(f"  Path:     {info['path']}")
        logger.info(f"  Database: {info['database_path']}")
        db_status = "exists" if info["database_exists"] else "not created"
        logger.info(f"  DB state: {db_status}")
        if info.get("config"):
            logger.info("  Config:")
            for section, values in info["config"].items():
                if isinstance(values, dict):
                    for k, v in values.items():
                        logger.info(f"    {section}.{k}: {v}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("set")
def profile_set(
    key: Annotated[str, typer.Argument(help="Config key (e.g., logging.level)")],
    value: Annotated[str, typer.Argument(help="Value to set")],
    name: Annotated[
        str | None,
        typer.Option("--profile", "-p", help="Profile name (defaults to active)"),
    ] = None,
) -> None:
    """Set a configuration value on a profile.

    Examples:
        moneybin profile set logging.level DEBUG
        moneybin profile set sync.enabled true --profile business
    """
    svc = ProfileService()
    target = (
        name
        or (svc.list() and next((p["name"] for p in svc.list() if p["active"]), None))
        or "default"
    )
    try:
        svc.set(str(target), key, value)
        logger.info(f"✅ Set {key}={value}")
    except (ProfileNotFoundError, ValueError) as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_profile_commands.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/profile.py tests/moneybin/test_cli/test_cli_profile_commands.py
git commit -m "feat: add profile CLI commands (create, list, switch, delete, show, set)

Thin wrappers around ProfileService. Replaces the old config command
group for profile management.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 6: Old-Format Migration

**Files:**
- Modify: `src/moneybin/services/profile_service.py`
- Test: `tests/moneybin/test_migration.py`

Auto-migrate from old directory layout (`data/<name>/`, `logs/<name>/`, `.env.<name>`) to new `profiles/<name>/` layout on first run.

- [ ] **Step 1: Write failing tests for migration**

```python
# tests/moneybin/test_migration.py
"""Tests for old config format migration."""

import os
from pathlib import Path

import pytest
import yaml

from moneybin.services.profile_service import ProfileService


class TestMigrateOldLayout:
    """Test migration from data/<name>/ + logs/<name>/ to profiles/<name>/."""

    def test_migrates_database(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Migrates moneybin.duckdb from data/<name>/ to profiles/<name>/."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        # Create old layout
        old_data = tmp_path / "data" / "alice"
        old_data.mkdir(parents=True)
        (old_data / "moneybin.duckdb").write_text("fake-db")
        old_logs = tmp_path / "logs" / "alice"
        old_logs.mkdir(parents=True)
        (old_logs / "moneybin.log").write_text("fake-log")

        svc = ProfileService()
        migrated = svc.migrate_old_layout()
        assert migrated == ["alice"]

        new_dir = tmp_path / "profiles" / "alice"
        assert (new_dir / "moneybin.duckdb").exists()
        assert (new_dir / "logs" / "moneybin.log").exists()
        assert (new_dir / "config.yaml").exists()

    def test_migrates_global_config(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """Migrates default_profile to active_profile in global config."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        config_path = tmp_path / "config.yaml"
        config_path.write_text("default_profile: alice\n")
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        # Create old data dir so migration has something to find
        old_data = tmp_path / "data" / "alice"
        old_data.mkdir(parents=True)
        (old_data / "moneybin.duckdb").write_text("fake-db")

        svc = ProfileService()
        svc.migrate_old_layout()

        data = yaml.safe_load(config_path.read_text())
        assert "active_profile" in data
        assert "default_profile" not in data

    def test_skip_if_no_old_layout(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """No-op when no old layout exists."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        assert svc.migrate_old_layout() == []

    def test_skip_if_already_migrated(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """No-op when profiles/ already exists with content."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        (tmp_path / "profiles" / "alice" / "config.yaml").parent.mkdir(parents=True)
        (tmp_path / "profiles" / "alice" / "config.yaml").write_text(
            "logging:\n  level: INFO\n"
        )
        svc = ProfileService()
        assert svc.migrate_old_layout() == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_migration.py -v`
Expected: FAIL — `migrate_old_layout` doesn't exist.

- [ ] **Step 3: Implement `migrate_old_layout` on ProfileService**

Add to `src/moneybin/services/profile_service.py`:

```python
def migrate_old_layout(self) -> list[str]:
    """Migrate from old data/<name>/ + logs/<name>/ layout to profiles/<name>/.

    Detects old-format directories under data/ and moves their contents
    to profiles/<name>/. Safe to call multiple times — no-ops if already migrated.

    Returns:
        List of profile names that were migrated.
    """
    old_data_dir = self._base / "data"
    if not old_data_dir.exists():
        return []

    # Skip if profiles/ already has content (already migrated)
    if self._profiles_dir.exists() and any(self._profiles_dir.iterdir()):
        return []

    migrated: list[str] = []

    for entry in old_data_dir.iterdir():
        if not entry.is_dir():
            continue
        # Skip non-profile directories (raw, temp at top level)
        if not (entry / "moneybin.duckdb").exists() and not any(entry.glob("*.duckdb")):
            continue

        profile_name = entry.name
        profile_dir = self._profiles_dir / profile_name
        profile_dir.mkdir(parents=True, exist_ok=True)

        # Move database file
        for db_file in entry.glob("*.duckdb"):
            dest = profile_dir / db_file.name
            if not dest.exists():
                shutil.move(str(db_file), str(dest))

        # Move backups directory
        old_backups = entry / "backups"
        if old_backups.exists():
            new_backups = profile_dir / "backups"
            if not new_backups.exists():
                shutil.move(str(old_backups), str(new_backups))

        # Move temp directory
        old_temp = entry / "temp"
        if old_temp.exists():
            new_temp = profile_dir / "temp"
            if not new_temp.exists():
                shutil.move(str(old_temp), str(new_temp))
            else:
                shutil.rmtree(old_temp)

        # Move logs
        old_logs = self._base / "logs" / profile_name
        if old_logs.exists():
            new_logs = profile_dir / "logs"
            if not new_logs.exists():
                shutil.move(str(old_logs), str(new_logs))
            else:
                # Merge: move individual log files
                for log_file in old_logs.iterdir():
                    dest = new_logs / log_file.name
                    if not dest.exists():
                        shutil.move(str(log_file), str(dest))
                shutil.rmtree(old_logs)

        # Ensure logs and temp dirs exist
        (profile_dir / "logs").mkdir(exist_ok=True)
        (profile_dir / "temp").mkdir(exist_ok=True)

        # Generate profile config.yaml if it doesn't exist
        if not (profile_dir / "config.yaml").exists():
            generate_profile_config(profile_dir, profile_name)

        migrated.append(profile_name)
        logger.info(f"Migrated profile: {profile_name}")

    # Migrate global config key
    from moneybin.utils.user_config import (
        get_user_config_path,
        load_user_config,
        save_user_config,
    )

    config_path = get_user_config_path()
    if config_path.exists():
        try:
            with open(config_path) as f:
                raw = yaml.safe_load(f)
            if isinstance(raw, dict) and "default_profile" in raw:
                raw["active_profile"] = raw.pop("default_profile")
                with open(config_path, "w") as f:
                    yaml.safe_dump(raw, f, default_flow_style=False, sort_keys=False)
                logger.info("Migrated global config: default_profile -> active_profile")
        except Exception as e:
            logger.warning(f"Could not migrate global config: {e}")

    return migrated
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_migration.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/profile_service.py tests/moneybin/test_migration.py
git commit -m "feat: add old-format migration (data/<name>/ to profiles/<name>/)

Detects old directory layout and moves database, logs, temp, and
backups to the new profiles/<name>/ structure. Migrates global config
key from default_profile to active_profile.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 7: CLI Restructure — Dissolve `data`, Remove `config`/`extract`, Promote Commands

**Files:**
- Modify: `src/moneybin/cli/main.py`
- Delete: `src/moneybin/cli/commands/config.py`
- Delete: `src/moneybin/cli/commands/extract.py`
- Delete: `src/moneybin/cli/commands/credentials.py`
- Delete: `src/moneybin/cli/commands/data.py`
- Modify: `src/moneybin/cli/commands/__init__.py`
- Test: `tests/moneybin/test_cli/test_cli_restructure.py`

- [ ] **Step 1: Write tests for new CLI structure**

```python
# tests/moneybin/test_cli/test_cli_restructure.py
"""Tests for CLI restructure: removed, moved, and promoted commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


class TestRemovedCommands:
    """Removed commands should not exist."""

    def test_config_group_removed(self) -> None:
        """config command group no longer exists."""
        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code != 0

    def test_data_extract_removed(self) -> None:
        """data extract subgroup no longer exists."""
        result = runner.invoke(app, ["data", "extract", "ofx", "test.ofx"])
        assert result.exit_code != 0

    def test_data_group_removed(self) -> None:
        """data command group no longer exists."""
        result = runner.invoke(app, ["data", "--help"])
        assert result.exit_code != 0


class TestPromotedCommands:
    """Commands promoted from data subgroup to top-level."""

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_categorize_at_top_level(self, mock_profile) -> None:
        """categorize is a top-level command group."""
        result = runner.invoke(app, ["categorize", "--help"])
        assert result.exit_code == 0
        assert "apply-rules" in result.output

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_transform_at_top_level(self, mock_profile) -> None:
        """transform is a top-level command group."""
        result = runner.invoke(app, ["transform", "--help"])
        assert result.exit_code == 0
        assert "plan" in result.output
        assert "apply" in result.output

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_profile_at_top_level(self, mock_profile) -> None:
        """profile is a top-level command group."""
        result = runner.invoke(app, ["profile", "--help"])
        assert result.exit_code == 0
        assert "create" in result.output
        assert "list" in result.output
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_restructure.py -v`
Expected: FAIL — config still exists, data still exists, categorize/transform not at top level.

- [ ] **Step 3: Rewrite `main.py` with new command tree**

```python
# src/moneybin/cli/main.py
"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into groups: profile, import, sync, categorize, transform,
db, mcp, logs, and stub groups for future features.
"""

import logging
from typing import Annotated

import typer

from ..config import set_current_profile
from ..logging import setup_logging
from ..utils.user_config import ensure_default_profile
from .commands import categorize, db, import_cmd, mcp, profile, sync, transform

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="moneybin",
    help="MoneyBin: Personal financial data aggregation and analysis tool",
    add_completion=False,
    rich_markup_mode="rich",
    no_args_is_help=True,
)


@app.callback()
def main_callback(
    profile_name: Annotated[
        str | None,
        typer.Option(
            "--profile",
            "-p",
            help="User profile to use. Uses saved default if not specified.",
            envvar="MONEYBIN_PROFILE",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Enable verbose debug logging",
        ),
    ] = False,
) -> None:
    """Global options for MoneyBin CLI."""
    if profile_name is None:
        try:
            profile_name = ensure_default_profile()
        except KeyboardInterrupt:
            raise typer.Abort() from None

    try:
        set_current_profile(profile_name)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from e

    setup_logging(cli_mode=True, verbose=verbose, profile=profile_name)
    logger.info(f"Using profile: {profile_name}")


# Core command groups
app.add_typer(
    profile.app,
    name="profile",
    help="Manage user profiles (create, list, switch, delete, show, set)",
)
app.add_typer(
    import_cmd.app,
    name="import",
    help="Import financial files into MoneyBin",
)
app.add_typer(
    sync.app,
    name="sync",
    help="Sync transactions from external services",
)
app.add_typer(
    categorize.app,
    name="categorize",
    help="Manage transaction categories, rules, and merchants",
)
app.add_typer(
    transform.app,
    name="transform",
    help="Run SQLMesh data transformations",
)
app.add_typer(
    db.app,
    name="db",
    help="Database management and exploration",
)
app.add_typer(
    mcp.app,
    name="mcp",
    help="MCP server for AI assistant integration",
)

# Import and register stub commands and logs
# (These are added in Tasks 10-12)


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Delete removed modules**

```bash
rm src/moneybin/cli/commands/config.py
rm src/moneybin/cli/commands/extract.py
rm src/moneybin/cli/commands/credentials.py
rm src/moneybin/cli/commands/data.py
```

- [ ] **Step 5: Update `commands/__init__.py`**

```python
# src/moneybin/cli/commands/__init__.py
"""CLI command modules for MoneyBin."""
```

- [ ] **Step 6: Run tests to verify the restructure works**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_restructure.py -v`
Expected: All PASS.

- [ ] **Step 7: Update existing CLI tests for new paths**

Update `tests/moneybin/test_cli/test_cli_profiles.py` to reference the new command paths (e.g., `profile list` instead of `config credentials list-services` for basic profile tests).

- [ ] **Step 8: Run full test suite**

Run: `uv run pytest tests/ -v`
Fix any import errors or broken references to deleted modules.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: restructure CLI — dissolve data, remove config/extract, promote commands

- profile, categorize, transform are now top-level command groups
- Removes config, data, extract, credentials command modules
- data subgroup fully dissolved
- Simplified main.py command registration

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 8: Move `mcp show/kill` to `db ps/kill`

**Files:**
- Modify: `src/moneybin/cli/commands/db.py`
- Modify: `src/moneybin/cli/commands/mcp.py`
- Test: `tests/moneybin/test_cli/test_cli_restructure.py`

- [ ] **Step 1: Write failing tests**

```python
# Add to tests/moneybin/test_cli/test_cli_restructure.py
class TestMovedCommands:
    """Commands moved between groups."""

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_db_ps_exists(self, mock_profile) -> None:
        """db ps command exists."""
        result = runner.invoke(app, ["db", "ps", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_db_kill_exists(self, mock_profile) -> None:
        """db kill command exists."""
        result = runner.invoke(app, ["db", "kill", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_mcp_show_removed(self, mock_profile) -> None:
        """mcp show no longer exists."""
        result = runner.invoke(app, ["mcp", "show"])
        assert result.exit_code != 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_mcp_kill_removed(self, mock_profile) -> None:
        """mcp kill no longer exists."""
        result = runner.invoke(app, ["mcp", "kill"])
        assert result.exit_code != 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_restructure.py::TestMovedCommands -v`

- [ ] **Step 3: Move `_find_db_processes`, `show`, and `kill` from mcp.py to db.py**

Copy `_find_db_processes` helper function from `mcp.py` to `db.py`. Add `ps` and `kill` commands to `db.py` (rename `show` → `ps`). Remove `show` and `kill` from `mcp.py`, and remove `_find_db_processes` from `mcp.py`.

In `src/moneybin/cli/commands/db.py`, add:

```python
# Add imports at top
import signal


def _find_db_processes(db_path: Path) -> list[dict[str, str | int]]:
    # (Copy from mcp.py — the entire function unchanged)
    ...


@app.command("ps")
def db_ps(
    database: Path | None = typer.Option(
        None, "--database", "-d", help="Path to DuckDB database file"
    ),
) -> None:
    """Show processes holding the MoneyBin database file open."""
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)
    from moneybin.config import get_settings

    db_path = database or get_settings().database.path
    if not db_path.exists():
        logger.info("Database file does not exist yet: %s", db_path)
        return
    processes = _find_db_processes(db_path)
    if not processes:
        logger.info("No other processes have %s open", db_path.name)
        return
    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in processes:
        typer.echo(f"  {proc['pid']:<8} {proc['command']:<16} {proc['cmdline']}")


@app.command("kill")
def db_kill(
    database: Path | None = typer.Option(
        None, "--database", "-d", help="Path to DuckDB database file"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Kill processes holding the MoneyBin database file open."""
    from moneybin.logging.config import setup_logging

    setup_logging(cli_mode=True)
    from moneybin.config import get_settings

    db_path = database or get_settings().database.path
    if not db_path.exists():
        logger.info("Database file does not exist yet: %s", db_path)
        return
    processes = _find_db_processes(db_path)
    if not processes:
        logger.info("No other processes have %s open", db_path.name)
        return
    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in processes:
        typer.echo(f"  {proc['pid']:<8} {proc['command']:<16} {proc['cmdline']}")
    typer.echo()

    count = len(processes)
    noun = "process" if count == 1 else "processes"
    if not yes and not typer.confirm(f"Send SIGTERM to {count} {noun}?"):
        raise typer.Exit(0)

    killed = 0
    for proc in processes:
        pid = int(proc["pid"])
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info("Sent SIGTERM to PID %d (%s)", pid, proc["command"])
            killed += 1
        except ProcessLookupError:
            logger.warning("⚠️  PID %d already exited", pid)
        except PermissionError:
            logger.error("❌ No permission to kill PID %d (%s)", pid, proc["command"])
    if killed:
        logger.info("✅ Sent SIGTERM to %d %s", killed, noun)
```

In `src/moneybin/cli/commands/mcp.py`, remove `_find_db_processes`, `show`, and `kill`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_restructure.py::TestMovedCommands -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/db.py src/moneybin/cli/commands/mcp.py tests/moneybin/test_cli/test_cli_restructure.py
git commit -m "feat: move mcp show/kill to db ps/kill

These commands are about database connections, not MCP. Renamed
show -> ps and moved to the db command group.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 9: New Transform Commands

**Files:**
- Modify: `src/moneybin/cli/commands/transform.py`
- Test: `tests/moneybin/test_cli/test_cli_transform.py`

Add `status`, `validate`, `audit`, and `restate` commands as thin SQLMesh wrappers.

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_cli/test_cli_transform.py
"""Tests for transform CLI commands."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transform import app

runner = CliRunner()


class TestTransformStatus:
    """Test transform status command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_status_succeeds(self, mock_ctx_cls: MagicMock) -> None:
        """transform status calls SQLMesh info."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0


class TestTransformValidate:
    """Test transform validate command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_validate_succeeds(self, mock_ctx_cls: MagicMock) -> None:
        """transform validate runs plan in dry-run mode."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
        mock_ctx.plan.assert_called_once()


class TestTransformAudit:
    """Test transform audit command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_audit_succeeds(self, mock_ctx_cls: MagicMock) -> None:
        """transform audit runs SQLMesh audit."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(app, ["audit"])
        assert result.exit_code == 0
        mock_ctx.audit.assert_called_once()


class TestTransformRestate:
    """Test transform restate command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_restate_requires_confirmation(self, mock_ctx_cls: MagicMock) -> None:
        """transform restate prompts for confirmation."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(
            app,
            ["restate", "--model", "core.fct_transactions", "--start", "2026-01-01"],
            input="n\n",
        )
        assert result.exit_code == 0
        mock_ctx.restate_model.assert_not_called()

    @patch("moneybin.cli.commands.transform.Context")
    def test_restate_with_yes(self, mock_ctx_cls: MagicMock) -> None:
        """transform restate --yes skips confirmation."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(
            app,
            [
                "restate",
                "--model",
                "core.fct_transactions",
                "--start",
                "2026-01-01",
                "--yes",
            ],
        )
        assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_transform.py -v`
Expected: FAIL — commands don't exist.

- [ ] **Step 3: Implement new transform commands**

Add to `src/moneybin/cli/commands/transform.py`:

```python
from sqlmesh import (
    Context,  # type: ignore[import-untyped] — sqlmesh has no type stubs
)

# ... (keep existing plan and apply commands) ...


@app.command("status")
def transform_status() -> None:
    """Show current model state and environment.

    Displays SQLMesh environment info, model counts, and last run timestamp.
    """
    logger.info("⚙️  Checking SQLMesh status...")
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        # SQLMesh doesn't have a simple info() method exposed on Context,
        # but we can get environment info
        env = ctx.state_reader.get_environment("prod")
        if env:
            logger.info("Environment: prod")
            logger.info("  Last updated: %s", env.expiration_ts)
        else:
            logger.info("No SQLMesh environment initialized yet")
            logger.info("💡 Run 'moneybin transform apply' to initialize")
    except Exception as e:
        logger.error("❌ SQLMesh status failed: %s", e)
        raise typer.Exit(1) from e


@app.command("validate")
def transform_validate() -> None:
    """Check that model SQL parses and resolves without errors.

    Runs a dry-run plan to validate SQL syntax and references.
    """
    logger.info("⚙️  Validating SQLMesh models...")
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.plan(no_prompts=True, auto_apply=False)
        logger.info("✅ All models valid")
    except Exception as e:
        logger.error("❌ Validation failed: %s", e)
        raise typer.Exit(1) from e


@app.command("audit")
def transform_audit() -> None:
    """Run data quality assertions defined in SQLMesh models."""
    logger.info("⚙️  Running SQLMesh audits...")
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.audit()
        logger.info("✅ All audits passed")
    except Exception as e:
        logger.error("❌ Audit failed: %s", e)
        raise typer.Exit(1) from e


@app.command("restate")
def transform_restate(
    model: str = typer.Option(
        ..., "--model", help="Model name (e.g., core.fct_transactions)"
    ),
    start: str = typer.Option(
        ..., "--start", help="Start date for restatement (YYYY-MM-DD)"
    ),
    end: str | None = typer.Option(None, "--end", help="End date (defaults to today)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Force recompute a model for a date range.

    Restates (recomputes) a specific model's data for the given date range.
    This is useful when upstream data has been corrected.
    """
    if not yes:
        confirm = typer.confirm(
            f"Restate {model} from {start}? This will recompute all affected data."
        )
        if not confirm:
            return

    logger.info(f"⚙️  Restating {model} from {start}...")
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.restate_model(model, start=start, end=end)
        ctx.plan(auto_apply=True, no_prompts=True)
        logger.info(f"✅ Restated {model}")
    except Exception as e:
        logger.error(f"❌ Restatement failed: {e}")
        raise typer.Exit(1) from e
```

Move the `Context` import to be at function level (lazy import) to match the existing pattern, or place it module-level behind `TYPE_CHECKING` and re-import at call time. Follow the existing pattern in the file.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_transform.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/transform.py tests/moneybin/test_cli/test_cli_transform.py
git commit -m "feat: add transform status/validate/audit/restate commands

Thin wrappers around SQLMesh primitives. restate requires confirmation
unless --yes is provided.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 10: MCP Enhancements

**Files:**
- Modify: `src/moneybin/cli/commands/mcp.py`
- Test: `tests/moneybin/test_cli/test_cli_mcp_enhancements.py`

Add `list-tools`, `list-prompts`, `config`, and `config generate --install`.

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_cli/test_cli_mcp_enhancements.py
"""Tests for MCP CLI enhancements."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.mcp import app

runner = CliRunner()


class TestMCPListTools:
    """Test mcp list-tools command."""

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_tools(self, mock_importlib: MagicMock) -> None:
        """list-tools enumerates registered MCP tools."""
        result = runner.invoke(app, ["list-tools"])
        assert result.exit_code == 0


class TestMCPListPrompts:
    """Test mcp list-prompts command."""

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_prompts(self, mock_importlib: MagicMock) -> None:
        """list-prompts enumerates registered MCP prompts."""
        result = runner.invoke(app, ["list-prompts"])
        assert result.exit_code == 0


class TestMCPConfig:
    """Test mcp config command."""

    def test_config_show(self) -> None:
        """mcp config shows current MCP server config."""
        result = runner.invoke(app, ["config"])
        assert result.exit_code == 0


class TestMCPConfigGenerate:
    """Test mcp config generate command."""

    def test_generate_claude_desktop(self, tmp_path) -> None:
        """Generates valid config for claude-desktop."""
        result = runner.invoke(
            app, ["config", "generate", "--client", "claude-desktop"]
        )
        assert result.exit_code == 0
        assert "moneybin" in result.output.lower() or "MoneyBin" in result.output

    def test_generate_with_install(self, tmp_path, monkeypatch) -> None:
        """--install writes config to client config file."""
        # Mock the config file location
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,
        )
        result = runner.invoke(
            app,
            ["config", "generate", "--client", "claude-desktop", "--install"],
            input="y\n",
        )
        assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_mcp_enhancements.py -v`

- [ ] **Step 3: Implement MCP enhancement commands**

Add to `src/moneybin/cli/commands/mcp.py`:

```python
# MCP config subgroup
config_app = typer.Typer(help="MCP server configuration")
app.add_typer(config_app, name="config")


@app.command("list-tools")
def list_tools() -> None:
    """Show all available MCP tools.

    Enumerates tools registered with the MCP server. Useful for debugging
    "why can't Claude see this tool?" scenarios.
    """
    from moneybin.mcp.server import mcp as mcp_server

    # Import tool modules to register decorators
    for module in (
        "moneybin.mcp.tools",
        "moneybin.mcp.write_tools",
    ):
        importlib.import_module(module)

    tools = mcp_server._tool_manager._tools  # type: ignore[reportAttributeAccessIssue]
    if not tools:
        logger.info("No MCP tools registered")
        return

    logger.info("Registered MCP tools:")
    for name in sorted(tools.keys()):
        tool = tools[name]
        desc = tool.description or ""
        # Truncate long descriptions
        if len(desc) > 60:
            desc = desc[:57] + "..."
        logger.info("  %-35s %s", name, desc)
    logger.info("\n%d tools total", len(tools))


@app.command("list-prompts")
def list_prompts() -> None:
    """Show all available MCP prompts."""
    from moneybin.mcp.server import mcp as mcp_server

    importlib.import_module("moneybin.mcp.prompts")

    prompts = mcp_server._prompt_manager._prompts  # type: ignore[reportAttributeAccessIssue]
    if not prompts:
        logger.info("No MCP prompts registered")
        return

    logger.info("Registered MCP prompts:")
    for name in sorted(prompts.keys()):
        prompt = prompts[name]
        desc = prompt.description or ""
        if len(desc) > 60:
            desc = desc[:57] + "..."
        logger.info("  %-35s %s", name, desc)
    logger.info("\n%d prompts total", len(prompts))


@config_app.callback(invoke_without_command=True)
def config_show(ctx: typer.Context) -> None:
    """Show current MCP server configuration."""
    if ctx.invoked_subcommand is not None:
        return

    from moneybin.config import get_current_profile, get_settings

    settings = get_settings()
    profile = get_current_profile()

    logger.info("MCP Server Configuration:")
    logger.info("  Profile:    %s", profile)
    logger.info("  Database:   %s", settings.database.path)
    logger.info("  Max rows:   %d", settings.mcp.max_rows)
    logger.info("  Max chars:  %d", settings.mcp.max_chars)


# Supported client configs
_CLIENT_CONFIG_PATHS = {
    "claude-desktop": {
        "darwin": Path.home()
        / "Library/Application Support/Claude/claude_desktop_config.json",
        "linux": Path.home() / ".config/claude/claude_desktop_config.json",
    },
    "claude-code": {
        "all": Path.home() / ".claude" / "settings.json",
    },
}


def _get_client_config_path(client: str) -> Path | None:
    """Get the config file path for a supported MCP client."""
    import sys

    paths = _CLIENT_CONFIG_PATHS.get(client, {})
    return paths.get(sys.platform, paths.get("all"))


@config_app.command("generate")
def config_generate(
    client: str = typer.Option(
        ...,
        "--client",
        help="MCP client: claude-desktop, claude-code, cursor, vscode",
    ),
    profile_name: str | None = typer.Option(
        None, "--profile", help="Profile to configure (default: active)"
    ),
    install: bool = typer.Option(
        False, "--install", help="Write config to client's config file"
    ),
) -> None:
    """Generate MCP client configuration for MoneyBin.

    Creates the JSON configuration needed to register MoneyBin as an MCP
    server in the specified client application.

    Examples:
        moneybin mcp config generate --client claude-desktop
        moneybin mcp config generate --client claude-desktop --profile alice --install
    """
    import json
    import shutil
    import sys

    from moneybin.config import get_current_profile

    profile = profile_name or get_current_profile()
    moneybin_path = shutil.which("moneybin") or "moneybin"

    server_name = f"MoneyBin ({profile})" if profile != "default" else "MoneyBin"

    config_entry = {
        "command": moneybin_path,
        "args": ["--profile", profile, "mcp", "serve"],
    }

    typer.echo(f"\n{server_name} MCP configuration:\n")
    typer.echo(json.dumps({server_name: config_entry}, indent=2))

    if install:
        config_path = _get_client_config_path(client)
        if config_path is None:
            logger.error(f"❌ Unknown client '{client}' or unsupported platform")
            raise typer.Exit(1)

        if not typer.confirm(f"\nAdd {server_name} to {config_path}?"):
            raise typer.Exit(0)

        # Read existing config or create new
        existing: dict = {}
        if config_path.exists():
            with open(config_path) as f:
                existing = json.load(f)

        if "mcpServers" not in existing:
            existing["mcpServers"] = {}

        existing["mcpServers"][server_name] = config_entry

        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            json.dump(existing, f, indent=2)

        logger.info(f"✅ {server_name} added to {config_path}")
        logger.info("💡 Restart the client to pick up the change")
```

Note: The exact internal API for accessing `_tool_manager._tools` and `_prompt_manager._prompts` on the FastMCP server object may differ. Check the FastMCP source to confirm the correct attribute names. If the API is different, adapt the list-tools and list-prompts implementations accordingly.

Also add DB lock error handling to `mcp serve`. The spec requires that when the database is locked, `mcp serve` shows a helpful error:

```python
# In the serve command, wrap init_db() with DB lock detection:
from moneybin.database import DatabaseKeyError

try:
    init_db(db_path)
except DatabaseKeyError:
    logger.error("❌ Database is locked by another process")
    logger.info(
        "💡 Run 'moneybin db ps' to see what's holding it, or 'moneybin db kill' to release"
    )
    raise typer.Exit(1) from None
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_mcp_enhancements.py -v`
Expected: All PASS (with necessary mocking adjustments).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/mcp.py tests/moneybin/test_cli/test_cli_mcp_enhancements.py
git commit -m "feat: add mcp list-tools, list-prompts, config generate --install

list-tools/list-prompts enumerate registered MCP tools and prompts.
config generate creates client configuration JSON for Claude Desktop,
Claude Code, Cursor, and VS Code. --install writes directly to the
client's config file.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 11: Logs Command Group

**Files:**
- Create: `src/moneybin/cli/commands/logs.py`
- Modify: `src/moneybin/cli/main.py`
- Test: `tests/moneybin/test_cli/test_cli_logs.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_cli/test_cli_logs.py
"""Tests for logs CLI commands."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from moneybin.cli.commands.logs import app

runner = CliRunner()


class TestLogsPath:
    """Test logs path command."""

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_path_prints_log_dir(self, mock_settings) -> None:
        """logs path prints the log directory."""
        mock_settings.return_value.logging.log_file_path = Path(
            "/fake/profiles/alice/logs/moneybin.log"
        )
        result = runner.invoke(app, ["path"])
        assert result.exit_code == 0
        assert "/fake/profiles/alice/logs" in result.output


class TestLogsClean:
    """Test logs clean command."""

    def test_clean_with_dry_run(self, tmp_path: Path, monkeypatch) -> None:
        """logs clean --dry-run shows what would be deleted."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "old.log").write_text("old log")

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(app, ["clean", "--older-than", "30d", "--dry-run"])
            assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_logs.py -v`

- [ ] **Step 3: Implement logs commands**

```python
# src/moneybin/cli/commands/logs.py
"""Log management commands for MoneyBin CLI.

Commands for viewing, cleaning, and tailing log files.
"""

import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import typer

from moneybin.config import get_settings

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage log files",
    no_args_is_help=True,
)


def _parse_duration(duration: str) -> timedelta:
    """Parse a duration string like '30d', '7d', '24h' into a timedelta.

    Args:
        duration: Duration string (e.g., "30d", "7d", "24h", "60m").

    Returns:
        timedelta for the specified duration.

    Raises:
        ValueError: If format is invalid.
    """
    match = re.match(r"^(\d+)([dhm])$", duration.strip())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration}'. Use <number><unit> "
            "where unit is d (days), h (hours), or m (minutes)."
        )
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return timedelta(days=value)
    elif unit == "h":
        return timedelta(hours=value)
    else:
        return timedelta(minutes=value)


@app.command("path")
def logs_path() -> None:
    """Print the log directory for the current profile.

    Example:
        moneybin logs path
    """
    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent
    typer.echo(str(log_dir))


@app.command("clean")
def logs_clean(
    older_than: str = typer.Option(
        ..., "--older-than", help="Delete logs older than this (e.g., 30d, 7d, 24h)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted without deleting"
    ),
) -> None:
    """Delete log files older than a specified duration.

    Examples:
        moneybin logs clean --older-than 30d
        moneybin logs clean --older-than 7d --dry-run
    """
    try:
        delta = _parse_duration(older_than)
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e

    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent
    cutoff = datetime.now() - delta

    if not log_dir.exists():
        logger.info("Log directory does not exist: %s", log_dir)
        return

    deleted = 0
    freed_bytes = 0

    for log_file in log_dir.iterdir():
        if not log_file.is_file():
            continue
        mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
        if mtime < cutoff:
            size = log_file.stat().st_size
            if dry_run:
                logger.info("  Would delete: %s (%.1f KB)", log_file.name, size / 1024)
            else:
                log_file.unlink()
                logger.info("  Deleted: %s", log_file.name)
            deleted += 1
            freed_bytes += size

    if deleted == 0:
        logger.info("No log files older than %s", older_than)
    elif dry_run:
        logger.info(
            "Would delete %d file(s), freeing %.1f KB",
            deleted,
            freed_bytes / 1024,
        )
    else:
        logger.info(
            "✅ Deleted %d file(s), freed %.1f KB",
            deleted,
            freed_bytes / 1024,
        )


@app.command("tail")
def logs_tail(
    stream: str | None = typer.Option(
        None, "--stream", help="Filter by stream: mcp, sqlmesh"
    ),
    follow: bool = typer.Option(False, "-f", "--follow", help="Follow log output"),
    lines: int = typer.Option(20, "-n", "--lines", help="Number of lines to show"),
) -> None:
    """Show recent log entries, optionally following new output.

    Examples:
        moneybin logs tail
        moneybin logs tail -f
        moneybin logs tail --stream mcp -n 50
    """
    settings = get_settings()
    log_path = settings.logging.log_file_path

    if not log_path.exists():
        logger.info("No log file found: %s", log_path)
        return

    # Read last N lines
    with open(log_path) as f:
        all_lines = f.readlines()

    # Filter by stream if specified
    if stream:
        all_lines = [l for l in all_lines if stream.lower() in l.lower()]

    # Show last N lines
    for line in all_lines[-lines:]:
        typer.echo(line.rstrip())

    if follow:
        typer.echo("--- Following (Ctrl+C to stop) ---")
        try:
            with open(log_path) as f:
                f.seek(0, 2)  # Seek to end
                while True:
                    line = f.readline()
                    if line:
                        if stream is None or stream.lower() in line.lower():
                            typer.echo(line.rstrip())
                    else:
                        time.sleep(0.5)
        except KeyboardInterrupt:
            pass
```

- [ ] **Step 4: Wire logs into main.py**

Add to `src/moneybin/cli/main.py`:

```python
from .commands import logs

# After existing app.add_typer calls:
app.add_typer(
    logs.app,
    name="logs",
    help="Manage log files",
)
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_logs.py -v`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/cli/commands/logs.py src/moneybin/cli/main.py tests/moneybin/test_cli/test_cli_logs.py
git commit -m "feat: add logs command group (clean, path, tail)

logs path - print log directory for current profile
logs clean --older-than <duration> - delete old log files
logs tail [-f] [--stream mcp|sqlmesh] - tail log output

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 12: Phase 2 Stubs

**Files:**
- Create: `src/moneybin/cli/commands/stubs.py`
- Modify: `src/moneybin/cli/commands/sync.py`
- Modify: `src/moneybin/cli/main.py`
- Test: `tests/moneybin/test_cli/test_cli_restructure.py`

Stub all Phase 2 command groups so they appear in `--help` with clear "not implemented" messages.

- [ ] **Step 1: Write failing tests**

```python
# Add to tests/moneybin/test_cli/test_cli_restructure.py
class TestStubbedCommands:
    """Stubbed commands show 'not implemented' messages."""

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_matches_stubbed(self, mock_profile) -> None:
        """matches group exists but shows not-implemented."""
        result = runner.invoke(app, ["matches", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_track_stubbed(self, mock_profile) -> None:
        """track group exists but shows not-implemented."""
        result = runner.invoke(app, ["track", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_export_stubbed(self, mock_profile) -> None:
        """export group exists."""
        result = runner.invoke(app, ["export", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_stats_stubbed(self, mock_profile) -> None:
        """stats command exists."""
        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_restructure.py::TestStubbedCommands -v`

- [ ] **Step 3: Create stubs module**

```python
# src/moneybin/cli/commands/stubs.py
"""Stub commands for features not yet implemented.

These reserve the CLI namespace and provide clear messages directing
users to the relevant spec or future release. Each stub will be replaced
by a real implementation when its owning spec is executed.
"""

import logging

import typer

logger = logging.getLogger(__name__)


def _not_implemented(owning_spec: str) -> None:
    """Print a not-implemented message and exit cleanly.

    Args:
        owning_spec: Spec filename that will implement this command.
    """
    logger.info("This command is not yet implemented.")
    logger.info(f"💡 See docs/specs/{owning_spec} for the design")


# --- matches ---
matches_app = typer.Typer(
    help="Review and manage transaction matches (dedup, transfers)",
    no_args_is_help=True,
)


@matches_app.command("run")
def matches_run() -> None:
    """Run matcher against existing transactions."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("review")
def matches_review() -> None:
    """Interactive: accept/reject/skip/quit match proposals."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("log")
def matches_log() -> None:
    """Show recent match decisions."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("undo")
def matches_undo() -> None:
    """Reverse a match decision."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("backfill")
def matches_backfill() -> None:
    """One-time scan of all existing transactions."""
    _not_implemented("matching-same-record-dedup.md")


# --- track ---
track_app = typer.Typer(
    help="Balance tracking, net worth, and financial monitoring",
    no_args_is_help=True,
)

track_balance_app = typer.Typer(help="Balance assertions and tracking")
track_app.add_typer(track_balance_app, name="balance")


@track_balance_app.command("show")
def track_balance_show() -> None:
    """Show current balance for an account."""
    _not_implemented("net-worth.md")


track_networth_app = typer.Typer(help="Net worth tracking")
track_app.add_typer(track_networth_app, name="networth")


@track_networth_app.command("show")
def track_networth_show() -> None:
    """Show current net worth."""
    _not_implemented("net-worth.md")


# budget, recurring, investments — future specs
track_budget_app = typer.Typer(help="Budget tracking")
track_app.add_typer(track_budget_app, name="budget")


@track_budget_app.callback(invoke_without_command=True)
def track_budget_stub() -> None:
    """Budget tracking commands."""
    _not_implemented("budget-tracking.md")


track_recurring_app = typer.Typer(help="Recurring transaction detection")
track_app.add_typer(track_recurring_app, name="recurring")


@track_recurring_app.callback(invoke_without_command=True)
def track_recurring_stub() -> None:
    """Recurring transaction commands."""
    logger.info("This command is not yet implemented.")
    logger.info("💡 This feature is planned for a future spec")


track_investments_app = typer.Typer(help="Investment tracking")
track_app.add_typer(track_investments_app, name="investments")


@track_investments_app.callback(invoke_without_command=True)
def track_investments_stub() -> None:
    """Investment tracking commands."""
    _not_implemented("investment-tracking.md")


# --- export ---
export_app = typer.Typer(help="Export data to CSV, Excel, and other formats")


@export_app.callback(invoke_without_command=True)
def export_callback() -> None:
    """Export financial data."""
    _not_implemented("export.md")


# --- stats ---
def stats_command() -> None:
    """Show lifetime metric aggregates."""
    _not_implemented("observability.md")


# --- db migrate ---
db_migrate_app = typer.Typer(help="Database migration management")


@db_migrate_app.command("apply")
def db_migrate_apply() -> None:
    """Apply pending database migrations."""
    _not_implemented("database-migration.md")


@db_migrate_app.command("status")
def db_migrate_status() -> None:
    """Show migration state."""
    _not_implemented("database-migration.md")
```

- [ ] **Step 4: Update sync.py with stubbed commands**

Replace the `sync_all` command in `src/moneybin/cli/commands/sync.py`:

```python
"""Data synchronization commands for MoneyBin CLI."""

import logging

import typer

app = typer.Typer(
    help="Sync financial data from external services",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


def _not_implemented(owning_spec: str) -> None:
    """Print a not-implemented message."""
    logger.info("This command is not yet implemented.")
    logger.info(f"💡 See docs/specs/{owning_spec} for the design")


@app.command("login")
def sync_login() -> None:
    """Authenticate with moneybin-server."""
    _not_implemented("sync-overview.md")


@app.command("logout")
def sync_logout() -> None:
    """Clear stored JWT from keychain."""
    _not_implemented("sync-overview.md")


@app.command("connect")
def sync_connect() -> None:
    """Connect a bank account."""
    _not_implemented("sync-overview.md")


@app.command("disconnect")
def sync_disconnect() -> None:
    """Remove an institution."""
    _not_implemented("sync-overview.md")


@app.command("pull")
def sync_pull(
    force: bool = typer.Option(False, "--force", "-f", help="Force full sync"),
    institution: str | None = typer.Option(
        None, "--institution", help="Sync specific institution"
    ),
) -> None:
    """Pull data from connected institutions."""
    _not_implemented("sync-overview.md")


@app.command("status")
def sync_status() -> None:
    """Show connected institutions and sync health."""
    _not_implemented("sync-overview.md")


@app.command("rotate-key")
def sync_rotate_key() -> None:
    """Rotate E2E encryption key pair."""
    _not_implemented("sync-overview.md")


# sync schedule subgroup
schedule_app = typer.Typer(help="Manage scheduled sync jobs")
app.add_typer(schedule_app, name="schedule")


@schedule_app.command("set")
def sync_schedule_set() -> None:
    """Install daily sync schedule."""
    _not_implemented("sync-overview.md")


@schedule_app.command("show")
def sync_schedule_show() -> None:
    """Show current schedule details."""
    _not_implemented("sync-overview.md")


@schedule_app.command("remove")
def sync_schedule_remove() -> None:
    """Uninstall scheduled sync job."""
    _not_implemented("sync-overview.md")
```

- [ ] **Step 5: Wire stubs into main.py**

Add to `src/moneybin/cli/main.py`:

```python
from .commands.stubs import (
    db_migrate_app,
    export_app,
    matches_app,
    stats_command,
    track_app,
)

app.add_typer(matches_app, name="matches", help="Review and manage transaction matches")
app.add_typer(track_app, name="track", help="Balance tracking and net worth")
app.add_typer(export_app, name="export", help="Export data to external formats")


# stats is a single command, not a group
@app.command("stats")
def stats() -> None:
    """Show lifetime metric aggregates."""
    stats_command()


# Add db migrate as a sub-typer of db
from .commands import db as db_module

db_module.app.add_typer(
    db_migrate_app, name="migrate", help="Database migration management"
)
```

- [ ] **Step 6: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/test_cli_restructure.py::TestStubbedCommands -v`
Expected: All PASS.

- [ ] **Step 7: Run full test suite**

Run: `uv run pytest tests/ -v`
Fix any remaining failures.

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/cli/commands/stubs.py src/moneybin/cli/commands/sync.py src/moneybin/cli/main.py tests/moneybin/test_cli/test_cli_restructure.py
git commit -m "feat: add Phase 2 stub commands (matches, track, export, stats, db migrate)

Reserves CLI namespace for future features. Each stub shows a
not-implemented message with a pointer to the owning spec.
sync subcommands updated from single 'all' to full stub surface.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 13: Integration Wiring and Migration Hook

**Files:**
- Modify: `src/moneybin/cli/main.py`
- Test: `tests/moneybin/test_cli/test_cli_profiles.py`

Wire migration into the main callback so it runs automatically on first use after upgrade. Update the `ensure_default_profile` path to create profiles in the new layout.

- [ ] **Step 1: Write integration test**

```python
# Add to tests/moneybin/test_cli/test_cli_profiles.py
class TestMigrationOnFirstRun:
    """Test auto-migration triggers on CLI startup."""

    def test_old_layout_migrated_on_startup(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch", mocker
    ) -> None:
        """Old data/<name>/ layout is migrated when CLI starts."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        monkeypatch.delenv("MONEYBIN_PROFILE", raising=False)

        # Create old layout
        old_data = tmp_path / "data" / "alice"
        old_data.mkdir(parents=True)
        (old_data / "moneybin.duckdb").write_text("fake-db")

        # Set up global config with old key name
        global_config = tmp_path / "config.yaml"
        global_config.write_text("default_profile: alice\n")
        mocker.patch(
            "moneybin.utils.user_config.get_user_config_path",
            return_value=global_config,
        )
        mocker.patch("moneybin.cli.main.ensure_default_profile", return_value="alice")

        result = runner.invoke(app, ["--profile=alice", "profile", "list"])
        # After migration, alice should be in profiles/
        assert (tmp_path / "profiles" / "alice" / "moneybin.duckdb").exists()
```

- [ ] **Step 2: Add migration hook to main callback**

In `src/moneybin/cli/main.py`, add to the `main_callback` function after profile resolution:

```python
# Auto-migrate old directory layout on first run
from moneybin.services.profile_service import ProfileService

try:
    svc = ProfileService()
    migrated = svc.migrate_old_layout()
    if migrated:
        logger.info("Migrated %d profile(s) to new directory layout", len(migrated))
except Exception:
    logger.debug("Migration check failed", exc_info=True)
```

- [ ] **Step 3: Update `ensure_default_profile` to create profile in new layout**

In `src/moneybin/utils/user_config.py`, update `ensure_default_profile`:

```python
def ensure_default_profile() -> str:
    """Ensure a default profile exists, prompting user if necessary."""
    default_profile = get_default_profile()

    if default_profile:
        return default_profile

    profile_name = prompt_for_profile_name()
    set_default_profile(profile_name)

    # Create the profile directory structure
    from moneybin.services.profile_service import ProfileService

    try:
        svc = ProfileService()
        profile_dir = svc.create(profile_name)
        print(f"\n🎉 Your default profile '{profile_name}' has been created!")
        print(f"    Data will be stored in: {profile_dir}\n")
    except Exception:
        # Profile creation is best-effort during first-run
        from moneybin.config import get_base_dir

        base = get_base_dir()
        print(f"\n🎉 Your default profile '{profile_name}' has been created!")
        print(f"    Data will be stored in: {base / 'profiles' / profile_name}\n")

    return profile_name
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_cli/ -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/main.py src/moneybin/utils/user_config.py tests/moneybin/test_cli/test_cli_profiles.py
git commit -m "feat: auto-migrate old layout on CLI startup, update first-run flow

Migration from data/<name>/ to profiles/<name>/ runs automatically
on first CLI invocation. ensure_default_profile now creates profiles
in the new directory layout.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 14: Update Spec Status, Final Checks, Format and Lint

**Files:**
- Modify: `docs/specs/cli-restructure.md`
- Modify: `docs/specs/INDEX.md`
- Modify: `private/implementation.md`

- [ ] **Step 1: Run format and lint**

Run: `uv run ruff format . && uv run ruff check .`
Fix any issues.

- [ ] **Step 2: Run type checker on modified files**

Run: `uv run pyright src/moneybin/cli/ src/moneybin/config.py src/moneybin/utils/user_config.py src/moneybin/services/profile_service.py`
Fix any type errors.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v`
All tests must pass.

- [ ] **Step 4: Update spec status to in-progress**

In `docs/specs/cli-restructure.md`, change status from `ready` to `in-progress`.

In `docs/specs/INDEX.md`, update the CLI restructure entry status.

In `private/implementation.md`, update item #0 status to "Plan: complete, Implementation: in-progress".

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: format, lint, type-check; update spec status to in-progress

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Post-Implementation Notes

### `sqlmesh/config.py` compatibility

The `sqlmesh/config.py` sets `MONEYBIN_HOME` to the project root before importing `get_base_dir()`. With the new resolution logic, this continues to work: priority 1 (MONEYBIN_HOME) returns the project root. When running `sqlmesh` from the repo checkout without that env var, priority 3 (repo detection) returns `cwd`. Both paths result in the same behavior as today.

### Test environment

Tests that call `get_base_dir()` and expect `cwd` must either:
1. Run in the moneybin repo checkout (repo detection triggers), or
2. Set `MONEYBIN_HOME` or `MONEYBIN_ENVIRONMENT=development` via monkeypatch.

The `conftest.py` fixture should ensure test isolation by setting one of these. Most tests already use `monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))`.

### What's NOT in this plan

- `import file` pipeline orchestration (auto match + categorize) — deferred to `smart-import-tabular.md`
- `categorize auto-*` commands — deferred to `categorization-auto-rules.md`
- `matches` implementation — deferred to matching specs
- `track` implementation — deferred to `net-worth.md`
- `db migrate` implementation — deferred to `database-migration.md`
- `stats` implementation — deferred to `observability.md`
- `--output json|table` universal flag — deferred to `cli-ux-standards.md`
