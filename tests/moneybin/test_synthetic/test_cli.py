# ruff: noqa: S108
"""Tests for synthetic data CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.synthetic import app
from moneybin.cli.main import app as main_app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols


def _terminal(*, interactive: bool) -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=interactive,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols(success="OK", attention="!", failure="X", action=">"),
        minus="-",
    )


class TestGenerateCommand:
    """Test the 'synthetic generate' CLI command."""

    @pytest.fixture(autouse=True)
    def mock_profile(self, mocker: Any) -> None:
        """Prevent generate/reset from mutating process-wide profile state."""
        self.mock_get_profile = mocker.patch(
            "moneybin.config.get_current_profile", return_value="default"
        )
        self.mock_set_profile = mocker.patch("moneybin.config.set_current_profile")
        self.mock_clear_profile = mocker.patch("moneybin.config.clear_current_profile")

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_get_database(self, mocker: Any) -> MagicMock:
        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        # Make the "already has data" check return 0 rows
        mock_db.execute.return_value.fetchone.return_value = (0,)
        return mocker.patch(
            "moneybin.database.get_database",
            return_value=mock_db,
        )

    @pytest.fixture
    def mock_engine(self, mocker: Any) -> MagicMock:
        mock_result = MagicMock()
        mock_result.persona = "basic"
        mock_result.seed = 42
        mock_result.accounts = [MagicMock()]
        mock_result.transactions = [MagicMock()] * 100
        mock_result.start_date = MagicMock(__str__=lambda s: "2024-01-01")  # type: ignore[reportUnknownLambdaType]  # MagicMock dunder override
        mock_result.end_date = MagicMock(__str__=lambda s: "2024-12-31")  # type: ignore[reportUnknownLambdaType]  # MagicMock dunder override
        mock_cls = mocker.patch(
            "moneybin.synthetic.engine.GeneratorEngine",
        )
        mock_cls.return_value.generate.return_value = mock_result
        return mock_cls

    @pytest.fixture
    def mock_writer(self, mocker: Any) -> MagicMock:
        mock_cls = mocker.patch(
            "moneybin.synthetic.writer.SyntheticWriter",
        )
        mock_cls.return_value.write.return_value = {
            "ofx_accounts": 1,
            "ofx_transactions": 80,
            "tabular_transactions": 20,
            "ground_truth": 100,
        }
        return mock_cls

    @pytest.fixture
    def mock_run_transforms(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.services.import_service.ImportService.run_transforms",
            return_value=True,
        )

    def test_generate_requires_persona(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["generate"])
        assert result.exit_code != 0

    def test_registered_cli_rejects_json_before_prompt_or_mutation(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """Synthetic is text-only; Click rejects JSON at the real command boundary."""
        confirm = mocker.patch("typer.confirm")
        get_database = mocker.patch("moneybin.database.get_database")

        result = runner.invoke(
            main_app,
            ["synthetic", "generate", "--persona", "basic", "--output", "json"],
        )

        assert result.exit_code == 2, result.output
        assert "No such option: --output" in result.output
        confirm.assert_not_called()
        get_database.assert_not_called()

    def test_generate_success(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
        mock_engine: MagicMock,
        mock_writer: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        result = runner.invoke(app, ["generate", "--persona", "basic", "--seed", "42"])
        assert result.exit_code == 0
        mock_engine.assert_called_once()
        mock_writer.return_value.write.assert_called_once()
        # Profile must be restored after successful generation
        self.mock_set_profile.assert_called_with("default")

    def test_generate_unknown_persona(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
    ) -> None:
        with patch(
            "moneybin.synthetic.engine.GeneratorEngine",
            side_effect=FileNotFoundError("Unknown persona: 'bad'"),
        ):
            result = runner.invoke(app, ["generate", "--persona", "bad"])
            assert result.exit_code == 1

    def test_generate_restores_profile_on_error(
        self,
        runner: CliRunner,
        mocker: Any,
    ) -> None:
        """Profile must be restored even when generate fails."""
        from moneybin.database import DatabaseKeyError

        mocker.patch(
            "moneybin.database.get_database",
            side_effect=DatabaseKeyError("no key"),
        )
        result = runner.invoke(app, ["generate", "--persona", "basic"])
        assert result.exit_code == 1
        self.mock_set_profile.assert_called_with("default")

    def test_generate_transform_failure_reports_partial_saved_counts_and_exits_one(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
        mock_engine: MagicMock,
        mock_writer: MagicMock,
        mock_run_transforms: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A requested transform is part of the operation, even after raw writes."""
        mock_run_transforms.side_effect = RuntimeError("private exception detail")
        caplog.set_level("INFO", logger="moneybin.cli.commands.synthetic")

        result = runner.invoke(app, ["generate", "--persona", "basic", "--seed", "42"])

        assert result.exit_code == 1, result.output
        assert "Generation partially completed" in result.stdout
        assert "Transactions saved:  100" in result.stdout
        assert "Reports are stale" in result.stdout
        assert "Report materialization failed (RuntimeError)" in caplog.text
        assert "private exception detail" not in caplog.text

    def test_generate_skip_transform_is_an_intentional_complete_mode(
        self,
        runner: CliRunner,
        mock_get_database: MagicMock,
        mock_engine: MagicMock,
        mock_writer: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        result = runner.invoke(
            app,
            ["generate", "--persona", "basic", "--seed", "42", "--skip-transform"],
        )

        assert result.exit_code == 0, result.output
        assert "Generation complete" in result.stdout
        assert "Transforms:          Skipped by request" in result.stdout
        mock_run_transforms.assert_not_called()

    def test_generate_partial_receipt_is_ascii_safe(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_get_database: MagicMock,
        mock_engine: MagicMock,
        mock_writer: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        mock_run_transforms.side_effect = RuntimeError("failed")
        mocker.patch(
            "moneybin.cli.commands.synthetic.get_terminal_policy",
            return_value=_terminal(interactive=False),
        )
        result = runner.invoke(app, ["generate", "--persona", "basic", "--seed", "42"])
        assert result.exit_code == 1, result.output
        assert "✓" not in result.stdout
        assert "×" not in result.stdout

    def test_generate_interrupt_restores_profile_and_reports_unknown_scope(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        mocker.patch("moneybin.database.get_database", side_effect=KeyboardInterrupt)
        result = runner.invoke(app, ["generate", "--persona", "basic"])
        assert result.exit_code == 130, result.output
        assert "Saved scope is unknown." in result.stdout
        self.mock_set_profile.assert_called_with("default")

    @pytest.mark.parametrize(
        ("outcome", "exit_code"),
        [("success", 0), ("classified_error", 1), ("interrupt", 130)],
    )
    def test_generate_clears_an_initially_unset_runtime_profile(
        self,
        runner: CliRunner,
        mocker: Any,
        request: pytest.FixtureRequest,
        outcome: str,
        exit_code: int,
    ) -> None:
        """Temporary synthetic generation must not leak its target profile."""
        from moneybin.database import DatabaseKeyError

        self.mock_get_profile.side_effect = RuntimeError("no runtime profile")
        if outcome == "success":
            request.getfixturevalue("mock_get_database")
            request.getfixturevalue("mock_engine")
            request.getfixturevalue("mock_writer")
            request.getfixturevalue("mock_run_transforms")
        elif outcome == "classified_error":
            mocker.patch(
                "moneybin.database.get_database", side_effect=DatabaseKeyError("no key")
            )
        else:
            mocker.patch(
                "moneybin.database.get_database", side_effect=KeyboardInterrupt
            )

        result = runner.invoke(app, ["generate", "--persona", "basic"])

        assert result.exit_code == exit_code, result.output
        self.mock_set_profile.assert_called_once_with("alice")
        self.mock_clear_profile.assert_called_once_with()


class TestResetCommand:
    """Test the 'synthetic reset' CLI command."""

    @pytest.fixture(autouse=True)
    def mock_profile(self, mocker: Any) -> None:
        """Prevent reset from mutating process-wide profile state."""
        self.mock_get_profile = mocker.patch(
            "moneybin.config.get_current_profile", return_value="default"
        )
        self.mock_set_profile = mocker.patch("moneybin.config.set_current_profile")
        self.mock_clear_profile = mocker.patch("moneybin.config.clear_current_profile")

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_reset_requires_persona(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["reset"])
        assert result.exit_code != 0

    def test_reset_success_with_yes(
        self,
        runner: CliRunner,
        mocker: Any,
    ) -> None:
        """Reset --yes with ground_truth present should delete and regenerate."""
        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        # ground_truth table exists
        mock_db.execute.return_value.fetchone.return_value = (1,)
        mock_db.path = Path("/tmp/test.duckdb")
        mocker.patch("moneybin.database.get_database", return_value=mock_db)
        # Profile-safety guard: the mocked profile is synthetic-only (the coarse
        # (1,) fetchone mock would otherwise read as real data present).
        mocker.patch(
            "moneybin.synthetic.reset.has_non_synthetic_data", return_value=False
        )

        # Mock _run_generate to avoid the full pipeline
        mock_run = mocker.patch(
            "moneybin.cli.commands.synthetic._run_generate",
        )
        mock_run.return_value = MagicMock(partial=False)
        mocker.patch("moneybin.cli.commands.synthetic._render_generation_receipt")

        result = runner.invoke(
            app, ["reset", "--persona", "basic", "--yes", "--seed", "42"]
        )
        assert result.exit_code == 0
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs | {"terminal": None} == {
            "persona": "basic",
            "profile": "alice",
            "years": None,
            "seed": 42,
            "skip_transform": False,
            "terminal": None,
            "cli_actor": "synthetic_reset",
        }
        # Profile must be restored after successful reset
        self.mock_set_profile.assert_called_with("default")

    def test_reset_requires_yes_or_prompt(self, runner: CliRunner) -> None:
        """Without --yes, reset should prompt for confirmation."""
        # CliRunner sends EOF on stdin by default, so prompt is declined
        with (
            patch("moneybin.database.get_database") as mock_get_db,
            patch(
                "moneybin.synthetic.reset.has_non_synthetic_data",
                return_value=False,
            ),
        ):
            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            # ground_truth table exists check returns 1
            mock_db.execute.return_value.fetchone.return_value = (1,)
            mock_db.path = Path("/tmp/test.duckdb")
            mock_get_db.return_value = mock_db
            result = runner.invoke(app, ["reset", "--persona", "basic"])
            # Should either prompt and abort, or succeed with --yes
            assert result.exit_code != 0 or "Aborted" in (result.output or "")
        # Profile must be restored even after user declines
        self.mock_set_profile.assert_called_with("default")

    def test_reset_noninteractive_refuses_before_opening_the_target_database(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        """An unanswerable destructive prompt must not begin a reset preflight."""
        get_database = mocker.patch("moneybin.database.get_database")
        mocker.patch(
            "moneybin.cli.commands.synthetic.get_terminal_policy",
            return_value=_terminal(interactive=False),
        )

        result = runner.invoke(app, ["reset", "--persona", "basic"])

        assert result.exit_code == 1, result.output
        get_database.assert_not_called()

    def test_reset_decline_names_the_exact_target_and_says_no_reset_started(
        self, runner: CliRunner, mocker: Any
    ) -> None:
        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.execute.return_value.fetchone.return_value = (1,)
        mocker.patch("moneybin.database.get_database", return_value=mock_db)
        mocker.patch(
            "moneybin.synthetic.reset.has_non_synthetic_data", return_value=False
        )
        mocker.patch(
            "moneybin.cli.commands.synthetic.get_terminal_policy",
            return_value=_terminal(interactive=True),
        )
        reset_rows = mocker.patch("moneybin.synthetic.reset.reset_synthetic_rows")

        result = runner.invoke(app, ["reset", "--persona", "basic"], input="n\n")

        assert result.exit_code == 1, result.output
        assert "profile 'alice'" in result.output
        assert "No reset was started." in result.output
        reset_rows.assert_not_called()

    @pytest.mark.parametrize(
        ("outcome", "exit_code"),
        [("success", 0), ("classified_error", 1), ("interrupt", 130)],
    )
    def test_reset_clears_an_initially_unset_runtime_profile(
        self,
        runner: CliRunner,
        mocker: Any,
        outcome: str,
        exit_code: int,
    ) -> None:
        """Reset must restore the unset runtime state on every exit path."""
        from moneybin.database import DatabaseKeyError

        self.mock_get_profile.side_effect = RuntimeError("no runtime profile")
        if outcome == "classified_error":
            mocker.patch(
                "moneybin.database.get_database", side_effect=DatabaseKeyError("no key")
            )
        else:
            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_db.execute.return_value.fetchone.return_value = (1,)
            mock_db.path = Path("/tmp/test.duckdb")
            mocker.patch("moneybin.database.get_database", return_value=mock_db)
            mocker.patch(
                "moneybin.synthetic.reset.has_non_synthetic_data", return_value=False
            )
            mocker.patch("moneybin.synthetic.reset.reset_synthetic_rows")
            run_generate = mocker.patch("moneybin.cli.commands.synthetic._run_generate")
            if outcome == "success":
                run_generate.return_value = MagicMock(partial=False)
                mocker.patch(
                    "moneybin.cli.commands.synthetic._render_generation_receipt"
                )
            else:
                run_generate.side_effect = KeyboardInterrupt

        result = runner.invoke(app, ["reset", "--persona", "basic", "--yes"])

        assert result.exit_code == exit_code, result.output
        self.mock_set_profile.assert_called_once_with("alice")
        self.mock_clear_profile.assert_called_once_with()


class TestPersonaEnumerationSites:
    """Every persona YAML must be reachable from both CLI surfaces.

    These are hand-maintained lists beside a directory of files, so they are
    asserted by set equality rather than membership — a subset check would
    still pass with a persona nobody wired up.
    """

    def _persona_names_on_disk(self) -> set[str]:
        from moneybin.synthetic import models

        personas_dir = Path(models.__file__).parent / "data" / "personas"
        return {path.stem for path in personas_dir.glob("*.yaml")}

    def test_demo_offers_every_persona(self) -> None:
        from moneybin.cli.commands.demo import (
            _PERSONAS,  # pyright: ignore[reportPrivateUsage]  # the guard's subject
        )

        assert set(_PERSONAS) == self._persona_names_on_disk()

    def test_every_persona_has_a_profile_name(self) -> None:
        from moneybin.cli.commands.synthetic import (
            _PERSONA_PROFILES,  # pyright: ignore[reportPrivateUsage]  # the subject
        )

        assert set(_PERSONA_PROFILES) == self._persona_names_on_disk()

    def test_profile_names_are_distinct(self) -> None:
        """Two personas sharing a profile would generate into one database."""
        from moneybin.cli.commands.synthetic import (
            _PERSONA_PROFILES,  # pyright: ignore[reportPrivateUsage]  # the subject
        )

        profiles = list(_PERSONA_PROFILES.values())
        assert len(profiles) == len(set(profiles))
