"""Data transformation commands for MoneyBin CLI.

This module provides commands for running dbt transformations on the loaded
DuckDB data, with proper error handling and logging integration.
"""

import logging
import subprocess  # noqa: S404
from pathlib import Path

import typer

from moneybin.config import get_current_profile

app = typer.Typer(help="Run data transformations using dbt")
logger = logging.getLogger(__name__)


def _validate_models_parameter(models: str) -> None:
    """Validate models parameter to prevent shell injection.

    Args:
        models: The models parameter to validate

    Raises:
        typer.Exit: If invalid characters are found
    """
    if any(
        char in models
        for char in [";", "|", "&", "$", "`", "(", ")", "<", ">", '"', "'"]
    ):
        logger.error("❌ Invalid characters in models parameter")
        raise typer.Exit(1)


def _validate_project_dir(project_dir: Path) -> str:
    """Validate project directory path to prevent path injection.

    Args:
        project_dir: The project directory path to validate

    Returns:
        str: The validated project directory as string

    Raises:
        typer.Exit: If invalid characters are found or directory doesn't exist
    """
    if not project_dir.exists():
        logger.error(f"dbt project directory does not exist: {project_dir}")
        raise typer.Exit(1)

    project_dir_str = str(project_dir)
    if any(char in project_dir_str for char in [";", "|", "&", "$", "`"]):
        logger.error("❌ Invalid characters in project directory path")
        raise typer.Exit(1)

    return project_dir_str


@app.command("run")
def run_transformations(
    models: str = typer.Option(
        "",
        "--models",
        "-m",
        help="Specific dbt models to run (e.g., 'staging' or 'marts.fct_transactions')",
    ),
    full_refresh: bool = typer.Option(
        False, "--full-refresh", help="Full refresh of incremental models"
    ),
    project_dir: Path = typer.Option(
        Path("dbt"), "--project-dir", help="dbt project directory"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Run dbt transformations on the loaded DuckDB data.

    This command is a wrapper around 'dbt run' with proper error handling
    and integration with MoneyBin's logging system.

    Args:
        models: Specific models to run (optional)
        full_refresh: Whether to do full refresh of incremental models
        project_dir: dbt project directory
        verbose: Enable debug level logging
    """
    profile = get_current_profile()
    logger.info(f"Starting dbt transformations (Profile: {profile})")

    # Validate inputs
    project_dir_str = _validate_project_dir(project_dir)

    # Build dbt command with input validation
    cmd = ["dbt", "run", "--project-dir", project_dir_str]

    if models:
        _validate_models_parameter(models)
        cmd.extend(["--models", models])
        logger.info(f"Running specific models: {models}")
    else:
        logger.info("Running all dbt models")

    if full_refresh:
        cmd.append("--full-refresh")
        logger.info("Using full refresh mode")

    if verbose:
        cmd.append("--debug")

    try:
        logger.info(f"Executing: {' '.join(cmd)}")

        # Run dbt with real-time output streaming
        if verbose:
            # Stream output in real-time for verbose mode
            process = subprocess.Popen(  # noqa: S603
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            # Stream output line by line
            if process.stdout:
                for line in iter(process.stdout.readline, ""):
                    if line.strip():
                        logger.info(f"dbt: {line.rstrip()}")

            process.wait()
            returncode = process.returncode
        else:
            # Capture output for non-verbose mode
            process = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603

            # Log dbt output
            if process.stdout:
                for line in process.stdout.strip().split("\n"):
                    if line.strip():
                        logger.info(f"dbt: {line}")

            if process.stderr:
                for line in process.stderr.strip().split("\n"):
                    if line.strip():
                        logger.warning(f"dbt stderr: {line}")

            returncode = process.returncode

        if returncode == 0:
            logger.info("✅ dbt transformations completed successfully")
        else:
            logger.error(f"❌ dbt transformations failed with return code {returncode}")
            raise typer.Exit(returncode)

    except FileNotFoundError as e:
        logger.error("❌ dbt command not found. Is dbt installed?")
        logger.info("Install with: uv add dbt-core dbt-duckdb")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"❌ Failed to run dbt transformations: {e}")
        raise typer.Exit(1) from e


@app.command("test")
def run_tests(
    models: str = typer.Option(
        "", "--models", "-m", help="Specific dbt models to test"
    ),
    project_dir: Path = typer.Option(
        Path("dbt"), "--project-dir", help="dbt project directory"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Run dbt tests on the transformed data.

    Args:
        models: Specific models to test (optional)
        project_dir: dbt project directory
        verbose: Enable debug level logging
    """
    logger.info("Starting dbt tests")

    # Validate inputs
    project_dir_str = _validate_project_dir(project_dir)

    # Build dbt command
    cmd = ["dbt", "test", "--project-dir", project_dir_str]

    if models:
        _validate_models_parameter(models)
        cmd.extend(["--models", models])
        logger.info(f"Testing specific models: {models}")
    else:
        logger.info("Running all dbt tests")

    if verbose:
        cmd.append("--debug")

    try:
        logger.info(f"Executing: {' '.join(cmd)}")

        process = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603

        # Log dbt output
        if process.stdout:
            for line in process.stdout.strip().split("\n"):
                if line.strip():
                    logger.info(f"dbt: {line}")

        if process.stderr:
            for line in process.stderr.strip().split("\n"):
                if line.strip():
                    logger.warning(f"dbt stderr: {line}")

        if process.returncode == 0:
            logger.info("✅ dbt tests completed successfully")
        else:
            logger.error(f"❌ dbt tests failed with return code {process.returncode}")
            raise typer.Exit(process.returncode)

    except FileNotFoundError as e:
        logger.error("❌ dbt command not found. Is dbt installed?")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"❌ Failed to run dbt tests: {e}")
        raise typer.Exit(1) from e


@app.command("docs")
def generate_docs(
    project_dir: Path = typer.Option(
        Path("dbt"), "--project-dir", help="dbt project directory"
    ),
    serve: bool = typer.Option(
        False, "--serve", help="Serve documentation after generation"
    ),
    port: int = typer.Option(8080, "--port", help="Port for documentation server"),
) -> None:
    """Generate and optionally serve dbt documentation.

    Args:
        project_dir: dbt project directory
        serve: Whether to serve docs after generation
        port: Port for documentation server
    """
    logger.info("Generating dbt documentation")

    # Validate inputs
    project_dir_str = _validate_project_dir(project_dir)

    try:
        # Generate docs
        cmd = ["dbt", "docs", "generate", "--project-dir", project_dir_str]
        logger.info(f"Executing: {' '.join(cmd)}")

        subprocess.run(cmd, capture_output=True, text=True, check=True)  # noqa: S603
        logger.info("✅ Documentation generated successfully")

        if serve:
            # Serve docs
            cmd = [
                "dbt",
                "docs",
                "serve",
                "--project-dir",
                project_dir_str,
                "--port",
                str(port),
            ]
            logger.info(f"Serving documentation on http://localhost:{port}")
            logger.info("Press Ctrl+C to stop the server")

            subprocess.run(cmd, check=True)  # noqa: S603

    except FileNotFoundError as e:
        logger.error("❌ dbt command not found. Is dbt installed?")
        raise typer.Exit(1) from e
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ dbt command failed: {e}")
        raise typer.Exit(e.returncode) from e
    except Exception as e:
        logger.error(f"❌ Failed to generate documentation: {e}")
        raise typer.Exit(1) from e


@app.command("compile")
def compile_models(
    models: str = typer.Option(
        "", "--models", "-m", help="Specific dbt models to compile"
    ),
    project_dir: Path = typer.Option(
        Path("dbt"), "--project-dir", help="dbt project directory"
    ),
) -> None:
    """Compile dbt models without running them.

    Useful for validating SQL syntax and checking compiled output.

    Args:
        models: Specific models to compile (optional)
        project_dir: dbt project directory
    """
    logger.info("Compiling dbt models")

    # Validate inputs
    project_dir_str = _validate_project_dir(project_dir)

    # Build dbt command
    cmd = ["dbt", "compile", "--project-dir", project_dir_str]

    if models:
        _validate_models_parameter(models)
        cmd.extend(["--models", models])
        logger.info(f"Compiling specific models: {models}")
    else:
        logger.info("Compiling all dbt models")

    try:
        logger.info(f"Executing: {' '.join(cmd)}")

        process = subprocess.run(cmd, capture_output=True, text=True, check=True)  # noqa: S603

        if process.stdout:
            for line in process.stdout.strip().split("\n"):
                if line.strip():
                    logger.info(f"dbt: {line}")

        logger.info("✅ dbt compilation completed successfully")
        logger.info(f"Compiled models available in: {project_dir}/target/compiled/")

    except FileNotFoundError as e:
        logger.error("❌ dbt command not found. Is dbt installed?")
        raise typer.Exit(1) from e
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ dbt compilation failed: {e}")
        if e.stderr:
            logger.error(f"Error details: {e.stderr}")
        raise typer.Exit(e.returncode) from e
    except Exception as e:
        logger.error(f"❌ Failed to compile models: {e}")
        raise typer.Exit(1) from e
