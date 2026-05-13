"""Shared process-inspection utilities for database lock management.

Used by both ``db ps`` (CLI) and ``_lock_error_message()`` in database.py.
Never raises — all subprocess failures return empty results.
"""

from __future__ import annotations

import logging
import os
import subprocess  # noqa: S404 — subprocess used for lsof/ps; static args only
from pathlib import Path

logger = logging.getLogger(__name__)


def find_blocking_processes(db_path: Path) -> list[dict[str, str | int]]:
    """Find processes that have db_path open, excluding the current process.

    Returns list of dicts with keys: pid (int), command (str), cmdline (str).
    Returns [] when lsof is unavailable, times out, or no other process has
    the file open. Never raises.
    """
    own_pid = os.getpid()
    try:
        result = subprocess.run(  # noqa: S603 — lsof with static args
            ["lsof", "-F", "pcn", str(db_path)],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=5,
        )
    except FileNotFoundError:
        logger.debug("lsof not found; cannot enumerate processes holding %s", db_path)
        return []
    except subprocess.TimeoutExpired:
        logger.debug("lsof timed out inspecting %s", db_path)
        return []
    except Exception:  # noqa: BLE001 — lsof can fail in containers/sandbox
        logger.debug("lsof failed inspecting %s", db_path, exc_info=True)
        return []

    if not result.stdout:
        return []

    processes: list[dict[str, str | int]] = []
    seen_pids: set[int] = set()
    current_pid: int | None = None
    current_cmd: str = ""

    for line in result.stdout.splitlines():
        if line.startswith("p"):
            current_pid = int(line[1:])
            current_cmd = ""
        elif line.startswith("c") and current_pid is not None:
            current_cmd = line[1:]
        elif (
            line.startswith("n")
            and current_pid is not None
            and current_pid not in seen_pids
        ):
            seen_pids.add(current_pid)
            if current_pid == own_pid:
                continue
            try:
                ps_result = subprocess.run(  # noqa: S603
                    ["ps", "-p", str(current_pid), "-o", "args="],  # noqa: S607
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                cmdline = ps_result.stdout.strip()
            except Exception:  # noqa: BLE001
                cmdline = current_cmd
            processes.append({
                "pid": current_pid,
                "command": current_cmd,
                "cmdline": cmdline,
            })

    return processes


def describe_process(cmdline: str) -> str:
    """Convert a process argv string to a human-readable name (first match wins)."""
    c = cmdline.strip()
    # Strip leading path prefix (e.g., /home/user/.venv/bin/moneybin → moneybin ...)
    for prefix_sep in ["/moneybin ", "/moneybin\t"]:
        idx = c.find(prefix_sep)
        if idx != -1:
            c = "moneybin " + c[idx + len(prefix_sep) :]
            break

    if "moneybin mcp serve" in c:
        return "MCP server"
    if "moneybin transform apply" in c:
        return "transform pipeline"
    if "moneybin import inbox sync" in c:
        return "inbox sync"
    if "moneybin import" in c:
        return "import command"
    if c == "moneybin sync" or c.startswith("moneybin sync "):
        return "Plaid sync"
    if "moneybin web" in c or ("uvicorn" in c and "moneybin" in c):
        return "Web UI server"
    if c.startswith("moneybin "):
        tokens = c.split()
        return f"moneybin {tokens[1]}" if len(tokens) > 1 else "moneybin"
    if "duckdb --ui" in c or c.startswith("duckdb-ui"):
        return "DuckDB UI"
    if c.startswith("duckdb"):
        return "DuckDB shell"
    return c[:40].rstrip()
