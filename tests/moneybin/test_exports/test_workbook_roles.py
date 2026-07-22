"""Concurrency tests for exclusive inbound/output workbook roles."""

from __future__ import annotations

import threading
from pathlib import Path

from moneybin.exports.workbook_roles import workbook_role_lease


def test_same_workbook_role_lease_serializes_threads(tmp_path: Path) -> None:
    """A second role decision cannot pass while publication owns the workbook."""
    database_path = tmp_path / "moneybin.duckdb"
    attempted = threading.Event()
    entered = threading.Event()

    def contend() -> None:
        attempted.set()
        with workbook_role_lease(database_path, "private-workbook-id"):
            entered.set()

    with workbook_role_lease(database_path, "private-workbook-id"):
        thread = threading.Thread(target=contend)
        thread.start()
        assert attempted.wait(timeout=2.0)
        assert not entered.wait(timeout=0.1)

    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert entered.is_set()
    assert all(
        "private-workbook-id" not in path.name
        for path in tmp_path.glob(".workbook-role-*.lock")
    )
