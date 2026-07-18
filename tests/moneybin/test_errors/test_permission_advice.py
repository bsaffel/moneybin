"""Table-driven tests for the permission-failure advice policy.

Split deliberately from OS behavior: this file tests OUR branching given
(errno, platform, path). That macOS reports EPERM (errno 1, "Operation not
permitted") for a TCC denial while a mode denial reports EACCES (errno 13) is
an observed fact, verified by hand probe 2026-07-18 — it is not asserted here,
because a test that fabricates the input cannot verify the OS.
"""

from pathlib import Path

import pytest

from moneybin.errors import permission_advice

EACCES = 13
EPERM = 1


def test_eacces_advises_ownership_and_mode() -> None:
    hint, details = permission_advice(EACCES, "Darwin", Path.home() / "Documents/a.pdf")
    assert "chmod" in hint or "ownership" in hint
    assert "Full Disk Access" not in hint
    assert details["errno"] == EACCES


def test_eperm_under_protected_root_on_macos_advises_full_disk_access() -> None:
    hint, details = permission_advice(EPERM, "Darwin", Path.home() / "Documents/a.pdf")
    assert "Full Disk Access" in hint
    assert "Privacy & Security" in hint
    assert details["protected_root"] == "~/Documents"
    assert details["platform"] == "Darwin"


@pytest.mark.parametrize("root", ["Documents", "Desktop", "Downloads"])
def test_all_three_protected_roots_are_recognized(root: str) -> None:
    hint, details = permission_advice(EPERM, "Darwin", Path.home() / root / "a.pdf")
    assert "Full Disk Access" in hint
    assert details["protected_root"] == f"~/{root}"


def test_eperm_outside_protected_root_stays_generic() -> None:
    """Not every EPERM is TCC — immutable flags and sandboxes produce it too.

    The path sits under home but outside the three gated roots, so it fails the
    root check alone — a path off home entirely would also fail it for being
    off home, proving less about which condition fired.
    """
    hint, details = permission_advice(EPERM, "Darwin", Path.home() / "Music/a.pdf")
    assert "Full Disk Access" not in hint
    assert details.get("protected_root") is None


def test_eperm_without_a_path_stays_generic() -> None:
    """An exception carrying no filename must not be judged by the cwd.

    Falling back to `Path()` would hand the Full-Disk-Access advice to any
    EPERM that happened to be raised while the working directory sat under a
    protected root — advice aimed at a path that never failed.
    """
    hint, details = permission_advice(EPERM, "Darwin", None)
    assert "Full Disk Access" not in hint
    assert details.get("protected_root") is None


def test_unrelated_errno_under_protected_root_stays_generic() -> None:
    """Only EPERM may reach the protected-root test — not "anything but EACCES".

    Every other condition of the conjunction is satisfied (Darwin, a path under
    ~/Documents), so a pass here proves the errno arm alone held. Guarding on
    `!= EACCES` instead of `== EPERM` would hand System-Settings advice to any
    future errno that happened to land in this branch.
    """
    hint, details = permission_advice(99, "Darwin", Path.home() / "Documents/a.pdf")
    assert "Full Disk Access" not in hint
    assert "System Settings" not in hint
    assert details.get("protected_root") is None


def test_eperm_on_linux_stays_generic() -> None:
    hint, _ = permission_advice(EPERM, "Linux", Path.home() / "Documents/a.pdf")
    assert "Full Disk Access" not in hint
    assert "System Settings" not in hint
