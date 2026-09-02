"""One builder owns the classify → redact → declare pipeline.

`mcp.md` promises "tools contain no privacy enforcement", and
`build_classified_envelope` is what makes that true. A second hand-rolled copy
beside it is how the four steps drift apart on a security-relevant path, so
this guard fails the moment one reappears — the behavioural partner to the
parity tests in `tests/moneybin/test_privacy/test_classified_envelope.py`,
which prove the builder does the same thing the copies did.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCANNED_ROOTS = (
    REPO_ROOT / "src" / "moneybin" / "mcp",
    REPO_ROOT / "src" / "moneybin" / "cli",
)

# The three primitives the builder composes. A surface module calling one
# directly is re-rolling a step the builder already owns.
GUARDED_CALLS = frozenset({
    "extract_data_classes",
    "derive_tier",
    "redact_typed",
})

# Each exemption is a call the builder cannot absorb, with the reason it stays.
EXEMPT: dict[tuple[str, str], str] = {
    ("mcp/decorator.py", "derive_tier"): (
        "The decorator folds a `discloses=` floor into the tier before mapping "
        "it to a Sensitivity enum, so it needs the Tier itself, not the "
        "envelope's sensitivity string."
    ),
    ("mcp/decorator.py", "redact_typed"): (
        "Statically classified tools redact in the decorator, after the tool "
        "body has already built its envelope — there is no builder call to "
        "route through."
    ),
    ("cli/output.py", "redact_typed"): (
        "`render_or_json` redacts an envelope a CLI command already built. "
        "MB-45 routes CLI JSON output through this path; until then the "
        "redaction has to happen on the finished envelope."
    ),
    ("cli/commands/import_inbox.py", "redact_typed"): (
        "Masks one pending entry through its declared type before it becomes "
        "part of a bare-dict payload; there is no typed envelope to build."
    ),
    ("cli/commands/import_cmd.py", "redact_typed"): (
        "Masks a nested `confirmation_payload` inside a bare dict, and an "
        "account proposal rendered to the terminal. Neither value rides a "
        "typed envelope, so neither reaches the envelope's redaction walk."
    ),
    ("mcp/tools/import_tools.py", "redact_typed"): (
        "Redacts the preview payload for the durable `import_previews` row, "
        "which is persisted rather than returned; the tool's own envelope "
        "goes through the builder."
    ),
}


def _surface_modules() -> list[Path]:
    return sorted(path for root in SCANNED_ROOTS for path in root.rglob("*.py"))


def _guarded_calls(path: Path) -> set[str]:
    """Return the guarded primitives ``path`` calls directly."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = (
            func.id
            if isinstance(func, ast.Name)
            else func.attr
            if isinstance(func, ast.Attribute)
            else None
        )
        if name in GUARDED_CALLS:
            found.add(name)
    return found


def test_surfaces_do_not_re_roll_the_classification_pipeline() -> None:
    unexpected: list[str] = []
    for path in _surface_modules():
        relative = path.relative_to(REPO_ROOT / "src" / "moneybin").as_posix()
        for call in sorted(_guarded_calls(path)):
            if (relative, call) not in EXEMPT:
                unexpected.append(f"{relative} calls {call}()")
    assert not unexpected, (
        "These surface modules call a privacy primitive directly instead of "
        "build_classified_envelope() / classify(). Route them through the "
        "builder, or add an EXEMPT entry stating why it cannot absorb the "
        f"call: {unexpected}"
    )


@pytest.mark.parametrize(("module", "call"), sorted(EXEMPT))
def test_every_exemption_still_describes_a_real_call(module: str, call: str) -> None:
    """A stale exemption is a hole that reopens silently when code moves."""
    path = REPO_ROOT / "src" / "moneybin" / module
    assert path.exists(), f"EXEMPT names a module that no longer exists: {module}"
    assert call in _guarded_calls(path), (
        f"EXEMPT claims {module} calls {call}(), but it no longer does. Drop the entry."
    )
