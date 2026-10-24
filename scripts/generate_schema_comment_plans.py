"""Regenerate the precomputed schema comment plans shipped with the package.

Usage:
    uv run python scripts/generate_schema_comment_plans.py

Writes ``src/moneybin/sql/schema/comment_plans.json``: for every core schema
DDL file, the sha256 of its text beside the sqlglot-derived comment plan, so a
fresh process skips the parse. At runtime a plan is used only when the hash
matches the file on disk; otherwise ``schema.py`` parses as before.
``tests/moneybin/test_schema_comment_plans.py`` fails CI if the checked-in file
is stale.
"""

from __future__ import annotations

import json
import sys
from hashlib import sha256
from typing import Any

from moneybin import schema


def build() -> dict[str, dict[str, Any]]:
    """Derive the artifact content from the DDL files on disk."""
    out: dict[str, dict[str, Any]] = {}
    for path in schema._all_schema_files():  # pyright: ignore[reportPrivateUsage]
        sql = path.read_text()
        key = path.resolve().relative_to(schema._PACKAGE_DIR).as_posix()  # pyright: ignore[reportPrivateUsage]
        out[key] = {
            "sha256": sha256(sql.encode()).hexdigest(),
            "plan": schema.plan_to_json(schema._derive_comment_plan(sql)),  # pyright: ignore[reportPrivateUsage]
        }
    return dict(sorted(out.items()))


def render() -> str:
    """Render the artifact as stable, diff-friendly JSON text."""
    return json.dumps(build(), indent=1, ensure_ascii=False) + "\n"


def main() -> int:
    """Write the artifact; return a process exit code."""
    schema.COMMENT_PLANS_PATH.write_text(render())
    print(f"Wrote {schema.COMMENT_PLANS_PATH}")  # noqa: T201  # script output
    return 0


if __name__ == "__main__":
    sys.exit(main())
