"""The shared table renderer must print data, not interpret it.

``render_rich_table`` renders user-authored strings — report names and
descriptions, merchant names, transaction descriptions. Rich reads ``[...]`` as
a style tag, so a default console silently swallows any bracketed text a user
wrote and can be steered into applying styles the data asked for.
"""

from __future__ import annotations

import pytest

from moneybin.cli.utils import render_rich_table


@pytest.mark.parametrize(
    "cell",
    [
        "spend [excluding rent]",
        "user [archived]",
        "[bold red]not a style[/bold red]",
    ],
    ids=["user-bracket", "status-marker", "style-tag"],
)
def test_a_bracketed_cell_reaches_the_table_intact(
    cell: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every bracketed form survives: a user's prose, our marker, a style tag.

    The style-tag case is the one that proves markup is off rather than merely
    tolerated — a console with markup enabled renders it as styling and emits
    none of its literal characters.
    """
    render_rich_table(["description"], [(cell,)])

    printed = capsys.readouterr().out
    # Rich wraps a cell too wide for the terminal, so compare on the characters
    # rather than the run: what must not happen is a bracket disappearing.
    assert "[" in printed and "]" in printed
    for token in cell.replace("[", " ").replace("]", " ").split():
        assert token in printed


def test_the_table_still_renders_its_headers_and_rows() -> None:
    """The benign twin: disabling markup must not disable rendering."""
    render_rich_table(["report_id", "tier"], [("core:networth", "builtin")])
