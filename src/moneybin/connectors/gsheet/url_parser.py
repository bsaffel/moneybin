"""Parse Google Sheets URLs into (spreadsheet_id, gid)."""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

_SPREADSHEET_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9_-]+)(?:/|$)")


def parse_sheet_url(url: str) -> tuple[str, int]:
    """Extract (spreadsheet_id, gid) from a Google Sheets URL.

    Handles both /edit#gid=N (fragment) and /edit?gid=N (query string) forms.
    Raises ValueError on malformed URLs, missing gid, or wrong host.
    """
    parsed = urlparse(url)
    if parsed.netloc != "docs.google.com":
        raise ValueError(f"Not a Google Sheets URL: host={parsed.netloc!r}")

    match = _SPREADSHEET_RE.search(parsed.path)
    if not match:
        raise ValueError(f"Could not extract spreadsheet_id from path: {parsed.path!r}")
    spreadsheet_id = match.group(1)
    if not spreadsheet_id:
        raise ValueError("Empty spreadsheet_id")

    gid_str: str | None = None
    if parsed.fragment:
        for part in parsed.fragment.split("&"):
            if part.startswith("gid="):
                gid_str = part[len("gid=") :]
                break
    if gid_str is None and parsed.query:
        q = parse_qs(parsed.query)
        if "gid" in q:
            gid_str = q["gid"][0]
    if gid_str is None:
        raise ValueError("URL is missing gid= (sheet tab is ambiguous)")
    try:
        gid = int(gid_str)
    except ValueError as exc:
        raise ValueError(f"gid is not an integer: {gid_str!r}") from exc

    return spreadsheet_id, gid
