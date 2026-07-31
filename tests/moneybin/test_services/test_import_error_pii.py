"""Raw third-party exception text must not reach the wire."""

from pathlib import Path
from typing import Any

import pytest

from moneybin.services.import_service import ImportService, per_file_failure

# Stands in for the payee/amount/memo content ofxparse and Polars embed in
# their exception strings when they choke on a row.
PII = "SAFEWAY #1234 CARDHOLDER JANE Q PUBLIC 4111111111111111"


def test_ofx_parse_failure_does_not_put_parser_text_on_the_wire(
    db: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ofxparse

    ofx = tmp_path / "statement.ofx"
    ofx.write_text("OFXHEADER:100\n<OFX></OFX>")

    def _boom(*args: object, **kwargs: object) -> Any:
        raise ValueError(f"could not parse line: {PII}")

    monkeypatch.setattr(ofxparse.OfxParser, "parse", staticmethod(_boom))

    with pytest.raises(ValueError) as raised:
        ImportService(db).import_file(ofx, refresh=False)

    message, _code, _hint, _details = per_file_failure(raised.value)
    assert PII not in message, f"parser text reached the wire: {message}"
    assert PII not in str(raised.value)
    # Still actionable: the user must learn the file could not be parsed.
    assert "OFX" in message


def test_transform_failure_does_not_put_polars_text_on_the_wire(
    db: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The tabular twin of the OFX guard.

    Polars conversion errors quote the offending cell, so a failed transform
    would otherwise publish a payee or amount through `PerFileResult.error`.
    The reviewer named only the OFX site; this path has the same shape.
    """

    def _boom(**kwargs: object) -> Any:
        raise ValueError(f"could not convert '{PII}' to date")

    # Patched at its definition module: import_service imports it inside the
    # function body, so the module-attribute route never resolves.
    monkeypatch.setattr(
        "moneybin.extractors.tabular.transforms.transform_dataframe", _boom
    )

    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2024-01-15,Coffee,-4.50\n")

    # auto_accept clears the unknown-layout confirmation gate, which is
    # upstream of the transform stage under test and has its own coverage.
    with pytest.raises(ValueError) as raised:
        ImportService(db).import_file(
            csv,
            refresh=False,
            account_name="Checking",
            confirm=True,
            auto_accept=True,
        )

    message, _code, _hint, _details = per_file_failure(raised.value)
    assert PII not in message, f"parser text reached the wire: {message}"
    assert "Transform failed" in str(raised.value)
