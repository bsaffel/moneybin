"""exchange_mic_map seed: one row per alias, identity rows present."""

import csv
from pathlib import Path

_CSV = (
    Path(__file__).parents[2] / "sqlmesh" / "models" / "seeds" / "exchange_mic_map.csv"
)


def test_mic_map_well_formed() -> None:
    with _CSV.open() as f:
        rows = list(csv.DictReader(f))
    assert rows, "seed CSV is empty"
    aliases = [r["alias"] for r in rows]
    assert len(aliases) == len(set(aliases)), "grain violation: duplicate alias"
    assert all(r["alias"] == r["alias"].strip().upper() for r in rows)
    mics = {r["mic"] for r in rows}
    identity = {r["alias"] for r in rows if r["alias"] == r["mic"]}
    assert mics == identity, "every MIC needs an identity row (alias == mic)"
