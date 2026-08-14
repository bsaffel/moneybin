"""Hypothesis profile for MoneyBin's property tests.

pytest runs `-n auto --dist=loadscope`. A per-example deadline under xdist
fails on scheduler latency rather than on slow code, so deadlines are off; the
example database is disabled so parallel workers do not contend for one file.

Losing the database means a shrunk counterexample is not replayed automatically
on the next run. Paste it into an example test instead — a counterexample worth
keeping is worth pinning by name.
"""

from __future__ import annotations

from hypothesis import settings

settings.register_profile("moneybin", deadline=None, database=None)
settings.load_profile("moneybin")
