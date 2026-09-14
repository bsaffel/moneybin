# Triaging scenario failures by symptom

Cited from [`.claude/rules/testing.md`](../rules/testing.md) §"Triaging Scenario
Failures by Symptom". Read this when a scenario fails and you are deciding where
to look; the derivation rule it defers to is §"Scenario Expectations Must Be
Independently Derived" in the same file.

`ScenarioResult.failure_summary()` (`tests/scenarios/_runner/result.py`) is the
only output pytest and CI ever show, so triage from its lines. The runner
separates a check that **crashed** from one that returned a verdict and says
which — either `crashed,` in the line or a `halted:` reason naming the phase.
Start from the named assertion, expectation, or evaluation, and fix code before
touching a YAML expectation, per the derivation rule in `testing.md`.

| Summary line | What it tells you | Where to look |
|---|---|---|
| `halted: catalog wiring failed pre-flight` | The SQLMesh catalog disagreed before the pipeline ran | `assert_sqlmesh_catalog_matches` and the model catalog |
| `halted: pipeline step crashed: <Type>` | A pipeline step raised | `tests/scenarios/_runner/steps.py` and the called service |
| `halted: expectations crashed: <kind> (<Type>)` | That adapter raised, aborting the rest | The adapter registered for `<kind>` in `_expectation_registry.py` |
| `halted: extra_assertions crashed: <Type>` | The scenario's own callback raised | That scenario's `extra_assertions` |
| `assertion <name>: crashed, ...` | The runner caught an exception out of the assertion fn | That assertion's own implementation |
| `evaluation <name>: crashed, ...` | The runner caught an exception out of the evaluation fn; its `0.0` is a placeholder, not a measured score | That evaluation's own implementation |
| `assertion <name>: ...`, `evaluation <name>: <metric>=... < threshold=...`, or `expectation <name>` | The named implementation ran and decided this, and wrote the message itself | That implementation first, then the pipeline step owning the data, then the fixture or scenario YAML if the expectation is stale |

An assertion's implementation is either the shared assertion library or the
scenario's own `extra_assertions` callback — the name in the result tells you
which, and the two fail identically otherwise. An assertion whose job is to
catch (`assert_empty_input_safe`) reports a verdict, not a crash.

Every halted result carries the passed pre-flight assertion, and the later
halts carry the scenario's own assertions too, so a populated assertion list is
not evidence the run got past the phase `halted` names. Read the string.

The `Scenarios` CI workflow shards `pytest -m scenarios` four ways and uploads
one `pytest-json-report` artifact per shard — `scenarios-results-<group>`
containing `scenarios-<group>.json` (group `1`-`4`) — pull that instead of
scraping logs.
