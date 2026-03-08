# SQLMesh Project

This directory contains the SQLMesh models for MoneyBin's medallion data pipeline.

## Structure

```
sqlmesh/
├── config.py              # SQLMesh config (reads DB path from MoneyBin settings)
├── external_models.yaml   # Definitions for raw tables produced by loaders
└── models/
    ├── prep/              # Staging views (light cleaning, type casting)
    └── core/              # Canonical tables (deduped, multi-source)
```

## Running Transformations via CLI

```bash
# Preview pending changes
moneybin transform plan

# Apply changes immediately
moneybin transform apply

# Or combine into one step
moneybin transform plan --apply
```

You can also invoke SQLMesh directly from this directory:

```bash
cd sqlmesh/
uv run sqlmesh plan
uv run sqlmesh run
```

## Browsing Models in VS Code (Recommended)

SQLMesh's web UI is deprecated. The official replacement is the
[SQLMesh VS Code extension](https://marketplace.visualstudio.com/items?itemName=Tobiko-Data.sqlmesh).

### Setup

1. **Install the extension** from the VS Code marketplace:
   [Tobiko-Data.sqlmesh](https://marketplace.visualstudio.com/items?itemName=Tobiko-Data.sqlmesh)

2. **Open this `sqlmesh/` directory** (or the workspace root) in VS Code.
   The extension auto-discovers `config.py` in the workspace.

3. **Set the Python interpreter** to the project's virtual environment:
   `Cmd+Shift+P` → *Python: Select Interpreter* → choose `.venv` at the repo root.

4. The extension activates automatically and provides:
   - Model lineage graph
   - Column-level lineage
   - Inline SQL validation
   - Plan/apply from the sidebar

### Docs

- [Extension guide](https://sqlmesh.readthedocs.io/en/stable/guides/vscode/)
- [SQLMesh concepts](https://sqlmesh.readthedocs.io/en/stable/concepts/overview/)
