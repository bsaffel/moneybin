# CLI Reference

MoneyBin's CLI is organized by domain in workflow order. Every command supports `--help`.

## Global Options

| Option | Short | Description |
|--------|-------|-------------|
| `--profile` | `-p` | Use a specific profile for this command |
| `--verbose` | `-v` | Enable debug logging |

Most query/list commands also accept `--output json` for machine-readable parity with the MCP server.

## Command Tree

```
moneybin
├── profile          Profile lifecycle
│   ├── create       Create a new profile
│   ├── list         List all profiles (marks active)
│   ├── switch       Change default profile
│   ├── delete       Delete a profile and all data
│   ├── show         Show resolved settings
│   └── set          Set a config value
│
├── import           File import
│   ├── file         Import a financial file (auto-detects type)
│   ├── status       Summary of all imported data
│   ├── history      List recent imports with batch details
│   ├── preview      Preview file structure (dry run)
│   ├── revert       Undo an import batch
│   └── formats      Manage saved import formats
│       ├── list     List all available formats
│       ├── show     Show format details
│       └── delete   Delete a user-saved format
│
├── sync             Bank sync via moneybin-server (📐 designed)
│   ├── login / logout
│   ├── connect / disconnect
│   ├── pull / status
│   ├── rotate-key
│   └── schedule {set,show,remove}
│
├── accounts         Financial account entities + per-account workflows
│   ├── list / show / rename / include  Entity ops (🗓️ account-management.md)
│   ├── balance      Per-account balance tracking (📐 net-worth.md)
│   │   ├── show / list / history
│   │   ├── assert   Assert balance at a point in time
│   │   ├── delete / reconcile
│   └── investments  Holdings, cost basis (🗓️ investment-tracking.md)
│       ├── show / list / holdings
│
├── transactions     Transactions + workflows on them
│   ├── review       Unified review queue
│   │   --status                         Counts pending matches + categorize
│   │   --type {matches,categorize,all}  Pick a queue
│   │   --confirm <id> / --reject <id>   Non-interactive item action
│   │   --confirm-all / --limit N
│   ├── matches      Dedup + transfer matching
│   │   ├── run / history / undo / backfill
│   └── categorize   Categorization workflow
│       ├── apply    Assign categories from JSON
│       ├── stats    Coverage statistics
│       ├── rules    Manual rule sub-group {list, apply, create, delete}
│       ├── auto     Auto-rule sub-group {review, confirm, rules, stats}
│       └── ml       ML-suggested categorization (📐)
│
├── categories       Taxonomy reference data (top-level)
│   └── list / create / toggle / delete
│
├── merchants        Merchant name mappings (top-level)
│   └── list / create
│
├── reports          Cross-domain analytical views
│   ├── networth     Replaces v1 `track networth` — show / history (📐)
│   ├── spending / cashflow / budget / health (📐 owning specs)
│
├── assets           Physical assets (🗓️ asset-tracking.md)
│
├── tax              W-2 forms, deductions (🗓️ owning specs)
│   └── w2 / deductions
│
├── budget           Budget mutation (🗓️ budget-tracking.md)
│   └── set / delete    (read views live under `reports budget`)
│
├── system           System-level orientation
│   └── status       Data inventory + pending review queues
│
├── transform        SQLMesh pipeline
│   ├── apply        Apply pending changes
│   ├── plan         Preview what will change
│   ├── status       Current model state
│   ├── validate     Check model SQL parses correctly
│   ├── audit        Run data quality audits
│   └── restate      Force recompute a model for a date range
│
├── stats            Lifetime metric aggregates (leaf)
│
├── export           Export to CSV/Excel/Sheets (🗓️ planned)
│   └── run
│
├── mcp              MCP server
│   ├── serve        Start the MCP server
│   ├── list-tools   List all registered tools
│   ├── list-prompts List all registered prompts
│   └── config
│       └── generate Generate client config (Claude Desktop, Cursor, Windsurf)
│
├── db               Database management
│   ├── init         Create a new encrypted database
│   ├── shell        Interactive DuckDB SQL shell
│   ├── ui           DuckDB web UI (browser-based)
│   ├── query        Run a SQL query
│   ├── info         Database metadata (size, tables, encryption, versions)
│   ├── backup       Create timestamped backup
│   ├── restore      Restore from a backup file
│   ├── lock / unlock
│   ├── key          Print the encryption key
│   ├── rotate-key   Re-encrypt with a new key
│   ├── ps / kill    Inspect or kill processes holding the DB
│   └── migrate {apply,status}
│
├── logs <stream>    View logs for cli/mcp/sqlmesh (leaf; --print-path, --prune)
│
└── synthetic        Test data + scenario verification
    ├── generate     Generate synthetic data for a persona
    ├── reset        Wipe and regenerate from scratch
    └── verify       Run scenario suites (--list, --scenario, --all)
```

Commands marked 📐 (designed) or 🗓️ (planned) reserve the namespace and print a pointer to the owning spec when invoked.

## Common Workflows

### First-time setup

```bash
moneybin profile create personal
moneybin import file ~/Downloads/checking.qfx
```

### Import, dedup, and categorize

```bash
moneybin import file ~/Downloads/chase_may.csv --account-name "Chase Checking"
moneybin transactions review --status              # Counts pending matches + categorize
moneybin transactions review --type matches        # Review pending dedup/transfer proposals
moneybin transactions categorize rules apply       # Apply rules + merchants
moneybin transactions categorize auto review       # Inspect auto-rule proposals
moneybin transactions categorize auto confirm --approve-all
moneybin transactions categorize stats
```

`import file` runs the matcher and rule-based categorization automatically. The explicit commands above are useful when reviewing pending work or tuning behavior.

### Query your data

```bash
moneybin db query "SELECT category, SUM(amount) FROM core.fct_transactions GROUP BY 1"
moneybin db shell
```

### Connect an AI assistant

```bash
moneybin mcp config generate --client claude-desktop --install
```

### Verify the pipeline (developer)

Whole-pipeline scenarios run as pytest tests under `tests/scenarios/`:

```bash
make test-scenarios                                    # Run the full scenario suite
uv run pytest tests/scenarios/ -m scenarios -v         # Same, via pytest directly
```

### Database maintenance

```bash
moneybin db backup
moneybin db info
moneybin db migrate status
```
