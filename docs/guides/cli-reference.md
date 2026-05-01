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
├── categorize       Categorization management
│   ├── apply-rules  Run rules + merchants on uncategorized transactions
│   ├── bulk         Bulk-assign categories from a JSON file or stdin
│   ├── seed         Initialize default categories (Plaid PFCv2)
│   ├── summary      Coverage statistics
│   ├── list-rules   Display active manual rules
│   └── auto         Auto-rule sub-group
│       ├── review   List pending auto-rule proposals
│       ├── confirm  Approve/reject auto-rule proposals
│       ├── rules    List active auto-generated rules
│       └── stats    Auto-rule health (active, pending, categorized)
│
├── matches          Dedup + transfer review
│   ├── run          Run matcher against existing transactions
│   ├── review       Review pending match proposals (interactive or flagged)
│   ├── history      Show recent match decisions
│   ├── undo         Reverse a match decision
│   └── backfill     One-time scan of existing data for latent matches
│
├── transform        SQLMesh pipeline
│   ├── apply        Apply pending changes
│   ├── plan         Preview what will change
│   ├── status       Current model state
│   ├── validate     Check model SQL parses correctly
│   ├── audit        Run data quality audits
│   └── restate      Force recompute a model for a date range
│
├── track            Balance, net worth, budgets, recurring (🗓️/📐)
│   ├── balance show
│   ├── networth show
│   ├── budget show
│   ├── recurring show
│   └── investments show
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
moneybin matches review                # Review any pending dedup/transfer proposals
moneybin categorize apply-rules        # Apply rules + merchants
moneybin categorize auto review        # Inspect auto-rule proposals
moneybin categorize auto confirm --approve-all
moneybin categorize summary
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
